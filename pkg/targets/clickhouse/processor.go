package clickhouse

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	ch "github.com/ClickHouse/clickhouse-go"
	"github.com/ClickHouse/clickhouse-go/lib/data"
	"github.com/jmoiron/sqlx"
	"github.com/timescale/tsbs/pkg/targets"
)

// load.Processor interface implementation
type processor struct {
	db   *sqlx.DB
	db2  ch.Clickhouse
	http http.Client
	_csi *syncCSI
	conf *ClickhouseConfig
}

const (
	insertTagsSQL = `INSERT INTO tags(%s) VALUES %s ON CONFLICT DO NOTHING RETURNING *`
	getTagsSQL    = `SELECT * FROM tags`
	numExtraCols  = 2 // one for json, one for tags_id
)

// load.Processor interface implementation
func (p *processor) Init(workerNum int, doLoad, hashWorkers bool) {
	if doLoad {
		p.db = sqlx.MustConnect(dbType, getConnectString(p.conf, true))
		p.db2, _ = ch.OpenDirect(getConnectString(p.conf, true))
		p.http = http.Client{Transport: &http.Transport{
			MaxConnsPerHost: 100,
		}}

		if hashWorkers {
			p._csi = newSyncCSI()
		} else {
			p._csi = globalSyncCSI
		}
	}
}

// load.ProcessorCloser interface implementation
func (p *processor) Close(doLoad bool) {
	if doLoad {
		p.db.Close()
		p.db2.Close()
	}
}

func (p *processor) splitTagsAndMetrics(rows []*insertData, dataCols int, data_as_line bool) ([][]string, [][]interface{}, uint64) {
	tagRows := make([][]string, 0, len(rows))
	dataRows := make([][]interface{}, 0, len(rows))
	numMetrics := uint64(0)
	commonTagsLen := len(tableCols[tagsKey])
	metricsInRow := uint64(len(strings.Split(rows[0].fields, ",")) - 1)

	for _, data := range rows {
		// Split the tags into individual common tags and an extra bit leftover
		// for non-common tags that need to be added separately. For each of
		// the common tags, remove everything before = in the form <label>=<val>
		// since we won't need it.
		tags := strings.SplitN(data.tags, ",", commonTagsLen+1)
		for i := 0; i < commonTagsLen; i++ {
			tags[i] = strings.Split(tags[i], "=")[1]
		}

		var json string
		if len(tags) > commonTagsLen {
			json = subsystemTagsToJSON(strings.Split(tags[commonTagsLen], ","))
		}

		//ts := time.Unix(0, timeInt)
		if data_as_line {
			r := make([]interface{}, 1)
			r = append(r, data.fields+","+json)
			dataRows = append(dataRows, r)
			numMetrics += metricsInRow

		} else {
			metrics := strings.Split(data.fields, ",")
			numMetrics += metricsInRow

			timestampNano, err := strconv.ParseInt(metrics[0], 10, 64)
			if err != nil {
				panic(err)
			}
			// use nil at 2-nd position as placeholder for tagKey
			r := make([]interface{}, 0, dataCols)
			// First columns in table are

			// tags_id - would be nil for now
			// additional_tags
			r = append(r,
				int32(timestampNano/1000000000), // unix
				nil,                             // tags_id
				json)                            // additional_tags

			if p.conf.InTableTag {
				r = append(r, tags[0]) // tags[0] = hostname
			}
			for _, v := range metrics[1:] {
				if v == "" {
					r = append(r, nil)
					continue
				}
				f64, err := strconv.ParseFloat(v, 64)
				if err != nil {
					panic(err)
				}
				r = append(r, f64)
			}
			// use nil at 2nd position as placeholder for tagKey
			dataRows = append(dataRows, r)
		}

		tagRows = append(tagRows, tags[:commonTagsLen])
	}

	return tagRows, dataRows, numMetrics
}

// load.Processor interface implementation
func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (uint64, uint64) {
	batches := b.(*tableArr)
	rowCnt := 0
	metricCnt := uint64(0)
	for tableName, rows := range batches.m {
		rowCnt += len(rows)
		if doLoad {
			start := time.Now()
			metricCnt += p.processCSI(tableName, rows)

			if p.conf.LogBatches {
				now := time.Now()
				took := now.Sub(start)
				batchSize := len(rows)
				fmt.Printf("BATCH: batchsize %d row rate %f/sec (took %v)\n", batchSize, float64(batchSize)/took.Seconds(), took)
			}
		}
	}
	batches.m = map[string][]*insertData{}
	batches.cnt = 0

	return metricCnt, uint64(rowCnt)
}

func newSyncCSI() *syncCSI {
	return &syncCSI{
		m:     make(map[string]int64),
		mutex: &sync.RWMutex{},
	}
}

type syncCSI struct {
	// Map hostname to tags.id for this host
	m     map[string]int64
	mutex *sync.RWMutex
}

// globalSyncCSI is used when data is not hashed by some function to a worker consistently so
// therefore all workers need to know about the same map from hostname -> tags_id
var globalSyncCSI = newSyncCSI()

func (p *processor) insertHTTP(tableName string, colLen int, dataRows *[][]interface{}, tags_id *[]int32) {

	cols := make([]string, 0, colLen)
	// First columns would be "unix", "tags_id", "additional_tags"
	// Inspite of "additional_tags" being added the last one in CREATE TABLE stmt
	// it goes as a third one here - because we can move columns - they are named
	// and it is easier to keep variable coumns at the end of the list

	cols = append(cols, "unix")
	if p.conf.InTableTag {
		cols = append(cols, tableCols["tags"][0]) // hostname
	}
	cols = append(cols, tableCols[tableName]...)
	cols = append(cols, "additional_tags,tags_id")

	sql := fmt.Sprintf(`INSERT INTO %s (%s) FORMAT CSV`, tableName, strings.Join(cols, ","))

	bf := bytes.NewBufferString("")

	for i := 0; i < len(*dataRows); i++ {
		bf.WriteString((*dataRows)[i][1].(string) + "," + strconv.FormatInt(int64((*tags_id)[i]), 10) + ",\n")
	}

	var urlx = fmt.Sprintf("http://%s:8123?user=%s&password=%s&database=%s&async_insert=1&wait_for_async_insert=0&query=%s", p.conf.Host, url.QueryEscape(p.conf.User), url.QueryEscape(p.conf.Password), p.conf.DbName, url.QueryEscape(sql))
	resp, err := p.http.Post(urlx, "application/octet-stream", bf)
	if err != nil {
		panic(err)
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
}

func (p *processor) insertDefault(tableName string, colLen int, dataRows *[][]interface{}, tags_id *[]int32) {

	tagsIdPosition := 1 // what is the position of the tags_id in the row - nil value

	for i := range *dataRows {
		// tagKey = hostname
		// Insert id of the tag (tags.id) for this host into tags_id position of the dataRows record
		// refers to
		// nil,		// tags_id

		(*dataRows)[i][tagsIdPosition] = (*tags_id)[i]
	}

	// Prepare column names
	cols := make([]string, 0, colLen)
	// First columns would be "created_date", "created_at", "time", "tags_id", "additional_tags"
	// Inspite of "additional_tags" being added the last one in CREATE TABLE stmt
	// it goes as a third one here - because we can move columns - they are named
	// and it is easier to keep variable coumns at the end of the list
	cols = append(cols, "created_at", "tags_id", "additional_tags")
	if p.conf.InTableTag {
		cols = append(cols, tableCols["tags"][0]) // hostname
	}
	cols = append(cols, tableCols[tableName]...)

	// INSERT statement template
	sql := fmt.Sprintf(`
			INSERT INTO %s (
				%s
			) VALUES (
				%s
			)
			`,
		tableName,
		strings.Join(cols, ","),
		strings.Repeat(",?", len(cols))[1:]) // We need '?,?,?', but repeat ",?" thus we need to chop off 1-st char

	tx := p.db.MustBegin()
	stmt, err := tx.Prepare(sql)
	if err != nil {
		panic(err)
	}
	for _, r := range *dataRows {
		_, err := stmt.Exec(r...)
		if err != nil {
			panic(err)
		}
	}
	err = stmt.Close()
	if err != nil {
		panic(err)
	}
	err = tx.Commit()
	if err != nil {
		panic(err)
	}
}

func (p *processor) insertColumnar(tableName string, colLen int, dataRows *[][]interface{}, tags_id *[]int32) {

	// Prepare column names
	cols := make([]string, 0, colLen)
	// First columns would be "created_date", "created_at", "time", "tags_id", "additional_tags"
	// Inspite of "additional_tags" being added the last one in CREATE TABLE stmt
	// it goes as a third one here - because we can move columns - they are named
	// and it is easier to keep variable coumns at the end of the list
	cols = append(cols, "created_at", "tags_id", "additional_tags")
	if p.conf.InTableTag {
		cols = append(cols, tableCols["tags"][0]) // hostname
	}
	cols = append(cols, tableCols[tableName]...)

	// INSERT statement template
	sql := fmt.Sprintf(`
				INSERT INTO %s (
					%s
				) VALUES (
					%s
				)
				`,
		tableName,
		strings.Join(cols, ","),
		strings.Repeat(",?", len(cols))[1:]) // We need '?,?,?', but repeat ",?" thus we need to chop off 1-st char

	var tx ch.Clickhouse
	var err error

	tx, err = ch.OpenDirect(getConnectString(p.conf, true))
	if err != nil {
		log.Fatal(err)
	}

	if _, err := tx.Begin(); err == nil {
		if _, err := tx.Prepare(sql); err == nil {
			block, err := tx.Block()
			if err != nil {
				log.Fatal(err)
			}
			writeBatch(block, dataRows, tags_id, []string{"int32", "tags_id", "string", "float64", "float64", "float64", "float64", "float64", "float64", "float64", "float64", "float64", "float64"})
			err = tx.WriteBlock(block)
			if err != nil {
				log.Fatal(err)
			}
			err = tx.Commit()
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	err = tx.Close()
	if err != nil {
		panic(err)
	}
}

// Process part of incoming data - insert into tables
func (p *processor) processCSI(tableName string, rows []*insertData) uint64 {
	//tagRows := make([][]string, 0, len(rows))
	//dataRows := make([][]interface{}, 0, len(rows))
	//commonTagsLen := len(tableCols["tags"])

	colLen := len(tableCols[tableName]) + numExtraCols

	tagRows, dataRows, numMetrics := p.splitTagsAndMetrics(rows, colLen, p.conf.InsertType == "HTTP" || p.conf.InsertType == "NothingSplit")

	// Check if any of these tags has yet to be inserted
	newTags := make([][]string, 0, len(rows))

	tagsId := make([]int32, 0, len(rows))

	p._csi.mutex.RLock()
	for i, tagRow := range tagRows {
		if _, ok := p._csi.m[tagRow[0]]; !ok {
			newTags = append(newTags, tagRow)
		} else {
			tagKey := tagRows[i][0]
			tagsId = append(tagsId, int32(p._csi.m[tagKey]))
		}
	}
	p._csi.mutex.RUnlock()

	if len(newTags) > 0 {
		p._csi.mutex.Lock()
		res := p.insertTags(p.conf, p.db, len(p._csi.m), newTags, true)
		for hostName, tagsId := range res {
			p._csi.m[hostName] = tagsId
		}
		p._csi.mutex.Unlock()

		p._csi.mutex.RLock()
		tagsId = make([]int32, 0, len(rows))
		for i := range tagRows {
			tagsId = append(tagsId, int32(p._csi.m[tagRows[i][0]]))
		}
		p._csi.mutex.RUnlock()
	}

	if p.conf.InTableTag {
		colLen++
	}

	if p.conf.InsertType == "HTTP" {
		p.insertHTTP(tableName, colLen, &dataRows, &tagsId)
	} else if p.conf.InsertType == "Columnar" {
		p.insertColumnar(tableName, colLen, &dataRows, &tagsId)
	} else if p.conf.InsertType == "Default" {
		p.insertDefault(tableName, colLen, &dataRows, &tagsId)
	} else if p.conf.InsertType == "Nothing" {

	}

	return numMetrics
}

// insertTags fills tags table with values
func (p *processor) insertTags(conf *ClickhouseConfig, db *sqlx.DB, startID int, rows [][]string, returnResults bool) map[string]int64 {
	// Map hostname to tags_id
	res := make(map[string]int64)

	// reflect tags table structure which is
	// CREATE TABLE tags(
	//	 created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	//	 id,
	//   %s
	// ) engine=MergeTree(created_at, (%s), 8192)

	// build insert-multiple-rows INSERT statement like:
	// INSERT INTO tags (
	//   ... list of column names ...
	// ) VALUES
	// ( ... row 1 values ... ),
	// ( ... row 2 values ... ),
	// ...
	// ( ... row N values ... ),

	// Columns. Ex.:
	// hostname,region,datacenter,rack,os,arch,team,service,service_version,service_environment
	cols := tableCols["tags"]
	// Add id column to prepared statement
	sql := fmt.Sprintf(`
		INSERT INTO tags(
			id,%s
		) VALUES (
			?%s
		)
		`,
		strings.Join(cols, ","),
		strings.Repeat(",?", len(cols)))
	if conf.Debug > 0 {
		fmt.Printf(sql)
	}

	// In a single transaction insert tags row-by-row
	// ClickHouse driver accumulates all rows inside a transaction into one batch
	tx, err := db.Begin()
	if err != nil {
		panic(err)
	}
	stmt, err := tx.Prepare(sql)
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	id := startID

	processed := make(map[string]int)

	for _, row := range rows {
		// id of the new tag
		id++

		// unfortunately, it is not possible to pass a slice into variadic function of type interface
		// more details on the item:
		// https://blog.learngoprogramming.com/golang-variadic-funcs-how-to-patterns-369408f19085
		// Passing a slice to variadic param with an empty-interface
		var variadicArgs []interface{} = make([]interface{}, len(row)+1) // +1 here for additional 'id' column value
		// Place id at the beginning
		variadicArgs[0] = id
		// And all the rest of column values afterwards
		for i, value := range row {
			variadicArgs[i+1] = convertBasedOnType(tagColumnTypes[i], value)
		}

		// And now expand []interface{} with the same data as 'row' contains (plus 'id') in Exec(args ...interface{})
		_, err := stmt.Exec(variadicArgs...)
		if err != nil {
			panic(err)
		}

		// Fill map hostname -> id
		if returnResults && processed[row[0]] == 0 {
			// Map hostname -> tags_id
			res[row[0]] = int64(id)
		}
		processed[row[0]] = 1
	}

	err = tx.Commit()
	if err != nil {
		panic(err)
	}

	return res
}

func writeBatch(block *data.Block, rows *[][]interface{}, tags_id *[]int32, types []string) {
	block.Reserve()
	n := len((*rows))
	block.NumRows += uint64(n)

	/**
	for m := 0; m < len(types); m++ {
		switch types[m] {
		case "tags_id":
			for i := 0; i < n; i++ {
				block.WriteInt32(m, (*tags_id)[i])
			}
		case "string":
			for i := 0; i < n; i++ {
				block.WriteString(m, (*rows)[i][m].(string))
			}
		case "float32":
			for i := 0; i < n; i++ {
				block.WriteFloat32(m, (*rows)[i][m].(float32))
			}
		case "float64":
			for i := 0; i < n; i++ {
				block.WriteUInt8(m, 0)
			}
			for i := 0; i < n; i++ {
				block.WriteFloat64(m, (*rows)[i][m].(float64))
			}
		case "int64":
			for i := 0; i < n; i++ {
				block.WriteInt64(m, (*rows)[i][m].(int64))
			}
		case "int32":
			for i := 0; i < n; i++ {
				block.WriteInt32(m, (*rows)[i][m].(int32))
			}
		default:
			panic(fmt.Sprintf("unrecognized type %s", types[m]))
		}

	}
	**/
	for i := 0; i < n; i++ {
		for m := 0; m < len(types); m++ {
			switch types[m] {
			case "tags_id":
				block.WriteInt32(m, (*tags_id)[i])
			case "string":
				block.WriteString(m, (*rows)[i][m].(string))
			case "float32":
				block.WriteFloat32(m, (*rows)[i][m].(float32))
			case "float64":
				if i == 0 {
					for i := 0; i < n; i++ {
						block.WriteUInt8(m, 0)
					}
				}
				block.WriteFloat64(m, (*rows)[i][m].(float64))
			case "int64":
				block.WriteInt64(m, (*rows)[i][m].(int64))
			case "int32":
				block.WriteInt32(m, (*rows)[i][m].(int32))
			default:
				panic(fmt.Sprintf("unrecognized type %s", types[m]))
			}
		}
	}
}

func convertBasedOnType(serializedType, value string) interface{} {
	if value == "" {
		return nil
	}

	switch serializedType {
	case "string":
		return value
	case "float32":
		f, err := strconv.ParseFloat(value, 32)
		if err != nil {
			panic(fmt.Sprintf("could not parse '%s' to float32", value))
		}
		return float32(f)
	case "float64":
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			panic(fmt.Sprintf("could not parse '%s' to float64", value))
		}
		return f
	case "int64":
		i, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			panic(fmt.Sprintf("could not parse '%s' to int64", value))
		}
		return i
	case "int32":
		i, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			panic(fmt.Sprintf("could not parse '%s' to int64", value))
		}
		return int32(i)
	default:
		panic(fmt.Sprintf("unrecognized type %s", serializedType))
	}
}

// subsystemTagsToJSON converts equations as
// a=b
// c=d
// into JSON STRING '{"a": "b", "c": "d"}'
func subsystemTagsToJSON(tags []string) string {
	json := "{"
	for i, t := range tags {
		args := strings.Split(t, "=")
		if i > 0 {
			json += ","
		}
		json += fmt.Sprintf("\"%s\": \"%s\"", args[0], args[1])
	}
	json += "}"
	return json
}
