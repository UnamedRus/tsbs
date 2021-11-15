package clickhouse

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
	"github.com/timescale/tsbs/pkg/targets/timescaledb"
)

func NewTarget() targets.ImplementedTarget {
	return &clickhouseTarget{}
}

type clickhouseTarget struct{}

func (c clickhouseTarget) Benchmark(string, *source.DataSourceConfig, *viper.Viper) (targets.Benchmark, error) {
	panic("implement me")
}

func (c clickhouseTarget) Serializer() serialize.PointSerializer {
	return &timescaledb.Serializer{}
}

func (c clickhouseTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	flagSet.String(flagPrefix+"host", "localhost", "Hostname of ClickHouse instance")
	flagSet.String(flagPrefix+"user", "default", "User to connect to ClickHouse as")
	flagSet.String(flagPrefix+"port", "9000", "Port of ClickHouse instance")
	flagSet.String(flagPrefix+"insert-type", "Default", "Type of Insert Strategy")
	flagSet.Bool(flagPrefix+"daily-partitioning", false, "Partition table by days")
	flagSet.Int(flagPrefix+"metricLZ4HC", 0, "Use CODEC(LZ4CH) for metrics. (default 0 mean use default LZ4)")
	flagSet.String(flagPrefix+"password", "", "Password to connect to ClickHouse")
	flagSet.Bool(flagPrefix+"log-batches", false, "Whether to time individual batches.")
	flagSet.Int(flagPrefix+"debug", 0, "Debug printing (choices: 0, 1, 2). (default 0)")
	flagSet.Bool(flagPrefix+"use-projections", false, "Use projections for tables. (default false)")
}

func (c clickhouseTarget) TargetName() string {
	return constants.FormatClickhouse
}
