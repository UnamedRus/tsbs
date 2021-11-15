#!/bin/bash

# Ensure loader is available
EXE_FILE_NAME=${EXE_FILE_NAME:-$(which tsbs_load_clickhouse)}
if [[ -z "$EXE_FILE_NAME" ]]; then
    echo "tsbs_load_clickhouse not available. It is not specified explicitly and not found in \$PATH"
    exit 1
fi

# Load parameters - common
DATA_FILE_NAME=${DATA_FILE_NAME:-clickhouse-data.gz}
DATABASE_USER=${DATABASE_USER:-default}
DATABASE_PASSWORD=${DATABASE_PASSWORD:-""}
DATABASE_PORT=${DATABASE_PORT:-9000}
INSERT_TYPE=${INSERT_TYPE:-Default}
USE_PROJECTIONS=${USE_PROJECTIONS:-false}
USE_DAILY_PARTITIONING=${USE_DAILY_PARTITIONING:-false}
USE_NULL_TABLE=${USE_NULL_TABLE:-false}
METRIC_LZ4HC=${METRIC_LZ4HC:-0}

# Load parameters - personal
PROGRESS_INTERVAL=${PROGRESS_INTERVAL:-10s}
HASH_WORKERS=${HASH_WORKERS:-false}
BATCH_SIZE=${BATCH_SIZE:-100}
NUM_WORKERS=${NUM_WORKERS:-16}
EXE_DIR=${EXE_DIR:-$(dirname $0)}
source ${EXE_DIR}/load_common.sh

cat ${DATA_FILE} | gunzip | $EXE_FILE_NAME \
                                --host=${DATABASE_HOST} \
                                --user=${DATABASE_USER} \
                                --port=${DATABASE_PORT} \
                                --password=${DATABASE_PASSWORD} \
                                --db-name=${DATABASE_NAME} \
                                --insert-type=${INSERT_TYPE} \
                                --batch-size=${BATCH_SIZE} \
                                --workers=${NUM_WORKERS} \
                                --reporting-period=${PROGRESS_INTERVAL} \
                                --hash-workers=${HASH_WORKERS}
                                --use-daily-partitioning=${USE_DAILY_PARTITIONING}
                                --use_projections=${USE_PROJECTIONS}
                                --use_null_table=${USE_NULL_TABLE}
                                --metric_lz4hc={$METRIC_LZ4HC}
