CREATE TABLE IF NOT EXISTS logs (
	timestamp UInt64 CODEC(Delta, ZSTD(1)),
	observed_timestamp UInt64 CODEC(Delta, ZSTD(1)),
	id String CODEC(ZSTD(1)),
	trace_id String CODEC(ZSTD(1)),
	span_id String CODEC(ZSTD(1)),
	trace_flags UInt32,
	severity_text LowCardinality(String) CODEC(ZSTD(1)),
	severity_number Int32,
	body String CODEC(ZSTD(1)),
	resources_string_key Array(String) CODEC(ZSTD(1)),
	resources_string_value Array(String) CODEC(ZSTD(1)),
	attributes_string_key Array(String) CODEC(ZSTD(1)),
	attributes_string_value Array(String) CODEC(ZSTD(1)),
	attributes_int_key Array(String) CODEC(ZSTD(1)),
	attributes_int_value Array(Int64) CODEC(ZSTD(1)),
	attributes_double_key Array(String) CODEC(ZSTD(1)),
	attributes_double_value Array(Float64) CODEC(ZSTD(1))
) ENGINE MergeTree()
PARTITION BY toDate(timestamp / 1000)
ORDER BY (toUnixTimestamp(toStartOfInterval(toDateTime(timestamp / 1000), INTERVAL 10 minute)), id);

-- // ^ choosing a primary key https://kb.altinity.com/engines/mergetree-table-engine-family/pick-keys/
-- // 10 mins of logs with 300k logs ingested per sec will have max of 300 * 60 * 10 = 180000k logs
-- // max it will go to 180000k / 8192 = 21k blocks during an search if the search space is less than 10 minutes
-- // https://github.com/ClickHouse/ClickHouse/issues/11063#issuecomment-631517273


CREATE TABLE IF NOT EXISTS logs_atrribute_keys (
name String,
datatype String
)ENGINE = AggregatingMergeTree
ORDER BY (name);

CREATE TABLE IF NOT EXISTS logs_resource_keys (
name String,
datatype String
)ENGINE = AggregatingMergeTree
ORDER BY (name);

CREATE MATERIALIZED VIEW IF NOT EXISTS  atrribute_keys_string_final_mv TO logs_atrribute_keys AS
SELECT
distinct arrayJoin(attributes_string_key) as name, 'string' datatype
FROM logs
ORDER BY name;

CREATE MATERIALIZED VIEW IF NOT EXISTS  atrribute_keys_int_final_mv TO logs_atrribute_keys AS
SELECT
distinct arrayJoin(attributes_int_key) as name, 'int' datatype
FROM logs
ORDER BY  name;

CREATE MATERIALIZED VIEW IF NOT EXISTS  atrribute_keys_double_final_mv TO logs_atrribute_keys AS
SELECT
distinct arrayJoin(attributes_double_key) as name, 'double' datatype
FROM logs
ORDER BY  name;

CREATE MATERIALIZED VIEW IF NOT EXISTS  resource_keys_string_final_mv TO logs_resource_keys AS
SELECT
distinct arrayJoin(resources_string_key) as name, 'string' datatype
FROM logs
ORDER BY  name;