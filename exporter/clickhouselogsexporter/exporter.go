// Copyright 2020, OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package clickhouselogsexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/clickhouselogsexporter"

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/segmentio/ksuid"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

type clickhouseLogsExporter struct {
	client        *sql.DB
	insertLogsSQL string

	logger *zap.Logger
	cfg    *Config
	ksuid  ksuid.KSUID
}

func newExporter(logger *zap.Logger, cfg *Config) (*clickhouseLogsExporter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	client, err := newClickhouseClient(cfg)
	if err != nil {
		return nil, err
	}

	insertLogsSQL := renderInsertLogsSQL(cfg)

	return &clickhouseLogsExporter{
		client:        client,
		insertLogsSQL: insertLogsSQL,
		logger:        logger,
		cfg:           cfg,
	}, nil
}

// Shutdown will shutdown the exporter.
func (e *clickhouseLogsExporter) Shutdown(_ context.Context) error {
	if e.client != nil {
		return e.client.Close()
	}
	return nil
}

func (e *clickhouseLogsExporter) pushLogsData(ctx context.Context, ld plog.Logs) error {
	start := time.Now()
	err := doWithTx(ctx, e.client, func(tx *sql.Tx) error {
		statement, err := tx.PrepareContext(ctx, e.insertLogsSQL)
		if err != nil {
			return fmt.Errorf("PrepareContext:%w", err)
		}
		defer func() {
			_ = statement.Close()
		}()
		for i := 0; i < ld.ResourceLogs().Len(); i++ {
			logs := ld.ResourceLogs().At(i)
			res := logs.Resource()
			resources := attributesToSlice(res.Attributes())

			for j := 0; j < logs.ScopeLogs().Len(); j++ {
				rs := logs.ScopeLogs().At(j).LogRecords()
				for k := 0; k < rs.Len(); k++ {
					r := rs.At(k)
					attributes := attributesToSlice(r.Attributes())
					_, err = statement.ExecContext(ctx,
						uint64(r.Timestamp()/1000000),
						uint64(r.ObservedTimestamp()/1000000),
						ksuid.New().String(),
						r.TraceID().HexString(),
						r.SpanID().HexString(),
						r.Flags(),
						r.SeverityText(),
						int32(r.SeverityNumber()),
						r.Body().AsString(),
						resources.StringKeys,
						resources.StringValues,
						attributes.StringKeys,
						attributes.StringValues,
						attributes.IntKeys,
						attributes.IntValues,
						attributes.DoubleKeys,
						attributes.DoubleValues,
					)
					if err != nil {
						return fmt.Errorf("ExecContext:%w", err)
					}
				}
			}
		}
		return nil
	})
	duration := time.Since(start)
	e.logger.Debug("insert logs", zap.Int("records", ld.LogRecordCount()),
		zap.String("cost", duration.String()))
	return err
}

type attributesToSliceResponse struct {
	StringKeys   []string
	StringValues []string
	IntKeys      []string
	IntValues    []int64
	DoubleKeys   []string
	DoubleValues []float64
}

func attributesToSlice(attributes pcommon.Map) (response attributesToSliceResponse) {
	attributes.Range(func(k string, v pcommon.Value) bool {
		switch v.Type().String() {
		case "INT":
			response.IntKeys = append(response.IntKeys, formatKey(k))
			response.IntValues = append(response.IntValues, v.IntVal())
		case "DOUBLE":
			response.DoubleKeys = append(response.DoubleKeys, formatKey(k))
			response.DoubleValues = append(response.DoubleValues, v.DoubleVal())
		default: // store it as string
			response.StringKeys = append(response.StringKeys, formatKey(k))
			response.StringValues = append(response.StringValues, v.AsString())
		}
		return true
	})
	return response
}

func formatKey(k string) string {
	return strings.ReplaceAll(k, ".", "_")
}

const (
	// language=ClickHouse SQL
	createLogsTableSQL = `
CREATE TABLE IF NOT EXISTS %s (
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
	attributes_int_value Array(Int) CODEC(ZSTD(1)),
	attributes_double_key Array(String) CODEC(ZSTD(1)),
	attributes_double_value Array(Float64) CODEC(ZSTD(1))
) ENGINE MergeTree()
%s
PARTITION BY toDate(timestamp / 1000)
ORDER BY (toUnixTimestamp(toStartOfInterval(toDateTime(timestamp / 1000), INTERVAL 10 minute)), id)
`
	// ^ choosing a primary key https://kb.altinity.com/engines/mergetree-table-engine-family/pick-keys/
	// 10 mins of logs with 300k logs ingested per sec will have max of 300 * 60 * 10 = 180000k logs
	// max it will go to 180000k / 8192 = 21k blocks during an search if the search space is less than 10 minutes
	// https://github.com/ClickHouse/ClickHouse/issues/11063#issuecomment-631517273

	// language=ClickHouse SQL
	insertLogsSQLTemplate = `INSERT INTO %s (
                        timestamp,
						observed_timestamp,
						id,
                        trace_id,
                        span_id,
                        trace_flags,
                        severity_text,
                        severity_number,
                        body,
                        resources_string_key,
						resources_string_value,
						attributes_string_key, 
						attributes_string_value,
						attributes_int_key,
						attributes_int_value,
						attributes_double_key,
						attributes_double_value
                        ) VALUES (
                                  ?,
								  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
                                  ?,
								  ?,
								  ?,
								  ?,
								  ?,
								  ?,
								  ?,
								  ?
                                  )`
)

var driverName = "clickhouse" // for testing

// newClickhouseClient create a clickhouse client.
func newClickhouseClient(cfg *Config) (*sql.DB, error) {
	// use empty database to create database
	db, err := sql.Open(driverName, cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("sql.Open:%w", err)
	}

	// allow experimental
	if _, err := db.Exec("SET allow_experimental_object_type=1"); err != nil {
		return nil, fmt.Errorf("exec create table sql: %w", err)
	}

	// create table
	query := fmt.Sprintf(createLogsTableSQL, cfg.LogsTableName, "")
	if cfg.TTLDays > 0 {
		query = fmt.Sprintf(createLogsTableSQL,
			cfg.LogsTableName,
			fmt.Sprintf(`TTL toDateTime(timestamp) + INTERVAL %d DAY`, cfg.TTLDays))
	}
	if _, err := db.Exec(query); err != nil {
		return nil, fmt.Errorf("exec create table sql: %w", err)
	}
	return db, nil
}

func renderInsertLogsSQL(cfg *Config) string {
	return fmt.Sprintf(insertLogsSQLTemplate, cfg.LogsTableName)
}

func doWithTx(_ context.Context, db *sql.DB, fn func(tx *sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("db.Begin: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
