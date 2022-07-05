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
	"encoding/json"
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
			// resources := attributesToJSONString(res.Attributes())
			resourceKeys, resourceValues := attributesToSlice(res.Attributes())

			for j := 0; j < logs.ScopeLogs().Len(); j++ {
				rs := logs.ScopeLogs().At(j).LogRecords()
				for k := 0; k < rs.Len(); k++ {
					r := rs.At(k)
					// attrs := attributesToJSONString(r.Attributes())
					attrKeys, attrValues := attributesToSlice(r.Attributes())
					_, err = statement.ExecContext(ctx,
						r.Timestamp().AsTime(),
						ksuid.New().String(),
						r.TraceID().HexString(),
						r.SpanID().HexString(),
						r.Flags(),
						r.SeverityText(),
						int32(r.SeverityNumber()),
						r.Body().AsString(),
						resourceKeys,
						resourceValues,
						attrKeys,
						attrValues,
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

func attributesToSlice(attributes pcommon.Map) ([]string, []string) {
	keys := make([]string, 0, attributes.Len())
	values := make([]string, 0, attributes.Len())
	attributes.Range(func(k string, v pcommon.Value) bool {
		keys = append(keys, formatKey(k))
		values = append(values, v.AsString())
		return true
	})
	return keys, values
}

func attributesToJSONString(attributes pcommon.Map) string {
	attributesMap := make(map[string]interface{})
	attributes.Range(func(k string, v pcommon.Value) bool {
		var val interface{}
		switch v.Type().String() {
		case "EMPTY":
			val = nil
		case "STRING":
			val = v.StringVal()
		case "BOOL":
			val = v.BoolVal()
		case "INT":
			val = v.IntVal()
		case "DOUBLE":
			val = v.DoubleVal()
		case "BYTESMAP":
			val = v.BytesVal().AsRaw()
		case "MAP":
			val = v.MapVal().AsRaw()
		case "SLICE":
			val = v.SliceVal().AsRaw()
		}
		attributesMap[k] = val
		return true
	})

	// marshalling it to string as clickhouse client gives error if emtyp map is passed
	bytes, _ := json.Marshal(attributesMap)
	return string(bytes)
}

func formatKey(k string) string {
	return strings.ReplaceAll(k, ".", "_")
}

const (
	// language=ClickHouse SQL
	createLogsTableSQL = `
CREATE TABLE IF NOT EXISTS %s (
     Timestamp DateTime CODEC(Delta, ZSTD(1)),
	 Id String CODEC(ZSTD(1)),
     TraceId String CODEC(ZSTD(1)),
     SpanId String CODEC(ZSTD(1)),
     TraceFlags UInt32,
     SeverityText LowCardinality(String) CODEC(ZSTD(1)),
     SeverityNumber Int32,
     Body String CODEC(ZSTD(1)),
     ResourceAttributes Nested
      (
          Key LowCardinality(String),
          Value String
      ) CODEC(ZSTD(1)),
	LogAttributes Nested
	(
		Key LowCardinality(String),
		Value String
	) CODEC(ZSTD(1))
) ENGINE MergeTree()
%s
PARTITION BY toDate(Timestamp)
ORDER BY (toUnixTimestamp(Timestamp), Id);
`
	// language=ClickHouse SQL
	insertLogsSQLTemplate = `INSERT INTO %s (
                        Timestamp,
						Id,
                        TraceId,
                        SpanId,
                        TraceFlags,
                        SeverityText,
                        SeverityNumber,
                        Body,
                        ResourceAttributes.Key,
						ResourceAttributes.Value,
						LogAttributes.Key, 
						LogAttributes.Value
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
			fmt.Sprintf(`TTL Timestamp + INTERVAL %d DAY`, cfg.TTLDays))
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
