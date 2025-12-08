package model

type DBType string

const (
	DBTypeDuckDB     DBType = "duckdb"
	DBTypeClickHouse DBType = "clickhouse"
)

type DBConfig struct {
	Type DBType
	DSN  string
}
