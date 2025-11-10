package database

import (
	"database/sql"
	"fmt"

	_ "github.com/duckdb/duckdb-go/v2"
)

var minLineColumns = []string{
	"symbol VARCHAR",
	"open DOUBLE",
	"high DOUBLE",
	"low DOUBLE",
	"close DOUBLE",
	"amount DOUBLE",
	"volume BIGINT",
	"datetime TIMESTAMP",
}

var OneMinLineSchema = TableSchema{
	Name:    "raw_stocks_1min",
	Columns: minLineColumns,
}

var FiveMinLineSchema = TableSchema{
	Name:    "raw_stocks_5min",
	Columns: minLineColumns,
}

func Import1MinLineCsv(db *sql.DB, csvPath string) error {
	if err := CreateTable(db, OneMinLineSchema); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	if err := ImportCSV(db, OneMinLineSchema, csvPath); err != nil {
		return fmt.Errorf("failed to import CSV: %w", err)
	}

	return nil
}

func Import5MinLineCsv(db *sql.DB, csvPath string) error {
	if err := CreateTable(db, FiveMinLineSchema); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	if err := ImportCSV(db, FiveMinLineSchema, csvPath); err != nil {
		return fmt.Errorf("failed to import CSV: %w", err)
	}

	return nil
}
