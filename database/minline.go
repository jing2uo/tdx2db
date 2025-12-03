package database

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

var OneMinQfqViewName = "v_qfq_1min"
var OneMinHfqViewName = "v_hfq_1min"

var FiveMinQfqViewName = "v_qfq_5min"
var FiveMinHfqViewName = "v_hfq_5min"

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

func Get1MinTableLatestDate(db *sql.DB) (time.Time, error) {
	if err := CreateTable(db, OneMinLineSchema); err != nil {
		return time.Time{}, fmt.Errorf("failed to create table: %w", err)
	}

	date, err := GetLatestDateFromTable(db, OneMinLineSchema.Name, "datetime")
	if err != nil {
		return time.Time{}, err
	}
	return date, nil
}

func Create1MinQfqView(db *sql.DB) error {
	query := fmt.Sprintf(`
	CREATE OR REPLACE VIEW %s AS
	SELECT
		s.symbol,
		s.datetime,
		s.volume,
		s.amount,
		ROUND(s.open  * f.qfq_factor, 2) AS open,
		ROUND(s.high  * f.qfq_factor, 2) AS high,
		ROUND(s.low   * f.qfq_factor, 2) AS low,
		ROUND(s.close * f.qfq_factor, 2) AS close
	FROM %s s
	JOIN %s f ON s.symbol = f.symbol AND DATE(s.datetime) = f.date;
	`, OneMinQfqViewName, FiveMinLineSchema.Name, FactorSchema.Name)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create or replace view %s: %w", OneMinQfqViewName, err)
	}
	return nil
}

func Create1MinHfqView(db *sql.DB) error {
	query := fmt.Sprintf(`
	CREATE OR REPLACE VIEW %s AS
	SELECT
		s.symbol,
		s.datetime,
		s.volume,
		s.amount,
		ROUND(s.open  * f.qfq_factor, 2) AS open,
		ROUND(s.high  * f.qfq_factor, 2) AS high,
		ROUND(s.low   * f.qfq_factor, 2) AS low,
		ROUND(s.close * f.qfq_factor, 2) AS close
	FROM %s s
	JOIN %s f ON s.symbol = f.symbol AND DATE(s.datetime) = f.date;
	`, OneMinHfqViewName, FiveMinLineSchema.Name, FactorSchema.Name)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create or replace view %s: %w", OneMinHfqViewName, err)
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

func Get5MinTableLatestDate(db *sql.DB) (time.Time, error) {
	if err := CreateTable(db, FiveMinLineSchema); err != nil {
		return time.Time{}, fmt.Errorf("failed to create table: %w", err)
	}

	date, err := GetLatestDateFromTable(db, FiveMinLineSchema.Name, "datetime")
	if err != nil {
		return time.Time{}, err
	}
	return date, nil
}

func Create5MinQfqView(db *sql.DB) error {
	query := fmt.Sprintf(`
	CREATE OR REPLACE VIEW %s AS
	SELECT
		s.symbol,
		s.datetime,
		s.volume,
		s.amount,
		ROUND(s.open  * f.qfq_factor, 2) AS open,
		ROUND(s.high  * f.qfq_factor, 2) AS high,
		ROUND(s.low   * f.qfq_factor, 2) AS low,
		ROUND(s.close * f.qfq_factor, 2) AS close
	FROM %s s
	JOIN %s f ON s.symbol = f.symbol AND DATE(s.datetime) = f.date;
	`, FiveMinQfqViewName, FiveMinLineSchema.Name, FactorSchema.Name)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create or replace view %s: %w", FiveMinQfqViewName, err)
	}
	return nil
}

func Create5MinHfqView(db *sql.DB) error {
	query := fmt.Sprintf(`
	CREATE OR REPLACE VIEW %s AS
	SELECT
		s.symbol,
		s.datetime,
		s.volume,
		s.amount,
		ROUND(s.open  * f.qfq_factor, 2) AS open,
		ROUND(s.high  * f.qfq_factor, 2) AS high,
		ROUND(s.low   * f.qfq_factor, 2) AS low,
		ROUND(s.close * f.qfq_factor, 2) AS close
	FROM %s s
	JOIN %s f ON s.symbol = f.symbol AND DATE(s.datetime) = f.date;
	`, FiveMinHfqViewName, FiveMinLineSchema.Name, FactorSchema.Name)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create or replace view %s: %w", FiveMinHfqViewName, err)
	}
	return nil
}
