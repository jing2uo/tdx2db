package database

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/model"
)

var StocksSchema = TableSchema{
	Name: "raw_stocks_daily",
	Columns: []string{
		"symbol VARCHAR",
		"open DOUBLE",
		"high DOUBLE",
		"low DOUBLE",
		"close DOUBLE",
		"amount DOUBLE",
		"volume BIGINT",
		"date DATE",
	},
}

var QfqViewName = "v_qfq_stocks"
var HfqViewName = "v_hfq_stocks"
var Qfq5MinViewName = "v_qfq_stocks_5min"
var Hfq5MinViewName = "v_hfq_stocks_5min"

func CreateQfqView(db *sql.DB) error {
	query := fmt.Sprintf(`
	CREATE OR REPLACE VIEW %s AS
	SELECT
		s.symbol,
		s.date,
		s.volume,
		s.amount,
		ROUND(s.open  * f.qfq_factor, 2) AS open,
		ROUND(s.high  * f.qfq_factor, 2) AS high,
		ROUND(s.low   * f.qfq_factor, 2) AS low,
		ROUND(s.close * f.qfq_factor, 2) AS close,
		t.turnover,
	FROM %s s
	JOIN %s f ON s.symbol = f.symbol AND s.date = f.date
	LEFT JOIN %s t ON s.symbol = t.symbol AND s.date = t.date;
	`, QfqViewName, StocksSchema.Name, FactorSchema.Name, TurnoverViewName)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create or replace view %s: %w", QfqViewName, err)
	}
	return nil
}

func CreateHfqView(db *sql.DB) error {
	query := fmt.Sprintf(`
	CREATE OR REPLACE VIEW %s AS
	SELECT
		s.symbol,
		s.date,
		s.volume,
		s.amount,
		ROUND(s.open  * f.hfq_factor, 2) AS open,
		ROUND(s.high  * f.hfq_factor, 2) AS high,
		ROUND(s.low   * f.hfq_factor, 2) AS low,
		ROUND(s.close * f.hfq_factor, 2) AS close,
		t.turnover,
	FROM %s s
	JOIN %s f ON s.symbol = f.symbol AND s.date = f.date
	LEFT JOIN %s t ON s.symbol = t.symbol AND s.date = t.date;
	`, HfqViewName, StocksSchema.Name, FactorSchema.Name, TurnoverViewName)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create or replace view %s: %w", HfqViewName, err)
	}
	return nil
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
	`, Qfq5MinViewName, FiveMinLineSchema.Name, FactorSchema.Name)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create or replace view %s: %w", Qfq5MinViewName, err)
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
		ROUND(s.open  * f.hfq_factor, 2) AS open,
		ROUND(s.high  * f.hfq_factor, 2) AS high,
		ROUND(s.low   * f.hfq_factor, 2) AS low,
		ROUND(s.close * f.hfq_factor, 2) AS close
	FROM %s s
	JOIN %s f ON s.symbol = f.symbol AND DATE(s.datetime) = f.date;
	`, Hfq5MinViewName, FiveMinLineSchema.Name, FactorSchema.Name)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create or replace view %s: %w", Hfq5MinViewName, err)
	}
	return nil
}

func ImportStockCsv(db *sql.DB, csvPath string) error {
	if err := CreateTable(db, StocksSchema); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	if err := ImportCSV(db, StocksSchema, csvPath); err != nil {
		return fmt.Errorf("failed to import CSV: %w", err)
	}

	return nil
}

func QueryStockData(db *sql.DB, symbol string, startDate, endDate *time.Time) ([]model.StockData, error) {
	query := fmt.Sprintf("SELECT symbol, open, high, low, close, amount, volume, date FROM %s WHERE symbol = ?", StocksSchema.Name)

	args := []interface{}{symbol}

	// Add date range filters if provided
	if startDate != nil {
		query += " AND date >= ?"
		args = append(args, *startDate)
	}
	if endDate != nil {
		query += " AND date <= ?"
		args = append(args, *endDate)
	}

	// Execute query
	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query stocks: %w", err)
	}
	defer rows.Close()

	// Collect results
	var results []model.StockData
	for rows.Next() {
		var stock model.StockData
		err := rows.Scan(
			&stock.Symbol,
			&stock.Open,
			&stock.High,
			&stock.Low,
			&stock.Close,
			&stock.Amount,
			&stock.Volume,
			&stock.Date,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan stock data: %w", err)
		}
		results = append(results, stock)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return results, nil
}

func QueryAllSymbols(db *sql.DB) ([]string, error) {
	// Get all unique symbols
	query := fmt.Sprintf("SELECT DISTINCT symbol FROM %s", StocksSchema.Name)
	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query symbols: %w", err)
	}
	defer rows.Close()

	var symbols []string
	symbols = make([]string, 0, 1000)
	for rows.Next() {
		var symbol string
		if err := rows.Scan(&symbol); err != nil {
			// Log the error but continue processing other rows
			fmt.Printf("failed to scan symbol: %v\n", err)
			continue
		}
		symbols = append(symbols, symbol)
	}

	return symbols, nil
}

func GetStockTableLatestDate(db *sql.DB) (time.Time, error) {
	date, err := GetLatestDateFromTable(db, StocksSchema.Name)
	if err != nil {
		return time.Time{}, err
	}
	return date, nil
}
