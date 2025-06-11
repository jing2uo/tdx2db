package database

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/jing2uo/tdx2db/model"
	_ "github.com/marcboeker/go-duckdb"
)

var StocksSchema = TableSchema{
	Name: "stocks",
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

func ImportStockCsv(db *sql.DB, csvPath string) error {
	if err := CreateTable(db, StocksSchema); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	if err := ImportCSV(db, StocksSchema, csvPath); err != nil {
		return fmt.Errorf("failed to import CSV: %w", err)
	}

	return nil
}

func QueryStocks(db *sql.DB, symbol string, startDate, endDate *time.Time) ([]model.StockData, error) {
	query := "SELECT symbol, open, high, low, close, amount, volume, date FROM stocks WHERE symbol = ?"
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

func GetAllSymbols(db *sql.DB) ([]string, error) {
	// Get all unique symbols
	rows, err := db.Query(`SELECT DISTINCT symbol FROM stocks`)
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

func GetLatestDate(db *sql.DB) (time.Time, error) {
	query := `
		SELECT symbol, MAX(date) as latest_date
		FROM stocks
		WHERE symbol IN ('sh000001', 'sz399001', 'bj899050')
		GROUP BY symbol;
	`
	rows, err := db.Query(query)
	if err != nil {
		return time.Time{}, fmt.Errorf("query execution failed: %v", err)
	}
	defer rows.Close()

	// Map to store symbol-date pairs
	dateMap := make(map[string]time.Time)
	expectedSymbols := map[string]bool{
		"sh000001": true,
		"sz399001": true,
		"bj899050": true,
	}

	// Collect dates for each symbol
	for rows.Next() {
		var symbol string
		var date time.Time
		if err := rows.Scan(&symbol, &date); err != nil {
			return time.Time{}, fmt.Errorf("failed to parse date for symbol %s: %v", symbol, err)
		}
		dateMap[symbol] = date
		delete(expectedSymbols, symbol)
	}

	// Check for missing symbols
	if len(expectedSymbols) > 0 {
		missing := make([]string, 0, len(expectedSymbols))
		for symbol := range expectedSymbols {
			missing = append(missing, symbol)
		}
		return time.Time{}, fmt.Errorf("missing data for symbols: %v", missing)
	}

	// Check date consistency
	referenceDate := dateMap["sh000001"]
	var inconsistent []string
	for symbol, date := range dateMap {
		if !date.Equal(referenceDate) {
			inconsistent = append(inconsistent, fmt.Sprintf("%s: %s", symbol, date.Format("2006-01-02")))
		}
	}

	if len(inconsistent) > 0 {
		return time.Time{}, fmt.Errorf("inconsistent dates detected:\n%v", inconsistent)
	}

	return referenceDate, nil
}
