package database

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/jing2uo/tdx2db/model"
	_ "github.com/marcboeker/go-duckdb"
)

var GBBQSchema = TableSchema{
	Name: "gbbq",
	Columns: []string{
		"date DATE",
		"code VARCHAR",
		"fenhong DOUBLE",
		"peigujia DOUBLE",
		"songzhuangu DOUBLE",
		"peigu DOUBLE",
	},
}

func ImportGbbqCsv(db *sql.DB, csvPath string) error {
	//每次导入都重新建表
	if err := DropTable(db, GBBQSchema); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	if err := CreateTable(db, GBBQSchema); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	if err := ImportCSV(db, GBBQSchema, csvPath); err != nil {
		return fmt.Errorf("failed to import CSV: %w", err)
	}

	return nil
}

func QueryGbbq(db *sql.DB, symbol string, startDate, endDate *time.Time) ([]model.GbbqData, error) {
	code := symbol[2:]
	query := "SELECT * FROM gbbq WHERE code = ? ORDER BY date"
	args := []interface{}{code}

	// Add date range filters if provided
	if startDate != nil {
		query += " AND date >= ?"
		args = append(args, *startDate)
	}
	if endDate != nil {
		query += " AND date <= ?"
		args = append(args, *endDate)
	}

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query gbbq: %w", err)
	}
	defer rows.Close()

	var results []model.GbbqData
	for rows.Next() {
		var gbbq model.GbbqData
		err := rows.Scan(&gbbq.Date, &gbbq.Fenhong, &gbbq.Peigu, &gbbq.Peigujia, &gbbq.Songzhuangu)
		if err != nil {
			return nil, fmt.Errorf("failed to scan gbbq data: %w", err)
		}
		results = append(results, gbbq)
	}

	return results, nil
}
