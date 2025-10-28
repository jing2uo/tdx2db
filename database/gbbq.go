package database

import (
	"database/sql"
	"fmt"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/model"
)

var GBBQSchema = TableSchema{
	Name: "raw_gbbq",
	Columns: []string{
		"category INT",
		"date DATE",
		"code VARCHAR",
		"c1 DOUBLE",
		"c2 DOUBLE",
		"c3 DOUBLE",
		"c4 DOUBLE",
	},
}

var XdxrViewName = "v_xdxr"
var CapitalChangeViewName = "v_capital_change"

func CreateXdxrView(db *sql.DB) error {
	query := fmt.Sprintf(`
	CREATE OR REPLACE VIEW %s AS
	SELECT
		date,
		code,
		c1 as fenhong,
		c2 as peigujia,
		c3 as songzhuangu,
		c4 as peigu,
	FROM %s
	WHERE category=1;
	`, XdxrViewName, GBBQSchema.Name)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create or replace view %s: %w", XdxrViewName, err)
	}
	return nil
}

func CreateCapitalChangeView(db *sql.DB) error {
	query := fmt.Sprintf(`
	CREATE OR REPLACE VIEW %s AS
	SELECT
		date,
		code,
		c1 as prev_outstanding,
		c2 as prev_total,
		c3 as outstanding,
		c4 as total,
	FROM %s
	WHERE category in (2,3,5,7,8,9,10);
	`, CapitalChangeViewName, GBBQSchema.Name)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create or replace view %s: %w", CapitalChangeViewName, err)
	}
	return nil
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

func QueryAllXdxr(db *sql.DB) ([]model.XdxrData, error) {
	query := fmt.Sprintf("SELECT * FROM %s ORDER BY code, date", XdxrViewName)

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query xdxr: %w", err)
	}
	defer rows.Close()

	var results []model.XdxrData
	for rows.Next() {
		var xdxr model.XdxrData
		err := rows.Scan(&xdxr.Date, &xdxr.Code, &xdxr.Fenhong, &xdxr.Peigujia, &xdxr.Songzhuangu, &xdxr.Peigu)
		if err != nil {
			return nil, fmt.Errorf("failed to scan xdxr data: %w", err)
		}
		results = append(results, xdxr)
	}

	return results, nil
}
