package database

import (
	"database/sql"
	"fmt"

	_ "github.com/duckdb/duckdb-go/v2"
)

// 预定义表结构
var FactorSchema = TableSchema{
	Name: "factor",
	Columns: []string{
		"symbol VARCHAR",
		"date DATE",
		"close DOUBLE",
		"pre_close DOUBLE",
		"factor DOUBLE",
	},
}

// 使用示例
func ImportFactorCsv(db *sql.DB, csvPath string) error {
	//每次导入都重新建表
	if err := DropTable(db, FactorSchema); err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}

	// 创建表
	if err := CreateTable(db, FactorSchema); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	if err := ImportCSV(db, FactorSchema, csvPath); err != nil {
		return fmt.Errorf("failed to import CSV: %w", err)
	}
	return nil
}
