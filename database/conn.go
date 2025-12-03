package database

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/model"
)

func Connect(cfg model.DBConfig) (*sql.DB, error) {
	db, err := sql.Open("duckdb", cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to DuckDB: %w", err)
	}

	// 配置连接池参数
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(0) // 永不过期

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("database ping failed: %w", err)
	}
	return db, nil
}

type TableSchema struct {
	Name    string
	Columns []string
}

func CreateTable(db *sql.DB, schema TableSchema) error {
	columnsStr := strings.Join(schema.Columns, ", ")
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			%s
		)
	`, schema.Name, columnsStr)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", schema.Name, err)
	}
	return nil
}

func DropTable(db *sql.DB, schema TableSchema) error {
	query := fmt.Sprintf(`
		DROP TABLE IF EXISTS %s
	`, schema.Name)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to drop table %s: %w", schema.Name, err)
	}
	return nil
}

// ImportCSV 使用TableSchema导入CSV
func ImportCSV(db *sql.DB, schema TableSchema, csvPath string) error {
	// 解析列名（保持顺序）
	var columnNames []string
	columns := make(map[string]string)
	for _, colDef := range schema.Columns {
		parts := strings.SplitN(colDef, " ", 2)
		if len(parts) < 2 {
			return fmt.Errorf("invalid column definition: %s", colDef)
		}
		columnNames = append(columnNames, parts[0])
		columns[parts[0]] = parts[1]
	}

	// 构建列定义字符串（用于 read_csv）
	colDefs := ""
	for _, col := range columnNames {
		colDefs += fmt.Sprintf("'%s': '%s', ", col, columns[col])
	}
	colDefs = strings.TrimSuffix(colDefs, ", ")

	// 构建 INSERT 语句的目标列列表
	targetCols := strings.Join(columnNames, ", ")

	// 构建 SELECT 语句，从 read_csv 中按 schema 顺序选择列
	selectCols := strings.Join(columnNames, ", ")

	query := fmt.Sprintf(`
		INSERT INTO %s (%s)
		SELECT %s
		FROM read_csv('%s',
			header=true,
			columns={%s},
			dateformat='%%Y-%%m-%%d'
		)
	`, schema.Name, targetCols, selectCols, csvPath, colDefs)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to import CSV to %s: %w", schema.Name, err)
	}
	return nil
}

func GetLatestDateFromTable(db *sql.DB, tableName, dateColumnName string) (time.Time, error) {
	var latestDate sql.NullTime

	query := fmt.Sprintf("SELECT MAX(%s) FROM %s", dateColumnName, tableName)

	err := db.QueryRow(query).Scan(&latestDate)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to query max date from %s: %w", tableName, err)
	}

	if latestDate.Valid {
		return latestDate.Time.Truncate(24 * time.Hour), nil
	}

	return time.Time{}, nil
}

func TableExists(db *sql.DB, tableName string) (bool, error) {
	query := `SELECT count(*) FROM information_schema.tables WHERE table_name = ?`

	var count int
	err := db.QueryRow(query, tableName).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check existence of table %s: %w", tableName, err)
	}

	return count > 0, nil
}
