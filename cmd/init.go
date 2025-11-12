package cmd

import (
	"fmt"
	"os"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/tdx"
)

func Init(dbPath, dayFileDir string) error {

	if dbPath == "" {
		return fmt.Errorf("database path cannot be empty")
	}

	fmt.Printf("📦 开始处理日线目录: %s\n", dayFileDir)
	fileInfo, err := os.Stat(dayFileDir)
	if os.IsNotExist(err) {
		return fmt.Errorf("day file directory does not exist: %s", dayFileDir)
	}
	if err != nil {
		return fmt.Errorf("error checking day file directory: %w", err)
	}
	if !fileInfo.IsDir() {
		return fmt.Errorf("the specified path for dayfiledir is not a directory: %s", dayFileDir)
	}

	fmt.Println("🐢 开始转换日线数据")
	_, err = tdx.ConvertDayfiles2Csv(dayFileDir, ValidPrefixes, StockCSV)
	if err != nil {
		return fmt.Errorf("failed to convert day files to CSV: %w", err)
	}

	fmt.Println("🔥 转换完成")

	dbConfig := model.DBConfig{Path: dbPath}
	db, err := database.Connect(dbConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	if err := database.ImportStockCsv(db, StockCSV); err != nil {
		return fmt.Errorf("failed to import stock CSV: %w", err)
	}
	fmt.Println("🚀 股票数据导入成功")
	return nil
}
