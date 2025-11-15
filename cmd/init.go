package cmd

import (
	"fmt"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/tdx"
	"github.com/jing2uo/tdx2db/utils"
)

func Init(dbPath, dayFileDir string) error {

	if dbPath == "" {
		return fmt.Errorf("database path cannot be empty")
	}

	fmt.Printf("📦 开始处理日线目录: %s\n", dayFileDir)
	err := utils.CheckDirectory(dayFileDir)
	if err != nil {
		return err
	}
	fmt.Println("🐢 开始转换日线数据")
	_, err = tdx.ConvertFiles2Csv(dayFileDir, ValidPrefixes, StockCSV, ".day")
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
