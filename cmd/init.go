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

	dbConfig := model.DBConfig{
		DSN:  dbPath,
		Type: model.DBTypeDuckDB,
	}

	// 2. 创建驱动实例
	// 关键修改：不要用 "_" 忽略错误！
	db, err := database.NewDatabase(dbConfig)
	if err != nil {
		return fmt.Errorf("failed to create database : %w", err)
	}

	// 防御性编程：虽然有 err 检查，但再检查一次 nil 更稳妥
	if db == nil {
		return fmt.Errorf("database driver is nil even though no error was returned")
	}
	// 2. 连接数据库
	if err := db.Connect(); err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	if err := db.InitSchema(); err != nil {
		return fmt.Errorf("failed to initialize schema: %w", err)
	}

	fmt.Printf("📦 开始处理日线目录: %s\n", dayFileDir)
	err = utils.CheckDirectory(dayFileDir)
	if err != nil {
		return err
	}

	fmt.Println("🐢 开始转换日线数据")
	_, err = tdx.ConvertFilesToParquet(dayFileDir, ValidPrefixes, StockDailyParquet, ".day")
	if err != nil {
		return fmt.Errorf("failed to convert day files to parquet: %w", err)
	}
	fmt.Println("🔥 转换完成")

	if err := db.ImportDailyStocks(StockDailyParquet); err != nil {
		return fmt.Errorf("failed to import stock parquet: %w", err)
	}

	fmt.Println("🚀 股票数据导入成功")
	return nil
}
