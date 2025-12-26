package cmd

import (
	"context"
	"fmt"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/tdx"
	"github.com/jing2uo/tdx2db/utils"
)

// Init 初始化导入日线数据
func Init(ctx context.Context, dbURI, dayFileDir string) error {
	db, err := database.NewDB(dbURI)
	if err != nil {
		return fmt.Errorf("failed to create database driver: %w", err)
	}

	if err := db.Connect(); err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	if err := db.InitSchema(); err != nil {
		return fmt.Errorf("failed to initialize schema: %w", err)
	}

	// 检查取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	fmt.Printf("📦 开始处理日线目录: %s\n", dayFileDir)
	if err := utils.CheckDirectory(dayFileDir); err != nil {
		return err
	}

	fmt.Println("🐢 开始转换日线数据")
	_, err = tdx.ConvertFilesToCSV(ctx, dayFileDir, ValidPrefixes, StockDailyCSV, ".day")
	if err != nil {
		return fmt.Errorf("failed to convert day files to csv: %w", err)
	}
	fmt.Println("🔥 转换完成")

	// 检查取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	if err := db.ImportDailyStocks(StockDailyCSV); err != nil {
		return fmt.Errorf("failed to import stock csv: %w", err)
	}

	fmt.Println("🚀 股票数据导入成功")
	return nil
}
