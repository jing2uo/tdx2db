package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/tdx"
	_ "github.com/marcboeker/go-duckdb"
)

func Init(dbPath, dayFileDir string) error {
	start := time.Now()

	// Validate inputs
	if dbPath == "" {
		return fmt.Errorf("database path cannot be empty")
	}

	// 新增：检查目标路径是否存在且为目录
	fileInfo, err := os.Stat(dayFileDir)
	if os.IsNotExist(err) {
		return fmt.Errorf("dayfiledir does not exist: %s", dayFileDir)
	}
	if !fileInfo.IsDir() {
		return fmt.Errorf("dayfiledir is not a directory: %s", dayFileDir)
	}

	fmt.Println("🛠️  开始转换 dayfiles 为 CSV")
	_, err = tdx.ConvertDayfiles2Csv(dayFileDir, ValidPrefixes, StockCSV)
	if err != nil {
		return fmt.Errorf("failed to convert .day files to CSV: %w", err)
	}

	fmt.Println("🔥 转换完成")

	// Connect to database
	dbConfig := model.DBConfig{Path: dbPath}
	db, err := database.Connect(dbConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Import stock CSV
	if err := database.ImportStockCsv(db, StockCSV); err != nil {
		return fmt.Errorf("failed to import stock data: %w", err)
	}
	fmt.Println("📊 股票数据导入成功")
	fmt.Printf("✅ 处理完成，耗时 %s\n", time.Since(start))
	return nil
}
