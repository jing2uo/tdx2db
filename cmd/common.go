package cmd

import (
	"database/sql"
	"fmt"
	"path/filepath"

	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/tdx"
	"github.com/jing2uo/tdx2db/utils"
)

var DataDir, _ = utils.GetCacheDir()
var ValidPrefixes = []string{
	"sz30",     // 创业板
	"sz00",     // 深证主板
	"sh6",      // 上证主板+科创板
	"bj",       // 北证股票
	"sh880",    // 通达信概念板块
	"sh881",    // 通达信行业板块
	"sh000001", // 上证指数
	"sz399001", // 深证指数
	"bj899050"} // 北证50
var StockCSV = filepath.Join(DataDir, "stock.csv")

func UpdateGbbq(db *sql.DB, dataDir string) error {
	if db == nil {
		return fmt.Errorf("database connection cannot be nil")
	}
	if dataDir == "" {
		return fmt.Errorf("data directory cannot be empty")
	}

	gbbqCSV := filepath.Join(dataDir, "gbbq.csv")
	if _, err := tdx.GetLatestGbbqCsv(dataDir, gbbqCSV); err != nil {
		return fmt.Errorf("failed to download GBBQ CSV: %w", err)
	}

	if err := database.ImportGbbqCsv(db, gbbqCSV); err != nil {
		return fmt.Errorf("failed to import GBBQ CSV into database: %w", err)
	}

	return nil
}
