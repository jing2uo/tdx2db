package cmd

import (
	"path/filepath"
	"time"

	"github.com/jing2uo/tdx2db/utils"
)

var Today = time.Now().Truncate(24 * time.Hour)

var TempDir, _ = utils.GetCacheDir()
var VipdocDir = filepath.Join(TempDir, "vipdoc")
var StockDailyCSV = filepath.Join(TempDir, "stock.csv")
var Stock1MinCSV = filepath.Join(TempDir, "1min.csv")
var Stock5MinCSV = filepath.Join(TempDir, "5min.csv")

var ValidPrefixes = []string{
	"sz30",     // 创业板
	"sz00",     // 深证主板
	"sh60",     // 上证主板
	"sh68",     // 科创板
	"bj920",    // 北证
	"sh000300", // 沪深300
	"sh000905", // 中证500
	"sh000852", // 中证1000
	"sh000001", // 上证指数
	"sz399001", // 深证指数
	"sz399006", // 创业板指
	"sh000680", // 科创综指
	"bj899050", // 北证50
	"sh880",    // 通达信概念、风格板块
	"sh881",    // 通达信行业
}
