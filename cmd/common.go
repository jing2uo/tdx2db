package cmd

import (
	"path/filepath"

	"github.com/jing2uo/tdx2db/utils"
)

var DataDir, _ = utils.GetCacheDir()
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
	"bj899050"} // 北证50
var StockCSV = filepath.Join(DataDir, "stock.csv")
