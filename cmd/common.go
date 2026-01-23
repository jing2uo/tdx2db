package cmd

import (
	"path/filepath"
	"time"

	"github.com/jing2uo/tdx2db/utils"
)

func GetToday() time.Time {
	return time.Now().Truncate(24 * time.Hour)
}

var TempDir, _ = utils.GetCacheDir()
var VipdocDir = filepath.Join(TempDir, "vipdoc")

var MarketPrefixes = []string{
	"sz30",  // 创业板
	"sz00",  // 深证主板
	"sh60",  // 上证主板
	"sh68",  // 科创板
	"bj920", // 北证
}

var IndexPrefixes = []string{
	"sh000300", // 沪深300
	"sh000905", // 中证500
	"sh000852", // 中证1000
	"sh000001", // 上证综指
	"sz399001", // 深证成指
	"sz399106", // 深证综指
	"sz399006", // 创业板指
	"sh000680", // 科创综指
	"sh000688", // 科创50
	"bj899050", // 北证50
}

var BlockPrefixes = []string{
	"sh880", // 通达信概念、风格板块
	"sh881", // 通达信行业
}

var ValidPrefixes = append(
	append(
		append([]string{}, MarketPrefixes...),
		IndexPrefixes...,
	),
	BlockPrefixes...,
)
