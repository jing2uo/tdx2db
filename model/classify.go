package model

import "strings"

const (
	ClassStock   = "stock"
	ClassIndex   = "index"
	ClassETF     = "etf"
	ClassBlock   = "block"
	ClassUnknown = "unknown"
)

// 规则按 (市场, 数字前缀) 匹配,前缀最长优先。
// 不在规则里的代码归为 unknown。
var classRules = []struct {
	Market string
	Prefix string
	Class  string
}{
	// 沪市股票
	{"sh", "600", ClassStock}, {"sh", "601", ClassStock},
	{"sh", "603", ClassStock}, {"sh", "605", ClassStock},
	{"sh", "688", ClassStock},
	// 沪市指数
	{"sh", "000", ClassIndex},
	// 通达信板块指数
	{"sh", "880", ClassBlock}, {"sh", "881", ClassBlock},
	// 沪市 ETF / 场内基金
	{"sh", "51", ClassETF}, {"sh", "52", ClassETF},
	{"sh", "53", ClassETF}, {"sh", "56", ClassETF},
	{"sh", "58", ClassETF},

	// 深市股票
	{"sz", "000", ClassStock}, {"sz", "001", ClassStock},
	{"sz", "002", ClassStock}, {"sz", "003", ClassStock},
	{"sz", "300", ClassStock}, {"sz", "301", ClassStock},
	// 深市指数
	{"sz", "399", ClassIndex},
	// 深市 ETF (15) / LOF (16)
	{"sz", "15", ClassETF}, {"sz", "16", ClassETF},

	// 北交所股票
	{"bj", "43", ClassStock}, {"bj", "83", ClassStock},
	{"bj", "87", ClassStock}, {"bj", "88", ClassStock},
	{"bj", "920", ClassStock},
	// 北交所指数
	{"bj", "899", ClassIndex},
}

// ClassifyCode 根据 symbol (如 "sh600000") 返回所属分类。
// 未匹配到任何规则返回 ClassUnknown。
func ClassifyCode(symbol string) string {
	if len(symbol) < 3 {
		return ClassUnknown
	}
	market := symbol[:2]
	num := symbol[2:]

	best := ""
	bestClass := ClassUnknown
	for _, r := range classRules {
		if r.Market != market {
			continue
		}
		if !strings.HasPrefix(num, r.Prefix) {
			continue
		}
		if len(r.Prefix) > len(best) {
			best = r.Prefix
			bestClass = r.Class
		}
	}
	return bestClass
}
