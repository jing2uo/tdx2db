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
	{"sh", "688", ClassStock}, {"sh", "689", ClassStock},
	// 沪市 B 股
	{"sh", "900", ClassStock},
	// 沪市指数
	{"sh", "000", ClassIndex},
	// 通达信板块指数
	{"sh", "880", ClassBlock}, {"sh", "881", ClassBlock},
	// 沪市 ETF / 场内基金 (50 = 老封基/LOF, 51-58 = ETF)
	{"sh", "50", ClassETF}, {"sh", "51", ClassETF},
	{"sh", "52", ClassETF}, {"sh", "53", ClassETF},
	{"sh", "56", ClassETF}, {"sh", "58", ClassETF},

	// 深市股票
	{"sz", "000", ClassStock}, {"sz", "001", ClassStock},
	{"sz", "002", ClassStock}, {"sz", "003", ClassStock},
	{"sz", "300", ClassStock}, {"sz", "301", ClassStock},
	{"sz", "302", ClassStock},
	// 深市 B 股
	{"sz", "20", ClassStock},
	// 深市指数
	{"sz", "399", ClassIndex},
	// 深市 ETF (15) / LOF (16/18)
	{"sz", "15", ClassETF}, {"sz", "16", ClassETF},
	{"sz", "18", ClassETF},

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

// SymbolFromCode 根据 6 位裸数字代码 (如 "600000") 反查市场前缀,
// 返回完整 symbol (如 "sh600000")。复用 classRules,只考虑 stock/etf 类型,
// 因为指数 (index) 和板块 (block) 没有公司行为数据,
// 也能避免 "000" 同时命中 sh 指数和 sz 股票的歧义。
// 多前缀匹配时取最长前缀。未匹配返回 (code, false)。
func SymbolFromCode(code string) (string, bool) {
	bestPrefix := ""
	bestMarket := ""
	for _, r := range classRules {
		if r.Class != ClassStock && r.Class != ClassETF {
			continue
		}
		if !strings.HasPrefix(code, r.Prefix) {
			continue
		}
		if len(r.Prefix) > len(bestPrefix) {
			bestPrefix = r.Prefix
			bestMarket = r.Market
		}
	}
	if bestMarket == "" {
		return code, false
	}
	return bestMarket + code, true
}
