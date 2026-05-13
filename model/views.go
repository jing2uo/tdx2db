package model

import "sync"

type ViewID string

var (
	viewRegistry   []ViewID
	viewRegistryMu sync.Mutex
)

func DefineView(name string) ViewID {
	viewRegistryMu.Lock()
	defer viewRegistryMu.Unlock()

	id := ViewID(name)
	viewRegistry = append(viewRegistry, id)
	return id
}

func AllViews() []ViewID {
	viewRegistryMu.Lock()
	defer viewRegistryMu.Unlock()

	result := make([]ViewID, len(viewRegistry))
	copy(result, viewRegistry)
	return result
}

// --- 定义视图 ---
//
// 命名约定：v_<class>_<fq>，class 放前面便于 tab-complete 按归属浏览
// （v_stock_<TAB> / v_etf_<TAB> 各列出 3 个）。
// stock / etf 拆开维护：ETF 价格 scale=1000、ROUND 精度 3 位，
// stock scale=100、ROUND 精度 2 位。
var (
	ViewStockBFQ = DefineView("v_stock_bfq")
	ViewStockQFQ = DefineView("v_stock_qfq")
	ViewStockHFQ = DefineView("v_stock_hfq")
	ViewETFBFQ   = DefineView("v_etf_bfq")
	ViewETFQFQ   = DefineView("v_etf_qfq")
	ViewETFHFQ   = DefineView("v_etf_hfq")
)
