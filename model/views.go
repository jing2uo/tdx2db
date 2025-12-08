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

var (
	ViewXdxr     = DefineView("v_xdxr")
	ViewTurnover = DefineView("v_turnover")
	ViewDailyQFQ = DefineView("v_qfq_daily")
	ViewDailyHFQ = DefineView("v_hfq_daily")
	View1MinQFQ  = DefineView("v_qfq_1min")
	View1MinHFQ  = DefineView("v_hfq_1min")
	View5MinQFQ  = DefineView("v_qfq_5min")
	View5MinHFQ  = DefineView("v_hfq_5min")
)
