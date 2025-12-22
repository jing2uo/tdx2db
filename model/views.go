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
	ViewDailyBFQ = DefineView("v_bfq_daily")
	ViewDailyQFQ = DefineView("v_qfq_daily")
	ViewDailyHFQ = DefineView("v_hfq_daily")
)
