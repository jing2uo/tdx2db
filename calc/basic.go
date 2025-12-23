package calc

import (
	"fmt"
	"math"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/utils"
)

// IncrementState 增量计算上下文
type IncrementState struct {
	PrevClose     float64 // 上一交易日收盘价
	LastPostFloat float64 // 上一交易日生效的流通股本 (万股)
	LastPostTotal float64 // 上一交易日生效的总股本 (万股)
}

// 内部使用的除权信息聚合
type xdxrInfo struct {
	Fenhong     float64
	Peigu       float64
	Peigujia    float64
	Songzhuangu float64
}

// 索引类型别名
type GbbqIndex map[string][]model.GbbqData
type StateIndex map[string]*IncrementState
type basicBatchData[T any] struct {
	Rows []T
	Err  error
}

var basicConcurrency = runtime.NumCPU()

// ExportStockBasicToCSV 计算并导出 StockBasic 数据
func ExportStockBasicToCSV(
	db database.DataRepository,
	gbbqData []model.GbbqData,
	csvPath string,
) (int, error) {

	// 1. 判定模式
	startDate, _ := db.GetLatestDate(model.TableBasic.TableName, "date")
	isIncremental := !startDate.IsZero() && startDate.Year() > 1900

	// 2. 构建 GBBQ 索引 (全量/增量都需要)
	gbbqIndex := buildGbbqIndex(gbbqData)

	// 3. 准备增量状态索引
	var stateIndex StateIndex
	if isIncremental {

		lastBasics, err := db.GetLatestBasics()
		if err != nil {
			return 0, fmt.Errorf("failed to query last basic state: %w", err)
		}
		stateIndex = buildStateIndex(lastBasics)
	}

	// 4. 获取股票列表
	symbols, err := db.GetAllSymbols()
	if err != nil {
		return 0, fmt.Errorf("failed to query symbols: %w", err)
	}

	// 5. 初始化 CSV Writer
	cw, err := utils.NewCSVWriter[model.StockBasic](csvPath)
	if err != nil {
		return 0, err
	}

	// 6. 并发管道流程
	batchChan := make(chan basicBatchData[model.StockBasic], basicConcurrency*2)
	sem := make(chan struct{}, basicConcurrency)
	var producerWg, consumerWg sync.WaitGroup

	var errors []string
	var errMu sync.Mutex
	collectError := func(e error) {
		errMu.Lock()
		errors = append(errors, e.Error())
		errMu.Unlock()
	}

	// --- Consumer ---
	rowCount := 0
	consumerWg.Add(1)
	go func() {
		defer consumerWg.Done()
		defer cw.Close()
		for batch := range batchChan {
			if batch.Err != nil {
				collectError(batch.Err)
				continue
			}
			if len(batch.Rows) > 0 {
				rowCount += len(batch.Rows)
				if err := cw.Write(batch.Rows); err != nil {
					collectError(fmt.Errorf("csv write error: %w", err))
				}
			}
		}
	}()

	// --- Producer ---
	for _, symbol := range symbols {
		producerWg.Add(1)
		go func() {
			sem <- struct{}{}
			defer func() { <-sem; producerWg.Done() }()

			rows, err := processStockBasic(db, gbbqIndex, stateIndex, symbol, startDate)
			batchChan <- basicBatchData[model.StockBasic]{Rows: rows, Err: err}
		}()
	}

	producerWg.Wait()
	close(batchChan)
	consumerWg.Wait()

	if len(errors) > 0 {
		return 0, fmt.Errorf("export completed with %d errors, first: %s", len(errors), errors[0])
	}
	return rowCount, nil
}

func processStockBasic(
	db database.DataRepository,
	gbbqIndex GbbqIndex,
	stateIndex StateIndex,
	symbol string,
	startDate time.Time,
) ([]model.StockBasic, error) {

	isIncremental := !startDate.IsZero() && startDate.Year() > 1900

	// A. 确定查询 K 线的时间范围
	var queryStart *time.Time
	if isIncremental {
		// 增量模式：查 startDate 之后的数据 (不包含 startDate)
		t := startDate.AddDate(0, 0, 1)
		queryStart = &t
	}

	// B. 查询 K 线
	stockData, err := db.QueryStockData(symbol, queryStart, nil)
	if err != nil {
		return nil, fmt.Errorf("query stock %s failed: %w", symbol, err)
	}

	if len(stockData) == 0 {
		return nil, nil // 没有新行情
	}

	// C. 获取 GBBQ 数据
	gbbqs := getGbbqBySymbol(gbbqIndex, symbol)

	if isIncremental {
		var filtered []model.GbbqData
		for _, g := range gbbqs {
			// 只保留除权日期 > startDate 的记录
			// startDate 及之前的除权都已经在历史计算中处理过了
			if g.Date.After(startDate) {
				filtered = append(filtered, g)
			}
		}
		gbbqs = filtered
	}

	// D. 准备初始状态
	var initState *IncrementState

	if isIncremental {
		// 1. 优先查快速索引 (Map O(1))
		if state, exists := stateIndex[symbol]; exists {
			initState = state
		} else {
			// 2. 索引未命中 (可能是基准日停牌)，执行兜底查询
			// 获取该股票的绝对最后一条记录

			lastRecords, err := db.GetLatestBasicBySymbol(symbol)
			if err != nil {
				return nil, fmt.Errorf("failed to fallback query for %s: %w", symbol, err)
			}

			if len(lastRecords) > 0 {
				// 反算股本
				lastRecord := lastRecords[0]
				var lastFloat, lastTotal float64
				if lastRecord.Close > 0 {
					lastFloat = lastRecord.FloatMV / lastRecord.Close / 10000
					lastTotal = lastRecord.TotalMV / lastRecord.Close / 10000
				}
				initState = &IncrementState{
					PrevClose:     lastRecord.Close,
					LastPostFloat: lastFloat,
					LastPostTotal: lastTotal,
				}
			} else {
				// 3. 确实是新股
				initState = nil
			}
		}
	}

	// E. 核心计算
	basics, err := CalculateStockBasic(stockData, gbbqs, initState)
	if err != nil {
		return nil, fmt.Errorf("calc %s failed: %w", symbol, err)
	}

	// F. 结果转换 (指针切片 -> 值切片)
	result := make([]model.StockBasic, len(basics))
	for i, b := range basics {
		result[i] = *b
	}

	return result, nil
}

func CalculateStockBasic(
	stockData []model.StockData,
	gbbqData []model.GbbqData,
	initialState *IncrementState,
) ([]*model.StockBasic, error) {

	if len(stockData) == 0 {
		return []*model.StockBasic{}, nil
	}

	// 1. 初始化
	results := make([]*model.StockBasic, len(stockData))
	dateMap := make(map[string]int, len(stockData))
	dateFormat := "2006-01-02"

	for i, sd := range stockData {
		dateMap[sd.Date.Format(dateFormat)] = i
	}

	// 2. GBBQ 数据分流
	xdxrMap := make(map[int]*xdxrInfo)
	sharesList := make([]model.GbbqData, 0, len(gbbqData))

	for _, item := range gbbqData {
		if item.Category == 1 {
			// 除权信息：归并到交易日
			dateStr := item.Date.Format(dateFormat)
			if idx, exists := dateMap[dateStr]; exists {
				mergeXdxrFromGbbq(xdxrMap, idx, item)
			} else {
				// 非交易日顺延
				idx := sort.Search(len(stockData), func(i int) bool {
					return !stockData[i].Date.Before(item.Date)
				})
				if idx < len(stockData) {
					mergeXdxrFromGbbq(xdxrMap, idx, item)
				}
			}
		} else if isShareCategory(item.Category) {
			// 股本变动
			sharesList = append(sharesList, item)
		}
	}

	// 3. 股本排序
	sort.Slice(sharesList, func(i, j int) bool {
		return sharesList[i].Date.Before(sharesList[j].Date)
	})

	// 4. 加载状态
	var currentFloat float64 = 0
	var currentTotal float64 = 0
	if initialState != nil {
		currentFloat = initialState.LastPostFloat
		currentTotal = initialState.LastPostTotal
	}

	shareIdx := 0
	shareLen := len(sharesList)

	// 5. 遍历计算
	for i, sd := range stockData {
		basic := &model.StockBasic{
			Date:   sd.Date,
			Symbol: sd.Symbol,
			Close:  sd.Close,
		}

		// --- A. 计算 PreClose ---
		var prevClose float64
		if i == 0 {
			if initialState != nil {
				prevClose = initialState.PrevClose // 增量衔接
			} else {
				prevClose = sd.Close // 上市首日或全量首日
			}
		} else {
			prevClose = stockData[i-1].Close
		}

		// 计算并应用除权
		basic.PreClose = calculatePreClosePrice(prevClose, xdxrMap[i])

		// --- B. 更新股本 (ASOF Join) ---
		for shareIdx < shareLen && !sharesList[shareIdx].Date.After(sd.Date) {
			currentFloat = sharesList[shareIdx].C3 // post_float
			currentTotal = sharesList[shareIdx].C4 // post_total
			shareIdx++
		}

		// --- C. 计算衍生指标 ---
		if currentFloat > 0 {
			// int64 -> float64
			volFloat := float64(sd.Volume)

			// 换手率
			val := volFloat / (currentFloat * 10000)
			basic.Turnover = math.Round(val*1000000) / 1000000

			// 流通市值
			fmv := currentFloat * 10000 * sd.Close
			basic.FloatMV = math.Round(fmv*100) / 100
		}

		if currentTotal > 0 {
			// 总市值
			tmv := currentTotal * 10000 * sd.Close
			basic.TotalMV = math.Round(tmv*100) / 100
		}

		results[i] = basic
	}

	return results, nil
}

func buildGbbqIndex(data []model.GbbqData) GbbqIndex {
	index := make(GbbqIndex)
	for _, d := range data {
		index[d.Symbol] = append(index[d.Symbol], d)
	}
	return index
}

func buildStateIndex(rows []model.StockBasic) StateIndex {
	index := make(StateIndex, len(rows))
	for _, row := range rows {
		if row.Close == 0 {
			continue
		}
		// 反算股本 (万股)
		lastPostFloat := row.FloatMV / row.Close / 10000
		lastPostTotal := row.TotalMV / row.Close / 10000

		index[row.Symbol] = &IncrementState{
			PrevClose:     row.Close,
			LastPostFloat: lastPostFloat,
			LastPostTotal: lastPostTotal,
		}
	}
	return index
}

func getGbbqBySymbol(index GbbqIndex, symbol string) []model.GbbqData {
	if data, exists := index[symbol]; exists {
		return data
	}
	return []model.GbbqData{}
}

func isShareCategory(cat int) bool {
	switch cat {
	case 2, 3, 5, 7, 8, 9, 10:
		return true
	}
	return false
}

func mergeXdxrFromGbbq(m map[int]*xdxrInfo, idx int, data model.GbbqData) {
	if _, ok := m[idx]; !ok {
		m[idx] = &xdxrInfo{}
	}
	info := m[idx]
	info.Fenhong += data.C1
	info.Peigu += data.C2
	info.Songzhuangu += data.C4
	if data.C3 > 0 {
		info.Peigujia = data.C3
	}
}

func calculatePreClosePrice(prevClose float64, info *xdxrInfo) float64 {
	if info == nil {
		return prevClose
	}
	denominator := 10 + info.Peigu + info.Songzhuangu
	if denominator == 0 {
		return prevClose
	}
	numerator := (prevClose*10 - info.Fenhong) + (info.Peigu * info.Peigujia)
	return numerator / denominator
}
