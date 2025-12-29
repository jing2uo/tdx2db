package calc

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/utils"
)

type IncrementState struct {
	PrevClose     float64
	LastPostFloat float64
	LastPostTotal float64
}

type xdxrInfo struct {
	Fenhong     float64
	Peigu       float64
	Peigujia    float64
	Songzhuangu float64
}

type GbbqIndex map[string][]model.GbbqData
type StateIndex map[string]*IncrementState

type BasicContext struct {
	DB         database.DataRepository
	GbbqIndex  GbbqIndex
	StateIndex StateIndex
	StartDate  time.Time
}

// ExportStockBasicToCSV 计算并导出 StockBasic 数据
func ExportStockBasicToCSV(
	ctx context.Context,
	db database.DataRepository,
	csvPath string,
) (int, error) {

	startDate, _ := db.GetLatestDate(model.TableBasic.TableName, "date")
	isIncremental := !startDate.IsZero() && startDate.Year() > 1900

	gbbqData, err := db.GetGbbq()
	if err != nil {
		return 0, fmt.Errorf("failed to query gbbq: %w", err)
	}
	gbbqIndex := buildGbbqIndex(gbbqData)

	var stateIndex StateIndex
	if isIncremental {
		lastBasics, err := db.GetBasicsSince(startDate)
		if err != nil {
			return 0, fmt.Errorf("failed to query last basic state: %w", err)
		}
		stateIndex = buildStateIndex(lastBasics)
	}

	symbols, err := db.GetAllSymbols()
	if err != nil {
		return 0, fmt.Errorf("failed to query symbols: %w", err)
	}

	cw, err := utils.NewCSVWriter[model.StockBasic](csvPath)
	if err != nil {
		return 0, err
	}
	defer cw.Close()

	basicCtx := &BasicContext{
		DB:         db,
		GbbqIndex:  gbbqIndex,
		StateIndex: stateIndex,
		StartDate:  startDate,
	}

	pipeline := utils.NewPipeline[string, model.StockBasic]()

	result, err := pipeline.Run(
		ctx,
		symbols,
		func(ctx context.Context, symbol string) ([]model.StockBasic, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
			return processStockBasic(basicCtx, symbol)
		},
		func(rows []model.StockBasic) error {
			return cw.Write(rows)
		},
	)

	if err != nil {
		return 0, err
	}

	if result.HasErrors() {
		return 0, fmt.Errorf("export completed with %s", result.ErrorSummary())
	}

	return int(result.OutputRows), nil
}

func processStockBasic(bc *BasicContext, symbol string) ([]model.StockBasic, error) {
	isIncremental := !bc.StartDate.IsZero() && bc.StartDate.Year() > 1900

	var queryStart *time.Time
	if isIncremental {
		t := bc.StartDate.AddDate(0, 0, 1)
		queryStart = &t
	}

	stockData, err := bc.DB.QueryStockData(symbol, queryStart, nil)
	if err != nil {
		return nil, fmt.Errorf("query stock %s failed: %w", symbol, err)
	}

	if len(stockData) == 0 {
		return nil, nil
	}

	gbbqs := getGbbqBySymbol(bc.GbbqIndex, symbol)

	if isIncremental {
		var filtered []model.GbbqData
		for _, g := range gbbqs {
			if g.Date.After(bc.StartDate) {
				filtered = append(filtered, g)
			}
		}
		gbbqs = filtered
	}

	var initState *IncrementState

	if isIncremental {
		if state, exists := bc.StateIndex[symbol]; exists {
			initState = state
		} else {
			lastRecords, err := bc.DB.GetLatestBasicBySymbol(symbol)
			if err != nil {
				return nil, fmt.Errorf("failed to fallback query for %s: %w", symbol, err)
			}

			if len(lastRecords) > 0 {
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
			}
		}
	}

	basics, err := CalculateStockBasic(stockData, gbbqs, initState)
	if err != nil {
		return nil, fmt.Errorf("calc %s failed: %w", symbol, err)
	}

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

	results := make([]*model.StockBasic, len(stockData))
	dateMap := make(map[string]int, len(stockData))
	dateFormat := "2006-01-02"

	for i, sd := range stockData {
		dateMap[sd.Date.Format(dateFormat)] = i
	}

	xdxrMap := make(map[int]*xdxrInfo)
	sharesList := make([]model.GbbqData, 0, len(gbbqData))

	for _, item := range gbbqData {
		if item.Category == 1 {
			dateStr := item.Date.Format(dateFormat)
			if idx, exists := dateMap[dateStr]; exists {
				mergeXdxrFromGbbq(xdxrMap, idx, item)
			} else {
				idx := sort.Search(len(stockData), func(i int) bool {
					return !stockData[i].Date.Before(item.Date)
				})
				if idx < len(stockData) {
					mergeXdxrFromGbbq(xdxrMap, idx, item)
				}
			}
		} else if isShareCategory(item.Category) {
			sharesList = append(sharesList, item)
		}
	}

	sort.Slice(sharesList, func(i, j int) bool {
		return sharesList[i].Date.Before(sharesList[j].Date)
	})

	var currentFloat float64 = 0
	var currentTotal float64 = 0
	if initialState != nil {
		currentFloat = initialState.LastPostFloat
		currentTotal = initialState.LastPostTotal
	}

	shareIdx := 0
	shareLen := len(sharesList)

	for i, sd := range stockData {
		basic := &model.StockBasic{
			Date:   sd.Date,
			Symbol: sd.Symbol,
			Close:  sd.Close,
		}

		var prevClose float64
		if i == 0 {
			if initialState != nil {
				prevClose = initialState.PrevClose
			} else {
				prevClose = sd.Close
			}
		} else {
			prevClose = stockData[i-1].Close
		}

		basic.PreClose = calculatePreClosePrice(prevClose, xdxrMap[i])

		if basic.PreClose > 0 {
			ChangePercent := (sd.Close - basic.PreClose) / basic.PreClose * 100
			basic.ChangePercent = math.Round(ChangePercent*100) / 100

			amplitude := (sd.High - sd.Low) / basic.PreClose * 100
			basic.Amplitude = math.Round(amplitude*100) / 100
		}

		for shareIdx < shareLen && !sharesList[shareIdx].Date.After(sd.Date) {
			currentFloat = sharesList[shareIdx].C3
			currentTotal = sharesList[shareIdx].C4
			shareIdx++
		}

		if currentFloat > 0 {
			volFloat := float64(sd.Volume)
			val := volFloat / (currentFloat * 10000)
			basic.Turnover = math.Round(val*1000000) / 1000000
			fmv := currentFloat * 10000 * sd.Close
			basic.FloatMV = math.Round(fmv*100) / 100
		}

		if currentTotal > 0 {
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
