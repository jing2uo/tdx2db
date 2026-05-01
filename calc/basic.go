package calc

import (
	"context"
	"fmt"
	"math"
	"sort"

	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/utils"
)

type xdxrInfo struct {
	Fenhong     float64
	Peigu       float64
	Peigujia    float64
	Songzhuangu float64
}

type GbbqIndex map[string][]model.GbbqData

type BasicContext struct {
	DB        database.DataRepository
	GbbqIndex GbbqIndex
}

// ExportBasicDailyToCSV 计算并导出 BasicDaily 数据 (覆盖 stock + etf)。
func ExportBasicDailyToCSV(
	ctx context.Context,
	db database.DataRepository,
	csvPath string,
) (int, error) {

	gbbqData, err := db.GetGbbq()
	if err != nil {
		return 0, fmt.Errorf("failed to query gbbq: %w", err)
	}
	gbbqIndex := buildGbbqIndex(gbbqData)

	symbols, err := db.GetSymbolsByClass(model.ClassStock, model.ClassETF)
	if err != nil {
		return 0, fmt.Errorf("failed to query symbols: %w", err)
	}

	cw, err := utils.NewCSVWriter[model.BasicDaily](csvPath)
	if err != nil {
		return 0, err
	}
	defer cw.Close()

	basicCtx := &BasicContext{
		DB:        db,
		GbbqIndex: gbbqIndex,
	}

	pipeline := utils.NewPipeline[string, model.BasicDaily]()

	result, err := pipeline.Run(
		ctx,
		symbols,
		func(ctx context.Context, symbol string) ([]model.BasicDaily, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
			return processBasicDaily(basicCtx, symbol)
		},
		func(rows []model.BasicDaily) error {
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

func processBasicDaily(bc *BasicContext, symbol string) ([]model.BasicDaily, error) {
	stockData, err := bc.DB.QueryKlineDaily(symbol, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("query stock %s failed: %w", symbol, err)
	}

	if len(stockData) == 0 {
		return nil, nil
	}

	gbbqs := getGbbqBySymbol(bc.GbbqIndex, symbol)

	basics, err := CalculateBasicDaily(stockData, gbbqs)
	if err != nil {
		return nil, fmt.Errorf("calc %s failed: %w", symbol, err)
	}

	result := make([]model.BasicDaily, len(basics))
	for i, b := range basics {
		result[i] = *b
	}

	return result, nil
}

func CalculateBasicDaily(
	stockData []model.KlineDay,
	gbbqData []model.GbbqData,
) ([]*model.BasicDaily, error) {

	if len(stockData) == 0 {
		return []*model.BasicDaily{}, nil
	}

	results := make([]*model.BasicDaily, len(stockData))
	dateMap := make(map[string]int, len(stockData))
	dateFormat := "2006-01-02"

	for i, sd := range stockData {
		dateMap[sd.Date.Format(dateFormat)] = i
	}

	xdxrMap := make(map[int]*xdxrInfo)
	sharesList := make([]model.GbbqData, 0, len(gbbqData))

	// 已知遗漏: ETF cat=11 (份额拆分/折算) 未处理。c3 表示 1 份拆分成 c3 份,
	// 拆分日 PreClose 应除以 c3, 流通/总份额应乘以 c3。
	// 影响范围:仅 ETF 拆分日的当日 PreClose/ChangePercent 失真,以及拆分后的
	// Turnover/MV 失准。多数 ETF 一辈子不拆分,全市场每月 1-3 例。
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

	shareIdx := 0
	shareLen := len(sharesList)

	var currentFloat float64 = 0
	var currentTotal float64 = 0

	// gbbq 不一定记录上市时的初始股本，第一条股本变动记录的 C1/C2
	// 携带了"变动前"的流通盘/总股本，可用于回填 stockData 开头到该记录之间的值。
	if shareLen > 0 && sharesList[0].Date.After(stockData[0].Date) {
		first := sharesList[0]
		if first.C1 > 0 {
			currentFloat = first.C1
			currentTotal = first.C2
		} else if first.C3 > 0 {
			currentFloat = first.C3
			currentTotal = first.C4
		} else {
			for _, s := range sharesList {
				if s.C3 > 0 {
					currentFloat = s.C3
					currentTotal = s.C4
					break
				}
			}
		}
	}

	for i, sd := range stockData {
		basic := &model.BasicDaily{
			Date:   sd.Date,
			Symbol: sd.Symbol,
			Close:  sd.Close,
		}

		var prevClose float64
		if i == 0 {
			prevClose = sd.Close
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
	info.Songzhuangu += data.C3
	if data.C4 > 0 {
		info.Peigujia = data.C4
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
