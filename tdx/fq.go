package tdx

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jing2uo/tdx2db/model"
)

func CalculateFqFactor(stockData []model.StockData, gbbqData []model.GbbqData) ([]model.Factor, error) {
	fqData, err := CalculatePreClose(stockData, gbbqData)
	if err != nil {
		return nil, err
	}

	result := make([]model.Factor, len(fqData))
	for i, data := range fqData {
		result[i] = model.Factor{
			Symbol:   data.Symbol,
			Date:     data.Date,
			Close:    data.Close,
			PreClose: data.PreClose,
			Factor:   1.0, // Initialize Factor (QfqFactor) to 1.0
		}
	}

	if len(fqData) < 1 {
		return result, nil
	}

	factors := make([]float64, len(fqData))
	for i := 0; i < len(fqData)-1; i++ {
		if fqData[i].Close != 0 {
			factors[i] = fqData[i+1].PreClose / fqData[i].Close
		} else {
			factors[i] = 1.0 // Avoid division by zero
		}
	}
	factors[len(fqData)-1] = 1.0 // Last day's factor is 1

	cumprod := 1.0
	for i := len(fqData) - 1; i >= 0; i-- {
		cumprod *= factors[i]
		result[i].Factor = cumprod
	}

	return result, nil
}

func CalculatePreClose(stockData []model.StockData, gbbqData []model.GbbqData) ([]model.StockDataWithPreClose, error) {
	if stockData == nil {
		return nil, fmt.Errorf("stock data cannot be nil")
	}
	if gbbqData == nil {
		return nil, fmt.Errorf("GBBQ data cannot be nil")
	}

	sortedStockData := make([]model.StockData, len(stockData))
	copy(sortedStockData, stockData)
	sort.Slice(sortedStockData, func(i, j int) bool {
		return sortedStockData[i].Date.Before(sortedStockData[j].Date)
	})

	// 获取股票代码名称
	var stockName string
	if len(sortedStockData) > 0 {
		stockName = sortedStockData[0].Symbol
	} else {
		return []model.StockDataWithPreClose{}, nil
	}

	// 处理北交所股票，把 2021-11-15 开市前的数据直接删除
	cutoffDate := time.Date(2021, 11, 15, 0, 0, 0, 0, time.UTC)
	if strings.HasPrefix(stockName, "bj") {
		// 过滤sortedStockData
		filteredStock := []model.StockData{}
		for _, data := range sortedStockData {
			if !data.Date.Before(cutoffDate) {
				filteredStock = append(filteredStock, data)
			}
		}
		sortedStockData = filteredStock

		// 过滤gbbqData
		filteredGbbq := []model.GbbqData{}
		for _, g := range gbbqData {
			if !g.Date.Before(cutoffDate) {
				filteredGbbq = append(filteredGbbq, g)
			}
		}
		gbbqData = filteredGbbq
	}

	// 获取最早的股票数据日期
	var earliestStockDate time.Time
	if len(sortedStockData) > 0 {
		earliestStockDate = sortedStockData[0].Date
	}

	// 过滤GBBQ数据，仅保留不早于earliestStockDate的日期
	filteredGbbqData := []model.GbbqData{}
	for _, g := range gbbqData {
		if !g.Date.Before(earliestStockDate) {
			filteredGbbqData = append(filteredGbbqData, g)
		}
	}

	// 排序过滤后的GBBQ数据
	sortedGbbqData := make([]model.GbbqData, len(filteredGbbqData))
	copy(sortedGbbqData, filteredGbbqData)
	sort.Slice(sortedGbbqData, func(i, j int) bool {
		return sortedGbbqData[i].Date.Before(sortedGbbqData[j].Date)
	})

	result := make([]model.StockDataWithPreClose, len(sortedStockData))
	for i, data := range sortedStockData {
		result[i] = model.StockDataWithPreClose{
			StockData: data,
			PreClose:  0,
		}
	}

	if len(sortedStockData) < 2 {
		return result, nil
	}

	result[0].PreClose = 0

	for i := 1; i < len(sortedStockData); i++ {
		currentDate := sortedStockData[i].Date
		prevClose := sortedStockData[i-1].Close
		prevDate := sortedStockData[i-1].Date

		if currentDate.Before(prevDate) || currentDate.Equal(prevDate) {
			return nil, fmt.Errorf("invalid date sequence at index %d: %v is not after %v", i, currentDate, prevDate)
		}

		adjustedPreClose := prevClose
		for _, g := range sortedGbbqData {
			if !g.Date.Before(prevDate) && g.Date.Before(currentDate) {
				denominator := 10 + g.Peigu + g.Songzhuangu
				if denominator == 0 {
					return nil, fmt.Errorf("division by zero in GBBQ adjustment for date %v", g.Date)
				}
				adjustedPreClose = ((adjustedPreClose*10 - g.Fenhong) + (g.Peigu * g.Peigujia)) / denominator
				if adjustedPreClose < 0 {
					fmt.Printf("Warning: %s pre-close price %f for date %v\n", stockName, adjustedPreClose, currentDate.Format("2006-01-02"))
				}
			}
		}

		result[i].PreClose = adjustedPreClose
	}

	return result, nil
}
