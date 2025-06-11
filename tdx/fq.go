package tdx

import (
	"fmt"
	"sort"

	"github.com/jing2uo/tdx2db/model"
)

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

	sortedGbbqData := make([]model.GbbqData, len(gbbqData))
	copy(sortedGbbqData, gbbqData)
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
					return nil, fmt.Errorf("negative adjusted pre-close price %f for date %v", adjustedPreClose, currentDate)
				}
			}
		}

		result[i].PreClose = adjustedPreClose
	}

	return result, nil
}
