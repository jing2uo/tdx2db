package tdx

import (
	"fmt"
	"sort"
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

	// Get the earliest stock data date
	var earliestStockDate time.Time
	if len(sortedStockData) > 0 {
		earliestStockDate = sortedStockData[0].Date
	}

	// Filter GBBQ data to only include dates on or after the earliest stock date
	filteredGbbqData := []model.GbbqData{}
	for _, g := range gbbqData {
		if !g.Date.Before(earliestStockDate) {
			filteredGbbqData = append(filteredGbbqData, g)
		}
	}

	// Sort filtered GBBQ data
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
			}
		}

		result[i].PreClose = adjustedPreClose
	}

	return result, nil
}
