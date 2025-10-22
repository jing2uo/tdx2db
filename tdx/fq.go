package tdx

import (
	"fmt"
	"sort"
	"time"

	"github.com/jing2uo/tdx2db/model"
)

// internalCombinedData 内部用于合并数据的结构体
type internalCombinedData struct {
	Date        time.Time
	Symbol      string
	Close       float64
	PreClose    float64
	IsTradeDay  bool
	Factor      float64
	Fenhong     float64
	Peigu       float64
	Peigujia    float64
	Songzhuangu float64
}

func CalculateFqFactor(stockData []model.StockData, gbbqData []model.GbbqData) ([]model.Factor, error) {
	combined, err := calculatePreClose(stockData, gbbqData)
	if err != nil {
		return nil, err
	}
	if len(combined) < 2 {
		return []model.Factor{}, nil
	}

	n := len(combined)
	ratios := make([]float64, n)
	for i := 0; i < n-1; i++ {
		if combined[i].IsTradeDay && combined[i].Close != 0 {
			ratios[i] = combined[i+1].PreClose / combined[i].Close
		} else {
			ratios[i] = 1.0
		}
	}
	ratios[n-1] = 1.0

	factors := make([]float64, n)
	acc := 1.0
	for i := n - 1; i >= 0; i-- {
		acc *= ratios[i]
		factors[i] = acc
	}

	result := make([]model.Factor, 0, len(stockData))
	for i, data := range combined {
		if data.IsTradeDay {
			result = append(result, model.Factor{
				Symbol:   data.Symbol,
				Date:     data.Date,
				Close:    data.Close,
				PreClose: data.PreClose,
				Factor:   factors[i],
			})
		}
	}
	return result, nil
}

func calculatePreClose(stockData []model.StockData, gbbqData []model.GbbqData) ([]*internalCombinedData, error) {
	if stockData == nil || len(stockData) == 0 {
		return []*internalCombinedData{}, nil
	}

	// 1. 数据合并与排序
	dataMap := make(map[string]*internalCombinedData)
	dateFormat := "2006-01-02"
	symbol := stockData[0].Symbol

	for _, sd := range stockData {
		dateStr := sd.Date.Format(dateFormat)
		dataMap[dateStr] = &internalCombinedData{
			Date: sd.Date, Symbol: sd.Symbol, Close: sd.Close, IsTradeDay: true,
		}
	}

	for _, gbbq := range gbbqData {
		dateStr := gbbq.Date.Format(dateFormat)
		if data, exists := dataMap[dateStr]; exists {
			data.Fenhong = gbbq.Fenhong
			data.Peigu = gbbq.Peigu
			data.Peigujia = gbbq.Peigujia
			data.Songzhuangu = gbbq.Songzhuangu
		} else {
			dataMap[dateStr] = &internalCombinedData{
				Date: gbbq.Date, Symbol: symbol, IsTradeDay: false,
				Fenhong: gbbq.Fenhong, Peigu: gbbq.Peigu, Peigujia: gbbq.Peigujia, Songzhuangu: gbbq.Songzhuangu,
			}
		}
	}

	combined := make([]*internalCombinedData, 0, len(dataMap))
	for _, v := range dataMap {
		combined = append(combined, v)
	}
	sort.Slice(combined, func(i, j int) bool { return combined[i].Date.Before(combined[j].Date) })

	if len(combined) == 0 {
		return combined, nil
	}

	// 2. 向前填充收盘价
	var lastClose float64
	for _, data := range combined {
		if data.IsTradeDay {
			lastClose = data.Close
		} else {
			data.Close = lastClose
		}
	}

	// 3. 应用复权公式
	if len(combined) > 0 {
		combined[0].PreClose = combined[0].Close
	}

	for i := 1; i < len(combined); i++ {
		prevClose := combined[i-1].Close
		currData := combined[i]

		if prevClose == 0 {
			currData.PreClose = currData.Close
			continue
		}

		// 在非除权日, fenhong/peigu/songzhuangu 都为 0, 公式自动简化
		denominator := 10 + currData.Peigu + currData.Songzhuangu
		if denominator == 0 {
			// 防止 GBBQ 数据异常导致除以0
			return nil, fmt.Errorf("division by zero on date %v for symbol %s", currData.Date, currData.Symbol)
		}

		numerator := (prevClose*10 - currData.Fenhong) + (currData.Peigu * currData.Peigujia)
		currData.PreClose = numerator / denominator
	}

	return combined, nil
}
