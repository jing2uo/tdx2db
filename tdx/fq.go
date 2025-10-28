package tdx

import (
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
	// 如果 gbbqData 为空，说明没有除权除息事件，采用快速路径处理。
	if len(gbbqData) == 0 {
		// 确保 stockData 按日期升序排序，因为后续逻辑依赖于此顺序。
		sort.Slice(stockData, func(i, j int) bool {
			return stockData[i].Date.Before(stockData[j].Date)
		})
		result := make([]model.Factor, 0, len(stockData))
		if len(stockData) == 0 {
			return result, nil
		}

		// 直接生成结果，复权因子全部为 1.0
		// 处理第一天的数据
		result = append(result, model.Factor{
			Symbol:   stockData[0].Symbol,
			Date:     stockData[0].Date,
			Close:    stockData[0].Close,
			PreClose: stockData[0].Close, // 第一天的 PreClose 就是当天的 Close
			Factor:   1.0,
		})

		// 处理剩余数据
		for i := 1; i < len(stockData); i++ {
			result = append(result, model.Factor{
				Symbol:   stockData[i].Symbol,
				Date:     stockData[i].Date,
				Close:    stockData[i].Close,
				PreClose: stockData[i-1].Close,
				Factor:   1.0,
			})
		}
		return result, nil
	}

	// 当 gbbqData 不为空时，执行完整复权计算逻辑
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
	if len(stockData) == 0 {
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
		if gbbq.Category != 1 {
			continue
		}

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
	// 先找到第一个有效的收盘价来初始化 lastClose，防止其为 0
	for _, data := range combined {
		if data.IsTradeDay && data.Close > 0 {
			lastClose = data.Close
			break // 找到后立即退出
		}
	}

	// 使用一个有效的 lastClose 来安全地向前填充
	for _, data := range combined {
		if data.IsTradeDay && data.Close > 0 {
			// 在每个交易日更新 lastClose
			lastClose = data.Close
		} else {
			// 对于非交易日或收盘价为0的异常交易日，用之前有效的收盘价填充
			data.Close = lastClose
		}
	}

	// 3. 应用 A 股复权公式计算 PreClose
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

		denominator := 10 + currData.Peigu + currData.Songzhuangu
		if denominator == 0 {
			// GBBQ 数据异常，但为了健壮性，我们认为价格不变，而不是返回错误中断整个流程
			// return nil, fmt.Errorf("division by zero on date %v for symbol %s", currData.Date, currData.Symbol)
			currData.PreClose = prevClose
			continue
		}

		numerator := (prevClose*10 - currData.Fenhong) + (currData.Peigu * currData.Peigujia)
		currData.PreClose = numerator / denominator
	}

	return combined, nil
}
