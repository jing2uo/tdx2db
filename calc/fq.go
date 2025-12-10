package calc

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
)

// internalCombinedData 内部用于合并数据的结构体
type internalCombinedData struct {
	Date        time.Time
	Symbol      string
	Close       float64
	PreClose    float64
	IsTradeDay  bool
	Fenhong     float64
	Peigu       float64
	Peigujia    float64
	Songzhuangu float64
}

// factorBatch 用于在 Channel 中传递处理结果
type factorBatch struct {
	Rows []model.Factor
	Err  error
}

var maxConcurrency = runtime.NumCPU()

// UpdateFactors 计算复权因子并导出为 Parquet
func ExportFactorsToParquet(db database.DataRepository, parquetPath string) error {

	// 1. 构建 XDXR 索引 (除权除息数据)
	xdxrIndex, err := buildXdxrIndex(db)
	if err != nil {
		return fmt.Errorf("failed to build XDXR index: %w", err)
	}

	// 2. 获取所有股票代码
	symbols, err := db.GetAllSymbols()
	if err != nil {
		return fmt.Errorf("failed to query all stock symbols: %w", err)
	}

	// 3. 创建 Parquet 文件 writer
	f, err := os.Create(parquetPath)
	if err != nil {
		return fmt.Errorf("failed to create parquet file %s: %w", parquetPath, err)
	}
	defer f.Close()

	// 配置 Parquet Writer
	writerConfig := []parquet.WriterOption{
		parquet.Compression(&parquet.Snappy),
		parquet.WriteBufferSize(48 * 1024 * 1024), // 48MB RowGroup Buffer
		parquet.PageBufferSize(64 * 1024),         // 64KB Page Buffer
	}

	pw := parquet.NewGenericWriter[model.Factor](f, writerConfig...)
	defer pw.Close() // 确保数据刷入磁盘

	// 4. 并发处理管道
	batchChan := make(chan factorBatch, maxConcurrency*4)
	var wg sync.WaitGroup
	var writerWg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrency) // 限制并发读取/计算的数量

	// --- Consumer: 单协程写入 Parquet ---
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		for batch := range batchChan {
			if batch.Err != nil {
				fmt.Printf("计算错误: %v\n", batch.Err)
				continue
			}
			if len(batch.Rows) > 0 {
				if _, err := pw.Write(batch.Rows); err != nil {
					fmt.Printf("Parquet 写入失败: %v\n", err)
				}
			}
		}
	}()

	// --- Producer: 并发计算 ---
	for _, symbol := range symbols {
		wg.Add(1)
		sem <- struct{}{} // 获取令牌

		go func(sym string) {
			defer wg.Done()
			defer func() { <-sem }() // 释放令牌

			// 查询个股行情数据
			stockData, err := db.QueryStockData(sym, nil, nil)
			if err != nil {
				batchChan <- factorBatch{Err: fmt.Errorf("symbol %s query failed: %w", sym, err)}
				return
			}

			// 获取除权数据
			xdxrData := getXdxrByCode(xdxrIndex, sym)

			// 计算因子 (假设返回的是 []model.Factor 或类似的结构切片)
			factors, err := CalculateFqFactor(stockData, xdxrData)
			if err != nil {
				batchChan <- factorBatch{Err: fmt.Errorf("symbol %s calc failed: %w", sym, err)}
				return
			}

			// 转换数据格式 (Model -> Parquet Struct)
			// 预分配内存，避免 append 扩容开销
			parquetRows := make([]model.Factor, len(factors))
			for i, factor := range factors {
				parquetRows[i] = model.Factor{
					Symbol:    factor.Symbol,
					Date:      factor.Date,
					Close:     factor.Close,
					PreClose:  factor.PreClose,
					QfqFactor: factor.QfqFactor,
					HfqFactor: factor.HfqFactor,
				}
			}

			// 发送整个切片到 Channel
			batchChan <- factorBatch{Rows: parquetRows}
		}(symbol)
	}

	// 等待所有计算任务完成
	wg.Wait()
	close(batchChan) // 关闭通道，通知 Writer 结束
	writerWg.Wait()  // 等待写入完成
	return nil
}

// --- 辅助函数 (保持逻辑不变，仅作类型适配) ---

type XdxrIndex map[string][]model.XdxrData

func buildXdxrIndex(db database.DataRepository) (XdxrIndex, error) {
	index := make(XdxrIndex)

	xdxrData, err := db.QueryAllXdxr()
	if err != nil {
		return nil, fmt.Errorf("failed to query xdxr data: %w", err)
	}

	for _, data := range xdxrData {
		code := data.Code
		index[code] = append(index[code], data)
	}

	return index, nil
}

func getXdxrByCode(index XdxrIndex, symbol string) []model.XdxrData {
	// 假设 symbol 格式为 "sh600000"，这里截取 "600000"
	if len(symbol) <= 2 {
		return []model.XdxrData{}
	}
	code := symbol[2:]
	if data, exists := index[code]; exists {
		return data
	}
	return []model.XdxrData{}
}

func CalculateFqFactor(stockData []model.StockData, xdxrData []model.XdxrData) ([]model.Factor, error) {
	// 如果 xdxrData 为空，说明没有除权除息事件，采用快速路径处理。
	// 此时，前复权和后复权的因子均为 1.0。
	if len(xdxrData) == 0 {
		// 确保 stockData 按日期升序排序
		sort.Slice(stockData, func(i, j int) bool {
			return stockData[i].Date.Before(stockData[j].Date)
		})
		result := make([]model.Factor, 0, len(stockData))
		if len(stockData) == 0 {
			return result, nil
		}

		// 直接生成结果，复权因子全部为 1.0
		result = append(result, model.Factor{
			Symbol:    stockData[0].Symbol,
			Date:      stockData[0].Date,
			Close:     stockData[0].Close,
			PreClose:  stockData[0].Close, // 第一天的 PreClose 就是当天的 Close
			QfqFactor: 1.0,
			HfqFactor: 1.0,
		})

		for i := 1; i < len(stockData); i++ {
			result = append(result, model.Factor{
				Symbol:    stockData[i].Symbol,
				Date:      stockData[i].Date,
				Close:     stockData[i].Close,
				PreClose:  stockData[i-1].Close,
				QfqFactor: 1.0,
				HfqFactor: 1.0,
			})
		}
		return result, nil
	}

	// 当 xdxrData 不为空时，执行完整复权计算逻辑
	combined, err := calculatePreClose(stockData, xdxrData)
	if err != nil {
		return nil, err
	}
	if len(combined) < 2 {
		return []model.Factor{}, nil
	}

	n := len(combined)

	// --- 1. 计算前复权因子 (QFQ) ---
	// 逻辑：基于 (pre_close.shift(-1) / close) 的倒序累乘
	qfqRatios := make([]float64, n)
	for i := 0; i < n-1; i++ {
		if combined[i].IsTradeDay && combined[i].Close != 0 {
			qfqRatios[i] = combined[i+1].PreClose / combined[i].Close
		} else {
			qfqRatios[i] = 1.0
		}
	}
	qfqRatios[n-1] = 1.0 // 最后一天的比率是1

	qfqFactors := make([]float64, n)
	accQfq := 1.0
	for i := n - 1; i >= 0; i-- {
		accQfq *= qfqRatios[i]
		qfqFactors[i] = accQfq
	}

	// --- 2. 计算后复权因子 (HFQ) ---
	// 逻辑：基于 (close / pre_close.shift(-1)) 的正序累乘，并向下平移一位
	hfqFactors := make([]float64, n)
	if n > 0 {
		hfqFactors[0] = 1.0 // 第一个因子总是 1
		accHfq := 1.0
		for i := 0; i < n-1; i++ {
			var hfqRatio float64
			if combined[i+1].PreClose != 0 {
				hfqRatio = combined[i].Close / combined[i+1].PreClose
			} else {
				hfqRatio = 1.0
			}
			accHfq *= hfqRatio
			hfqFactors[i+1] = accHfq
		}
	}

	// --- 3. 组装最终结果 ---
	result := make([]model.Factor, 0, len(stockData))
	for i, data := range combined {
		// 只返回实际交易日的数据
		if data.IsTradeDay {
			result = append(result, model.Factor{
				Symbol:    data.Symbol,
				Date:      data.Date,
				Close:     data.Close,
				PreClose:  data.PreClose,
				QfqFactor: qfqFactors[i],
				HfqFactor: hfqFactors[i],
			})
		}
	}
	return result, nil
}

func calculatePreClose(stockData []model.StockData, xdxrData []model.XdxrData) ([]*internalCombinedData, error) {
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

	for _, xdxr := range xdxrData {
		dateStr := xdxr.Date.Format(dateFormat)
		if data, exists := dataMap[dateStr]; exists {
			data.Fenhong = xdxr.Fenhong
			data.Peigu = xdxr.Peigu
			data.Peigujia = xdxr.Peigujia
			data.Songzhuangu = xdxr.Songzhuangu
		} else {
			dataMap[dateStr] = &internalCombinedData{
				Date: xdxr.Date, Symbol: symbol, IsTradeDay: false,
				Fenhong: xdxr.Fenhong, Peigu: xdxr.Peigu, Peigujia: xdxr.Peigujia, Songzhuangu: xdxr.Songzhuangu,
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
