package calc

import (
	"fmt"
	"runtime"
	"sync"

	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/utils"
)

var factorConcurrency = runtime.NumCPU()

type factorBatchData[T any] struct {
	Rows []T
	Err  error
}

func ExportFactorsToCSV(db database.DataRepository, csvPath string) error {
	// 1. 获取所有股票代码
	symbols, err := db.GetAllSymbols()
	if err != nil {
		return fmt.Errorf("failed to query all stock symbols: %w", err)
	}

	// 2. 创建CSV写入器
	cw, err := utils.NewCSVWriter[model.Factor](csvPath)
	if err != nil {
		return err
	}

	// 3. 并发管道设置
	batchChan := make(chan factorBatchData[model.Factor], factorConcurrency*2)
	sem := make(chan struct{}, factorConcurrency)

	var producerWg sync.WaitGroup
	var consumerWg sync.WaitGroup

	// 错误收集器
	var errors []string
	var errMu sync.Mutex
	collectError := func(e error) {
		errMu.Lock()
		errors = append(errors, e.Error())
		errMu.Unlock()
	}

	// Consumer: 写入 CSV
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
				if err := cw.Write(batch.Rows); err != nil {
					collectError(fmt.Errorf("csv write error: %w", err))
				}
			}
		}
	}()

	// Producer: 并发查询与计算
	for _, symbol := range symbols {
		producerWg.Add(1)

		go func(sym string) {
			sem <- struct{}{}
			defer func() {
				<-sem
				producerWg.Done()
			}()

			// 执行业务逻辑
			rows, err := processStockFactor(db, sym)

			// 发送结果到 Consumer
			batchChan <- factorBatchData[model.Factor]{
				Rows: rows,
				Err:  err,
			}
		}(symbol)
	}

	// 等待所有生产者完成 -> 关闭通道 -> 等待消费者完成
	producerWg.Wait()
	close(batchChan)
	consumerWg.Wait()

	// 4. 返回结果
	if len(errors) > 0 {
		return fmt.Errorf("export completed with %d errors, first: %s", len(errors), errors[0])
	}
	return nil
}

// processStockFactor 处理单只股票的复权因子计算
func processStockFactor(db database.DataRepository, symbol string) ([]model.Factor, error) {
	// 查询股票基础数据（已按日期排序，包含 close 和 preclose）
	stockBasics, err := db.GetBasicsBySymbol(symbol)
	if err != nil {
		return nil, fmt.Errorf("symbol %s query failed: %w", symbol, err)
	}

	if len(stockBasics) == 0 {
		return nil, nil
	}

	// 计算复权因子
	factors := CalculateFqFactor(stockBasics)
	return factors, nil
}

// 前复权因子(QFQ)：从最后一天倒推，累乘 preclose[i+1]/close[i]
// 后复权因子(HFQ)：从第一天正推，累乘 close[i]/preclose[i+1]
func CalculateFqFactor(stockBasics []model.StockBasic) []model.Factor {
	n := len(stockBasics)
	if n == 0 {
		return []model.Factor{}
	}

	result := make([]model.Factor, n)

	// 特殊情况：只有一条数据
	if n == 1 {
		result[0] = model.Factor{
			Symbol:    stockBasics[0].Symbol,
			Date:      stockBasics[0].Date,
			QfqFactor: 1.0,
			HfqFactor: 1.0,
		}
		return result
	}

	// --- 1. 计算前复权因子 (QFQ) ---
	// 从后往前累乘：QFQ[i] = QFQ[i+1] * (PreClose[i+1] / Close[i])
	// 最后一天的因子为 1.0
	qfqFactors := make([]float64, n)
	qfqFactors[n-1] = 1.0

	for i := n - 2; i >= 0; i-- {
		if stockBasics[i].Close != 0 {
			ratio := stockBasics[i+1].PreClose / stockBasics[i].Close
			qfqFactors[i] = qfqFactors[i+1] * ratio
		} else {
			qfqFactors[i] = qfqFactors[i+1]
		}
	}

	// --- 2. 计算后复权因子 (HFQ) ---
	// 从前往后累乘：HFQ[i+1] = HFQ[i] * (Close[i] / PreClose[i+1])
	// 第一天的因子为 1.0
	hfqFactors := make([]float64, n)
	hfqFactors[0] = 1.0

	for i := 0; i < n-1; i++ {
		if stockBasics[i+1].PreClose != 0 {
			ratio := stockBasics[i].Close / stockBasics[i+1].PreClose
			hfqFactors[i+1] = hfqFactors[i] * ratio
		} else {
			hfqFactors[i+1] = hfqFactors[i]
		}
	}

	// --- 3. 组装结果 ---
	for i := 0; i < n; i++ {
		result[i] = model.Factor{
			Symbol:    stockBasics[i].Symbol,
			Date:      stockBasics[i].Date,
			QfqFactor: qfqFactors[i],
			HfqFactor: hfqFactors[i],
		}
	}

	return result
}
