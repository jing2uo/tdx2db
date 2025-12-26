package calc

import (
	"context"
	"fmt"

	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/utils"
)

// ExportFactorsToCSV 计算并导出复权因子
func ExportFactorsToCSV(ctx context.Context, db database.DataRepository, csvPath string) error {
	symbols, err := db.GetAllSymbols()
	if err != nil {
		return fmt.Errorf("failed to query all stock symbols: %w", err)
	}

	cw, err := utils.NewCSVWriter[model.Factor](csvPath)
	if err != nil {
		return err
	}
	defer cw.Close()

	pipeline := utils.NewPipeline[string, model.Factor]()

	result, err := pipeline.Run(
		ctx,
		symbols,
		func(ctx context.Context, symbol string) ([]model.Factor, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}
			return processStockFactor(db, symbol)
		},
		func(rows []model.Factor) error {
			return cw.Write(rows)
		},
	)

	if err != nil {
		return err
	}

	if result.HasErrors() {
		return fmt.Errorf("export completed with %s", result.ErrorSummary())
	}

	return nil
}

func processStockFactor(db database.DataRepository, symbol string) ([]model.Factor, error) {
	stockBasics, err := db.GetBasicsBySymbol(symbol)
	if err != nil {
		return nil, fmt.Errorf("symbol %s query failed: %w", symbol, err)
	}

	if len(stockBasics) == 0 {
		return nil, nil
	}

	factors := CalculateFqFactor(stockBasics)
	return factors, nil
}

func CalculateFqFactor(stockBasics []model.StockBasic) []model.Factor {
	n := len(stockBasics)
	if n == 0 {
		return []model.Factor{}
	}

	result := make([]model.Factor, n)

	if n == 1 {
		result[0] = model.Factor{
			Symbol:    stockBasics[0].Symbol,
			Date:      stockBasics[0].Date,
			QfqFactor: 1.0,
			HfqFactor: 1.0,
		}
		return result
	}

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
