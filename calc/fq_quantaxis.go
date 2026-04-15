package calc

import (
	"context"
	"fmt"
	"math"

	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/utils"
)

// FactorContext 处理上下文
type FactorContext struct {
	DB database.DataRepository
}

// ExportFactorsToCSV 导出后复权因子（每天一条记录，和日线对齐）
func ExportFactorsToCSV(
	ctx context.Context,
	db database.DataRepository,
	csvPath string,
) (int, error) {
	symbols, err := db.GetSymbolsByClass(model.ClassStock)
	if err != nil {
		return 0, fmt.Errorf("failed to query symbols: %w", err)
	}

	if len(symbols) == 0 {
		return 0, nil
	}

	cw, err := utils.NewCSVWriter[model.Factor](csvPath)
	if err != nil {
		return 0, err
	}
	defer cw.Close()

	fctx := &FactorContext{
		DB: db,
	}

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
			return processFactorSymbol(fctx, symbol)
		},
		func(rows []model.Factor) error {
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

func processFactorSymbol(fctx *FactorContext, symbol string) ([]model.Factor, error) {
	basics, err := fctx.DB.GetBasicsBySymbol(symbol)
	if err != nil {
		return nil, fmt.Errorf("query %s failed: %w", symbol, err)
	}
	if len(basics) == 0 {
		return nil, nil
	}

	return calculateFullHfq(basics), nil
}

// calculateFullHfq 全量计算 HFQ（每天一条记录）
// basic.PreClose 已包含除权调整，prevClose != PreClose 即为除权日，无需独立的 xdxr 日期集合
func calculateFullHfq(basics []model.StockBasic) []model.Factor {
	if len(basics) == 0 {
		return nil
	}

	results := make([]model.Factor, 0, len(basics))
	currentHfq := 1.0
	prevClose := basics[0].Close

	// 第一天
	results = append(results, model.Factor{
		Symbol:    basics[0].Symbol,
		Date:      basics[0].Date,
		HfqFactor: currentHfq,
	})

	// 后续每天：通过 prevClose / PreClose 检测除权
	for i := 1; i < len(basics); i++ {
		basic := basics[i]
		if basic.PreClose != 0 {
			ratio := prevClose / basic.PreClose
			if !floatEqual(ratio, 1.0) {
				currentHfq *= ratio
			}
		}
		results = append(results, model.Factor{
			Symbol:    basic.Symbol,
			Date:      basic.Date,
			HfqFactor: currentHfq,
		})
		prevClose = basic.Close
	}

	return results
}

func floatEqual(a, b float64) bool {
	return math.Abs(a-b) < 1e-9
}
