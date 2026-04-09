package calc

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/utils"
)

// FactorContext 处理上下文
type FactorContext struct {
	DB        database.DataRepository
	GbbqIndex GbbqIndex
}

// ExportFactorsToCSV 导出后复权因子（每天一条记录，和日线对齐）
func ExportFactorsToCSV(
	ctx context.Context,
	db database.DataRepository,
	csvPath string,
) (int, error) {
	gbbqData, err := db.GetGbbq()
	if err != nil {
		return 0, fmt.Errorf("failed to query gbbq: %w", err)
	}
	gbbqIndex := buildGbbqIndex(gbbqData)

	symbols, err := db.GetAllSymbols()
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
		DB:        db,
		GbbqIndex: gbbqIndex,
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
	xdxrs := fctx.GbbqIndex[symbol]

	basics, err := fctx.DB.GetBasicsBySymbol(symbol)
	if err != nil {
		return nil, fmt.Errorf("query %s failed: %w", symbol, err)
	}
	if len(basics) == 0 {
		return nil, nil
	}

	return calculateFullHfq(basics, xdxrs), nil
}

// calculateFullHfq 全量计算 HFQ（每天一条记录）
func calculateFullHfq(basics []model.StockBasic, xdxrs []model.GbbqData) []model.Factor {
	if len(basics) == 0 {
		return nil
	}

	xdxrDates := buildXdxrDateSet(xdxrs)
	results := make([]model.Factor, 0, len(basics))
	currentHfq := 1.0
	prevClose := basics[0].Close

	// 第一天
	results = append(results, model.Factor{
		Symbol:    basics[0].Symbol,
		Date:      basics[0].Date,
		HfqFactor: currentHfq,
	})

	// 后续每天
	for i := 1; i < len(basics); i++ {
		basic := basics[i]
		currentHfq = updateHfq(currentHfq, prevClose, basic, xdxrDates)
		results = append(results, model.Factor{
			Symbol:    basic.Symbol,
			Date:      basic.Date,
			HfqFactor: currentHfq,
		})
		prevClose = basic.Close
	}

	return results
}

// buildXdxrDateSet 构建 xdxr 日期集合
func buildXdxrDateSet(xdxrs []model.GbbqData) map[time.Time]struct{} {
	dates := make(map[time.Time]struct{})
	for _, x := range xdxrs {
		if x.Category == 1 {
			dates[x.Date] = struct{}{}
		}
	}
	return dates
}

// updateHfq 更新 HFQ（如果是除权日）
func updateHfq(currentHfq, prevClose float64, basic model.StockBasic, xdxrDates map[time.Time]struct{}) float64 {
	if _, isXdxr := xdxrDates[basic.Date]; isXdxr {
		if basic.PreClose != 0 {
			ratio := prevClose / basic.PreClose
			if !floatEqual(ratio, 1.0) {
				return currentHfq * ratio
			}
		}
	}
	return currentHfq
}

func floatEqual(a, b float64) bool {
	return math.Abs(a-b) < 1e-9
}
