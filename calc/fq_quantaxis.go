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

// FactorState 增量计算状态
type FactorState struct {
	LastHfq   float64
	PrevClose float64
}

// FactorStateIndex 按股票索引
type FactorStateIndex map[string]*FactorState

// FactorContext 处理上下文
type FactorContext struct {
	DB            database.DataRepository
	GbbqIndex     GbbqIndex
	StateIndex    FactorStateIndex
	BasicsMap     map[string][]model.StockBasic // 增量模式：按 symbol 分组的新 basic
	IsIncremental bool
}

// ExportFactorsToCSV 导出后复权因子（每天一条记录，和日线对齐）
func ExportFactorsToCSV(
	ctx context.Context,
	db database.DataRepository,
	csvPath string,
) (int, error) {
	// 检查是否需要计算
	factorLatest, _ := db.GetLatestDate(model.TableAdjustFactor.TableName, "date")
	basicLatest, _ := db.GetLatestDate(model.TableBasic.TableName, "date")

	isIncremental := !factorLatest.IsZero() && factorLatest.Year() > 1900

	if isIncremental && !basicLatest.After(factorLatest) {
		return 0, nil // 无新数据
	}

	// 获取 gbbq
	gbbqData, err := db.GetGbbq()
	if err != nil {
		return 0, fmt.Errorf("failed to query gbbq: %w", err)
	}
	gbbqIndex := buildGbbqIndex(gbbqData)

	// 构建状态索引和 BasicsMap（增量模式）
	var stateIndex FactorStateIndex
	var basicsMap map[string][]model.StockBasic
	if isIncremental {
		stateIndex, basicsMap, err = buildFactorStateIndex(db, factorLatest)
		if err != nil {
			return 0, fmt.Errorf("failed to build factor state index: %w", err)
		}
	}

	// 获取所有股票
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
		DB:            db,
		GbbqIndex:     gbbqIndex,
		StateIndex:    stateIndex,
		BasicsMap:     basicsMap,
		IsIncremental: isIncremental,
	}

	// 并发计算
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

// buildFactorStateIndex 构建增量计算的状态索引和 basicsMap
func buildFactorStateIndex(db database.DataRepository, factorLatest time.Time) (FactorStateIndex, map[string][]model.StockBasic, error) {
	// 获取每只股票的最新 factor
	latestFactors, err := db.GetLatestFactors()
	if err != nil {
		return nil, nil, err
	}

	// 获取 factorLatest 那天及之后的 basic
	basics, err := db.GetBasicsSince(factorLatest)
	if err != nil {
		return nil, nil, err
	}

	// 按 symbol 分组
	basicsMap := make(map[string][]model.StockBasic)
	for _, b := range basics {
		basicsMap[b.Symbol] = append(basicsMap[b.Symbol], b)
	}

	// 构建状态索引
	index := make(FactorStateIndex, len(latestFactors))
	for _, f := range latestFactors {
		symbolBasics := basicsMap[f.Symbol]
		if len(symbolBasics) == 0 {
			continue
		}
		// 第一条是 factorLatest 那天的记录
		index[f.Symbol] = &FactorState{
			LastHfq:   f.HfqFactor,
			PrevClose: symbolBasics[0].Close,
		}
	}

	return index, basicsMap, nil
}

// processFactorSymbol 处理单只股票
func processFactorSymbol(fctx *FactorContext, symbol string) ([]model.Factor, error) {
	xdxrs := fctx.GbbqIndex[symbol]

	if fctx.IsIncremental {
		state := fctx.StateIndex[symbol]
		basics := fctx.BasicsMap[symbol]

		if state == nil {
			// 新股：全量计算
			allBasics, err := fctx.DB.GetBasicsBySymbol(symbol)
			if err != nil {
				return nil, fmt.Errorf("query %s failed: %w", symbol, err)
			}
			if len(allBasics) == 0 {
				return nil, nil
			}
			return calculateFullHfq(allBasics, xdxrs), nil
		}

		// 有状态但无新 basic（停牌或只有 prevClose 那天）
		if len(basics) <= 1 {
			return nil, nil
		}

		// 增量计算：第一条是 prevClose，后续是新数据
		newBasics := basics[1:]
		return calculateIncrementalHfq(newBasics, state.LastHfq, state.PrevClose, xdxrs), nil
	}

	// 全量模式
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

// calculateIncrementalHfq 增量计算 HFQ
func calculateIncrementalHfq(basics []model.StockBasic, lastHfq, prevClose float64, xdxrs []model.GbbqData) []model.Factor {
	if len(basics) == 0 {
		return nil
	}

	xdxrDates := buildXdxrDateSet(xdxrs)
	results := make([]model.Factor, 0, len(basics))
	currentHfq := lastHfq

	for _, basic := range basics {
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
