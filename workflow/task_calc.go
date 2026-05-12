package workflow

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/jing2uo/tdx2db/calc"
	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
)

var (
	TaskCalcBasic  *Task
	TaskCalcFactor *Task
)

func init() {
	TaskCalcBasic = &Task{
		Name:      "calc_basic",
		DependsOn: []string{"update_daily", "update_gbbq"},
		SkipIf:    skipIfPlan(func(p *WorkPlan) bool { return !p.NeedBasic }),
		Executor:  executeCalcBasic,
	}
	registerTask(TaskCalcBasic, "update")

	TaskCalcFactor = &Task{
		Name:      "calc_factor",
		DependsOn: []string{"calc_basic"},
		SkipIf:    skipIfPlan(func(p *WorkPlan) bool { return !p.NeedFactor }),
		Executor:  executeCalcFactor,
	}
	registerTask(TaskCalcFactor, "update")
}

func executeCalcBasic(ctx context.Context, db database.DataRepository, args *TaskArgs) (*TaskResult, error) {
	fmt.Println("📟 计算股票基础行情")
	basicCSV := filepath.Join(args.TempDir, "basics.csv")

	rowCount, err := calc.ExportBasicDailyToCSV(ctx, db, basicCSV)
	if err != nil {
		return nil, fmt.Errorf("failed to export basic to csv: %w", err)
	}

	if rowCount == 0 {
		fmt.Println("🌲 股票基础行情无需更新")
		return &TaskResult{State: StateSkipped, Message: "no new basic data"}, nil
	}

	db.TruncateTable(model.TableBasicDaily)
	if err := db.ImportBasic(basicCSV); err != nil {
		return nil, fmt.Errorf("failed to import basic data: %w", err)
	}
	fmt.Println("🔢 基础行情导入成功")
	return &TaskResult{State: StateCompleted, Rows: rowCount, Message: "basic data calculated"}, nil
}

func executeCalcFactor(ctx context.Context, db database.DataRepository, args *TaskArgs) (*TaskResult, error) {
	fmt.Println("📟 计算股票复权因子")
	factorCSV := filepath.Join(args.TempDir, "factor.csv")

	factorCount, err := calc.ExportFactorsToCSV(ctx, db, factorCSV)
	if err != nil {
		return nil, fmt.Errorf("failed to export factor to csv: %w", err)
	}

	if factorCount == 0 {
		fmt.Println("🌲 复权因子无需更新")
		return &TaskResult{State: StateSkipped, Message: "no new factor data"}, nil
	}

	db.TruncateTable(model.TableAdjustFactor)
	if err := db.ImportAdjustFactors(factorCSV); err != nil {
		return nil, fmt.Errorf("failed to append factor data: %w", err)
	}
	fmt.Printf("🔢 复权因子导入成功\n")
	return &TaskResult{State: StateCompleted, Rows: factorCount, Message: "factors calculated"}, nil
}
