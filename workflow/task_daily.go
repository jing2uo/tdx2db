package workflow

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/tdx"
	"github.com/jing2uo/tdx2db/utils"
)

var (
	TaskUpdateDaily *Task
	TaskInitDaily   *Task
)

func init() {
	TaskUpdateDaily = &Task{
		Name:      "update_daily",
		DependsOn: []string{},
		SkipIf:    skipIfPlan(func(p *WorkPlan) bool { return !p.NeedDaily }),
		Executor:  executeUpdateDaily,
	}
	registerTask(TaskUpdateDaily, "update")

	TaskInitDaily = &Task{
		Name:      "init_daily",
		DependsOn: []string{},
		Executor:  executeInitDaily,
	}
	registerTask(TaskInitDaily, "init")
}

func executeUpdateDaily(ctx context.Context, db database.DataRepository, args *TaskArgs) (*TaskResult, error) {
	latestDate, err := db.GetLatestDate(model.TableKlineDaily.TableName, "date")
	if err != nil {
		return nil, fmt.Errorf("failed to get latest date from database: %w", err)
	}
	fmt.Printf("📅 日线数据最新日期为 %s\n", latestDate.Format("2006-01-02"))

	src := pullSource{
		targetDir:   filepath.Join(args.VipdocDir, "refmhq"),
		urlTemplate: "https://www.tdx.com.cn/products/data/data/g4day/%s.zip",
		fileSuffix:  "day",
		label:       "日线",
	}
	validDates, err := pullDateRange(ctx, latestDate, src, args)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch daily data: %w", err)
	}

	if len(validDates) == 0 {
		fmt.Println("🌲 日线数据无需更新")
		return &TaskResult{State: StateSkipped, Message: "no new daily data"}, nil
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	endDate := validDates[len(validDates)-1]
	if err := tdx.DatatoolCreate(args.TempDir, "day", endDate); err != nil {
		return nil, fmt.Errorf("failed to run DatatoolDayCreate: %w", err)
	}

	return executeDailyImport(ctx, db, args, args.VipdocDir)
}

func executeInitDaily(ctx context.Context, db database.DataRepository, args *TaskArgs) (*TaskResult, error) {
	fmt.Printf("📦 开始处理日线目录: %s\n", args.DayFileDir)
	if err := utils.CheckDirectory(args.DayFileDir); err != nil {
		return nil, err
	}

	return executeDailyImport(ctx, db, args, args.DayFileDir)
}

func executeDailyImport(ctx context.Context, db database.DataRepository, args *TaskArgs, sourceDir string) (*TaskResult, error) {
	fmt.Println("🐌 开始转换日线数据")

	stockDailyCSV := filepath.Join(args.TempDir, "stock.csv")

	_, err := tdx.ConvertFilesToCSV(ctx, sourceDir, stockDailyCSV, ".day")
	if err != nil {
		return nil, fmt.Errorf("failed to convert day files to csv: %w", err)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if err := db.ImportKlineDaily(stockDailyCSV); err != nil {
		return nil, fmt.Errorf("failed to import stock csv: %w", err)
	}

	if err := db.RebuildSymbolClass(); err != nil {
		return nil, fmt.Errorf("failed to rebuild symbol_class: %w", err)
	}

	fmt.Println("🚀 股票数据导入成功")
	return &TaskResult{State: StateCompleted, Message: "daily data imported"}, nil
}
