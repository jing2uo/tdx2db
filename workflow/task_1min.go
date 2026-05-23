package workflow

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/tdx"
)

var TaskUpdate1Min *Task

func init() {
	TaskUpdate1Min = &Task{
		Name:      "update_1min",
		DependsOn: []string{"prepare_tic"},
		SkipIf: func(ctx context.Context, db database.DataRepository, args *TaskArgs) bool {
			return !args.Min
		},
		Executor: executeUpdate1Min,
		OnError:  ErrorModeSkip,
	}
	registerTask(TaskUpdate1Min, "update")
}

func executeUpdate1Min(ctx context.Context, db database.DataRepository, args *TaskArgs) (*TaskResult, error) {
	validDates, _ := args.Extra[ExtraTicValidDates].([]time.Time)
	if len(validDates) == 0 {
		fmt.Println("🌲 分时数据无需更新")
		return &TaskResult{State: StateSkipped, Message: "no new 1min data"}, nil
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	endDate := validDates[len(validDates)-1]
	fmt.Printf("🐌 开始转换分钟数据\n")
	if err := tdx.DatatoolCreate(args.TempDir, "min", endDate); err != nil {
		return nil, fmt.Errorf("failed to run DatatoolMinCreate: %w", err)
	}

	fmt.Printf("🐌 开始转换分时数据\n")
	stock1MinCSV := filepath.Join(args.TempDir, "1min.csv")
	if _, err := tdx.ConvertFilesToCSV(ctx, args.VipdocDir, stock1MinCSV, ".01"); err != nil {
		return nil, fmt.Errorf("failed to convert .01 files to csv: %w", err)
	}

	if err := db.ImportKline1Min(stock1MinCSV); err != nil {
		return nil, fmt.Errorf("failed to import 1-minute line csv: %w", err)
	}
	fmt.Println("📊 分时数据导入成功")
	return &TaskResult{State: StateCompleted, Message: "1min data imported"}, nil
}
