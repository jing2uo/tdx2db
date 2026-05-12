package workflow

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/tdx"
)

var TaskUpdateHolidays *Task

func init() {
	TaskUpdateHolidays = &Task{
		Name:      "update_holidays",
		DependsOn: []string{"fetch_gbbq"},
		SkipIf:    skipIfPlan(func(p *WorkPlan) bool { return !p.NeedHolidays }),
		Executor:  executeUpdateHolidays,
		OnError:   ErrorModeSkip,
	}
	registerTask(TaskUpdateHolidays, "update")
}

func executeUpdateHolidays(ctx context.Context, db database.DataRepository, args *TaskArgs) (*TaskResult, error) {
	zhbZipPath := filepath.Join(args.TempDir, "gbbq-temp", "zhb.zip")
	holidaysFile, err := tdx.ExportTdxHolidaysToCSV(zhbZipPath, args.TempDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "🚨 警告: %v\n", err)
		return &TaskResult{State: StateFailed, Error: err, Message: "holidays import warning"}, nil
	}

	if err := db.ImportHolidays(holidaysFile); err != nil {
		return nil, fmt.Errorf("failed to import holidays: %w", err)
	}

	fmt.Println("🗓️  交易日历导入成功")
	return &TaskResult{State: StateCompleted, Message: "holidays data imported"}, nil
}
