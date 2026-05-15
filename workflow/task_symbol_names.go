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

var TaskUpdateSymbolNames *Task

func init() {
	TaskUpdateSymbolNames = &Task{
		Name:      "update_symbol_names",
		DependsOn: []string{},
		Executor:  executeUpdateSymbolNames,
	}
	registerTask(TaskUpdateSymbolNames, "update")
}

func executeUpdateSymbolNames(ctx context.Context, db database.DataRepository, args *TaskArgs) (*TaskResult, error) {
	fmt.Println("🧩 开始拉取通达信在线品种名称")

	names, err := tdx.FetchOnlineSymbolNames(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch online symbol names: %w", err)
	}

	csvPath := filepath.Join(args.TempDir, "symbol_name.csv")
	writer, err := utils.NewCSVWriter[model.SymbolName](csvPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create symbol name CSV writer: %w", err)
	}
	if err := writer.Write(names); err != nil {
		writer.Close()
		return nil, err
	}
	writer.Close()

	if err := db.ImportSymbolNames(csvPath); err != nil {
		return nil, fmt.Errorf("failed to import symbol name csv: %w", err)
	}

	msg := fmt.Sprintf("symbol names imported: %d rows", len(names))
	fmt.Printf("🚀 品种名称导入成功: %d 条记录\n", len(names))
	return &TaskResult{State: StateCompleted, Rows: len(names), Message: msg}, nil
}
