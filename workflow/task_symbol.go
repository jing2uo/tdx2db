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
	fmt.Println("🧩 开始下载代码名称")

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
	fmt.Printf("🚀 代码名称导入成功\n")
	return &TaskResult{State: StateCompleted, Rows: len(names), Message: msg}, nil
}
