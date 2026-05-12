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

var TaskUpdateGBBQ *Task

func init() {
	TaskUpdateGBBQ = &Task{
		Name:      "update_gbbq",
		DependsOn: []string{"fetch_gbbq"},
		SkipIf:    skipIfPlan(func(p *WorkPlan) bool { return !p.NeedGbbq }),
		Executor:  executeUpdateGBBQ,
	}
	registerTask(TaskUpdateGBBQ, "update")
}

func executeUpdateGBBQ(ctx context.Context, db database.DataRepository, args *TaskArgs) (*TaskResult, error) {
	gbbqFile := filepath.Join(args.TempDir, "gbbq-temp", "gbbq")

	gbbqData, err := tdx.DecodeGbbqFile(gbbqFile)
	if err != nil {
		return nil, fmt.Errorf("failed to decode GBBQ: %w", err)
	}

	gbbqCSV := filepath.Join(args.TempDir, "gbbq.csv")
	gbbqCw, err := utils.NewCSVWriter[model.GbbqData](gbbqCSV)
	if err != nil {
		return nil, fmt.Errorf("failed to create GBBQ CSV writer: %w", err)
	}
	if err := gbbqCw.Write(gbbqData); err != nil {
		return nil, err
	}
	gbbqCw.Close()

	if err := db.ImportGBBQ(gbbqCSV); err != nil {
		return nil, fmt.Errorf("failed to import GBBQ csv into database: %w", err)
	}

	fmt.Println("📈 股本变迁数据导入成功")
	return &TaskResult{State: StateCompleted, Rows: len(gbbqData), Message: "gbbq data imported"}, nil
}
