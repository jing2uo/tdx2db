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

var TaskUpdateBlocks *Task

func init() {
	TaskUpdateBlocks = &Task{
		Name:      "update_blocks",
		DependsOn: []string{},
		Executor:  executeUpdateBlocks,
	}
	registerTask(TaskUpdateBlocks, "update")
}

func executeUpdateBlocks(ctx context.Context, db database.DataRepository, args *TaskArgs) (*TaskResult, error) {
	fmt.Println("🧩 开始拉取通达信在线板块数据")

	blockInfos, blockMembers, err := tdx.FetchOnlineBlocks(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch online blocks: %w", err)
	}

	infoCSV := filepath.Join(args.TempDir, "tdx_blocks_info.csv")
	infoWriter, err := utils.NewCSVWriter[model.BlockInfo](infoCSV)
	if err != nil {
		return nil, fmt.Errorf("failed to create block info CSV writer: %w", err)
	}
	if err := infoWriter.Write(blockInfos); err != nil {
		infoWriter.Close()
		return nil, err
	}
	infoWriter.Close()

	memberCSV := filepath.Join(args.TempDir, "tdx_blocks_member.csv")
	memberWriter, err := utils.NewCSVWriter[model.BlockMember](memberCSV)
	if err != nil {
		return nil, fmt.Errorf("failed to create block member CSV writer: %w", err)
	}
	if err := memberWriter.Write(blockMembers); err != nil {
		memberWriter.Close()
		return nil, err
	}
	memberWriter.Close()

	if err := db.ImportBlockInfo(infoCSV); err != nil {
		return nil, fmt.Errorf("failed to import block info csv: %w", err)
	}
	if err := db.ImportBlockMembers(memberCSV); err != nil {
		return nil, fmt.Errorf("failed to import block member csv: %w", err)
	}

	msg := fmt.Sprintf("blocks imported: %d info rows, %d member rows", len(blockInfos), len(blockMembers))
	fmt.Printf("🚀 板块数据导入成功: %d 个板块, %d 条成分关系\n", len(blockInfos), len(blockMembers))
	return &TaskResult{State: StateCompleted, Rows: len(blockInfos) + len(blockMembers), Message: msg}, nil
}
