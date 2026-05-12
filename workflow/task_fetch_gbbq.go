package workflow

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/utils"
)

var TaskFetchGBBQ *Task

func init() {
	TaskFetchGBBQ = &Task{
		Name:      "fetch_gbbq",
		DependsOn: []string{},
		SkipIf:    skipIfPlan(func(p *WorkPlan) bool { return !p.NeedGbbq }),
		Executor:  executeFetchGBBQ,
	}
	registerTask(TaskFetchGBBQ, "update")
}

// executeFetchGBBQ 下载 gbbq.zip 并解压到 args.TempDir/gbbq-temp/。
// gbbq 二进制 (gbbq-temp/gbbq) 供 update_gbbq 解码，
// 内嵌的 zhb.zip (gbbq-temp/zhb.zip) 供 update_holidays 读取。
func executeFetchGBBQ(ctx context.Context, db database.DataRepository, args *TaskArgs) (*TaskResult, error) {
	fmt.Println("🐢 开始下载股本变迁数据")

	zipPath := filepath.Join(args.TempDir, "gbbq.zip")
	gbbqURL := "http://www.tdx.com.cn/products/data/data/dbf/gbbq.zip"
	if _, err := utils.DownloadFile(gbbqURL, zipPath); err != nil {
		return nil, fmt.Errorf("failed to download GBBQ zip file: %w", err)
	}

	unzipPath := filepath.Join(args.TempDir, "gbbq-temp")
	if err := utils.UnzipFile(zipPath, unzipPath, true); err != nil {
		return nil, fmt.Errorf("failed to unzip GBBQ file: %w", err)
	}

	return &TaskResult{State: StateCompleted, Message: "gbbq fetched"}, nil
}
