package cmd

import (
	"context"
	"fmt"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/workflow"
)

func Init(ctx context.Context, dbURI, dayFileDir string) error {
	db, err := database.NewDB(dbURI)
	if err != nil {
		return fmt.Errorf("failed to create database driver: %w", err)
	}

	if err := db.Connect(); err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	if err := db.InitSchema(); err != nil {
		return fmt.Errorf("failed to initialize schema: %w", err)
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	count, err := db.CountStocksDaily()
	if err != nil {
		return fmt.Errorf("failed to check database status: %w", err)
	}

	if count > 0 {
		fmt.Printf("🙈 数据库已包含 %d 条日线记录\n", count)
		fmt.Printf("🎉 无需初始化\n")
		return nil
	}

	executor := workflow.NewTaskExecutor(db, workflow.GetRegisteredTasks())

	args := &workflow.TaskArgs{
		DayFileDir:    dayFileDir,
		TempDir:       TempDir,
		VipdocDir:     VipdocDir,
		ValidPrefixes: ValidPrefixes,
		Today:         GetToday(),
	}

	taskNames := workflow.GetInitTaskNames()

	if err := executor.Run(ctx, taskNames, args); err != nil {
		return fmt.Errorf("workflow execution failed: %w", err)
	}

	fmt.Println("🚀 初始化完成")
	return nil
}
