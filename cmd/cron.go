package cmd

import (
	"context"
	"fmt"

	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/workflow"
)

func Cron(ctx context.Context, dbURI, minline, tdxhome string) error {
	db, err := database.NewDB(dbURI)
	if err != nil {
		return fmt.Errorf("failed to create database driver: %w", err)
	}

	if err := db.Connect(); err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	if err := db.InitSchema(); err != nil {
		return fmt.Errorf("failed to initialize schema: %w", err)
	}

	defer db.Close()

	if err := ctx.Err(); err != nil {
		return err
	}

	executor := workflow.NewTaskExecutor(db, workflow.GetRegisteredTasks())

	args := &workflow.TaskArgs{
		Minline:       minline,
		TdxHome:       tdxhome,
		TempDir:       TempDir,
		VipdocDir:     VipdocDir,
		ValidPrefixes: ValidPrefixes,
		Today:         GetToday(),
	}

	taskNames := workflow.GetUpdateTaskNames()

	if err := executor.Run(ctx, taskNames, args); err != nil {
		return fmt.Errorf("workflow execution failed: %w", err)
	}

	fmt.Println("🚀 今日任务执行成功")
	return nil
}
