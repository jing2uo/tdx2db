package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jing2uo/tdx2db/tdx"
	"github.com/jing2uo/tdx2db/utils"
)

type InputSourceType int

type ConvertOptions struct {
	InputPath  string
	InputType  InputSourceType
	OutputPath string
}

const (
	DayFileDir InputSourceType = iota
	TicZip
	DayZip
	Min1FileDir
	Min5FileDir
)

func isDirType(t InputSourceType) bool {
	switch t {
	case DayFileDir, Min1FileDir, Min5FileDir:
		return true
	default:
		return false
	}
}

// Convert 转换 TDX 数据到 CSV
func Convert(ctx context.Context, opts ConvertOptions) error {
	if opts.InputPath == "" {
		return errors.New("input path cannot be empty")
	}
	if opts.OutputPath == "" {
		return errors.New("output path cannot be empty")
	}

	if err := utils.CheckOutputDir(opts.OutputPath); err != nil {
		return err
	}

	if isDirType(opts.InputType) {
		if err := utils.CheckDirectory(opts.InputPath); err != nil {
			return err
		}
	} else {
		if err := utils.CheckFile(opts.InputPath); err != nil {
			return err
		}
	}

	// 检查取消
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	dataDir := TempDir

	var bjPrefixes = []string{"bj43", "bj83", "bj87"}

	var stocksPrefixes = append(
		append([]string{}, MarketPrefixes...),
		bjPrefixes...,
	)

	var allPrefixes = append(
		append([]string{}, ValidPrefixes...),
		bjPrefixes...,
	)

	switch opts.InputType {

	case DayFileDir:
		fmt.Printf("📦 开始处理日线目录: %s\n", opts.InputPath)
		output := filepath.Join(opts.OutputPath, "tdx2db_day.csv")

		fmt.Println("🐢 开始转换日线数据")
		_, err := tdx.ConvertFilesToCSV(ctx, opts.InputPath, allPrefixes, output, ".day")
		if err != nil {
			return fmt.Errorf("failed to convert day files: %w", err)
		}

		fmt.Printf("🔥 转换完成: %s\n", output)

	case Min1FileDir:
		fmt.Printf("📦 开始处理分时数据目录: %s\n", opts.InputPath)
		output := filepath.Join(opts.OutputPath, "tdx2db_1min.csv")

		fmt.Println("🐢 开始转换 1 分钟数据")
		_, err := tdx.ConvertFilesToCSV(ctx, opts.InputPath, stocksPrefixes, output, ".01")
		if err != nil {
			return fmt.Errorf("failed to convert 1min files: %w", err)
		}

		fmt.Printf("🔥 转换完成: %s\n", output)

	case Min5FileDir:
		fmt.Printf("📦 开始处理分时数据目录: %s\n", opts.InputPath)
		output := filepath.Join(opts.OutputPath, "tdx2db_5min.csv")

		fmt.Println("🐢 开始转换 5 分钟数据")
		_, err := tdx.ConvertFilesToCSV(ctx, opts.InputPath, stocksPrefixes, output, ".5")
		if err != nil {
			return fmt.Errorf("failed to convert 5min files: %w", err)
		}

		fmt.Printf("🔥 转换完成: %s\n", output)

	case TicZip:
		fmt.Printf("📦 开始处理四代 TIC 压缩文件: %s\n", opts.InputPath)

		filename := filepath.Base(opts.InputPath)
		baseName := filename[:len(filename)-len(filepath.Ext(filename))]

		targetPath := filepath.Join(VipdocDir, "newdatetick")
		if err := os.MkdirAll(targetPath, 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
		if err := utils.UnzipFile(opts.InputPath, targetPath); err != nil {
			return fmt.Errorf("failed to unzip file %s: %w", opts.InputPath, err)
		}

		// 检查取消
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fmt.Printf("🐢 开始转档分笔数据\n")
		if err := tdx.DatatoolCreate(dataDir, "tick", GetToday()); err != nil {
			return fmt.Errorf("failed to execute DatatoolTickCreate: %w", err)
		}
		if err := tdx.DatatoolCreate(dataDir, "min", GetToday()); err != nil {
			return fmt.Errorf("failed to execute DatatoolMinCreate: %w", err)
		}

		min1_output := filepath.Join(opts.OutputPath, fmt.Sprintf("%s_1min.csv", baseName))
		min5_output := filepath.Join(opts.OutputPath, fmt.Sprintf("%s_5min.csv", baseName))

		fmt.Printf("🐢 开始转换 1 分钟数据\n")
		_, err := tdx.ConvertFilesToCSV(ctx, VipdocDir, stocksPrefixes, min1_output, ".01")
		if err != nil {
			return fmt.Errorf("failed to convert 1-minute files: %w", err)
		}

		// 检查取消
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fmt.Printf("🐢 开始转换 5 分钟数据\n")
		_, err = tdx.ConvertFilesToCSV(ctx, VipdocDir, stocksPrefixes, min5_output, ".5")
		if err != nil {
			return fmt.Errorf("failed to convert 5-minute files: %w", err)
		}

		fmt.Printf("🔥 转换完成\n")
		fmt.Printf("📊 1 分钟数据: %s\n", min1_output)
		fmt.Printf("📊 5 分钟数据: %s\n", min5_output)

	case DayZip:
		fmt.Printf("📦 开始处理四代行情压缩文件: %s\n", opts.InputPath)

		filename := filepath.Base(opts.InputPath)
		baseName := filename[:len(filename)-len(filepath.Ext(filename))]

		unzipDestPath := filepath.Join(VipdocDir, "refmhq")
		if err := os.MkdirAll(unzipDestPath, 0755); err != nil {
			return fmt.Errorf("failed to create unzip destination directory: %w", err)
		}
		if err := utils.UnzipFile(opts.InputPath, unzipDestPath); err != nil {
			return fmt.Errorf("failed to unzip file %s: %w", opts.InputPath, err)
		}

		// 检查取消
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fmt.Printf("🐢 开始转换日线数据\n")
		if err := tdx.DatatoolCreate(dataDir, "day", GetToday()); err != nil {
			return fmt.Errorf("failed to execute DatatoolDayCreate: %w", err)
		}

		output := filepath.Join(opts.OutputPath, fmt.Sprintf("%s_day.csv", baseName))

		_, err := tdx.ConvertFilesToCSV(ctx, VipdocDir, allPrefixes, output, ".day")
		if err != nil {
			return fmt.Errorf("failed to convert day files: %w", err)
		}

		fmt.Printf("🔥 转换完成: %s\n", output)
	}

	return nil
}
