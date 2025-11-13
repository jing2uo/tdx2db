package cmd

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jing2uo/tdx2db/tdx"
	"github.com/jing2uo/tdx2db/utils"
)

type InputSourceType int

const (
	DayFileDir InputSourceType = iota
	TicZip
	DayZip
	Min1FileDir
	Min5FileDir
)

type ConvertOptions struct {
	InputPath  string
	InputType  InputSourceType
	OutputPath string
}

func Convert(opts ConvertOptions) error {
	if opts.InputPath == "" {
		return errors.New("input path cannot be empty")
	}
	if opts.OutputPath == "" {
		return errors.New("output path cannot be empty")
	}

	if err := checkDirWritePermission(opts.OutputPath); err != nil {
		return err
	}

	dataDir := DataDir

	switch opts.InputType {

	case DayFileDir:
		fmt.Printf("📦 开始处理日线目录: %s\n", opts.InputPath)
		fileInfo, err := os.Stat(opts.InputPath)
		if os.IsNotExist(err) {
			return fmt.Errorf("day file directory does not exist: %s", opts.InputPath)
		}
		if err != nil {
			return fmt.Errorf("error checking day file directory: %w", err)
		}
		if !fileInfo.IsDir() {
			return fmt.Errorf("the specified path for dayfiledir is not a directory: %s", opts.InputPath)
		}

		var ValidPrefixes = []string{"sh", "sz", "bj"}

		output := filepath.Join(opts.OutputPath, "tdx2db_day.csv")

		fmt.Println("🐢 开始转换日线数据")
		_, err = tdx.ConvertDayfiles2Csv(opts.InputPath, ValidPrefixes, output)
		if err != nil {
			return fmt.Errorf("failed to convert day files: %w", err)
		}

		fmt.Printf("🔥 转换完成: %s\n", output)

	case Min1FileDir:
		fmt.Printf("📦 开始处理分时数据目录: %s\n", opts.InputPath)
		fileInfo, err := os.Stat(opts.InputPath)
		if os.IsNotExist(err) {
			return fmt.Errorf("1min file directory does not exist: %s", opts.InputPath)
		}
		if err != nil {
			return fmt.Errorf("error checking 1min file directory: %w", err)
		}
		if !fileInfo.IsDir() {
			return fmt.Errorf("the specified path for m1filedir is not a directory: %s", opts.InputPath)
		}

		var ValidPrefixes = []string{"sh", "sz", "bj"}

		output := filepath.Join(opts.OutputPath, "tdx2db_1min.csv")

		fmt.Println("🐢 开始转换 1 分钟数据")
		_, err = tdx.ConvertMinfiles2Csv(opts.InputPath, ValidPrefixes, ".01", output)
		if err != nil {
			return fmt.Errorf("failed to convert 1min files: %w", err)
		}

		fmt.Printf("🔥 转换完成: %s\n", output)

	case Min5FileDir:
		fmt.Printf("📦 开始处理分时数据目录: %s\n", opts.InputPath)
		fileInfo, err := os.Stat(opts.InputPath)
		if os.IsNotExist(err) {
			return fmt.Errorf("5min file directory does not exist: %s", opts.InputPath)
		}
		if err != nil {
			return fmt.Errorf("error checking 5min file directory: %w", err)
		}
		if !fileInfo.IsDir() {
			return fmt.Errorf("the specified path for m5filedir is not a directory: %s", opts.InputPath)
		}

		var ValidPrefixes = []string{"sh", "sz", "bj"}

		output := filepath.Join(opts.OutputPath, "tdx2db_5min.csv")

		fmt.Println("🐢 开始转换 5 分钟数据")
		_, err = tdx.ConvertMinfiles2Csv(opts.InputPath, ValidPrefixes, ".5", output)
		if err != nil {
			return fmt.Errorf("failed to convert 5min files: %w", err)
		}

		fmt.Printf("🔥 转换完成: %s\n", output)

	case TicZip:
		fmt.Printf("📦 开始处理四代 TIC 压缩文件: %s\n", opts.InputPath)
		_, err := os.Stat(opts.InputPath)
		if os.IsNotExist(err) {
			return fmt.Errorf("file does not exist: %s", opts.InputPath)
		}

		baseName := filepath.Base(opts.InputPath)
		ext := filepath.Ext(baseName)
		dateString := strings.TrimSuffix(baseName, ext)

		parsedDate, err := time.Parse("20060102", dateString)
		if err != nil {
			return fmt.Errorf("cannot parse date from filename '%s', please ensure the format is YYYYMMDD.zip: %w", baseName, err)
		}

		targetPath := filepath.Join(dataDir, "vipdoc", "newdatetick")
		if err := os.MkdirAll(targetPath, 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
		if err := utils.UnzipFile(opts.InputPath, targetPath); err != nil {
			return fmt.Errorf("failed to unzip file %s: %w", opts.InputPath, err)
		}

		fmt.Printf("🐢 开始转档分笔数据\n")
		if err := tdx.DatatoolTickCreate(dataDir, parsedDate, parsedDate); err != nil {
			return fmt.Errorf("failed to execute DatatoolTickCreate: %w", err)
		}
		if err := tdx.DatatoolMinCreate(dataDir, parsedDate, parsedDate); err != nil {
			return fmt.Errorf("failed to execute DatatoolMinCreate: %w", err)
		}

		dayFilesSourcePath := filepath.Join(dataDir, "vipdoc")
		var ValidPrefixes = []string{"sh", "sz", "bj"}

		min1_output := filepath.Join(opts.OutputPath, fmt.Sprintf("%s_1min.csv", dateString))
		min5_output := filepath.Join(opts.OutputPath, fmt.Sprintf("%s_5min.csv", dateString))

		fmt.Printf("🐢 开始转换 1 分钟数据\n")
		_, err = tdx.ConvertMinfiles2Csv(dayFilesSourcePath, ValidPrefixes, ".01", min1_output)
		if err != nil {
			return fmt.Errorf("failed to convert 1-minute files: %w", err)
		}

		fmt.Printf("🐢 开始转换 5 分钟数据\n")
		_, err = tdx.ConvertMinfiles2Csv(dayFilesSourcePath, ValidPrefixes, ".5", min5_output)
		if err != nil {
			return fmt.Errorf("failed to convert 5-minute files: %w", err)
		}

		fmt.Printf("🔥 转换完成\n")
		fmt.Printf("📊 1 分钟数据: %s\n", min1_output)
		fmt.Printf("📊 5 分钟数据: %s\n", min5_output)

	case DayZip:
		fmt.Printf("📦 开始处理四代行情压缩文件: %s\n", opts.InputPath)
		_, err := os.Stat(opts.InputPath)
		if os.IsNotExist(err) {
			return fmt.Errorf("file does not exist: %s", opts.InputPath)
		}
		baseName := filepath.Base(opts.InputPath)
		ext := filepath.Ext(baseName)
		dateString := strings.TrimSuffix(baseName, ext)

		parsedDate, err := time.Parse("20060102", dateString)
		if err != nil {
			return fmt.Errorf("cannot parse date from filename '%s', please ensure the format is YYYYMMDD.zip: %w", baseName, err)
		}

		unzipDestPath := filepath.Join(dataDir, "vipdoc", "refmhq")
		if err := os.MkdirAll(unzipDestPath, 0755); err != nil {
			return fmt.Errorf("failed to create unzip destination directory: %w", err)
		}
		if err := utils.UnzipFile(opts.InputPath, unzipDestPath); err != nil {
			return fmt.Errorf("failed to unzip file %s: %w", opts.InputPath, err)
		}

		fmt.Printf("🐢 开始转换日线数据\n")
		if err := tdx.DatatoolDayCreate(dataDir, parsedDate, parsedDate); err != nil {
			return fmt.Errorf("failed to execute DatatoolDayCreate: %w", err)
		}

		dayFilesSourcePath := filepath.Join(dataDir, "vipdoc")
		var ValidPrefixes = []string{"sh", "sz", "bj"}

		output := filepath.Join(opts.OutputPath, fmt.Sprintf("%s_day.csv", dateString))

		_, err = tdx.ConvertDayfiles2Csv(dayFilesSourcePath, ValidPrefixes, output)
		if err != nil {
			return fmt.Errorf("failed to convert day files: %w", err)
		}

		fmt.Printf("🔥 转换完成: %s\n", output)
	}

	return nil
}

func checkDirWritePermission(dirPath string) error {
	fileInfo, err := os.Stat(dirPath)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, 0755); err != nil {
			return fmt.Errorf("could not create output directory %s: %w", dirPath, err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("could not access output directory %s: %w", dirPath, err)
	}
	if !fileInfo.IsDir() {
		return fmt.Errorf("the specified output path is not a directory: %s", dirPath)
	}

	tmpFile, err := os.CreateTemp(dirPath, "test-")
	if err != nil {
		return fmt.Errorf("output directory %s is not writable: %w", dirPath, err)
	}
	tmpFile.Close()
	os.Remove(tmpFile.Name())

	return nil
}
