package tdx

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/jing2uo/tdx2db/model"
)

// ConvertMinfiles2Csv 遍历指定目录，将所有符合前缀和后缀条件的分钟线数据文件转换为一个CSV文件。
func ConvertMinfiles2Csv(minFilePath string, validPrefixes []string, suffix string, outputCSV string) (string, error) {
	// 1. 收集所有匹配的分钟线文件
	var files []string
	err := filepath.WalkDir(minFilePath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("failed to access path %s: %w", path, err)
		}
		if !d.IsDir() {
			baseName := filepath.Base(path)
			// 检查后缀是否匹配
			if strings.HasSuffix(baseName, suffix) {

				// 提取股票代码 (symbol)
				symbol := strings.TrimSuffix(baseName, suffix)
				// 检查前缀是否匹配
				for _, prefix := range validPrefixes {
					if strings.HasPrefix(symbol, prefix) {
						files = append(files, path)
						return nil // 找到匹配项，处理下一个文件
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		return "", fmt.Errorf("failed to traverse directory %s: %w", minFilePath, err)
	}

	if len(files) == 0 {
		return "", fmt.Errorf("no valid %s files found", suffix)
	}

	// 2. 创建并写入 CSV 文件头
	outFile, err := os.Create(outputCSV)
	if err != nil {
		return "", fmt.Errorf("failed to create CSV file %s: %w", outputCSV, err)
	}
	defer outFile.Close()

	if _, err := outFile.WriteString("symbol,open,high,low,close,amount,volume,datetime\n"); err != nil {
		return "", fmt.Errorf("failed to write CSV header to %s: %w", outputCSV, err)
	}

	// 3. 设置并发处理
	type result struct {
		rows []string
		err  error
	}
	results := make(chan result, len(files))
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrency) // 使用信号量控制并发数量

	// 4. 并发处理所有文件
	for _, file := range files {
		wg.Add(1)
		sem <- struct{}{}
		go func(minfile string) {
			defer func() { <-sem; wg.Done() }()
			rows, err := processMinFile(minfile, suffix)
			results <- result{rows, err}
		}(file)
	}

	// 启动一个 goroutine 等待所有任务完成，然后关闭 results channel
	go func() { wg.Wait(); close(results) }()

	// 5. 收集并写入结果
	var errors []string
	for res := range results {
		if res.err != nil {
			errors = append(errors, res.err.Error())
			continue
		}
		for _, row := range res.rows {
			if _, err := outFile.WriteString(row); err != nil {
				// 避免因单行写入失败而中断整个过程
				errors = append(errors, fmt.Sprintf("failed to write CSV row to %s: %v", outputCSV, err))
			}
		}
	}

	if len(errors) > 0 {
		return outputCSV, fmt.Errorf("errors occurred during processing:\n%s", strings.Join(errors, "\n"))
	}

	return outputCSV, nil
}

// processMinFile 读取并解析单个分钟线数据文件。
func processMinFile(minfile string, suffix string) ([]string, error) {
	fileInfo, err := os.Stat(minfile)
	if err != nil {
		return nil, fmt.Errorf("failed to access file %s: %w", minfile, err)
	}
	if fileInfo.Size() == 0 {
		return nil, nil // 空文件直接跳过，不报错
	}

	inFile, err := os.Open(minfile)
	if err != nil {
		return nil, fmt.Errorf("failed to open minute file %s: %w", minfile, err)
	}
	defer inFile.Close()

	// 从文件名中提取 symbol
	baseName := filepath.Base(minfile)
	var symbol string
	if strings.HasSuffix(baseName, suffix) {
		symbol = strings.TrimSuffix(baseName, suffix)
	}
	if symbol == "" {
		return nil, fmt.Errorf("invalid filename: %s, does not have a valid suffix", minfile)
	}

	// 设置缓冲区 (例如 100条记录 * 32字节)
	const batchSize = 100
	buffer := make([]byte, batchSize*32)
	var rows []string
	var csvBuilder strings.Builder
	csvBuilder.Grow(1024 * 1024) // 预分配 1MB 内存以提高性能

	for {
		n, err := inFile.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read file %s: %w", minfile, err)
		}
		// 确保读取的数据是 32 字节的整数倍
		if n%32 != 0 {
			return nil, fmt.Errorf("invalid file format in %s: data length %d is not a multiple of 32", minfile, n)
		}

		// 逐条解析记录
		for i := 0; i < n/32; i++ {
			record, err := parseMinRecord(buffer[i*32 : (i+1)*32])
			if err != nil {
				return nil, fmt.Errorf("failed to parse record at offset %d in %s: %w", i*32, minfile, err)
			}
			dateTimeStr, err := formatDateTime(record.DateRaw, record.TimeRaw)
			if err != nil {
				return nil, fmt.Errorf("failed to format datetime for record at offset %d in %s: %w", i*32, minfile, err)
			}
			// 格式化为CSV行
			csvBuilder.WriteString(fmt.Sprintf("%s,%.2f,%.2f,%.2f,%.2f,%.2f,%d,%s\n",
				symbol,
				float64(record.Open)/100,
				float64(record.High)/100,
				float64(record.Low)/100,
				float64(record.Close)/100,
				record.Amount,
				record.Volume,
				dateTimeStr))

			// 当 builder 达到一定大小时，将其内容存入 rows 并重置，避免内存占用过大
			if csvBuilder.Len() >= 1024*1024 {
				rows = append(rows, csvBuilder.String())
				csvBuilder.Reset()
			}
		}
	}

	// 添加剩余部分
	if csvBuilder.Len() > 0 {
		rows = append(rows, csvBuilder.String())
	}

	return rows, nil
}

// parseMinRecord 将32字节的二进制数据解析为 MinfileRecord 结构体。
func parseMinRecord(data []byte) (model.MinfileRecord, error) {
	if len(data) != 32 {
		return model.MinfileRecord{}, fmt.Errorf("invalid record length: expected 32 bytes, got %d", len(data))
	}

	var record model.MinfileRecord
	reader := bytes.NewReader(data)
	if err := binary.Read(reader, binary.LittleEndian, &record); err != nil {
		return model.MinfileRecord{}, fmt.Errorf("failed to parse binary record: %w", err)
	}

	return record, nil
}

// formatDateTime 根据通达信的压缩算法将日期和时间值转换为 "YYYY-MM-DD HH:MM" 格式。
func formatDateTime(dateRaw, timeRaw uint16) (string, error) {
	// 解析日期
	year := int(dateRaw)/2048 + 2004
	month := (int(dateRaw) % 2048) / 100
	day := (int(dateRaw) % 2048) % 100

	// 解析时间
	hour := int(timeRaw) / 60
	minute := int(timeRaw) % 60

	// 基本的有效性检查
	if year < 2004 || year > 9999 || month < 1 || month > 12 || day < 1 || day > 31 {
		return "", fmt.Errorf("invalid date value from raw: %d", dateRaw)
	}
	if hour < 0 || hour > 23 || minute < 0 || minute > 59 {
		return "", fmt.Errorf("invalid time value from raw: %d", timeRaw)
	}

	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d", year, month, day, hour, minute), nil
}
