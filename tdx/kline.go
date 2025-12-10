package tdx

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/jing2uo/tdx2db/model"
	"github.com/parquet-go/parquet-go"
)

// Constants & Configuration

var maxConcurrency = runtime.NumCPU()

const (
	recordSize = 32 // TDX 记录固定字节大小
)

// batchData 泛型容器，用于在 Channel 中传递解析好的数据块
type batchData[T any] struct {
	Rows []T
	Err  error
}

// Main Entry Point

// ConvertFilesToParquet 自动根据后缀选择类型进行转换
func ConvertFilesToParquet(inputDir string, validPrefixes []string, outputFile string, suffix string) (string, error) {
	switch suffix {
	case ".day":
		// 使用泛型函数处理 StockData
		return runConversion[model.StockData](inputDir, validPrefixes, outputFile, suffix, processDayFile)
	case ".01", ".5":
		// 使用泛型函数处理 StockMinData
		return runConversion[model.StockMinData](inputDir, validPrefixes, outputFile, suffix, processMinFile)
	default:
		return "", fmt.Errorf("unsupported suffix: %s", suffix)
	}
}

// Generic Conversion Engine

func runConversion[T any](
	inputDir string,
	validPrefixes []string,
	outputFile string,
	suffix string,
	parser func([]byte, string) ([]T, error),
) (string, error) {

	// 1. 收集文件
	files, err := collectFiles(inputDir, validPrefixes, suffix)
	if err != nil {
		return "", err
	}

	// 2. 创建输出文件
	f, err := os.Create(outputFile)
	if err != nil {
		return "", fmt.Errorf("create file error: %w", err)
	}
	defer f.Close()

	// 3. 初始化 Parquet Generic Writer
	writerConfig := []parquet.WriterOption{
		// 使用 Snappy 压缩 (速度与压缩率平衡)
		parquet.Compression(&parquet.Snappy),

		// WriteBufferSize 相当于 RowGroup Size。
		// 设置为 48MB (通常 16MB-128MB 之间较好)，缓冲区满后会刷新为一个 RowGroup。
		// 大的 RowGroup 有利于压缩，但占用更多内存。
		parquet.WriteBufferSize(48 * 1024 * 1024),

		// PageBufferSize 是列数据页的目标大小。
		// 设置为 64KB，这是 Parquet 的推荐值，利于读取时的粒度控制。
		parquet.PageBufferSize(64 * 1024),
	}

	// 创建泛型 Writer
	pw := parquet.NewGenericWriter[T](f, writerConfig...)

	// 4. 并发管道设置
	batchChan := make(chan batchData[T], maxConcurrency*2)
	var producerWg sync.WaitGroup
	var consumerWg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrency) // 信号量控制读取并发度

	// 错误收集
	var errors []string
	var errMu sync.Mutex
	collectError := func(e error) {
		errMu.Lock()
		errors = append(errors, e.Error())
		errMu.Unlock()
	}

	// Consumer: 写入 Parquet
	consumerWg.Go(func() {
		defer pw.Close() // 确保数据落盘
		for batch := range batchChan {
			if batch.Err != nil {
				collectError(batch.Err)
				continue
			}
			if len(batch.Rows) > 0 {
				if _, err := pw.Write(batch.Rows); err != nil {
					collectError(fmt.Errorf("parquet write error: %w", err))
				}
			}
		}
	})

	// Producer: 读取并解析文件
	for _, file := range files {
		file := file
		producerWg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()

			rows, err := readFileAndParse(file, suffix, parser)
			batchChan <- batchData[T]{Rows: rows, Err: err}
		})
	}

	producerWg.Wait()
	close(batchChan)
	consumerWg.Wait()

	if len(errors) > 0 {
		return outputFile, fmt.Errorf("occurred %d errors, first: %s", len(errors), errors[0])
	}
	return outputFile, nil
}

// readFileAndParse 读取文件并解析
func readFileAndParse[T any](filename, suffix string, parser func([]byte, string) ([]T, error)) ([]T, error) {
	// TDX 单个文件通常很小，直接 ReadFile 读入内存是最快的，避免了 bufio 的开销
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", filename, err)
	}
	if len(data) == 0 {
		return nil, nil
	}

	symbol := strings.TrimSuffix(filepath.Base(filename), suffix)
	return parser(data, symbol)
}

// Parsers (Performance Critical)

// processDayFile 解析日线数据 (.day)
func processDayFile(data []byte, symbol string) ([]model.StockData, error) {
	n := len(data)
	if n%recordSize != 0 {
		return nil, fmt.Errorf("invalid file size: %d", n)
	}
	count := n / recordSize
	rows := make([]model.StockData, 0, count) // 预分配内存

	var offset int
	for i := 0; i < count; i++ {
		offset = i * recordSize

		// 格式布局 (32字节):
		// 00-03: Date (uint32 YYYYMMDD)
		// 04-07: Open (uint32 / 100)
		// 08-11: High (uint32 / 100)
		// 12-15: Low  (uint32 / 100)
		// 16-19: Close (uint32 / 100)
		// 20-23: Amount (float32)
		// 24-27: Volume (uint32)
		// 28-31: Reserved

		dateRaw := binary.LittleEndian.Uint32(data[offset : offset+4])
		openRaw := binary.LittleEndian.Uint32(data[offset+4 : offset+8])
		highRaw := binary.LittleEndian.Uint32(data[offset+8 : offset+12])
		lowRaw := binary.LittleEndian.Uint32(data[offset+12 : offset+16])
		closeRaw := binary.LittleEndian.Uint32(data[offset+16 : offset+20])

		// Amount 是 float32，需要 bits 转换
		amountBits := binary.LittleEndian.Uint32(data[offset+20 : offset+24])
		amount := math.Float32frombits(amountBits)

		volRaw := binary.LittleEndian.Uint32(data[offset+24 : offset+28])

		t, err := parseDate(dateRaw)
		if err != nil {
			continue // 忽略错误行
		}

		rows = append(rows, model.StockData{
			Symbol: symbol,
			Open:   float64(openRaw) / 100.0,
			High:   float64(highRaw) / 100.0,
			Low:    float64(lowRaw) / 100.0,
			Close:  float64(closeRaw) / 100.0,
			Amount: float64(amount),
			Volume: int64(volRaw),
			Date:   t,
		})
	}
	return rows, nil
}

// processMinFile 解析分钟数据 (.01 / .5)
func processMinFile(data []byte, symbol string) ([]model.StockMinData, error) {
	n := len(data)
	if n%recordSize != 0 {
		return nil, fmt.Errorf("invalid file size: %d", n)
	}
	count := n / recordSize
	rows := make([]model.StockMinData, 0, count)

	var offset int
	for i := 0; i < count; i++ {
		offset = i * recordSize

		// 格式布局 (32字节):
		// 00-01: Date (uint16 compressed)
		// 02-03: Time (uint16 compressed)
		// 04-07: Open ...

		dateRaw := binary.LittleEndian.Uint16(data[offset : offset+2])
		timeRaw := binary.LittleEndian.Uint16(data[offset+2 : offset+4])
		openRaw := binary.LittleEndian.Uint32(data[offset+4 : offset+8])
		highRaw := binary.LittleEndian.Uint32(data[offset+8 : offset+12])
		lowRaw := binary.LittleEndian.Uint32(data[offset+12 : offset+16])
		closeRaw := binary.LittleEndian.Uint32(data[offset+16 : offset+20])

		amountBits := binary.LittleEndian.Uint32(data[offset+20 : offset+24])
		amount := math.Float32frombits(amountBits)

		volRaw := binary.LittleEndian.Uint32(data[offset+24 : offset+28])

		t, err := parseDateTime(dateRaw, timeRaw)
		if err != nil {
			continue
		}

		rows = append(rows, model.StockMinData{
			Symbol:   symbol,
			Open:     float64(openRaw) / 100.0,
			High:     float64(highRaw) / 100.0,
			Low:      float64(lowRaw) / 100.0,
			Close:    float64(closeRaw) / 100.0,
			Amount:   float64(amount),
			Volume:   int64(volRaw),
			Datetime: t,
		})
	}
	return rows, nil
}

// Helpers

func parseDate(d uint32) (time.Time, error) {
	year := int(d / 10000)
	month := int((d % 10000) / 100)
	day := int(d % 100)
	// 基本校验
	if year < 1900 || month < 1 || month > 12 || day < 1 || day > 31 {
		return time.Time{}, fmt.Errorf("invalid date")
	}
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.Local), nil
}

func parseDateTime(dateRaw, timeRaw uint16) (time.Time, error) {
	// 通达信分钟线时间压缩算法
	year := int(dateRaw)/2048 + 2004
	month := (int(dateRaw) % 2048) / 100
	day := (int(dateRaw) % 2048) % 100
	hour := int(timeRaw) / 60
	minute := int(timeRaw) % 60

	if month < 1 || month > 12 || day < 1 || day > 31 {
		return time.Time{}, fmt.Errorf("invalid date")
	}
	return time.Date(year, time.Month(month), day, hour, minute, 0, 0, time.Local), nil
}

func collectFiles(root string, prefixes []string, suffix string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, suffix) {
			fname := filepath.Base(path)
			symbol := strings.TrimSuffix(fname, suffix)

			match := false
			if len(prefixes) == 0 {
				match = true
			} else {
				for _, p := range prefixes {
					if strings.HasPrefix(symbol, p) {
						match = true
						break
					}
				}
			}

			if match {
				files = append(files, path)
			}
		}
		return nil
	})
	return files, err
}
