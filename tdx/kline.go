package tdx

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/jing2uo/tdx2db/model"
)

var maxConcurrency = runtime.NumCPU()

// RowData 用于在生产者和消费者之间传递单行CSV数据或错误。
type RowData struct {
	Line string
	Err  error
}

const (
	// 定义写入CSV时批处理的大小，累积到这个数量再一次性写入文件
	writeBatchSize = 16284
	// 定义从源文件中一次读取的缓冲区大小 (例如 1MB)
	readBufferSize = 1024 * 1024
	// 每条记录的固定大小（字节）
	recordSize = 32
)

// 将通达信的 .day, .01, 或 .5 文件转换为CSV文件。
func ConvertFiles2Csv(filePath string, validPrefixes []string, outputCSV string, suffix string) (string, error) {
	// 1. 根据文件后缀选择CSV头部和记录处理器
	var csvHeader string
	var recordProcessor func(recordBytes []byte, symbol string) (string, error)

	switch suffix {
	case ".day":
		csvHeader = "symbol,open,high,low,close,amount,volume,date\n"
		recordProcessor = processDayRecord
	case ".01", ".5":
		csvHeader = "symbol,open,high,low,close,amount,volume,datetime\n"
		recordProcessor = processMinRecord
	default:
		return "", fmt.Errorf("unsupported file suffix: '%s'. Supported are .day, .01, .5", suffix)
	}

	// 2. 收集所有匹配的文件
	files, err := collectFiles(filePath, validPrefixes, suffix)
	if err != nil {
		return "", err
	}

	// 3. 创建CSV文件并写入头部
	outFile, err := os.Create(outputCSV)
	if err != nil {
		return "", fmt.Errorf("failed to create CSV file %s: %w", outputCSV, err)
	}
	defer outFile.Close()

	if _, err := outFile.WriteString(csvHeader); err != nil {
		return "", fmt.Errorf("failed to write CSV header: %w", err)
	}

	// 4. 设置生产者-消费者模型
	rowChan := make(chan RowData, 1024)
	var producerWg sync.WaitGroup
	var consumerWg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrency)

	var errors []string
	var errorMutex sync.Mutex
	collectError := func(err error) {
		errorMutex.Lock()
		errors = append(errors, err.Error())
		errorMutex.Unlock()
	}

	// 5. 启动消费者 (CSV写入器) Goroutine
	consumerWg.Add(1)
	go func() {
		defer consumerWg.Done()
		batch := make([]string, 0, writeBatchSize)

		for data := range rowChan {
			if data.Err != nil {
				collectError(data.Err)
				continue
			}
			batch = append(batch, data.Line)
			if len(batch) >= writeBatchSize {
				if err := writeBatchToFile(outFile, batch); err != nil {
					collectError(err)
				}
				batch = batch[:0] // 高效清空切片
			}
		}

		// 处理最后一个未满的批次
		if len(batch) > 0 {
			if err := writeBatchToFile(outFile, batch); err != nil {
				collectError(err)
			}
		}
	}()

	// 6. 启动生产者 (文件读取器) Goroutines
	for _, file := range files {
		producerWg.Add(1)
		sem <- struct{}{}
		go func(filename string) {
			defer func() {
				<-sem
				producerWg.Done()
			}()
			// 调用通用的文件处理函数，它会将结果发送到channel
			processAndProduce(filename, suffix, rowChan, recordProcessor)
		}(file)
	}

	// 7. 等待所有任务完成
	producerWg.Wait()
	close(rowChan) // 关闭channel，通知消费者没有更多数据了
	consumerWg.Wait()

	if len(errors) > 0 {
		return outputCSV, fmt.Errorf("errors occurred during processing:\n%s", strings.Join(errors, "\n"))
	}

	return outputCSV, nil
}

// collectFiles 遍历目录并收集所有符合条件的文件路径。
func collectFiles(filePath string, validPrefixes []string, suffix string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(filePath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, suffix) {
			symbol := strings.TrimSuffix(filepath.Base(path), suffix)
			for _, prefix := range validPrefixes {
				if strings.HasPrefix(symbol, prefix) {
					files = append(files, path)
					return nil
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to traverse directory %s: %w", filePath, err)
	}
	if len(files) == 0 {
		return nil, fmt.Errorf("no valid '%s' files found with the given prefixes", suffix)
	}
	return files, nil
}

// processAndProduce 读取单个文件，使用指定的处理器函数解析记录，并将结果发送到channel。
func processAndProduce(filename, suffix string, rowChan chan<- RowData, processor func([]byte, string) (string, error)) {
	fileInfo, err := os.Stat(filename)
	if err != nil {
		rowChan <- RowData{Err: fmt.Errorf("could not stat file %s: %w", filename, err)}
		return
	}
	if fileInfo.Size() == 0 {
		return // 静默跳过空文件
	}

	inFile, err := os.Open(filename)
	if err != nil {
		rowChan <- RowData{Err: fmt.Errorf("failed to open file %s: %w", filename, err)}
		return
	}
	defer inFile.Close()

	symbol := strings.TrimSuffix(filepath.Base(filename), suffix)
	buffer := make([]byte, readBufferSize)

	for {
		n, err := inFile.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			rowChan <- RowData{Err: fmt.Errorf("failed to read file %s: %w", filename, err)}
			return
		}
		if n%recordSize != 0 {
			rowChan <- RowData{Err: fmt.Errorf("invalid file format in %s: data length %d is not a multiple of %d", filename, n, recordSize)}
			return
		}

		for i := 0; i < n/recordSize; i++ {
			recordBytes := buffer[i*recordSize : (i+1)*recordSize]
			csvLine, err := processor(recordBytes, symbol)
			if err != nil {
				// 发送错误，但继续处理文件中的其他记录
				rowChan <- RowData{Err: fmt.Errorf("failed to process record in %s: %w", filename, err)}
				continue
			}
			rowChan <- RowData{Line: csvLine}
		}
	}
}

// writeBatchToFile 将一批字符串高效地写入文件。
func writeBatchToFile(file *os.File, batch []string) error {
	if _, err := file.WriteString(strings.Join(batch, "")); err != nil {
		return fmt.Errorf("failed to write batch to %s: %v", file.Name(), err)
	}
	return nil
}

// --- 特定记录处理函数 ---

func processDayRecord(data []byte, symbol string) (string, error) {
	var record model.DayfileRecord
	if err := binary.Read(bytes.NewReader(data), binary.LittleEndian, &record); err != nil {
		return "", fmt.Errorf("binary read failed: %w", err)
	}
	dateStr, err := formatDate(record.Date)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s,%.2f,%.2f,%.2f,%.2f,%.2f,%d,%s\n",
		symbol,
		float64(record.Open)/100,
		float64(record.High)/100,
		float64(record.Low)/100,
		float64(record.Close)/100,
		record.Amount,
		record.Volume,
		dateStr), nil
}

func processMinRecord(data []byte, symbol string) (string, error) {
	var record model.MinfileRecord
	if err := binary.Read(bytes.NewReader(data), binary.LittleEndian, &record); err != nil {
		return "", fmt.Errorf("binary read failed: %w", err)
	}
	dateTimeStr, err := formatDateTime(record.DateRaw, record.TimeRaw)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s,%.2f,%.2f,%.2f,%.2f,%.2f,%d,%s\n",
		symbol,
		float64(record.Open)/100,
		float64(record.High)/100,
		float64(record.Low)/100,
		float64(record.Close)/100,
		record.Amount,
		record.Volume,
		dateTimeStr), nil
}

func formatDate(date uint32) (string, error) {
	d := int(date)
	year, month, day := d/10000, (d%10000)/100, d%100
	if year < 1990 || year > 2100 || month < 1 || month > 12 || day < 1 || day > 31 {
		return "", fmt.Errorf("invalid date value: %08d", date)
	}
	return fmt.Sprintf("%04d-%02d-%02d", year, month, day), nil
}

func formatDateTime(dateRaw, timeRaw uint16) (string, error) {
	year := int(dateRaw)/2048 + 2004
	month := (int(dateRaw) % 2048) / 100
	day := (int(dateRaw) % 2048) % 100
	hour := int(timeRaw) / 60
	minute := int(timeRaw) % 60
	if year < 1990 || year > 2100 || month < 1 || month > 12 || day < 1 || day > 31 {
		return "", fmt.Errorf("invalid date value from raw: %d", dateRaw)
	}
	if hour < 0 || hour > 23 || minute < 0 || minute > 59 {
		return "", fmt.Errorf("invalid time value from raw: %d", timeRaw)
	}
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d", year, month, day, hour, minute), nil
}
