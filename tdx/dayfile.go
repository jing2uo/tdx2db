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

const maxConcurrency = 16

func ConvertDayFiles2Csv(dayFilePath string, validPrefixes []string, outputCSV string) (string, error) {
	// Collect matching .day files
	var files []string
	err := filepath.WalkDir(dayFilePath, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("failed to access path %s: %w", path, err)
		}
		if !d.IsDir() && strings.HasSuffix(path, ".day") {
			symbol := strings.TrimSuffix(filepath.Base(path), ".day")
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
		return "", fmt.Errorf("failed to traverse directory %s: %w", dayFilePath, err)
	}

	if len(files) == 0 {
		return "", fmt.Errorf("no valid .day files found")
	}

	// Create output CSV file
	outFile, err := os.Create(outputCSV)
	if err != nil {
		return "", fmt.Errorf("failed to create CSV file %s: %w", outputCSV, err)
	}
	defer outFile.Close()

	// Write CSV header
	if _, err := outFile.WriteString("symbol,open,high,low,close,amount,volume,date\n"); err != nil {
		return "", fmt.Errorf("failed to write CSV header to %s: %w", outputCSV, err)
	}

	// Channel for results and error collection
	type result struct {
		rows []string
		err  error
	}
	results := make(chan result, len(files))
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrency)

	// Process files concurrently
	for _, file := range files {
		wg.Add(1)
		sem <- struct{}{}
		go func(dayfile string) {
			defer func() { <-sem; wg.Done() }()
			rows, err := processDayFile(dayfile)
			results <- result{rows, err}
		}(file)
	}

	// Close results channel after all goroutines finish
	go func() { wg.Wait(); close(results) }()

	// Collect and write results
	var errors []string
	for res := range results {
		if res.err != nil {
			errors = append(errors, res.err.Error())
			continue
		}
		for _, row := range res.rows {
			if _, err := outFile.WriteString(row); err != nil {
				errors = append(errors, fmt.Sprintf("failed to write CSV row to %s: %v", outputCSV, err))
			}
		}
	}

	if len(errors) > 0 {
		return outputCSV, fmt.Errorf("errors occurred during processing:\n%s", strings.Join(errors, "\n"))
	}

	return outputCSV, nil
}

func processDayFile(dayfile string) ([]string, error) {
	fileInfo, err := os.Stat(dayfile)
	if err != nil {
		return nil, fmt.Errorf("failed to access file %s: %w", dayfile, err)
	}
	if fileInfo.Size() == 0 {
		return nil, nil
	}

	inFile, err := os.Open(dayfile)
	if err != nil {
		return nil, fmt.Errorf("failed to open DAY file %s: %w", dayfile, err)
	}
	defer inFile.Close()

	// Extract symbol from filename
	symbol := strings.TrimSuffix(filepath.Base(dayfile), ".day")
	if symbol == "" {
		return nil, fmt.Errorf("invalid filename: %s, expected .day extension", dayfile)
	}

	// Buffer for reading (32 records * 32 bytes)
	const batchSize = 32
	buffer := make([]byte, batchSize*32)
	var rows []string
	var csvBuilder strings.Builder
	csvBuilder.Grow(1024 * 1024) // 1MB buffer

	for {
		n, err := inFile.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read file %s: %w", dayfile, err)
		}
		if n%32 != 0 {
			return nil, fmt.Errorf("invalid file format in %s: data length %d is not a multiple of 32", dayfile, n)
		}

		for i := 0; i < n/32; i++ {
			record, err := parseDayRecord(buffer[i*32 : (i+1)*32])
			if err != nil {
				return nil, fmt.Errorf("failed to parse record at offset %d in %s: %w", i*32, dayfile, err)
			}
			dateStr, err := formatDate(record.Date)
			if err != nil {
				return nil, fmt.Errorf("failed to format date for record at offset %d in %s: %w", i*32, dayfile, err)
			}
			csvBuilder.WriteString(fmt.Sprintf("%s,%.2f,%.2f,%.2f,%.2f,%.2f,%d,%s\n",
				symbol,
				float64(record.Open)/100,
				float64(record.High)/100,
				float64(record.Low)/100,
				float64(record.Close)/100,
				record.Amount,
				record.Volume,
				dateStr))
			if csvBuilder.Len() >= 1024*1024 {
				rows = append(rows, csvBuilder.String())
				csvBuilder.Reset()
			}
		}
	}

	if csvBuilder.Len() > 0 {
		rows = append(rows, csvBuilder.String())
	}

	if len(rows) == 0 {
		return nil, fmt.Errorf("no valid records found in %s", dayfile)
	}

	return rows, nil
}

func parseDayRecord(data []byte) (model.DayfileRecord, error) {
	if len(data) != 32 {
		return model.DayfileRecord{}, fmt.Errorf("invalid record length: expected 32 bytes, got %d", len(data))
	}

	var record model.DayfileRecord
	if err := binary.Read(bytes.NewReader(data), binary.LittleEndian, &record); err != nil {
		return model.DayfileRecord{}, fmt.Errorf("failed to parse binary record: %w", err)
	}

	return record, nil
}

func formatDate(date uint32) (string, error) {
	d := int(date)
	year := d / 10000
	month := (d % 10000) / 100
	day := d % 100

	if year < 1900 || year > 9999 || month < 1 || month > 12 || day < 1 || day > 31 {
		return "", fmt.Errorf("invalid date value: %08d", date)
	}

	return fmt.Sprintf("%04d-%02d-%02d", year, month, day), nil
}
