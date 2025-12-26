package tdx

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/utils"
)

const (
	recordSize = 32
)

// ConvertFilesToCSV 转换 TDX 文件到 CSV
func ConvertFilesToCSV(ctx context.Context, inputDir string, validPrefixes []string, outputFile string, suffix string) (string, error) {
	switch suffix {
	case ".day":
		return runConversion[model.StockData](ctx, inputDir, validPrefixes, outputFile, suffix, processDayFile)
	case ".01", ".5":
		return runConversion[model.StockMinData](ctx, inputDir, validPrefixes, outputFile, suffix, processMinFile)
	default:
		return "", fmt.Errorf("unsupported suffix: %s", suffix)
	}
}

func runConversion[T any](
	ctx context.Context,
	inputDir string,
	validPrefixes []string,
	outputFile string,
	suffix string,
	parser func([]byte, string) ([]T, error),
) (string, error) {

	files, err := collectFiles(inputDir, validPrefixes, suffix)
	if err != nil {
		return "", err
	}

	if len(files) == 0 {
		return outputFile, nil
	}

	cw, err := utils.NewCSVWriter[T](outputFile)
	if err != nil {
		return "", fmt.Errorf("failed to create CSV writer: %w", err)
	}
	defer cw.Close()

	pipeline := utils.NewPipeline[string, T]()

	result, err := pipeline.Run(
		ctx,
		files,
		func(ctx context.Context, file string) ([]T, error) {
			return readFileAndParse(ctx, file, suffix, parser)
		},
		func(rows []T) error {
			return cw.Write(rows)
		},
	)

	if err != nil {
		return outputFile, err
	}

	if result.HasErrors() {
		return outputFile, fmt.Errorf("occurred %d errors, first: %v",
			len(result.Errors), result.FirstError())
	}

	return outputFile, nil
}

func readFileAndParse[T any](
	ctx context.Context,
	filename string,
	suffix string,
	parser func([]byte, string) ([]T, error),
) ([]T, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

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

func processDayFile(data []byte, symbol string) ([]model.StockData, error) {
	n := len(data)
	if n%recordSize != 0 {
		return nil, fmt.Errorf("invalid file size: %d", n)
	}
	count := n / recordSize
	rows := make([]model.StockData, 0, count)

	var offset int
	for i := 0; i < count; i++ {
		offset = i * recordSize

		dateRaw := binary.LittleEndian.Uint32(data[offset : offset+4])
		openRaw := binary.LittleEndian.Uint32(data[offset+4 : offset+8])
		highRaw := binary.LittleEndian.Uint32(data[offset+8 : offset+12])
		lowRaw := binary.LittleEndian.Uint32(data[offset+12 : offset+16])
		closeRaw := binary.LittleEndian.Uint32(data[offset+16 : offset+20])

		amountBits := binary.LittleEndian.Uint32(data[offset+20 : offset+24])
		amount := math.Float32frombits(amountBits)

		volRaw := binary.LittleEndian.Uint32(data[offset+24 : offset+28])

		t, err := parseDate(dateRaw)
		if err != nil {
			continue
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

func parseDate(d uint32) (time.Time, error) {
	year := int(d / 10000)
	month := int((d % 10000) / 100)
	day := int(d % 100)
	if year < 1900 || month < 1 || month > 12 || day < 1 || day > 31 {
		return time.Time{}, fmt.Errorf("invalid date")
	}
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.Local), nil
}

func parseDateTime(dateRaw, timeRaw uint16) (time.Time, error) {
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
