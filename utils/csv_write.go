package utils

import (
	"encoding/csv"
	"fmt"
	"os"
	"reflect"
	"time"
)

// CSVWriter 通用 CSV 写入器
type CSVWriter[T any] struct {
	file          *os.File
	writer        *csv.Writer
	headerWritten bool
	columns       []columnInfo
}

type columnInfo struct {
	Index      int    // 字段索引
	HeaderName string // CSV 表头 (来自 col 标签)
	IsTime     bool   // 字段本身是否是 time.Time
	IsPtrTime  bool   // 字段本身是否是 *time.Time
	IsDateType bool   // 是否标记了 type:"date"
}

// NewCSVWriter 初始化
func NewCSVWriter[T any](filename string) (*CSVWriter[T], error) {
	// 1. 创建文件
	f, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	// 2. 初始化 CSV Writer
	w := csv.NewWriter(f)

	// 3. 解析结构体 Tag (只需做一次)
	cols, err := analyzeStructTags[T]()
	if err != nil {
		f.Close()
		return nil, err
	}

	return &CSVWriter[T]{
		file:    f,
		writer:  w,
		columns: cols,
	}, nil
}

// analyzeStructTags 解析 col 和 type 标签
func analyzeStructTags[T any]() ([]columnInfo, error) {
	var t T
	typ := reflect.TypeOf(t)
	if typ.Kind() == reflect.Ptr {
		typ = typ.Elem()
	}
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("generic type T must be a struct")
	}

	var cols []columnInfo
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)

		// 1. 获取 col 标签作为表头
		colTag := field.Tag.Get("col")
		if colTag == "" {
			// 如果没有 col 标签，可以选择跳过，或者用字段名，这里假设用字段名兜底
			colTag = field.Name
		}

		// 2. 获取 type 标签
		typeTag := field.Tag.Get("type")
		isDateType := (typeTag == "date") // 标记是否需要转 yyyy-mm-dd

		// 3. 判断是否为 Time 类型
		isTime := field.Type == reflect.TypeOf(time.Time{})
		isPtrTime := field.Type == reflect.TypeOf((*time.Time)(nil))

		cols = append(cols, columnInfo{
			Index:      i,
			HeaderName: colTag,
			IsTime:     isTime,
			IsPtrTime:  isPtrTime,
			IsDateType: isDateType,
		})
	}
	return cols, nil
}

// Write 写入数据
func (cw *CSVWriter[T]) Write(data []T) error {
	if len(data) == 0 {
		return nil
	}

	// 1. 写入表头
	if !cw.headerWritten {
		headers := make([]string, len(cw.columns))
		for i, col := range cw.columns {
			headers[i] = col.HeaderName
		}
		if err := cw.writer.Write(headers); err != nil {
			return fmt.Errorf("failed to write header: %w", err)
		}
		cw.headerWritten = true
	}

	// 2. 写入数据行
	record := make([]string, len(cw.columns))
	for _, item := range data {
		val := reflect.ValueOf(item)
		if val.Kind() == reflect.Ptr {
			val = val.Elem()
		}

		for i, col := range cw.columns {
			fieldVal := val.Field(col.Index)

			// --- 日期处理逻辑 ---
			if col.IsTime || col.IsPtrTime {
				var t time.Time
				isValid := false

				// 获取时间对象
				if col.IsTime {
					t = fieldVal.Interface().(time.Time)
					isValid = !t.IsZero()
				} else if !fieldVal.IsNil() {
					t = *fieldVal.Interface().(*time.Time)
					isValid = !t.IsZero()
				}

				if !isValid {
					record[i] = "" // 空时间或 nil 指针留空
				} else {
					// 核心判断：如果 type:"date"，用短格式；否则用默认长格式
					if col.IsDateType {
						record[i] = t.Format("2006-01-02")
					} else {
						record[i] = t.Format(time.RFC3339) // 或 "2006-01-02 15:04:05"
					}
				}
				continue
			}
			// ------------------

			// 其他类型通用处理
			record[i] = fmt.Sprint(fieldVal.Interface())
		}

		if err := cw.writer.Write(record); err != nil {
			return fmt.Errorf("failed to write record: %w", err)
		}
	}

	return nil
}

func (cw *CSVWriter[T]) Close() error {
	cw.writer.Flush()
	if err := cw.writer.Error(); err != nil {
		cw.file.Close()
		return fmt.Errorf("failed to flush: %w", err)
	}
	return cw.file.Close()
}
