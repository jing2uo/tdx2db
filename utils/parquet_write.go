package utils

import (
	"fmt"
	"os"

	"github.com/parquet-go/parquet-go"
)

type ParquetWriter[T any] struct {
	file   *os.File
	writer *parquet.GenericWriter[T]
}

// NewParquetWriter 初始化一个新的写入器
// filename: 文件路径
// options: Parquet 配置（如压缩、Buffer大小）
func NewParquetWriter[T any](filename string, options ...parquet.WriterOption) (*ParquetWriter[T], error) {
	// 1. 创建文件
	f, err := os.Create(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	// 2. 设置默认配置
	defaultOpts := []parquet.WriterOption{
		parquet.Compression(&parquet.Snappy),
		parquet.WriteBufferSize(50 * 1024 * 1024),
		parquet.PageBufferSize(64 * 1024),
	}
	finalOpts := append(defaultOpts, options...)

	// 3. 创建 GenericWriter
	pw := parquet.NewGenericWriter[T](f, finalOpts...)

	return &ParquetWriter[T]{
		file:   f,
		writer: pw,
	}, nil
}

// Write 写入一批数据
func (p *ParquetWriter[T]) Write(data []T) error {
	_, err := p.writer.Write(data)
	return err
}

// Close 关闭 Writer 和文件
func (p *ParquetWriter[T]) Close() error {
	// 1. 先关闭 Parquet Writer (写入 Footer)
	if err := p.writer.Close(); err != nil {
		p.file.Close()
		return fmt.Errorf("failed to close parquet writer: %w", err)
	}

	// 2. 再关闭物理文件
	if err := p.file.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	}

	return nil
}
