package cmd

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/jing2uo/tdx2db/database"
)

func ExportToCSV(dbPath string, outputDir string, fromDate string) error {
	start := time.Now()

	// 验证 fromDate 参数的格式
	if fromDate != "" {
		_, err := time.Parse("2006-01-02", fromDate)
		if err != nil {
			return fmt.Errorf("fromDate 参数格式无效: %w. 请务必使用 'YYYY-MM-DD' 格式", err)
		}
	}

	// 1. 连接到 DuckDB 数据库
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return fmt.Errorf("failed to open database at %s: %w", dbPath, err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}

	// 2. 确保输出的子目录存在
	dataDir := filepath.Join(outputDir, "data")
	factorDir := filepath.Join(outputDir, "factor")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return fmt.Errorf("failed to create data directory %s: %w", dataDir, err)
	}
	if err := os.MkdirAll(factorDir, 0755); err != nil {
		return fmt.Errorf("failed to create factor directory %s: %w", factorDir, err)
	}

	// 3. 查询所有唯一的股票代码 (symbols)
	fmt.Println("🔍 查询所有股票代码")
	symbols, err := database.QueryAllSymbols(db)
	if err != nil {
		return fmt.Errorf("查询所有符号失败：%w", err)
	}
	fmt.Printf("✅ 找到 %d 只股票，开始导出\n", len(symbols))

	// 4. 设置并发处理
	var wg sync.WaitGroup
	maxConcurrency := runtime.NumCPU()
	sem := make(chan struct{}, maxConcurrency)
	errChan := make(chan error, len(symbols)*2) // 错误通道容量加倍，每个 symbol 有两个任务

	// 5. 遍历所有股票代码并启动 goroutine 进行处理
	for _, symbol := range symbols {
		wg.Add(1)
		sem <- struct{}{}

		go func(sym string) {
			defer wg.Done()
			defer func() { <-sem }()

			// --- 任务1: 导出股票数据 (data) ---
			dataCsvPath := filepath.Join(dataDir, fmt.Sprintf("%s.csv", sym))
			dataWhereClause := fmt.Sprintf("WHERE symbol = '%s'", sym)
			if fromDate != "" {
				// 如果提供了 fromDate，则在 WHERE 子句中添加日期过滤条件
				dataWhereClause += fmt.Sprintf(" AND date > '%s'", fromDate)
			}
			dataQuery := fmt.Sprintf(
				"COPY (SELECT * FROM %s %s ORDER BY date) TO '%s' (FORMAT CSV, HEADER)",
				database.StocksSchema.Name,
				dataWhereClause,
				dataCsvPath,
			)

			if _, err := db.Exec(dataQuery); err != nil {
				errChan <- fmt.Errorf("[data] 导出 %s 到 %s 失败：%w", sym, dataCsvPath, err)
				return // 如果数据导出失败，则不继续导出因子
			}

			// --- 任务2: 导出因子数据 (factor)，始终为全量 ---
			factorCsvPath := filepath.Join(factorDir, fmt.Sprintf("%s.csv", sym))
			factorTableName := database.FactorSchema.Name                // 明确因子表名
			factorWhereClause := fmt.Sprintf("WHERE symbol = '%s'", sym) // 因子查询不使用 fromDate

			factorQuery := fmt.Sprintf(
				"COPY (SELECT * FROM %s %s ORDER BY date) TO '%s' (FORMAT CSV, HEADER)",
				factorTableName,
				factorWhereClause,
				factorCsvPath,
			)

			if _, err := db.Exec(factorQuery); err != nil {
				errChan <- fmt.Errorf("[factor] 导出 %s 到 %s 失败：%w", sym, factorCsvPath, err)
				return
			}

		}(symbol)
	}

	// 6. 等待所有 goroutine 完成
	wg.Wait()
	close(errChan)

	// 7. 检查在处理过程中是否有错误发生
	var exportErrors []error
	for err := range errChan {
		exportErrors = append(exportErrors, err)
		log.Printf("导出错误: %v", err)
	}

	if len(exportErrors) > 0 {
		return fmt.Errorf("导出过程中发生 %d 个错误", len(exportErrors))
	}

	fmt.Printf("🎉 导出成功，数据位于 %s，耗时 %s\n", outputDir, time.Since(start))
	return nil
}
