package cmd

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/tdx"
)

const maxConcurrency = 16

type GbbqIndex map[string][]model.GbbqData

func Factor(dbPath string) error {
	start := time.Now()

	if dbPath == "" {
		return fmt.Errorf("database path cannot be empty")
	}
	fmt.Println("📟 计算所有股票的前收盘价和复权因子")
	dbConfig := model.DBConfig{Path: dbPath}
	db, err := database.Connect(dbConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	csvPath := filepath.Join(DataDir, "factors.csv")

	err = ProcessAllFactors(db, csvPath)
	if err != nil {
		return fmt.Errorf("处理因子失败：%w", err)
	}
	fmt.Println("🔢 导入前收盘价和复权因子")

	if err := database.ImportFactorCsv(db, csvPath); err != nil {
		return fmt.Errorf("failed to import factor data: %w", err)
	}

	fmt.Printf("✅ 处理完成，耗时 %s\n", time.Since(start))
	return nil
}

func BuildGbbqIndex(db *sql.DB) (GbbqIndex, error) {
	index := make(GbbqIndex)

	gbbqData, err := database.QueryAllGbbq(db)
	if err != nil {
		return nil, fmt.Errorf("failed to query GBBQ data: %v", err)
	}

	for _, data := range gbbqData {
		code := data.Code
		index[code] = append(index[code], data)
	}

	return index, nil
}

func GetGbbqByCode(index GbbqIndex, symbol string) []model.GbbqData {
	code := symbol[2:]
	if data, exists := index[code]; exists {
		return data
	}
	return []model.GbbqData{}
}

func ProcessAllFactors(db *sql.DB, outputCSV string) error {
	outFile, err := os.Create(outputCSV)
	if err != nil {
		return fmt.Errorf("failed to create CSV file %s: %w", outputCSV, err)
	}
	defer outFile.Close()

	// 构建 GBBQ 索引
	gbbqIndex, err := BuildGbbqIndex(db)

	if err != nil {
		return fmt.Errorf("构建 GBBQ 索引失败：%w", err)
	}

	symbols, err := database.QueryAllSymbols(db)
	if err != nil {
		return fmt.Errorf("查询所有符号失败：%w", err)
	}

	// 定义结果通道
	type result struct {
		rows string
		err  error
	}
	results := make(chan result, len(symbols))
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrency)

	// 启动写入协程
	var writerWg sync.WaitGroup
	writerWg.Add(1)
	go func() {
		defer writerWg.Done()
		for res := range results {
			if res.err != nil {
				fmt.Printf("错误：%v\n", res.err)
				continue
			}
			if _, err := outFile.WriteString(res.rows); err != nil {
				fmt.Printf("写入 CSV 失败：%v\n", err)
			}
		}
	}()

	// 并发处理每个符号
	for _, symbol := range symbols {
		wg.Add(1)
		sem <- struct{}{}
		go func(sym string) {
			defer wg.Done()
			defer func() { <-sem }()
			stockData, err := database.QueryStockData(db, sym, nil, nil)
			if err != nil {
				results <- result{"", fmt.Errorf("获取 %s 的股票数据失败：%w", sym, err)}
				return
			}
			gbbqData := GetGbbqByCode(gbbqIndex, sym)

			factors, err := tdx.CalculateFqFactor(stockData, gbbqData)
			if err != nil {
				results <- result{"", fmt.Errorf("计算 %s 的因子失败：%w", sym, err)}
				return
			}
			// 将因子格式化为 CSV 行
			var sb strings.Builder
			for _, factor := range factors {
				row := fmt.Sprintf("%s,%s,%.4f,%.4f,%.4f\n",
					factor.Symbol,
					factor.Date.Format("2006-01-02"),
					factor.Close,
					factor.PreClose,
					factor.Factor)
				sb.WriteString(row)
			}
			results <- result{sb.String(), nil}
		}(symbol)
	}

	// 等待所有处理完成并关闭结果通道
	go func() {
		wg.Wait()
		close(results)
	}()

	// 等待写入协程完成
	writerWg.Wait()
	return nil
}
