package cmd

import (
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/tdx"
	"github.com/jing2uo/tdx2db/utils"
)

const maxConcurrency = 16

type GbbqIndex map[string][]model.GbbqData

func Cron(dbPath string) error {
	start := time.Now()

	if dbPath == "" {
		return fmt.Errorf("database path cannot be empty")
	}
	dbConfig := model.DBConfig{Path: dbPath}
	db, err := database.Connect(dbConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	err = UpdateStocks(db)
	if err != nil {
		return fmt.Errorf("更新日线数据失败：%w", err)
	}

	err = UpdateGbbq(db)
	if err != nil {
		return fmt.Errorf("更新 GBBQ 数据失败：%w", err)
	}

	err = UpdateFactors(db)
	if err != nil {
		return fmt.Errorf("计算前收盘价和复权因子失败：%w", err)
	}

	fmt.Printf("🔄 创建/更新前复权数据视图 (%s)\n", database.QfqViewName)
	if err := database.CreateQfqView(db); err != nil {
		return fmt.Errorf("failed to create qfq view: %w", err)
	}

	fmt.Printf("✅ 处理完成，耗时 %s\n", time.Since(start))
	return nil
}

func UpdateStocks(db *sql.DB) error {
	latestDate, err := database.GetLatestDate(db)
	if err != nil {
		return fmt.Errorf("failed to get latest date from database: %w", err)
	}
	fmt.Printf("📅 日线数据的最新日期为 %s\n", latestDate.Format("2006-01-02"))

	today := time.Now().Truncate(24 * time.Hour)
	var dates []time.Time
	for d := latestDate.Add(24 * time.Hour); !d.After(today); d = d.Add(24 * time.Hour) {
		dates = append(dates, d)
	}

	refmhqPath := filepath.Join(DataDir, "vipdoc", "refmhq")
	if err := os.MkdirAll(filepath.Dir(refmhqPath), 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", refmhqPath, err)
	}
	fmt.Println("🛠️  开始下载日线数据")
	validDates := make([]time.Time, 0, len(dates))
	for _, date := range dates {
		dateStr := date.Format("20060102")
		url := fmt.Sprintf("https://www.tdx.com.cn/products/data/data/g4day/%s.zip", dateStr)
		fileName := fmt.Sprintf("%s.zip", dateStr)
		filePath := filepath.Join(DataDir, fileName)

		// Download file
		resp, err := http.Get(url)
		if err != nil {
			fmt.Printf("⚠️ 下载 %s 数据失败: %v\n", dateStr, err)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			fmt.Printf("🟡 %s 非交易日或数据尚未更新\n", dateStr)
			continue
		}

		// Save file
		out, err := os.Create(filePath)
		if err != nil {
			fmt.Printf("⚠️ 创建文件 %s 失败: %v\n", filePath, err)
			continue
		}

		if _, err := io.Copy(out, resp.Body); err != nil {
			out.Close()
			fmt.Printf("⚠️ 保存文件 %s 失败: %v\n", filePath, err)
			continue
		}
		out.Close()

		fmt.Printf("✅ 已下载 %s 的数据\n", dateStr)

		// Unzip file
		if err := utils.UnzipFile(filePath, refmhqPath); err != nil {
			fmt.Printf("⚠️ 解压文件 %s 失败: %v\n", filePath, err)
			continue
		}

		// Add date to valid dates
		validDates = append(validDates, date)
	}

	if len(validDates) > 0 {
		startDate := validDates[0]
		endDate := validDates[len(validDates)-1]
		if err := tdx.DatatoolCreate(DataDir, startDate, endDate); err != nil {
			return fmt.Errorf("failed to run DatatoolCreate: %w", err)
		}

		fmt.Printf("🛠  开始转换 dayfiles 为 CSV\n")
		_, err := tdx.ConvertDayfiles2Csv(filepath.Join(DataDir, "vipdoc"), ValidPrefixes, StockCSV)
		if err != nil {
			return fmt.Errorf("failed to convert day files to CSV: %w", err)
		}

		fmt.Printf("🔥 转换完成\n")

		// Import stock CSV
		if err := database.ImportStockCsv(db, StockCSV); err != nil {
			return fmt.Errorf("failed to import stock CSV: %w", err)
		}
		fmt.Println("📊 股票数据导入成功")
	} else {
		fmt.Println("🌲 无需下载")

	}
	return nil
}

func UpdateGbbq(db *sql.DB) error {
	fmt.Println("🛠️  开始下载除权除息数据")
	if db == nil {
		return fmt.Errorf("database connection cannot be nil")
	}

	gbbqCSV := filepath.Join(DataDir, "gbbq.csv")
	if _, err := tdx.GetLatestGbbqCsv(DataDir, gbbqCSV); err != nil {
		return fmt.Errorf("failed to download GBBQ CSV: %w", err)
	}

	if err := database.ImportGbbqCsv(db, gbbqCSV); err != nil {
		return fmt.Errorf("failed to import GBBQ CSV into database: %w", err)
	}

	fmt.Println("📈 除权除息数据更新成功")
	return nil
}

func UpdateFactors(db *sql.DB) error {
	csvPath := filepath.Join(DataDir, "factors.csv")

	outFile, err := os.Create(csvPath)
	if err != nil {
		return fmt.Errorf("failed to create CSV file %s: %w", csvPath, err)
	}
	defer outFile.Close()

	fmt.Println("📟 计算所有股票前收盘价")
	// 构建 GBBQ 索引
	gbbqIndex, err := buildGbbqIndex(db)

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
			gbbqData := getGbbqByCode(gbbqIndex, sym)

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

	if err := database.ImportFactorCsv(db, csvPath); err != nil {
		return fmt.Errorf("failed to import factor data: %w", err)
	}
	fmt.Println("🔢 复权因子导入成功")

	return nil
}
func buildGbbqIndex(db *sql.DB) (GbbqIndex, error) {
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

func getGbbqByCode(index GbbqIndex, symbol string) []model.GbbqData {
	code := symbol[2:]
	if data, exists := index[code]; exists {
		return data
	}
	return []model.GbbqData{}
}
