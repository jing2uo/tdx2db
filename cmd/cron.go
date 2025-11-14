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
	"github.com/jing2uo/tdx2db/utils"
)

type XdxrIndex map[string][]model.XdxrData

func Cron(dbPath string, minline string) error {

	if dbPath == "" {
		return fmt.Errorf("database path cannot be empty")
	}
	dbConfig := model.DBConfig{Path: dbPath}
	db, err := database.Connect(dbConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	latestStockDate, err := database.GetStockTableLatestDate(db)
	if err != nil {
		return fmt.Errorf("failed to get latest date from database: %w", err)
	}
	fmt.Printf("📅 日线数据的最新日期为 %s\n", latestStockDate.Format("2006-01-02"))

	err = UpdateStocksDaily(db, latestStockDate)
	if err != nil {
		return fmt.Errorf("failed to update daily stock data: %w", err)
	}

	err = UpdateStocksMinLine(db, latestStockDate, minline)
	if err != nil {
		return fmt.Errorf("failed to update minute-line stock data: %w", err)
	}

	err = UpdateGbbq(db)
	if err != nil {
		return fmt.Errorf("failed to update GBBQ: %w", err)
	}

	err = UpdateFactors(db)
	if err != nil {
		return fmt.Errorf("failed to calculate factors: %w", err)
	}

	fmt.Printf("🔄 更新前复权数据视图 (%s)\n", database.QfqViewName)
	if err := database.CreateQfqView(db); err != nil {
		return fmt.Errorf("failed to create qfq view: %w", err)
	}

	fmt.Printf("🔄 更新后复权数据视图 (%s)\n", database.HfqViewName)
	if err := database.CreateHfqView(db); err != nil {
		return fmt.Errorf("failed to create hfq view: %w", err)
	}

	fmt.Println("🚀 今日任务执行成功")
	return nil
}

func UpdateStocksDaily(db *sql.DB, latestDate time.Time) error {
	validDates, err := prepareTdxData(latestDate, "day")
	if err != nil {
		return fmt.Errorf("failed to prepare tdx data: %w", err)
	}
	if len(validDates) > 0 {
		fmt.Printf("🐢 开始转换日线数据\n")
		_, err := tdx.ConvertDayFiles2Csv(VipdocDir, ValidPrefixes, StockCSV)
		if err != nil {
			return fmt.Errorf("failed to convert day files to CSV: %w", err)
		}
		if err := database.ImportStockCsv(db, StockCSV); err != nil {
			return fmt.Errorf("failed to import stock CSV: %w", err)
		}
		fmt.Println("📊 日线数据导入成功")
	} else {
		fmt.Println("🌲 日线数据无需更新")

	}
	return nil
}

func UpdateStocksMinLine(db *sql.DB, latestDate time.Time, minline string) error {
	if minline == "" {
		return nil
	}

	validDates, err := prepareTdxData(latestDate, "tic")
	if err != nil {
		return fmt.Errorf("failed to prepare tdx data: %w", err)
	}
	if len(validDates) > 0 {
		parts := strings.Split(minline, ",")
		for _, p := range parts {
			switch p {
			case "1":
				_, err := tdx.ConvertMinFiles2Csv(VipdocDir, ValidPrefixes, ".01", OneMinLineCSV)
				if err != nil {
					return fmt.Errorf("failed to convert .01 files to CSV: %w", err)
				}
				if err := database.Import1MinLineCsv(db, OneMinLineCSV); err != nil {
					return fmt.Errorf("failed to import 1-minute line CSV: %w", err)
				}
				fmt.Println("📊 1分钟数据导入成功")

			case "5":
				_, err := tdx.ConvertMinFiles2Csv(VipdocDir, ValidPrefixes, ".5", FiveMinLineCSV)
				if err != nil {
					return fmt.Errorf("failed to convert .5 files to CSV: %w", err)
				}
				if err := database.Import5MinLineCsv(db, FiveMinLineCSV); err != nil {
					return fmt.Errorf("failed to import 5-minute line CSV: %w", err)
				}
				fmt.Println("📊 5分钟数据导入成功")
			}
		}

	} else {
		fmt.Println("🌲 分时数据无需更新")

	}
	return nil
}

func UpdateGbbq(db *sql.DB) error {
	fmt.Println("🐢 开始下载股本变迁数据")

	gbbqFile, err := getGbbqFile(DataDir)
	if err != nil {
		return fmt.Errorf("failed to download GBBQ file: %w", err)
	}
	gbbqCSV := filepath.Join(DataDir, "gbbq.csv")
	if _, err := tdx.ConvertGbbqFile2Csv(gbbqFile, gbbqCSV); err != nil {
		return fmt.Errorf("failed to convert GBBQ to CSV: %w", err)
	}

	if err := database.ImportGbbqCsv(db, gbbqCSV); err != nil {
		return fmt.Errorf("failed to import GBBQ CSV into database: %w", err)
	}

	fmt.Printf("🔄 更新除权除息数据视图 (%s)\n", database.XdxrViewName)
	if err := database.CreateXdxrView(db); err != nil {
		return fmt.Errorf("failed to create xdxr view: %w", err)
	}

	fmt.Printf("🔄 更新市值换手数据视图 (%s)\n", database.TurnoverViewName)
	if err := database.CreateTurnoverView(db); err != nil {
		return fmt.Errorf("failed to create turnover view: %w", err)
	}

	fmt.Println("📈 股本变迁数据导入成功")
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
	xdxrIndex, err := buildXdxrIndex(db)

	if err != nil {
		return fmt.Errorf("failed to build GBBQ index: %w", err)
	}

	symbols, err := database.QueryAllSymbols(db)
	if err != nil {
		return fmt.Errorf("failed to query all stock symbols: %w", err)
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
				results <- result{"", fmt.Errorf("failed to query stock data for symbol %s: %w", sym, err)}
				return
			}
			xdxrData := getXdxrByCode(xdxrIndex, sym)

			factors, err := tdx.CalculateFqFactor(stockData, xdxrData)
			if err != nil {
				results <- result{"", fmt.Errorf("failed to calculate factor for symbol %s: %w", sym, err)}
				return
			}
			// 将因子格式化为 CSV 行
			var sb strings.Builder
			for _, factor := range factors {
				row := fmt.Sprintf("%s,%s,%.4f,%.4f,%.4f,%.4f\n",
					factor.Symbol,
					factor.Date.Format("2006-01-02"),
					factor.Close,
					factor.PreClose,
					factor.QfqFactor,
					factor.HfqFactor,
				)
				sb.WriteString(row)
			}
			results <- result{sb.String(), nil}
		}(symbol)
	}

	// 等待所有处理n完成并关闭结果通道
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

func buildXdxrIndex(db *sql.DB) (XdxrIndex, error) {
	index := make(XdxrIndex)

	xdxrData, err := database.QueryAllXdxr(db)
	if err != nil {
		return nil, fmt.Errorf("failed to query xdxr data: %w", err)
	}

	for _, data := range xdxrData {
		code := data.Code
		index[code] = append(index[code], data)
	}

	return index, nil
}

func getXdxrByCode(index XdxrIndex, symbol string) []model.XdxrData {
	code := symbol[2:]
	if data, exists := index[code]; exists {
		return data
	}
	return []model.XdxrData{}
}

func prepareTdxData(latestDate time.Time, dataType string) ([]time.Time, error) {
	var dates []time.Time

	for d := latestDate.Add(24 * time.Hour); !d.After(Today); d = d.Add(24 * time.Hour) {
		dates = append(dates, d)
	}

	if len(dates) == 0 {
		return nil, nil
	}

	var targetPath, urlTemplate, fileSuffix, dataTypeCN string

	switch dataType {
	case "day":
		targetPath = filepath.Join(VipdocDir, "refmhq")
		urlTemplate = "https://www.tdx.com.cn/products/data/data/g4day/%s.zip"
		fileSuffix = "day"
		dataTypeCN = "日线"
	case "tic":
		targetPath = filepath.Join(VipdocDir, "newdatetick")
		urlTemplate = "https://www.tdx.com.cn/products/data/data/g4tic/%s.zip"
		fileSuffix = "tic"
		dataTypeCN = "分时"
	default:
		return nil, fmt.Errorf("unknown data type: %s", dataType)
	}

	if err := os.MkdirAll(targetPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create target directory: %w", err)
	}

	fmt.Printf("🐢 开始下载%s数据\n", dataTypeCN)

	validDates := make([]time.Time, 0, len(dates))

	for _, date := range dates {
		dateStr := date.Format("20060102")
		url := fmt.Sprintf(urlTemplate, dateStr)
		fileName := fmt.Sprintf("%s%s.zip", dateStr, fileSuffix)
		filePath := filepath.Join(targetPath, fileName)

		status, err := utils.DownloadFile(url, filePath)
		switch status {
		case 200:

			fmt.Printf("✅ 已下载 %s 的数据\n", dateStr)

			if err := utils.UnzipFile(filePath, targetPath); err != nil {
				fmt.Printf("⚠️ 解压文件 %s 失败: %v\n", filePath, err)
				continue
			}

			validDates = append(validDates, date)
		case 404:
			fmt.Printf("🟡 %s 非交易日或数据尚未更新\n", dateStr)
			continue
		default:
			if err != nil {
				return nil, nil
			}
		}

	}

	if len(validDates) > 0 {
		endDate := validDates[len(validDates)-1]
		switch dataType {
		case "day":
			if err := tdx.DatatoolCreate(DataDir, "day", endDate); err != nil {
				return nil, fmt.Errorf("failed to run DatatoolDayCreate: %w", err)
			}

		case "tic":
			endDate := validDates[len(validDates)-1]
			fmt.Printf("🐢 开始转档分笔数据\n")
			if err := tdx.DatatoolCreate(DataDir, "tick", endDate); err != nil {
				return nil, fmt.Errorf("failed to run DatatoolTickCreate: %w", err)
			}
			fmt.Printf("🐢 开始转换分钟数据\n")
			if err := tdx.DatatoolCreate(DataDir, "min", endDate); err != nil {
				return nil, fmt.Errorf("failed to run DatatoolMinCreate: %w", err)
			}
		}
	}

	return validDates, nil
}

func getGbbqFile(cacheDir string) (string, error) {
	zipPath := filepath.Join(cacheDir, "gbbq.zip")
	gbbqURL := "http://www.tdx.com.cn/products/data/data/dbf/gbbq.zip"
	if _, err := utils.DownloadFile(gbbqURL, zipPath); err != nil {
		return "", fmt.Errorf("failed to download GBBQ zip file: %w", err)
	}

	unzipPath := filepath.Join(cacheDir, "gbbq-temp")
	if err := utils.UnzipFile(zipPath, unzipPath); err != nil {
		return "", fmt.Errorf("failed to unzip GBBQ file: %w", err)
	}

	return filepath.Join(unzipPath, "gbbq"), nil
}
