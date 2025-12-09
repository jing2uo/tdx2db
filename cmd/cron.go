package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/jing2uo/tdx2db/calc"
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

	dbConfig := model.DBConfig{
		DSN:  dbPath,
		Type: model.DBTypeDuckDB,
	}

	// 2. 创建驱动实例
	db, err := database.NewDatabase(dbConfig)
	if err != nil {
		return fmt.Errorf("failed to create database driver: %w", err)
	}

	// 防御性编程：虽然有 err 检查，但再检查一次 nil 更稳妥
	if db == nil {
		return fmt.Errorf("database driver is nil even though no error was returned")
	}
	// 2. 连接数据库
	if err := db.Connect(); err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	err = UpdateStocksDaily(db)
	if err != nil {
		return fmt.Errorf("failed to update daily stock data: %w", err)
	}

	err = UpdateStocksMinLine(db, minline)
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

	fmt.Println("🚀 今日任务执行成功")
	return nil
}

func UpdateStocksDaily(db database.DataRepository) error {
	latestDate, err := db.GetLatestDate(model.TableStocksDaily.TableName, "date")
	if err != nil {
		return fmt.Errorf("failed to get latest date from database: %w", err)
	}
	fmt.Printf("📅 日线数据最新日期为 %s\n", latestDate.Format("2006-01-02"))

	validDates, err := prepareTdxData(latestDate, "day")
	if err != nil {
		return fmt.Errorf("failed to prepare tdx data: %w", err)
	}
	if len(validDates) > 0 {
		fmt.Printf("🐢 开始转换日线数据\n")
		_, err := tdx.ConvertFiles2Csv(VipdocDir, ValidPrefixes, StockCSV, ".day")
		if err != nil {
			return fmt.Errorf("failed to convert day files to CSV: %w", err)
		}
		if err := db.ImportDailyStocks(StockCSV); err != nil {
			return fmt.Errorf("failed to import stock CSV: %w", err)
		}
		fmt.Println("📊 日线数据导入成功")
	} else {
		fmt.Println("🌲 日线数据无需更新")

	}
	return nil
}

func UpdateStocksMinLine(db database.DataRepository, minline string) error {
	if minline == "" {
		return nil
	}

	parts := strings.Split(minline, ",")
	need1Min := false
	need5Min := false
	for _, p := range parts {
		if p == "1" {
			need1Min = true
		}
		if p == "5" {
			need5Min = true
		}
	}

	var latestDate time.Time
	yesterday := Today.AddDate(0, 0, -1)

	if need1Min && need5Min {

		d1, err1 := db.GetLatestDate(model.TableStocks1Min.TableName, "datetime")
		is1MinEmpty := (err1 != nil || d1.IsZero())

		d5, err2 := db.GetLatestDate(model.TableStocks5Min.TableName, "datetime")
		is5MinEmpty := (err2 != nil || d5.IsZero())

		if is1MinEmpty && is5MinEmpty {
			fmt.Println("🛑 警告：数据库中没有分时数据")
			fmt.Println("🚧 将处理今天的数据，历史请自行导入")
			latestDate = yesterday

		} else if !d1.Equal(d5) {
			return fmt.Errorf("1分钟数据最新日期[%s] 与 5分钟数据最新日期[%s] 不同。请先单独执行 '1' 或 '5' 保持一致后再使用组合命令。",
				d1.Format("2006-01-02"), d5.Format("2006-01-02"))

		} else {
			latestDate = d1
			fmt.Printf("📅 分时数据最新日期为 %s\n", latestDate.Format("2006-01-02"))
		}

	} else {
		var typeLabel string

		if need1Min {
			latestDate, _ = db.GetLatestDate(model.TableStocks1Min.TableName, "datetime")
			typeLabel = "1分钟"
		} else {
			latestDate, _ = db.GetLatestDate(model.TableStocks5Min.TableName, "datetime")
			typeLabel = "5分钟"
		}

		if latestDate.IsZero() {
			fmt.Printf("🛑 警告：数据库中没有 %s 数据\n", typeLabel)
			fmt.Println("🚧 将处理今天的数据，历史请自行导入")
			latestDate = yesterday
		} else {
			fmt.Printf("📅 %s数据最新日期为 %s\n", typeLabel, latestDate.Format("2006-01-02"))
		}
	}

	validDates, err := prepareTdxData(latestDate, "tic")
	if err != nil {
		return fmt.Errorf("failed to prepare tdx data: %w", err)
	}

	if len(validDates) >= 30 {
		return fmt.Errorf("分时数据超过30天未更新，请手动补齐后继续")

	}

	if len(validDates) > 0 {
		fmt.Printf("🐢 开始转换分时数据\n")
		for _, p := range parts {
			switch p {
			case "1":
				_, err := tdx.ConvertFiles2Csv(VipdocDir, ValidPrefixes, OneMinLineCSV, ".01")
				if err != nil {
					return fmt.Errorf("failed to convert .01 files to CSV: %w", err)
				}
				if err := db.Import1MinStocks(OneMinLineCSV); err != nil {
					return fmt.Errorf("failed to import 1-minute line CSV: %w", err)
				}
				fmt.Println("📊 1分钟数据导入成功")

			case "5":
				_, err := tdx.ConvertFiles2Csv(VipdocDir, ValidPrefixes, FiveMinLineCSV, ".5")
				if err != nil {
					return fmt.Errorf("failed to convert .5 files to CSV: %w", err)
				}
				if err := db.Import5MinStocks(FiveMinLineCSV); err != nil {
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

func UpdateGbbq(db database.DataRepository) error {
	fmt.Println("🐢 开始下载股本变迁数据")

	gbbqFile, err := getGbbqFile(DataDir)
	if err != nil {
		return fmt.Errorf("failed to download GBBQ file: %w", err)
	}
	gbbqCSV := filepath.Join(DataDir, "gbbq.csv")
	if _, err := tdx.ConvertGbbqFile2Csv(gbbqFile, gbbqCSV); err != nil {
		return fmt.Errorf("failed to convert GBBQ to CSV: %w", err)
	}

	if err := db.ImportGBBQ(gbbqCSV); err != nil {
		return fmt.Errorf("failed to import GBBQ CSV into database: %w", err)
	}

	fmt.Println("📈 股本变迁数据导入成功")
	return nil
}

func UpdateFactors(db database.DataRepository) error {
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

	symbols, err := db.GetAllSymbols()
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
	writerWg.Go(func() {
		for res := range results {
			if res.err != nil {
				fmt.Printf("错误：%v\n", res.err)
				continue
			}
			if _, err := outFile.WriteString(res.rows); err != nil {
				fmt.Printf("写入 CSV 失败：%v\n", err)
			}
		}
	})

	// 并发处理每个符号
	for _, symbol := range symbols {
		wg.Add(1)
		sem <- struct{}{}
		go func(sym string) {
			defer wg.Done()
			defer func() { <-sem }()
			stockData, err := db.QueryStockData(sym, nil, nil)
			if err != nil {
				results <- result{"", fmt.Errorf("failed to query stock data for symbol %s: %w", sym, err)}
				return
			}
			xdxrData := getXdxrByCode(xdxrIndex, sym)

			factors, err := calc.CalculateFqFactor(stockData, xdxrData)
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

	if err := db.ImportAdjustFactors(csvPath); err != nil {
		return fmt.Errorf("failed to import factor data: %w", err)
	}
	fmt.Println("🔢 复权因子导入成功")

	return nil
}

func buildXdxrIndex(db database.DataRepository) (XdxrIndex, error) {
	index := make(XdxrIndex)

	xdxrData, err := db.QueryAllXdxr()
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
