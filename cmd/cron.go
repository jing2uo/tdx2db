package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jing2uo/tdx2db/calc"
	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/tdx"
	"github.com/jing2uo/tdx2db/utils"
)

type XdxrIndex map[string][]model.XdxrData

func Cron(dbURI string, minline string) error {
	db, err := database.NewDB(dbURI)
	if err != nil {
		return fmt.Errorf("failed to create database driver: %w", err)
	}

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
		_, err := tdx.ConvertFilesToParquet(VipdocDir, ValidPrefixes, StockDailyParquet, ".day")
		if err != nil {
			return fmt.Errorf("failed to convert day files to parquet: %w", err)
		}
		if err := db.ImportDailyStocks(StockDailyParquet); err != nil {
			return fmt.Errorf("failed to import stock parquet: %w", err)
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
				_, err := tdx.ConvertFilesToParquet(VipdocDir, ValidPrefixes, Stock1MinParquet, ".01")
				if err != nil {
					return fmt.Errorf("failed to convert .01 files to parquet: %w", err)
				}
				if err := db.Import1MinStocks(Stock1MinParquet); err != nil {
					return fmt.Errorf("failed to import 1-minute line parquet: %w", err)
				}
				fmt.Println("📊 1分钟数据导入成功")

			case "5":
				_, err := tdx.ConvertFilesToParquet(VipdocDir, ValidPrefixes, Stock5MinParquet, ".5")
				if err != nil {
					return fmt.Errorf("failed to convert .5 files to parquet: %w", err)
				}
				if err := db.Import5MinStocks(Stock5MinParquet); err != nil {
					return fmt.Errorf("failed to import 5-minute line parquet: %w", err)
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

	gbbqFile, err := getGbbqFile(TempDir)
	if err != nil {
		return fmt.Errorf("failed to download GBBQ file: %w", err)
	}
	gbbqParquet := filepath.Join(TempDir, "gbbq.parquet")
	if _, err := tdx.ConvertGbbqFileToParquet(gbbqFile, gbbqParquet); err != nil {
		return fmt.Errorf("failed to convert GBBQ to parquet: %w", err)
	}

	if err := db.ImportGBBQ(gbbqParquet); err != nil {
		return fmt.Errorf("failed to import GBBQ parquet into database: %w", err)
	}

	fmt.Println("📈 股本变迁数据导入成功")
	return nil
}

func UpdateFactors(db database.DataRepository) error {
	parquetPath := filepath.Join(TempDir, "factors.parquet")

	fmt.Println("📟 计算所有股票前收盘价")
	calc.ExportFactorsToParquet(db, parquetPath)
	if err := db.ImportAdjustFactors(parquetPath); err != nil {
		return fmt.Errorf("failed to import factor data: %w", err)
	}
	fmt.Println("🔢 复权因子导入成功")

	return nil
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
			if err := tdx.DatatoolCreate(TempDir, "day", endDate); err != nil {
				return nil, fmt.Errorf("failed to run DatatoolDayCreate: %w", err)
			}

		case "tic":
			endDate := validDates[len(validDates)-1]
			fmt.Printf("🐢 开始转档分笔数据\n")
			if err := tdx.DatatoolCreate(TempDir, "tick", endDate); err != nil {
				return nil, fmt.Errorf("failed to run DatatoolTickCreate: %w", err)
			}
			fmt.Printf("🐢 开始转换分钟数据\n")
			if err := tdx.DatatoolCreate(TempDir, "min", endDate); err != nil {
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
