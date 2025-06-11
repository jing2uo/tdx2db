package cmd

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/tdx"
	"github.com/jing2uo/tdx2db/utils"
)

func Update(dbPath string) error {
	if dbPath == "" {
		return fmt.Errorf("database path cannot be empty")
	}

	defer os.RemoveAll(DataDir)
	start := time.Now()

	dbConfig := model.DBConfig{Path: dbPath}
	db, err := database.Connect(dbConfig)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	latestDate, err := database.GetLatestDate(db)
	if err != nil {
		return fmt.Errorf("failed to get latest date from database: %w", err)
	}
	fmt.Printf("📅 数据库中日线数据的最新日期为 %s\n", latestDate.Format("2006-01-02"))

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
			fmt.Printf("ℹ️ %s 非交易日或数据尚未更新\n", dateStr)
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

		fmt.Printf("✅ 成功下载 %s 的数据\n", dateStr)

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

		_, err := tdx.ConvertDayfiles2Csv(filepath.Join(DataDir, "vipdoc"), ValidPrefixes, StockCSV)
		if err != nil {
			return fmt.Errorf("failed to convert day files to CSV: %w", err)
		}
		fmt.Printf("🔥 成功转换为 CSV\n")

		// Import stock CSV
		if err := database.ImportStockCsv(db, StockCSV); err != nil {
			return fmt.Errorf("failed to import stock CSV: %w", err)
		}
		fmt.Println("📊 股票数据导入成功")
	} else {
		fmt.Println("🌲 无需下载")

	}

	fmt.Println("🛠️  开始下载除权除息数据")
	// Update GBBQ data
	if err := UpdateGbbq(db, DataDir); err != nil {
		return fmt.Errorf("failed to update GBBQ data: %w", err)
	}
	fmt.Println("📈 除权除息数据更新成功")

	fmt.Printf("✅ 处理完成，耗时 %s\n", time.Since(start))
	return nil
}
