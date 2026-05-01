package tdx

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/utils"
)

// ExportTdxHolidaysToCSV 解压 zhb.zip 并从 needini.dat 中导出交易日历。
// zhbZipPath 指向通达信 gbbq.zip 中嵌套的 zhb.zip；
// outputDir 用作 CSV 输出目录，同时在其下创建 zhb/ 存放解压结果。
func ExportTdxHolidaysToCSV(zhbZipPath, outputDir string) (string, error) {
	if err := utils.CheckFile(zhbZipPath); err != nil {
		return "", fmt.Errorf("zhb.zip 检查失败: %w", err)
	}
	if err := utils.CheckOutputDir(outputDir); err != nil {
		return "", fmt.Errorf("输出目录检查失败: %w", err)
	}

	unzipDir := filepath.Join(outputDir, "zhb")
	if err := utils.UnzipFile(zhbZipPath, unzipDir); err != nil {
		return "", fmt.Errorf("解压 zhb.zip 失败: %w", err)
	}

	needini := filepath.Join(unzipDir, "needini.dat")
	if err := utils.CheckFile(needini); err != nil {
		return "", fmt.Errorf("needini.dat 检查失败: %w", err)
	}

	holidays, err := ReadHolidays(needini)
	if err != nil {
		return "", err
	}

	outputFile := filepath.Join(outputDir, "holidays.csv")
	cw, err := utils.NewCSVWriter[model.Holiday](outputFile)
	if err != nil {
		return "", err
	}
	defer cw.Close()
	if err := cw.Write(holidays); err != nil {
		return "", err
	}

	return outputFile, nil
}

// ReadHolidays 读取 needini.dat 并解析为 Holiday 列表。
func ReadHolidays(filePath string) ([]model.Holiday, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var allHolidays []string
	lines := strings.Split(string(content), "\n")

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || !strings.HasPrefix(line, "Y") || !strings.Contains(line, "=") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		items := strings.Split(parts[1], ",")

		var cleanItems []string
		for _, item := range items {
			item = strings.TrimSpace(item)
			if item != "" {
				cleanItems = append(cleanItems, item)
			}
		}

		if len(cleanItems) == 0 {
			continue
		}

		year := cleanItems[0]
		for _, dateMMDD := range cleanItems[1:] {
			if len(dateMMDD) == 4 {
				fullDate := fmt.Sprintf("%s-%s-%s", year, dateMMDD[:2], dateMMDD[2:])
				allHolidays = append(allHolidays, fullDate)
			}
		}
	}

	sort.Strings(allHolidays)

	holidays := make([]model.Holiday, 0, len(allHolidays))
	for _, dateStr := range allHolidays {
		date, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			fmt.Printf("⚠️  解析日期失败 %s: %v\n", dateStr, err)
			continue
		}
		holidays = append(holidays, model.Holiday{Date: date})
	}

	return holidays, nil
}
