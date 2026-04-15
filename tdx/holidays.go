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

// ExportTdxHolidaysToCSV 从通达信安装目录导出交易日历到 CSV。
func ExportTdxHolidaysToCSV(tdxHome, outputDir string) (string, error) {
	tdxHome = expandPath(tdxHome)
	outputDir = expandPath(outputDir)

	if err := utils.CheckOutputDir(outputDir); err != nil {
		return "", fmt.Errorf("输出目录检查失败: %w", err)
	}

	hqCache := filepath.Join(tdxHome, "T0002/hq_cache")
	if err := utils.CheckDirectory(hqCache); err != nil {
		return "", fmt.Errorf("通达信安装目录检查失败: %w", err)
	}

	inputFile := filepath.Join(hqCache, "needini.dat")
	if err := utils.CheckFile(inputFile); err != nil {
		return "", fmt.Errorf("假期日历文件检查失败: %w", err)
	}

	holidays, err := ReadHolidays(inputFile)
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

// ReadHolidays 读取假日数据文件
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

		datePart := parts[1]
		items := strings.Split(datePart, ",")

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
		dates := cleanItems[1:]

		for _, dateMMDD := range dates {
			if len(dateMMDD) == 4 {
				month := dateMMDD[:2]
				day := dateMMDD[2:]
				fullDate := fmt.Sprintf("%s-%s-%s", year, month, day)
				allHolidays = append(allHolidays, fullDate)
			}
		}
	}

	sort.Strings(allHolidays)

	holidays := make([]model.Holiday, 0, len(allHolidays))
	for _, dateStr := range allHolidays {
		date, err := time.Parse("2006-01-02", dateStr)
		if err != nil {
			fmt.Printf("警告: 解析日期失败 %s: %v\n", dateStr, err)
			continue
		}
		holidays = append(holidays, model.Holiday{Date: date})
	}

	return holidays, nil
}

// expandPath 展开路径中的 ~ 符号
func expandPath(path string) string {
	if strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return path
		}
		return filepath.Join(home, path[2:])
	}
	return path
}
