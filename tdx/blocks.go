package tdx

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/utils"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/transform"
)

// ExportResult 导出结果，包含所有生成的CSV文件路径
type ExportResult struct {
	StockInfoFile            string
	HolidaysFile             string
	BlockMembersConceptFile  string
	BlockMembersIndustryFile string
	BlockInfoFile            string
}

func ExportTdxBlocksDataToCSV(tdxHome, outputDir string) (*ExportResult, error) {
	// 展开路径（如果有 ~ 等符号）
	tdxHome = expandPath(tdxHome)
	outputDir = expandPath(outputDir)

	// 1. 检查并创建输出目录（这是必须的，如果失败则返回错误）
	if err := utils.CheckOutputDir(outputDir); err != nil {
		return nil, fmt.Errorf("输出目录检查失败: %w", err)
	}

	// 2. 检查 hq_cache 目录（这是必须的，如果失败则返回错误）
	hqCache := filepath.Join(tdxHome, "T0002/hq_cache")
	if err := utils.CheckDirectory(hqCache); err != nil {
		return nil, fmt.Errorf("通达信安装目录检查失败: %w", err)
	}

	// 3. 定义所有输入文件
	inputFiles := map[string]string{
		"股票信息文件": filepath.Join(hqCache, "infoharbor_ex.code"),
		"假期日历文件": filepath.Join(hqCache, "needini.dat"),
		"概念板块文件": filepath.Join(hqCache, "infoharbor_block.dat"),
		"行业数据文件": filepath.Join(hqCache, "tdxhy.cfg"),
		"板块信息文件": filepath.Join(hqCache, "tdxzs3.cfg"),
	}

	// 4. 检查所有输入文件，记录哪些文件可用
	fileAvailable := make(map[string]bool)
	for name, path := range inputFiles {
		if err := utils.CheckFile(path); err != nil {
			fmt.Printf("🚨 警告: %s检查失败: %v\n", name, err)
			fileAvailable[name] = false
		} else {
			fileAvailable[name] = true
		}
	}

	// 5. 构建输出结果结构（初始化所有路径）
	result := &ExportResult{
		StockInfoFile:            filepath.Join(outputDir, "stocks_info.csv"),
		HolidaysFile:             filepath.Join(outputDir, "holidays.csv"),
		BlockMembersConceptFile:  filepath.Join(outputDir, "blocks_concept_member.csv"),
		BlockMembersIndustryFile: filepath.Join(outputDir, "blocks_industry_member.csv"),
		BlockInfoFile:            filepath.Join(outputDir, "blocks_info.csv"),
	}

	// 6. 导出股票信息
	if fileAvailable["股票信息文件"] {
		stockInfo, err := ReadInfoharborCode(inputFiles["股票信息文件"])
		if err != nil {
			result.StockInfoFile = ""
		} else if err := writeCSV(result.StockInfoFile, stockInfo); err != nil {
			result.StockInfoFile = ""
		}
	} else {
		result.StockInfoFile = ""
	}

	// 7. 导出交易日历
	if fileAvailable["假期日历文件"] {
		holidays, err := ReadHolidays(inputFiles["假期日历文件"])
		if err != nil {
			result.HolidaysFile = ""
		} else if err := writeCSV(result.HolidaysFile, holidays); err != nil {
			result.HolidaysFile = ""
		}
	} else {
		result.HolidaysFile = ""
	}

	// 8. 导出概念板块成员
	if fileAvailable["概念板块文件"] {
		gnbkData, err := ReadInfoharborBlock(inputFiles["概念板块文件"])
		if err != nil {
			result.BlockMembersConceptFile = ""
		} else if err := writeCSV(result.BlockMembersConceptFile, gnbkData); err != nil {
			result.BlockMembersConceptFile = ""
		}
	} else {
		result.BlockMembersConceptFile = ""
	}

	// 9. 导出行业成员
	if fileAvailable["行业数据文件"] {
		tdxhyData, err := ReadTDXHY(inputFiles["行业数据文件"])
		if err != nil {
			result.BlockMembersIndustryFile = ""
		} else if err := writeCSV(result.BlockMembersIndustryFile, tdxhyData); err != nil {
			result.BlockMembersIndustryFile = ""
		}
	} else {
		result.BlockMembersIndustryFile = ""
	}

	// 10. 导出板块信息
	if fileAvailable["板块信息文件"] {
		blockInfo, err := ReadTDXZS3(inputFiles["板块信息文件"])
		if err != nil {
			result.BlockInfoFile = ""
		} else if err := writeCSV(result.BlockInfoFile, blockInfo); err != nil {
			result.BlockInfoFile = ""
		}
	} else {
		result.BlockInfoFile = ""
	}

	return result, nil
}

// writeCSV 通用CSV写入函数
func writeCSV[T any](csvPath string, data []T) error {
	cw, err := utils.NewCSVWriter[T](csvPath)
	if err != nil {
		return err
	}
	defer cw.Close()
	return cw.Write(data)
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

// ReadInfoharborCode 读取股票代码文件
func ReadInfoharborCode(filePath string) ([]model.StockInfo, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := transform.NewReader(file, simplifiedchinese.GBK.NewDecoder())
	scanner := bufio.NewScanner(decoder)

	var stockInfos []model.StockInfo

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Split(line, "|")
		if len(parts) >= 2 {
			code := strings.TrimSpace(parts[0])
			name := strings.TrimSpace(parts[1])
			symbol, ok := utils.GenerateSymbol(code)
			if ok {
				stockInfos = append(stockInfos, model.StockInfo{
					Symbol: symbol,
					Name:   name,
				})
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return stockInfos, nil
}

// ReadInfoharborBlock 读取概念、风格、指数成分股
func ReadInfoharborBlock(filePath string) ([]model.BlockMember, error) {
	content, err := readGBKFile(filePath)
	if err != nil {
		return nil, err
	}

	blockTypeMap := map[string]string{
		"GN": "gn",
		"FG": "fg",
	}

	var parsedData []struct {
		BlockCode string
		Stocks    []string
	}

	re := regexp.MustCompile(`(?m)^#(GN_|FG_|ZS_)`)
	matches := re.FindAllStringIndex(content, -1)

	var sections []string
	if len(matches) > 0 {
		if matches[0][0] > 0 {
			sections = append(sections, content[:matches[0][0]])
		}

		for i := 0; i < len(matches); i++ {
			start := matches[i][0]
			var end int
			if i+1 < len(matches) {
				end = matches[i+1][0]
			} else {
				end = len(content)
			}
			sections = append(sections, content[start:end])
		}
	}

	for _, section := range sections {
		section = strings.TrimSpace(section)
		if section == "" {
			continue
		}

		if !strings.HasPrefix(section, "#GN_") &&
			!strings.HasPrefix(section, "#FG_") &&
			!strings.HasPrefix(section, "#ZS_") {
			continue
		}

		sectionPrefix := section[1:3]

		if _, ok := blockTypeMap[sectionPrefix]; !ok {
			continue
		}

		lines := strings.Split(section, "\n")
		if len(lines) == 0 {
			continue
		}

		headerLine := lines[0]
		headerParts := strings.Split(headerLine, ",")

		if len(headerParts) < 3 {
			continue
		}

		headerFirst := strings.TrimPrefix(headerParts[0], "#")
		blockCodeParts := strings.SplitN(headerFirst, "_", 2)
		if len(blockCodeParts) < 2 {
			continue
		}
		blockCode := blockCodeParts[1]

		stockLines := lines[1:]
		allCodesStr := strings.Join(stockLines, "")

		var formattedStocks []string
		for _, rawCode := range strings.Split(allCodesStr, ",") {
			code := strings.TrimSpace(rawCode)
			if code == "" || !strings.Contains(code, "#") {
				continue
			}

			parts := strings.Split(code, "#")
			if len(parts) != 2 {
				continue
			}

			prefix := parts[0]
			stockNum := parts[1]

			switch prefix {
			case "0":
				formattedStocks = append(formattedStocks, "sz"+stockNum)
			case "1":
				formattedStocks = append(formattedStocks, "sh"+stockNum)
			case "2":
				formattedStocks = append(formattedStocks, "bj"+stockNum)
			}
		}

		parsedData = append(parsedData, struct {
			BlockCode string
			Stocks    []string
		}{
			BlockCode: blockCode,
			Stocks:    formattedStocks,
		})
	}

	var records []model.BlockMember
	for _, item := range parsedData {
		for _, stockSymbol := range item.Stocks {
			records = append(records, model.BlockMember{
				StockSymbol: stockSymbol,
				BlockCode:   item.BlockCode,
			})
		}
	}

	return records, nil
}

// ReadTDXZS3 读取板块名称、代码、编码
func ReadTDXZS3(filePath string) ([]model.BlockInfo, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := transform.NewReader(file, simplifiedchinese.GBK.NewDecoder())
	reader := csv.NewReader(decoder)
	reader.Comma = '|'
	reader.FieldsPerRecord = -1

	var blockInfos []model.BlockInfo

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if len(record) < 6 {
			continue
		}

		blkName := record[0]
		blkCode := record[1]
		blkType := record[2]
		blkLabel := strings.TrimSpace(record[5])

		blockSymbol := "sh" + blkCode

		var parentCode string
		blockLevel := 1

		labelLen := len(blkLabel)

		// 规则 A: Type 12 (通达信研究行业)
		if blkType == "12" {
			switch labelLen {
			case 5:
				parent := blkLabel[:3]
				parentCode = parent
				blockLevel = 2
			case 7:
				parent := blkLabel[:5]
				parentCode = parent
				blockLevel = 3
			}
		}

		// 规则 B: Type 2 (通达信普通行业)
		if blkType == "2" {
			if labelLen == 7 {
				parent := blkLabel[:5]
				parentCode = parent
				blockLevel = 2
			}
		}

		// 类型映射
		typeMapping := map[string]string{
			"2":  "tdx_general",
			"3":  "region",
			"4":  "concept",
			"5":  "style",
			"12": "tdx_research",
		}

		blockType := blkType
		if mappedType, ok := typeMapping[blkType]; ok {
			blockType = mappedType
		}

		blockInfos = append(blockInfos, model.BlockInfo{
			BlockType:   blockType,
			BlockName:   blkName,
			BlockSymbol: blockSymbol,
			BlockCode:   blkLabel,
			ParentCode:  parentCode,
			BlockLevel:  blockLevel,
		})
	}

	return blockInfos, nil
}

// ReadTDXHY 读取通达信研究和普通行业成分股
func ReadTDXHY(filePath string) ([]model.BlockMember, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = '|'
	reader.FieldsPerRecord = -1

	var members []model.BlockMember

	prefixMap := map[string]string{
		"0": "sz",
		"1": "sh",
		"2": "bj",
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if len(record) < 6 {
			continue
		}

		exchange := record[0]
		code := record[1]
		tdxhyT := record[2]
		tdxhyX := record[5]

		// 过滤条件
		if exchange == "0" && strings.HasPrefix(code, "20") {
			continue
		}

		prefix, ok := prefixMap[exchange]
		if !ok {
			continue
		}

		stockSymbol := prefix + code

		// 添加 tdxhy_T
		if tdxhyT != "" {
			members = append(members, model.BlockMember{
				StockSymbol: stockSymbol,
				BlockCode:   tdxhyT,
			})
		}

		// 添加 tdxhy_X
		if tdxhyX != "" {
			members = append(members, model.BlockMember{
				StockSymbol: stockSymbol,
				BlockCode:   tdxhyX,
			})
		}
	}

	return members, nil
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

// readGBKFile 读取GBK编码的文件
func readGBKFile(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	decoder := transform.NewReader(file, simplifiedchinese.GBK.NewDecoder())
	content, err := io.ReadAll(decoder)
	if err != nil {
		return "", err
	}

	return string(content), nil
}
