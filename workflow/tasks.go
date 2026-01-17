package workflow

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jing2uo/tdx2db/calc"
	"github.com/jing2uo/tdx2db/database"
	"github.com/jing2uo/tdx2db/model"
	"github.com/jing2uo/tdx2db/tdx"
	"github.com/jing2uo/tdx2db/utils"
)

var (
	TaskUpdateDaily  *Task
	TaskInitDaily    *Task
	TaskUpdateGBBQ   *Task
	TaskCalcBasic    *Task
	TaskCalcFactor   *Task
	TaskUpdate1Min   *Task
	TaskUpdate5Min   *Task
	TaskUpdateBlocks *Task
)

func init() {
	TaskUpdateDaily = &Task{
		Name:      "update_daily",
		DependsOn: []string{},
		Executor:  executeUpdateDaily,
	}

	TaskInitDaily = &Task{
		Name:      "init_daily",
		DependsOn: []string{},
		Executor:  executeInitDaily,
	}

	TaskUpdateGBBQ = &Task{
		Name:      "update_gbbq",
		DependsOn: []string{},
		Executor:  executeUpdateGBBQ,
	}

	TaskCalcBasic = &Task{
		Name:      "calc_basic",
		DependsOn: []string{"update_daily", "update_gbbq"},
		Executor:  executeCalcBasic,
	}

	TaskCalcFactor = &Task{
		Name:      "calc_factor",
		DependsOn: []string{"calc_basic"},
		Executor:  executeCalcFactor,
	}

	TaskUpdate1Min = &Task{
		Name:      "update_1min",
		DependsOn: []string{},
		SkipIf: func(ctx context.Context, db database.DataRepository, args *TaskArgs) bool {
			need1Min, _, _ := ParseMinline(args.Minline)
			return !need1Min
		},
		Executor: executeUpdate1Min,
		OnError:  ErrorModeSkip,
	}

	TaskUpdate5Min = &Task{
		Name:      "update_5min",
		DependsOn: []string{},
		SkipIf: func(ctx context.Context, db database.DataRepository, args *TaskArgs) bool {
			_, need5Min, _ := ParseMinline(args.Minline)
			return !need5Min
		},
		Executor: executeUpdate5Min,
		OnError:  ErrorModeSkip,
	}

	TaskUpdateBlocks = &Task{
		Name:      "update_blocks",
		DependsOn: []string{},
		SkipIf: func(ctx context.Context, db database.DataRepository, args *TaskArgs) bool {
			return args.TdxHome == ""
		},
		Executor: executeUpdateBlocks,
		OnError:  ErrorModeSkip,
	}
}

func executeUpdateDaily(ctx context.Context, db database.DataRepository, args *TaskArgs) (*TaskResult, error) {

	latestDate, err := db.GetLatestDate(model.TableStocksDaily.TableName, "date")
	if err != nil {
		return nil, fmt.Errorf("failed to get latest date from database: %w", err)
	}
	fmt.Printf("📅 日线数据最新日期为 %s\n", latestDate.Format("2006-01-02"))

	validDates, err := prepareTdxData(ctx, latestDate, "day", args)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare tdx data: %w", err)
	}

	if len(validDates) == 0 {
		fmt.Println("🌲 日线数据无需更新")
		return &TaskResult{State: StateSkipped, Message: "no new daily data"}, nil
	}

	return executeDailyImport(ctx, db, args, args.VipdocDir)
}

func executeInitDaily(ctx context.Context, db database.DataRepository, args *TaskArgs) (*TaskResult, error) {
	fmt.Printf("📦 开始处理日线目录: %s\n", args.DayFileDir)
	if err := utils.CheckDirectory(args.DayFileDir); err != nil {
		return nil, err
	}

	return executeDailyImport(ctx, db, args, args.DayFileDir)
}

func executeDailyImport(ctx context.Context, db database.DataRepository, args *TaskArgs, sourceDir string) (*TaskResult, error) {
	fmt.Println("🐢 开始转换日线数据")

	stockDailyCSV := filepath.Join(args.TempDir, "stock.csv")

	_, err := tdx.ConvertFilesToCSV(ctx, sourceDir, args.ValidPrefixes, stockDailyCSV, ".day")
	if err != nil {
		return nil, fmt.Errorf("failed to convert day files to csv: %w", err)
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	if err := db.ImportDailyStocks(stockDailyCSV); err != nil {
		return nil, fmt.Errorf("failed to import stock csv: %w", err)
	}

	fmt.Println("🚀 股票数据导入成功")
	return &TaskResult{State: StateCompleted, Message: "daily data imported"}, nil
}

func executeUpdateGBBQ(ctx context.Context, db database.DataRepository, args *TaskArgs) (*TaskResult, error) {
	fmt.Println("🐢 开始下载股本变迁数据")

	gbbqFile, err := getGbbqFile(args.TempDir)
	if err != nil {
		return nil, fmt.Errorf("failed to download GBBQ file: %w", err)
	}

	gbbqData, err := tdx.DecodeGbbqFile(gbbqFile)
	if err != nil {
		return nil, fmt.Errorf("failed to decode GBBQ: %w", err)
	}

	gbbqCSV := filepath.Join(args.TempDir, "gbbq.csv")
	gbbqCw, err := utils.NewCSVWriter[model.GbbqData](gbbqCSV)
	if err != nil {
		return nil, fmt.Errorf("failed to create GBBQ CSV writer: %w", err)
	}
	if err := gbbqCw.Write(gbbqData); err != nil {
		return nil, err
	}
	gbbqCw.Close()

	if err := db.ImportGBBQ(gbbqCSV); err != nil {
		return nil, fmt.Errorf("failed to import GBBQ csv into database: %w", err)
	}

	fmt.Println("📈 股本变迁数据导入成功")
	return &TaskResult{State: StateCompleted, Rows: len(gbbqData), Message: "gbbq data imported"}, nil
}

func executeCalcBasic(ctx context.Context, db database.DataRepository, args *TaskArgs) (*TaskResult, error) {
	fmt.Println("📟 计算股票基础行情")
	basicCSV := filepath.Join(args.TempDir, "basics.csv")

	rowCount, err := calc.ExportStockBasicToCSV(ctx, db, basicCSV)
	if err != nil {
		return nil, fmt.Errorf("failed to export basic to csv: %w", err)
	}

	if rowCount == 0 {
		fmt.Println("🌲 股票基础行情无需更新")
		return &TaskResult{State: StateSkipped, Message: "no new basic data"}, nil
	}

	if err := db.ImportBasic(basicCSV); err != nil {
		return nil, fmt.Errorf("failed to import basic data: %w", err)
	}
	fmt.Println("🔢 基础行情导入成功")
	return &TaskResult{State: StateCompleted, Rows: rowCount, Message: "basic data calculated"}, nil
}

func executeCalcFactor(ctx context.Context, db database.DataRepository, args *TaskArgs) (*TaskResult, error) {
	fmt.Println("📟 计算股票复权因子")
	factorCSV := filepath.Join(args.TempDir, "factor.csv")

	factorCount, err := calc.ExportFactorsToCSV(ctx, db, factorCSV)
	if err != nil {
		return nil, fmt.Errorf("failed to export factor to csv: %w", err)
	}

	if factorCount == 0 {
		fmt.Println("🌲 复权因子无需更新")
		return &TaskResult{State: StateSkipped, Message: "no new factor data"}, nil
	}

	if err := db.ImportAdjustFactors(factorCSV); err != nil {
		return nil, fmt.Errorf("failed to append factor data: %w", err)
	}
	fmt.Printf("🔢 复权因子导入成功\n")
	return &TaskResult{State: StateCompleted, Rows: factorCount, Message: "factors calculated"}, nil
}

func executeUpdate1Min(ctx context.Context, db database.DataRepository, args *TaskArgs) (*TaskResult, error) {
	latestDate, err := getMinLineLatestDate(db, "1", args)
	if err != nil {
		return nil, err
	}

	validDates, err := prepareTdxData(ctx, latestDate, "tic", args)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare tdx data: %w", err)
	}

	if len(validDates) >= 30 {
		return nil, fmt.Errorf("分时数据超过30天未更新，请手动补齐后继续")
	}

	if len(validDates) > 0 {
		fmt.Printf("🐢 开始转换1分钟分时数据\n")

		stock1MinCSV := filepath.Join(args.TempDir, "1min.csv")

		_, err := tdx.ConvertFilesToCSV(ctx, args.VipdocDir, args.ValidPrefixes, stock1MinCSV, ".01")
		if err != nil {
			return nil, fmt.Errorf("failed to convert .01 files to csv: %w", err)
		}

		if err := db.Import1MinStocks(stock1MinCSV); err != nil {
			return nil, fmt.Errorf("failed to import 1-minute line csv: %w", err)
		}
		fmt.Println("📊 1分钟数据导入成功")
		return &TaskResult{State: StateCompleted, Message: "1min data imported"}, nil
	}

	fmt.Println("🌲 1分钟分时数据无需更新")
	return &TaskResult{State: StateSkipped, Message: "no new 1min data"}, nil
}

func executeUpdate5Min(ctx context.Context, db database.DataRepository, args *TaskArgs) (*TaskResult, error) {
	latestDate, err := getMinLineLatestDate(db, "5", args)
	if err != nil {
		return nil, err
	}

	validDates, err := prepareTdxData(ctx, latestDate, "tic", args)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare tdx data: %w", err)
	}

	if len(validDates) >= 30 {
		return nil, fmt.Errorf("分时数据超过30天未更新，请手动补齐后继续")
	}

	if len(validDates) > 0 {
		fmt.Printf("🐢 开始转换5分钟分时数据\n")

		stock5MinCSV := filepath.Join(args.TempDir, "5min.csv")

		_, err := tdx.ConvertFilesToCSV(ctx, args.VipdocDir, args.ValidPrefixes, stock5MinCSV, ".5")
		if err != nil {
			return nil, fmt.Errorf("failed to convert .5 files to csv: %w", err)
		}

		if err := db.Import5MinStocks(stock5MinCSV); err != nil {
			return nil, fmt.Errorf("failed to import 5-minute line csv: %w", err)
		}
		fmt.Println("📊 5分钟数据导入成功")
		return &TaskResult{State: StateCompleted, Message: "5min data imported"}, nil
	}

	fmt.Println("🌲 5分钟分时数据无需更新")
	return &TaskResult{State: StateSkipped, Message: "no new 5min data"}, nil
}

func executeUpdateBlocks(ctx context.Context, db database.DataRepository, args *TaskArgs) (*TaskResult, error) {
	fmt.Printf("🐢 导入通达信概念行业等信息\n")
	result, err := tdx.ExportTdxBlocksDataToCSV(args.TdxHome, args.TempDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "🚨 警告: %v\n", err)
		return &TaskResult{State: StateFailed, Error: err, Message: "blocks import warning"}, nil
	}

	if result.HolidaysFile != "" {
		db.ImportHolidays(result.HolidaysFile)
	}
	if result.BlockInfoFile != "" {
		db.ImportBlocksInfo(result.BlockInfoFile)
	}
	if result.BlockMembersConceptFile != "" {
		db.TruncateTable(model.TableBlockMember)
		db.ImportBlocksMember(result.BlockMembersConceptFile)
	}
	if result.BlockMembersIndustryFile != "" {
		db.ImportBlocksMember(result.BlockMembersIndustryFile)
	}

	return &TaskResult{State: StateCompleted, Message: "blocks data imported"}, nil
}

func getMinLineLatestDate(db database.DataRepository, minline string, args *TaskArgs) (time.Time, error) {
	var tableName string
	if minline == "1" {
		tableName = model.TableStocks1Min.TableName
	} else {
		tableName = model.TableStocks5Min.TableName
	}

	latestDate, err := db.GetLatestDate(tableName, "datetime")
	if err != nil {
		return time.Time{}, err
	}

	if latestDate.IsZero() {
		fmt.Printf("🛑 警告：数据库中没有 %s分钟 数据\n", minline)
		fmt.Println("🚧 将处理今天的数据，历史请自行导入")
		return args.Today.AddDate(0, 0, -1), nil
	}

	fmt.Printf("📅 %s分钟数据最新日期为 %s\n", minline, latestDate.Format("2006-01-02"))
	return latestDate, nil
}

func prepareTdxData(ctx context.Context, latestDate time.Time, dataType string, args *TaskArgs) ([]time.Time, error) {
	var dates []time.Time

	for d := latestDate.Add(24 * time.Hour); !d.After(args.Today); d = d.Add(24 * time.Hour) {
		dates = append(dates, d)
	}

	if len(dates) == 0 {
		return nil, nil
	}

	var targetPath, urlTemplate, fileSuffix, dataTypeCN string

	switch dataType {
	case "day":
		targetPath = filepath.Join(args.VipdocDir, "refmhq")
		urlTemplate = "https://www.tdx.com.cn/products/data/data/g4day/%s.zip"
		fileSuffix = "day"
		dataTypeCN = "日线"
	case "tic":
		targetPath = filepath.Join(args.VipdocDir, "newdatetick")
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
		select {
		case <-ctx.Done():
			return validDates, ctx.Err()
		default:
		}

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
				return nil, fmt.Errorf("download failed: %w", err)
			}
		}
	}

	if len(validDates) > 0 {
		select {
		case <-ctx.Done():
			return validDates, ctx.Err()
		default:
		}

		endDate := validDates[len(validDates)-1]
		switch dataType {
		case "day":
			if err := tdx.DatatoolCreate(args.TempDir, "day", endDate); err != nil {
				return nil, fmt.Errorf("failed to run DatatoolDayCreate: %w", err)
			}

		case "tic":
			fmt.Printf("🐢 开始转档分笔数据\n")
			if err := tdx.DatatoolCreate(args.TempDir, "tick", endDate); err != nil {
				return nil, fmt.Errorf("failed to run DatatoolTickCreate: %w", err)
			}
			fmt.Printf("🐢 开始转换分钟数据\n")
			if err := tdx.DatatoolCreate(args.TempDir, "min", endDate); err != nil {
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

func GetUpdateTaskNames() []string {
	return []string{
		"update_daily",
		"update_gbbq",
		"calc_basic",
		"calc_factor",
		"update_1min",
		"update_5min",
		"update_blocks",
	}
}

func GetRegisteredTasks() map[string]*Task {
	return map[string]*Task{
		"update_daily":  TaskUpdateDaily,
		"init_daily":    TaskInitDaily,
		"update_gbbq":   TaskUpdateGBBQ,
		"calc_basic":    TaskCalcBasic,
		"calc_factor":   TaskCalcFactor,
		"update_1min":   TaskUpdate1Min,
		"update_5min":   TaskUpdate5Min,
		"update_blocks": TaskUpdateBlocks,
	}
}

func GetInitTaskNames() []string {
	return []string{
		"init_daily",
	}
}
