package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/jing2uo/tdx2db/cmd"
	"github.com/spf13/cobra"
)

const dbURIInfo = "数据库连接信息"
const dbURIHelp = `

Database URI:
  ClickHouse: clickhouse://[user[:password]@][host][:port][/database][?http_port=p&]
  DuckDB:     duckdb://[path]`

const dayFileInfo = "通达信日线文件目录"
const minLineInfo = `导入分时数据（可选）
  1    导入1分钟分时数据
  5    导入5分钟分时数据
  1,5  导入两种`

const convertHelp = `

Type & Input:
  -t day   转换日线文件     -i 包含 .day 的目录
  -t 1min  转换 1 分钟分时  -i 包含 .1 的目录
  -t 5min  转换 5 分钟分时  -i 包含 .05 的目录
  -t tic4  转换四代分笔     -i 四代 TIC 压缩文件
  -t day4  转换四代日线     -i 四代行情压缩文件`

func main() {
	// 创建可取消的 context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		fmt.Printf("\n🚨 收到信号 %v，正在退出...\n", sig)
		cancel()
	}()

	var rootCmd = &cobra.Command{
		Use:           "tdx2db",
		Short:         "Load TDX Data to DuckDB",
		SilenceErrors: true,
	}

	var (
		dbURI      string
		dayFileDir string
		minline    string

		// Convert
		inputType  string
		inputPath  string
		outputPath string
	)

	var initCmd = &cobra.Command{
		Use:   "init",
		Short: "Fully import stocks data from TDX",
		Example: `  tdx2db init --dburi 'clickhouse://localhost' --dayfiledir /path/to/vipdoc/
  tdx2db init --dburi 'duckdb://./tdx.db' --dayfiledir /path/to/vipdoc/` + dbURIHelp,
		RunE: func(c *cobra.Command, args []string) error {
			return cmd.Init(ctx, dbURI, dayFileDir)
		},
	}

	var cronCmd = &cobra.Command{
		Use:   "cron",
		Short: "Cron for update data and calc factor",
		Example: `  tdx2db cron --dburi 'clickhouse://localhost' --minline 1,5
  tdx2db cron --dburi 'duckdb://./tdx.db'` + dbURIHelp,
		RunE: func(c *cobra.Command, args []string) error {
			if c.Flags().Changed("minline") {
				valid := map[string]bool{"1": true, "5": true, "1,5": true, "5,1": true}
				if !valid[minline] {
					return fmt.Errorf("--minline 仅支持 '1'、'5'、'1,5', 传入: %s", minline)
				}
			}
			return cmd.Cron(ctx, dbURI, minline)
		},
	}

	var convertCmd = &cobra.Command{
		Use:   "convert",
		Short: "Convert TDX data to CSV",
		Example: `  tdx2db convert -t day -i /path/to/vipdoc/ -o ./
  tdx2db convert -t day4 -i /path/to/20251212.zip -o ./` + convertHelp,
		RunE: func(c *cobra.Command, args []string) error {
			opts := cmd.ConvertOptions{
				InputPath:  inputPath,
				OutputPath: outputPath,
			}

			switch strings.ToLower(inputType) {
			case "day":
				opts.InputType = cmd.DayFileDir
			case "1min":
				opts.InputType = cmd.Min1FileDir
			case "5min":
				opts.InputType = cmd.Min5FileDir
			case "tic4":
				opts.InputType = cmd.TicZip
			case "day4":
				opts.InputType = cmd.DayZip
			default:
				return fmt.Errorf("未知的类型: %s%s", inputType, convertHelp)
			}

			return cmd.Convert(ctx, opts)
		},
	}

	// Init Flags
	initCmd.Flags().StringVar(&dbURI, "dburi", "", dbURIInfo)
	initCmd.Flags().StringVar(&dayFileDir, "dayfiledir", "", dayFileInfo)
	initCmd.MarkFlagRequired("dburi")
	initCmd.MarkFlagRequired("dayfiledir")

	// Cron Flags
	cronCmd.Flags().StringVar(&dbURI, "dburi", "", dbURIInfo)
	cronCmd.MarkFlagRequired("dburi")
	cronCmd.Flags().StringVar(&minline, "minline", "", minLineInfo)

	// Convert Flags
	convertCmd.Flags().StringVarP(&inputType, "type", "t", "", "转换类型")
	convertCmd.Flags().StringVarP(&inputPath, "input", "i", "", "输入文件或目录路径")
	convertCmd.Flags().StringVarP(&outputPath, "output", "o", "", "CSV 文件输出目录")
	convertCmd.MarkFlagRequired("type")
	convertCmd.MarkFlagRequired("input")
	convertCmd.MarkFlagRequired("output")

	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(cronCmd)
	rootCmd.AddCommand(convertCmd)

	cobra.OnFinalize(func() {
		os.RemoveAll(cmd.TempDir)
	})

	if err := rootCmd.Execute(); err != nil {
		if err == context.Canceled {
			fmt.Fprintln(os.Stderr, "✅ 任务安全中断")
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "🛑 错误: %v\n", err)
		os.Exit(1)
	}
}
