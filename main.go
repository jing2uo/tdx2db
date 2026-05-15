package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"

	"github.com/jing2uo/tdx2db/cmd"
	"github.com/spf13/cobra"
)

// 由 ldflags 注入（见 .github/workflows/release.yaml）；
// 未注入时（裸 go build）从 runtime/debug 读 VCS 信息。
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func buildVersionString() string {
	v, c, d := version, commit, date
	var dirty bool
	if v == "dev" {
		if info, ok := debug.ReadBuildInfo(); ok {
			for _, s := range info.Settings {
				switch s.Key {
				case "vcs.revision":
					c = s.Value
				case "vcs.time":
					d = s.Value
				case "vcs.modified":
					dirty = s.Value == "true"
				}
			}
		}
	}
	if len(c) > 7 {
		c = c[:7]
	}
	if dirty {
		c += " (dirty)"
	}
	return fmt.Sprintf("tdx2db %s\ncommit: %s\nbuilt:  %s", v, c, d)
}

const dbURIInfo = "数据库连接信息"
const dbURIHelp = `

Database URI:
  ClickHouse: clickhouse://[user[:password]@][host][:port][/database][?http_port=p&]
  DuckDB:     duckdb://[path]`

const dayFileInfo = "通达信日线文件目录"
const minInfo = "导入 1 分钟分时数据（可选）"

const convertHelp = `

Type & Input:
  -t day   转换日线文件          -i 包含 .day 的目录
  -t 1min  转换 1 分钟分时       -i 包含 .1 的目录
  -t tic4  四代分笔转 1 分钟分时 -i 四代 TIC 压缩文件
  -t day4  转换四代日线          -i 四代行情压缩文件`

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

	versionStr := buildVersionString()
	var tempDirOverride string
	var rootCmd = &cobra.Command{
		Use:           "tdx2db",
		Short:         "Load TDX Data to DuckDB",
		SilenceErrors: true,
		Version:       versionStr,
		PersistentPreRunE: func(c *cobra.Command, args []string) error {
			return cmd.OverrideTempDir(tempDirOverride)
		},
	}
	// 预注册 -v 短选项；cobra 默认只挂 --version
	rootCmd.Flags().BoolP("version", "v", false, "version for tdx2db")
	rootCmd.SetVersionTemplate("{{.Version}}\n")
	// --temp: 把临时目录的父目录从默认 $TMPDIR 切到指定路径,
	// 适用 $TMPDIR (常见 /tmp tmpfs) 容量被占满时的兜底。
	rootCmd.PersistentFlags().StringVar(&tempDirOverride, "temp", "",
		"临时文件父目录, 留空走 $TMPDIR")

	var versionCmd = &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Run: func(c *cobra.Command, args []string) {
			fmt.Println(versionStr)
		},
	}

	var (
		dbURI      string
		dayFileDir string
		minEnable  bool

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
		Example: `  tdx2db cron --dburi 'clickhouse://localhost' --min
  tdx2db cron --dburi 'duckdb://./tdx.db'` + dbURIHelp,
		RunE: func(c *cobra.Command, args []string) error {
			return cmd.Cron(ctx, dbURI, minEnable)
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
	cronCmd.Flags().BoolVar(&minEnable, "min", false, minInfo)

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
	rootCmd.AddCommand(versionCmd)

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
