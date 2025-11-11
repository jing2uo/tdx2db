package main

import (
	"fmt"
	"os"

	"github.com/jing2uo/tdx2db/cmd"
	"github.com/spf13/cobra"
)

const dbPathInfo = "DuckDB 文件路径 (必填)"
const dayFileInfo = ".day 文件目录路径 (必填)"
const minLineInfo = `导入分时数据（可选，默认不处理）：
1    导入1分钟数据
5    导入5分钟数据
1,5  导入1分钟和5分钟数据
`

func main() {
	var rootCmd = &cobra.Command{
		Use:           "tdx2db",
		Short:         "Load TDX Data to DuckDB",
		SilenceErrors: true,
	}

	var dbPath, dayFileDir, minline string

	var initCmd = &cobra.Command{
		Use:   "init",
		Short: "Fully initialize stocks and gbbq data",
		RunE: func(c *cobra.Command, args []string) error {
			if dbPath == "" || dayFileDir == "" {
				return fmt.Errorf("both --dbpath and --dayfiledir are required")
			}
			if err := cmd.Init(dbPath, dayFileDir); err != nil {
				return err
			}
			return nil
		},
	}

	var cronCmd = &cobra.Command{
		Use:   "cron",
		Short: "Cron for update data and calc factor",
		RunE: func(c *cobra.Command, args []string) error {
			if dbPath == "" {
				return fmt.Errorf("--dbpath is required")
			}

			if c.Flags().Changed("minline") {
				valid := map[string]bool{"1": true, "5": true, "1,5": true, "5,1": true}
				if !valid[minline] {
					return fmt.Errorf("--minline 允许 '1'、'5'、'1,5'、'5,1'（传入: %s）", minline)
				}
			}
			if err := cmd.Cron(dbPath, minline); err != nil {
				return err
			}
			return nil
		},
	}

	initCmd.Flags().StringVar(&dbPath, "dbpath", "", dbPathInfo)
	initCmd.Flags().StringVar(&dayFileDir, "dayfiledir", "", dayFileInfo)
	initCmd.MarkFlagRequired("dbpath")
	initCmd.MarkFlagRequired("dayfiledir")

	cronCmd.Flags().StringVar(&dbPath, "dbpath", "", dbPathInfo)
	cronCmd.MarkFlagRequired("dbpath")

	cronCmd.Flags().StringVar(&minline, "minline", "", minLineInfo)

	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(cronCmd)

	cobra.OnFinalize(func() {
		cleanup(cmd.DataDir)
	})

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "🛑 错误: %v\n", err)
		os.Exit(1)
	}
}

func cleanup(dataDir string) {
	os.RemoveAll(dataDir)
}
