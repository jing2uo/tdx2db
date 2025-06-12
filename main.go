package main

import (
	"fmt"
	"os"

	"github.com/jing2uo/tdx2db/cmd"
	"github.com/spf13/cobra"
)

const dbPathInfo = "DuckDB 文件路径 (必填)"

func main() {
	var rootCmd = &cobra.Command{
		Use:           "tdx2db",
		Short:         "Load TDX Data to DuckDB.",
		SilenceErrors: true,
	}

	var dbPath, dayFileDir string
	var initCmd = &cobra.Command{
		Use:   "init",
		Short: "Fully initialize stocks and gbbq data.",
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

	var updateCmd = &cobra.Command{
		Use:   "update",
		Short: "Update data in DuckDB.",
		RunE: func(c *cobra.Command, args []string) error {
			if dbPath == "" {
				return fmt.Errorf("--dbpath is required")
			}
			if err := cmd.Update(dbPath); err != nil {
				return err
			}
			return nil
		},
	}

	var factorCmd = &cobra.Command{
		Use:   "factor",
		Short: "Calculate fq factor.",
		RunE: func(c *cobra.Command, args []string) error {
			if dbPath == "" {
				return fmt.Errorf("--dbpath is required")
			}
			if err := cmd.Factor(dbPath); err != nil {
				return err
			}
			return nil
		},
	}

	// 定义 init 命令的标志
	initCmd.Flags().StringVar(&dbPath, "dbpath", "", dbPathInfo)
	initCmd.Flags().StringVar(&dayFileDir, "dayfiledir", "", ".day 文件目录路径 (必填)")

	// 定义 update 命令的标志
	updateCmd.Flags().StringVar(&dbPath, "dbpath", "", dbPathInfo)

	factorCmd.Flags().StringVar(&dbPath, "dbpath", "", dbPathInfo)

	// 标记必须参数
	initCmd.MarkFlagRequired("dbpath")
	initCmd.MarkFlagRequired("dayfiledir")
	updateCmd.MarkFlagRequired("dbpath")
	factorCmd.MarkFlagRequired("dbpath")

	// 添加子命令
	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(updateCmd)
	rootCmd.AddCommand(factorCmd)

	// 执行 root 命令
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "🛑 错误: %v\n", err)
		os.Exit(1)
	}
}
