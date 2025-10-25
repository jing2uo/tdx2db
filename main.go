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

	var cronCmd = &cobra.Command{
		Use:   "cron",
		Short: "Cron for update and calc factor.",
		RunE: func(c *cobra.Command, args []string) error {
			if dbPath == "" {
				return fmt.Errorf("--dbpath is required")
			}
			if err := cmd.Cron(dbPath); err != nil {
				return err
			}
			return nil
		},
	}

	initCmd.Flags().StringVar(&dbPath, "dbpath", "", dbPathInfo)
	initCmd.Flags().StringVar(&dayFileDir, "dayfiledir", "", ".day 文件目录路径 (必填)")

	cronCmd.Flags().StringVar(&dbPath, "dbpath", "", dbPathInfo)

	initCmd.MarkFlagRequired("dbpath")
	initCmd.MarkFlagRequired("dayfiledir")
	cronCmd.MarkFlagRequired("dbpath")

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
