package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/jing2uo/tdx2db/cmd"
	"github.com/spf13/cobra"
)

const dbPathInfo = "DuckDB 文件路径"
const dayFileInfo = "通达信日线 .day 文件目录"
const minLineInfo = `导入分时数据（可选）
  1    导入1分钟数据
  5    导入5分钟数据
  1,5  导入两种
`

func main() {

	var rootCmd = &cobra.Command{
		Use:           "tdx2db",
		Short:         "Load TDX Data to DuckDB",
		SilenceErrors: true,
	}

	var dbPath, dayFileDir, minline string
	var (
		m1FileDir   string
		m5FileDir   string
		ticZipFile  string
		gbbqZipFile string
		dayZipFile  string
		outPutFile  string
	)

	var initCmd = &cobra.Command{
		Use:   "init",
		Short: "Fully import stocks data from TDX",
		RunE: func(c *cobra.Command, args []string) error {
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

	var convertCmd = &cobra.Command{
		Use:   "convert",
		Short: "Convert TDX data to parquet",
		PreRunE: func(c *cobra.Command, args []string) error {
			setFlags := 0
			if c.Flags().Changed("dayfiledir") {
				setFlags++
			}
			if c.Flags().Changed("ticzip") {
				setFlags++
			}
			if c.Flags().Changed("dayzip") {
				setFlags++
			}
			if c.Flags().Changed("gbbqzip") {
				setFlags++
			}
			if c.Flags().Changed("m1filedir") {
				setFlags++
			}
			if c.Flags().Changed("m5filedir") {
				setFlags++
			}

			if setFlags == 0 {
				return errors.New("必需 --dayfiledir, --m1filefir, --m5filedir 或 --ticzip,  --dayzip, --gbbqzip")
			}
			if setFlags > 1 {
				return errors.New("--dayfiledir, --m1filedir, --m5filedir, --ticzip, --dayzip, --gbbqzip 不能一起使用")
			}
			return nil
		},
		RunE: func(c *cobra.Command, args []string) error {
			opts := cmd.ConvertOptions{
				OutputPath: outPutFile,
			}

			if c.Flags().Changed("dayfiledir") {
				opts.InputPath = dayFileDir
				opts.InputType = cmd.DayFileDir
			} else if c.Flags().Changed("m1filedir") {
				opts.InputPath = m1FileDir
				opts.InputType = cmd.Min1FileDir
			} else if c.Flags().Changed("m5filedir") {
				opts.InputPath = m5FileDir
				opts.InputType = cmd.Min5FileDir
			} else if c.Flags().Changed("ticzip") {
				opts.InputPath = ticZipFile
				opts.InputType = cmd.TicZip
			} else if c.Flags().Changed("gbbqzip") {
				opts.InputPath = gbbqZipFile
				opts.InputType = cmd.GbbqZip
			} else if c.Flags().Changed("dayzip") {
				opts.InputPath = dayZipFile
				opts.InputType = cmd.DayZip
			}

			if err := cmd.Convert(opts); err != nil {
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

	convertCmd.Flags().StringVar(&dayFileDir, "dayfiledir", "", dayFileInfo)
	convertCmd.Flags().StringVar(&m1FileDir, "m1filedir", "", "通达信 1 分钟 .01 文件目录")
	convertCmd.Flags().StringVar(&m5FileDir, "m5filedir", "", "通达信 5 分钟 .5 文件目录")
	convertCmd.Flags().StringVar(&ticZipFile, "ticzip", "", "通达信四代 TIC 压缩文件")
	convertCmd.Flags().StringVar(&dayZipFile, "dayzip", "", "通达信四代行情压缩文件")
	convertCmd.Flags().StringVar(&gbbqZipFile, "gbbqzip", "", "通达信股本变迁压缩文件")
	convertCmd.Flags().StringVar(&outPutFile, "output", "", "parquet 文件输出目录")
	convertCmd.MarkFlagRequired("output")

	rootCmd.AddCommand(initCmd)
	rootCmd.AddCommand(cronCmd)
	rootCmd.AddCommand(convertCmd)

	cobra.OnFinalize(func() {
		os.RemoveAll(cmd.TempDir)
	})

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "🛑 错误: %v\n", err)
		os.Exit(1)
	}
}
