package clickhouse

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jing2uo/tdx2db/model"
)

func (d *ClickHouseDriver) ImportCSV(meta *model.TableMeta, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	req, err := http.NewRequest("POST", d.httpImportUrl, file)
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "text/csv")

	// 设置参数
	q := req.URL.Query()

	if d.database != "" {
		q.Set("database", d.database)
	}

	q.Add("query", fmt.Sprintf("INSERT INTO %s FORMAT CSVWithNames", meta.TableName))
	q.Add("date_time_input_format", "best_effort")
	q.Add("session_timezone", "Asia/Shanghai")

	req.URL.RawQuery = q.Encode()

	if d.authUser != "" {
		req.SetBasicAuth(d.authUser, d.authPass)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		errMsg := strings.TrimSpace(string(bodyBytes))
		return fmt.Errorf("clickhouse insert failed (db: %s, status %d): %s", d.database, resp.StatusCode, errMsg)
	}

	return nil
}

func (d *ClickHouseDriver) TruncateTable(meta *model.TableMeta) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	query := fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s", meta.TableName)

	if _, err := d.db.ExecContext(ctx, query); err != nil {
		return fmt.Errorf("clickhouse truncate via tcp failed: %w", err)
	}

	return nil
}

func (d *ClickHouseDriver) ImportKlineDaily(path string) error {
	return d.ImportCSV(model.TableKlineDaily, path)
}

func (d *ClickHouseDriver) ImportKline1Min(path string) error {
	return d.ImportCSV(model.TableKline1Min, path)
}

func (d *ClickHouseDriver) ImportGBBQ(path string) error {
	d.TruncateTable(model.TableGbbq)
	return d.ImportCSV(model.TableGbbq, path)
}

func (d *ClickHouseDriver) ImportBasic(path string) error {
	return d.ImportCSV(model.TableBasicDaily, path)
}

func (d *ClickHouseDriver) ImportAdjustFactors(path string) error {
	return d.ImportCSV(model.TableAdjustFactor, path)
}

func (d *ClickHouseDriver) ImportHolidays(path string) error {
	d.TruncateTable(model.TableHoliday)
	return d.ImportCSV(model.TableHoliday, path)
}

func (d *ClickHouseDriver) ImportBlockInfo(path string) error {
	if err := d.TruncateTable(model.TableBlockInfo); err != nil {
		return err
	}
	return d.ImportCSV(model.TableBlockInfo, path)
}

func (d *ClickHouseDriver) ImportBlockMembers(path string) error {
	if err := d.TruncateTable(model.TableBlockMember); err != nil {
		return err
	}
	return d.ImportCSV(model.TableBlockMember, path)
}

func (d *ClickHouseDriver) ImportSymbolNames(path string) error {
	if err := d.TruncateTable(model.TableSymbolName); err != nil {
		return err
	}
	return d.ImportCSV(model.TableSymbolName, path)
}

// RebuildSymbolClass 全量重建 symbol_class 表（从 raw_kline_daily 取 distinct symbol 后归类）。
// 写入逻辑跟 Import 系列同性质，放这里。
func (d *ClickHouseDriver) RebuildSymbolClass() error {
	kline := model.TableKlineDaily.TableName
	class := model.TableSymbolClass.TableName

	var codes []string
	if err := d.db.Select(&codes, fmt.Sprintf("SELECT DISTINCT symbol FROM %s", kline)); err != nil {
		return fmt.Errorf("failed to collect symbols: %w", err)
	}

	if err := d.TruncateTable(model.TableSymbolClass); err != nil {
		return err
	}

	if len(codes) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("INSERT INTO %s (symbol, class) VALUES ", class))
	for i, c := range codes {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("('%s', '%s')", c, model.ClassifyCode(c)))
	}

	if _, err := d.db.Exec(sb.String()); err != nil {
		return fmt.Errorf("failed to insert symbol_class: %w", err)
	}
	return nil
}
