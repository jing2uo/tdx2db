package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/jing2uo/tdx2db/model"
)

func (d *ClickHouseDriver) importParquet(meta *model.TableMeta, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	req, err := http.NewRequest("POST", d.httpImportUrl, file)
	if err != nil {
		return err
	}

	// 设置参数
	q := req.URL.Query()

	if d.database != "" {
		q.Set("database", d.database)
	}

	q.Add("query", fmt.Sprintf("INSERT INTO %s FORMAT Parquet", meta.TableName))
	q.Add("max_partitions_per_insert_block", "1000")
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

func (d *ClickHouseDriver) truncateTable(meta *model.TableMeta) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	query := fmt.Sprintf("TRUNCATE TABLE IF EXISTS %s", meta.TableName)

	_, err := d.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("clickhouse truncate via tcp failed: %w", err)
	}

	return nil
}

func (d *ClickHouseDriver) ImportDailyStocks(path string) error {
	return d.importParquet(model.TableStocksDaily, path)
}

func (d *ClickHouseDriver) Import1MinStocks(path string) error {
	return d.importParquet(model.TableStocks1Min, path)
}

func (d *ClickHouseDriver) Import5MinStocks(path string) error {
	return d.importParquet(model.TableStocks5Min, path)
}

func (d *ClickHouseDriver) ImportGBBQ(path string) error {
	d.truncateTable(model.TableGbbq)
	return d.importParquet(model.TableGbbq, path)
}

func (d *ClickHouseDriver) ImportAdjustFactors(path string) error {
	d.truncateTable(model.TableAdjustFactor)
	return d.importParquet(model.TableAdjustFactor, path)
}

func (d *ClickHouseDriver) Query(table string, conditions map[string]interface{}, dest interface{}) error {
	query := fmt.Sprintf("SELECT * FROM %s", table)
	args := []interface{}{}
	if len(conditions) > 0 {
		whereParts := []string{}
		for k, v := range conditions {
			whereParts = append(whereParts, fmt.Sprintf("%s = ?", k))
			args = append(args, v)
		}
		query += " WHERE " + strings.Join(whereParts, " AND ")
	}

	return d.db.Select(dest, query, args...)
}

func (d *ClickHouseDriver) GetLatestDate(tableName string, dateCol string) (time.Time, error) {
	// 性能优化：利用索引快速获取最大时间
	query := fmt.Sprintf("SELECT max(%s) AS latest FROM %s", dateCol, tableName)

	var latest sql.NullTime
	err := d.db.Get(&latest, query)
	if err != nil {
		return time.Time{}, err
	}

	if !latest.Valid {
		return time.Time{}, nil
	}
	return latest.Time, nil
}

func (d *ClickHouseDriver) GetAllSymbols() ([]string, error) {
	query := fmt.Sprintf("SELECT DISTINCT symbol FROM %s", model.TableStocksDaily.TableName)

	var symbols []string
	err := d.db.Select(&symbols, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query symbols: %w", err)
	}
	return symbols, nil
}

func (d *ClickHouseDriver) QueryAllXdxr() ([]model.XdxrData, error) {
	var results []model.XdxrData
	query := fmt.Sprintf("SELECT * FROM %s ORDER BY code, date", model.ViewXdxr)

	err := d.db.Select(&results, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query xdxr: %w", err)
	}
	return results, nil
}

func (d *ClickHouseDriver) QueryStockData(symbol string, startDate, endDate *time.Time) ([]model.StockData, error) {
	query := fmt.Sprintf(
		"SELECT symbol, open, high, low, close, amount, volume, date FROM %s WHERE symbol = ?",
		model.TableStocksDaily.TableName,
	)

	args := []interface{}{symbol}

	if startDate != nil {
		query += " AND date >= ?"
		args = append(args, *startDate)
	}
	if endDate != nil {
		query += " AND date <= ?"
		args = append(args, *endDate)
	}

	var results []model.StockData
	err := d.db.Select(&results, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query stocks: %w", err)
	}

	return results, nil
}
