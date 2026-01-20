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

func (d *ClickHouseDriver) importCSV(meta *model.TableMeta, filePath string) error {
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

	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", meta.TableName)
	_, err := d.db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("clickhouse drop table failed: %w", err)
	}

	return d.createTableInternal(meta)
}

func (d *ClickHouseDriver) ImportDailyStocks(path string) error {
	return d.importCSV(model.TableStocksDaily, path)
}

func (d *ClickHouseDriver) Import1MinStocks(path string) error {
	return d.importCSV(model.TableStocks1Min, path)
}

func (d *ClickHouseDriver) Import5MinStocks(path string) error {
	return d.importCSV(model.TableStocks5Min, path)
}

func (d *ClickHouseDriver) ImportGBBQ(path string) error {
	d.TruncateTable(model.TableGbbq)
	return d.importCSV(model.TableGbbq, path)
}

func (d *ClickHouseDriver) ImportBasic(path string) error {
	return d.importCSV(model.TableBasic, path)
}

func (d *ClickHouseDriver) ImportAdjustFactors(path string) error {
	return d.importCSV(model.TableAdjustFactor, path)
}

func (d *ClickHouseDriver) ImportHolidays(path string) error {
	d.TruncateTable(model.TableHoliday)
	return d.importCSV(model.TableHoliday, path)
}

func (d *ClickHouseDriver) ImportBlocksInfo(path string) error {
	d.TruncateTable(model.TableBlockInfo)
	return d.importCSV(model.TableBlockInfo, path)
}

func (d *ClickHouseDriver) ImportBlocksMember(path string) error {
	return d.importCSV(model.TableBlockMember, path)
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
	query := fmt.Sprintf("SELECT toDate(maxOrNull(%s)) AS latest FROM %s", dateCol, tableName)
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

func (d *ClickHouseDriver) CountStocksDaily() (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", model.TableStocksDaily.TableName)

	var count int64
	err := d.db.Get(&count, query)
	if err != nil {
		return 0, fmt.Errorf("failed to count stocks: %w", err)
	}

	return count, nil
}

func (d *ClickHouseDriver) QueryStockData(symbol string, startDate, endDate *time.Time) ([]model.StockData, error) {

	conditions := []string{"symbol = ?"}
	args := []interface{}{symbol}

	if startDate != nil {
		conditions = append(conditions, "date >= ?")
		args = append(args, *startDate)
	}
	if endDate != nil {
		conditions = append(conditions, "date <= ?")
		args = append(args, *endDate)
	}

	query := fmt.Sprintf(
		`SELECT * FROM %s WHERE %s ORDER BY date ASC`,
		model.TableStocksDaily.TableName,
		strings.Join(conditions, " AND "),
	)

	var results []model.StockData
	if err := d.db.Select(&results, query, args...); err != nil {
		return nil, fmt.Errorf("failed to query stocks: %w", err)
	}

	return results, nil
}

func (d *ClickHouseDriver) GetBasicsBySymbol(symbol string) ([]model.StockBasic, error) {
	query := fmt.Sprintf(
		"SELECT * FROM %s WHERE symbol = ? ORDER BY date",
		model.TableBasic.TableName,
	)

	var results []model.StockBasic
	if err := d.db.Select(&results, query, symbol); err != nil {
		return nil, fmt.Errorf("failed to query daily basics by symbol %s: %w", symbol, err)
	}

	return results, nil
}

func (d *ClickHouseDriver) GetLatestBasicBySymbol(symbol string) ([]model.StockBasic, error) {
	query := fmt.Sprintf(
		"SELECT * FROM %s WHERE symbol = ? ORDER BY date DESC LIMIT 1",
		model.TableBasic.TableName,
	)

	var results []model.StockBasic
	if err := d.db.Select(&results, query, symbol); err != nil {
		return nil, fmt.Errorf("failed to query latest daily basic by symbol %s: %w", symbol, err)
	}

	return results, nil
}

func (d *ClickHouseDriver) GetBasicsSince(sinceDate time.Time) ([]model.StockBasic, error) {
	table := model.TableBasic.TableName

	query := fmt.Sprintf(`
		SELECT date, symbol, close, preclose, turnover, floatmv, totalmv
		FROM %s WHERE date >= ? ORDER BY symbol, date
	`, table)

	var results []model.StockBasic
	if err := d.db.Select(&results, query, sinceDate); err != nil {
		return nil, fmt.Errorf("failed to query basics since %v: %w", sinceDate, err)
	}

	return results, nil
}

func (d *ClickHouseDriver) GetGbbq() ([]model.GbbqData, error) {
	table := model.TableGbbq.TableName

	query := fmt.Sprintf(`SELECT * FROM %s ORDER BY symbol, date`, table)

	var results []model.GbbqData
	if err := d.db.Select(&results, query); err != nil {
		return nil, fmt.Errorf("failed to query gbbq: %w", err)
	}

	return results, nil
}

func (d *ClickHouseDriver) GetLatestFactors() ([]model.Factor, error) {
	table := model.TableAdjustFactor.TableName

	query := fmt.Sprintf(`
		SELECT symbol, date, hfq_factor
		FROM %s
		QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1
	`, table)

	var results []model.Factor
	if err := d.db.Select(&results, query); err != nil {
		return nil, fmt.Errorf("failed to query latest factors: %w", err)
	}

	return results, nil
}
