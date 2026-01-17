package duckdb

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jing2uo/tdx2db/model"
)

func (d *DuckDBDriver) importCSV(meta *model.TableMeta, csvPath string) error {
	var colMaps []string
	for _, col := range meta.Columns {
		duckType := d.mapType(col.Type)
		colMaps = append(colMaps, fmt.Sprintf("'%s': '%s'", col.Name, duckType))
	}

	columnsStr := strings.Join(colMaps, ", ")

	query := fmt.Sprintf(`
		INSERT INTO %s
		SELECT * FROM read_csv('%s',
			header=true,
			columns={%s},
			dateformat='%%Y-%%m-%%d',
			timestampformat='%%Y-%%m-%%d %%H:%%M'
		)
	`, meta.TableName, csvPath, columnsStr)

	_, err := d.db.Exec(query)
	return err
}

func (d *DuckDBDriver) TruncateTable(meta *model.TableMeta) error {

	query := fmt.Sprintf("DELETE FROM %s", meta.TableName)

	_, err := d.db.Exec(query)
	if err != nil {
		return fmt.Errorf("duckdb truncate failed: %w", err)
	}

	return nil
}

func (d *DuckDBDriver) ImportDailyStocks(path string) error {
	return d.importCSV(model.TableStocksDaily, path)
}

func (d *DuckDBDriver) Import1MinStocks(path string) error {
	return d.importCSV(model.TableStocks1Min, path)
}

func (d *DuckDBDriver) Import5MinStocks(path string) error {
	return d.importCSV(model.TableStocks5Min, path)
}

func (d *DuckDBDriver) ImportGBBQ(path string) error {
	d.TruncateTable(model.TableGbbq)
	return d.importCSV(model.TableGbbq, path)
}

func (d *DuckDBDriver) ImportBasic(path string) error {
	return d.importCSV(model.TableBasic, path)
}

func (d *DuckDBDriver) ImportAdjustFactors(path string) error {
	return d.importCSV(model.TableAdjustFactor, path)
}

func (d *DuckDBDriver) ImportHolidays(path string) error {
	d.TruncateTable(model.TableHoliday)
	return d.importCSV(model.TableHoliday, path)
}

func (d *DuckDBDriver) ImportBlocksInfo(path string) error {
	d.TruncateTable(model.TableBlockInfo)
	return d.importCSV(model.TableBlockInfo, path)
}

func (d *DuckDBDriver) ImportBlocksMember(path string) error {
	return d.importCSV(model.TableBlockMember, path)
}

func (d *DuckDBDriver) Query(table string, conditions map[string]interface{}, dest interface{}) error {
	query := fmt.Sprintf("SELECT * FROM %s", table)
	args := []interface{}{}
	if len(conditions) > 0 {
		whereParts := []string{}
		i := 1
		for k, v := range conditions {
			whereParts = append(whereParts, fmt.Sprintf("%s = $%d", k, i))
			args = append(args, v)
			i++
		}
		query += " WHERE " + strings.Join(whereParts, " AND ")
	}

	return d.db.Select(dest, query, args...)
}

func (d *DuckDBDriver) GetLatestDate(tableName string, dateCol string) (time.Time, error) {
	query := fmt.Sprintf("SELECT DATE(max(%s)) AS latest FROM %s", dateCol, tableName)

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

func (d *DuckDBDriver) GetAllSymbols() ([]string, error) {
	query := fmt.Sprintf("SELECT DISTINCT symbol FROM %s", model.TableStocksDaily.TableName)

	var symbols []string
	err := d.db.Select(&symbols, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query symbols: %w", err)
	}

	return symbols, nil
}

func (d *DuckDBDriver) CountStocksDaily() (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", model.TableStocksDaily.TableName)

	var count int64
	err := d.db.Get(&count, query)
	if err != nil {
		return 0, fmt.Errorf("failed to count stocks: %w", err)
	}

	return count, nil
}

func (d *DuckDBDriver) QueryStockData(symbol string, startDate, endDate *time.Time) ([]model.StockData, error) {

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

func (d *DuckDBDriver) GetBasicsBySymbol(symbol string) ([]model.StockBasic, error) {
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

func (d *DuckDBDriver) GetLatestBasicBySymbol(symbol string) ([]model.StockBasic, error) {
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

// GetBasicsSince 获取指定日期及之后的所有 basic
func (d *DuckDBDriver) GetBasicsSince(sinceDate time.Time) ([]model.StockBasic, error) {
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

func (d *DuckDBDriver) GetGbbq() ([]model.GbbqData, error) {
	table := model.TableGbbq.TableName

	query := fmt.Sprintf(`SELECT * FROM %s ORDER BY symbol, date`, table)

	var results []model.GbbqData
	if err := d.db.Select(&results, query); err != nil {
		return nil, fmt.Errorf("failed to query gbbq: %w", err)
	}

	return results, nil
}

func (d *DuckDBDriver) GetLatestFactors() ([]model.Factor, error) {
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
