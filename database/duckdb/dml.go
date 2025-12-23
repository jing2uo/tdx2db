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

func (d *DuckDBDriver) truncateTable(meta *model.TableMeta) error {

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
	d.truncateTable(model.TableGbbq)
	return d.importCSV(model.TableGbbq, path)
}

func (d *DuckDBDriver) ImportBasic(path string) error {
	return d.importCSV(model.TableBasic, path)
}

func (d *DuckDBDriver) ImportAdjustFactors(path string) error {
	d.truncateTable(model.TableAdjustFactor)
	return d.importCSV(model.TableAdjustFactor, path)
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

func (d *DuckDBDriver) GetLatestBasics() ([]model.StockBasic, error) {
	table := model.TableBasic.TableName

	query := fmt.Sprintf(
		`SELECT * FROM %s WHERE date = (SELECT max(date) FROM %s) ORDER BY symbol`,
		table, table,
	)

	var results []model.StockBasic
	if err := d.db.Select(&results, query); err != nil {
		return nil, fmt.Errorf("failed to query latest daily basics: %w", err)
	}

	return results, nil
}
