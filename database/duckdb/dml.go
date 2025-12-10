package duckdb

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jing2uo/tdx2db/model"
)

func (d *DuckDBDriver) importParquet(meta *model.TableMeta, parquetPath string) error {
	query := fmt.Sprintf(`
        INSERT INTO %s BY NAME
        SELECT * FROM read_parquet('%s')
    `, meta.TableName, parquetPath)

	_, err := d.db.Exec(query)
	return err
}

func (d *DuckDBDriver) ImportDailyStocks(path string) error {
	return d.importParquet(model.TableStocksDaily, path)
}

func (d *DuckDBDriver) Import1MinStocks(path string) error {
	return d.importParquet(model.TableStocks1Min, path)
}

func (d *DuckDBDriver) Import5MinStocks(path string) error {
	return d.importParquet(model.TableStocks5Min, path)
}

func (d *DuckDBDriver) ImportGBBQ(path string) error {
	return d.importParquet(model.TableGbbq, path)
}

func (d *DuckDBDriver) ImportAdjustFactors(path string) error {
	return d.importParquet(model.TableAdjustFactor, path)
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

func (d *DuckDBDriver) GetAllSymbols() ([]string, error) {
	query := fmt.Sprintf("SELECT DISTINCT symbol FROM %s", model.TableStocksDaily.TableName)

	var symbols []string
	err := d.db.Select(&symbols, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query symbols: %w", err)
	}

	return symbols, nil
}

func (d *DuckDBDriver) QueryAllXdxr() ([]model.XdxrData, error) {
	var results []model.XdxrData

	query := fmt.Sprintf(
		"SELECT * FROM %s ORDER BY code, date",
		model.ViewXdxr,
	)

	err := d.db.Select(&results, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query xdxr: %w", err)
	}

	return results, nil
}

func (d *DuckDBDriver) QueryStockData(symbol string, startDate, endDate *time.Time) ([]model.StockData, error) {
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
