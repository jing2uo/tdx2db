package duckdb

import (
	"fmt"
	"strings"

	"github.com/jing2uo/tdx2db/model"
)

func (d *DuckDBDriver) ImportCSV(meta *model.TableMeta, csvPath string) error {
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
	if _, err := d.db.Exec(query); err != nil {
		return fmt.Errorf("duckdb truncate failed: %w", err)
	}
	return nil
}

func (d *DuckDBDriver) ImportKlineDaily(path string) error {
	return d.ImportCSV(model.TableKlineDaily, path)
}

func (d *DuckDBDriver) ImportKline1Min(path string) error {
	return d.ImportCSV(model.TableKline1Min, path)
}

func (d *DuckDBDriver) ImportGBBQ(path string) error {
	d.TruncateTable(model.TableGbbq)
	return d.ImportCSV(model.TableGbbq, path)
}

func (d *DuckDBDriver) ImportBasic(path string) error {
	return d.ImportCSV(model.TableBasicDaily, path)
}

func (d *DuckDBDriver) ImportAdjustFactors(path string) error {
	return d.ImportCSV(model.TableAdjustFactor, path)
}

func (d *DuckDBDriver) ImportHolidays(path string) error {
	d.TruncateTable(model.TableHoliday)
	return d.ImportCSV(model.TableHoliday, path)
}

// RebuildSymbolClass 全量重建 symbol_class 表（从 raw_kline_daily 取 distinct symbol 后归类）。
// 写入逻辑跟 Import 系列同性质，放这里。
func (d *DuckDBDriver) RebuildSymbolClass() error {
	kline := model.TableKlineDaily.TableName
	class := model.TableSymbolClass.TableName

	var codes []string
	if err := d.db.Select(&codes, fmt.Sprintf("SELECT DISTINCT symbol FROM %s", kline)); err != nil {
		return fmt.Errorf("failed to collect symbols: %w", err)
	}

	tx, err := d.db.Beginx()
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(fmt.Sprintf("DELETE FROM %s", class)); err != nil {
		return fmt.Errorf("failed to clear symbol_class: %w", err)
	}

	stmt, err := tx.Preparex(fmt.Sprintf("INSERT INTO %s (symbol, class) VALUES (?, ?)", class))
	if err != nil {
		return fmt.Errorf("failed to prepare insert: %w", err)
	}
	defer stmt.Close()

	for _, c := range codes {
		if _, err := stmt.Exec(c, model.ClassifyCode(c)); err != nil {
			return fmt.Errorf("failed to insert class for %s: %w", c, err)
		}
	}

	return tx.Commit()
}
