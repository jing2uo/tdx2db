package duckdb

import (
	"fmt"
	"strings"

	"github.com/jing2uo/tdx2db/model"
)

// mapType 将通用 DataType 转换为 DuckDB 的 SQL 类型
func (d *DuckDBDriver) mapType(dt model.DataType) string {
	switch dt {
	case model.TypeString:
		return "VARCHAR"
	case model.TypeFloat64:
		return "DOUBLE"
	case model.TypeInt64:
		return "BIGINT"
	case model.TypeDate:
		return "DATE"
	case model.TypeDateTime:
		return "TIMESTAMPTZ"
	default:
		return "VARCHAR"
	}
}

func (d *DuckDBDriver) createTableInternal(meta *model.TableMeta) error {
	var colDefs []string
	for _, col := range meta.Columns {
		// 1. 获取 DuckDB 具体的类型
		sqlType := d.mapType(col.Type)
		// 2. 拼接字段定义
		colDefs = append(colDefs, fmt.Sprintf("%s %s", col.Name, sqlType))
	}

	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)",
		meta.TableName, strings.Join(colDefs, ", "))

	_, err := d.db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", meta.TableName, err)
	}
	return nil
}

func (d *DuckDBDriver) registerViews() {
	// 不复权视图 (BFQ)
	d.viewImpls[model.ViewDailyBFQ] = func() error {
		query := fmt.Sprintf(`
			CREATE OR REPLACE VIEW %s AS
			SELECT
				s.symbol   AS symbol,
				s.date     AS date,
				s.open     AS open,
				s.high     AS high,
				s.low      AS low,
				s.close    AS close,
				b.preclose AS preclose,
				s.volume   AS volume,
				s.amount   AS amount,
				b.turnover AS turnover,
				b.floatmv  AS floatmv,
				b.totalmv  AS totalmv
			FROM %s s
			LEFT JOIN %s b ON s.symbol = b.symbol AND s.date = b.date
		`,
			model.ViewDailyBFQ,
			model.TableStocksDaily.TableName,
			model.TableBasic.TableName,
		)
		_, err := d.db.Exec(query)
		return err
	}

	// 后复权视图 (HFQ) - factor 表已和日线对齐，直接 JOIN
	d.viewImpls[model.ViewDailyHFQ] = func() error {
		query := fmt.Sprintf(`
			CREATE OR REPLACE VIEW %s AS
			SELECT
				s.symbol   AS symbol,
				s.date     AS date,
				ROUND(s.open  * COALESCE(f.hfq_factor, 1), 2) AS open,
				ROUND(s.high  * COALESCE(f.hfq_factor, 1), 2) AS high,
				ROUND(s.low   * COALESCE(f.hfq_factor, 1), 2) AS low,
				ROUND(s.close * COALESCE(f.hfq_factor, 1), 2) AS close,
				ROUND(b.preclose * COALESCE(f.hfq_factor, 1), 2) AS preclose,
				s.volume   AS volume,
				s.amount   AS amount,
				b.turnover AS turnover,
				b.floatmv  AS floatmv,
				b.totalmv  AS totalmv,
				COALESCE(f.hfq_factor, 1) AS hfq_factor
			FROM %s s
			LEFT JOIN %s f ON s.symbol = f.symbol AND s.date = f.date
			LEFT JOIN %s b ON s.symbol = b.symbol AND s.date = b.date
		`,
			model.ViewDailyHFQ,
			model.TableStocksDaily.TableName,
			model.TableAdjustFactor.TableName,
			model.TableBasic.TableName,
		)
		_, err := d.db.Exec(query)
		return err
	}

	// 前复权视图 (QFQ) - qfq_factor = hfq_factor / latest_hfq_factor
	d.viewImpls[model.ViewDailyQFQ] = func() error {
		query := fmt.Sprintf(`
			CREATE OR REPLACE VIEW %s AS
			WITH latest_factors AS (
				SELECT symbol, argMax(hfq_factor, date) AS latest_hfq
				FROM %s
				GROUP BY symbol
			)
			SELECT
				s.symbol   AS symbol,
				s.date     AS date,
				ROUND(s.open  * COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1), 2) AS open,
				ROUND(s.high  * COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1), 2) AS high,
				ROUND(s.low   * COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1), 2) AS low,
				ROUND(s.close * COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1), 2) AS close,
				ROUND(b.preclose * COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1), 2) AS preclose,
				s.volume   AS volume,
				s.amount   AS amount,
				b.turnover AS turnover,
				b.floatmv  AS floatmv,
				b.totalmv  AS totalmv,
				COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1) AS qfq_factor
			FROM %s s
			LEFT JOIN %s f ON s.symbol = f.symbol AND s.date = f.date
			LEFT JOIN latest_factors lf ON s.symbol = lf.symbol
			LEFT JOIN %s b ON s.symbol = b.symbol AND s.date = b.date
		`,
			model.ViewDailyQFQ,
			model.TableAdjustFactor.TableName,
			model.TableStocksDaily.TableName,
			model.TableAdjustFactor.TableName,
			model.TableBasic.TableName,
		)
		_, err := d.db.Exec(query)
		return err
	}
}
