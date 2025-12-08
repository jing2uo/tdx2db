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
		return "TIMESTAMP"
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

	// 1. 除权除息视图 (ViewXDXR)
	d.viewImpls[model.ViewXdxr] = func() error {
		query := fmt.Sprintf(`
			CREATE OR REPLACE VIEW %s AS
			SELECT
				date,
				code,
				c1 as fenhong,
				c2 as peigujia,
				c3 as songzhuangu,
				c4 as peigu
			FROM %s
			WHERE category = 1
		`, model.ViewXdxr, model.TableGbbq.TableName)

		_, err := d.db.Exec(query)
		return err
	}

	// 2. 换手率与市值视图 (ViewTurnover)
	d.viewImpls[model.ViewTurnover] = func() error {
		query := fmt.Sprintf(`
			CREATE OR REPLACE VIEW %s AS
			WITH base_capital AS (
				SELECT
					date,
					code,
					c3 AS float_shares, -- 流通股本
					c4 AS total_shares  -- 总股本
				FROM %s
				WHERE category IN (2, 3, 5, 7, 8, 9, 10) -- 股本变动类别
			),
			expanded AS (
				SELECT
					d.date,
					d.symbol,
					-- 填充股本数据：如果当天没有变动记录，取最近一次的值
					LAST_VALUE(c.float_shares IGNORE NULLS)
						OVER (PARTITION BY d.symbol ORDER BY d.date) AS float_shares,
					LAST_VALUE(c.total_shares IGNORE NULLS)
						OVER (PARTITION BY d.symbol ORDER BY d.date) AS total_shares
				FROM %s d
				LEFT JOIN base_capital c
					ON c.code = SUBSTR(d.symbol, 3)
					AND c.date = d.date
			)
			SELECT
				r.date,
				r.symbol,
				CASE WHEN e.float_shares > 0 THEN
					ROUND(r.volume / (e.float_shares * 10000), 6)
				ELSE 0 END AS turnover,

				-- 流通市值 = 流通股本(万) * 10000 * 收盘价
				ROUND(e.float_shares * 10000 * r.close, 2) AS circ_mv,

				-- 总市值
				ROUND(e.total_shares * 10000 * r.close, 2) AS total_mv
			FROM %s r
			JOIN expanded e ON r.symbol = e.symbol AND r.date = e.date
		`,
			model.ViewTurnover,
			model.TableGbbq.TableName,
			model.TableStocksDaily.TableName,
			model.TableStocksDaily.TableName)

		_, err := d.db.Exec(query)
		return err
	}

	// 3. 日线前复权 (ViewDailyQFQ)
	d.viewImpls[model.ViewDailyQFQ] = func() error {
		query := fmt.Sprintf(`
			CREATE OR REPLACE VIEW %s AS
			SELECT
				s.symbol,
				s.date,
				s.volume,
				s.amount,
				ROUND(s.open  * f.qfq_factor, 2) AS open,
				ROUND(s.high  * f.qfq_factor, 2) AS high,
				ROUND(s.low   * f.qfq_factor, 2) AS low,
				ROUND(s.close * f.qfq_factor, 2) AS close,
				t.turnover,
				t.circ_mv,
				t.total_mv
			FROM %s s
			JOIN %s f ON s.symbol = f.symbol AND s.date = f.date
			LEFT JOIN %s t ON s.symbol = t.symbol AND s.date = t.date
		`,
			model.ViewDailyQFQ,
			model.TableStocksDaily.TableName,
			model.TableAdjustFactor.TableName,
			model.ViewTurnover)

		_, err := d.db.Exec(query)
		return err
	}

	// 4. 日线后复权 (ViewDailyHFQ)
	d.viewImpls[model.ViewDailyHFQ] = func() error {
		query := fmt.Sprintf(`
			CREATE OR REPLACE VIEW %s AS
			SELECT
				s.symbol,
				s.date,
				s.volume,
				s.amount,
				ROUND(s.open  * f.hfq_factor, 2) AS open,
				ROUND(s.high  * f.hfq_factor, 2) AS high,
				ROUND(s.low   * f.hfq_factor, 2) AS low,
				ROUND(s.close * f.hfq_factor, 2) AS close,
				t.turnover,
				t.circ_mv,
				t.total_mv
			FROM %s s
			JOIN %s f ON s.symbol = f.symbol AND s.date = f.date
			LEFT JOIN %s t ON s.symbol = t.symbol AND s.date = t.date
		`,
			model.ViewDailyHFQ,
			model.TableStocksDaily.TableName,
			model.TableAdjustFactor.TableName,
			model.ViewTurnover)

		_, err := d.db.Exec(query)
		return err
	}

	// 5. 1分钟前复权 (View1MinQFQ)
	d.viewImpls[model.View1MinQFQ] = func() error {
		query := fmt.Sprintf(`
			CREATE OR REPLACE VIEW %s AS
			SELECT
				s.symbol,
				s.datetime,
				s.volume,
				s.amount,
				ROUND(s.open  * f.qfq_factor, 2) AS open,
				ROUND(s.high  * f.qfq_factor, 2) AS high,
				ROUND(s.low   * f.qfq_factor, 2) AS low,
				ROUND(s.close * f.qfq_factor, 2) AS close
			FROM %s s
			-- CAST(s.datetime AS DATE) 将 datetime 转换为 date 以匹配因子表
			JOIN %s f ON s.symbol = f.symbol AND CAST(s.datetime AS DATE) = f.date
		`,
			model.View1MinQFQ,
			model.TableStocks1Min.TableName,
			model.TableAdjustFactor.TableName)

		_, err := d.db.Exec(query)
		return err
	}

	// 6. 1分钟后复权 (View1MinHFQ)
	d.viewImpls[model.View1MinHFQ] = func() error {
		query := fmt.Sprintf(`
			CREATE OR REPLACE VIEW %s AS
			SELECT
				s.symbol,
				s.datetime,
				s.volume,
				s.amount,
				ROUND(s.open  * f.hfq_factor, 2) AS open,
				ROUND(s.high  * f.hfq_factor, 2) AS high,
				ROUND(s.low   * f.hfq_factor, 2) AS low,
				ROUND(s.close * f.hfq_factor, 2) AS close
			FROM %s s
			JOIN %s f ON s.symbol = f.symbol AND CAST(s.datetime AS DATE) = f.date
		`,
			model.View1MinHFQ,
			model.TableStocks1Min.TableName,
			model.TableAdjustFactor.TableName)

		_, err := d.db.Exec(query)
		return err
	}

	// 7. 5分钟前复权 (View5MinQFQ)
	d.viewImpls[model.View5MinQFQ] = func() error {
		query := fmt.Sprintf(`
			CREATE OR REPLACE VIEW %s AS
			SELECT
				s.symbol,
				s.datetime,
				s.volume,
				s.amount,
				ROUND(s.open  * f.qfq_factor, 2) AS open,
				ROUND(s.high  * f.qfq_factor, 2) AS high,
				ROUND(s.low   * f.qfq_factor, 2) AS low,
				ROUND(s.close * f.qfq_factor, 2) AS close
			FROM %s s
			JOIN %s f ON s.symbol = f.symbol AND CAST(s.datetime AS DATE) = f.date
		`,
			model.View5MinQFQ,
			model.TableStocks5Min.TableName,
			model.TableAdjustFactor.TableName)

		_, err := d.db.Exec(query)
		return err
	}

	// 8. 5分钟后复权 (View5MinHFQ)
	d.viewImpls[model.View5MinHFQ] = func() error {
		query := fmt.Sprintf(`
			CREATE OR REPLACE VIEW %s AS
			SELECT
				s.symbol,
				s.datetime,
				s.volume,
				s.amount,
				ROUND(s.open  * f.hfq_factor, 2) AS open,
				ROUND(s.high  * f.hfq_factor, 2) AS high,
				ROUND(s.low   * f.hfq_factor, 2) AS low,
				ROUND(s.close * f.hfq_factor, 2) AS close
			FROM %s s
			JOIN %s f ON s.symbol = f.symbol AND CAST(s.datetime AS DATE) = f.date
		`,
			model.View5MinHFQ,
			model.TableStocks5Min.TableName,
			model.TableAdjustFactor.TableName)

		_, err := d.db.Exec(query)
		return err
	}
}
