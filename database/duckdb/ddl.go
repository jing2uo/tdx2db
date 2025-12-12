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
			SELECT date, code, c1 as fenhong, c2 as peigujia, c3 as songzhuangu, c4 as peigu
			FROM %s WHERE category = 1
		`, model.ViewXdxr, model.TableGbbq.TableName)
		_, err := d.db.Exec(query)
		return err
	}

	// 2. 换手率与市值视图 (ViewTurnover)
	d.viewImpls[model.ViewTurnover] = func() error {
		query := fmt.Sprintf(`
			CREATE OR REPLACE VIEW %s AS
			WITH gbbq_sorted AS (
				SELECT
					date,
					code,
					c3 AS float_shares,
					c4 AS total_shares
				FROM %s
				WHERE category IN (2, 3, 5, 7, 8, 9, 10)
				ORDER BY code, date -- ASOF JOIN 要求右表必须排序
			)
			SELECT
				r.date,
				r.symbol,
				CASE WHEN g.float_shares > 0 THEN
					ROUND(r.volume / (g.float_shares * 10000), 6)
				ELSE 0 END AS turnover,
				ROUND(g.float_shares * 10000 * r.close, 2) AS circ_mv,
				ROUND(g.total_shares * 10000 * r.close, 2) AS total_mv
			FROM %s r
			ASOF JOIN gbbq_sorted g
				ON SUBSTR(r.symbol, 3) = g.code -- 关联代码 (假设 r.symbol 是 sh600000)
				AND r.date >= g.date            -- 时间对齐 (找最近的股本)
		`,
			model.ViewTurnover,
			model.TableGbbq.TableName,
			model.TableStocksDaily.TableName)
		_, err := d.db.Exec(query)
		return err
	}

	// 通用复权视图构建函数
	createAdjustView := func(viewName model.ViewID, sourceTable string, factorCol string, isMin bool) error {
		var joinClause string
		timeCol := "date"

		if isMin {
			timeCol = "datetime"
			joinClause = fmt.Sprintf(`
				ASOF JOIN (SELECT * FROM %s ORDER BY symbol, date) f
				ON s.symbol = f.symbol AND s.datetime >= CAST(f.date AS TIMESTAMP)
			`, model.TableAdjustFactor.TableName)

		} else {
			// --- 日线逻辑 ---
			joinClause = fmt.Sprintf("LEFT JOIN %s f ON s.symbol = f.symbol AND s.date = f.date", model.TableAdjustFactor.TableName)
		}

		query := fmt.Sprintf(`
			CREATE OR REPLACE VIEW %s AS
			SELECT
				s.symbol,
				s.%s, -- date or datetime
				s.volume,
				s.amount,
				ROUND(s.open  * f.%s, 2) AS open,
				ROUND(s.high  * f.%s, 2) AS high,
				ROUND(s.low   * f.%s, 2) AS low,
				ROUND(s.close * f.%s, 2) AS close
			FROM %s s
			%s -- Factor Join (Standard or ASOF)
		`,
			viewName,
			timeCol,
			factorCol, factorCol, factorCol, factorCol,
			sourceTable,
			joinClause,
		)

		_, err := d.db.Exec(query)
		return err
	}

	// 3. 注册日线复权
	d.viewImpls[model.ViewDailyQFQ] = func() error {
		return createAdjustView(model.ViewDailyQFQ, model.TableStocksDaily.TableName, "qfq_factor", false)
	}
	d.viewImpls[model.ViewDailyHFQ] = func() error {
		return createAdjustView(model.ViewDailyHFQ, model.TableStocksDaily.TableName, "hfq_factor", false)
	}

	// 4. 注册分钟线复权
	d.viewImpls[model.View1MinQFQ] = func() error {
		return createAdjustView(model.View1MinQFQ, model.TableStocks1Min.TableName, "qfq_factor", true)
	}
	d.viewImpls[model.View1MinHFQ] = func() error {
		return createAdjustView(model.View1MinHFQ, model.TableStocks1Min.TableName, "hfq_factor", true)
	}
	d.viewImpls[model.View5MinQFQ] = func() error {
		return createAdjustView(model.View5MinQFQ, model.TableStocks5Min.TableName, "qfq_factor", true)
	}
	d.viewImpls[model.View5MinHFQ] = func() error {
		return createAdjustView(model.View5MinHFQ, model.TableStocks5Min.TableName, "hfq_factor", true)
	}
}
