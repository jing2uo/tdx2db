package clickhouse

import (
	"fmt"
	"strings"

	"github.com/jing2uo/tdx2db/model"
)

// mapType 针对 ClickHouse 进行类型优化
func (d *ClickHouseDriver) mapType(colName string, dt model.DataType) string {
	// 性能优化：对于 code 或 symbol 使用 LowCardinality
	isKey := strings.Contains(strings.ToLower(colName), "code") ||
		strings.Contains(strings.ToLower(colName), "symbol") ||
		strings.Contains(strings.ToLower(colName), "category")

	switch dt {
	case model.TypeString:
		if isKey {
			return "LowCardinality(String)"
		}
		return "String"
	case model.TypeFloat64:
		return "Float64"
	case model.TypeInt64:
		return "Int64"
	case model.TypeDate:
		return "Date32" // Date32 范围比 Date 更大 (1900-2299)
	case model.TypeDateTime:
		return "DateTime64(0)"
	default:
		return "String"
	}
}

func (d *ClickHouseDriver) createTableInternal(meta *model.TableMeta) error {
	var colDefs []string
	var dateCol, keyCol string

	// 1. 构建列定义
	for _, col := range meta.Columns {
		sqlType := d.mapType(col.Name, col.Type)
		colDefs = append(colDefs, fmt.Sprintf("%s %s", col.Name, sqlType))

		// 自动探测用于排序键的列
		lowerName := strings.ToLower(col.Name)
		if lowerName == "date" || lowerName == "datetime" {
			dateCol = col.Name
		}
		if lowerName == "symbol" || lowerName == "code" {
			keyCol = col.Name
		}
	}

	// 2. 确定排序键 (MergeTree 必须)
	// 通常是 (symbol, date) 以便快速查找某只股票的历史
	orderBy := "tuple()"
	if keyCol != "" && dateCol != "" {
		orderBy = fmt.Sprintf("(%s, %s)", keyCol, dateCol)
	} else if dateCol != "" {
		orderBy = dateCol
	} else if keyCol != "" {
		orderBy = keyCol
	}

	// 3. 建表语句
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			%s
		) ENGINE = MergeTree()
		ORDER BY %s
	`, meta.TableName, strings.Join(colDefs, ", "), orderBy)

	_, err := d.db.Exec(query)
	return err
}

func (d *ClickHouseDriver) registerViews() {
	// 1. 除权除息视图
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

	// 2. 换手率与市值视图
	d.viewImpls[model.ViewTurnover] = func() error {
		query := fmt.Sprintf(`
			CREATE OR REPLACE VIEW %s AS
			WITH base_capital AS (
				SELECT
					date,
					code,
					c3 AS float_shares,
					c4 AS total_shares
				FROM %s
				WHERE category IN (2, 3, 5, 7, 8, 9, 10)
			),
			expanded AS (
				SELECT
					d.date,
					d.symbol,
					last_value(c.float_shares) IGNORE NULLS
						OVER (PARTITION BY d.symbol ORDER BY d.date) AS float_shares,
					last_value(c.total_shares) IGNORE NULLS
						OVER (PARTITION BY d.symbol ORDER BY d.date) AS total_shares
				FROM %s d
				LEFT JOIN base_capital c
					ON c.code = substring(d.symbol, 3)
					AND c.date = d.date
			)
			SELECT
				r.date,
				r.symbol,
				CASE WHEN e.float_shares > 0 THEN
					ROUND(r.volume / (e.float_shares * 10000), 6)
				ELSE 0 END AS turnover,
				ROUND(e.float_shares * 10000 * r.close, 2) AS circ_mv,
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

	createAdjustView := func(viewName model.ViewID, sourceTable, factorCol string, isMin bool) error {
		dateJoinCond := "s.date = f.date"
		cols := "s.date"

		if isMin {
			dateJoinCond = "toDate(s.datetime) = f.date"
			cols = "s.datetime"
		}

		query := fmt.Sprintf(`
			CREATE OR REPLACE VIEW %s AS
			SELECT
				s.symbol,
				%s,
				s.volume,
				s.amount,
				ROUND(s.open  * f.%s, 2) AS open,
				ROUND(s.high  * f.%s, 2) AS high,
				ROUND(s.low   * f.%s, 2) AS low,
				ROUND(s.close * f.%s, 2) AS close
				%s
			FROM %s s
			JOIN %s f ON s.symbol = f.symbol AND %s
			%s
		`,
			viewName,
			cols,
			factorCol, factorCol, factorCol, factorCol,
			func() string {
				if !isMin {
					return ", t.turnover, t.circ_mv, t.total_mv"
				}
				return ""
			}(),
			sourceTable,
			model.TableAdjustFactor.TableName,
			dateJoinCond,
			func() string {
				if !isMin {
					return fmt.Sprintf("LEFT JOIN %s t ON s.symbol = t.symbol AND s.date = t.date", model.ViewTurnover)
				}
				return ""
			}(),
		)
		_, err := d.db.Exec(query)
		return err
	}

	d.viewImpls[model.ViewDailyQFQ] = func() error {
		return createAdjustView(model.ViewDailyQFQ, model.TableStocksDaily.TableName, "qfq_factor", false)
	}
	d.viewImpls[model.ViewDailyHFQ] = func() error {
		return createAdjustView(model.ViewDailyHFQ, model.TableStocksDaily.TableName, "hfq_factor", false)
	}
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
