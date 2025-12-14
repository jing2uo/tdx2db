package clickhouse

import (
	"fmt"
	"strings"

	"github.com/jing2uo/tdx2db/model"
)

// mapType 针对 ClickHouse 进行类型优化
func (d *ClickHouseDriver) mapType(colName string, dt model.DataType) string {
	isKey := strings.Contains(strings.ToLower(colName), "symbol") //||
	//strings.Contains(strings.ToLower(colName), "category")

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
		return "DateTime64(0, 'Asia/Shanghai')"
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
		if lowerName == "symbol" {
			keyCol = col.Name
		}
	}

	// 2. 确定排序键 (MergeTree 必须)
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
	// 换手率与市值视图
	d.viewImpls[model.ViewTurnover] = func() error {
		query := fmt.Sprintf(`
			CREATE OR REPLACE VIEW %s AS
			WITH gbbq_sorted AS (
				SELECT
					t.symbol,
					t.date,
					t.post_float,
					t.post_total
				FROM %s AS t
				WHERE t.category IN (2, 3, 5, 7, 8, 9, 10)
				ORDER BY t.symbol, t.date
			)
			SELECT
				r.date,
				r.symbol,
				CASE WHEN g.post_float > 0 THEN
					ROUND(r.volume / (g.post_float * 10000), 6)
				ELSE 0 END AS turnover,
				ROUND(g.post_float * 10000 * r.close, 2) AS float_mv,
				ROUND(g.post_total * 10000 * r.close, 2) AS total_mv
			FROM %s r
			ASOF LEFT JOIN gbbq_sorted g
				ON r.symbol = g.symbol AND r.date >= g.date
		`,
			model.ViewTurnover,
			model.TableGbbq.TableName,
			model.TableStocksDaily.TableName)
		_, err := d.db.Exec(query)
		return err
	}

	// 通用创建复权视图函数
	createAdjustView := func(viewName model.ViewID, sourceTable, factorCol string, isMin bool) error {
		// 默认日线逻辑
		joinClause := fmt.Sprintf("LEFT JOIN %s f ON s.symbol = f.symbol AND s.date = f.date", model.TableAdjustFactor.TableName)
		timeCol := "date"

		// 分钟线逻辑
		if isMin {
			timeCol = "datetime"
			joinClause = fmt.Sprintf(`
				ASOF LEFT JOIN (
					SELECT symbol, toDateTime(date) as dt_start, %s
					FROM %s
					ORDER BY symbol, dt_start
				) f ON s.symbol = f.symbol AND s.datetime >= f.dt_start
			`, factorCol, model.TableAdjustFactor.TableName)
		}

		// 组装 SQL
		query := fmt.Sprintf(`
			CREATE OR REPLACE VIEW %s AS
			SELECT
				s.symbol,
				%s, -- date or datetime
				s.volume,
				s.amount,
				ROUND(s.open  * f.%s, 2) AS open,
				ROUND(s.high  * f.%s, 2) AS high,
				ROUND(s.low   * f.%s, 2) AS low,
				ROUND(s.close * f.%s, 2) AS close
			FROM %s s
			%s -- Factor Join
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

	// 注册各个视图
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
