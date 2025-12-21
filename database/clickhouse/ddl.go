package clickhouse

import (
	"fmt"
	"strings"

	"github.com/jing2uo/tdx2db/model"
)

// mapType 针对 ClickHouse 进行类型优化
func (d *ClickHouseDriver) mapType(colName string, dt model.DataType) string {
	isKey := strings.Contains(strings.ToLower(colName), "symbol")

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
		return "Date32"
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
	// 通用创建复权视图函数
	createAdjustView := func(viewName model.ViewID, factorCol string) error {
		// 组装 SQL
		query := fmt.Sprintf(`
            CREATE OR REPLACE VIEW %s AS
            SELECT
                s.date   AS date,
                s.symbol AS symbol,
                ROUND(s.open  * f.%s, 2) AS open,
                ROUND(s.high  * f.%s, 2) AS high,
                ROUND(s.low   * f.%s, 2) AS low,
                ROUND(s.close * f.%s, 2) AS close,
                b.preclose AS preclose,
                s.volume   AS volume,
                s.amount   AS amount,
                b.turnover AS turnover,
                b.floatmv  AS floatmv,
                b.totalmv  AS totalmv
            FROM %s s
            LEFT JOIN %s f ON s.symbol = f.symbol AND s.date = f.date
            LEFT JOIN %s b ON s.symbol = b.symbol AND s.date = b.date
        `,
			viewName,
			factorCol, factorCol, factorCol, factorCol,
			model.TableStocksDaily.TableName,
			model.TableAdjustFactor.TableName,
			model.TableBasic.TableName,
		)

		_, err := d.db.Exec(query)
		return err
	}

	// 注册各个视图
	d.viewImpls[model.ViewDailyQFQ] = func() error {
		return createAdjustView(model.ViewDailyQFQ, "qfq_factor")
	}

	d.viewImpls[model.ViewDailyHFQ] = func() error {
		return createAdjustView(model.ViewDailyHFQ, "hfq_factor")
	}
}
