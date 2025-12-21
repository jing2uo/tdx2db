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
		return "TIMESTAMP WITH TIME ZONE"
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
	// 通用创建复权视图函数
	createAdjustView := func(viewName model.ViewID, factorCol string) error {
		// 组装 SQL
		query := fmt.Sprintf(`
            CREATE OR REPLACE VIEW %s AS
            SELECT
                s.date,
                s.symbol,
                ROUND(s.open  * f.%s, 2) AS open,
                ROUND(s.high  * f.%s, 2) AS high,
                ROUND(s.low   * f.%s, 2) AS low,
                ROUND(s.close * f.%s, 2) AS close,
                b.preclose,
                s.volume,
                s.amount,
                b.turnover,
                b.floatmv,
                b.totalmv
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
