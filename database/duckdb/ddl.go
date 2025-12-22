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
	// 通用创建复权视图函数
	createAdjustView := func(viewName model.ViewID, factorCol string) error {
		// 组装 SQL
		priceFactor := "1"
		if factorCol != "" {
			priceFactor = "f." + factorCol
		}

		factorSelect := ""
		switch factorCol {
		case "qfq_factor":
			factorSelect = "f.qfq_factor AS qfq_factor"
		case "hfq_factor":
			factorSelect = "f.hfq_factor AS hfq_factor"
		default:
			factorSelect = `
                f.qfq_factor AS qfq_factor,
                f.hfq_factor AS hfq_factor`
		}

		query := fmt.Sprintf(`
            CREATE OR REPLACE VIEW %s AS
            SELECT
                s.symbol AS symbol,
                s.date   AS date,
                ROUND(s.open  * %s, 2) AS open,
                ROUND(s.high  * %s, 2) AS high,
                ROUND(s.low   * %s, 2) AS low,
                ROUND(s.close * %s, 2) AS close,
                b.preclose   AS preclose,
                s.volume     AS volume,
                s.amount     AS amount,
                b.turnover   AS turnover,
                b.floatmv    AS floatmv,
                b.totalmv    AS totalmv,
				%s
            FROM %s s
            LEFT JOIN %s f ON s.symbol = f.symbol AND s.date = f.date
            LEFT JOIN %s b ON s.symbol = b.symbol AND s.date = b.date
        `,
			viewName,
			priceFactor, priceFactor, priceFactor, priceFactor,
			factorSelect,
			model.TableStocksDaily.TableName,
			model.TableAdjustFactor.TableName,
			model.TableBasic.TableName,
		)

		_, err := d.db.Exec(query)
		return err
	}

	// 注册各个视图
	d.viewImpls[model.ViewDailyBFQ] = func() error {
		return createAdjustView(model.ViewDailyBFQ, "")
	}

	d.viewImpls[model.ViewDailyQFQ] = func() error {
		return createAdjustView(model.ViewDailyQFQ, "qfq_factor")
	}

	d.viewImpls[model.ViewDailyHFQ] = func() error {
		return createAdjustView(model.ViewDailyHFQ, "hfq_factor")
	}
}
