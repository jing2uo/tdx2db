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
		sqlType := d.mapType(col.Type)
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

// InitSchema 创建所有表 + 视图。
// 视图实现由 views_stock.go / views_etf.go 各自注册。
func (d *DuckDBDriver) InitSchema() error {
	for _, t := range model.AllTables() {
		if err := d.createTableInternal(t); err != nil {
			return fmt.Errorf("failed to create table %s: %w", t.TableName, err)
		}
	}

	d.registerStockViews()
	d.registerETFViews()

	for _, viewID := range model.AllViews() {
		impl, ok := d.viewImpls[viewID]
		if !ok {
			return fmt.Errorf("[DuckDB] Missing implementation for required view: %s", viewID)
		}
		if err := impl(); err != nil {
			return fmt.Errorf("failed to create view %s: %w", viewID, err)
		}
	}
	return nil
}
