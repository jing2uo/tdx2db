package clickhouse

import (
	"fmt"
	"strings"

	"github.com/jing2uo/tdx2db/model"
)

// mapType 针对 ClickHouse 进行类型优化
func (d *ClickHouseDriver) mapType(col model.Column) string {
	colName := col.Name
	dt := col.Type
	isKey := strings.Contains(strings.ToLower(colName), "symbol")

	var sqlType string
	switch dt {
	case model.TypeString:
		if isKey {
			sqlType = "LowCardinality(String)"
		} else {
			sqlType = "String"
		}
	case model.TypeFloat64:
		sqlType = "Float64"
	case model.TypeInt64:
		sqlType = "Int64"
	case model.TypeDate:
		sqlType = "Date32"
	case model.TypeDateTime:
		sqlType = "DateTime64(0, 'Asia/Shanghai')"
	default:
		sqlType = "String"
	}
	if col.Nullable {
		return fmt.Sprintf("Nullable(%s)", sqlType)
	}
	return sqlType
}

func (d *ClickHouseDriver) createViewInternal(view model.ViewDef) error {
	if view.ClickHouse == "" {
		return fmt.Errorf("view %s has no ClickHouse SQL", view.Name)
	}
	_, err := d.db.Exec(fmt.Sprintf("CREATE OR REPLACE VIEW %s AS\n%s", view.Name, view.ClickHouse))
	return err
}

func (d *ClickHouseDriver) createTableInternal(meta *model.TableMeta) error {
	var colDefs []string
	var dateCol, keyCol string

	// 1. 构建列定义
	for _, col := range meta.Columns {
		sqlType := d.mapType(col)
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
	if keys := cleanOrderByKey(meta.OrderByKey); len(keys) > 0 {
		orderBy = strings.Join(keys, ", ")
		if len(keys) > 1 {
			orderBy = fmt.Sprintf("(%s)", orderBy)
		}
	} else if keyCol != "" && dateCol != "" {
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

func cleanOrderByKey(keys []string) []string {
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key != "" {
			result = append(result, key)
		}
	}
	return result
}

// InitSchema 创建所有表 + 视图，表与视图定义均来自 model registry。
func (d *ClickHouseDriver) InitSchema() error {
	for _, t := range model.AllTables() {
		if err := d.createTableInternal(t); err != nil {
			return fmt.Errorf("failed to create table %s: %w", t.TableName, err)
		}
	}

	for _, view := range model.AllViews() {
		if err := d.createViewInternal(view); err != nil {
			return fmt.Errorf("failed to create view %s: %w", view.Name, err)
		}
	}
	return nil
}
