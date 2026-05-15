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

// InitSchema 创建所有表 + 视图。
// 视图实现由 views_stock.go / views_etf.go 各自注册。
func (d *ClickHouseDriver) InitSchema() error {
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
			return fmt.Errorf("[ClickHouse] Missing implementation for view: %s", viewID)
		}
		if err := impl(); err != nil {
			return fmt.Errorf("failed to create view %s: %w", viewID, err)
		}
	}
	return nil
}
