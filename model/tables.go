package model

import (
	"reflect"
	"strings"
	"sync"
	"time"
)

type DataType int

const (
	TypeString DataType = iota
	TypeFloat64
	TypeInt64
	TypeDate     // YYYY-MM-DD
	TypeDateTime // YYYY-MM-DD HH:MM:SS
)

type Column struct {
	Name string
	Type DataType
}

type TableMeta struct {
	TableName  string
	Columns    []Column
	OrderByKey []string
}

var (
	tableRegistry   []*TableMeta
	tableRegistryMu sync.Mutex
)

func registerTable(t *TableMeta) {
	tableRegistryMu.Lock()
	defer tableRegistryMu.Unlock()
	tableRegistry = append(tableRegistry, t)
}

// AllTables 返回当前所有已注册的表结构
func AllTables() []*TableMeta {
	tableRegistryMu.Lock()
	defer tableRegistryMu.Unlock()

	result := make([]*TableMeta, len(tableRegistry))
	copy(result, tableRegistry)
	return result
}

// SchemaFromStruct 通过反射生成 TableMeta 并自动注册
// 返回值为指针类型 *TableMeta
func SchemaFromStruct(tableName string, model interface{}, orderByKey []string) *TableMeta {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	var cols []Column

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// 1. 获取列名
		colName := field.Tag.Get("col")
		if colName == "" {
			colName = strings.ToLower(field.Name)
		}

		// 2. 推断类型 (保持原有逻辑)
		var dType DataType
		customType := field.Tag.Get("type")
		switch {
		case customType == "date":
			dType = TypeDate
		case customType == "datetime":
			dType = TypeDateTime
		default:
			switch field.Type.Kind() {
			case reflect.String:
				dType = TypeString
			case reflect.Float64, reflect.Float32:
				dType = TypeFloat64
			case reflect.Int, reflect.Int64, reflect.Int32, reflect.Uint32:
				dType = TypeInt64
			case reflect.Struct:
				if field.Type == reflect.TypeOf(time.Time{}) {
					dType = TypeDateTime
				}
			default:
				dType = TypeString
			}
		}

		cols = append(cols, Column{Name: colName, Type: dType})
	}

	meta := &TableMeta{
		TableName:  tableName,
		Columns:    cols,
		OrderByKey: orderByKey,
	}

	registerTable(meta)

	return meta
}

// --- 表结构元数据 (TableMeta) ---

var TableStocksDaily = SchemaFromStruct(
	"raw_stocks_daily",
	StockData{},
	[]string{"symbol", "date"},
)

var TableStocks1Min = SchemaFromStruct(
	"raw_stocks_1min",
	StockMinData{},
	[]string{"symbol", "datetime"},
)

var TableStocks5Min = SchemaFromStruct(
	"raw_stocks_5min",
	StockMinData{},
	[]string{"symbol", "datetime"},
)

var TableAdjustFactor = SchemaFromStruct(
	"raw_adjust_factor",
	Factor{},
	[]string{"symbol", "date"},
)

var TableGbbq = SchemaFromStruct(
	"raw_gbbq",
	GbbqData{},
	[]string{"symbol", "date"},
)

var TableBasic = SchemaFromStruct(
	"raw_stocks_basic",
	StockBasic{},
	[]string{"symbol", "date"},
)

var TableHoliday = SchemaFromStruct(
	"raw_holidays",
	Holiday{},
	[]string{""},
)

var TableBlockInfo = SchemaFromStruct(
	"raw_tdx_blocks_info",
	BlockInfo{},
	[]string{""},
)

var TableBlockMember = SchemaFromStruct(
	"raw_tdx_blocks_member",
	BlockMember{},
	[]string{""},
)
