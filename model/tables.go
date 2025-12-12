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

	// === 核心改动：自动注册 ===
	registerTable(meta)

	return meta
}

// --- 结构体定义 (Schema) ---
type StockData struct {
	Symbol string    `col:"symbol" parquet:"symbol,dict"`
	Open   float64   `col:"open"   parquet:"open"`
	High   float64   `col:"high"   parquet:"high"`
	Low    float64   `col:"low"    parquet:"low"`
	Close  float64   `col:"close"  parquet:"close"`
	Amount float64   `col:"amount" parquet:"amount"`
	Volume int64     `col:"volume" parquet:"volume"`
	Date   time.Time `col:"date"   parquet:"date"      type:"date"`
}

type StockMinData struct {
	Symbol   string    `col:"symbol"   parquet:"symbol,dict"`
	Open     float64   `col:"open"     parquet:"open"`
	High     float64   `col:"high"     parquet:"high"`
	Low      float64   `col:"low"      parquet:"low"`
	Close    float64   `col:"close"    parquet:"close"`
	Amount   float64   `col:"amount"   parquet:"amount"`
	Volume   int64     `col:"volume"   parquet:"volume"`
	Datetime time.Time `col:"datetime" parquet:"datetime"    type:"datetime" `
}

type Factor struct {
	Symbol    string    `col:"symbol"      parquet:"symbol,dict"`
	Date      time.Time `col:"date"        parquet:"date"         type:"date"`
	Close     float64   `col:"close"       parquet:"close"`
	PreClose  float64   `col:"pre_close"   parquet:"pre_close"`
	QfqFactor float64   `col:"qfq_factor"  parquet:"qfq_factor"`
	HfqFactor float64   `col:"hfq_factor"  parquet:"hfq_factor"`
}

type GbbqData struct {
	Category int       `col:"category" parquet:"category"`
	Code     string    `col:"code"     parquet:"code,dict"`
	Date     time.Time `col:"date"     parquet:"date"    type:"date"`
	C1       float64   `col:"c1"       parquet:"c1"`
	C2       float64   `col:"c2"       parquet:"c2"`
	C3       float64   `col:"c3"       parquet:"c3"`
	C4       float64   `col:"c4"       parquet:"c4"`
}

type XdxrData struct {
	Code        string    `col:"code"`
	Date        time.Time `col:"date"         type:"date"`
	Fenhong     float64   `col:"fenhong"`
	Peigujia    float64   `col:"peigujia"`
	Songzhuangu float64   `col:"songzhuangu"`
	Peigu       float64   `col:"peigu"`
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
	[]string{"code", "date"},
)
