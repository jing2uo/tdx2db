package model

import "time"

// SchemaMajor 表示数据库 schema 的主版本号。
// 当发生破坏性变更（表重命名、字段语义变化等）时递增。
// 已安装的数据库 major 版本与当前代码不匹配时，工具将拒绝操作并提示用户查看文档。
const SchemaMajor = 3

// SchemaMinor 表示数据库 schema 的次版本号。
// 当发生非破坏性变更（新增表、新增字段等）时递增。
const SchemaMinor = 0

type KlineDay struct {
	Symbol string    `col:"symbol"`
	Open   float64   `col:"open"`
	High   float64   `col:"high"`
	Low    float64   `col:"low"`
	Close  float64   `col:"close"`
	Amount float64   `col:"amount"`
	Volume int64     `col:"volume"`
	Date   time.Time `col:"date" type:"date"`
}

type KlineMin struct {
	Symbol   string    `col:"symbol"`
	Open     float64   `col:"open"`
	High     float64   `col:"high"`
	Low      float64   `col:"low"`
	Close    float64   `col:"close"`
	Amount   float64   `col:"amount"`
	Volume   int64     `col:"volume"`
	Datetime time.Time `col:"datetime" type:"datetime" `
}

type SymbolClass struct {
	Symbol string `col:"symbol"`
	Class  string `col:"class"`
}

type Factor struct {
	Symbol    string    `col:"symbol"`
	Date      time.Time `col:"date" type:"date"`
	HfqFactor float64   `col:"hfq_factor"`
}

type StockBasic struct {
	Date          time.Time `col:"date" type:"date"`
	Symbol        string    `col:"symbol"`
	Close         float64   `col:"close"`
	PreClose      float64   `col:"preclose"`
	ChangePercent float64   `col:"change_pct"`
	Amplitude     float64   `col:"amplitude"`
	Turnover      float64   `col:"turnover"`
	FloatMV       float64   `col:"floatmv"`
	TotalMV       float64   `col:"totalmv"`
}

type GbbqData struct {
	Category int       `col:"category"`
	Symbol   string    `col:"symbol"`
	Date     time.Time `col:"date" type:"date"`
	C1       float64   `col:"c1"`
	C2       float64   `col:"c2"`
	C3       float64   `col:"c3"`
	C4       float64   `col:"c4"`
}

type Holiday struct {
	Date time.Time `col:"date" type:"date"`
}

type BlockMember struct {
	StockSymbol string `col:"stock_symbol"`
	BlockCode   string `col:"block_code"`
}

type Meta struct {
	Key   string `col:"key"`
	Value string `col:"value"`
}

type BlockInfo struct {
	BlockType   string `col:"block_type"`
	BlockName   string `col:"block_name"`
	BlockSymbol string `col:"block_symbol"`
	BlockCode   string `col:"block_code"`
	ParentCode  string `col:"parent_code"`
	BlockLevel  int    `col:"block_level"`
}
