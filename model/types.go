package model

import "time"

type DBConfig struct {
	Path string
}

type DayfileRecord struct {
	Date   uint32
	Open   uint32
	High   uint32
	Low    uint32
	Close  uint32
	Amount float32
	Volume uint32
}

type StockData struct {
	Symbol string
	Open   float64
	High   float64
	Low    float64
	Close  float64
	Amount float64
	Volume int64
	Date   time.Time
}

type Factor struct {
	Symbol   string
	Date     time.Time
	Close    float64
	PreClose float64
	Factor   float64
}

type GbbqData struct {
	Category    int
	Code        string
	Date        time.Time
	Fenhong     float64
	Peigujia    float64
	Songzhuangu float64
	Peigu       float64
}
