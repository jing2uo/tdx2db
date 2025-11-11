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

type MinfileRecord struct {
	DateRaw  uint16
	TimeRaw  uint16
	Open     uint32
	High     uint32
	Low      uint32
	Close    uint32
	Amount   float32
	Volume   uint32
	Reserved uint32
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
	Category int
	Code     string
	Date     time.Time
	C1       float64
	C2       float64
	C3       float64
	C4       float64
}

type XdxrData struct {
	Code        string
	Date        time.Time
	Fenhong     float64
	Peigujia    float64
	Songzhuangu float64
	Peigu       float64
}

type CapitalData struct {
	Code            string
	Date            time.Time
	PrevOutstanding float64
	PrevTotal       float64
	Outstanding     float64
	Total           float64
}
