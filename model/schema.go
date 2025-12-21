package model

import "time"

type StockData struct {
	Symbol string    `col:"symbol"`
	Open   float64   `col:"open"`
	High   float64   `col:"high"`
	Low    float64   `col:"low"`
	Close  float64   `col:"close"`
	Amount float64   `col:"amount"`
	Volume int64     `col:"volume"`
	Date   time.Time `col:"date" type:"date"`
}

type StockMinData struct {
	Symbol   string    `col:"symbol"`
	Open     float64   `col:"open"`
	High     float64   `col:"high"`
	Low      float64   `col:"low"`
	Close    float64   `col:"close"`
	Amount   float64   `col:"amount"`
	Volume   int64     `col:"volume"`
	Datetime time.Time `col:"datetime" type:"datetime" `
}

type Factor struct {
	Symbol    string    `col:"symbol"`
	Date      time.Time `col:"date" type:"date"`
	QfqFactor float64   `col:"qfq_factor"`
	HfqFactor float64   `col:"hfq_factor"`
}

type StockBasic struct {
	Date     time.Time `col:"date" type:"date"`
	Symbol   string    `col:"symbol"`
	Close    float64   `col:"close"`
	PreClose float64   `col:"preclose"`
	Turnover float64   `col:"turnover"`
	FloatMV  float64   `col:"floatmv"`
	TotalMV  float64   `col:"totalmv"`
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
