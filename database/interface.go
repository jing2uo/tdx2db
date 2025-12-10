package database

import (
	"time"

	"github.com/jing2uo/tdx2db/model"
)

type DataRepository interface {
	Connect() error
	Close() error

	InitSchema() error

	ImportDailyStocks(parquetPath string) error
	Import1MinStocks(parquetPath string) error
	Import5MinStocks(parquetPath string) error
	ImportAdjustFactors(parquetPath string) error
	ImportGBBQ(parquetPath string) error

	GetLatestDate(tableName string, dateCol string) (time.Time, error)
	Query(table string, conditions map[string]interface{}, dest interface{}) error
	GetAllSymbols() ([]string, error)
	QueryAllXdxr() ([]model.XdxrData, error)
	QueryStockData(symbol string, startDate, endDate *time.Time) ([]model.StockData, error)
}
