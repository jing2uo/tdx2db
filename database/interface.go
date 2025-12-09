package database

import (
	"time"

	"github.com/jing2uo/tdx2db/model"
)

type DataRepository interface {
	Connect() error
	Close() error

	InitSchema() error

	ImportDailyStocks(csvPath string) error
	Import1MinStocks(csvPath string) error
	Import5MinStocks(csvPath string) error
	ImportAdjustFactors(csvPath string) error
	ImportGBBQ(csvPath string) error

	GetLatestDate(tableName string, dateCol string) (time.Time, error)
	Query(table string, conditions map[string]interface{}, dest interface{}) error
	GetAllSymbols() ([]string, error)
	QueryAllXdxr() ([]model.XdxrData, error)
	QueryStockData(symbol string, startDate, endDate *time.Time) ([]model.StockData, error)
}
