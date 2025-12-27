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
	ImportBasic(csvPath string) error
	ImportStocksInfo(csvPath string) error
	ImportHolidays(csvPath string) error
	ImportBlocksInfo(csvPath string) error
	ImportBlocksMember(csvPath string) error

	TruncateTable(meta *model.TableMeta) error
	Query(table string, conditions map[string]interface{}, dest interface{}) error
	QueryStockData(symbol string, startDate, endDate *time.Time) ([]model.StockData, error)
	GetLatestDate(tableName string, dateCol string) (time.Time, error)
	GetAllSymbols() ([]string, error)

	GetBasicsBySymbol(symbol string) ([]model.StockBasic, error)
	GetLatestBasicBySymbol(symbol string) ([]model.StockBasic, error)
	GetBasicsSince(sinceDate time.Time) ([]model.StockBasic, error)

	GetGbbq() ([]model.GbbqData, error)
	GetLatestFactors() ([]model.Factor, error)
}
