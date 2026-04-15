package database

import (
	"time"

	"github.com/jing2uo/tdx2db/model"
)

type DataRepository interface {
	Connect() error
	Close() error

	InitSchema() error

	ImportKlineDaily(csvPath string) error
	ImportKline1Min(csvPath string) error
	ImportKline5Min(csvPath string) error
	ImportAdjustFactors(csvPath string) error
	ImportGBBQ(csvPath string) error
	ImportBasic(csvPath string) error
	ImportHolidays(csvPath string) error
	ImportBlocksInfo(csvPath string) error
	ImportBlocksMember(csvPath string) error

	TruncateTable(meta *model.TableMeta) error
	Query(table string, conditions map[string]interface{}, dest interface{}) error
	QueryKlineDaily(symbol string, startDate, endDate *time.Time) ([]model.KlineDay, error)
	GetLatestDate(tableName string, dateCol string) (time.Time, error)
	GetSymbolsByClass(class string) ([]string, error)
	RebuildSymbolClass() error
	CountKlineDaily() (int64, error)

	GetBasicsBySymbol(symbol string) ([]model.StockBasic, error)

	GetGbbq() ([]model.GbbqData, error)
}
