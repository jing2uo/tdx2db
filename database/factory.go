package database

import (
	"fmt"

	"github.com/jing2uo/tdx2db/database/duckdb"
	"github.com/jing2uo/tdx2db/model"
)

func NewDatabase(cfg model.DBConfig) (DataRepository, error) {
	switch cfg.Type {
	case model.DBTypeDuckDB:
		return duckdb.NewDriver(cfg), nil
	// case model.DBTypeClickHouse:
	//    return clickhouse.NewDriver(cfg), nil
	default:
		return nil, fmt.Errorf("unsupported db type: %s", cfg.Type)
	}
}
