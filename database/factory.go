package database

import (
	"fmt"
	"net/url"

	"github.com/jing2uo/tdx2db/database/clickhouse"
	"github.com/jing2uo/tdx2db/database/duckdb"
)

func NewDB(dbURI string) (DataRepository, error) {
	if dbURI == "" {
		return nil, fmt.Errorf("db uri cannot be empty")
	}
	u, err := url.Parse(dbURI)
	if err != nil {
		return nil, err
	}

	switch u.Scheme {
	case "clickhouse":
		return clickhouse.NewClickHouseDriver(u)
	case "duckdb":
		return duckdb.NewDuckDBDriver(u)
	default:
		return nil, fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}
}
