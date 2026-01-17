# DATABASE LAYER

**Purpose:** DB abstraction with ClickHouse + DuckDB implementations

## STRUCTURE
```
./database/
├── interface.go   # DataRepository interface
├── factory.go     # Driver factory (scheme-based)
├── clickhouse/
│   ├── driver.go  # CH connection + config
│   ├── dml.go     # CH query + import (HTTP)
│   └── ddl.go     # CH schema
└── duckdb/
    ├── driver.go  # DuckDB connection
    ├── dml.go     # DuckDB query + import
    └── ddl.go     # DuckDB schema
```

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Add DB backend | factory.go | Parse URI scheme, return DataRepository |
| Implement interface | */driver.go | Implement all DataRepository methods |
| Change import method | */dml.go | CH uses HTTP POST, DuckDB uses file copy |
| Modify schema | */ddl.go | DDL for tables + views |

## CONVENTIONS

**URI parsing (factory.go):**
- Scheme determines driver: `clickhouse://` or `duckdb://`
- CH defaults: user=default, password="", port=9000, http_port=8123

**DataRepository interface:**
- `Connect()/Close()` - Lifecycle management
- `InitSchema()` - Create tables + views
- `Import*()` - CSV import methods
- `Query/Get*()` - Read operations

**CH import (clickhouse/dml.go):**
- HTTP POST to `/` endpoint
- Headers: `Content-Type: text/csv`
- Query param: `INSERT INTO table FORMAT CSVWithNames`
- Settings: `date_time_input_format=best_effort`, `session_timezone=Asia/Shanghai`

**DuckDB import:**
- Uses `COPY table FROM '/path' (AUTO_DETECT TRUE)`

**Query methods:**
- `Query()` - Generic query with conditions map
- `GetLatestDate()` - Used by cron for incremental updates
- `QueryStockData()` - Date range filtered OHLCV data

## ANTI-PATTERNS

**DO NOT:**
- Use TCP import for ClickHouse - must use HTTP POST
- Skip timezone settings - data dates are Asia/Shanghai
- Omit `CSVWithNames` format - CSV has header row

**NEVER:**
- Mix driver-specific code in interface - keep implementations separate
- Add new import method without adding to interface
- Hardcode table names - use `model.Table*` constants

## NOTES

**Table naming:**
- `raw_*` - Imported data tables
- `v_*` - Views (复权 calculations)
- Views defined in ddl.go files

**Incremental update flow:**
- `GetLatestDate()` → fetch delta from TDX → `Import*()` → Calculate factors

**Error handling:**
- Wrap driver errors with context (`failed to query X: %w`)
