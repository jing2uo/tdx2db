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

**DataRepository interface (interface.go):**
- `Connect()/Close()` - Lifecycle management
- `InitSchema()` - Create tables + views (auto-registered via model package)
- `ReadSchemaVersion()/WriteSchemaVersion()` - _meta table schema version read/write (no judgment logic)
- `Import*()` - CSV import: KlineDaily, Kline1Min, Kline5Min, AdjustFactors, GBBQ, Basic, Holidays
- `TruncateTable(meta)` - Clear table (used by full-recalc tasks)
- `Query()` - Generic query with conditions map
- `QueryKlineDaily()` - Date range filtered OHLCV data
- `GetLatestDate()` - Used by cron for incremental updates
- `GetSymbolsByClass(classes ...)` - Symbols filtered by class (calc 端传 stock + etf)
- `RebuildSymbolClass()` - Rebuild symbol_class table from raw_kline_daily
- `GetBasicsBySymbol()` - BasicDaily data (含 PreClose) for factor calculation
- `GetGbbq()` - All gbbq records (loaded once, indexed in calc)
- `GetHolidays()` - 全量节假日 (workflow.BuildWorkPlan 启动时读取)

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
- `QueryKlineDaily()` - Date range filtered OHLCV data

## ANTI-PATTERNS

**DO NOT:**
- Use TCP import for ClickHouse - must use HTTP POST
- Skip timezone settings - data dates are Asia/Shanghai
- Omit `CSVWithNames` format - CSV has header row

**NEVER:**
- Mix driver-specific code in interface - keep implementations separate
- Add new import method without adding to interface
- Hardcode table names - use `model.Table*` / `model.MetaTable` constants

## NOTES

**Table naming:**
- `raw_*` - Imported data tables (raw_kline_daily, raw_basic_daily, raw_adjust_factor, raw_gbbq, raw_symbol_class, raw_holidays, raw_kline_1min, raw_kline_5min)
- `v_*` - Views (v_bfq_daily, v_qfq_daily, v_hfq_daily) - join `raw_symbol_class` 过滤 `class IN ('stock','etf')`
- `_meta` - Schema version and metadata (key/value)
- Tables auto-registered via `model.SchemaFromStruct()`, views via `model.DefineView()`
- View implementations are driver-specific (registered in ddl.go via `registerViews()`)

**Incremental update flow:**
- `GetLatestDate()` → fetch delta from TDX → `Import*()` → Calculate factors

**Error handling:**
- Wrap driver errors with context (`failed to query X: %w`)
