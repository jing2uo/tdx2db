# DATABASE LAYER

**Purpose:** DB abstraction with ClickHouse + DuckDB implementations

## STRUCTURE
```
./database/
├── interface.go        # DataRepository interface
├── factory.go          # Driver factory (scheme-based)
├── clickhouse/
│   ├── driver.go       # 连接 / 生命周期 / 结构体
│   ├── schema.go       # 建表 + 视图编排 + mapType
│   ├── meta.go         # _meta 表 I/O：schema 版本 r/w；规划中：run 审计日志
│   ├── import.go       # ImportCSV / Import* / TruncateTable / RebuildSymbolClass
│   ├── query.go        # 所有 Get* / Query / Count* (纯读)
│   ├── views_stock.go  # 3 个 stock 视图 builder + registerStockViews
│   └── views_etf.go    # 3 个 etf 视图 builder + registerETFViews
└── duckdb/
    └── (同上七文件)
```

## WHERE TO LOOK
| Task | Location |
|------|----------|
| Add DB backend | factory.go (URI scheme dispatch) |
| 改连接参数 / Connect / Close | `*/driver.go` |
| 改建表 / mapType / 加视图编排 | `*/schema.go` |
| _meta 表读写 / schema 版本 / 未来 run 审计 | `*/meta.go` |
| 改导入流程 / 加 Import* / Truncate | `*/import.go` |
| 加查询 / 调 query 性能 | `*/query.go` |
| 改 stock 视图 SQL | `*/views_stock.go` |
| 改 ETF 视图 SQL | `*/views_etf.go` |

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

**CH import (clickhouse/import.go):**
- HTTP POST to `/` endpoint
- Headers: `Content-Type: text/csv`
- Query param: `INSERT INTO table FORMAT CSVWithNames`
- Settings: `date_time_input_format=best_effort`, `session_timezone=Asia/Shanghai`

**DuckDB import (duckdb/import.go):**
- Uses `INSERT INTO ... SELECT * FROM read_csv(...)`

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
- `raw_*` - Imported data tables (raw_kline_daily, raw_basic_daily, raw_adjust_factor, raw_gbbq, raw_symbol_class, raw_holidays, raw_kline_1min)
- `v_*` - Views: v_{stock,etf}_{bfq,qfq,hfq} 6 个视图，按 class 过滤、stock 价格 ROUND 2 位 / etf 3 位
- `_meta` - Schema version and metadata (key/value)
- Tables auto-registered via `model.SchemaFromStruct()`, views via `model.DefineView()`
- View implementations are driver-specific：stock 视图在 `views_stock.go`、ETF 视图在 `views_etf.go`，分别通过 `registerStockViews()` / `registerETFViews()` 挂到 driver 的 `viewImpls` map，再由 `schema.go::InitSchema` 编排创建

**Incremental update flow:**
- `GetLatestDate()` → fetch delta from TDX → `Import*()` → Calculate factors

**Error handling:**
- Wrap driver errors with context (`failed to query X: %w`)
