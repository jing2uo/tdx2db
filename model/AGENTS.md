# DATA MODELS

**Purpose:** Schema definitions, table registry, and view registry

## STRUCTURE
```
./model/
├── classify.go  # Symbol classification (ClassifyCode → stock/index/etf/block/unknown)
├── schema.go    # Data structs (KlineDay, KlineMin, StockBasic, Factor, GbbqData, Meta, etc.)
├── tables.go    # Table metadata registry (SchemaFromStruct)
└── views.go     # View ID registry (DefineView)
```

## WHERE TO LOOK
| Task | File | Notes |
|------|------|-------|
| Add/modify data struct | schema.go | Use `col:"name"` + optional `type:"date"` tags |
| Register a new table | tables.go | Call `SchemaFromStruct(tableName, struct{}, orderByKey)` |
| Register a new view | views.go | Call `DefineView("v_name")` |
| Change column mapping | schema.go | `col` tag = DB column name, `type` tag = date/datetime hint |

## DATA STRUCTS

| Struct | Table | Description |
|--------|-------|-------------|
| KlineDay | raw_kline_daily | Raw OHLCV + date |
| KlineMin | raw_kline_1min / raw_kline_5min | Minute OHLCV + datetime |
| SymbolClass | raw_symbol_class | Symbol → class mapping (stock/index/etf/block/unknown) |
| StockBasic | raw_stocks_basic | Calculated: preclose, change_pct, amplitude, turnover, floatmv, totalmv |
| Factor | raw_adjust_factor | HFQ factor per symbol per date |
| GbbqData | raw_gbbq | 股本变迁: category + C1-C4 |
| Meta | _meta | Key-value metadata (schema version, etc.) |
| Holiday | raw_holidays | Holiday dates |

## CONVENTIONS

**Struct tags:**
- `col:"name"` — DB column name (required on all fields)
- `type:"date"` — Date-only formatting (YYYY-MM-DD)
- `type:"datetime"` — Datetime formatting (YYYY-MM-DD HH:MM:SS)
- Tags used by CSV writer, DB DDL generation, and query mapping

**Table registration (tables.go):**
- `SchemaFromStruct()` uses reflection to build `TableMeta` from struct tags
- Auto-registers into global `tableRegistry` at init time
- `AllTables()` returns all registered tables for `InitSchema()`
- `OrderByKey` defines the table's sort key (e.g., `["symbol", "date"]`)

**View registration (views.go):**
- `DefineView()` registers view ID into global `viewRegistry`
- View SQL is driver-specific (implemented in database/*/ddl.go)
- Three views: v_bfq_daily (不复权), v_qfq_daily (前复权), v_hfq_daily (后复权)

## ANTI-PATTERNS

**DO NOT:**
- Hardcode table names — use `model.Table*` variables
- Add fields without `col` tag — breaks CSV/DB mapping
- Forget to register new tables via `SchemaFromStruct()`

**NEVER:**
- Change `col` tag names without updating DB schema — breaks existing data
- Remove auto-registration in package init — tables must register before `InitSchema()`
