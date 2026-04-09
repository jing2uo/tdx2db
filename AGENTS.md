# PROJECT KNOWLEDGE BASE

**Updated:** 2026-04-09
**Branch:** fix/calc_basic_fq

## OVERVIEW
通达信(TDX) stock data importer — loads .day/.1/.05 files to DuckDB/ClickHouse, calculates preclose/turnover/market-value (basic), and 后复权因子 (hfq_factor).

## STRUCTURE
```
./
├── calc/       # Financial calculations (basic indicators + 复权因子)
│   ├── basic.go           # preclose, turnover, floatmv, totalmv
│   ├── fq_quantaxis.go    # HFQ factor (QUANTAXIS-based)
│   └── fq_quantaxis_test.go
├── cmd/        # CLI commands (init, cron, convert)
├── database/   # DB interface + implementations (duckdb/clickhouse)
├── model/      # Data models, table registry, view registry
├── tdx/        # TDX binary format parsing
├── utils/      # Utilities (cache, pipeline, CSV, download)
├── workflow/   # Task execution framework (dependency resolution, DAG)
└── main.go     # Cobra CLI entry point
```

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Add new database backend | ./database/ | Implement DataRepository interface |
| Parse new TDX format | ./tdx/ | Binary format parsers (day, minline, blocks) |
| Modify basic calculation | ./calc/basic.go | preclose / turnover / market-value |
| Modify HFQ factor | ./calc/fq_quantaxis.go | 后复权因子算法 |
| Add CLI command | ./cmd/ + main.go | Cobra subcommand with ctx cancel support |
| Data model changes | ./model/ | Schema + struct tags + table/view registry |
| Database queries | ./database/*/dml.go | DB-specific query implementations |
| Add/modify workflow task | ./workflow/tasks.go | Define task with dependencies |
| Run specific tasks | ./workflow/engine.go | Use TaskExecutor with task names |

## CODE MAP
| Symbol | Type | Location | Role |
|--------|------|----------|------|
| main | func | main.go:37 | Cobra root + ctx setup |
| DataRepository | interface | database/interface.go:9 | DB abstraction (Connect/Import/Query) |
| NewDB | func | database/factory.go:11 | Driver factory (duckdb/clickhouse) |
| Task | type | workflow/engine.go:46 | Task definition with dependencies |
| TaskExecutor | type | workflow/engine.go:66 | DAG-based task execution |
| Init | func | cmd/init.go | Full import via workflow |
| Cron | func | cmd/cron.go:11 | Incremental update via workflow |
| Convert | func | cmd/convert.go | TDX to CSV conversion |
| CalculateStockBasic | func | calc/basic.go:111 | Core basic calculation (preclose/turnover/MV) |
| calculateFullHfq | func | calc/fq_quantaxis.go:97 | Core HFQ factor calculation |
| buildXdxrDateSet | func | calc/fq_quantaxis.go:130 | Map gbbq dates → trading days for HFQ |
| StockData | type | model/schema.go:5 | Raw daily OHLCV |
| StockBasic | type | model/schema.go:33 | Calculated basic (preclose/turnover/MV) |
| Factor | type | model/schema.go:27 | Adjust factor (hfq_factor) |
| GbbqData | type | model/schema.go:45 | 股本变迁 data (category 1=除权, 2/3/5/7/8/9/10=股本变动) |
| SchemaFromStruct | func | model/tables.go:54 | Reflect-based table registration |

## CONVENTIONS

**Database URI format:**
- ClickHouse: `clickhouse://[user[:password]@][host][:port][/database][?http_port=8123]`
- DuckDB: `duckdb://[path]`

**Table naming:**
- `raw_*` — raw imported data (raw_stocks_daily, raw_stocks_basic, raw_adjust_factor, raw_gbbq, etc.)
- `v_*` — views (v_bfq_daily, v_qfq_daily, v_hfq_daily)

**Table registration:**
- All tables auto-registered via `SchemaFromStruct()` init-time calls in `model/tables.go`
- Views registered via `DefineView()` in `model/views.go`
- Use `model.Table*` constants for table references (never hardcode table names)

**TDX file prefixes (cmd/common.go):**
- Market: `sz30`, `sz00`, `sh60`, `sh68`, `bj920`
- Index: `sh000300`, `sh000905`, `sz399001`, etc.
- Block: `sh880` (concept/style), `sh881` (industry)

**GbbqData categories:**
- Category 1: 除权除息 (dividends/bonus shares) — C1=分红, C2=配股, C3=送转股, C4=配股价
- Category 2/3/5/7/8/9/10: 股本变动 — C1=变动前流通, C2=变动前总, C3=变动后流通, C4=变动后总
- Stock units in gbbq are 万股 (×10000 = actual shares)

**Error handling:**
- Wrap errors with `%w` for error chain
- Context cancellation returns `ctx.Err()` directly
- CLI exits 0 on context cancel, 1 on error

**CLI pattern:**
- Cobra for commands + flags
- Context passed to all long-running ops
- Signal handler (SIGINT/SIGTERM) → ctx cancel → safe exit
- Temp dir: `$TMPDIR/tdx2db-temp-*`

## CALCULATION LOGIC

### calc/basic.go — CalculateStockBasic
Input: `[]StockData` (raw daily) + `[]GbbqData` → Output: `[]StockBasic`

1. **xdxr date mapping** (category=1): gbbq date → trading day via `sort.Search` (handles non-trading days)
2. **Shares tracking** (category 2/3/5/7/8/9/10): maintains running float/total share counts
3. **Initial share backfill**: if first gbbq share record is after IPO date, uses its C1/C2 (pre-change values) to backfill the gap
4. **Per-day**: preclose (adjusted for xdxr), change_pct, amplitude, turnover (vol/float_shares), floatmv, totalmv

**PreClose formula** (with xdxr):
```
preclose = (prevClose×10 - 分红 + 配股×配股价) / (10 + 配股 + 送转股)
```

### calc/fq_quantaxis.go — calculateFullHfq
Input: `[]StockBasic` + `[]GbbqData` → Output: `[]Factor`

1. **buildXdxrDateSet**: maps category=1 gbbq dates to trading days via `sort.Search` (same logic as basic.go)
2. **Factor accumulation**: starts at 1.0, on xdxr days: `hfq *= prevClose / preclose`
3. Factor only changes when ratio ≠ 1.0 (uses 1e-9 tolerance)

**Critical invariant**: basic.go and fq_quantaxis.go MUST use the same date mapping logic (gbbq date → trading day). Both use `sort.Search` to find first trading day ≥ gbbq date.

## ANTI-PATTERNS (THIS PROJECT)

**DO NOT:**
- Remove `context.CancelFunc` defer — required for signal handling
- Ignore `ctx.Done()` checks in loops — prevents graceful shutdown
- Use hardcoded paths — all paths use TempDir cache
- Use raw gbbq dates for matching against trading days — always map via sort.Search
- Assume gbbq dates are trading days — they can fall on weekends/holidays

**NEVER:**
- Commit `tdx/embed/datatool` to git — downloaded at build time
- Import `_ "github.com/duckdb/duckdb-go/v2"` outside init package — register driver early
- Break the date mapping consistency between basic.go and fq_quantaxis.go

## UNIQUE STYLES

**Task-based workflow (workflow/):**
- `TaskExecutor` manages task execution with dependency resolution (DAG topological sort)
- Tasks defined in `workflow/tasks.go` with explicit `DependsOn` arrays
- Parallel execution of tasks with no dependencies
- Optional tasks via `SkipIf` condition (e.g., `--minline`, `--tdxhome`)
- Error modes: `ErrorModeStop` (default) vs `ErrorModeSkip`

**Incremental update logic:**
- `cron` command uses workflow tasks with dependency: `update_daily → update_gbbq → calc_basic → calc_factor`
- `calc_basic` and `calc_factor` run in **full recalculation mode** (truncate + reimport)
- Each update task checks latest date → fetches delta from TDX
- Supports 1min/5min incremental import (optional tasks)

**CSV pipeline pattern:**
- All calculation exports use `utils.Pipeline[I,O]` for concurrent per-symbol processing
- TDX files → convert to CSV → temp dir → DB import
- Temp dir cleaned up on `cobra.OnFinalize`

## COMMANDS
```bash
# Build (downloads datatool)
make build

# Install
make user-install    # ~/.local/bin
make sudo-install    # /usr/local/bin

# Clean
make clean

# GoReleaser release
goreleaser release --clean

# Daily update (with ClickHouse)
tdx2db cron --dburi 'clickhouse://localhost'

# Full init from TDX day files
tdx2db init --dburi 'clickhouse://localhost' --dayfiledir /path/to/vipdoc/
```

## NOTES

**Gotchas:**
- 复权因子算法 based on QUANTAXIS — verify before modifying
- 分时数据无历史 — need to backfill manually
- Symbol code changes not handled (历史记录不更新)
- Indices (sh000xxx, sz399xxx, sh880/881xxx) have no float shares → turnover/floatmv = 0, this is expected
