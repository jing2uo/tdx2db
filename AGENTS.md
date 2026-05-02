# PROJECT KNOWLEDGE BASE

**Updated:** 2026-05-02
**Branch:** main

## OVERVIEW
通达信(TDX) stock data importer — loads .day/.01/.5 files to DuckDB/ClickHouse, calculates preclose/turnover/market-value (basic), and 后复权因子 (hfq_factor). basic/factor 链路覆盖 stock + ETF (含 LOF/老封基/B股)。

Current schema version: **v3.0** (`model.SchemaMajor=3, SchemaMinor=0`). The `_meta` table stores the schema version; init/cron check major version compatibility at startup.

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
├── workflow/   # Task execution framework (DAG, calendar, work plan)
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
| CalculateBasicDaily | func | calc/basic.go:115 | Core basic calculation (preclose/turnover/MV, stock+ETF) |
| calculateFullHfq | func | calc/fq_quantaxis.go:86 | Core HFQ factor calculation |
| BuildWorkPlan | func | workflow/plan.go:34 | 读 holidays + 各表最新日期，决定哪些任务要跑 |
| TradingCalendar | type | workflow/calendar.go:5 | 节假日/周末判定 + 最近交易日查找 |
| KlineDay | type | model/schema.go:14 | Raw daily OHLCV |
| BasicDaily | type | model/schema.go:49 | Calculated basic (preclose/turnover/MV, stock+ETF) |
| Factor | type | model/schema.go:41 | Adjust factor (hfq_factor) |
| GbbqData | type | model/schema.go:61 | 股本变迁 data (cat 1=除权, 11=ETF份额折算, 2/3/5/7/8/9/10=股本变动) |
| ClassifyCode | func | model/classify.go:58 | symbol → stock/index/etf/block/unknown |
| PriceScale | func | model/classify.go:89 | TDX 原始整数价格换算 (股票=100, ETF/B股=1000) |
| SchemaFromStruct | func | model/tables.go:54 | Reflect-based table registration |

## CONVENTIONS

**Database URI format:**
- ClickHouse: `clickhouse://[user[:password]@][host][:port][/database][?http_port=8123]`
- DuckDB: `duckdb://[path]`

**Table naming:**
- `raw_*` — raw imported data (raw_kline_daily, raw_kline_1min, raw_kline_5min, raw_basic_daily, raw_adjust_factor, raw_gbbq, raw_symbol_class, raw_holidays)
- `v_*` — views (v_bfq_daily, v_qfq_daily, v_hfq_daily) — 通过 `raw_symbol_class` 过滤，仅保留 stock + etf
- `_meta` — schema version metadata (key/value store)

**Table registration:**
- All tables auto-registered via `SchemaFromStruct()` init-time calls in `model/tables.go`
- Views registered via `DefineView()` in `model/views.go`
- Use `model.Table*` / `model.MetaTable` constants for table references (never hardcode table names)

**TDX file collection (cmd/common.go):**
- All .day/.01/.5 files collected by suffix, filtered only by `^(sh|sz|bj)\d+$` regex
- No prefix whitelist — full ingest of everything TDX provides
- Symbol classification via `raw_symbol_class` table (rebuilt after each daily import)

**Symbol classification (model/classify.go):**
- `ClassifyCode(symbol) → stock/index/etf/block/unknown`
- Rules match by (market, numeric prefix) — longest prefix wins
- basic/factor calculation uses `GetSymbolsByClass("stock", "etf")` — index/block 仍排除在 calc 输出之外
- 复权视图 (`v_*fq_daily`) 通过 `raw_symbol_class` 过滤，仅保留 stock + etf
- B 股 (sh900/sz20) 归为 stock；ETF/LOF 归为 etf
- `PriceScale(symbol)`：股票/指数/板块 价格单位 0.01 元(=100)，ETF/LOF/B股 0.001 元(=1000)
- `SymbolFromCode(code)`：6 位裸数字反查市场前缀（仅考虑 stock/etf）
- Class `unknown` 包括：可转债 (sh11xxxx, sz12xxxx, sz13xxxx), 国债 (sh24xxxx) 等

**Schema versioning (cmd/schema_version.go):**
- `model.SchemaMajor` / `model.SchemaMinor` define current version
- DB stores version in `_meta` table via `ReadSchemaVersion()` / `WriteSchemaVersion()`
- `init`: auto-writes version on fresh DB; rejects if existing major doesn't match
- `cron`: rejects if version missing or major doesn't match
- Breaking changes (table rename, field semantics) → increment `SchemaMajor`

**GbbqData categories:**
- Category 1: 除权除息 (dividends/bonus shares) — C1=分红, C2=配股, C3=送转股, C4=配股价
- Category 11: ETF/LOF 份额折算 — C3=折算系数 (拆分日 PreClose 需除以该系数；多次折算同日累乘)
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

### calc/basic.go — CalculateBasicDaily
Input: `[]KlineDay` (raw daily) + `[]GbbqData` → Output: `[]*BasicDaily`

1. **xdxr date mapping** (category=1, 11): gbbq date → KlineDay 索引 via `sort.Search` (handles non-trading days)
2. **ETF cat=11 split**: `mergeSplitFromGbbq` 累乘 Splitc3，PreClose 末尾再除一次
3. **Shares tracking** (category 2/3/5/7/8/9/10): maintains running float/total share counts (ETF 一般无此类记录)
4. **Initial share backfill**: if first gbbq share record is after IPO date, uses its C1/C2 (pre-change values) to backfill the gap
5. **Per-day**: preclose (adjusted for xdxr/split), change_pct, amplitude, turnover (vol/float_shares), floatmv, totalmv

**PreClose formula** (with xdxr + split):
```
preclose = ((prevClose×10 - 分红 + 配股×配股价) / (10 + 配股 + 送转股)) / Splitc3
```
Splitc3 默认 1（无 cat=11）；cat=1 与 cat=11 同日时两者都生效。

### calc/fq_quantaxis.go — calculateFullHfq
Input: `[]BasicDaily` + `[]GbbqData` → Output: `[]Factor`

1. 直接消费 BasicDaily 中已计算好的 PreClose（不再单独 buildXdxrDateSet）
2. **Factor accumulation**: starts at 1.0, on xdxr days: `hfq *= prevClose / preclose`
3. Factor only changes when ratio ≠ 1.0 (uses 1e-9 tolerance)

**Critical invariant**: basic.go 是 PreClose 唯一计算源。fq_quantaxis.go 直接读 BasicDaily.PreClose，所以 basic.go 的日期映射 + cat=1/cat=11 处理是单一事实源，不需要在 fq 里重复。

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
- 在 fq_quantaxis 里再单独做 xdxr 日期映射 — 必须复用 basic.go 写出的 PreClose
- Put version-check logic in the DB layer — DB only does Read/Write; judgment stays in cmd/

## UNIQUE STYLES

**Task-based workflow (workflow/):**
- `TaskExecutor` (engine.go) manages 任务执行 + DAG 拓扑排序
- `WorkPlan` (plan.go) 在任务图启动前汇总日历 + 各表最新日期，决定 NeedDaily/NeedGbbq/NeedBasic/NeedFactor/NeedHolidays；任务的 `SkipIf` 只读 plan，不再各自查询数据库
- `TradingCalendar` (calendar.go) 用 raw_holidays + 周末规则识别交易日，下载日线/分时遇到 404 时区分"节假日跳过"/"数据未发布"
- Tasks defined in `workflow/tasks.go` with explicit `DependsOn`
- Parallel execution of tasks with no dependencies
- Optional tasks via `SkipIf` condition (e.g., `--minline`)
- Error modes: `ErrorModeStop` (default) vs `ErrorModeSkip`

**Incremental update logic:**
- `cron` command 先 `BuildWorkPlan(db, today)` → 全 `Skip` 则直接退出，否则跑 DAG: `update_daily → update_gbbq → calc_basic → calc_factor`，并行 `update_holidays`
- raw_holidays 为空（首次/旧库）时 plan 强制走全流程，让 holidays 自行写入
- `calc_basic` 和 `calc_factor` 永远 truncate + 重算（依赖完整历史）
- 1min/5min 增量导入由 `--minline` 控制（可选任务）

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
- 指数/板块 (sh000xxx, sz399xxx, sh880/881xxx) 不在 calc 输出范围内（GetSymbolsByClass 只取 stock + etf）
- ETF/LOF/B股 K线价格按 0.001 元解析 (`PriceScale`)，否则有 10x 偏差；新增品种前缀时务必检查
- ETF 拆分日 (cat=11) PreClose 必须除以 Splitc3，否则当天涨跌幅会异常
- `raw_symbol_class` is rebuilt from `raw_kline_daily` on each daily import — adding classification rules retroactively will auto-classify on next import
- holidays 来自 gbbq.zip 内嵌的 zhb.zip (needini.dat)，不再依赖 tdxhome 安装目录
