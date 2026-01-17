# PROJECT KNOWLEDGE BASE

**Generated:** 2026-01-16
**Commit:** 48e5ce6
**Branch:** main

## OVERVIEW
通达信(TDX) stock data importer - loads .day/.1/.05 files to DuckDB/ClickHouse with adjust factor calculation.

## STRUCTURE
```
./
├── calc/       # Financial calculations (复权因子计算)
├── cmd/        # CLI commands (init, cron, convert)
├── database/   # DB interface + implementations (duckdb/clickhouse)
├── model/      # Data models & schemas
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
| Modify calculation logic | ./calc/ | 复权因子算法 (QUANTAXIS-based) |
| Add CLI command | ./cmd/ + main.go | Cobra subcommand with ctx cancel support |
| Data model changes | ./model/ | Schema + struct tags for DB mapping |
| Database queries | ./database/*/dml.go | DB-specific query implementations |
| Add/modify workflow task | ./workflow/tasks.go | Define task with dependencies |
| Run specific tasks | ./workflow/engine.go | Use TaskExecutor with task names |

## CODE MAP
| Symbol | Type | Location | Role |
|--------|------|----------|------|
| main | func | main.go:37 | Cobra root + ctx setup |
| DataRepository | interface | database/interface.go:9 | DB abstraction (Connect/Import/Query) |
| NewDB | func | database/factory.go:11 | Driver factory (duckdb/clickhouse) |
| Task | type | workflow/engine.go:47 | Task definition with dependencies |
| TaskExecutor | type | workflow/engine.go:68 | Task execution framework |
| Init | func | cmd/init.go:14 | Full import via workflow |
| Cron | func | cmd/cron.go:12 | Incremental update via workflow |
| Convert | func | cmd/convert.go:17 | TDX to CSV conversion |
| StockData | type | model/schema.go:5 | Daily OHLCV + date |
| StockMinData | type | model/schema.go:16 | Minute OHLCV + datetime |
| Factor | type | model/schema.go:27 | Adjust factor (hfq_factor) |
| GbbqData | type | model/schema.go:45 | 股本变迁 data |

## CONVENTIONS

**Database URI format:**
- ClickHouse: `clickhouse://[user[:password]@][host][:port][/database][?http_port=8123]`
- DuckDB: `duckdb://[path]`

**TDX file prefixes (cmd/common.go):**
- Market: `sz30`, `sz00`, `sh60`, `sh68`, `bj920`
- Index: `sh000300`, `sh000905`, `sz399001`, etc.
- Block: `sh880` (concept/style), `sh881` (industry)

**Error handling:**
- Wrap errors with `%w` for error chain
- Context cancellation returns `ctx.Err()` directly
- CLI exits 0 on context cancel, 1 on error

**CLI pattern:**
- Cobra for commands + flags
- Context passed to all long-running ops
- Signal handler (SIGINT/SIGTERM) → ctx cancel → safe exit
- Temp dir: `$TMPDIR/tdx2db-temp-*`

## ANTI-PATTERNS (THIS PROJECT)

**DO NOT:**
- Remove `context.CancelFunc` defer - required for signal handling
- Ignore `ctx.Done()` checks in loops - prevents graceful shutdown
- Skip unrar check before build - `make build` requires unrar
- Modify datatool binary - downloaded from TDX official URL
- Use hardcoded paths - all paths use TempDir cache

**NEVER:**
- Commit `tdx/embed/datatool` to git - downloaded at build time
- Import `_ "github.com/duckdb/duckdb-go/v2"` outside init package - register driver early

## UNIQUE STYLES

**Embedded binary build:**
- `make build` downloads TDX datatool from official URL → embeds in `tdx/embed/`
- Requires `unrar` installed for extraction
- Binary embedded as `embed.FS` at build time

**Task-based workflow (workflow/):**
- `TaskExecutor` manages task execution with dependency resolution (DAG)
- Tasks defined in `workflow/tasks.go` with explicit `DependsOn` arrays
- Parallel execution of tasks with no dependencies
- Optional tasks via `SkipIf` condition (e.g., `--minline`, `--tdxhome`)
- Error modes: `ErrorModeStop` (default) vs `ErrorModeSkip`

**Incremental update logic:**
- `cron` command uses workflow tasks with dependency: `update_daily → update_gbbq → calc_basic → calc_factor`
- Each task checks latest date → fetches delta from TDX
- Handles gaps >30 days by manual intervention
- Supports 1min/5min incremental import (optional tasks)

**CSV pipeline pattern:**
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
```

## NOTES

**Gotchas:**
- 复权因子算法 from QUANTAXIS - verify before modifying
- 分时数据无历史 - need to backfill manually
- 30+ day gaps require manual data fill before cron continues
- Symbol code changes not handled (历史记录不更新)
- Binary only works on x86 Linux (ARM builds fail with embedded datatool)

**Table naming:**
- `raw_*` - raw imported data
- `v_*` - views (复权, basic info merged)
- `v_qfq_daily`, `v_hfq_daily` - 复权 views
