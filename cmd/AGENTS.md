# CLI COMMANDS

**Purpose:** Cobra CLI commands (init, cron, convert)

## STRUCTURE
```
./cmd/
├── common.go     # Shared constants (prefixes, paths)
├── init.go       # Full import (calls workflow)
├── cron.go       # Incremental update (calls workflow)
└── convert.go    # TDX to CSV conversion
```

## WHERE TO LOOK
| Task | File | Notes |
|------|------|-------|
| Add new command | main.go + cmd/*.go | Cobra subcommand with ctx |
| Modify flags | main.go | Add flags to command |
| Change prefixes | common.go | Market/Index/Block prefixes |
| Modify cron logic | workflow/tasks.go | Task executor functions |
| Run specific tasks | workflow/task.go | Use TaskExecutor directly |

## CONVENTIONS

**Command pattern (main.go):**
- Cobra for CLI structure
- All commands receive `context.Context` as first arg
- Signal handler (SIGINT/SIGTERM) → ctx cancel → safe exit
- Exit 0 on context cancel, 1 on error

**Temp directory:**
- Created via `utils.GetCacheDir()` → `$TMPDIR/tdx2db-temp-*`
- CSV paths constructed in `workflow/tasks.go` using `filepath.Join(TempDir, "stock.csv")`
- Cleaned up by `cobra.OnFinalize`

**Common prefixes (common.go):**
- Market: `sz30`, `sz00`, `sh60`, `sh68`, `bj920`
- Index: `sh000300`, `sh000905`, `sz399001`, etc.
- Block: `sh880` (concept/style), `sh881` (industry)
- `ValidPrefixes` = all concatenated

**init command flow:**
1. Create DB driver via `database.NewDB()`
2. `Connect()` → `InitSchema()`
3. `CheckDirectory(dayFileDir)`
4. `tdx.ConvertFilesToCSV()` → TempDir/stock.csv
5. `db.ImportDailyStocks()`

**cron command flow:**
1. Get `db.GetLatestDate()` for incremental range
2. Fetch delta from TDX (or full if new)
3. Import daily + minute (if --minline set)
4. Calculate adjust factors via `calc/`

**convert command:**
- Types: `day`, `1min`, `5min`, `tic4`, `day4`
- Input: directory or zip file
- Output: CSV files

## ANTI-PATTERNS

**DO NOT:**
- Remove context checks in loops - prevents graceful shutdown
- Use hardcoded paths - use `filepath.Join(TempDir, "filename")` in workflow
- Skip `defer cancel()` - required for signal handler

**NEVER:**
- Forget ctx check in long-running loops - causes stuck shutdown
- Change `cmd.TempDir` logic - must use temp dir
- Add command without required flags validation

## NOTES

**Flag validation:**
- `--minline` only accepts: `1`, `5`, `1,5`, `5,1`

**Error handling:**
- Wrap errors with `%w` for error chain
- Context cancellation returns `ctx.Err()` directly (don't wrap)
