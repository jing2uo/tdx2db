# CLI COMMANDS

**Purpose:** Cobra CLI commands (init, cron, convert) — thin wrappers over workflow engine

## STRUCTURE
```
./cmd/
├── common.go          # Shared constants (paths, GetToday)
├── schema_version.go  # Schema version check logic (writeSchemaVersion/checkSchemaVersion)
├── init.go            # Full import (calls workflow with init tasks)
├── cron.go            # Incremental update (calls workflow with update tasks)
└── convert.go         # TDX to CSV conversion (standalone, no DB)
```

## WHERE TO LOOK
| Task | File | Notes |
|------|------|-------|
| Add new command | main.go + cmd/*.go | Cobra subcommand with ctx |
| Modify flags | main.go | Add flags to command |
| Change symbol collection | model/classify.go | ClassifyCode + RebuildSymbolClass |
| Schema version check | schema_version.go | writeSchemaVersion / checkSchemaVersion |
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

**Common (common.go):**
- `GetToday()` - Current date
- `TempDir` / `VipdocDir` - Temp directory paths

**Schema version (schema_version.go):**
- `writeSchemaVersion(db)` - Used by init: auto-write on fresh DB, reject if major mismatch
- `checkSchemaVersion(db)` - Used by cron: reject if version missing or major mismatch
- DB layer (`ReadSchemaVersion`/`WriteSchemaVersion`) only does read/write, no judgment

**init command flow:**
1. Create DB driver via `database.NewDB()`
2. `Connect()` → `InitSchema()` → `writeSchemaVersion()`
3. Check `CountKlineDaily()` — skip if data already exists
4. Run workflow tasks for init

**cron command flow (cmd/cron.go):**
1. Create DB + Connect + InitSchema
2. `checkSchemaVersion()` — reject if version missing or incompatible
3. Run `GetUpdateTaskNames()` → DAG execution:
   - `update_daily` → `update_gbbq` → `calc_basic` → `calc_factor`
   - Optional: `update_1min`, `update_5min`, `update_holidays` (via --minline, --tdxhome)
4. calc_basic and calc_factor run full recalculation (truncate + reimport)

**convert command:**
- Types: `day`, `1min`, `5min`, `tic4`, `day4`
- Input: directory or zip file
- Output: CSV files
- Standalone: no database connection needed

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
