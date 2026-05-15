# WORKFLOW EXECUTION FRAMEWORK

**Purpose:** Task execution framework (DAG) + 交易日历 + 任务前置规划 (WorkPlan)

## STRUCTURE
```
./workflow/
├── engine.go          # TaskExecutor, Task, TaskArgs (含 Plan), TaskResult, registerTask
├── task_*.go          # Task definitions (init/update modes) + executors, split by concern
├── tdx_helper.go      # shared date-range pull helpers for TDX archives
├── plan.go            # WorkPlan + BuildWorkPlan (cron 启动前的全局规划)
└── calendar.go        # TradingCalendar (节假日/周末/最近交易日)
```

## WHERE TO LOOK
| Task | File | Notes |
|------|------|-------|
| Add new task | task_*.go | Define task with dependencies and call `registerTask()` in init |
| Run specific tasks | engine.go | Use TaskExecutor.Run with task names |
| Modify task logic | task_*.go | Update executor function in the concern-specific file |
| 调整 cron 跑哪些任务 | plan.go | BuildWorkPlan / NeedXxx 推导 |
| 节假日判定 | calendar.go | TradingCalendar |

## CONVENTIONS

**Task definition:**
```go
TaskUpdateDaily = &Task{
    Name:      "update_daily",
    DependsOn: []string{},
    Executor:  executeUpdateDaily,
}
```

**Task with dependencies:**
```go
TaskCalcBasic = &Task{
    Name:      "calc_basic",
    DependsOn: []string{"update_daily", "update_gbbq"},
    Executor:  executeCalcBasic,
}
```

**Optional task:**
```go
TaskUpdate1Min = &Task{
    Name:      "update_1min",
    DependsOn: []string{},
    SkipIf: func(ctx, db, args) bool {
        return !args.Min
    },
    Executor: executeUpdate1Min,
    OnError: ErrorModeSkip,
}
```

**Plan-driven SkipIf（cron 主链路）：**
```go
TaskCalcBasic = &Task{
    Name: "calc_basic",
    DependsOn: []string{"update_daily", "update_gbbq"},
    SkipIf: skipIfPlan(func(p *WorkPlan) bool { return !p.NeedBasic }),
    Executor: executeCalcBasic,
}
```
`skipIfPlan` 在 `args.Plan == nil` (init 流程) 时不跳过，保持原行为。

**Common abstraction:**
- `executeDailyImport()` - Shared logic for daily data conversion + import
- `executeUpdateDaily()` - Downloads data, then calls `executeDailyImport()`
- `executeInitDaily()` - Uses user-provided directory, then calls `executeDailyImport()`
- `fetch_gbbq` downloads/extracts gbbq.zip once; `update_gbbq` decodes gbbq and `update_holidays` reads embedded zhb.zip from that extracted directory
- `fetch_tick` downloads TIC archives and stores valid dates in `args.Extra[ExtraTickValidDates]`; `update_1min` imports only those dates
- `update_blocks` pulls online block/industry/concept data into raw_tdx_blocks_info/raw_tdx_blocks_member
- `update_symbol_names` pulls online code names into raw_symbol_name

**TaskArgs:**
- `Min` - bool, 设为 true 时 cron 会跑 update_1min
- `TempDir`, `VipdocDir` - Temp directories
- `DayFileDir` - User-provided TDX data directory (for init)
- `Today` - Current date for incremental updates
- `Plan *WorkPlan` - cron 由 `BuildWorkPlan` 注入；init 留空，`skipIfPlan` 自动放行所有任务

**WorkPlan / TradingCalendar：**
- `BuildWorkPlan(db, today)`：先读 `raw_holidays`，空表（首次/旧库）→ 强制全流程跑；否则用 `TradingCalendar.LastTradingDayOnOrBefore(today)` 与各表最新日期比较，标记 `NeedDaily/NeedGbbq/NeedBasic/NeedFactor/NeedHolidays`
- `plan.AnyNeeded() == false` → cron 直接退出
- `Calendar` 还会回流到 `prepareTdxData`，下载日线 404 时区分"节假日跳过 🎉"/"周末 🌴"/"未发布 ⏳"

**Executor behavior:**
- `topologicalSort()` and `findReadyTasks()` ignore dependencies that are not in the selected task set, so partial task runs only require in-graph deps
- Ready tasks are started as soon as their selected deps are completed/skipped; the executor does not wait for a whole dependency layer to finish before launching newly ready tasks
- On `ErrorModeStop` failure, executor cancels the run context and drains already-started task goroutines before returning the wrapped task error

## ANTI-PATTERNS

**DO NOT:**
- Modify task dependencies at runtime - dependencies are static
- Use blocking calls in executor functions - check ctx.Done()
- Skip error handling in tasks - return proper TaskResult

**NEVER:**
- Create circular dependencies - topological sort will fail
- Ignore task results - check TaskResult.State and Error
- Remove SkipIf for optional tasks - causes errors when args missing

## NOTES

**Task chains:**
- Update mode includes independent roots: `update_daily`, `fetch_gbbq`, `fetch_tick`, `update_blocks`, `update_symbol_names`
- Core daily chain: `update_daily + fetch_gbbq → update_gbbq → calc_basic → calc_factor`
- Holidays chain: `fetch_gbbq → update_holidays`
- Minute chain: `fetch_tick → update_1min` (only when `--min`)
- Init mode: `init_daily` (only import daily data from user-provided directory)

**calc_basic and calc_factor run in full recalculation mode** — they truncate the target table and reimport all rows. This is intentional because preclose/factor depend on the entire history chain.

**Error modes:**
- `ErrorModeStop` - Stop execution on error (default)
- `ErrorModeSkip` - Continue execution even if task fails (for optional tasks like update_1min, update_holidays)

**Task skipping:**
- Tasks can be skipped by `SkipIf` condition (e.g., --min not set)
- Tasks can also return `StateSkipped` when no new data exists (e.g., non-trading day, data already up to date)

**Usage from cmd/init.go:**
```go
executor := workflow.NewTaskExecutor(db, workflow.GetRegisteredTasks())
args := &workflow.TaskArgs{
    DayFileDir: dayFileDir,
    TempDir:    TempDir,
    VipdocDir:  VipdocDir,
    Today:      GetToday(),
}
executor.Run(ctx, workflow.GetInitTaskNames(), args)
```

**Usage from cmd/cron.go:**
```go
plan, err := workflow.BuildWorkPlan(db, GetToday())
if !plan.AnyNeeded() {
    fmt.Println(plan.Reason)
    return nil
}
executor := workflow.NewTaskExecutor(db, workflow.GetRegisteredTasks())
args := &workflow.TaskArgs{
    Min:       minEnable,
    TempDir:   TempDir,
    VipdocDir: VipdocDir,
    Today:     GetToday(),
    Plan:      plan,
}
executor.Run(ctx, workflow.GetUpdateTaskNames(), args)
```

**Common abstraction:**
- `executeDailyImport()` - Shared logic for daily data conversion + import
- `executeUpdateDaily()` - Downloads data, then calls `executeDailyImport()`
- `executeInitDaily()` - Uses user-provided directory, then calls `executeDailyImport()`

**Init behavior:**
- Only imports daily data from `--dayfiledir`
- Does NOT calculate basic indicators or factors (use cron for that)
