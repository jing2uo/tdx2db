# WORKFLOW EXECUTION FRAMEWORK

**Purpose:** Task execution framework with dependency resolution (DAG)

## STRUCTURE
```
./workflow/
├── engine.go   # TaskExecutor, Task, TaskArgs, TaskResult
└── tasks.go    # Task definitions (init/update modes)
```

## WHERE TO LOOK
| Task | File | Notes |
|------|------|-------|
| Add new task | tasks.go | Define task with dependencies |
| Run specific tasks | task.go | Use TaskExecutor with task names |
| Modify task logic | tasks.go | Update executor function |

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
        return !strings.Contains(args.Minline, "1")
    },
    Executor: executeUpdate1Min,
    OnError: ErrorModeSkip,
}
```

**Common abstraction:**
- `executeDailyImport()` - Shared logic for daily data conversion + import
- `executeUpdateDaily()` - Downloads data, then calls `executeDailyImport()`
- `executeInitDaily()` - Uses user-provided directory, then calls `executeDailyImport()`

**TaskArgs:**
- `Minline` - "1", "5", "1,5"
- `TdxHome` - 通达信安装目录
- `TempDir`, `VipdocDir` - Temp directories
- `DayFileDir` - User-provided TDX data directory (for init)
- `ValidPrefixes` - Market/Index/Block prefixes (passed from cmd)
- `Today` - Current date for incremental updates

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
- Update mode: `update_daily → update_gbbq → calc_basic → calc_factor → update_1min → update_5min → update_blocks`
- Init mode: `init_daily` (only import daily data from user-provided directory)

**Error modes:**
- `ErrorModeStop` - Stop execution on error (default)
- `ErrorModeSkip` - Continue execution even if task fails (for optional tasks like update_1min, update_5min, update_blocks)

**Task skipping:**
- Tasks can be skipped by `SkipIf` condition (e.g., minline not set, tdxhome empty)
- Tasks can also return `StateSkipped` when no new data exists (e.g., non-trading day, data already up to date)

**Usage from cmd/init.go:**
```go
executor := workflow.NewTaskExecutor(db, workflow.GetRegisteredTasks())
args := &workflow.TaskArgs{
    DayFileDir:    dayFileDir,
    TempDir:       TempDir,
    VipdocDir:     VipdocDir,
    ValidPrefixes: ValidPrefixes,
    Today:         GetToday(),
}
executor.Run(ctx, workflow.GetInitTaskNames(), args)
```

**Usage from cmd/cron.go:**
```go
executor := workflow.NewTaskExecutor(db, workflow.GetRegisteredTasks())
args := &workflow.TaskArgs{
    Minline:       minline,
    TdxHome:       tdxhome,
    TempDir:       TempDir,
    VipdocDir:     VipdocDir,
    ValidPrefixes: ValidPrefixes,
    Today:         GetToday(),
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

