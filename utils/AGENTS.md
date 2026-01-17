# UTILITIES

**Purpose:** Shared utilities (cache, pipeline, CSV, download, iocheck)

## STRUCTURE
```
./utils/
├── cache.go       # Temp directory creation
├── pipeline.go    # Concurrent processing pipeline
├── csv_write.go   # CSV writer
├── download.go    # HTTP download
├── iocheck.go     # File/directory validation
├── symbol.go      # Symbol parsing utilities
└── unzip.go       # RAR/ZIP extraction
```

## WHERE TO LOOK
| Task | File | Notes |
|------|------|-------|
| Add pipeline step | pipeline.go | Generic Pipeline[I,O] type |
| Change CSV format | csv_write.go | Uses struct tags |
| Add validation | iocheck.go | Directory/file checks |
| Modify download | download.go | HTTP download with retry |

## CONVENTIONS

**Pipeline (pipeline.go):**
- Generic type: `Pipeline[I, O]` for flexible input/output
- Default concurrency: `runtime.NumCPU()`
- Default buffer size: `concurrency * 4`
- Options: `WithConcurrency(n)`, `WithBufferSize(n)`

**Pipeline.Run():**
- Inputs: `[]I` slice
- Process function: `func(ctx context.Context, input I) ([]O, error)`
- Consume function: `func(rows []O) error`
- Returns: `PipelineResult` with stats

**Pipeline result:**
- `TotalItems` - Input count
- `ProcessedItems` - Successfully processed
- `OutputRows` - Rows written
- `Errors` - Array of errors
- `Duration` - Execution time

**CSV writer (csv_write.go):**
- Uses struct `col:"name"` tags for column mapping
- Header row from struct field names
- Type hints via `type:"date"` or `type:"datetime"`

**Cache dir (cache.go):**
- Uses `os.MkdirTemp("", "tdx2db-temp-")`
- Returns temp dir path for use by commands

**Directory check (iocheck.go):**
- `CheckDirectory()` validates directory exists + readable

**Download (download.go):**
- HTTP GET with timeout
- Basic retry logic on failure

## ANTI-PATTERNS

**DO NOT:**
- Modify pipeline concurrency defaults - tuned for performance
- Remove panic recovery in pipeline goroutines - required for safety
- Skip context checks in process functions - prevents graceful shutdown

**NEVER:**
- Use pipeline for single-file operations - overhead not needed
- Close CSV writer multiple times - defer handles cleanup
- Ignore pipeline errors - check `result.HasErrors()`

## NOTES

**CSV tags:** Required on all exported struct fields, matches DB columns, type hints for dates.
**Temp dir cleanup:** Managed by `cobra.OnFinalize` in main.go.
