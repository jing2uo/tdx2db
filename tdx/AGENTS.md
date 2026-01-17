# TDX DATA PARSING

**Purpose:** Parse 通达信(TDX) binary format files (.day, .1, .05, blocks)

## STRUCTURE
```
./tdx/
├── kline.go       # K-line parsing (.day, .1, .05)
├── gbbq.go        # 股本变迁 parsing
├── blocks.go      # Block/concept parsing
├── gbbq_var.go    # TDX datatool variable definitions
├── datatool.go    # Embedded datatool interface
└── embed/         # Embedded TDX datatool binary
```

## WHERE TO LOOK
| Task | File | Notes |
|------|------|-------|
| Add new K-line format | kline.go | Follow 32-byte record pattern |
| Parse block data | blocks.go | Binary format from TDX |
| Modify gbbq parsing | gbbq.go | Uses embedded datatool |
| TDX binary format | All files | Little-endian byte order |

## CONVENTIONS

**Record formats:**
- Day K-line: 32 bytes per record (date, open, high, low, close, amount, volume)
- Min K-line: 32 bytes per record (datetime, ohlc, amount, volume)
- Dates: uint32 packed (YYYYMMDD) or uint16 (custom offset)

**Date parsing (kline.go):**
- Day: `year = date/10000`, `month = (date%10000)/100`, `day = date%100`
- Min: `year = date/2048 + 2004`, `month = (date%2048)/100`, `day = (date%2048)%100`

**Price conversion:**
- All prices divide by 100.0 (raw * 100 = actual price)
- Amount stored as float32 bits

**Pipeline usage:**
- `ConvertFilesToCSV()` uses `utils.Pipeline` for concurrent file processing
- Files filtered by `validPrefixes` before parsing
- Context cancellation checked in loops

## ANTI-PATTERNS

**DO NOT:**
- Change record size from 32 bytes - TDX format requirement
- Modify date parsing logic - verified against TDX spec
- Assume all files valid - skip bad dates (continue on parse error)

**NEVER:**
- Remove context checks in loops - prevents graceful shutdown
- Commit embedded binary to git - downloaded at build time
- Use big-endian byte order - TDX uses little-endian

## NOTES

**File suffixes:**
- `.day` - Daily K-line data
- `.1` - 1-minute K-line
- `.05` - 5-minute K-line

**Symbol extraction:**
- Symbol = filename without suffix (e.g., `sh000001.day` → `sh000001`)

**Error handling:**
- Skip individual record parse errors (continue loop)
- Return error on file I/O failures
