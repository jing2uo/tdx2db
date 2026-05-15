# TDX DATA PARSING

**Purpose:** Parse 通达信(TDX) binary format files (.day, .01), 假期日历, and online OpenTDX quote/block data

## STRUCTURE
```
./tdx/
├── kline.go       # K-line parsing (.day, .01)
├── kline_test.go
├── merge.go       # native day merge (跨平台合并 vipdoc 输出)
├── merge_test.go
├── gbbq.go        # 股本变迁 parsing
├── holidays.go    # 交易日历解析 (zhb.zip → needini.dat)
├── client.go      # OpenTDX protocol client/session helpers
├── block.go       # 在线板块/行业/概念信息 + 成分关系拉取
├── stock.go       # 在线代码名称拉取 (quote server 0x44d)
├── gbbq_var.go    # TDX datatool variable definitions
├── datatool.go    # Embedded datatool interface (Linux only)
└── embed/         # Embedded TDX datatool binary
```

## WHERE TO LOOK
| Task | File | Notes |
|------|------|-------|
| Add new K-line format | kline.go | Follow 32-byte record pattern |
| Parse holidays | holidays.go | needini.dat 文本解析 |
| Modify gbbq parsing | gbbq.go | Uses embedded datatool |
| Modify online block data | block.go | OpenTDX protocol block/category/member requests |
| Modify online code names | stock.go | TDX quote server 0x44d interface |
| Modify OpenTDX session/protocol | client.go | Connection, request/response framing |
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
- 价格刻度由 `model.PriceScale(symbol)` 给出：股票/指数/板块 = 100，ETF/LOF/B股 = 1000
- 解析每条记录时按 symbol 取 scale，再 `float64(raw) / scale`
- Amount stored as float32 bits

**Pipeline usage:**
- `ConvertFilesToCSV()` uses `utils.Pipeline` for concurrent file processing
- Files filtered by `^(sh|sz|bj)\d+$` regex — full ingest, no prefix whitelist
- Context cancellation checked in loops

**Online data:**
- `FetchOnlineBlocks(ctx)` returns `[]model.BlockInfo` + `[]model.BlockMember` for block/industry/concept imports
- `FetchOnlineSymbolNames(ctx)` returns `[]model.SymbolName` for online code-name imports, filtering stock / etf / index classes
- These online fetchers are update-time helpers; raw daily ingestion and basic/factor calculations still use parsed K-line files + `raw_symbol_class`

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
- `.01` - 1-minute K-line

**Symbol extraction:**
- Symbol = filename without suffix (e.g., `sh000001.day` → `sh000001`)

**Error handling:**
- Skip individual record parse errors (continue loop)
- Return error on file I/O failures
