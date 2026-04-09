# FINANCIAL CALCULATIONS

**Purpose:** Calculate stock basic indicators (preclose, turnover, market value) and 后复权因子 (HFQ factor)

## STRUCTURE
```
./calc/
├── basic.go              # StockBasic calculation (preclose, turnover, MV)
├── fq_quantaxis.go       # HFQ factor calculation (QUANTAXIS-based)
└── fq_quantaxis_test.go  # Factor calculation tests
```

## WHERE TO LOOK
| Task | File | Notes |
|------|------|-------|
| Modify preclose formula | basic.go:265 | `calculatePreClosePrice()` |
| Modify turnover/MV | basic.go:210-221 | Per-day loop in `CalculateStockBasic()` |
| Modify share backfill | basic.go:161-178 | Initial float/total share logic |
| Modify HFQ factor | fq_quantaxis.go:97 | `calculateFullHfq()` |
| Modify xdxr date mapping | fq_quantaxis.go:130 | `buildXdxrDateSet()` |

## SHARED TYPES
- `GbbqIndex` (basic.go:21): `map[string][]GbbqData` — per-symbol gbbq lookup, built once, shared by basic and factor
- `xdxrInfo` (basic.go:14): accumulated 分红/配股/送转股 for a single xdxr event
- `BasicContext` / `FactorContext`: hold DB + GbbqIndex for pipeline processing

## DATA FLOW

```
DB: raw_stocks_daily + raw_gbbq
        │
        ├──► calc/basic.go ──► raw_stocks_basic (preclose, turnover, floatmv, totalmv)
        │
        └──► calc/fq_quantaxis.go ──► raw_adjust_factor (hfq_factor)
                (reads raw_stocks_basic for preclose)
```

Task dependency: `calc_basic` runs before `calc_factor` (factor needs preclose from basic).

## KEY ALGORITHMS

### PreClose (basic.go:265)
```
preclose = (prevClose×10 - 分红 + 配股×配股价) / (10 + 配股 + 送转股)
```
When no xdxr event: `preclose = prevClose`

### HFQ Factor (fq_quantaxis.go:97)
- Starts at 1.0 on day 1
- On xdxr days: `hfq *= prevClose / preclose` (ratio captures the adjustment)
- Factor only changes when `|ratio - 1.0| > 1e-9`

### Gbbq Date → Trading Day Mapping
Both basic.go and fq_quantaxis.go use `sort.Search` to map gbbq dates to the first trading day >= gbbq date. This handles gbbq events on weekends/holidays.

```go
idx := sort.Search(len(data), func(i int) bool {
    return !data[i].Date.Before(gbbqDate)
})
```

### Gbbq Categories
| Category | Meaning | Fields used |
|----------|---------|-------------|
| 1 | 除权除息 | C1=分红, C2=配股, C3=送转股, C4=配股价 |
| 2,3,5,7,8,9,10 | 股本变动 | C1=变前流通, C2=变前总股本, C3=变后流通, C4=变后总股本 |

### Share Backfill (basic.go:161-178)
If the first gbbq share record date > IPO date, uses its C1/C2 (pre-change values) to backfill float/total shares for the gap period. Fallback chain: first.C1 → first.C3 → scan for first record with C3 > 0.

## ANTI-PATTERNS

**DO NOT:**
- Use raw gbbq dates to match against trading days — always map via `sort.Search`
- Break date mapping consistency between basic.go and fq_quantaxis.go
- Assume gbbq dates are always trading days — weekends/holidays are common
- Modify preclose formula without understanding 除权除息 semantics

**NEVER:**
- Change factor calculation to incremental — full recalc is intentional (factor depends on entire history)
- Remove the `floatEqual` tolerance check — floating point ratios need it

## NOTES

**Both calc modules run in full recalculation mode** (truncate table + reimport all). This is intentional — preclose/factor depend on the entire history chain.

**Pipeline concurrency:** Both exports use `utils.Pipeline` to process symbols concurrently. Each symbol is independent.

**Units:** gbbq share values are in 万股 (×10000 = actual shares). Turnover = volume / (float_shares × 10000).
