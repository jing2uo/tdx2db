# FINANCIAL CALCULATIONS

**Purpose:** Calculate basic indicators (preclose, turnover, market value) and 后复权因子 (HFQ factor)，覆盖 stock + ETF/LOF/B股

## STRUCTURE
```
./calc/
├── basic.go              # BasicDaily calculation (preclose, turnover, MV)
├── fq_quantaxis.go       # HFQ factor (QUANTAXIS-based, 直接消费 BasicDaily.PreClose)
└── fq_quantaxis_test.go  # Factor calculation tests
```

## WHERE TO LOOK
| Task | File | Notes |
|------|------|-------|
| Modify preclose formula | basic.go:298 | `calculatePreClosePrice()` (含 Splitc3) |
| Modify turnover/MV | basic.go:228-241 | Per-day loop in `CalculateBasicDaily()` |
| Modify share backfill | basic.go:179-197 | Initial float/total share logic |
| ETF cat=11 split | basic.go:286 | `mergeSplitFromGbbq()` |
| Modify HFQ factor | fq_quantaxis.go:86 | `calculateFullHfq()` |

## SHARED TYPES
- `GbbqIndex` (basic.go:25): `map[string][]GbbqData` — per-symbol gbbq lookup
- `xdxrInfo` (basic.go:15): 累积 分红/配股/送转股 + Splitc3 (cat=11 折算系数)
- `BasicContext` / `FactorContext`: hold DB (+ GbbqIndex for basic) for pipeline processing

## DATA FLOW

```
DB: raw_kline_daily + raw_gbbq
        │
        └──► calc/basic.go (CalculateBasicDaily)
                 │
                 └──► raw_basic_daily (preclose, turnover, floatmv, totalmv) [stock + etf]
                        │
                        └──► calc/fq_quantaxis.go (calculateFullHfq)
                                │
                                └──► raw_adjust_factor (hfq_factor)
```

Task dependency: `calc_basic` runs before `calc_factor` (factor 直接读 basic.PreClose)。
Symbol filter: `GetSymbolsByClass(stock, etf)` —— index/block 不参与计算。

## KEY ALGORITHMS

### PreClose (basic.go:298)
```
preclose = ((prevClose×10 - 分红 + 配股×配股价) / (10 + 配股 + 送转股)) / Splitc3
```
- 无 xdxr/split：`preclose = prevClose`
- Splitc3 来自 cat=11，多次同日折算累乘

### HFQ Factor (fq_quantaxis.go:86)
- Starts at 1.0 on day 1
- 通过 `prevClose / basic.PreClose` 检测除权日；ratio ≠ 1.0 时 `hfq *= ratio`
- Factor only changes when `|ratio - 1.0| > 1e-9`
- 不再独立维护 xdxr 日期集合 —— PreClose 已经把所有除权信息带进来

### Gbbq Date → KlineDay Index Mapping (basic.go:136)
`findIdx`：先查 dateMap (精确匹配)，否则用 `sort.Search` 落到下一交易日。处理 gbbq 事件落在周末/假期的情况。

### Gbbq Categories
| Category | Meaning | Fields used |
|----------|---------|-------------|
| 1 | 除权除息 | C1=分红, C2=配股, C3=送转股, C4=配股价 |
| 11 | ETF/LOF 份额折算 | C3=折算系数 (PreClose 除以它) |
| 2,3,5,7,8,9,10 | 股本变动 | C1=变前流通, C2=变前总股本, C3=变后流通, C4=变后总股本 |

### Share Backfill (basic.go:179-197)
If the first gbbq share record date > IPO date, uses its C1/C2 (pre-change values) to backfill float/total shares for the gap period. Fallback chain: first.C1 → first.C3 → scan for first record with C3 > 0.

## ANTI-PATTERNS

**DO NOT:**
- Use raw gbbq dates to match against trading days — always go through `findIdx` / `sort.Search`
- 在 fq_quantaxis 里再做一遍 xdxr 日期映射 —— 单一事实源在 basic.go 写出的 PreClose
- Assume gbbq dates are always trading days — weekends/holidays are common
- Modify preclose formula without understanding 除权除息 + cat=11 折算 semantics
- 忘记 cat=11 处理 —— ETF 拆分日不除以 Splitc3 会让当天涨跌幅看起来像异常暴跌

**NEVER:**
- Change factor calculation to incremental — full recalc is intentional (factor depends on entire history)
- Remove the `floatEqual` tolerance check — floating point ratios need it

## NOTES

**Both calc modules run in full recalculation mode** (truncate table + reimport all). This is intentional — preclose/factor depend on the entire history chain.

**Pipeline concurrency:** Both exports use `utils.Pipeline` to process symbols concurrently. Each symbol is independent.

**Units:** gbbq share values are in 万股 (×10000 = actual shares). Turnover = volume / (float_shares × 10000).

**ETF 一般无 cat 2/3/5/7/8/9/10 记录**，因此 turnover/floatmv/totalmv 通常为 0；这是预期行为。
