package model

import (
	"fmt"
	"sync"
)

// ViewDef 描述一个数据库视图：名字 + 每种方言的 SELECT 主体 SQL。
// 与 TableMeta 一样是纯数据、dialect 无关——具体取哪一栏、怎么拼 CREATE VIEW
// 由各 driver 自己决定（对应 driver 里的 mapType）。声明即自注册到 model registry。
type ViewDef struct {
	Name       string
	DuckDB     string
	ClickHouse string
}

var (
	viewRegistry   []ViewDef
	viewRegistryMu sync.Mutex
)

// DefineView 注册一个视图并原样返回，供 var 声明处持有。
func DefineView(view ViewDef) ViewDef {
	viewRegistryMu.Lock()
	defer viewRegistryMu.Unlock()

	viewRegistry = append(viewRegistry, view)
	return view
}

// AllViews 返回当前所有已注册的视图定义
func AllViews() []ViewDef {
	viewRegistryMu.Lock()
	defer viewRegistryMu.Unlock()

	result := make([]ViewDef, len(viewRegistry))
	copy(result, viewRegistry)
	return result
}

// --- 定义视图 ---
//
// 命名约定：v_<class>_<fq>，class 放前面便于 tab-complete 按归属浏览
// （v_stock_<TAB> / v_etf_<TAB> 各列出 3 个）。
// stock / etf 拆开维护：ETF 价格 scale=1000、ROUND 精度 3 位，
// stock scale=100、ROUND 精度 2 位。
var (
	ViewStockBFQ = DefineView(stockBFQView("v_stock_bfq", "stock"))
	ViewStockQFQ = DefineView(adjustedQFQView("v_stock_qfq", "stock", 2))
	ViewStockHFQ = DefineView(adjustedHFQView("v_stock_hfq", "stock", 2))
	ViewETFBFQ   = DefineView(stockBFQView("v_etf_bfq", "etf"))
	ViewETFQFQ   = DefineView(adjustedQFQView("v_etf_qfq", "etf", 3))
	ViewETFHFQ   = DefineView(adjustedHFQView("v_etf_hfq", "etf", 3))
)

func stockBFQView(name, class string) ViewDef {
	sql := fmt.Sprintf(`
		WITH latest_factors AS (
			SELECT symbol, argMax(hfq_factor, date) AS latest_hfq
			FROM %s
			GROUP BY symbol
		)
		SELECT
			s.symbol     AS symbol,
			s.date       AS date,
			s.open       AS open,
			s.high       AS high,
			s.low        AS low,
			s.close      AS close,
			b.preclose   AS preclose,
			s.volume     AS volume,
			s.amount     AS amount,
			b.turnover   AS turnover,
			b.floatmv    AS floatmv,
			b.totalmv    AS totalmv,
			b.change_pct AS change_pct,
			b.amplitude  AS amplitude,
			COALESCE(f.hfq_factor, 1) AS hfq_factor,
			COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1) AS qfq_factor
		FROM %s s
		INNER JOIN %s sc ON s.symbol = sc.symbol AND sc.class = '%s'
		LEFT JOIN %s f ON s.symbol = f.symbol AND s.date = f.date
		LEFT JOIN latest_factors lf ON s.symbol = lf.symbol
		LEFT JOIN %s b ON s.symbol = b.symbol AND s.date = b.date
	`,
		TableAdjustFactor.TableName,
		TableKlineDaily.TableName,
		TableSymbolClass.TableName,
		class,
		TableAdjustFactor.TableName,
		TableBasicDaily.TableName,
	)
	return ViewDef{Name: name, DuckDB: sql, ClickHouse: sql}
}

func adjustedHFQView(name, class string, precision int) ViewDef {
	sql := fmt.Sprintf(`
		SELECT
			s.symbol AS symbol,
			s.date   AS date,
			ROUND(s.open  * COALESCE(f.hfq_factor, 1), %d) AS open,
			ROUND(s.high  * COALESCE(f.hfq_factor, 1), %d) AS high,
			ROUND(s.low   * COALESCE(f.hfq_factor, 1), %d) AS low,
			ROUND(s.close * COALESCE(f.hfq_factor, 1), %d) AS close,
			ROUND(b.preclose * COALESCE(f.hfq_factor, 1), %d) AS preclose,
			s.volume     AS volume,
			s.amount     AS amount,
			b.turnover   AS turnover,
			b.floatmv    AS floatmv,
			b.totalmv    AS totalmv,
			b.change_pct AS change_pct,
			b.amplitude  AS amplitude,
			COALESCE(f.hfq_factor, 1) AS hfq_factor
		FROM %s s
		INNER JOIN %s sc ON s.symbol = sc.symbol AND sc.class = '%s'
		LEFT JOIN %s f ON s.symbol = f.symbol AND s.date = f.date
		LEFT JOIN %s b ON s.symbol = b.symbol AND s.date = b.date
	`,
		precision,
		precision,
		precision,
		precision,
		precision,
		TableKlineDaily.TableName,
		TableSymbolClass.TableName,
		class,
		TableAdjustFactor.TableName,
		TableBasicDaily.TableName,
	)
	return ViewDef{Name: name, DuckDB: sql, ClickHouse: sql}
}

func adjustedQFQView(name, class string, precision int) ViewDef {
	sql := fmt.Sprintf(`
		WITH latest_factors AS (
			SELECT symbol, argMax(hfq_factor, date) AS latest_hfq
			FROM %s
			GROUP BY symbol
		)
		SELECT
			s.symbol AS symbol,
			s.date   AS date,
			ROUND(s.open  * COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1), %d) AS open,
			ROUND(s.high  * COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1), %d) AS high,
			ROUND(s.low   * COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1), %d) AS low,
			ROUND(s.close * COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1), %d) AS close,
			ROUND(b.preclose * COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1), %d) AS preclose,
			s.volume     AS volume,
			s.amount     AS amount,
			b.turnover   AS turnover,
			b.floatmv    AS floatmv,
			b.totalmv    AS totalmv,
			b.change_pct AS change_pct,
			b.amplitude  AS amplitude,
			COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1) AS qfq_factor
		FROM %s s
		INNER JOIN %s sc ON s.symbol = sc.symbol AND sc.class = '%s'
		LEFT JOIN %s f ON s.symbol = f.symbol AND s.date = f.date
		LEFT JOIN latest_factors lf ON s.symbol = lf.symbol
		LEFT JOIN %s b ON s.symbol = b.symbol AND s.date = b.date
	`,
		TableAdjustFactor.TableName,
		precision,
		precision,
		precision,
		precision,
		precision,
		TableKlineDaily.TableName,
		TableSymbolClass.TableName,
		class,
		TableAdjustFactor.TableName,
		TableBasicDaily.TableName,
	)
	return ViewDef{Name: name, DuckDB: sql, ClickHouse: sql}
}
