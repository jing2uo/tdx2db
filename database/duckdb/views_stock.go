package duckdb

import (
	"fmt"

	"github.com/jing2uo/tdx2db/model"
)

func (d *DuckDBDriver) registerStockViews() {
	d.viewImpls[model.ViewStockBFQ] = d.buildStockBFQView
	d.viewImpls[model.ViewStockHFQ] = d.buildStockHFQView
	d.viewImpls[model.ViewStockQFQ] = d.buildStockQFQView
}

// buildStockBFQView 股票不复权视图：原始价已 PriceScale 过，不再 ROUND。
func (d *DuckDBDriver) buildStockBFQView() error {
	query := fmt.Sprintf(`
		CREATE OR REPLACE VIEW %s AS
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
		INNER JOIN %s sc ON s.symbol = sc.symbol AND sc.class = 'stock'
		LEFT JOIN %s f ON s.symbol = f.symbol AND s.date = f.date
		LEFT JOIN latest_factors lf ON s.symbol = lf.symbol
		LEFT JOIN %s b ON s.symbol = b.symbol AND s.date = b.date
	`,
		model.ViewStockBFQ,
		model.TableAdjustFactor.TableName,
		model.TableKlineDaily.TableName,
		model.TableSymbolClass.TableName,
		model.TableAdjustFactor.TableName,
		model.TableBasicDaily.TableName,
	)
	_, err := d.db.Exec(query)
	return err
}

// buildStockHFQView 股票后复权视图：价 = 原始价 * hfq_factor，ROUND 2 位。
func (d *DuckDBDriver) buildStockHFQView() error {
	query := fmt.Sprintf(`
		CREATE OR REPLACE VIEW %s AS
		SELECT
			s.symbol AS symbol,
			s.date   AS date,
			ROUND(s.open  * COALESCE(f.hfq_factor, 1), 2) AS open,
			ROUND(s.high  * COALESCE(f.hfq_factor, 1), 2) AS high,
			ROUND(s.low   * COALESCE(f.hfq_factor, 1), 2) AS low,
			ROUND(s.close * COALESCE(f.hfq_factor, 1), 2) AS close,
			ROUND(b.preclose * COALESCE(f.hfq_factor, 1), 2) AS preclose,
			s.volume     AS volume,
			s.amount     AS amount,
			b.turnover   AS turnover,
			b.floatmv    AS floatmv,
			b.totalmv    AS totalmv,
			b.change_pct AS change_pct,
			b.amplitude  AS amplitude,
			COALESCE(f.hfq_factor, 1) AS hfq_factor
		FROM %s s
		INNER JOIN %s sc ON s.symbol = sc.symbol AND sc.class = 'stock'
		LEFT JOIN %s f ON s.symbol = f.symbol AND s.date = f.date
		LEFT JOIN %s b ON s.symbol = b.symbol AND s.date = b.date
	`,
		model.ViewStockHFQ,
		model.TableKlineDaily.TableName,
		model.TableSymbolClass.TableName,
		model.TableAdjustFactor.TableName,
		model.TableBasicDaily.TableName,
	)
	_, err := d.db.Exec(query)
	return err
}

// buildStockQFQView 股票前复权视图：价 = 原始价 * hfq_factor / 最新 hfq_factor，ROUND 2 位。
func (d *DuckDBDriver) buildStockQFQView() error {
	query := fmt.Sprintf(`
		CREATE OR REPLACE VIEW %s AS
		WITH latest_factors AS (
			SELECT symbol, argMax(hfq_factor, date) AS latest_hfq
			FROM %s
			GROUP BY symbol
		)
		SELECT
			s.symbol AS symbol,
			s.date   AS date,
			ROUND(s.open  * COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1), 2) AS open,
			ROUND(s.high  * COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1), 2) AS high,
			ROUND(s.low   * COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1), 2) AS low,
			ROUND(s.close * COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1), 2) AS close,
			ROUND(b.preclose * COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1), 2) AS preclose,
			s.volume     AS volume,
			s.amount     AS amount,
			b.turnover   AS turnover,
			b.floatmv    AS floatmv,
			b.totalmv    AS totalmv,
			b.change_pct AS change_pct,
			b.amplitude  AS amplitude,
			COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1) AS qfq_factor
		FROM %s s
		INNER JOIN %s sc ON s.symbol = sc.symbol AND sc.class = 'stock'
		LEFT JOIN %s f ON s.symbol = f.symbol AND s.date = f.date
		LEFT JOIN latest_factors lf ON s.symbol = lf.symbol
		LEFT JOIN %s b ON s.symbol = b.symbol AND s.date = b.date
	`,
		model.ViewStockQFQ,
		model.TableAdjustFactor.TableName,
		model.TableKlineDaily.TableName,
		model.TableSymbolClass.TableName,
		model.TableAdjustFactor.TableName,
		model.TableBasicDaily.TableName,
	)
	_, err := d.db.Exec(query)
	return err
}
