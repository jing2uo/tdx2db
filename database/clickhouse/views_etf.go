package clickhouse

import (
	"fmt"

	"github.com/jing2uo/tdx2db/model"
)

// ETF 视图规则速查：
//   - 过滤条件：sc.class = 'etf'
//   - 价格精度：3 位小数（ETF 申赎价含 NAV，精度需求高于股票）
//   - 复权事件：gbbq cat=11 份额折算（ETF 唯一会影响 hfq_factor 的事件）
//   - basic_daily 字段：preclose / change_pct / amplitude 有效；
//     turnover / floatmv / totalmv 一般为 0（gbbq 通常无 ETF 股本变动记录）

func (d *ClickHouseDriver) registerETFViews() {
	d.viewImpls[model.ViewETFBFQ] = d.buildETFBFQView
	d.viewImpls[model.ViewETFHFQ] = d.buildETFHFQView
	d.viewImpls[model.ViewETFQFQ] = d.buildETFQFQView
}

// buildETFBFQView ETF 不复权视图：原始价已 PriceScale 过，不再 ROUND。
func (d *ClickHouseDriver) buildETFBFQView() error {
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
		INNER JOIN %s sc ON s.symbol = sc.symbol AND sc.class = 'etf'
		LEFT JOIN %s f ON s.symbol = f.symbol AND s.date = f.date
		LEFT JOIN latest_factors lf ON s.symbol = lf.symbol
		LEFT JOIN %s b ON s.symbol = b.symbol AND s.date = b.date
	`,
		model.ViewETFBFQ,
		model.TableAdjustFactor.TableName,
		model.TableKlineDaily.TableName,
		model.TableSymbolClass.TableName,
		model.TableAdjustFactor.TableName,
		model.TableBasicDaily.TableName,
	)
	_, err := d.db.Exec(query)
	return err
}

// buildETFHFQView ETF 后复权视图：价 = 原始价 * hfq_factor，ROUND 3 位。
func (d *ClickHouseDriver) buildETFHFQView() error {
	query := fmt.Sprintf(`
		CREATE OR REPLACE VIEW %s AS
		SELECT
			s.symbol AS symbol,
			s.date   AS date,
			ROUND(s.open  * COALESCE(f.hfq_factor, 1), 3) AS open,
			ROUND(s.high  * COALESCE(f.hfq_factor, 1), 3) AS high,
			ROUND(s.low   * COALESCE(f.hfq_factor, 1), 3) AS low,
			ROUND(s.close * COALESCE(f.hfq_factor, 1), 3) AS close,
			ROUND(b.preclose * COALESCE(f.hfq_factor, 1), 3) AS preclose,
			s.volume     AS volume,
			s.amount     AS amount,
			b.turnover   AS turnover,
			b.floatmv    AS floatmv,
			b.totalmv    AS totalmv,
			b.change_pct AS change_pct,
			b.amplitude  AS amplitude,
			COALESCE(f.hfq_factor, 1) AS hfq_factor
		FROM %s s
		INNER JOIN %s sc ON s.symbol = sc.symbol AND sc.class = 'etf'
		LEFT JOIN %s f ON s.symbol = f.symbol AND s.date = f.date
		LEFT JOIN %s b ON s.symbol = b.symbol AND s.date = b.date
	`,
		model.ViewETFHFQ,
		model.TableKlineDaily.TableName,
		model.TableSymbolClass.TableName,
		model.TableAdjustFactor.TableName,
		model.TableBasicDaily.TableName,
	)
	_, err := d.db.Exec(query)
	return err
}

// buildETFQFQView ETF 前复权视图：价 = 原始价 * hfq_factor / 最新 hfq_factor，ROUND 3 位。
func (d *ClickHouseDriver) buildETFQFQView() error {
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
			ROUND(s.open  * COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1), 3) AS open,
			ROUND(s.high  * COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1), 3) AS high,
			ROUND(s.low   * COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1), 3) AS low,
			ROUND(s.close * COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1), 3) AS close,
			ROUND(b.preclose * COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1), 3) AS preclose,
			s.volume     AS volume,
			s.amount     AS amount,
			b.turnover   AS turnover,
			b.floatmv    AS floatmv,
			b.totalmv    AS totalmv,
			b.change_pct AS change_pct,
			b.amplitude  AS amplitude,
			COALESCE(f.hfq_factor, 1) / COALESCE(lf.latest_hfq, 1) AS qfq_factor
		FROM %s s
		INNER JOIN %s sc ON s.symbol = sc.symbol AND sc.class = 'etf'
		LEFT JOIN %s f ON s.symbol = f.symbol AND s.date = f.date
		LEFT JOIN latest_factors lf ON s.symbol = lf.symbol
		LEFT JOIN %s b ON s.symbol = b.symbol AND s.date = b.date
	`,
		model.ViewETFQFQ,
		model.TableAdjustFactor.TableName,
		model.TableKlineDaily.TableName,
		model.TableSymbolClass.TableName,
		model.TableAdjustFactor.TableName,
		model.TableBasicDaily.TableName,
	)
	_, err := d.db.Exec(query)
	return err
}
