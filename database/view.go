package database

import (
	"database/sql"
	"fmt"
)

var qfq_view_name = "v_qfq_stocks"
var ma_view_name = "v_ma"
var volume_ratio_view_name = "v_volume_ratio"
var atr_view_name = "v_atr"

func CreateQfqView(db *sql.DB) error {
	query := fmt.Sprintf(`
	CREATE OR REPLACE VIEW %s AS
	SELECT
		s.symbol,
		s.date,
		s.volume,
		s.amount,
		ROUND(s.open * f.factor, 2) AS open,
		ROUND(s.high * f.factor, 2) AS high,
		ROUND(s.low * f.factor, 2) AS low,
		ROUND(s.close * f.factor, 2) AS close,
	FROM %s s
	JOIN %s f ON s.symbol = f.symbol AND s.date = f.date
	`, qfq_view_name, StocksSchema.Name, FactorSchema.Name)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create or replace view %s: %w", qfq_view_name, err)
	}
	return nil
}

func CreateMaView(db *sql.DB) error {
	query := fmt.Sprintf(`
	CREATE OR REPLACE VIEW %s AS
	SELECT
	    symbol,
	    date,
	    close,
	    -- 计算10周期移动平均线
	    AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) AS ma10,

	    -- 计算20周期移动平均线
	    AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS ma20,

	    -- 计算60周期移动平均线
	    AVG(close) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) AS ma60
	FROM %s;`,
		ma_view_name, qfq_view_name)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create or replace view %s: %w", ma_view_name, err)
	}
	return nil
}

func CreateVolumeRatioView(db *sql.DB) error {
	query := fmt.Sprintf(`
	CREATE OR REPLACE VIEW %s AS
	WITH daily_volume_with_moving_average AS (
	    SELECT
	        "date",                                     -- 日期
	        symbol,                                     -- 股票代码
	        volume,                                     -- 当日成交量
	        AVG(volume) OVER (
	            PARTITION BY symbol
	            ORDER BY "date"
	            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
	        ) AS avg_volume_5d
	    FROM
	        %s
	)
	-- 从CTE中查询并计算最终的量比
	SELECT
	    "date",
	    symbol,
	    volume AS current_volume,
	    avg_volume_5d,
	    -- 计算量比：当日成交量 / 过去5日平均成交量
	    -- 使用 CASE 来处理 avg_volume_5d 为 0 或 NULL 的情况，避免计算出错
	    CASE
	        WHEN avg_volume_5d IS NULL OR avg_volume_5d = 0 THEN NULL
	        ELSE (volume * 1.0) / avg_volume_5d -- 乘以1.0确保进行浮点数除法
	    END AS volume_ratio
	FROM
	    daily_volume_with_moving_average
	ORDER BY
	    symbol,
	    "date";
	`,
		volume_ratio_view_name, qfq_view_name)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create or replace view %s: %w", ma_view_name, err)
	}
	return nil
}

func CreateAtrView(db *sql.DB) error {
	query := fmt.Sprintf(`
	CREATE OR REPLACE VIEW %s AS
	WITH
	-- 步骤 1: 计算每日的真实波幅 (True Range, TR)
	true_range AS (
	    SELECT
	        symbol,
	        date,
	        -- TR 是以下三者中的最大值:
	        -- 1. 当日最高价与最低价的差值
	        -- 2. 当日最高价与昨日收盘价差值的绝对值
	        -- 3. 当日最低价与昨日收盘价差值的绝对值
	        GREATEST(
	            high - low,
	            ABS(high - LAG(close, 1, close) OVER (PARTITION BY symbol ORDER BY date)),
	            ABS(low - LAG(close, 1, close) OVER (PARTITION BY symbol ORDER BY date))
	        ) AS tr
	    FROM
	        %s
	)
	-- 步骤 2: 对TR进行14周期移动平均，得到ATR，并格式化输出
	SELECT
	    symbol,
	    date,
	    -- ATR是TR的14周期简单移动平均值
	    -- 按照惯例，将结果四舍五入到4位小数
	    ROUND(
	        AVG(tr) OVER (PARTITION BY symbol ORDER BY date ROWS BETWEEN 13 PRECEDING AND CURRENT ROW),
	        4
	    ) AS atr14
	FROM
	    true_range;`,
		atr_view_name, qfq_view_name)

	_, err := db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to create or replace view %s: %w", atr_view_name, err)
	}
	return nil
}
