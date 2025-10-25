CREATE OR REPLACE VIEW v_atr AS
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
        v_qfq_stocks
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
    true_range;
