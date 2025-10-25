CREATE OR REPLACE VIEW v_volume_ratio AS
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
        v_qfq_stocks
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
