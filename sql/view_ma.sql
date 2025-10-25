CREATE OR REPLACE VIEW v_ma AS
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
FROM v_qfq_stocks;
