CREATE OR REPLACE VIEW v_boll AS
SELECT
    symbol,
    "date",
    "close",
    ma20 AS middle_band,
    ma20 + (2 * STDDEV_SAMP("close") OVER (PARTITION BY symbol ORDER BY "date" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)) AS upper_band,
    ma20 - (2 * STDDEV_SAMP("close") OVER (PARTITION BY symbol ORDER BY "date" ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)) AS lower_band
FROM
    v_ma;
