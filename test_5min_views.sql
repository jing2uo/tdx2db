-- 测试5分钟复权视图的SQL语句
-- 这些语句可以用来验证创建的视图是否正确

-- 测试前复权视图创建
CREATE OR REPLACE VIEW v_qfq_stocks_5min AS
SELECT
    s.symbol,
    s.datetime,
    s.volume,
    s.amount,
    ROUND(s.open  * f.qfq_factor, 2) AS open,
    ROUND(s.high  * f.qfq_factor, 2) AS high,
    ROUND(s.low   * f.qfq_factor, 2) AS low,
    ROUND(s.close * f.qfq_factor, 2) AS close
FROM raw_stocks_5min s
JOIN raw_adjust_factor f ON s.symbol = f.symbol AND DATE(s.datetime) = f.date;

-- 测试后复权视图创建
CREATE OR REPLACE VIEW v_hfq_stocks_5min AS
SELECT
    s.symbol,
    s.datetime,
    s.volume,
    s.amount,
    ROUND(s.open  * f.hfq_factor, 2) AS open,
    ROUND(s.high  * f.hfq_factor, 2) AS high,
    ROUND(s.low   * f.hfq_factor, 2) AS low,
    ROUND(s.close * f.hfq_factor, 2) AS close
FROM raw_stocks_5min s
JOIN raw_adjust_factor f ON s.symbol = f.symbol AND DATE(s.datetime) = f.date;

-- 测试查询语句
-- 查看某个股票的5分钟前复权数据
SELECT * FROM v_qfq_stocks_5min WHERE symbol = 'sz000001' ORDER BY datetime DESC LIMIT 10;

-- 查看某个股票的5分钟后复权数据
SELECT * FROM v_hfq_stocks_5min WHERE symbol = 'sz000001' ORDER BY datetime DESC LIMIT 10;

-- 检查视图是否创建成功
SELECT table_name, table_type FROM information_schema.tables WHERE table_name LIKE '%5min%';