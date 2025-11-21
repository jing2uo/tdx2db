# 5分钟线复权视图实现文档

## 实现概述

在现有的 `tdx2db` 项目中成功添加了5分钟线的前复权（QFQ）和后复权（HFQ）视图功能。

## 修改的文件

### 1. database/stock.go

#### 新增变量
```go
var Qfq5MinViewName = "v_qfq_stocks_5min"
var Hfq5MinViewName = "v_hfq_stocks_5min"
```

#### 新增函数

**Create5MinQfqView(db *sql.DB) error**
- 创建5分钟前复权视图 `v_qfq_stocks_5min`
- 复用现有的 `raw_adjust_factor` 表中的前复权因子
- 使用 `DATE(s.datetime) = f.date` 关联分钟线数据与日线复权因子

**Create5MinHfqView(db *sql.DB) error**
- 创建5分钟后复权视图 `v_hfq_stocks_5min`
- 复用现有的 `raw_adjust_factor` 表中的后复权因子
- 使用相同的日期关联逻辑

#### 核心SQL逻辑

前复权视图：
```sql
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
```

后复权视图：
```sql
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
```

### 2. cmd/cron.go

在现有的日线复权视图更新逻辑后（第66行之后），添加了5分钟视图的条件更新：

```go
// 更新5分钟复权视图（当minline参数包含5时）
if minline != "" && strings.Contains(minline, "5") {
    fmt.Printf("🔄 更新5分钟前复权数据视图 (%s)\n", database.Qfq5MinViewName)
    if err := database.Create5MinQfqView(db); err != nil {
        return fmt.Errorf("failed to create 5min qfq view: %w", err)
    }

    fmt.Printf("🔄 更新5分钟后复权数据视图 (%s)\n", database.Hfq5MinViewName)
    if err := database.Create5MinHfqView(db); err != nil {
        return fmt.Errorf("failed to create 5min hfq view: %w", err)
    }
}
```

## 功能特性

### 1. 复权因子复用
- 完全复用现有的日线复权因子，无需重新计算
- 确保不同时间周期的复权数据一致性

### 2. 条件更新逻辑
- 只有当 `--minline` 参数包含 "5" 时才更新5分钟视图
- 支持以下参数格式：
  - `--minline 5` ✅ 更新5分钟视图
  - `--minline 1,5` ✅ 更新5分钟视图
  - `--minline 5,1` ✅ 更新5分钟视图
  - `--minline 1` ❌ 不更新5分钟视图
  - 无参数 ❌ 不更新5分钟视图

### 3. 数据一致性
- 使用 `DATE(s.datetime) = f.date` 正确关联分钟线时间戳与日线日期
- 保持与现有代码完全一致的错误处理和日志输出格式

## 使用示例

### 命令行使用
```bash
# 只更新5分钟数据和视图
tdx2db cron --dbpath tdx.db --minline 5

# 同时更新1分钟和5分钟数据和视图
tdx2db cron --dbpath tdx.db --minline 1,5

# 只更新日线数据（不更新分钟视图）
tdx2db cron --dbpath tdx.db
```

### SQL查询示例
```sql
-- 查询5分钟前复权数据
SELECT * FROM v_qfq_stocks_5min
WHERE symbol = 'sz000001'
ORDER BY datetime DESC
LIMIT 100;

-- 查询5分钟后复权数据
SELECT * FROM v_hfq_stocks_5min
WHERE symbol = 'sz000001'
ORDER BY datetime DESC
LIMIT 100;
```

## 优势

1. **性能高效**：复用现有复权因子，无需重复计算
2. **逻辑一致**：与现有分钟线数据处理逻辑完全一致
3. **用户可控**：通过命令行参数精确控制功能启用
4. **代码简洁**：最小化修改，保持现有架构不变
5. **数据准确**：确保复权数据在不同时间周期间的一致性

## 测试验证

项目包含以下测试文件：
- `test_5min_views.sql`：SQL测试脚本
- `test_logic.go`：Go逻辑测试脚本

## 预期输出示例

当执行 `tdx2db cron --minline 5` 时，将看到：
```
🔄 更新前复权数据视图 (v_qfq_stocks)
🔄 更新后复权数据视图 (v_hfq_stocks)
🔄 更新5分钟前复权数据视图 (v_qfq_stocks_5min)
🔄 更新5分钟后复权数据视图 (v_hfq_stocks_5min)
🚀 今日任务执行成功
```