# tdx2db - 通达信数据导入到 DuckDB

## 概述

`tdx2db` 是一个命令行工具。

用于将通达信数据导入并更新至 DuckDB 数据库，支持股票历史数据的全量初始化和增量更新。

## 功能特性

- **快速运行**：Go 并发处理，全量导入仅需 10s（Ultra 5 228V + 32G 供参考）
- **增量更新**：支持每天或隔几天增量更新数据
- **使用通达信券商数据**：收盘后更新，不用频繁发起 api 请求，稳定可靠
- **单文件无依赖**：打包通达信数据处理工具 datatool 在程序内部执行

## 安装说明

1. 从 [releases](https://github.com/jing2uo/tdx2db/releases) 下载对应系统的二进制文件，解压后移至 `$PATH`：

   ```bash
   sudo mv tdx2db /usr/local/bin/
   ```

2. 验证安装：

   ```bash
   tdx2db -h
   ```

## 使用方法

### 初始化数据库

首次使用必须先全量导入历史数据，使用通达信软件目录时请先下载全量数据：

```shell
tdx2db init --dbpath /数据库路径/数据库名.db --dayfiledir /通达信/vipdoc/

# 示例输出
🛠 开始转换 dayfiles 为 CSV
🔥 转换完成
📊 股票数据导入成功
🛠 开始下载除权除息数据
📈 除权除息数据更新成功
✅ 处理完成，耗时 10.518574975s
```

也可以从 [通达信券商数据](https://www.tdx.com.cn/article/alldata.html) 下载 **上证所有证券日线(TCKV4=0)** 、**深证所有证券日线(TCKV4=0)** 、**北证所有证券日线** 后使用，下载前从网页上检查下三个文件时间是否是同一天：

```bash
wget https://www.tdx.com.cn/products/data/data/vipdoc/shlday.zip # 上证日线
wget https://www.tdx.com.cn/products/data/data/vipdoc/szlday.zip # 深证日线
wget https://www.tdx.com.cn/products/data/data/vipdoc/bjlday.zip # 北证日线

mkdir ~/vipdoc
unzip -q shlday.zip -d ~/vipdoc
unzip -q szlday.zip -d ~/vipdoc
unzip -q bjlday.zip -d ~/vipdoc

tdx2db init --dbpath ~/tdx.db --dayfiledir ~/vipdoc

# rm -r shlday.zip szlday.zip bjlday.zip ~/vipdoc  # 初始化后可以删除
```

**必填参数**：

- `--dayfiledir`：通达信 .day 文件所在目录路径（如`/TDX/vipdoc/`）
- `--dbpath`：DuckDB 数据库文件路径（不存在时将创建）可以使用任意路径和名字

### 增量更新数据

更新数据库至最新日期，包括股票数据、股本变迁数据 (gbbq):

```bash
tdx2db update --dbpath /数据库路径/数据库名.db

# 示例输出
📅 数据库中日线数据的最新日期为 2025-06-11
🛠 开始下载日线数据
✅ 成功下载 20250612 的数据
🔥 成功转换为 CSV
📊 股票数据导入成功
🛠 开始下载除权除息数据
📈 除权除息数据更新成功
✅ 处理完成，耗时 12.74528605s
```

**必填参数**：

- `--dbpath`：DuckDB 数据库文件路径（需使用 init 时创建的文件）

### 前收盘价和复权因子

根据股本变迁数据 (gbbq) 计算所有股票除权除息后的前收盘价和前复权因子:

```shell
tdx2db factor --dbpath /数据库路径/数据库名.db

# 示例输出
📟 计算所有股票的前收盘价和复权因子
🔢 导入前收盘价和复权因子
✅ 处理完成，耗时 9.675334127s
```

**必填参数**：

- `--dbpath`：DuckDB 数据库文件路径（需使用 init 时创建的文件）

**注意事项**：

- 调用 factor 子命令时总是重新计算，清空 factor 表后导入。

### 查询复权价格

duckdb 提供了二进制命令行工具，也可以使用你喜欢的其他 sql 软件查询。

聚合 `stocks` 和 `factor` 表查询数据：

```sql
# duckdb /数据库路径/数据库名.db

SELECT s.*, f.pre_close, f.factor
  FROM stocks s
  INNER JOIN factor f
  ON s.symbol = f.symbol AND s.date = f.date
  WHERE s.symbol = 'sh603893' ORDER BY s.date;

```

查询前复权价格：

```sql
SELECT
      s.symbol,
      s.date,
      s.volume,
      s.amount,
      ROUND(s.open * f.factor, 2) AS open,
      ROUND(s.high * f.factor, 2) AS high,
      ROUND(s.low * f.factor, 2) AS low,
      ROUND(s.close * f.factor, 2) AS close
  FROM stocks s
  INNER JOIN factor f
  ON s.symbol = f.symbol AND s.date = f.date
  WHERE s.symbol = 'sh603893'
  ORDER BY s.date;
```

**复权原理**：

```python
    # 计算复权前的前一日收盘价
    data["pre_close"] = (
        (data["close"].shift(1) * 10 - data["fenhong"])
        + (data["peigu"] * data["peigujia"])
    ) / (10 + data["peigu"] + data["songzhuangu"])

    # 计算前复权因子 (qfq_adj)
    data["qfq_adj"] = (
        (data["pre_close"].shift(-1) / data["close"]).fillna(1)[::-1].cumprod()
    )

    # 计算后复权因子 (hfq_adj)，后复权因子暂时没实现
    data["hfq_adj"] = (
        (data["close"] / data["pre_close"].shift(-1)).cumprod().shift(1).fillna(1)
    )

    # 计算前复权价格
    if qfq:
        for col in ["open", "high", "low", "close", "pre_close"]:
            data[col] = data[col] * data["qfq_adj"]
            data[col] = round(data[col], 2)
        data.drop(["qfq_adj", "hfq_adj"], axis=1, inplace=True)
```

## 自动运行

Linux 下通过 cron 实现每日 17:00 自动更新：

1. 编辑定时任务：

```bash
crontab -e
```

2. 添加以下内容（请替换实际路径）：

```shell
# 更新股票和股本变迁数据，计算前收盘价和复权因子
00 17 * * * tdx2db cron --dbpath /数据库路径/数据库名.db >> /日志路径/tdx2db.log 2>&1
```

**注意事项**：

- cron 命令顺序调用 update 和 factor
- 通达信每日数据不是收盘后立即更新，下午 5 点后是合适的时间
- 确保日志文件有写入权限

## TODO

- [x] 前收盘价和复权因子计算
- [x] 使用 github release 发布二进制
- [x] 增加新的命令用于 cron
- [ ] 导入到 clickhouse、questdb 等数据库
- [ ] Windows 支持

---
