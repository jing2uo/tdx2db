# tdx2db - 简单可靠的 A 股行情数据库

## 概述

`tdx2db` 可以将通达信数据导入到 DuckDB 中。

使用 DuckDB 中数据的代码示例见: [ko_trading](https://github.com/jing2uo/ko_trading)

## 亮点

- **快速运行**：Go 语言实现，全量导入不到 6s
- **增量更新**：支持增量更新数据
- **复权计算**：视图 v_qfq_stocks 存放了前复权行情数据
- **换手率和市值**：视图 v_turnover 存放了换手率和市值信息
- **使用通达信券商数据**：稳定可靠，不用买积分或被限流
- **单文件无依赖**：程序和数据库都只有一个文件

## 安装说明

### 使用 Docker 或 podman

项目会利用 github action 构建容器镜像，windows 和 mac 可以通过 docker 或 podman 使用:

```bash
docker run --rm --platform=linux/amd64 ghcr.io/jing2uo/tdx2db:latest -h
```

### 二进制安装

从 [releases](https://github.com/jing2uo/tdx2db/releases) 下载对应系统的二进制文件，解压后移至 `$PATH`，二进制仅支持在 Linux 中直接使用：

```bash
sudo mv tdx2db /usr/local/bin/
tdx2db -h # 验证安装
```

## 使用方法

### 初始化

首次使用必须先全量导入历史数据，可以从 [通达信券商数据](https://www.tdx.com.cn/article/vipdata.html) 下载**沪深京日线数据完整包**使用。

Linux 或 mac ：

```shell
mkdir vipdoc
wget https://data.tdx.com.cn/vipdoc/hsjday.zip && unzip -q hsjday.zip -d vipdoc

# docker
docker run --rm --platform=linux/amd64 \
  -v "$(pwd)":/data \
  ghcr.io/jing2uo/tdx2db:latest \
  init --dayfiledir /data/vipdoc --dbpath /data/tdx.db

# Linux 二进制
tdx2db init --dayfiledir vipdoc --dbpath tdx.db
```

Windows powershell ：

```shell
# 下载文件
Invoke-WebRequest -Uri "https://data.tdx.com.cn/vipdoc/hsjday.zip" -OutFile "hsjday.zip"
# 解压文件
Expand-Archive -Path "hsjday.zip" -DestinationPath "vipdoc" -Force
# 执行 init
docker run --rm --platform=linux/amd64 \
  -v "${PWD}:/data" \
  ghcr.io/jing2uo/tdx2db:latest \
  init --dayfiledir /data/vipdoc --dbpath /data/tdx.db
```

示例输出:

```shell
🛠 开始转换 dayfiles 为 CSV
🔥 转换完成
📊 股票数据导入成功
✅ 处理完成，耗时 5.007506252s
```

运行结束后 tdx.db 会在当前工作目录，和 vipdoc 在同一级， hsjday.zip 和 vipdoc 初始化后可删除。

**必填参数**：

- `--dayfiledir`：通达信 .day 文件所在目录路径
- `--dbpath`：DuckDB 数据库文件路径

### 增量更新

cron 命令会更新数据库至最新日期，包括股票数据、股本变迁数据 (gbbq)，并计算前收盘价和复权因子。

初次使用时，请在 init 后立刻执行一次 cron，以获得复权相关数据。

```bash
# 二进制安装运行
tdx2db cron --dbpath tdx.db

# 通过 docker 运行
docker run --rm --platform=linux/amd64 \
  -v "$(pwd)":/data \
  ghcr.io/jing2uo/tdx2db:latest \
  cron --dbpath /data/tdx.db

# windows docker 运行
docker run --rm --platform=linux/amd64 \
  -v "${PWD}:/data" \
  ghcr.io/jing2uo/tdx2db:latest \
  cron --dbpath /data/tdx.db


# 示例输出
📅 日线数据的最新日期为 2025-11-07
🛠 开始下载日线数据
🌲 无需下载
🛠 开始下载股本变迁数据
🔄 更新除权除息数据视图 (v_xdxr)
🔄 更新市值换手数据视图 (v_turnover)
📈 股本变迁数据更新成功
📟 计算所有股票前收盘价
🔢 复权因子导入成功
🔄 更新前复权数据视图 (v_qfq_stocks)
✅ 处理完成，耗时 14.386134606s
```

**必填参数**：

- `--dbpath`：DuckDB 数据库文件路径（使用 init 时创建的文件，db 文件可以移动，通过路径能找到即可）

### 前复权价查询

**v_qfq_stocks** 保存了前复权数据：

```sql
select * from v_qfq_stocks where symbol='sz000001' order by date;
```

**raw_adjust_factor** 保存了前收盘价和前复权因子，可以根据前收盘价拓展其他复权算法：

```sql
select * from raw_adjust_factor where symbol='sz000001';
```

复权原理参考：[点击查看](https://www.yuque.com/zhoujiping/programming/eb17548458c94bc7c14310f5b38cf25c#djL6L) , 算法来自 QUANTAXIS，复权结果和雪球、新浪两家结果一致，和同花顺及常见券商的结果不一致。

### 导出 Qlib 需要的 CSV

Qlib 需要 "sh000001.csv" 命名的日线文件，前复权因子会变化需要单独导出。

--fromdate 是可选参数，会导出日期后（不包含当天）的股票日线，不填时全量导出，factor 始终全量导出。

```shell
docker run --rm --platform=linux/amd64 --entrypoint "" \
  -v "$(pwd)":/data \
  ghcr.io/jing2uo/tdx2db:latest \
  /export_for_qlib --db-path /data/tdx.db --output /data/aabb --fromdate 2024-01-01

# 示例输出
数据过滤启用: date > 2024-01-01
导出 DuckDB 数据中...
拆分: /data/aabb/factor.csv → /data/aabb/factor
拆分: /data/aabb/data.csv → /data/aabb/data
清理中间文件：/data/aabb/factor.csv, /data/aabb/data.csv
完成 ✅ 输出目录: /data/aabb

# Linux 可以直接下载项目根目录下的 export_for_qlib 使用，依赖 duckdb 和 awk
./export_for_qlib --db-path tdx.db --output aabb --fromdate 2024-01-01
```

运行结束后当前目录会有 aabb 文件夹，里面有 data (股票日线 csv) 和 factor(全量复权因子 csv)，使用 dump_bin.py 处理即可。

在 [ko_trading](https://github.com/jing2uo/ko_trading) 中有可执行的范例。

### 表简介

raw\_ 前缀的表名用于存储基础数据，v\_ 前缀的表名是视图

- raw_adjust_factor: 前收盘价和前复权因子
- raw_gbbq：股本变迁数据
- raw_stocks_daily： 股票日线
- v_qfq_stocks：前复权股票日线
- v_xdxr：股票除权除息记录视图
- v_turnover：换手率和市值信息

## 备份

1. 可以直接复制一份 db 文件，简单快捷
2. 可以用 duckdb 命令导出行情数据为 parquet

duckdb 命令使用：

```bash
# 导出 stocks 表到 stocks.parquet
duckdb tdx.db -s "copy (select * from raw_stocks_daily) to 'stocks.parquet' (format parquet, compression 'zstd')"

# 从 stocks.parquet 重新建表
duckdb new.db -s "create table raw_stocks_daily as select * from read_parquet('stocks.parquet');"
```

## TODO

- [ ] 导入到 clickhouse、questdb 等数据库

## 欢迎 issue 和 pr

有任何使用问题都可以开 issue 讨论，也期待 pr~
