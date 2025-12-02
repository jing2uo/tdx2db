# tdx2db - 简单可靠的 A 股行情数据库

## 概述

`tdx2db` 可以将通达信数据导入到 DuckDB 中。

## 亮点

- **快速运行**：Go 语言实现，全量导入不到 6s
- **增量更新**：支持间隔数天后数据补全
- **分时数据**：增量更新可选导入 1min 和 5min 分时数据
- **复权计算**：增量更新会自动计算前后复权因子和行情
- **换手率和市值**：视图 v_turnover 存放了换手率和市值信息
- **使用通达信数据**：稳定可靠，不用买积分或被限流
- **单文件无依赖**：程序和数据库都只有一个文件

## 安装说明

### 使用 docker 或 podman

项目会利用 github action 构建容器镜像，windows 和 mac 可以通过 docker 或 podman 使用:

```bash
docker run --rm --platform=linux/amd64 ghcr.io/jing2uo/tdx2db:latest -h
```

### 二进制安装

从 [releases](https://github.com/jing2uo/tdx2db/releases) 下载，解压后移至 `$PATH`，二进制**仅支持在 Linux 中**直接使用：

```bash
sudo mv tdx2db /usr/local/bin/ && tdx2db -h
```

## 导入到 DuckDB

### 初始化

首次使用需要全量导入历史数据，可以从 [通达信券商数据](https://www.tdx.com.cn/article/vipdata.html) 下载**沪深京日线数据完整包**。

下载文件：

```shell
# linux mac
mkdir -p vipdoc
wget https://data.tdx.com.cn/vipdoc/hsjday.zip && unzip -q hsjday.zip -d vipdoc
# 若 unzip 解压后文件名如 sh\lday\sh000001.day，可以批量重命名
# cd vipdoc
# for f in *.day; do mv "$f" "${f##*\\}"; done

# windows powershell
Invoke-WebRequest -Uri "https://data.tdx.com.cn/vipdoc/hsjday.zip" -OutFile "hsjday.zip"
Expand-Archive -Path "hsjday.zip" -DestinationPath "vipdoc" -Force
```

二进制：

```shell
tdx2db init --dayfiledir vipdoc --dbpath tdx.db
```

docker 或 podman：

```shell
# linux、mac docker
docker run --rm --platform=linux/amd64 -v "$(pwd)":/data \
  ghcr.io/jing2uo/tdx2db:latest \
  init --dayfiledir /data/vipdoc --dbpath /data/tdx.db

# windows docker
docker run --rm --platform=linux/amd64 -v "${PWD}:/data" \
  ghcr.io/jing2uo/tdx2db:latest \
  init --dayfiledir /data/vipdoc --dbpath /data/tdx.db

# 后续不再提示 docker 用法
# 根据二进制示例修改第三行命令即可
```

运行结束后 tdx.db 会在当前工作目录，和 vipdoc 在同一级， hsjday.zip 和 vipdoc 初始化后可删除。

**必填参数**：

- `--dayfiledir`：通达信 .day 文件所在目录路径
- `--dbpath`：DuckDB 数据库文件路径

### 增量更新

cron 命令会更新数据库至最新日期，包括股票数据、股本变迁数据 (gbbq)，并计算前收盘价和复权因子。

初次使用时，请在 init 后立刻执行一次 cron，以获得复权相关数据。

```bash
tdx2db cron --dbpath tdx.db
```

**必填参数**：

- `--dbpath`：DuckDB 数据库文件路径（使用 init 时创建的文件，db 文件可以移动，通过路径能找到即可）

### 分时数据

cron 命令支持 1min 和 5min 分时数据导入

```bash
# --minline 可选 1、5、1,5 ，分别表示只处理1分钟、只处理5分钟、两种都处理
tdx2db cron --dbpath tdx.db --minline 1,5
```

**注意**

1. 分时数据下载和导入比较耗时，数据量极大，确认需要再开启
2. 历史分时数据通达信没提供，请自行检索后使用 duckdb 导入
3. 更新分时间隔超过 30 天以上，需手动补齐数据后才能继续处理
4. 股票代码变更不会处理历史记录

### 表查询

raw\_ 前缀的表名用于存储基础数据，v\_ 前缀的表名是视图

- raw_adjust_factor: 前收盘价和前复权因子
- raw_gbbq：股本变迁数据
- raw_stocks_daily： 股票日线
- raw_stocks_1min: 1 分钟 K 线(cron 导入后才有)
- raw_stocks_5min: 5 分钟 K 线(cron 导入后才有)
- v_qfq_stocks：前复权股票日线
- v_hfq_stocks：后复权股票日线
- v_xdxr：股票除权除息记录
- v_turnover：换手率和市值信息

复权数据：

```sql
# 前复权
select * from v_qfq_stocks where symbol='sz000001' order by date;

# 后复权
select * from v_hfq_stocks where symbol='sz000001' order by date;
```

前收盘价和复权因子，复权因子支持分时数据使用，可以根据前收盘价拓展其他复权算法：

```sql
select * from raw_adjust_factor where symbol='sz000001';
```

算法来自 QUANTAXIS，原理参考：[点击查看](https://www.yuque.com/zhoujiping/programming/eb17548458c94bc7c14310f5b38cf25c#djL6L)

复权结果和 QUANTAXIS、通达信等比复权一致；其中前复权结果和雪球、新浪也一致。

分时表字段和类型如下：
| symbol | open | high | low | close | amount | volume | datetime |
|:--------|:------|:------|:------|:------|:--------|:--------|:----------------|
| varchar | double | double | double | double | double | int64 | timestamp |

## 通达信数据转 CSV

convert 命令支持转换通达信 .day .01 .5 文件、四代行情 zip、四代 TIC zip 到 csv，四代数据可以在 [每日数据](https://www.tdx.com.cn/article/daydata.html) 下载。

```shell
tdx2db convert --output ./ --dayfiledir vipdoc       # 转换 .day 日线文件
tdx2db convert --output ./ --m1filedir vipdoc        # 转换 .01 1分钟线文件
tdx2db convert --output ./ --m5filedir vipdoc        # 转换 .5  5分钟线文件
tdx2db convert --output ./ --ticzip 20251110.zip     # 转换四代 TIC
tdx2db convert --output ./ --dayzip 20251111.zip     # 转换四代行情
tdx2db convert --output ./ --gbbqzip gbbq.zip        # 转换股本变迁数据
```

转换会查找目录中所有文件，包含指数、概念等很多非股票的记录，空文件会跳过处理。

## 备份

1. 可以直接复制一份 db 文件，简单快捷
2. 可以用 duckdb 命令导出行情数据为 parquet 或 csv

duckdb 命令使用：

```bash
# 导出 stocks 表
duckdb tdx.db -s "copy (select * from raw_stocks_daily order by symbol,date) to 'stocks.parquet' (format parquet, compression 'zstd')"

duckdb tdx.db -s "copy (select * from raw_stocks_daily order by symbol,date) to 'stocks.csv' (format csv)"

# 从 paruet 或 csv 建表
duckdb new.db -s "create table raw_stocks_daily as select * from read_parquet('stocks.parquet');"

duckdb new.db -s "create table raw_stocks_daily as select * from read_csv('stocks.csv');"
```

## 欢迎 issue 和 pr

有任何使用问题都可以开 issue 讨论，也期待 pr~
