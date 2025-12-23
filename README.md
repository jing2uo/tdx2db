# tdx2db - 获得专属的 A 股行情数据库

[![GitHub release](https://img.shields.io/github/v/release/jing2uo/tdx2db?style=flat-square)](https://github.com/jing2uo/tdx2db/releases)
[![Docker Image](https://img.shields.io/badge/docker-pull-blue?style=flat-square&logo=docker)](https://github.com/jing2uo/tdx2db/pkgs/container/tdx2db)
[![License](https://img.shields.io/github/license/jing2uo/tdx2db?style=flat-square)](LICENSE)

## 概述

`tdx2db` 是一个高效的工具，用于将通达信数据导入本地数据库，支持 DuckDB 和 ClickHouse。

## 亮点

- **增量更新**: 支持间隔数天后数据补全，维护简单
- **分时数据**: 支持导入 1min 和 5min 分时数据
- **复权计算**: 自动计算前后复权因子，且因子支持分时使用
- **衍生指标**: 自动计算换手率和市值信息
- **稳定可靠**: 基于通达信数据，不依赖收费或限流接口

## 声明

- 代码不会向后兼容且会写出 bug，用于实盘前请谨慎检查数据正确性，不对你的损失负责。
- 如果导入了分时请保留原始数据并定期备份，数据更新出问题日线可以快速还原，分时很麻烦。

## 安装说明

### 使用二进制

从 [releases](https://github.com/jing2uo/tdx2db/releases) 下载，解压后移至 `$PATH`，二进制**仅支持在 x86 Linux 中**直接使用：

```bash
sudo mv tdx2db /usr/local/bin/ && tdx2db -h
```

### 使用 docker

项目会利用 github action 构建容器镜像，windows 和 mac 可以通过 docker 使用:

```bash
docker run --rm --platform=linux/amd64 ghcr.io/jing2uo/tdx2db:latest -h
```

## 导入到数据库

### 初始化

首次使用需要全量导入历史数据，可以从 [通达信券商数据](https://www.tdx.com.cn/article/vipdata.html) 下载**沪深京日线数据完整包**。

下载文件:

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

二进制:

```shell
  # 导入 DuckDB, dburi 格式： duckdb://[path]，path 支持相对路径
  tdx2db init --dburi 'duckdb://./tdx.db' --dayfiledir ./vipdoc

  # 导入 ClickHouse, dburi 格式： clickhouse:[user[:password]@][host][:port][/database][?http_port=value1&param2=value2&...]
  tdx2db init --dburi 'clickhouse://default:123456@localhost:9000/mydb?http_port=8123' --dayfiledir ./vipdoc

  # ClickHouse 有以下默认值: user=default, password="", port=9000, http_port=8123, database=default，可以根据情况简写
  tdx2db init --dburi 'clickhouse://localhost' --dayfiledir ./vipdoc
```

docker:

```shell
# linux、mac docker
docker run --rm --platform=linux/amd64 -v "$(pwd)":/data \
  ghcr.io/jing2uo/tdx2db:latest \
  init --dayfiledir /data/vipdoc --dburi 'duckdb:///data/tdx.db'

# windows docker
docker run --rm --platform=linux/amd64 -v "${PWD}:/data" \
  ghcr.io/jing2uo/tdx2db:latest \
  init --dayfiledir /data/vipdoc --dburi 'duckdb:///data/tdx.db'

# 后续不再提示 docker 用法, 根据二进制示例修改第三行命令即可
```

**必填参数**:

- `--dayfiledir`: 通达信 .day 文件所在目录
- `--dburi`: 数据库连接信息

### 增量更新

cron 命令会更新股票数据、股本变迁数据到最新日期，并计算前收盘价和复权因子。

初次使用时，请在 init 后立刻执行一次 cron，以获得复权相关数据。

```bash
tdx2db cron --dburi 'duckdb://tdx.db'    # ClickHouse schema 参考 init 部分
```

**必填参数**:

- `--dburi`: 数据库连接信息

### 分时数据

cron 命令支持 1min 和 5min 分时数据导入

```bash
# --minline 可选 1、5、1,5 ，分别表示只处理1分钟、只处理5分钟、两种都处理
tdx2db cron --dburi 'duckdb://tdx.db' --minline 1,5
```

**注意**

1. 分时数据下载和导入耗时，表数据量大
2. 通达信没提供历史分时数据，请自行检索后导入
3. 更新间隔超过 30 天以上，需手动补齐数据后才能继续处理
4. 股票代码变更不会处理历史记录

### 表查询

raw\_ 前缀的表名用于存储基础数据，v\_ 前缀的表名是视图

| 表/视图名           | 说明                   |
| :------------------ | :--------------------- |
| `raw_stocks_daily`  | 股票日线数据           |
| `raw_stocks_1min`   | 1 分钟 K 线            |
| `raw_stocks_5min`   | 5 分钟 K 线            |
| `raw_stocks_basic`  | 前收盘价、换手率与市值 |
| `raw_adjust_factor` | 复权因子表             |
| `v_qfq_daily`       | 前复权日线数据         |
| `v_hfq_daily`       | 后复权日线数据         |

复权数据，默认创建日线前后复权视图，如需分时参考 v_qfq_daily 调整即可：

```sql
# 前复权
select * from v_qfq_daily where symbol='sz000001' order by date;

# 后复权
select * from v_hfq_daily where symbol='sz000001' order by date;
```

复权算法来自 QUANTAXIS，原理参考：[点击查看](https://www.yuque.com/zhoujiping/programming/eb17548458c94bc7c14310f5b38cf25c#djL6L)，复权结果和 QUANTAXIS、通达信等比复权一致；其中前复权结果和雪球、新浪也一致。

## 通达信数据转 CSV

convert 命令支持转换通达信日线、分时文件和四代行情、TIC 压缩包到 CSV，四代数据可以在 [每日数据](https://www.tdx.com.cn/article/daydata.html) 下载。

```shell
tdx2db convert -t day -i ./vipdoc/ -o ./   # 转换 .day 日线文件
tdx2db convert -h   # 其他类型查看 help
```

日线转换会查找目录中所有文件，包括指数、板块等，分时只处理股票。

## 欢迎 issue 和 pr

有任何使用问题都可以开 issue 讨论，也期待 pr~
