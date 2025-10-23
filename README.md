# tdx2db - 通达信数据导入到 DuckDB

## 概述

`tdx2db` 是一个命令行工具。

用于将通达信数据导入并更新至 DuckDB 数据库，支持股票历史数据的全量初始化和增量更新。

使用 DuckDB 中数据的代码示例见: [quant-base](https://github.com/jing2uo/quant-base)

## 功能特性

- **快速运行**：Go 并发处理，全量导入不到 7s（Ultra 5 228V + 32G 供参考）
- **增量更新**：支持每天或隔几天增量更新数据
- **复权计算**：视图 stocks_qfq 存放了前复权后的行情数据，自动更新
- **使用通达信券商数据**：收盘后更新，不用频繁发起 api 请求，稳定可靠
- **单文件无依赖**：打包通达信数据处理工具 datatool 在程序内部执行

## 安装说明

### 使用 Docker 或 podman

项目会利用 github action 构建容器镜像，windows 和 mac 可以通过 docker 或 podman 使用:

```bash
docker run --rm --platform=linux/amd64 ghcr.io/jing2uo/tdx2db:latest -h
```

### Linux 二进制安装

从 [releases](https://github.com/jing2uo/tdx2db/releases) 下载对应系统的二进制文件，解压后移至 `$PATH`：

```bash
sudo mv tdx2db /usr/local/bin/
tdx2db -h # 验证安装
```

## 使用方法

### 初始化数据库

首次使用必须先全量导入历史数据，可以从 [通达信券商数据](https://www.tdx.com.cn/article/vipdata.html) 下载**沪深京日线数据完整包**使用：

```bash
# 以下命令在终端执行

wget https://data.tdx.com.cn/vipdoc/hsjday.zip  # 沪深京日线数据完整包，可以使用浏览器下载

mkdir vipdoc
unzip -q hsjday.zip -d vipdoc

# 二进制安装运行
tdx2db init --dbpath tdx.db --dayfiledir vipdoc

# 通过 docker 运行，运行结束后 tdx.db 会在当前工作目录，和 vipdoc 目录在同一级
docker run --rm --platform=linux/amd64 -v "$(pwd)":/data ghcr.io/jing2uo/tdx2db:latest init --dayfiledir /data/vipdoc --dbpath /data/tdx.db

# 示例输出
🛠 开始转换 dayfiles 为 CSV
🔥 转换完成
📊 股票数据导入成功
✅ 处理完成，耗时 7.283595071s

# rm -r hsjday.zip ~/vipdoc  # 初始化后可以删除
```

**必填参数**：

- `--dayfiledir`：通达信 .day 文件所在目录路径（如`/TDX/vipdoc/`）
- `--dbpath`：DuckDB 数据库文件路径（不存在时将创建）可以使用任意路径和名字

### 增量更新

cron 命令会更新数据库至最新日期，包括股票数据、股本变迁数据 (gbbq)，并计算前收盘价和复权因子。

初次使用时，请在 init 后立刻执行一次 cron，以获得复权相关数据。

```bash
# 二进制安装运行
tdx2db cron --dbpath ~/tdx.db

# 通过 docker 运行
docker run --rm --platform=linux/amd64 -v "$(pwd)":/data ghcr.io/jing2uo/tdx2db:latest cron --dbpath /data/tdx.db

# 示例输出
📅 数据库中日线数据的最新日期为 2025-10-22
🛠 开始下载日线数据
🌲 无需下载
🛠 开始下载除权除息数据
📈 除权除息数据更新成功
✅ 处理完成，耗时 4.061312713s
📟 计算所有股票的前收盘价
🔢 导入前收盘价和复权因子
🔄 创建/更新前复权数据视图 (stocks_qfq)
✅ 处理完成，耗时 11.739020832s
```

**必填参数**：

- `--dbpath`：DuckDB 数据库文件路径（使用 init 时创建的文件，db 文件可以移动，通过路径能找到即可）

### 前复权价查询

stocks_qfq 视图保存了前复权数据，执行 factor 和 cron 子命令时视图会自动更新：

```sql
select * from stocks_qfq where symbol='sz000001'; # 平安银行
```

factor 表中保存了计算好的前收盘价和前复权因子，可以根据前收盘价自行拓展其他复权算法：

```sql
select * from factor where symbol='sz000001';
```

复权原理，[点击查看参考链接](https://www.yuque.com/zhoujiping/programming/eb17548458c94bc7c14310f5b38cf25c#djL6L) 。

### 子命令简介

- completion：生成 tab 补全需要的文件
- init：从 tdx 数据初始化 db
- cron: 用于每日更新，会顺序执行下方的 upadte 和 factor 命令
- update：更新行情数据和 GBBQ (股本变迁)数据到最新交易日
- factor：根据 GBBQ 计算前收盘价和前复权因子，并刷新 stocks_qfq 视图

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

- 通达信每日数据不是收盘后立即更新，下午 5 点后是合适的时间
- 确保日志文件有写入权限

## 备份

1. 可以直接复制一份 db 文件，简单快捷
2. 可以用 duckdb 命令导出行情数据为 parquet

duckdb 命令使用：

```bash
# 导出 stocks 表中所有数据到 stocks.parquet
duckdb ~/tdx.db -s "COPY (SELECT * FROM stocks) TO 'stocks.parquet' (FORMAT PARQUET, COMPRESSION 'ZSTD')"

duckdb ~/tdx.db # 此处直接回车进入 duckdb 的交互终端

# 从 stocks.parquet 重新建表
create table  stocks as select * from read_parquet('stocks.parquet');

# CTRL+D 退出 duckdb
```

## TODO

- [x] 前收盘价和复权因子计算
- [ ] 导入到 clickhouse、questdb 等数据库

## 欢迎 issue 和 pr

有任何使用问题都可以开 issue 讨论，也期待 pr，如果项目有帮到你可以点个 star ~

---
