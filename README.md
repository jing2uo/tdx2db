# tdx2db - 通达信数据导入到 DuckDB

## 概述

`tdx2db` 是一个命令行工具。

用于将通达信数据导入并更新至 DuckDB 数据库，支持股票历史数据的全量初始化和增量更新。

使用 DuckDB 中数据的代码示例见: [quant-base](https://github.com/jing2uo/quant-base)

## 功能特性

- **快速运行**：Go 并发处理，全量导入不到 7s（Ultra 5 228V + 32G 供参考）
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
✅ 处理完成，耗时 6.518574975s
```

也可以从 [通达信券商数据](https://www.tdx.com.cn/article/alldata.html) 下载 **上证所有证券日线(TCKV4=0)** 、**深证所有证券日线(TCKV4=0)** 、**北证所有证券日线** 、**通达信板块指数日线** 后使用，下载前在网页上确认几个文件更新日期是否是同一天：

```bash
wget https://www.tdx.com.cn/products/data/data/vipdoc/shlday.zip # 上证日线
wget https://www.tdx.com.cn/products/data/data/vipdoc/szlday.zip # 深证日线
wget https://www.tdx.com.cn/products/data/data/vipdoc/bjlday.zip # 北证日线
wget https://www.tdx.com.cn/products/data/data/vipdoc/tdxzs_day.zip # 指数日线

mkdir ~/vipdoc
unzip -q shlday.zip -d ~/vipdoc
unzip -q szlday.zip -d ~/vipdoc
unzip -q bjlday.zip -d ~/vipdoc
unzip -q tdxzs_day.zip -d ~/vipdoc

tdx2db init --dbpath ~/tdx.db --dayfiledir ~/vipdoc

# rm -r shlday.zip szlday.zip bjlday.zip tdxzs_day.zip ~/vipdoc  # 初始化后可以删除
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

## 自动运行

Linux 下通过 cron 实现每日 17:00 自动更新：

1. 编辑定时任务：

```bash
crontab -e
```

2. 添加以下内容（请替换实际路径）：

```shell
# 更新股票和股本变迁数据，计算前收盘价和复权因子
00 17 * * * tdx2db update --dbpath /数据库路径/数据库名.db >> /日志路径/tdx2db.log 2>&1
```

**注意事项**：

- 通达信每日数据不是收盘后立即更新，下午 4:30 后是合适的时间
- 确保日志文件有写入权限

## TODO

- [ ] 前收盘价和复权因子计算
- [ ] 导入到 clickhouse、questdb 等数据库

---
