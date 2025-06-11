# tdx2db - 通达信数据导入到 DuckDB

## 概述

`tdx2db` 是一个命令行工具。

用于将通达信数据导入并更新至 DuckDB 数据库，支持股票历史数据的全量初始化和增量更新。

## 功能特性

- **快速运行**：go 并发处理，全量导入仅需 10s（Ultra 5 228V + 32G 供参考）
- **增量更新**：支持每天或隔几天增量更新数据
- **使用通达信券商数据**：收盘后更新数据，不用频繁发起 api 请求
- **单文件无依赖**：通达信数据处理工具 datatool 使用 embed 打包

## 安装说明

```bash
git clone https://github.com/jing2uo/tdx2db.git
cd tdx2db
make build
make local-install  # 编译并安装到 ~/.loca/bin
make sudo-install # 编译并安装到 /usr/local/bin ，需要 sudo
```

## 使用方法

### 初始化数据库

**全量导入历史数据**（首次使用必选）：

```bash
tdx2db init --dbpath /数据库路径/数据库名.db --dayfiledir /通达信/vipdoc/
```

也可以从 [通达信券商数据](https://www.tdx.com.cn/article/alldata.html) 下载 **上证所有证券日线(TCKV4=0)** 、**深证所有证券日线(TCKV4=0)** 、**北证所有证券日线** 后使用：

```bash
wget https://www.tdx.com.cn/products/data/data/vipdoc/shlday.zip # 上证日线
wget https://www.tdx.com.cn/products/data/data/vipdoc/szlday.zip # 深证日线
wget https://www.tdx.com.cn/products/data/data/vipdoc/bjlday.zip # 北证日线

mkdir ~/vipdoc
unzip -q shlday.zip -d ~/vipdoc
unzip -q szlday.zip -d ~/vipdoc
unzip -q bjlday.zip -d ~/vipdoc

tdx2db init --dbpath ~/tdx.db --dayfiledir ~/vipdoc

# 数据库文件可以使用任意喜欢的名字, 也可以随意移动
# rm -r shlday.zip szlday.zip bjlday.zip ~/vipdoc  # 初始化后可以删除
```

**必填参数**：

- `--dayfiledir`：通达信 .day 文件所在目录路径（如`/TDX/vipdoc/`）
- `--dbpath`：DuckDB 数据库文件路径（不存在时将自动创建）

### 增量更新数据

**更新现有数据库至最新状态**：

```bash
tdx2db update --dbpath /数据库路径/数据库名.db
```

**必填参数**：

- `--dbpath`：DuckDB 数据库文件路径（需使用 init 时创建的文件）

## 自动化配置

通过 cron 实现每日 17:00 自动更新：

1. 编辑定时任务：

```bash
crontab -e
```

2. 添加以下内容（请替换实际路径）：

```bash
00 17 * * * /usr/local/bin/tdx2db update --dbpath /home/用户/数据库路径/数据库名.db > /home/用户/tdx2db.log 2>&1
```

**注意事项**：

- 通达信数据不是收盘后立即更新，下午 5 点是合适的时间
- 必须使用`绝对路径`确保 cron 正确执行
- `>` 操作符会覆盖日志，如需保留历史请改用`>>`
- 确保日志文件有写入权限

## TODO

- 前收盘价和复权因子计算
- 导入到 clickhouse、questdb 等数据库
- 使用 github release 发布二进制
- Windows 支持

---
