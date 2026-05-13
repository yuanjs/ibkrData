# IBKR Data System — 运维手册

## 系统概况

当前订阅 6 个品种：

| Symbol | 显示名 | 证券类型 | 交易所 | 货币 |
|--------|--------|---------|--------|------|
| SPI | 澳指 | FUT | SNFE | AUD |
| USD.JPY | 汇率 | CASH | IDEALPRO | JPY |
| MYM | 道指 | FUT | CBOT | USD |
| N225M | 日经 | FUT | OSE.JPN | JPY |
| 10Y | 美债 | FUT | CBOT | USD |
| ZC | 玉米 | FUT | CBOT | USD |

## 数据存储配置

| 配置项 | 值 | 说明 |
|--------|:--:|------|
| 数据保留期限 | 365 天 | TimescaleDB retention policy，超期 chunk 自动删除 |
| tick 压缩策略 | 15 天后自动压缩 | segmentby=symbol, orderby=time DESC |
| 预期年存储量 | ~50 GB（未压缩）/ ~5 GB（压缩后） |

## 数据库备份

备份脚本位于 `db/backup.sh`，通过 Docker 在 timescaledb 容器内运行 `pg_dump`（避免宿主机与容器内 PostgreSQL 版本不匹配），导出为自定义格式（可压缩、可并行恢复）。

### 手动备份

```bash
# 默认输出到项目根目录的 backups/ 文件夹
cd ~/projects/ibkrData
./db/backup.sh
```

环境变量控制：

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `BACKUP_DIR` | `项目目录/backups` | 备份文件存放路径 |
| `RETENTION_DAYS` | `30` | 保留天数，超过自动清理 |

备份文件格式：`ibkrdata_YYYYMMDD_HHMMSS.sql.gz`（pg_dump custom 格式 + 最高压缩）。

### 恢复备份

```bash
# 通过 Docker 恢复（版本匹配）
cd ~/projects/ibkrData
docker compose exec -T timescaledb pg_restore -U ibkr -d ibkrdata \
  --clean --if-exists < /path/to/ibkrdata_xxx.sql.gz
```

> 注意：恢复后需要重建 TimescaleDB 的压缩策略和保留策略，因为 `pg_dump` custom 格式默认不导出这些策略：
>
> ```sql
> SELECT add_compression_policy('ticks', compress_after => INTERVAL '15 days');
> SELECT add_retention_policy('ticks', INTERVAL '365 days');
> ```

## 服务器定时备份设定

### 1. 编辑 crontab

```bash
crontab -e
```

### 2. 添加定时任务

以下示例每天凌晨 3:00 执行备份，保留 30 天历史：

```cron
# 每天 3:00 备份数据库
0 3 * * * cd ~/projects/ibkrData && ./db/backup.sh --cron >> /dev/null 2>&1
```

如果需要保留 60 天备份：

```cron
0 3 * * * cd ~/projects/ibkrData && BACKUP_DIR=~/backups/ibkrdata RETENTION_DAYS=60 ./db/backup.sh --cron >> /dev/null 2>&1
```

### 3. 验证

```bash
# 查看备份是否成功生成
ls -lh ~/projects/ibkrData/backups/

# 查看备份日志
tail -20 ~/projects/ibkrData/backups/backup.log
```

### 说明

- `--cron` 参数让脚本只写日志，不输出到终端
- 备份脚本通过 `docker compose exec` 在 timescaledb 容器内运行，避免宿主机 `pg_dump` 版本不匹配
- 脚本会自动清理超出保留天数的旧备份文件
