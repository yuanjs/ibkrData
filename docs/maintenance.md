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
| `RETENTION_DAYS` | `2` | 保留天数，超过自动清理 |

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

## 定时备份

### 服务器（igzmf）— crontab

```bash
# 编辑 crontab
crontab -e
```

添加以下行，每天上午 7:00 执行：

```cron
0 7 * * * cd ~/projects/ibkrData && RETENTION_DAYS=2 ./db/backup.sh --cron >> /dev/null 2>&1
```

#### 自定义（保留 60 天，备份到其他目录）：

```cron
0 3 * * * cd ~/projects/ibkrData && BACKUP_DIR=~/backups/ibkrdata RETENTION_DAYS=60 ./db/backup.sh --cron >> /dev/null 2>&1
```

#### 验证：

```bash
ls -lh ~/projects/ibkrData/backups/
tail -20 ~/projects/ibkrData/backups/backup.log
```

### 本机（CachyOS/Arch）— systemd timer

如果系统没有安装 cron，可以用 systemd 用户定时器（无需 root）。配置文件已纳入版本管理，位于 `config/systemd/`。

#### 安装：

```bash
# 从项目目录创建软链接
cd ~/projects/ibkrData
ln -sf "$(pwd)/config/systemd/ibkrdata-backup.service" ~/.config/systemd/user/
ln -sf "$(pwd)/config/systemd/ibkrdata-backup.timer" ~/.config/systemd/user/
```

#### 启用并启动：

```bash
systemctl --user daemon-reload
systemctl --user enable ibkrdata-backup.timer
systemctl --user start ibkrdata-backup.timer
```

> 确保 linger 已启用，否则注销后定时器不运行：
> ```bash
> loginctl enable-linger
> ```

#### 查看状态：

```bash
systemctl --user list-timers | grep ibkrdata
journalctl --user -u ibkrdata-backup.service -e
```

### 说明

- `--cron` 参数让脚本只写日志，不输出到终端
- 备份脚本通过 `docker compose exec` 在 timescaledb 容器内运行，避免宿主机 `pg_dump` 版本不匹配
- 脚本会自动清理超出保留天数的旧备份文件

## Gateway 重启 / 二次验证期间的重连行为

Gateway 每天自动重启、有时需手动二次验证，期间可能数分钟到十几分钟不可用。
collector（`collector/ibkr_client.py`）的表现与历史故障关系见
[`reconnect_leak_analysis.md`](./reconnect_leak_analysis.md)。

要点：

- 断线后由**唯一一个** `connect_with_retry` 协程以指数退避（封顶 60s）持续
  重试，不再多派生协程（已修复历史上的指数级协程泄漏）。
- 每次连接尝试 2s 超时，失败即进入下一轮退避，**不会卡死在单次 `await`**。
- 持续故障超过 `NOTIFY_THRESHOLD_SECONDS`（默认 120s）时通过 Bark 发送一次
  告警（只发一次）。
- **Gateway 恢复后自动重连并重新订阅全部行情，无需重启 collector、无需人工介入。**
  恢复时延 = Gateway 就绪 + 最多一次退避间隔（≤60s）。

### 真实重启验证（2026-07-11，igzmf 受控压测）

受控重现端口拒绝 + 恢复全过程，证实修复效果：

1. **动作**：`docker stop ibkr-ib-gateway-1`（16:22，约 18 分钟不可用）→ `docker start`。
2. **采集**：受影响进程 = collector（容器）+ 7 个 kdjclient 系 node 实例
   （kdjclient/japclient/dowclient/spxclient/copclient/usdclient/audclient）。
   igzmf **未装 lsof**，FD 监测改用 `/proc/<pid>/fd` 周期采样。
3. **结果**：
   - 不可用期 4 分钟采样：`TOTAL_FD` 稳定在 206–207，无单调上升；无 `localhost:6379`
     连接失败（原故障标志）；SSH 全程可达。
   - 7 个 kdjclient 各自 `Starting automatic reconnection loop` 计数 = **1**
     （重连循环只启动 1 次，不指数增长——泄漏修复核心证据）。
   - `docker start` 后 8 客户端全部自动恢复：collector `Connected to IB Gateway` +
     `Re-subscribing to 2 symbols...`；每个 client `Connection established and
     initialized.`；物理层各持 1 条 `127.0.0.1:<hport> -> 127.0.0.1:4002` ESTAB。
     **零人工、零进程重启。**
4. **Disconnected 日志噪声（kdjclient 侧，非泄漏）**：端口拒绝/半开窗口内每个 client
   可能被 `@stoqey/ib` 反复 emit `EventName.disconnected`（每次 `net.connect` 失败后
   `'close'` 回调 `onEnd` 会 emit；`connectWithRetry` catch 里主动调的
   `this.ib.disconnect()` 也会再 emit 一次）。原监听器无条件 `console.log` 导致刷屏。
   已把该日志行收进 `!isReconnecting` 守卫内，只在确实要派生新一轮重连时才打印。
   （此项在 kdjclient 的 `IbkrBrokerAdapter.js`，与 ibkrData 仓库独立。）
5. **未覆盖（如实）**：本次不会卡在 gateway 半开"握手完成又断"的 wasReady 路径，
   原报告指数泄漏分支未被本次重现。彻底证伪仍需过渡期复现或自然 07:30 重启窗口。
   collector 在 `ConnectionRefused` 路径本就不 emit `disconnectedEvent`，本次
   顺带验证该路径新旧代码均无泄漏。
