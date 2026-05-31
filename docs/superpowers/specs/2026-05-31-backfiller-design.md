# IBKR 历史1分钟K线数据拉取器 — 设计文档

## 1. 项目概述

为回测和分析需求，从 IBKR Gateway 的历史数据服务（HMDS）拉取指定产品 2-3 年的 1 分钟 K 线数据，持久化存储到 TimescaleDB 专用表。拉取全程不得干扰现有实时行情采集和交易操作。

## 2. 设计原则

| 原则 | 说明 |
|------|------|
| 不干扰现有系统 | 用独立 clientId 连接 Gateway，单线程顺序执行，慢速节流 |
| 幂等安全 | 重复拉取同一窗口不会产生重复数据 |
| 断点续传 | 任意中断（Ctrl+C、Gateway 重启）后重跑自动续传 |
| 自动重连 | Gateway 重启或连接异常时自动指数退避重连 |
| 配置即源码 | 新增产品只需编辑 config.yaml，程序自动识别 |

## 3. 架构

```
ibkrData/
├── backfiller/                  # 新增模块
│   ├── main.py                  # CLI 入口
│   ├── config.py                # YAML 配置加载
│   ├── contract.py              # 合约解析（独立实现）
│   ├── scheduler.py             # 时间窗口管理 + 调度循环
│   ├── db_writer.py             # minute_bars 表写入
│   ├── progress_store.py        # JSON checkpoint 持久化
│   └── config.yaml              # 产品配置
├── collector/                   # 现有：实时采集（不受影响）
├── api/                         # 现有
└── ...
```

### 数据流

```
config.yaml ──→ main.py ──→ scheduler.py 分割窗口
                │                  │
                ├─ --check   ──→ 合约解析 + 小请求验证 HMDS 可达性
                ├─ --pull    ──→ 调度循环（见下）
                └─ --status  ──→ 查 minute_bars 表 + 缺口分析

调度循环 (CASH / STK):
  for (symbol, start, end) in 所有待完成任务:
    ├─ 检查连接 → 断开则自动重连
    ├─ ib.reqHistoricalDataAsync(barSizeSetting="1 min", endDateTime=窗口结束)
    ├─ 写入 minute_bars (ON CONFLICT DO NOTHING)
    ├─ 更新 checkpoint
    ├─ 休眠 25s
    └─ [完成一个 symbol 后] 重读 config.yaml，检测新增产品

FUT (期货) — 过期主力合约链回填:
  IBKR CONTFUT 不支持 endDateTime，无法做窗口式回填。
  改用 Future(symbol, exchange, includeExpired=True) 获取所有
  合约（含已过期），过滤出季度主力合约（月份 03/06/09/12）。
  每个合约的活跃期（从上个到期日到本合约到期日）用标准窗口式
  reqHistoricalDataAsync + endDateTime 回填。
  
  - 优点：支持 endDateTime（单个合约支持），~2年历史可用
  - 过滤：跳过周月/系列合约，只保留下季合约
  - 数据覆盖：约 2024-06 至今（IBKR 端过期合约保留上限）
```

## 4. 数据库设计

### minute_bars 表

```sql
CREATE TABLE minute_bars (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    open        NUMERIC(12,4),
    high        NUMERIC(12,4),
    low         NUMERIC(12,4),
    close       NUMERIC(12,4),
    volume      BIGINT,
    bar_count   INTEGER,           -- IBKR 返回的该 K 线内 tick 数量
    PRIMARY KEY (symbol, time)
);
SELECT create_hypertable('minute_bars', 'time');

-- 已拉取数据概览查询（--status 用）
CREATE INDEX ON minute_bars (symbol, time DESC);
```

### 查询示例

```sql
-- 某产品数据时间范围
SELECT MIN(time), MAX(time), COUNT(*) FROM minute_bars WHERE symbol = 'SPI';

-- 缺口检测（相邻 bar 间隔 > 90s 视为缺口）
SELECT time
FROM (
  SELECT time, LAG(time) OVER (ORDER BY time) AS prev_time
  FROM minute_bars WHERE symbol = 'SPI'
) sub
WHERE prev_time IS NOT NULL
  AND time > prev_time + INTERVAL '90 seconds';
```

## 5. 配置与 CLI

### config.yaml

```yaml
products:
  - symbol: SPI
    sec_type: FUT
    exchange: SNFE
    currency: AUD
  - symbol: USD.JPY
    sec_type: CASH
    exchange: IDEALPRO
    currency: JPY
  # ... 新增产品直接加在这里

start: 2024-01-01
end: 2026-05-31
request_interval_seconds: 25

# 以下参数也可通过 .env 设置（优先级: .env > config.yaml）
ib_host: 127.0.0.1
ib_port: 4002
ib_client_id: 99        # 独立 clientId，不与 collector 冲突
db_url: postgresql://ibkr:password@localhost:5432/ibkrdata
```

### CLI 命令

| 命令 | 用途 |
|------|------|
| `python -m backfiller.main --pull` | 拉取所有待完成任务 |
| `python -m backfiller.main --pull --only SPI USD.JPY` | 仅拉取指定产品 |
| `python -m backfiller.main --status` | 查看所有产品拉取状态概览 |
| `python -m backfiller.main --status --only SPI` | 查看指定产品详细状态 |
| `python -m backfiller.main --check` | 验证 config 中所有产品的 IBKR 可达性 |
| `python -m backfiller.main --check --only AAPL` | 验证单个新产品 |

## 6. 调度与进度管理

### 时间窗口分割

- 每个 symbol 独立分割为 2 天一个窗口（应对 IBKR 1 分钟 K 线单次请求限制）
- 分割前查询 `minute_bars` 表已有数据范围，跳过已存在的窗口
- 剩余窗口序列化为 JSON checkpoint 文件 `backfiller/progress/<symbol>.json`

### 调度循环

```
while 有未完成任务:
    1. 取下一个 (symbol, start, end) 窗口
    2. 检查 ib 连接 → 断开则自动重连（指数退避，max 60s）
    3. 调用 reqHistoricalData
    4. 成功 → 写入 DB → 从 checkpoint 移除该窗口 → 等待 25s
    5. 失败（权限/HMDS不可用等）→ 跳过该窗口，记录错误
    6. 连接异常 → 重连后重试当前窗口
    7. [完成一个 symbol 全部窗口时] 重读 config.yaml 检测新增产品
```

### 自动重连

```
断开检测:
  - reqHistoricalData 抛出连接异常
  - ib.isConnected() 返回 False
  - errorEvent 收到 10197 / 2107 等

重连策略:
  - 指数退避: 2^n 秒, max 60s
  - 重连成功后等待 3s（等 HMDS 通道就绪）
  - 重试当前窗口
```

### 自动检测新增 symbol

每完成一个 symbol 后重新读取 config.yaml：
- YAML 解析失败 → 日志警告，不崩溃
- 检测到新 symbol → 分割窗口 → 追加到任务队列末尾
- 已有 symbol 新增时间范围 → 重新分割并追加

### 优雅退出

收到 `SIGINT` (Ctrl+C)：
1. 完成当前窗口的 `reqHistoricalData` 请求
2. 写入 `minute_bars` 表
3. 更新 checkpoint 文件
4. 断开 IB 连接
5. 退出

## 7. 状态查询

### --status 输出示例

```
$ python -m backfiller.main --status

产品状态总览 (2026-05-31):

SPI    (FUT/SNFE/AUD)     ✅ 已完成    | 2024-01-02 ~ 2026-05-30 | 245,760 bars
USD.JPY (CASH/IDEALPRO/JPY) ⏳ 拉取中  | 2024-01-02 ~ 2025-11-14 | 312,450 bars | 剩余 ~45天窗口
MNQ    (FUT/CME/USD)      🔜 等待中    | 尚未开始
10Y    (FUT/CBOT/USD)     ⏳ 拉取中    | 2024-01-02 ~ 2025-08-03 | 180,224 bars | 剩余 ~90天窗口
ZC     (FUT/CBOT/USD)     ➕ 新配置    | 尚未开始

缺口分析:
  SPI    : 无缺口
  USD.JPY: 1个缺口 (2025-03-15 ~ 2025-03-16)
```

### --check 输出示例

```
$ python -m backfiller.main --check

SPI    : ✅ 合约解析成功 | 432 条1分钟K线 (2026-05-29 ~ 2026-05-31)
USD.JPY: ✅ 合约解析成功 | 2880 条1分钟K线 (2026-05-29 ~ 2026-05-31)
MNQ    : ✅ 合约解析成功 | 1856 条1分钟K线 (2026-05-29 ~ 2026-05-31)
BTC    : ❌ 合约解析失败

$ python -m backfiller.main --check --only AAPL

AAPL   : ✅ 合约解析成功 | 780 条1分钟K线 (2026-05-29 ~ 2026-05-31)
```

注：--check 使用 `whatToShow=MIDPOINT`（CASH）或 `TRADES`（其他），与正式拉取一致。

## 8. 错误处理

| 错误 | 处理方式 |
|------|----------|
| Gateway 连接断开 | 自动重连，指数退避 |
| HMDS 不可用 (code 2107) | 等待 60s 重试，3 次失败后跳过 |
| 无权限 (code 162) | 跳过该产品，输出错误信息 |
| 合约解析失败 | 跳过该产品，输出错误信息 |
| reqHistoricalData 超时 | 重试 3 次，耗时加倍每次 |
| YAML 配置无效 | 日志警告，使用上次有效配置 |
| DB 写入失败 | 重试 3 次，否则跳过窗口 |

## 9. 不干扰现有系统的保证措施

| 措施 | 说明 |
|------|------|
| 独立连接 | 使用 clientId=99，不与 collector 的 clientId=1 冲突 |
| 零并发 | 单线程顺序执行，每次只发起一个请求 |
| 慢速节流 | 每次请求后固定等待 25s，远低于 IBKR 限流阈值 |
| 错误隔离 | backfiller 的错误不会传播到 collector 进程 |
| 独立进程 | backfiller 是独立 Python 进程，与 collector 完全解耦 |
