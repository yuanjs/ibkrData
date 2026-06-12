# 实盘期货动态滚动实施计划

## 目标

回测侧已经基于单合约 raw 数据、as-of roll event 和连续期货 SQL function 实现
walk-forward 动态滚动。实盘侧下一步要和回测侧统一：

1. 实时采集和交易始终绑定真实期货合约，保留 `con_id` / `local_symbol` /
   `contract_month`。
2. 新旧合约 overlap 期间，两边数据都保存，不压平成同一个 `(symbol, time)`。
3. 策略、行情展示和下单只使用本地 roll calendar 决定的 active contract。
4. 到 `effective_roll_time` 后自动切换行情合约，并刷新 adjusted 历史窗口。
5. API 暴露 futures 专用接口，不复用普通 `/api/history/{symbol}`。

自动 roll 持仓是交易行为，第一阶段不和行情切换绑定。

## 当前状态

### 已具备能力

- `futures_minute_bars` 按 `(symbol, con_id, time)` 保存历史分钟 raw 数据。
- `futures_daily_bars` 按 `(symbol, con_id, date_str)` 保存历史日 K raw 数据。
- `futures_roll_events_asof` 保存 walk-forward 可审计 roll events。
- `continuous_futures_daily_asof()` 支持 as-of adjusted 日 K。
- `continuous_futures_minute_asof_raw()` 支持 active raw 分钟序列。
- `continuous_futures_minute_asof_adjusted()` 支持 roll 后重建 adjusted 分钟窗口。
- `backfiller.roll_calendar.RollCalendarGenerator.generate_asof()` 可以生成 as-of
  roll events。

### 实盘缺口

- `collector/ibkr_client.py` 对期货仍通过 `CONTFUT` 解析当前合约。
- tick callback 只传 `symbol, price, size, time`，没有真实合约身份。
- `collector/data_writer.py` 只写普通 `ticks` / `daily_bars`，期货实时数据没有进入
  `futures_*` raw 表。
- API 只有普通 `/api/history/{symbol}`，没有 futures 专用 active contract、as-of
  daily、active raw minute、roll state 接口。
- 计划中需要的 `active_futures_contract_asof()` 尚未实现。
- 下单和平仓仍主要按 `symbol` 构造合约，未显式绑定 `con_id`。

## 新旧合约并行数据处理原则

当新旧合约 overlap 期间同一分钟都有数据时，系统按三层处理。

### 1. Raw 层全部保留

期货原始数据必须按真实合约保存：

```text
futures_minute_bars primary key = (symbol, con_id, time)
futures_daily_bars primary key = (symbol, con_id, date_str)
```

因此同一个 `symbol=SPI`、同一分钟可以同时存在：

```text
SPI old contract APH6, con_id=111, time=2026-03-17 07:09:00
SPI new contract APM6, con_id=222, time=2026-03-17 07:09:00
```

raw 层不判断谁是主力，不做价格调整，也不删除 overlap 数据。

### 2. Roll 决策层使用 overlap 数据

roll engine 使用新旧合约 overlap 期间的日成交量和 bar_count 判断切换点：

- 新合约连续 N 天成交量超过旧合约，生成 volume confirmed roll。
- 如果没有成交量确认，则使用 safety date。
- as-of event 必须记录 `known_at` 和 `effective_roll_time`。
- `price_gap` / `ratio` 使用 `price_session_date` 的新旧合约日 K close 计算。

结果写入 `futures_roll_events_asof`。

### 3. 连续读取层只选择 active segment

连续期货读取函数按 roll event 切分时间段：

```text
effective_roll_time 之前  -> 使用旧合约
effective_roll_time 之后  -> 使用新合约
```

即使数据库里同一分钟新旧合约都有数据，`continuous_futures_minute_asof_raw()`
也只返回 active segment 对应的一条。

示例：

```text
旧合约 APH6: 2026-03-10 到 2026-03-20 都有数据
新合约 APM6: 2026-03-12 开始也有数据
effective_roll_time = 2026-03-17 07:10 UTC
```

active raw 连续序列：

```text
2026-03-17 07:09:00 及以前 -> APH6
2026-03-17 07:10:00 及以后 -> APM6
```

旧合约在 07:10 之后的成交仍保留在 raw 表，但不进入 active continuous series。
新合约在 07:10 之前的成交用于 overlap 决策和审计，不作为当时策略成交价。

## 阶段 1：统一 active contract 查询

新增数据库函数：

```sql
active_futures_contract_asof(
    p_symbol TEXT,
    p_as_of TIMESTAMPTZ
)
RETURNS TABLE (
    symbol TEXT,
    con_id BIGINT,
    contract_month TEXT,
    local_symbol TEXT,
    trading_class TEXT,
    exchange TEXT,
    currency TEXT,
    multiplier TEXT,
    last_trade_date DATE,
    effective_from TIMESTAMPTZ,
    roll_event_id BIGINT
)
```

规则：

1. 查找 `known_at <= p_as_of` 且 `effective_roll_time <= p_as_of` 的最新
   `futures_roll_events_asof`。
2. 如果存在，返回该 event 的 `to_con_id`。
3. 如果不存在 roll event，返回该 symbol 初始合约：
   - 优先使用最早 roll event 的 `from_con_id`。
   - 否则从 `futures_daily_bars` / `futures_minute_bars` 中取最早
     `contract_month`。
4. 返回字段从 futures raw 表中补齐合约元数据。

这个函数是 collector、API、kdjclient 的唯一 active contract 来源。

验收：

- roll 前查询返回旧合约。
- roll 后查询返回新合约。
- 没有 roll event 的 symbol 仍能返回首个合约。

## 阶段 2：期货实时 raw 表

新增实时 tick 表：

```sql
CREATE TABLE futures_ticks (
    time              TIMESTAMPTZ NOT NULL,
    symbol            TEXT NOT NULL,
    con_id            BIGINT NOT NULL,
    local_symbol      TEXT,
    contract_month    TEXT,
    trading_class     TEXT,
    exchange          TEXT,
    currency          TEXT,
    multiplier        TEXT,
    last              NUMERIC(16,6),
    bid               NUMERIC(16,6),
    ask               NUMERIC(16,6),
    volume            BIGINT,
    source            TEXT NOT NULL DEFAULT 'IBKR',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

如使用 TimescaleDB，应创建 hypertable：

```sql
SELECT create_hypertable('futures_ticks', 'time', if_not_exists => TRUE);
```

索引：

```sql
CREATE INDEX idx_futures_ticks_symbol_contract_time
    ON futures_ticks (symbol, con_id, time DESC);
```

实时分钟 K 有两种实现方式：

1. 第一版：collector 内部按分钟聚合后 upsert 到 `futures_minute_bars`。
2. 或新增 `futures_realtime_minute_bars`，后续再合并到正式 raw 表。

推荐第一版直接 upsert 到 `futures_minute_bars`，因为现有 as-of SQL function 已经
读取这张表。

## 阶段 3：collector 期货订阅改造

### IBKRClient

修改 `collector/ibkr_client.py`：

- 新增 `subscribe_futures_contract(symbol, contract_info)`。
- 期货订阅使用 `active_futures_contract_asof()` 返回的真实合约。
- 不再默认使用 IBKR `CONTFUT` 决定实盘 active 合约。
- 保留非期货现有 `subscribe()` 流程。

tick callback 从位置参数改成结构化 payload：

```python
{
    "symbol": symbol,
    "sec_type": "FUT",
    "con_id": contract.conId,
    "local_symbol": contract.localSymbol,
    "contract_month": contract.lastTradeDateOrContractMonth,
    "trading_class": contract.tradingClass,
    "exchange": contract.exchange,
    "currency": contract.currency,
    "multiplier": contract.multiplier,
    "price": price,
    "size": size,
    "time": tick_time,
}
```

### DataWriter

修改 `collector/data_writer.py`：

- 新增 `write_futures_ticks(rows)`。
- 新增 `upsert_futures_minute_bars_from_live(rows)`。
- 新增 `upsert_futures_daily_bars_from_live(rows)`。

普通产品继续写 `ticks` / `daily_bars`，期货产品写 `futures_*` raw 表。

## 阶段 4：实盘 roll state loop

collector 增加后台任务：

```text
每 30-60 秒：
1. 对每个期货 symbol 调 active_futures_contract_asof(symbol, now)
2. 与当前订阅 con_id 比较
3. 如果 con_id 变化：
   - cancel old market data
   - subscribe new active contract
   - 切换 tick/minute/daily tracker 的 active contract
   - publish roll state
```

Redis channel：

```text
futures:active-contract:{symbol}
futures:roll-state:{symbol}
```

payload：

```json
{
  "symbol": "SPI",
  "active": {
    "con_id": 123456,
    "local_symbol": "APM6",
    "contract_month": "202606",
    "exchange": "SNFE",
    "currency": "AUD"
  },
  "previous": {
    "con_id": 111111,
    "local_symbol": "APH6",
    "contract_month": "202603"
  },
  "roll_event_id": 1001,
  "effective_from": "2026-03-17T07:10:00Z",
  "time": "2026-03-17T07:10:05Z"
}
```

第一阶段只切行情合约，不自动平旧仓或开新仓。

## 阶段 5：futures 专用 API

新增 `api/routers/futures.py`，并在 `api/main.py` 注册。

接口：

```text
GET /api/futures/{symbol}/active-contract?as_of=...
GET /api/futures/{symbol}/daily?start=...&as_of=...&adjustment=back_adjusted
GET /api/futures/{symbol}/minute?start=...&end=...&mode=active_raw|adjusted&as_of=...
GET /api/futures/{symbol}/roll-events?start=...&end=...
GET /api/futures/{symbol}/roll-state
```

实现映射：

- `active-contract` 调 `active_futures_contract_asof()`。
- `daily` 调 `continuous_futures_daily_asof()`。
- `minute?mode=active_raw` 调 `continuous_futures_minute_asof_raw()`。
- `minute?mode=adjusted` 调 `continuous_futures_minute_asof_adjusted()`。
- `roll-events` 查 `futures_roll_events_asof`。
- `roll-state` 返回 active contract、最近 roll event 和下一条 pending event。

普通 `/api/history/{symbol}` 保留，避免影响已有前端页面。

## 阶段 6：下单和平仓绑定真实合约

期货下单、平仓命令必须携带真实合约身份：

```json
{
  "symbol": "SPI",
  "sec_type": "FUT",
  "con_id": 123456,
  "local_symbol": "APM6",
  "contract_month": "202606",
  "exchange": "SNFE",
  "currency": "AUD",
  "side": "SELL",
  "quantity": 1
}
```

collector 下单时优先使用 `con_id` 构造并 qualify contract。不能在 roll 后重新
解析当前 front month 去平旧仓。

数据库后续建议扩展：

- `positions` 增加 `con_id`, `local_symbol`, `contract_month`。
- `orders` 增加 `con_id`, `local_symbol`, `contract_month`。
- `executions` 增加 `con_id`, `local_symbol`, `contract_month`。

## 实盘状态机

```text
NORMAL
  |
  | active_futures_contract_asof shows next active con_id
  v
ROLLING_MARKET_DATA
  - cancel old ticker
  - subscribe new ticker
  - reset realtime minute/daily tracker active contract
  - publish roll state
  |
  v
NORMAL
```

持仓 roll 另设策略，不在第一阶段自动执行：

```text
NO_AUTO_ROLL_POSITION
MANUAL_CONFIRM_ROLL_POSITION
AUTO_CLOSE_OLD_ONLY
AUTO_CLOSE_OLD_AND_OPEN_NEW
```

默认使用 `NO_AUTO_ROLL_POSITION`。

## 推荐实施顺序

1. 新增 DB migration：`active_futures_contract_asof()`、`futures_ticks`、必要索引。
2. 新增 futures API，只读 active contract、daily、minute、roll events。
3. 修改 collector tick payload，使期货 tick 带 `con_id`。
4. 修改 DataWriter，使期货实时 tick/minute/daily 写入 futures raw 表。
5. 修改 collector 期货订阅，使用本地 active contract，停止依赖 `CONTFUT`。
6. 增加 roll state loop，实现到点自动切行情合约。
7. 增加 Redis/WebSocket roll state 转发。
8. 修改期货下单和平仓 command payload，绑定 `con_id`。
9. 扩展 positions/orders/executions 合约字段。
10. 接入 kdjclient 实盘：roll 后刷新 adjusted 日 K 和分钟信号窗口。

## 测试与验收

### 数据库

- `active_futures_contract_asof(symbol, before_roll)` 返回旧合约。
- `active_futures_contract_asof(symbol, after_roll)` 返回新合约。
- overlap 期间 `futures_minute_bars` 同一分钟可存在多个 `con_id`。
- `continuous_futures_minute_asof_raw()` 同一分钟只返回 active 合约的一条。

### Collector

- 期货 tick 写入 `futures_ticks` 时包含 `con_id`。
- 非期货 tick 仍写普通 `ticks`，行为不变。
- 模拟 `effective_roll_time` 跨越后，collector 取消旧 ticker 并订阅新 ticker。
- roll state payload 包含 previous、active、roll_event_id。

### API

- `/api/futures/{symbol}/active-contract` 返回 active 合约。
- `/api/futures/{symbol}/daily` 不注入未来 roll gap。
- `/api/futures/{symbol}/minute?mode=active_raw` 返回 raw active price。
- `/api/futures/{symbol}/minute?mode=adjusted` 只用于指标窗口重建，不用于成交价。

### 交易

- 期货 close command 带 `con_id`。
- roll 后平旧仓不会误用新合约。
- 没有 `con_id` 的旧命令应拒绝或降级到明确的兼容路径，并记录 warning。

## 风险点

- IBKR `CONTFUT` 和本地 roll calendar 可能不同步，实盘必须以本地函数为准。
- 旧合约仍有成交时，不能因为 active 已切换就删除旧合约数据。
- 新合约提前有数据时，不能在 `effective_roll_time` 前把它作为策略成交价。
- 下单和平仓如果只按 `symbol` 解析，roll 后有平错合约风险。
- 日 K 的 session date 必须继续使用交易所 roll boundary，不能简单用 UTC 日期。
