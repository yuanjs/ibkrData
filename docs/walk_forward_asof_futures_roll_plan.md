# Walk-forward / As-of 连续期货回测与实盘一致化技术方案

## 目标

当前 `ibkrData` 已经能保存期货单合约历史数据，并通过 roll event 生成
continuous futures view。但这些 view 是 hindsight 视角：查询时会使用数据库中
已经存在的全部未来 roll gap。`kdjclient` 如果直接读取整段
`continuous_futures_*_back_adjusted` 数据做回测，会把未来合约切换信息注入早期
日 K。

本方案目标是统一回测和实盘：

1. 采集和成交始终使用真实期货合约数据，保留 `con_id` / `contract_month`。
2. 策略需要的历史日 K 使用本地合成的 as-of back-adjusted 数据。
3. roll 发生时，实盘自动切换真实合约，并刷新 adjusted 历史日 K。
4. 回测和实盘调用同一套 active contract、roll event、as-of adjusted daily
   逻辑；区别只有 `as_of` 时间来源不同。

## 当前状态

### ibkrData

- `futures_minute_bars` 保存期货分钟 raw bars，主键包含
  `(symbol, con_id, time)`。
- `futures_daily_bars` 保存期货日 K raw bars，主键包含
  `(symbol, con_id, date_str)`。
- `futures_roll_events` 和 `futures_roll_events_volume_safety` 保存 roll event。
- `continuous_futures_*` view 使用完整 roll event 表生成 raw/back-adjusted/ratio
  adjusted 连续合约。
- `continuous_futures_daily_volume_safety_*` 短 view 已指向 session-normalized
  日 K view。
- `/api/history/{symbol}` 仍从 `ticks` / `daily_bars` 返回普通 symbol 级别数据，
  不是单合约期货数据，也没有 `con_id`。

### kdjclient

- `backtestDataLoader.js` 的 `ContinuousFuturesDataLoader` 默认读取
  `continuous_futures_back_adjusted` 和
  `continuous_futures_daily_back_adjusted`。
- `main.js` 回测启动时一次性加载整段日 K 到 `global_testDailyData`，再加载整段
  分钟 K 并逐条 replay。
- `getLocalHistoryDailyData(utm)` 从静态 `global_testDailyData` 按日期切片。
- `IbkrBrokerAdapter.getHistoryDailyPrices()` 对期货使用 IBKR `CONTFUT`。
- `subscribeMarketData()`、`createPosition()`、`deletePosition()` 在无明确月份时
  通过 IBKR 前月解析选择合约。

这些行为不满足 as-of 约束，也没有保证实盘和回测使用相同的本地 roll 逻辑。

## 设计原则

1. **raw 永不改写**：所有真实合约行情按 `con_id` 保留。
2. **as-of 可复现**：任何回测时点只能使用 `known_at <= as_of` 的 roll event。
3. **信号和成交分离**：
   - 信号：使用 as-of adjusted daily history。
   - 当前分钟价、成交价、PnL、止损价：使用真实 active contract raw price。
4. **合约选择本地化**：实盘不依赖 IBKR `CONTFUT` 自动滚动，统一由本地 roll
   calendar 决定 active contract。
5. **roll 可审计**：每次 roll 必须记录旧合约、新合约、触发规则、可知时间、
   生效时间、gap 计算价格和成交量依据。

## 数据模型改造

### 1. roll event 增加 as-of 字段

对 `futures_roll_events_volume_safety` 增加字段，或新建 v2 表。推荐新建
`futures_roll_events_asof`，避免破坏现有 view。

```sql
CREATE TABLE futures_roll_events_asof (
    id                      BIGSERIAL PRIMARY KEY,
    symbol                  TEXT NOT NULL,
    from_con_id             BIGINT NOT NULL,
    to_con_id               BIGINT NOT NULL,
    from_contract_month     TEXT,
    to_contract_month       TEXT,
    from_local_symbol       TEXT,
    to_local_symbol         TEXT,

    -- 连续合约从此时开始使用新合约。
    effective_roll_time     TIMESTAMPTZ NOT NULL,

    -- 回测/实盘从此时起才允许知道该 roll event。
    known_at                TIMESTAMPTZ NOT NULL,

    -- 用于审计和复现。
    decision_session_date   DATE NOT NULL,
    price_session_date      DATE NOT NULL,
    roll_rule               TEXT NOT NULL,
    price_gap               NUMERIC(16,6) NOT NULL,
    ratio                   NUMERIC(18,10) NOT NULL,
    old_price               NUMERIC(16,6),
    new_price               NUMERIC(16,6),
    old_volume              BIGINT,
    new_volume              BIGINT,
    old_bar_count           BIGINT,
    new_bar_count           BIGINT,
    status                  TEXT NOT NULL DEFAULT 'confirmed',
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (symbol, from_con_id, to_con_id, effective_roll_time)
);

CREATE INDEX idx_futures_roll_events_asof_symbol_known
    ON futures_roll_events_asof (symbol, known_at);

CREATE INDEX idx_futures_roll_events_asof_symbol_effective
    ON futures_roll_events_asof (symbol, effective_roll_time);
```

字段语义：

- `decision_session_date`：roll 规则选出的决策日期。
- `known_at`：决策真正可知的时间。成交量连续 2 天确认时，`known_at` 不能是第
  一天；必须是确认完成后。
- `effective_roll_time`：真实合约切换时间。保守实现可以设为 `known_at` 之后的
  下一交易 session 起点。
- `price_session_date`：计算 `price_gap` / `ratio` 使用的新旧合约价格日期。

### 2. 实时期货 raw 数据必须带 con_id

当前 `ticks` 是 symbol 级别，不足以支持真实合约审计。需要新增期货实时表：

```sql
CREATE TABLE futures_ticks (
    time              TIMESTAMPTZ NOT NULL,
    symbol            TEXT NOT NULL,
    con_id            BIGINT NOT NULL,
    local_symbol      TEXT,
    contract_month    TEXT,
    last              NUMERIC(16,6),
    bid               NUMERIC(16,6),
    ask               NUMERIC(16,6),
    volume            BIGINT,
    source            TEXT NOT NULL DEFAULT 'IBKR',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('futures_ticks', 'time', if_not_exists => TRUE);

CREATE INDEX idx_futures_ticks_symbol_contract_time
    ON futures_ticks (symbol, con_id, time DESC);
```

也可以直接维护 `futures_realtime_minute_bars`：

```sql
CREATE TABLE futures_realtime_minute_bars (
    time              TIMESTAMPTZ NOT NULL,
    symbol            TEXT NOT NULL,
    con_id            BIGINT NOT NULL,
    local_symbol      TEXT,
    contract_month    TEXT,
    open              NUMERIC(16,6),
    high              NUMERIC(16,6),
    low               NUMERIC(16,6),
    close             NUMERIC(16,6),
    volume            BIGINT,
    bar_count         INTEGER,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, con_id, time)
);
```

### 3. active contract 状态

为了让 `kdjclient` 快速查询当前应订阅/交易的真实合约，增加状态表或 view：

```sql
CREATE VIEW futures_active_contract_asof AS
-- 实际实现建议做成 SQL function，参数为 symbol/as_of。
-- 规则：选取 known_at <= as_of 且 effective_roll_time <= as_of 的最新 roll event；
-- 若没有 roll event，则使用当前 raw 数据中最早链条的 from contract。
```

推荐实现参数化函数：

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
    exchange TEXT,
    currency TEXT,
    multiplier TEXT,
    effective_from TIMESTAMPTZ,
    roll_event_id BIGINT
)
```

## As-of 连续日 K 函数

不要用静态 view 做 walk-forward。新增 SQL function：

```sql
continuous_futures_daily_asof(
    p_symbol TEXT,
    p_start_date DATE,
    p_as_of_date DATE,
    p_adjustment TEXT DEFAULT 'back_adjusted',
    p_roll_table TEXT DEFAULT 'asof'
)
```

核心逻辑：

1. 从 `futures_daily_bars_session_normalized` 读取日 K raw 数据。
2. 只使用 `known_at::date <= p_as_of_date` 的 roll event。
3. 只返回 `session_date <= p_as_of_date` 且 `session_date >= p_start_date`。
4. back-adjusted 时，对每根历史 bar 累加截至 `p_as_of_date` 已知的未来 gap：

```sql
SELECT COALESCE(SUM(e.price_gap), 0)
FROM known_rolls e
WHERE e.symbol = r.symbol
  AND e.effective_roll_time::date > r.session_date
  AND e.known_at::date <= p_as_of_date
```

5. ratio-adjusted 同理只乘已知 ratio。

注意：`p_as_of_date` 对当天未完成日 K 的处理要和 `kdjclient` 现有逻辑一致。策略
计算“昨日”日 K 时，应只返回已完成 session；当前交易日的实时价格由分钟 K 传入。

## Roll engine 改造

### 历史生成

`backfiller.roll_calendar` 需要生成 as-of event，而不是只有 `roll_time`。

成交量确认规则：

```text
D1: new_volume > old_volume
D2: new_volume > old_volume
confirm_days = 2
```

历史上可以知道 D1 是连续信号的第一天，但真实在 D2 收盘后才知道。因此：

- `decision_session_date = D1`
- `known_at = D2 session close`，或保守使用 D2 后的 UTC 日期边界
- `effective_roll_time = next open session after known_at`
- `price_session_date = D2`，使用 D2 的 old/new close 计算 gap

安全换月规则：

- safety date 可以提前计算，但 gap 只有在 price session 完成后才知道。
- `known_at` 应不早于 `price_session_date` 收盘后。

### 实盘生成

实盘需要同时维护旧合约和候选新合约的数据：

1. 在旧合约到期前的 overlap 窗口开始订阅/采集新合约。
2. 每个 session 完成后更新 old/new daily volume。
3. 达成成交量确认或触发 safety date 后写入 `futures_roll_events_asof`。
4. 到 `effective_roll_time` 后，active contract 切到新合约。
5. 发布 roll state 变更给 `kdjclient`，或由 `kdjclient` 定时轮询。

## ibkrData API 设计

新增 futures 专用 API，不复用普通 `/api/history/{symbol}`：

```text
GET /api/futures/{symbol}/active-contract?as_of=...
GET /api/futures/{symbol}/daily?start=...&as_of=...&adjustment=back_adjusted
GET /api/futures/{symbol}/minute?start=...&end=...&mode=active_raw&as_of=...
GET /api/futures/{symbol}/roll-events?start=...&end=...
GET /api/futures/{symbol}/roll-state
```

返回日 K 时保留审计字段：

```json
{
  "time": "2026-06-09T00:00:00Z",
  "date_str": "20260609",
  "symbol": "SPI",
  "open": 7800,
  "high": 7850,
  "low": 7780,
  "close": 7830,
  "source_con_id": 123,
  "contract_month": "202606",
  "adjustment_value": 42,
  "adjustment_method": "back_adjusted_asof",
  "as_of_date": "2026-06-10",
  "latest_roll_known_at": "2026-06-08T00:00:00Z"
}
```

返回 active contract：

```json
{
  "symbol": "SPI",
  "con_id": 123456,
  "local_symbol": "APM6",
  "contract_month": "202606",
  "exchange": "SNFE",
  "currency": "AUD",
  "multiplier": "25",
  "effective_from": "2026-06-08T00:00:00Z",
  "roll_event_id": 1001
}
```

## kdjclient 改造

### 1. 新增 FuturesDataProvider

新增模块，统一回测和实盘的数据读取：

```js
class FuturesDataProvider {
  async getActiveContract(symbol, asOf) {}
  async getAdjustedDailyBarsAsOf(symbol, asOf, lookbackDays) {}
  async getRawActiveMinuteBars(symbol, start, end, asOf) {}
  async getRollState(symbol) {}
}
```

实现可以先基于 HTTP API，后续再优化为直连 PostgreSQL。

### 2. 回测数据流

替换 `ContinuousFuturesDataLoader` 的用法：

- 分钟 K：加载真实 active contract raw minute series。
- 日 K：不再一次性加载最终 adjusted 数组。
- 每个交易日或每次 `getLocalHistoryDailyData(utm)` 时，使用当前 `utm` 查询
  `getAdjustedDailyBarsAsOf()`。

最小改造点：

1. 在 `main.js` 增加全局 `backtestDataProvider`。
2. `testingHistoryDataFromLocalPg()` 不再调用 `loader.loadDailyBars()` 生成最终版
   `global_testDailyData`。
3. `getLocalHistoryDailyData(utm)` 在 futures walk-forward 模式下改为异步 provider
   查询，或新增 `getHistoryDailyDataAsOf(utm)` 并让
   `checkCurrentHistoryDailyData()` 调用它。
4. 按 `effectiveDate` 缓存 daily bars，避免每根 1 分钟 K 查询数据库。

缓存 key：

```text
symbol + effective_date + latest_roll_event_id
```

### 3. 实盘日 K 数据

`IbkrBrokerAdapter.getHistoryDailyPrices()` 对期货不再请求 IBKR `CONTFUT`。

新流程：

```text
asOf = now
daily = provider.getAdjustedDailyBarsAsOf(symbol, asOf, maxPoints)
return daily in kdjclient MID_* format
```

这会让实盘的 `global_currentHistoryDayPrices` 和回测同源。

### 4. 实盘分钟和行情订阅

`subscribeMarketData()` 不再用 `_resolveFrontMonth()` 决定行情合约。

新流程：

```text
active = provider.getActiveContract(symbol, now)
contract = build IBKR FUT contract from active
reqMktData(contract)
start polling raw active minute bars from ibkrData
```

如果 `roll-state` 发现 `active.con_id` 变化：

1. cancel old market data。
2. subscribe new active contract。
3. reset 1m/3m/5m/15m CandleStack。
4. clear `global_currentHistoryDayPrices` and `global_history_daily_kdj`。
5. force daily history refresh。

### 5. 下单和平仓

`createPosition()` 必须记录真实合约：

```json
{
  "symbol": "SPI",
  "con_id": 123456,
  "local_symbol": "APM6",
  "contract_month": "202606",
  "direction": "BUY",
  "dealSize": 1
}
```

`deletePosition()` 平仓时不能重新解析当前 front month。它必须：

1. 优先使用持仓记录里的 `con_id/local_symbol/contract_month`。
2. 或使用 IBKR `position` 回调返回的真实 contract。
3. 只有在没有持仓合约信息时，才查询 broker 当前仓位并匹配真实合约。

否则 roll 后可能用新合约去平旧仓。

## Roll 发生时的实盘状态机

```text
NORMAL
  |
  | roll-state shows pending event
  v
PENDING_ROLL
  - ensure no stale orders on old contract
  - if position policy requires, close old contract before effective time
  |
  | now >= effective_roll_time
  v
ROLLING
  - cancel old market data
  - subscribe new contract
  - refresh adjusted daily history
  - reset intraday stacks
  |
  v
NORMAL
```

持仓策略需要单独配置：

- `ROLL_POSITION_POLICY=close_old`
  - roll 前关闭旧合约，不自动开新仓。
- `ROLL_POSITION_POLICY=transfer`
  - roll 时关闭旧合约并按同方向打开新合约。
- `ROLL_POSITION_POLICY=manual`
  - 只报警，不自动处理持仓。

第一阶段建议使用 `close_old` 或 `manual`，不要一开始自动 transfer。

## 回测执行模型

Walk-forward 回测循环：

```js
for (const minuteBar of rawActiveMinuteBars) {
  const asOf = minuteBar.UTM;

  if (dailyCache.isStale(symbol, asOf)) {
    global_currentHistoryDayPrices =
      await provider.getAdjustedDailyBarsAsOf(symbol, asOf, 30);
    global_history_daily_kdj = [];
  }

  await on_candle_price_update(minuteBar);
}
```

原则：

- `minuteBar.MID_CLOSE` 是真实 active contract raw price。
- `global_currentHistoryDayPrices` 是截至 `asOf` 的 adjusted daily history。
- 当前交易日的价格由 minute bar 进入日均线/KDJ计算，不从未完成日 K 读取。
- 交易结果和止损用 raw price 计算。

## 测试计划

### 数据库测试

1. `known_at` 过滤测试：
   - 同一个 `p_start_date`，用不同 `p_as_of_date` 查询。
   - 在 `known_at` 前，不应包含该 roll gap。
   - 在 `known_at` 后，历史段应被重新 adjusted。

2. roll 边界测试：
   - `effective_roll_time` 前返回旧 `source_con_id`。
   - `effective_roll_time` 后返回新 `source_con_id`。

3. gap 计算测试：
   - `price_gap = new_price - old_price`。
   - back-adjusted 只加 `known_at <= as_of` 的 gap。

### kdjclient 回测测试

1. 用同一段历史做两次回测：
   - 旧 `continuous_futures_daily_back_adjusted`。
   - 新 walk-forward as-of daily。
   - 对 roll 前后的日 K/MVA/KDJ 差异做快照对比。

2. 验证 `getLocalHistoryDailyData(utm)`：
   - roll 前不包含未来 gap。
   - roll 后历史日 K 刷新并重算 KDJ。

3. 验证分钟价格：
   - feed 给策略的分钟价等于真实 active contract raw price。
   - 不再使用 adjusted minute view 作为成交/PnL基础。

### 实盘模拟测试

1. 在 paper 环境构造 pending roll。
2. 验证 active contract 变化时：
   - market data 取消旧合约并订阅新合约。
   - daily history 刷新。
   - intraday stacks 被 reset。

3. 持仓合约测试：
   - 开仓后记录 `con_id`。
   - roll 后平仓仍使用原持仓 `con_id`，不会解析到新合约。

## 实施阶段

### Phase 1: ibkrData as-of 数据能力

1. 新增 `futures_roll_events_asof`。
2. 迁移/生成已有 `futures_roll_events_volume_safety` 到 as-of 表。
3. 实现 `continuous_futures_daily_asof()`。
4. 实现 `active_futures_contract_asof()`。
5. 增加 SQL tests / smoke queries。

### Phase 2: ibkrData 实时真实合约采集

1. collector 改为订阅本地 active contract 的具体 FUT。
2. overlap 窗口同时采集 old/new 合约。
3. 写入 `futures_ticks` 或 `futures_realtime_minute_bars`，保留 `con_id`。
4. 增加 futures API endpoints。

### Phase 3: kdjclient walk-forward 回测

1. 新增 `FuturesDataProvider`。
2. 新增 `WalkForwardFuturesDataLoader`。
3. 改 `testingHistoryDataFromLocalPg()`，日 K 不再预加载最终 adjusted view。
4. 改 `checkCurrentHistoryDailyData()` / `getLocalHistoryDailyData()` 的 futures
   walk-forward 分支。
5. 增加缓存和回测对比测试。

### Phase 4: kdjclient 实盘数据一致化

1. `getHistoryDailyPrices()` 期货分支改为调用 ibkrData as-of daily API。
2. `subscribeMarketData()` 使用本地 active contract。
3. `_fetchMinutePricesFromIBKRData()` 改为 futures raw active minute API。
4. 增加 roll-state polling。
5. roll 时刷新日K、重置 CandleStack。

### Phase 5: 交易合约绑定与 roll 持仓策略

1. `createPosition()` 记录真实合约信息。
2. `deletePosition()` 使用持仓真实合约平仓。
3. 增加 roll position policy。
4. 加 paper flow 测试。

## 风险和待定决策

1. **roll 生效时间**
   - 保守选择：确认完成后的下一交易 session。
   - 如果选择回填到成交量信号第一天，会引入真实不可知信息，不建议用于实盘一致回测。

2. **持仓 roll 策略**
   - 自动 transfer 有执行风险。
   - 第一阶段建议只支持 `manual` 或 `close_old`。

3. **分钟级 adjusted 是否需要**
   - 当前目标是日 K adjusted、分钟和成交 raw。
   - 如果后续策略需要跨 roll 的分钟指标，再增加 as-of adjusted minute series。

4. **CONTFUT 兼容**
   - 可以保留为诊断 fallback，但不应作为正式回测或实盘主数据源。

5. **性能**
   - as-of daily function 可以按交易日缓存。
   - 如果回测很长，可生成带 `as_of_date` 维度的 snapshot 表，但不能覆盖成单条最终序列。

