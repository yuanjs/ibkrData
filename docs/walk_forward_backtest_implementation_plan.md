# 历史数据 Walk-forward / As-of 回测改造实施方案

## 范围

本阶段只做历史数据回测，不做实盘实时采集、不做自动换仓、不改真实下单逻辑。

目标是让 `kdjclient` 用 `ibkrData` 的期货单合约历史数据进行 walk-forward 回测：

1. 分钟回放价格使用真实 active contract raw price。
2. 日 K 指标历史使用截至当前回放时间可知的 as-of back-adjusted daily bars。
3. 回测过程中不能使用未来 roll gap。
4. 当前策略主流程 `on_candle_price_update()` 尽量保持不动，只替换数据供应层。

## 当前回测问题

`kdjclient` 现有 `LOCAL_PG` / `CONTINUOUS_FUTURES_DB` 回测流程：

1. `ContinuousFuturesDataLoader.loadDailyBars()` 一次性读取完整
   `continuous_futures_daily_back_adjusted` 或配置的 daily view。
2. `global_testDailyData` 保存整段最终版 adjusted daily bars。
3. `ContinuousFuturesDataLoader.loadMinuteCandles()` 一次性读取完整
   `continuous_futures_back_adjusted` 或配置的 minute view。
4. 回测逐条 replay 分钟 K。

问题：

- 日 K 里包含未来 roll gap。
- 分钟 K 也是 adjusted，不是真实合约价格。
- `getLocalHistoryDailyData(utm)` 虽然按当前时间切片，但切的是最终版 daily
  adjusted 数组。

## 第一阶段总体方案

分两条数据流：

```text
分钟回放流:
    futures_minute_bars
    + futures_roll_events_asof.effective_roll_time
    => raw active contract minute bars

日 K 指标流:
    futures_daily_bars_session_normalized
    + futures_roll_events_asof
    + as_of_date
    => as-of back-adjusted daily bars
```

`kdjclient` 回测循环仍然逐条调用 `on_candle_price_update(itemUpdate)`。
区别是：

- `itemUpdate.MID_CLOSE` 等价格字段来自真实 active contract raw minute bars。
- 每当策略需要历史日 K 时，按当前 `utm` 查询或缓存 as-of adjusted daily bars。

## ibkrData 当前实施状态

已完成第一阶段在 `ibkrData` 侧需要提供的数据能力：

1. 新增 `futures_roll_events_asof`，独立保存 walk-forward 可审计 roll events。
2. 新增 `--roll-calendar-asof`，按 volume+safety 规则生成 `decision_session_date`、
   `known_at`、`effective_roll_time` 和 `price_session_date`。
3. 新增 `continuous_futures_daily_asof()`，按 `as_of_date` 只使用当时已知 roll gap
   生成日 K adjusted 序列。
4. 新增 `continuous_futures_minute_asof_raw()`，按 `effective_roll_time` 切换真实合约，
   不做价格调整。
5. 新增 `continuous_futures_minute_asof_adjusted()`，用于 roll 时重建分钟指标历史窗口。
6. 已补充 roll candidate helper 单元测试。

本地 smoke test 结果：

```bash
python -m backfiller.main --roll-calendar-asof --only SPI --dry-run
python -m backfiller.main --roll-calendar-asof --only SPI --replace-rolls
pytest backfiller/tests -q
```

`SPI` 生成 8 个 as-of roll events。以第一个 roll 为例：

```text
APM4 -> APU4
decision_session_date = 2024-06-18
price_session_date    = 2024-06-18
known_at              = 2024-06-18 07:10 UTC
effective_roll_time   = 2024-06-18 07:10 UTC
old_price             = 7783
new_price             = 7771
price_gap             = -12
```

验证结果：

- `continuous_futures_daily_asof('SPI', ..., '2024-06-18')` 不包含该 roll gap。
- `continuous_futures_daily_asof('SPI', ..., '2024-06-19')` 对 2024-06-17、
  2024-06-18 的旧合约日 K 加上 `-12`。
- `continuous_futures_minute_asof_raw()` 在 `2024-06-18 07:10 UTC` 从
  `202406` 合约切到 `202409` 合约，价格不做 back-adjustment。
- `continuous_futures_minute_asof_adjusted()` 在 roll 后重拉窗口时，对 roll 前旧合约
  分钟 K 加上 `-12`，roll 后新合约分钟 K 保持当前合约价格。

## ibkrData 改造任务

### Task 1: 新增 as-of roll event 表

新增迁移：

```text
db/migration_010_asof_roll_events.sql
```

创建表：

```sql
CREATE TABLE IF NOT EXISTS futures_roll_events_asof (
    id                      BIGSERIAL PRIMARY KEY,
    symbol                  TEXT NOT NULL,
    from_con_id             BIGINT NOT NULL,
    to_con_id               BIGINT NOT NULL,
    from_contract_month     TEXT,
    to_contract_month       TEXT,
    from_local_symbol       TEXT,
    to_local_symbol         TEXT,
    effective_roll_time     TIMESTAMPTZ NOT NULL,
    known_at                TIMESTAMPTZ NOT NULL,
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
    source                  TEXT NOT NULL DEFAULT 'volume_safety_asof',
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, from_con_id, to_con_id, effective_roll_time)
);

CREATE INDEX IF NOT EXISTS idx_futures_roll_events_asof_symbol_known
    ON futures_roll_events_asof (symbol, known_at);

CREATE INDEX IF NOT EXISTS idx_futures_roll_events_asof_symbol_effective
    ON futures_roll_events_asof (symbol, effective_roll_time);
```

说明：

- 不直接修改 `futures_roll_events_volume_safety`，避免影响现有 view。
- 旧 view 继续用于诊断和对比。
- 新回测只使用 `futures_roll_events_asof`。

### Task 2: 修改 roll calendar 生成器

文件：

```text
backfiller/roll_calendar.py
backfiller/main.py
backfiller/tests/test_roll_calendar.py
```

新增命令：

```bash
python -m backfiller.main \
  --roll-calendar-asof \
  --only SPI MYM MES MNQ HG \
  --replace-rolls
```

规则：

1. 仍然按相邻合约 `old -> new` 查重叠 session。
2. 成交量确认必须返回两个日期：
   - `decision_session_date`：连续确认 streak 的第一天。
   - `known_session_date`：连续确认完成的那一天。
3. 对 `confirm_days=2`：
   - D1 满足 `new_volume > old_volume`
   - D2 也满足
   - `decision_session_date = D1`
   - `known_session_date = D2`
4. `price_session_date = known_session_date`。
5. `known_at = effective_roll_time`，第一阶段按下一交易 session 的开始时间确认可用。
6. `effective_roll_time = next trading session after known_session_date` 的交易所时区
   session boundary，再转换成 UTC `timestamptz` 存储。
7. `price_gap = new_close(price_session_date) - old_close(price_session_date)`。
   价格来自 `futures_daily_bars_session_normalized` 的同一交易 session 日 K close。
8. `ratio = new_close / old_close`。

安全换月：

- 如果成交量确认没有在 safety date 前出现，使用 safety candidate。
- `decision_session_date = safety_candidate.session_date`。
- `known_session_date = safety_candidate.session_date`。
- `effective_roll_time = next trading session after known_session_date`。

需要新增 helper：

```python
def choose_volume_safety_candidate_asof(rows, min_confirm_days, safety_date):
    """
    Return:
      {
        "decision_row": row,
        "known_row": row,
        "rule_source": "volume" | "safety",
      }
    """
```

现有 `_first_volume_confirmed_candidate()` 只返回 streak 第一天，不够用。

### Task 3: 新增 as-of 日 K SQL function

放在 `db/migration_010_asof_roll_events.sql`。

函数：

```sql
continuous_futures_daily_asof(
    p_symbol TEXT,
    p_start_date DATE,
    p_as_of_date DATE,
    p_adjustment TEXT DEFAULT 'back_adjusted'
)
```

返回字段至少包含：

```text
time
date_str
session_date
symbol
open/high/low/close
volume
bar_count
source_con_id
contract_month
local_symbol
roll_event_id
roll_time
known_at
adjustment_value
adjustment_method
```

核心过滤：

```sql
known_rolls AS (
    SELECT *
    FROM futures_roll_events_asof
    WHERE symbol = p_symbol
      AND known_at::date <= p_as_of_date
      AND effective_roll_time::date <= p_as_of_date
)
```

back-adjusted 累计：

```sql
SELECT COALESCE(SUM(e.price_gap), 0)
FROM known_rolls e
WHERE e.effective_roll_time::date > r.session_date
```

注意：

- 返回的最后一天应该是已完成 session。
- 对 `kdjclient` 来说，当前交易日实时价由分钟 K 提供，不应把未完成日 K 放进
  `global_currentHistoryDayPrices`。

### Task 4: 新增 raw active minute view/function

为了回测分钟流使用真实 active contract raw price，新增：

```sql
continuous_futures_minute_asof_raw(
    p_symbol TEXT,
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ
)
```

这个函数不做价格调整，只按 `effective_roll_time` 切换合约。

它可以使用完整 `futures_roll_events_asof`，因为每根分钟 bar 的合约选择只取决于
其所在时间是否已经到达 `effective_roll_time`。只要 roll event 的
`effective_roll_time` 是按真实可知逻辑生成的，提前加载整段 raw minute replay
不会引入 adjusted price lookahead。

返回字段：

```text
time
symbol
open/high/low/close
volume
bar_count
source_con_id
contract_month
local_symbol
```

### Task 4.1: 新增 as-of adjusted minute window function

由于 `kdjclient` 实盘和回测都是每分钟 append 一根 bar，roll 后不能只 append 新合约
raw bar，否则内存指标窗口会混合旧合约 raw 历史和新合约 raw 当前价。更稳的方案是：

1. 分钟 replay / 实盘报价继续使用 raw active contract price。
2. 检测到 roll event 生效时，不在 `kdjclient` 手工修补历史数组。
3. 直接从 `ibkrData` 重新拉取最近 N 根 as-of adjusted minute bars，替换指标历史窗口。
4. 清空派生指标缓存，让下一次计算基于替换后的 adjusted window 重算。

新增函数：

```sql
continuous_futures_minute_asof_adjusted(
    p_symbol TEXT,
    p_start_time TIMESTAMPTZ,
    p_end_time TIMESTAMPTZ,
    p_as_of_time TIMESTAMPTZ,
    p_adjustment TEXT DEFAULT 'back_adjusted'
)
```

核心过滤：

```sql
known_rolls AS (
    SELECT *
    FROM futures_roll_events_asof
    WHERE symbol = p_symbol
      AND known_at <= p_as_of_time
      AND effective_roll_time <= p_as_of_time
)
```

back-adjusted 累计：

```sql
SELECT COALESCE(SUM(e.price_gap), 0)
FROM known_rolls e
WHERE e.effective_roll_time > r.time
```

返回字段包含：

```text
time
symbol
open/high/low/close
volume
bar_count
source_con_id
contract_month
local_symbol
roll_event_id
roll_time
known_at
is_roll_time
adjustment_value
adjustment_method
```

这个函数只用于重建指标窗口，不用于成交价格和 PnL。

为了避免调用方误传过大的 `p_end_time` 造成 lookahead，函数内部还会强制：

```sql
b.time <= p_as_of_time
```

### Task 5: ibkrData 测试

新增或扩展测试：

```text
backfiller/tests/test_roll_calendar.py
```

覆盖：

1. volume 2-day confirm 返回 `decision_row=D1`、`known_row=D2`。
2. volume signal 晚于 safety date 时使用 safety。
3. 无 volume 且未到 safety 时不生成 event。
4. `price_gap` 使用 `price_session_date` 的 old/new close。

SQL smoke 测试可先用手工查询记录在文档中，后续再接 pytest + test database。

## kdjclient 改造任务

这些文件在 `~/projects/kdjclient`：

```text
backtestDataLoader.js
main.js
tests/check_walk_forward_futures.js
```

### Task 6: 新增 WalkForwardFuturesDataLoader

在 `backtestDataLoader.js` 新增 class：

```js
class WalkForwardFuturesDataLoader {
  constructor(options) {
    this.dbUrl = options.dbUrl;
    this.tradeSymbol = options.tradeSymbol;
    this.dbSymbol = resolveDbSymbol(options.dbSymbol || options.tradeSymbol);
    this.startDate = options.startDate;
    this.endDate = options.endDate;
    this.pool = new Pool({ connectionString: options.dbUrl });
    this.dailyCache = new Map();
  }

  async loadMinuteCandles() {}
  async loadMinuteSignalWindowAsOf(utm, lookbackMinutes) {}
  async loadDailyBarsAsOf(utm, lookbackDays = 30) {}
  async checkDataAvailability() {}
  async close() {}
}
```

`loadMinuteCandles()` 查询：

```sql
SELECT time, open, high, low, close, volume, source_con_id, contract_month
FROM continuous_futures_minute_asof_raw($1, $2, ($3::date + INTERVAL '1 day')::timestamptz)
ORDER BY time;
```

返回给策略的格式仍然是现有 candle object：

```js
{
  UTM,
  OFR_OPEN, OFR_HIGH, OFR_LOW, OFR_CLOSE,
  BID_OPEN, BID_HIGH, BID_LOW, BID_CLOSE,
  LTP_OPEN, LTP_HIGH, LTP_LOW, LTP_CLOSE,
  MID_OPEN, MID_HIGH, MID_LOW, MID_CLOSE,
  LTV,
  CONS_END: "1",
  CONS_TICK_COUNT: "1",
  SOURCE_CON_ID,
  CONTRACT_MONTH
}
```

`loadMinuteSignalWindowAsOf(utm, lookbackMinutes)` 查询：

```sql
SELECT time, open, high, low, close, volume,
       source_con_id, contract_month, adjustment_value
FROM continuous_futures_minute_asof_adjusted(
    $1,
    $2::timestamptz,
    $3::timestamptz,
    $3::timestamptz,
    'back_adjusted'
)
ORDER BY time;
```

用途：

- 只在 roll event 生效时调用。
- 用返回结果替换 `kdjclient` 当前用于分钟指标的历史窗口。
- 替换后清空分钟级派生指标缓存。

`loadDailyBarsAsOf(utm)`：

1. 用 `getEffectiveDate(utm, TRADE_SYMBOL, tradingDays)` 算 `asOfDate`。
2. cache key 使用 `dbSymbol + asOfDate`。
3. 查询：

```sql
SELECT date_str, open, high, low, close, volume,
       source_con_id, contract_month, adjustment_value
FROM continuous_futures_daily_asof($1, $2::date, $3::date, 'back_adjusted')
ORDER BY session_date;
```

4. 转成 `kdjclient` 日 K 格式，返回老到新排序：

```js
{
  snapshotTime: "YYYY/MM/DD 02:00:00",
  MID_OPEN,
  MID_HIGH,
  MID_LOW,
  MID_CLOSE,
  SOURCE_CON_ID,
  CONTRACT_MONTH,
  ADJUSTMENT_VALUE
}
```

### Task 7: 新增回测模式

在 `main.js` 支持：

```bash
BACKTEST_SOURCE=WALK_FORWARD_FUTURES
```

新增函数：

```js
async function testingHistoryDataWalkForwardFutures() {}
```

流程：

1. 创建 `WalkForwardFuturesDataLoader`。
2. 不预加载 `global_testDailyData`。
3. 只预加载 raw active minute candles。
4. 设置全局：

```js
global_walkForwardDataLoader = loader;
```

5. 逐条 replay minute candles。

### Task 8: 改造历史 K 获取路径

当前 `checkCurrentHistoryDailyData(utm)` 在 `globalTestingHistory` 下调用：

```js
global_currentHistoryDayPrices = getLocalHistoryDailyData(utm);
```

新增 walk-forward 分支：

```js
if (globalTestingHistory && global_walkForwardDataLoader) {
  const dailyPrices =
    await global_walkForwardDataLoader.loadDailyBarsAsOf(utm, 30);

  if (dailyPrices.cacheKey !== global_currentDailyAsOfCacheKey) {
    global_currentHistoryDayPrices = dailyPrices.bars;
    global_history_daily_kdj = [];
    global_currentDailyAsOfCacheKey = dailyPrices.cacheKey;
  }
}
```

注意：

- `getLocalHistoryDailyData()` 可以先保留给 FILE 模式和旧 DB 模式。
- walk-forward 模式不要读取 `global_testDailyData`。
- 当 cache key 变化时必须清空 `global_history_daily_kdj`，强制重新计算。

分钟级历史窗口处理：

```js
if (globalTestingHistory && global_walkForwardDataLoader) {
  const rollEvent = global_walkForwardDataLoader.getRollEventAt(utm);

  if (rollEvent && rollEvent.id !== global_lastAppliedRollEventId) {
    const window =
      await global_walkForwardDataLoader.loadMinuteSignalWindowAsOf(
        utm,
        requiredLookbackMinutes,
      );

    replaceMinuteIndicatorWindow(window.bars);
    clearMinuteIndicatorCaches();
    global_lastAppliedRollEventId = rollEvent.id;
  }
}
```

这里的替换逻辑应封装在数据层或历史 K 管理层，策略主体继续按 append 模式工作。

### Task 9: 交易和 PnL 价格保持 raw

第一阶段不改策略主体，但要保证回放分钟 bar 的 `MID_CLOSE` 是 raw active
contract price。

不要在 `loadMinuteCandles()` 使用：

```text
continuous_futures_back_adjusted
continuous_futures_volume_safety_back_adjusted
```

只使用新的 raw active minute function。

## 验收标准

### 数据验收

1. 对同一 symbol，roll 之前的 `continuous_futures_daily_asof(..., as_of < known_at)`
   不包含该 roll 的 gap。
2. `as_of >= known_at` 后，历史日 K 发生调整。
3. `continuous_futures_minute_asof_raw()` 在 roll 前后切换 `source_con_id`，但价格
   不做 gap adjustment。
4. `continuous_futures_minute_asof_adjusted()` 在 roll 后重建窗口时只调整旧合约历史，
   不调整新合约当前交易价格。
5. 返回给 `kdjclient` 的日 K 和分钟 K 都带 `SOURCE_CON_ID` 以便审计。

### 回测验收

1. `BACKTEST_SOURCE=WALK_FORWARD_FUTURES` 可以完成一段历史回测。
2. 日 K 加载日志显示按 as-of date 缓存，而不是一次性加载最终 adjusted 日 K。
3. roll 后第一次刷新日 K 时，`global_history_daily_kdj` 被清空并重新计算。
4. 与旧 `LOCAL_PG` 模式相比，roll 前历史段的日 K/MVA/KDJ 在 roll 发生前不应提前
   出现未来 gap。

## 推荐实施顺序

1. `ibkrData`: 新增 `migration_010_asof_roll_events.sql` 表结构。
2. `ibkrData`: 改 `backfiller.roll_calendar`，生成 as-of roll events。
3. `ibkrData`: 实现 `continuous_futures_daily_asof()`。
4. `ibkrData`: 实现 `continuous_futures_minute_asof_raw()`。
5. `ibkrData`: 写 roll calendar 单元测试。
6. `ibkrData`: 实现 `continuous_futures_minute_asof_adjusted()`。
7. `kdjclient`: 新增 `WalkForwardFuturesDataLoader`。
8. `kdjclient`: 新增 `BACKTEST_SOURCE=WALK_FORWARD_FUTURES` 模式。
9. `kdjclient`: 改 `checkCurrentHistoryDailyData()` walk-forward 分支。
10. `kdjclient`: roll event 生效时重拉分钟 adjusted window 并替换指标窗口。
11. 跑 SPI / MES 小区间回测，对比旧模式。
12. 扩展到其他期货品种。

## 暂不纳入第一阶段

- 实时 collector 按真实合约采集。
- 实盘自动切换行情合约。
- 自动 roll 持仓。
- 下单/平仓绑定 `con_id`。
- 分钟级 adjusted 信号序列。

这些留到第二阶段实盘一致化再做。
