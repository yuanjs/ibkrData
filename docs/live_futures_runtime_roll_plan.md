# 实盘期货 Roll Runtime 实施方案

## 背景

当前代码已经把期货 raw 数据表设计成按真实合约保存：

- `futures_ticks`
- `futures_minute_bars`
- `futures_daily_bars`

但 live collector 的运行模型仍然是单 active contract 订阅。它会查询
`active_futures_contract_asof(symbol, now)`，拿到一个 `con_id` 后只订阅这个真实
合约。`futures_roll_state_loop` 只负责发现数据库 active contract 变化，然后取消旧
订阅、订阅新合约。

这对历史回放是可行的，但对实盘 roll 不完整。原因是：

- `backfiller` 是历史补数工具，不是实盘常驻服务。
- live collector 当前没有同时采集 old/new overlap 合约。
- roll event 生成依赖数据库里的 old/new 日级数据。
- 如果 backfiller 没运行，数据库不一定有实盘 overlap 期间的新旧合约成交量。

因此实盘系统必须自己完成：

- 合约链发现
- old/new overlap 双订阅
- IBKR 日线级别成交量刷新
- roll decision
- active contract 切换

## 当前代码问题

### 1. `IBKRClient` 只能按 symbol 管一个 ticker

当前 `IBKRClient._tickers` 是：

```python
self._tickers: dict[str, Ticker] = {}
```

`subscribe()` 里也用 `symbol` 做 key：

```python
if symbol in self._tickers:
    return
...
self._tickers[symbol] = ticker
```

这意味着一个 symbol 只能有一个实时行情订阅。对于期货 overlap，需要同时订阅
`old_con_id` 和 `new_con_id`，当前结构不支持。

### 2. tick 去重也是 symbol 级别

`_is_new_trade(symbol, price)` 用 `symbol` 记录上一笔成交价。若同一个 symbol 下同时
订阅两个合约，旧合约和新合约会互相影响去重。

实盘双订阅后，去重 key 必须至少包含：

```text
(symbol, con_id)
```

### 3. `futures_roll_calendar_loop` 依赖数据库已有日级合约数据

当前 `futures_roll_calendar_loop` 调：

```python
RollCalendarGenerator.generate_asof(...)
```

而 `RollCalendarGenerator` 的规则输入来自：

```text
futures_daily_bars_session_normalized
```

如果实盘过程中没有刷新 old/new 的 `futures_daily_bars`，roll decision 就没有可靠数据源。

### 4. 当前 live minute bars 不能作为 roll volume 的权威来源

collector 会从 live tick 聚合 `futures_minute_bars`，但实盘系统可能因为停机、网络、IBKR
连接中断、进程重启等原因漏 tick。

因此 roll volume 不应依赖本地 live minute bars 聚合，而应以 IBKR 日线级别数据为准。

## 新目标

实盘系统拆成两条流：

```text
raw capture flow:
  overlap 期间 old/new 合约都订阅，都保存真实合约 tick/minute bar

active flow:
  策略、主行情展示、下单只使用 active contract
```

roll 判断的数据源：

```text
IBKR per-contract daily bars
  -> futures_daily_bars
  -> futures_daily_bars_session_normalized
  -> futures_roll_events_asof
  -> active_futures_contract_asof()
```

live tick/minute bars 可以用于图表和审计，但不作为 roll volume 的权威来源。

## 设计方案

### 1. 增加 Live Futures Contract Manager

新增一个 collector 内部组件，例如：

```text
collector/futures_contract_manager.py
```

职责：

- 从 IBKR 获取当前 symbol 的真实 FUT 合约链。
- 过滤出要参与实盘 roll 的合约。
- 维护每个 symbol 的 runtime state：
  - `active_contract`
  - `next_contract`
  - `subscribed_contracts`
  - `roll_state`
  - `last_daily_refresh_at`

合约链来源：

```python
reqContractDetailsAsync(Future(symbol, exchange=..., includeExpired=False))
```

实盘只需要当前和未来可交易合约，一般不需要 `includeExpired=True`。如果 IBKR 对某些品种不返回足够合约，再按产品单独处理。

合约排序：

```text
lastTradeDateOrContractMonth ASC
```

初始 active contract 优先级：

1. `active_futures_contract_asof(symbol, now)` 返回的本地 active contract。
2. 如果没有本地状态，用 IBKR 合约链中第一个未过期且可交易的近月合约。
3. 记录 bootstrap 状态，后续由 live daily bars 生成正式 roll event。

### 2. 改造 `IBKRClient` 支持同 symbol 多合约订阅

把 ticker 存储从：

```python
dict[str, Ticker]
```

改成：

```python
dict[tuple[str, int], Ticker]
```

新增接口：

```python
subscribe_futures_contract(symbol, contract_identity, role)
unsubscribe_futures_contract(symbol, con_id)
get_futures_snapshots()
```

`role` 用于标记订阅目的：

- `active`
- `candidate`
- `tail`

tick payload 必须包含：

```text
symbol
con_id
local_symbol
contract_month
role
price
size
time
```

去重 key 改为：

```text
(symbol, con_id)
```

前端主 tick 发布仍只发布 active role。candidate tick 可以只入库，不推主行情。

### 3. 实盘 overlap 订阅策略

对每个 FUT symbol，collector 常驻维护：

```text
active = 当前交易/展示合约
next = active 后面的下一张可交易合约
```

订阅规则：

1. 始终订阅 `active`。
2. 当进入 tick overlap window 时订阅 `next`。
3. tick overlap window 的开始时间建议使用：

```text
old.last_trade_date - FUTURES_LIVE_TICK_OVERLAP_TRADING_DAYS
```

实盘 tick 双订阅不需要沿用 backfiller 的 30 个交易日历史 overlap。backfiller 的长 overlap
是为了离线重建完整 roll calendar；实盘 tick 双订阅只是为了覆盖临近换月的交易和展示窗口。

建议默认：

```env
FUTURES_LIVE_TICK_OVERLAP_TRADING_DAYS=5
```

产品可以覆盖，例如流动性迁移更慢的商品合约可设为 10 个交易日。

tick 双订阅不能只由到期日前 N 天触发。如果 IBKR daily bars 提前显示新合约已经满足
roll 条件，或者 `futures_roll_events_asof` 已经生成了 future/pending roll event，那么即使
距离旧合约到期还超过 5 个交易日，也必须立即订阅新合约。

next tick 订阅触发条件是任一条件成立：

```text
1. now >= old.last_trade_date - FUTURES_LIVE_TICK_OVERLAP_TRADING_DAYS
2. 存在 pending roll event: active old -> next，且 effective_roll_time > now
3. live daily refresh 已确认 new_volume > old_volume 的连续确认已完成
4. active_futures_contract_asof(symbol, now) 已经返回 next
```

第 4 条是兜底规则：如果 active 已经切到新合约，但 collector 尚未订阅它，必须立即订阅。

4. roll 切换后：
   - `next` 变成新的 `active`
   - 旧合约可以继续保留 `tail` 订阅到 expiry，或保留 N 天后取消
   - 再发现新的 next 并订阅

建议第一版：

```text
常态: active 单订阅
daily lookahead 期间: active + next daily refresh
pending roll 或 tick overlap 期间: active + next tick 双订阅
切换后: next role -> active，旧 active 取消或降级为 tail
```

旧合约 tail 订阅可以作为第二阶段增强。

### 4. IBKR 日线成交量刷新

新增 live daily refresh loop，例如：

```text
futures_daily_volume_refresh_loop
```

它负责对每个 symbol 的 active/next 合约定期拉 IBKR 日线：

```python
reqHistoricalDataAsync(
    contract,
    endDateTime="",
    durationStr="10 D",
    barSizeSetting="1 day",
    whatToShow="TRADES",
    useRTH=False,
    formatDate=1,
)
```

日线刷新窗口和 tick 双订阅窗口应分开配置。roll 判断依赖 IBKR 日线 volume，所以可以在更早的
lookahead 窗口里开始刷新 next 合约日线，而不必同时打开 next 的实时 tick 订阅。

建议默认：

```env
FUTURES_LIVE_DAILY_LOOKAHEAD_TRADING_DAYS=15
FUTURES_LIVE_DAILY_REFRESH_DAYS=10
```

含义：

- 距旧合约到期 15 个交易日内，开始刷新 active/next 的 IBKR daily bars。
- 距旧合约到期 5 个交易日内，开始 active/next 实时 tick 双订阅。
- roll volume 来自 IBKR daily bars，不来自本地 minute bars。

写入：

```text
futures_daily_bars
```

第一版可以把 `backfiller.db_writer.MinuteBarWriter.upsert_futures_daily_bars()` 的逻辑移动或复制到 collector 的 `DataWriter`，让 collector 能独立写 `futures_daily_bars`。

刷新时机：

- collector 启动后立即刷新 active/next 最近 N 天日线。
- 每个产品 session boundary 后延迟一段时间刷新，例如 `roll_hour + 30min`。
- 日线可能有 settlement 修订，所以在 session 后多次重刷：
  - `+30min`
  - `+2h`
  - `+8h`

关键原则：

- roll volume 用 IBKR 日线 `volume`。
- live tick 聚合的 minute volume 只做实时图表和审计。
- 每次刷新覆盖最近 N 天，允许 IBKR 修订日线数据。

### 5. roll event 生成改为 live daily 数据驱动

当前 `RollCalendarGenerator.generate_asof()` 可以保留，但要调整它的前提：

- 不能假设 backfiller 填好了数据。
- 必须在 live daily refresh 成功后再运行。

新链路：

```text
daily refresh active/next
  -> upsert futures_daily_bars
  -> run RollCalendarGenerator.generate_asof(symbol, replace=False)
  -> upsert futures_roll_events_asof
  -> active_futures_contract_asof() 可返回新合约
```

为了避免 `generate_asof()` 依赖 `futures_minute_bars` 发现合约链，需要改造
`RollCalendarGenerator._load_contracts()`：

当前：

```text
FROM futures_minute_bars
```

建议：

```text
FROM futures_daily_bars
UNION
FROM futures_minute_bars
```

或者新增专用表：

```text
futures_contracts
```

推荐新增 `futures_contracts`，因为实盘合约链发现不应该依赖是否已有 bar 数据。

字段：

```sql
symbol
con_id
local_symbol
trading_class
contract_month
last_trade_date
exchange
currency
multiplier
first_seen_at
last_seen_at
is_active_candidate
```

collector 每次发现合约链时 upsert 这张表。

注意：这个改造不能改变回测路径的行为。推荐做法是新增 live 专用生成路径，例如：

```text
LiveRollEventGenerator
```

或给 `RollCalendarGenerator` 增加显式参数：

```text
contract_source = "historical_bars" | "live_contracts"
```

默认值必须保持现状，继续使用历史 bars，以保证现有 backtest 和历史 as-of 生成不变。collector
实盘 runtime 只能显式选择 `live_contracts`。

### 6. active contract 切换逻辑

保留 `active_futures_contract_asof()` 作为唯一 active contract 查询来源。

但 `futures_roll_state_loop` 的职责要变成：

```text
1. 查询 active_futures_contract_asof(symbol, now)
2. 如果 latest.con_id != current_active.con_id:
     - 将 latest 标记为 active role
     - active 主行情和下单使用 latest
     - 保证 latest 已订阅
     - 取消或降级旧 active
     - 发布 roll-state
3. 确保 next candidate 仍被订阅
```

注意：在双订阅模型下，切换时不一定需要重新向 IBKR 订阅新合约。因为 `next` 很可能已经订阅了。切换主要是本地 role 转换：

```text
next role: candidate -> active
old role: active -> cancelled 或 tail
```

这比当前的 `unsubscribe(symbol) -> subscribe(symbol)` 更稳定。

如果切换时发现 `latest` 尚未订阅，必须先订阅 `latest`，确认 ticker 建立后再把 active role
切过去。不能因为 tick overlap 时间窗尚未到达而拒绝订阅新合约。

### 7. 前端和策略数据边界

Redis tick 发布需要区分：

- 主行情 tick：只发布 active contract。
- raw futures tick：可选发布带 `con_id` 的调试频道。

建议频道：

```text
tick:{symbol}
  active contract only, backward compatible

futures:tick:{symbol}:{con_id}
  optional raw per-contract stream

futures:roll-state:{symbol}
  active/next/previous state changes
```

策略下单必须继续使用 active contract identity：

```text
con_id
local_symbol
contract_month
trading_class
multiplier
```

不能再通过 symbol 重新解析合约。

### 8. kdjclient API 触发检查

kdjclient 当前在调用日 K 数据时，会通过 API 触发检查是否需要进行合约切换。这个模式可以保留，
但必须限定职责。

允许的行为：

```text
kdjclient 调 /api/futures/{symbol}/daily
  -> API 触发 live-safe roll refresh/check
  -> 服务端刷新或读取 IBKR daily bars
  -> 服务端生成/更新 futures_roll_events_asof
  -> API 返回 as-of adjusted daily bars

kdjclient 调 /api/futures/{symbol}/roll-state 或 /active-contract
  -> 读取 active con_id
  -> 如 active con_id 变化，kdjclient 清理本地日K/KDJ缓存并重新加载
```

不允许的行为：

```text
kdjclient 不直接决定 active contract
kdjclient 不直接触发 IBKR market data subscribe/unsubscribe
kdjclient 不用 symbol 重新解析交易合约
```

实际行情订阅和 role 切换仍由 collector 负责：

```text
collector live futures runtime
  -> 管理 active/next tick 订阅
  -> 刷新 per-contract IBKR daily bars
  -> 生成 live roll event
  -> 根据 active_futures_contract_asof() 切换 active role
```

API 层的 roll check 必须是幂等的。重复调用日 K 接口可以刷新 roll state，但不能造成重复
订阅、重复切换或改变回测 as-of 语义。

## 推荐实施阶段

### Phase 1: 合约链与日线刷新

目标：实盘 collector 能独立发现合约并刷新 IBKR 日线。

改动：

- 新增 `futures_contracts` 表。
- collector 增加合约链发现 loop。
- collector `DataWriter` 增加 `upsert_futures_daily_bars_from_ibkr()`。
- 新增 daily refresh loop，对 active/next 拉 `1 day` bars。
- 新增 live 专用 roll event generator，或给 `RollCalendarGenerator` 增加显式
  `contract_source="live_contracts"` 参数。
- 默认 historical/backtest 路径保持原来的 contract source 和 SQL 语义。

验收：

- 新品种没有 backfiller 数据时，collector 也能写入当前/下一合约 metadata。
- session 后能看到 active/next 的 `futures_daily_bars` 被更新。

### Phase 2: 双合约订阅

目标：overlap 期间 live collector 同时采 active/next raw tick。

改动：

- `IBKRClient._tickers` 改为 `(symbol, con_id)` key。
- 新增 per-contract subscribe/unsubscribe。
- tick 去重改为 `(symbol, con_id)`。
- `TickBuffer` 维持按 `con_id` 写库。
- 主 `tick:{symbol}` 只发布 active role。
- next 订阅触发条件支持 pending roll event、volume confirmed 和 active fallback，不只按
  `FUTURES_LIVE_TICK_OVERLAP_TRADING_DAYS`。

验收：

- overlap 期间 `futures_ticks` 中同一个 symbol 可以同时看到两个 `con_id`。
- 前端主行情仍只显示 active 合约。
- 当 roll 条件早于到期前 5 个交易日成立时，next 合约也会被实时订阅。

### Phase 3: live roll decision 闭环

目标：roll event 由 collector 刷新的 IBKR 日线触发，不依赖 backfiller 常驻。

改动：

- daily refresh 成功后触发 `generate_asof()`。
- safety date 和 confirm days 继续复用现有配置。
- `futures_roll_state_loop` 改成 role 切换，而不是简单 unsubscribe/resubscribe。

验收：

- 当 IBKR 日线显示 `new_volume > old_volume` 连续 N 天后，生成
  `futures_roll_events_asof`。
- 到 `effective_roll_time` 后，active contract 自动切到新合约。

### Phase 4: 补偿和监控

目标：处理实盘断线、停机、IBKR 日线修订。

改动：

- collector 启动时补拉 active/next 最近 10 天日线。
- session 后多次刷新最近 10 天日线。
- roll state 加告警：
  - next contract 未发现
  - daily volume 缺失
  - active contract 已过期但没有新 roll event
  - daily refresh 失败超过阈值

验收：

- collector 停机一段时间后重启，能通过 IBKR 日线补齐 roll decision 所需数据。
- 不依赖本地 minute volume 判断是否换月。

## 数据源原则

| 用途 | 数据源 |
| --- | --- |
| 实时图表 | live tick / live minute bars |
| raw 审计 | `futures_ticks`, `futures_minute_bars` |
| roll volume 判断 | IBKR per-contract daily bars |
| price gap / ratio | IBKR per-contract daily close |
| active contract | `active_futures_contract_asof()` |
| 下单合约 | active contract identity |

## 回测隔离原则

实盘 roll runtime 改造不能影响回测逻辑。

必须保持不变的部分：

- `continuous_futures_daily_asof()`
- `continuous_futures_minute_asof_raw()`
- `continuous_futures_minute_asof_adjusted()`
- 现有 backfiller 历史补数行为
- 现有 walk-forward backtest 的 as-of 语义

允许新增的部分：

- live-only 配置项，例如 `FUTURES_LIVE_*`
- live-only 合约 metadata 表，例如 `futures_contracts`
- live-only collector loop
- live-only roll event generation entrypoint
- live-safe API roll refresh hook，供 kdjclient 调日 K 时触发

`futures_roll_events_asof` 可以继续作为共享事实表，但 live 写入必须满足原有 as-of 字段语义：

- `known_at`
- `effective_roll_time`
- `decision_session_date`
- `price_session_date`

如果 live 生成规则需要和历史回测规则有差异，应通过 `source` 或 `roll_rule` 明确标记，不要隐式改变历史生成器的默认行为。

## 关键设计决定

1. roll volume 不使用本地 live minute bars 聚合。
2. overlap 期间必须同时订阅 active 和 next。
3. active flow 与 raw capture flow 分离。
4. `backfiller` 只作为历史初始化和修复工具，不参与实盘闭环。
5. `futures_roll_events_asof` 继续作为 active contract 的事实来源，但它必须能由 collector 自己用 IBKR 日线生成。
6. live tick overlap 默认使用短窗口，建议 5 个交易日；但 pending roll event、volume
   confirmed 或 active fallback 可以提前触发 next tick 订阅。
7. live runtime 改造必须与回测路径隔离，不能改变现有回测 SQL/function 的语义。
8. kdjclient 可以通过 API 触发 roll refresh/check，但不拥有 active 合约决策和行情订阅切换。
