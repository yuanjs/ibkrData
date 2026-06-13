# 实时期货 Tick 与 Roll 切换时序说明

本文说明当前 `collector` 的实时期货采集逻辑，重点回答两个问题：

1. 新旧合约在 overlap 期间同时有交易时，实时 tick 实际如何处理。
2. `futures_roll_state_loop` 如何决定何时把实时订阅从旧合约切到新合约。

结论先说：

- 当前实现是单订阅模型，不是双订阅模型。
- 对每个期货品种，live collector 任一时刻只订阅一个 active contract。
- overlap 期间即使新旧两个真实合约都在交易，collector 也只会收到当前 active contract 的实时 tick。
- 旧合约和新合约不会在 live tick 链路里同时进入内存 buffer。
- 切换发生在本地 roll state 生效后，由 `futures_roll_state_loop` 执行取消旧订阅并订阅新合约。

相关代码：

- `collector/main.py`
- `collector/ibkr_client.py`
- `collector/data_writer.py`
- `db/migration_011_live_futures_active_contract.sql`

## 1. 关键对象

### `active_futures_contract_asof(symbol, as_of)`

数据库函数 `active_futures_contract_asof()` 是 live 侧当前 active contract 的来源。

它会按 `p_as_of` 查询：

- 已知且已生效的最新 roll event
- 如果没有已生效 roll，则回退到首个 roll 的 `from_con_id`
- 再不行则从 raw futures bars 里回退到可用合约

返回字段包括：

- `con_id`
- `local_symbol`
- `contract_month`
- `effective_from`
- `roll_event_id`

也就是说，live collector 并不是自己判断哪个月合约该订阅，而是依赖数据库中的本地 roll 结果。

### `IBKRClient.subscribe(..., contract_identity=...)`

对期货来说，`subscribe()` 在拿到 `contract_identity` 后，会构造一个明确的真实 FUT 合约：

- `secType="FUT"`
- `conId=...`
- `localSymbol=...`
- `lastTradeDateOrContractMonth=...`

然后调用 `reqMktData(contract, "", False, False)`。

这一步非常关键，因为它说明当前 live 行情订阅绑定的是某一个真实合约，而不是一个抽象的连续品种。

### `TickBuffer`

实时期货 tick 进入 `TickBuffer` 后会走两条写库路径：

1. 逐笔写入 `futures_ticks`
2. 按 `(symbol, con_id, minute)` 聚合后 upsert 到 `futures_minute_bars`

这里确实保留了真实合约身份，但前提是这些 tick 已经被 collector 收到。当前 collector 只订阅一个 active contract，所以 live buffer 里不会天然同时出现新旧两边 tick。

## 2. 启动阶段时序

以下时序描述 collector 刚启动时会发生什么。

```text
collector.main()
  -> load_subscriptions()
  -> 对每个 FUT symbol 调 load_active_futures_contract(pool, symbol)
      -> SELECT * FROM active_futures_contract_asof(symbol, now)
      -> 得到当前 active contract 的 con_id/local_symbol/contract_month
  -> client.subscribe(..., contract_identity=active_contract)
      -> 构造真实 FUT contract
      -> qualifyContractsAsync()
      -> reqMktData(real FUT contract)
  -> ticker.updateEvent 绑定 _on_mkt_data_update()
```

这意味着：

- 启动时只会订阅一个期货真实合约。
- 如果数据库当前认为旧合约仍是 active contract，那么启动后只会收到旧合约 tick。
- 如果数据库当前认为新合约已经生效，那么启动后只会收到新合约 tick。

## 3. overlap 期间的实时 Tick 时序线

假设某个品种：

- 旧合约 `OLD` 仍在交易
- 新合约 `NEW` 也已经开始交易
- 二者存在 overlap
- 本地 roll event 的 `effective_roll_time = T_roll`

### 3.1 `T_roll` 之前

```text
时间 < T_roll

DB active contract = OLD
collector subscription = OLD

OLD 有实时成交 -> IBKR 推送 OLD tick -> collector 收到
NEW 有实时成交 -> collector 没有订阅 NEW -> collector 收不到
```

此时 collector 对每个 tick 的处理链路是：

```text
OLD tick
  -> IBKRClient._on_mkt_data_update()
  -> 组装 futures payload:
       symbol, con_id=OLD, local_symbol=OLD, contract_month=OLD, price, size, time...
  -> 回调 on_trade_tick(payload)
  -> TickBuffer.add_futures_tick(payload)
  -> flush 时:
       write_futures_ticks()
       upsert_futures_minute_bars_from_live()
  -> Publisher.publish_tick(symbol, price, size, time)
```

结果：

- 数据库里只会新增旧合约的 live tick / live minute bar。
- 前端 `tick:{symbol}` 频道里看到的也是旧合约价格流。
- 新合约虽然市场上有成交，但不会出现在当前 live tick 链路里。

### 3.2 `T_roll` 到达并完成切换

`effective_roll_time` 到达后，数据库层的 `active_futures_contract_asof(symbol, now)` 开始返回 `NEW`。

但 collector 不会在 `T_roll` 精确瞬间自动切，它依赖后台轮询任务 `futures_roll_state_loop`。

因此真实行为是：

```text
T_roll 到达
  -> DB active contract 变成 NEW
  -> collector 还在等下一轮 futures_roll_state_loop

下一轮轮询触发
  -> load_active_futures_contract(pool, symbol)
  -> 发现 current=OLD, latest=NEW
  -> unsubscribe(symbol)   # 取消旧订阅
  -> subscribe(..., contract_identity=NEW)  # 改订阅新合约
  -> publish_futures_roll_state(...)
```

这表示切换不是完全事件驱动，而是轮询驱动，默认轮询周期是 60 秒。

### 3.3 切换完成之后

```text
时间 > T_switch

DB active contract = NEW
collector subscription = NEW

OLD 有实时成交 -> collector 没有订阅 OLD -> collector 收不到
NEW 有实时成交 -> collector 收到
```

处理链路与前面相同，只是 payload 中的合约身份变成 `NEW`。

结果：

- 此后写入 `futures_ticks` / `futures_minute_bars` 的都是新合约数据。
- 前端 `tick:{symbol}` 上看到的是新合约价格流。
- 旧合约在切换之后若继续交易，也不会继续进入 live 采集链路。

## 4. 一条完整的时序线

下面把整个过程画成一条连续时间线。

```text
t0: collector 启动
  DB active contract = OLD
  subscribe(OLD)

t1: OLD/NEW overlap 开始
  市场上 OLD 有成交
  市场上 NEW 也有成交
  collector 只订阅 OLD
  -> 只收到 OLD tick
  -> futures_ticks / futures_minute_bars 只新增 OLD con_id 的 live 数据
  -> 前端 tick:{symbol} 只看到 OLD 价格

t2: roll calendar / roll event 已知，effective_roll_time = T_roll
  但当前时刻还没到 T_roll
  collector 继续订阅 OLD

t3: 到达 T_roll
  DB 查询 active_futures_contract_asof(symbol, now) 开始返回 NEW
  collector 当下未必立刻切换
  因为要等 futures_roll_state_loop 下一次轮询

t4: futures_roll_state_loop 下一轮运行
  current = OLD
  latest = NEW
  发现 con_id 变化
  -> unsubscribe(symbol) 取消 OLD
  -> subscribe(NEW) 开始 NEW
  -> 发布 futures roll state 事件

t5: 切换完成后
  collector 只订阅 NEW
  -> 只收到 NEW tick
  -> futures_ticks / futures_minute_bars 只新增 NEW con_id 的 live 数据
  -> 前端 tick:{symbol} 只看到 NEW 价格
```

## 5. `futures_roll_state_loop` 逻辑拆解

`futures_roll_state_loop` 的职责不是计算 roll rule，而是执行 live 订阅切换。

它的输入有四个：

- `client`: 当前 IBKR live 客户端
- `pub`: Redis publisher
- `pool`: 数据库连接池
- `active_contracts`: 进程内维护的“当前已订阅合约”缓存

### 5.1 初始化前提

在 `main()` 里，collector 启动时会先：

1. 调 `load_active_futures_contract(pool, symbol)`
2. 把结果写入 `active_futures_contracts[symbol]`
3. 用这个合约完成首次 `subscribe()`

所以 `futures_roll_state_loop` 后面拿到的 `current`，本质上是“当前进程以为自己已经订阅的合约”。

### 5.2 每轮执行步骤

每轮循环步骤如下：

```text
sleep(interval)
if client 未连接:
    continue

for 每个 futures subscription:
    symbol = 当前品种
    current = active_contracts.get(symbol)
    latest = load_active_futures_contract(pool, symbol)

    if latest is None:
        continue

    if current 和 latest 的 con_id 相同:
        continue

    进入切换流程
```

其中“是否同一个合约”的判断只比较 `con_id`：

```text
_same_contract(left, right):
    return str(left.get("con_id")) == str(right.get("con_id"))
```

这意味着：

- 如果 `con_id` 没变，即便别的元数据变化了，也不会重新订阅。
- 只要 `con_id` 变了，就会执行切换。

### 5.3 切换流程

一旦判定要切换，执行顺序是：

```text
1. logger.info(old_con_id -> new_con_id)
2. client.unsubscribe(symbol)
3. await client.subscribe(symbol, ..., contract_identity=latest)
4. active_contracts[symbol] = latest
5. pub.publish_futures_roll_state(symbol, {...})
```

具体含义：

1. 先取消 symbol 当前绑定的旧 ticker。
2. 再按新的 `contract_identity` 订阅新的真实 FUT 合约。
3. 进程内缓存更新为新合约。
4. 对外发布一条 roll-state 消息，包含：
   - `previous`
   - `active`
   - `roll_event_id`
   - `effective_from`
   - `time`

### 5.4 它不做什么

`futures_roll_state_loop` 当前不会做以下事情：

- 不会同时保留旧订阅和新订阅一段时间。
- 不会补采切换窗口里新合约漏掉的 tick。
- 不会补采旧合约在切换后继续交易的 tick。
- 不会改写前端 tick 发布结构；前端仍只收到 symbol 级 `price/size/time`。
- 不会自动处理持仓迁移；它只切行情订阅，不做自动换仓。

## 6. `effective_roll_time` 前后，策略/前端/数据库各自看到什么

### `effective_roll_time` 之前

- 策略如果依赖当前 live tick，则看到旧合约价格。
- 前端 `tick:{symbol}` 看到旧合约价格。
- `futures_ticks` 写入的是旧合约 `con_id`。
- `futures_minute_bars` upsert 的是旧合约 `(symbol, old_con_id, minute)`。

### `effective_roll_time` 已到，但 live loop 尚未轮询到

- 数据库 active contract 已经是新合约。
- 但 collector 可能仍在订阅旧合约。
- 因此这段短窗口内：
  - live tick 仍可能是旧合约
  - 前端看到的仍可能是旧合约
  - 新合约实时 tick 还没有进入 collector

这是当前轮询切换模型的正常行为。

### 切换完成之后

- live tick 改为新合约。
- 前端 `tick:{symbol}` 改为新合约。
- `futures_ticks` 开始写入新合约 `con_id`。
- `futures_minute_bars` 开始 upsert 新合约 `(symbol, new_con_id, minute)`。

## 7. 为什么文档里的“overlap 两边都保存”和当前 live 行为看起来不同

设计目标里写过：

- overlap 期间两边数据都保存
- raw 层按真实合约保留，不压平

这描述的是系统目标和 raw 数据原则，不代表当前 live collector 会同时订阅两边。

当前代码已经具备的能力是：

- 如果 tick 被收到，就能按真实合约身份分开保存
- 数据表主键也允许新旧两个合约并存

但当前 live collector 仍是：

- 单 active contract 订阅
- 到切换时再从旧订阅切到新订阅

所以“表结构允许双边并存”与“当前 live 订阅只采单边”这两件事并不矛盾。

## 8. 当前实现的实际影响

### 优点

- live 行情、下单、roll state 全部围绕同一个 active contract，行为简单。
- 不会把不同合约的实时价格混在一起。
- `futures_ticks` 和 `futures_minute_bars` 保留了真实合约身份，审计上清晰。

### 局限

- overlap 期间不会同时看到新旧两个合约的 live tick。
- `effective_roll_time` 到实际切换之间存在最长约一个轮询周期的滞后窗口。
- 前端收到的是 symbol 级 tick，不携带 `con_id`，无法直接看出当前显示的是哪一月合约。
- 若要做到“overlap 两边都实时采集”，需要改造成双订阅模型，并在上层明确区分：
  - raw 全量采集流
  - active trading/display 流

## 9. 一句话总结

当前实时期货链路的真实行为是：

```text
overlap 期间只采当前 active contract；
等 futures_roll_state_loop 轮询到本地 roll state 已变后，
取消旧合约订阅并切到新合约订阅。
```
