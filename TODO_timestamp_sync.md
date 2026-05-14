# TODO: IBKR 服务器时间戳同步

## 问题

ibkrData 和 kdjclient 两个系统在聚合1分钟K线时，时间戳来源不一致：

```
ibkrData (ib_insync):  ticker.time = self.lastTime = datetime.now(timezone.utc)   ← 本地TCP包到达时间
kdjclient (@stoqey/ib): Math.floor(Date.now() / 1000)                              ← 本地回调执行时间
```

两者均使用本地机器时间，**均未使用 IBKR 服务器时间戳**，导致：
- 分钟边界可能因时钟漂移/时区设置而错位
- 两台机器之间无法复现一致的K线结果

## 分析结论

### ib_insync (ibkrData)

`tickByTick` 消息的解析链路：

```
IBKR 原始消息 → decoder.py:785 解析出 time (epoch秒)
             → wrapper.py:798-800 将 time 传给 wrapper.tickByTickAllLast(reqId, tickType, **time**, ...)
             → wrapper.py:750-752 **丢弃 time**，改用 self.lastTime（本地时间）
```

ib_insync 的 `reqTickByTickData` 支持存在，但 wrapper 层主动丢弃了服务器时间戳。

### @stoqey/ib (kdjclient)

- `reqMktData` 路径：`tickPrice` / `tickSize` 事件**不带时间戳**（库不暴露）
- `reqTickByTickData` 路径：`tickByTickBidAsk` / `tickByTickAllLast` 事件**带时间戳**（测试文件 `verify_tick_precision.js` 已验证）
- 测试文件已证明 `tickByTickBidAsk` 的 `time` 参数是 IBKR 服务器时间

## 两套方案

### 方案一：改 `reqTickByTickData`（推荐）

**kdjclient 侧** — 纯配置变更，不改库：
1. `subscribeMarketData` 从 `reqMktData` 改为 `reqTickByTickData`
2. 事件监听从 `tickPrice` / `tickSize` 改为 `tickByTickAllLast` / `tickByTickBidAsk`
3. 使用事件的 `time` 参数作为时间戳
4. 外汇用 `BidAsk`，非外汇用 `AllLast`

**ibkrData 侧** — 需补丁：
1. 使用 `ib.reqTickByTickData()` 替代 `ib.reqMktData()`
2. 修改 `ib_insync/wrapper.py` 中 3 处 `tickByTick*` 方法：
   - `tickByTickAllLast` (wrapper.py:750): `self.lastTime` → `datetime.fromtimestamp(time, timezone.utc)`
   - `tickByTickBidAsk` (wrapper.py:776-777): `self.lastTime` → `datetime.fromtimestamp(time, timezone.utc)`
   - `tickByTickMidPoint` (wrapper.py:787): `self.lastTime` → `datetime.fromtimestamp(time, timezone.utc)`
3. 注意：两个 venv 各有一份独立的 `wrapper.py`：
   - `collector/venv/lib/python3.14/site-packages/ib_insync/wrapper.py`
   - `api/venv/lib/python3.14/site-packages/ib_insync/wrapper.py`

### 方案二：`reqCurrentTime()` + offset 校正

不改订阅方式，在校正层处理：
1. 连接时或周期性调用 `reqCurrentTime()` 获取服务器时间
2. 计算 offset = serverTime - localTime
3. 每次 tick 用 `localTime + offset` 作为估计时间
4. 优点：改动小；缺点：时钟漂移累积，分钟级精度但不够可靠

## 实施优先级

低优先级 — 当前两个系统各自使用本地时间，在单机运行场景下功能正常。
跨系统对比或需要精确历史回溯时再实施此方案。
