# Collector 重连协程泄漏分析与修复

来源：`server_hang_analysis.md`（Antigravity brain 快照）根因一。
对象：`collector/ibkr_client.py` 的 gateway 断线重连。
本文为针对该报告的源码二次核对与定稿结论，沉淀到此供后续维护参考。

---

## 1. 症状

2026-07-11 07:30 左右 Gateway 自动重启后，服务器 `igzmf` 网络栈与 CPU 饥饿，
SSH 不可达，硬件重启。`collector.log` 中出现连本地 Redis 都报
`Error connecting to localhost:6379. Connect call failed` —— 典型的 FD / 套接字
耗尽征兆。

## 2. 报告诊断（源码核对后成立）

`ibkr_client.py` 原实现：

```python
def _on_disconnect(self):
    logger.warning("Disconnected from IB Gateway")
    asyncio.ensure_future(self.connect_with_retry())
```

`_on_disconnect` 每次收到 `disconnectedEvent` 都**无条件**派生新的
`connect_with_retry` 协程；而 `connect_with_retry` 自身 `while True` 已在退避
重试。Gateway 重启时半开连接反复失败 → socket 反复 `connection_lost` → 反复
emit `disconnectedEvent` → 反复派生新协程 → 协程数指数增长 → 耗尽 FD/CPU。

### 2.1 ib_insync 源码链路核对（装在 collector/venv 的版本）

指数泄漏的**真正发射路径**（比报告表述更精确）：

1. `Client.connectAsync` 失败抛异常 → `except` 调 `Client.disconnect()`
   (`ib_insync/client.py:202-226`，L220)。注意是 `Client.disconnect`，**不是**
   `IB.disconnect`，因此不走 `ib_insync/ib.py:298` 那条显式
   `disconnectedEvent.emit()`。
2. `Client.disconnect` → `self.conn.disconnect()`
   (`ib_insync/client.py:228-233`) → `transport.write_eof(); transport.close()`
   (`ib_insync/connection.py:42-45`)。
3. asyncio 随后回调 `Connection.connection_lost(exc)`
   (`ib_insync/connection.py:56-59`) → `self.disconnected.emit(msg)`。
4. `Client.__init__` 绑定 `self.conn.disconnected += self._onSocketDisconnected`
   (`ib_insync/client.py:104`)。`_onSocketDisconnected` 在 `wasReady` 为真时
   `self.apiEnd.emit()` (`ib_insync/client.py:365-381`)。
5. `IB.__init__` 绑定 `self.client.apiEnd += self.disconnectedEvent`
   (`ib_insync/ib.py:212`) → 即 `apiEnd` emit → `disconnectedEvent` emit。
6. `IBKRClient.__init__` 绑定 `self.ib.disconnectedEvent += self._on_disconnect`
   (`ibkr_client.py:57`) → `asyncio.ensure_future(connect_with_retry())`。

**关键**：只有 `wasReady`（已完成握手、`_hasReqId and _accounts` 已到，`apiStart`
已 emit，`ib_insync/client.py:355-357`）为真时，才会经 `apiEnd` 触发
`disconnectedEvent`。Gateway 重启时的半开“连上又断”场景恰好命中：握手完成、
ready 之后、`apiStart` 超时或对端再次断开 → `wasReady=True` → `apiEnd.emit`
→ `_on_disconnect` → 再派生协程。报告里“$2^n$ 指数级”判断方向正确，此处为其
补全精确发射链。

### 2.2 eventkit 同线程同步 emit

`disconnectedEvent.emit()` 在 asyncio `connection_lost` 回调（loop 线程）内同步
触发全部订阅者，`_on_disconnect` 同步执行 `ensure_future` 派生新 task。无跨线程
竞争；但“失败 → 派生 → 再失败 → 再派生”的递归放大在多协程并存时指数成立。

## 3. 报告修复 diff 的两处问题（不照搬）

### 问题 A：`_reconnect_task` 已被 10197 占用

现有 `ibkr_client.py` 已有 `self._reconnect_task = None`（L61），但它**专属于
10197 行情中断恢复**，赋值仅出现在 `_on_error`
(`ibkr_client.py:70-79`，派生 `_auto_reconnect_market_data`)，与 gateway 重连
语义正交。报告 diff 让 `_on_disconnect` 也写这个句柄，会把两类不同任务塞进同一
字段。

### 问题 B：报告建议在 `connect()` 成功时置 `self._reconnect_task = None`

这是**危险的**。`_on_error` 判断 `if self._reconnect_task is None or
self._reconnect_task.done()`：若在 gateway `connect()` 成功时把它置 None，会把
仍在跑的 `_auto_reconnect_market_data` 句柄丢掉（task 未取消，仅丢句柄），下次
10197 来时判定“无在跑”又新建一个 → **把指数泄漏从 gateway 重连迁移到行情恢复**
路径。违反“矛盾模式不要平均”原则。

## 4. 定稿修复

核心：gateway 重连与 10197 行情恢复使用**两个独立句柄**；`_on_disconnect` 用
done 判断去重；`connect()` / `connect_with_retry()` 不动（task 协程 `return`
后 `done()` 自然为真，不需要、也不应手动置 None）。

### 4.1 `__init__`（L61 附近）

```python
self._reconnect_task = None           # 10197 行情恢复专用，保持现状
self._gateway_reconnect_task = None   # 新增：gateway 重连去重
```

### 4.2 `_on_disconnect`（替换 L133-135）

```python
def _on_disconnect(self):
    logger.warning("Disconnected from IB Gateway")
    task = self._gateway_reconnect_task
    if task is None or task.done():
        self._gateway_reconnect_task = asyncio.create_task(self.connect_with_retry())
    else:
        logger.info("Gateway reconnect already in progress, skipping.")
```

### 4.3 收敛性

- 启动 `await client.connect_with_retry()` 成功后协程 return → task done。
- 断线 → `_on_disconnect` 见 done → 新建**唯一一个** `connect_with_retry`，其
  内 `while True` 自退避自重试。
- 重试期间若 ib_insync 经 `wasReady` 路径再 emit `disconnectedEvent` →
  `_on_disconnect` 见 `_gateway_reconnect_task` 未 done → 跳过。指数放大被截断。
- `connectAsync` 失败抛异常被 `connect_with_retry` 的 `except` (L111) 捕获退避
  重试，不依赖事件层。两条重试路径合一，不再分叉。
- 滚月逻辑（`futures_runtime.py` 的 `subscribe/unsubscribe_futures_contract`）
  只调 `cancelMktData`，不动 gateway socket，去重锁不卡滚月。
- `main.py:1621` 的 `client.ib.disconnect()` 位于进程关闭流程末尾（已 cancel
  所有 task、flush ticks），派生 `disconnectedEvent` 时进程刚退出，无运行期
  副作用。

## 5. Gateway 日常重启 / 手动二次验证期间的行为

Gateway 每天自动重启、有时需手动二次验证，期间可能数分钟到十几分钟不可用。
修复后的客户端表现：

1. **初始断开**：socket `connection_lost`（`wasReady` 路径）→ 新建唯一
   `connect_with_retry`。后续 `disconnectedEvent` 全部被 done 判断跳过，不再
   多派生。**指数泄漏根治于此。**
2. **退避重试**：`connect_with_retry` 以 `wait = min(2**_retry, 60)` 退避
   (1→2→...→60s)，**封顶 60s**。每轮 `connectAsync` 默认 2s 超时
   (`ib_insync/client.py:202`，`IBKRClient.connect` 未覆盖 timeout)，失败即抛
   异常进 `except` 退避，**不会 hang 在某次 `await` 上**——这是长久不可用期间
   循环能持续推进的前提。
3. **告警**：失败持续超过 `NOTIFY_THRESHOLD_SECONDS`（默认 120s，`config.py:90`）
   时经 Bark 发送一次 `🚨 IBKR 连接故障` 告警 (`ibkr_client.py:119-126`)，
   `_alert_sent` 标志保证只发一次，不告警风暴。
4. **Gateway 恢复后**：`connect_with_retry` 下一轮 `connect()` 成功 → 发
   `✅ IBKR 连接已恢复` 通知 (`ibkr_client.py:93-100`)、`_retry=0`、
   `_on_connect` 触发 `_resubscribe_all` 重新订阅全部行情
   (`ibkr_client.py:139`)。

**结论：Gateway 恢复后能自动重连，无需重启 collector 进程、无需任何人工干预。**
重连时延 = Gateway 真正就绪时间 + 最多一次退避间隔（封顶 60s）的可接受滞后。

### 5.1 可选调参（非 bug）

如希望 Gateway 一就绪即秒连，可把退避封顶从 60s 降到 15-30s
（`ibkr_client.py:128` `wait = min(2**self._retry, 60)`）。代价：Gateway 长期
不可用时每分钟多打几个连接尝试（几十个无关 TCP/分钟，远低于系统限额，无泄漏
风险），换取恢复时的更短滞后。此为偏好项，不在本次 bug 修复范围内，待决定再改。

## 6. 报告 §4.2 一处澄清（Python 端不适用）

报告 §4.2 建议“把连接超时从 10000ms 降至 4000ms”。此 10s 超时指 **Node.js 端**
`IbkrBrokerAdapter.js` 的连接超时，对 Python 端不适用：

- Python 端 `IBKRClient.connect` 调 `await self.ib.connectAsync(...)` 时**未传
  timeout** (`ibkr_client.py:92`)，使用 `ib_insync` 默认值 `timeout=2.0`
  (`ib_insync/client.py:202`)，即 Python 端每次连接尝试本就只有 2s 超时，无需
  在 Python 端做任何超时调整。

报告针对 Node.js 端的超时建议另在 `kdjclient` 仓库处理，不在此文档范围。