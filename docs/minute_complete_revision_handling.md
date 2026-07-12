# Futures Minute Complete 延迟与修正机制说明

## 背景

`ibkrData` collector 会从 IBKR 实时 tick 聚合出 1 分钟 K，并通过 Redis / WebSocket 的
`futures:minute-complete:{symbol}` 通道推送给客户端，例如 `kdjclient`、`spxclient`、`dowclient`。

客户端收到 1 分钟 K 后，会继续聚合成 3 分 K、5 分 K、15 分 K，并基于这些 K 线计算 KDJ、RSI、
ATR、Supertrend 等指标。因此，1 分 K 的收盘价是否及时且正确，会直接影响客户端的技术分析结果。

## 问题现象

之前发现 `spxclient` 的 08:30 这根 3 分 K 与 `ibkrData` 前端显示不一致：

- `spxclient` 本地 13:30-13:32 UTC 三根 1 分 K 的 13:32 close 是 `7531.5`
- `ibkrData` 数据库和前端最终显示 13:32 close 是 `7535.25`
- 因为 08:30 的 3 分 K 由 08:30、08:31、08:32 三根 1 分 K 聚合而来，13:32 的 close 差异导致 3 分 K close 和 KDJ 都不同

进一步检查数据库后确认，13:32 这一分钟有晚到 tick：

- tick time: `2026-07-09 13:32:59 UTC`
- created_at: `2026-07-09 13:33:58.692 UTC`
- 这个 tick 把 13:32 的最终 close 修正为 `7535.25`

也就是说，客户端在分钟刚结束后收到的是早期版本的 1 分 K；数据库后来又合并了晚到 tick，但客户端没有收到或没有应用这个修正版。

## 根因

根因有两部分。

第一，IBKR tick 到达 collector 的时间并不总是严格实时。在 CPU 满载、IB Gateway/网络延迟、collector 事件循环拥塞或重连恢复时，tick 可能晚于其 `time` 很久才被写入数据库。

第二，旧的客户端逻辑会把同一个 UTM 的 `minute-complete` 当作重复消息丢弃：

```js
if (seenBarUTMs && seenBarUTMs.has(candle.UTM)) {
  return;
}
```

这对普通去重是正确的，但对“同一分钟的修正版”是错误的。即使服务端后续发出修正后的 1 分 K，客户端也会因为 UTM 已见过而忽略。

此外，`CandleStack` 原本只适合追加最新 K 或更新最后一根聚合 K。如果 08:32 的修正版在 08:33 之后才到达，客户端不能简单把它再次 `add_candle`，否则可能把旧分钟追加到末尾，或者重复累计 volume。

## 解决方案

最终方案是“两阶段发布 + 客户端 revision 回写”。

### 服务端

collector 对每根 1 分 K 维护内部 `_revision`。

每收到同一分钟的新 tick：

- 更新 open/high/low/close/volume/bar_count
- `_revision` 加 1

发布逻辑分成两类：

- `provisional`：分钟结束后默认 5 秒发布首版，以及之后只要 revision 变大就继续发布修正版
- `final`：分钟结束后默认 75 秒发布最终版，并从内存缓冲中清除

新增 payload 字段：

```json
{
  "status": "provisional",
  "final": false,
  "revision": 3,
  "bar_start": "...",
  "bar_end": "..."
}
```

final 版示例：

```json
{
  "status": "final",
  "final": true,
  "revision": 3
}
```

相关配置：

- `FUTURES_MINUTE_COMPLETE_DELAY_SECONDS=5`
- `FUTURES_MINUTE_COMPLETE_FINAL_DELAY_SECONDS=75`

这样正常情况下客户端仍然在 5 秒左右拿到第一版 1 分 K，不会等 75 秒才做技术分析；如果后面有晚到 tick，服务端会再推送 revision。

### 客户端

客户端需要识别 revision 消息，不能再按 UTM 直接丢弃。

对于带有以下任一字段的消息，应允许通过：

- `revision`
- `final`
- `status`

`FuturesDataMapper` 会把服务端字段保留下来：

- `IBKR_SOURCE_UTM`
- `IBKR_MINUTE_REVISION`
- `IBKR_MINUTE_FINAL`
- `IBKR_MINUTE_STATUS`

`CandleStack` 对 IBKR 的 1 分 K 做源分钟缓存：

- 每根聚合 K 内部保存它由哪些源 1 分钟 K 组成
- 如果同一个源分钟的新 revision 到达，就替换对应源分钟
- 根据源分钟重新计算该聚合 K 的 open/high/low/close/volume
- 从受影响的聚合 K 开始，重算后续 KDJ/RSI/ATR/Supertrend 等指标

这样可以处理以下场景：

1. 08:30、08:31、08:32 首版 1 分 K 已聚合成 08:30 的 3 分 K
2. 08:33 的新 3 分 K 已经开始
3. 08:32 final revision 晚到
4. 客户端回写 08:30 这根 3 分 K，而不是把 08:32 追加到末尾
5. 后续指标重新计算

## 为什么不直接等 75 秒

如果所有客户端都只等 final 版，会导致技术分析固定落后 75 秒以上。对于 1 分 K、3 分 K 交易逻辑来说，这个延迟太大。

当前方案保留实时性：

- 5 秒左右先给出可交易的 provisional K
- 若晚到 tick 改变 close，再通过 revision 修正
- 客户端可以选择是否对 provisional 信号立即交易，或对关键策略只在 final 后确认

## 监控与验证

为验证 5 秒窗口是否足够，写了监控脚本：

```text
tools/monitor_minute_close_latency.py
```

脚本设计：

- 订阅 Redis `futures:minute-complete:*`
- 记录每根 1 分 K 的首次推送时间、首次 close、revision/status/final
- 结束后查询数据库最终 close
- 输出 JSON/CSV，判断首次 close 是否等于最终 close，以及是否超过 5 秒窗口

由于服务器 `igzmf` 后来重启，完整 1 小时监控报告没有正常落盘。重启后根据数据库 tick 重建了已执行窗口的结果：

- 窗口：`2026-07-09 15:08:41 UTC` 到 `2026-07-09 16:21:30 UTC`
- 实际 tick 到：`2026-07-09 16:18:19 UTC`
- 重建报告：
  - `monitor_reports/reconstructed_minute_close_latency_20260710.csv`

结果：

| 客户端 | Symbol | 分钟数 | p50 延迟 | p95 延迟 | 最大延迟 | 超过 5.5 秒 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| dowclient | MYM | 59 | -1.064s | 0.512s | 4.170s | 0 |
| spxclient | MES | 60 | 0.178s | 0.723s | 200.885s | 1 |

异常分钟：

```text
spxclient / MES
minute: 2026-07-09 16:14:00 UTC
last_tick_time: 2026-07-09 16:14:35 UTC
last_tick_created_at: 2026-07-09 16:18:20.884139 UTC
delay: 200.885s
close: 7580.25
```

这个异常发生在服务器重启/断流附近。该分钟只有 1 个 tick，并且这个 tick 到 16:18:20 才写入，所以 5 秒窗口内不可能拿到正确 close。除这个重启附近的异常外，MES/MYM 的最终 close tick 基本都在 5 秒内到达。

## 当前结论

1. 正常行情和服务器 CPU 增强后，5 秒窗口对 `dowclient/MYM` 和大多数 `spxclient/MES` 分钟是足够的。
2. 重启、断流或 IBKR 数据恢复时，仍可能出现分钟级晚到 tick。这类情况必须依赖 revision/final 机制修正。
3. 客户端必须支持 revision 回写，否则即使服务端发出修正版，3 分 K/5 分 K 和 KDJ 仍会停留在错误首版。
4. 如果策略不能接受 provisional 风险，可以在关键交易判断上增加 `IBKR_MINUTE_FINAL` 检查；如果追求实时性，则使用 provisional，但必须允许后续 revision 修正图表和指标。

## 后续建议

- 把监控脚本输出目录改为 host bind mount，避免服务器或容器重启后丢失报告。
- 在 collector 日志中增加每次 `minute-complete` 发布的 `symbol/time/revision/status/delay` 采样日志，方便事后追踪。
- 对客户端增加可观测日志：当 revision 回写历史聚合 K 时，记录原 close、新 close、revision、影响的 KDJ。
- 长期监控 `created_at - bar_end` 的 p95/p99，如果 p99 长期超过 5 秒，需要继续排查 CPU、IB Gateway、网络和 collector 事件循环阻塞。
