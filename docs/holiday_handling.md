# 节假日日K线处理机制

## 问题背景

某些金融市场在法定节假日仍有电子交易（缩短时段），但 IBKR 的 **Trade Date（交易日）** 会跳过节假日，将交易量归属到下一个有效交易日。

### 典型场景：美国阵亡将士纪念日

| 项目 | 说明 |
|------|------|
| 日期 | 2026-05-25（周一） |
| CME交易时段 | 周日18:00 CT — 周一12:00 CT（缩短交易） |
| Trade Date | **2026-05-26（周二）**，非周一 |
| IBKR backfill 返回的 bar | `2026-05-19, 20, 21, 22, 26`（无 25） |
| 业务要求 | 日K线图上**不展示** 5月25日的K线，当日量价归入5月26日 |

## 核心机制

### 1. Backfill 锚点（`latest_bar_date`）

`_effective_date_str()` 函数新增 `latest_bar_date` 参数。backfill 从 IBKR 获取历史 bars 后，将**最新 bar 的日期**传递给 tracker：

```python
# daily_tracker.py
def _effective_date_str(bar_time, symbol, trading_days=None, latest_bar_date=None):
    ...
    result = local_dt.strftime("%Y%m%d")   # 时钟计算结果
    if latest_bar_date is not None and result < latest_bar_date:
        return latest_bar_date              # 锚点纠正
    return result
```

### 2. 触发流程

```
collector 启动
    │
    ├── load_from_db() ──── 加载DB最新bar，同时设 _latest_bar_date
    │
    ├── backfill 运行 ────── 从IBKR获取bars
    │       │
    │       ├── upsert 到DB
    │       ├── update_latest_bar_date(symbol, latest_date)  ← 设锚点
    │       └── 保护层：删除DB中在backfill日期范围内但不属于
    │                   backfill结果的bar（节假日残留）
    │
    └── on_tick() ────────── _effective_date_str(tick, ..., latest)
             │
             ├── latest 可用 → 用锚点纠正日期 → 正确的交易日
             │
             └── latest 不可用（backfill未完成）
                  → 保留已加载的bar，不transition
```

### 3. 运行时场景对比

| 场景 | 时钟计算 | 锚点 | 结果 |
|------|---------|------|------|
| **节假日**（5/25 Memorial Day） | `20260525` | `20260526` | 用 `20260526` ✅ |
| **常规交易日**（5/26 周二） | `20260526` | `20260526` | 用 `20260526` ✅ |
| **Rollhour后**（5/26 16:01 CT） | `20260527` | `20260526` | 用 `20260527`（时钟 > 锚点）✅ |
| **周末**（周六 tick） | weekday≥5 → next trading day | — | 跳到周一 ✅ |

## 保护层

每次 backfill 完成后自动执行：

```python
# 扫描 backfill 日期范围内、但不在 backfill 结果中的 DB bar
for row in stale_rows:
    if row["date_str"] not in backfill_dates:
        DELETE FROM daily_bars WHERE symbol=$1 AND date_str=$2
```

作用：清除旧版本 collector 或边缘情况产生的节假日残留 bar。

## 涉及文件

| 文件 | 改动 |
|------|------|
| `collector/daily_tracker.py` | `_effective_date_str()` 新增 `latest_bar_date` 参数；`DailyBarTracker` 新增 `_latest_bar_dates`、`update_latest_bar_date()`、`on_tick` 中 backfill 未完成时保留bar |
| `collector/data_writer.py` | 新增 `delete_daily_bars()` 方法 |
| `collector/main.py` | `backfill_daily_bars()` 接收 tracker 并调用 `update_latest_bar_date()`；新增保护层清理残留bar |

## 适用范围

所有走 `_effective_date_str` + `PRODUCT_ROLL_CONFIG` 的产品都会自动享受此机制：

| 产品 | 时区 | RollHour | 
|------|------|---------|
| MES/ES（标普） | America/Chicago | 16:00 |
| MNQ/NQ（纳指） | America/Chicago | 16:00 |
| MYM/YM（道指） | America/Chicago | 16:00 |
| 10Y（美债） | America/Chicago | 16:00 |
| ZC（玉米） | America/Chicago | 16:00 |
| SPI（澳指） | Australia/Sydney | 17:10 |
| N225M（日经） | Asia/Tokyo | 16:30 |
| USD.JPY（汇率） | America/New_York | 17:00 |

日本、澳洲等地的节假日同样适用——只要 IBKR backfill 跳过该日期，锚点机制自动纠正。

## 验证命令

```bash
# 查看日K数据，确认无节假日K线
curl -s "http://localhost:8002/api/history/MES?start=2026-05-24T00:00:00Z&end=2026-05-27T00:00:00Z&interval=1d" \
  -H "Authorization: Bearer yuanjs666" | python3 -m json.tool | grep "2026-05-"

# 查看collector日志中的锚点更新
docker compose logs collector | grep "Updated latest bar date"

# 查看保护层清理日志
docker compose logs collector | grep "Cleaned up"
```
