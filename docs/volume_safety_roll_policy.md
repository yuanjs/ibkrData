# Volume + Safety Roll Policy

## 目标

这套规则用于生成一组可和现有 roll calendar 并行比较的连续期货 view。
它不覆盖 `futures_roll_events`，而是写入：

- `futures_roll_events_volume_safety`

并生成独立 view：

- `continuous_futures_volume_safety_raw`
- `continuous_futures_volume_safety_back_adjusted`
- `continuous_futures_volume_safety_ratio_adjusted`
- `continuous_futures_daily_volume_safety_raw`
- `continuous_futures_daily_volume_safety_back_adjusted`
- `continuous_futures_daily_volume_safety_ratio_adjusted`

## 切换规则

对每一对相邻合约 `old -> new`：

1. 计算重叠交易日内两个合约的日成交量。
2. 找到第一个满足以下条件的日期：
   - `new_volume > old_volume`
   - 条件连续成立 `confirm_days` 天，默认 `2` 天
3. 计算临近到期安全换月日：
   - 指数期货：旧合约到期前 `2` 个工作日
   - 商品期货：旧合约到期前 `5` 个工作日
4. 最终 roll 日期取两者较早者：

```text
roll_date = min(volume_confirmed_date, safety_date)
```

如果成交量确认信号在安全换月日前出现，则优先使用成交量确认日。如果到安全换月日
仍未确认，则强制使用安全换月日。如果安全换月日不是重叠交易日，
使用第一个不早于安全换月日的重叠交易日。如果当前数据还没有覆盖到安全换月日，
且没有成交量确认信号，则不为这对合约生成 roll event。

## 当前产品分组

指数期货，默认临近到期安全网 2 个工作日：

- `SPI`
- `MYM`
- `N225M`
- `MNQ`
- `MES`

商品期货，默认临近到期安全网 5 个工作日：

- `HG`
- `ZC`

## 生成命令

先创建数据库对象：

```bash
psql "$DB_URL" -f db/migration_006_volume_safety_roll_views.sql
```

为指定产品生成新 roll calendar：

```bash
python -m backfiller.main \
  --roll-calendar-volume-safety \
  --only SPI MYM N225M HG \
  --replace-rolls
```

预览但不写库：

```bash
python -m backfiller.main \
  --roll-calendar-volume-safety \
  --only SPI MYM N225M HG \
  --dry-run
```

临时覆盖所有产品的安全天数：

```bash
python -m backfiller.main \
  --roll-calendar-volume-safety \
  --only HG \
  --safety-days 20 \
  --replace-rolls
```

## 查询示例

检查 roll events：

```sql
SELECT symbol, from_local_symbol, to_local_symbol,
       roll_time::date, roll_rule, old_volume, new_volume,
       price_gap, ratio
FROM futures_roll_events_volume_safety
WHERE symbol IN ('SPI', 'MYM', 'N225M', 'HG')
ORDER BY symbol, roll_time;
```

回测用分钟 back-adjusted 数据：

```sql
SELECT *
FROM continuous_futures_volume_safety_back_adjusted
WHERE symbol = 'HG'
ORDER BY time;
```

回测用日 K back-adjusted 数据：

```sql
SELECT *
FROM continuous_futures_daily_volume_safety_back_adjusted
WHERE symbol = 'HG'
ORDER BY time;
```

如果策略使用期货日 K，`continuous_futures_daily_volume_safety_*`
这些短名称已经指向 session-normalized 日 K。也可以显式使用
`*_session_*` view：

```sql
SELECT *
FROM continuous_futures_daily_volume_safety_session_back_adjusted
WHERE symbol = 'SPI'
ORDER BY session_date;
```

这组 view 会先把 `futures_daily_bars` 的 roll-hour 碎片合并到真实交易
session。归一化由交易所日历驱动：

- `exchange_trading_days` 保存每个交易所日历的 open/closed 日期。
- `futures_daily_symbol_calendars` 保存 symbol 使用哪个交易所日历。
- 当前日历分组：
  - `SPI` -> `AU_ASX`
  - `N225M` -> `JP_OSE`
  - `MYM`, `MNQ`, `MES`, `HG`, `ZC`, `10Y` -> `US_CME`

- 周六/周日日 K 合并到同一合约的下一根工作日日 K。
- 交易所假日，例如 Easter、Christmas、日本 Golden Week 和其他日本假日，
  合并到该交易所日历的下一根 open trading day。
- OHLCV 合并规则：
  - `open` 使用最早碎片的 open
  - `high` 使用最大 high
  - `low` 使用最小 low
  - `close` 使用最后碎片的 close
  - `volume` / `bar_count` 求和

原始日 K 保留在 `futures_daily_bars`，不被修改。归一化结果可直接查询：

```sql
SELECT *
FROM futures_daily_bars_session_normalized
WHERE symbol = 'SPI'
ORDER BY session_date, contract_month;
```

初始化或更新交易所日历：

```bash
psql "$DB_URL" -f db/migration_008_exchange_calendar_daily_normalization.sql
python -m backfiller.exchange_calendar \
  --db-url "$DB_URL" \
  --start 2023-01-01 \
  --end 2027-12-31
```

## 和旧 view 的区别

旧 view 使用 `futures_roll_events`，当前默认规则是“成交量或 bar_count 连续确认，
找不到信号时才 fallback”。

新 view 使用 `futures_roll_events_volume_safety`，规则是“优先成交量连续确认；
只有临近到期仍未确认时才触发安全换月”。这会避免固定提前换月过早切到
流动性不足的新合约。
