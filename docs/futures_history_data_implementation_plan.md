# 期货历史数据处理实施方案

## 目标

以后通过 `backfiller` 下载期货历史数据时，必须先保存单合约原始数据，再由后处理生成连续期货。下载阶段不再把不同合约月份压平成同一个 `(symbol, time)` 序列。

## 已落地的第一阶段改动

1. 新增 `futures_minute_bars` raw 表。
   - 文件：`db/migration_003_futures_raw_bars.sql`
   - 主键：`(symbol, con_id, time)`
   - 保存字段：`con_id`, `local_symbol`, `trading_class`, `contract_month`, `last_trade_date`, `exchange`, `currency`, `multiplier`, `OHLCV`, `volume`, `bar_count`

2. 新增 `futures_roll_events` 表。
   - 用于记录从哪个合约滚到哪个合约。
   - 保存 `roll_time`, `roll_rule`, `price_gap`, `ratio`。

3. 修改 backfiller 写入逻辑。
   - 非期货仍写 `minute_bars`。
   - 期货写 `futures_minute_bars`。
   - 期货写入时使用 `ON CONFLICT (symbol, con_id, time) DO NOTHING`。
   - 期货后续合约从上一合约到期日前 `futures_overlap_trading_days`
     个工作日开始下载，默认 `30`，用于保留新旧合约重叠窗口。

4. 修改 `--status`。
   - 期货状态从 `futures_minute_bars` 读取。
   - 非期货状态仍从 `minute_bars` 读取。

## 数据库部署步骤

在运行新版 backfiller 之前执行：

```bash
psql "$DB_URL" -f db/migration_003_futures_raw_bars.sql
psql "$DB_URL" -f db/migration_005_futures_daily_bars.sql
```

当前已有的 `minute_bars` 期货数据可保留为诊断数据，但不要作为正式回测源。

## 第二阶段：重新下载期货 raw 数据

1. 对期货品种重新执行 backfill。期货 raw 写入按
   `(symbol, con_id, time)` 去重，已有数据不会重复插入；打开 overlap
   后重跑会补入新合约提前开始的重叠窗口。
2. 如果要从零开始重下，可先清理目标品种在 `futures_minute_bars` 中的
   rows；否则可以直接重跑，让 `ON CONFLICT` 跳过已有 bars。
3. 验证每个品种在 `futures_minute_bars` 中存在多个 `con_id`。
4. 抽查同一 roll 附近是否能同时看到前后两个合约的数据。

期货 backfill 支持窗口级断点续传：

- checkpoint 保存在 `backfiller/progress/<symbol>.json` 的 `tasks` 字段中。
- 每个任务 key 对应一个具体合约，例如 `FUT:<con_id>:<contract_month>`。
- 只有当某个 2 天窗口请求成功并写库完成后，才会从 checkpoint 中移除。
- 如果程序在窗口中途被中断，该窗口不会标记完成，下次会重新下载。
- 首次为旧数据创建 checkpoint 时，会查询 `futures_minute_bars`；已经完整覆盖窗口起止日期的窗口会被跳过，只有缺失或部分覆盖的窗口会重新请求。
- 不要随意删除 progress 文件；删除后程序会重新扫描 DB 覆盖情况，但会增加启动时的数据库检查成本。

建议验证 SQL：

```sql
SELECT symbol, contract_month, con_id, MIN(time), MAX(time), COUNT(*)
FROM futures_minute_bars
GROUP BY symbol, contract_month, con_id
ORDER BY symbol, contract_month;
```

检查是否已有新旧合约重叠分钟：

```sql
SELECT symbol, COUNT(*) AS overlapping_minutes
FROM (
    SELECT symbol, time
    FROM futures_minute_bars
    GROUP BY symbol, time
    HAVING COUNT(DISTINCT con_id) > 1
) x
GROUP BY symbol
ORDER BY symbol;
```

## 第三阶段：生成 roll calendar

使用 backfiller 的 roll calendar 命令生成 `futures_roll_events`。先对已完成
overlap 下载的品种执行；不要对仍在下载的品种执行。

预览 SPI，不写数据库：

```bash
python -m backfiller.main --roll-calendar --only SPI --dry-run
```

确认后写入，并替换该 symbol 的旧 roll events：

```bash
python -m backfiller.main --roll-calendar --only SPI --replace-rolls
```

参数：

- `--confirm-days`：新合约成交量或 bar_count 连续超过旧合约 N 天后换月，默认 `2`
- `--fallback-days`：没有活跃度切换信号时，使用旧合约到期前 N 个工作日，默认 `5`
- `--dry-run`：只打印结果，不写入 `futures_roll_events`
- `--replace-rolls`：写入前删除目标 symbol 的旧 roll events

当前规则：

1. 每个 symbol 按 `contract_month` 排序。
2. 对相邻合约找重叠交易日。
3. 若新合约 `volume` 或 `bar_count` 连续 N 天超过旧合约，则选择这段连续信号的第一天。
4. 如果没有活跃度切换信号，则 fallback 到旧合约到期前 N 个工作日。
5. 在 roll 点计算：
   - `price_gap = new_price - old_price`
   - `ratio = new_price / old_price`
6. roll_time 暂时设为该 UTC 日期的 `00:00:00`，后续连续合约生成时按日期边界切换。

检查已写入的 roll events：

```sql
SELECT symbol, from_con_id, to_con_id, roll_time, roll_rule, price_gap, ratio
FROM futures_roll_events
WHERE symbol = 'SPI'
ORDER BY roll_time;
```

## 第四阶段：生成连续期货数据

从 raw 表和 roll events 生成三套数据：

1. `continuous_futures_raw`
   - 只选择当前合约，不调价。
   - 用于实盘价格对齐和审计。

2. `continuous_futures_back_adjusted`
   - 差分后向调整。
   - 适合传统技术指标和价差形态策略。

3. `continuous_futures_ratio_adjusted`
   - 比例调整。
   - 适合收益率建模、RL、多品种归一化。

日 K 数据也有对应的连续期货 view：

1. `continuous_futures_daily_raw`
   - 从 `futures_daily_bars` 按 roll calendar 选择当前合约，不调价。

2. `continuous_futures_daily_back_adjusted`
   - 按 `futures_roll_events.price_gap` 做差分后向调整。

3. `continuous_futures_daily_ratio_adjusted`
   - 按 `futures_roll_events.ratio` 做比例后向调整。

所有连续数据都应保留：

- `source_con_id`
- `contract_month`
- `is_roll_bar`
- `bars_since_roll`
- `bars_to_roll`
- `is_zero_trade`

## 第五阶段：质量检查

每次生成连续数据后输出 QA 报告：

1. 每个 symbol 的合约月份覆盖范围。
2. 每次 roll 的旧合约价格、新合约价格、gap、ratio。
3. roll 前后最大分钟收益。
4. `volume=0` 或 `bar_count=0` 比例。
5. 超过阈值的异常分钟收益列表。

重点回归检查：

- `ZC 2026-03-12` 不应再出现 476 到 448 的人为分钟跳变。
- `HG 2025-07-30` 不应再把不同合约的价差作为可交易收益。

## 后续开发顺序

1. 实现 roll calendar 生成脚本。
2. 实现连续期货 materialized view 或生成表。
3. 增加 QA 报告命令。
4. API/回测入口改为读取连续期货数据，而不是 `minute_bars`。

## 历史日 K 下载

回测如果需要日 K 指标预热，使用新的 `--pull-daily` 命令。它会把日 K
起始日期设为 `config.start` 往前推 31 个自然日，保证至少比 1 分钟数据
提前约一个月。

下载全部配置产品的日 K：

```bash
python -m backfiller.main --pull-daily
```

只下载指定产品：

```bash
python -m backfiller.main --pull-daily --only SPI MYM N225M
```

写入位置：

- 期货：`futures_daily_bars`，主键 `(symbol, con_id, date_str)`，每个合约月
  单独保存，不覆盖、不压平。
- 非期货：`daily_bars`，主键 `(symbol, date_str)`。

日 K 下载同样支持断点续传：

- 期货日 K checkpoint key：`FUT_DAILY:<con_id>:<contract_month>`
- 非期货日 K checkpoint key：`DAILY:<symbol>`
- 只有窗口请求成功并写库后才会标记完成。
- 如果中断发生在窗口中途，该窗口下次会重新下载。
- 首次创建 checkpoint 时会检查数据库已有覆盖，完整覆盖的窗口不会重复请求。

下载完成后可以检查：

```sql
SELECT symbol, contract_month, con_id, MIN(time), MAX(time), COUNT(*)
FROM futures_daily_bars
GROUP BY symbol, contract_month, con_id
ORDER BY symbol, contract_month;
```

回测读取日 K 连续数据时，优先使用：

```sql
SELECT *
FROM continuous_futures_daily_back_adjusted
WHERE symbol = 'SPI'
ORDER BY time;
```
