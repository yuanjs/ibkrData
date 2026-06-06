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

4. 修改 `--status`。
   - 期货状态从 `futures_minute_bars` 读取。
   - 非期货状态仍从 `minute_bars` 读取。

## 数据库部署步骤

在运行新版 backfiller 之前执行：

```bash
psql "$DB_URL" -f db/migration_003_futures_raw_bars.sql
```

当前已有的 `minute_bars` 期货数据可保留为诊断数据，但不要作为正式回测源。

## 第二阶段：重新下载期货 raw 数据

1. 清理 backfiller 对应品种的 progress 文件，避免旧进度跳过重新下载。
2. 对期货品种重新执行 backfill。
3. 验证每个品种在 `futures_minute_bars` 中存在多个 `con_id`。
4. 抽查同一 roll 附近是否能同时看到前后两个合约的数据。

建议验证 SQL：

```sql
SELECT symbol, contract_month, con_id, MIN(time), MAX(time), COUNT(*)
FROM futures_minute_bars
GROUP BY symbol, contract_month, con_id
ORDER BY symbol, contract_month;
```

## 第三阶段：生成 roll calendar

建立脚本或服务生成 `futures_roll_events`：

1. 每个 symbol 按 `contract_month` 排序。
2. 根据品种配置选择 roll 规则：
   - 指数期货：到期前 N 个交易日或固定交易日。
   - 农产品/金属：优先避开首通知日，使用流动性切换或到期前 N 日。
   - 没有成交量/持仓量可靠数据时，使用配置化固定规则。
3. 在 roll 点计算：
   - `price_gap = new_price - old_price`
   - `ratio = new_price / old_price`
4. roll 点必须落在 session 边界或明确的可交易切换点，不能发生在任意盘中分钟。

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
