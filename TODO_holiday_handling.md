# 节假日日K数据处理问题

## 问题描述

现有代码只处理了周末（weekday >= 5），没有处理节假日。

当节假日前一天（如圣诞节前夕12月24日）的 roll hour 之后收到 tick，
`_effective_date_str` 会把它归到12月25日（节假日），但实际应归到下一个交易日（12月26日）。

## 影响范围

- `collector/daily_tracker.py`: `_effective_date_str()` 的 skip_weekend 逻辑
- `collector/ibkr_client.py`: 历史数据导入时的周末偏移逻辑

## 修复方案

### 方案1：维护节假日列表（推荐）
在 `config.py` 的 `PRODUCT_ROLL_CONFIG` 里加 `holidays` 字段，
每年更新一次。`_effective_date_str` 和历史数据导入时跳过节假日。

### 方案2：使用 IBKR tradingHours
从 `reqContractDetails` 获取合约的 `tradingHours` 字段，
动态判断某天是否为交易日。更准确但实现复杂。

## 受影响产品

- MYM（美国期货）：感恩节、圣诞、元旦、独立日、劳工节、阵亡将士纪念日
- USD.JPY（外汇）：圣诞、元旦（外汇市场节假日较少）
- SPI（澳大利亚）：澳大利亚公众假日
- N225M（日本）：日本公众假日

## 优先级

低。节假日 bar 数量极少，且市场关闭时通常没有实时 tick。
真正有风险的只有节假日前一天 roll hour 之后的孤立 tick。
