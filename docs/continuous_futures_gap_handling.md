# 连续期货合约换月跳空（Gap）处理方法

## 背景

通过 `backfiller` 从 IBKR 拉取的期货历史数据是**未调整的原始连续合约链**。每个季度主力合约在其活跃期内保存原始价格，换月时合约切换会产生价格跳空（Gap）。这个 Gap 来源于两个合约在换月时的基差（Basis），并非市场真实价格变动。

以下方法可以在本地对数据进行平滑处理，适应不同的使用场景。

---

## 方法一：差分后向调整（Subtraction Back-Adjustment）

**原理：** 在每个换月点计算新旧合约的收盘价差，将价差累加后**平移换月前所有价格**。

```
换月日: 旧合约收盘 7500, 新合约收盘 7600 → 价差 = +100
调整后: 换月前所有 bar 的 OHLC 都 +100
        换月后价格不变
```

**特点：**
- ✅ 保留价格波动形态（波动率结构不变）
- ⚠️ 早期价格可能因多次累加后变为负值（农产品/低价格品种常见）
- 最常用的处理方式

```python
def back_adjust(bars: list[dict], roll_dates: list[datetime]) -> list[dict]:
    """差分后向调整。
    
    Args:
        bars: 按时间升序排列的 K 线列表，每个元素含 time/open/high/low/close/contract_id
        roll_dates: 每个换月点的日期列表，对应新合约开始使用的日期
    
    Returns:
        调整后的 K 线列表（open/high/low/close 被平移）
    """
    bars = sorted(bars, key=lambda x: x['time'])
    cumulative_gap = 0.0
    roll_idx = 0
    
    for bar in reversed(bars):
        if roll_idx < len(roll_dates) and bar['time'].date() < roll_dates[roll_idx]:
            roll_idx += 1
            # 计算此次换月的价差
            pass  # 实际在遍历中计算 gap
        
        bar['open'] -= cumulative_gap
        bar['high'] -= cumulative_gap
        bar['low'] -= cumulative_gap
        bar['close'] -= cumulative_gap
    
    return bars
```

**适用场景：** 时序预测、技术指标、依赖价格波动形态的策略。

---

## 方法二：比例调整（Ratio / Proportional Adjustment）

**原理：** 在换月点计算新旧合约收盘价的比例，将换月前所有价格乘以该比例。

```
换月日: 旧合约收盘 7500, 新合约收盘 7600 → 比例 = 7600/7500 ≈ 1.0133
调整后: 换月前所有 bar 的 OHLC 均 × 1.0133
```

**特点：**
- ✅ 价格不会变为负值
- ✅ 对数收益率序列保持完整
- ✅ 适合 RL 模型（价格归一化后影响最小）
- ⚠️ 低价格品种波动率可能被扭曲

```python
import pandas as pd
import numpy as np

def ratio_adjust(df: pd.DataFrame, roll_dates: list) -> pd.DataFrame:
    """比例调整。
    
    以最新合约为基准，向前逐段乘以换月比例。
    df 需包含列: time, open, high, low, close, contract_id
    """
    df = df.sort_values('time').copy()
    cumulative_ratio = 1.0
    result = []
    
    for i in range(len(roll_dates) - 1, -1, -1):
        mask = df['time'].dt.date < roll_dates[i]
        old_prices = df.loc[mask, ['open', 'high', 'low', 'close']]
        new_first = df.loc[~mask].iloc[0]
        old_last = df.loc[mask].iloc[-1]
        
        ratio = new_first['close'] / old_last['close']
        cumulative_ratio *= ratio
        
        df.loc[mask, ['open', 'high', 'low', 'close']] *= cumulative_ratio
    
    return df
```

**适用场景：** 强化学习、对数收益率建模、多品种比较。

---

## 方法三：不做调整（Raw / Unadjusted）— 默认

直接使用从 IBKR 获取的原始数据，保留每个合约自己的价格水平。

```
合约价格: [APM4]_[APU4]_[APZ4]_[APH5]_...
                                ↑ 换月 Gap 原样保留
```

**特点：**
- ✅ 零数据失真
- ✅ 每个合约的价格绝对值真实
- ⚠️ 换月处的价格跳空不是市场真实变动
- ⚠️ 需要模型能理解换月事件

**如果选择不做调整，建议在特征中加入辅助信息：**

```python
def add_roll_features(df: pd.DataFrame) -> pd.DataFrame:
    """添加入换月特征，帮助模型理解 Gap。"""
    # 距下次换月的 bar 数
    df['bars_since_roll'] = df.groupby('contract_id').cumcount()
    df['bars_to_next_roll'] = df.groupby('contract_id').cumcount(ascending=False)
    
    # 是否为换月后的前 N 根 K 线
    df['is_post_roll'] = (df['bars_since_roll'] < 20).astype(int)
    
    # 当前合约在序列中的序号
    df['contract_rank'] = df['contract_id'].factorize()[0]
    
    return df
```

**适用场景：** 模型自行学习换月模式、特征工程中包含合约元信息。

---

## 方法四：前置调整（Panama / Forward Adjustment）

**原理：** 固定**当前最新合约**的价格为基准，把所有历史合约的价格向前对齐至当前水平。

```
当前合约价格 = 10000
最老合约原始价格 = 1000
→ 调整后 = 10000 - (各次换月累计价差)
```

**特点：**
- ✅ 当前价格不变（可直接映射到实盘信号）
- ⚠️ 每次换月后历史数据需要重新调整

**适用场景：** 实盘策略信号与历史回测直接对齐。

---

## 方法对比总结

| 方法 | 价格形态 | 波动率结构 | 负值风险 | RL 友好度 | 实盘对齐 |
|------|---------|-----------|---------|----------|---------|
| 差分调整 | 保留 | 保留 | ⚠️ 有 | 中 | 否 |
| 比例调整 | 保留 | 轻微变化 | ✅ 无 | **高** | 否 |
| 不做调整 | 原始 | 原始 | ✅ 无 | 中（需加特征） | ✅ |
| 前置调整 | 保留 | 保留 | ⚠️ 有 | 中 | **✅** |

---

## 实施建议

- **backfiller 当前输出** — 原始未调整数据（方法三）
- **RL 模型推荐** — 比例调整（方法二），保持收益率结构完整
- **迁移学习/多品种** — 差分调整（方法一），统一的参考基准
- **实盘信号对齐** — 前置调整（方法四）

具体实施会在后续开发中根据使用场景决定。
