# IBKR 实时数据系统 需求文档

## 1. 项目概述

构建一套完整的金融数据平台，从 Interactive Brokers (IBKR) 获取实时市场行情、账户及交易数据，以1秒粒度持久化存储，并通过 Web 前端提供实时展示、历史查询、账户监控与交易记录分析功能。

---

## 2. 系统架构

```
┌─────────────┐    ┌──────────────────┐    ┌─────────────┐
│ IB Gateway  │───▶│  数据采集服务     │───▶│    Redis    │
│  (本地/VPS) │    │  (Python/        │    │  Pub/Sub +  │
└─────────────┘    │   ib_insync)     │    │  Cache      │
                   └──────────────────┘    └──────┬──────┘
                            │                     │
                            ▼                     ▼
                   ┌──────────────────┐    ┌─────────────┐
                   │  TimescaleDB     │    │  API 服务   │
                   │  (时序数据存储)  │◀───│  (FastAPI)  │
                   └──────────────────┘    └──────┬──────┘
                                                  │ WebSocket/REST
                                                  ▼
                                          ┌─────────────┐
                                          │  前端 Web   │
                                          │  (React)    │
                                          └─────────────┘
```

---

## 3. 后端需求

### 3.1 数据采集服务

| 需求 | 描述 |
|------|------|
| IBKR连接 | 通过 `ib_insync` 连接 IB Gateway，支持自动重连（心跳间隔30s） |
| 订阅管理 | 支持动态添加/删除订阅标的（股票、ETF、期货） |
| 行情数据 | 每条记录包含：symbol, timestamp, bid, ask, last, volume, open, high, low, close |
| 账户数据 | 每30s轮询：净值、现金余额、保证金、每日盈亏；持仓数据实时订阅 |
| 交易数据 | 实时监听订单状态变化；启动时拉取当日成交记录和历史订单 |
| 写入频率 | 行情每秒批量写入；账户/持仓变化时写入；成交/订单变化时写入 |
| 实时推送 | 行情发布到 `market:{symbol}`；账户变化发布到 `account:update`；订单变化发布到 `order:update` |
| 错误处理 | 连接断开后指数退避重连，最大重试间隔60s |

### 3.2 数据存储（TimescaleDB）

```sql
-- 主表（自动按时间分区）
CREATE TABLE ticks (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    bid         NUMERIC(12,4),
    ask         NUMERIC(12,4),
    last        NUMERIC(12,4),
    volume      BIGINT,
    open        NUMERIC(12,4),
    high        NUMERIC(12,4),
    low         NUMERIC(12,4),
    close       NUMERIC(12,4)
);
SELECT create_hypertable('ticks', 'time');
CREATE INDEX ON ticks (symbol, time DESC);
```

数据保留策略：原始1s数据保留30天，自动聚合为1min数据保留1年。

### 3.3 账户与交易数据存储

**账户快照表**（每30s一条）：
- 字段：time, account_id, net_liquidation, total_cash, available_funds, excess_liquidity, init_margin_req, maint_margin_req, daily_pnl, unrealized_pnl, realized_pnl

**持仓表**（变化时更新）：
- 字段：time, account_id, symbol, sec_type, quantity, avg_cost, market_value, unrealized_pnl, realized_pnl

**订单表**（状态变化时写入）：
- 字段：time, order_id, account_id, symbol, action(BUY/SELL), order_type, quantity, limit_price, status, filled_qty, avg_fill_price, created_at, updated_at

**成交记录表**（每笔成交写入）：
- 字段：time, exec_id, order_id, account_id, symbol, side, quantity, price, commission

### 3.4 API 服务（FastAPI）

**REST 接口：**

| 方法 | 路径 | 描述 |
|------|------|------|
| GET | `/api/symbols` | 获取所有订阅标的列表 |
| POST | `/api/symbols` | 添加订阅标的 |
| DELETE | `/api/symbols/{symbol}` | 取消订阅 |
| GET | `/api/history/{symbol}` | 查询历史行情（参数：start, end, interval） |
| GET | `/api/history/{symbol}/export` | 导出行情CSV |
| GET | `/api/status` | 系统状态（IBKR连接状态、数据延迟） |
| GET | `/api/account` | 当前账户净值/现金/保证金/盈亏快照 |
| GET | `/api/account/history` | 账户净值历史（参数：start, end） |
| GET | `/api/positions` | 当前持仓列表 |
| GET | `/api/orders` | 订单列表（参数：status=open/all, start, end） |
| GET | `/api/trades` | 成交记录（参数：start, end, symbol） |
| GET | `/api/trades/export` | 导出成交记录CSV |
| GET | `/api/pnl` | 盈亏报告（按标的汇总已实现/未实现盈亏） |

**WebSocket 接口：**

| 路径 | 描述 |
|------|------|
| `WS /ws/market` | 实时行情推送，客户端发送 `{"subscribe": ["AAPL","TSLA"]}` |
| `WS /ws/account` | 账户/持仓/盈亏实时推送 |
| `WS /ws/orders` | 订单状态实时推送 |

**认证：** JWT Bearer Token，单用户模式，token通过环境变量配置。

---

## 4. 前端需求

### 4.1 页面结构

```
├── 实时监控页（默认页）
├── 账户总览页
├── 持仓管理页
├── 订单与成交页
├── 历史数据查询页
└── 系统设置页
```

### 4.2 实时监控页

- **行情列表**：表格展示所有订阅标的的最新 bid/ask/last/volume，价格变动时高亮闪烁（涨绿跌红）
- **实时图表**：点击标的后展示该标的的实时K线图（使用 TradingView Lightweight Charts）
  - 支持切换周期：1s / 5s / 1min
  - 图表自动滚动到最新数据
- **连接状态栏**：顶部显示 IBKR 连接状态（绿色●已连接 / 红色●断开）及最后数据时间

### 4.3 账户总览页

- **账户摘要卡片**：净值、现金余额、可用资金、保证金占用
- **每日盈亏**：当日已实现/未实现盈亏，数字颜色涨绿跌红
- **净值历史折线图**：可选时间范围（近7天/近30天）
- **实时更新**：通过 `WS /ws/account` 推送，数值变化时高亮

### 4.4 持仓管理页

- **持仓表格**：symbol, 数量, 均价, 市值, 未实现盈亏, 盈亏%
- **盈亏排行**：按未实现盈亏排序，快速识别盈亏最大标的
- **点击标的**：跳转到该标的的历史行情图表

### 4.5 订单与成交页

- **实时订单列表**：展示当前挂单，状态实时更新（通过 `WS /ws/orders`）
- **成交记录表格**：时间、标的、方向、数量、成交价、手续费
- **历史订单查询**：时间范围筛选 + 状态筛选（已成交/已取消/全部）
- **盈亏报告**：按标的汇总已实现盈亏，支持导出CSV

- 标的选择器（支持搜索）
- 时间范围选择器（快捷选项：今日/昨日/近7天/近30天 + 自定义）
- 数据周期选择：1s / 1min / 5min / 1h
- 结果展示：K线图 + 数据表格（分页）
- 导出按钮：下载 CSV

### 4.4 系统设置页

设置页分为以下四个分组：

**IBKR 连接配置**
- IB Gateway 主机地址、端口、客户端ID
- 连接测试按钮（实时验证连通性）

**数据采集配置**
- 订阅标的管理（添加/删除，支持搜索验证）
- 原始1s数据保留天数
- 账户数据刷新间隔（默认30s，可调5s~300s）

**告警/通知配置**
- 价格告警：为指定标的设置上下限价格，触发时前端弹窗提示
- 盈亏告警：单日亏损超过阈值时提示
- 通知方式：前端弹窗（必选）

**前端显示配置**
- 默认K线周期（1s / 5s / 1min）
- 界面语言（中文/英文）
- 时区设置

所有设置持久化到数据库 `settings` 表，修改后实时生效（无需重启服务）。

### 4.5 通用要求

- 响应式布局，支持桌面和平板
- WebSocket 断线自动重连，重连期间显示"重连中..."提示
- 深色主题（金融软件惯例）

---

## 5. 技术栈汇总

| 层级 | 技术 |
|------|------|
| IBKR接入 | `ib_insync` (Python) |
| 数据采集 | Python 3.11+, asyncio |
| 消息队列 | Redis 7 |
| 数据库 | TimescaleDB 2.x (PostgreSQL 16) |
| 后端API | FastAPI + uvicorn |
| 前端框架 | React 18 + TypeScript |
| 图表库 | TradingView Lightweight Charts |
| 状态管理 | Zustand |
| 部署 | Docker Compose |

---

## 6. 非功能需求

| 指标 | 目标 |
|------|------|
| 数据写入延迟 | < 500ms（从IBKR到数据库） |
| WebSocket推送延迟 | < 200ms |
| 历史查询响应 | < 2s（查询30天内数据） |
| 系统可用性 | 交易时段内 99%+ |
| 数据完整性 | 采集服务重启后不丢失已收到数据 |
