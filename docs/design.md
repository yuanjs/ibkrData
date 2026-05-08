# IBKR 实时数据系统 设计文档

## 1. 项目结构

```
ibkrData/
├── collector/              # 数据采集服务
│   ├── main.py
│   ├── ibkr_client.py      # ib_insync 封装（行情+账户+交易）
│   ├── data_writer.py      # TimescaleDB 写入
│   ├── publisher.py        # Redis 发布
│   └── config.py
├── api/                    # FastAPI 服务
│   ├── main.py
│   ├── routers/
│   │   ├── symbols.py
│   │   ├── history.py
│   │   ├── account.py
│   │   ├── orders.py
│   │   ├── settings.py     # 设置读写
│   │   └── status.py
│   ├── websocket.py        # WS 推送管理（market/account/orders）
│   ├── db.py               # 数据库连接
│   └── auth.py             # JWT 认证
├── frontend/               # React 前端
│   ├── src/
│   │   ├── pages/
│   │   │   ├── Monitor.tsx
│   │   │   ├── Account.tsx
│   │   │   ├── Positions.tsx
│   │   │   ├── Orders.tsx
│   │   │   ├── History.tsx
│   │   │   └── Settings.tsx    # 设置页（四个分组）
│   │   ├── components/
│   │   │   ├── QuoteTable.tsx
│   │   │   ├── CandleChart.tsx
│   │   │   ├── StatusBar.tsx
│   │   │   ├── AccountSummary.tsx
│   │   │   ├── PositionTable.tsx
│   │   │   ├── OrderTable.tsx
│   │   │   └── TimeRangePicker.tsx
│   │   ├── store/
│   │   │   ├── marketStore.ts
│   │   │   ├── accountStore.ts  # 账户/持仓状态
│   │   │   └── orderStore.ts    # 订单状态
│   │   ├── hooks/
│   │   │   └── useWebSocket.ts
│   │   └── api/            # REST 客户端
│   └── package.json
├── db/
│   └── init.sql            # 建表脚本
├── docker-compose.yml
└── .env.example
```

---

## 2. 数据库设计

### 2.1 主表

```sql
CREATE TABLE ticks (
    time        TIMESTAMPTZ     NOT NULL,
    symbol      TEXT            NOT NULL,
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

### 2.2 订阅配置表

```sql
CREATE TABLE subscriptions (
    symbol      TEXT PRIMARY KEY,
    sec_type    TEXT DEFAULT 'STK',
    exchange    TEXT DEFAULT 'SMART',
    currency    TEXT DEFAULT 'USD',
    active      BOOLEAN DEFAULT TRUE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
```

### 2.3 账户快照表

```sql
CREATE TABLE account_snapshots (
    time                TIMESTAMPTZ NOT NULL,
    account_id          TEXT NOT NULL,
    net_liquidation     NUMERIC(16,2),
    total_cash          NUMERIC(16,2),
    available_funds     NUMERIC(16,2),
    excess_liquidity    NUMERIC(16,2),
    init_margin_req     NUMERIC(16,2),
    maint_margin_req    NUMERIC(16,2),
    daily_pnl           NUMERIC(16,2),
    unrealized_pnl      NUMERIC(16,2),
    realized_pnl        NUMERIC(16,2)
);
SELECT create_hypertable('account_snapshots', 'time');
```

### 2.4 持仓表

```sql
CREATE TABLE positions (
    time            TIMESTAMPTZ NOT NULL,
    account_id      TEXT NOT NULL,
    symbol          TEXT NOT NULL,
    sec_type        TEXT,
    quantity        NUMERIC(16,4),
    avg_cost        NUMERIC(12,4),
    market_value    NUMERIC(16,2),
    unrealized_pnl  NUMERIC(16,2),
    realized_pnl    NUMERIC(16,2)
);
SELECT create_hypertable('positions', 'time');
CREATE INDEX ON positions (account_id, symbol, time DESC);
```

### 2.5 订单表

```sql
CREATE TABLE orders (
    time            TIMESTAMPTZ NOT NULL,
    order_id        BIGINT NOT NULL,
    account_id      TEXT,
    symbol          TEXT,
    action          TEXT,        -- BUY/SELL
    order_type      TEXT,        -- LMT/MKT/STP
    quantity        NUMERIC(16,4),
    limit_price     NUMERIC(12,4),
    status          TEXT,        -- Submitted/Filled/Cancelled/...
    filled_qty      NUMERIC(16,4),
    avg_fill_price  NUMERIC(12,4),
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ
);
SELECT create_hypertable('orders', 'time');
CREATE INDEX ON orders (order_id, time DESC);
```

### 2.6 成交记录表

```sql
CREATE TABLE executions (
    time        TIMESTAMPTZ NOT NULL,
    exec_id     TEXT NOT NULL,
    order_id    BIGINT,
    account_id  TEXT,
    symbol      TEXT,
    side        TEXT,
    quantity    NUMERIC(16,4),
    price       NUMERIC(12,4),
    commission  NUMERIC(10,4)
);
SELECT create_hypertable('executions', 'time');
CREATE INDEX ON executions (account_id, symbol, time DESC);
```

### 2.7 设置表

```sql
CREATE TABLE settings (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

-- 默认值
INSERT INTO settings VALUES
    ('ib_host',                '127.0.0.1',  NOW()),
    ('ib_port',                '4002',       NOW()),
    ('ib_client_id',           '1',          NOW()),
    ('account_refresh_interval', '30',       NOW()),
    ('tick_retention_days',    '30',         NOW()),
    ('default_chart_interval', '1min',       NOW()),
    ('ui_language',            'zh',         NOW()),
    ('ui_timezone',            'America/New_York', NOW());
```

### 2.8 告警配置表

```sql
CREATE TABLE alerts (
    id          SERIAL PRIMARY KEY,
    symbol      TEXT,
    alert_type  TEXT,        -- price_above / price_below / daily_loss
    threshold   NUMERIC(16,4),
    active      BOOLEAN DEFAULT TRUE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
```

### 2.9 数据保留策略

```sql
-- 原始1s数据保留30天
SELECT add_retention_policy('ticks', INTERVAL '30 days');

-- 1分钟聚合视图（保留1年）
CREATE MATERIALIZED VIEW ticks_1min
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
    symbol,
    first(open, time)  AS open,
    max(high)          AS high,
    min(low)           AS low,
    last(close, time)  AS close,
    sum(volume)        AS volume
FROM ticks
GROUP BY bucket, symbol;
```

---

## 3. 后端服务设计

### 3.1 数据采集服务核心流程

```
启动
 │
 ├─ 连接 TimescaleDB / Redis / IB Gateway
 │
 ├─ 行情采集循环
 │    ├─ 从 subscriptions 表加载活跃标的
 │    ├─ reqMktData 订阅每个 symbol
 │    └─ 每秒：批量写入 ticks + PUBLISH market:{symbol}
 │
 ├─ 账户数据循环（每30s）
 │    ├─ reqAccountSummary → 写入 account_snapshots
 │    ├─ reqPositions → 写入 positions
 │    └─ PUBLISH account:update
 │
 └─ 交易数据监听（事件驱动）
      ├─ openOrderEvent → 写入/更新 orders + PUBLISH order:update
      ├─ orderStatusEvent → 更新 orders.status + PUBLISH order:update
      └─ execDetailsEvent → 写入 executions + PUBLISH order:update
```
           ├─ 收集当前 ticker 快照
           ├─ 批量 INSERT INTO ticks
           └─ PUBLISH market:{symbol} → Redis
```

**重连策略（指数退避）：**
```
重试间隔 = min(2^n 秒, 60秒)，n 从 0 开始
```

### 3.2 API 服务设计

**WebSocket 连接管理：**
```
客户端连接 /ws/market
 │
 ├─ 验证 JWT token (query param)
 ├─ 注册到 ConnectionManager
 │
 └─ 后台任务：订阅 Redis channel
      └─ 收到消息 → 转发给对应客户端
```

**历史数据查询参数：**
```
GET /api/history/{symbol}?start=&end=&interval=1s|1min|5min|1h
```
- `interval=1s`：查 `ticks` 原表（限30天内）
- `interval>=1min`：查 `ticks_1min` 聚合视图

**设置接口：**
```
GET  /api/settings          → 返回所有配置项 {key: value}
PUT  /api/settings          → 批量更新配置项
GET  /api/alerts            → 获取告警规则列表
POST /api/alerts            → 创建告警规则
DELETE /api/alerts/{id}     → 删除告警规则
```

设置变更后通过 Redis PUBLISH `settings:update` 通知采集服务热加载（如刷新间隔变更立即生效）。

---

## 4. 前端设计

### 4.1 状态管理（Zustand）

```typescript
// market store
{
  quotes: Record<string, Quote>,      // 最新行情
  connected: boolean,                  // WS连接状态
  lastUpdate: Date | null,
  subscribe: (symbols: string[]) => void,
}

// chart store
{
  activeSymbol: string | null,
  interval: '1s' | '5s' | '1min',
  candles: CandleData[],
}
```

### 4.2 WebSocket 消息格式

**客户端 → 服务端：**
```json
{ "subscribe": ["AAPL", "TSLA"] }
{ "unsubscribe": ["AAPL"] }
```

**服务端 → 客户端：**
```json
{
  "symbol": "AAPL",
  "time": "2026-04-24T09:30:01Z",
  "bid": 189.50,
  "ask": 189.52,
  "last": 189.51,
  "volume": 1234567
}
```

### 4.3 K线图聚合逻辑（前端）

1s 原始数据在前端聚合为 5s K线：
```
每5条1s数据 → 取 open[0], max(high), min(low), close[4], sum(volume)
```

---

## 5. 环境配置

### .env.example

```env
# IB Gateway
IB_HOST=127.0.0.1
IB_PORT=4002
IB_CLIENT_ID=1

# Database
DB_URL=postgresql://ibkr:password@timescaledb:5432/ibkrdata

# Redis
REDIS_URL=redis://redis:6379

# Auth
JWT_SECRET=change-me-in-production
JWT_TOKEN=your-static-token-here

# API
API_PORT=8000
```

---

## 6. Docker Compose 设计

```yaml
services:
  timescaledb:
    image: timescale/timescaledb:latest-pg16
    volumes: [pgdata:/var/lib/postgresql/data]
    environment:
      POSTGRES_DB: ibkrdata
      POSTGRES_USER: ibkr
      POSTGRES_PASSWORD: password

  redis:
    image: redis:7-alpine

  collector:
    build: ./collector
    depends_on: [timescaledb, redis]
    network_mode: host   # 需要访问本机 IB Gateway

  api:
    build: ./api
    ports: ["8000:8000"]
    depends_on: [timescaledb, redis]

  frontend:
    build: ./frontend
    ports: ["3000:80"]
    depends_on: [api]
```

> **注意**：collector 使用 `network_mode: host` 以访问运行在宿主机的 IB Gateway。
