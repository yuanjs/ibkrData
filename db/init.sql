-- IBKR Data System - Database Initialization

-- 行情数据表
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

-- 压缩配置 (按品种分段压缩，15天前的chunk自动压缩)
ALTER TABLE ticks SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'symbol',
  timescaledb.compress_orderby = 'time DESC'
);
SELECT add_compression_policy('ticks', compress_after => INTERVAL '15 days');

-- 日K线表 (存储来自 IBKR 的准确日线数据)
CREATE TABLE daily_bars (
    symbol      TEXT NOT NULL,
    date_str    TEXT NOT NULL, -- IBKR返回的交易日字符串 "YYYYMMDD"
    time        TIMESTAMPTZ,   -- 对应交易日开始的 UTC 时间
    open        NUMERIC(12,4),
    high        NUMERIC(12,4),
    low         NUMERIC(12,4),
    close       NUMERIC(12,4),
    volume      BIGINT,
    PRIMARY KEY (symbol, date_str)
);

-- 订阅配置表
CREATE TABLE subscriptions (
    symbol      TEXT PRIMARY KEY,
    sec_type    TEXT DEFAULT 'STK',
    exchange    TEXT DEFAULT 'SMART',
    currency    TEXT DEFAULT 'USD',
    active      BOOLEAN DEFAULT TRUE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- 账户快照表
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

-- 持仓表
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

-- 订单表（普通表，非 hypertable，以支持 UNIQUE 约束实现 UPSERT）
CREATE TABLE orders (
    order_id        BIGINT PRIMARY KEY,
    account_id      TEXT,
    symbol          TEXT,
    action          TEXT,
    order_type      TEXT,
    quantity        NUMERIC(16,4),
    limit_price     NUMERIC(12,4),
    status          TEXT,
    filled_qty      NUMERIC(16,4),
    avg_fill_price  NUMERIC(12,4),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX ON orders (status);
CREATE INDEX ON orders (updated_at DESC);

-- 成交记录表
CREATE TABLE executions (
    time        TIMESTAMPTZ NOT NULL,
    exec_id     TEXT NOT NULL UNIQUE,
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

-- 设置表
CREATE TABLE settings (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);
INSERT INTO settings VALUES
    ('ib_host',                  '127.0.0.1',         NOW()),
    ('ib_port',                  '4002',               NOW()),
    ('ib_client_id',             '1',                  NOW()),
    ('account_refresh_interval', '30',                 NOW()),
    ('tick_retention_days',      '180',                NOW()),
    ('default_chart_interval',   '1min',               NOW()),
    ('ui_language',              'zh',                 NOW()),
    ('ui_timezone',              'America/New_York',   NOW());

-- 初始订阅
INSERT INTO subscriptions (symbol, sec_type, exchange, currency) VALUES
    ('SPI',   'FUT',  'SNFE',     'AUD'),
    ('USD.JPY', 'CASH', 'IDEALPRO', 'JPY'),
    ('MYM',   'FUT',  'CBOT',     'USD'),
    ('N225M', 'FUT',  'OSE.JPN',  'JPY'),
    ('10Y',   'FUT',  'CBOT',     'USD'),
    ('ZC',    'FUT',  'CBOT',     'USD')
ON CONFLICT (symbol) DO NOTHING;

-- 告警配置表
CREATE TABLE alerts (
    id          SERIAL PRIMARY KEY,
    symbol      TEXT,
    alert_type  TEXT,
    threshold   NUMERIC(16,4),
    active      BOOLEAN DEFAULT TRUE,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- 1分钟聚合视图
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

-- 数据保留策略 (延长至365天以支持长期回测)
SELECT add_retention_policy('ticks', INTERVAL '365 days');
