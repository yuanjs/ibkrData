-- IBKR Data System - Migration 003: Raw futures minute bars
--
-- Stores futures history at the individual-contract level.  The old
-- minute_bars table is keyed by (symbol, time), which collapses different
-- contract months into one product stream and makes roll-gap adjustment
-- impossible to audit.  This table keeps con_id in the key so overlapping
-- contracts can coexist safely.

CREATE TABLE IF NOT EXISTS futures_minute_bars (
    time              TIMESTAMPTZ NOT NULL,
    symbol            TEXT NOT NULL,
    con_id            BIGINT NOT NULL,
    local_symbol      TEXT,
    trading_class     TEXT,
    contract_month    TEXT,
    last_trade_date   DATE,
    exchange          TEXT,
    currency          TEXT,
    multiplier        TEXT,
    open              NUMERIC(16,6),
    high              NUMERIC(16,6),
    low               NUMERIC(16,6),
    close             NUMERIC(16,6),
    volume            BIGINT,
    bar_count         INTEGER,
    source            TEXT NOT NULL DEFAULT 'IBKR',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, con_id, time)
);

SELECT create_hypertable('futures_minute_bars', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_futures_minute_bars_symbol_time
    ON futures_minute_bars (symbol, time DESC);

CREATE INDEX IF NOT EXISTS idx_futures_minute_bars_contract
    ON futures_minute_bars (symbol, contract_month, con_id, time DESC);

CREATE TABLE IF NOT EXISTS futures_roll_events (
    id                BIGSERIAL PRIMARY KEY,
    symbol            TEXT NOT NULL,
    from_con_id       BIGINT NOT NULL,
    to_con_id         BIGINT NOT NULL,
    roll_time         TIMESTAMPTZ NOT NULL,
    roll_rule         TEXT NOT NULL,
    price_gap         NUMERIC(16,6),
    ratio             NUMERIC(20,10),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, from_con_id, to_con_id, roll_time)
);
