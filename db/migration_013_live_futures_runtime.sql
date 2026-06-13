-- IBKR Data System - Migration 013: Live futures runtime metadata
--
-- Stores live-discovered futures contract metadata independently from bar
-- availability.  The live collector uses this to manage active/next contracts
-- without relying on backfiller-populated minute history.

CREATE TABLE IF NOT EXISTS futures_contracts (
    symbol            TEXT NOT NULL,
    con_id            BIGINT NOT NULL,
    local_symbol      TEXT,
    trading_class     TEXT,
    contract_month    TEXT,
    last_trade_date   DATE,
    exchange          TEXT,
    currency          TEXT,
    multiplier        TEXT,
    first_seen_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    source            TEXT NOT NULL DEFAULT 'live_collector',
    PRIMARY KEY (symbol, con_id)
);

CREATE INDEX IF NOT EXISTS idx_futures_contracts_symbol_month
    ON futures_contracts (symbol, contract_month, con_id);

CREATE INDEX IF NOT EXISTS idx_futures_contracts_symbol_last_trade
    ON futures_contracts (symbol, last_trade_date, con_id);

