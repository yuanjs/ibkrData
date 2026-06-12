-- IBKR Data System - Migration 012: Contract identity on trading tables
--
-- Positions, orders, and executions must keep the real futures contract
-- identity so roll-time audit and close-position routing do not collapse
-- different contract months into the same symbol.

ALTER TABLE positions
    ADD COLUMN IF NOT EXISTS con_id BIGINT,
    ADD COLUMN IF NOT EXISTS local_symbol TEXT,
    ADD COLUMN IF NOT EXISTS contract_month TEXT,
    ADD COLUMN IF NOT EXISTS trading_class TEXT,
    ADD COLUMN IF NOT EXISTS exchange TEXT,
    ADD COLUMN IF NOT EXISTS currency TEXT,
    ADD COLUMN IF NOT EXISTS multiplier TEXT;

CREATE INDEX IF NOT EXISTS idx_positions_contract_identity
    ON positions (account_id, symbol, con_id, local_symbol, time DESC);

ALTER TABLE orders
    ADD COLUMN IF NOT EXISTS con_id BIGINT,
    ADD COLUMN IF NOT EXISTS local_symbol TEXT,
    ADD COLUMN IF NOT EXISTS contract_month TEXT,
    ADD COLUMN IF NOT EXISTS trading_class TEXT,
    ADD COLUMN IF NOT EXISTS exchange TEXT,
    ADD COLUMN IF NOT EXISTS currency TEXT,
    ADD COLUMN IF NOT EXISTS multiplier TEXT;

CREATE INDEX IF NOT EXISTS idx_orders_contract_identity
    ON orders (account_id, symbol, con_id, local_symbol, updated_at DESC);

ALTER TABLE executions
    ADD COLUMN IF NOT EXISTS con_id BIGINT,
    ADD COLUMN IF NOT EXISTS local_symbol TEXT,
    ADD COLUMN IF NOT EXISTS contract_month TEXT,
    ADD COLUMN IF NOT EXISTS trading_class TEXT,
    ADD COLUMN IF NOT EXISTS exchange TEXT,
    ADD COLUMN IF NOT EXISTS currency TEXT,
    ADD COLUMN IF NOT EXISTS multiplier TEXT;

CREATE INDEX IF NOT EXISTS idx_executions_contract_identity
    ON executions (account_id, symbol, con_id, local_symbol, time DESC);
