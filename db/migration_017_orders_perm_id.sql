-- 订单表主键从 order_id 迁移到 perm_id。
--
-- 原因：orderId 由每个 API client 各自从 1 开始编号。IB Gateway 启用
-- Master API client ID 后 collector 会收到其他交易程序的订单事件，
-- 不同 client 的 orderId 会撞主键并互相覆盖。permId 由 IBKR 全局分配，
-- 跨 client、跨会话唯一，是订单的稳定标识。

ALTER TABLE orders ADD COLUMN IF NOT EXISTS perm_id BIGINT;
ALTER TABLE orders ADD COLUMN IF NOT EXISTS client_id BIGINT;

-- 存量行全部来自 collector 自身 client，用 order_id 回填即可保证唯一。
UPDATE orders SET perm_id = order_id WHERE perm_id IS NULL;

ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_pkey;
ALTER TABLE orders ALTER COLUMN perm_id SET NOT NULL;
ALTER TABLE orders ADD PRIMARY KEY (perm_id);

CREATE INDEX IF NOT EXISTS idx_orders_order_id ON orders (order_id);
