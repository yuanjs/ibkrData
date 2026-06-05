# 手动平仓功能设计

## 概述

在账户页面的持仓列表中，为每个持仓提供"平仓"按钮，点击后以市价单自动平掉该仓位。平仓前自动取消该品种的所有待成交关联订单（止损单、止盈单等）。

---

## 数据流

```
Account 页面
  │ 点击持仓行的"平仓"按钮
  │ 弹出确认框 → 确认
  │
  ▼
POST /api/positions/close  { symbol }
  │
  │ API 层
  │ ├─ 查 DB 获取该标的当前持仓方向/数量
  │ ├─ 校验有仓位
  │ ├─ 自动计算平仓方向（多→SELL，空→BUY）
  │ ├─ 生成 close_id (UUID)
  │ └─ Redis PUB "order:command"
  │    { close_id, symbol, side, quantity, sec_type, exchange, currency }
  │
  ▼
Collector 层 (order_command_listener 协程)
  │ Redis SUB "order:command"
  │
  │ 1. ib.openOrders() 查询该 symbol 的待成交订单
  │ 2. ib.cancelOrder() 逐一取消
  │ 3. ib.placeOrder() 下市价平仓单
  │
  │ on_order() 回调自动触发
  │ ├─ write DB (upsert_order)
  │ └─ Redis PUB "order:update"
  │    { close_id, order_id, symbol, status, cancelled_orders }
  │
  ▼
API Redis forwarder → WebSocket → 前端
  │ 前端匹配 close_id → toast 通知平仓结果
  │ 订单列表实时更新
```

---

## 改动清单

### 1. Collector: ibkr_client.py

新增两个方法：

```python
def cancel_orders_for_symbol(self, symbol: str) -> list[int]:
    """取消某个品种的所有未完成订单，返回被取消的 order_id 列表"""
    cancelled = []
    for trade in self.ib.openOrders():
        if trade.contract.symbol == symbol and \
           trade.orderStatus.status not in ('Filled', 'Cancelled', 'Inactive'):
            self.ib.cancelOrder(trade.order.orderId)
            cancelled.append(trade.order.orderId)
    return cancelled

def place_market_order(self, symbol: str, side: str, quantity: float,
                        sec_type: str, exchange: str, currency: str) -> tuple[int, str]:
    """下市价单，返回 (orderId, status)"""
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.exchange = exchange
    contract.currency = currency
    order = MarketOrder(side, quantity)
    trade = self.ib.placeOrder(contract, order)
    return trade.order.orderId, trade.orderStatus.status
```

### 2. Collector: main.py

新增协程 `order_command_listener()`，在 `main()` 的 `asyncio.gather()` 中启动：

```python
async def order_command_listener(client, writer, pub):
    """监听 Redis order:command 通道，执行平仓指令"""
    redis = aioredis.from_url(REDIS_URL)
    pubsub = redis.pubsub()
    await pubsub.subscribe("order:command")

    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue
        data = json.loads(msg["data"])

        # 1. 取消该品种所有待成交订单
        cancelled_ids = client.cancel_orders_for_symbol(data["symbol"])

        # 2. 下市价平仓单
        order_id, status = client.place_market_order(
            data["symbol"], data["side"], data["quantity"],
            data["sec_type"], data["exchange"], data["currency"]
        )

        # 3. 发布带 close_id 的订单状态（供前端匹配回执）
        await pub.publish_order({
            "close_id": data["close_id"],
            "order_id": order_id,
            "symbol": data["symbol"],
            "side": data["side"],
            "quantity": data["quantity"],
            "status": status,
            "cancelled_orders": cancelled_ids,
        })
```

### 3. API: routers/orders.py

新增 Pydantic 模型和 POST 端点：

```python
class ClosePositionRequest(BaseModel):
    symbol: str

@router.post("/positions/close")
async def close_position(req: ClosePositionRequest):
    close_id = str(uuid4())

    pool = await get_pool()

    # 查持仓
    row = await pool.fetchrow(
        "SELECT DISTINCT ON (symbol) * FROM positions "
        "WHERE symbol = $1 ORDER BY symbol, time DESC",
        req.symbol
    )
    if not row or row["quantity"] == 0:
        raise HTTPException(400, f"{req.symbol} 无持仓")

    # 自动计算平仓方向
    side = "SELL" if row["quantity"] > 0 else "BUY"
    qty = abs(row["quantity"])

    # 从 subscriptions 表获取品种参数（exchange/currency 因品种而异）
    sub = await pool.fetchrow(
        "SELECT sec_type, exchange, currency FROM subscriptions WHERE symbol = $1",
        req.symbol
    )
    sec_type = sub["sec_type"] if sub else row.get("sec_type", "STK")
    exchange = sub["exchange"] if sub else "SMART"
    currency = sub["currency"] if sub else "USD"

    # 发往 Redis
    redis = await get_redis()
    await redis.publish("order:command", json.dumps({
        "close_id": close_id,
        "symbol": req.symbol,
        "side": side,
        "quantity": qty,
        "sec_type": sec_type,
        "exchange": exchange,
        "currency": currency,
    }))

    return {
        "close_id": close_id,
        "symbol": req.symbol,
        "side": side,
        "quantity": qty,
        "message": "平仓指令已发送",
    }
```

### 4. 前端: pages/Account.tsx

- 持仓表格每行最右列加"平仓"按钮
- 点击弹出 `window.confirm()` 确认
- 确认后调 `api.post('/positions/close', { symbol })`
- WebSocket 收到订单更新时匹配 `close_id`，弹出 toast 通知

### 5. 移动端: mobile/app/account.tsx

- 同上，确认用 `Alert.alert()`
- 通知用 `Alert.alert()` 展示平仓结果

### 6. 测试: tests/test_orders.py

新增测试用例：

```python
@pytest.mark.asyncio
async def test_close_position(api_base_url, auth_headers):
    async with httpx.AsyncClient(base_url=api_base_url, headers=auth_headers) as client:
        resp = await client.post("/api/positions/close", json={"symbol": "MES"})
        assert resp.status_code == 200
        data = resp.json()
        assert "close_id" in data
        assert data["symbol"] == "MES"
        assert data["side"] in ("BUY", "SELL")

        # 无持仓标的
        resp = await client.post("/api/positions/close", json={"symbol": "NONEXIST"})
        assert resp.status_code == 400
```

---

## 错误处理

| 场景 | API 响应 | 前端表现 |
|------|---------|---------|
| 该标的无持仓 | 400 "无持仓" | 弹出错误提示 |
| 网络/Redis 不可用 | 500 | alert("平仓指令发送失败") |
| IBKR 连接断开 | Collector 下单失败 → order:update 状态为 Rejected | WebSocket 收到 Rejected → toast 提示 |
| cancelOrder 失败 | 跳过（继续执行平仓），cancelled_orders 列表不含被跳过的 | 不影响平仓 |

---

## 安全

- `POST /api/positions/close` 复用现有的 `require_auth` Bearer token 认证
- Redis channel `order:command` 运行在内网，无额外鉴权（与现有 `order:update` 一致）

---

## 未纳入 / 后续可扩展

- 部分平仓（当前全仓平）
- 限价单平仓
- 一键反向开仓（平仓+开新仓）
- 批量平仓
- 平仓操作审计日志
