# 手动平仓功能 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在账户页面持仓表格中为每个持仓增加"平仓"按钮，一键以市价单平仓并自动取消该品种的所有待成交关联订单。

**Architecture:** API 通过 Redis channel `order:command` 向 Collector 发送平仓指令，Collector 执行 "取消所有待成交订单 → 下市价平仓单" 两步操作，结果通过现有 `order:update` → WebSocket 通道实时推送回前端。

**Tech Stack:** ib_insync (placeOrder/cancelOrder), FastAPI, Redis Pub/Sub, React/TypeScript, React Native

---

## File Structure

| 操作 | 文件 | 改动说明 |
|------|------|---------|
| Modify | `collector/ibkr_client.py` | 新增 `cancel_orders_for_symbol()`、`place_market_order()` |
| Modify | `collector/main.py` | 新增 `order_command_listener()` 协程，注册到 tasks 列表 |
| Modify | `api/routers/orders.py` | 新增 `POST /api/positions/close` 端点 |
| Modify | `frontend/src/pages/Account.tsx` | 持仓表格加"平仓"按钮 + 确认弹窗 + 结果通知 |
| Modify | `mobile/app/account.tsx` | 同上，React Native 版本 |
| Modify | `tests/test_orders.py` | 新增平仓端点的测试用例 |

---

### Task 1: Collector — 给 IBKRClient 加下单方法

**Files:**
- Modify: `collector/ibkr_client.py` (末尾，`register_order_handlers` 方法之后)

- [ ] **Step 1: 新增 `cancel_orders_for_symbol()` 和 `place_market_order()`**

在 `register_order_handlers` 方法之后添加：

```python
def cancel_orders_for_symbol(self, symbol: str) -> list[int]:
    """取消某个品种所有未完成订单，返回取消的 order_id 列表。"""
    cancelled = []
    for trade in self.ib.openOrders():
        if trade.contract.symbol == symbol and \
           trade.orderStatus.status not in ('Filled', 'Cancelled', 'Inactive'):
            self.ib.cancelOrder(trade.order.orderId)
            cancelled.append(trade.order.orderId)
    return cancelled

def place_market_order(self, symbol: str, side: str, quantity: float,
                        sec_type: str, exchange: str, currency: str) -> tuple[int, str]:
    """下市价单，返回 (orderId, status)。"""
    from ib_insync import Contract, MarketOrder
    contract = Contract()
    contract.symbol = symbol
    contract.secType = sec_type
    contract.exchange = exchange
    contract.currency = currency
    order = MarketOrder(side, quantity)
    trade = self.ib.placeOrder(contract, order)
    return trade.order.orderId, trade.orderStatus.status
```

注意：`MarketOrder` 和 `Contract` 需要在文件顶部导入。检查当前 import：第 11 行已导入 `Contract`，但未导入 `MarketOrder`。

- [ ] **Step 2: 在文件顶部导入 MarketOrder**

```python
# 第 11 行修改
from ib_insync import IB, Contract, Stock, Ticker, MarketOrder
```

- [ ] **Step 3: 验证无语法错误**

运行：`python -c "import ast; ast.parse(open('collector/ibkr_client.py').read()); print('OK')"`

---

### Task 2: Collector — 新增 order_command_listener 协程

**Files:**
- Modify: `collector/main.py`

- [ ] **Step 1: 添加 `order_command_listener()` 协程**

在 `settings_listener` 函数之后添加（约第 150 行）：

```python
async def order_command_listener(client, pub):
    """监听 Redis order:command 通道，执行平仓指令。"""
    redis = aioredis.from_url(REDIS_URL)
    pubsub = redis.pubsub()
    await pubsub.subscribe("order:command")
    logger.info("Order command listener started, subscribed to order:command")

    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue
        try:
            data = json.loads(msg["data"])
            symbol = data["symbol"]
            close_id = data["close_id"]
            logger.info(f"Close position command received: {symbol} (close_id={close_id})")

            # 1. 取消该品种所有待成交订单
            cancelled_ids = client.cancel_orders_for_symbol(symbol)

            # 2. 下市价平仓单
            order_id, status = client.place_market_order(
                symbol, data["side"], data["quantity"],
                data["sec_type"], data["exchange"], data["currency"],
            )

            # 3. 发布带 close_id 的订单状态（供前端匹配回执）
            await pub.publish_order({
                "close_id": close_id,
                "order_id": order_id,
                "symbol": symbol,
                "side": data["side"],
                "quantity": data["quantity"],
                "status": status,
                "cancelled_orders": cancelled_ids,
            })
        except Exception as e:
            logger.error(f"order_command_listener error: {e}")
```

- [ ] **Step 2: 在 main() 的 tasks 列表中注册该协程**

找到 `main()` 函数中的 `tasks = [` 列表（第 317 行），在末尾添加：

```python
        asyncio.create_task(
            order_command_listener(client, pub),
            name="order_command_listener",
        ),
```

tasks 列表最终为：

```python
    tasks = [
        asyncio.create_task(tick_loop(client, pub), name="tick_loop"),
        asyncio.create_task(tick_flush_loop(tick_buffer), name="tick_flush"),
        asyncio.create_task(account_loop(client, writer, pub, ACCOUNT_REFRESH_INTERVAL), name="account_loop"),
        asyncio.create_task(settings_listener(redis_client), name="settings_listener"),
        asyncio.create_task(daily_bar_refresh_loop(client, writer, pool, daily_tracker), name="daily_bar_refresh"),
        asyncio.create_task(daily_bar_flush_loop(daily_tracker, writer), name="daily_bar_flush"),
        asyncio.create_task(trading_days_refresh_loop(client, daily_tracker), name="trading_days_refresh"),
        asyncio.create_task(order_command_listener(client, pub), name="order_command_listener"),
    ]
```

- [ ] **Step 3: 验证语法**

运行：`python -c "import ast; ast.parse(open('collector/main.py').read()); print('OK')"`

---

### Task 3: API — 新增 POST /api/positions/close 端点

**Files:**
- Modify: `api/routers/orders.py`

- [ ] **Step 1: 在文件头部添加新的 import**

```python
from pydantic import BaseModel
from uuid import uuid4
import json
import redis.asyncio as aioredis
from config import REDIS_URL
```

- [ ] **Step 2: 添加 Pydantic 请求模型**

在 `_OPEN_STATUSES` 之后添加：

```python
class ClosePositionRequest(BaseModel):
    symbol: str
```

- [ ] **Step 3: 添加 POST 端点**

在 `get_pnl` 函数之后添加：

```python
@router.post("/positions/close")
async def close_position(req: ClosePositionRequest):
    pool = await get_pool()
    close_id = str(uuid4())

    # 查最新持仓
    row = await pool.fetchrow(
        "SELECT DISTINCT ON (symbol) * FROM positions "
        "WHERE symbol = $1 ORDER BY symbol, time DESC",
        req.symbol
    )
    if not row or row["quantity"] == 0:
        from fastapi import HTTPException
        raise HTTPException(400, f"{req.symbol} 无持仓")

    # 自动计算平仓方向
    side = "SELL" if row["quantity"] > 0 else "BUY"
    qty = abs(row["quantity"])

    # 从 subscriptions 表获取品种参数
    sub = await pool.fetchrow(
        "SELECT sec_type, exchange, currency FROM subscriptions WHERE symbol = $1",
        req.symbol
    )
    sec_type = sub["sec_type"] if sub else row.get("sec_type", "STK")
    exchange = sub["exchange"] if sub else "SMART"
    currency = sub["currency"] if sub else "USD"

    # 发往 Redis
    r = aioredis.from_url(REDIS_URL)
    await r.publish("order:command", json.dumps({
        "close_id": close_id,
        "symbol": req.symbol,
        "side": side,
        "quantity": qty,
        "sec_type": sec_type,
        "exchange": exchange,
        "currency": currency,
    }))
    await r.aclose()

    return {
        "close_id": close_id,
        "symbol": req.symbol,
        "side": side,
        "quantity": qty,
        "message": "平仓指令已发送",
    }
```

- [ ] **Step 4: 验证语法**

运行：`python -c "import ast; ast.parse(open('api/routers/orders.py').read()); print('OK')"`

---

### Task 4: 前端 — Account.tsx 加平仓按钮

**Files:**
- Modify: `frontend/src/pages/Account.tsx`

完整替换文件内容（改动：增加 `api` import、`closePosition` 函数、持仓表操作列、toast 通知）：

- [ ] **Step 1: 改写 Account.tsx**

`import { api } from '../api/client'` 已存在，无需重复导入。

完整修改后的文件：

```tsx
import { useState } from 'react'
import { api } from '../api/client'
import { useAccountStore } from '../store/accountStore'
import { useOrderStore } from '../store/orderStore'

export function Account() {
  const summary = useAccountStore(s => s.summary) as Record<string, number>
  const positions = useAccountStore(s => s.positions) as Array<Record<string, unknown>>
  const orders = useOrderStore(s => s.orders) as Array<Record<string, unknown>>
  const [closePending, setClosePending] = useState<{ closeId: string; symbol: string } | null>(null)
  const [closeMsg, setCloseMsg] = useState<string | null>(null)

  const fmt = (v: number | undefined) => v != null ? v.toLocaleString('en-US', { style: 'currency', currency: 'USD' }) : '-'
  const pnlColor = (v: number | undefined) => v == null ? '' : v >= 0 ? '#26a641' : '#d32f2f'

  // 检查 close_id 是否已有成交回执
  const lastOrder = orders[0] as Record<string, unknown> | undefined
  const closeConfirmed = closePending && lastOrder?.close_id === closePending.closeId && lastOrder?.status === 'Filled'
  const closeRejected = closePending && lastOrder?.close_id === closePending.closeId && lastOrder?.status === 'Rejected'

  const handleClose = async (symbol: string) => {
    const pos = positions.find(p => p.symbol === symbol) as Record<string, unknown> | undefined
    if (!pos) return
    const sideLabel = (pos.quantity as number) > 0 ? '卖出' : '买入'
    const qty = Math.abs(pos.quantity as number)
    if (!window.confirm(`确定以市价平仓 ${symbol}？\n方向: ${sideLabel}\n数量: ${qty}`)) return

    try {
      setCloseMsg(null)
      const res = await api.post<{ close_id: string }>('/positions/close', { symbol })
      setClosePending({ closeId: res.close_id, symbol })
      setCloseMsg(`平仓指令已发送: ${symbol}`)
    } catch (e: any) {
      setCloseMsg(`平仓失败: ${e.message}`)
    }
  }

  const closeSymbol = closePending?.symbol ?? ''

  return (
    <div className="p-4 space-y-6">
      {closeMsg && (
        <div className="px-4 py-2 rounded text-sm" style={{
          backgroundColor: closeConfirmed ? '#1b5e20' : closeRejected ? '#b71c1c' : 'var(--bg-surface)',
          color: '#fff',
        }}>
          {closeConfirmed ? `${closeSymbol} 平仓成功 🎉` :
           closeRejected ? `${closeSymbol} 平仓失败` :
           closeMsg}
        </div>
      )}

      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {[
          { label: '净值', key: 'net_liquidation' },
          { label: '现金余额', key: 'total_cash' },
          { label: '可用资金', key: 'available_funds' },
          { label: '今日盈亏', key: 'daily_pnl' },
        ].map(({ label, key }) => (
          <div key={key} className="rounded-lg p-4" style={{ backgroundColor: 'var(--bg-surface)' }}>
            <div className="text-xs mb-1" style={{ color: 'var(--text-secondary)' }}>{label}</div>
            <div className="text-lg font-mono font-bold"
              style={{ color: key.includes('pnl') ? pnlColor(summary[key]) : 'var(--text-primary)' }}>
              {fmt(summary[key])}
            </div>
          </div>
        ))}
      </div>

      <div className="overflow-x-auto">
        <h2 className="text-sm mb-2" style={{ color: 'var(--text-secondary)' }}>当前持仓</h2>
        <table className="w-full text-sm min-w-[700px] md:min-w-0">
          <thead>
            <tr className="border-b" style={{ color: 'var(--text-secondary)', borderColor: 'var(--border)' }}>
              <th className="text-left py-2 px-3">标的</th>
              <th className="text-right py-2 px-3">数量</th>
              <th className="text-right py-2 px-3">均价</th>
              <th className="text-right py-2 px-3">市值</th>
              <th className="text-right py-2 px-3">未实现盈亏</th>
              <th className="text-center py-2 px-3">操作</th>
            </tr>
          </thead>
          <tbody>
            {positions.map((p, i) => {
              const sym = p.symbol as string
              const isPending = closePending?.symbol === sym
              return (
                <tr key={i} className="border-b" style={{
                  borderColor: 'var(--border-light)',
                  opacity: isPending ? 0.6 : 1,
                }}>
                  <td className="py-2 px-3 font-mono font-bold" style={{ color: 'var(--text-primary)' }}>{sym}</td>
                  <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>{p.quantity as number}</td>
                  <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>{fmt(p.avg_cost as number)}</td>
                  <td className="py-2 px-3 text-right font-mono" style={{ color: 'var(--text-primary)' }}>{fmt(p.market_value as number)}</td>
                  <td className="py-2 px-3 text-right font-mono" style={{ color: pnlColor(p.unrealized_pnl as number) }}>
                    {fmt(p.unrealized_pnl as number)}
                  </td>
                  <td className="py-2 px-3 text-center">
                    <button
                      onClick={() => handleClose(sym)}
                      disabled={!!isPending}
                      className="px-3 py-1 text-xs rounded font-medium"
                      style={{
                        backgroundColor: isPending ? 'var(--bg-raised)' : '#d32f2f',
                        color: isPending ? 'var(--text-secondary)' : '#fff',
                        border: 'none',
                        cursor: isPending ? 'not-allowed' : 'pointer',
                      }}
                    >
                      {isPending ? '平仓中...' : '平仓'}
                    </button>
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
```

- [ ] **Step 2: 用 `tsc` 检查类型**

运行：`cd frontend && npx tsc --noEmit --pretty 2>&1 | head -30`

---

### Task 5: 移动端 — account.tsx 加平仓按钮

**Files:**
- Modify: `mobile/app/account.tsx`

- [ ] **Step 1: 改写移动端 account.tsx**

完整替换为带平仓功能的版本：

```tsx
import { useState } from 'react'
import { View, Text, TouchableOpacity, StyleSheet, ScrollView, Alert } from 'react-native'
import { api } from '../src/api/client'
import { useAccountStore } from '../src/stores/accountStore'
import { useOrderStore } from '../src/stores/orderStore'
import { useTheme } from '../src/theme'

function fmt(v: number | undefined) {
  if (v == null) return '-'
  return '$' + v.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

function pnlColor(v: number | undefined) {
  if (v == null) return undefined
  return v >= 0 ? '#26a641' : '#d32f2f'
}

export default function Account() {
  const summary = useAccountStore(s => s.summary) as Record<string, number>
  const positions = useAccountStore(s => s.positions) as Array<Record<string, unknown>>
  const orders = useOrderStore(s => s.orders) as Array<Record<string, unknown>>
  const [closePending, setClosePending] = useState<{ closeId: string; symbol: string } | null>(null)
  const { colors } = useTheme()

  const lastOrder = orders[0] as Record<string, unknown> | undefined
  const closeConfirmed = closePending && lastOrder?.close_id === closePending.closeId && lastOrder?.status === 'Filled'

  const handleClose = (symbol: string) => {
    const pos = positions.find(p => p.symbol === symbol) as Record<string, unknown> | undefined
    if (!pos) return
    const qty = Math.abs(pos.quantity as number)
    const dir = (pos.quantity as number) > 0 ? '卖出' : '买入'

    Alert.alert(
      `平仓 ${symbol}`,
      `方向: ${dir}\n数量: ${qty}\n以市价全平？`,
      [
        { text: '取消', style: 'cancel' },
        {
          text: '确认平仓',
          style: 'destructive',
          onPress: async () => {
            try {
              const res = await api.post<{ close_id: string }>('/positions/close', { symbol })
              setClosePending({ closeId: res.close_id, symbol })
            } catch (e: any) {
              Alert.alert('平仓失败', e.message)
            }
          },
        },
      ],
    )
  }

  // 监听平仓成功
  if (closeConfirmed) {
    Alert.alert('平仓成功', `${closePending.symbol} 已平仓`)
    setClosePending(null)
  }

  const cards = [
    { label: '净值', key: 'net_liquidation' },
    { label: '现金余额', key: 'total_cash' },
    { label: '可用资金', key: 'available_funds' },
    { label: '今日盈亏', key: 'daily_pnl' },
  ]

  return (
    <ScrollView style={[styles.container, { backgroundColor: colors.background }]}>
      <View style={styles.cardGrid}>
        {cards.map(({ label, key }) => (
          <View key={key} style={[styles.card, { backgroundColor: colors.surface }]}>
            <Text style={[styles.cardLabel, { color: colors.textSecondary }]}>{label}</Text>
            <Text style={[
              styles.cardValue,
              { color: key.includes('pnl') ? pnlColor(summary[key]) ?? colors.textPrimary : colors.textPrimary },
            ]}>
              {fmt(summary[key])}
            </Text>
          </View>
        ))}
      </View>

      <View style={styles.section}>
        <Text style={[styles.sectionTitle, { color: colors.textSecondary }]}>当前持仓</Text>
        <View style={[styles.tableHeader, { borderBottomColor: colors.border }]}>
          <Text style={[styles.th, styles.colSym, { color: colors.textSecondary }]}>标的</Text>
          <Text style={[styles.th, styles.colNum, { color: colors.textSecondary }]}>数量</Text>
          <Text style={[styles.th, styles.colNum, { color: colors.textSecondary }]}>均价</Text>
          <Text style={[styles.th, styles.colNum, { color: colors.textSecondary }]}>市值</Text>
          <Text style={[styles.th, styles.colNum, { color: colors.textSecondary }]}>盈亏</Text>
          <Text style={[styles.th, styles.colAction, { color: colors.textSecondary }]}>操作</Text>
        </View>

        {positions.map((p, i) => {
          const sym = p.symbol as string
          const isPending = closePending?.symbol === sym
          return (
            <View key={i} style={[styles.tableRow, { borderBottomColor: colors.borderLight, opacity: isPending ? 0.6 : 1 }]}>
              <Text style={[styles.td, styles.colSym, { color: colors.textPrimary, fontFamily: 'monospace', fontWeight: '700' }]}>
                {sym}
              </Text>
              <Text style={[styles.td, styles.colNum, { color: colors.textPrimary, fontFamily: 'monospace' }]}>
                {p.quantity as number}
              </Text>
              <Text style={[styles.td, styles.colNum, { color: colors.textPrimary, fontFamily: 'monospace' }]}>
                {fmt(p.avg_cost as number)}
              </Text>
              <Text style={[styles.td, styles.colNum, { color: colors.textPrimary, fontFamily: 'monospace' }]}>
                {fmt(p.market_value as number)}
              </Text>
              <Text style={[styles.td, styles.colNum, { color: pnlColor(p.unrealized_pnl as number) ?? colors.textPrimary, fontFamily: 'monospace' }]}>
                {fmt(p.unrealized_pnl as number)}
              </Text>
              <View style={[styles.td, styles.colAction]}>
                <TouchableOpacity
                  onPress={() => handleClose(sym)}
                  disabled={isPending}
                  style={[styles.closeBtn, { backgroundColor: isPending ? colors.raised : '#d32f2f' }]}
                >
                  <Text style={{ color: '#fff', fontSize: 12, fontWeight: '600' }}>
                    {isPending ? '平仓中' : '平仓'}
                  </Text>
                </TouchableOpacity>
              </View>
            </View>
          )
        })}
      </View>
    </ScrollView>
  )
}

const styles = StyleSheet.create({
  container: { flex: 1, padding: 12 },
  cardGrid: { flexDirection: 'row', flexWrap: 'wrap', gap: 8 },
  card: { width: '48%', borderRadius: 8, padding: 14 },
  cardLabel: { fontSize: 12, marginBottom: 4 },
  cardValue: { fontSize: 17, fontFamily: 'monospace', fontWeight: '700' },
  section: { marginTop: 20 },
  sectionTitle: { fontSize: 13, marginBottom: 8 },
  tableHeader: { flexDirection: 'row', borderBottomWidth: 1, paddingVertical: 8, paddingHorizontal: 8 },
  th: { fontSize: 12 },
  tableRow: { flexDirection: 'row', borderBottomWidth: 1, paddingVertical: 10, paddingHorizontal: 8 },
  td: { fontSize: 13 },
  colSym: { flex: 1.2 },
  colNum: { flex: 1, textAlign: 'right' },
  colAction: { flex: 0.6, alignItems: 'center', justifyContent: 'center' },
  closeBtn: { paddingHorizontal: 10, paddingVertical: 4, borderRadius: 4 },
})
```

- [ ] **Step 2: 用 tsc 检查**

运行：`cd mobile && npx tsc --noEmit --pretty 2>&1 | head -30`

---

### Task 6: 测试 — 新增平仓端点测试

**Files:**
- Modify: `tests/test_orders.py`

- [ ] **Step 1: 添加测试用例**

在 `test_order_endpoints` 函数末尾或作为单独测试函数：

```python
@pytest.mark.asyncio
async def test_close_position_endpoint(api_base_url, auth_headers):
    async with httpx.AsyncClient(base_url=api_base_url, headers=auth_headers) as client:
        # 调用平仓端点（如果数据库没有真实持仓，会返回 400）
        resp = await client.post("/api/positions/close", json={"symbol": "MES"})

        # 两种可能结果：
        # 1) 200 — 平仓指令已发送（有持仓）
        # 2) 400 — 无持仓（测试环境可能无数据）
        assert resp.status_code in (200, 400)

        if resp.status_code == 200:
            data = resp.json()
            assert "close_id" in data
            assert data["symbol"] == "MES"
            assert data["side"] in ("BUY", "SELL")
            assert data["quantity"] > 0
            assert "平仓" in data["message"]
        else:
            data = resp.json()
            assert "无持仓" in data.get("detail", "")
```

- [ ] **Step 2: 运行测试**

运行：`cd tests && python -m pytest test_orders.py::test_close_position_endpoint -v`（需 API 和 DB 可连）

---

### Task 7: 提交

- [ ] **Step 1: Commit**

```bash
git add \
  collector/ibkr_client.py \
  collector/main.py \
  api/routers/orders.py \
  frontend/src/pages/Account.tsx \
  mobile/app/account.tsx \
  tests/test_orders.py
git commit -m "feat: add manual close position with market order

- Collector: place_market_order() + cancel_orders_for_symbol() in IBKRClient
- Collector: order_command_listener subscribes to Redis order:command
- API: POST /api/positions/close publishes close command to Redis
- Frontend: close button in Account positions table with confirmation
- Mobile: same close button for React Native
- Auto-cancels all open orders for the symbol before closing

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```
