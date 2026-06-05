# Paper Trading Gateway 支持 — 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 支持同时连接实盘和模拟 IBKR Gateway，前端通过标签切换查看账户/订单数据。

**Architecture:** 单 collector 进程创建双 `IBKRClient` 实例。Live Client 阻塞启动（原有逻辑不变），Paper Client 后台 fire-and-forget 初始化，不阻塞 Live。Paper 只处理账户/订单，不订阅行情 tick。account_id 自动发现映射，DB 无 schema 变更。

**Tech Stack:** Python (ib_insync, asyncio, FastAPI), TypeScript (React, Zustand), Redis, PostgreSQL/TimescaleDB

---

### Task 1: 配置层 — Paper Gateway 连接参数

**Files:**
- Modify: `collector/config.py`
- Modify: `.env.example`

- [ ] **Step 1: 在 `collector/config.py` 末尾添加 Paper 参数**

```python
PAPER_IB_HOST = os.getenv("PAPER_IB_HOST", "")
PAPER_IB_PORT = int(os.getenv("PAPER_IB_PORT", "4002"))
PAPER_IB_CLIENT_ID = int(os.getenv("PAPER_IB_CLIENT_ID", "99"))

HAS_PAPER = bool(PAPER_IB_HOST)
```

- [ ] **Step 2: 更新 `.env.example`，在末尾追加**

```env
# Paper Gateway (optional — leave empty to disable)
PAPER_IB_HOST=
PAPER_IB_PORT=4002
PAPER_IB_CLIENT_ID=99
```

- [ ] **Step 3: 验证配置只在 `PAPER_IB_HOST` 非空时启用**

Run: `cd /home/yuanjs/projects/ibkrData && python -c "from collector.config import HAS_PAPER, PAPER_IB_HOST; print(f'HAS_PAPER={HAS_PAPER}, host={PAPER_IB_HOST!r}')"`

Expected: `HAS_PAPER=False, host=''`

- [ ] **Step 4: Commit**

```bash
git add collector/config.py .env.example
git commit -m "feat: add paper gateway config params (PAPER_IB_HOST/PORT/CLIENT_ID)"
```

---

### Task 2: Collector — 双 Client 启动 + Paper account_loop + Gateway Map

**Files:**
- Modify: `collector/main.py`

- [ ] **Step 1: 添加 gateway map 发布函数**

在 `collector/main.py` 的 `_close_id_map` 定义之后，新增：

```python
async def _update_gateway_map(redis, gateway: str, accounts: list[dict]):
    key = "gateway:account_map"
    raw = await redis.get(key)
    mapping: dict[str, list[str]] = json.loads(raw) if raw else {}
    ids = [a["account_id"] for a in accounts]
    mapping[gateway] = ids
    await redis.set(key, json.dumps(mapping))
    await redis.publish("gateway:map:update", json.dumps(mapping))
```

- [ ] **Step 2: 重构 `account_loop`，添加 gateway 标签 + 首次推送 gateway map**

```python
async def account_loop(client, writer, pub, interval, gateway="live", redis=None):
    first_fetch = True
    while True:
        await asyncio.sleep(interval)
        try:
            if not client.is_connected:
                continue
            accounts = await client.get_account_summary()
            positions = client.get_positions()
            await writer.write_account(accounts)
            await writer.write_positions(positions)
            await pub.publish_account({"accounts": accounts, "positions": positions})
            if first_fetch and redis and accounts:
                await _update_gateway_map(redis, gateway, accounts)
                first_fetch = False
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Account loop ({gateway}) error: {e}")
```

- [ ] **Step 3: 添加 Paper Client 初始化函数**

在 `_on_task_done` 后、`main()` 前添加：

```python
async def init_paper(pool, redis_client, writer, pub):
    try:
        from config import PAPER_IB_HOST, PAPER_IB_PORT, PAPER_IB_CLIENT_ID, ACCOUNT_REFRESH_INTERVAL
        paper_client = IBKRClient(PAPER_IB_HOST, PAPER_IB_PORT, PAPER_IB_CLIENT_ID)
        logger.info(f"Paper gateway connecting to {PAPER_IB_HOST}:{PAPER_IB_PORT}...")
        await paper_client.connect_with_retry()

        def on_paper_order(trade):
            t = asyncio.ensure_future(writer.upsert_order(trade))
            t.add_done_callback(_on_task_done)
            payload = {"order_id": trade.order.orderId, "status": trade.orderStatus.status}
            t2 = asyncio.ensure_future(pub.publish_order(payload))
            t2.add_done_callback(_on_task_done)

        def on_paper_exec(trade, fill):
            t = asyncio.ensure_future(writer.write_execution(trade, fill))
            t.add_done_callback(_on_task_done)
            t2 = asyncio.ensure_future(
                pub.publish_order({"type": "execution", "symbol": trade.contract.symbol})
            )
            t2.add_done_callback(_on_task_done)

        paper_client.register_order_handlers(on_paper_order, on_paper_exec)

        asyncio.create_task(
            account_loop(paper_client, writer, pub, ACCOUNT_REFRESH_INTERVAL,
                         gateway="paper", redis=redis_client),
            name="paper_account_loop",
        )
        asyncio.create_task(
            order_command_listener(paper_client, pub, channel="order:command:paper"),
            name="paper_order_listener",
        )
        logger.info("Paper gateway initialized successfully")
    except Exception as e:
        logger.error(f"Paper gateway init failed (will retry): {e}")
        await asyncio.sleep(30)
        asyncio.create_task(init_paper(pool, redis_client, writer, pub))
```

- [ ] **Step 4: 修改 `order_command_listener` 支持参数化 channel**

```python
async def order_command_listener(client, pub, channel="order:command:live"):
    redis = aioredis.from_url(REDIS_URL)
    pubsub = redis.pubsub()
    await pubsub.subscribe(channel)
    logger.info(f"Order command listener started, subscribed to {channel}")
    async for msg in pubsub.listen():
        if msg["type"] != "message":
            continue
        try:
            data = json.loads(msg["data"])
            symbol = data["symbol"]
            close_id = data["close_id"]
            logger.info(f"Close position command received: {symbol} (close_id={close_id})")
            cancelled_ids = client.cancel_orders_for_symbol(symbol)
            order_id, status = client.place_market_order(
                symbol, data["side"], data["quantity"],
                data["sec_type"], data["exchange"], data["currency"],
            )
            _close_id_map[order_id] = close_id
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
            logger.error(f"order_command_listener ({channel}) error: {e}")
```

- [ ] **Step 5: 修改 `main()` 中的 live account_loop 调用**

将 `account_loop(client, writer, pub, ACCOUNT_REFRESH_INTERVAL)` 改为：
```python
account_loop(live_client, writer, pub, ACCOUNT_REFRESH_INTERVAL,
             gateway="live", redis=redis_client),
```

变量名 `client` 保持原样指向 live client 即可。

- [ ] **Step 6: 修改 `main()` 中的 live order_command_listener 调用**

将 `order_command_listener(client, pub)` 改为：
```python
order_command_listener(live_client, pub, channel="order:command:live"),
```

- [ ] **Step 7: 在 `main()` 中 Live 启动完成后添加 Paper 异步初始化**

在 `tasks` 列表定义之后、`shutdown_event.wait()` 之前插入：
```python
    if HAS_PAPER:
        asyncio.create_task(
            init_paper(pool, redis_client, writer, pub),
            name="init_paper",
        )
```

- [ ] **Step 8: Commit**

```bash
git add collector/main.py
git commit -m "feat: add paper gateway dual client, account loop, gateway map"
```

---

### Task 3: API — Gateway Map 端点 + WebSocket 通道

**Files:**
- Create: `api/routers/gateway.py`
- Modify: `api/websocket.py`
- Modify: `api/main.py`

- [ ] **Step 1: 创建 `api/routers/gateway.py`**

```python
import json
from fastapi import APIRouter, Depends
from auth import require_auth
from config import REDIS_URL
import redis.asyncio as aioredis

router = APIRouter(prefix="/api", dependencies=[Depends(require_auth)])

@router.get("/gateway/map")
async def get_gateway_map():
    r = aioredis.from_url(REDIS_URL)
    raw = await r.get("gateway:account_map")
    await r.aclose()
    if raw:
        return json.loads(raw)
    return {}
```

- [ ] **Step 2: 在 `api/websocket.py` 添加 `ws_gateway_map`**

```python
async def ws_gateway_map(ws: WebSocket, token: str = Query(default="")):
    if not _verify_token(token):
        await ws.close(code=4001, reason="Unauthorized")
        return
    await manager.connect("gateway:map:update", ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect("gateway:map:update", ws)
```

- [ ] **Step 3: 在 `api/main.py` 中注册新路由和 WebSocket**

```python
# import
from routers import symbols, history, account, orders, settings, gateway
from websocket import manager, redis_forwarder, ws_market, ws_account, ws_orders, ws_tick, ws_gateway_map

# lifespan forwarders
asyncio.create_task(redis_forwarder("gateway:map:update"), name="fwd_gateway_map")

# router
app.include_router(gateway.router)

# websocket
app.add_api_websocket_route("/ws/gateway/map", ws_gateway_map)
```

- [ ] **Step 4: 验证**

Run: `cd /home/yuanjs/projects/ibkrData && python -c "from api.main import app; print(len(app.routes))"`

- [ ] **Step 5: Commit**

```bash
git add api/routers/gateway.py api/websocket.py api/main.py
git commit -m "feat: add gateway map API endpoint and WebSocket channel"
```

---

### Task 4: API — 账户/持仓 gateway 过滤 + 平仓路由

**Files:**
- Modify: `api/routers/account.py`
- Modify: `api/routers/orders.py`

- [ ] **Step 1: 在 `api/routers/account.py` 添加 `?gateway=` 过滤**

```python
import json
import redis.asyncio as aioredis
from config import REDIS_URL


async def _gateway_account_ids(gateway: str) -> list[str]:
    r = aioredis.from_url(REDIS_URL)
    raw = await r.get("gateway:account_map")
    await r.aclose()
    if raw:
        mapping = json.loads(raw)
        return mapping.get(gateway, [])
    return []


@router.get("/account")
async def get_account(gateway: str | None = None):
    pool = await get_pool()
    query = "SELECT DISTINCT ON (account_id) * FROM account_snapshots"
    args = []
    if gateway:
        ids = await _gateway_account_ids(gateway)
        if ids:
            args.append(ids)
            query += " WHERE account_id = ANY($1)"
    query += " ORDER BY account_id, time DESC"
    rows = await pool.fetch(query, *args)
    return [dict(r) for r in rows]


@router.get("/positions")
async def get_positions(gateway: str | None = None):
    pool = await get_pool()
    query = "SELECT DISTINCT ON (account_id, symbol) * FROM positions"
    args = []
    if gateway:
        ids = await _gateway_account_ids(gateway)
        if ids:
            args.append(ids)
            query += " WHERE account_id = ANY($1)"
    query += " ORDER BY account_id, symbol, time DESC"
    rows = await pool.fetch(query, *args)
    return [dict(r) for r in rows]
```

- [ ] **Step 2: 在 `api/routers/orders.py` 修改平仓端点**

导入和 helper：

```python
import json
import redis.asyncio as aioredis
from config import REDIS_URL


async def _resolve_gateway(account_id: str) -> str:
    r = aioredis.from_url(REDIS_URL)
    raw = await r.get("gateway:account_map")
    await r.aclose()
    if raw:
        mapping = json.loads(raw)
        for gw, ids in mapping.items():
            if account_id in ids:
                return gw
    return "live"
```

在 `close_position` 函数中，将原有的 Redis publish 改为：
```python
    gateway = await _resolve_gateway(row["account_id"])
    channel = f"order:command:{gateway}"

    r = aioredis.from_url(REDIS_URL)
    await r.publish(channel, json.dumps({...}))
    await r.aclose()
```

- [ ] **Step 3: 更新 `tests/test_account.py` 添加 gateway 过滤测试**

```python
@pytest.mark.asyncio
async def test_account_gateway_filter(api_base_url, auth_headers):
    async with httpx.AsyncClient(base_url=api_base_url, headers=auth_headers) as client:
        resp = await client.get("/api/account", params={"gateway": "live"})
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

        resp = await client.get("/api/account", params={"gateway": "paper"})
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

        resp = await client.get("/api/positions", params={"gateway": "live"})
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)
```

- [ ] **Step 4: Run tests**

Run: `cd /home/yuanjs/projects/ibkrData && python -m pytest tests/test_account.py tests/test_orders.py -v`

- [ ] **Step 5: Commit**

```bash
git add api/routers/account.py api/routers/orders.py tests/test_account.py
git commit -m "feat: add gateway filter to account/positions API, route close by gateway"
```

---

### Task 5: Frontend — Store 重构支持双网关

**Files:**
- Modify: `frontend/src/store/accountStore.ts`

- [ ] **Step 1: 重写 `accountStore.ts`**

```typescript
import { create } from 'zustand'

interface GatewayData {
  summary: Record<string, unknown>
  positions: unknown[]
}

interface AccountStore {
  live: GatewayData
  paper: GatewayData
  activeGateway: 'live' | 'paper'
  gatewayMap: Record<string, string>
  hasPaper: boolean
  setAccount: (data: { accounts: Record<string, unknown>[]; positions: Record<string, unknown>[] }) => void
  setActiveGateway: (g: 'live' | 'paper') => void
  setGatewayMap: (map: Record<string, string[]>) => void
}

export const useAccountStore = create<AccountStore>((set) => ({
  live: { summary: {}, positions: [] },
  paper: { summary: {}, positions: [] },
  activeGateway: 'live',
  gatewayMap: {},
  hasPaper: false,

  setAccount: (data) => set(state => {
    const { accounts, positions } = data
    const result: Record<string, unknown> = {}

    const liveAccs = accounts.filter(a => state.gatewayMap[a.account_id as string] === 'live')
    const paperAccs = accounts.filter(a => state.gatewayMap[a.account_id as string] === 'paper')
    const livePos = positions.filter(p => state.gatewayMap[p.account_id as string] === 'live')
    const paperPos = positions.filter(p => state.gatewayMap[p.account_id as string] === 'paper')

    if (liveAccs.length) result.live = { summary: liveAccs[0], positions: state.live.positions }
    if (paperAccs.length) result.paper = { summary: paperAccs[0], positions: state.paper.positions }
    if (livePos.length) result.live = { ...(result.live as GatewayData ?? state.live), positions: livePos }
    if (paperPos.length) result.paper = { ...(result.paper as GatewayData ?? state.paper), positions: paperPos }

    return result
  }),

  setActiveGateway: (g) => set({ activeGateway: g }),

  setGatewayMap: (map) => set({
    gatewayMap: Object.entries(map).reduce((acc, [gw, ids]) => {
      ;(ids as string[]).forEach(id => { acc[id] = gw })
      return acc
    }, {} as Record<string, string>),
    hasPaper: Boolean((map.paper as string[] | undefined)?.length),
  }),
}))
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/store/accountStore.ts
git commit -m "feat: refactor account store for dual gateway support"
```

---

### Task 6: Frontend — WebSocketProvider + Account 页面

**Files:**
- Modify: `frontend/src/components/WebSocketProvider.tsx`
- Modify: `frontend/src/pages/Account.tsx`

- [ ] **Step 1: 在 `WebSocketProvider.tsx` 添加 gateway map WebSocket 订阅**

```typescript
import { useAccountStore } from '../store/accountStore'

export function WebSocketProvider() {
  const setGatewayMap = useAccountStore(s => s.setGatewayMap)

  useWebSocket('/ws/gateway/map', (data: any) => setGatewayMap(data))

  return null
}
```

- [ ] **Step 2: 修改 `Account.tsx` 添加 Live/Paper 标签切换**

核心变更：
1. 导入 `useAccountStore` 中新增的字段
2. 页面顶部添加 gateway 切换标签按钮
3. 根据 `activeGateway` 从对应 store 取值

```tsx
export function Account() {
  const activeGateway = useAccountStore(s => s.activeGateway)
  const setActiveGateway = useAccountStore(s => s.setActiveGateway)
  const hasPaper = useAccountStore(s => s.hasPaper)
  const summary = useAccountStore(s => activeGateway === 'live' ? s.live.summary : s.paper.summary)
  const positions = useAccountStore(s => activeGateway === 'live' ? s.live.positions : s.paper.positions)
  const setGatewayMap = useAccountStore(s => s.setGatewayMap)
  // ... existing closePending, closeMsg, orders state and handlers

  // 加载 gateway map (首次挂载时)
  useEffect(() => {
    api.get('/gateway/map').then(setGatewayMap).catch(() => {})
  }, [setGatewayMap])

  return (
    <div className="p-4 space-y-6">
      {/* Gateway 切换标签 */}
      {hasPaper && (
        <div className="flex gap-2 mb-2">
          <button onClick={() => setActiveGateway('live')}
            className={`px-4 py-1.5 text-sm rounded ${
              activeGateway === 'live'
                ? 'bg-blue-600 text-white'
                : 'text-[var(--text-secondary)] bg-[var(--bg-raised)] hover:text-[var(--text-primary)]'
            }`}>
            实盘
          </button>
          <button onClick={() => setActiveGateway('paper')}
            className={`px-4 py-1.5 text-sm rounded ${
              activeGateway === 'paper'
                ? 'bg-blue-600 text-white'
                : 'text-[var(--text-secondary)] bg-[var(--bg-raised)] hover:text-[var(--text-primary)]'
            }`}>
            模拟
          </button>
        </div>
      )}
      {/* ... rest of existing JSX unchanged */}
    </div>
  )
}
```

- [ ] **Step 3: Commit**

```bash
git add frontend/src/components/WebSocketProvider.tsx frontend/src/pages/Account.tsx
git commit -m "feat: add gateway map WS subscription and account page tab switcher"
```
