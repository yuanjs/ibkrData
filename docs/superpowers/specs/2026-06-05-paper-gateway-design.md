# Paper Trading Gateway 支持设计文档

## 概述

为 ibkrData 系统增加对 IBKR Paper Trading 账户网关的支持，使系统可同时连接实盘和模拟交易网关，在测试时实时观察模拟账户的账户摘要、持仓、订单和成交数据。

## 背景

当前系统仅支持一个 IBKR Gateway 连接（`igzmf:4001`）。Paper Trading 网关部署在不同机器不同端口。用户希望：
- 增加一个 Paper 账户网关连接
- 实时查看 Paper 账户的账户摘要、持仓、订单、成交
- 前端的实盘/模拟数据统一页面，通过标签切换
- Paper 故障不得影响实盘运行

## 架构设计

### 整体架构

```
collector (single process)
  ┌─────────────────────────┐
  │  Live Client             │  ──→ 实盘 Gateway (igzmf:4001)
  │  ├── 行情 tick 订阅        │
  │  ├── account_loop         │
  │  ├── order handlers       │
  │  └── order:command:live   │
  └────────┬─────────────────┘
           │
  ┌────────▼─────────────────┐
  │  Paper Client (可选)       │  ──→ 模拟 Gateway (paper-host:4002)
  │  ├── 无行情 tick 订阅       │
  │  ├── account_loop          │
  │  ├── order handlers        │
  │  └── order:command:paper   │
  └────────┬──────────────────┘
           │
           ├── DataWriter (共享 DB 连接池)
           └── Publisher (共享 Redis)
                    │
                    ▼
              Redis → WebSocket → Frontend
                    │
                    ▼
              API (+ gateway 参数过滤) → DB
```

### 核心原则

1. **Paper 配置可选** — `PAPER_IB_HOST` 为空则不启动 Paper Client
2. **Live 优先** — Live Client 先阻塞启动，Paper 后台异步初始化，不阻塞任何 Live 流程
3. **独立错误域** — Paper 断线/重连只影响自身，Bark 告警也只发 Live 的
4. **无数据冗余** — Paper Client 不订阅市场行情 tick，只处理账户和订单
5. **DB 无变更** — `account_id` 字段天然区分实盘和模拟数据

## 详细设计

### 1. 配置层（`collector/config.py`）

新增 Paper Gateway 连接参数：

```python
# --- 现有 Live 参数 ---
IB_HOST = os.getenv("IB_HOST", "127.0.0.1")
IB_PORT = int(os.getenv("IB_PORT", "4002"))
IB_CLIENT_ID = int(os.getenv("IB_CLIENT_ID", "1"))

# --- 新增 Paper 参数 ---
PAPER_IB_HOST = os.getenv("PAPER_IB_HOST", "")      # 空 = 不启用 Paper
PAPER_IB_PORT = int(os.getenv("PAPER_IB_PORT", "4002"))
PAPER_IB_CLIENT_ID = int(os.getenv("PAPER_IB_CLIENT_ID", "99"))

HAS_PAPER = bool(PAPER_IB_HOST)  # 全局开关
```

### 2. Collector 启动流程（`collector/main.py`）

```
main()
├── 初始化: pool, redis, writer, publisher (共享资源)
├── 创建 live_client = IBKRClient(IB_HOST, IB_PORT, IB_CLIENT_ID)
├── [if HAS_PAPER] 创建 paper_client = IBKRClient(PAPER_IB_HOST, ...)
│
├── 注册 live 的 tick handlers（行情、DailyBarTracker、tick publish）
│
├── ▸ live_client.connect_with_retry()          ← 阻塞等待，原有逻辑
├── ▸ 订阅 live 的行情 symbols
├── ▸ 启动 live 的所有后台循环任务（tick_loop, account_loop 等）
├── ▸ 注册 live 的 order/exec handlers
├── ▸ 启动 live 的 order:command:live 监听器
│
├── [if HAS_PAPER]
│   └── ▸ asyncio.create_task(init_paper())     ← 异步后台，不阻塞
│       ├── paper_client.connect_with_retry()   ← 失败只记日志，不阻塞任何东西
│       ├── 注册 paper 的 order/exec handlers
│       ├── 启动 paper 的 account_loop（共享 DataWriter + Publisher）
│       └── 启动 paper 的 order:command:paper 监听器
│
└── 等待 shutdown 信号
```

**关键保护**：`init_paper()` 是 fire-and-forget task，内部所有异常被捕获，不会传播到主循环。

### 3. Paper Client 的 account_loop

与 Live 的 `account_loop` 相同逻辑，但使用 `paper_client` 实例：

```python
async def paper_account_loop(client, writer, pub, interval):
    """同 account_loop，但限制重试次数，失败只打日志不告警"""
    while True:
        await asyncio.sleep(interval)
        try:
            if not client.is_connected:
                continue  # 未连接则跳过本轮
            accounts = await client.get_account_summary()
            positions = client.get_positions()
            await writer.write_account(accounts)
            await writer.write_positions(positions)
            await pub.publish_account({"accounts": accounts, "positions": positions})
        except Exception as e:
            logger.error(f"Paper account loop error: {e}")
```

### 4. 订单命令路由

**当前**：API → `order:command`（单通道）→ Collector 下单

**变更**：分两个 Redis 通道

| 通道 | 用途 |
|------|------|
| `order:command:live` | 实盘 Gateway 的订单指令 |
| `order:command:paper` | 模拟 Gateway 的订单指令 |

**API 侧 `positions/close` 变更**：

```python
@router.post("/positions/close")
async def close_position(req: ClosePositionRequest):
    row = await pool.fetchrow(
        "SELECT DISTINCT ON (symbol) * FROM positions WHERE symbol=$1 ORDER BY symbol, time DESC",
        req.symbol
    )
    # 根据 account_id 判断目标 gateway
    gateway = _resolve_gateway(row["account_id"])  # → "live" | "paper"
    channel = f"order:command:{gateway}"
    await r.publish(channel, json.dumps({...}))
```

`_resolve_gateway` 函数用 **Collector 自动发现的映射**：

Collector 在每次 account_loop 成功后，记录每个 Gateway 返回的 account_id 列表，写入 Redis key `gateway:account_map`：

```python
# collector — 首次 account_loop 后自动记录
# live loop 后: redis.set("gateway:account_map", json.dumps({
#     "live": ["U123456"],
#     "paper": ["DU987654"],
# }))

# API — 读取映射做路由
async def _resolve_gateway(account_id: str) -> str:
    r = aioredis.from_url(REDIS_URL)
    raw = await r.get("gateway:account_map")
    await r.aclose()
    if raw:
        mapping = json.loads(raw)
        for gw, ids in mapping.items():
            if account_id in ids:
                return gw
    return "live"  # fallback
```

**为什么不用硬编码**：IBKR 的 account_id 用户不一定提前知道，尤其是 Paper 账户的 ID 格式不确定。自动发现更可靠，也省去额外配置项。

**Collector 侧**：

两个独立的 `order_command_listener`，各自绑定对应的 IBKRClient：

```python
if HAS_PAPER:
    asyncio.create_task(
        order_command_listener(paper_client, pub, channel="order:command:paper")
    )
```

### 5. API 层变更

账户相关端点增加 `?gateway=live|paper` 查询参数：

```python
@router.get("/account")
async def get_account(gateway: Optional[str] = None):
    pool = await get_pool()
    query = "SELECT DISTINCT ON (account_id) * FROM account_snapshots"
    args = []
    if gateway:
        account_ids = GATEWAY_ACCOUNTS.get(gateway, [])
        args.append(list(account_ids))
        query += f" WHERE account_id = ANY($1)"
    query += " ORDER BY account_id, time DESC"
    ...
```

`/positions` 同理。

**`/orders` 端点**：orders 表的 `account_id` 也可用于过滤，但通常用户想看到所有订单（实盘+模拟），可保留不加 gateway 过滤。

### 6. Account ID → Gateway 映射的前端广播

Collector 在第一次 account_loop 后，通过 Redis 广播 gateway 映射：

```python
# collector — 在 account_loop 中首次成功获取后
async def _publish_gateway_map(redis, live_accounts, paper_accounts):
    mapping = {}
    if live_accounts:
        mapping["live"] = [a["account_id"] for a in live_accounts]
    if paper_accounts:
        mapping["paper"] = [a["account_id"] for a in paper_accounts]
    await redis.set("gateway:account_map", json.dumps(mapping))
    await redis.publish("gateway:map:update", json.dumps(mapping))
```

WebSocket `gateway:map:update` 通道将映射实时推送给前端。

### 7. 前端 Store 变更（`frontend/src/store/accountStore.ts`）

```typescript
interface GatewayData {
  summary: Record<string, unknown>
  positions: unknown[]
}

interface AccountStore {
  live: GatewayData
  paper: GatewayData
  activeGateway: 'live' | 'paper'
  gatewayMap: Record<string, string>  // account_id → "live"|"paper"
  
  setAccount: (data: { accounts: unknown[]; positions: unknown[] }) => void
  setActiveGateway: (g: 'live' | 'paper') => void
  setGatewayMap: (map: Record<string, string>) => void
}
```

**WebSocket 数据分拣逻辑**：

`account:update` 消息包含所有账户的最新数据。Store 的 `setAccount` 根据 `gatewayMap` 将 `accounts` 和 `positions` 数组按 account_id 分入 `live` 或 `paper`：

```typescript
setAccount: (data) => set(state => {
  const liveAccs = data.accounts.filter(a => state.gatewayMap[a.account_id] === 'live')
  const paperAccs = data.accounts.filter(a => state.gatewayMap[a.account_id] === 'paper')
  const livePos = data.positions.filter(p => state.gatewayMap[p.account_id] === 'live')
  const paperPos = data.positions.filter(p => state.gatewayMap[p.account_id] === 'paper')
  
  return {
    live: { summary: liveAccs[0] ?? {}, positions: livePos },
    paper: { summary: paperAccs[0] ?? {}, positions: paperPos },
  }
})
```

如果 `gatewayMap` 尚未加载，首次 WebSocket 连接时调用 API `/gateway/map` 获取映射。

### 8. 前端 Account 页面变更

### 9. 前端 Account 页面变更

账户页面顶部添加 `实盘 | 模拟` 标签切换：

```tsx
<div className="flex gap-2 mb-4">
  <button onClick={() => setGateway('live')}
    className={activeGateway === 'live' ? 'bg-blue-600 text-white' : '...'}>
    实盘
  </button>
  <button onClick={() => setGateway('paper')}
    className={activeGateway === 'paper' ? 'bg-blue-600 text-white' : '...'}>
    模拟
  </button>
</div>
```

下方所有卡片和表格内容根据 `activeGateway` 从对应 store 取值。

### 10. `.env.example` 变更

```env
# Paper Gateway (optional - leave empty to disable)
PAPER_IB_HOST=
PAPER_IB_PORT=4002
PAPER_IB_CLIENT_ID=99
LIVE_ACCOUNT_ID=
PAPER_ACCOUNT_ID=
```

## 错误处理

| 场景 | 行为 |
|------|------|
| Paper 网关未配置 | 完全不启动 Paper Client，Live 正常运行 |
| Paper 网关连接失败 | 后台重试，只打日志，不影响 Live |
| Paper 连接断开 | 自动重连（同 Live 的重连机制），期间跳过 account_loop |
| Paper 网关持续不可用 | 后台重试直到成功，不阻塞主循环 |
| Live 网关异常 | 同现有行为，Bark 告警，Live 重连 |
| 两个网关都故障 | 各自独立处理 |

## 不变项

- DB 表结构无需变更（`account_id` 已支持多账户）
- `docker-compose.yml` 无需变更（单 collector 进程）
- Live 的行情 tick 流完全不受影响
- 现有 API 端点不变（向后兼容，不传 `gateway` 返回所有数据）
- 订单/成交记录写入逻辑不变

## 实现计划

见 [plans/2026-06-05-paper-gateway-plan.md](../plans/2026-06-05-paper-gateway-plan.md)

## 涉及文件清单

| 文件 | 改动类型 |
|------|----------|
| `collector/config.py` | 修改 — 新增 Paper 参数 |
| `collector/main.py` | 修改 — 双 Client 初始化与任务管理 |
| `api/routers/account.py` | 修改 — 新增 `gateway` 查询参数 |
| `api/routers/orders.py` | 修改 — 平仓路由到正确通道 |
| `api/routers/gateway.py` | 新增 — 提供 gateway 映射 API (`GET /gateway/map`) |
| `api/websocket.py` | 修改 — 新增 `ws_gateway_map` WebSocket 通道 |
| `frontend/src/store/accountStore.ts` | 修改 — 双账户数据存储 + 映射分拣 |
| `frontend/src/pages/Account.tsx` | 修改 — Live/Paper 标签切换 |
| `frontend/src/components/WebSocketProvider.tsx` | 修改 — 订阅 `gateway:map:update` |
| `.env.example` | 修改 — 新增 Paper 配置项 |
