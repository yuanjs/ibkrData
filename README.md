# IBKR 数据采集与可视化平台

基于 Interactive Brokers（盈透证券）的实时行情数据采集、存储和可视化系统。

## 系统架构

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  IB Gateway  │────▶│   Collector  │────▶│  TimescaleDB │
│  (TWS/IBGW)  │     │  (Python)    │     │  (PostgreSQL)│
└──────────────┘     └──────┬───────┘     └──────┬───────┘
                            │                    │
                            ▼                    ▼
                     ┌──────────────┐     ┌──────────────┐
                     │    Redis     │     │   FastAPI    │
                     │  (Pub/Sub)   │     │  (REST/WS)   │
                     └──────┬───────┘     └──────┬───────┘
                            │                    │
                            └──────┬─────────────┘
                                   ▼
                          ┌─────────────────┐
                          │   Frontend      │
                          │  React + Vite   │
                          │  + Nginx        │
                          └─────────────────┘
```

### 组件说明

| 组件 | 技术栈 | 职责 |
|------|--------|------|
| **Collector** | Python 3.11, ib_insync | 连接 IB Gateway，采集实时 tick 和日线数据，写入数据库和 Redis |
| **API** | FastAPI, asyncpg | 提供 REST API 和 WebSocket 代理，支持历史数据查询和实时数据推送 |
| **TimescaleDB** | TimescaleDB 16 (PG16) | 存储 ticks 超表、daily_bars、账户快照、订单等时序数据 |
| **Redis** | Redis 7 | 实时数据中转，Collector → Redis Pub/Sub → WebSocket → 前端 |
| **Frontend** | React 19, Vite, lightweight-charts | 可视化：K 线图、报价表、账户、订单管理 |
| **IB Gateway** | TWS/IB Gateway | 盈透证券行情网关，提供实时和历史的市场数据 |

## 部署前准备

### 环境要求

- Docker 24+ 和 Docker Compose v2+
- 运行中的 IB Gateway 或 TWS（需配置 API 端口）
- （可选）Node.js 20+ 用于前端开发调试

### 端口分配

| 端口 | 服务 | 说明 |
|------|------|------|
| 4001/4002 | IB Gateway | API 端口（客户端配置） |
| 5432 | TimescaleDB | 数据库 |
| 6379 | Redis | 缓存/消息 |
| 8002 | API | REST + WebSocket |
| 3000 | Frontend | Nginx 代理前端页面（生产） |
| 5173 | Vite Dev Server | 前端开发服务器 |
| 8001 | Collector 健康检查 | 内部使用 |

### IB Gateway 配置

1. 下载并启动 [IB Gateway](https://www.interactivebrokers.com/en/trading/ibgateway.php) 或 TWS
2. 登录后进入 **Config → API → Settings**
3. 确认启用 "Enable ActiveX and Socket Clients"
4. 设置 Socket 端口（如 `4001`）
5. 将 IB 客户端 IP 白名单中的 `127.0.0.1` 勾选上
6. 记录下端口号，填入 `.env` 中的 `IB_PORT`

## 快速部署（Docker Compose）

### 1. 克隆项目

```bash
git clone <项目地址>
cd ibkrData
```

### 2. 配置环境变量

```bash
# 复制示例配置文件
cp .env.example .env

# 编辑 .env 根据实际情况修改
```

必要参数说明：

```ini
# IB Gateway 连接配置
IB_HOST=127.0.0.1          # IB Gateway 主机地址
IB_PORT=4001               # IB Gateway API 端口（与 TWS/IBGW 配置一致）
IB_CLIENT_ID=10            # 客户端 ID（唯一，不可与其他连接重复）

# 数据库（Docker 部署时通常无需修改）
POSTGRES_PASSWORD=password  # 数据库密码，建议修改

# 认证
JWT_TOKEN=your-token        # 前端和 API 之间的认证令牌，自行设定
JWT_SECRET=change-me        # JWT 签名密钥，生产环境请修改

# API
API_PORT=8002               # API 监听端口
```

### 3. 配置前端认证令牌

```bash
# 创建前端环境变量文件
echo "VITE_TOKEN=your-token" > frontend/.env.local
```

`VITE_TOKEN` 必须与 `.env` 中的 `JWT_TOKEN` 一致。

### 4. 启动所有服务

```bash
docker compose up -d

# 查看启动日志
docker compose logs -f
```

首次启动会自动：
- 创建 TimescaleDB 数据库和 hypertable
- 插入默认配置和订阅数据
- 启动 API 和前端服务
- Collector 会自动连接 IB Gateway 并开始采集数据

### 5. 验证服务

```bash
# 检查所有服务状态
docker compose ps

# 预期输出包含以下 4 个服务，状态均为 "Up"
# - ibkrdata-collector-1
# - ibkrdata-api-1
# - ibkrdata-timescaledb-1
# - ibkrdata-frontend-1
```

### 6. 访问页面

打开浏览器访问 `http://localhost:3000`

## 开发环境部署

### 后端 API

```bash
# 启动数据库和 Redis
docker compose up -d timescaledb redis

# 安装 Python 依赖
cd api
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 启动 API 开发服务器（热重载）
uvicorn main:app --reload --host 0.0.0.0 --port 8002
```

### 数据采集器

```bash
cd collector
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 运行采集器
python main.py
```

### 前端

```bash
cd frontend
npm install

# 启动开发服务器（默认端口 5173）
npm run dev
```

开发模式下前端代理配置在 `vite.config.ts` 中，确保 API 请求能转发到正确的端口。

## 数据库

### 表结构

数据库初始化脚本位于 `db/init.sql`，包含以下主要表：

| 表名 | 类型 | 用途 |
|------|------|------|
| `ticks` | TimescaleDB hypertable | 逐笔成交和 1 秒 OHLC 聚合数据 |
| `daily_bars` | 普通表 | IBKR 日线数据（upsert by symbol + date_str） |
| `subscriptions` | 普通表 | 监控标的配置（symbol, sec_type, exchange, currency） |
| `account_snapshots` | TimescaleDB hypertable | 账户净值、现金、保证金快照 |
| `positions` | TimescaleDB hypertable | 持仓快照 |
| `orders` | 普通表 | 订单记录（upsert by order_id） |
| `executions` | TimescaleDB hypertable | 成交记录 |
| `settings` | 普通表 | 系统配置键值对 |
| `alerts` | 普通表 | 价格告警配置 |

TimescaleDB 超表自动按时间分区，并内置 30 天数据保留策略。

### 管理数据库

```bash
# 直接连接数据库
docker compose exec timescaledb psql -U ibkr -d ibkrdata

# 查看最新的日线数据
SELECT * FROM daily_bars ORDER BY time DESC LIMIT 10;

# 查看活跃订阅
SELECT * FROM subscriptions WHERE active = true;

# 查看系统设置
SELECT * FROM settings;
```

## 配置订阅标的

系统启动时会自动从 `subscriptions` 表中读取需要监控的交易品种。可以通过数据库直接添加：

```sql
INSERT INTO subscriptions (symbol, sec_type, exchange, currency)
VALUES ('AAPL', 'STK', 'SMART', 'USD')
ON CONFLICT (symbol) DO NOTHING;
```

目前预置的订阅：

| Symbol | 品种 | 交易所 | 货币 |
|--------|------|--------|------|
| SPI | 澳大利亚 SPI200 期货 | SNFE | AUD |
| USD.JPY | 美元/日元 | IDEALPRO | USD |
| MYM | 微型道琼斯期货 | CBOT | USD |
| N225M | 微型日经 225 期货 | OSE.JPN | JPY |

### 品种类型说明

- `STK` — 股票
- `FUT` — 期货
- `CASH` — 外汇（Forex）
- `IND` — 指数（IBKR 部分数据源不支持实时）

## 时区配置

前端通过 `frontend/src/config/productConfig.ts` 配置各品种的显示时区：

```typescript
export const PRODUCT_CONFIGS: Record<string, { timezone: string }> = {
  'ASX200':    { timezone: 'Australia/Sydney' },
  'WALLSTREET': { timezone: 'America/Chicago' },
  'NIKKEI_MINI': { timezone: 'Asia/Tokyo' },
  'USDJPY':    { timezone: 'America/New_York' },
};
```

添加新品种时需要同时更新此配置中的时区映射和 `getProductConfig()` 函数中的归一化逻辑。

## 常用操作

### 查看日志

```bash
# 查看所有服务日志
docker compose logs -f

# 查看特定服务日志
docker compose logs -f collector
docker compose logs -f api
docker compose logs -f frontend
```

### 重启服务

```bash
# 重启单个服务
docker compose restart collector
docker compose restart api
docker compose restart frontend

# 重新构建并启动（代码更新后）
docker compose up -d --build collector
docker compose up -d --build api
docker compose up -d --build frontend
```

### 完全重置

```bash
# 停止并删除所有容器
docker compose down

# 删除数据卷（⚠️ 会清除所有数据）
docker compose down -v

# 重新部署
docker compose up -d
```

## 故障排查

### Collector 无法连接 IB Gateway

```
Error 10197: Market data disconnected (likely competing login)
```

**原因：** IB Gateway 只允许一个 API 连接，检测到重复登录。

**解决：**
1. 关闭其他连接到 IB Gateway 的程序
2. 在 `.env` 中修改 `IB_CLIENT_ID` 为一个不同的值
3. 重启 collector：`docker compose restart collector`

### Collector 持续断开重连

```
Connection failed: .... Retrying in Xs
```

检查：
- IB Gateway 是否已登录
- `.env` 中的 `IB_HOST`、`IB_PORT` 是否与 IB Gateway 配置一致
- 是否有防火墙阻止连接

### API 返回 "Invalid token"

```
{"detail": "Invalid token"}
```

**原因：** API 的 `JWT_TOKEN` 配置与前端请求使用的 token 不一致。

**解决：** 确保 `.env` 中的 `JWT_TOKEN` 与 `frontend/.env.local` 中的 `VITE_TOKEN` 值相同。

### TimescaleDB 容器健康检查失败

```
Container timescaledb unhealthy
```

**解决：** 查看日志 `docker compose logs timescaledb`，检查磁盘空间和端口冲突。
首次启动时数据库初始化需要一些时间，等待几十秒后会自动恢复。

### 前端页面空白或无法加载

检查：
1. `docker compose ps` 确认所有服务都在运行
2. 浏览器开发者工具 → 网络面板 → 查看 API 请求是否有错误
3. 前端 Nginx 将 `/api` 和 `/ws` 请求代理到 API 服务，确保 `API_PORT` 配置一致
4. 如果前端容器无法解析 API 主机名，需检查网络配置

## 许可证

本项目仅供个人学习和研究使用。使用前请确保遵守 Interactive Brokers 的服务条款和相关法规。
