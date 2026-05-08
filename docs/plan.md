# IBKR 实时数据系统 开发计划

## 阶段一：基础设施搭建

### 任务 1.1 项目初始化
- [ ] 创建项目目录结构
- [ ] 初始化 git 仓库
- [ ] 创建 `.env.example` 和 `.gitignore`

### 任务 1.2 数据库初始化
- [ ] 编写 `db/init.sql`（ticks、account_snapshots、positions、orders、executions、subscriptions、settings、alerts 建表）
- [ ] 配置 TimescaleDB Docker 服务
- [ ] 验证建表成功

### 任务 1.3 Redis 配置
- [ ] 配置 Redis Docker 服务
- [ ] 验证 pub/sub 连通性

---

## 阶段二：数据采集服务

### 任务 2.1 IBKR 客户端封装（`collector/ibkr_client.py`）
- [ ] 封装 `ib_insync` 连接/断开逻辑
- [ ] 实现指数退避自动重连
- [ ] 实现行情 `subscribe(symbol)` / `unsubscribe(symbol)`
- [ ] 实现账户数据轮询（reqAccountSummary / reqPositions）
- [ ] 实现交易事件监听（openOrderEvent / orderStatusEvent / execDetailsEvent）

### 任务 2.2 数据写入（`collector/data_writer.py`）
- [ ] 批量 INSERT 到 `ticks` 表（每秒）
- [ ] INSERT 到 `account_snapshots` / `positions` 表（每30s）
- [ ] INSERT/UPDATE `orders` 表（事件驱动）
- [ ] INSERT `executions` 表（事件驱动）

### 任务 2.3 Redis 发布（`collector/publisher.py`）
- [ ] PUBLISH `market:{symbol}`（每秒）
- [ ] PUBLISH `account:update`（账户/持仓变化时）
- [ ] PUBLISH `order:update`（订单/成交变化时）

### 任务 2.4 采集服务主程序（`collector/main.py`）
- [ ] 启动时从 `subscriptions` 表加载活跃标的
- [ ] 从 `settings` 表读取配置（IB连接、刷新间隔等）
- [ ] 订阅 Redis `settings:update` 实现热加载
- [ ] 整合 ibkr_client + data_writer + publisher
- [ ] 健康检查端点（HTTP /health）

---

## 阶段三：API 服务

### 任务 3.1 数据库连接层（`api/db.py`）
- [ ] asyncpg 连接池配置
- [ ] 封装常用查询函数

### 任务 3.2 认证（`api/auth.py`）
- [ ] JWT 验证中间件
- [ ] 静态 token 模式（从环境变量读取）

### 任务 3.3 REST 路由
- [ ] `GET/POST/DELETE /api/symbols`
- [ ] `GET /api/history/{symbol}`（支持 interval 参数）
- [ ] `GET /api/history/{symbol}/export`
- [ ] `GET /api/status`
- [ ] `GET /api/account` / `GET /api/account/history`
- [ ] `GET /api/positions`
- [ ] `GET /api/orders` / `GET /api/trades` / `GET /api/trades/export` / `GET /api/pnl`
- [ ] `GET/PUT /api/settings`（读写设置，变更后 PUBLISH settings:update）
- [ ] `GET/POST/DELETE /api/alerts`（告警规则管理）

### 任务 3.4 WebSocket 推送（`api/websocket.py`）
- [ ] ConnectionManager（管理多客户端连接）
- [ ] `WS /ws/market`：订阅 Redis `market:{symbol}`，转发行情
- [ ] `WS /ws/account`：订阅 Redis `account:update`，推送账户/持仓变化
- [ ] `WS /ws/orders`：订阅 Redis `order:update`，推送订单/成交变化

---

## 阶段四：前端

### 任务 4.1 项目初始化
- [ ] `create vite@latest frontend --template react-ts`
- [ ] 安装依赖：`lightweight-charts zustand`
- [ ] 配置 Tailwind CSS（深色主题）
- [ ] 配置 API 代理（开发环境）

### 任务 4.2 基础设施
- [ ] `hooks/useWebSocket.ts`：WebSocket 连接管理，自动重连
- [ ] `api/client.ts`：REST 请求封装
- [ ] `store/marketStore.ts`：行情状态
- [ ] `store/accountStore.ts`：账户/持仓状态
- [ ] `store/orderStore.ts`：订单/成交状态
- [ ] `components/StatusBar.tsx`：连接状态栏

### 任务 4.3 实时监控页（`pages/Monitor.tsx`）
- [ ] `components/QuoteTable.tsx`：行情列表，价格变动高亮
- [ ] `components/CandleChart.tsx`：K线图，支持 1s/5s/1min 切换
- [ ] 整合：点击行情行 → 更新图表

### 任务 4.4 账户总览页（`pages/Account.tsx`）
- [ ] `components/AccountSummary.tsx`：净值/现金/保证金/盈亏卡片
- [ ] 净值历史折线图（Lightweight Charts）
- [ ] 通过 `WS /ws/account` 实时更新

### 任务 4.5 持仓管理页（`pages/Positions.tsx`）
- [ ] `components/PositionTable.tsx`：持仓表格，含未实现盈亏
- [ ] 点击标的跳转历史行情

### 任务 4.6 订单与成交页（`pages/Orders.tsx`）
- [ ] `components/OrderTable.tsx`：实时订单列表（WS更新）
- [ ] 成交记录表格（分页）
- [ ] 历史订单查询（时间/状态筛选）
- [ ] 盈亏报告 + CSV 导出

### 任务 4.7 历史数据查询页（`pages/History.tsx`）
- [ ] `components/TimeRangePicker.tsx`：时间范围选择器
- [ ] 历史K线图展示
- [ ] 数据表格（分页）+ CSV 导出

### 任务 4.8 系统设置页（`pages/Settings.tsx`）
- [ ] IBKR连接配置表单（host/port/client_id + 连接测试）
- [ ] 数据采集配置（订阅标的管理、保留天数、账户刷新间隔）
- [ ] 告警规则管理（价格告警/盈亏告警 增删）
- [ ] 前端显示配置（默认周期、语言、时区）
- [ ] 所有配置通过 `PUT /api/settings` 保存

---

## 阶段五：集成与部署

### 任务 5.1 Docker Compose
- [ ] 编写完整 `docker-compose.yml`
- [ ] 编写各服务 `Dockerfile`
- [ ] 验证全链路联调

### 任务 5.2 端到端测试
- [ ] 验证：IBKR数据 → 数据库写入 → WebSocket推送 → 前端展示
- [ ] 验证：历史查询各周期正确
- [ ] 验证：断线重连场景

---

## 依赖关系

```
阶段一 → 阶段二 → 阶段三 → 阶段四 → 阶段五
         （2.1-2.4 可并行）  （3.1-3.4 可并行）  （4.1后 4.2-4.5 可并行）
```

## 关键技术决策记录

| 决策 | 选择 | 原因 |
|------|------|------|
| 时序数据库 | TimescaleDB | 兼容 SQL，聚合视图方便，无需学新查询语言 |
| IBKR接入库 | ib_insync | 异步支持好，比官方 TWS API 更简洁 |
| 图表库 | Lightweight Charts | 专业金融图表，性能好，免费开源 |
| 实时推送 | WebSocket + Redis Pub/Sub | 解耦采集和推送，支持多客户端 |
| 认证方式 | 静态 JWT token | 单用户系统，简单够用 |
