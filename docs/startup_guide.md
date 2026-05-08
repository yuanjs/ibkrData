# IBKR Data System 启动文档

本文档为您提供了该项目的本地开发启动（非 Docker）以及生产环境（Docker）启动的详细说明与注意事项。

## 1. 环境与依赖准备 (Prerequisites)

无论您采用哪种部署方式，系统都强依赖于物理机或可访问的外部关键服务：
- [Interactive Brokers (IBKR)](https://www.interactivebrokers.com/) 的 TWS (Trader Workstation) 或 IB Gateway。
  - 需要在 TWS/Gateway 的 API 设置中开启 **API 允许连接 (Enable ActiveX and Socket Clients)**。
  - 确保已知悉 API 监听的 IP 和 端口 (通常是 `127.0.0.1:4002` (Gateway) 或 `127.0.0.1:7497` (TWS))。

根据运行模式的不同，还需要以下构建依赖：
- **Docker & Docker Compose** (生产环境，或本地快速拉起 DB 等中间件时必须)
- **Python 3.10+** (本地非 Docker 运行 API 和 Collector 节点时必须)
- **Node.js 18+** (本地非 Docker 运行 Frontend 节点时必须)
- **PostgreSQL (带有 TimescaleDB 扩展项)** 与 **Redis 服务**

## 2. 配置文件说明

项目根目录下有一份 `.env.example` 环境变量参照表及 `db/init.sql` 预设初始脚本。无论是本地开发还是生产环境，在启动前，请将 `.env.example` 复制一份并重命名为 `.env`。

```bash
cp .env.example .env
```

您需要按照您的网络环境更新 `.env` 中的核心参数：
- `IB_HOST`：IB Gateway / TWS 监听地址 (如果 IB Client 部署在同一主机则为 127.0.0.1)
- `IB_PORT`：IB Gateway / TWS 监听端口 
- `DB_URL`：数据库连接串 (例如: `postgresql://ibkr:password@localhost:5432/ibkrdata`)
- `REDIS_URL`：Redis 连接串 (例如: `redis://localhost:6379`)
- `API_PORT`：API 服务监听端口（通常为 8000）

> **注意：** 在非 Docker 本地启动时，`DB_URL` 和 `REDIS_URL` 的 `localhost` 能够正确指向系统内的端口。在使用 Docker 并采取宿主网卡直通（`network_mode: host`）的环境下，通常也可以配置为 `localhost` 或 `127.0.0.1` 指向宿主机的独立中间件或容器。

---

## 3. 开发环境启动说明（非 Docker）

针对开发调试场景，通常只需使用容器拉开单独的服务基础架构（Redis + PostgreSQL）。Collector 和 API 采用物理机运行以方便断点及日志打印；前端启用 Vite 的热重载（HMR）特性。

### 3.1 启动基础设施层 (PG + Redis)

推荐使用 Docker 启动干净的数据中枢引擎环境，以免污染物理机：

```bash
# 后台启动带 timescaledb 扩展的 PostgeSQL 数据库 (docker-compose 内已经涵盖了该服务)
docker compose up timescaledb -d

# 注：当前项目 docker-compose 尚未直接声明 redis 容器节点，如果无可用 Redis 服务，请单独拉起
docker run -d -p 6379:6379 --name redis redis:latest
```

### 3.2 启动 Collector (数据采集节点)

进入 `collector` 目录进行环境挂载：

```bash
cd collector
# (可选操作)建议在项目中创建 venv 层
python3 -m venv venv
source venv/bin/activate
# 安装相关依赖包
pip install -r requirements.txt
# 运行主程序连入 IB 网关开始执行订阅任务
python main.py
```

### 3.3 启动 Backend API (FastAPI 服务节点)

新开一个终端窗口：

```bash
cd api
# 同理可使用 venv
pip install -r requirements.txt
# 使用 Uvicorn 与 Hot Reload 热刷新模式运行服务端, API 默认监听8000
uvicorn main:app --reload --port 8000
```
启动后可以通过浏览器访问 `http://127.0.0.1:8000/docs` 体验 Swagger UI 测试。

### 3.4 启动 Frontend (前端构建)

新开一个终端窗口：

```bash
cd frontend
# 安装所需依赖组件 (NPM 或 Yarn 皆可)
npm install
# 启动带有 HMR 特性的开发环境
npm run dev
```

启动后会弹出如 `http://localhost:5173` 等访问地址，点击即可预览前端界面的实时开发效果及图表绘制情况。

---

## 4. 生产环境启动说明（Docker Compose）

如果您准备进行测试服部署或生产化应用部署，可以直接调用项目根目录下的 `docker-compose.yml` 实现多容器编排发布流程。前端会通过 Nginx 反向代理后绑定端口容器运行。

### 4.1. 编写运行配置文件

请确保根目录的 `.env` 配置完善。如果通过 docker compose 运行了项目：
- 默认 docker-compose 为 `api` 与 `collector` 两个子服务指定了 `network_mode: host` 网络模式。
- 这意味着这两个服务与宿主机共享网络空间，能够借此**直接连接并通讯**宿主机自身跑的 IB Gateway 软件 (`127.0.0.1:4002`)，从而避免在 docker 内部找寻网关主机的 IP 发生网络隔离断档问题。网络连接串直接保持跟物理机直联方式一致即可（如 `localhost`）。

### 4.2. 一键构建并启动服务簇

处于本项目根目录下运行指令即可开始构建后端与前端镜像，并拉起服务组合：

```bash
docker compose up --build -d
```

启动相关验证命令:
```bash
# 验证运行期容器的生命周期
docker compose ps
# 查看持续滚动的全局日志
docker compose logs -f
```

### 4.3. 访问与使用

此时所有的核心组件应该全部就绪：
- **Web 前端站点**: `http://<服务器IP>:3000` （源自 docker-compose 显式暴露 `3000:80`，供公网或内网映射使用）。
- **REST 及 WebSocket 服务端**: `http://<服务器IP>:8000` 或相应的内网访问层（因 host 网络模式运行）。

### 4.4. 停止与数据清理

```bash
# 常规停止与关闭项目应用簇
docker compose down

# 如果需要将所有的底层数据卷 (如 PG 历史沉淀表) 都彻底销毁抹除
docker compose down -v
```

---

## 5. 项目架构逻辑与通信补充说明

- **基础职责划分**:
  - `API (FastAPI)`: 面向前端提供 REST 增删改查路由支持与 WebSocket 实时管道；扮演下游的中间件路由。
  - `Collector (Python/ib_insync)`: 负责系统最核心的接入器和轮询守护职能，长连接到 IB 交易系统后持续地灌注实时行情的 Tick 与全生命周期的订单轨迹更新。
  - `Frontend (Vite+React)`: 负责利用 Lightweight Charts 消费 API 取证流以及 Websockets 获取的数据以进行人机交互与面板可视化展现。
- **并发机制**: `Redis` 用于支持 Python 内的高低频流媒体通信 Pub/Sub；时序 PostgreSQL(Timescaledb) 起到了持久化底盘的职能。
