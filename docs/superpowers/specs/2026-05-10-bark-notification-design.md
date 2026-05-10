# IBKR 连接告警系统设计文档 (Bark)

## 1. 目标
当 IBKR 连接多次失败且持续时间超过 2 分钟时，通过 Bark 发送移动端告警通知。并在连接恢复后发送恢复通知。

## 2. 架构设计
系统采用独立通知模块设计，将告警逻辑与 IB 业务逻辑解耦。

### 2.1 核心组件
- **`collector/config.py`**: 负责从 `.env` 读取配置。
- **`collector/notifier.py` (新增)**: 封装 Bark API 调用，使用 `aiohttp` 异步发送消息。
- **`collector/ibkr_client.py`**: 维护连接状态，判断是否达到告警阈值，并触发通知。

## 3. 详细设计

### 3.1 配置项 (.env)
```env
# Bark Notification
BARK_SERVER=https://api.day.app
BARK_KEY=your_bark_key_here
NOTIFY_THRESHOLD_SECONDS=120
```

### 3.2 模块实现逻辑

#### BarkNotifier (`collector/notifier.py`)
- 方法 `send_notification(title, body, group="IBKR")`
- 使用 `aiohttp.ClientSession` 发送异步 POST 请求。
- 请求 URL 格式: `{BARK_SERVER}/{BARK_KEY}/{title}/{body}?group={group}`
- 异常处理: 捕获所有网络异常并记录日志，不影响主流程。

#### IBKRClient 状态管理 (`collector/ibkr_client.py`)
- `_first_fail_time`: `Optional[float]`，记录首次连接失败的时间点。
- `_alert_sent`: `bool`，当前故障周期是否已发送过告警。
- `_notifier`: `BarkNotifier` 实例。

#### 连接逻辑流程
1. **失败处理**:
   - 在 `connect_with_retry` 的 `except` 块中：
     - 如果 `_first_fail_time` 为 `None`，设置为当前时间。
     - 计算持续时间 `elapsed = time.time() - _first_fail_time`。
     - 如果 `elapsed >= NOTIFY_THRESHOLD_SECONDS` 且 `not _alert_sent`：
       - 构建告警信息（包含错误原因、已重试次数、持续时间）。
       - 调用 `notifier.send_notification`。
       - 设置 `_alert_sent = True`。
2. **成功处理**:
   - 在 `connect` 成功后：
     - 如果 `_alert_sent` 为 `True`：
       - 构建恢复信息（包含故障持续总时间）。
       - 调用 `notifier.send_notification`。
     - **重置所有状态**: `_first_fail_time = None`, `_alert_sent = False`, `_retry = 0`。

## 4. 告警示例
- **告警标题**: 🚨 IBKR 连接故障
- **告警正文**: 
  - 错误详情: Connection refused
  - 重试次数: 5 次
  - 持续时间: 124 秒
  - 发生时间: 2026-05-10 10:00:05
- **恢复标题**: ✅ IBKR 连接已恢复
- **恢复正文**: 连接已成功建立，故障持续 150 秒。

## 5. 验收标准
1. 连接失败超过 120 秒后，成功收到一次 Bark 告警。
2. 连接失败期间不会收到重复告警。
3. 连接恢复后，成功收到一次恢复通知。
4. 在 `.env` 未配置 `BARK_KEY` 时，程序应正常运行不崩溃。
