# IBKR Bark 告警系统实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 当 IBKR 连接失败超过 2 分钟时通过 Bark 发送告警，并在恢复时通知。

**Architecture:** 
- 在 `collector/config.py` 中添加配置项。
- 新建 `collector/notifier.py` 封装 Bark 异步请求。
- 修改 `collector/ibkr_client.py` 增加状态追踪和通知逻辑。

**Tech Stack:** Python, aiohttp, ib_insync

---

### Task 1: 配置项更新

**Files:**
- Modify: `collector/config.py`
- Modify: `.env.example`

- [ ] **Step 1: 修改 `collector/config.py` 添加 Bark 相关配置**

```python
BARK_SERVER = os.getenv("BARK_SERVER", "https://api.day.app")
BARK_KEY = os.getenv("BARK_KEY", "")
NOTIFY_THRESHOLD_SECONDS = int(os.getenv("NOTIFY_THRESHOLD_SECONDS", "120"))
```

- [ ] **Step 2: 修改 `.env.example` 添加配置模版**

```env
# Bark Notification
BARK_SERVER=https://api.day.app
BARK_KEY=
NOTIFY_THRESHOLD_SECONDS=120
```

- [ ] **Step 3: 提交更改**

```bash
git add collector/config.py .env.example
git commit -m "chore: add bark notification configurations"
```

---

### Task 2: 实现 BarkNotifier 模块

**Files:**
- Create: `collector/notifier.py`
- Test: `tests/test_notifier.py`

- [ ] **Step 1: 编写测试用例验证 `BarkNotifier`**

```python
import pytest
from collector.notifier import BarkNotifier
from unittest.mock import AsyncMock, patch

@pytest.mark.asyncio
async def test_send_notification_success():
    notifier = BarkNotifier(server="https://api.day.app", key="test_key")
    with patch("aiohttp.ClientSession.post") as mock_post:
        mock_post.return_value.__aenter__.return_value.status = 200
        success = await notifier.send_notification("Title", "Body")
        assert success is True
```

- [ ] **Step 2: 实现 `BarkNotifier` 类**

```python
import aiohttp
import logging

logger = logging.getLogger(__name__)

class BarkNotifier:
    def __init__(self, server: str, key: str):
        self.server = server.rstrip("/")
        self.key = key

    async def send_notification(self, title: str, body: str, group: str = "IBKR"):
        if not self.key:
            return False
        
        url = f"{self.server}/{self.key}/{title}/{body}"
        params = {"group": group}
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, params=params, timeout=10) as response:
                    if response.status == 200:
                        return True
                    else:
                        logger.error(f"Bark notification failed with status {response.status}")
                        return False
        except Exception as e:
            logger.error(f"Error sending Bark notification: {e}")
            return False
```

- [ ] **Step 3: 运行测试并提交**

```bash
pytest tests/test_notifier.py
git add collector/notifier.py tests/test_notifier.py
git commit -m "feat: implement BarkNotifier module"
```

---

### Task 3: 集成到 IBKRClient

**Files:**
- Modify: `collector/ibkr_client.py`

- [ ] **Step 1: 初始化状态变量和 Notifier**

在 `IBKRClient.__init__` 中增加：
```python
from notifier import BarkNotifier
from config import BARK_SERVER, BARK_KEY, NOTIFY_THRESHOLD_SECONDS

# ...
self._notifier = BarkNotifier(BARK_SERVER, BARK_KEY)
self._first_fail_time = None
self._alert_sent = False
```

- [ ] **Step 2: 修改 `connect` 方法重置状态**

```python
    async def connect(self):
        await self.ib.connectAsync(self.host, self.port, clientId=self.client_id)
        
        # 发送恢复通知
        if self._alert_sent:
            duration = int(asyncio.get_event_loop().time() - self._first_fail_time)
            asyncio.create_task(self._notifier.send_notification(
                "✅ IBKR 连接已恢复", 
                f"连接已成功建立，故障持续 {duration} 秒。"
            ))

        self._retry = 0
        self._first_fail_time = None
        self._alert_sent = False
        logger.info("Connected to IB Gateway")
```

- [ ] **Step 3: 修改 `connect_with_retry` 增加告警逻辑**

```python
    async def connect_with_retry(self):
        while True:
            try:
                await self.connect()
                return
            except Exception as e:
                loop = asyncio.get_event_loop()
                now = loop.time()
                
                if self._first_fail_time is None:
                    self._first_fail_time = now
                
                elapsed = now - self._first_fail_time
                if elapsed >= NOTIFY_THRESHOLD_SECONDS and not self._alert_sent:
                    asyncio.create_task(self._notifier.send_notification(
                        "🚨 IBKR 连接故障",
                        f"错误详情: {e}\n重试次数: {self._retry}\n持续时间: {int(elapsed)} 秒"
                    ))
                    self._alert_sent = True

                wait = min(2**self._retry, 60)
                logger.warning(f"Connection failed: {e}. Retrying in {wait}s")
                self._retry += 1
                await asyncio.sleep(wait)
```

- [ ] **Step 4: 提交代码**

```bash
git add collector/ibkr_client.py
git commit -m "feat: integrate Bark notifications into IBKRClient"
```

---

### Task 4: 验证与清理

- [ ] **Step 1: 手动验证 (模拟连接失败)**
临时修改 `IB_PORT` 为错误端口，运行 `collector/main.py` 观察是否在 120 秒后收到 Bark 告警。

- [ ] **Step 2: 恢复配置并提交**
确保测试代码已清理。

---
