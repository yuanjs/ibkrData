"""锁住"重连触发 minute 回填"的意图。

背景见 docs 及计划：collector 断线重连（gateway 半开重连 / 10197 竞争登录挤断）
恢复后，除了重订阅行情，还应回填断线窗口缺失的 minute（复用 startup 的 gap 检测）。

这些测试锁住的核心不变量：
  - 首连不回填（回填由 main() 启动序列显式跑），只有真·重连才触发；
  - 一个回调抛异常不拖垮其余回调；
  - 先完成重订阅、再触发回填；
  - worker 用 Event 合并突发触发：在途期间到达的触发会补跑一次、且绝不重叠；
  - 未连接时不发历史请求；
  - _resubscribe_all 加锁后两次并发调用不交叠（防半开风暴双重订阅/句柄泄漏）；
  - 10197 数据恢复路径同样触发回填。
"""

import asyncio
import os
import sys
import types

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.modules.pop("config", None)

import pytest  # noqa: E402

if "aiohttp" not in sys.modules:  # 与其他 collector 测试一致的 import stub
    aiohttp = types.ModuleType("aiohttp")
    aiohttp.web = types.SimpleNamespace(
        Response=object,
        Application=object,
        AppRunner=object,
        TCPSite=object,
    )
    sys.modules["aiohttp"] = aiohttp
    sys.modules["aiohttp.web"] = aiohttp.web

import main  # noqa: E402
from main import IBKRClient  # noqa: E402


def _make_client():
    """绕过 __init__ 构造 IBKRClient，补齐被测方法用到的字段，避免真连 IB Gateway。"""
    client = IBKRClient.__new__(IBKRClient)
    client._reconnect_callbacks = []
    client._has_connected_once = False
    client._resubscribe_lock = asyncio.Lock()
    client._data_suspended = False
    client._subscriptions = {}
    client._tickers = {}
    client._ticker_roles = {}
    client._symbol_map = {}
    client._last_trade_prices = {}
    return client


# --------------------------------------------------------------------------
# 历史请求超时：防止卡死的历史请求永久持有 _historical_lock
# --------------------------------------------------------------------------

def test_ib_has_finite_request_timeout():
    """collector 的 IB 必须设有限的 RequestTimeout。

    历史请求经 _historical_lock 串行化；若无请求超时，一个卡死的历史请求会
    永久持锁、拖死所有 daily 刷新循环。此测试锁住该修复：删掉 RequestTimeout
    设置（回到默认 0=永久等待）即失败。
    """
    client = IBKRClient("h", 1, 2)
    assert client.ib.RequestTimeout, "IB.RequestTimeout 必须为非零有限值，否则卡死请求会永久持锁"


# --------------------------------------------------------------------------
# _handle_reconnect：首连 vs 重连
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_first_connect_skips_backfill_callbacks():
    """首连不触发回填回调，且事后标记 _has_connected_once。"""
    client = _make_client()
    called = []
    client.register_reconnect_handler(lambda: called.append(1))

    async def _noop_resubscribe():
        return None

    client._resubscribe_all = _noop_resubscribe

    await client._handle_reconnect()

    assert called == [], "首连不应触发回填回调（回填由 main() 启动序列显式执行）"
    assert client._has_connected_once is True


@pytest.mark.asyncio
async def test_second_connect_fires_callbacks_once():
    """真·重连（第二次及以后）触发回填回调恰一次。"""
    client = _make_client()
    called = []
    client.register_reconnect_handler(lambda: called.append(1))

    async def _noop_resubscribe():
        return None

    client._resubscribe_all = _noop_resubscribe

    await client._handle_reconnect()  # 首连：skip
    await client._handle_reconnect()  # 重连：fire

    assert called == [1], "重连应恰好触发一次回填回调"


def test_callback_exception_isolated():
    """一个回调抛异常不外泄、不拖垮其余回调。"""
    client = _make_client()
    order = []

    def _boom():
        order.append("boom")
        raise RuntimeError("callback failed")

    client.register_reconnect_handler(_boom)
    client.register_reconnect_handler(lambda: order.append("ok"))

    client._fire_reconnect_callbacks()  # 不应抛出

    assert order == ["boom", "ok"], "前一个回调崩溃后，后一个回调仍应被调用"


@pytest.mark.asyncio
async def test_resubscribe_awaited_before_callbacks():
    """先完成重订阅、再触发回填回调（避免边补边丢实时）。"""
    client = _make_client()
    client._has_connected_once = True
    order = []

    async def _resubscribe():
        order.append("resubscribe")

    client._resubscribe_all = _resubscribe
    client.register_reconnect_handler(lambda: order.append("callback"))

    await client._handle_reconnect()

    assert order == ["resubscribe", "callback"]


# --------------------------------------------------------------------------
# _resubscribe_all 加锁：并发不交叠
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_resubscribe_reentrancy_serialized_by_lock():
    """两次并发 _resubscribe_all 不交叠执行——锁防半开风暴双重订阅/句柄泄漏。

    若移除 self._resubscribe_lock，两个协程会并发进入 subscribe 循环，running.max 达到 2，
    本测试即失败——这正是并发重入 bug 的信号。
    """
    client = _make_client()
    client._subscriptions = {
        "A": {"symbol": "A", "sec_type": "STK", "exchange": "SMART", "currency": "USD"},
        "B": {"symbol": "B", "sec_type": "STK", "exchange": "SMART", "currency": "USD"},
    }
    running = {"n": 0, "max": 0}

    async def _fake_subscribe(**kwargs):
        running["n"] += 1
        running["max"] = max(running["max"], running["n"])
        await asyncio.sleep(0.01)
        running["n"] -= 1

    client.subscribe = _fake_subscribe

    await asyncio.gather(client._resubscribe_all(), client._resubscribe_all())

    assert running["max"] == 1, "加锁后两次重订阅不应交叠执行"


# --------------------------------------------------------------------------
# 10197 数据恢复路径
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_10197_recovery_fires_callbacks():
    """10197 挂起结束、_data_suspended 清零后，退出重连循环并触发回填回调。"""
    client = _make_client()
    client._data_suspended = False  # 已恢复：while 循环不进入
    called = []
    client.register_reconnect_handler(lambda: called.append(1))

    await client._auto_reconnect_market_data()

    assert called == [1], "10197 数据恢复应触发一次 gap 回填"


# --------------------------------------------------------------------------
# reconnect_backfill_worker：Event 合并 + 在途补跑 + 未连接跳过
# --------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_worker_coalesces_and_reruns_pending(monkeypatch):
    """在途回填期间到达的触发不丢失：跑完后补跑一次，且两次不重叠。

    同时锁住 finding #1 的修复：回填必须在 client._historical_lock 之内执行，
    以便与周期性 daily 刷新循环互斥、避免并发触发 IBKR pacing。
    """
    calls = {"n": 0}
    running = {"n": 0, "max": 0}
    lock_held = {"all": True}
    event = asyncio.Event()
    client = types.SimpleNamespace(is_connected=True, _historical_lock=asyncio.Lock())

    async def _fake_backfill(c, pool, symbols):
        calls["n"] += 1
        running["n"] += 1
        running["max"] = max(running["max"], running["n"])
        if not client._historical_lock.locked():
            lock_held["all"] = False
        if calls["n"] == 1:
            event.set()  # 模拟回填在途时又有一次重连触发
        await asyncio.sleep(0.01)
        running["n"] -= 1

    monkeypatch.setattr(main, "startup_minute_gap_backfill", _fake_backfill)

    worker = asyncio.create_task(
        main.reconnect_backfill_worker(event, client, None, [])
    )
    event.set()
    await asyncio.sleep(0.05)
    worker.cancel()
    with pytest.raises(asyncio.CancelledError):
        await worker

    assert calls["n"] == 2, "在途期间的触发应让 worker 补跑一次"
    assert running["max"] == 1, "回填串行执行，不应重叠"
    assert lock_held["all"], "回填必须在 client._historical_lock 之内执行（防 pacing 并发）"


@pytest.mark.asyncio
async def test_worker_skips_when_disconnected(monkeypatch):
    """未连接时 worker 不发历史请求。"""
    calls = {"n": 0}

    async def _fake_backfill(client, pool, symbols):
        calls["n"] += 1

    monkeypatch.setattr(main, "startup_minute_gap_backfill", _fake_backfill)
    client = types.SimpleNamespace(is_connected=False)
    event = asyncio.Event()

    worker = asyncio.create_task(
        main.reconnect_backfill_worker(event, client, None, [])
    )
    event.set()
    await asyncio.sleep(0.02)
    worker.cancel()
    with pytest.raises(asyncio.CancelledError):
        await worker

    assert calls["n"] == 0, "未连接时不应调用 startup_minute_gap_backfill"
