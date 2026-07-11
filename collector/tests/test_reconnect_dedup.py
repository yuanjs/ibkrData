"""锁住 gateway 重连去重意图：半开连接反复断开时只派生一个重连协程。

背景见 docs/reconnect_leak_analysis.md。历史缺陷是 _on_disconnect 每次
disconnectedEvent 都无条件 ensure_future(connect_with_retry)，Gateway 重启时
ib_insync 经 wasReady 路径反复 emit disconnectedEvent，导致重连协程数指数级
增长、耗尽 FD/CPU。

修复用 _gateway_reconnect_task 的 done 判断去重。这些测试验证该意图：
  - 存活期间再次断开不新建协程（阻断指数增长）
  - 协程结束后再次断开会新建协程（证明是 done 收敛、不是死锁）
"""

import asyncio
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
sys.modules.pop("config", None)

import pytest  # noqa: E402

if "aiohttp" not in sys.modules:  # 与其他 collector 测试一致的 import stub
    import types

    aiohttp = types.ModuleType("aiohttp")
    aiohttp.web = types.SimpleNamespace(
        Response=object,
        Application=object,
        AppRunner=object,
        TCPSite=object,
    )
    sys.modules["aiohttp"] = aiohttp
    sys.modules["aiohttp.web"] = aiohttp.web

from main import IBKRClient  # noqa: E402


def _make_client():
    """绕过 __init__ 构造 IBKRClient，避免真连 IB Gateway。"""
    client = IBKRClient.__new__(IBKRClient)
    client._gateway_reconnect_task = None
    return client


def _install_fake_retry(client, loop):
    """让 connect_with_retry 变成受事件控制的常驻协程。

    返回 (release, started)。协程在被 release 前一直存活（done() 为 False），
    模拟真实 while True 退避重试循环仍在跑。release() 让协程正常 return。
    started 记录被派生的次数——这正是历史缺陷里指数增长的那个计数器。
    """
    started = []

    async def fake_connect_with_retry():
        started.append(1)
        await asyncio.Event().wait()  # 存活直到被 release（协程被取消即结束）

    client.connect_with_retry = fake_connect_with_retry
    return started


@pytest.mark.asyncio
async def test_on_disconnect_only_starts_one_reconnect_during_half_open_storm():
    """连续多次 disconnectedEvent 期间，全局只有一个 connect_with_retry 协程。

    若回归到无条件 ensure_future，此测试会因为 started > 1 而失败——那正是
    指数级协程泄漏的起点。"""
    client = _make_client()
    started = _install_fake_retry(client, asyncio.get_event_loop())

    # 模拟 Gateway 重启时的“半开连接反复断开”：ib_insync 反复 emit
    # disconnectedEvent，_on_disconnect 被连续触发。
    for _ in range(5):
        client._on_disconnect()

    # 让 ensure_future/create_task 派生的协程真跑起来（进入 wait）。
    await asyncio.sleep(0)

    assert sum(started) == 1, (
        f"应当只派生 1 个重连协程，实际派生 {sum(started)} 个 —— "
        "回归到无条件 ensure_future 的指数泄漏"
    )
    # 同时核对去重句柄即为那个唯一在跑的协程，且未 done。
    assert client._gateway_reconnect_task is not None
    assert not client._gateway_reconnect_task.done()

    # 清理：取消正在 wait 的协程，避免 task 泄漏影响后续用例。
    client._gateway_reconnect_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await client._gateway_reconnect_task


@pytest.mark.asyncio
async def test_on_disconnect_starts_new_retry_only_after_previous_done():
    """协程结束后再次断开应新建协程 —— 证明是 done 判断的收敛，不是永久锁死。

    修复旨在阻断“多余派生”，但不能误伤“正常断开→重试→失败→重试”。
    done 判断保证：协程还在跑就跳过；协程结束了下次断开能正常重启。"""

    client = _make_client()
    started = _install_fake_retry(client, asyncio.get_event_loop())

    client._on_disconnect()
    await asyncio.sleep(0)
    assert sum(started) == 1
    first_task = client._gateway_reconnect_task
    assert not first_task.done()

    # 模拟第一轮重试协程自行结束（真实场景中 connect_with_retry 是 return 出去）。
    first_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first_task
    assert first_task.done()

    # 再次断开：因为前一个协程已 done，应能正常新建一个重连协程。
    client._on_disconnect()
    await asyncio.sleep(0)
    assert sum(started) == 2, "前一轮协程已结束后，新一轮断开应能新建重连协程"
    second_task = client._gateway_reconnect_task
    assert second_task is not first_task
    assert not second_task.done()

    second_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await second_task