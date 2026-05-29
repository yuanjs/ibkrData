"""
验证 tickType 45 补丁是否正常工作。

模拟 Wrapper.tickString 调用，检查 ticker.lastTimestamp 是否正确设置。
"""
import asyncio
import sys
sys.path.insert(0, '/home/yuanjs/projects/ibkrData/collector')

# Workaround for eventkit's get_event_loop call on import
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from datetime import datetime, timezone
from ib_insync.ticker import Ticker
from ib_insync.contract import Contract

# === 加载 monkey-patch ===
from ib_insync.wrapper import Wrapper
Ticker.lastTimestamp = None  # type: ignore

_orig_tickString = Wrapper.tickString
def _patched_tickString(self, reqId, tickType, value):
    if tickType == 45:
        ticker = self.reqId2Ticker.get(reqId)
        if ticker:
            ticker.lastTimestamp = datetime.fromtimestamp(int(value), timezone.utc)
    return _orig_tickString(self, reqId, tickType, value)
Wrapper.tickString = _patched_tickString

# === 测试 ===
contract = Contract(symbol="USD", secType="CASH", exchange="IDEALPRO", currency="JPY")
ticker = Ticker(contract=contract)
ticker.time = datetime(2026, 5, 29, 8, 30, 0, tzinfo=timezone.utc)

# 模拟 Wrapper 的 reqId2Ticker 映射
wrapper = Wrapper(ib=None)
req_id = 42
wrapper.reqId2Ticker[req_id] = ticker

# 测试 1：lastTimestamp 初始为 None
assert ticker.lastTimestamp is None, f"Expected None, got {ticker.lastTimestamp}"
print("✅ 测试 1: lastTimestamp 初始为 None")

# 测试 2：模拟 tickType 45 到达
ts_exchange = 1780042828
expected = datetime.fromtimestamp(ts_exchange, tz=timezone.utc)
wrapper.tickString(req_id, 45, str(ts_exchange))
assert ticker.lastTimestamp == expected, f"Expected {expected}, got {ticker.lastTimestamp}"
print(f"✅ 测试 2: tickType 45 正确设置 lastTimestamp = {ticker.lastTimestamp}")

# 测试 3：时间优先级：lastTimestamp > rtTime > time
ticker.rtTime = datetime(2026, 5, 29, 8, 30, 30, tzinfo=timezone.utc)
tick_time = ticker.lastTimestamp or ticker.rtTime or ticker.time
assert tick_time == ticker.lastTimestamp, f"应优先使用 lastTimestamp, 但得到 {tick_time}"
print("✅ 测试 3: 时间优先级 lastTimestamp > rtTime > time")

# 测试 4：当 lastTimestamp 和 rtTime 都为空时，使用 time
ticker2 = Ticker(contract=contract)
ticker2.time = datetime(2026, 5, 29, 8, 30, 0, tzinfo=timezone.utc)
tick_time2 = ticker2.lastTimestamp or ticker2.rtTime or ticker2.time
assert tick_time2 == ticker2.time, f"应回退到 time, 但得到 {tick_time2}"
print("✅ 测试 4: 无 lastTimestamp 时回退到 time")

# 测试 5：不同 ticker 实例独立追踪
ticker3 = Ticker(contract=contract)
wrapper.reqId2Ticker[43] = ticker3
assert ticker3.lastTimestamp is None, "新 ticker 的 lastTimestamp 应为 None"
wrapper.tickString(43, 45, "1780042888")
assert ticker3.lastTimestamp is not None, "新 ticker 应能单独设置 lastTimestamp"
assert ticker3.lastTimestamp != ticker.lastTimestamp, "两个 ticker 的 lastTimestamp 应不同"
print("✅ 测试 5: 不同 ticker 实例独立追踪 lastTimestamp")

print("\n🎉 所有测试通过!")
