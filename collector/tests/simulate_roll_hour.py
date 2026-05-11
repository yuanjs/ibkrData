"""
模拟实时tick数据经过 DailyBarTracker 处理的完整流程，
验证roll hour前后日K线的OHLC数据是否正确。

用法:
  直接运行:  python simulate_roll_hour.py              # 纯模拟，不写DB
  写入DB:    python simulate_roll_hour.py --write-db    # 需要DB运行时写入daily_bars，供前端查看
"""

import argparse
import sys
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

# 添加父目录到路径，以便导入collector模块
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from daily_tracker import DailyBarTracker, _effective_date_str, _bucket_time
from config import PRODUCT_ROLL_CONFIG


def simulate_scenario(description: str, ticks: list[tuple], tracker: DailyBarTracker, symbol: str):
    """模拟一组tick输入tracker，打印每一步的状态变化"""
    print(f"\n{'='*70}")
    print(f"场景: {description}")
    print(f"{'='*70}")

    for i, (tick_time, price, size) in enumerate(ticks):
        tracker.on_tick(symbol, price, size, tick_time)
        bar = tracker._bars.get(symbol)
        print(f"\n  Tick #{i+1}: time={tick_time} (local={tick_time.astimezone(ZoneInfo(PRODUCT_ROLL_CONFIG[symbol]['timezone']))}), "
              f"price={price}")
        if bar:
            print(f"  -> Bar: date_str={bar['date_str']}, time={bar['time']}, "
                  f"O={bar['open']}, H={bar['high']}, L={bar['low']}, C={bar['close']}, V={bar['volume']}")

    # 最终验证
    bar = tracker._bars.get(symbol)
    print(f"\n  >>> 最终结果: date_str={bar['date_str']}, "
          f"O={bar['open']}, H={bar['high']}, L={bar['low']}, C={bar['close']}, V={bar['volume']}")
    return bar


def verify_bar(bar: dict, expected_date: str, expected_open: float, expected_high: float,
               expected_low: float, expected_close: float, scenario: str):
    """验证OHLC数据是否符合预期"""
    errors = []
    if bar["date_str"] != expected_date:
        errors.append(f"date_str: 期望 {expected_date}, 实际 {bar['date_str']}")
    if bar["open"] != expected_open:
        errors.append(f"open: 期望 {expected_open}, 实际 {bar['open']}")
    if bar["high"] != expected_high:
        errors.append(f"high: 期望 {expected_high}, 实际 {bar['high']}")
    if bar["low"] != expected_low:
        errors.append(f"low: 期望 {expected_low}, 实际 {bar['low']}")
    if bar["close"] != expected_close:
        errors.append(f"close: 期望 {expected_close}, 实际 {bar['close']}")

    if errors:
        print(f"\n  ❌ {scenario} 验证失败:")
        for e in errors:
            print(f"     - {e}")
    else:
        print(f"\n  ✅ {scenario} 验证通过!")


def scenario_1_spi_before_roll(tracker: DailyBarTracker):
    """场景1: SPI在roll hour之前 — tick应归属当天"""
    symbol = "SPI"
    sydney = ZoneInfo("Australia/Sydney")
    # 2026-05-11 16:00 Sydney = 2026-05-11 06:00 UTC (roll hour是17:10)
    dt = datetime(2026, 5, 11, 6, 0, 0, tzinfo=timezone.utc)

    ticks = [
        (dt, 8700.0, 10),                            # tick #1: open=8700
        (dt + timedelta(minutes=30), 8710.0, 5),      # tick #2: high=8710
        (dt + timedelta(minutes=50), 8690.0, 8),      # tick #3: low=8690 (16:50 Sydney, roll hour前)
        (dt + timedelta(minutes=55), 8705.0, 3),      # tick #4: close=8705 (16:55 Sydney, roll hour前)
    ]
    bar = simulate_scenario("SPI roll hour之前 — tick归属当天(2026-05-11)", ticks, tracker, symbol)
    verify_bar(bar, "20260511", 8700.0, 8710.0, 8690.0, 8705.0, "SPI roll hour之前")


def scenario_2_spi_after_roll(tracker: DailyBarTracker):
    """场景2: SPI在roll hour之后 — tick应归属次日"""
    symbol = "SPI"
    sydney = ZoneInfo("Australia/Sydney")
    # 2026-05-11 17:30 Sydney = 2026-05-11 07:30 UTC (roll hour是17:10，已过)
    dt = datetime(2026, 5, 11, 7, 30, 0, tzinfo=timezone.utc)

    ticks = [
        (dt, 8750.0, 10),                            # tick #1: open=8750 (新日K线open)
        (dt + timedelta(minutes=15), 8762.0, 5),      # tick #2: high=8762
        (dt + timedelta(minutes=30), 8744.0, 8),      # tick #3: low=8744
        (dt + timedelta(hours=1), 8761.0, 3),         # tick #4: close=8761
    ]
    bar = simulate_scenario("SPI roll hour之后 — tick归属次日(2026-05-12)", ticks, tracker, symbol)
    verify_bar(bar, "20260512", 8750.0, 8762.0, 8744.0, 8761.0, "SPI roll hour之后")


def scenario_3_spi_crossing_roll_hour(tracker: DailyBarTracker):
    """场景3: SPI横跨roll hour — 之前的tick归今天，之后的归明天"""
    symbol = "SPI"
    # 2026-05-11 16:30 Sydney = 2026-05-11 06:30 UTC (roll hour前)
    dt_before = datetime(2026, 5, 11, 6, 30, 0, tzinfo=timezone.utc)
    # 2026-05-11 17:20 Sydney = 2026-05-11 07:20 UTC (roll hour后)
    dt_after = datetime(2026, 5, 11, 7, 20, 0, tzinfo=timezone.utc)

    ticks = [
        (dt_before, 8700.0, 10),       # tick #1: 今天, open=8700, high=8700, low=8700
        (dt_before + timedelta(minutes=20), 8710.0, 5),  # tick #2: 今天, high=8710
        (dt_after, 8750.0, 8),          # tick #3: 明天! 新bar, open=8750
        (dt_after + timedelta(minutes=10), 8762.0, 3),   # tick #4: 明天, high=8762
        (dt_after + timedelta(minutes=20), 8744.0, 2),   # tick #5: 明天, low=8744
    ]

    # 注意：tracker只保留每个symbol的最新bar
    # 先模拟roll hour之前的bar，再模拟roll hour之后的bar
    print(f"\n{'='*70}")
    print(f"场景: SPI横跨roll hour")
    print(f"{'='*70}")

    for i, (tick_time, price, size) in enumerate(ticks):
        tracker.on_tick(symbol, price, size, tick_time)
        bar = tracker._bars.get(symbol)
        date_str = _effective_date_str(tick_time, symbol)
        print(f"\n  Tick #{i+1}: time={tick_time} (Sydney={tick_time.astimezone(ZoneInfo('Australia/Sydney'))}), "
              f"price={price}")
        print(f"  -> 归属日期: {date_str}")
        if bar:
            print(f"  -> Tracker: date_str={bar['date_str']}, "
                  f"O={bar['open']}, H={bar['high']}, L={bar['low']}, C={bar['close']}, V={bar['volume']}")

    bar = tracker._bars.get(symbol)
    print(f"\n  >>> Tracker当前bar (显示跨roll hour后最新的bar):")
    print(f"       date_str={bar['date_str']}, O={bar['open']}, H={bar['high']}, L={bar['low']}, C={bar['close']}")
    print(f"  >>> 注意: tracker只保留最新的bar（2026-05-12）。之前的bar (2026-05-11) 已被flush到DB")


def scenario_4_n225m_after_roll(tracker: DailyBarTracker):
    """场景4: N225M在roll hour之后 — tick应归属次日"""
    symbol = "N225M"
    tokyo = ZoneInfo("Asia/Tokyo")
    # 2026-05-11 17:00 Tokyo = 2026-05-11 08:00 UTC (roll hour是16:30，已过)
    dt = datetime(2026, 5, 11, 8, 0, 0, tzinfo=timezone.utc)

    ticks = [
        (dt, 38500.0, 10),                             # tick #1: open=38500
        (dt + timedelta(minutes=20), 38600.0, 5),       # tick #2: high=38600
        (dt + timedelta(minutes=40), 38450.0, 8),       # tick #3: low=38450
        (dt + timedelta(hours=1), 38550.0, 3),          # tick #4: close=38550
    ]
    bar = simulate_scenario("N225M roll hour之后 — tick归属次日(2026-05-12)", ticks, tracker, symbol)
    verify_bar(bar, "20260512", 38500.0, 38600.0, 38450.0, 38550.0, "N225M roll hour之后")


def scenario_5_mym_before_roll(tracker: DailyBarTracker):
    """场景5: MYM在roll hour之前 — tick应归属当天"""
    symbol = "MYM"
    chicago = ZoneInfo("America/Chicago")
    # 2026-05-11 15:00 Chicago = 2026-05-11 20:00 UTC (roll hour是16:00)
    # 注意: Chicago此时是CDT (UTC-5)
    dt = datetime(2026, 5, 11, 20, 0, 0, tzinfo=timezone.utc)

    ticks = [
        (dt, 41000.0, 10),
        (dt + timedelta(minutes=30), 41100.0, 5),
        (dt + timedelta(minutes=45), 40950.0, 8),
    ]
    bar = simulate_scenario("MYM roll hour之前(Chicago 15:00) — tick归属当天(2026-05-11)", ticks, tracker, symbol)
    verify_bar(bar, "20260511", 41000.0, 41100.0, 40950.0, 40950.0, "MYM roll hour之前")


def scenario_6_mym_after_roll(tracker: DailyBarTracker):
    """场景6: MYM在roll hour之后 — tick应归属次日"""
    symbol = "MYM"
    # 2026-05-11 17:00 Chicago = 2026-05-11 22:00 UTC (roll hour是16:00，已过)
    dt = datetime(2026, 5, 11, 22, 0, 0, tzinfo=timezone.utc)

    ticks = [
        (dt, 41200.0, 10),
        (dt + timedelta(minutes=15), 41300.0, 5),
        (dt + timedelta(minutes=30), 41150.0, 8),
    ]
    bar = simulate_scenario("MYM roll hour之后(Chicago 17:00) — tick归属次日(2026-05-12)", ticks, tracker, symbol)
    verify_bar(bar, "20260512", 41200.0, 41300.0, 41150.0, 41150.0, "MYM roll hour之后")


def scenario_7_weekend_roll(tracker: DailyBarTracker):
    """场景7: 周末tick — 应跳过周末"""
    symbol = "SPI"
    # 2026-05-09 周六 18:00 Sydney = 2026-05-09 08:00 UTC (周末tick)
    dt = datetime(2026, 5, 9, 8, 0, 0, tzinfo=timezone.utc)

    ticks = [
        (dt, 8800.0, 10),
        (dt + timedelta(hours=1), 8810.0, 5),
    ]
    bar = simulate_scenario("周末(周六) tick — 应跳到周一(2026-05-11)", ticks, tracker, symbol)
    verify_bar(bar, "20260511", 8800.0, 8810.0, 8800.0, 8810.0, "周末roll")


def scenario_8_real_market_data(tracker: DailyBarTracker):
    """场景8: 模拟当前真实市场状态 — SPI刚刚过roll hour"""
    symbol = "SPI"

    # 模拟从DB查到的今天盘中数据（2026-05-11, roll hour之前的tick）
    dt_before = datetime(2026, 5, 11, 5, 0, 0, tzinfo=timezone.utc)  # 16:00 Sydney

    # 模拟roll hour之后的tick（当前实时情况）
    dt_after = datetime(2026, 5, 11, 13, 0, 0, tzinfo=timezone.utc)  # 23:00 Sydney

    ticks = [
        # roll hour之前的盘中tick
        (dt_before, 8770.0, 100),                  # 今天(2026-05-11)
        (dt_before + timedelta(minutes=30), 8778.0, 50),
        (dt_before + timedelta(hours=1), 8668.0, 80),
        (dt_before + timedelta(hours=1, minutes=30), 8732.0, 30),

        # roll hour之后的新tick — 属于明天(2026-05-12)
        (dt_after, 8750.0, 10),                     # 新bar, open=8750
        (dt_after + timedelta(minutes=1), 8762.0, 5),   # high=8762
        (dt_after + timedelta(minutes=2), 8744.0, 8),   # low=8744
        (dt_after + timedelta(minutes=3), 8761.0, 3),   # close=8761
    ]

    print(f"\n{'='*70}")
    print(f"场景: 模拟当前真实市场 — 盘中(roll hour前) + roll hour后实时tick")
    print(f"{'='*70}")

    for i, (tick_time, price, size) in enumerate(ticks):
        tracker.on_tick(symbol, price, size, tick_time)
        bar = tracker._bars.get(symbol)
        date_str = _effective_date_str(tick_time, symbol)
        is_roll = "🔴 ROLL后" if date_str == "20260512" else "🟢 盘中"
        print(f"\n  {is_roll} Tick #{i+1}: time={tick_time} (Sydney={tick_time.astimezone(ZoneInfo('Australia/Sydney'))}), "
              f"price={price}")
        print(f"  -> 归属: {date_str}")
        if bar:
            print(f"  -> Tracker: date_str={bar['date_str']}, "
                  f"O={bar['open']}, H={bar['high']}, L={bar['low']}, C={bar['close']}")

    print(f"\n>>> 验证:")
    print(f"   - 前4个tick应归属 2026-05-11 (盘中数据) → 已由tracker之前flush到DB")
    print(f"   - 后4个tick应归属 2026-05-12 (roll hour后) → open=8750 (正确!)")


def write_to_db(tracker: DailyBarTracker, symbol: str):
    """将tracker的脏bar写入数据库，供前端查看"""
    bars = tracker.get_dirty_bars()
    if not bars:
        print("  (没有脏bar需要写入)")
        return

    try:
        import asyncpg
        from config import DB_URL
    except ImportError:
        print("  无法导入必要的模块 (asyncpg)。跳过DB写入。")
        return

    import asyncio

    async def _write():
        pool = await asyncpg.create_pool(DB_URL)
        writer = type('W', (), {'pool': pool})()
        from data_writer import DataWriter
        dw = DataWriter(pool)
        for bar in bars:
            print(f"  写入DB: {symbol} date_str={bar['date_str']} O={bar['open']} H={bar['high']} L={bar['low']} C={bar['close']}")
        await dw.upsert_daily_bars(bars)
        await pool.close()

    asyncio.run(_write())


def main():
    parser = argparse.ArgumentParser(description="模拟实时tick数据验证roll hour日K线逻辑")
    parser.add_argument("--write-db", action="store_true", help="将模拟数据写入数据库")
    parser.add_argument("--scenario", type=int, default=0,
                        help="运行指定场景 (1-8, 0=全部)")
    args = parser.parse_args()

    if args.scenario == 0 or args.scenario == 1:
        tracker = DailyBarTracker()
        scenario_1_spi_before_roll(tracker)
        if args.write_db: write_to_db(tracker, "SPI")

    if args.scenario == 0 or args.scenario == 2:
        tracker = DailyBarTracker()
        scenario_2_spi_after_roll(tracker)
        if args.write_db: write_to_db(tracker, "SPI")

    if args.scenario == 0 or args.scenario == 3:
        tracker = DailyBarTracker()
        scenario_3_spi_crossing_roll_hour(tracker)

    if args.scenario == 0 or args.scenario == 4:
        tracker = DailyBarTracker()
        scenario_4_n225m_after_roll(tracker)

    if args.scenario == 0 or args.scenario == 5:
        tracker = DailyBarTracker()
        scenario_5_mym_before_roll(tracker)

    if args.scenario == 0 or args.scenario == 6:
        tracker = DailyBarTracker()
        scenario_6_mym_after_roll(tracker)

    if args.scenario == 0 or args.scenario == 7:
        tracker = DailyBarTracker()
        scenario_7_weekend_roll(tracker)

    if args.scenario == 0 or args.scenario == 8:
        tracker = DailyBarTracker()
        scenario_8_real_market_data(tracker)

    print(f"\n{'='*70}")
    print("测试完成!")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
