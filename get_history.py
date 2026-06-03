#!/usr/bin/env python3
"""
IBKR 历史数据获取测试工具
从 IB Gateway 获取指定产品的 1 分钟 K 线历史数据。

用法:
  python get_history.py --list                    # 查看所有可用产品
  python get_history.py SPI                       # 获取 SPI 最近 2 天的 1 分 K 线
  python get_history.py MNQ --duration "1 D"      # 获取 MNQ 最近 1 天
  python get_history.py USD.JPY --csv             # 获取 USD.JPY 并导出 CSV
  python get_history.py MYM --show 50             # 显示 50 条 K 线
  python get_history.py AAPL --duration "5 D"     # 任意产品

IBKR duration 格式说明（只支持 S/D/W/M/Y）:
  "1800 S"  = 最近 30 分钟    "1 D" = 最近 1 天
  "5 D"     = 最近 5 天        "1 M" = 最近 1 个月
  "1 Y"     = 最近 1 年
"""

import argparse
import asyncio
import csv
import os
import sys

# Python 3.12+ 兼容: ib_insync 的 eventkit 在导入时需要事件循环
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from dotenv import load_dotenv
from ib_insync import IB, Contract, util

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

# ============================================================
# 预配置产品列表
# ============================================================
PREDEFINED_SYMBOLS = [
    {"symbol": "SPI", "sec_type": "FUT", "exchange": "SNFE", "currency": "AUD"},
    {"symbol": "USD.JPY", "sec_type": "CASH", "exchange": "IDEALPRO", "currency": "JPY"},
    {"symbol": "AUD.USD", "sec_type": "CASH", "exchange": "IDEALPRO", "currency": "USD"},
    {"symbol": "MYM", "sec_type": "FUT", "exchange": "CBOT", "currency": "USD"},
    {"symbol": "N225M", "sec_type": "FUT", "exchange": "OSE.JPN", "currency": "JPY"},
    {"symbol": "10Y", "sec_type": "FUT", "exchange": "CBOT", "currency": "USD"},
    {"symbol": "ZC", "sec_type": "FUT", "exchange": "CBOT", "currency": "USD"},
    {"symbol": "MNQ", "sec_type": "FUT", "exchange": "CME", "currency": "USD"},
    {"symbol": "MES", "sec_type": "FUT", "exchange": "CME", "currency": "USD"},
]
SYMBOL_MAP = {s["symbol"]: s for s in PREDEFINED_SYMBOLS}

# 用于收集 IBKR 错误事件
_ibkr_errors: list[str] = []


def resolve_contract(ib: IB, symbol: str, sec_type: str, exchange: str, currency: str) -> Contract:
    """解析合约为 IBKR Contract 对象。期货自动通过 CONTFUT 获取当前活跃合约。"""
    contract_symbol = symbol.split(".")[0] if sec_type == "CASH" and "." in symbol else symbol

    if sec_type == "FUT":
        cont = Contract(secType="CONTFUT", symbol=symbol, exchange=exchange, currency=currency)
        details = ib.reqContractDetails(cont)
        if details:
            r = details[0].contract
            contract = Contract(
                secType="FUT", symbol=r.symbol, exchange=r.exchange,
                currency=r.currency, lastTradeDateOrContractMonth=r.lastTradeDateOrContractMonth,
                tradingClass=r.tradingClass, multiplier=r.multiplier,
            )
            qualified = ib.qualifyContracts(contract)
            return qualified[0] if qualified else contract
        fallback = Contract(secType="FUT", symbol=symbol, exchange=exchange, currency=currency)
        cds = ib.reqContractDetails(fallback)
        if cds:
            return sorted(cds, key=lambda x: x.contract.lastTradeDateOrContractMonth)[0].contract
        raise ValueError(f"无法解析期货合约: {symbol}")

    contract = Contract(symbol=contract_symbol, secType=sec_type, exchange=exchange, currency=currency)
    qualified = ib.qualifyContracts(contract)
    return qualified[0] if qualified else contract


def print_summary(bars, symbol: str):
    if not bars:
        print(f"  ❌ {symbol}: 没有获取到数据")
        return
    opens = [float(b.open) for b in bars]
    highs = [float(b.high) for b in bars]
    lows = [float(b.low) for b in bars]
    closes = [float(b.close) for b in bars]
    volumes = [int(b.volume) if b.volume and b.volume > 0 else 0 for b in bars]
    print(f"\n  📊 {symbol} 数据摘要:")
    print(f"     总 K 线数: {len(bars)}")
    print(f"     时间范围: {bars[0].date}  ~  {bars[-1].date}")
    print(f"     开盘区间: {min(opens):.4f}  ~  {max(opens):.4f}")
    print(f"     最高区间: {min(highs):.4f}  ~  {max(highs):.4f}")
    print(f"     最低区间: {min(lows):.4f}  ~  {max(lows):.4f}")
    print(f"     收盘区间: {min(closes):.4f}  ~  {max(closes):.4f}")
    print(f"     总成交量: {sum(volumes):,}")


def _on_error(reqId, errorCode, errorString, contract):
    """IBKR 错误事件回调。"""
    _ibkr_errors.append(f"reqId={reqId} code={errorCode} msg={errorString}")
    if errorCode == 162:
        print(f"\n  ⚠️  IBKR 历史数据服务错误 (code=162): {errorString}")
        print(f"     这通常表示账户没有该产品的历史数据权限。")
        print(f"     请检查 IB Gateway/TWS → 配置 → API → 市场数据权限。")
    elif errorCode == 2107:
        print(f"\n  ⚠️  HMDS (历史数据服务) 连接未激活 (code=2107)")
        print(f"     HMDS 历史数据服务器当前处于非活跃状态。")
        print(f"     可能需要重启 IB Gateway/TWS 来激活 HMDS 连接。")
    elif errorCode == 321:
        print(f"\n  ⚠️  IBKR 请求参数错误 (code=321): {errorString}")


def main():
    parser = argparse.ArgumentParser(
        description="IBKR 历史数据获取测试工具 - 获取指定产品的 1 分钟 K 线",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
IBKR duration 格式说明:
  "1800 S" = 30 分钟    "1 D" = 1 天    "5 D" = 5 天
  "1 M"    = 1 个月     "1 Y" = 1 年

示例:
  python get_history.py --list                   # 查看所有可用产品
  python get_history.py SPI                      # 获取 SPI 最近 2 天的 1 分 K 线
  python get_history.py MNQ --duration "1 D"     # 获取 MNQ 最近 1 天
  python get_history.py USD.JPY --csv            # 导出 USD.JPY 为 CSV
  python get_history.py MYM --show 50            # 显示 50 条 K 线
  python get_history.py AAPL --duration "1 M"    # 任意产品
        """,
    )
    parser.add_argument("symbol", nargs="?", help="产品代码，如 SPI, MNQ, USD.JPY")
    parser.add_argument("--list", action="store_true", help="列出所有预配置产品")
    parser.add_argument("--duration", default="2 D",
                        help="时间范围 (默认: '2 D'); 支持格式: 1800 S, 1 D, 5 D, 1 M, 1 Y")
    parser.add_argument("--show", type=int, default=20, help="显示多少条 K 线 (默认: 20)")
    parser.add_argument("--csv", action="store_true", help="导出为 CSV 文件")
    parser.add_argument("--output", default=None, help="CSV 输出文件名 (默认: {symbol}_1min.csv)")
    parser.add_argument("--timeout", type=int, default=30,
                        help="历史数据请求超时秒数 (默认: 30)")

    args = parser.parse_args()

    if args.list:
        print("\n📋 预配置产品列表:\n")
        print(f"  {'Symbol':>10}  {'类型':>6}  {'交易所':>12}  {'货币':>6}  {'示例命令'}")
        print(f"  {'-'*70}")
        for s in PREDEFINED_SYMBOLS:
            print(f"  {s['symbol']:>10}  {s['sec_type']:>6}  {s['exchange']:>12}  {s['currency']:>6}  python get_history.py {s['symbol']}")
        print("\n  你也可以传入任意 symbol（如 'AAPL'），脚本会尝试自动解析。\n")
        return

    if not args.symbol:
        parser.print_help()
        print("\n  提示: 使用 --list 查看所有可用产品\n")
        return

    symbol = args.symbol.upper()
    output_csv = args.output or (f"{symbol}_1min.csv" if args.csv else None)

    # ---------- 验证 duration 格式 ----------
    dur = args.duration
    parts = dur.split()
    if len(parts) == 2:
        num, unit = parts
        if unit.upper() not in ("S", "D", "W", "M", "Y"):
            print(f"\n  ❌ 不支持的 duration 单位 '{unit}'。仅支持: S(秒), D(天), W(周), M(月), Y(年)")
            print(f"     例如: '1800 S' (30分钟), '1 D' (1天), '1 M' (1个月)")
            sys.exit(1)

    # ---------- 连接 IB Gateway ----------
    host = os.getenv("IB_HOST", "127.0.0.1")
    port = int(os.getenv("IB_PORT", "4001"))
    client_id = int(os.getenv("IB_CLIENT_ID", "10"))
    # 避免与 collector 冲突
    test_id = 99

    print(f"\n🔌 正在连接 IB Gateway: {host}:{port} (clientId={test_id})")

    ib = IB()
    ib.RequestTimeout = args.timeout  # 全局超时
    ib.errorEvent += _on_error        # 注册错误事件

    try:
        ib.connect(host, port, clientId=test_id)
    except ConnectionRefusedError:
        print(f"\n  ❌ 连接被拒绝！请确保 IB Gateway/TWS 已启动并配置了 API 端口 {port}")
        print(f"     检查: 配置 → API → 启用 'Enable ActiveX and Socket Clients'")
        sys.exit(1)
    except Exception as e:
        print(f"\n  ❌ 连接失败: {e}")
        sys.exit(1)

    server_time = ib.reqCurrentTime()
    print(f"  ✅ 已连接 (服务器时间: {server_time})")

    if symbol not in SYMBOL_MAP:
        print(f"\n  ⚠️  产品 '{symbol}' 不在预配置列表中，将尝试自动解析...")

    info = SYMBOL_MAP.get(symbol, {})
    sec_type = info.get("sec_type", "STK")
    exchange = info.get("exchange", "SMART")
    currency = info.get("currency", "USD")

    # ---------- 解析合约 ----------
    try:
        contract = resolve_contract(ib, symbol, sec_type, exchange, currency)
        print(f"\n  📋 合约信息:")
        for k in ("symbol", "secType", "exchange", "currency", "localSymbol",
                   "lastTradeDateOrContractMonth", "multiplier", "primaryExchange"):
            v = getattr(contract, k, None)
            if v:
                print(f"     {k:>18}: {v}")
    except Exception as e:
        print(f"\n  ❌ 合约解析失败: {e}")
        ib.disconnect()
        sys.exit(1)

    # ---------- 获取历史数据 ----------
    what_to_show = "MIDPOINT" if sec_type == "CASH" else "TRADES"
    print(f"\n  📥 获取 1 分钟 K 线: duration={dur}, whatToShow={what_to_show}, timeout={args.timeout}s")

    bars = []
    try:
        bars = ib.reqHistoricalData(
            contract,
            endDateTime="",
            durationStr=dur,
            barSizeSetting="1 min",
            whatToShow=what_to_show,
            useRTH=False,
            formatDate=1,
        )
    except (TimeoutError, asyncio.TimeoutError):
        print(f"\n  ❌ 请求超时 (>{args.timeout}秒)")
        print(f"     通常是因为 IB Gateway 无法获取该产品的历史数据。")
        print(f"     可能原因: 没有市场数据权限 / 非交易时段 / 数据源限制")

        # 诊断: 尝试获取日线
        print(f"\n  🔍 尝试获取日线数据做诊断...")
        try:
            daily_bars = ib.reqHistoricalData(
                contract, endDateTime="", durationStr="1 M",
                barSizeSetting="1 day", whatToShow=what_to_show,
                useRTH=False, formatDate=1,
            )
            if daily_bars:
                print(f"     日线数据获取成功! ({len(daily_bars)} 条)")
            else:
                print(f"     日线数据也返回空")
        except Exception:
            print(f"     日线数据也失败")

    except Exception as e:
        print(f"\n  ❌ 获取历史数据失败: {e}")

    # ---------- 打印错误摘要 ----------
    if _ibkr_errors:
        print(f"\n  📋 IBKR 错误事件摘要 ({len(_ibkr_errors)} 条):")
        for err in _ibkr_errors[-5:]:  # 最多显示 5 条
            print(f"     {err}")

    # ---------- 输出数据 ----------
    if not bars:
        print(f"\n  ⚠️  没有获取到 K 线数据")
        print(f"\n  💡 历史数据获取失败的常见原因:")
        print(f"     1. HMDS 历史数据服务连接未激活 (重启 IB Gateway 可解决)")
        print(f"     2. IB Gateway/TWS 没有开通该产品的市场数据订阅")
        print(f"     3. 如果是模拟交易 (paper) 账户，历史数据可能受限")
        print(f"     4. 当前非交易时段（有些产品只在交易时段有历史数据）")
        print(f"     5. 防火墙阻止了历史数据端口")
        print(f"\n     🔧 修复建议:")
        print(f"     重启 IB Gateway/TWS (断开后重新登录，HMDS 会自动连接)")
        print(f"     如果重启无效，检查 IB Gateway: 编辑 → 设置 → API → 市场数据权限")
    else:
        print_summary(bars, symbol)
        show = args.show

        # 最新 N 条
        print(f"\n  📋 最近 {min(show, len(bars))} 条 K 线:")
        print(f"  {'时间':>19}  {'开盘':>10}  {'最高':>10}  {'最低':>10}  {'收盘':>10}  {'成交量':>8}")
        print(f"  {'-'*75}")
        for bar in bars[-show:]:
            t = bar.date.strftime("%Y-%m-%d %H:%M:%S") if hasattr(bar.date, 'strftime') else str(bar.date)
            print(f"  {t}  {bar.open:>10.4f}  {bar.high:>10.4f}  {bar.low:>10.4f}  {bar.close:>10.4f}  {int(bar.volume) if bar.volume and bar.volume > 0 else 0:>8,}")

        # 最早 N 条
        if len(bars) > show * 2:
            print(f"\n  📋 最早 {min(show, len(bars))} 条 K 线:")
            print(f"  {'时间':>19}  {'开盘':>10}  {'最高':>10}  {'最低':>10}  {'收盘':>10}  {'成交量':>8}")
            print(f"  {'-'*75}")
            for bar in bars[:show]:
                t = bar.date.strftime("%Y-%m-%d %H:%M:%S") if hasattr(bar.date, 'strftime') else str(bar.date)
                print(f"  {t}  {bar.open:>10.4f}  {bar.high:>10.4f}  {bar.low:>10.4f}  {bar.close:>10.4f}  {int(bar.volume) if bar.volume and bar.volume > 0 else 0:>8,}")

        # 导出 CSV
        if output_csv:
            filepath = output_csv
            if not os.path.isabs(filepath):
                filepath = os.path.join(os.path.dirname(__file__), filepath)
            with open(filepath, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["time", "symbol", "open", "high", "low", "close", "volume", "barCount"])
                for bar in bars:
                    t = bar.date.strftime("%Y-%m-%d %H:%M:%S") if hasattr(bar.date, 'strftime') else str(bar.date)
                    w.writerow([t, symbol, bar.open, bar.high, bar.low, bar.close,
                                int(bar.volume) if bar.volume and bar.volume > 0 else 0,
                                bar.barCount if hasattr(bar, 'barCount') else 0])
            print(f"\n  💾 已导出 CSV: {filepath} ({len(bars)} 行)")

    ib.disconnect()
    print()


if __name__ == "__main__":
    main()
