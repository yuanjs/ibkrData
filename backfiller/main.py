#!/usr/bin/env python3
"""
IBKR 历史 1 分钟 K 线数据拉取工具

Usage:
  python -m backfiller.main --pull
  python -m backfiller.main --pull --only SPI MNQ
  python -m backfiller.main --status
  python -m backfiller.main --status --only SPI
  python -m backfiller.main --check
  python -m backfiller.main --check --only AAPL
"""

import argparse
import asyncio
import logging
import signal
from pathlib import Path

# Python 3.12+ compatibility for ib_insync's eventkit
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from ib_insync import IB

from backfiller.config import load_config, AppConfig
from backfiller.contract import resolve_contract, resolve_what_to_show
from backfiller.db_writer import MinuteBarWriter
from backfiller.scheduler import PullScheduler

logger = logging.getLogger(__name__)

PROGRESS_DIR = Path(__file__).parent / "progress"


# ---------------------------------------------------------------------------
# --status
# ---------------------------------------------------------------------------

async def cmd_status(args, cfg):
    pool = await MinuteBarWriter.create_pool(cfg.db_url)
    try:
        writer = MinuteBarWriter(pool)
        products = cfg.products
        if args.only:
            products = [p for p in products if p.symbol in args.only]

        print(f"\n产品状态总览:\n")
        for p in products:
            t_min, t_max, cnt = await writer.get_range(p.symbol, p.sec_type)
            gaps = await writer.detect_gaps(p.symbol, sec_type=p.sec_type)
            bar_count = f"{cnt:,}"
            if t_min and t_max:
                date_range = f"{t_min.date()} ~ {t_max.date()}"
                status_icon = "✅"
                status_str = "已完成"
                if gaps:
                    status_icon = "⚠️"
                    status_str = f"有缺口 ({len(gaps)}处)"
            else:
                date_range = "—"
                bar_count = "0"
                status_icon = "🔜"
                status_str = "尚未拉取"

            print(f"  {p.symbol:<10} ({p.sec_type}/{p.exchange}/{p.currency})  "
                  f"{status_icon} {status_str}")
            print(f"  {'':>10} {date_range} | {bar_count} bars")
            if gaps:
                for g in gaps[:3]:
                    print(f"  {'':>10}   ⚠ 缺口 {g['gap_start']} ~ {g['gap_end']} ({g['diff_minutes']}min)")
                if len(gaps) > 3:
                    print(f"  {'':>10}   ... and {len(gaps)-3} more gaps")
            print()
    finally:
        await pool.close()


# ---------------------------------------------------------------------------
# --check  (纯同步，不涉及 asyncio)
# ---------------------------------------------------------------------------

def cmd_check(args, cfg):
    """Synchronous check: verify IBKR contract resolution + HMDS data availability."""
    print()
    ib = IB()
    ib.RequestTimeout = 30
    products = cfg.products
    if args.only:
        products = [p for p in products if p.symbol in args.only]

    try:
        ib.connect(cfg.ib_host, cfg.ib_port, clientId=cfg.ib_client_id)
    except Exception as e:
        print(f"  ❌ 无法连接 IB Gateway: {e}")
        return

    try:
        for p in products:
            try:
                contract = resolve_contract(ib, p.symbol, p.sec_type, p.exchange, p.currency)
                if contract is None:
                    print(f"  {p.symbol:<10} ❌ 合约解析失败")
                    continue
                what = resolve_what_to_show(p.sec_type)
                bars = ib.reqHistoricalData(
                    contract, endDateTime="", durationStr="2 D",
                    barSizeSetting="1 min", whatToShow=what,
                    useRTH=False, formatDate=1,
                )
                if bars:
                    print(f"  {p.symbol:<10} ✅ {len(bars)}条1分钟K线")
                else:
                    print(f"  {p.symbol:<10} ⚠️ 合约有效但无历史数据")
            except Exception as e:
                print(f"  {p.symbol:<10} ❌ {e}")
    finally:
        ib.disconnect()
    print()


# ---------------------------------------------------------------------------
# --pull
# ---------------------------------------------------------------------------

async def cmd_pull(args, cfg):
    products = cfg.products
    if args.only:
        products = [p for p in products if p.symbol in args.only]

    filtered_cfg = AppConfig(
        products=products,
        start=cfg.start, end=cfg.end,
        request_interval_seconds=cfg.request_interval_seconds,
        ib_host=cfg.ib_host, ib_port=cfg.ib_port,
        ib_client_id=cfg.ib_client_id, db_url=cfg.db_url,
    )

    pool = await MinuteBarWriter.create_pool(cfg.db_url)
    writer = MinuteBarWriter(pool)
    scheduler = PullScheduler(filtered_cfg, writer, PROGRESS_DIR,
                              allow_new_products=args.only is None)

    # Signal handler for graceful Ctrl+C
    loop = asyncio.get_running_loop()

    def _signal_handler():
        logger.info("SIGINT received, finishing current window...")
        scheduler.request_stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    try:
        await scheduler.run()
    finally:
        scheduler.disconnect()
        await pool.close()
    logger.info("Pull complete")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="IBKR 历史 1 分钟 K 线数据拉取工具",
    )
    parser.add_argument("--config", default=None,
                        help="config.yaml 路径 (默认: backfiller/config.yaml)")

    sub = parser.add_mutually_exclusive_group(required=True)
    sub.add_argument("--pull", action="store_true", help="拉取历史数据")
    sub.add_argument("--status", action="store_true", help="查询已拉取数据状态")
    sub.add_argument("--check", action="store_true", help="验证 IBKR 可拉取性")

    parser.add_argument("--only", nargs="+", default=None,
                        help="只操作指定产品 (空格分隔)")
    return parser.parse_args(argv)


def main():
    args = parse_args()
    cfg = load_config(args.config)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if args.pull:
        asyncio.run(cmd_pull(args, cfg))
    elif args.status:
        asyncio.run(cmd_status(args, cfg))
    elif args.check:
        cmd_check(args, cfg)  # synchronous, no asyncio needed


if __name__ == "__main__":
    main()
