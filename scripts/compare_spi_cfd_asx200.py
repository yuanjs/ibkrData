#!/usr/bin/env python3
"""Compare synthetic SPI continuous futures with ASX200 CFD history.

The script intentionally uses only the Python standard library plus asyncpg,
which is already used by this project.  It writes CSV snapshots, SVG charts,
and a Chinese markdown report under docs/asx200_spi_cfd_comparison/.
"""

from __future__ import annotations

import asyncio
import csv
import json
import math
import sqlite3
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import asyncpg


DB_URL = "postgresql://ibkr:password@localhost:5432/ibkrdata"
TRADEHISTORY_DIR = Path("/home/yuanjs/projects/tradehistory")
TRADEHISTORY_DB = TRADEHISTORY_DIR / "backend/databases/tradehistory.db"
OUT_DIR = Path("docs/asx200_spi_cfd_comparison")
START = datetime(2026, 1, 1, tzinfo=timezone.utc)
END = datetime(2026, 4, 1, tzinfo=timezone.utc)
START_DATE = date(2026, 1, 1)
END_DATE = date(2026, 3, 31)
ASX_TZ = ZoneInfo("Australia/Sydney")
SESSION_ROLL = time(17, 10)


@dataclass
class MinuteBar:
    ts: datetime
    open: float
    high: float
    low: float
    close: float
    bid_close: float | None = None
    ofr_close: float | None = None


def fnum(value: object) -> float | None:
    if value is None or value == "":
        return None
    return float(value)


def mid(a: float | None, b: float | None) -> float | None:
    if a is None and b is None:
        return None
    if a is None:
        return b
    if b is None:
        return a
    return (a + b) / 2.0


def session_date_for_asx(ts: datetime) -> date:
    local = ts.astimezone(ASX_TZ)
    session = local.date()
    if local.timetz().replace(tzinfo=None) >= SESSION_ROLL:
        session += timedelta(days=1)
    return session


def parse_tradehistory_datetime(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def load_cfd_minutes_from_db() -> dict[datetime, MinuteBar]:
    bars: dict[datetime, MinuteBar] = {}
    conn = sqlite3.connect(TRADEHISTORY_DB)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT timestamp, datetime,
                   ofr_open, ofr_high, ofr_low, ofr_close,
                   bid_open, bid_high, bid_low, bid_close,
                   mid_open, mid_high, mid_low, mid_close
            FROM candles_1m
            WHERE product = 'ASX200'
              AND datetime >= '2026-01-01 00:00:00'
              AND datetime <  '2026-04-01 00:00:00'
            ORDER BY datetime
            """
        )
        for row in rows:
            ts = parse_tradehistory_datetime(row["datetime"])
            bars[ts] = MinuteBar(
                ts=ts,
                open=float(row["mid_open"]),
                high=float(row["mid_high"]),
                low=float(row["mid_low"]),
                close=float(row["mid_close"]),
                bid_close=float(row["bid_close"]),
                ofr_close=float(row["ofr_close"]),
            )
    finally:
        conn.close()
    return dict(sorted(bars.items()))


def load_cfd_daily_from_db() -> dict[date, dict]:
    daily = {}
    conn = sqlite3.connect(TRADEHISTORY_DB)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT date, mid_open, mid_high, mid_low, mid_close
            FROM candles_daily
            WHERE product = 'ASX200'
              AND date >= '2026-01-01'
              AND date <= '2026-03-31'
            ORDER BY date
            """
        )
        minute_counts = defaultdict(int)
        for bar in load_cfd_minutes_from_db().values():
            sd = session_date_for_asx(bar.ts)
            if START_DATE <= sd <= END_DATE:
                minute_counts[sd] += 1
        for row in rows:
            sd = date.fromisoformat(row["date"])
            if START_DATE <= sd <= END_DATE:
                daily[sd] = {
                    "session_date": sd,
                    "open": float(row["mid_open"]),
                    "high": float(row["mid_high"]),
                    "low": float(row["mid_low"]),
                    "close": float(row["mid_close"]),
                    "minutes": minute_counts.get(sd, 0),
                }
    finally:
        conn.close()
    return dict(sorted(daily.items()))


def aggregate_daily_from_minutes(minutes: dict[datetime, MinuteBar]) -> dict[date, dict]:
    grouped: dict[date, list[MinuteBar]] = defaultdict(list)
    for bar in minutes.values():
        sd = session_date_for_asx(bar.ts)
        if START_DATE <= sd <= END_DATE:
            grouped[sd].append(bar)
    daily = {}
    for sd, rows in grouped.items():
        rows.sort(key=lambda x: x.ts)
        daily[sd] = {
            "session_date": sd,
            "open": rows[0].open,
            "high": max(r.high for r in rows),
            "low": min(r.low for r in rows),
            "close": rows[-1].close,
            "minutes": len(rows),
        }
    return dict(sorted(daily.items()))


async def load_spi_data() -> tuple[dict[datetime, MinuteBar], dict[date, dict], list[dict]]:
    conn = await asyncpg.connect(DB_URL)
    try:
        minute_rows = await conn.fetch(
            """
            SELECT time, open, high, low, close, contract_month, local_symbol,
                   roll_event_id, adjustment_value, adjustment_method
            FROM continuous_futures_minute_asof_adjusted(
                'SPI',
                $1::timestamptz,
                $2::timestamptz,
                $2::timestamptz,
                'back_adjusted'
            )
            ORDER BY time
            """,
            START,
            END,
        )
        daily_rows = await conn.fetch(
            """
            SELECT session_date, open, high, low, close, volume, bar_count,
                   contract_month, local_symbol, roll_event_id,
                   adjustment_value, adjustment_method
            FROM continuous_futures_daily_asof(
                'SPI',
                $1::date,
                $2::date,
                'back_adjusted'
            )
            ORDER BY session_date
            """,
            START_DATE,
            END_DATE,
        )
        roll_rows = await conn.fetch(
            """
            SELECT from_local_symbol, to_local_symbol, decision_session_date,
                   price_session_date, effective_roll_time, price_gap, ratio,
                   roll_rule
            FROM futures_roll_events_asof
            WHERE symbol = 'SPI'
              AND effective_roll_time >= $1::timestamptz
              AND effective_roll_time < $2::timestamptz
            ORDER BY effective_roll_time
            """,
            START,
            END,
        )
    finally:
        await conn.close()

    minutes = {
        r["time"]: MinuteBar(
            ts=r["time"],
            open=float(r["open"]),
            high=float(r["high"]),
            low=float(r["low"]),
            close=float(r["close"]),
        )
        for r in minute_rows
    }
    daily = {
        r["session_date"]: {
            "session_date": r["session_date"],
            "open": float(r["open"]),
            "high": float(r["high"]),
            "low": float(r["low"]),
            "close": float(r["close"]),
            "volume": int(r["volume"] or 0),
            "bar_count": int(r["bar_count"] or 0),
            "local_symbol": r["local_symbol"],
            "contract_month": r["contract_month"],
            "adjustment_value": float(r["adjustment_value"] or 0),
            "adjustment_method": r["adjustment_method"],
        }
        for r in daily_rows
    }
    rolls = [dict(r) for r in roll_rows]
    return dict(sorted(minutes.items())), dict(sorted(daily.items())), rolls


def pctile(values: list[float], p: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    idx = (len(ordered) - 1) * p
    lo = math.floor(idx)
    hi = math.ceil(idx)
    if lo == hi:
        return ordered[lo]
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (idx - lo)


def stats(values: list[float]) -> dict[str, float]:
    if not values:
        return {k: float("nan") for k in ["mean", "median", "min", "max", "std", "p05", "p95", "mae"]}
    return {
        "mean": statistics.fmean(values),
        "median": statistics.median(values),
        "min": min(values),
        "max": max(values),
        "std": statistics.pstdev(values) if len(values) > 1 else 0.0,
        "p05": pctile(values, 0.05),
        "p95": pctile(values, 0.95),
        "mae": statistics.fmean(abs(v) for v in values),
    }


def ensure_out() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)


def write_csv(path: Path, rows: list[dict], fields: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def svg_line_chart(path: Path, title: str, series: list[tuple[str, list[tuple[object, float]], str]], ylabel: str) -> None:
    width, height = 1000, 460
    ml, mr, mt, mb = 76, 28, 54, 58
    xs = []
    ys = []
    for _, pts, _ in series:
        xs.extend(x for x, _ in pts)
        ys.extend(y for _, y in pts if math.isfinite(y))
    if not xs or not ys:
        return
    def xnum(x: object) -> float:
        if isinstance(x, datetime):
            return x.timestamp()
        if isinstance(x, date):
            return datetime.combine(x, time.min, timezone.utc).timestamp()
        return float(x)
    xmin, xmax = min(xnum(x) for x in xs), max(xnum(x) for x in xs)
    ymin, ymax = min(ys), max(ys)
    pad = (ymax - ymin) * 0.08 or 1
    ymin -= pad
    ymax += pad
    def sx(x: object) -> float:
        return ml + (xnum(x) - xmin) / (xmax - xmin or 1) * (width - ml - mr)
    def sy(y: float) -> float:
        return mt + (ymax - y) / (ymax - ymin or 1) * (height - mt - mb)
    grid = []
    for i in range(6):
        y = mt + i * (height - mt - mb) / 5
        val = ymax - i * (ymax - ymin) / 5
        grid.append(f'<line x1="{ml}" y1="{y:.1f}" x2="{width-mr}" y2="{y:.1f}" stroke="#e5e7eb"/>')
        grid.append(f'<text x="{ml-10}" y="{y+4:.1f}" font-size="12" text-anchor="end" fill="#4b5563">{val:.1f}</text>')
    paths = []
    legend = []
    for idx, (name, pts, color) in enumerate(series):
        d = " ".join(("M" if i == 0 else "L") + f"{sx(x):.1f},{sy(y):.1f}" for i, (x, y) in enumerate(pts) if math.isfinite(y))
        paths.append(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="2"/>')
        lx = ml + idx * 210
        legend.append(f'<line x1="{lx}" y1="{height-24}" x2="{lx+28}" y2="{height-24}" stroke="{color}" stroke-width="3"/>')
        legend.append(f'<text x="{lx+36}" y="{height-20}" font-size="13" fill="#111827">{name}</text>')
    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<rect width="100%" height="100%" fill="white"/>
<text x="{width/2}" y="28" font-size="20" text-anchor="middle" font-weight="700" fill="#111827">{title}</text>
<text x="22" y="{height/2}" font-size="13" text-anchor="middle" fill="#374151" transform="rotate(-90 22 {height/2})">{ylabel}</text>
{''.join(grid)}
<line x1="{ml}" y1="{height-mb}" x2="{width-mr}" y2="{height-mb}" stroke="#9ca3af"/>
<line x1="{ml}" y1="{mt}" x2="{ml}" y2="{height-mb}" stroke="#9ca3af"/>
{''.join(paths)}
{''.join(legend)}
</svg>'''
    path.write_text(svg, encoding="utf-8")


def svg_histogram(path: Path, title: str, values: list[float]) -> None:
    width, height = 900, 420
    ml, mr, mt, mb = 72, 28, 54, 54
    if not values:
        return
    lo, hi = pctile(values, 0.01), pctile(values, 0.99)
    if lo == hi:
        lo, hi = min(values), max(values)
    bins = 30
    counts = [0] * bins
    for v in values:
        vv = min(max(v, lo), hi)
        idx = min(bins - 1, int((vv - lo) / (hi - lo or 1) * bins))
        counts[idx] += 1
    maxc = max(counts) or 1
    bw = (width - ml - mr) / bins
    bars = []
    for i, c in enumerate(counts):
        x = ml + i * bw + 1
        h = c / maxc * (height - mt - mb)
        y = height - mb - h
        bars.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bw-2:.1f}" height="{h:.1f}" fill="#2563eb" opacity="0.82"/>')
    labels = []
    for i in range(6):
        x = ml + i * (width - ml - mr) / 5
        val = lo + i * (hi - lo) / 5
        labels.append(f'<text x="{x:.1f}" y="{height-24}" font-size="12" text-anchor="middle" fill="#4b5563">{val:.1f}</text>')
    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">
<rect width="100%" height="100%" fill="white"/>
<text x="{width/2}" y="28" font-size="20" text-anchor="middle" font-weight="700" fill="#111827">{title}</text>
<line x1="{ml}" y1="{height-mb}" x2="{width-mr}" y2="{height-mb}" stroke="#9ca3af"/>
<line x1="{ml}" y1="{mt}" x2="{ml}" y2="{height-mb}" stroke="#9ca3af"/>
{''.join(bars)}
{''.join(labels)}
<text x="{width/2}" y="{height-6}" font-size="13" text-anchor="middle" fill="#374151">SPI close - CFD mid close</text>
</svg>'''
    path.write_text(svg, encoding="utf-8")


def sample_points(points: list[tuple[datetime, float]], max_points: int = 1200) -> list[tuple[datetime, float]]:
    if len(points) <= max_points:
        return points
    step = math.ceil(len(points) / max_points)
    return points[::step]


def fmt(x: float, digits: int = 2) -> str:
    if x != x:
        return "N/A"
    return f"{x:.{digits}f}"


async def main() -> None:
    ensure_out()
    cfd_minutes = load_cfd_minutes_from_db()
    cfd_daily = load_cfd_daily_from_db()
    cfd_daily_from_minutes = aggregate_daily_from_minutes(cfd_minutes)
    spi_minutes, spi_daily, rolls = await load_spi_data()

    common_minute_keys = sorted(set(spi_minutes) & set(cfd_minutes))
    minute_rows = []
    minute_diffs = []
    minute_spreads = []
    for ts in common_minute_keys:
        s = spi_minutes[ts]
        c = cfd_minutes[ts]
        diff = s.close - c.close
        minute_diffs.append(diff)
        if c.bid_close is not None and c.ofr_close is not None:
            minute_spreads.append(c.ofr_close - c.bid_close)
        minute_rows.append({
            "time_utc": ts.isoformat(),
            "spi_close": f"{s.close:.6f}",
            "cfd_mid_close": f"{c.close:.6f}",
            "diff": f"{diff:.6f}",
            "cfd_spread": "" if c.bid_close is None or c.ofr_close is None else f"{c.ofr_close - c.bid_close:.6f}",
        })

    common_daily_keys = sorted(set(spi_daily) & set(cfd_daily))
    daily_rows = []
    daily_diffs = []
    daily_range_diffs = []
    for sd in common_daily_keys:
        s = spi_daily[sd]
        c = cfd_daily[sd]
        diff = s["close"] - c["close"]
        daily_diffs.append(diff)
        range_diff = (s["high"] - s["low"]) - (c["high"] - c["low"])
        daily_range_diffs.append(range_diff)
        daily_rows.append({
            "session_date": sd.isoformat(),
            "spi_open": f"{s['open']:.6f}",
            "spi_high": f"{s['high']:.6f}",
            "spi_low": f"{s['low']:.6f}",
            "spi_close": f"{s['close']:.6f}",
            "spi_contract": s["local_symbol"],
            "spi_adjustment": f"{s['adjustment_value']:.6f}",
            "cfd_open": f"{c['open']:.6f}",
            "cfd_high": f"{c['high']:.6f}",
            "cfd_low": f"{c['low']:.6f}",
            "cfd_close": f"{c['close']:.6f}",
            "cfd_minutes": c["minutes"],
            "close_diff": f"{diff:.6f}",
            "range_diff": f"{range_diff:.6f}",
        })

    write_csv(
        OUT_DIR / "daily_comparison.csv",
        daily_rows,
        [
            "session_date", "spi_open", "spi_high", "spi_low", "spi_close",
            "spi_contract", "spi_adjustment", "cfd_open", "cfd_high",
            "cfd_low", "cfd_close", "cfd_minutes", "close_diff", "range_diff",
        ],
    )
    write_csv(
        OUT_DIR / "minute_comparison_sample.csv",
        minute_rows[:: max(1, len(minute_rows) // 5000)],
        ["time_utc", "spi_close", "cfd_mid_close", "diff", "cfd_spread"],
    )

    daily_diff_stats = stats(daily_diffs)
    minute_diff_stats = stats(minute_diffs)
    spread_stats = stats(minute_spreads)

    spi_daily_close = [(sd, spi_daily[sd]["close"]) for sd in common_daily_keys]
    cfd_daily_close = [(sd, cfd_daily[sd]["close"]) for sd in common_daily_keys]
    daily_diff_series = [(sd, spi_daily[sd]["close"] - cfd_daily[sd]["close"]) for sd in common_daily_keys]
    minute_diff_series = sample_points([(ts, spi_minutes[ts].close - cfd_minutes[ts].close) for ts in common_minute_keys])

    svg_line_chart(
        OUT_DIR / "daily_close_overlay.svg",
        "SPI 连续合约 vs ASX200 CFD：日K收盘价",
        [("SPI back-adjusted close", spi_daily_close, "#1d4ed8"), ("CFD mid close", cfd_daily_close, "#dc2626")],
        "价格",
    )
    svg_line_chart(
        OUT_DIR / "daily_close_diff.svg",
        "日K收盘价差：SPI - CFD",
        [("close diff", daily_diff_series, "#7c3aed")],
        "点数",
    )
    svg_line_chart(
        OUT_DIR / "minute_close_diff_sample.svg",
        "1分钟收盘价差抽样：SPI - CFD",
        [("minute diff sampled", minute_diff_series, "#059669")],
        "点数",
    )
    svg_histogram(OUT_DIR / "minute_diff_histogram.svg", "1分钟价差分布", minute_diffs)

    roll_md = "\n".join(
        f"| {r['from_local_symbol']} -> {r['to_local_symbol']} | {r['effective_roll_time']} | {float(r['price_gap']):.2f} | {float(r['ratio']):.8f} | {r['roll_rule']} |"
        for r in rolls
    ) or "| 无 | 无 | 无 | 无 | 无 |"

    largest_daily = sorted(daily_rows, key=lambda r: abs(float(r["close_diff"])), reverse=True)[:10]
    largest_daily_md = "\n".join(
        f"| {r['session_date']} | {float(r['spi_close']):.1f} | {float(r['cfd_close']):.1f} | {float(r['close_diff']):.1f} | {r['spi_contract']} | {r['cfd_minutes']} |"
        for r in largest_daily
    )

    report = f"""# SPI 连续合约与 ASX200 CFD 数据对比分析报告

## 结论摘要

本报告比较 `2026-01-01` 到 `2026-03-31` 期间两组数据：

- **SPI**：`ibkrData` 合成的 SPI as-of 连续期货，使用 `back_adjusted` 调整。
- **ASX200 CFD**：`/home/yuanjs/projects/tradehistory/backend/databases/tradehistory.db` 中的 CFD 日K和 1 分钟数据，使用数据库内 `mid_*` 字段。

核心结论：

1. 两组数据走势高度一致，但价格层级存在稳定偏差。日K close 差值 `SPI - CFD` 的均值为 **{fmt(daily_diff_stats['mean'])} 点**，中位数为 **{fmt(daily_diff_stats['median'])} 点**。
2. 1分钟级别可精确对齐的 bar 数为 **{len(common_minute_keys):,}**，分钟 close 差值均值为 **{fmt(minute_diff_stats['mean'])} 点**，平均绝对差为 **{fmt(minute_diff_stats['mae'])} 点**。
3. 2026-03-17 发生一次 SPI 换月：`APH6 -> APM6`，gap 为 **46 点**。由于 SPI 使用 back-adjusted，换月前的期货历史价格会整体加上该 gap；CFD 数据没有这种显式换月调整。
4. CFD 分钟数据存在 bid/offer spread。样本期内 CFD 1分钟 close spread 均值约为 **{fmt(spread_stats['mean'])} 点**，这会让使用 bid、offer 或 mid 的对比结果不同。

## 数据口径

### SPI 连续合约

- 日K来源：`continuous_futures_daily_asof('SPI', '2026-01-01', '2026-03-31', 'back_adjusted')`
- 1分钟来源：`continuous_futures_minute_asof_adjusted('SPI', '2026-01-01', '2026-04-01', '2026-04-01', 'back_adjusted')`
- 时区：数据库时间为 UTC；SPI session boundary 按 `Australia/Sydney 17:10` 计算并转换为 UTC。

### ASX200 CFD

- 日K来源：`tradehistory/backend/databases/tradehistory.db.candles_daily`
- 1分钟来源：`tradehistory/backend/databases/tradehistory.db.candles_1m`
- CFD 价格：使用数据库内 `mid_open/mid_high/mid_low/mid_close` 字段；spread 由 `ofr_close - bid_close` 统计。
- CFD 日K date：直接使用 `candles_daily.date`；分钟统计中的 session date 按 `Australia/Sydney 17:10` 计算，用于估算该日的分钟覆盖数量。

## 数据覆盖

| 项目 | SPI | CFD | 共同样本 |
|---|---:|---:|---:|
| 日K session 数 | {len(spi_daily):,} | {len(cfd_daily):,} | {len(common_daily_keys):,} |
| 1分钟 bar 数 | {len(spi_minutes):,} | {len(cfd_minutes):,} | {len(common_minute_keys):,} |

## 图表

### 日K收盘价对比

![日K收盘价对比](daily_close_overlay.svg)

### 日K收盘价差

![日K收盘价差](daily_close_diff.svg)

### 1分钟收盘价差抽样

![1分钟收盘价差抽样](minute_close_diff_sample.svg)

### 1分钟价差分布

![1分钟价差分布](minute_diff_histogram.svg)

## 统计摘要

### 日K close 差值：SPI - CFD

| 指标 | 点数 |
|---|---:|
| 样本数 | {len(daily_diffs):,} |
| 均值 | {fmt(daily_diff_stats['mean'])} |
| 中位数 | {fmt(daily_diff_stats['median'])} |
| 平均绝对差 | {fmt(daily_diff_stats['mae'])} |
| 标准差 | {fmt(daily_diff_stats['std'])} |
| 最小值 | {fmt(daily_diff_stats['min'])} |
| 5%分位 | {fmt(daily_diff_stats['p05'])} |
| 95%分位 | {fmt(daily_diff_stats['p95'])} |
| 最大值 | {fmt(daily_diff_stats['max'])} |

### 1分钟 close 差值：SPI - CFD

| 指标 | 点数 |
|---|---:|
| 样本数 | {len(minute_diffs):,} |
| 均值 | {fmt(minute_diff_stats['mean'])} |
| 中位数 | {fmt(minute_diff_stats['median'])} |
| 平均绝对差 | {fmt(minute_diff_stats['mae'])} |
| 标准差 | {fmt(minute_diff_stats['std'])} |
| 最小值 | {fmt(minute_diff_stats['min'])} |
| 5%分位 | {fmt(minute_diff_stats['p05'])} |
| 95%分位 | {fmt(minute_diff_stats['p95'])} |
| 最大值 | {fmt(minute_diff_stats['max'])} |

### CFD bid/offer spread

| 指标 | 点数 |
|---|---:|
| 样本数 | {len(minute_spreads):,} |
| 均值 | {fmt(spread_stats['mean'])} |
| 中位数 | {fmt(spread_stats['median'])} |
| 95%分位 | {fmt(spread_stats['p95'])} |
| 最大值 | {fmt(spread_stats['max'])} |

## SPI 换月事件

| 合约切换 | effective_roll_time | gap | ratio | rule |
|---|---|---:|---:|---|
{roll_md}

## 日K差异最大的日期

| session_date | SPI close | CFD close | SPI-CFD | SPI合约 | CFD分钟数 |
|---|---:|---:|---:|---|---:|
{largest_daily_md}

## 差异来源分析

1. **标的不同**：SPI 是交易所期货，ASX200 CFD 是券商报价产品。CFD 通常会跟随指数/期货价格，但包含券商定价、spread、融资和交易时段处理差异。
2. **换月调整不同**：SPI 连续合约在 2026-03-17 发生 `APH6 -> APM6` 换月，back-adjusted 会把已知 gap 加到更早的历史价格上。CFD 没有期货合约换月链条，因此不会出现同样的历史回调。
3. **日切规则敏感**：ASX200 的交易 session 跨 UTC 日期。如果按 UTC 日历聚合，日K close 和 volume 会错位。本报告的 CFD 日K直接采用 `candles_daily.date`，分钟覆盖统计才按 `Australia/Sydney 17:10` 估算。
4. **bid/offer 与 mid 选择**：CFD 文件同时有 BID/OFR。本报告用 mid 作为中性价格；如果策略实盘用买价或卖价，和 SPI 的差值会系统性偏移约半个 spread。
5. **缺失分钟与交易时段差异**：共同分钟样本只统计两边都有 bar 的 UTC 分钟。任一方在休市、维护、缺数据时都会被排除；日K对比则直接使用两边各自的日K口径。

## 对回测的建议

1. 如果目标是用 SPI 替代 ASX200 CFD 回测，应优先验证信号对 **相对变化** 的敏感性，而不是只比较绝对价格。
2. 对使用 KDJ、均线、突破等指标的策略，建议同时跑：
   - SPI `back_adjusted`
   - SPI `ratio_adjusted`
   - CFD mid
3. 对涉及止损、止盈、固定点差阈值的逻辑，需要单独校正 SPI 与 CFD 的平均价格偏移和 spread。
4. 如果未来要更贴近 CFD 实盘，应考虑在 SPI 回测成交价格上叠加 CFD spread/slippage 模型。

## 输出文件

- `daily_comparison.csv`：逐 session 日K对比。
- `minute_comparison_sample.csv`：1分钟对齐样本抽样。
- `daily_close_overlay.svg`：日K close 走势对比。
- `daily_close_diff.svg`：日K close 差值。
- `minute_close_diff_sample.svg`：分钟 close 差值抽样。
- `minute_diff_histogram.svg`：分钟 close 差值分布。
"""
    (OUT_DIR / "report.md").write_text(report, encoding="utf-8")
    print(f"report={OUT_DIR / 'report.md'}")
    print(f"daily_common={len(common_daily_keys)} minute_common={len(common_minute_keys)}")
    print(f"daily_mean_diff={daily_diff_stats['mean']:.4f} minute_mean_diff={minute_diff_stats['mean']:.4f}")


if __name__ == "__main__":
    asyncio.run(main())
