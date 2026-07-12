#!/usr/bin/env python3
"""Monitor whether first minute-complete closes match final minute closes.

The collector publishes futures minute bars in two phases:
1. a quick provisional revision after FUTURES_MINUTE_COMPLETE_DELAY_SECONDS
2. a later final revision after FUTURES_MINUTE_COMPLETE_FINAL_DELAY_SECONDS

This script records the first message seen for each minute and compares it with
the final websocket message and the database value after a settle window.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import signal
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import asyncpg
import redis.asyncio as redis
from dotenv import load_dotenv


DEFAULT_CLIENTS = {
    "spxclient": "MES",
    "dowclient": "MYM",
}


@dataclass
class MinuteRecord:
    client: str
    symbol: str
    con_id: int | None
    bar_start: datetime
    bar_end: datetime
    first_received_at: datetime
    first_delay_seconds: float
    first_close: float
    first_revision: int | None
    first_status: str | None
    first_final: bool
    updates: list[dict[str, Any]] = field(default_factory=list)
    final_received_at: datetime | None = None
    final_delay_seconds: float | None = None
    final_close: float | None = None
    final_revision: int | None = None
    db_close: float | None = None
    db_checked_at: datetime | None = None

    @property
    def key(self) -> tuple[str, int | None, datetime]:
        return (self.symbol, self.con_id, self.bar_start)

    @property
    def first_matches_final(self) -> bool | None:
        if self.final_close is None:
            return None
        return self.first_close == self.final_close

    @property
    def first_matches_db(self) -> bool | None:
        if self.db_close is None:
            return None
        return self.first_close == self.db_close

    @property
    def final_matches_db(self) -> bool | None:
        if self.db_close is None or self.final_close is None:
            return None
        return self.final_close == self.db_close


def parse_utc_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value)
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def as_float(value: Any) -> float:
    if isinstance(value, Decimal):
        return float(value)
    return float(value)


def parse_clients(values: list[str]) -> dict[str, str]:
    if not values:
        return dict(DEFAULT_CLIENTS)

    clients: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise argparse.ArgumentTypeError(
                f"Invalid --client {value!r}; expected name=SYMBOL"
            )
        name, symbol = value.split("=", 1)
        name = name.strip()
        symbol = symbol.strip().upper()
        if not name or not symbol:
            raise argparse.ArgumentTypeError(
                f"Invalid --client {value!r}; expected name=SYMBOL"
            )
        clients[name] = symbol
    return clients


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * pct
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    if lower == upper:
        return ordered[lower]
    fraction = index - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def build_summary(records: list[MinuteRecord], target_delay_seconds: float) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for client in sorted({record.client for record in records}):
        rows = [record for record in records if record.client == client]
        first_delays = [record.first_delay_seconds for record in rows]
        final_delays = [
            record.final_delay_seconds
            for record in rows
            if record.final_delay_seconds is not None
        ]
        wrong_first = [
            record
            for record in rows
            if record.first_matches_db is False
            or (record.first_matches_db is None and record.first_matches_final is False)
        ]
        missing_final = [record for record in rows if record.final_close is None]
        missing_db = [record for record in rows if record.db_close is None]
        late_first = [
            record for record in rows if record.first_delay_seconds > target_delay_seconds
        ]

        summary[client] = {
            "symbol": rows[0].symbol if rows else None,
            "bars_seen": len(rows),
            "first_close_wrong_count": len(wrong_first),
            "first_arrived_after_target_count": len(late_first),
            "missing_final_count": len(missing_final),
            "missing_db_count": len(missing_db),
            "first_delay_seconds": {
                "min": min(first_delays) if first_delays else None,
                "p50": percentile(first_delays, 0.50),
                "p95": percentile(first_delays, 0.95),
                "max": max(first_delays) if first_delays else None,
            },
            "final_delay_seconds": {
                "min": min(final_delays) if final_delays else None,
                "p50": percentile(final_delays, 0.50),
                "p95": percentile(final_delays, 0.95),
                "max": max(final_delays) if final_delays else None,
            },
            "wrong_first_bars": [
                {
                    "bar_start": record.bar_start.isoformat(),
                    "first_close": record.first_close,
                    "final_close": record.final_close,
                    "db_close": record.db_close,
                    "first_delay_seconds": record.first_delay_seconds,
                    "final_delay_seconds": record.final_delay_seconds,
                    "updates": len(record.updates),
                }
                for record in wrong_first
            ],
            "late_first_bars": [
                {
                    "bar_start": record.bar_start.isoformat(),
                    "first_delay_seconds": record.first_delay_seconds,
                    "first_close": record.first_close,
                    "final_close": record.final_close,
                    "db_close": record.db_close,
                }
                for record in late_first
            ],
        }
    return summary


async def fetch_db_closes(
    db_url: str,
    records: list[MinuteRecord],
    connect_timeout_seconds: float,
) -> None:
    if not records:
        return

    pool = await asyncio.wait_for(
        asyncpg.create_pool(db_url, min_size=1, max_size=2, timeout=connect_timeout_seconds),
        timeout=connect_timeout_seconds + 1,
    )
    try:
        async with pool.acquire() as conn:
            for record in records:
                row = await conn.fetchrow(
                    """
                    SELECT close
                    FROM futures_minute_bars
                    WHERE symbol = $1
                      AND time = $2
                      AND ($3::bigint IS NULL OR con_id = $3::bigint)
                    ORDER BY time DESC
                    LIMIT 1
                    """,
                    record.symbol,
                    record.bar_start,
                    record.con_id,
                )
                record.db_checked_at = datetime.now(timezone.utc)
                if row is not None and row["close"] is not None:
                    record.db_close = as_float(row["close"])
    finally:
        await pool.close()


def record_to_row(record: MinuteRecord) -> dict[str, Any]:
    return {
        "client": record.client,
        "symbol": record.symbol,
        "con_id": record.con_id,
        "bar_start": record.bar_start.isoformat(),
        "bar_end": record.bar_end.isoformat(),
        "first_received_at": record.first_received_at.isoformat(),
        "first_delay_seconds": record.first_delay_seconds,
        "first_close": record.first_close,
        "first_revision": record.first_revision,
        "first_status": record.first_status,
        "first_final": record.first_final,
        "final_received_at": (
            record.final_received_at.isoformat()
            if record.final_received_at is not None
            else None
        ),
        "final_delay_seconds": record.final_delay_seconds,
        "final_close": record.final_close,
        "final_revision": record.final_revision,
        "db_checked_at": (
            record.db_checked_at.isoformat()
            if record.db_checked_at is not None
            else None
        ),
        "db_close": record.db_close,
        "first_matches_final": record.first_matches_final,
        "first_matches_db": record.first_matches_db,
        "final_matches_db": record.final_matches_db,
        "update_count": len(record.updates),
    }


def write_reports(
    output_dir: Path,
    started_at: datetime,
    finished_at: datetime,
    clients: dict[str, str],
    records: list[MinuteRecord],
    target_delay_seconds: float,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    stamp = started_at.strftime("%Y%m%dT%H%M%SZ")
    json_path = output_dir / f"minute_close_latency_{stamp}.json"
    csv_path = output_dir / f"minute_close_latency_{stamp}.csv"

    rows = [record_to_row(record) for record in sorted(
        records,
        key=lambda item: (item.client, item.bar_start, item.con_id or 0),
    )]
    payload = {
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "clients": clients,
        "target_delay_seconds": target_delay_seconds,
        "summary": build_summary(records, target_delay_seconds),
        "records": rows,
    }
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True))

    with csv_path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(record_to_row(records[0]).keys()) if records else [
            "client", "symbol", "con_id", "bar_start", "bar_end",
            "first_received_at", "first_delay_seconds", "first_close",
            "first_revision", "first_status", "first_final",
            "final_received_at", "final_delay_seconds", "final_close",
            "final_revision", "db_checked_at", "db_close",
            "first_matches_final", "first_matches_db", "final_matches_db",
            "update_count",
        ])
        writer.writeheader()
        writer.writerows(rows)

    return json_path, csv_path


async def monitor(args: argparse.Namespace) -> int:
    load_dotenv(args.env_file)

    clients = parse_clients(args.client)
    symbol_to_client = {symbol: client for client, symbol in clients.items()}
    symbols = set(symbol_to_client)
    db_url = args.db_url or os.getenv("DB_URL")
    redis_url = args.redis_url or os.getenv("REDIS_URL", "redis://localhost:6379")

    if not db_url:
        raise SystemExit("DB_URL is required via --db-url or .env")

    print(
        json.dumps({
            "event": "starting",
            "duration_seconds": args.duration_seconds,
            "settle_seconds": args.settle_seconds,
            "clients": clients,
            "redis_url": redis_url,
            "db_url_set": True,
        }),
        file=sys.stderr,
        flush=True,
    )

    started_at = datetime.now(timezone.utc)
    stop_at = asyncio.get_running_loop().time() + args.duration_seconds
    records: dict[tuple[str, int | None, datetime], MinuteRecord] = {}

    redis_client = redis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=args.connect_timeout_seconds,
        socket_timeout=args.connect_timeout_seconds,
    )
    pubsub = redis_client.pubsub()
    await asyncio.wait_for(
        pubsub.psubscribe("futures:minute-complete:*"),
        timeout=args.connect_timeout_seconds + 1,
    )

    stop_event = asyncio.Event()

    def request_stop() -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, request_stop)
        except NotImplementedError:
            pass

    try:
        while not stop_event.is_set() and loop.time() < stop_at:
            remaining = max(0.0, stop_at - loop.time())
            timeout = min(1.0, remaining)
            if timeout <= 0:
                break
            try:
                message = await asyncio.wait_for(
                    pubsub.get_message(
                        ignore_subscribe_messages=True,
                        timeout=timeout,
                    ),
                    timeout=timeout + 0.5,
                )
            except asyncio.TimeoutError:
                continue
            if not message:
                continue

            received_at = datetime.now(timezone.utc)
            try:
                payload = json.loads(message["data"])
            except (TypeError, json.JSONDecodeError):
                continue

            symbol = str(payload.get("symbol") or "").upper()
            if symbol not in symbols:
                continue

            close = payload.get("close")
            if close is None:
                continue

            bar_start_value = payload.get("bar_start") or payload.get("time")
            if not bar_start_value:
                continue

            bar_start = parse_utc_datetime(bar_start_value)
            bar_end = parse_utc_datetime(payload.get("bar_end")) if payload.get("bar_end") else bar_start.replace(second=59, microsecond=999000)
            con_id = payload.get("con_id")
            con_id = int(con_id) if con_id is not None else None
            key = (symbol, con_id, bar_start)
            client = symbol_to_client[symbol]
            close_float = as_float(close)
            revision = payload.get("revision")
            revision = int(revision) if revision is not None else None
            status = payload.get("status")
            final = payload.get("final") is True
            delay_seconds = (received_at - bar_end).total_seconds()

            if key not in records:
                records[key] = MinuteRecord(
                    client=client,
                    symbol=symbol,
                    con_id=con_id,
                    bar_start=bar_start,
                    bar_end=bar_end,
                    first_received_at=received_at,
                    first_delay_seconds=delay_seconds,
                    first_close=close_float,
                    first_revision=revision,
                    first_status=status,
                    first_final=final,
                )

            record = records[key]
            record.updates.append({
                "received_at": received_at.isoformat(),
                "delay_seconds": delay_seconds,
                "close": close_float,
                "revision": revision,
                "status": status,
                "final": final,
            })

            if final:
                record.final_received_at = received_at
                record.final_delay_seconds = delay_seconds
                record.final_close = close_float
                record.final_revision = revision

        if args.settle_seconds > 0:
            await asyncio.sleep(args.settle_seconds)

        try:
            await fetch_db_closes(
                db_url,
                list(records.values()),
                args.connect_timeout_seconds,
            )
        except Exception as exc:
            print(
                json.dumps({
                    "event": "db_check_failed",
                    "error": str(exc),
                }),
                file=sys.stderr,
                flush=True,
            )
    finally:
        try:
            await asyncio.wait_for(
                pubsub.punsubscribe("futures:minute-complete:*"),
                timeout=2,
            )
        except Exception:
            pass
        await pubsub.aclose()
        await redis_client.aclose()

    finished_at = datetime.now(timezone.utc)
    record_list = list(records.values())
    json_path, csv_path = write_reports(
        Path(args.output_dir),
        started_at,
        finished_at,
        clients,
        record_list,
        args.target_delay_seconds,
    )

    summary = build_summary(record_list, args.target_delay_seconds)
    print(json.dumps({
        "json_report": str(json_path),
        "csv_report": str(csv_path),
        "summary": summary,
    }, indent=2, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Monitor 1m close correctness for futures minute-complete clients.",
    )
    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=3600,
        help="How long to listen for minute-complete messages.",
    )
    parser.add_argument(
        "--settle-seconds",
        type=int,
        default=90,
        help="Extra wait before checking DB closes for the last observed bars.",
    )
    parser.add_argument(
        "--target-delay-seconds",
        type=float,
        default=5.5,
        help="First message delay threshold. 5.5 allows scheduler/network jitter around a 5s grace window.",
    )
    parser.add_argument(
        "--client",
        action="append",
        default=[],
        help="Client to symbol mapping, e.g. --client spxclient=MES --client dowclient=MYM.",
    )
    parser.add_argument("--db-url", default=None)
    parser.add_argument("--redis-url", default=None)
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--output-dir", default="monitor_reports")
    parser.add_argument(
        "--connect-timeout-seconds",
        type=float,
        default=5.0,
        help="DB/Redis connection timeout.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return asyncio.run(monitor(args))


if __name__ == "__main__":
    raise SystemExit(main())
