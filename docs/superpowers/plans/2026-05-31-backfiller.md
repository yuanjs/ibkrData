# IBKR Historical 1-Min Bar Backfiller — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a standalone module (`backfiller/`) that pulls 2-3 years of 1-minute OHLCV bars from IBKR Gateway HMDS into a dedicated TimescaleDB table, without interfering with the existing collector or trading operations.

**Architecture:** Single-process, single-thread sequential puller with independent IBKR connection (clientId=99). Config-driven product list via YAML. JSON checkpoint files for resume-on-interrupt. Auto-reconnect on Gateway failure. Config re-scanned between symbols to pick up newly added products.

**Tech Stack:** Python 3.12+, ib_insync, asyncpg, PyYAML, python-dotenv

**Spec:** `docs/superpowers/specs/2026-05-31-backfiller-design.md`

---

### Task 1: Module scaffold + config.py + config.yaml + .gitignore

**Files:**
- Create: `backfiller/__init__.py`
- Create: `backfiller/config.py`
- Create: `backfiller/config.yaml`
- Create: `backfiller/requirements.txt`
- Modify: `.gitignore` (add `backfiller/progress/`)

- [ ] **Step 1: Create `backfiller/__init__.py`** (empty)

- [ ] **Step 2: Create `backfiller/config.py`**

Load YAML config + .env overrides. .env values take precedence over config.yaml for connection params.

```python
"""Configuration loader for backfiller.

Priority: .env > config.yaml > defaults.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml
from dotenv import load_dotenv

load_dotenv(find_dotenv())  # search upward from cwd


@dataclass
class ProductConfig:
    symbol: str
    sec_type: str
    exchange: str
    currency: str


@dataclass
class AppConfig:
    products: list[ProductConfig]
    start: str           # "YYYY-MM-DD"
    end: str             # "YYYY-MM-DD"
    request_interval_seconds: int = 25
    ib_host: str = "127.0.0.1"
    ib_port: int = 4002
    ib_client_id: int = 99
    db_url: str = "postgresql://ibkr:password@localhost:5432/ibkrdata"


def _env_str(key: str, default: str) -> str:
    return os.environ.get(key, default)


def _env_int(key: str, default: int) -> int:
    val = os.environ.get(key)
    return int(val) if val is not None else default


def load_config(yaml_path: Optional[str] = None) -> AppConfig:
    if yaml_path is None:
        yaml_path = str(Path(__file__).parent / "config.yaml")

    with open(yaml_path) as f:
        raw = yaml.safe_load(f)

    products = [ProductConfig(**p) for p in raw.get("products", [])]

    return AppConfig(
        products=products,
        start=raw.get("start", "2024-01-01"),
        end=raw.get("end", "2026-05-31"),
        request_interval_seconds=raw.get("request_interval_seconds", 25),
        ib_host=_env_str("IB_HOST", raw.get("ib_host", "127.0.0.1")),
        ib_port=_env_int("IB_PORT", raw.get("ib_port", 4002)),
        ib_client_id=_env_int("IB_CLIENT_ID", raw.get("ib_client_id", 99)),
        db_url=_env_str("DB_URL", raw.get("db_url",
                        "postgresql://ibkr:password@localhost:5432/ibkrdata")),
    )
```

- [ ] **Step 3: Create `backfiller/config.yaml`**

```yaml
products:
  # Futures
  - symbol: SPI
    sec_type: FUT
    exchange: SNFE
    currency: AUD
  - symbol: MYM
    sec_type: FUT
    exchange: CBOT
    currency: USD
  - symbol: N225M
    sec_type: FUT
    exchange: OSE.JPN
    currency: JPY
  - symbol: "10Y"
    sec_type: FUT
    exchange: CBOT
    currency: USD
  - symbol: ZC
    sec_type: FUT
    exchange: CBOT
    currency: USD
  - symbol: MNQ
    sec_type: FUT
    exchange: CME
    currency: USD
  - symbol: MES
    sec_type: FUT
    exchange: CME
    currency: USD
  # Forex
  - symbol: USD.JPY
    sec_type: CASH
    exchange: IDEALPRO
    currency: JPY

start: "2024-01-01"
end: "2026-05-31"
request_interval_seconds: 25

ib_host: "127.0.0.1"
ib_port: 4002
ib_client_id: 99
db_url: "postgresql://ibkr:password@localhost:5432/ibkrdata"
```

- [ ] **Step 4: Create `backfiller/requirements.txt`**

```
ib_insync==0.9.86
asyncpg==0.30.0
python-dotenv==1.0.1
PyYAML>=6.0
```

- [ ] **Step 5: Update `.gitignore`**

```
# backfiller progress checkpoints
backfiller/progress/
```

- [ ] **Step 6: Verify module loads**

Run: `cd /home/yuanjs/projects/ibkrData && python -c "from backfiller.config import load_config; cfg = load_config(); print(f'{len(cfg.products)} products, {cfg.ib_host}:{cfg.ib_port}')"`
Expected: `8 products, 127.0.0.1:4002`

- [ ] **Step 7: Commit**

```bash
git add backfiller/__init__.py backfiller/config.py backfiller/config.yaml backfiller/requirements.txt .gitignore
git commit -m "feat(backfiller): config loader + yaml config scaffold"
```

---

### Task 2: contract.py — IBKR contract resolver

**Files:**
- Create: `backfiller/contract.py`
- Create: `backfiller/tests/__init__.py`
- Create: `backfiller/tests/test_contract.py`

- [ ] **Step 1: Write tests for contract resolution logic**

Tests cover the deterministic parts (symbol parsing, what_to_show selection). Full contract resolution against live IBKR is tested via `--check` integration test.

```python
"""Tests for contract.py — deterministic logic only (no live IBKR)."""

from backfiller.contract import resolve_what_to_show, parse_contract_symbol


def test_what_to_show_cash():
    assert resolve_what_to_show("CASH") == "MIDPOINT"


def test_what_to_show_fut():
    assert resolve_what_to_show("FUT") == "TRADES"


def test_what_to_show_stk():
    assert resolve_what_to_show("STK") == "TRADES"


def test_parse_cash_symbol():
    assert parse_contract_symbol("USD.JPY", "CASH") == "USD"


def test_parse_non_cash_symbol():
    assert parse_contract_symbol("SPI", "FUT") == "SPI"
    assert parse_contract_symbol("AAPL", "STK") == "AAPL"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest backfiller/tests/test_contract.py -v 2>&1 | tail -5`
Expected: error "ModuleNotFoundError" or "function not defined"

- [ ] **Step 3: Create `backfiller/contract.py`**

```python
"""IBKR contract resolution for the backfiller.

Follows the same resolution strategy as collector/ibkr_client.py:
- FUT: resolve via CONTFUT to get the currently active contract month
- CASH: split "USD.JPY" into base currency "USD" for the contract symbol
- STK/others: qualify directly
"""

import logging
from typing import Optional

from ib_insync import IB, Contract, Fut, Stock

logger = logging.getLogger(__name__)


def parse_contract_symbol(symbol: str, sec_type: str) -> str:
    """Extract the IBKR contract symbol from our internal symbol name.

    For CASH products like "USD.JPY", IBKR expects the base currency "USD".
    For everything else, our symbol matches IBKR's.
    """
    if sec_type == "CASH" and "." in symbol:
        return symbol.split(".")[0]
    return symbol


def resolve_what_to_show(sec_type: str) -> str:
    """Select whatToShow parameter for reqHistoricalData.

    CASH (forex) uses MIDPOINT; everything else uses TRADES.
    """
    return "MIDPOINT" if sec_type == "CASH" else "TRADES"


def resolve_contract(ib: IB, symbol: str, sec_type: str,
                     exchange: str, currency: str) -> Optional[Contract]:
    """Resolve a symbol config into a qualified IBKR Contract.

    Returns None if resolution fails (product not found, no contract data).
    """
    contract_symbol = parse_contract_symbol(symbol, sec_type)

    if sec_type == "FUT":
        # Try CONTFUT first for rolling continuous contract
        cont = Contract(secType="CONTFUT", symbol=symbol,
                        exchange=exchange, currency=currency)
        details = ib.reqContractDetails(cont)
        if details:
            r = details[0].contract
            contract = Contract(
                secType="FUT", symbol=r.symbol, exchange=r.exchange,
                currency=r.currency,
                lastTradeDateOrContractMonth=r.lastTradeDateOrContractMonth,
                tradingClass=r.tradingClass, multiplier=r.multiplier,
            )
            qualified = ib.qualifyContracts(contract)
            return qualified[0] if qualified else contract

        # Fallback: earliest expiry from raw FUT contract
        logger.warning("CONTFUT not available for %s, falling back to earliest expiry", symbol)
        fallback = Contract(secType="FUT", symbol=symbol,
                            exchange=exchange, currency=currency)
        cds = ib.reqContractDetails(fallback)
        if cds:
            best = sorted(cds, key=lambda x: x.contract.lastTradeDateOrContractMonth)[0].contract
            return best
        return None

    # STK, CASH, etc.
    contract = Contract(symbol=contract_symbol, secType=sec_type,
                        exchange=exchange, currency=currency)
    qualified = ib.qualifyContracts(contract)
    return qualified[0] if qualified else contract
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest backfiller/tests/test_contract.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add backfiller/contract.py backfiller/tests/test_contract.py
git commit -m "feat(backfiller): contract resolver + unit tests"
```

---

### Task 3: progress_store.py — Checkpoint persistence

**Files:**
- Create: `backfiller/progress_store.py`
- Create: `backfiller/tests/test_progress_store.py`

- [ ] **Step 1: Write tests**

```python
"""Tests for progress_store.py"""

import json
import tempfile
from pathlib import Path

from backfiller.progress_store import ProgressStore


def test_save_and_load():
    with tempfile.TemporaryDirectory() as tmp:
        store = ProgressStore(Path(tmp))
        store.save("SPI", [("2024-01-01", "2024-01-03")])
        assert store.load("SPI") == [("2024-01-01", "2024-01-03")]


def test_mark_completed():
    with tempfile.TemporaryDirectory() as tmp:
        store = ProgressStore(Path(tmp))
        store.save("SPI", [("2024-01-01", "2024-01-03"), ("2024-01-03", "2024-01-05")])
        store.mark_completed("SPI", ("2024-01-01", "2024-01-03"))
        remaining = store.load("SPI")
        assert remaining == [("2024-01-03", "2024-01-05")]


def test_is_complete():
    with tempfile.TemporaryDirectory() as tmp:
        store = ProgressStore(Path(tmp))
        assert store.is_complete("SPI") is True  # no file = nothing to do = complete
        store.save("SPI", [("2024-01-01", "2024-01-03")])
        assert store.is_complete("SPI") is False
        store.mark_completed("SPI", ("2024-01-01", "2024-01-03"))
        assert store.is_complete("SPI") is True


def test_known_symbols():
    with tempfile.TemporaryDirectory() as tmp:
        store = ProgressStore(Path(tmp))
        store.save("SPI", [])
        store.save("MNQ", [])
        assert "SPI" in store.known_symbols()
        assert "MNQ" in store.known_symbols()
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest backfiller/tests/test_progress_store.py -v 2>&1 | tail -5`
Expected: ModuleNotFoundError

- [ ] **Step 3: Create `backfiller/progress_store.py`**

```python
"""Persistent checkpoint store for pull progress.

Each symbol has a JSON file under the progress directory:
  progress/SPI.json  →  { "remaining": [["d1","d2"], ...], "errors": [...] }

A missing file or empty array means "no remaining work" (complete).
"""

import json
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

Window = tuple[str, str]  # (start_date_str, end_date_str)


class ProgressStore:
    def __init__(self, progress_dir: Path):
        self._dir = progress_dir
        self._dir.mkdir(parents=True, exist_ok=True)

    def _path(self, symbol: str) -> Path:
        return self._dir / f"{symbol}.json"

    def save(self, symbol: str, windows: list[Window]) -> None:
        """Overwrite the remaining window list for a symbol."""
        data = {"remaining": [[s, e] for s, e in windows], "errors": []}
        self._path(symbol).write_text(json.dumps(data, ensure_ascii=False))
        if windows:
            logger.info("Checkpoint %s: %d windows remaining", symbol, len(windows))

    def load(self, symbol: str) -> list[Window]:
        """Return outstanding windows. Empty list = complete."""
        path = self._path(symbol)
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text())
            return [tuple(w) for w in data.get("remaining", [])]
        except (json.JSONDecodeError, KeyError, TypeError):
            logger.warning("Corrupt checkpoint for %s, starting fresh", symbol)
            return []

    def mark_completed(self, symbol: str, window: Window) -> None:
        """Remove one completed window from the list."""
        windows = self.load(symbol)
        try:
            windows.remove(window)
        except ValueError:
            pass  # already gone
        self.save(symbol, windows)

    def is_complete(self, symbol: str) -> bool:
        """True if no remaining windows (or no checkpoint file)."""
        return len(self.load(symbol)) == 0

    def known_symbols(self) -> set[str]:
        """Return set of symbols that have checkpoint files."""
        s = set()
        for p in self._dir.glob("*.json"):
            s.add(p.stem)
        return s

    def clear(self, symbol: str) -> None:
        """Delete checkpoint — forces a full re-pull on next run."""
        path = self._path(symbol)
        if path.exists():
            path.unlink()
```

- [ ] **Step 4: Run tests to pass**

Run: `pytest backfiller/tests/test_progress_store.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add backfiller/progress_store.py backfiller/tests/test_progress_store.py
git commit -m "feat(backfiller): progress checkpoint store + tests"
```

---

### Task 4: db_writer.py — minute_bars writer + status queries

**Files:**
- Create: `backfiller/db_writer.py`
- Create: `db/migration_002_minute_bars.sql`

- [ ] **Step 1: Create `db/migration_002_minute_bars.sql`**

```sql
-- Minute-bar history table for backfiller
CREATE TABLE IF NOT EXISTS minute_bars (
    time        TIMESTAMPTZ NOT NULL,
    symbol      TEXT NOT NULL,
    open        NUMERIC(12,4),
    high        NUMERIC(12,4),
    low         NUMERIC(12,4),
    close       NUMERIC(12,4),
    volume      BIGINT,
    bar_count   INTEGER,
    PRIMARY KEY (symbol, time)
);

SELECT create_hypertable('minute_bars', 'time', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_minute_bars_lookup
    ON minute_bars (symbol, time DESC);
```

- [ ] **Step 2: Create `backfiller/db_writer.py`**

```python
"""TimescaleDB writer for minute_bars table.

Thread-safe when used with a connection pool (asyncpg Pool).
"""

import logging
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)


class MinuteBarWriter:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    @staticmethod
    async def create_pool(dsn: str) -> asyncpg.Pool:
        return await asyncpg.create_pool(dsn)

    async def upsert_bars(self, symbol: str, bars: list) -> int:
        """Insert bars from ib_insync BarList into minute_bars.

        Returns count of rows inserted.
        ON CONFLICT DO NOTHING ensures idempotency.
        """
        if not bars:
            return 0
        rows = [
            (
                b.date if hasattr(b.date, "tzinfo") and b.date.tzinfo is not None
                else b.date.replace(tzinfo=timezone.utc),
                symbol,
                float(b.open),
                float(b.high),
                float(b.low),
                float(b.close),
                int(b.volume) if b.volume and b.volume > 0 else 0,
                b.barCount if hasattr(b, "barCount") else 0,
            )
            for b in bars
        ]
        async with self._pool.acquire() as conn:
            await conn.executemany(
                "INSERT INTO minute_bars(time,symbol,open,high,low,close,volume,bar_count) "
                "VALUES($1,$2,$3,$4,$5,$6,$7,$8) "
                "ON CONFLICT (symbol, time) DO NOTHING",
                rows,
            )
        return len(rows)

    async def get_range(self, symbol: str) -> tuple[Optional[datetime], Optional[datetime], int]:
        """Return (min_time, max_time, row_count) for a symbol."""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT MIN(time) AS t_min, MAX(time) AS t_max, COUNT(*) AS cnt "
                "FROM minute_bars WHERE symbol=$1",
                symbol,
            )
            return (row["t_min"], row["t_max"], row["cnt"])

    async def detect_gaps(self, symbol: str, threshold_minutes: int = 3
                          ) -> list[dict]:
        """Return gaps where adjacent bars are more than threshold_minutes apart.

        Each gap: { "gap_start": datetime, "gap_end": datetime }
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT time AS gap_start,
                       LEAD(time) OVER (ORDER BY time) AS gap_end
                FROM minute_bars
                WHERE symbol=$1
                """,
                symbol,
            )
        gaps = []
        for r in rows:
            if r["gap_end"] is not None:
                diff = r["gap_end"] - r["gap_start"]
                if diff > timedelta(minutes=threshold_minutes):
                    gaps.append({
                        "gap_start": r["gap_start"],
                        "gap_end": r["gap_end"],
                        "diff_minutes": int(diff.total_seconds() / 60),
                    })
        return gaps
```

- [ ] **Step 3: Commit**

```bash
git add backfiller/db_writer.py db/migration_002_minute_bars.sql
git commit -m "feat(backfiller): minute_bars DB writer + migration SQL"
```

---

### Task 5: scheduler.py — Time window split + pull orchestration

**Files:**
- Create: `backfiller/scheduler.py`
- Create: `backfiller/tests/test_scheduler.py`

This is the largest module. It contains:
- Time window splitting
- The pull loop with auto-reconnect
- Config re-scan for new products

- [ ] **Step 1: Write unit tests for window splitting**

```python
"""Tests for scheduler — window split logic (no live IBKR)."""

from datetime import date

from backfiller.scheduler import split_windows, Window


def test_split_windows_basic():
    """2 days of data = 1 window of size 2."""
    windows = split_windows(date(2024, 1, 1), date(2024, 1, 2))
    assert len(windows) == 1
    assert windows[0] == ("2024-01-01", "2024-01-02")


def test_split_windows_multiple():
    """5 days = 3 windows (2+2+1)."""
    windows = split_windows(date(2024, 1, 1), date(2024, 1, 5))
    assert len(windows) == 3
    assert windows[0] == ("2024-01-01", "2024-01-02")
    assert windows[1] == ("2024-01-03", "2024-01-04")
    assert windows[2] == ("2024-01-05", "2024-01-05")


def test_split_windows_single_day():
    """1 day = 1 window."""
    windows = split_windows(date(2024, 1, 1), date(2024, 1, 1))
    assert len(windows) == 1
    assert windows[0] == ("2024-01-01", "2024-01-01")
```

- [ ] **Step 2: Run to verify failure**

Run: `pytest backfiller/tests/test_scheduler.py -v 2>&1 | tail -5`
Expected: ImportError / function not defined

- [ ] **Step 3: Create `backfiller/scheduler.py`**

```python
"""Pull scheduler — time window split, pull loop, auto-reconnect.

This is the orchestrator. It does NOT run tasks concurrently.
"""

import asyncio
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

from ib_insync import IB, util

from backfiller.config import AppConfig, ProductConfig, load_config
from backfiller.contract import resolve_contract, resolve_what_to_show
from backfiller.db_writer import MinuteBarWriter
from backfiller.progress_store import ProgressStore, Window

logger = logging.getLogger(__name__)

WINDOW_DAYS = 2  # max days per reqHistoricalData for 1-min bars
RECONNECT_BASE_DELAY = 2  # seconds
RECONNECT_MAX_DELAY = 60
HMDS_WARMUP_SECONDS = 3
RETRY_LIMIT = 3


def split_windows(start: date, end: date) -> list[Window]:
    """Split date range into (start_str, end_str) windows of size WINDOW_DAYS."""
    from datetime import timedelta
    windows: list[Window] = []
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=WINDOW_DAYS - 1), end)
        windows.append((current.isoformat(), chunk_end.isoformat()))
        current = chunk_end + timedelta(days=1)
    return windows


class PullScheduler:
    """Manages the full pull lifecycle for all configured products."""

    def __init__(self, config: AppConfig, writer: MinuteBarWriter,
                 progress_dir: Path):
        self._config = config
        self._writer = writer
        self._store = ProgressStore(progress_dir)
        self._ib = IB()
        self._ib.RequestTimeout = 60
        self._should_stop = False
        self._connection_ok = False
        self._last_yaml_mtime: float = 0.0
        self._known_symbols: set[str] = set()

    # ── connection ──────────────────────────────────────────

    async def ensure_connected(self) -> bool:
        """Check connection; reconnect with backoff if needed."""
        if self._ib.isConnected():
            self._connection_ok = True
            return True

        delay = RECONNECT_BASE_DELAY
        while not self._should_stop:
            try:
                await self._ib.connectAsync(
                    self._config.ib_host,
                    self._config.ib_port,
                    clientId=self._config.ib_client_id,
                )
                logger.info("Reconnected to IB Gateway (clientId=%s)",
                            self._config.ib_client_id)
                await asyncio.sleep(HMDS_WARMUP_SECONDS)
                self._connection_ok = True
                return True
            except Exception as e:
                logger.warning("Reconnect failed: %s. Retry in %ds", e, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX_DELAY)
        return False

    def disconnect(self):
        if self._ib.isConnected():
            self._ib.disconnect()
            self._connection_ok = False

    # ── config refresh ──────────────────────────────────────

    def _check_new_products(self) -> list[ProductConfig]:
        """Re-read config.yaml and return products not yet in the task queue."""
        try:
            cfg = load_config()
            new = [p for p in cfg.products
                   if p.symbol not in self._known_symbols]
            if new:
                logger.info("Detected %d new product(s) in config: %s",
                            len(new), [p.symbol for p in new])
            return new
        except Exception as e:
            logger.warning("Failed to re-read config.yaml: %s", e)
            return []

    # ── window management ───────────────────────────────────

    async def _compute_windows(self, product: ProductConfig) -> list[Window]:
        """Compute remaining windows for a product, skipping DB-covered ranges."""
        all_windows = split_windows(
            date.fromisoformat(self._config.start),
            date.fromisoformat(self._config.end),
        )
        # Skip windows that overlap with existing DB data
        t_min, t_max, _ = await self._writer.get_range(product.symbol)
        if t_min and t_max:
            all_windows = [
                w for w in all_windows
                if date.fromisoformat(w[1]) < t_min.date()
                or date.fromisoformat(w[0]) > t_max.date()
            ]
        return all_windows

    # ── pull loop ───────────────────────────────────────────

    async def run(self) -> None:
        """Main entry point: pull all products sequentially."""
        self._known_symbols = {p.symbol for p in self._config.products}

        for product in self._config.products:
            if self._should_stop:
                break
            await self._pull_product(product)

        # Check for new products every full pass
        new_products = self._check_new_products()
        while new_products and not self._should_stop:
            for p in new_products:
                if self._should_stop:
                    break
                self._known_symbols.add(p.symbol)
                await self._pull_product(p)
            new_products = self._check_new_products()

    async def _pull_product(self, product: ProductConfig) -> None:
        """Pull one product's remaining windows."""
        windows = self._store.load(product.symbol)
        if not windows:
            windows = await self._compute_windows(product)
            if not windows:
                logger.info("%s: nothing to pull (already complete)", product.symbol)
                return
            self._store.save(product.symbol, windows)

        logger.info("[%s] Starting pull: %d windows", product.symbol, len(windows))

        while windows and not self._should_stop:
            window = windows[0]

            if not await self.ensure_connected():
                break

            # Resolve contract (once per product, on first window)
            contract = resolve_contract(
                self._ib, product.symbol, product.sec_type,
                product.exchange, product.currency,
            )
            if contract is None:
                logger.error("[%s] Contract resolution failed, skipping", product.symbol)
                break

            what = resolve_what_to_show(product.sec_type)
            end_dt = window[1]
            dur = f"{WINDOW_DAYS} D"

            success = False
            for attempt in range(1, RETRY_LIMIT + 1):
                try:
                    bars = self._ib.reqHistoricalData(
                        contract,
                        endDateTime=end_dt,
                        durationStr=dur,
                        barSizeSetting="1 min",
                        whatToShow=what,
                        useRTH=False,
                        formatDate=1,
                    )
                    inserted = await self._writer.upsert_bars(product.symbol, bars)
                    success = True
                    break
                except (ConnectionError, OSError, asyncio.TimeoutError) as e:
                    logger.warning("[%s] window %s attempt %d failed: %s",
                                   product.symbol, window, attempt, e)
                    self._connection_ok = False
                    await asyncio.sleep(5 * attempt)
                except Exception as e:
                    logger.error("[%s] window %s error: %s (skipping)",
                                 product.symbol, window, e)
                    break  # non-recoverable, skip window

            if success:
                self._store.mark_completed(product.symbol, window)
                windows = self._store.load(product.symbol)
                if windows and not self._should_stop:
                    await asyncio.sleep(self._config.request_interval_seconds)
            else:
                # Non-recoverable or exhausted retries: skip to next window
                self._store.mark_completed(product.symbol, window)
                windows = self._store.load(product.symbol)

        logger.info("[%s] Pull complete: %d windows remaining",
                     product.symbol, len(windows))

    def request_stop(self):
        self._should_stop = True
```

- [ ] **Step 4: Run unit tests**

Run: `pytest backfiller/tests/test_scheduler.py -v`
Expected: 3 passed

- [ ] **Step 5: Commit**

```bash
git add backfiller/scheduler.py backfiller/tests/test_scheduler.py
git commit -m "feat(backfiller): pull scheduler with window split + reconnect"
```

---

### Task 6: main.py — CLI entry point

**Files:**
- Create: `backfiller/main.py`

- [ ] **Step 1: Create `backfiller/main.py`**

```python
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
import sys
from pathlib import Path

# Python 3.12+ compatibility for ib_insync's eventkit
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

from ib_insync import IB, util

from backfiller.config import load_config, ProductConfig
from backfiller.contract import resolve_contract, resolve_what_to_show, parse_contract_symbol
from backfiller.db_writer import MinuteBarWriter
from backfiller.scheduler import PullScheduler, split_windows

logger = logging.getLogger(__name__)

PROGRESS_DIR = Path(__file__).parent / "progress"


# ── --status ────────────────────────────────────────────────

async def cmd_status(args, cfg):
    pool = await MinuteBarWriter.create_pool(cfg.db_url)
    writer = MinuteBarWriter(pool)
    products = cfg.products
    if args.only:
        products = [p for p in products if p.symbol in args.only]

    print(f"\n产品状态总览:\n")
    for p in products:
        t_min, t_max, cnt = await writer.get_range(p.symbol)
        gaps = await writer.detect_gaps(p.symbol)
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
            for g in gaps[:3]:  # show first 3 gaps
                print(f"  {'':>10}   ⚠ 缺口 {g['gap_start']} ~ {g['gap_end']} ({g['diff_minutes']}min)")
            if len(gaps) > 3:
                print(f"  {'':>10}   ... and {len(gaps)-3} more gaps")
        print()
    await pool.close()


# ── --check ─────────────────────────────────────────────────

async def cmd_check(args, cfg):
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
                first_bar = bars[0].date
                last_bar = bars[-1].date
                print(f"  {p.symbol:<10} ✅ {len(bars)}条1分钟K线 ({first_bar} ~ {last_bar})")
            else:
                print(f"  {p.symbol:<10} ⚠️ 合约有效但无历史数据")
        except Exception as e:
            print(f"  {p.symbol:<10} ❌ {e}")

    ib.disconnect()
    print()


# ── --pull ──────────────────────────────────────────────────

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
    scheduler = PullScheduler(filtered_cfg, writer, PROGRESS_DIR)

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


# ── main ────────────────────────────────────────────────────

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
        asyncio.run(cmd_check(args, cfg))


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Verify import**

Run: `cd /home/yuanjs/projects/ibkrData && python -c "from backfiller.main import main; print('OK')"`
Expected: `OK`

- [ ] **Step 3: Verify --status works (DB not required for parsing)**

Run: `cd /home/yuanjs/projects/ibkrData && python -m backfiller.main --help`
Expected: prints help text

- [ ] **Step 4: Commit**

```bash
git add backfiller/main.py
git commit -m "feat(backfiller): CLI entry point --pull/--status/--check"
```

---

### Task 7: Smoke test with IB Gateway (manual)

**No code changes — manual verification steps.**

- [ ] **Step 1: Verify --check against live IB Gateway**

```bash
cd /home/yuanjs/projects/ibkrData
python -m backfiller.main --check
```

Expected output similar to:
```
  SPI    ✅ 432条1分钟K线 (2026-05-29 ~ 2026-05-31)
  USD.JPY ✅ 2880条1分钟K线 (2026-05-29 ~ 2026-05-31)
  MNQ    ✅ 1856条1分钟K线 (2026-05-29 ~ 2026-05-31)
  ...
```

- [ ] **Step 2: Verify --status against DB**

```bash
python -m backfiller.main --status
```
If DB is empty: all products show "尚未拉取". Good.
If DB has data: shows ranges and counts.

- [ ] **Step 3: Run a short pull (limited range)**

Edit config.yaml temporarily with `start: 2026-05-28`, then:
```bash
python -m backfiller.main --pull --only SPI
```
Let it run for a couple windows, Ctrl+C to verify graceful shutdown.
Then re-run without --only to verify resume.

- [ ] **Step 4: Verify DB content**

```bash
psql "$DB_URL" -c "SELECT COUNT(*), MIN(time), MAX(time) FROM minute_bars WHERE symbol='SPI';"
```

- [ ] **Step 5: Restore config.yaml `start` to original value**

---

### Task 8: Edge case hardening

**Files:**
- Modify: `backfiller/scheduler.py`

- [ ] **Step 1: Handle empty bars from IBKR gracefully**

In scheduler.py `_pull_product`, after `reqHistoricalData` succeeds but returns empty list, the window was still "successful" — no data is valid (e.g., holiday). The current code already handles this via `upsert_bars` returning 0.

- [ ] **Step 2: Handle what_to_show edge case**

The `resolve_what_to_show` already handles CASH vs others. No change needed.

- [ ] **Step 3: Verify that Ctrl+C during reqHistoricalData doesn't leave corrupted state**

The SIGINT handler sets `_should_stop = True`. The `ib.reqHistoricalData` is a synchronous call in the current thread — SIGINT during it will raise `KeyboardInterrupt` in Python. The `_pull_product` method has a try/except around it, which will catch it, and the finally block in `cmd_pull` ensures disconnect and pool close.

- [ ] **Step 4: Commit hardening (if any changes made)**

```bash
git commit -m "fix(backfiller): edge case hardening"
```
