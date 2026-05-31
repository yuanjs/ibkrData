"""Pull orchestration core — drives the IBKR historical data backfill.

Coordinates connection management, contract resolution, window-based
data fetching, checkpoint persistence, and re-discovery of products
added to the config at runtime.
"""

import asyncio
import logging
from datetime import date, timedelta
from pathlib import Path

from ib_insync import IB, Contract, Future

from backfiller.config import AppConfig, ProductConfig, load_config
from backfiller.contract import resolve_contract_async, resolve_what_to_show
from backfiller.db_writer import MinuteBarWriter
from backfiller.progress_store import ProgressStore

logger = logging.getLogger(__name__)

Window = tuple[str, str]

WINDOW_DAYS = 2
RECONNECT_BASE_DELAY = 2
RECONNECT_MAX_DELAY = 60
HMDS_WARMUP_SECONDS = 3
RETRY_LIMIT = 3


def split_windows(start: date, end: date) -> list[Window]:
    """Split *start* .. *end* into ``WINDOW_DAYS``-sized windows.

    Each window is ``(iso_start, iso_end)``.  The last window may be
    shorter than ``WINDOW_DAYS`` when the range length is not an exact
    multiple.
    """
    windows: list[Window] = []
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=WINDOW_DAYS - 1), end)
        windows.append((current.isoformat(), chunk_end.isoformat()))
        current = chunk_end + timedelta(days=1)
    return windows


class PullScheduler:
    """Orchestrate pulling historical minute bars for the configured products.

    Parameters
    ----------
    config:
        Application-wide configuration (products, date range, IB connection).
    writer:
        Database writer for persisting minute bars.
    progress_dir:
        Directory for JSON checkpoint files (one per symbol).
    """

    def __init__(
        self, config: AppConfig, writer: MinuteBarWriter, progress_dir: Path,
        *,
        allow_new_products: bool = True,
    ) -> None:
        self._config = config
        self._writer = writer
        self._store = ProgressStore(progress_dir)
        self._ib = IB()
        self._ib.RequestTimeout = 60
        self._should_stop = False
        self._connection_ok = False
        self._known_symbols: set[str] = set()
        self._allow_new_products = allow_new_products

    # ------------------------------------------------------------------
    # public helpers
    # ------------------------------------------------------------------

    async def ensure_connected(self) -> bool:
        """Return True when the IB Gateway session is healthy.

        If disconnected, attempt reconnection with exponential backoff
        (2, 4, 8, 16, … capped at ``RECONNECT_MAX_DELAY`` seconds).
        After a successful reconnect the method waits
        ``HMDS_WARMUP_SECONDS`` for the Market Data service to settle.
        """
        if self._ib.isConnected():
            self._connection_ok = True
            return True

        delay = RECONNECT_BASE_DELAY
        while delay <= RECONNECT_MAX_DELAY:
            if self._should_stop:
                return False

            try:
                await self._ib.connectAsync(
                    self._config.ib_host,
                    self._config.ib_port,
                    clientId=self._config.ib_client_id,
                )
                await asyncio.sleep(HMDS_WARMUP_SECONDS)
                self._connection_ok = True
                logger.info("Reconnected to IB Gateway")
                return True
            except Exception as exc:
                logger.warning(
                    "Reconnection failed, retrying in %ds: %s", delay, exc,
                )
                await asyncio.sleep(delay)
                delay = min(delay * 2, RECONNECT_MAX_DELAY)

        logger.error("Exhausted reconnection attempts")
        return False

    def disconnect(self) -> None:
        """Safely tear down the IB Gateway connection."""
        try:
            if self._ib.isConnected():
                self._ib.disconnect()
                logger.info("Disconnected from IB Gateway")
        except Exception as exc:
            logger.debug("Ignored error during disconnect: %s", exc)

    def request_stop(self) -> None:
        """Signal a graceful stop after the current window completes."""
        self._should_stop = True

    # ------------------------------------------------------------------
    # main entry point
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Pull historical data for every configured product.

        After the initial product list has been processed the config
        YAML is re-read and any newly-added products are pulled too.
        The loop repeats until no new products are discovered.
        """
        for product in self._config.products:
            self._known_symbols.add(product.symbol)
            await self._pull_product(product)

        if not self._allow_new_products:
            return
        while True:
            new_products = self._check_new_products()
            if not new_products:
                break
            for product in new_products:
                self._known_symbols.add(product.symbol)
                await self._pull_product(product)

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------

    def _check_new_products(self) -> list[ProductConfig]:
        """Re-read the config YAML; return products not yet known."""
        try:
            cfg = load_config()
        except Exception as exc:
            logger.warning("Could not reload config: %s", exc)
            return []
        return [
            p for p in cfg.products if p.symbol not in self._known_symbols
        ]

    async def _compute_windows(self, product: ProductConfig) -> list[Window]:
        """Return windows that still need to be pulled for *product*.

        Windows whose full date-range is covered by data already
        present in the database are excluded.
        """
        start = date.fromisoformat(self._config.start)
        end = date.fromisoformat(self._config.end)
        all_windows = split_windows(start, end)

        min_ts, max_ts, _ = await self._writer.get_range(product.symbol)
        if min_ts is None or max_ts is None:
            return all_windows  # no existing data — pull everything

        db_start = min_ts.date()
        db_end = max_ts.date()

        return [
            (ws, we) for ws, we in all_windows
            if date.fromisoformat(ws) < db_start
            or date.fromisoformat(we) > db_end
        ]

    async def _pull_product(self, product: ProductConfig) -> None:
        """Pull all remaining windows for *product*.

        Uses ``ProgressStore`` checkpoints so interrupted runs resume
        where they left off.

        *FUT*: queries all expired + active contracts via
        ``Future(includeExpired=True)``, filters to quarterly main
        contracts, and backfills each contract's active period using
        the standard windowed approach with ``endDateTime``.

        *CASH* / *STK*: windowed backfill directly.
        """
        # ── FUT: backfill via expired/active quarterly contracts ──
        if product.sec_type == "FUT":
            await self._pull_fut_via_expired_contracts(product)
            return

        # ── CASH / STK windowed approach ────────────────────────
        windows = self._store.load(product.symbol)
        if not windows:
            windows = await self._compute_windows(product)
            if not windows:
                logger.info(
                    "%s: all windows covered by existing data",
                    product.symbol,
                )
                return
            self._store.save(product.symbol, windows)

        logger.info(
            "%s: pulling %d windows from %s to %s",
            product.symbol, len(windows), windows[0][0], windows[-1][1],
        )

        while windows and not self._should_stop:
            window = windows[0]
            if not await self.ensure_connected():
                return

            # resolve contract
            try:
                contract = await resolve_contract_async(
                    self._ib, product.symbol, product.sec_type,
                    product.exchange, product.currency,
                )
            except Exception as exc:
                logger.error("%s: contract resolution failed: %s, skipping",
                             product.symbol, exc)
                return
            if contract is None:
                logger.error("%s: contract resolution returned None, skipping",
                             product.symbol)
                return

            # retry loop
            success, connection_lost = False, False
            for attempt in range(1, RETRY_LIMIT + 1):
                if self._should_stop:
                    return
                try:
                    end_dt = window[1].replace("-", "") + "-23:59:59"
                    bars = await self._ib.reqHistoricalDataAsync(
                        contract, endDateTime=end_dt,
                        durationStr=f"{WINDOW_DAYS} D",
                        barSizeSetting="1 min",
                        whatToShow=resolve_what_to_show(product.sec_type),
                        useRTH=False, formatDate=1,
                    )
                    await self._writer.upsert_bars(product.symbol, bars)
                    self._store.mark_completed(product.symbol, window)
                    self._connection_ok = True
                    await asyncio.sleep(self._config.request_interval_seconds)
                    success = True
                    break
                except (ConnectionError, OSError, TimeoutError) as exc:
                    self._connection_ok = False
                    connection_lost = True
                    logger.warning("%s window %s (attempt %d/%d): %s",
                                   product.symbol, window, attempt, RETRY_LIMIT, exc)
                    if attempt < RETRY_LIMIT and not await self.ensure_connected():
                        return
                except Exception as exc:
                    logger.warning("%s window %s (attempt %d/%d): %s",
                                   product.symbol, window, attempt, RETRY_LIMIT, exc)
                    if attempt < RETRY_LIMIT:
                        await asyncio.sleep(RECONNECT_BASE_DELAY * (2 ** (attempt - 1)))

            if not success:
                if connection_lost:
                    logger.error("%s: connection lost on %s, giving up",
                                 product.symbol, window)
                    return
                logger.error("%s: max retries for %s, skipping",
                             product.symbol, window)
                self._store.mark_completed(product.symbol, window)

            windows = self._store.load(product.symbol)

        if not self._should_stop:
            logger.info("%s: done — all windows completed", product.symbol)

    # ── FUT: quarterly-contract chain backfill ──────────────────

    _QUARTERLY_MONTHS = frozenset({"03", "06", "09", "12"})

    @staticmethod
    def _is_quarterly_contract(contract: Contract) -> bool:
        """True for contracts expiring in Mar/Jun/Sep/Dec."""
        exp = (contract.lastTradeDateOrContractMonth or "0000")
        return len(exp) >= 6 and exp[4:6] in PullScheduler._QUARTERLY_MONTHS

    async def _resolve_fut_contracts(
        self, product: ProductConfig,
    ) -> list[Contract]:
        """Fetch all available (incl. expired) contracts; return quarterly ones
        sorted by expiry ascending."""
        if not await self.ensure_connected():
            return []
        try:
            details = await self._ib.reqContractDetailsAsync(
                Future(product.symbol, exchange=product.exchange,
                       includeExpired=True),
            )
        except Exception as exc:
            logger.error("%s: failed to list contracts: %s", product.symbol, exc)
            return []

        quarterly = [
            d.contract for d in details
            if self._is_quarterly_contract(d.contract)
        ]
        quarterly.sort(key=lambda c: c.lastTradeDateOrContractMonth or "")
        logger.info(
            "%s: resolved %d/%d quarterly contracts [%s .. %s]",
            product.symbol, len(quarterly), len(details),
            quarterly[0].lastTradeDateOrContractMonth[:6] if quarterly else "?",
            quarterly[-1].lastTradeDateOrContractMonth[:6] if quarterly else "?",
        )
        return quarterly

    async def _pull_fut_via_expired_contracts(
        self, product: ProductConfig,
    ) -> None:
        """Backfill continuous futures by pulling each quarterly contract's
        active period via windowed ``reqHistoricalData`` with ``endDateTime``.

        Each quarterly contract (expiry months 03/06/09/12) is active
        from the previous expiry to its own expiry.  We backfill those
        windows using the individual contract (by conId), which **does**
        support ``endDateTime`` — unlike CONTFUT.

        The resulting data in *minute_bars* is a continuous chain that
        the user can roll themselves (or simply use as-is for ML).
        """
        contracts = await self._resolve_fut_contracts(product)
        if not contracts:
            logger.error("%s: no quarterly contracts available, skipping",
                         product.symbol)
            return

        cfg_start = date.fromisoformat(self._config.start)
        cfg_end = date.fromisoformat(self._config.end)

        # Determine each contract's active period and generate windows
        all_tasks: list[tuple[Contract, list[Window]]] = []
        prev_expiry: date | None = None

        for c in contracts:
            exp_str = (c.lastTradeDateOrContractMonth or "")[:8]
            if len(exp_str) < 8:
                continue
            try:
                exp_date = date.fromisoformat(f"{exp_str[:4]}-{exp_str[4:6]}-{exp_str[6:8]}")
            except ValueError:
                continue

            # Active period: from previous expiry (or 3 months back) to this expiry
            if prev_expiry is None:
                from datetime import timedelta
                period_start = max(cfg_start, exp_date - timedelta(days=100))
            else:
                period_start = max(cfg_start, prev_expiry)
            period_end = min(cfg_end, exp_date)

            if period_start >= period_end:
                prev_expiry = exp_date
                continue

            windows = split_windows(period_start, period_end)
            if windows:
                all_tasks.append((c, windows))

            prev_expiry = exp_date

        logger.info("%s: %d contract-periods to backfill, total ~%d windows",
                     product.symbol, len(all_tasks),
                     sum(len(w) for _, w in all_tasks))

        # Process each contract-period using the standard window loop
        for contract, contract_windows in all_tasks:
            contract.includeExpired = True
            for w in contract_windows:
                if self._should_stop:
                    return
                if not await self.ensure_connected():
                    return

                success = False
                for attempt in range(1, RETRY_LIMIT + 1):
                    if self._should_stop:
                        return
                    try:
                        end_dt = w[1].replace("-", "") + "-23:59:59"
                        bars = await self._ib.reqHistoricalDataAsync(
                            contract, endDateTime=end_dt,
                            durationStr=f"{WINDOW_DAYS} D",
                            barSizeSetting="1 min",
                            whatToShow=resolve_what_to_show(product.sec_type),
                            useRTH=False, formatDate=1,
                        )
                        await self._writer.upsert_bars(product.symbol, bars)
                        self._connection_ok = True
                        await asyncio.sleep(
                            self._config.request_interval_seconds)
                        success = True
                        break
                    except (ConnectionError, OSError, TimeoutError):
                        if attempt < RETRY_LIMIT and not await self.ensure_connected():
                            return
                    except Exception:
                        if attempt < RETRY_LIMIT:
                            await asyncio.sleep(
                                RECONNECT_BASE_DELAY * (2 ** (attempt - 1)))
                # Log progress every window
                if not success:
                    logger.error("%s: failed window %s, moving on", product.symbol, w)
                elif len(contract_windows) <= 10 or (
                    contract_windows.index(w) + 1) % 10 == 0:
                    pct = (contract_windows.index(w) + 1) / len(contract_windows) * 100
                    logger.info(
                        "%s (conId=%s exp=%s): %d/%d windows (%d%%)",
                        product.symbol, contract.conId,
                        contract.lastTradeDateOrContractMonth,
                        contract_windows.index(w) + 1,
                        len(contract_windows), int(pct),
                    )

        logger.info("%s: FUT backfill complete (%d contract periods)",
                     product.symbol, len(all_tasks))
