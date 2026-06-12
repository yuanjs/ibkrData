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
FUTURES_WINDOW_DAYS = 1
DAILY_WINDOW_DAYS = 365
DAILY_START_PADDING_DAYS = 31
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


def split_date_windows(start: date, end: date, days: int) -> list[Window]:
    """Split *start* .. *end* into larger date windows."""
    windows: list[Window] = []
    current = start
    while current <= end:
        chunk_end = min(current + timedelta(days=days - 1), end)
        windows.append((current.isoformat(), chunk_end.isoformat()))
        current = chunk_end + timedelta(days=1)
    return windows


def subtract_trading_days(d: date, days: int) -> date:
    """Subtract weekday trading days from *d*.

    Exchange-specific holidays are intentionally not handled here; the goal is
    to create a stable overlap window without adding a calendar dependency to
    the download path.
    """
    current = d
    remaining = days
    while remaining > 0:
        current -= timedelta(days=1)
        if current.weekday() < 5:
            remaining -= 1
    return current


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

    async def run_daily(self) -> None:
        """Pull historical daily bars for configured products.

        Futures daily bars are written per concrete contract into
        ``futures_daily_bars``.  Other security types are written into the
        existing ``daily_bars`` table.  The daily start date is padded by
        ``DAILY_START_PADDING_DAYS`` so backtests have indicator warm-up data
        before the configured minute-bar start.
        """
        for product in self._config.products:
            self._known_symbols.add(product.symbol)
            if product.sec_type == "FUT":
                await self._pull_fut_daily_via_expired_contracts(product)
            else:
                await self._pull_daily_product(product)

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
        contracts, and backfills each contract's active period into
        ``futures_minute_bars`` with contract identity preserved.

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
        """Backfill raw futures by pulling each quarterly contract's
        active period via windowed ``reqHistoricalData`` with ``endDateTime``.

        Each quarterly contract (expiry months 03/06/09/12) is pulled from
        the previous expiry minus the configured overlap window to its own
        expiry.  This preserves a shared trading window between adjacent
        contracts for later roll selection and gap calculation.  Requests use
        the individual contract (by conId), which **does** support
        ``endDateTime`` — unlike CONTFUT.

        The resulting data in *futures_minute_bars* is raw single-contract
        history.  Continuous futures roll selection and adjustment should be
        derived from this raw table, not performed during download.
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

            # Active period with overlap: start the next contract before the
            # previous one expires so roll logic can compare both contracts.
            if prev_expiry is None:
                # The first visible quarterly contract must cover the configured
                # backfill start; limiting it to expiry-100 truncates the
                # earliest history and leaves the front of the dataset empty.
                period_start = cfg_start
            else:
                period_start = max(
                    cfg_start,
                    subtract_trading_days(
                        prev_expiry,
                        self._config.futures_overlap_trading_days,
                    ),
                )
            period_end = min(cfg_end, exp_date)

            if period_start >= period_end:
                prev_expiry = exp_date
                continue

            windows = split_date_windows(
                period_start,
                period_end,
                FUTURES_WINDOW_DAYS,
            )
            task_key = self._fut_task_key(c)
            if windows and not self._store.has_task(product.symbol, task_key):
                windows = await self._filter_existing_futures_windows(
                    product.symbol,
                    c.conId,
                    windows,
                )
                self._store.save_task_windows(product.symbol, task_key, windows)

            remaining_windows = self._store.load_task_windows(
                product.symbol, task_key,
            )
            if remaining_windows:
                all_tasks.append((c, remaining_windows))

            prev_expiry = exp_date

        logger.info("%s: %d contract-periods to backfill, total ~%d windows",
                     product.symbol, len(all_tasks),
                     sum(len(w) for _, w in all_tasks))

        # Process each contract-period using the standard window loop
        for contract, contract_windows in all_tasks:
            contract.includeExpired = True
            task_key = self._fut_task_key(contract)
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
                            durationStr=f"{FUTURES_WINDOW_DAYS} D",
                            barSizeSetting="1 min",
                            whatToShow=resolve_what_to_show(product.sec_type),
                            useRTH=False, formatDate=1,
                        )
                        if not bars:
                            logger.warning(
                                "%s conId=%s window %s returned no bars",
                                product.symbol,
                                contract.conId,
                                w,
                            )
                            break
                        await self._writer.upsert_futures_bars(
                            product.symbol, contract, bars,
                        )
                        if not await self._futures_window_is_complete(
                            product.symbol,
                            contract,
                            w,
                        ):
                            break
                        self._store.mark_task_completed(
                            product.symbol, task_key, w,
                        )
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

    async def _futures_window_is_complete(
        self,
        symbol: str,
        contract: Contract,
        window: Window,
    ) -> bool:
        """Validate a just-pulled futures window before checkpointing it."""
        gaps = await self._writer.detect_futures_session_gaps(
            symbol,
            start_date=date.fromisoformat(window[0]),
            end_date=date.fromisoformat(window[1]),
            con_id=int(contract.conId),
        )
        if not gaps:
            return True

        for gap in gaps[:3]:
            logger.warning(
                "%s conId=%s window %s still incomplete: "
                "session=%s minutes=%s day_minutes=%s loaded=%s..%s",
                symbol,
                contract.conId,
                window,
                gap["session_date"],
                gap["minute_count"],
                gap.get("day_session_count"),
                gap["minute_min_time"],
                gap["minute_max_time"],
            )
        if len(gaps) > 3:
            logger.warning(
                "%s conId=%s window %s has %d incomplete sessions",
                symbol,
                contract.conId,
                window,
                len(gaps),
            )
        return False

    async def repair_futures_session_gaps(
        self,
        product: ProductConfig,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        min_minutes: int = 300,
        dry_run: bool = True,
    ) -> list[dict]:
        """Repair futures sessions that have daily bars but too few minutes."""
        gaps = await self._writer.detect_futures_session_gaps(
            product.symbol,
            start_date=start_date,
            end_date=end_date,
            min_minutes=min_minutes,
        )
        if not gaps:
            logger.info("%s: no incomplete futures sessions found", product.symbol)
            return []

        logger.info(
            "%s: found %d incomplete futures sessions (min_minutes=%d)",
            product.symbol,
            len(gaps),
            min_minutes,
        )
        if dry_run:
            return gaps

        contracts = await self._resolve_fut_contracts(product)
        contracts_by_con_id = {int(c.conId): c for c in contracts if c.conId}

        for gap in gaps:
            if self._should_stop:
                break
            contract = contracts_by_con_id.get(int(gap["con_id"]))
            if contract is None:
                logger.warning(
                    "%s: cannot resolve conId=%s for session repair",
                    product.symbol,
                    gap["con_id"],
                )
                continue
            contract.includeExpired = True
            if not await self.ensure_connected():
                break

            session_end = gap["session_end"]
            end_dt = session_end.strftime("%Y%m%d %H:%M:%S UTC")
            logger.info(
                "%s %s conId=%s: repairing session %s (%s..%s), existing minutes=%s",
                product.symbol,
                gap["local_symbol"],
                gap["con_id"],
                gap["session_date"],
                gap["session_start"],
                gap["session_end"],
                gap["minute_count"],
            )
            bars = await self._ib.reqHistoricalDataAsync(
                contract,
                endDateTime=end_dt,
                durationStr=f"{FUTURES_WINDOW_DAYS} D",
                barSizeSetting="1 min",
                whatToShow=resolve_what_to_show(product.sec_type),
                useRTH=False,
                formatDate=1,
            )
            await self._writer.upsert_futures_bars(product.symbol, contract, bars)
            repaired_gaps = await self._writer.detect_futures_session_gaps(
                product.symbol,
                start_date=gap["session_date"],
                end_date=gap["session_date"],
                con_id=int(gap["con_id"]),
                min_minutes=min_minutes,
            )
            if repaired_gaps:
                logger.warning(
                    "%s %s conId=%s: session %s still incomplete after repair",
                    product.symbol,
                    gap["local_symbol"],
                    gap["con_id"],
                    gap["session_date"],
                )
            await asyncio.sleep(self._config.request_interval_seconds)

        return gaps

    async def _pull_daily_product(self, product: ProductConfig) -> None:
        """Pull non-futures daily bars into ``daily_bars``."""
        daily_start = (
            date.fromisoformat(self._config.start)
            - timedelta(days=DAILY_START_PADDING_DAYS)
        )
        cfg_end = date.fromisoformat(self._config.end)
        windows = split_date_windows(daily_start, cfg_end, DAILY_WINDOW_DAYS)
        task_key = f"DAILY:{product.symbol}"

        if windows and not self._store.has_task(product.symbol, task_key):
            windows = await self._filter_existing_daily_windows(
                product.symbol,
                windows,
            )
            self._store.save_task_windows(product.symbol, task_key, windows)

        windows = self._store.load_task_windows(product.symbol, task_key)
        if not windows:
            logger.info("%s: daily bars already covered", product.symbol)
            return

        logger.info(
            "%s: pulling %d daily windows from %s to %s",
            product.symbol,
            len(windows),
            windows[0][0],
            windows[-1][1],
        )

        contract: Contract | None = None
        for w in windows:
            if self._should_stop:
                return
            if not await self.ensure_connected():
                return
            if contract is None:
                try:
                    contract = await resolve_contract_async(
                        self._ib,
                        product.symbol,
                        product.sec_type,
                        product.exchange,
                        product.currency,
                    )
                except Exception as exc:
                    logger.error(
                        "%s: daily contract resolution failed: %s",
                        product.symbol,
                        exc,
                    )
                    return
                if contract is None:
                    logger.error(
                        "%s: daily contract resolution returned None",
                        product.symbol,
                    )
                    return

            success = await self._request_daily_window(
                product,
                contract,
                w,
                writer_method="daily",
            )
            if success:
                self._store.mark_task_completed(product.symbol, task_key, w)
            else:
                logger.error("%s: failed daily window %s, moving on",
                             product.symbol, w)

        logger.info("%s: daily backfill complete", product.symbol)

    async def _pull_fut_daily_via_expired_contracts(
        self,
        product: ProductConfig,
    ) -> None:
        """Pull raw daily bars for each quarterly futures contract."""
        contracts = await self._resolve_fut_contracts(product)
        if not contracts:
            logger.error("%s: no quarterly contracts available, skipping daily",
                         product.symbol)
            return

        minute_start = date.fromisoformat(self._config.start)
        daily_start = minute_start - timedelta(days=DAILY_START_PADDING_DAYS)
        cfg_end = date.fromisoformat(self._config.end)

        all_tasks: list[tuple[Contract, list[Window]]] = []
        prev_expiry: date | None = None

        for c in contracts:
            exp_date = self._contract_expiry_date(c)
            if exp_date is None:
                continue

            if prev_expiry is None:
                minute_period_start = minute_start
            else:
                minute_period_start = max(
                    minute_start,
                    subtract_trading_days(
                        prev_expiry,
                        self._config.futures_overlap_trading_days,
                    ),
                )
            period_start = max(
                daily_start,
                minute_period_start - timedelta(days=DAILY_START_PADDING_DAYS),
            )
            period_end = min(cfg_end, exp_date)

            if period_start >= period_end:
                prev_expiry = exp_date
                continue

            windows = split_date_windows(period_start, period_end, DAILY_WINDOW_DAYS)
            task_key = self._fut_daily_task_key(c)
            if windows and not self._store.has_task(product.symbol, task_key):
                windows = await self._filter_existing_futures_daily_windows(
                    product.symbol,
                    c.conId,
                    windows,
                )
                self._store.save_task_windows(product.symbol, task_key, windows)

            remaining_windows = self._store.load_task_windows(
                product.symbol, task_key,
            )
            if remaining_windows:
                all_tasks.append((c, remaining_windows))

            prev_expiry = exp_date

        logger.info(
            "%s: %d contract-periods to daily backfill, total ~%d windows",
            product.symbol,
            len(all_tasks),
            sum(len(w) for _, w in all_tasks),
        )

        for contract, contract_windows in all_tasks:
            contract.includeExpired = True
            task_key = self._fut_daily_task_key(contract)
            for idx, w in enumerate(contract_windows, start=1):
                if self._should_stop:
                    return
                if not await self.ensure_connected():
                    return

                success = await self._request_daily_window(
                    product,
                    contract,
                    w,
                    writer_method="futures_daily",
                )
                if success:
                    self._store.mark_task_completed(product.symbol, task_key, w)
                    pct = idx / len(contract_windows) * 100
                    logger.info(
                        "%s daily (conId=%s exp=%s): %d/%d windows (%d%%)",
                        product.symbol,
                        contract.conId,
                        contract.lastTradeDateOrContractMonth,
                        idx,
                        len(contract_windows),
                        int(pct),
                    )
                else:
                    logger.error("%s: failed daily window %s, moving on",
                                 product.symbol, w)

        logger.info("%s: FUT daily backfill complete (%d contract periods)",
                    product.symbol, len(all_tasks))

    async def _request_daily_window(
        self,
        product: ProductConfig,
        contract: Contract,
        window: Window,
        *,
        writer_method: str,
    ) -> bool:
        duration_days = (
            date.fromisoformat(window[1]) - date.fromisoformat(window[0])
        ).days + 1

        for attempt in range(1, RETRY_LIMIT + 1):
            if self._should_stop:
                return False
            try:
                end_dt = window[1].replace("-", "") + "-23:59:59"
                bars = await self._ib.reqHistoricalDataAsync(
                    contract,
                    endDateTime=end_dt,
                    durationStr=f"{duration_days} D",
                    barSizeSetting="1 day",
                    whatToShow=resolve_what_to_show(product.sec_type),
                    useRTH=False,
                    formatDate=1,
                )
                if writer_method == "futures_daily":
                    await self._writer.upsert_futures_daily_bars(
                        product.symbol,
                        contract,
                        bars,
                    )
                else:
                    await self._writer.upsert_daily_bars(product.symbol, bars)
                self._connection_ok = True
                await asyncio.sleep(self._config.request_interval_seconds)
                return True
            except (ConnectionError, OSError, TimeoutError):
                self._connection_ok = False
                if attempt < RETRY_LIMIT and not await self.ensure_connected():
                    return False
            except Exception as exc:
                logger.warning(
                    "%s daily window %s (attempt %d/%d): %s",
                    product.symbol,
                    window,
                    attempt,
                    RETRY_LIMIT,
                    exc,
                )
                if attempt < RETRY_LIMIT:
                    await asyncio.sleep(
                        RECONNECT_BASE_DELAY * (2 ** (attempt - 1)))
        return False

    @staticmethod
    def _fut_task_key(contract: Contract) -> str:
        contract_month = (contract.lastTradeDateOrContractMonth or "")[:6]
        return f"FUT:{contract.conId}:{contract_month}"

    @staticmethod
    def _fut_daily_task_key(contract: Contract) -> str:
        contract_month = (contract.lastTradeDateOrContractMonth or "")[:6]
        return f"FUT_DAILY:{contract.conId}:{contract_month}"

    @staticmethod
    def _contract_expiry_date(contract: Contract) -> date | None:
        exp_str = (contract.lastTradeDateOrContractMonth or "")[:8]
        if len(exp_str) < 8:
            return None
        try:
            return date.fromisoformat(
                f"{exp_str[:4]}-{exp_str[4:6]}-{exp_str[6:8]}"
            )
        except ValueError:
            return None

    async def _filter_existing_futures_windows(
        self,
        symbol: str,
        con_id: int,
        windows: list[Window],
    ) -> list[Window]:
        """Return only windows not fully covered in futures_minute_bars."""
        remaining: list[Window] = []
        for window in windows:
            is_covered = await self._writer.has_futures_window_coverage(
                symbol,
                con_id,
                window[0],
                window[1],
            )
            if not is_covered:
                remaining.append(window)
        return remaining

    async def _filter_existing_daily_windows(
        self,
        symbol: str,
        windows: list[Window],
    ) -> list[Window]:
        """Return only daily windows not fully covered in ``daily_bars``."""
        remaining: list[Window] = []
        for window in windows:
            is_covered = await self._writer.has_daily_window_coverage(
                symbol,
                window[0],
                window[1],
            )
            if not is_covered:
                remaining.append(window)
        return remaining

    async def _filter_existing_futures_daily_windows(
        self,
        symbol: str,
        con_id: int,
        windows: list[Window],
    ) -> list[Window]:
        """Return only daily windows not fully covered in futures daily raw."""
        remaining: list[Window] = []
        for window in windows:
            is_covered = await self._writer.has_futures_daily_window_coverage(
                symbol,
                con_id,
                window[0],
                window[1],
            )
            if not is_covered:
                remaining.append(window)
        return remaining
