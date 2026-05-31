"""Pull orchestration core — drives the IBKR historical data backfill.

Coordinates connection management, contract resolution, window-based
data fetching, checkpoint persistence, and re-discovery of products
added to the config at runtime.
"""

import asyncio
import logging
from datetime import date, timedelta
from pathlib import Path

from ib_insync import IB

from backfiller.config import AppConfig, ProductConfig, load_config
from backfiller.contract import resolve_contract, resolve_what_to_show
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
    ) -> None:
        self._config = config
        self._writer = writer
        self._store = ProgressStore(progress_dir)
        self._ib = IB()
        self._ib.RequestTimeout = 60
        self._should_stop = False
        self._connection_ok = False
        self._known_symbols: set[str] = set()

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
        """
        # --- restore or compute the window list ---
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

            # --- connection ---
            if not await self.ensure_connected():
                return

            # --- contract resolution (failure = skip product) ---
            try:
                contract = resolve_contract(
                    self._ib,
                    product.symbol,
                    product.sec_type,
                    product.exchange,
                    product.currency,
                )
            except Exception as exc:
                logger.error(
                    "%s: contract resolution raised %s, skipping product",
                    product.symbol, exc,
                )
                return

            if contract is None:
                logger.error(
                    "%s: contract resolution returned None, skipping product",
                    product.symbol,
                )
                return

            # --- data-fetch retry loop ---
            success = False
            connection_lost = False

            for attempt in range(1, RETRY_LIMIT + 1):
                if self._should_stop:
                    return

                try:
                    end_dt = f"{window[1]} 23:59:59"
                    bars = self._ib.reqHistoricalData(
                        contract,
                        endDateTime=end_dt,
                        durationStr=f"{WINDOW_DAYS} D",
                        barSizeSetting="1 min",
                        whatToShow=resolve_what_to_show(product.sec_type),
                        useRTH=False,
                        formatDate=1,
                    )
                    await self._writer.upsert_bars(product.symbol, bars)
                    self._store.mark_completed(product.symbol, window)
                    self._connection_ok = True

                    await asyncio.sleep(
                        self._config.request_interval_seconds,
                    )
                    success = True
                    break

                except (ConnectionError, OSError, TimeoutError) as exc:
                    self._connection_ok = False
                    connection_lost = True
                    logger.warning(
                        "%s window %s (attempt %d/%d): %s",
                        product.symbol, window, attempt, RETRY_LIMIT, exc,
                    )
                    if attempt < RETRY_LIMIT:
                        if not await self.ensure_connected():
                            return  # cannot reconnect — give up

                except Exception as exc:
                    logger.warning(
                        "%s window %s (attempt %d/%d): %s",
                        product.symbol, window, attempt, RETRY_LIMIT, exc,
                    )
                    if attempt < RETRY_LIMIT:
                        await asyncio.sleep(
                            RECONNECT_BASE_DELAY * (2 ** (attempt - 1)),
                        )

            if not success:
                if connection_lost:
                    logger.error(
                        "%s: connection lost on window %s, giving up on "
                        "product", product.symbol, window,
                    )
                    return
                logger.error(
                    "%s: max retries for window %s, skipping",
                    product.symbol, window,
                )
                self._store.mark_completed(product.symbol, window)

            # Reload checkpoint state for the next iteration
            windows = self._store.load(product.symbol)

        if not self._should_stop:
            logger.info("%s: done — all windows completed", product.symbol)
