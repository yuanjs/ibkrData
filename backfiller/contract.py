"""IBKR contract resolver for backfiller.

Provides contract parsing and resolution functions following the same
strategies as collector/ibkr_client.py but as standalone utilities
suitable for batch historical data backfilling.
"""

from typing import Optional

from ib_insync import IB, Contract


def parse_contract_symbol(symbol: str, sec_type: str) -> str:
    """
    For CASH (forex) products like ``"USD.JPY"``, IBKR contracts only need
    the base currency (e.g. ``"USD"``) as the symbol.  Other security types
    return the original symbol unchanged.
    """
    if sec_type == "CASH" and "." in symbol:
        return symbol.split(".")[0]
    return symbol


def resolve_what_to_show(sec_type: str) -> str:
    """
    Return the IBKR ``whatToShow`` parameter appropriate for *sec_type*.

    - ``"CASH"`` (forex) → ``"MIDPOINT"`` (mid-point of bid/ask)
    - All others → ``"TRADES"``
    """
    return "MIDPOINT" if sec_type == "CASH" else "TRADES"


# ── Sync resolver (used by cmd_check) ───────────────────────


def resolve_contract(
    ib: IB,
    symbol: str,
    sec_type: str,
    exchange: str,
    currency: str,
) -> Optional[Contract]:
    """Synchronous contract resolution (sync ib_insync API).

    Resolution strategy by security type:

    **FUT** — futures
        Try ``CONTFUT`` to obtain the currently active rolling contract first;
        fall back to the earliest-dated contract month if ``CONTFUT`` is
        unavailable.

    **CASH** — forex
        Split a dotted pair like ``"USD.JPY"`` via :func:`parse_contract_symbol`
        and qualify the resulting contract.

    **STK / others**
        Build a contract and qualify it directly.
    """
    contract_symbol = parse_contract_symbol(symbol, sec_type)

    if sec_type == "FUT":
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

        fallback = Contract(secType="FUT", symbol=symbol,
                            exchange=exchange, currency=currency)
        cds = ib.reqContractDetails(fallback)
        if cds:
            return sorted(
                cds, key=lambda x: x.contract.lastTradeDateOrContractMonth or ""
            )[0].contract
        return None

    contract = Contract(symbol=contract_symbol, secType=sec_type,
                        exchange=exchange, currency=currency)
    qualified = ib.qualifyContracts(contract)
    return qualified[0] if qualified else contract


# ── Async resolver (used by PullScheduler) ──────────────────


async def resolve_contract_async(
    ib: IB,
    symbol: str,
    sec_type: str,
    exchange: str,
    currency: str,
) -> Optional[Contract]:
    """Asynchronous contract resolution (async ib_insync API).

    Same resolution strategy as :func:`resolve_contract` but uses the
    ``*Async`` variants of ib_insync methods so it does not interfere
    with a running event loop when called from within asyncio coroutines.

    .. seealso:: :func:`resolve_contract` for the synchronous equivalent.
    """
    contract_symbol = parse_contract_symbol(symbol, sec_type)

    if sec_type == "FUT":
        cont = Contract(secType="CONTFUT", symbol=symbol,
                        exchange=exchange, currency=currency)
        details = await ib.reqContractDetailsAsync(cont)
        if details:
            r = details[0].contract
            contract = Contract(
                secType="FUT", symbol=r.symbol, exchange=r.exchange,
                currency=r.currency,
                lastTradeDateOrContractMonth=r.lastTradeDateOrContractMonth,
                tradingClass=r.tradingClass, multiplier=r.multiplier,
            )
            qualified = await ib.qualifyContractsAsync(contract)
            return qualified[0] if qualified else contract

        fallback = Contract(secType="FUT", symbol=symbol,
                            exchange=exchange, currency=currency)
        cds = await ib.reqContractDetailsAsync(fallback)
        if cds:
            return sorted(
                cds, key=lambda x: x.contract.lastTradeDateOrContractMonth or ""
            )[0].contract
        return None

    contract = Contract(symbol=contract_symbol, secType=sec_type,
                        exchange=exchange, currency=currency)
    qualified = await ib.qualifyContractsAsync(contract)
    return qualified[0] if qualified else contract
