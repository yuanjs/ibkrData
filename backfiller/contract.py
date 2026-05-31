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


def resolve_contract(
    ib: IB,
    symbol: str,
    sec_type: str,
    exchange: str,
    currency: str,
) -> Optional[Contract]:
    """
    Resolve *symbol* to an :class:`ib_insync.Contract` object connected to a
    live IB Gateway session.

    Resolution strategy by security type:

    **FUT** — futures
        Try ``CONTFUT`` to obtain the currently active rolling contract first;
        fall back to the earliest-dated contract month if ``CONTFUT`` is
        unavailable for the product.

    **CASH** — forex
        Split a dotted pair like ``"USD.JPY"`` via :func:`parse_contract_symbol`
        and qualify the resulting contract.

    **STK / others**
        Build a contract and qualify it directly.

    Returns ``None`` if resolution fails.
    """
    contract_symbol = parse_contract_symbol(symbol, sec_type)

    if sec_type == "FUT":
        # --- Attempt CONTFUT (continuously-linked active contract) ---
        cont_contract = Contract(
            secType="CONTFUT",
            symbol=symbol,
            exchange=exchange,
            currency=currency,
        )
        cont_details = ib.reqContractDetails(cont_contract)
        if cont_details:
            resolved = cont_details[0].contract
            contract = Contract(
                secType="FUT",
                symbol=resolved.symbol,
                exchange=resolved.exchange,
                currency=resolved.currency,
                lastTradeDateOrContractMonth=resolved.lastTradeDateOrContractMonth,
                tradingClass=resolved.tradingClass,
                multiplier=resolved.multiplier,
            )
            qualified = ib.qualifyContracts(contract)
            return qualified[0] if qualified else contract

        # --- Fallback: earliest-dated contract ---
        fallback = Contract(
            secType="FUT",
            symbol=symbol,
            exchange=exchange,
            currency=currency,
        )
        cds = ib.reqContractDetails(fallback)
        if cds:
            sorted_cds = sorted(
                cds, key=lambda x: x.contract.lastTradeDateOrContractMonth or ""
            )
            return sorted_cds[0].contract

        return None

    # --- CASH / STK / others ---
    contract = Contract(
        symbol=contract_symbol,
        secType=sec_type,
        exchange=exchange,
        currency=currency,
    )
    qualified = ib.qualifyContracts(contract)
    return qualified[0] if qualified else contract
