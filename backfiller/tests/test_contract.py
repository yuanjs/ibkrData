"""Unit tests for backfiller.contract — deterministic logic only, no IBKR connection."""

from backfiller.contract import parse_contract_symbol, resolve_what_to_show


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
