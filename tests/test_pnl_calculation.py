from datetime import datetime, timezone

from api.routers.orders import _realized_pnl_rows


def test_realized_pnl_deducts_open_and_close_commissions_and_keeps_currency():
    rows = [
        {
            "time": datetime(2026, 8, 27, 0, tzinfo=timezone.utc),
            "account_id": "test",
            "symbol": "N225M",
            "con_id": 1,
            "local_symbol": "N225M-test",
            "side": "SLD",
            "quantity": 81,
            "price": 66855,
            "multiplier": 100,
            "commission": 1360,
            "currency": "JPY",
        },
        {
            "time": datetime(2026, 8, 27, 1, tzinfo=timezone.utc),
            "account_id": "test",
            "symbol": "N225M",
            "con_id": 1,
            "local_symbol": "N225M-test",
            "side": "BOT",
            "quantity": 81,
            "price": 66295,
            "multiplier": 100,
            "commission": 3240,
            "currency": "JPY",
        },
    ]

    result = _realized_pnl_rows(rows)

    assert len(result) == 1
    assert result[0]["realized_pnl"] == 4_531_400
    assert result[0]["commission"] == 4_600
    assert result[0]["currency"] == "JPY"


def test_realized_pnl_resets_stale_lots_after_confirmed_flat_position():
    common = {
        "account_id": "test",
        "symbol": "MES",
        "con_id": 793356217,
        "local_symbol": "MESU6",
        "multiplier": 5,
        "commission": 0,
        "currency": "USD",
    }
    rows = [
        {
            **common,
            "time": datetime(2026, 8, 25, 14, 28, tzinfo=timezone.utc),
            "side": "SLD",
            "quantity": 132,
            "price": 7683,
            "reset_lots": False,
        },
        {
            **common,
            "time": datetime(2026, 8, 28, 14, 21, tzinfo=timezone.utc),
            "side": "BOT",
            "quantity": 111,
            "price": 7750.7027027027,
            "reset_lots": True,
        },
        {
            **common,
            "time": datetime(2026, 8, 28, 15, 5, tzinfo=timezone.utc),
            "side": "SLD",
            "quantity": 111,
            "price": 7771.0585585586,
            "commission": 67.71,
            "reset_lots": False,
        },
    ]

    result = _realized_pnl_rows(rows)

    assert len(result) == 1
    assert result[0]["quantity"] == 111
    assert result[0]["entry_price"] == rows[1]["price"]
    assert result[0]["exit_price"] == rows[2]["price"]
    assert round(result[0]["realized_pnl"], 2) == 11_229.79
