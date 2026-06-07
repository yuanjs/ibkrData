"""Unit tests for backfiller.progress_store — JSON checkpoint persistence."""
import json
import tempfile
from pathlib import Path

import pytest

from backfiller.progress_store import ProgressStore

WindowType = tuple[str, str]


def _write_json(path: Path, data: dict) -> None:
    """Helper — write a JSON dict directly, bypassing ProgressStore.save()."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data))


# ---------------------------------------------------------------------------
# save / load
# ---------------------------------------------------------------------------


def test_save_and_load():
    windows: list[WindowType] = [("2024-01-01", "2024-01-03"),
                                 ("2024-01-05", "2024-01-10")]
    with tempfile.TemporaryDirectory() as tmp:
        store = ProgressStore(Path(tmp))
        store.save("SPI", windows)
        loaded = store.load("SPI")
    assert loaded == windows


def test_save_overwrites_previous():
    windows1: list[WindowType] = [("2024-01-01", "2024-01-03")]
    windows2: list[WindowType] = [("2024-02-01", "2024-02-10")]
    with tempfile.TemporaryDirectory() as tmp:
        store = ProgressStore(Path(tmp))
        store.save("SPI", windows1)
        store.save("SPI", windows2)
        loaded = store.load("SPI")
    assert loaded == windows2


# ---------------------------------------------------------------------------
# mark_completed
# ---------------------------------------------------------------------------


def test_mark_completed():
    windows: list[WindowType] = [
        ("2024-01-01", "2024-01-03"),
        ("2024-01-05", "2024-01-10"),
        ("2024-01-12", "2024-01-15"),
    ]
    with tempfile.TemporaryDirectory() as tmp:
        store = ProgressStore(Path(tmp))
        store.save("MNQ", windows)

        store.mark_completed("MNQ", ("2024-01-05", "2024-01-10"))

        remaining = store.load("MNQ")
    assert remaining == [("2024-01-01", "2024-01-03"),
                         ("2024-01-12", "2024-01-15")]


def test_mark_completed_nonexistent_window():
    """mark_completed with a window not in the list is a no-op (no crash)."""
    windows: list[WindowType] = [("2024-01-01", "2024-01-03")]
    with tempfile.TemporaryDirectory() as tmp:
        store = ProgressStore(Path(tmp))
        store.save("MNQ", windows)
        store.mark_completed("MNQ", ("2099-01-01", "2099-01-02"))
        assert store.load("MNQ") == windows


def test_task_windows_round_trip():
    windows: list[WindowType] = [
        ("2024-03-01", "2024-03-02"),
        ("2024-03-03", "2024-03-04"),
    ]
    with tempfile.TemporaryDirectory() as tmp:
        store = ProgressStore(Path(tmp))
        store.save_task_windows("SPI", "FUT:123:202403", windows)

        assert store.has_task("SPI", "FUT:123:202403") is True
        assert store.load_task_windows("SPI", "FUT:123:202403") == windows


def test_mark_task_completed_only_updates_task():
    with tempfile.TemporaryDirectory() as tmp:
        store = ProgressStore(Path(tmp))
        store.save("SPI", [("2024-01-01", "2024-01-02")])
        store.save_task_windows("SPI", "FUT:123:202403", [
            ("2024-03-01", "2024-03-02"),
            ("2024-03-03", "2024-03-04"),
        ])

        store.mark_task_completed(
            "SPI",
            "FUT:123:202403",
            ("2024-03-01", "2024-03-02"),
        )

        assert store.load("SPI") == [("2024-01-01", "2024-01-02")]
        assert store.load_task_windows("SPI", "FUT:123:202403") == [
            ("2024-03-03", "2024-03-04"),
        ]


def test_missing_task_is_not_complete_checkpoint():
    with tempfile.TemporaryDirectory() as tmp:
        store = ProgressStore(Path(tmp))
        assert store.has_task("SPI", "FUT:123:202403") is False
        assert store.load_task_windows("SPI", "FUT:123:202403") == []


# ---------------------------------------------------------------------------
# is_complete
# ---------------------------------------------------------------------------


def test_is_complete_no_file():
    with tempfile.TemporaryDirectory() as tmp:
        store = ProgressStore(Path(tmp))
        assert store.is_complete("SPI") is True


def test_is_complete_with_remaining():
    with tempfile.TemporaryDirectory() as tmp:
        store = ProgressStore(Path(tmp))
        store.save("SPI", [("2024-01-01", "2024-01-03")])
        assert store.is_complete("SPI") is False


def test_is_complete_all_cleared():
    with tempfile.TemporaryDirectory() as tmp:
        store = ProgressStore(Path(tmp))
        store.save("SPI", [])
        assert store.is_complete("SPI") is True


# ---------------------------------------------------------------------------
# known_symbols
# ---------------------------------------------------------------------------


def test_known_symbols():
    with tempfile.TemporaryDirectory() as tmp:
        store = ProgressStore(Path(tmp))
        store.save("SPI", [("2024-01-01", "2024-01-03")])
        store.save("MNQ", [("2024-02-01", "2024-02-10")])
        assert store.known_symbols() == {"SPI", "MNQ"}


def test_known_symbols_empty():
    with tempfile.TemporaryDirectory() as tmp:
        store = ProgressStore(Path(tmp))
        assert store.known_symbols() == set()


# ---------------------------------------------------------------------------
# clear
# ---------------------------------------------------------------------------


def test_clear_removes_file():
    with tempfile.TemporaryDirectory() as tmp:
        store = ProgressStore(Path(tmp))
        store.save("SPI", [("2024-01-01", "2024-01-03")])
        assert (Path(tmp) / "SPI.json").exists()

        store.clear("SPI")
        assert not (Path(tmp) / "SPI.json").exists()
        assert store.load("SPI") == []


def test_clear_nonexistent_is_noop():
    """Clearing a symbol with no checkpoint doesn't crash."""
    with tempfile.TemporaryDirectory() as tmp:
        store = ProgressStore(Path(tmp))
        store.clear("UNKNOWN")  # should not raise


# ---------------------------------------------------------------------------
# Edge cases — missing / corrupted file
# ---------------------------------------------------------------------------


def test_load_missing_file_returns_empty():
    with tempfile.TemporaryDirectory() as tmp:
        store = ProgressStore(Path(tmp))
        assert store.load("NONEXISTENT") == []


def test_load_corrupted_file_returns_empty(caplog: pytest.LogCaptureFixture):
    with tempfile.TemporaryDirectory() as tmp:
        bad_file = Path(tmp) / "BROKEN.json"
        bad_file.write_text("this is not json")
        store = ProgressStore(Path(tmp))
        result = store.load("BROKEN")
    assert result == []
    assert len(caplog.records) >= 1
    assert "BROKEN" in caplog.text


def test_load_partial_corruption_non_list_remaining(caplog: pytest.LogCaptureFixture):
    """When 'remaining' is not a list, load() returns empty and warns."""
    with tempfile.TemporaryDirectory() as tmp:
        _write_json(Path(tmp) / "BAD.json", {"remaining": "not-a-list"})
        store = ProgressStore(Path(tmp))
        result = store.load("BAD")
    assert result == []
    assert "not a list" in caplog.text


def test_load_skips_malformed_windows(caplog: pytest.LogCaptureFixture):
    """Elements with wrong type/size are skipped individually."""
    with tempfile.TemporaryDirectory() as tmp:
        _write_json(Path(tmp) / "MIXED.json", {
            "remaining": [
                ["2024-01-01", "2024-01-03"],  # valid
                ["only-one"],                   # malformed (len=1)
                [1, 2],                         # malformed (not strings)
                {"a": 1},                       # malformed (not a list)
            ]
        })
        store = ProgressStore(Path(tmp))
        result = store.load("MIXED")
    assert result == [("2024-01-01", "2024-01-03")]
    assert len(caplog.records) == 3
