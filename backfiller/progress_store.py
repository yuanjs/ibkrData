"""JSON checkpoint persistence for backfiller progress.

Each symbol gets its own ``progress/<symbol>.json`` file with the structure::

    {"remaining": [["2024-01-01", "2024-01-03"], ...], "errors": []}
"""

import json
import logging
from pathlib import Path
from typing import Union

logger = logging.getLogger(__name__)

Window = tuple[str, str]


class ProgressStore:
    """Persist and restore remaining window lists per symbol."""

    def __init__(self, progress_dir: Union[str, Path]) -> None:
        self._dir = Path(progress_dir)
        self._dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # public API
    # ------------------------------------------------------------------

    def save(self, symbol: str, windows: list[Window]) -> None:
        """Overwrite the remaining window list for *symbol*."""
        data = {"remaining": [list(w) for w in windows], "errors": []}
        self._path(symbol).write_text(json.dumps(data))

    def load(self, symbol: str) -> list[Window]:
        """Return outstanding windows.  Empty list = fully caught up."""
        path = self._path(symbol)
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text())
            raw = data.get("remaining", [])
            return [tuple(r) for r in raw]  # type: ignore[return-value]
        except (json.JSONDecodeError, OSError, TypeError) as exc:
            logger.warning("Corrupted checkpoint %s: %s", path, exc)
            return []

    def mark_completed(self, symbol: str, window: Window) -> None:
        """Remove *window* from the remaining list and persist."""
        windows = self.load(symbol)
        try:
            windows.remove(window)
        except ValueError:
            pass  # window already absent — nothing to do
        self.save(symbol, windows)

    def is_complete(self, symbol: str) -> bool:
        """True when no remaining windows (or no checkpoint file)."""
        return len(self.load(symbol)) == 0

    def known_symbols(self) -> set[str]:
        """Return the set of symbols that have checkpoint files."""
        return {p.stem for p in self._dir.glob("*.json")}

    def clear(self, symbol: str) -> None:
        """Delete checkpoint — forces a full re-pull on next run."""
        path = self._path(symbol)
        if path.exists():
            path.unlink()

    # ------------------------------------------------------------------
    # internal helpers
    # ------------------------------------------------------------------

    def _path(self, symbol: str) -> Path:
        return self._dir / f"{symbol}.json"
