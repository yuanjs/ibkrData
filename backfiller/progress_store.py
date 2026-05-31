"""JSON checkpoint persistence for backfiller progress.

Each symbol gets its own ``progress/<symbol>.json`` file with the structure::

    {"remaining": [["2024-01-01", "2024-01-03"], ...]}

Concurrent-safe: save() uses a temporary file + atomic rename on POSIX.
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
        """Overwrite the remaining window list for *symbol*.

        Atomic write: data is written to a ``.tmp`` file first, then renamed
        to the final path (atomic on POSIX when on the same filesystem).
        """
        data = {"remaining": [list(w) for w in windows]}
        path = self._path(symbol)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False))
        tmp.rename(path)

    def load(self, symbol: str) -> list[Window]:
        """Return outstanding windows.  Empty list = fully caught up."""
        path = self._path(symbol)
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text())
            raw = data.get("remaining", [])
            if not isinstance(raw, list):
                logger.warning("Corrupted checkpoint %s: 'remaining' is not a list", path)
                return []
            result: list[Window] = []
            for r in raw:
                if isinstance(r, list) and len(r) == 2 and all(isinstance(v, str) for v in r):
                    result.append(tuple(r))
                else:
                    logger.warning("Skipping malformed window in %s: %s", path, r)
            return result
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
