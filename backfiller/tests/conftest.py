"""pytest configuration for backfiller tests.

ib_insync's eventkit requires an event loop at import time.
This workaround mirrors the same pattern used in get_history.py.
"""

import asyncio

try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())
