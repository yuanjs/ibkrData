import sys
from pathlib import Path


COLLECTOR_ROOT = str(Path(__file__).resolve().parents[1])
if COLLECTOR_ROOT not in sys.path:
    sys.path.insert(0, COLLECTOR_ROOT)

# API tests import api/config.py under the top-level name "config". Collector
# modules also import "config", so clear the cached module before collector
# tests import their local modules.
sys.modules.pop("config", None)
