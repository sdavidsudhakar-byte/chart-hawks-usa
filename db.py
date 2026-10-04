"""
db.py — Database router.

Reads DB_MODE from config.py and delegates to the correct backend:
  DB_MODE = 'neon'  → db_neon.py  (Neon PostgreSQL, default)
  DB_MODE = 'local' → db_local.py (local SQLite, zero Neon transfer)

To switch modes: edit config.py and restart the app. No other changes needed.
"""

import sys as _sys

try:
    from config import DB_MODE as _DB_MODE
except ImportError:
    _DB_MODE = "neon"

if _DB_MODE == "local":
    import db_local as _backend
    print("[db] LOCAL mode — SQLite (local.db)", file=_sys.stderr)
else:
    import db_neon as _backend
    print("[db] NEON mode — PostgreSQL", file=_sys.stderr)

# Re-export everything from the chosen backend so callers can do:
#   import db; db.get_conn() / db.upsert_tickers() / etc.
from importlib import import_module as _im
_mod = _im("db_local" if _DB_MODE == "local" else "db_neon")
_sys.modules[__name__].__dict__.update(
    {k: v for k, v in vars(_mod).items() if not k.startswith("__")}
)
