# ══════════════════════════════════════════════════════════════════════════════
#  CHART HAWKS USA — User Configuration Template
#  Copy this file to config.py and fill in your real values.
#  NEVER commit config.py — it is in .gitignore.
# ══════════════════════════════════════════════════════════════════════════════

# ── Database mode ──────────────────────────────────────────────────────────────
# Set to 'local' to use local SQLite (local.db) — zero Neon network transfer.
# Set to 'neon'  to use Neon PostgreSQL (DATABASE_URL in .env).
DB_MODE = "local"

# ── Server Ports ──────────────────────────────────────────────────────────────
SCANNER_PORT  = 8000
TRADER_PORT   = 8001
SIM_PORT      = 8002
