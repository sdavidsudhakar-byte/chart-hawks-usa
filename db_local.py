"""
db_local.py — SQLite-backed drop-in replacement for db.py.

Activated when config.DB_MODE == 'local'.
Reads from/writes to local.db in the project directory.

All public function signatures match db.py exactly so callers need zero changes.
SQL is translated from psycopg2 style (%s, Postgres types) to sqlite3 style (?, TEXT).
"""

import sqlite3
import json
import os
import re
import threading
import time

_SQLITE_PATH = os.path.join(os.path.dirname(__file__), "local.db")

# Thread-local connection — sqlite3 connections can't be shared across threads
_local = threading.local()


def _pg_to_sqlite(sql: str, params) -> tuple:
    """
    Translate Postgres-specific SQL constructs to SQLite equivalents.
    Handles the common patterns used across main.py, rs.py, stock_rs.py.
    """
    # %% → % (escaped literal percent), then %s → ?
    sql = sql.replace("%%", "\x00PCTPCT\x00").replace("%s", "?").replace("\x00PCTPCT\x00", "%")

    # date::text → date  (SQLite stores dates as TEXT already)
    sql = re.sub(r"(\bdate)\s*::\s*text", r"\1", sql, flags=re.IGNORECASE)

    # ::date cast on a param — e.g. (?::date - INTERVAL '120 days') → date(?, '-120 days')
    # Pattern: (?::date - INTERVAL 'N days') → date(?, '-N days')
    def _interval_sub(m):
        n = m.group(1)
        return f"date(?, '-{n} days')"
    sql = re.sub(r"\?\s*::\s*date\s*-\s*INTERVAL\s*'(\d+)\s*days'", _interval_sub, sql, flags=re.IGNORECASE)

    # Standalone INTERVAL subqueries: MAX(date) - INTERVAL 'N days' → date(MAX(date), '-N days')
    def _interval_max(m):
        expr = m.group(1).strip()
        n = m.group(2)
        return f"date({expr}, '-{n} days')"
    sql = re.sub(r"([\w().'`\"]+)\s*-\s*INTERVAL\s*'(\d+)\s*days'", _interval_max, sql, flags=re.IGNORECASE)

    # NOW() → date('now')
    sql = re.sub(r"\bNOW\s*\(\s*\)", "date('now')", sql, flags=re.IGNORECASE)

    # = ANY(%s) / = ANY(?) with a list param → IN (?,?,...)
    # This requires expanding the list param into multiple ? placeholders
    any_match = re.search(r"=\s*ANY\s*\(\s*\?\s*\)", sql, flags=re.IGNORECASE)
    if any_match and params:
        params = list(params)
        # Find the positional index of the ANY(?) — count ?s before it
        pre = sql[:any_match.start()]
        param_idx = pre.count("?")
        if param_idx < len(params) and isinstance(params[param_idx], (list, tuple)):
            lst = params[param_idx]
            placeholders = ",".join(["?"] * len(lst))
            sql = sql[:any_match.start()] + f"IN ({placeholders})" + sql[any_match.end():]
            params = params[:param_idx] + list(lst) + params[param_idx + 1:]

    # LIKE 'NSE:%%' (after %% already handled) is for index filter — keep as-is
    # (SQLite LIKE works the same way with % wildcard)

    return sql, tuple(params) if not isinstance(params, tuple) else params


class _CursorCM:
    """Wraps sqlite3.Cursor so it can be used as a context manager (like psycopg2)."""
    def __init__(self, cur):
        self._cur = cur

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self._cur.close()

    # Delegate everything else to the real cursor
    def execute(self, sql, params=()):
        sql, params = _pg_to_sqlite(sql, params)
        self._cur.execute(sql, params)
        return self

    def executemany(self, sql, seq):
        sql, _ = _pg_to_sqlite(sql, ())
        self._cur.executemany(sql, seq)
        return self

    def fetchone(self):  return self._cur.fetchone()
    def fetchall(self):  return self._cur.fetchall()
    def fetchmany(self, n): return self._cur.fetchmany(n)
    def close(self):    self._cur.close()

    @property
    def rowcount(self): return self._cur.rowcount
    @property
    def description(self): return self._cur.description
    @property
    def lastrowid(self): return self._cur.lastrowid

    def __iter__(self): return iter(self._cur)


class _ConnWrapper:
    """Wraps sqlite3.Connection so conn.cursor() returns a _CursorCM."""
    def __init__(self, conn):
        self._conn = conn

    def cursor(self):
        return _CursorCM(self._conn.cursor())

    # Delegate common methods
    def execute(self, sql, params=()):
        sql, params = _pg_to_sqlite(sql, params)
        return self._conn.execute(sql, params)

    def executemany(self, sql, seq):
        sql, _ = _pg_to_sqlite(sql, ())
        return self._conn.executemany(sql, seq)

    def executescript(self, s): return self._conn.executescript(s)
    def commit(self):           return self._conn.commit()
    def rollback(self):         return self._conn.rollback()
    def close(self):            return self._conn.close()

    @property
    def total_changes(self):    return self._conn.total_changes

    @property
    def row_factory(self):          return self._conn.row_factory
    @row_factory.setter
    def row_factory(self, v):       self._conn.row_factory = v

    def __enter__(self):            return self
    def __exit__(self, *a):         return self._conn.__exit__(*a)


def _get_conn() -> _ConnWrapper:
    if not hasattr(_local, "conn") or _local.conn is None:
        conn = sqlite3.connect(_SQLITE_PATH, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.row_factory = sqlite3.Row
        _local.conn = _ConnWrapper(conn)
    return _local.conn


# Public API — match db.py signatures
def get_conn():
    return _get_conn()


def release_conn(conn):
    pass  # No pooling needed for SQLite


def init_db():
    """Create tables if not present. Mirrors db.py DDL but in SQLite syntax."""
    conn = _get_conn()
    # Pre-migrate: drop old scanner tables before CREATE TABLE IF NOT EXISTS runs,
    # so the new schema (with direction column + UNIQUE constraint) is applied cleanly.
    sr_cols = {r[1] for r in conn.execute("PRAGMA table_info(scanner_results)").fetchall()}
    if sr_cols and "direction" not in sr_cols:
        conn.execute("DROP TABLE IF EXISTS scanner_results")
        conn.commit()
    srl_cols = {r[1] for r in conn.execute("PRAGMA table_info(scanner_run_log)").fetchall()}
    if srl_cols and "hits_bull" not in srl_cols:
        conn.execute("DROP TABLE IF EXISTS scanner_run_log")
        conn.commit()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS tickers (
        symbol          TEXT PRIMARY KEY,
        company_name    TEXT,
        macro           TEXT,
        sector          TEXT,
        industry        TEXT,
        basic_industry  TEXT,
        index_list      TEXT DEFAULT '[]',
        fetch_failures  INTEGER DEFAULT 0,
        refreshed_at    TEXT,
        is_active       INTEGER DEFAULT 1
    );

    CREATE TABLE IF NOT EXISTS refresh_log (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        started_at      TEXT DEFAULT (datetime('now')),
        finished_at     TEXT,
        total_symbols   INTEGER,
        success_count   INTEGER,
        failed_symbols  TEXT DEFAULT '[]',
        status          TEXT DEFAULT 'running'
    );

    CREATE TABLE IF NOT EXISTS app_state (
        key        TEXT PRIMARY KEY,
        value      TEXT,
        updated_at TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS scanner_results (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol           TEXT    NOT NULL,
        company_name     TEXT,
        macro            TEXT,
        sector           TEXT,
        industry         TEXT,
        basic_industry   TEXT,
        direction        TEXT    NOT NULL DEFAULT 'bull',
        m_date           TEXT,
        m_price          REAL,
        bull_cross_date  TEXT,
        s_pullback_date  TEXT,
        c_pullback_date  TEXT,
        above_200sma     INTEGER DEFAULT 0,
        scanned_at       TEXT    DEFAULT (datetime('now')),
        UNIQUE (symbol, direction)
    );
    CREATE INDEX IF NOT EXISTS idx_scan_direction  ON scanner_results (direction);
    CREATE INDEX IF NOT EXISTS idx_scan_scanned_at ON scanner_results (scanned_at);
    CREATE INDEX IF NOT EXISTS idx_scan_macro      ON scanner_results (macro);
    CREATE INDEX IF NOT EXISTS idx_scan_sector     ON scanner_results (sector);
    CREATE INDEX IF NOT EXISTS idx_scan_industry   ON scanner_results (industry);
    CREATE INDEX IF NOT EXISTS idx_scan_bi         ON scanner_results (basic_industry);

    CREATE TABLE IF NOT EXISTS scan_signals (
        symbol           TEXT    NOT NULL,
        direction        TEXT    NOT NULL,
        step1_date       TEXT,
        m_date           TEXT,
        m_price          REAL,
        daily_cross_date TEXT,
        above_200sma     INTEGER DEFAULT 0,
        phase_end_date   TEXT,
        hourly_signals   TEXT    DEFAULT '[]',
        wh_count         INTEGER DEFAULT 0,
        hx_count         INTEGER DEFAULT 0,
        latest_hx_ts     INTEGER,
        computed_at      TEXT    DEFAULT (datetime('now')),
        PRIMARY KEY (symbol, direction)
    );
    CREATE INDEX IF NOT EXISTS idx_ss_direction  ON scan_signals (direction);
    CREATE INDEX IF NOT EXISTS idx_ss_latest_hx  ON scan_signals (latest_hx_ts DESC);
    CREATE INDEX IF NOT EXISTS idx_ss_computed   ON scan_signals (computed_at);

    CREATE TABLE IF NOT EXISTS scanner_run_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        started_at  TEXT DEFAULT (datetime('now')),
        finished_at TEXT,
        scanned     INTEGER,
        hits_bull   INTEGER DEFAULT 0,
        hits_bear   INTEGER DEFAULT 0,
        errors      INTEGER,
        status      TEXT DEFAULT 'running'
    );



    CREATE TABLE IF NOT EXISTS ohlcv_daily (
        symbol  TEXT    NOT NULL,
        date    TEXT    NOT NULL,
        open    REAL,
        high    REAL,
        low     REAL,
        close   REAL,
        volume  INTEGER,
        PRIMARY KEY (symbol, date)
    );
    CREATE INDEX IF NOT EXISTS idx_ohlcv_daily_symbol   ON ohlcv_daily (symbol);
    CREATE INDEX IF NOT EXISTS idx_ohlcv_daily_date     ON ohlcv_daily (date);
    CREATE INDEX IF NOT EXISTS idx_ohlcv_daily_sym_date ON ohlcv_daily (symbol, date);

    CREATE TABLE IF NOT EXISTS ohlcv_intraday (
        symbol  TEXT    NOT NULL,
        ts      INTEGER NOT NULL,
        open    REAL,
        high    REAL,
        low     REAL,
        close   REAL,
        volume  INTEGER,
        PRIMARY KEY (symbol, ts)
    );
    CREATE INDEX IF NOT EXISTS idx_ohlcv_intraday_symbol ON ohlcv_intraday (symbol);
    CREATE INDEX IF NOT EXISTS idx_ohlcv_intraday_ts     ON ohlcv_intraday (ts);

    CREATE TABLE IF NOT EXISTS index_rs (
        symbol            TEXT    NOT NULL,
        date              TEXT    NOT NULL,
        rs_raw            REAL,
        rs_rating         INTEGER,
        mid_score         INTEGER,
        short_score       INTEGER,
        group_short_score INTEGER,
        group_mid_score   INTEGER,
        group_lt_score    INTEGER,
        PRIMARY KEY (symbol, date)
    );
    CREATE INDEX IF NOT EXISTS idx_index_rs_date        ON index_rs (date);
    CREATE INDEX IF NOT EXISTS idx_index_rs_symbol      ON index_rs (symbol);
    CREATE INDEX IF NOT EXISTS idx_index_rs_sym_date    ON index_rs (symbol, date);

    CREATE TABLE IF NOT EXISTS stock_rs_daily (
        symbol      TEXT    NOT NULL,
        date        TEXT    NOT NULL,
        rs21        REAL,
        rs21_rank   INTEGER,
        rs55        REAL,
        rs55_rank   INTEGER,
        day_return  REAL,
        rs_line     REAL,
        rs252       REAL,
        rs252_rank  INTEGER,
        PRIMARY KEY (symbol, date)
    );
    CREATE INDEX IF NOT EXISTS idx_stock_rs_date        ON stock_rs_daily (date);
    CREATE INDEX IF NOT EXISTS idx_stock_rs_symbol      ON stock_rs_daily (symbol);
    CREATE INDEX IF NOT EXISTS idx_stock_rs_sym_date    ON stock_rs_daily (symbol, date);

    CREATE TABLE IF NOT EXISTS ohlcv_weekly (
        symbol  TEXT    NOT NULL,
        week    TEXT    NOT NULL,
        open    REAL,
        high    REAL,
        low     REAL,
        close   REAL,
        volume  INTEGER,
        PRIMARY KEY (symbol, week)
    );
    CREATE INDEX IF NOT EXISTS idx_ohlcv_weekly_symbol ON ohlcv_weekly (symbol);
    CREATE INDEX IF NOT EXISTS idx_ohlcv_weekly_week   ON ohlcv_weekly (week);

    CREATE TABLE IF NOT EXISTS indicator_daily (
        symbol   TEXT  NOT NULL,
        date     TEXT  NOT NULL,
        ema20    REAL,
        ema50    REAL,
        sma200   REAL,
        rs_line  REAL,
        PRIMARY KEY (symbol, date)
    );
    CREATE INDEX IF NOT EXISTS idx_ind_daily_symbol   ON indicator_daily (symbol);
    CREATE INDEX IF NOT EXISTS idx_ind_daily_date     ON indicator_daily (date);
    CREATE INDEX IF NOT EXISTS idx_ind_daily_sym_date ON indicator_daily (symbol, date);

    CREATE TABLE IF NOT EXISTS indicator_weekly (
        symbol   TEXT  NOT NULL,
        week     TEXT  NOT NULL,
        ema20    REAL,
        ema50    REAL,
        sma200   REAL,
        rs_line  REAL,
        PRIMARY KEY (symbol, week)
    );
    CREATE INDEX IF NOT EXISTS idx_ind_weekly_symbol ON indicator_weekly (symbol);
    CREATE INDEX IF NOT EXISTS idx_ind_weekly_week   ON indicator_weekly (week);

    CREATE TABLE IF NOT EXISTS indicator_hourly (
        symbol   TEXT    NOT NULL,
        ts       INTEGER NOT NULL,
        ema20    REAL,
        ema50    REAL,
        sma200   REAL,
        rs_line  REAL,
        PRIMARY KEY (symbol, ts)
    );
    CREATE INDEX IF NOT EXISTS idx_ind_hourly_symbol ON indicator_hourly (symbol);
    CREATE INDEX IF NOT EXISTS idx_ind_hourly_ts     ON indicator_hourly (ts);

    CREATE TABLE IF NOT EXISTS sector_index_map (
        symbol        TEXT NOT NULL,
        index_name    TEXT NOT NULL,
        index_symbol  TEXT NOT NULL,
        source        TEXT NOT NULL,
        confidence    TEXT NOT NULL,
        PRIMARY KEY (symbol, index_name)
    );
    CREATE INDEX IF NOT EXISTS idx_sim_symbol     ON sector_index_map (symbol);
    CREATE INDEX IF NOT EXISTS idx_sim_index_name ON sector_index_map (index_name);

    CREATE TABLE IF NOT EXISTS watchlist (
        symbol    TEXT PRIMARY KEY,
        added_at  TEXT NOT NULL DEFAULT (datetime('now'))
    );

    -- id-keyed: one row per TRADE (multiple trades per symbol allowed).
    -- Migrated from the old symbol-PK schema; see journal_entries_old for the
    -- pre-migration copy retained as a safety net.
    CREATE TABLE IF NOT EXISTS journal_entries (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol        TEXT NOT NULL,
        status        TEXT NOT NULL DEFAULT 'open',
        snapshot_json TEXT NOT NULL,
        plan_json     TEXT NOT NULL DEFAULT '{}',
        created_at    TEXT NOT NULL DEFAULT (datetime('now')),
        updated_at    TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_journal_symbol ON journal_entries(symbol);
    CREATE INDEX IF NOT EXISTS idx_journal_status ON journal_entries(status);
    """)
    conn.commit()
    # Migrate existing stock_rs_daily tables that predate rs55 columns
    existing = {r[1] for r in conn.execute("PRAGMA table_info(stock_rs_daily)").fetchall()}
    if "rs55" not in existing:
        conn.execute("ALTER TABLE stock_rs_daily ADD COLUMN rs55      REAL")
        conn.execute("ALTER TABLE stock_rs_daily ADD COLUMN rs55_rank INTEGER")
        conn.commit()
    if "day_return" not in existing:
        conn.execute("ALTER TABLE stock_rs_daily ADD COLUMN day_return REAL")
        conn.commit()
    if "rs252" not in existing:
        conn.execute("ALTER TABLE stock_rs_daily ADD COLUMN rs252      REAL")
        conn.execute("ALTER TABLE stock_rs_daily ADD COLUMN rs252_rank INTEGER")
        conn.commit()
    # Migrate sector_index_map PK from (symbol, index_symbol) → (symbol, index_name)
    # Check by inspecting the PRIMARY KEY via index list
    pk_cols = {r[2] for r in conn.execute("PRAGMA index_info('sqlite_autoindex_sector_index_map_1')").fetchall()}
    if "index_symbol" in pk_cols:
        conn.execute("DROP TABLE IF EXISTS sector_index_map")
        conn.execute("""
            CREATE TABLE sector_index_map (
                symbol        TEXT NOT NULL,
                index_name    TEXT NOT NULL,
                index_symbol  TEXT NOT NULL,
                source        TEXT NOT NULL,
                confidence    TEXT NOT NULL,
                PRIMARY KEY (symbol, index_name)
            )""")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sim_symbol     ON sector_index_map (symbol)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_sim_index_name ON sector_index_map (index_name)")
        conn.commit()
    # Migrate scanner_results (Daily Hunt): add m_price column if missing
    sr_cols = {r[1] for r in conn.execute("PRAGMA table_info(scanner_results)").fetchall()}
    if "m_price" not in sr_cols:
        conn.execute("ALTER TABLE scanner_results ADD COLUMN m_price REAL")
        conn.commit()
    # Migrate tickers: add is_active flag if missing (1=active, 0=delisted)
    nt_cols = {r[1] for r in conn.execute("PRAGMA table_info(tickers)").fetchall()}
    if "is_active" not in nt_cols:
        conn.execute("ALTER TABLE tickers ADD COLUMN is_active INTEGER DEFAULT 1")
        conn.commit()


# ── Indicator tables: upsert + fetch ─────────────────────────────────────────

def upsert_ohlcv_weekly(rows: list[dict]):
    """rows: [{symbol, week, open, high, low, close, volume}]"""
    if not rows:
        return
    conn = _get_conn()
    conn.executemany(
        """INSERT INTO ohlcv_weekly (symbol,week,open,high,low,close,volume)
           VALUES (?,?,?,?,?,?,?)
           ON CONFLICT(symbol,week) DO UPDATE SET
               open=excluded.open, high=excluded.high, low=excluded.low,
               close=excluded.close, volume=excluded.volume""",
        [(r["symbol"],r["week"],r["open"],r["high"],r["low"],r["close"],r["volume"]) for r in rows]
    )
    conn.commit()


def upsert_indicator_daily(rows: list[dict]):
    """rows: [{symbol, date, ema20, ema50, sma200, rs_line}]"""
    if not rows:
        return
    conn = _get_conn()
    conn.executemany(
        """INSERT INTO indicator_daily (symbol,date,ema20,ema50,sma200,rs_line)
           VALUES (?,?,?,?,?,?)
           ON CONFLICT(symbol,date) DO UPDATE SET
               ema20=excluded.ema20, ema50=excluded.ema50,
               sma200=excluded.sma200, rs_line=excluded.rs_line""",
        [(r["symbol"],r["date"],r.get("ema20"),r.get("ema50"),r.get("sma200"),r.get("rs_line")) for r in rows]
    )
    conn.commit()


def upsert_indicator_weekly(rows: list[dict]):
    """rows: [{symbol, week, ema20, ema50, sma200, rs_line}]"""
    if not rows:
        return
    conn = _get_conn()
    conn.executemany(
        """INSERT INTO indicator_weekly (symbol,week,ema20,ema50,sma200,rs_line)
           VALUES (?,?,?,?,?,?)
           ON CONFLICT(symbol,week) DO UPDATE SET
               ema20=excluded.ema20, ema50=excluded.ema50,
               sma200=excluded.sma200, rs_line=excluded.rs_line""",
        [(r["symbol"],r["week"],r.get("ema20"),r.get("ema50"),r.get("sma200"),r.get("rs_line")) for r in rows]
    )
    conn.commit()


def upsert_indicator_hourly(rows: list[dict]):
    """rows: [{symbol, ts, ema20, ema50, sma200, rs_line}]"""
    if not rows:
        return
    conn = _get_conn()
    conn.executemany(
        """INSERT INTO indicator_hourly (symbol,ts,ema20,ema50,sma200,rs_line)
           VALUES (?,?,?,?,?,?)
           ON CONFLICT(symbol,ts) DO UPDATE SET
               ema20=excluded.ema20, ema50=excluded.ema50,
               sma200=excluded.sma200, rs_line=excluded.rs_line""",
        [(r["symbol"],r["ts"],r.get("ema20"),r.get("ema50"),r.get("sma200"),r.get("rs_line")) for r in rows]
    )
    conn.commit()


# ── Helpers ───────────────────────────────────────────────────────────────────

_SECTOR_NORMALISE = {s.upper(): s for s in [
    "Automobile and Auto Components", "Capital Goods", "Chemicals",
    "Construction", "Construction Materials", "Consumer Durables",
    "Consumer Services", "Diversified", "Fast Moving Consumer Goods",
    "Financial Services", "Forest Materials", "Healthcare",
    "Information Technology", "Media Entertainment & Publication",
    "Metals & Mining", "Oil Gas & Consumable Fuels", "Power",
    "Realty", "Services", "Telecommunication", "Textiles", "Utilities",
]}


def _normalise_sector(val: str) -> str:
    return _SECTOR_NORMALISE.get((val or "").strip().upper(), (val or "").strip())


def _arr_to_json(v):
    if v is None:
        return "[]"
    if isinstance(v, list):
        return json.dumps(v)
    return str(v)


def _json_to_list(v):
    if not v:
        return []
    try:
        return json.loads(v)
    except Exception:
        return []


# ── Ticker functions ──────────────────────────────────────────────────────────

def upsert_tickers(rows: list[dict]):
    if not rows:
        return
    for r in rows:
        r["sector"] = _normalise_sector(r.get("sector", ""))
    conn = _get_conn()
    conn.executemany(
        """INSERT INTO tickers (symbol,company_name,macro,sector,industry,basic_industry,index_list,fetch_failures,refreshed_at,is_active)
           VALUES (?,?,?,?,?,?,?,0,datetime('now'),1)
           ON CONFLICT(symbol) DO UPDATE SET
             company_name=excluded.company_name,
             macro=excluded.macro, sector=excluded.sector, industry=excluded.industry,
             basic_industry=excluded.basic_industry, index_list=excluded.index_list,
             refreshed_at=datetime('now'), is_active=1""",
        [(r["symbol"], r.get("company_name"),
          r.get("macro"), r.get("sector"), r.get("industry"), r.get("basic_industry"),
          _arr_to_json(r.get("index_list")))
         for r in rows]
    )
    conn.commit()


def mark_all_tickers_inactive():
    """Mark every ticker is_active=0 before a refresh run. Seen symbols get is_active=1 via upsert_tickers."""
    conn = _get_conn()
    conn.execute("UPDATE tickers SET is_active=0")
    conn.commit()


def get_ticker_count():
    conn = _get_conn()
    return conn.execute("SELECT COUNT(*) FROM tickers").fetchone()[0]


def get_fetch_failures(symbols: list[str]) -> dict[str, int]:
    if not symbols:
        return {}
    conn = _get_conn()
    ph = ",".join(["?"] * len(symbols))
    rows = conn.execute(
        f"SELECT symbol, COALESCE(fetch_failures,0) FROM tickers WHERE symbol IN ({ph})",
        symbols
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def increment_fetch_failure(symbol: str):
    conn = _get_conn()
    conn.execute(
        "UPDATE tickers SET fetch_failures=COALESCE(fetch_failures,0)+1 WHERE symbol=?",
        (symbol,)
    )
    conn.commit()


def reset_fetch_failure(symbol: str):
    conn = _get_conn()
    conn.execute(
        "UPDATE tickers SET fetch_failures=0 WHERE symbol=? AND fetch_failures>0",
        (symbol,)
    )
    conn.commit()


def get_all_tickers_with_index(index_name: str) -> list[dict]:
    conn = _get_conn()
    rows = conn.execute(
        "SELECT symbol, company_name, macro, sector, industry, basic_industry, index_list "
        "FROM tickers WHERE index_list LIKE ?",
        (f'%"{index_name}"%',)
    ).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["index_list"] = _json_to_list(d.get("index_list"))
        result.append(d)
    return result


# ── Refresh log ───────────────────────────────────────────────────────────────

def log_refresh_start() -> int:
    conn = _get_conn()
    cur = conn.execute("INSERT INTO refresh_log (status) VALUES ('running')")
    conn.commit()
    return cur.lastrowid


def log_refresh_finish(log_id: int | None, total: int, success: int, failed: list[str]):
    if log_id is None:
        return
    status = "done" if not failed else "done_with_errors"
    conn = _get_conn()
    conn.execute(
        "UPDATE refresh_log SET finished_at=datetime('now'), total_symbols=?, success_count=?, failed_symbols=?, status=? WHERE id=?",
        (total, success, json.dumps(failed), status, log_id)
    )
    conn.commit()


def get_last_refresh():
    conn = _get_conn()
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM refresh_log ORDER BY started_at DESC LIMIT 1").fetchone()
    return dict(row) if row else None


# ── OHLCV daily ───────────────────────────────────────────────────────────────

def upsert_ohlcv_daily(rows: list[dict]):
    if not rows:
        return
    conn = _get_conn()
    conn.executemany(
        """INSERT INTO ohlcv_daily (symbol,date,open,high,low,close,volume)
           VALUES (?,?,?,?,?,?,?)
           ON CONFLICT(symbol,date) DO UPDATE SET
             open=excluded.open, high=excluded.high, low=excluded.low,
             close=excluded.close, volume=excluded.volume""",
        [(r["symbol"], str(r["date"]), r.get("open"), r.get("high"),
          r.get("low"), r.get("close"), r.get("volume")) for r in rows]
    )
    conn.commit()


def upsert_prices(rows: list[dict]):
    upsert_ohlcv_daily(rows)


def get_prices_date_range(exclude_symbols: list[str] | None = None) -> dict:
    """exclude_symbols: index/ETF symbols to leave out of the stock date-range stat
    (stocks and indices share ohlcv_daily with no distinguishing symbol prefix)."""
    conn = _get_conn()
    if exclude_symbols:
        ph = ",".join(["?"] * len(exclude_symbols))
        row = conn.execute(
            f"SELECT COUNT(DISTINCT symbol), MIN(date), MAX(date) FROM ohlcv_daily WHERE symbol NOT IN ({ph})",
            exclude_symbols,
        ).fetchone()
    else:
        row = conn.execute(
            "SELECT COUNT(DISTINCT symbol), MIN(date), MAX(date) FROM ohlcv_daily"
        ).fetchone()
    return {
        "symbols": row[0] or 0,
        "from":    row[1],
        "to":      row[2],
    }


def get_latest_price_dates(symbols: list[str]) -> dict[str, str]:
    if not symbols:
        return {}
    conn = _get_conn()
    ph = ",".join(["?"] * len(symbols))
    rows = conn.execute(
        f"SELECT symbol, MAX(date) FROM ohlcv_daily WHERE symbol IN ({ph}) GROUP BY symbol",
        symbols
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def purge_old_prices(keep_days: int = 2400, exclude_symbols: list[str] | None = None):
    """exclude_symbols: index/ETF symbols to never purge (full index history is
    required for RS backfill), same stock/index split issue as get_prices_date_range."""
    from datetime import datetime, timedelta
    cutoff = (datetime.utcnow() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    conn = _get_conn()
    if exclude_symbols:
        ph = ",".join(["?"] * len(exclude_symbols))
        conn.execute(
            f"DELETE FROM ohlcv_daily WHERE symbol NOT IN ({ph}) AND date < ?",
            exclude_symbols + [cutoff],
        )
    else:
        conn.execute("DELETE FROM ohlcv_daily WHERE date < ?", (cutoff,))
    deleted = conn.total_changes
    conn.commit()
    return deleted


# ── OHLCV intraday ────────────────────────────────────────────────────────────

def upsert_ohlcv_intraday(rows: list[dict]):
    if not rows:
        return
    conn = _get_conn()
    conn.executemany(
        """INSERT INTO ohlcv_intraday (symbol,ts,open,high,low,close,volume)
           VALUES (?,?,?,?,?,?,?)
           ON CONFLICT(symbol,ts) DO UPDATE SET
             open=excluded.open, high=excluded.high, low=excluded.low,
             close=excluded.close, volume=excluded.volume""",
        [(r["symbol"], r["ts"], r.get("open"), r.get("high"),
          r.get("low"), r.get("close"), r.get("volume")) for r in rows]
    )
    conn.commit()


def upsert_intraday_prices(rows: list[dict]):
    upsert_ohlcv_intraday(rows)


def purge_old_intraday_prices(keep_days: int = 100):
    cutoff_epoch = int(time.time()) - keep_days * 86400
    conn = _get_conn()
    conn.execute("DELETE FROM ohlcv_intraday WHERE ts < ?", (cutoff_epoch,))
    deleted = conn.total_changes
    conn.commit()
    return deleted


def get_intraday_candles(symbol: str, from_epoch: int) -> list[tuple]:
    conn = _get_conn()
    return conn.execute(
        "SELECT ts,open,high,low,close,volume FROM ohlcv_intraday WHERE symbol=? AND ts>=? ORDER BY ts ASC",
        (symbol, from_epoch)
    ).fetchall()


def get_intraday_candles_range(symbol: str, from_epoch: int, to_epoch: int) -> list[tuple]:
    conn = _get_conn()
    return conn.execute(
        "SELECT ts,open,high,low,close,volume FROM ohlcv_intraday WHERE symbol=? AND ts>=? AND ts<=? ORDER BY ts ASC",
        (symbol, from_epoch, to_epoch)
    ).fetchall()


# ── Index RS (IBD RS ratings) ─────────────────────────────────────────────────

def upsert_index_rs(rows: list[dict]):
    if not rows:
        return
    conn = _get_conn()
    conn.executemany(
        """INSERT INTO index_rs
               (symbol,date,rs_raw,rs_rating,mid_score,short_score,rs21,rs55,day_return,week_return,close,group_short_score,group_mid_score,group_lt_score)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(symbol,date) DO UPDATE SET
               rs_raw=excluded.rs_raw, rs_rating=excluded.rs_rating,
               mid_score=excluded.mid_score, short_score=excluded.short_score,
               rs21=COALESCE(excluded.rs21, index_rs.rs21),
               rs55=COALESCE(excluded.rs55, index_rs.rs55),
               day_return=COALESCE(excluded.day_return, index_rs.day_return),
               week_return=COALESCE(excluded.week_return, index_rs.week_return),
               close=COALESCE(excluded.close, index_rs.close),
               group_short_score=COALESCE(excluded.group_short_score, index_rs.group_short_score),
               group_mid_score=COALESCE(excluded.group_mid_score, index_rs.group_mid_score),
               group_lt_score=COALESCE(excluded.group_lt_score, index_rs.group_lt_score)""",
        [(r["symbol"], str(r["date"]), r.get("rs_raw"), r.get("rs_rating"),
          r.get("mid_score"), r.get("short_score"),
          r.get("rs21"), r.get("rs55"), r.get("day_return"), r.get("week_return"), r.get("close"),
          r.get("group_short_score"), r.get("group_mid_score"), r.get("group_lt_score")) for r in rows]
    )
    conn.commit()


def get_index_rs_dates(symbols: list[str]) -> list[str]:
    if not symbols:
        return []
    conn = _get_conn()
    ph = ",".join(["?"] * len(symbols))
    rows = conn.execute(
        f"SELECT DISTINCT date FROM index_rs WHERE symbol IN ({ph}) ORDER BY date ASC",
        symbols
    ).fetchall()
    return [r[0] for r in rows]


def purge_old_index_rs(keep_days: int = 2400):
    from datetime import datetime, timedelta
    cutoff = (datetime.utcnow() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    conn = _get_conn()
    conn.execute("DELETE FROM index_rs WHERE date < ?", (cutoff,))
    deleted = conn.total_changes
    conn.commit()
    return deleted


# ── Stock RS daily ────────────────────────────────────────────────────────────

def upsert_stock_rs_daily(rows: list[dict]):
    if not rows:
        return
    conn = _get_conn()
    conn.executemany(
        """INSERT INTO stock_rs_daily (symbol,date,rs21,rs21_rank,rs55,rs55_rank,day_return,rs_line,rs252,rs252_rank)
           VALUES (?,?,?,?,?,?,?,?,?,?)
           ON CONFLICT(symbol,date) DO UPDATE SET
             rs21=excluded.rs21, rs21_rank=excluded.rs21_rank,
             rs55=excluded.rs55, rs55_rank=excluded.rs55_rank,
             day_return=excluded.day_return,
             rs_line=excluded.rs_line,
             rs252=excluded.rs252, rs252_rank=excluded.rs252_rank""",
        [(r["symbol"], str(r["date"]), r.get("rs21"), r.get("rs21_rank"),
          r.get("rs55"), r.get("rs55_rank"),
          r.get("day_return"), r.get("rs_line"),
          r.get("rs252"), r.get("rs252_rank")) for r in rows]
    )
    conn.commit()


def get_stock_rs_dates(symbols: list[str]) -> list[str]:
    if not symbols:
        return []
    conn = _get_conn()
    ph = ",".join(["?"] * len(symbols))
    rows = conn.execute(
        f"SELECT DISTINCT date FROM stock_rs_daily WHERE symbol IN ({ph}) ORDER BY date ASC",
        symbols
    ).fetchall()
    return [r[0] for r in rows]


def get_stock_rs_latest(symbols: list[str] | None = None) -> list[dict]:
    conn = _get_conn()
    cols = ["symbol", "date", "rs21", "rs21_rank", "rs55", "rs55_rank", "day_return", "rs_line", "rs252", "rs252_rank"]
    if symbols:
        ph = ",".join(["?"] * len(symbols))
        rows = conn.execute(
            f"""SELECT s.symbol, s.date, s.rs21, s.rs21_rank, s.rs55, s.rs55_rank,
                       s.day_return, s.rs_line, s.rs252, s.rs252_rank
                FROM stock_rs_daily s
                INNER JOIN (SELECT MAX(date) AS max_date FROM stock_rs_daily) m ON s.date=m.max_date
                WHERE s.symbol IN ({ph})
                ORDER BY s.rs21_rank DESC""",
            symbols
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT s.symbol, s.date, s.rs21, s.rs21_rank, s.rs55, s.rs55_rank,
                      s.day_return, s.rs_line, s.rs252, s.rs252_rank
               FROM stock_rs_daily s
               INNER JOIN (SELECT MAX(date) AS max_date FROM stock_rs_daily) m ON s.date=m.max_date
               ORDER BY s.rs21_rank DESC"""
        ).fetchall()
    return [dict(zip(cols, r)) for r in rows]


def get_stock_rs_history(symbols: list[str], days: int = 30) -> dict:
    if not symbols:
        return {}
    conn = _get_conn()
    from datetime import datetime, timedelta
    cutoff = (datetime.utcnow() - timedelta(days=days + 60)).strftime("%Y-%m-%d")
    ph = ",".join(["?"] * len(symbols))
    rows = conn.execute(
        f"""SELECT symbol, date, rs21_rank, rs55_rank, rs252_rank
            FROM stock_rs_daily WHERE symbol IN ({ph}) AND date >= ? ORDER BY symbol, date ASC""",
        symbols + [cutoff]
    ).fetchall()
    out: dict = {}
    for r in rows:
        sym, date = r[0], r[1]
        out.setdefault(sym, {})[date] = {"rs21_rank": r[2], "rs55_rank": r[3], "rs252_rank": r[4]}
    return out


def purge_old_stock_rs(keep_days: int = 2400):
    from datetime import datetime, timedelta
    cutoff = (datetime.utcnow() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    conn = _get_conn()
    conn.execute("DELETE FROM stock_rs_daily WHERE date < ?", (cutoff,))
    deleted = conn.total_changes
    conn.commit()
    return deleted


# ── App state ─────────────────────────────────────────────────────────────────

def get_app_state(key: str) -> str | None:
    conn = _get_conn()
    row = conn.execute("SELECT value FROM app_state WHERE key=?", (key,)).fetchone()
    return row[0] if row else None


def set_app_state(key: str, value: str):
    conn = _get_conn()
    conn.execute(
        "INSERT INTO app_state (key,value,updated_at) VALUES (?,?,datetime('now')) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=datetime('now')",
        (key, value)
    )
    conn.commit()


# ── Scanner results ───────────────────────────────────────────────────────────

def upsert_scanner_results(rows: list[dict]):
    if not rows:
        return
    conn = _get_conn()
    conn.executemany(
        """INSERT INTO scanner_results
           (symbol, company_name, macro, sector, industry, basic_industry,
            direction, m_date, m_price, bull_cross_date, s_pullback_date, c_pullback_date,
            above_200sma, scanned_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
           ON CONFLICT(symbol, direction) DO UPDATE SET
             company_name=excluded.company_name, macro=excluded.macro,
             sector=excluded.sector, industry=excluded.industry,
             basic_industry=excluded.basic_industry,
             m_date=excluded.m_date, m_price=excluded.m_price,
             bull_cross_date=excluded.bull_cross_date,
             s_pullback_date=excluded.s_pullback_date, c_pullback_date=excluded.c_pullback_date,
             above_200sma=excluded.above_200sma, scanned_at=excluded.scanned_at""",
        [(r["symbol"], r.get("company_name"), r.get("macro"), r.get("sector"),
          r.get("industry"), r.get("basic_industry"), r["direction"],
          r.get("m_date"), r.get("m_price"), r.get("bull_cross_date"),
          r.get("s_pullback_date"), r.get("c_pullback_date"),
          1 if r.get("above_200sma") else 0) for r in rows]
    )
    conn.commit()


def clear_stale_scanner_results(keep_days: int = 60):
    from datetime import datetime, timedelta
    cutoff = (datetime.utcnow() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    conn = _get_conn()
    conn.execute("DELETE FROM scanner_results WHERE scanned_at < ?", (cutoff,))
    conn.commit()


def delete_scanner_results_not_in(hit_keys: list[tuple[str, str]]):
    """Delete (symbol, direction) rows not produced by the current scan run."""
    if not hit_keys:
        return
    conn = _get_conn()
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS _sc_hits (symbol TEXT, direction TEXT)")
    conn.execute("DELETE FROM _sc_hits")
    conn.executemany("INSERT INTO _sc_hits VALUES (?,?)", hit_keys)
    conn.execute("""DELETE FROM scanner_results WHERE NOT EXISTS (
        SELECT 1 FROM _sc_hits h
        WHERE h.symbol=scanner_results.symbol AND h.direction=scanner_results.direction)""")
    conn.commit()


# ── Scanner run log ───────────────────────────────────────────────────────────

def log_scan_start() -> int:
    conn = _get_conn()
    cur = conn.execute("INSERT INTO scanner_run_log (status) VALUES ('running')")
    conn.commit()
    return cur.lastrowid


def log_scan_finish(log_id: int, scanned: int, hits_bull: int = 0, hits_bear: int = 0,
                    errors: int = 0):
    conn = _get_conn()
    conn.execute(
        "UPDATE scanner_run_log SET finished_at=datetime('now'), scanned=?, hits_bull=?, hits_bear=?, errors=?, status='done' WHERE id=?",
        (scanned, hits_bull, hits_bear, errors, log_id)
    )
    conn.commit()


def get_last_scan_log() -> dict | None:
    conn = _get_conn()
    row = conn.execute(
        "SELECT * FROM scanner_run_log ORDER BY started_at DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


# ── State aliases ─────────────────────────────────────────────────────────────

def get_state(key):
    return get_app_state(key)

def set_state(key, value):
    set_app_state(key, value)


# ── Index prices ──────────────────────────────────────────────────────────────

def upsert_index_prices(rows):
    upsert_ohlcv_daily(rows)

def get_index_prices(symbol, from_date, to_date):
    conn = _get_conn()
    rows = conn.execute(
        "SELECT date, open, high, low, close, volume FROM ohlcv_daily WHERE symbol=? AND date BETWEEN ? AND ? ORDER BY date",
        (symbol, from_date, to_date)
    ).fetchall()
    return [{"date": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4], "volume": r[5]} for r in rows]

def get_index_closes(symbols, from_date, to_date):
    if not symbols:
        return {}
    conn = _get_conn()
    ph = ",".join(["?"] * len(symbols))
    rows = conn.execute(
        f"SELECT symbol, date, close FROM ohlcv_daily WHERE symbol IN ({ph}) AND date BETWEEN ? AND ? ORDER BY symbol, date",
        symbols + [from_date, to_date]
    ).fetchall()
    result = {}
    for sym, dt, close in rows:
        result.setdefault(sym, []).append((dt, float(close) if close is not None else None))
    return result

def get_latest_index_price_dates(symbols):
    if not symbols:
        return {}
    conn = _get_conn()
    ph = ",".join(["?"] * len(symbols))
    rows = conn.execute(
        f"SELECT symbol, MAX(date) FROM ohlcv_daily WHERE symbol IN ({ph}) GROUP BY symbol", symbols
    ).fetchall()
    return {r[0]: r[1] for r in rows}

def get_index_prices_date_range(symbols: list[str] | None = None):
    """symbols: the known index/ETF symbols (stocks and indices share ohlcv_daily
    with no distinguishing symbol prefix, so this must be told which rows are indices)."""
    conn = _get_conn()
    if symbols:
        ph = ",".join(["?"] * len(symbols))
        row = conn.execute(
            f"SELECT COUNT(DISTINCT symbol), MIN(date), MAX(date) FROM ohlcv_daily WHERE symbol IN ({ph})",
            symbols,
        ).fetchone()
    else:
        row = (0, None, None)
    return {"symbols": row[0] or 0, "from": row[1], "to": row[2]}

def get_latest_index_rs(date=None):
    conn = _get_conn()
    if date:
        rows = conn.execute(
            "SELECT symbol, date, rs_raw, rs_rating, group_lt_score FROM index_rs WHERE date=? ORDER BY rs_rating DESC", (date,)
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT ir.symbol, ir.date, ir.rs_raw, ir.rs_rating, ir.group_lt_score
               FROM index_rs ir
               INNER JOIN (
                   SELECT symbol,
                     COALESCE(
                       MAX(CASE WHEN group_lt_score IS NOT NULL THEN date END),
                       MAX(CASE WHEN close IS NOT NULL THEN date END)
                     ) AS max_date
                   FROM index_rs GROUP BY symbol
               ) latest ON ir.symbol = latest.symbol AND ir.date = latest.max_date
               ORDER BY ir.rs_rating DESC"""
        ).fetchall()
    return [{"symbol": r[0], "date": r[1], "rs_raw": r[2], "rs_rating": r[3], "group_lt_score": r[4]} for r in rows]

# ── Sector index map ──────────────────────────────────────────────────────────

def clear_sector_index_map():
    conn = _get_conn()
    conn.execute("DELETE FROM sector_index_map")
    conn.commit()


def purge_sector_index_map_deleted(active_symbols: set):
    """Remove rows for symbols no longer in tickers (delisted/removed stocks)."""
    if not active_symbols:
        return
    conn = _get_conn()
    ph = ",".join(["?"] * len(active_symbols))
    conn.execute(f"DELETE FROM sector_index_map WHERE symbol NOT IN ({ph})", list(active_symbols))
    conn.commit()


def upsert_sector_index_map(rows: list[tuple]):
    """Bulk insert (symbol, index_name, index_symbol, source, confidence) tuples."""
    if not rows:
        return
    conn = _get_conn()
    conn.executemany(
        """INSERT INTO sector_index_map (symbol, index_name, index_symbol, source, confidence)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT (symbol, index_name) DO UPDATE SET
               index_symbol = excluded.index_symbol,
               source       = excluded.source,
               confidence   = excluded.confidence""",
        rows,
    )
    conn.commit()


# ── Scanner run log ───────────────────────────────────────────────────────────

def scanner_run_start():
    return log_scan_start()

def scanner_run_finish(run_id, scanned, hits_bull, hits_bear, errors, status="done"):
    conn = _get_conn()
    conn.execute(
        "UPDATE scanner_run_log SET finished_at=datetime('now'), scanned=?, hits_bull=?, hits_bear=?, errors=?, status=? WHERE id=?",
        (scanned, hits_bull, hits_bear, errors, status, run_id)
    )
    conn.commit()

def get_scanner_last_run():
    conn = _get_conn()
    row = conn.execute("SELECT * FROM scanner_run_log ORDER BY started_at DESC LIMIT 1").fetchone()
    return dict(row) if row else None

def get_scanner_run_by_id(run_id):
    conn = _get_conn()
    row = conn.execute("SELECT * FROM scanner_run_log WHERE id=?", (run_id,)).fetchone()
    return dict(row) if row else None


# ── Scanner results query ─────────────────────────────────────────────────────

def get_scanner_results(direction, macros, sectors, industries, basic_industries,
                        indices=None, hawks=None, symbols=None):
    conn = _get_conn()
    params = [direction]
    if symbols:
        ph = ",".join(["?"] * len(symbols))
        extra = f"AND sr.symbol IN ({ph})"
        params.extend(symbols)
    elif hawks:
        ph = ",".join(["?"] * len(hawks))
        extra = f"AND sr.symbol IN (SELECT symbol FROM sector_index_map WHERE index_name IN ({ph}))"
        params.extend(hawks)
    elif indices:
        ph = ",".join(["?"] * len(indices))
        extra = f"AND sr.symbol IN (SELECT symbol FROM sector_index_map WHERE index_name IN ({ph}))"
        params.extend(indices)
    else:
        group_conds, group_params = [], []
        for col, vals in [("macro", macros), ("sector", sectors), ("industry", industries), ("basic_industry", basic_industries)]:
            if vals:
                ph = ",".join(["?"] * len(vals))
                group_conds.append(f"sr.{col} IN ({ph})")
                group_params.extend(vals)
        extra = ("AND (" + " OR ".join(group_conds) + ")") if group_conds else ""
        params.extend(group_params)
    sql = f"""SELECT sr.symbol, sr.company_name, sr.macro, sr.sector, sr.industry,
                     sr.basic_industry, sr.direction, sr.m_date, sr.bull_cross_date,
                     sr.s_pullback_date, sr.c_pullback_date, sr.above_200sma, sr.scanned_at,
                     sr.m_price
              FROM scanner_results sr
              WHERE sr.direction=? {extra}
              ORDER BY MAX(COALESCE(sr.m_date,''), COALESCE(sr.bull_cross_date,''),
                           COALESCE(sr.s_pullback_date,''), COALESCE(sr.c_pullback_date,'')) DESC,
                       sr.symbol"""
    rows = conn.execute(sql, params).fetchall()
    return [{"symbol": r[0], "company_name": r[1], "macro": r[2], "sector": r[3],
             "industry": r[4], "basic_industry": r[5], "direction": r[6],
             "m_date": r[7], "bull_cross_date": r[8], "s_pullback_date": r[9],
             "c_pullback_date": r[10], "above_200sma": bool(r[11]), "scanned_at": r[12],
             "m_price": r[13]} for r in rows]


# ── Scan Signals (hourly cycle enrichment) ───────────────────────────────────

def upsert_scan_signals(rows: list[dict]):
    if not rows:
        return
    conn = _get_conn()
    conn.executemany(
        """INSERT INTO scan_signals
           (symbol, direction, step1_date, m_date, m_price, daily_cross_date,
            above_200sma, phase_end_date, hourly_signals, wh_count, hx_count,
            latest_hx_ts, computed_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
           ON CONFLICT(symbol, direction) DO UPDATE SET
             step1_date=excluded.step1_date, m_date=excluded.m_date,
             m_price=excluded.m_price, daily_cross_date=excluded.daily_cross_date,
             above_200sma=excluded.above_200sma, phase_end_date=excluded.phase_end_date,
             hourly_signals=excluded.hourly_signals, wh_count=excluded.wh_count,
             hx_count=excluded.hx_count, latest_hx_ts=excluded.latest_hx_ts,
             computed_at=excluded.computed_at""",
        [(r["symbol"], r["direction"], r.get("step1_date"), r.get("m_date"),
          r.get("m_price"), r.get("daily_cross_date"),
          1 if r.get("above_200sma") else 0, r.get("phase_end_date"),
          json.dumps(r.get("hourly_signals", [])),
          r.get("wh_count", 0), r.get("hx_count", 0),
          r.get("latest_hx_ts")) for r in rows]
    )
    conn.commit()


def get_scan_signals(direction: str | None = None) -> list[dict]:
    conn = _get_conn()
    if direction:
        rows = conn.execute(
            "SELECT * FROM scan_signals WHERE direction=? ORDER BY latest_hx_ts IS NULL, latest_hx_ts DESC",
            (direction,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM scan_signals ORDER BY latest_hx_ts IS NULL, latest_hx_ts DESC"
        ).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        try:
            d["hourly_signals"] = json.loads(d["hourly_signals"] or "[]")
        except Exception:
            d["hourly_signals"] = []
        result.append(d)
    return result


def delete_scan_signals_not_in(hit_keys: list[tuple[str, str]]):
    if not hit_keys:
        return
    conn = _get_conn()
    conn.execute("CREATE TEMP TABLE IF NOT EXISTS _ss_hits (symbol TEXT, direction TEXT)")
    conn.execute("DELETE FROM _ss_hits")
    conn.executemany("INSERT INTO _ss_hits VALUES (?,?)", hit_keys)
    conn.execute("""DELETE FROM scan_signals WHERE NOT EXISTS (
        SELECT 1 FROM _ss_hits h
        WHERE h.symbol=scan_signals.symbol AND h.direction=scan_signals.direction)""")
    conn.commit()


# ── Watchlist ─────────────────────────────────────────────────────────────────

def get_watchlist():
    """Return list of saved watchlist symbols (active tickers only), newest-added first."""
    conn = _get_conn()
    rows = conn.execute(
        """SELECT w.symbol FROM watchlist w
           JOIN tickers t ON t.symbol=w.symbol AND t.is_active=1
           ORDER BY w.added_at DESC, w.symbol ASC"""
    ).fetchall()
    return [r[0] for r in rows]


def add_to_watchlist(symbol):
    """Add a symbol to the watchlist (idempotent)."""
    conn = _get_conn()
    conn.execute(
        "INSERT OR IGNORE INTO watchlist (symbol) VALUES (?)", (symbol,)
    )
    conn.commit()


def remove_from_watchlist(symbol):
    """Remove a symbol from the watchlist."""
    conn = _get_conn()
    conn.execute("DELETE FROM watchlist WHERE symbol = ?", (symbol,))
    conn.commit()


# ── Journal ───────────────────────────────────────────────────────────────────
# One entry per symbol. snapshot_json is a FROZEN capture (never auto-updated);
# plan_json holds the editable trade plan (entry/SL/exits/notes).

def _journal_row_to_dict(r):
    import json as _json
    return {
        "id": r[0], "symbol": r[1], "status": r[2],
        "snapshot": _json.loads(r[3]) if r[3] else {},
        "plan":     _json.loads(r[4]) if r[4] else {},
        "created_at": r[5], "updated_at": r[6],
    }


def list_journal_entries():
    """Return all journal entries (newest first), parsed JSON included.
    id-keyed: multiple trades per symbol each appear as their own entry."""
    conn = _get_conn()
    rows = conn.execute(
        """SELECT id, symbol, status, snapshot_json, plan_json, created_at, updated_at
           FROM journal_entries ORDER BY created_at DESC, id DESC"""
    ).fetchall()
    return [_journal_row_to_dict(r) for r in rows]


def get_journal_entry(entry_id):
    """Return a single journal entry by its trade id (parsed) or None."""
    conn = _get_conn()
    r = conn.execute(
        """SELECT id, symbol, status, snapshot_json, plan_json, created_at, updated_at
           FROM journal_entries WHERE id = ?""", (entry_id,)
    ).fetchone()
    return _journal_row_to_dict(r) if r else None


def create_journal_entry(symbol, snapshot, plan, status="open"):
    """Insert a NEW trade entry (always a fresh row, even for a repeat symbol).
    Returns the new trade id."""
    import json as _json
    conn = _get_conn()
    cur = conn.execute(
        """INSERT INTO journal_entries (symbol, status, snapshot_json, plan_json)
           VALUES (?, ?, ?, ?)""",
        (symbol, status, _json.dumps(snapshot), _json.dumps(plan or {})),
    )
    conn.commit()
    return cur.lastrowid


def update_journal_plan(entry_id, plan, status=None):
    """Update ONLY the editable trade plan (and optionally status). Snapshot untouched."""
    import json as _json
    conn = _get_conn()
    if status is not None:
        conn.execute(
            """UPDATE journal_entries
               SET plan_json = ?, status = ?, updated_at = datetime('now')
               WHERE id = ?""",
            (_json.dumps(plan or {}), status, entry_id),
        )
    else:
        conn.execute(
            """UPDATE journal_entries
               SET plan_json = ?, updated_at = datetime('now')
               WHERE id = ?""",
            (_json.dumps(plan or {}), entry_id),
        )
    conn.commit()


def recapture_journal_snapshot(entry_id, snapshot):
    """Deliberately overwrite the frozen snapshot (explicit user action only)."""
    import json as _json
    conn = _get_conn()
    conn.execute(
        """UPDATE journal_entries
           SET snapshot_json = ?, updated_at = datetime('now')
           WHERE id = ?""",
        (_json.dumps(snapshot), entry_id),
    )
    conn.commit()


def delete_journal_entry(entry_id):
    """Delete a single journal entry by its trade id."""
    conn = _get_conn()
    conn.execute("DELETE FROM journal_entries WHERE id = ?", (entry_id,))
    conn.commit()


# ── Intraday bulk fetch ───────────────────────────────────────────────────────

def get_intraday_candles_bulk(symbols, from_epoch):
    if not symbols:
        return {}
    conn = _get_conn()
    ph = ",".join(["?"] * len(symbols))
    rows = conn.execute(
        f"SELECT symbol, ts, open, high, low, close, volume FROM ohlcv_intraday WHERE symbol IN ({ph}) AND ts >= ? ORDER BY symbol, ts ASC",
        symbols + [from_epoch]
    ).fetchall()
    result = {}
    for r in rows:
        result.setdefault(r[0], []).append(r[1:])
    return result
