"""
db_neon.py — Neon PostgreSQL connection and all database operations.
Uses a connection pool so every request reuses an existing connection
instead of opening a new TCP handshake to Neon each time.

Do not import this directly — use `import db` which routes here when DB_MODE='neon'.
"""

import datetime
import os
import psycopg2
import psycopg2.extras
import psycopg2.pool
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL environment variable is not set. "
        "Add it to your .env file (local) or GitHub Actions secrets (CI)."
    )

# Pool: min 2 connections kept warm, max 10 — enough for concurrent API calls
_pool = psycopg2.pool.ThreadedConnectionPool(2, 10, DATABASE_URL)


def get_conn():
    conn = _pool.getconn()
    try:
        # Validate the connection is still alive — Neon suspends idle compute,
        # leaving stale connections in the pool that fail on first use.
        conn.cursor().execute("SELECT 1")
    except Exception:
        try:
            _pool.putconn(conn, close=True)
        except Exception:
            pass
        conn = _pool.getconn()
    return conn


def release_conn(conn):
    _pool.putconn(conn)


def _keepalive_loop():
    """Ping the pool every 3 minutes so Neon compute never suspends between requests."""
    import time
    while True:
        time.sleep(180)
        try:
            conn = _pool.getconn()
            conn.cursor().execute("SELECT 1")
            _pool.putconn(conn)
        except Exception:
            pass


import threading as _threading
_threading.Thread(target=_keepalive_loop, daemon=True).start()


def init_db():
    """Create tables if they don't exist."""
    ddl = """
    CREATE TABLE IF NOT EXISTS tickers (
        symbol           TEXT PRIMARY KEY,
        company_name     TEXT,
        macro            TEXT,
        sector           TEXT,
        industry         TEXT,
        basic_industry   TEXT,
        index_list       TEXT[],
        refreshed_at     TIMESTAMPTZ DEFAULT NOW(),
        is_active        BOOLEAN DEFAULT TRUE
    );

    -- Add index_list to existing tables (idempotent)
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='tickers' AND column_name='index_list'
        ) THEN
            ALTER TABLE tickers ADD COLUMN index_list TEXT[];
        END IF;
    END$$;

    CREATE TABLE IF NOT EXISTS refresh_log (
        id              SERIAL PRIMARY KEY,
        started_at      TIMESTAMPTZ DEFAULT NOW(),
        finished_at     TIMESTAMPTZ,
        total_symbols   INT,
        success_count   INT,
        failed_symbols  TEXT[],
        status          TEXT DEFAULT 'running'
    );

    CREATE TABLE IF NOT EXISTS app_state (
        key        TEXT PRIMARY KEY,
        value      TEXT,
        updated_at TIMESTAMPTZ DEFAULT NOW()
    );

    -- Track consecutive fetch failures per symbol (idempotent)
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='tickers' AND column_name='fetch_failures'
        ) THEN
            ALTER TABLE tickers ADD COLUMN fetch_failures INT DEFAULT 0;
        END IF;
    END$$;

    -- is_active flag: false for delisted stocks, keeps history intact (idempotent)
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='tickers' AND column_name='is_active'
        ) THEN
            ALTER TABLE tickers ADD COLUMN is_active BOOLEAN DEFAULT TRUE;
        END IF;
    END$$;

    -- Daily Hunt scanner results (bull/bear 3-milestone sequence)
    CREATE TABLE IF NOT EXISTS scanner_results (
        id               SERIAL PRIMARY KEY,
        symbol           TEXT    NOT NULL,
        company_name     TEXT,
        macro            TEXT,
        sector           TEXT,
        industry         TEXT,
        basic_industry   TEXT,
        direction        TEXT    NOT NULL,       -- 'bull' | 'bear'
        m_date           DATE,                   -- M-High (bull) or M-Low (bear) date
        m_price          NUMERIC,                -- prev-month H/L threshold price (chart overlay)
        bull_cross_date  DATE,                   -- Step 3: 20 EMA crossed above/below 50 EMA
        s_pullback_date  DATE,                   -- Step 4: first S-Pullback to 20 EMA
        c_pullback_date  DATE,                   -- Step 5: first C-Pullback to 50 EMA
        above_200sma     BOOLEAN DEFAULT FALSE,
        scanned_at       TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (symbol, direction)               -- one bull row + one bear row per symbol
    );

    CREATE INDEX IF NOT EXISTS idx_scan_direction  ON scanner_results (direction);
    CREATE INDEX IF NOT EXISTS idx_scan_scanned_at ON scanner_results (scanned_at);
    CREATE INDEX IF NOT EXISTS idx_scan_macro      ON scanner_results (macro);
    CREATE INDEX IF NOT EXISTS idx_scan_sector     ON scanner_results (sector);
    CREATE INDEX IF NOT EXISTS idx_scan_industry   ON scanner_results (industry);
    CREATE INDEX IF NOT EXISTS idx_scan_bi         ON scanner_results (basic_industry);

    CREATE TABLE IF NOT EXISTS scanner_run_log (
        id          SERIAL PRIMARY KEY,
        started_at  TIMESTAMPTZ DEFAULT NOW(),
        finished_at TIMESTAMPTZ,
        scanned     INT,
        hits_bull   INT DEFAULT 0,
        hits_bear   INT DEFAULT 0,
        errors      INT,
        status      TEXT DEFAULT 'running'   -- 'running' | 'done' | 'error'
    );

    -- Migrate old scanner_results to new schema (drop old columns, add new ones — idempotent)
    DO $$
    BEGIN
        -- Add new columns if missing
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='scanner_results' AND column_name='direction') THEN
            ALTER TABLE scanner_results ADD COLUMN direction TEXT NOT NULL DEFAULT 'bull';
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='scanner_results' AND column_name='m_date') THEN
            ALTER TABLE scanner_results ADD COLUMN m_date DATE;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='scanner_results' AND column_name='bull_cross_date') THEN
            ALTER TABLE scanner_results ADD COLUMN bull_cross_date DATE;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='scanner_results' AND column_name='s_pullback_date') THEN
            ALTER TABLE scanner_results ADD COLUMN s_pullback_date DATE;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='scanner_results' AND column_name='c_pullback_date') THEN
            ALTER TABLE scanner_results ADD COLUMN c_pullback_date DATE;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='scanner_results' AND column_name='above_200sma') THEN
            ALTER TABLE scanner_results ADD COLUMN above_200sma BOOLEAN DEFAULT FALSE;
        END IF;
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='scanner_results' AND column_name='m_price') THEN
            ALTER TABLE scanner_results ADD COLUMN m_price NUMERIC;
        END IF;
        -- Add new columns to scanner_run_log if missing
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='scanner_run_log' AND column_name='hits_bull') THEN
            ALTER TABLE scanner_run_log ADD COLUMN hits_bull INT DEFAULT 0;
            ALTER TABLE scanner_run_log ADD COLUMN hits_bear INT DEFAULT 0;
        END IF;
    END$$;

    -- ── Unified daily OHLCV: stocks (bare symbol) + indices (NSE:X-INDEX) ────
    CREATE TABLE IF NOT EXISTS ohlcv_daily (
        symbol     TEXT    NOT NULL,
        date       DATE    NOT NULL,
        open       NUMERIC,
        high       NUMERIC,
        low        NUMERIC,
        close      NUMERIC,
        volume     BIGINT,
        PRIMARY KEY (symbol, date)
    );
    CREATE INDEX IF NOT EXISTS idx_ohlcv_daily_symbol   ON ohlcv_daily (symbol);
    CREATE INDEX IF NOT EXISTS idx_ohlcv_daily_date     ON ohlcv_daily (date);
    CREATE INDEX IF NOT EXISTS idx_ohlcv_daily_sym_date ON ohlcv_daily (symbol, date);

    -- ── Monthly OHLCV, resampled from ohlcv_daily (no extra fetch needed) ─────
    CREATE TABLE IF NOT EXISTS ohlcv_monthly (
        symbol  TEXT    NOT NULL,
        month   TEXT    NOT NULL,
        open    NUMERIC,
        high    NUMERIC,
        low     NUMERIC,
        close   NUMERIC,
        volume  BIGINT,
        PRIMARY KEY (symbol, month)
    );
    CREATE INDEX IF NOT EXISTS idx_ohlcv_monthly_symbol ON ohlcv_monthly (symbol);
    CREATE INDEX IF NOT EXISTS idx_ohlcv_monthly_month  ON ohlcv_monthly (month);

    -- ── IBD RS: daily RS ratings (1-99 percentile rank) ──────────────────────
    CREATE TABLE IF NOT EXISTS index_rs (
        symbol            TEXT    NOT NULL,
        date              DATE    NOT NULL,
        rs_raw            NUMERIC,             -- weighted score before ranking (NULL for early dates with no LT)
        rs_rating         INT,                -- percentile rank 1-99 (LT score, NULL for early dates)
        mid_score         INT,                -- RS55 peer rank 1-99 (mid-term)
        short_score       INT,                -- RS21 peer rank 1-99 (short-term)
        rs21              NUMERIC,
        rs55              NUMERIC,
        day_return        NUMERIC,
        week_return       NUMERIC,
        close             NUMERIC,
        group_short_score INT,
        group_mid_score   INT,
        group_lt_score    INT,
        PRIMARY KEY (symbol, date)
    );
    CREATE INDEX IF NOT EXISTS idx_index_rs_date   ON index_rs (date);
    CREATE INDEX IF NOT EXISTS idx_index_rs_symbol ON index_rs (symbol);

    -- Add rs21/rs55/day_return/week_return/close/group_* to existing index_rs tables (idempotent)
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='index_rs' AND column_name='rs21'
        ) THEN
            ALTER TABLE index_rs ADD COLUMN rs21 NUMERIC;
            ALTER TABLE index_rs ADD COLUMN rs55 NUMERIC;
            ALTER TABLE index_rs ADD COLUMN day_return NUMERIC;
            ALTER TABLE index_rs ADD COLUMN week_return NUMERIC;
            ALTER TABLE index_rs ADD COLUMN close NUMERIC;
            ALTER TABLE index_rs ADD COLUMN group_short_score INT;
            ALTER TABLE index_rs ADD COLUMN group_mid_score INT;
            ALTER TABLE index_rs ADD COLUMN group_lt_score INT;
        END IF;
    END$$;

    -- ── Stock RS daily: pre-computed RS21, RS55, RS252 scores per stock per date ─
    CREATE TABLE IF NOT EXISTS stock_rs_daily (
        symbol      TEXT    NOT NULL,
        date        DATE    NOT NULL,
        rs21        NUMERIC,   -- raw RS21 value (%)
        rs21_rank   INT,       -- percentile rank 1-99 within full universe
        rs55        NUMERIC,   -- raw RS55 value (%)
        rs55_rank   INT,       -- percentile rank 1-99 within full universe
        day_return  NUMERIC,   -- % price change vs previous trading day
        rs_line     NUMERIC,   -- stock_close / nifty500_close on this date
        rs252       NUMERIC,   -- raw O'Neil 12-month RS score (4-quarter weighted)
        rs252_rank  INT,       -- percentile rank 1-99 across all ~2100 stocks
        PRIMARY KEY (symbol, date)
    );
    CREATE INDEX IF NOT EXISTS idx_stock_rs_date        ON stock_rs_daily (date);
    CREATE INDEX IF NOT EXISTS idx_stock_rs_symbol      ON stock_rs_daily (symbol);
    CREATE INDEX IF NOT EXISTS idx_stock_rs_sym_date    ON stock_rs_daily (symbol, date);

    -- Add rs55/rs55_rank to existing stock_rs_daily tables (idempotent)
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='stock_rs_daily' AND column_name='rs55'
        ) THEN
            ALTER TABLE stock_rs_daily ADD COLUMN rs55      NUMERIC;
            ALTER TABLE stock_rs_daily ADD COLUMN rs55_rank INT;
        END IF;
    END$$;

    -- Add day_return to existing stock_rs_daily tables (idempotent)
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='stock_rs_daily' AND column_name='day_return'
        ) THEN
            ALTER TABLE stock_rs_daily ADD COLUMN day_return NUMERIC;
        END IF;
    END$$;

    -- Add rs252/rs252_rank to existing stock_rs_daily tables (idempotent)
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='stock_rs_daily' AND column_name='rs252'
        ) THEN
            ALTER TABLE stock_rs_daily ADD COLUMN rs252      NUMERIC;
            ALTER TABLE stock_rs_daily ADD COLUMN rs252_rank INT;
        END IF;
    END$$;

    CREATE TABLE IF NOT EXISTS ohlcv_weekly (
        symbol  TEXT    NOT NULL,
        week    DATE    NOT NULL,
        open    NUMERIC,
        high    NUMERIC,
        low     NUMERIC,
        close   NUMERIC,
        volume  BIGINT,
        PRIMARY KEY (symbol, week)
    );
    CREATE INDEX IF NOT EXISTS idx_ohlcv_weekly_symbol ON ohlcv_weekly (symbol);
    CREATE INDEX IF NOT EXISTS idx_ohlcv_weekly_week   ON ohlcv_weekly (week);

    CREATE TABLE IF NOT EXISTS indicator_daily (
        symbol   TEXT    NOT NULL,
        date     DATE    NOT NULL,
        ema20    NUMERIC,
        ema50    NUMERIC,
        sma200   NUMERIC,
        rs_line  NUMERIC,
        PRIMARY KEY (symbol, date)
    );
    CREATE INDEX IF NOT EXISTS idx_ind_daily_symbol   ON indicator_daily (symbol);
    CREATE INDEX IF NOT EXISTS idx_ind_daily_date     ON indicator_daily (date);
    CREATE INDEX IF NOT EXISTS idx_ind_daily_sym_date ON indicator_daily (symbol, date);

    CREATE TABLE IF NOT EXISTS indicator_weekly (
        symbol   TEXT    NOT NULL,
        week     DATE    NOT NULL,
        ema20    NUMERIC,
        ema50    NUMERIC,
        sma200   NUMERIC,
        rs_line  NUMERIC,
        PRIMARY KEY (symbol, week)
    );
    CREATE INDEX IF NOT EXISTS idx_ind_weekly_symbol ON indicator_weekly (symbol);
    CREATE INDEX IF NOT EXISTS idx_ind_weekly_week   ON indicator_weekly (week);

    CREATE TABLE IF NOT EXISTS indicator_monthly (
        symbol   TEXT    NOT NULL,
        month    TEXT    NOT NULL,
        ema20    NUMERIC,
        ema50    NUMERIC,
        sma200   NUMERIC,
        rs_line  NUMERIC,
        PRIMARY KEY (symbol, month)
    );
    CREATE INDEX IF NOT EXISTS idx_ind_monthly_symbol ON indicator_monthly (symbol);
    CREATE INDEX IF NOT EXISTS idx_ind_monthly_month  ON indicator_monthly (month);

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

    CREATE TABLE IF NOT EXISTS scan_signals (
        symbol           TEXT    NOT NULL,
        direction        TEXT    NOT NULL,
        step1_date       TEXT,
        m_date           TEXT,
        m_price          REAL,
        daily_cross_date TEXT,
        above_200sma     BOOLEAN DEFAULT FALSE,
        phase_end_date   TEXT,
        hourly_signals   TEXT    DEFAULT '[]',
        wh_count         INTEGER DEFAULT 0,
        hx_count         INTEGER DEFAULT 0,
        latest_hx_ts     BIGINT,
        computed_at      TIMESTAMPTZ DEFAULT NOW(),
        PRIMARY KEY (symbol, direction)
    );
    CREATE INDEX IF NOT EXISTS idx_ss_direction  ON scan_signals (direction);
    CREATE INDEX IF NOT EXISTS idx_ss_latest_hx  ON scan_signals (latest_hx_ts DESC NULLS LAST);
    CREATE INDEX IF NOT EXISTS idx_ss_computed   ON scan_signals (computed_at);

    CREATE TABLE IF NOT EXISTS watchlist (
        symbol    TEXT PRIMARY KEY,
        added_at  TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE TABLE IF NOT EXISTS journal_entries (
        symbol        TEXT PRIMARY KEY,
        status        TEXT NOT NULL DEFAULT 'open',
        snapshot_json JSONB NOT NULL,
        plan_json     JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(ddl)
            # Hourly intraday data retired (monthly cadence usage made it dead
            # weight — heaviest per-symbol fetch in every refresh, nothing reads it).
            cur.execute("DROP TABLE IF EXISTS ohlcv_intraday")
            cur.execute("DROP TABLE IF EXISTS indicator_hourly")
        conn.commit()
    finally:
        release_conn(conn)


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

def upsert_tickers(rows: list[dict]):
    """Bulk upsert ticker rows. No-op if list is empty."""
    if not rows:
        return
    for r in rows:
        r["sector"] = _normalise_sector(r.get("sector", ""))

    sql = """
    INSERT INTO tickers (
        symbol, company_name,
        macro, sector, industry, basic_industry, index_list, refreshed_at, is_active
    ) VALUES (
        %(symbol)s, %(company_name)s,
        %(macro)s, %(sector)s, %(industry)s, %(basic_industry)s, %(index_list)s,
        NOW(), TRUE
    )
    ON CONFLICT (symbol) DO UPDATE SET
        company_name    = EXCLUDED.company_name,
        macro           = EXCLUDED.macro,
        sector          = EXCLUDED.sector,
        industry        = EXCLUDED.industry,
        basic_industry  = EXCLUDED.basic_industry,
        index_list      = EXCLUDED.index_list,
        refreshed_at    = NOW(),
        is_active       = TRUE;
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
        conn.commit()
    finally:
        release_conn(conn)


def mark_all_tickers_inactive():
    """Mark every ticker is_active=FALSE before a refresh run. Seen symbols get is_active=TRUE via upsert_tickers."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE tickers SET is_active=FALSE")
        conn.commit()
    finally:
        release_conn(conn)


def log_refresh_start() -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO refresh_log (status) VALUES ('running') RETURNING id"
            )
            row_id = cur.fetchone()[0]
        conn.commit()
        return row_id
    finally:
        release_conn(conn)


def log_refresh_finish(log_id: int | None, total: int, success: int, failed: list[str]):
    if log_id is None:
        return
    status = "done" if not failed else "done_with_errors"
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE refresh_log
                SET finished_at    = NOW(),
                    total_symbols  = %s,
                    success_count  = %s,
                    failed_symbols = %s,
                    status         = %s
                WHERE id = %s
                """,
                (total, success, failed, status, log_id),
            )
        conn.commit()
    finally:
        release_conn(conn)


def get_last_refresh():
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM refresh_log ORDER BY started_at DESC LIMIT 1"
            )
            return cur.fetchone()
    finally:
        release_conn(conn)


def get_ticker_count():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM tickers")
            return cur.fetchone()[0]
    finally:
        release_conn(conn)


def upsert_ohlcv_daily(rows: list[dict]):
    """Bulk upsert daily OHLCV into ohlcv_daily. Works for both stocks and indices."""
    if not rows:
        return
    sql = """
    INSERT INTO ohlcv_daily (symbol, date, open, high, low, close, volume)
    VALUES (%(symbol)s, %(date)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
    ON CONFLICT (symbol, date) DO UPDATE SET
        open   = EXCLUDED.open,
        high   = EXCLUDED.high,
        low    = EXCLUDED.low,
        close  = EXCLUDED.close,
        volume = EXCLUDED.volume
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
        conn.commit()
    finally:
        release_conn(conn)

# Legacy alias — prices.py writes via this name
def upsert_prices(rows: list[dict]):
    upsert_ohlcv_daily(rows)


def get_fetch_failures(symbols: list[str]) -> dict[str, int]:
    """Return {symbol: failure_count} for the given symbols."""
    if not symbols:
        return {}
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            ph = ",".join(["%s"] * len(symbols))
            cur.execute(
                f"SELECT symbol, COALESCE(fetch_failures, 0) FROM tickers WHERE symbol IN ({ph})",
                symbols,
            )
            return {r[0]: r[1] for r in cur.fetchall()}
    finally:
        release_conn(conn)


def increment_fetch_failure(symbol: str):
    """Increment consecutive fetch failure counter for a symbol."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE tickers SET fetch_failures = COALESCE(fetch_failures, 0) + 1 WHERE symbol = %s",
                (symbol,),
            )
        conn.commit()
    finally:
        release_conn(conn)


def reset_fetch_failure(symbol: str):
    """Reset failure counter after a successful fetch."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE tickers SET fetch_failures = 0 WHERE symbol = %s AND fetch_failures > 0",
                (symbol,),
            )
        conn.commit()
    finally:
        release_conn(conn)


def get_prices_date_range(exclude_symbols: list[str] | None = None) -> dict:
    """Return the min/max date and symbol count for stock rows in ohlcv_daily.
    exclude_symbols: index/ETF symbols to leave out (stocks and indices share
    ohlcv_daily with no distinguishing symbol prefix)."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if exclude_symbols:
                ph = ",".join(["%s"] * len(exclude_symbols))
                cur.execute(
                    f"SELECT COUNT(DISTINCT symbol), MIN(date), MAX(date) "
                    f"FROM ohlcv_daily WHERE symbol NOT IN ({ph})",
                    exclude_symbols,
                )
            else:
                cur.execute("SELECT COUNT(DISTINCT symbol), MIN(date), MAX(date) FROM ohlcv_daily")
            row = cur.fetchone()
            return {
                "symbols": row[0] or 0,
                "from":    str(row[1]) if row[1] else None,
                "to":      str(row[2]) if row[2] else None,
            }
    finally:
        release_conn(conn)


def get_latest_price_dates(symbols: list[str]) -> dict[str, str]:
    if not symbols:
        return {}
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            ph = ",".join(["%s"] * len(symbols))
            cur.execute(
                f"SELECT symbol, MAX(date)::text FROM ohlcv_daily WHERE symbol IN ({ph}) GROUP BY symbol",
                symbols,
            )
            return {r[0]: r[1] for r in cur.fetchall()}
    finally:
        release_conn(conn)


def purge_old_prices(keep_days: int = 2400, exclude_symbols: list[str] | None = None):
    """
    Delete stock daily rows older than keep_days from ohlcv_daily.
    NEVER touches index/ETF symbols (exclude_symbols) — full index history is
    required for the 200SMA chart warmup and must never be purged.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if exclude_symbols:
                ph = ",".join(["%s"] * len(exclude_symbols))
                cur.execute(
                    f"DELETE FROM ohlcv_daily WHERE symbol NOT IN ({ph}) AND date < NOW() - INTERVAL '%s days'",
                    exclude_symbols + [keep_days],
                )
            else:
                cur.execute(
                    "DELETE FROM ohlcv_daily WHERE date < NOW() - INTERVAL '%s days'",
                    (keep_days,),
                )
            deleted = cur.rowcount
        conn.commit()
        return deleted
    finally:
        release_conn(conn)


def upsert_ohlcv_monthly(rows: list[dict]):
    """rows: [{symbol, month, open, high, low, close, volume}]"""
    if not rows:
        return
    sql = """
    INSERT INTO ohlcv_monthly (symbol, month, open, high, low, close, volume)
    VALUES (%(symbol)s, %(month)s, %(open)s, %(high)s, %(low)s, %(close)s, %(volume)s)
    ON CONFLICT (symbol, month) DO UPDATE SET
        open   = EXCLUDED.open,
        high   = EXCLUDED.high,
        low    = EXCLUDED.low,
        close  = EXCLUDED.close,
        volume = EXCLUDED.volume
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
        conn.commit()
    finally:
        release_conn(conn)


# ── Stock RS daily cache ───────────────────────────────────────────────────────

def upsert_stock_rs_daily(rows: list[dict]):
    """Bulk upsert pre-computed stock RS rows into stock_rs_daily.
    Each row must have: symbol, date, rs21, rs21_rank, rs55, rs55_rank,
                        day_return, rs_line, rs252, rs252_rank
    Uses ON CONFLICT so re-runs are safe — existing rows for the same (symbol, date) are overwritten.
    """
    if not rows:
        return
    for r in rows:
        r.setdefault("day_return", None)
        r.setdefault("rs252", None)
        r.setdefault("rs252_rank", None)
    sql = """
    INSERT INTO stock_rs_daily
        (symbol, date, rs21, rs21_rank, rs55, rs55_rank, day_return, rs_line, rs252, rs252_rank)
    VALUES
        (%(symbol)s, %(date)s, %(rs21)s, %(rs21_rank)s,
         %(rs55)s, %(rs55_rank)s,
         %(day_return)s, %(rs_line)s,
         %(rs252)s, %(rs252_rank)s)
    ON CONFLICT (symbol, date) DO UPDATE SET
        rs21       = EXCLUDED.rs21,
        rs21_rank  = EXCLUDED.rs21_rank,
        rs55       = EXCLUDED.rs55,
        rs55_rank  = EXCLUDED.rs55_rank,
        day_return = EXCLUDED.day_return,
        rs_line    = EXCLUDED.rs_line,
        rs252      = EXCLUDED.rs252,
        rs252_rank = EXCLUDED.rs252_rank
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
        conn.commit()
    finally:
        release_conn(conn)


def get_stock_rs_dates(days: int = 30) -> list[str]:
    """Return the last `days` distinct dates present in stock_rs_daily, newest first."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT DISTINCT date::text FROM stock_rs_daily
                   ORDER BY date DESC LIMIT %s""",
                (days,),
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        release_conn(conn)


def get_stock_rs_latest(symbols: list[str] | None = None) -> list[dict]:
    """Return the latest date's RS rows for all (or specified) symbols."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if symbols:
                ph = ",".join(["%s"] * len(symbols))
                cur.execute(
                    f"""SELECT s.symbol, s.date::text, s.rs21, s.rs21_rank,
                               s.rs55, s.rs55_rank,
                               s.day_return, s.rs_line,
                               s.rs252, s.rs252_rank
                        FROM stock_rs_daily s
                        INNER JOIN (
                            SELECT MAX(date) AS max_date FROM stock_rs_daily
                        ) m ON s.date = m.max_date
                        WHERE s.symbol IN ({ph})
                        ORDER BY s.rs21_rank DESC""",
                    symbols,
                )
            else:
                cur.execute(
                    """SELECT s.symbol, s.date::text, s.rs21, s.rs21_rank,
                              s.rs55, s.rs55_rank,
                              s.day_return, s.rs_line,
                              s.rs252, s.rs252_rank
                       FROM stock_rs_daily s
                       INNER JOIN (
                           SELECT MAX(date) AS max_date FROM stock_rs_daily
                       ) m ON s.date = m.max_date
                       ORDER BY s.rs21_rank DESC"""
                )
            cols = ["symbol", "date", "rs21", "rs21_rank",
                    "rs55", "rs55_rank",
                    "day_return", "rs_line",
                    "rs252", "rs252_rank"]
            return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        release_conn(conn)


def get_stock_rs_history(symbols: list[str], days: int = 30) -> dict:
    """Return {symbol: {date: {rs21_rank, rs55_rank, rs252_rank}}} for the last `days` dates."""
    if not symbols:
        return {}
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            ph = ",".join(["%s"] * len(symbols))
            cur.execute(
                f"""SELECT symbol, date::text, rs21_rank, rs55_rank, rs252_rank
                    FROM stock_rs_daily
                    WHERE symbol IN ({ph})
                      AND date >= (SELECT MAX(date) - INTERVAL '{days} days' FROM stock_rs_daily)
                    ORDER BY symbol, date ASC""",
                symbols,
            )
            out: dict = {}
            for sym, date, rs21_rank, rs55_rank, rs252_rank in cur.fetchall():
                if sym not in out:
                    out[sym] = {}
                out[sym][date] = {
                    "rs21_rank":  rs21_rank,
                    "rs55_rank":  rs55_rank,
                    "rs252_rank": rs252_rank,
                }
            return out
    finally:
        release_conn(conn)


def purge_old_stock_rs(keep_days: int = 2400):
    """Delete stock_rs_daily rows older than keep_days."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM stock_rs_daily WHERE date < NOW() - INTERVAL '%s days'",
                (keep_days,),
            )
            deleted = cur.rowcount
        conn.commit()
        return deleted
    finally:
        release_conn(conn)


def purge_old_index_rs(keep_days: int = 2400):
    """Delete index_rs rows older than keep_days."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM index_rs WHERE date < NOW() - INTERVAL '%s days'",
                (keep_days,),
            )
            deleted = cur.rowcount
        conn.commit()
        return deleted
    finally:
        release_conn(conn)


def upsert_ohlcv_weekly(rows: list[dict]):
    if not rows:
        return
    sql = """INSERT INTO ohlcv_weekly (symbol,week,open,high,low,close,volume)
             VALUES (%(symbol)s,%(week)s,%(open)s,%(high)s,%(low)s,%(close)s,%(volume)s)
             ON CONFLICT (symbol,week) DO UPDATE SET
                 open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                 close=EXCLUDED.close, volume=EXCLUDED.volume"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=1000)
        conn.commit()
    finally:
        release_conn(conn)


def upsert_indicator_daily(rows: list[dict]):
    if not rows:
        return
    sql = """INSERT INTO indicator_daily (symbol,date,ema20,ema50,sma200,rs_line)
             VALUES (%(symbol)s,%(date)s,%(ema20)s,%(ema50)s,%(sma200)s,%(rs_line)s)
             ON CONFLICT (symbol,date) DO UPDATE SET
                 ema20=EXCLUDED.ema20, ema50=EXCLUDED.ema50,
                 sma200=EXCLUDED.sma200, rs_line=EXCLUDED.rs_line"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=1000)
        conn.commit()
    finally:
        release_conn(conn)


def upsert_indicator_weekly(rows: list[dict]):
    if not rows:
        return
    sql = """INSERT INTO indicator_weekly (symbol,week,ema20,ema50,sma200,rs_line)
             VALUES (%(symbol)s,%(week)s,%(ema20)s,%(ema50)s,%(sma200)s,%(rs_line)s)
             ON CONFLICT (symbol,week) DO UPDATE SET
                 ema20=EXCLUDED.ema20, ema50=EXCLUDED.ema50,
                 sma200=EXCLUDED.sma200, rs_line=EXCLUDED.rs_line"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=1000)
        conn.commit()
    finally:
        release_conn(conn)


def upsert_indicator_monthly(rows: list[dict]):
    """rows: [{symbol, month, ema20, ema50, sma200, rs_line}]"""
    if not rows:
        return
    sql = """INSERT INTO indicator_monthly (symbol,month,ema20,ema50,sma200,rs_line)
             VALUES (%(symbol)s,%(month)s,%(ema20)s,%(ema50)s,%(sma200)s,%(rs_line)s)
             ON CONFLICT (symbol,month) DO UPDATE SET
                 ema20=EXCLUDED.ema20, ema50=EXCLUDED.ema50,
                 sma200=EXCLUDED.sma200, rs_line=EXCLUDED.rs_line"""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=1000)
        conn.commit()
    finally:
        release_conn(conn)


def set_state(key: str, value: str):
    """Upsert a key-value pair into app_state."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app_state (key, value, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (key) DO UPDATE
                SET value = EXCLUDED.value, updated_at = NOW()
                """,
                (key, value),
            )
        conn.commit()
    finally:
        release_conn(conn)


def get_state(key: str) -> str | None:
    """Read a value from app_state. Returns None if key not found."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM app_state WHERE key = %s", (key,))
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        release_conn(conn)


# ── Scanner results cache ─────────────────────────────────────────────────────

def scanner_run_start() -> int:
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO scanner_run_log (status) VALUES ('running') RETURNING id"
            )
            run_id = cur.fetchone()[0]
        conn.commit()
        return run_id
    finally:
        release_conn(conn)


def scanner_run_finish(run_id: int, scanned: int, hits_bull: int, hits_bear: int,
                       errors: int, status: str = "done"):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE scanner_run_log
                   SET finished_at = NOW(), scanned = %s, hits_bull = %s,
                       hits_bear = %s, errors = %s, status = %s
                   WHERE id = %s""",
                (scanned, hits_bull, hits_bear, errors, status, run_id),
            )
        conn.commit()
    finally:
        release_conn(conn)


def upsert_scanner_results(rows: list[dict]):
    """
    Bulk upsert Daily Hunt scanner result rows.
    Each dict: {symbol, company_name, macro, sector, industry, basic_industry,
                direction, m_date, bull_cross_date, s_pullback_date, c_pullback_date,
                above_200sma}
    On conflict (symbol, direction) replace the entire row — one row per symbol per direction.
    """
    if not rows:
        return
    sql = """
    INSERT INTO scanner_results
        (symbol, company_name, macro, sector, industry, basic_industry,
         direction, m_date, m_price, bull_cross_date, s_pullback_date, c_pullback_date,
         above_200sma, scanned_at)
    VALUES
        (%(symbol)s, %(company_name)s, %(macro)s, %(sector)s, %(industry)s,
         %(basic_industry)s, %(direction)s, %(m_date)s, %(m_price)s, %(bull_cross_date)s,
         %(s_pullback_date)s, %(c_pullback_date)s, %(above_200sma)s, NOW())
    ON CONFLICT (symbol, direction) DO UPDATE SET
        company_name    = EXCLUDED.company_name,
        macro           = EXCLUDED.macro,
        sector          = EXCLUDED.sector,
        industry        = EXCLUDED.industry,
        basic_industry  = EXCLUDED.basic_industry,
        m_date          = EXCLUDED.m_date,
        m_price         = EXCLUDED.m_price,
        bull_cross_date = EXCLUDED.bull_cross_date,
        s_pullback_date = EXCLUDED.s_pullback_date,
        c_pullback_date = EXCLUDED.c_pullback_date,
        above_200sma    = EXCLUDED.above_200sma,
        scanned_at      = NOW()
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # ensure m_price key present for every row (execute_batch needs all params)
            for r in rows:
                r.setdefault("m_price", None)
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=200)
        conn.commit()
    finally:
        release_conn(conn)


def clear_stale_scanner_results(keep_days: int = 60):
    """Remove scanner rows not refreshed in keep_days days."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM scanner_results WHERE scanned_at < NOW() - INTERVAL '%s days'",
                (keep_days,),
            )
        conn.commit()
    finally:
        release_conn(conn)


def delete_scanner_results_not_in(hit_keys: list[tuple[str, str]]):
    """Delete (symbol, direction) rows not produced by the current scan run."""
    if not hit_keys:
        return
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM scanner_results
                WHERE (symbol, direction) NOT IN %s
            """, (tuple(hit_keys),))
        conn.commit()
    finally:
        release_conn(conn)


def get_scanner_results(
    direction: str,
    macros: list[str],
    sectors: list[str],
    industries: list[str],
    basic_industries: list[str],
    indices: list[str] | None = None,
    hawks: list[str] | None = None,
    symbols: list[str] | None = None,
) -> list[dict]:
    """
    Return cached Daily Hunt results for the given direction filtered by group selection.
    direction: 'bull' | 'bear'
    """
    conditions = ["sr.direction = %s"]
    params: list = [direction]

    if symbols:
        from_clause = "scanner_results sr"
        ph = ",".join(["%s"] * len(symbols))
        conditions.append(f"sr.symbol IN ({ph})")
        params.extend(symbols)
    elif hawks:
        from_clause = "scanner_results sr JOIN sector_index_map sim ON sim.symbol = sr.symbol"
        ph = ",".join(["%s"] * len(hawks))
        conditions.append(f"sim.index_name IN ({ph})")
        params.extend(hawks)
    elif indices:
        from_clause = "scanner_results sr JOIN sector_index_map sim ON sim.symbol = sr.symbol"
        ph = ",".join(["%s"] * len(indices))
        conditions.append(f"sim.index_name IN ({ph})")
        params.extend(indices)
    else:
        from_clause = "scanner_results sr"
        group_conds = []
        if macros:
            ph = ",".join(["%s"] * len(macros))
            group_conds.append(f"sr.macro IN ({ph})")
            params.extend(macros)
        if sectors:
            ph = ",".join(["%s"] * len(sectors))
            group_conds.append(f"sr.sector IN ({ph})")
            params.extend(sectors)
        if industries:
            ph = ",".join(["%s"] * len(industries))
            group_conds.append(f"sr.industry IN ({ph})")
            params.extend(industries)
        if basic_industries:
            ph = ",".join(["%s"] * len(basic_industries))
            group_conds.append(f"sr.basic_industry IN ({ph})")
            params.extend(basic_industries)
        if group_conds:
            conditions.append("(" + " OR ".join(group_conds) + ")")

    where = " AND ".join(conditions)
    sql = f"""
        SELECT sr.symbol, sr.company_name, sr.macro, sr.sector, sr.industry, sr.basic_industry,
               sr.direction,
               sr.m_date::text,
               sr.m_price,
               sr.bull_cross_date::text,
               sr.s_pullback_date::text,
               sr.c_pullback_date::text,
               COALESCE(sr.above_200sma, FALSE) AS above_200sma,
               sr.scanned_at
        FROM {from_clause}
        WHERE {where}
        ORDER BY GREATEST(
            COALESCE(sr.m_date, '1970-01-01'::date),
            COALESCE(sr.bull_cross_date, '1970-01-01'::date),
            COALESCE(sr.s_pullback_date, '1970-01-01'::date),
            COALESCE(sr.c_pullback_date, '1970-01-01'::date)
        ) DESC, sr.symbol
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
    finally:
        release_conn(conn)


def get_scanner_last_run() -> dict | None:
    """Return the most recent completed scanner run log entry."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT * FROM scanner_run_log
                   ORDER BY started_at DESC LIMIT 1"""
            )
            row = cur.fetchone()
            if not row:
                return None
            r = dict(row)
            for k in ("started_at", "finished_at"):
                if r.get(k):
                    r[k] = str(r[k])
            return r
    finally:
        release_conn(conn)


def get_scanner_run_by_id(run_id: int) -> dict | None:
    """Return a specific scanner run log entry by ID."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM scanner_run_log WHERE id = %s", (run_id,))
            row = cur.fetchone()
            if not row:
                return None
            r = dict(row)
            for k in ("started_at", "finished_at"):
                if r.get(k):
                    r[k] = str(r[k])
            return r
    finally:
        release_conn(conn)


# ── Scan Signals (hourly cycle enrichment) ────────────────────────────────────

def upsert_scan_signals(rows: list[dict]) -> None:
    if not rows:
        return
    import json as _json
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO scan_signals
                   (symbol, direction, step1_date, m_date, m_price, daily_cross_date,
                    above_200sma, phase_end_date, hourly_signals, wh_count, hx_count,
                    latest_hx_ts, computed_at)
                   VALUES %s
                   ON CONFLICT (symbol, direction) DO UPDATE SET
                     step1_date=EXCLUDED.step1_date,
                     m_date=EXCLUDED.m_date,
                     m_price=EXCLUDED.m_price,
                     daily_cross_date=EXCLUDED.daily_cross_date,
                     above_200sma=EXCLUDED.above_200sma,
                     phase_end_date=EXCLUDED.phase_end_date,
                     hourly_signals=EXCLUDED.hourly_signals,
                     wh_count=EXCLUDED.wh_count,
                     hx_count=EXCLUDED.hx_count,
                     latest_hx_ts=EXCLUDED.latest_hx_ts,
                     computed_at=EXCLUDED.computed_at""",
                [(r["symbol"], r["direction"], r.get("step1_date"), r.get("m_date"),
                  r.get("m_price"), r.get("daily_cross_date"),
                  bool(r.get("above_200sma")), r.get("phase_end_date"),
                  _json.dumps(r.get("hourly_signals", [])),
                  r.get("wh_count", 0), r.get("hx_count", 0),
                  r.get("latest_hx_ts"), datetime.datetime.utcnow())
                 for r in rows],
            )
        conn.commit()
    finally:
        release_conn(conn)


def get_scan_signals(direction: str | None = None) -> list[dict]:
    import json as _json
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if direction:
                cur.execute(
                    "SELECT * FROM scan_signals WHERE direction=%s ORDER BY latest_hx_ts DESC NULLS LAST",
                    (direction,),
                )
            else:
                cur.execute(
                    "SELECT * FROM scan_signals ORDER BY latest_hx_ts DESC NULLS LAST"
                )
            cols = [d[0] for d in cur.description]
            result = []
            for row in cur.fetchall():
                d = dict(zip(cols, row))
                try:
                    d["hourly_signals"] = _json.loads(d["hourly_signals"] or "[]")
                except Exception:
                    d["hourly_signals"] = []
                result.append(d)
            return result
    finally:
        release_conn(conn)


def delete_scan_signals_not_in(hit_keys: list[tuple[str, str]]) -> None:
    if not hit_keys:
        return
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Build a VALUES list and delete in one atomic query — avoids temp table pool issues
            values_sql = ",".join(cur.mogrify("(%s,%s)", k).decode() for k in hit_keys)
            cur.execute(f"""
                DELETE FROM scan_signals
                WHERE (symbol, direction) NOT IN ({values_sql})
            """)
        conn.commit()
    finally:
        release_conn(conn)


# ── Watchlist ─────────────────────────────────────────────────────────────────

def get_watchlist():
    """Return list of saved watchlist symbols (active tickers only), newest-added first."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT w.symbol FROM watchlist w
                   JOIN tickers t ON t.symbol=w.symbol AND t.is_active=TRUE
                   ORDER BY w.added_at DESC, w.symbol ASC"""
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        release_conn(conn)


def add_to_watchlist(symbol):
    """Add a symbol to the watchlist (idempotent)."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO watchlist (symbol) VALUES (%s) ON CONFLICT (symbol) DO NOTHING",
                (symbol,),
            )
        conn.commit()
    finally:
        release_conn(conn)


def remove_from_watchlist(symbol):
    """Remove a symbol from the watchlist."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM watchlist WHERE symbol = %s", (symbol,))
        conn.commit()
    finally:
        release_conn(conn)


# ── Journal ───────────────────────────────────────────────────────────────────
# One entry per symbol. snapshot_json is a FROZEN capture (never auto-updated);
# plan_json holds the editable trade plan (entry/SL/exits/notes).
# JSONB columns: psycopg returns dict directly, accepts dict via Json() adapter.

def _as_dict(v):
    """psycopg returns JSONB as dict already; tolerate str just in case."""
    import json as _json
    if v is None:
        return {}
    if isinstance(v, (dict, list)):
        return v
    try:
        return _json.loads(v)
    except Exception:
        return {}


def list_journal_entries():
    """Return all journal entries (newest first), parsed JSON included."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT symbol, status, snapshot_json, plan_json, created_at, updated_at
                   FROM journal_entries ORDER BY created_at DESC, symbol ASC"""
            )
            out = []
            for r in cur.fetchall():
                out.append({
                    "symbol": r[0], "status": r[1],
                    "snapshot": _as_dict(r[2]), "plan": _as_dict(r[3]),
                    "created_at": str(r[4]), "updated_at": str(r[5]),
                })
            return out
    finally:
        release_conn(conn)


def get_journal_entry(symbol):
    """Return a single journal entry (parsed) or None."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT symbol, status, snapshot_json, plan_json, created_at, updated_at
                   FROM journal_entries WHERE symbol = %s""", (symbol,)
            )
            r = cur.fetchone()
            if not r:
                return None
            return {
                "symbol": r[0], "status": r[1],
                "snapshot": _as_dict(r[2]), "plan": _as_dict(r[3]),
                "created_at": str(r[4]), "updated_at": str(r[5]),
            }
    finally:
        release_conn(conn)


def upsert_journal_entry(symbol, snapshot, plan, status="open"):
    """Insert a new journal entry. No-op if symbol already exists."""
    import json as _json
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """INSERT INTO journal_entries (symbol, status, snapshot_json, plan_json)
                   VALUES (%s, %s, %s::jsonb, %s::jsonb)
                   ON CONFLICT (symbol) DO NOTHING""",
                (symbol, status, _json.dumps(snapshot), _json.dumps(plan or {})),
            )
        conn.commit()
    finally:
        release_conn(conn)


def update_journal_plan(symbol, plan, status=None):
    """Update ONLY the editable trade plan (and optionally status). Snapshot untouched."""
    import json as _json
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if status is not None:
                cur.execute(
                    """UPDATE journal_entries
                       SET plan_json = %s::jsonb, status = %s, updated_at = now()
                       WHERE symbol = %s""",
                    (_json.dumps(plan or {}), status, symbol),
                )
            else:
                cur.execute(
                    """UPDATE journal_entries
                       SET plan_json = %s::jsonb, updated_at = now()
                       WHERE symbol = %s""",
                    (_json.dumps(plan or {}), symbol),
                )
        conn.commit()
    finally:
        release_conn(conn)


def recapture_journal_snapshot(symbol, snapshot):
    """Deliberately overwrite the frozen snapshot (explicit user action only)."""
    import json as _json
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE journal_entries
                   SET snapshot_json = %s::jsonb, updated_at = now()
                   WHERE symbol = %s""",
                (_json.dumps(snapshot), symbol),
            )
        conn.commit()
    finally:
        release_conn(conn)


def delete_journal_entry(symbol):
    """Delete a journal entry entirely."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM journal_entries WHERE symbol = %s", (symbol,))
        conn.commit()
    finally:
        release_conn(conn)


# Legacy alias — index_data.py writes via this name; now routes to ohlcv_daily
def upsert_index_prices(rows: list[dict]):
    upsert_ohlcv_daily(rows)


def get_index_prices(symbol: str, from_date: str, to_date: str) -> list[dict]:
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """SELECT date::text, open, high, low, close, volume
                   FROM ohlcv_daily
                   WHERE symbol = %s AND date BETWEEN %s AND %s
                   ORDER BY date""",
                (symbol, from_date, to_date),
            )
            return [dict(r) for r in cur.fetchall()]
    finally:
        release_conn(conn)


def get_index_closes(symbols: list[str], from_date: str, to_date: str) -> dict[str, list[tuple]]:
    if not symbols:
        return {}
    conn = get_conn()
    try:
        ph = ",".join(["%s"] * len(symbols))
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT symbol, date, close
                    FROM ohlcv_daily
                    WHERE symbol IN ({ph}) AND date BETWEEN %s AND %s
                    ORDER BY symbol, date""",
                symbols + [from_date, to_date],
            )
            result: dict[str, list] = {}
            for sym, dt, close in cur.fetchall():
                result.setdefault(sym, []).append((dt, float(close) if close else None))
        return result
    finally:
        release_conn(conn)


def get_latest_index_price_dates(symbols: list[str]) -> dict[str, str]:
    if not symbols:
        return {}
    conn = get_conn()
    try:
        ph = ",".join(["%s"] * len(symbols))
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT symbol, MAX(date)::text FROM ohlcv_daily WHERE symbol IN ({ph}) GROUP BY symbol",
                symbols,
            )
            return {r[0]: r[1] for r in cur.fetchall()}
    finally:
        release_conn(conn)


def upsert_index_rs(rows: list[dict]):
    """Bulk upsert RS ratings. Each dict: {symbol, date, rs_raw, rs_rating, mid_score, short_score, rs21, rs55, day_return, week_return, close, group_short_score, group_mid_score}."""
    if not rows:
        return
    sql = """
    INSERT INTO index_rs (symbol, date, rs_raw, rs_rating, mid_score, short_score, rs21, rs55, day_return, week_return, close, group_short_score, group_mid_score, group_lt_score)
    VALUES (%(symbol)s, %(date)s, %(rs_raw)s, %(rs_rating)s, %(mid_score)s, %(short_score)s,
            %(rs21)s, %(rs55)s, %(day_return)s, %(week_return)s, %(close)s, %(group_short_score)s, %(group_mid_score)s, %(group_lt_score)s)
    ON CONFLICT (symbol, date) DO UPDATE SET
        rs_raw            = EXCLUDED.rs_raw,
        rs_rating         = EXCLUDED.rs_rating,
        mid_score         = EXCLUDED.mid_score,
        short_score       = EXCLUDED.short_score,
        rs21              = COALESCE(EXCLUDED.rs21,              index_rs.rs21),
        rs55              = COALESCE(EXCLUDED.rs55,              index_rs.rs55),
        day_return        = COALESCE(EXCLUDED.day_return,        index_rs.day_return),
        week_return       = COALESCE(EXCLUDED.week_return,       index_rs.week_return),
        close             = COALESCE(EXCLUDED.close,             index_rs.close),
        group_short_score = COALESCE(EXCLUDED.group_short_score, index_rs.group_short_score),
        group_mid_score   = COALESCE(EXCLUDED.group_mid_score,   index_rs.group_mid_score),
        group_lt_score    = COALESCE(EXCLUDED.group_lt_score,    index_rs.group_lt_score)
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
        conn.commit()
    finally:
        release_conn(conn)


def get_latest_index_rs(date: str | None = None) -> list[dict]:
    """
    Return RS ratings for the most recent computed date (or specific date).
    Returns list of {symbol, date, rs_raw, rs_rating} ordered by rs_rating desc.
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if date:
                cur.execute(
                    """SELECT symbol, date::text, rs_raw, rs_rating, group_lt_score
                       FROM index_rs WHERE date = %s ORDER BY rs_rating DESC""",
                    (date,),
                )
            else:
                cur.execute(
                    """SELECT ir.symbol, ir.date::text, ir.rs_raw, ir.rs_rating, ir.group_lt_score
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
                )
            return [dict(r) for r in cur.fetchall()]
    finally:
        release_conn(conn)

def get_index_prices_date_range(symbols: list[str] | None = None) -> dict:
    """Return coverage stats for index/ETF symbols in ohlcv_daily.
    symbols: the known index/ETF symbols (must be told which rows are indices —
    stocks and indices share ohlcv_daily with no distinguishing symbol prefix)."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            if symbols:
                ph = ",".join(["%s"] * len(symbols))
                cur.execute(
                    f"SELECT COUNT(DISTINCT symbol), MIN(date), MAX(date) FROM ohlcv_daily WHERE symbol IN ({ph})",
                    symbols,
                )
                row = cur.fetchone()
            else:
                row = (0, None, None)
            return {
                "symbols": row[0] or 0,
                "from":    str(row[1]) if row[1] else None,
                "to":      str(row[2]) if row[2] else None,
            }
    finally:
        release_conn(conn)




# ── Sector index map ──────────────────────────────────────────────────────────

def clear_sector_index_map():
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM sector_index_map")
        conn.commit()
    finally:
        release_conn(conn)


def purge_sector_index_map_deleted(active_symbols: set):
    """Remove rows for symbols no longer in tickers (delisted/removed stocks)."""
    if not active_symbols:
        return
    ph = ",".join(["%s"] * len(active_symbols))
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM sector_index_map WHERE symbol NOT IN ({ph})", list(active_symbols))
        conn.commit()
    finally:
        release_conn(conn)


def upsert_sector_index_map(rows: list[tuple]):
    """Bulk insert (symbol, index_name, index_symbol, source, confidence) tuples."""
    if not rows:
        return
    sql = """
    INSERT INTO sector_index_map (symbol, index_name, index_symbol, source, confidence)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (symbol, index_name) DO UPDATE SET
        index_symbol = EXCLUDED.index_symbol,
        source       = EXCLUDED.source,
        confidence   = EXCLUDED.confidence
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
        conn.commit()
    finally:
        release_conn(conn)
