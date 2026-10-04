"""
run_indicator_backfill.py — Pre-compute EMA20, EMA50, SMA200, RS line for all
symbols (stocks + indices) across daily, weekly and monthly timeframes.

Stores results in:
  indicator_daily   — one row per (symbol, date)
  indicator_weekly  — one row per (symbol, week/Monday)
  indicator_monthly — one row per (symbol, month/1st)
  ohlcv_weekly      — weekly OHLCV candles resampled from ohlcv_daily
  ohlcv_monthly     — monthly OHLCV candles resampled from ohlcv_daily

No intraday/hourly fetch or storage — retired along with the hourly cycle
scanner since this product runs on a monthly cadence; daily history already
covers everything the chart/indicator pipeline needs, including monthly bars
(resampled, not fetched separately).

Warmup:
  Daily   SMA200 → needs 200 daily bars   (~10 months)
  Weekly  SMA200 → needs 200 weekly bars  (~4 years)
  Monthly SMA200 → needs 200 monthly bars (~16.7 years) — won't populate until
                    enough history accumulates; harmless, same as any other
                    instrument with insufficient history.
  EMA50/20 computed from bar-1, accurate from bar 50/20 onwards.

Benchmark for RS line: ^GSPC (S&P 500)

Every run recomputes each symbol's full stored history (cheap — one indexed
SELECT + an O(n) EMA/SMA pass per symbol), so results stay correct however
long the gap since the last run; there is no lookback-windowed mode.

Run:
    python run_indicator_backfill.py
    python run_indicator_backfill.py --test AAPL ^GSPC
"""

import datetime
import logging
import sqlite3
import sys
from collections import defaultdict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("indicator_backfill.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)

import db_local as db

BENCH        = "^GSPC"
BATCH_SIZE   = 50   # symbols per DB write batch


# ── Math helpers ──────────────────────────────────────────────────────────────

def _ema_full(closes: list) -> tuple:
    """Return (ema20, ema50) full arrays, same length as closes. None until warmed up."""
    n = len(closes)
    ema20 = [None] * n
    ema50 = [None] * n
    k20, k50 = 2/(20+1), 2/(50+1)

    s20 = s50 = 0.0
    for i, c in enumerate(closes):
        if i < 19:
            s20 += c
        elif i == 19:
            s20 += c
            ema20[i] = round(s20 / 20, 4)
        else:
            ema20[i] = round(ema20[i-1] * (1-k20) + c * k20, 4)

        if i < 49:
            s50 += c
        elif i == 49:
            s50 += c
            ema50[i] = round(s50 / 50, 4)
        else:
            ema50[i] = round(ema50[i-1] * (1-k50) + c * k50, 4)

    return ema20, ema50


def _sma200_full(closes: list) -> list:
    """Return SMA200 full array. None until bar 199."""
    n = len(closes)
    sma = [None] * n
    if n < 200:
        return sma
    win = sum(closes[:200])
    sma[199] = round(win / 200, 4)
    for i in range(200, n):
        win += closes[i] - closes[i - 200]
        sma[i] = round(win / 200, 4)
    return sma


def _rs_line(closes: list, bench_closes: list) -> list:
    """RS line = close / bench_close. None where bench is missing/zero."""
    return [
        round(c / b, 6) if (b and b > 0) else None
        for c, b in zip(closes, bench_closes)
    ]


# ── Daily ─────────────────────────────────────────────────────────────────────

def backfill_daily(con: sqlite3.Connection, symbols: list, bench_closes: dict):
    """Compute and store indicator_daily for all symbols."""
    logger.info("Daily: processing %d symbols", len(symbols))
    total = 0

    for i, sym in enumerate(symbols):
        rows = con.execute(
            "SELECT date, close FROM ohlcv_daily WHERE symbol=? AND close IS NOT NULL ORDER BY date ASC", (sym,)
        ).fetchall()
        if not rows:
            continue

        dates  = [r[0] for r in rows]
        closes = [float(r[1]) for r in rows]

        ema20, ema50 = _ema_full(closes)
        sma200       = _sma200_full(closes)
        bench_arr    = [bench_closes.get(d) for d in dates]
        rs           = _rs_line(closes, bench_arr)

        out = []
        for j, date in enumerate(dates):
            out.append({
                "symbol":  sym,
                "date":    date,
                "ema20":   ema20[j],
                "ema50":   ema50[j],
                "sma200":  sma200[j],
                "rs_line": rs[j],
            })

        db.upsert_indicator_daily(out)
        total += len(out)

        if (i + 1) % 100 == 0 or (i + 1) == len(symbols):
            logger.info("  Daily [%d/%d] done — %d rows upserted so far", i+1, len(symbols), total)

    logger.info("Daily complete: %d rows", total)


# ── Weekly ────────────────────────────────────────────────────────────────────

def _resample_weekly(dates: list, opens: list, highs: list, lows: list,
                     closes: list, vols: list) -> dict:
    """Resample daily OHLCV to weekly (Monday-keyed ISO buckets)."""
    buckets = defaultdict(lambda: {"o": None, "h": -1e18, "l": 1e18, "c": None, "v": 0})
    for d, o, h, l, c, v in zip(dates, opens, highs, lows, closes, vols):
        dt  = datetime.date.fromisoformat(d)
        mon = (dt - datetime.timedelta(days=dt.weekday())).isoformat()
        bk  = buckets[mon]
        if bk["o"] is None:
            bk["o"] = o
        bk["h"] = max(bk["h"], h)
        bk["l"] = min(bk["l"], l)
        bk["c"] = c
        bk["v"] += int(v or 0)
    return buckets


def backfill_weekly(con: sqlite3.Connection, symbols: list, bench_daily_closes: dict):
    """Compute and store ohlcv_weekly + indicator_weekly for all symbols."""
    logger.info("Weekly: processing %d symbols", len(symbols))

    # Build weekly benchmark closes using the LAST available day of each week
    # (same as w_closes for each stock — Friday close / Friday bench = correct ratio).
    # Using first-day bench was wrong: stock uses Friday close, bench used Monday close.
    bench_weekly: dict = {}
    for date_str, bc in sorted(bench_daily_closes.items()):
        dt  = datetime.date.fromisoformat(date_str)
        mon = (dt - datetime.timedelta(days=dt.weekday())).isoformat()
        bench_weekly[mon] = bc  # overwrite every day → ends up as last (Friday) close

    total_ohlcv = total_ind = 0

    for i, sym in enumerate(symbols):
        rows = con.execute(
            "SELECT date, open, high, low, close, volume FROM ohlcv_daily WHERE symbol=? AND close IS NOT NULL ORDER BY date ASC",
            (sym,)
        ).fetchall()
        if not rows:
            continue

        dates  = [r[0] for r in rows]
        opens  = [float(r[1] or 0) for r in rows]
        highs  = [float(r[2] or 0) for r in rows]
        lows   = [float(r[3] or 0) for r in rows]
        closes = [float(r[4]) for r in rows]
        vols   = [int(r[5] or 0) for r in rows]

        buckets   = _resample_weekly(dates, opens, highs, lows, closes, vols)
        w_weeks   = sorted(buckets.keys())
        w_opens   = [buckets[w]["o"] for w in w_weeks]
        w_highs   = [buckets[w]["h"] for w in w_weeks]
        w_lows    = [buckets[w]["l"] for w in w_weeks]
        w_closes  = [buckets[w]["c"] for w in w_weeks]
        w_vols    = [buckets[w]["v"] for w in w_weeks]

        # Store weekly OHLCV
        ohlcv_rows = [
            {"symbol": sym, "week": w, "open": o, "high": h,
             "low": l, "close": c, "volume": v}
            for w, o, h, l, c, v in zip(w_weeks, w_opens, w_highs, w_lows, w_closes, w_vols)
        ]
        db.upsert_ohlcv_weekly(ohlcv_rows)
        total_ohlcv += len(ohlcv_rows)

        # Compute indicators on full weekly history
        ema20, ema50 = _ema_full(w_closes)
        sma200       = _sma200_full(w_closes)
        bench_arr    = [bench_weekly.get(w) for w in w_weeks]
        rs           = _rs_line(w_closes, bench_arr)

        ind_rows = [
            {"symbol": sym, "week": w,
             "ema20": ema20[j], "ema50": ema50[j],
             "sma200": sma200[j], "rs_line": rs[j]}
            for j, w in enumerate(w_weeks)
        ]
        db.upsert_indicator_weekly(ind_rows)
        total_ind += len(ind_rows)

        if (i + 1) % 100 == 0 or (i + 1) == len(symbols):
            logger.info("  Weekly [%d/%d] done — ohlcv=%d ind=%d", i+1, len(symbols), total_ohlcv, total_ind)

    logger.info("Weekly complete: ohlcv=%d rows, indicators=%d rows", total_ohlcv, total_ind)


# ── Hourly ────────────────────────────────────────────────────────────────────

def _resample_monthly(dates: list, opens: list, highs: list, lows: list,
                      closes: list, vols: list) -> dict:
    """Resample daily OHLCV to monthly (1st-of-month-keyed buckets)."""
    buckets = defaultdict(lambda: {"o": None, "h": -1e18, "l": 1e18, "c": None, "v": 0})
    for d, o, h, l, c, v in zip(dates, opens, highs, lows, closes, vols):
        dt  = datetime.date.fromisoformat(d)
        mon = dt.replace(day=1).isoformat()
        bk  = buckets[mon]
        if bk["o"] is None:
            bk["o"] = o
        bk["h"] = max(bk["h"], h)
        bk["l"] = min(bk["l"], l)
        bk["c"] = c
        bk["v"] += int(v or 0)
    return buckets


def backfill_monthly(con: sqlite3.Connection, symbols: list, bench_daily_closes: dict):
    """Compute and store ohlcv_monthly + indicator_monthly for all symbols.
    Resampled entirely from ohlcv_daily — no extra fetch, works with whatever
    daily history is already on disk."""
    logger.info("Monthly: processing %d symbols", len(symbols))

    # Build monthly benchmark closes using the LAST available day of each month.
    bench_monthly: dict = {}
    for date_str, bc in sorted(bench_daily_closes.items()):
        dt  = datetime.date.fromisoformat(date_str)
        mon = dt.replace(day=1).isoformat()
        bench_monthly[mon] = bc  # overwrite every day → ends up as last close of month

    total_ohlcv = total_ind = 0

    for i, sym in enumerate(symbols):
        rows = con.execute(
            "SELECT date, open, high, low, close, volume FROM ohlcv_daily WHERE symbol=? AND close IS NOT NULL ORDER BY date ASC",
            (sym,)
        ).fetchall()
        if not rows:
            continue

        dates  = [r[0] for r in rows]
        opens  = [float(r[1] or 0) for r in rows]
        highs  = [float(r[2] or 0) for r in rows]
        lows   = [float(r[3] or 0) for r in rows]
        closes = [float(r[4]) for r in rows]
        vols   = [int(r[5] or 0) for r in rows]

        buckets   = _resample_monthly(dates, opens, highs, lows, closes, vols)
        m_months  = sorted(buckets.keys())
        m_opens   = [buckets[m]["o"] for m in m_months]
        m_highs   = [buckets[m]["h"] for m in m_months]
        m_lows    = [buckets[m]["l"] for m in m_months]
        m_closes  = [buckets[m]["c"] for m in m_months]
        m_vols    = [buckets[m]["v"] for m in m_months]

        ohlcv_rows = [
            {"symbol": sym, "month": m, "open": o, "high": h,
             "low": l, "close": c, "volume": v}
            for m, o, h, l, c, v in zip(m_months, m_opens, m_highs, m_lows, m_closes, m_vols)
        ]
        db.upsert_ohlcv_monthly(ohlcv_rows)
        total_ohlcv += len(ohlcv_rows)

        ema20, ema50 = _ema_full(m_closes)
        sma200       = _sma200_full(m_closes)
        bench_arr    = [bench_monthly.get(m) for m in m_months]
        rs           = _rs_line(m_closes, bench_arr)

        ind_rows = [
            {"symbol": sym, "month": m,
             "ema20": ema20[j], "ema50": ema50[j],
             "sma200": sma200[j], "rs_line": rs[j]}
            for j, m in enumerate(m_months)
        ]
        db.upsert_indicator_monthly(ind_rows)
        total_ind += len(ind_rows)

        if (i + 1) % 100 == 0 or (i + 1) == len(symbols):
            logger.info("  Monthly [%d/%d] done — ohlcv=%d ind=%d", i+1, len(symbols), total_ohlcv, total_ind)

    logger.info("Monthly complete: ohlcv=%d rows, indicators=%d rows", total_ohlcv, total_ind)


# ── Entry point (called from main.py / run_price_refresh.py / run_flash_refresh.py) ──

def run_incremental() -> dict:
    """
    Recompute EMA20/50, SMA200 and RS line for every symbol across daily,
    weekly and monthly, called automatically after every price refresh.

    Despite the name (kept for backward compatibility with existing callers),
    this is NOT scoped to a trailing lookback window — backfill_daily/weekly/
    monthly always reprocess each symbol's full stored history (cheap: it's a
    single indexed SELECT + an O(n) EMA/SMA pass per symbol, not a per-row
    fetch), so results stay correct however long the gap since the last run
    — safe for an infrequent (e.g. monthly) refresh cadence, not just daily.

    Returns a summary dict with row counts.
    """
    import config
    if getattr(config, "DB_MODE", "local") != "local":
        # Neon mode: raw sqlite3 not available — skip (Neon backfill handled separately)
        logger.warning("run_incremental: skipped (DB_MODE != local)")
        return {"ok": False, "reason": "non-local DB mode"}

    con = sqlite3.connect("local.db")
    con.row_factory = sqlite3.Row
    try:
        # All symbols with daily prices
        all_syms = [r[0] for r in con.execute(
            "SELECT DISTINCT symbol FROM ohlcv_daily ORDER BY symbol"
        ).fetchall()]

        # Full benchmark daily closes (needed for full EMA warmup on RS line)
        bench_daily = {
            r[0]: float(r[1])
            for r in con.execute(
                "SELECT date, close FROM ohlcv_daily WHERE symbol=? AND close IS NOT NULL ORDER BY date ASC", (BENCH,)
            ).fetchall()
        }

        logger.info("Indicator refresh: %d symbols, full history each (no lookback window)", len(all_syms))

        logger.info("STEP 1/3 — Daily indicators")
        backfill_daily(con, all_syms, bench_daily)

        logger.info("STEP 2/3 — Weekly OHLCV + indicators")
        backfill_weekly(con, all_syms, bench_daily)

        logger.info("STEP 3/3 — Monthly OHLCV + indicators")
        backfill_monthly(con, all_syms, bench_daily)

        # Row counts for SSE status message
        counts = {}
        for tbl in ["ohlcv_weekly", "ohlcv_monthly", "indicator_daily", "indicator_weekly", "indicator_monthly"]:
            counts[tbl] = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]

        logger.info("Indicator refresh complete: %s", counts)
        return {"ok": True, "counts": counts, "symbols": len(all_syms)}
    except Exception as e:
        logger.exception("run_incremental failed: %s", e)
        return {"ok": False, "error": str(e)}
    finally:
        con.close()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    args = sys.argv[1:]
    test_syms   = []
    if "--test" in args:
        idx = args.index("--test")
        test_syms = [s.upper() for s in args[idx+1:] if not s.startswith("--")]

    db.init_db()
    con = sqlite3.connect("local.db")
    con.row_factory = sqlite3.Row

    if test_syms:
        symbols = test_syms
        logger.info("TEST mode: %s", symbols)
    else:
        # All stocks + indices
        all_syms = [r[0] for r in con.execute(
            "SELECT DISTINCT symbol FROM ohlcv_daily ORDER BY symbol"
        ).fetchall()]
        symbols = all_syms
        logger.info("Full backfill: %d symbols", len(symbols))

    # Load full daily benchmark closes for RS line
    bench_daily = {
        r[0]: float(r[1])
        for r in con.execute(
            "SELECT date, close FROM ohlcv_daily WHERE symbol=? AND close IS NOT NULL ORDER BY date ASC", (BENCH,)
        ).fetchall()
    }
    logger.info("Benchmark loaded: %d daily bars", len(bench_daily))

    logger.info("=" * 60)
    logger.info("STEP 1/3 — Daily indicators")
    backfill_daily(con, symbols, bench_daily)

    logger.info("=" * 60)
    logger.info("STEP 2/3 — Weekly OHLCV + indicators")
    backfill_weekly(con, symbols, bench_daily)

    logger.info("=" * 60)
    logger.info("STEP 3/3 — Monthly OHLCV + indicators")
    backfill_monthly(con, symbols, bench_daily)

    logger.info("=" * 60)
    logger.info("All done. Verifying row counts...")

    for tbl in ["ohlcv_weekly", "ohlcv_monthly", "indicator_daily", "indicator_weekly", "indicator_monthly"]:
        cnt  = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        syms = con.execute(f"SELECT COUNT(DISTINCT symbol) FROM {tbl}").fetchone()[0]
        logger.info("  %-22s rows=%d  symbols=%d", tbl, cnt, syms)

    con.close()


if __name__ == "__main__":
    main()
