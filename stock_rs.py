"""
stock_rs.py — Pre-compute RS21, RS55, RS252 and RS line
for every stock in ohlcv_daily, ranked across the full US stock universe.

Called nightly by run_stock_rs.py after price data is fresh.

Key design rules:
- Each date's ranks are computed independently from data available up to that date only.
  No future data bleeds into historical ranks.
- Universe = all symbols in ohlcv_daily that are NOT one of the known sector/
  broad-market index symbols (index_data.SYMBOLS) — stocks and indices share
  the ohlcv_daily table with no distinguishing symbol prefix.
- Benchmark = market_data.BENCHMARK_SYMBOL (^GSPC) for RS line and RS21 ratio.
- Minimum bars required per symbol: 56 (for RS21 + EMA20 warmup).
"""

import logging
from collections import defaultdict

import index_data
import market_data

logger = logging.getLogger(__name__)

try:
    from config import DB_MODE as _DB_MODE
except ImportError:
    _DB_MODE = "neon"

_BENCHMARK    = market_data.BENCHMARK_SYMBOL
_MIN_BARS     = 56    # need close[-56] for RS55; covers RS21 + EMA20 warmup
_OHLCV_DAYS   = 730   # fetch 730 calendar days of OHLCV — gives ~520 trading days (need 253+ for rs252)
_RS252_BARS   = 63    # O'Neil quarter = ~63 trading days; 4 quarters = 252 bars


def _index_symbols_clause(placeholder: str) -> tuple[str, list[str]]:
    """Returns (sql_fragment, params) for 'symbol NOT IN (...)' over the known index symbols."""
    syms = index_data.SYMBOLS
    ph = ",".join([placeholder] * len(syms))
    return f"symbol NOT IN ({ph})", syms


# ── Math helpers ──────────────────────────────────────────────────────────────

def _ema(prices: list, period: int) -> list:
    """Exponential moving average. Returns list same length as prices."""
    if not prices:
        return []
    k = 2.0 / (period + 1)
    out = [prices[0]]
    for p in prices[1:]:
        out.append(out[-1] + k * (p - out[-1]))
    return out


def _rank_1_99(values: list[float]) -> list[int]:
    """Given a list of floats, return their 1-99 percentile ranks (highest value = rank 99).
    Ties get the same rank. Returns list of ints same length as values."""
    n = len(values)
    if n == 0:
        return []
    if n == 1:
        return [50]
    indexed = sorted(enumerate(values), key=lambda x: x[1])
    ranks = [0] * n
    for pos, (orig_idx, _) in enumerate(indexed):
        pct = pos / (n - 1)
        ranks[orig_idx] = max(1, min(99, round(pct * 98 + 1)))
    return ranks


# ── Per-symbol metrics for a single date ─────────────────────────────────────

def _compute_one(closes: list[float]) -> dict:
    """Compute all metrics for one symbol using data up to a given date."""
    n = len(closes)

    close_now = closes[-1] if n >= 1 else None
    close_1d  = closes[-2] if n >= 2 else None
    close_21  = closes[-22] if n >= 22 else None
    close_55  = closes[-56] if n >= 56 else None

    day_return = round((close_now / close_1d - 1) * 100, 2) if close_now and close_1d else None

    return {
        "close":      close_now,
        "close_21":   close_21,
        "close_55":   close_55,
        "day_return": day_return,
    }


# ── Main compute function ─────────────────────────────────────────────────────

def compute_and_store(dates_to_compute: list[str]) -> dict:
    """
    For each date in dates_to_compute, compute RS21/UDVR/CMF ranks for all stocks
    and upsert into stock_rs_daily.

    dates_to_compute: list of 'YYYY-MM-DD' strings, must be trading days with price data.
    Returns: {"computed": N, "skipped": N, "failed": [dates]}
    """
    import db

    if not dates_to_compute:
        return {"computed": 0, "skipped": 0, "failed": []}

    logger.info("stock_rs: fetching OHLCV for %d dates to compute", len(dates_to_compute))

    # Determine how far back we need to fetch (need _MIN_BARS + 21 extra days of history
    # before the earliest compute date, so compute from 120 calendar days before earliest).
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            # Fetch all OHLCV for stocks within the required window.
            # We need data from well before the earliest compute date for warmup.
            earliest = min(dates_to_compute)
            if _DB_MODE == "local":
                cur.execute(
                    """SELECT symbol, date, open, high, low, close, volume
                       FROM ohlcv_daily
                       WHERE date >= date(?, '-730 days')
                         AND symbol IN (SELECT symbol FROM tickers WHERE is_active=1)
                       ORDER BY symbol, date ASC""",
                    (earliest,),
                )
            else:
                cur.execute(
                    """SELECT symbol, date::text, open, high, low, close, volume
                       FROM ohlcv_daily
                       WHERE date >= (%s::date - INTERVAL '730 days')
                         AND symbol IN (SELECT symbol FROM tickers WHERE is_active=TRUE)
                       ORDER BY symbol, date ASC""",
                    (earliest,),
                )
            stock_rows = cur.fetchall()

            # Fetch benchmark (NIFTY500) closes for RS line + RS21 ratio
            if _DB_MODE == "local":
                cur.execute(
                    """SELECT date, close
                       FROM ohlcv_daily
                       WHERE symbol = ?
                         AND date >= date(?, '-730 days')
                       ORDER BY date ASC""",
                    (_BENCHMARK, earliest),
                )
            else:
                cur.execute(
                    """SELECT date::text, close
                       FROM ohlcv_daily
                       WHERE symbol = %s
                         AND date >= (%s::date - INTERVAL '730 days')
                       ORDER BY date ASC""",
                    (_BENCHMARK, earliest),
                )
            bench_rows = cur.fetchall()
    finally:
        db.release_conn(conn)

    # Build per-symbol OHLCV history dict
    # sym_data[sym] = list of (date, open, high, low, close, volume) sorted ASC
    sym_data: dict[str, list] = defaultdict(list)
    for sym, date, o, h, l, c, v in stock_rows:
        sym_data[sym].append((date, float(o or 0), float(h or 0),
                              float(l or 0), float(c or 0), float(v or 0)))

    bench_by_date: dict[str, float] = {date: float(c) for date, c in bench_rows}

    # Also fetch ticker meta for company/sector info (used by API, not stored here)
    # (API joins tickers separately)

    computed_total = 0
    skipped_total  = 0
    failed_dates   = []

    for target_date in sorted(dates_to_compute):
        logger.info("stock_rs: computing %s ...", target_date)

        bench_close = bench_by_date.get(target_date)
        if not bench_close:
            logger.warning("stock_rs: no benchmark close for %s, skipping", target_date)
            skipped_total += 1
            continue

        # For this date, build per-symbol slice up to and including target_date
        day_metrics: dict[str, dict] = {}
        for sym, rows in sym_data.items():
            # Keep only rows with date <= target_date
            slice_ = [(r[0], r[1], r[2], r[3], r[4], r[5])
                      for r in rows if r[0] <= target_date]
            if not slice_:
                continue
            closes  = [r[4] for r in slice_]
            day_metrics[sym] = _compute_one(closes)

        if not day_metrics:
            logger.warning("stock_rs: no symbols computed for %s", target_date)
            skipped_total += 1
            continue

        # Compute RS21 raw for each symbol (needs benchmark)
        # RS21 = (stock_close/bench_close) / (stock_close_21/bench_close_21) - 1
        # We need bench_close_21d_ago — find the benchmark close 21 trading days before target_date
        bench_dates_sorted = sorted(bench_by_date.keys())
        bench_dates_up_to  = [d for d in bench_dates_sorted if d <= target_date]
        bench_close_21 = bench_by_date[bench_dates_up_to[-22]] if len(bench_dates_up_to) >= 22 else None
        bench_close_55 = bench_by_date[bench_dates_up_to[-56]] if len(bench_dates_up_to) >= 56 else None

        syms      = list(day_metrics.keys())

        # Pre-compute benchmark closes at Q1/Q2/Q3/Q4 boundaries for rs252
        bench_close_q1 = bench_by_date[bench_dates_up_to[-(_RS252_BARS + 1)]]      if len(bench_dates_up_to) > _RS252_BARS       else None
        bench_close_q2 = bench_by_date[bench_dates_up_to[-(2 * _RS252_BARS + 1)]]  if len(bench_dates_up_to) > 2 * _RS252_BARS   else None
        bench_close_q3 = bench_by_date[bench_dates_up_to[-(3 * _RS252_BARS + 1)]]  if len(bench_dates_up_to) > 3 * _RS252_BARS   else None
        bench_close_q4 = bench_by_date[bench_dates_up_to[-(4 * _RS252_BARS + 1)]]  if len(bench_dates_up_to) > 4 * _RS252_BARS   else None

        # Compute RS21/RS55 raw values (None if not enough history)
        for sym in syms:
            m = day_metrics[sym]
            rp_now = m["close"] / bench_close if (m["close"] and bench_close and bench_close > 0) else None

            if rp_now and bench_close_21 and bench_close_21 > 0 and m["close_21"] and m["close_21"] > 0:
                m["rs21"] = round((rp_now / (m["close_21"] / bench_close_21) - 1) * 100, 4)
            else:
                m["rs21"] = None

            if rp_now and bench_close_55 and bench_close_55 > 0 and m["close_55"] and m["close_55"] > 0:
                m["rs55"] = round((rp_now / (m["close_55"] / bench_close_55) - 1) * 100, 4)
            else:
                m["rs55"] = None

            # rs252: O'Neil 4-quarter formula using stock close history
            m["rs252"] = None
            sc = m.get("_closes")  # set below per-symbol

        # For rs252 we need per-symbol close history — re-slice from sym_data
        sym_closes: dict[str, list[float]] = {}
        for sym, rows_ in sym_data.items():
            slice_ = [r[4] for r in rows_ if r[0] <= target_date]
            if len(slice_) >= 4 * _RS252_BARS + 1:  # need 253+ bars: index [end-252] must be >= 0
                sym_closes[sym] = slice_

        def _pct(closes, start, end):
            if start < 0 or end >= len(closes): return None
            p0, p1 = closes[start], closes[end]
            if not p0 or not p1: return None
            return (p1 - p0) / p0 * 100.0

        for sym in syms:
            m = day_metrics[sym]
            closes = sym_closes.get(sym)
            if closes is None:
                m["rs252"] = None
                continue
            n = len(closes)
            end = n - 1
            q1 = _pct(closes, end - _RS252_BARS,         end)
            q2 = _pct(closes, end - 2 * _RS252_BARS,     end - _RS252_BARS)
            q3 = _pct(closes, end - 3 * _RS252_BARS,     end - 2 * _RS252_BARS)
            q4 = _pct(closes, end - 4 * _RS252_BARS,     end - 3 * _RS252_BARS)
            if any(v is None for v in (q1, q2, q3, q4)):
                m["rs252"] = None
            else:
                m["rs252"] = round(q1 * 0.40 + q2 * 0.20 + q3 * 0.20 + q4 * 0.20, 4)

        # Rank only symbols that have a valid value; others get None rank
        def _rank_with_nones(syms, key):
            valid = [(i, day_metrics[s][key]) for i, s in enumerate(syms) if day_metrics[s][key] is not None]
            ranks = [None] * len(syms)
            if valid:
                idxs, vals = zip(*valid)
                for idx, rank in zip(idxs, _rank_1_99(list(vals))):
                    ranks[idx] = rank
            return ranks

        rs21_ranks  = _rank_with_nones(syms, "rs21")
        rs55_ranks  = _rank_with_nones(syms, "rs55")
        rs252_ranks = _rank_with_nones(syms, "rs252")

        # Build upsert rows
        upsert_rows = []
        for i, sym in enumerate(syms):
            m = day_metrics[sym]
            # RS line = stock_close / bench_close on this date
            rs_line = round(m["close"] / bench_close, 6) if (m["close"] and bench_close and bench_close > 0) else None
            upsert_rows.append({
                "symbol":     sym,
                "date":       target_date,
                "rs21":       m["rs21"],
                "rs21_rank":  rs21_ranks[i],
                "rs55":       m["rs55"],
                "rs55_rank":  rs55_ranks[i],
                "rs252":      m.get("rs252"),
                "rs252_rank": rs252_ranks[i],
                "day_return": m.get("day_return"),
                "rs_line":    rs_line,
            })

        try:
            db.upsert_stock_rs_daily(upsert_rows)
            computed_total += len(upsert_rows)
            logger.info("stock_rs: %s done — %d symbols upserted", target_date, len(upsert_rows))
        except Exception as e:
            logger.error("stock_rs: upsert failed for %s: %s", target_date, e)
            failed_dates.append(target_date)

    return {"computed": computed_total, "skipped": skipped_total, "failed": failed_dates}


def get_all_historical_dates() -> list[str]:
    """Return ALL distinct trading dates from ohlcv_daily for full historical RS computation."""
    import db
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            if _DB_MODE == "local":
                clause, params = _index_symbols_clause("?")
                cur.execute(
                    f"""SELECT DISTINCT date FROM ohlcv_daily
                       WHERE {clause}
                       ORDER BY date ASC""",
                    params,
                )
            else:
                clause, params = _index_symbols_clause("%s")
                cur.execute(
                    f"""SELECT DISTINCT date::text FROM ohlcv_daily
                       WHERE {clause}
                       ORDER BY date ASC""",
                    params,
                )
            return [r[0] for r in cur.fetchall()]
    finally:
        db.release_conn(conn)


def get_all_recent_dates(backfill_days: int = 30) -> list[str]:
    """Return last `backfill_days` trading dates from ohlcv_daily, ignoring what's already computed."""
    import db
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            if _DB_MODE == "local":
                clause, params = _index_symbols_clause("?")
                cur.execute(
                    f"""SELECT DISTINCT date FROM ohlcv_daily
                       WHERE {clause}
                         AND date >= date('now', ? || ' days')
                       ORDER BY date ASC""",
                    params + [f"-{backfill_days + 30}"],
                )
            else:
                clause, params = _index_symbols_clause("%s")
                cur.execute(
                    f"""SELECT DISTINCT date::text FROM ohlcv_daily
                       WHERE {clause}
                         AND date >= NOW() - INTERVAL '%s days'
                       ORDER BY date ASC""",
                    params + [backfill_days + 30],
                )
            return [r[0] for r in cur.fetchall()][-backfill_days:]
    finally:
        db.release_conn(conn)


def get_dates_to_compute(backfill_days: int = 30) -> list[str]:
    """
    Return trading dates that need (re)computing:
    - Latest date in ohlcv_daily for stocks (today's data if refresh ran)
    - Any of the last backfill_days trading dates NOT yet in stock_rs_daily
    """
    import db
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            # All distinct trading dates in ohlcv_daily for stocks in last backfill_days calendar days
            if _DB_MODE == "local":
                clause, params = _index_symbols_clause("?")
                cur.execute(
                    f"""SELECT DISTINCT date FROM ohlcv_daily
                       WHERE {clause}
                         AND date >= date('now', ? || ' days')
                       ORDER BY date ASC""",
                    params + [f"-{backfill_days + 30}"],
                )
            else:
                clause, params = _index_symbols_clause("%s")
                cur.execute(
                    f"""SELECT DISTINCT date::text FROM ohlcv_daily
                       WHERE {clause}
                         AND date >= NOW() - INTERVAL '%s days'
                       ORDER BY date ASC""",
                    params + [backfill_days + 30],
                )
            all_trading_dates = [r[0] for r in cur.fetchall()]
            # Keep only last backfill_days trading dates
            all_trading_dates = all_trading_dates[-backfill_days:]

            # Dates already fully computed in stock_rs_daily
            if all_trading_dates:
                earliest = all_trading_dates[0]
                cur.execute(
                    """SELECT DISTINCT date FROM stock_rs_daily
                       WHERE date >= ?
                       ORDER BY date ASC""" if _DB_MODE == "local" else
                    """SELECT DISTINCT date::text FROM stock_rs_daily
                       WHERE date >= %s
                       ORDER BY date ASC""",
                    (earliest,),
                )
                already_done = {r[0] for r in cur.fetchall()}
            else:
                already_done = set()
    finally:
        db.release_conn(conn)

    # Always recompute the latest date (price data may have been updated today)
    missing = [d for d in all_trading_dates if d not in already_done]
    if all_trading_dates and all_trading_dates[-1] not in missing:
        missing.append(all_trading_dates[-1])   # force recompute latest

    return sorted(set(missing))
