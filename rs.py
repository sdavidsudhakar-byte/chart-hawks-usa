"""
rs.py — Authentic IBD Relative Strength (William O'Neil) for US sector/broad-market indices.

Formula (original O'Neil):
  - Split the trailing 12 months into 4 quarters:
      Q1 = most recent quarter  (last ~63 trading days)   weight 40%
      Q2 = 3-6 months ago       (~63 trading days)        weight 20%
      Q3 = 6-9 months ago       (~63 trading days)        weight 20%
      Q4 = 9-12 months ago      (~63 trading days)        weight 20%
  - Each Qn = % price change over that period
  - RS raw score = Q1*0.40 + Q2*0.20 + Q3*0.20 + Q4*0.20
  - RS rating = percentile rank of raw score among all symbols (1-99)

Computation:
  - Runs for every date where we have a full year of data for enough symbols
  - Typically called once per day after market close (or after index price refresh)
  - Stores results in index_rs table

Public API:
  compute_and_store_rs(date=None) → dict with summary
"""

import datetime
import logging
from typing import Optional

import db
import index_data
import market_data

try:
    from config import DB_MODE as _DB_MODE
except ImportError:
    _DB_MODE = "neon"

logger = logging.getLogger(__name__)

# Trading days per quarter (approximate — O'Neil uses calendar quarters, we use bars)
Q_BARS = 63   # ~3 months of trading days

# Minimum number of symbols that must have data to compute a valid ranking.
# NSE's ~80-index universe used 30; the US index universe is only 15 (4
# broad-market + 11 sector SPDR ETFs), so the floor must fit that population.
MIN_SYMBOLS_REQUIRED = 10

# Sector Lens tab groupings — mirrors the index.html category tabs. With only
# 15 total indices (4 broad-market ETFs + 11 sector SPDRs, no overlap), this
# is a straight category→group mapping — no per-symbol exclude/extra lists
# needed (those existed only to handle NSE's cross-listed derivative indices).
_SWING_GROUPS = {
    "BROAD":    {"cats": {"BROAD MARKET"}, "excl": set(), "extra": set()},
    "SECTORAL": {"cats": {"SECTORAL"},     "excl": set(), "extra": set()},
}


def _pct_change(prices: list[float], start_idx: int, end_idx: int) -> Optional[float]:
    """
    Compute % change from prices[start_idx] to prices[end_idx].
    Returns None if either index is out of range or price is 0/None.
    """
    if start_idx < 0 or end_idx >= len(prices):
        return None
    p_start = prices[start_idx]
    p_end   = prices[end_idx]
    if not p_start or not p_end:
        return None
    return ((p_end - p_start) / p_start) * 100.0


def _percentile_rank(value: float, all_values: list[float]) -> int:
    """
    Return IBD-style percentile rank: what % of the universe scores BELOW this value.
    Result clamped to 1-99 (O'Neil never uses 0 or 100).
    """
    if not all_values:
        return 50
    n_below = sum(1 for v in all_values if v < value)
    rank    = (n_below / len(all_values)) * 100
    return max(1, min(99, round(rank)))


def compute_rs_for_date(
    symbol_closes: dict[str, list[float]],
    date_str: str,
) -> list[dict]:
    """
    Compute IBD RS ratings for all symbols on a single date.

    symbol_closes: {symbol: [close_price, ...]} — prices in chronological order,
                   ending on or before the target date.
                   Must have at least 4*Q_BARS bars for a symbol to be ranked.

    Returns list of {symbol, date, rs_raw, rs_rating}.
    """
    raw_scores: dict[str, float] = {}

    for symbol, closes in symbol_closes.items():
        n = len(closes)
        if n < Q_BARS * 4 + 1:
            continue   # not enough history — need 4*Q+1 bars for all 4 quarter pivots

        # Most recent price is the last element
        # Q1: end = n-1, start = n-1-Q_BARS
        # Q2: end = n-1-Q_BARS, start = n-1-2*Q_BARS
        # Q3: end = n-1-2*Q_BARS, start = n-1-3*Q_BARS
        # Q4: end = n-1-3*Q_BARS, start = n-1-4*Q_BARS
        end   = n - 1
        q1 = _pct_change(closes, end - Q_BARS,     end)
        q2 = _pct_change(closes, end - 2 * Q_BARS, end - Q_BARS)
        q3 = _pct_change(closes, end - 3 * Q_BARS, end - 2 * Q_BARS)
        q4 = _pct_change(closes, end - 4 * Q_BARS, end - 3 * Q_BARS)

        if any(q is None for q in (q1, q2, q3, q4)):
            continue

        raw = q1 * 0.40 + q2 * 0.20 + q3 * 0.20 + q4 * 0.20
        raw_scores[symbol] = raw

    if len(raw_scores) < MIN_SYMBOLS_REQUIRED:
        logger.warning(
            "RS compute on %s: only %d symbols have enough history (need %d), skipping",
            date_str, len(raw_scores), MIN_SYMBOLS_REQUIRED,
        )
        return []

    all_raw = list(raw_scores.values())
    results = []
    for symbol, raw in raw_scores.items():
        rating = _percentile_rank(raw, all_raw)
        results.append({
            "symbol":    symbol,
            "date":      date_str,
            "rs_raw":    round(raw, 4),
            "rs_rating": rating,
        })

    return results


def compute_and_store_rs(date: Optional[str] = None) -> dict:
    """
    Compute IBD RS ratings for the given date (defaults to today) and store in DB.
    Fetches price history from index_prices table.

    Returns summary dict.
    """
    if date is None:
        date = datetime.date.today().isoformat()

    logger.info("Computing IBD RS for date %s ...", date)

    # Need 4 quarters + buffer — fetch 300 trading days worth of calendar days
    from_date = (
        datetime.date.fromisoformat(date) - datetime.timedelta(days=450)
    ).isoformat()

    symbols = index_data.SYMBOLS

    # Fetch all closes in one DB query
    raw_data = db.get_index_closes(symbols, from_date, date)

    if not raw_data:
        return {"ok": False, "error": "No index price data found. Run index refresh first."}

    # Build {symbol: [close, ...]} — only the close price, in date order
    symbol_closes: dict[str, list[float]] = {}
    for sym, rows in raw_data.items():
        closes = [r[1] for r in rows if r[1] is not None]
        if closes:
            symbol_closes[sym] = closes

    results = compute_rs_for_date(symbol_closes, date)

    if not results:
        return {
            "ok":    False,
            "error": f"Not enough data to compute RS on {date}. Need at least {MIN_SYMBOLS_REQUIRED} symbols with 252+ days of history.",
        }

    db.upsert_index_rs(results)
    logger.info("RS computed: %d ratings stored for %s", len(results), date)

    return {
        "ok":      True,
        "date":    date,
        "rated":   len(results),
        "skipped": len(symbols) - len(results),
    }


def _compute_peer_rank(values: dict) -> dict:
    """Given {symbol: float_value}, return {symbol: 1-99 percentile rank}."""
    items = [(sym, v) for sym, v in values.items() if v is not None]
    if not items:
        return {}
    all_vals = [v for _, v in items]
    out = {}
    for sym, v in items:
        n_below = sum(1 for x in all_vals if x < v)
        out[sym] = max(1, min(99, round((n_below / len(all_vals)) * 100)))
    return out


def _build_group_membership() -> dict[str, str]:
    """Return {symbol: group_name} for all indices using _SWING_GROUPS."""
    membership = {}
    for grp, cfg in _SWING_GROUPS.items():
        for idx in index_data.ALL_INDICES:
            sym = idx.symbol
            if idx.category in cfg["cats"] and sym not in cfg["excl"]:
                membership[sym] = grp
        for sym in cfg["extra"]:
            membership[sym] = grp
    return membership


def _compute_group_scores(rs21_vals: dict, rs55_vals: dict, rs_raw_vals: dict,
                          membership: dict) -> tuple[dict, dict, dict]:
    """
    Compute within-group peer rank scores for rs21 (short), rs55 (mid), rs_raw (lt).
    Returns (group_short, group_mid, group_lt) each {sym: 1-99}.
    """
    from collections import defaultdict
    grp_rs21: dict = defaultdict(dict)
    grp_rs55: dict = defaultdict(dict)
    grp_lt:   dict = defaultdict(dict)
    for sym, grp in membership.items():
        if sym in rs21_vals and rs21_vals[sym] is not None:
            grp_rs21[grp][sym] = rs21_vals[sym]
        if sym in rs55_vals and rs55_vals[sym] is not None:
            grp_rs55[grp][sym] = rs55_vals[sym]
        if sym in rs_raw_vals and rs_raw_vals[sym] is not None:
            grp_lt[grp][sym] = rs_raw_vals[sym]

    group_short: dict = {}
    group_mid:   dict = {}
    group_lt:    dict = {}
    for grp in _SWING_GROUPS:
        group_short.update(_compute_peer_rank(grp_rs21[grp]))
        group_mid.update(_compute_peer_rank(grp_rs55[grp]))
        group_lt.update(_compute_peer_rank(grp_lt[grp]))
    return group_short, group_mid, group_lt


def _compute_rs21_rs55_for_date(symbol_closes: dict, benchmark: str) -> dict:
    """
    For a single date, compute RS21 and RS55 for all symbols vs benchmark.
    Returns {symbol: {rs21: float, rs55: float}} — only symbols with enough bars.
    RS21 = (close_now/bench_now) / (close_21ago/bench_21ago) - 1
    RS55 = (close_now/bench_now) / (close_55ago/bench_55ago) - 1
    Requires at least 56 bars per symbol.
    """
    bench_closes = symbol_closes.get(benchmark)
    if not bench_closes or len(bench_closes) < 56:
        return {}
    b_now = bench_closes[-1]
    b_21  = bench_closes[-22]
    b_55  = bench_closes[-56]
    if not b_now or not b_21 or not b_55:
        return {}

    out = {}
    for sym, closes in symbol_closes.items():
        if sym == benchmark or len(closes) < 56:
            continue
        s_now = closes[-1]
        s_21  = closes[-22]
        s_55  = closes[-56]
        if not s_now or not s_21 or not s_55:
            continue
        rs21 = (s_now / b_now) / (s_21 / b_21) - 1
        rs55 = (s_now / b_55) / (s_55 / b_55) - 1 if b_55 else None
        out[sym] = {"rs21": rs21, "rs55": rs55}
    return out


def backfill_index_mid_short(days: int = 2000) -> dict:
    """
    Compute and upsert mid_score (RS55 peer rank) and short_score (RS21 peer rank)
    for all historical OHLCV dates where >= 56 bars exist.
    Loops over ALL ohlcv_daily dates (not just existing index_rs rows) so early dates
    (before LT can be computed) also get mid/short scores.
    Benchmark: index_data.market_data.BENCHMARK_SYMBOL (^GSPC)

    Stocks and indices share the ohlcv_daily table with no distinguishing
    symbol prefix (unlike NSE's "NSE:...-INDEX" suffix), so index rows are
    isolated with an explicit IN-list of the known index symbols instead of
    a LIKE pattern.
    """
    BENCHMARK = market_data.BENCHMARK_SYMBOL
    MIN_BARS  = 56

    from collections import defaultdict

    # BENCHMARK (^GSPC) isn't one of the 15 sector/broad-market ETFs in
    # index_data.SYMBOLS but is needed below for the RS21/RS55 ratio — include
    # it in the query explicitly so its close series is loaded into sym_series.
    index_symbols = list(index_data.SYMBOLS) + [BENCHMARK]
    ph = ",".join(["?"] * len(index_symbols)) if _DB_MODE == "local" else ",".join(["%s"] * len(index_symbols))

    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            # All index OHLCV
            if _DB_MODE == "local":
                cur.execute(
                    f"SELECT symbol, date, close FROM ohlcv_daily WHERE symbol IN ({ph}) ORDER BY symbol, date",
                    index_symbols,
                )
            else:
                cur.execute(
                    f"SELECT symbol, date::text, close FROM ohlcv_daily WHERE symbol IN ({ph}) ORDER BY symbol, date",
                    index_symbols,
                )
            ohlcv_rows = cur.fetchall()

            # All existing index_rs rows — load into memory for fast lookup
            if _DB_MODE == "local":
                cur.execute("SELECT symbol, date, rs_raw, rs_rating FROM index_rs")
            else:
                cur.execute("SELECT symbol, date::text, rs_raw, rs_rating FROM index_rs")
            existing_rs = {(r[0], str(r[1])[:10]): (r[2], r[3]) for r in cur.fetchall()}

            # All distinct OHLCV trading dates
            if _DB_MODE == "local":
                cur.execute(f"SELECT DISTINCT date FROM ohlcv_daily WHERE symbol IN ({ph}) ORDER BY date", index_symbols)
            else:
                cur.execute(f"SELECT DISTINCT date::text FROM ohlcv_daily WHERE symbol IN ({ph}) ORDER BY date", index_symbols)
            all_ohlcv_dates = [r[0] for r in cur.fetchall()]
    finally:
        db.release_conn(conn)

    # Build per-symbol chronological close series
    sym_series: dict = defaultdict(list)
    for sym, date, close in ohlcv_rows:
        sym_series[sym].append((str(date)[:10], float(close) if close else None))

    # Target dates: last `days` ohlcv dates where benchmark has >= MIN_BARS
    bench_series = sym_series.get(BENCHMARK, [])
    eligible_dates = [d for i, (d, _) in enumerate(bench_series) if i >= MIN_BARS - 1]
    target_dates = eligible_dates[-days:]

    logger.info("backfill_index_mid_short: %d total OHLCV dates | %d eligible | computing %d",
                len(all_ohlcv_dates), len(eligible_dates), len(target_dates))

    import datetime as _dt
    membership = _build_group_membership()

    total_upserted = 0
    for date in target_dates:
        snap: dict = {}
        for sym, series in sym_series.items():
            closes = [c for d, c in series if d <= date and c is not None]
            if len(closes) >= MIN_BARS:
                snap[sym] = closes

        rs_data = _compute_rs21_rs55_for_date(snap, BENCHMARK)
        if not rs_data:
            continue

        rs21_vals  = {sym: v["rs21"] for sym, v in rs_data.items()}
        rs55_vals  = {sym: v["rs55"] for sym, v in rs_data.items() if v["rs55"] is not None}
        rs_raw_vals = {sym: existing_rs[(sym, date)][0] for sym in rs_data
                       if (sym, date) in existing_rs and existing_rs[(sym, date)][0] is not None}
        short_ranks = _compute_peer_rank(rs21_vals)
        mid_ranks   = _compute_peer_rank(rs55_vals)
        group_short, group_mid, group_lt = _compute_group_scores(rs21_vals, rs55_vals, rs_raw_vals, membership)

        # day_return and week_return per symbol for this date
        date_dt = _dt.date.fromisoformat(date)
        mon_str = (date_dt - _dt.timedelta(days=date_dt.weekday())).isoformat()

        upsert_rows = []
        for sym in rs_data:
            existing  = existing_rs.get((sym, date))
            series    = sym_series.get(sym, [])
            # closes up to and including this date (chronological)
            sym_closes_dated = [(d, c) for d, c in series if d <= date and c is not None]
            close_now  = sym_closes_dated[-1][1]  if sym_closes_dated else None
            close_prev = sym_closes_dated[-2][1]  if len(sym_closes_dated) >= 2 else None
            day_ret = round((close_now / close_prev - 1) * 100, 4) if close_now and close_prev else None
            # week_return: vs last close strictly before this Monday
            week_ret = None
            if close_now:
                for d, c in reversed(sym_closes_dated[:-1]):
                    if d < mon_str:
                        if c:
                            week_ret = round((close_now / c - 1) * 100, 4)
                        break

            upsert_rows.append({
                "symbol":            sym,
                "date":              date,
                "rs_raw":            existing[0] if existing else None,
                "rs_rating":         existing[1] if existing else None,
                "mid_score":         mid_ranks.get(sym),
                "short_score":       short_ranks.get(sym),
                "rs21":              round(rs_data[sym]["rs21"] * 100, 4),
                "rs55":              round(rs_data[sym]["rs55"] * 100, 4) if rs_data[sym]["rs55"] is not None else None,
                "day_return":        day_ret,
                "week_return":       week_ret,
                "close":             round(close_now, 4) if close_now else None,
                "group_short_score": group_short.get(sym),
                "group_mid_score":   group_mid.get(sym),
                "group_lt_score":    group_lt.get(sym),
            })

        if upsert_rows:
            db.upsert_index_rs(upsert_rows)
            total_upserted += len(upsert_rows)

    logger.info("backfill_index_mid_short: done — %d rows upserted across %d dates",
                total_upserted, len(target_dates))
    return {"ok": True, "dates": len(target_dates), "upserted": total_upserted}


def backfill_rs(days: int = 30) -> dict:
    """
    Compute and store RS ratings for all eligible past dates in index_prices.
    Skips dates that already have RS data. Limits to last `days` eligible dates.
    Returns summary dict.
    """
    # Get all distinct dates in index_prices
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            if _DB_MODE == "local":
                cur.execute("SELECT DISTINCT date FROM ohlcv_daily ORDER BY date")
            else:
                cur.execute("SELECT DISTINCT date::text FROM ohlcv_daily ORDER BY date")
            all_price_dates = [r[0] for r in cur.fetchall()]
            if _DB_MODE == "local":
                cur.execute("SELECT DISTINCT date FROM index_rs")
            else:
                cur.execute("SELECT DISTINCT date::text FROM index_rs")
            already_done = {r[0] for r in cur.fetchall()}
    finally:
        db.release_conn(conn)

    # Only dates that have enough bars (index >= MIN_BARS-1 in the sorted list)
    min_bars = Q_BARS * 4 + 1
    eligible = all_price_dates[min_bars - 1:]

    # Limit to last `days` — always recompute (upsert overwrites stale values)
    to_compute = eligible[-days:]

    if not to_compute:
        logger.info("backfill_rs: nothing to compute — all dates already done")
        return {"ok": True, "computed": 0, "skipped": len(eligible)}

    logger.info("backfill_rs: computing RS for %d dates", len(to_compute))
    computed = 0
    failed   = []

    for date in to_compute:
        result = compute_and_store_rs(date)
        if result.get("ok"):
            computed += 1
        else:
            failed.append(date)
            logger.warning("backfill_rs: failed for %s — %s", date, result.get("error"))

    logger.info("backfill_rs: done — %d computed, %d failed", computed, len(failed))
    return {"ok": True, "computed": computed, "failed": failed, "total_eligible": len(eligible)}
