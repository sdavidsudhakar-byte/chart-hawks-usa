"""
scanner.py — Daily Hunt Scanner

Scans all symbols for a 2-step bull/bear EMA cross sequence on daily bars:

Bull sequence:
  Step 1: 20 EMA crosses BELOW 50 EMA  (bear phase begins)
  Step 2: 20 EMA crosses ABOVE 50 EMA  (bull cross — the Daily Cross signal)

Bear sequence (mirror):
  Step 1: 20 EMA crosses ABOVE 50 EMA  (bull phase begins)
  Step 2: 20 EMA crosses BELOW 50 EMA  (bear cross)

A stock appears in results only when its Daily Cross date falls within the
last SCAN_DAYS trading days.

All data is read from ohlcv_daily (daily OHLC) — no Fyers calls, no intraday data.
"""

import datetime
import logging
import queue
import threading
from collections import defaultdict
from typing import Optional

import db

logger = logging.getLogger(__name__)

# ── Constants ──────────────────────────────────────────────────────────────────

DAILY_WARMUP   = 300    # bars loaded per symbol for EMA warm-up (6× 50-period)
BEAR_LOOKBACK  = 252    # how far back to search for Step 1 (~1 trading year)
SCAN_DAYS      = 60     # Daily Cross date must fall within this many trading days


# ── EMA / ATR helpers ─────────────────────────────────────────────────────────

def _compute_ema(closes: list[float], period: int) -> list[Optional[float]]:
    n = len(closes)
    result: list[Optional[float]] = [None] * n
    if n < period:
        return result
    k = 2.0 / (period + 1)
    seed = sum(closes[:period]) / period
    result[period - 1] = seed
    for i in range(period, n):
        result[i] = closes[i] * k + result[i - 1] * (1 - k)
    return result


# ── Bulk data loader ───────────────────────────────────────────────────────────

def _bulk_load_daily_ohlc(symbols: list[str], num_bars: int) -> dict[str, list[tuple]]:
    """
    Load the latest `num_bars` daily OHLC rows for ALL symbols in ONE query.
    Returns {symbol: [(date_str, open, high, low, close), ...]} oldest→newest.
    """
    if not symbols:
        return {}
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            ph = ",".join(["%s"] * len(symbols))
            cur.execute(
                f"""
                SELECT symbol, date, open, high, low, close
                FROM (
                    SELECT symbol, date, open, high, low, close,
                           ROW_NUMBER() OVER (
                               PARTITION BY symbol ORDER BY date DESC
                           ) AS rn
                    FROM ohlcv_daily
                    WHERE symbol IN ({ph}) AND close > 0
                ) ranked
                WHERE rn <= %s
                ORDER BY symbol, date ASC
                """,
                symbols + [num_bars],
            )
            rows = cur.fetchall()
    finally:
        db.release_conn(conn)

    result: dict[str, list[tuple]] = defaultdict(list)
    for symbol, date, o, h, l, c in rows:
        result[symbol].append((str(date), float(o), float(h), float(l), float(c)))
    return result


def _get_latest_n_trading_dates(n: int) -> list[str]:
    """Return the n most recent trading dates from the S&P 500 benchmark series."""
    import market_data
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT date FROM ohlcv_daily
                WHERE symbol = %s
                ORDER BY date DESC LIMIT %s
                """,
                (market_data.BENCHMARK_SYMBOL, n),
            )
            return [str(r[0]) for r in cur.fetchall()]
    finally:
        db.release_conn(conn)


def _get_all_symbols() -> list[str]:
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol FROM tickers
                WHERE is_active=1 AND macro != 'Uncategorized' AND macro != ''
                ORDER BY symbol
                """
            )
            return [r[0] for r in cur.fetchall()]
    finally:
        db.release_conn(conn)


# ── Per-symbol scan logic ─────────────────────────────────────────────────────

def _scan_symbol(dates: list[str], closes: list[float],
                 oldest_scan_date: str) -> list[dict]:
    """
    Run the full bull+bear cross scan for one symbol.
    Returns a list of 0, 1, or 2 result dicts (one per direction that qualifies).
    """
    n = len(closes)
    if n < 60:
        return []

    ema20 = _compute_ema(closes, 20)
    ema50 = _compute_ema(closes, 50)

    results = []
    for direction in ("bull", "bear"):
        row = _scan_direction(direction, dates, ema20, ema50, closes, oldest_scan_date)
        if row:
            results.append(row)
    return results


def _scan_direction(direction: str, dates, ema20, ema50, closes,
                    oldest_scan_date: str) -> Optional[dict]:
    """
    Bull: look for 20 crosses below 50 (Step 1), then 20 crosses above 50
          after that (Step 2 — the Daily Cross signal).
    Bear: mirror (cross above → cross below).
    """
    n = len(dates)

    # ── Step 1: find the most recent bear/bull phase start ────────────────────
    # Bull: 20 EMA crosses BELOW 50 EMA
    # Bear: 20 EMA crosses ABOVE 50 EMA
    step1_idx = None
    for i in range(n - 1, 0, -1):
        if ema20[i] is None or ema50[i] is None:
            continue
        if ema20[i - 1] is None or ema50[i - 1] is None:
            continue
        if direction == "bull":
            # looking for 20 dipping below 50
            if ema20[i - 1] >= ema50[i - 1] and ema20[i] < ema50[i]:
                step1_idx = i
                break
        else:
            # looking for 20 rising above 50
            if ema20[i - 1] <= ema50[i - 1] and ema20[i] > ema50[i]:
                step1_idx = i
                break

    if step1_idx is None:
        return None

    step1_date = dates[step1_idx]

    # ── Step 3: find bull/bear cross AFTER step1 ─────────────────────────────
    # Bull: 20 crosses ABOVE 50 after step1
    # Bear: 20 crosses BELOW 50 after step1
    cross_idx = None
    for i in range(step1_idx + 1, n):
        if ema20[i] is None or ema50[i] is None:
            continue
        if ema20[i - 1] is None or ema50[i - 1] is None:
            continue
        if direction == "bull":
            if ema20[i - 1] <= ema50[i - 1] and ema20[i] > ema50[i]:
                cross_idx = i
                break
        else:
            if ema20[i - 1] >= ema50[i - 1] and ema20[i] < ema50[i]:
                cross_idx = i
                break

    cross_date = dates[cross_idx] if cross_idx is not None else None

    # Skip if no cross happened yet
    if cross_date is None:
        return None

    # ── Check if the cross falls within the scan window ────────────────────
    if cross_date < oldest_scan_date:
        return None

    # Compute above_200sma flag at the cross date
    check_idx = cross_idx
    sma200_val = None
    if check_idx >= 199:
        sma200_val = sum(closes[check_idx - 199: check_idx + 1]) / 200
    e50_at_cross = ema50[check_idx]
    above_200sma = bool(
        sma200_val is not None and e50_at_cross is not None and e50_at_cross > sma200_val
    )

    return {
        "direction":       direction,
        "step1_date":      step1_date,
        "bull_cross_date": cross_date,
        "above_200sma":    above_200sma,
    }


# ── Enrichment ────────────────────────────────────────────────────────────────

def _enrich_with_ticker_meta(results: list[dict]) -> list[dict]:
    """Attach index_list, hawks_mappings, and latest RS scores to each result row."""
    if not results:
        return results
    import json as _json
    syms = [r["symbol"] for r in results]
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            ph = ",".join(["%s"] * len(syms))
            cur.execute(
                f"SELECT symbol, index_list, company_name, macro, sector, industry, basic_industry FROM tickers WHERE symbol IN ({ph})",
                syms,
            )
            meta = {}
            for sym, il, cname, macro, sector, industry, bi in cur.fetchall():
                if isinstance(il, str):
                    try: il = _json.loads(il)
                    except Exception: il = []
                elif il is None:
                    il = []
                meta[sym] = {"index_list": il,
                             "company_name": cname or "", "macro": macro or "",
                             "sector": sector or "", "industry": industry or "",
                             "basic_industry": bi or ""}
            cur.execute(
                f"""SELECT symbol, index_name, source, confidence
                    FROM sector_index_map WHERE symbol IN ({ph})
                    ORDER BY symbol,
                      CASE source WHEN 'official' THEN 0 ELSE 1 END,
                      CASE confidence WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END,
                      index_name""",
                syms,
            )
            hawks_map: dict = {}
            for sym, idx_name, src, conf in cur.fetchall():
                hawks_map.setdefault(sym, []).append({"name": idx_name, "source": src, "confidence": conf})
            try:
                cur.execute(
                    f"""SELECT s.symbol, s.rs21, s.rs21_rank, s.rs55, s.rs55_rank, s.rs252, s.rs252_rank
                        FROM stock_rs_daily s
                        INNER JOIN (SELECT MAX(date) AS max_date FROM stock_rs_daily) m ON s.date = m.max_date
                        WHERE s.symbol IN ({ph})""",
                    syms,
                )
                rs_meta = {}
                for sym, rs21, rs21_rank, rs55, rs55_rank, rs252, rs252_rank in cur.fetchall():
                    rs_meta[sym] = {
                        "rs21": float(rs21) if rs21 is not None else None,
                        "rs21_rank": rs21_rank,
                        "rs55": float(rs55) if rs55 is not None else None,
                        "rs55_rank": rs55_rank,
                        "rs252": float(rs252) if rs252 is not None else None,
                        "rs252_rank": rs252_rank,
                    }
            except Exception:
                rs_meta = {}
            # 52-week high/low from weekly candles (last 52 weeks).
            # last_close from ohlcv_daily latest date.
            try:
                cur.execute(
                    "SELECT week FROM (SELECT DISTINCT week FROM ohlcv_weekly "
                    "ORDER BY week DESC LIMIT 52) t ORDER BY week ASC LIMIT 1"
                )
                cutoff_row = cur.fetchone()
                cutoff_52w = cutoff_row[0] if cutoff_row else "2000-01-01"
                cur.execute(
                    f"""SELECT symbol, MAX(high) AS w52_high, MIN(low) AS w52_low
                        FROM ohlcv_weekly
                        WHERE symbol IN ({ph}) AND week >= %s
                        GROUP BY symbol""",
                    syms + [cutoff_52w],
                )
                w52_meta: dict = {}
                for sym, h, l in cur.fetchall():
                    w52_meta[sym] = {"w52_high": float(h) if h is not None else None,
                                     "w52_low":  float(l) if l is not None else None,
                                     "last_close": None}
                cur.execute(
                    f"""SELECT symbol, close
                        FROM ohlcv_daily
                        WHERE symbol IN ({ph})
                          AND date = (SELECT MAX(date) FROM ohlcv_daily)""",
                    syms,
                )
                for sym, c in cur.fetchall():
                    if sym in w52_meta:
                        w52_meta[sym]["last_close"] = float(c) if c is not None else None
                    else:
                        w52_meta[sym] = {"w52_high": None, "w52_low": None,
                                         "last_close": float(c) if c is not None else None}
            except Exception:
                w52_meta = {}
    finally:
        db.release_conn(conn)

    for r in results:
        m = meta.get(r["symbol"], {})
        r["index_list"]     = m.get("index_list", [])
        r["hawks_mappings"] = hawks_map.get(r["symbol"], [])
        # Fill company_name/macro/sector/industry/basic_industry from tickers
        # only when not already set (scanner_results rows carry stored values)
        if not r.get("company_name"): r["company_name"]  = m.get("company_name", "")
        if not r.get("macro"):        r["macro"]          = m.get("macro", "")
        if not r.get("sector"):       r["sector"]         = m.get("sector", "")
        if not r.get("industry"):     r["industry"]       = m.get("industry", "")
        if not r.get("basic_industry"): r["basic_industry"] = m.get("basic_industry", "")
        rs = rs_meta.get(r["symbol"], {})
        r["rs21"]           = rs.get("rs21")
        r["rs21_rank"]      = rs.get("rs21_rank")
        r["rs55"]           = rs.get("rs55")
        r["rs55_rank"]      = rs.get("rs55_rank")
        r["rs252"]          = rs.get("rs252")
        r["rs252_rank"]     = rs.get("rs252_rank")
        w52 = w52_meta.get(r["symbol"], {})
        r["w52_high"]       = w52.get("w52_high")
        r["w52_low"]        = w52.get("w52_low")
        r["last_close"]     = w52.get("last_close")
    return results


# ── Main scan entry point ─────────────────────────────────────────────────────

def run_full_scan(run_id: int | None = None) -> dict:
    """
    Scan ALL symbols for a Daily Cross (EMA20/50) within the last SCAN_DAYS
    trading days. Fully DB-backed — no Fyers calls.
    Results written to scanner_results table.
    """
    logger.info("Daily Hunt scanner starting...")
    if run_id is None:
        run_id = db.scanner_run_start()

    recent_dates = _get_latest_n_trading_dates(SCAN_DAYS)
    if not recent_dates:
        db.scanner_run_finish(run_id, 0, 0, 0, 0, "error")
        return {"ok": False, "error": "No price data in DB."}

    oldest_scan_date = min(recent_dates)
    as_of            = recent_dates[0]

    symbols  = _get_all_symbols()
    sym_list = symbols
    logger.info("Daily Hunt: %d symbols, window back to %s", len(symbols), oldest_scan_date)

    # Bulk load OHLC (only close is needed for EMA cross detection)
    total_bars = DAILY_WARMUP + BEAR_LOOKBACK
    all_ohlc = _bulk_load_daily_ohlc(sym_list, total_bars)
    logger.info("Daily Hunt: loaded OHLC for %d symbols — computing crosses...", len(all_ohlc))

    # Load ticker metadata in one query
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            ph = ",".join(["%s"] * len(sym_list))
            cur.execute(
                f"SELECT symbol, company_name, macro, sector, industry, basic_industry "
                f"FROM tickers WHERE symbol IN ({ph})",
                sym_list,
            )
            cols = [d[0] for d in cur.description]
            ticker_info = {r[cols.index("symbol")]: dict(zip(cols, r)) for r in cur.fetchall()}
    finally:
        db.release_conn(conn)

    hits_bull = 0
    hits_bear = 0
    errors    = 0
    result_rows: list[dict] = []

    for symbol in symbols:
        ohlc = all_ohlc.get(symbol, [])
        if len(ohlc) < 60:
            continue
        dates  = [r[0] for r in ohlc]
        closes = [r[4] for r in ohlc]
        try:
            rows = _scan_symbol(dates, closes, oldest_scan_date)
        except Exception as e:
            logger.warning("Scan error %s: %s", symbol, e)
            errors += 1
            continue

        info = ticker_info.get(symbol, {})
        for row in rows:
            row.update({
                "symbol":         symbol,
                "company_name":   info.get("company_name", ""),
                "macro":          info.get("macro", ""),
                "sector":         info.get("sector", ""),
                "industry":       info.get("industry", ""),
                "basic_industry": info.get("basic_industry", ""),
            })
            result_rows.append(row)
            if row["direction"] == "bull":
                hits_bull += 1
            else:
                hits_bear += 1

    logger.info("Daily Hunt: %d bull hits, %d bear hits, %d errors out of %d symbols",
                hits_bull, hits_bear, errors, len(symbols))

    db.upsert_scanner_results(result_rows)
    # Remove any symbol+direction that no longer qualifies in this scan run
    db.delete_scanner_results_not_in([(r["symbol"], r["direction"]) for r in result_rows])
    db.clear_stale_scanner_results()
    db.scanner_run_finish(
        run_id,
        scanned   = len(symbols),
        hits_bull = hits_bull,
        hits_bear = hits_bear,
        errors    = errors,
        status    = "done",
    )

    return {
        "ok":               True,
        "scanned":          len(symbols),
        "hits_bull":        hits_bull,
        "hits_bear":        hits_bear,
        "errors":           errors,
        "as_of":            as_of,
        "oldest_scan_date": oldest_scan_date,
    }


# ── Read cached results ───────────────────────────────────────────────────────

# In-memory cache: keyed by scanner last_run finished_at timestamp.
# Invalidated automatically when a new scan completes.
_enrich_cache: dict = {"ts": None, "data": {}}  # data: sym → enriched fields


def _get_enrich_cache(syms: list[str], as_of: str | None) -> dict | None:
    """Return cached enrichment dict if still valid for this scan run, else None.
    Unknown symbols get an empty dict (graceful degradation)."""
    if _enrich_cache["ts"] != as_of or as_of is None:
        return None
    return _enrich_cache["data"]


def _set_enrich_cache(as_of: str | None, data: dict) -> None:
    _enrich_cache["ts"] = as_of
    _enrich_cache["data"] = data


def get_cached_results(
    direction: str,
    macros: list[str],
    sectors: list[str],
    industries: list[str],
    basic_industries: list[str],
    indices: list[str] | None = None,
    hawks: list[str] | None = None,
    symbols: list[str] | None = None,
) -> dict:
    """
    Read pre-computed Daily Hunt results from DB, filtered by direction + group selection.
    direction: 'bull' | 'bear'
    """
    rows = db.get_scanner_results(
        direction        = direction,
        macros           = macros,
        sectors          = sectors,
        industries       = industries,
        basic_industries = basic_industries,
        indices          = indices,
        hawks            = hawks,
        symbols          = symbols,
    )

    last_run = db.get_scanner_last_run()
    as_of    = last_run.get("finished_at") if last_run else None

    results = []
    for r in rows:
        if not r.get("bull_cross_date"):
            continue
        results.append({
            "symbol":          r["symbol"],
            "company_name":    r["company_name"] or "",
            "macro":           r["macro"] or "",
            "sector":          r["sector"] or "",
            "industry":        r["industry"] or "",
            "basic_industry":  r["basic_industry"] or "",
            "direction":       r["direction"],
            "bull_cross_date": r.get("bull_cross_date"),
            "above_200sma":    bool(r.get("above_200sma", False)),
        })

    # Use cached enrichment if this scan run hasn't changed.
    syms = [r["symbol"] for r in results]
    cached = _get_enrich_cache(syms, as_of)
    if cached is not None:
        for r in results:
            r.update(cached.get(r["symbol"], {}))
    else:
        _enrich_with_ticker_meta(results)
        # Store enrichment fields in cache keyed by symbol.
        enrich_fields = ("index_list", "hawks_mappings",
                         "rs21", "rs21_rank", "rs55", "rs55_rank",
                         "rs252", "rs252_rank", "w52_high", "w52_low", "last_close")
        _set_enrich_cache(as_of, {
            r["symbol"]: {k: r.get(k) for k in enrich_fields}
            for r in results
        })

    return {
        "results":   results,
        "as_of":     as_of,
        "last_run":  last_run,
        "direction": direction,
    }


def _get_all_group_values() -> dict:
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            result = {}
            for col in ("macro", "sector", "industry", "basic_industry"):
                cur.execute(
                    f"SELECT DISTINCT {col} FROM tickers "
                    f"WHERE is_active=1 AND {col} != '' AND {col} != 'Uncategorized' ORDER BY {col}"
                )
                result[col] = [r[0] for r in cur.fetchall()]
        return result
    finally:
        db.release_conn(conn)
