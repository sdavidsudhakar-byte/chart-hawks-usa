"""
scanner.py — Daily Hunt Scanner

Scans all symbols for a 3-milestone bull/bear sequence on daily bars:

Bull sequence:
  Step 1: 20 EMA crosses BELOW 50 EMA  (bear phase begins)
  Step 2: Monthly high > prev month high → M-High date (keep overwriting until Step 3)
  Step 3: 20 EMA crosses ABOVE 50 EMA  (bull cross — freeze M-High, watch pullbacks)
  Step 4: Price low ≤ 20 EMA + ATR_MULT×ATR  → S-Pullback date (first occurrence)
  Step 5: Price low ≤ 50 EMA + ATR_MULT×ATR  → C-Pullback date (first occurrence)

Bear sequence (mirror):
  Step 1: 20 EMA crosses ABOVE 50 EMA  (bull phase begins)
  Step 2: Monthly low < prev month low  → M-Low date (keep overwriting until Step 3)
  Step 3: 20 EMA crosses BELOW 50 EMA  (bear cross)
  Step 4: Price high ≥ 20 EMA - ATR_MULT×ATR → S-Pullback date
  Step 5: Price high ≥ 50 EMA - ATR_MULT×ATR → C-Pullback date

A stock appears in results only when at least one of its 3 milestone dates
falls within the last SCAN_DAYS trading days.

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
SCAN_DAYS      = 30     # milestone dates must fall within this many trading days
ATR_PERIOD     = 14     # ATR period
DEFAULT_ATR_MULT = 0.5  # default ATR multiplier for pullback proximity


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


def _compute_atr(highs: list[float], lows: list[float], closes: list[float],
                 period: int = ATR_PERIOD) -> list[Optional[float]]:
    """Standard ATR using Wilder's smoothing (EMA with period as divisor)."""
    n = len(closes)
    result: list[Optional[float]] = [None] * n
    if n < period + 1:
        return result

    # True ranges
    trs: list[float] = []
    for i in range(1, n):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        trs.append(tr)
    # trs[i] corresponds to bar i+1

    # Seed with simple average of first `period` true ranges
    if len(trs) < period:
        return result
    atr_val = sum(trs[:period]) / period
    result[period] = atr_val  # first valid ATR is at bar index `period`
    k = 1.0 / period           # Wilder's multiplier = 1/period
    for i in range(period + 1, n):
        atr_val = trs[i - 1] * k + atr_val * (1 - k)
        result[i] = atr_val
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


# ── Monthly OHLC helper ───────────────────────────────────────────────────────

def _build_monthly_ohlc(dates: list[str], highs: list[float],
                         lows: list[float]) -> list[tuple]:
    """
    Group daily bars into calendar months.
    Returns [(year, month, month_high, month_low), ...] in chronological order.
    Only includes fully computable months (includes in-progress current month).
    """
    monthly: dict[tuple, list] = {}
    for i, d in enumerate(dates):
        ym = (int(d[:4]), int(d[5:7]))
        if ym not in monthly:
            monthly[ym] = {"high": highs[i], "low": lows[i]}
        else:
            if highs[i] > monthly[ym]["high"]:
                monthly[ym]["high"] = highs[i]
            if lows[i] < monthly[ym]["low"]:
                monthly[ym]["low"] = lows[i]
    return [(ym[0], ym[1], v["high"], v["low"]) for ym, v in sorted(monthly.items())]


# ── Per-symbol scan logic ─────────────────────────────────────────────────────

def _scan_symbol(dates: list[str], opens: list[float], highs: list[float],
                 lows: list[float], closes: list[float],
                 oldest_scan_date: str, atr_mult: float,
                 c_atr_mult: float) -> list[dict]:
    """
    Run the full bull+bear milestone scan for one symbol.
    Returns a list of 0, 1, or 2 result dicts (one per direction that qualifies).
    """
    n = len(closes)
    if n < 60:
        return []

    ema20 = _compute_ema(closes, 20)
    ema50 = _compute_ema(closes, 50)
    atr   = _compute_atr(highs, lows, closes, ATR_PERIOD)
    monthly = _build_monthly_ohlc(dates, highs, lows)

    results = []
    for direction in ("bull", "bear"):
        row = _scan_direction(
            direction, dates, highs, lows, closes,
            ema20, ema50, atr, monthly,
            oldest_scan_date, atr_mult, c_atr_mult,
        )
        if row:
            results.append(row)
    return results


def _scan_direction(direction: str, dates, highs, lows, closes,
                    ema20, ema50, atr, monthly,
                    oldest_scan_date: str, atr_mult: float,
                    c_atr_mult: float = None) -> Optional[dict]:
    """
    Bull: look for 20 crosses below 50 (Step 1), monthly high break (Step 2),
          20 crosses above 50 (Step 3), pullbacks to 20/50 EMA (Steps 4-5).
    Bear: mirror (cross above → monthly low break → cross below → pullbacks).
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

    # ── Step 2: M-High / M-Low between step1 and cross (overwrite each month) ─
    # Include the cross month but cap daily bars at the cross date itself.
    # If no breach found pre-cross, scan post-cross months for the FIRST breach
    # and freeze there (one-time catch, no further overwriting).
    m_date: Optional[str] = None
    m_price: Optional[float] = None   # prev-month H/L threshold that was broken (for chart overlay)
    if len(monthly) >= 2:
        step1_ym = (int(step1_date[:4]), int(step1_date[5:7]))
        cross_ym  = (int(cross_date[:4]), int(cross_date[5:7])) if cross_date else None

        # Pass 1: pre-cross window (step1 month → cross month inclusive, capped at cross date)
        for mi in range(1, len(monthly)):
            yr, mo, mh, ml = monthly[mi]
            cur_ym = (yr, mo)
            if cur_ym < step1_ym:
                continue
            if cross_ym and cur_ym > cross_ym:
                break
            prev_mh = monthly[mi - 1][2]
            prev_ml = monthly[mi - 1][3]
            cap_date   = cross_date if (cross_ym and cur_ym == cross_ym) else None
            floor_date = step1_date if cur_ym == step1_ym else None
            if direction == "bull" and mh > prev_mh:
                d = _first_date_in_month(dates, highs, yr, mo, prev_mh, "high", cap_date=cap_date, floor_date=floor_date)
                if d:
                    m_date = d; m_price = prev_mh
            elif direction == "bear" and ml < prev_ml:
                d = _first_date_in_month(dates, lows, yr, mo, prev_ml, "low", cap_date=cap_date, floor_date=floor_date)
                if d:
                    m_date = d; m_price = prev_ml

        # Pass 2: if still no m_date, scan post-cross for the first breach and stop.
        # Include the cross month itself (bars after cross_date) before moving to next months.
        if m_date is None and cross_ym:
            for mi in range(1, len(monthly)):
                yr, mo, mh, ml = monthly[mi]
                cur_ym = (yr, mo)
                if cur_ym < cross_ym:
                    continue
                prev_mh = monthly[mi - 1][2]
                prev_ml = monthly[mi - 1][3]
                # For the cross month: only look at bars strictly after cross_date
                start_after = cross_date if cur_ym == cross_ym else None
                if direction == "bull" and mh > prev_mh:
                    m_date = _first_date_in_month_after(dates, highs, yr, mo, prev_mh, "high", start_after)
                    if m_date:
                        m_price = prev_mh; break
                elif direction == "bear" and ml < prev_ml:
                    m_date = _first_date_in_month_after(dates, lows, yr, mo, prev_ml, "low", start_after)
                    if m_date:
                        m_price = prev_ml; break

    # ── Validate: at least one milestone must be recent enough ────────────────
    # S-Pullback and C-Pullback only exist after the cross
    s_pullback_date: Optional[str] = None
    c_pullback_date: Optional[str] = None
    _c_atr_mult = c_atr_mult if c_atr_mult is not None else atr_mult
    _c_gate_triggered = False  # True once price breaches below/above 20 EMA

    if cross_idx is not None:
        for i in range(cross_idx + 1, n):
            e20 = ema20[i]
            e50 = ema50[i]
            at  = atr[i]
            if e20 is None or e50 is None or at is None:
                continue
            s_band = atr_mult * at
            c_band = _c_atr_mult * at
            if direction == "bull":
                # S-Pullback: low touches within ATR band above 20 EMA
                if s_pullback_date is None and lows[i] <= e20 + s_band:
                    s_pullback_date = dates[i]
                # C-Pullback gate: price breaches below 20 EMA
                if not _c_gate_triggered and lows[i] < e20:
                    _c_gate_triggered = True
                # C-Pullback target: once gate triggered, low within ATR band above 50 EMA
                if c_pullback_date is None and _c_gate_triggered and lows[i] <= e50 + c_band:
                    c_pullback_date = dates[i]
            else:
                # S-Pullback: high touches within ATR band below 20 EMA
                if s_pullback_date is None and highs[i] >= e20 - s_band:
                    s_pullback_date = dates[i]
                # C-Pullback gate: price breaches above 20 EMA
                if not _c_gate_triggered and highs[i] > e20:
                    _c_gate_triggered = True
                # C-Pullback target: once gate triggered, high within ATR band below 50 EMA
                if c_pullback_date is None and _c_gate_triggered and highs[i] >= e50 - c_band:
                    c_pullback_date = dates[i]
            # Stop once both pullbacks found
            if s_pullback_date and c_pullback_date:
                break

    # Skip if only cross exists — need at least one displayable signal
    if not m_date and not s_pullback_date and not c_pullback_date:
        return None

    # ── Check if any milestone falls within the scan window ───────────────────
    milestone_dates = [d for d in (m_date, cross_date, s_pullback_date, c_pullback_date) if d]
    if not any(d >= oldest_scan_date for d in milestone_dates):
        return None

    # Compute above_200sma flag at the cross date (or latest bar if no cross yet)
    check_idx = cross_idx if cross_idx is not None else (n - 1)
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
        "m_date":          m_date,
        "m_price":         m_price,
        "bull_cross_date": cross_date,
        "s_pullback_date": s_pullback_date,
        "c_pullback_date": c_pullback_date,
        "above_200sma":    above_200sma,
    }


def _first_date_in_month(dates: list[str], values: list[float],
                          yr: int, mo: int, threshold: float,
                          direction: str,
                          cap_date: Optional[str] = None,
                          floor_date: Optional[str] = None) -> Optional[str]:
    """
    Find the first date in (yr, mo) where value strictly breaks the threshold.
    direction='high': value > threshold; direction='low': value < threshold.
    floor_date: skip bars before this date (for the step1 month).
    cap_date: if set, ignore any bar after this date (used for the cross month).
    Returns None if no bar actually breaches — no fallback.
    """
    month_str = f"{yr:04d}-{mo:02d}"
    for i, d in enumerate(dates):
        if not d.startswith(month_str):
            continue
        if floor_date and d < floor_date:
            continue
        if cap_date and d > cap_date:
            break
        if direction == "high" and values[i] > threshold:
            return d
        if direction == "low" and values[i] < threshold:
            return d
    return None


def _first_date_in_month_after(dates: list[str], values: list[float],
                                yr: int, mo: int, threshold: float,
                                direction: str,
                                start_after: Optional[str] = None) -> Optional[str]:
    """
    Like _first_date_in_month but skips bars up to and including start_after.
    Used for post-cross M-High/Low search within the cross month.
    """
    month_str = f"{yr:04d}-{mo:02d}"
    for i, d in enumerate(dates):
        if not d.startswith(month_str):
            continue
        if start_after and d <= start_after:
            continue
        if direction == "high" and values[i] > threshold:
            return d
        if direction == "low" and values[i] < threshold:
            return d
    return None


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

def run_full_scan(run_id: int | None = None, atr_mult: float | None = None, c_atr_mult: float | None = None) -> dict:
    """
    Scan ALL symbols for the 3-milestone bull/bear sequence on daily bars.
    Fully DB-backed — no Fyers calls.
    Results written to scanner_results table.
    """
    if atr_mult is None:
        saved = db.get_app_state("atr_mult")
        atr_mult = float(saved) if saved else DEFAULT_ATR_MULT
    if c_atr_mult is None:
        saved = db.get_app_state("c_atr_mult")
        c_atr_mult = float(saved) if saved else DEFAULT_ATR_MULT
    saved_days = db.get_app_state("scan_days")
    scan_days = int(saved_days) if saved_days else SCAN_DAYS
    logger.info("Daily Hunt scanner starting (atr_mult=%.2f)...", atr_mult)
    if run_id is None:
        run_id = db.scanner_run_start()

    recent_dates = _get_latest_n_trading_dates(scan_days)
    if not recent_dates:
        db.scanner_run_finish(run_id, 0, 0, 0, 0, "error")
        return {"ok": False, "error": "No price data in DB."}

    oldest_scan_date = min(recent_dates)
    as_of            = recent_dates[0]

    symbols  = _get_all_symbols()
    sym_list = symbols
    logger.info("Daily Hunt: %d symbols, window back to %s", len(symbols), oldest_scan_date)

    # Bulk load OHLC (need high/low for monthly breakout + ATR + pullback proximity)
    total_bars = DAILY_WARMUP + BEAR_LOOKBACK
    all_ohlc = _bulk_load_daily_ohlc(sym_list, total_bars)
    logger.info("Daily Hunt: loaded OHLC for %d symbols — computing milestones...", len(all_ohlc))

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
        opens  = [r[1] for r in ohlc]
        highs  = [r[2] for r in ohlc]
        lows   = [r[3] for r in ohlc]
        closes = [r[4] for r in ohlc]
        try:
            rows = _scan_symbol(dates, opens, highs, lows, closes, oldest_scan_date, atr_mult, c_atr_mult)
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
        # Skip stale cross-only rows that have no displayable signals
        if not r.get("m_date") and not r.get("s_pullback_date") and not r.get("c_pullback_date"):
            continue
        results.append({
            "symbol":          r["symbol"],
            "company_name":    r["company_name"] or "",
            "macro":           r["macro"] or "",
            "sector":          r["sector"] or "",
            "industry":        r["industry"] or "",
            "basic_industry":  r["basic_industry"] or "",
            "direction":       r["direction"],
            "m_date":          r.get("m_date"),
            "m_price":         r.get("m_price"),
            "bull_cross_date": r.get("bull_cross_date"),
            "s_pullback_date": r.get("s_pullback_date"),
            "c_pullback_date": r.get("c_pullback_date"),
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
