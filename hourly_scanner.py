"""
hourly_scanner.py — Hourly cycle enrichment for Daily Hunt results.

For each stock that passes the daily scan (Steps 1-3), scans hourly bars
within the active daily bull/bear phase for:
  Step 4: Hourly EMA20 crosses below/above EMA50 (mini bear phase start)
  Step 5: Price breaks prior calendar week's high/low (weekly momentum signal)
  Step 6: Hourly EMA20 crosses back above/below EMA50 (hourly bull/bear cross)

Cycles repeat until the daily phase ends (daily EMA20 crosses back).
Partial cycles (W breach without H cross yet) are stored as in-progress signals.

Results stored in scan_signals table. Called after run_full_scan() completes.
"""

import datetime
import json
import logging

import db

logger = logging.getLogger(__name__)

HOURLY_DATA_FLOOR = "2026-03-19"   # earliest date we have hourly data


def _ts_to_date(ts: int) -> str:
    """Unix epoch → IST date string YYYY-MM-DD."""
    dt = datetime.datetime.utcfromtimestamp(ts) + datetime.timedelta(hours=5, minutes=30)
    return dt.strftime("%Y-%m-%d")


def _ts_to_week_monday(ts: int) -> str:
    """Return the Monday (week start) for the IST date of a timestamp."""
    dt = datetime.datetime.utcfromtimestamp(ts) + datetime.timedelta(hours=5, minutes=30)
    monday = dt - datetime.timedelta(days=dt.weekday())
    return monday.strftime("%Y-%m-%d")


def _build_weekly_map(weekly_rows) -> dict:
    """
    Build {week_monday: (high, low)} from ohlcv_weekly rows.
    week column in DB is the Monday date string.
    """
    return {r["week"]: (r["high"], r["low"]) for r in weekly_rows}


def _prior_week_ref(ts: int, weekly_map: dict):
    """
    Return (prior_week_high, prior_week_low, prior_week_monday) for the week
    before the week containing ts. Returns (None, None, None) if not available.
    """
    current_week = _ts_to_week_monday(ts)
    weeks_sorted = sorted(weekly_map.keys(), reverse=True)
    for w in weeks_sorted:
        if w < current_week:
            pw_high, pw_low = weekly_map[w]
            return pw_high, pw_low, w
    return None, None, None


def _find_pw_source_ts(pw_monday: str, pw_level: float, is_high: bool, hourly_rows: list) -> int | None:
    """
    Among hourly candles that fall within the prior week (Monday–Friday IST),
    find the candle whose high (bull) or low (bear) equals pw_level.
    Returns the ts of that candle, or None.
    """
    week_start = datetime.datetime.strptime(pw_monday, "%Y-%m-%d")
    # Prior week Mon 09:15 IST → Sun 23:59 IST (we just need Mon–Fri)
    pw_start_ts = int((week_start.replace(hour=3, minute=45) ).timestamp())
    pw_end_ts   = pw_start_ts + 7 * 86400  # 7 days covers Mon–Sun

    source_ts = None
    for row in hourly_rows:
        ts = row["ts"]
        if ts < pw_start_ts:
            continue
        if ts >= pw_end_ts:
            break
        val = row["high"] if is_high else row["low"]
        if val is None:
            continue
        # Match within floating point tolerance
        if abs(val - pw_level) < 0.01:
            source_ts = ts  # take the last matching candle of the week
    return source_ts


def _scan_hourly_cycles(
    direction: str,
    hourly_rows: list,         # [{ts, ema20, ema50, close, high, low}, ...]
    weekly_map: dict,          # {week_monday: (high, low)}
    phase_start_ts: int,       # daily cross timestamp floor
    phase_end_ts: int | None,  # daily phase end (None = still active)
) -> dict:
    """
    Scan hourly bars for repeating bull/bear cycles within the daily phase.

    Returns:
        {
          hourly_signals: [...],  # list of signal dicts
          wh_count: int,
          hx_count: int,
          latest_hx_ts: int | None,
          phase_end_date: str | None,
        }
    """
    is_bull = direction == "bull"
    signals = []
    wh_count = 0
    hx_count = 0
    latest_hx_ts = None

    in_mini_bear = False    # Step 4 triggered
    pending_wh = None       # Weekly H/L breach found, waiting for cross

    prev_ema20 = None
    prev_ema50 = None

    for row in hourly_rows:
        ts   = row["ts"]
        e20  = row["ema20"]
        e50  = row["ema50"]
        close = row["close"]
        high  = row["high"]
        low   = row["low"]

        # Skip bars before phase start or after phase end
        if ts < phase_start_ts:
            prev_ema20, prev_ema50 = e20, e50
            continue
        if phase_end_ts and ts >= phase_end_ts:
            break

        if e20 is None or e50 is None or prev_ema20 is None or prev_ema50 is None:
            prev_ema20, prev_ema50 = e20, e50
            continue

        if is_bull:
            # Step 4: EMA20 crosses below EMA50 → mini bear phase starts
            if not in_mini_bear:
                if prev_ema20 >= prev_ema50 and e20 < e50:
                    in_mini_bear = True
                    pending_wh = None

            if in_mini_bear:
                # Step 5: price breaks prior week's HIGH
                if pending_wh is None:
                    pw_high, _, pw_monday = _prior_week_ref(ts, weekly_map)
                    if pw_high is not None and high > pw_high:
                        wh_count += 1
                        pw_src = _find_pw_source_ts(pw_monday, pw_high, True, hourly_rows) if pw_monday else None
                        pending_wh = {
                            "wh_ts":       ts,
                            "wh_count":    wh_count,
                            "pw_level":    pw_high,
                            "pw_source_ts": pw_src,
                        }

                # Step 6: EMA20 crosses back above EMA50 → hourly bull cross
                if prev_ema20 <= prev_ema50 and e20 > e50:
                    hx_count += 1
                    latest_hx_ts = ts
                    signals.append({
                        "n":           hx_count,
                        "cross_ts":    ts,
                        "wh_ts":       pending_wh["wh_ts"]        if pending_wh else None,
                        "wh_count":    pending_wh["wh_count"]      if pending_wh else None,
                        "pw_level":    pending_wh["pw_level"]      if pending_wh else None,
                        "pw_source_ts": pending_wh["pw_source_ts"] if pending_wh else None,
                        "in_progress": False,
                    })
                    in_mini_bear = False
                    pending_wh = None

        else:  # bear
            # Step 4: EMA20 crosses above EMA50 → mini bull phase starts
            if not in_mini_bear:
                if prev_ema20 <= prev_ema50 and e20 > e50:
                    in_mini_bear = True
                    pending_wh = None

            if in_mini_bear:
                # Step 5: price breaks prior week's LOW
                if pending_wh is None:
                    _, pw_low, pw_monday = _prior_week_ref(ts, weekly_map)
                    if pw_low is not None and low < pw_low:
                        wh_count += 1
                        pw_src = _find_pw_source_ts(pw_monday, pw_low, False, hourly_rows) if pw_monday else None
                        pending_wh = {
                            "wh_ts":        ts,
                            "wh_count":     wh_count,
                            "pw_level":     pw_low,
                            "pw_source_ts": pw_src,
                        }

                # Step 6: EMA20 crosses back below EMA50 → hourly bear cross
                if prev_ema20 >= prev_ema50 and e20 < e50:
                    hx_count += 1
                    latest_hx_ts = ts
                    signals.append({
                        "n":           hx_count,
                        "cross_ts":    ts,
                        "wh_ts":       pending_wh["wh_ts"]        if pending_wh else None,
                        "wh_count":    pending_wh["wh_count"]      if pending_wh else None,
                        "pw_level":    pending_wh["pw_level"]      if pending_wh else None,
                        "pw_source_ts": pending_wh["pw_source_ts"] if pending_wh else None,
                        "in_progress": False,
                    })
                    in_mini_bear = False
                    pending_wh = None

        prev_ema20, prev_ema50 = e20, e50

    # In-progress: mini bear started + weekly breach found but no cross yet
    if in_mini_bear and pending_wh is not None:
        signals.append({
            "n":            None,
            "cross_ts":     None,
            "wh_ts":        pending_wh["wh_ts"],
            "wh_count":     pending_wh["wh_count"],
            "pw_level":     pending_wh["pw_level"],
            "pw_source_ts": pending_wh["pw_source_ts"],
            "in_progress":  True,
        })

    return {
        "hourly_signals": signals,
        "wh_count":       wh_count,
        "hx_count":       hx_count,
        "latest_hx_ts":   latest_hx_ts,
    }


def _date_to_midnight_ts(date_str: str) -> int:
    """Convert YYYY-MM-DD (IST) to unix epoch of 09:15 IST (market open)."""
    dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    # 09:15 IST = 03:45 UTC
    dt_utc = dt.replace(hour=3, minute=45)
    return int(dt_utc.timestamp())


def run_hourly_scan() -> dict:
    """
    For every symbol in scanner_results, compute hourly cycles and upsert
    into scan_signals. Called after run_full_scan() completes.
    """
    logger.info("Hourly cycle scan starting...")
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            # Load all daily scan results
            cur.execute("""
                SELECT symbol, direction, m_date, m_price, bull_cross_date, above_200sma
                FROM scanner_results
            """)
            cols = [d[0] for d in cur.description]
            daily_rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        db.release_conn(conn)

    if not daily_rows:
        logger.info("Hourly scan: no daily results to enrich.")
        return {"ok": True, "processed": 0}

    symbols = list({r["symbol"] for r in daily_rows})
    logger.info("Hourly scan: enriching %d symbols...", len(symbols))

    # Bulk load hourly indicators
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            ph = ",".join(["%s"] * len(symbols))
            # Load hourly EMA20/50 + OHLC joined
            cur.execute(f"""
                SELECT i.symbol, i.ts, i.ema20, i.ema50,
                       o.close, o.high, o.low
                FROM indicator_hourly i
                JOIN ohlcv_intraday o ON o.symbol = i.symbol AND o.ts = i.ts
                WHERE i.symbol IN ({ph})
                  AND i.ema20 IS NOT NULL AND i.ema50 IS NOT NULL
                ORDER BY i.symbol, i.ts
            """, symbols)
            cols = [d[0] for d in cur.description]
            hourly_all: dict[str, list] = {}
            for row in cur.fetchall():
                d = dict(zip(cols, row))
                hourly_all.setdefault(d["symbol"], []).append(d)

            # Bulk load weekly candles
            cur.execute(f"""
                SELECT symbol, week, high, low
                FROM ohlcv_weekly
                WHERE symbol IN ({ph})
                ORDER BY symbol, week
            """, symbols)
            cols = [d[0] for d in cur.description]
            weekly_all: dict[str, list] = {}
            for row in cur.fetchall():
                d = dict(zip(cols, row))
                weekly_all.setdefault(d["symbol"], []).append(d)

            # Determine daily phase end: find if EMA20 crossed back since daily_cross_date
            cur.execute(f"""
                SELECT symbol, date, ema20, ema50
                FROM indicator_daily
                WHERE symbol IN ({ph})
                ORDER BY symbol, date
            """, symbols)
            cols = [d[0] for d in cur.description]
            daily_ema_all: dict[str, list] = {}
            for row in cur.fetchall():
                d = dict(zip(cols, row))
                daily_ema_all.setdefault(d["symbol"], []).append(d)
    finally:
        db.release_conn(conn)

    result_rows = []
    processed = 0

    for dr in daily_rows:
        symbol    = dr["symbol"]
        direction = dr["direction"]
        is_bull   = direction == "bull"
        daily_cross_date = dr.get("bull_cross_date")

        if not daily_cross_date:
            continue

        # Find daily phase end: first date after daily_cross_date where
        # bull: EMA20 < EMA50 again; bear: EMA20 > EMA50 again
        phase_end_date = None
        for ema_row in daily_ema_all.get(symbol, []):
            if ema_row["date"] <= daily_cross_date:
                continue
            e20 = ema_row.get("ema20")
            e50 = ema_row.get("ema50")
            if e20 is None or e50 is None:
                continue
            if is_bull and e20 < e50:
                phase_end_date = ema_row["date"]
                break
            if not is_bull and e20 > e50:
                phase_end_date = ema_row["date"]
                break

        # Phase window in unix timestamps
        phase_start_ts = _date_to_midnight_ts(
            max(daily_cross_date, HOURLY_DATA_FLOOR)
        )
        phase_end_ts = _date_to_midnight_ts(phase_end_date) if phase_end_date else None

        hourly_rows  = hourly_all.get(symbol, [])
        weekly_rows  = weekly_all.get(symbol, [])
        weekly_map   = _build_weekly_map(weekly_rows)

        cycles = _scan_hourly_cycles(
            direction, hourly_rows, weekly_map, phase_start_ts, phase_end_ts
        )

        result_rows.append({
            "symbol":           symbol,
            "direction":        direction,
            "step1_date":       None,   # not stored in scanner_results currently
            "m_date":           dr.get("m_date"),
            "m_price":          dr.get("m_price"),
            "daily_cross_date": daily_cross_date,
            "above_200sma":     dr.get("above_200sma", 0),
            "phase_end_date":   phase_end_date,
            "hourly_signals":   cycles["hourly_signals"],
            "wh_count":         cycles["wh_count"],
            "hx_count":         cycles["hx_count"],
            "latest_hx_ts":     cycles["latest_hx_ts"],
        })
        processed += 1

    db.upsert_scan_signals(result_rows)
    db.delete_scan_signals_not_in([(r["symbol"], r["direction"]) for r in result_rows])
    logger.info("Hourly scan: upserted %d rows into scan_signals.", len(result_rows))
    return {"ok": True, "processed": processed}
