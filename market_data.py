"""
market_data.py — Full daily OHLCV redownload from yfinance for all US tickers.
Replaces prices.py (Fyers). Same event-stream contract as the old module so
main.py's /refresh-prices SSE consumer needs no changes.

Strategy
--------
* Every run fetches ~1 year of daily history for ALL symbols — no incremental
  logic (yfinance has no per-call range limit worth optimizing around for
  this window size). Weekly/monthly bars are resampled from this daily data,
  not fetched separately.
* A thread pool (NUM_WORKERS threads) processes symbols in parallel, throttled
  by a shared token-bucket rate limiter — yfinance is an unofficial scrape of
  Yahoo Finance and will throttle/block aggressive concurrent access.
* No auth/session management needed (yfinance requires no credentials).
* Old price rows beyond KEEP_CALENDAR_DAYS are purged at the end (disabled,
  matching the previous behavior, until the project matures).

Retention
---------
KEEP_CALENDAR_DAYS = 550 calendar days, covers the 300-bar EMA warmup.

Note: 60m intraday fetch/storage was retired (along with the hourly cycle
scanner) — this product runs on a monthly cadence, so intraday data was pure
dead weight: the single heaviest per-symbol step in every refresh, fetched
individually per symbol with no batching, buying signals that were stale
within hours.
"""

import datetime
import logging
import queue
import threading
import time
from typing import Generator
from zoneinfo import ZoneInfo

import pandas_market_calendars as mcal
import yfinance as yf

import db

logger = logging.getLogger(__name__)

# ── Tunables ──────────────────────────────────────────────────────────────────

NUM_WORKERS = 6

# Max requests/sec across ALL threads combined. yfinance has no published
# limit; this has been stable in practice against the unofficial Yahoo API.
RATE_LIMIT_RPS = 3.0

KEEP_CALENDAR_DAYS = 2400

ET = ZoneInfo("America/New_York")
_NYSE = mcal.get_calendar("NYSE")

BENCHMARK_SYMBOL = "^GSPC"


# ── Market calendar ───────────────────────────────────────────────────────────

def _last_trading_day() -> datetime.date:
    """
    Returns the request-range upper bound for yfinance history calls.
    - Before 4:00 PM ET → previous NYSE session
    - At/after 4:00 PM ET → today's session if NYSE was open, else the prior one
    """
    now_et = datetime.datetime.now(ET)
    cutoff = now_et.date()
    market_close = now_et.replace(hour=16, minute=0, second=0, microsecond=0)
    if now_et < market_close:
        cutoff -= datetime.timedelta(days=1)

    sched = _NYSE.schedule(start_date=cutoff - datetime.timedelta(days=15), end_date=cutoff)
    if sched.empty:
        sched = _NYSE.schedule(start_date=cutoff - datetime.timedelta(days=45), end_date=cutoff)
    return sched.index[-1].date()


def _full_history_from_date() -> datetime.date:
    return _last_trading_day() - datetime.timedelta(days=366)


# ── Rate limiter (token bucket) ───────────────────────────────────────────────

class _RateLimiter:
    def __init__(self, rps: float):
        self._interval = 1.0 / rps
        self._lock = threading.Lock()
        self._next_at = time.monotonic()

    def acquire(self):
        with self._lock:
            now = time.monotonic()
            wait = self._next_at - now
            if wait > 0:
                time.sleep(wait)
            self._next_at = max(time.monotonic(), self._next_at) + self._interval


# ── Core fetch helpers ────────────────────────────────────────────────────────

def _history_to_rows(symbol: str, df, to_date: str) -> list[dict]:
    if df is None or df.empty:
        return []
    rows = []
    for ts, row in df.iterrows():
        d = ts.date().isoformat()
        if d > to_date:
            continue
        try:
            rows.append({
                "symbol": symbol,
                "date":   d,
                "open":   float(row["Open"]),
                "high":   float(row["High"]),
                "low":    float(row["Low"]),
                "close":  float(row["Close"]),
                "volume": int(row["Volume"]) if row["Volume"] == row["Volume"] else 0,
            })
        except Exception:
            continue
    return rows


def fetch_daily(limiter: _RateLimiter, symbol: str, from_date: str, to_date: str, retries: int = 3) -> list[dict]:
    end = (datetime.date.fromisoformat(to_date) + datetime.timedelta(days=1)).isoformat()
    for attempt in range(1, retries + 1):
        limiter.acquire()
        try:
            df = yf.Ticker(symbol).history(start=from_date, end=end, interval="1d", auto_adjust=False)
            return _history_to_rows(symbol, df, to_date)
        except Exception as e:
            logger.warning("%s: daily fetch failed (attempt %s): %s", symbol, attempt, e)
            time.sleep(1.5 * attempt)
    return []


def fetch_benchmark(limiter: _RateLimiter, from_date: str, to_date: str) -> list[dict]:
    rows = fetch_daily(limiter, BENCHMARK_SYMBOL, from_date, to_date)
    return [{**r, "symbol": BENCHMARK_SYMBOL} for r in rows]


# ── Worker ────────────────────────────────────────────────────────────────────

def _worker(
    work_queue: "queue.Queue[str | None]",
    limiter: _RateLimiter,
    from_date: str,
    to_date: str,
    result_queue: "queue.Queue[dict]",
):
    while True:
        symbol = work_queue.get()
        if symbol is None:
            work_queue.task_done()
            break

        rows = fetch_daily(limiter, symbol, from_date, to_date)

        result_queue.put({
            "symbol": symbol,
            "status": "ok" if rows else "no_data",
            "rows":   rows,
        })
        work_queue.task_done()


# ── Main generator ────────────────────────────────────────────────────────────

def run_prices_refresh() -> Generator[dict, None, None]:
    """
    Generator yielding SSE-style progress events (same shape as prices.py):
      {"type": "status", "message": str}
      {"type": "price_batch", "rows": list[dict]}
      {"type": "price_progress", "current", "total", "symbol", "status", "bars"}
      {"type": "price_done", "total", "success", "failed": list[str]}
      {"type": "error", "message": str}
    """
    limiter = _RateLimiter(RATE_LIMIT_RPS)

    calendar_today = _last_trading_day()
    full_from      = _full_history_from_date().isoformat()
    calendar_str   = calendar_today.isoformat()

    yield {"type": "status", "message": f"Fetching {BENCHMARK_SYMBOL} benchmark..."}
    bench_rows = fetch_benchmark(limiter, full_from, calendar_str)
    if bench_rows:
        db.upsert_prices(bench_rows)
        yield {"type": "price_batch", "rows": []}
        logger.info("%s: %d bars (%s to %s)", BENCHMARK_SYMBOL, len(bench_rows), full_from, calendar_str)
    else:
        logger.warning("%s benchmark fetch returned no data", BENCHMARK_SYMBOL)

    bench_now = db.get_latest_price_dates([BENCHMARK_SYMBOL])
    today_str = bench_now.get(BENCHMARK_SYMBOL) or calendar_str
    if today_str > calendar_str:
        today_str = calendar_str
    logger.info("Last trading day (from %s): %s (calendar: %s)", BENCHMARK_SYMBOL, today_str, calendar_str)

    yield {"type": "status", "message": "Loading symbols from DB..."}
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT symbol FROM tickers WHERE is_active=1 ORDER BY symbol")
            symbols = [r[0] for r in cur.fetchall()]
    finally:
        db.release_conn(conn)

    total = len(symbols)
    yield {
        "type":    "status",
        "message": f"Starting full redownload of {total} symbols — daily ({full_from} to {today_str}) "
                   f"({NUM_WORKERS} threads, {RATE_LIMIT_RPS:.0f} req/sec)...",
    }

    work_queue: queue.Queue = queue.Queue()
    result_queue: queue.Queue = queue.Queue()

    for sym in symbols:
        work_queue.put(sym)
    for _ in range(NUM_WORKERS):
        work_queue.put(None)

    threads = []
    for wid in range(NUM_WORKERS):
        t = threading.Thread(
            target=_worker,
            args=(work_queue, limiter, full_from, today_str, result_queue),
            daemon=True,
            name=f"price-worker-{wid}",
        )
        t.start()
        threads.append(t)

    success = 0
    failed: list[str] = []
    daily_batch: list[dict] = []
    completed = 0
    DAILY_BATCH_SIZE = 50 * 250

    while completed < total:
        try:
            result = result_queue.get(timeout=300)
        except queue.Empty:
            logger.error("Result queue timed out — workers may have stalled")
            break

        completed += 1
        symbol = result["symbol"]
        status = result["status"]
        rows = result.get("rows", [])

        if rows:
            daily_batch.extend(rows)
            success += 1
        else:
            failed.append(symbol)

        yield {
            "type":    "price_progress",
            "current": completed,
            "total":   total,
            "symbol":  symbol,
            "status":  status,
            "bars":    len(rows),
        }

        if len(daily_batch) >= DAILY_BATCH_SIZE:
            yield {"type": "price_batch", "rows": daily_batch}
            daily_batch = []

    for t in threads:
        t.join(timeout=10)

    if daily_batch:
        yield {"type": "price_batch", "rows": daily_batch}

    yield {
        "type":    "price_done",
        "total":   total,
        "success": success,
        "failed":  failed,
    }
