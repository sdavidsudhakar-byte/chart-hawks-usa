"""
flash_prices.py — Fast INCREMENTAL daily+60m OHLCV refresh via yfinance.

This is the "Flash Price Refresh" companion to market_data.py. It is a
drop-in alternative for the P1-Stocks phase that fetches only *new* bars per
symbol instead of re-downloading a full year of history every run.

Design:
  * Incremental daily fetch — from_date = (per-symbol last DB date − OVERLAP_DAYS).
    On a normal daily run this pulls ~1–5 bars/symbol instead of ~366. The small
    trailing overlap self-heals bars yfinance revises after the fact.
  * Brand-new symbols (no rows in ohlcv_daily) fall back to a full-history pull
    so they are seeded correctly on first sight.
  * Intraday 60m fetched for ALL symbols (same scope as the full refresh).
  * NEVER purges history — merges by (symbol,date)/(symbol,ts), safe to run
    alongside the full refresh against the same tables.

market_data.py is imported and reused for all the low-level yfinance plumbing
(rate limiter, fetch helpers, benchmark) — this module only overrides the
orchestration so the fetch/rate-limit code stays in exactly one place.
"""

import datetime
import logging
import queue
import threading
from typing import Generator

import db
import market_data

logger = logging.getLogger(__name__)

# ── Tunables (independent of the full refresh so they can be tuned live) ────
FLASH_WORKERS        = 8
FLASH_RATE_LIMIT_RPS = 5.0

# Trailing trading-day overlap re-fetched every run so late bar revisions
# self-correct. 5 sessions is a good balance; the full refresh covers the rest.
OVERLAP_TRADING_DAYS = 5
_OVERLAP_CALENDAR_DAYS = OVERLAP_TRADING_DAYS + 4  # + weekend/holiday cushion

_SEED_CALENDAR_DAYS = 366
INTRADAY_KEEP_DAYS = market_data.INTRADAY_KEEP_DAYS


def _incremental_from_date(last_date: str | None, calendar_today: datetime.date) -> str:
    if not last_date:
        return (calendar_today - datetime.timedelta(days=_SEED_CALENDAR_DAYS)).isoformat()
    try:
        d = datetime.date.fromisoformat(last_date[:10])
    except Exception:
        return (calendar_today - datetime.timedelta(days=_SEED_CALENDAR_DAYS)).isoformat()
    return (d - datetime.timedelta(days=_OVERLAP_CALENDAR_DAYS)).isoformat()


def _worker(
    work_queue: "queue.Queue",
    limiter: "market_data._RateLimiter",
    intraday_from_date: str,
    to_date: str,
    result_queue: "queue.Queue",
):
    """Like market_data._worker, but each queue item carries its own per-symbol
    daily from_date: (symbol, from_date). Sentinel None exits."""
    while True:
        item = work_queue.get()
        if item is None:
            work_queue.task_done()
            break

        symbol, from_date = item
        rows = market_data.fetch_daily(limiter, symbol, from_date, to_date)
        intraday_rows = market_data.fetch_intraday_60m(limiter, symbol, intraday_from_date, to_date)

        result_queue.put({
            "symbol":        symbol,
            "status":        "ok" if rows else "no_data",
            "rows":          rows,
            "intraday_rows": intraday_rows,
        })
        work_queue.task_done()


def run_flash_refresh() -> Generator[dict, None, None]:
    """
    Generator yielding the same SSE-style events as market_data.run_prices_refresh()
    (price_batch / intraday_batch / price_progress / price_done), so the existing
    main.py P1 consumer works unchanged — but fetches incrementally.
    """
    limiter = market_data._RateLimiter(FLASH_RATE_LIMIT_RPS)

    calendar_today = market_data._last_trading_day()
    seed_from      = (calendar_today - datetime.timedelta(days=_SEED_CALENDAR_DAYS)).isoformat()
    intraday_from  = (calendar_today - datetime.timedelta(days=INTRADAY_KEEP_DAYS)).isoformat()
    calendar_str   = calendar_today.isoformat()

    yield {"type": "status", "message": f"Fetching {market_data.BENCHMARK_SYMBOL} benchmark (Flash)..."}
    bench_rows = market_data.fetch_benchmark(limiter, seed_from, calendar_str)
    if bench_rows:
        db.upsert_prices(bench_rows)
        yield {"type": "price_batch", "rows": []}
    else:
        logger.warning("Flash: %s benchmark fetch returned no data", market_data.BENCHMARK_SYMBOL)

    bench_now = db.get_latest_price_dates([market_data.BENCHMARK_SYMBOL])
    today_str = bench_now.get(market_data.BENCHMARK_SYMBOL) or calendar_str
    if today_str > calendar_str:
        today_str = calendar_str
    logger.info("Flash: last trading day (from %s): %s (calendar: %s)",
                market_data.BENCHMARK_SYMBOL, today_str, calendar_str)

    yield {"type": "status", "message": "Loading symbols from DB..."}
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT symbol FROM tickers WHERE is_active=1 ORDER BY symbol")
            symbols = [r[0] for r in cur.fetchall()]
    finally:
        db.release_conn(conn)

    total = len(symbols)

    last_dates = db.get_latest_price_dates(symbols)  # {symbol: 'YYYY-MM-DD'}
    seeded     = sum(1 for s in symbols if not last_dates.get(s))

    yield {
        "type": "status",
        "message": (f"Flash incremental refresh of {total} symbols "
                    f"({seeded} new/seed, overlap {OVERLAP_TRADING_DAYS}d) "
                    f"+ 60m ({intraday_from} to {today_str}) "
                    f"({FLASH_WORKERS} threads, {FLASH_RATE_LIMIT_RPS:.0f} req/sec)..."),
    }

    work_queue: queue.Queue = queue.Queue()
    result_queue: queue.Queue = queue.Queue()

    for sym in symbols:
        frm = _incremental_from_date(last_dates.get(sym), calendar_today)
        work_queue.put((sym, frm))
    for _ in range(FLASH_WORKERS):
        work_queue.put(None)

    threads = []
    for wid in range(FLASH_WORKERS):
        t = threading.Thread(
            target=_worker,
            args=(work_queue, limiter, intraday_from, today_str, result_queue),
            daemon=True,
            name=f"flash-worker-{wid}",
        )
        t.start()
        threads.append(t)

    success = 0
    failed: list[str] = []
    daily_batch: list[dict] = []
    intraday_batch: list[dict] = []
    completed = 0
    DAILY_BATCH_SIZE = 50 * 250
    INTRADAY_BATCH_SIZE = 50 * 470

    while completed < total:
        try:
            result = result_queue.get(timeout=300)
        except queue.Empty:
            logger.error("Flash: result queue timed out — workers may have stalled")
            break

        completed += 1
        symbol = result["symbol"]
        status = result["status"]
        rows = result.get("rows", [])
        intraday_rows = result.get("intraday_rows", [])

        if rows:
            daily_batch.extend(rows)
            success += 1
        else:
            failed.append(symbol)

        if intraday_rows:
            intraday_batch.extend(intraday_rows)

        yield {
            "type": "price_progress",
            "current": completed,
            "total": total,
            "symbol": symbol,
            "status": status,
            "bars": len(rows),
            "intraday_bars": len(intraday_rows),
        }

        if len(daily_batch) >= DAILY_BATCH_SIZE:
            yield {"type": "price_batch", "rows": daily_batch}
            daily_batch = []

        if len(intraday_batch) >= INTRADAY_BATCH_SIZE:
            yield {"type": "intraday_batch", "rows": intraday_batch}
            intraday_batch = []

    for t in threads:
        t.join(timeout=10)

    if daily_batch:
        yield {"type": "price_batch", "rows": daily_batch}
    if intraday_batch:
        yield {"type": "intraday_batch", "rows": intraday_batch}

    yield {
        "type": "price_done",
        "total": total,
        "success": success,
        "failed": failed,
    }
