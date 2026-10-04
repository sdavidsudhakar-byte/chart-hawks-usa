"""
run_deep_history_seed.py — One-time deep historical seed (~6 years) for all
stocks + indices via yfinance.

The regular refresh (market_data.run_prices_refresh / index_data.run_index_refresh)
only fetches a 366-day top-up window — fine for day-to-day operation, but not
enough depth for: weekly SMA200 (needs ~200 weeks ≈ 5+ years), the IBD LT score
history strip (needs 252+ trading days of buffer per historical point), or
multi-year chart views. This script seeds real depth once; the regular refresh
keeps it topped up afterward (never purges, per market_data.py's design).

Usage:
    python run_deep_history_seed.py [--years N] [--workers N] [--indices-only] [--stocks-only]
"""

import argparse
import datetime
import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

import db
import market_data
import index_data


def seed_symbol(limiter, symbol, from_date, to_date):
    rows = market_data.fetch_daily(limiter, symbol, from_date, to_date)
    if rows:
        db.upsert_prices(rows)
    return len(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--years", type=float, default=6.2)
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--indices-only", action="store_true")
    parser.add_argument("--stocks-only", action="store_true")
    args = parser.parse_args()

    db.init_db()

    today = market_data._last_trading_day()
    from_date = (today - datetime.timedelta(days=int(args.years * 365.25))).isoformat()
    to_date = today.isoformat()
    logger.info("Deep history seed: %s -> %s (%.1f years)", from_date, to_date, args.years)

    limiter = market_data._RateLimiter(market_data.RATE_LIMIT_RPS)

    symbols = []
    if not args.stocks_only:
        symbols += list(index_data.SYMBOLS)
    if not args.indices_only:
        conn = db.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT symbol FROM tickers WHERE is_active=1 ORDER BY symbol")
                symbols += [r[0] for r in cur.fetchall()]
        finally:
            db.release_conn(conn)

    total = len(symbols)
    logger.info("Seeding %d symbols...", total)

    completed = success = 0
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(seed_symbol, limiter, sym, from_date, to_date): sym for sym in symbols}
        for fut in as_completed(futures):
            sym = futures[fut]
            completed += 1
            try:
                n = fut.result()
                if n:
                    success += 1
            except Exception as e:
                logger.warning("%s: seed failed: %s", sym, e)
            if completed % 200 == 0 or completed == total:
                logger.info("[%d/%d] seeded so far: %d", completed, total, success)

    logger.info("Deep history seed done: %d/%d symbols got bars", success, total)


if __name__ == "__main__":
    main()
