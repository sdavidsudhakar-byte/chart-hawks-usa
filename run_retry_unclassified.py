"""
run_retry_unclassified.py — Retry yfinance classification for tickers that
came back "Uncategorized" from a full universe refresh.

run_ticker_refresh.py classifies ~6-8k non-S&P500 symbols via yfinance's
unofficial Ticker.info endpoint using 10 concurrent workers, which reliably
triggers Yahoo rate-limiting at that concurrency/scale. This script re-tries
just the failures with much lower concurrency and longer backoff — safe to
run repeatedly (each pass should recover a further chunk) until the
Uncategorized count stops shrinking.

Usage:
    python run_retry_unclassified.py [--workers N] [--retries N]
"""

import argparse
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

import db
import universe
import sector_etf_map


def fetch_with_backoff(symbol: str, retries: int) -> dict:
    for attempt in range(1, retries + 1):
        try:
            import yfinance as yf
            info = yf.Ticker(symbol).get_info()
            sector = (info.get("sector") or "").strip() or universe.UNCATEGORIZED
            sector = sector_etf_map.normalize_sector(sector)
            industry = (info.get("industry") or "").strip() or universe.UNCATEGORIZED
            name = (info.get("longName") or info.get("shortName") or "").strip()
            if sector != universe.UNCATEGORIZED:
                return {
                    "macro": sector, "sector": sector,
                    "industry": industry, "basic_industry": industry,
                    "company_name": name,
                }
        except Exception as e:
            logger.warning("%s: attempt %d/%d failed: %s", symbol, attempt, retries, e)
        time.sleep(3.0 * attempt)  # 3s, 6s, 9s... much gentler than the main refresh
    return {
        "macro": universe.UNCATEGORIZED, "sector": universe.UNCATEGORIZED,
        "industry": universe.UNCATEGORIZED, "basic_industry": universe.UNCATEGORIZED,
        "company_name": "",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--retries", type=int, default=3)
    args = parser.parse_args()

    db.init_db()
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT symbol, company_name FROM tickers WHERE sector='Uncategorized'")
            rows = [(r[0], r[1]) for r in cur.fetchall()]
    finally:
        db.release_conn(conn)

    total = len(rows)
    logger.info("Retrying classification for %d Uncategorized tickers (%d workers, %d retries)...",
                total, args.workers, args.retries)
    if not total:
        return

    success = 0
    batch = []
    completed = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(fetch_with_backoff, sym, args.retries): (sym, name) for sym, name in rows}
        for fut in as_completed(futures):
            sym, name = futures[fut]
            completed += 1
            info = fut.result()
            ok = info["sector"] != universe.UNCATEGORIZED
            if ok:
                success += 1
            batch.append({
                "symbol": sym,
                "company_name": info.get("company_name") or name or "",
                "macro": info["macro"], "sector": info["sector"],
                "industry": info["industry"], "basic_industry": info["basic_industry"],
                "index_list": [],
            })
            if len(batch) >= 50:
                db.upsert_tickers(batch)
                batch = []
            if completed % 200 == 0 or completed == total:
                logger.info("[%d/%d] recovered so far: %d", completed, total, success)

    if batch:
        db.upsert_tickers(batch)

    logger.info("Retry pass done: %d/%d recovered, %d still Uncategorized", success, total, total - success)

    logger.info("Rebuilding sector index map...")
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT symbol, sector FROM tickers WHERE is_active=1")
            tickers = [{"symbol": r[0], "sector": r[1]} for r in cur.fetchall()]
    finally:
        db.release_conn(conn)
    db.clear_sector_index_map()
    map_rows = sector_etf_map.build_sector_index_map_rows(tickers)
    db.upsert_sector_index_map(map_rows)
    logger.info("Sector map rebuilt: %d stocks -> %d rows, %d unclassified",
                len(tickers), len(map_rows), len(tickers) - len(map_rows))


if __name__ == "__main__":
    main()
