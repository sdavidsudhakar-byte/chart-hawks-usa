"""
run_ticker_refresh.py — Standalone script for GitHub Actions.
Downloads the US stock universe (S&P 500 + broader Nasdaq/NYSE listings),
classifies each symbol by GICS/Yahoo sector + industry, upserts into tickers.
Scheduled: every Saturday before market open.
"""

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

import db
import universe
import sector_etf_map
import industry_etf_map


def main():
    db.init_db()
    logger.info("Starting US stock universe refresh (S&P 500 + broader listings)...")

    failed  = []
    total   = 0
    success = 0
    log_id  = db.log_refresh_start()
    db.mark_all_tickers_inactive()

    try:
        for event in universe.run_full_refresh():
            etype = event.get("type")
            if etype == "batch":
                rows = event.get("rows", [])
                if rows:
                    db.upsert_tickers(rows)
            elif etype == "progress":
                logger.info(
                    "[%d/%d] %s — %s | %s",
                    event["current"], event.get("total", "?"),
                    event.get("symbol", ""),
                    event.get("status", ""),
                    event.get("basic_industry", ""),
                )
            elif etype == "status":
                logger.info(event.get("message", ""))
            elif etype == "done":
                total   = event.get("total", 0)
                success = event.get("success", 0)
                failed  = event.get("failed", [])
            elif etype == "error":
                logger.error("Refresh error: %s", event.get("message"))
    except Exception as e:
        logger.exception("Ticker refresh crashed: %s", e)
        db.log_refresh_finish(log_id, total, success, failed)
        sys.exit(1)

    db.log_refresh_finish(log_id, total, success, failed)
    logger.info("Done. total=%d  success=%d  failed=%d", total, success, len(failed))
    if failed:
        logger.warning("Failed symbols: %s", failed)

    # Rebuild sector_index_map after every ticker refresh so new stocks get mapped
    # and delisted stocks get pruned automatically.
    logger.info("Rebuilding sector index map...")
    try:
        conn = db.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT symbol, sector, industry FROM tickers WHERE is_active=1")
                tickers = [{"symbol": r[0], "sector": r[1], "industry": r[2]} for r in cur.fetchall()]
        finally:
            db.release_conn(conn)
        db.clear_sector_index_map()
        rows = sector_etf_map.build_sector_index_map_rows(tickers)
        rows += industry_etf_map.build_industry_index_map_rows(tickers)
        db.upsert_sector_index_map(rows)
        unclassified = [t["symbol"] for t in tickers if t["symbol"] not in {r[0] for r in rows}]
        logger.info("Sector map rebuilt: %d stocks -> %d rows, %d unclassified",
                    len(tickers), len(rows), len(unclassified))
        if unclassified:
            logger.warning("Unclassified stocks: %s", unclassified)
    except Exception as e:
        logger.exception("Sector map rebuild failed: %s", e)

    sys.exit(0 if not failed else 1)


if __name__ == "__main__":
    main()
