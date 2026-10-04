"""
run_index_refresh.py — Standalone script for GitHub Actions.
Fetches sector/broad-market ETF OHLCV data, computes IBD RS ratings, backfills last 30 days.
Scheduled: every weekday after 4:00 PM ET market close.
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
import index_data
import rs


def main():
    db.init_db()

    # ── Index price fetch ─────────────────────────────────────────────────────
    logger.info("Starting index price refresh (%d symbols)...", len(index_data.SYMBOLS))
    total = success = 0
    failed = []

    try:
        for event in index_data.run_index_refresh():
            etype = event.get("type")
            if etype == "progress":
                logger.info("[%d/%d] %s — %d bars (%s)",
                            event.get("current", 0), event.get("total", 0),
                            event.get("symbol", ""), event.get("candles", 0),
                            event.get("status", ""))
            elif etype == "done":
                total   = event.get("total", 0)
                success = event.get("success", 0)
                failed  = event.get("failed", [])
            elif etype == "status":
                logger.info(event.get("message", ""))
            elif etype == "error":
                logger.error("Index refresh error: %s", event.get("message"))
                sys.exit(1)
    except Exception as e:
        logger.exception("Index refresh crashed: %s", e)
        sys.exit(1)

    logger.info("Index fetch done. success=%d failed=%d", success, len(failed))
    if failed:
        logger.warning("Failed symbols: %s", failed)

    # ── IBD RS computation ────────────────────────────────────────────────────
    logger.info("Computing IBD RS ratings for today...")
    try:
        result = rs.compute_and_store_rs()
        if result.get("ok"):
            logger.info("IBD RS done: %d rated, %d skipped", result["rated"], result["skipped"])
        else:
            logger.warning("IBD RS skipped: %s", result.get("error"))
    except Exception as e:
        logger.exception("IBD RS computation failed: %s", e)

    # ── Backfill any missing historical dates ─────────────────────────────────
    # Default 30 for daily runs; set BACKFILL_DAYS=300 for an initial full-history seed
    import os
    backfill_days = int(os.environ.get("BACKFILL_DAYS", "30"))
    logger.info("Backfilling historical RS dates (last %d)...", backfill_days)
    try:
        bf = rs.backfill_rs(days=backfill_days)
        logger.info("Backfill done: computed=%d failed=%d total_eligible=%d",
                    bf.get("computed", 0), len(bf.get("failed", [])), bf.get("total_eligible", 0))
    except Exception as e:
        logger.exception("Backfill failed: %s", e)

    sys.exit(0)


if __name__ == "__main__":
    main()
