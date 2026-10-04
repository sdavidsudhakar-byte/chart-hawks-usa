"""
run_stock_rs.py — Compute and cache stock RS21, UDVR-21, OBV scores.

Scheduled: every weekday (Mon-Fri) before 9:30 AM ET market open.
Can also be run manually: python run_stock_rs.py

Flags:
  --force         Recompute last 30 dates regardless of what's in DB
  --full-history  Compute RS for ALL historical trading dates in ohlcv_daily

Sequence:
  1. Determine which trading dates need computing
  2. For each date: compute RS21/RS55/RS252 ranks for all ~2100 stocks
  3. Upsert into stock_rs_daily
  4. Purge rows older than 2400 days (preserves full 6.5yr history)
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
import stock_rs


def main():
    force        = "--force" in sys.argv
    full_history = "--full-history" in sys.argv
    db.init_db()

    logger.info("stock_rs_runner: determining dates to compute...")
    try:
        if full_history:
            dates = stock_rs.get_all_historical_dates()
            logger.info("stock_rs_runner: --full-history mode, computing %d dates", len(dates))
        elif force:
            dates = stock_rs.get_all_recent_dates(backfill_days=30)
            logger.info("stock_rs_runner: --force mode, recomputing %d dates", len(dates))
        else:
            dates = stock_rs.get_dates_to_compute(backfill_days=30)
    except Exception as e:
        logger.exception("Failed to determine compute dates: %s", e)
        sys.exit(1)

    if not dates:
        logger.info("stock_rs_runner: nothing to compute, all dates up to date.")
        sys.exit(0)

    logger.info("stock_rs_runner: computing %d date(s): %s", len(dates), dates)

    try:
        result = stock_rs.compute_and_store(dates)
        logger.info(
            "stock_rs_runner: done. computed=%d skipped=%d failed=%s",
            result["computed"], result["skipped"], result["failed"],
        )
    except Exception as e:
        logger.exception("stock_rs compute crashed: %s", e)
        sys.exit(1)

    if result["failed"]:
        logger.warning("stock_rs_runner: failed dates: %s", result["failed"])

    # purge_old_stock_rs disabled until project matures
    # db.purge_old_stock_rs(keep_days=2400)

    sys.exit(0)


if __name__ == "__main__":
    main()
