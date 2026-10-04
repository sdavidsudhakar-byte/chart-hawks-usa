"""
run_flash_refresh.py — Fast INCREMENTAL data fetch for GitHub Actions.

The "Flash Price Refresh" CLI companion to run_price_refresh.py, for the
daily / market-hours cadence. Same 6-step sequence, but:

  1. Stock daily OHLCV   — INCREMENTAL: only new bars/symbol (+5-day overlap)
  2. Stock 60M intraday  — 100 days, all symbols (unchanged)
  3. Index daily + 60M   — full index refresh (81 indices; cheap) — NO purge
  4. IBD RS compute + 5-day backfill (narrowed from 30)
  5. Stock RS compute (only missing dates)
  6. Summary email

Never purges history. Writes merge by (symbol,date)/(symbol,ts), so this is
safe to run alongside/between the weekend full run_price_refresh.py, which
remains the ground-truth resync.
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
import flash_prices as flash_prices_module
import index_data as index_data_module
import rs as rs_module
import stock_rs as stock_rs_module
import notifier

_RS_BACKFILL_DAYS = 5  # incremental window for Flash


def main():
    db.init_db()

    # ── 1 + 2: Stock daily (incremental) + 60M intraday ──────────────────────
    logger.info("Starting FLASH stock price refresh (incremental daily + 60M)...")
    price_success = 0
    price_failed  = []

    try:
        for event in flash_prices_module.run_flash_refresh():
            etype = event.get("type")
            if etype == "price_batch":
                rows = event.get("rows", [])
                if rows:
                    db.upsert_ohlcv_daily(rows)
            elif etype == "intraday_batch":
                rows = event.get("rows", [])
                if rows:
                    db.upsert_ohlcv_intraday(rows)
            elif etype == "price_progress":
                if event.get("current", 0) % 200 == 0:
                    logger.info("[%d/%d] %s — %d daily bars, %d intraday bars (%s)",
                                event["current"], event.get("total", "?"),
                                event.get("symbol", ""), event.get("bars", 0),
                                event.get("intraday_bars", 0), event.get("status", ""))
            elif etype == "price_done":
                price_success = event.get("success", 0)
                price_failed  = event.get("failed", [])
            elif etype == "status":
                logger.info(event.get("message", ""))
            elif etype == "error":
                logger.error("Flash stock price refresh error: %s", event.get("message"))
                sys.exit(1)
    except Exception as e:
        logger.exception("Flash stock price refresh crashed: %s", e)
        sys.exit(1)

    logger.info("Flash stock price refresh done. success=%d failed=%d", price_success, len(price_failed))
    if price_failed:
        logger.warning("Failed symbols: %s", price_failed[:20])

    # ── 3: Index daily + 60M intraday ────────────────────────────────────────
    logger.info("Starting index price refresh (daily + 60M)...")
    index_success = 0
    index_failed  = []

    try:
        for event in index_data_module.run_index_refresh():
            etype = event.get("type")
            if etype == "progress":
                logger.info("[%d/%d] %-45s daily=%d 60M=%d (%s)",
                            event.get("current", 0), event.get("total", 0),
                            event.get("symbol", ""), event.get("candles", 0),
                            event.get("intraday_candles", 0), event.get("status", ""))
            elif etype == "done":
                index_success = event.get("success", 0)
                index_failed  = event.get("failed", [])
            elif etype == "status":
                logger.info(event.get("message", ""))
            elif etype == "error":
                logger.error("Index refresh error: %s", event.get("message"))
    except Exception as e:
        logger.exception("Index refresh crashed: %s", e)

    logger.info("Index refresh done. success=%d failed=%d", index_success, len(index_failed))
    if index_failed:
        logger.warning("Failed index symbols: %s", index_failed)

    # ── 4: IBD RS compute + incremental backfill ─────────────────────────────
    logger.info("Computing IBD RS ratings...")
    try:
        result = rs_module.compute_and_store_rs()
        if result.get("ok"):
            logger.info("IBD RS done: %d rated, %d skipped", result["rated"], result["skipped"])
        else:
            logger.warning("IBD RS skipped: %s", result.get("error"))
    except Exception as e:
        logger.exception("IBD RS computation failed: %s", e)

    logger.info("Backfilling historical RS (last %d dates)...", _RS_BACKFILL_DAYS)
    try:
        bf = rs_module.backfill_rs(days=_RS_BACKFILL_DAYS)
        logger.info("Backfill done: computed=%d failed=%d", bf.get("computed", 0), len(bf.get("failed", [])))
    except Exception as e:
        logger.exception("RS backfill failed: %s", e)

    logger.info("Backfilling index mid/short scores (last %d dates)...", _RS_BACKFILL_DAYS)
    try:
        ms = rs_module.backfill_index_mid_short(days=_RS_BACKFILL_DAYS)
        logger.info("Mid/short backfill done: %d rows upserted", ms.get("upserted", 0))
    except Exception as e:
        logger.exception("Index mid/short backfill failed: %s", e)

    # ── 5: Stock RS compute (only missing dates) ─────────────────────────────
    logger.info("Computing stock RS (RS21/RS55/RS252 ranks)...")
    try:
        sr_dates = stock_rs_module.get_dates_to_compute(backfill_days=_RS_BACKFILL_DAYS)
        if sr_dates:
            sr_result = stock_rs_module.compute_and_store(sr_dates)
            logger.info("Stock RS done: computed=%d skipped=%d failed=%s",
                        sr_result["computed"], sr_result["skipped"], sr_result["failed"])
        else:
            logger.info("Stock RS: nothing to compute, all dates up to date.")
    except Exception as e:
        logger.exception("Stock RS computation failed: %s", e)

    # ── 6: Email summary ──────────────────────────────────────────────────────
    logger.info("Sending Flash summary email...")
    try:
        notifier.send_summary(
            price_total=price_success + len(price_failed),
            price_success=price_success,
            price_failed=price_failed,
            scanner_result=None,
        )
    except Exception as e:
        logger.exception("Email notification failed: %s", e)

    sys.exit(0)


if __name__ == "__main__":
    main()
