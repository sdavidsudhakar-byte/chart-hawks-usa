"""
run_scan.py — Standalone script for GitHub Actions.
Runs the full EMA scanner after price refresh completes.
Called as a separate step so price refresh and scan are independent.
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
import scanner as scanner_module
import hourly_scanner as hourly_scanner_module
import notifier


def main():
    db.init_db()

    # ── Daily EMA scanner ─────────────────────────────────────────────────────
    logger.info("Starting EMA scanner...")
    scanner_result = None
    try:
        scanner_result = scanner_module.run_full_scan()
        if scanner_result and scanner_result.get("oldest_scan_date"):
            db.set_app_state("oldest_scan_date", scanner_result["oldest_scan_date"])
        logger.info("Scanner done: %s", scanner_result)
    except Exception as e:
        logger.exception("Scanner failed: %s", e)
        sys.exit(1)

    # ── Hourly cycle enrichment ───────────────────────────────────────────────
    logger.info("Starting hourly cycle scan...")
    try:
        hourly_result = hourly_scanner_module.run_hourly_scan()
        logger.info("Hourly cycle scan done: %s", hourly_result)
    except Exception as e:
        logger.exception("Hourly cycle scan failed: %s", e)

    # ── Email summary ─────────────────────────────────────────────────────────
    logger.info("Sending scanner summary email...")
    try:
        notifier.send_summary(
            price_total=0,
            price_success=0,
            price_failed=[],
            scanner_result=scanner_result,
        )
    except Exception as e:
        logger.exception("Email notification failed: %s", e)

    sys.exit(0)


if __name__ == "__main__":
    main()
