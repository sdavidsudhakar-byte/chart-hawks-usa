"""
index_data.py — US sector/broad-market/industry index master list and
yfinance OHLCV fetcher.

Replaces the ~80-index Nifty master list with a 3-tier US structure:
  BROAD MARKET — 4 broad-market benchmark ETFs
  SECTORAL     — 11 GICS Sector SPDR ETFs (sector_etf_map.py)
  INDUSTRY     — ~24 real liquid industry ETFs, one level deeper
                 (industry_etf_map.py)

Public API:
  ALL_INDICES      — list of IndexInfo namedtuples
  SYMBOLS          — list of all ticker symbols
  by_category()    — dict grouped by category
  run_index_refresh() — generator: fetches/updates OHLCV for all indices
"""

import datetime
import logging
import time
from typing import Generator, NamedTuple

import db
import market_data
import sector_etf_map
import industry_etf_map

logger = logging.getLogger(__name__)


class IndexInfo(NamedTuple):
    symbol:   str    # ETF ticker, e.g. "XLK"
    name:     str    # Display name
    category: str    # BROAD MARKET | SECTORAL | INDUSTRY


ALL_INDICES: list[IndexInfo] = (
    [IndexInfo(sym, name, "BROAD MARKET") for name, sym in sector_etf_map.BROAD_MARKET_ETFS.items()]
    + [IndexInfo(sym, sector, "SECTORAL") for sector, sym in sector_etf_map.SECTOR_ETF.items()]
    + [IndexInfo(sym, name, "INDUSTRY") for name, sym in industry_etf_map.INDUSTRY_ETF.items()]
)

SYMBOLS: list[str] = [idx.symbol for idx in ALL_INDICES]
_SYMBOL_MAP: dict[str, IndexInfo] = {idx.symbol: idx for idx in ALL_INDICES}

CATEGORY_ORDER = ["BROAD MARKET", "SECTORAL", "INDUSTRY"]


def by_category() -> dict[str, list[IndexInfo]]:
    result: dict[str, list[IndexInfo]] = {cat: [] for cat in CATEGORY_ORDER}
    for idx in ALL_INDICES:
        result[idx.category].append(idx)
    return result


def get_info(symbol: str) -> IndexInfo | None:
    return _SYMBOL_MAP.get(symbol)


# ── Fetch windows ─────────────────────────────────────────────────────────────

HISTORY_DAYS = 366
INTRADAY_DAYS = 100


def run_index_refresh() -> Generator[dict, None, None]:
    """
    Generator that fetches daily + 60m OHLCV for all sector/broad-market ETFs.

    Yields event dicts:
      {"type": "status",   "message": str}
      {"type": "progress", "current": int, "total": int, "symbol": str,
                           "name": str, "status": "ok"|"fail",
                           "candles": int, "intraday_candles": int}
      {"type": "done",     "total": int, "success": int, "failed": list[str]}
    """
    yield {"type": "status", "message": "Starting index price refresh (daily + 60m)..."}

    limiter = market_data._RateLimiter(market_data.RATE_LIMIT_RPS)

    today         = market_data._last_trading_day()
    today_str     = today.isoformat()
    from_date     = (today - datetime.timedelta(days=HISTORY_DAYS)).isoformat()
    intraday_from = (today - datetime.timedelta(days=INTRADAY_DAYS)).isoformat()

    total = len(SYMBOLS)
    success = 0
    failed: list[str] = []

    yield {"type": "status", "message": f"Fetching daily + 60m OHLCV for {total} indices (as of {today_str})..."}

    for i, sym in enumerate(SYMBOLS, 1):
        info = _SYMBOL_MAP[sym]
        daily_candles = 0
        intraday_candles = 0

        try:
            rows = market_data.fetch_daily(limiter, sym, from_date, today_str)
            if rows:
                db.upsert_index_prices(rows)
                daily_candles = len(rows)

            irows = market_data.fetch_intraday_60m(limiter, sym, intraday_from, today_str)
            if irows:
                db.upsert_ohlcv_intraday(irows)
                intraday_candles = len(irows)

            success += 1
            yield {
                "type": "progress", "current": i, "total": total,
                "symbol": sym, "name": info.name, "status": "ok",
                "candles": daily_candles, "intraday_candles": intraday_candles,
            }
        except Exception as e:
            logger.warning("Failed to fetch %s: %s", sym, e)
            failed.append(sym)
            yield {
                "type": "progress", "current": i, "total": total,
                "symbol": sym, "name": info.name, "status": "fail",
                "candles": 0, "intraday_candles": 0,
            }

    yield {
        "type":    "done",
        "total":   total,
        "success": success,
        "failed":  failed,
    }
