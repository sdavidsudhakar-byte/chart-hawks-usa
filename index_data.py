"""
index_data.py — US sector/broad-market/industry index master list and
yfinance OHLCV fetcher.

Replaces the ~80-index Nifty master list with a 4-tier US structure:
  BROAD MARKET — 4 broad-market benchmark ETFs
  SECTORAL     — 11 GICS Sector SPDR ETFs (sector_etf_map.py)
  BREADTH      — equal-weight counterpart of each broad-market/sector ETF,
                 for cap-weight-vs-equal-weight narrow-leadership analysis
                 (sector_etf_map.EQUAL_WEIGHT_ETF)
  INDUSTRY     — real liquid industry/thematic ETFs, one level deeper. Two
                 sources merged into one tab/peer-ranking pool:
                   - industry_etf_map.py: each entry has real mapped stock
                     backing in the tickers table (feeds the Hawks Index
                     stock filter via sector_index_map)
                   - thematic_etf_map.py: broad ETF discovery with NO stock-
                     backing requirement (doesn't feed the stock filter)
                 Both rank together as one combined peer group in rs.py.

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
import thematic_etf_map

logger = logging.getLogger(__name__)


class IndexInfo(NamedTuple):
    symbol:      str          # ETF ticker, e.g. "XLK"
    name:        str          # Display name — NOT ticker-prefixed: this is the
                               # same string sector_etf_map/industry_etf_map use
                               # as index_name in sector_index_map, matched 1:1
                               # against hawks_mappings by the Hawks Index filter
                               # (templates/index.html ~6325) — renaming it here
                               # would silently break that filter for every stock.
                               # Ticker-prefixed display ("XLK - ...") is done at
                               # render time in the UI instead (see renderSwingRadar).
    category:    str          # BROAD MARKET | SECTORAL | BREADTH | INDUSTRY
    pair_symbol: str | None = None  # cap-weight <-> equal-weight counterpart ticker


# Cap-weight symbol -> plain description, needed to build the BREADTH tier's
# names ("<description> (Equal Weight)") and to backfill pair_symbol onto the
# cap-weight rows.
_CW_DESC_BY_SYMBOL: dict[str, str] = {
    **{sym: name for name, sym in sector_etf_map.BROAD_MARKET_ETFS.items()},
    **{sym: sector for sector, sym in sector_etf_map.SECTOR_ETF.items()},
}
_EW_PAIR = sector_etf_map.EQUAL_WEIGHT_ETF          # {cw_sym: ew_sym}
_CW_PAIR = {ew: cw for cw, ew in _EW_PAIR.items()}  # {ew_sym: cw_sym}

ALL_INDICES: list[IndexInfo] = (
    [IndexInfo(sym, name, "BROAD MARKET", _EW_PAIR.get(sym)) for name, sym in sector_etf_map.BROAD_MARKET_ETFS.items()]
    + [IndexInfo(sym, sector, "SECTORAL", _EW_PAIR.get(sym)) for sector, sym in sector_etf_map.SECTOR_ETF.items()]
    + [IndexInfo(ew_sym, f"{_CW_DESC_BY_SYMBOL[cw_sym]} (Equal Weight)", "BREADTH", cw_sym)
       for cw_sym, ew_sym in _EW_PAIR.items()]
    + [IndexInfo(sym, name, "INDUSTRY") for name, sym in industry_etf_map.INDUSTRY_ETF.items()]
    + [IndexInfo(sym, name, "INDUSTRY") for name, sym in thematic_etf_map.THEMATIC_ETF.items()]
)

SYMBOLS: list[str] = [idx.symbol for idx in ALL_INDICES]
_SYMBOL_MAP: dict[str, IndexInfo] = {idx.symbol: idx for idx in ALL_INDICES}

CATEGORY_ORDER = ["BROAD MARKET", "SECTORAL", "BREADTH", "INDUSTRY"]


def by_category() -> dict[str, list[IndexInfo]]:
    result: dict[str, list[IndexInfo]] = {cat: [] for cat in CATEGORY_ORDER}
    for idx in ALL_INDICES:
        result[idx.category].append(idx)
    return result


def get_info(symbol: str) -> IndexInfo | None:
    return _SYMBOL_MAP.get(symbol)


# ── Fetch windows ─────────────────────────────────────────────────────────────

HISTORY_DAYS = 366


def run_index_refresh() -> Generator[dict, None, None]:
    """
    Generator that fetches daily OHLCV for all sector/broad-market ETFs.
    Weekly/monthly bars are resampled from this daily data, not fetched
    separately; no intraday fetch (retired along with the hourly scanner).

    Yields event dicts:
      {"type": "status",   "message": str}
      {"type": "progress", "current": int, "total": int, "symbol": str,
                           "name": str, "status": "ok"|"fail", "candles": int}
      {"type": "done",     "total": int, "success": int, "failed": list[str]}
    """
    yield {"type": "status", "message": "Starting index price refresh..."}

    limiter = market_data._RateLimiter(market_data.RATE_LIMIT_RPS)

    today     = market_data._last_trading_day()
    today_str = today.isoformat()
    from_date = (today - datetime.timedelta(days=HISTORY_DAYS)).isoformat()

    total = len(SYMBOLS)
    success = 0
    failed: list[str] = []

    yield {"type": "status", "message": f"Fetching daily OHLCV for {total} indices (as of {today_str})..."}

    for i, sym in enumerate(SYMBOLS, 1):
        info = _SYMBOL_MAP[sym]
        daily_candles = 0

        try:
            rows = market_data.fetch_daily(limiter, sym, from_date, today_str)
            if rows:
                db.upsert_index_prices(rows)
                daily_candles = len(rows)

            success += 1
            yield {
                "type": "progress", "current": i, "total": total,
                "symbol": sym, "name": info.name, "status": "ok",
                "candles": daily_candles,
            }
        except Exception as e:
            logger.warning("Failed to fetch %s: %s", sym, e)
            failed.append(sym)
            yield {
                "type": "progress", "current": i, "total": total,
                "symbol": sym, "name": info.name, "status": "fail",
                "candles": 0,
            }

    yield {
        "type":    "done",
        "total":   total,
        "success": success,
        "failed":  failed,
    }
