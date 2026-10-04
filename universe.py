"""
universe.py — US equity universe + GICS-style classification.
Replaces nse.py. Builds the ticker master list from:
  1. S&P 500 constituents (Wikipedia) — gives real GICS Sector + Sub-Industry.
  2. Nasdaq Trader symbol directories (nasdaqlisted.txt / otherlisted.txt) —
     broadens the universe beyond the S&P 500.
  3. yfinance Ticker.info sector/industry — fallback classification for any
     symbol not covered by the Wikipedia S&P 500 table.

The 4-slot classification shape (macro/sector/industry/basic_industry) from
the old NSE schema is kept so the existing filter-tree UI/API need no change:
  - S&P 500 names: macro=sector=GICS Sector, industry=basic_industry=GICS Sub-Industry
  - Everything else: macro=sector=Yahoo sector, industry=basic_industry=Yahoo industry
"""

import io
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Generator

import pandas as pd
import requests
import yfinance as yf

logger = logging.getLogger(__name__)

UNCATEGORIZED = "Uncategorized"

SP500_WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED_URL  = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}


def _clean_symbol(sym: str) -> str:
    """Nasdaq directory files use '.' for share classes (BRK.B); yfinance wants '-'."""
    return sym.strip().upper().replace(".", "-")


# ── step 1: S&P 500 constituents with real GICS sector/sub-industry ─────────

def fetch_sp500_constituents() -> dict[str, dict]:
    """Returns {symbol: {company_name, macro, sector, industry, basic_industry}}."""
    resp = requests.get(SP500_WIKI_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    tables = pd.read_html(io.StringIO(resp.text))
    table = tables[0]
    out = {}
    for _, row in table.iterrows():
        symbol = _clean_symbol(str(row["Symbol"]))
        sector = str(row.get("GICS Sector", "")).strip() or UNCATEGORIZED
        sub_industry = str(row.get("GICS Sub-Industry", "")).strip() or UNCATEGORIZED
        out[symbol] = {
            "company_name":   str(row.get("Security", "")).strip(),
            "macro":          sector,
            "sector":         sector,
            "industry":       sub_industry,
            "basic_industry": sub_industry,
        }
    return out


# ── step 2: broader Nasdaq/NYSE/AMEX-listed symbol universe ─────────────────

def fetch_listed_symbols() -> list[dict]:
    """
    Downloads the Nasdaq Trader symbol directories and returns common-stock
    symbols only (excludes ETFs, test issues, warrants, units, rights).
    Each dict: {symbol, company_name}
    """
    out: list[dict] = []
    seen: set[str] = set()

    resp = requests.get(NASDAQ_LISTED_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.text), sep="|")
    df = df[df["Test Issue"] == "N"]
    df = df[df["ETF"] == "N"]
    for _, row in df.iterrows():
        sym = str(row.get("Symbol", "")).strip()
        if not sym or "$" in sym or sym in seen or str(row.get("Symbol", "")) == "File Creation Time":
            continue
        seen.add(sym)
        out.append({"symbol": _clean_symbol(sym), "company_name": str(row.get("Security Name", "")).strip()})

    resp = requests.get(OTHER_LISTED_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.text), sep="|")
    df = df[df["Test Issue"] == "N"]
    if "ETF" in df.columns:
        df = df[df["ETF"] == "N"]
    for _, row in df.iterrows():
        sym = str(row.get("ACT Symbol", "")).strip()
        if not sym or "$" in sym or sym in seen or sym == "File Creation Time":
            continue
        seen.add(sym)
        out.append({"symbol": _clean_symbol(sym), "company_name": str(row.get("Security Name", "")).strip()})

    return out


# ── step 3: yfinance fallback classification for non-S&P500 symbols ─────────

def fetch_yfinance_classification(symbol: str, retries: int = 2) -> dict:
    """Returns {macro, sector, industry, basic_industry, company_name}, falling
    back to UNCATEGORIZED on any failure."""
    for attempt in range(1, retries + 1):
        try:
            info = yf.Ticker(symbol).get_info()
            sector = (info.get("sector") or "").strip() or UNCATEGORIZED
            industry = (info.get("industry") or "").strip() or UNCATEGORIZED
            name = (info.get("longName") or info.get("shortName") or "").strip()
            return {
                "macro": sector, "sector": sector,
                "industry": industry, "basic_industry": industry,
                "company_name": name,
            }
        except Exception as e:
            logger.warning("%s: yfinance info fetch failed (attempt %s): %s", symbol, attempt, e)
            time.sleep(1)
    return {
        "macro": UNCATEGORIZED, "sector": UNCATEGORIZED,
        "industry": UNCATEGORIZED, "basic_industry": UNCATEGORIZED,
        "company_name": "",
    }


# ── step 4: full universe refresh generator (mirrors nse.run_full_refresh) ──

def run_full_refresh() -> Generator[dict, None, None]:
    """
    Event types (same shape as the old nse.run_full_refresh):
      {"type": "status",   "message": str}
      {"type": "progress", "current": int, "total": int, "symbol": str,
                           "status": "ok"|"fail", "basic_industry": str}
      {"type": "batch",    "rows": list[dict]}
      {"type": "done",     "total": int, "success": int, "failed": list[str]}
      {"type": "error",    "message": str}
    """
    yield {"type": "status", "message": "Downloading S&P 500 constituent list (Wikipedia)..."}
    try:
        sp500 = fetch_sp500_constituents()
    except Exception as e:
        yield {"type": "error", "message": f"Failed to download S&P 500 list: {e}"}
        return

    yield {"type": "status", "message": f"Found {len(sp500)} S&P 500 names. Downloading Nasdaq/NYSE listed symbols..."}
    try:
        listed = fetch_listed_symbols()
    except Exception as e:
        yield {"type": "error", "message": f"Failed to download listed-symbol directory: {e}"}
        return

    # Merge: start from the broader listed universe, S&P 500 classification wins where known.
    by_symbol: dict[str, dict] = {}
    for row in listed:
        by_symbol[row["symbol"]] = {"symbol": row["symbol"], "company_name": row["company_name"]}
    for sym, info in sp500.items():
        by_symbol.setdefault(sym, {"symbol": sym, "company_name": info["company_name"]})

    total = len(by_symbol)
    yield {"type": "status", "message": f"Classifying {total} symbols..."}

    failed: list[str] = []
    success = 0
    batch: list[dict] = []
    idx = 0

    def _row_for(symbol: str, base: dict, info: dict, ok: bool) -> dict:
        return {
            "symbol":         symbol,
            "company_name":   info.get("company_name") or base.get("company_name", ""),
            "macro":          info["macro"],
            "sector":         info["sector"],
            "industry":       info["industry"],
            "basic_industry": info["basic_industry"],
            "index_list":     ["S&P 500"] if symbol in sp500 else [],
        }

    # S&P 500 names: classification already known, no network call needed.
    for symbol, base in by_symbol.items():
        if symbol not in sp500:
            continue
        idx += 1
        info = sp500[symbol]
        success += 1
        batch.append(_row_for(symbol, base, info, True))
        yield {"type": "progress", "current": idx, "total": total, "symbol": symbol,
               "status": "ok", "basic_industry": info["basic_industry"]}
        if len(batch) >= 20:
            yield {"type": "batch", "rows": batch}
            batch = []

    # Everything else: parallel yfinance classification lookups (slow — this is
    # the known cost of a broader-than-S&P500 universe over an unofficial API).
    non_sp500 = [(s, b) for s, b in by_symbol.items() if s not in sp500]
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(fetch_yfinance_classification, s): (s, b) for s, b in non_sp500}
        for fut in as_completed(futures):
            symbol, base = futures[fut]
            idx += 1
            info = fut.result()
            ok = info["sector"] != UNCATEGORIZED
            if ok:
                success += 1
            else:
                failed.append(symbol)
            batch.append(_row_for(symbol, base, info, ok))
            yield {"type": "progress", "current": idx, "total": total, "symbol": symbol,
                   "status": "ok" if ok else "fail", "basic_industry": info["basic_industry"]}
            if len(batch) >= 20:
                yield {"type": "batch", "rows": batch}
                batch = []

    if batch:
        yield {"type": "batch", "rows": batch}

    yield {
        "type":    "done",
        "total":   total,
        "success": success,
        "failed":  failed,
    }
