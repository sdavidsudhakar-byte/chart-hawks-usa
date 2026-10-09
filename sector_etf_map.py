"""
sector_etf_map.py — GICS sector → Sector SPDR ETF mapping.
Replaces build_sector_map.py / sector_index_map.csv.

NSE's "Sector Lens" mapped each stock to one or more of ~80 Nifty thematic
indices with a fuzzy confidence score (many-to-many, since a stock can belong
to several thematic baskets). The US equivalent is a clean 1:1 categorical
join: every stock has exactly one GICS Sector, and every GICS Sector maps to
exactly one Sector SPDR ETF — so no confidence-scoring mechanism is needed.

Public API:
  SECTOR_ETF        — {GICS Sector name: ETF ticker}
  BROAD_MARKET_ETFS — {display name: ETF ticker}
  EQUAL_WEIGHT_ETF   — {cap-weight ETF ticker: equal-weight counterpart ticker}
  build_sector_index_map_rows(tickers) — rows for db.upsert_sector_index_map
"""

# 11 GICS sectors → their Sector SPDR ETF
SECTOR_ETF: dict[str, str] = {
    "Energy":                   "XLE",
    "Materials":                "XLB",
    "Industrials":              "XLI",
    "Consumer Discretionary":   "XLY",
    "Consumer Staples":         "XLP",
    "Health Care":              "XLV",
    "Financials":               "XLF",
    "Information Technology":   "XLK",
    "Communication Services":   "XLC",
    "Utilities":                "XLU",
    "Real Estate":              "XLRE",
}

# Broad-market benchmark ETFs shown alongside the sector ETFs in Sector Lens /
# Swing Radar's "BROAD MARKET" category.
BROAD_MARKET_ETFS: dict[str, str] = {
    "S&P 500":        "SPY",
    "Nasdaq 100":     "QQQ",
    "Dow Jones":      "DIA",
    "Russell 2000":   "IWM",
}

# Equal-weight counterpart for each cap-weighted broad-market/sector ETF above —
# powers Sector Lens' "Breadth" tab (narrow mega-cap-led rally vs broad
# participation, e.g. SPY vs RSP, XLK vs RSPT). Invesco's "S&P 500 Equal Weight"
# ETF family; tickers confirmed live against yfinance (1y history, resolves) —
# not from memory, same discipline as industry_etf_map.py's AUM/volume checks.
EQUAL_WEIGHT_ETF: dict[str, str] = {
    "SPY":  "RSP",    # S&P 500 Equal Weight
    "XLK":  "RSPT",   # Technology EW
    "XLV":  "RSPH",   # Health Care EW
    "XLF":  "RSPF",   # Financials EW
    "XLY":  "RSPD",   # Consumer Discretionary EW
    "XLP":  "RSPS",   # Consumer Staples EW
    "XLI":  "RSPN",   # Industrials EW
    "XLB":  "RSPM",   # Materials EW
    "XLE":  "RSPG",   # Energy EW
    "XLU":  "RSPU",   # Utilities EW
    "XLRE": "RSPR",   # Real Estate EW
    "XLC":  "RSPC",   # Communication Services EW
}

# yfinance's Ticker.info['sector'] uses Yahoo's own sector taxonomy, which
# differs from the official GICS sector names Wikipedia's S&P 500 table uses
# (e.g. "Technology" vs "Information Technology"). Every non-S&P500 stock
# classified via yfinance comes back with these Yahoo names, so they must be
# normalized to the canonical GICS name before the SECTOR_ETF join — applied
# in universe.py at classification time, so tickers.sector is always GICS.
YAHOO_SECTOR_ALIASES: dict[str, str] = {
    "Technology":          "Information Technology",
    "Financial Services":  "Financials",
    "Healthcare":          "Health Care",
    "Consumer Cyclical":   "Consumer Discretionary",
    "Consumer Defensive":  "Consumer Staples",
    "Basic Materials":     "Materials",
}


def normalize_sector(name: str) -> str:
    """Translate a Yahoo-taxonomy sector name to its canonical GICS name.
    Names already in GICS form (or unrecognized) pass through unchanged."""
    return YAHOO_SECTOR_ALIASES.get(name, name)


def build_sector_index_map_rows(tickers: list[dict]) -> list[tuple]:
    """
    tickers: [{symbol, sector, ...}] from the tickers table.
    Returns rows for db.upsert_sector_index_map:
      (symbol, index_name, index_symbol, source, confidence)
    One row per ticker — its GICS sector's ETF. Unmapped/uncategorized
    sectors are skipped (no row), same as the old low-confidence skip.

    source/confidence are set to "official"/"high" — the strongest tier the
    UI recognizes (sort order, confidence-tier checkboxes) — since this is an
    exact 1:1 categorical join, not a fuzzy multi-index match like NSE's.
    """
    rows = []
    for t in tickers:
        sector = t.get("sector")
        etf = SECTOR_ETF.get(sector)
        if not etf:
            continue
        rows.append((t["symbol"], sector, etf, "official", "high"))
    return rows
