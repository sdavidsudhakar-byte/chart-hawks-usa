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
