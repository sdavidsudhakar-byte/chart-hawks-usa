"""
thematic_etf_map.py — broad US sector/industry/thematic ETF coverage for
Sector Lens, with NO stock-backing requirement.

Unlike sector_etf_map.py (11 GICS sectors, exactly 1:1 with stocks) and
industry_etf_map.py (~27 clusters, each requiring real mapped stocks from the
tickers table), every entry here is included purely because it is a real,
liquid-enough, distinctly-themed US equity ETF — whether or not any stock in
the live ticker universe is individually tagged to it. This is the
"Thematic" tab in Sector Lens: broad discovery of real indices/ETFs, not a
stock-filter mechanism (these rows carry no sector_index_map / hawks_mappings
entries, so they don't appear in the Hawks Index stock filter).

Every ticker below was checked live against yfinance (resolves, has >=1y
daily history) before being added — never from memory. Where multiple funds
track essentially the same theme, only the more liquid/representative one is
kept (same one-ETF-per-theme discipline as industry_etf_map.py), unless two
funds trade genuinely different baskets.

Public API:
  THEMATIC_ETF — {display name: ETF ticker}
"""

THEMATIC_ETF: dict[str, str] = {
    # ── Technology ──
    "Cloud Computing":        "SKYY",  # $3.3B AUM — vs WCLD ($282M, pure small-cap SaaS)
    "AI & Robotics":          "BOTZ",  # $3.3B AUM — vs ROBO ($2.0B)
    "Internet of Things":     "SNSR",  # $212M AUM
    "Fintech":                "FINX",  # $155M AUM — vs IPAY ($146M, near-duplicate)
    "5G Infrastructure":      "NXTG",  # $563M AUM
    "Blockchain":             "BLOK",  # $1.1B AUM
    "Video Gaming & Esports": "ESPO",  # $247M AUM — vs HERO ($62M)
    "Social Media":           "SOCL",  # $85M AUM
    "3D Printing":            "PRNT",  # $61M AUM
    "Quantum Computing":      "QTUM",  # $5.6B AUM
    "Metaverse":              "METV",  # $275M AUM — vs CLOU ($375M, near-
                                        # duplicate of existing Cloud
                                        # Computing/SKYY, skipped) and XT
                                        # ($4.0B, too broad/overlaps multiple
                                        # existing themes at once, skipped)

    # ── Health Care ──
    "Genomics & Biotech Innovation": "ARKG",  # $2.2B AUM

    # ── Materials / Energy ──
    "Lithium & Battery Tech":        "LIT",   # $1.4B AUM
    "Solar":                         "TAN",   # $900M AUM — distinct pure-play
                                               # from the existing broader
                                               # "Clean Energy & Solar"/ICLN
    "Copper Miners":                 "COPX",  # $7.3B AUM
    "Timber & Forestry":             "WOOD",  # $258M AUM — vs CUT ($30M)
    "Rare Earth & Strategic Metals": "REMX",  # $1.8B AUM
    "Nuclear Energy":                "NLR",   # $3.5B AUM — nuclear utilities/
                                               # generation, distinct basket
                                               # from the existing pure
                                               # uranium-miner URA entry
    "Steel":                "SLX",   # $161M AUM — distinct from the broad
                                      # "Metals & Mining" cluster (XME)
    "Gold Junior Miners":   "GDXJ",  # $8.4B AUM — smaller/earlier-stage
                                      # explorers, distinct risk/size profile
                                      # from the existing majors-only "Gold
                                      # Miners" (GDX) entry
    "Silver Miners":        "SIL",   # $4.4B AUM — no dedicated silver-miner
                                      # entry existed before (Silver was
                                      # folded into the broad Metals & Mining
                                      # cluster)
    "Silver Junior Miners": "SILJ",  # $3.6B AUM — same majors/juniors split
                                      # rationale as Gold above
    "Energy Infrastructure (MLPs)": "AMLP",  # $12.6B AUM — vs MLPX ($3.4B,
                                              # broader/overlapping), distinct
                                              # midstream-MLP structure from
                                              # the existing upstream-focused
                                              # "Oil & Gas" cluster (XOP)
    "Natural Gas":           "FCG",   # $638M AUM — pure gas-focused, distinct
                                       # from the broader Oil & Gas cluster

    # ── Real Estate ──
    "Data Center Real Estate": "SRVR",  # $320M AUM

    # ── Consumer / Industrials ──
    "Airlines & Travel":  "JETS",  # $838M AUM
    "Travel Technology":  "AWAY",  # $23M AUM — booking/travel-tech platforms,
                                    # distinct from JETS' airlines/aerospace basket
    "Cannabis":           "MSOS",  # $896M AUM — vs MJ ($104M, broader/global)
    "Pet Care":           "PAWZ",  # $31M AUM
    "Space & Defense Innovation": "ARKX",  # $737M AUM — commercial space/
                                            # innovation, distinct from the
                                            # existing traditional defense
                                            # primes basket (Aerospace &
                                            # Defense / ITA); vs UFO ($536M,
                                            # near-duplicate pure-space play)

    # ── Financials ──
    "Business Development Companies": "BIZD",  # $1.5B AUM
    "Private Equity":                 "PSP",   # $214M AUM

    # ── Industrials ──
    "Smart Grid Infrastructure": "GRID",  # $11.9B AUM — electric grid
                                           # modernization, distinct from the
                                           # existing broad Infrastructure &
                                           # Machinery (PAVE) and Clean
                                           # Energy & Solar (ICLN) clusters
    "US Industrial Renaissance": "AIRR",  # $8.9B AUM — domestic reshoring
                                           # theme (small/mid-cap industrials
                                           # + regional banks), distinct angle
                                           # from the broad Infrastructure &
                                           # Machinery cluster
}
