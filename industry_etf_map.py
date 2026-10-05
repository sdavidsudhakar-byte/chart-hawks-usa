"""
industry_etf_map.py — GICS/Yahoo industry → real liquid industry ETF mapping.

Sits one level deeper than sector_etf_map.py (11 broad GICS sectors): maps
each stock's `tickers.industry` value to the nearest real, liquid, tradable
industry-themed ETF, giving Sector Lens a third "Industry" tier with genuine
depth instead of just 11 broad buckets.

Design constraint: only ~25 of the ~250+ distinct industry strings observed
in the live US universe have a real liquid ETF behind them (the rest are too
niche for any fund to track). Rather than 1:1 industry->ETF (which would
leave most industries — and therefore most stocks — unmapped), each ETF here
represents a CLUSTER of related industry strings, chosen by checking the
actual distribution of `tickers.industry` values in the live DB (not GICS's
official taxonomy, which doesn't match either Wikipedia's or Yahoo's naming
exactly — see sector_etf_map.normalize_sector for the same class of issue at
the sector level). This trades 100% coverage for an index that is never
empty: every mapped industry ETF is backed by real, actively-updated stock
mappings.

Where two+ ETFs track essentially the same theme (e.g. Biotech: XBI vs IBB,
Oil & Gas: XOP vs OIH), the pick was the more liquid one by real AUM/
average-volume pulled live from yfinance, not a guess or memory — see the
inline comments in INDUSTRY_ETF for the numbers. Every entry here is a
single ETF per theme (no near-duplicate rows for the same underlying
basket) and has a confirmed non-empty stock mapping below — an index with
zero stocks behind it is never shown.

"Shell Companies" (pre-merger SPACs) is intentionally never mapped — it is
not an investable theme, no ETF tracks it, and it would just be empty noise.

Public API:
  INDUSTRY_ETF      — {display name: ETF ticker}
  INDUSTRY_NAME_MAP — {tickers.industry value: display name}
  build_industry_index_map_rows(tickers) — extra rows for db.upsert_sector_index_map
"""

# Display name -> ETF ticker. Display name is what shows up in Sector Lens'
# "Industry" tab and in hawks_mappings — must be stable (used for the
# Stocks Lens Hawks Index filter join, same mechanism as sector_etf_map).
INDUSTRY_ETF: dict[str, str] = {
    # ── Mapped to real stock clusters (industry_etf_map.INDUSTRY_NAME_MAP) ──
    # Ticker picks verified against real AUM/volume on yfinance — where two+
    # ETFs track essentially the same theme, the most liquid one wins.
    "Biotechnology":              "XBI",   # vs IBB ($11.4B vs $10.5B AUM)
    "Pharmaceuticals":            "IHE",   # vs XPH ($1.7B vs $0.6B)
    "Medical Devices & Equipment":"IHI",
    "Health Care Services":       "IHF",   # vs XHS ($1.2B vs $0.2B)
    "Capital Markets & Asset Mgmt":"KCE",  # vs IAI — IAI has more AUM but is
                                            # narrower (broker-dealers only);
                                            # KCE better matches our large
                                            # Asset Management stock cluster
    "Regional Banks":             "KRE",   # vs KBE, IAT ($4.0B vs $1.6B/$0.6B)
    "Insurance":                  "KIE",
    "Software & IT Services":     "IGV",   # vs IGM, XSW
    "Semiconductors":             "SMH",   # vs SOXX, XSD ($67.8B vs $41.8B/$2.6B)
    "Aerospace & Defense":        "ITA",   # vs XAR ($13.6B vs $5.9B)
    "Oil & Gas":                  "XOP",   # one entry, not split by sub-theme —
                                            # XOP ($4.0B) > OIH ($2.0B) AUM
    "Internet & Digital Media":   "FDN",
    "Infrastructure & Machinery": "PAVE",
    "Food & Beverage":            "PBJ",
    "Metals & Mining":            "XME",
    "Gold Miners":                "GDX",   # vs GDXJ (juniors — different tier)
    "Leisure & Restaurants":      "PEJ",
    "Auto":                       "CARZ",
    "Telecom":                    "IYZ",   # vs XTL ($1.2B vs $0.6B)
    "Mortgage REITs":             "REM",
    "Transportation":             "IYT",   # vs XTN ($2.2B vs $0.4B)
    "Retail":                     "XRT",
    "Homebuilders":               "ITB",   # vs XHB ($2.4B vs $1.3B)
    "Clean Energy & Solar":       "ICLN",  # vs PBW ($2-3B typical vs $0.4B)
    "Real Estate":                "VNQ",   # $70.8B AUM — equity REITs across
                                            # all property types (retail/office/
                                            # residential/industrial/healthcare/
                                            # specialty); kept separate from the
                                            # existing Mortgage REITs (REM) entry
    "Uranium":                    "URA",   # $6.4B AUM. Re-added: a 2025 pass
                                            # dropped this for having zero real
                                            # stock backing at the time; a later
                                            # universe refresh picked up real
                                            # uranium-miner tickers.industry
                                            # values, so the ETF now has genuine
                                            # stocks behind it (see below).
}
# Every entry above has a confirmed non-empty stock mapping — see
# INDUSTRY_NAME_MAP below. (Previously this dict also listed MOO/BOTZ/LIT/
# FINX/SNSR as real-but-unmapped candidates for review, since removed per
# decision not to show any index with zero stocks behind it — Uranium/URA
# was in that same batch but has since gained real stock backing, see above.)

# tickers.industry (as actually observed in the live DB) -> INDUSTRY_ETF display name.
# Deliberately many-to-one: related industry strings cluster onto one ETF.
INDUSTRY_NAME_MAP: dict[str, str] = {
    # Biotechnology
    "Biotechnology": "Biotechnology",
    "Diagnostics & Research": "Biotechnology",
    # Pharmaceuticals
    "Drug Manufacturers - Specialty & Generic": "Pharmaceuticals",
    "Drug Manufacturers - General": "Pharmaceuticals",
    # Medical Devices & Equipment
    "Medical Devices": "Medical Devices & Equipment",
    "Medical Instruments & Supplies": "Medical Devices & Equipment",
    "Health Care Equipment": "Medical Devices & Equipment",
    # Health Care Services
    "Medical Care Facilities": "Health Care Services",
    "Health Care Facilities": "Health Care Services",
    "Health Information Services": "Health Care Services",
    "Health Care Technology": "Health Care Services",
    "Health Care Distributors": "Health Care Services",
    # Capital Markets & Asset Mgmt
    "Asset Management": "Capital Markets & Asset Mgmt",
    "Capital Markets": "Capital Markets & Asset Mgmt",
    "Credit Services": "Capital Markets & Asset Mgmt",
    "Insurance Brokers": "Capital Markets & Asset Mgmt",
    "Consumer Finance": "Capital Markets & Asset Mgmt",
    # Regional Banks
    "Banks - Regional": "Regional Banks",
    "Banks - Diversified": "Regional Banks",
    # Insurance
    "Insurance - Property & Casualty": "Insurance",
    "Insurance - Life": "Insurance",
    "Insurance - Specialty": "Insurance",
    "Multi-line Insurance": "Insurance",
    "Reinsurance": "Insurance",
    # Software & IT Services
    "Software - Application": "Software & IT Services",
    "Software - Infrastructure": "Software & IT Services",
    "Application Software": "Software & IT Services",
    "Information Technology Services": "Software & IT Services",
    "IT Consulting & Other Services": "Software & IT Services",
    "Consulting Services": "Software & IT Services",
    "Data Processing & Outsourced Services": "Software & IT Services",
    # Semiconductors
    "Semiconductors": "Semiconductors",
    "Semiconductor Equipment & Materials": "Semiconductors",
    # Aerospace & Defense
    "Aerospace & Defense": "Aerospace & Defense",
    # Oil & Gas — one entry, not split by sub-theme (consistent with every
    # other industry here being a single ETF, not several near-duplicates)
    "Oil & Gas E&P": "Oil & Gas",
    "Oil & Gas Midstream": "Oil & Gas",
    "Oil & Gas Integrated": "Oil & Gas",
    "Oil & Gas Drilling": "Oil & Gas",
    "Oil & Gas Equipment & Services": "Oil & Gas",
    "Oil & Gas Refining & Marketing": "Oil & Gas",
    # Internet & Digital Media
    "Internet Content & Information": "Internet & Digital Media",
    "Internet Retail": "Internet & Digital Media",
    "Internet Services & Infrastructure": "Internet & Digital Media",
    "Electronic Gaming & Multimedia": "Internet & Digital Media",
    "Broadcasting": "Internet & Digital Media",
    "Interactive Media & Services": "Internet & Digital Media",
    "Interactive Home Entertainment": "Internet & Digital Media",
    # Infrastructure & Machinery
    "Specialty Industrial Machinery": "Infrastructure & Machinery",
    "Electrical Equipment & Parts": "Infrastructure & Machinery",
    "Engineering & Construction": "Infrastructure & Machinery",
    "Farm & Heavy Construction Machinery": "Infrastructure & Machinery",
    "Industrial Machinery & Supplies & Components": "Infrastructure & Machinery",
    "Industrial Distribution": "Infrastructure & Machinery",
    "Metal Fabrication": "Infrastructure & Machinery",
    "Construction Machinery & Heavy Transportation Equipment": "Infrastructure & Machinery",
    "Heavy Electrical Equipment": "Infrastructure & Machinery",
    "Construction & Engineering": "Infrastructure & Machinery",
    # Food & Beverage
    "Packaged Foods": "Food & Beverage",
    "Farm Products": "Food & Beverage",
    "Beverages - Non-Alcoholic": "Food & Beverage",
    "Agricultural Products & Services": "Food & Beverage",
    "Beverages - Wineries & Distilleries": "Food & Beverage",
    "Confectioners": "Food & Beverage",
    "Food Distributors": "Food & Beverage",
    "Food Retail": "Food & Beverage",
    # Metals & Mining
    "Other Industrial Metals & Mining": "Metals & Mining",
    "Steel": "Metals & Mining",
    "Other Precious Metals & Mining": "Metals & Mining",
    "Aluminum": "Metals & Mining",
    # Gold Miners
    "Gold": "Gold Miners",
    # Leisure & Restaurants
    "Restaurants": "Leisure & Restaurants",
    "Leisure": "Leisure & Restaurants",
    "Resorts & Casinos": "Leisure & Restaurants",
    "Gambling": "Leisure & Restaurants",
    "Casinos & Gaming": "Leisure & Restaurants",
    # Auto
    "Auto Parts": "Auto",
    "Auto Manufacturers": "Auto",
    "Auto & Truck Dealerships": "Auto",
    "Recreational Vehicles": "Auto",
    "Automotive Retail": "Auto",
    "Automotive Parts & Equipment": "Auto",
    "Automobile Manufacturers": "Auto",
    # Telecom
    "Telecom Services": "Telecom",
    "Wireless Telecommunication Services": "Telecom",
    "Integrated Telecommunication Services": "Telecom",
    # Mortgage REITs
    "REIT - Mortgage": "Mortgage REITs",
    # Transportation
    "Marine Shipping": "Transportation",
    "Integrated Freight & Logistics": "Transportation",
    "Airlines": "Transportation",
    "Passenger Airlines": "Transportation",
    "Trucking": "Transportation",
    "Air Freight & Logistics": "Transportation",
    "Cargo Ground Transportation": "Transportation",
    "Rail Transportation": "Transportation",
    "Passenger Ground Transportation": "Transportation",
    # Retail
    "Apparel Retail": "Retail",
    "Apparel Manufacturing": "Retail",
    "Specialty Retail": "Retail",
    "Furnishings, Fixtures & Appliances": "Retail",
    "Packaging & Containers": "Retail",
    "Apparel, Accessories & Luxury Goods": "Retail",
    "Department Stores": "Retail",
    "Discount Stores": "Retail",
    "Broadline Retail": "Retail",
    "Other Specialty Retail": "Retail",
    "Footwear": "Retail",
    "Homefurnishing Retail": "Retail",
    "Computer & Electronics Retail": "Retail",
    # Homebuilders
    "Building Products & Equipment": "Homebuilders",
    "Residential Construction": "Homebuilders",
    "Homebuilding": "Homebuilders",
    "Building Materials": "Homebuilders",
    "Construction Materials": "Homebuilders",
    # Clean Energy & Solar
    "Utilities - Renewable": "Clean Energy & Solar",
    "Solar": "Clean Energy & Solar",

    # ── Added in a later pass: GICS-dialect synonyms of Yahoo-dialect strings
    # already mapped above (e.g. "Pharmaceuticals" is the GICS name for the
    # exact same theme Yahoo calls "Drug Manufacturers - ..."), plus a few
    # genuinely uncovered clusters. The original pass only checked Yahoo's own
    # taxonomy; S&P 500 names are classified from Wikipedia's official GICS
    # table instead (see universe.py), which uses different strings for the
    # same real-world industries — so any S&P 500 stock whose GICS sub-industry
    # had no Yahoo-dialect twin fell through unmapped (e.g. SanDisk, GICS
    # "Technology Hardware, Storage & Peripherals").
    "Pharmaceuticals": "Pharmaceuticals",
    "Systems Software": "Software & IT Services",
    "Research & Consulting Services": "Software & IT Services",
    "Diversified Banks": "Regional Banks",
    "Property & Casualty Insurance": "Insurance",
    "Life & Health Insurance": "Insurance",
    "Insurance - Reinsurance": "Insurance",
    "Insurance - Diversified": "Insurance",
    "Investment Banking & Brokerage": "Capital Markets & Asset Mgmt",
    "Financial Exchanges & Data": "Capital Markets & Asset Mgmt",
    "Financial Data & Stock Exchanges": "Capital Markets & Asset Mgmt",
    "Financial Conglomerates": "Capital Markets & Asset Mgmt",
    "Transaction & Payment Processing Services": "Capital Markets & Asset Mgmt",
    "Mortgage Finance": "Capital Markets & Asset Mgmt",  # originators/servicers,
                                                          # distinct from the
                                                          # Mortgage REITs entry
    "Managed Health Care": "Health Care Services",
    "Healthcare Plans": "Health Care Services",
    "Medical Distribution": "Health Care Services",
    "Health Care Supplies": "Medical Devices & Equipment",
    "Life Sciences Tools & Services": "Medical Devices & Equipment",
    "Grocery Stores": "Food & Beverage",
    "Food Distribution": "Food & Beverage",
    "Consumer Staples Merchandise Retail": "Food & Beverage",
    "Beverages - Brewers": "Food & Beverage",
    "Soft Drinks & Non-alcoholic Beverages": "Food & Beverage",
    "Packaged Foods & Meats": "Food & Beverage",
    "Distillers & Vintners": "Food & Beverage",
    "Home Improvement Retail": "Retail",
    "Pharmaceutical Retailers": "Retail",
    "Luxury Goods": "Retail",
    "Footwear & Accessories": "Retail",
    "Textile Manufacturing": "Retail",
    "Railroads": "Transportation",
    "Airports & Air Services": "Transportation",
    "Lodging": "Leisure & Restaurants",
    "Hotels, Resorts & Cruise Lines": "Leisure & Restaurants",
    "Travel Services": "Leisure & Restaurants",
    "Oil & Gas Exploration & Production": "Oil & Gas",
    "Oil & Gas Storage & Transportation": "Oil & Gas",
    "Integrated Oil & Gas": "Oil & Gas",
    "Copper": "Metals & Mining",
    "Silver": "Metals & Mining",
    "Building Products": "Homebuilders",
    "Lumber & Wood Production": "Homebuilders",
    "Tools & Accessories": "Infrastructure & Machinery",
    "Electrical Components & Equipment": "Infrastructure & Machinery",
    "Trading Companies & Distributors": "Infrastructure & Machinery",
    "Industrial Conglomerates": "Infrastructure & Machinery",
    "Conglomerates": "Infrastructure & Machinery",
    "Multi-Sector Holdings": "Infrastructure & Machinery",
    "Industrial Gases": "Infrastructure & Machinery",
    "Agricultural & Farm Machinery": "Infrastructure & Machinery",
    "Business Equipment & Supplies": "Infrastructure & Machinery",
    "Security & Protection Services": "Infrastructure & Machinery",
    "Waste Management": "Infrastructure & Machinery",
    "Pollution & Treatment Controls": "Infrastructure & Machinery",
    "Environmental & Facilities Services": "Infrastructure & Machinery",
    "Rental & Leasing Services": "Infrastructure & Machinery",
    "Diversified Support Services": "Infrastructure & Machinery",
    "Advertising Agencies": "Internet & Digital Media",
    "Advertising": "Internet & Digital Media",
    "Entertainment": "Internet & Digital Media",
    "Movies & Entertainment": "Internet & Digital Media",
    "Cable & Satellite": "Internet & Digital Media",
    "Publishing": "Internet & Digital Media",
    # A few GICS sub-industry names are IDENTICAL to a display name already
    # used above (Yahoo's dialect for the same stocks uses a different
    # string) — these self-map so GICS-classified S&P 500 names match too.
    "Regional Banks": "Regional Banks",
    "Health Care Services": "Health Care Services",
    "Asset Management & Custody Banks": "Capital Markets & Asset Mgmt",
    "Semiconductor Materials & Equipment": "Semiconductors",  # word-order
                                                               # twin of
                                                               # "Semiconductor
                                                               # Equipment &
                                                               # Materials"
    "Tobacco": "Food & Beverage",
    "Household & Personal Products": "Food & Beverage",
    "Household Products": "Food & Beverage",
    "Personal Care Products": "Food & Beverage",
    "Paper & Plastic Packaging Products & Materials": "Retail",  # twin of
                                                                  # "Packaging
                                                                  # & Containers"
    "Paper & Paper Products": "Retail",
    "Metal, Glass & Plastic Containers": "Retail",
    # Hardware/electronics: no dedicated liquid pure-play ETF exists distinct
    # from broad Info Tech (XLK, already the sector-tier ETF) or pure-play
    # semiconductors (SMH, already above) — folded in as the closest real,
    # liquid home rather than left unmapped. This is the cluster SanDisk
    # ("Technology Hardware, Storage & Peripherals") belongs to.
    "Computer Hardware": "Semiconductors",
    "Technology Hardware, Storage & Peripherals": "Semiconductors",
    "Electronic Components": "Semiconductors",
    "Communication Equipment": "Semiconductors",
    "Communications Equipment": "Semiconductors",
    "Consumer Electronics": "Semiconductors",
    "Electronic Equipment & Instruments": "Semiconductors",
    "Electronic Manufacturing Services": "Semiconductors",
    "Electronics & Computer Distribution": "Semiconductors",
    "Technology Distributors": "Semiconductors",
    "Distributors": "Semiconductors",
    "Scientific & Technical Instruments": "Semiconductors",

    # Real Estate (equity REITs across every property type + real estate
    # operating/development companies) — VNQ, $70.8B AUM. Deliberately one
    # broad entry rather than split by property type (retail/office/
    # residential/...), consistent with how every other cluster here works
    # (e.g. Food & Beverage isn't split by packaged-foods vs restaurants).
    "REIT - Retail": "Real Estate",
    "REIT - Office": "Real Estate",
    "REIT - Diversified": "Real Estate",
    "REIT - Healthcare Facilities": "Real Estate",
    "REIT - Specialty": "Real Estate",
    "REIT - Residential": "Real Estate",
    "REIT - Hotel & Motel": "Real Estate",
    "REIT - Industrial": "Real Estate",
    "Real Estate - Development": "Real Estate",
    "Real Estate - Diversified": "Real Estate",
    "Real Estate Services": "Real Estate",
    "Retail REITs": "Real Estate",
    "Office REITs": "Real Estate",
    "Health Care REITs": "Real Estate",
    "Multi-Family Residential REITs": "Real Estate",
    "Single-Family Residential REITs": "Real Estate",
    "Hotel & Resort REITs": "Real Estate",
    "Industrial REITs": "Real Estate",
    "Self-Storage REITs": "Real Estate",
    "Data Center REITs": "Real Estate",
    "Telecom Tower REITs": "Real Estate",
    "Timber REITs": "Real Estate",
    "Other Specialized REITs": "Real Estate",

    # Uranium — real miner/producer stocks (Cameco-type names), not a Yahoo-
    # dialect synonym of anything already above.
    "Uranium": "Uranium",

    # Deliberately NOT mapped (checked, no good fit):
    #   Specialty Chemicals / Chemicals / Fertilizers & Agricultural Chemicals /
    #     Agricultural Inputs / Commodity Chemicals — no chemicals-specific ETF
    #     with meaningfully better liquidity than the Materials sector SPDR
    #     (XLB, already the sector-tier ETF); mapping here would just duplicate
    #     the sector tier under a different name.
    #   Education & Training Services / Specialty Business Services /
    #     Staffing & Employment Services / Human Resource & Employment
    #     Services / Personal Services — no liquid dedicated ETF.
    #   Utilities - Regulated Electric/Gas/Water, Electric Utilities,
    #     Multi-Utilities, Utilities - Independent Power Producers,
    #     Utilities - Diversified, Independent Power Producers & Energy
    #     Traders, Water Utilities, Gas Utilities — already fully covered by
    #     the Utilities sector tier (XLU); no sub-utility ETF liquid enough
    #     to be worth a separate Industry-tier entry.
    #   Thermal Coal / Coking Coal — the obvious candidate (KOL) was
    #     delisted; checked live on yfinance, ticker no longer resolves.
}


def build_industry_index_map_rows(tickers: list[dict]) -> list[tuple]:
    """
    tickers: [{symbol, industry, ...}] from the tickers table.
    Returns extra rows for db.upsert_sector_index_map, one per ticker whose
    industry has a known ETF mapping — ADDED alongside (not replacing) each
    ticker's sector-level row, so a stock carries both its sector ETF and its
    industry ETF in hawks_mappings. Unmapped industries (long tail + Shell
    Companies) are skipped — same graceful degradation as sector_etf_map.
    """
    rows = []
    for t in tickers:
        industry = t.get("industry")
        display_name = INDUSTRY_NAME_MAP.get(industry)
        if not display_name:
            continue
        etf = INDUSTRY_ETF.get(display_name)
        if not etf:
            continue
        rows.append((t["symbol"], display_name, etf, "official", "high"))
    return rows
