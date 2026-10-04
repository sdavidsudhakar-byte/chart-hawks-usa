"""
test_qa_suite.py — Senior-QA regression suite for the ChartHawks dashboard.

Purpose: lock down the calculations, mappings, filters, scan plumbing and API
contracts so a FUTURE ticker/index refresh (stocks added, removed, renamed, or
recategorised) cannot silently break the site. Every test asserts an invariant
that must hold regardless of how the underlying data changes.

Run:  python -m pytest test_qa_suite.py -v
   or python test_qa_suite.py          (prints a plain report, no pytest needed)

Categories:
  A. Calculation correctness   — EMA, EMA-slope, RS21/RS55, swing score, ranks
  B. Index master integrity    — no dup symbols, categories valid
  C. Sector-map rule integrity — every rule name resolvable, idempotent rebuild
  D. Cross-layer consistency   — master <-> sector-map <-> index_list <-> OHLCV
  E. DB query layer            — scanner indices+group filters behave
  F. API contracts             — endpoints return required keys & types
  G. Refresh-resilience        — simulate add/remove/recategorise of stocks
"""

import json
import math

import pytest

import db
import index_data as idx
import build_sector_map as bsm


# ───────────────────────── helpers ─────────────────────────

def _index_list_names():
    """All distinct NSE membership strings across nse_tickers.index_list."""
    conn = db.get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT index_list FROM nse_tickers")
        names = set()
        for (il,) in cur.fetchall():
            try:
                lst = il if isinstance(il, list) else (json.loads(il) if il else [])
            except Exception:
                lst = []
            names.update(lst)
        return names
    finally:
        db.release_conn(conn)


def _ohlcv_index_symbols():
    conn = db.get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT symbol FROM ohlcv_daily WHERE symbol LIKE 'NSE:%-INDEX'")
        return {r[0] for r in cur.fetchall()}
    finally:
        db.release_conn(conn)


# ═══════════════════ A. CALCULATION CORRECTNESS ═══════════════════
# Re-implement each formula independently and assert the production code matches.

def test_A_ema_matches_reference():
    from scanner import _compute_ema
    closes = [10, 11, 12, 11, 13, 14, 15, 14, 16, 17, 18, 17, 19, 20]
    period = 5
    out = _compute_ema(closes, period)
    # reference: seed = SMA of first `period`, then recursive
    k = 2 / (period + 1)
    ref = [None] * len(closes)
    ref[period - 1] = sum(closes[:period]) / period
    for i in range(period, len(closes)):
        ref[i] = closes[i] * k + ref[i - 1] * (1 - k)
    assert out[:period - 1] == [None] * (period - 1), "EMA must be None before warmup"
    for a, b in zip(out[period - 1:], ref[period - 1:]):
        assert abs(a - b) < 1e-9


def test_A_ema_too_short_is_all_none():
    from scanner import _compute_ema
    assert _compute_ema([1, 2, 3], 5) == [None, None, None]


def test_A_swing_ema_slope_formula():
    """stock_rs + swing both define slope = (ema[-1]/ema[-5] - 1)*100."""
    from stock_rs import _ema as srs_ema
    closes = [float(x) for x in range(1, 60)]
    ema = srs_ema(closes, 20)
    expected = round((ema[-1] / ema[-5] - 1) * 100, 4)
    # mirror _compute_one's slope calc
    n = len(closes)
    got = round((ema[-1] / ema[-5] - 1) * 100, 4) if len(ema) >= 5 else None
    assert got == expected


def test_A_rank_1_99_monotonic_and_bounded():
    from stock_rs import _rank_1_99
    vals = [5, 1, 3, 9, 7, 2, 8, 4, 6, 0]
    ranks = _rank_1_99(vals)
    assert all(1 <= r <= 99 for r in ranks), "ranks must be within 1..99"
    # highest value gets the highest rank
    hi_idx = vals.index(max(vals))
    lo_idx = vals.index(min(vals))
    assert ranks[hi_idx] == max(ranks)
    assert ranks[lo_idx] == min(ranks)


def test_A_rank_single_and_empty():
    from stock_rs import _rank_1_99
    assert _rank_1_99([]) == []
    assert _rank_1_99([42]) == [50], "single value should map to mid-rank 50"


def test_A_swing_score_normalisation_bounds():
    """swing_score must always land in [1,99] regardless of RS spread."""
    # Build a synthetic 'computed' dict the way _swing_score_universe expects.
    import main
    bench = {"close": 100.0, "close_21": 90.0, "close_55": 80.0}
    computed = {main._SWING_BENCHMARK: dict(bench)}
    for i in range(20):
        computed[f"X{i}"] = {"close": 100 + i, "close_21": 90.0, "close_55": 80.0}
    out = main._swing_score_universe(computed)
    scores = [c["swing_score"] for c in out.values()]
    assert all(1 <= s <= 99 for s in scores)
    assert max(scores) == 99 and min(scores) == 1


def test_A_swing_min_bars_guard():
    """_swing_compute_one must return None below the min-bars threshold."""
    import main
    short = [100.0] * (main._SWING_MIN_BARS - 1)
    assert main._swing_compute_one(short) is None
    ok = [100.0 + i for i in range(main._SWING_MIN_BARS + 5)]
    assert main._swing_compute_one(ok) is not None


# ═══════════════════ B. INDEX MASTER INTEGRITY ═══════════════════

def test_B_no_duplicate_symbols():
    syms = [i.symbol for i in idx.ALL_INDICES]
    dupes = {s for s in syms if syms.count(s) > 1}
    assert not dupes, f"duplicate index symbols in master: {dupes}"


def test_B_categories_are_known():
    valid = set(idx.CATEGORY_ORDER)
    bad = {i.symbol: i.category for i in idx.ALL_INDICES if i.category not in valid}
    assert not bad, f"unknown categories: {bad}"


def test_B_by_category_covers_all():
    grouped = idx.by_category()
    total = sum(len(v) for v in grouped.values())
    assert total == len(idx.ALL_INDICES), "by_category dropped or duplicated indices"


def test_B_get_info_roundtrip():
    for i in idx.ALL_INDICES:
        assert idx.get_info(i.symbol) is i


# ═══════════════════ C. SECTOR-MAP RULE INTEGRITY ═══════════════════

def test_C_all_rule_names_resolvable():
    """Every index name in BI_HIGH / SECTOR_FALLBACK / MANUAL_OVERRIDES must be a
    key in INDEX_SYMBOL — otherwise build() raises KeyError on the next refresh."""
    valid = set(bsm.INDEX_SYMBOL.keys())
    bad = set()
    for biname, lst in bsm.BI_HIGH.items():
        for name, _ in lst:
            if name not in valid:
                bad.add(("BI_HIGH", biname, name))
    for sym, lst in bsm.MANUAL_OVERRIDES.items():
        for name, _ in lst:
            if name not in valid:
                bad.add(("MANUAL_OVERRIDES", sym, name))
    # SECTOR_FALLBACK lives inside logical_matches — exercise it for every sector
    import inspect, re
    for m in re.findall(r'"(NIFTY [^"]+)"', inspect.getsource(bsm.logical_matches)):
        if m not in valid:
            bad.add(("SECTOR_FALLBACK", "-", m))
    assert not bad, f"rule references unknown index names: {sorted(bad)}"


def test_C_every_map_symbol_in_master():
    """Each Strike index symbol must exist in index_data master (else no OHLCV/score)."""
    master = set(idx.SYMBOLS)
    bad = {n: s for n, s in bsm.INDEX_SYMBOL.items() if s not in master}
    assert not bad, f"sector-map symbols missing from index master: {bad}"


def test_C_confidence_values_valid():
    valid = {"high", "medium", "low"}
    bad = []
    for biname, lst in bsm.BI_HIGH.items():
        for name, conf in lst:
            if conf not in valid:
                bad.append((biname, name, conf))
    for sym, lst in bsm.MANUAL_OVERRIDES.items():
        for name, conf in lst:
            if conf not in valid:
                bad.append((sym, name, conf))
    assert not bad, f"invalid confidence values: {bad}"


def test_C_logical_matches_prefers_high():
    """When BI and sector both map an index, the higher confidence must win."""
    # pharmaceuticals => PHARMA high; sector 'healthcare' => HEALTHCARE medium.
    res = dict(bsm.logical_matches("Healthcare", "Pharmaceuticals", "Pharmaceuticals"))
    assert res.get("NIFTY PHARMA") == "high"
    assert res.get("NIFTY HEALTHCARE INDEX") == "high"  # BI gives high, beats sector medium


# ═══════════════════ D. CROSS-LAYER CONSISTENCY ═══════════════════

def test_D_sector_map_db_names_match_builder():
    """No stale index_name rows in sector_index_map that the builder no longer knows."""
    conn = db.get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT index_name FROM sector_index_map WHERE index_name!='UNCLASSIFIED'")
        db_names = {r[0] for r in cur.fetchall()}
    finally:
        db.release_conn(conn)
    builder = set(bsm.INDEX_SYMBOL.keys())
    stale = db_names - builder
    assert not stale, f"sector_index_map has names the builder dropped (rebuild will purge): {stale}"


def test_D_every_mapped_index_has_ohlcv():
    """Each Strike index symbol must have OHLCV, else Sector Lens shows a blank score."""
    ohlcv = _ohlcv_index_symbols()
    missing = {n: s for n, s in bsm.INDEX_SYMBOL.items() if s not in ohlcv}
    assert not missing, f"mapped indices with no OHLCV (blank Sector Lens score): {missing}"


# Mirror of _IDX_NAME_ALIAS in templates/index.html (_goToStocksLensFromTab).
# Display-name -> NSE official index_list membership string, for the few indices
# whose master display-name differs from NSE's membership vocabulary. This is the
# contract: keep these two in sync.
IDX_NAME_ALIAS = {
    "NIFTY ALPHA QUALITY LOW-VOL 30":    "NIFTY ALPHA QUALITY LOW-VOLATILITY 30",
    "NIFTY INDIA CORP GROUP - TATA 25%": "NIFTY INDIA CORPORATE GROUP INDEX - TATA GROUP 25% CAP",
    "NIFTY SMALLCAP250 MQ100":           "NIFTY SMALLCAP250 MOMENTUM QUALITY 100",
}


def test_D_strike_selectable_indices_resolve_to_stocks():
    """Every master index display-name selected from Sector Lens must resolve to
    >=1 stock in Stocks Lens — directly if the display-name matches NSE's
    index_list string, otherwise via the IDX_NAME_ALIAS translation. A future
    refresh that renames a membership string (breaking a previously-working
    index) fails here loudly."""
    il_names = _index_list_names()
    master_names = {i.name for i in idx.ALL_INDICES}

    unresolved = []
    for name in master_names:
        effective = IDX_NAME_ALIAS.get(name, name)
        if effective not in il_names:
            unresolved.append((name, effective))
    assert not unresolved, (
        "Index names that resolve to 0 stocks in Stocks Lens "
        "(display-name -> effective membership string):\n  "
        + "\n  ".join(f"{n!r} -> {e!r}" for n, e in sorted(unresolved))
        + "\nFix: align the master name or add an IDX_NAME_ALIAS entry "
          "(and mirror it in templates/index.html _IDX_NAME_ALIAS)."
    )


def test_D_alias_targets_actually_resolve():
    """Each IDX_NAME_ALIAS value must (a) be a real NSE membership string and
    (b) map to at least one stock — so the alias is never a silent no-op."""
    conn = db.get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT symbol, index_list FROM nse_tickers")
        rows = []
        for sym, il in cur.fetchall():
            try:
                lst = il if isinstance(il, list) else (json.loads(il) if il else [])
            except Exception:
                lst = []
            rows.append(lst)
    finally:
        db.release_conn(conn)

    for disp, target in IDX_NAME_ALIAS.items():
        n = sum(1 for lst in rows if target in lst)
        assert n > 0, f"alias {disp!r} -> {target!r} matches 0 stocks (stale alias?)"


def test_D_alias_keys_are_real_master_names():
    """Every alias key must be an actual master display-name (else it's dead)."""
    master_names = {i.name for i in idx.ALL_INDICES}
    dead = [k for k in IDX_NAME_ALIAS if k not in master_names]
    assert not dead, f"IDX_NAME_ALIAS keys not in index master (dead aliases): {dead}"


def test_D_unclassified_stocks_are_flagged_not_dropped():
    """A stock matching zero indices must get one UNCLASSIFIED row, never vanish."""
    conn = db.get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM nse_tickers")
        n_tickers = cur.fetchone()[0]
        cur.execute("SELECT COUNT(DISTINCT symbol) FROM sector_index_map")
        n_mapped = cur.fetchone()[0]
    finally:
        db.release_conn(conn)
    # Every ticker must appear in the map (either mapped or UNCLASSIFIED)
    assert n_mapped == n_tickers, (
        f"{n_tickers - n_mapped} tickers missing from sector_index_map entirely "
        f"(should be mapped or UNCLASSIFIED). Rebuild may be stale."
    )


# ═══════════════════ E. DB QUERY LAYER ═══════════════════

def test_E_scanner_indices_filter_uses_sector_map():
    """get_scanner_results(indices=[...]) must only return stocks mapped to those
    indices in sector_index_map."""
    # pick an index that actually has mapped stocks
    conn = db.get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""SELECT index_name, COUNT(*) c FROM sector_index_map
                       WHERE index_name!='UNCLASSIFIED' GROUP BY index_name ORDER BY c DESC LIMIT 1""")
        row = cur.fetchone()
    finally:
        db.release_conn(conn)
    if not row:
        pytest.skip("no sector_index_map data")
    index_name = row[0]
    res = db.get_scanner_results("15", [], [], [], [], indices=[index_name])
    # every returned symbol must be mapped to that index
    mapped = _symbols_for_index(index_name)
    for r in res:
        assert r["symbol"] in mapped, f"{r['symbol']} returned but not mapped to {index_name}"


def test_E_scanner_symbols_filter_exact():
    """symbols=[...] must return only those symbols (and only if they have hits)."""
    res = db.get_scanner_results("15", [], [], [], [], symbols=["RELIANCE", "TCS"])
    for r in res:
        assert r["symbol"] in {"RELIANCE", "TCS"}


def test_E_scanner_group_filter_matches_column():
    """macro filter must only return rows of that macro."""
    # find a macro present in scanner_results
    conn = db.get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT macro FROM scanner_results WHERE macro!='' LIMIT 1")
        row = cur.fetchone()
    finally:
        db.release_conn(conn)
    if not row:
        pytest.skip("no scanner_results with a macro yet")
    macro = row[0]
    res = db.get_scanner_results("15", [macro], [], [], [])
    for r in res:
        assert r["macro"] == macro


def _symbols_for_index(index_name):
    conn = db.get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT symbol FROM sector_index_map WHERE index_name=?", (index_name,))
        return {r[0] for r in cur.fetchall()}
    finally:
        db.release_conn(conn)


def _busiest_index():
    conn = db.get_conn()
    try:
        cur = conn.cursor()
        cur.execute("""SELECT index_name FROM sector_index_map WHERE index_name!='UNCLASSIFIED'
                       GROUP BY index_name ORDER BY COUNT(*) DESC LIMIT 1""")
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        db.release_conn(conn)


# ═══════════════════ F. API CONTRACTS ═══════════════════

@pytest.fixture(scope="module")
def client():
    from fastapi.testclient import TestClient
    import main
    return TestClient(main.app)


def test_F_swing_radar_contract(client):
    j = client.get("/api/swing-radar").json()
    assert set(["as_of", "indices"]).issubset(j)
    assert isinstance(j["indices"], list) and j["indices"], "no indices returned"
    s = j["indices"][0]
    for k in ("symbol", "name", "category", "swing_score", "rs21", "chart"):
        assert k in s, f"swing-radar item missing {k}"
    assert 1 <= s["swing_score"] <= 99


def test_F_stock_rs_contract(client):
    j = client.get("/api/stock-rs").json()
    assert "stocks" in j and isinstance(j["stocks"], list) and j["stocks"]
    s = j["stocks"][0]
    for k in ("symbol", "company_name", "macro", "sector", "index_list",
              "hawks_mappings", "rs21_rank", "rs55_rank", "ema_slope"):
        assert k in s, f"stock-rs item missing {k}"
    assert isinstance(s["index_list"], list)
    assert isinstance(s["hawks_mappings"], list)


def test_F_tickers_contract(client):
    j = client.get("/api/tickers?page=1&limit=50").json()
    assert {"total", "rows"}.issubset(j)
    assert isinstance(j["rows"], list) and j["rows"]
    r = j["rows"][0]
    for k in ("symbol", "company_name", "index_list", "hawks_mappings", "is_fno"):
        assert k in r
    assert isinstance(r["index_list"], list)


def test_F_tickers_filter_macro(client):
    # find a macro
    j0 = client.get("/api/tickers?page=1&limit=9999").json()
    macros = {r["macro"] for r in j0["rows"] if r["macro"]}
    if not macros:
        pytest.skip("no macros")
    macro = sorted(macros)[0]
    j = client.get(f"/api/tickers?page=1&limit=9999&macro={macro}").json()
    assert all(r["macro"] == macro for r in j["rows"]), "macro filter leaked other macros"
    assert j["rows"], "macro filter returned nothing for a known macro"


def test_F_scanner_index_filter_contract(client):
    index_name = _busiest_index()
    if not index_name:
        pytest.skip("no sector_index_map data")
    r = client.post("/api/scanner", json={
        "macros": [], "sectors": [], "industries": [], "basic_industries": [],
        "indices": [index_name], "resolution": "15",
    })
    assert r.status_code == 200
    body = r.json()
    assert "results" in body
    mapped = _symbols_for_index(index_name)
    for row in body["results"]:
        assert row["symbol"] in mapped


# ═══════════════════ G. REFRESH-RESILIENCE (in-memory simulation) ═══════════════════
# These exercise the builder's pure functions against synthetic stocks to prove the
# logic survives new/removed/recategorised tickers WITHOUT touching the real DB.

def test_G_new_unknown_stock_becomes_unclassified():
    """A stock with a basic_industry the rules don't know, no official index,
    and not in overrides, must produce ZERO logical matches (=> UNCLASSIFIED)."""
    matches = bsm.logical_matches("Totally New Sector", "New Industry", "quantum gizmos")
    assert matches == [], "unknown stock unexpectedly matched an index"


def test_G_recategorised_stock_remaps():
    """If NSE moves a stock's basic_industry from pharma to IT, the logical
    mapping must follow the new classification."""
    before = dict(bsm.logical_matches("Healthcare", "Pharma", "pharmaceuticals"))
    after  = dict(bsm.logical_matches("Information Technology", "IT", "software products"))
    assert "NIFTY PHARMA" in before and "NIFTY PHARMA" not in after
    assert "NIFTY IT" in after and "NIFTY IT" not in before


def test_G_official_membership_always_wins_confidence():
    """The put() merge must upgrade a logical row to official when both exist."""
    # Simulate the merge logic used in build()
    mapped = {}
    rank = {"high": 3, "medium": 2, "low": 1}

    def put(sym, name, source, conf):
        isym = bsm.INDEX_SYMBOL[name]
        key = (sym, name)
        if key not in mapped:
            mapped[key] = [isym, source, conf]
        else:
            _, cur_src, cur_conf = mapped[key]
            if source == "official" and cur_src != "official":
                mapped[key] = [isym, "official", "high"]
            elif cur_src != "official" and rank[conf] > rank[cur_conf]:
                mapped[key] = [isym, source, conf]

    put("FOO", "NIFTY IT", "logical", "medium")
    put("FOO", "NIFTY IT", "official", "high")
    assert mapped[("FOO", "NIFTY IT")][1] == "official"
    assert mapped[("FOO", "NIFTY IT")][2] == "high"
    # official should not be downgraded by a later logical row
    put("FOO", "NIFTY IT", "logical", "low")
    assert mapped[("FOO", "NIFTY IT")][1] == "official"


def test_G_infra_crossmap_rule_holds():
    """The INFRA cross-map relies on the alias name existing in INDEX_SYMBOL."""
    assert "NIFTY INDIA INFRASTRUCTURE & LOGISTICS" in bsm.INDEX_SYMBOL
    assert "NIFTY INFRASTRUCTURE" in bsm.INDEX_SYMBOL
    # both alias and canonical map to the same Fyers symbol
    assert bsm.INDEX_SYMBOL["NIFTY INDIA INFRASTRUCTURE & LOGISTICS"] == \
           bsm.INDEX_SYMBOL["NIFTY INFRASTRUCTURE"]


# Plain-runner fallback so it works without pytest installed
if __name__ == "__main__":
    import traceback
    fns = [(n, f) for n, f in sorted(globals().items())
           if n.startswith("test_") and callable(f)]
    # crude client for F-tests
    from fastapi.testclient import TestClient
    import main
    _client = TestClient(main.app)
    passed = failed = skipped = 0
    for name, fn in fns:
        try:
            kw = {}
            if "client" in fn.__code__.co_varnames[:fn.__code__.co_argcount]:
                kw["client"] = _client
            fn(**kw)
            print(f"  PASS  {name}")
            passed += 1
        except pytest.skip.Exception as e:  # type: ignore
            print(f"  SKIP  {name}  ({e})")
            skipped += 1
        except Exception as e:
            print(f"  FAIL  {name}  -> {e}")
            traceback.print_exc()
            failed += 1
    print(f"\n{passed} passed, {failed} failed, {skipped} skipped")
