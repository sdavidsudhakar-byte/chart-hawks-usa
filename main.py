"""
main.py — FastAPI app for the Chart Hawks USA dashboard.

Endpoints:
  GET  /                   → Dashboard HTML page
  POST /refresh            → Trigger US ticker universe refresh
  GET  /refresh/stream     → SSE: live ticker refresh progress
  POST /refresh-prices     → Trigger yfinance price fetch for all symbols
  GET  /prices/stream      → SSE: live price fetch progress
  POST /shutdown           → Gracefully stop the server
  GET  /api/status         → Last refresh info + ticker count
  GET  /api/prices-status  → Price data date range info
  GET  /api/tickers        → Paginated ticker list with search/filter
  GET  /api/sectors        → Distinct macros/sectors for dropdowns
"""

import asyncio
import json
import logging
import os
import queue
import signal
import threading
import time

import psycopg2.extras
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from fastapi.templating import Jinja2Templates

import db
import universe
import market_data as prices_module
import flash_prices as flash_prices_module
import scanner as scanner_module
import notifier
import index_data as index_data_module
import rs as rs_module
import stock_rs as stock_rs_module
import run_indicator_backfill as indicators_module
import sector_etf_map
import industry_etf_map

try:
    from config import DB_MODE as _DB_MODE
except ImportError:
    _DB_MODE = "neon"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="NSE Ticker Dashboard")
templates = Jinja2Templates(directory="templates")


# ── shared state ──────────────────────────────────────────────────────────────

class RefreshState:
    def __init__(self):
        self.running = False
        self.queue   = queue.Queue()
        self.log_id  = None

state       = RefreshState()   # US ticker universe refresh
price_state = RefreshState()   # Prices refresh



# ── date-keyed compute cache ──────────────────────────────────────────────────
# Swing-radar, swing-radar-history and stock-rs recompute the same result every
# page load until new price data arrives. Cache each computed payload keyed by the
# latest ohlcv_daily date; the key changes (and the cache rebuilds once) only when
# a price refresh lands. Read-through, in-memory — no schema/formula/data change.

_compute_cache: dict = {}            # name -> {"key": <date>, "value": <payload>}
_compute_cache_lock = threading.Lock()

# Probe-result TTL: the cache key (latest data date) can only change after a refresh,
# which happens at most a few times a day. Re-probing the DB on every request would
# wake Neon needlessly, so memoise probe results for this many seconds.
_PROBE_TTL_SEC = 30.0
_probe_cache: dict = {}              # probe_name -> (expires_at, value)
_probe_lock = threading.Lock()


def _probe_memoised(name: str, fn):
    """Run probe `fn` at most once per _PROBE_TTL_SEC; reuse the last value otherwise."""
    now = time.time()
    with _probe_lock:
        hit = _probe_cache.get(name)
        if hit and hit[0] > now:
            return hit[1]
    val = fn()
    with _probe_lock:
        _probe_cache[name] = (now + _PROBE_TTL_SEC, val)
    return val


def _fingerprint(name: str, sql: str) -> str | None:
    """Run a cheap fingerprint query (returns one row of scalars) and join the
    result into a string cache key. Any change to the underlying rows — from the
    app or an external job writing the DB directly — changes the fingerprint and
    forces a rebuild. Memoised for _PROBE_TTL_SEC so Neon isn't hit per request."""
    def _q():
        try:
            conn = db.get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(sql)
                    row = cur.fetchone()
            finally:
                db.release_conn(conn)
            if not row:
                return None
            return "|".join("" if v is None else str(v) for v in row)
        except Exception as e:
            logger.warning("compute-cache: fingerprint '%s' failed: %s", name, e)
            return None
    return _probe_memoised(name, _q)


def _index_symbols_sql_list() -> str:
    """Comma-joined, quoted list of the known sector/broad-market index symbols,
    for inline use in fingerprint SQL (stocks and indices share ohlcv_daily with
    no distinguishing symbol prefix, unlike NSE's 'NSE:...-INDEX' suffix)."""
    return ",".join(f"'{s}'" for s in index_data_module.SYMBOLS)


def _ohlcv_index_fp() -> str | None:
    """Fingerprint for index OHLCV (swing-radar / swing-radar-history read these rows).
    Catches new bars (MAX date, COUNT) and same-date value edits (SUM of closes)."""
    syms = _index_symbols_sql_list()
    if _DB_MODE == "local":
        sql = f"SELECT MAX(date), COUNT(*), ROUND(SUM(close),2) FROM ohlcv_daily WHERE symbol IN ({syms})"
    else:
        sql = f"SELECT MAX(date)::text, COUNT(*), ROUND(SUM(close)::numeric,2) FROM ohlcv_daily WHERE symbol IN ({syms})"
    return _fingerprint("ohlcv_index_fp", sql)


def _index_rs_fp() -> str | None:
    """Fingerprint for index_rs (IBD RS history reads it, plus index OHLCV for the
    weekly-close warmup). Catches new RS dates/edits and any index OHLCV change."""
    syms = _index_symbols_sql_list()
    if _DB_MODE == "local":
        sql = ("SELECT (SELECT MAX(date) FROM index_rs), (SELECT COUNT(*) FROM index_rs), "
               "(SELECT ROUND(COALESCE(SUM(rs_raw),0),2) FROM index_rs), "
               f"(SELECT MAX(date) FROM ohlcv_daily WHERE symbol IN ({syms}))")
    else:
        sql = ("SELECT (SELECT MAX(date)::text FROM index_rs), (SELECT COUNT(*) FROM index_rs), "
               "(SELECT ROUND(COALESCE(SUM(rs_raw)::numeric,0),2) FROM index_rs), "
               f"(SELECT MAX(date)::text FROM ohlcv_daily WHERE symbol IN ({syms}))")
    return _fingerprint("index_rs_fp", sql)


def _stock_rs_fp() -> str | None:
    """Fingerprint for stock_rs_daily (the table /api/stock-rs reads). Catches new
    dates, row-count changes, and same-date rank recomputes (SUM of rank columns)."""
    if _DB_MODE == "local":
        sql = ("SELECT MAX(date), COUNT(*), "
               "COALESCE(SUM(rs21_rank),0), COALESCE(SUM(rs55_rank),0), "
               "COALESCE(SUM(CAST(day_return*100 AS INTEGER)),0) "
               "FROM stock_rs_daily")
    else:
        sql = ("SELECT MAX(date)::text, COUNT(*), "
               "COALESCE(SUM(rs21_rank),0), COALESCE(SUM(rs55_rank),0), "
               "COALESCE(SUM(CAST(day_return*100 AS INTEGER)),0) "
               "FROM stock_rs_daily")
    return _fingerprint("stock_rs_fp", sql)


def _latest_tickers_key() -> str | None:
    """Fingerprint for the Directory payload. Invalidates on any tickers change
    (count + max refreshed_at) or any sector_index_map change (count + max rowid),
    whether written by the app or an external job. Memoised for _PROBE_TTL_SEC."""
    if _DB_MODE == "local":
        sql = ("SELECT (SELECT COUNT(*) FROM tickers), "
               "(SELECT MAX(refreshed_at) FROM tickers), "
               "(SELECT COUNT(*) FROM sector_index_map), "
               "(SELECT COALESCE(MAX(rowid),0) FROM sector_index_map)")
    else:
        sql = ("SELECT (SELECT COUNT(*) FROM tickers), "
               "(SELECT MAX(refreshed_at)::text FROM tickers), "
               "(SELECT COUNT(*) FROM sector_index_map), "
               "(SELECT COALESCE(MAX(ctid::text),'') FROM sector_index_map)")
    return _fingerprint("tickers_key", sql)


def _cache_get(name: str, key: str | None):
    """Return cached value for `name` if its stored key matches `key`, else None."""
    if key is None:
        return None
    with _compute_cache_lock:
        entry = _compute_cache.get(name)
        if entry and entry["key"] == key:
            return entry["value"]
    return None


def _cache_put(name: str, key: str | None, value) -> None:
    if key is None:
        return
    with _compute_cache_lock:
        _compute_cache[name] = {"key": key, "value": value}


def _cache_get_response(name: str, key: str | None):
    """Like _cache_get but returns a ready-to-send JSON Response built from cached
    bytes — skips FastAPI's per-request re-serialization of large payloads."""
    if key is None:
        return None
    with _compute_cache_lock:
        entry = _compute_cache.get(name)
        if entry and entry["key"] == key and "bytes" in entry:
            return Response(content=entry["bytes"], media_type="application/json")
    return None


def _cache_put_response(name: str, key: str | None, value) -> Response | None:
    """Serialize `value` once, store the bytes, and return a fast Response.
    Subsequent hits via _cache_get_response reuse the bytes with no re-encode."""
    body = json.dumps(value, default=str).encode("utf-8")
    if key is not None:
        with _compute_cache_lock:
            _compute_cache[name] = {"key": key, "value": value, "bytes": body}
    return Response(content=body, media_type="application/json")


def _invalidate_compute_caches() -> None:
    """Force re-probe + recompute on the next request. Called after a refresh so
    fresh data appears immediately instead of waiting out the probe TTL."""
    with _probe_lock:
        _probe_cache.clear()
    with _compute_cache_lock:
        _compute_cache.clear()


def _warm_compute_caches():
    """Pre-build every cached payload in the background so the first user visit
    after a refresh (or after server start) hits a warm cache instead of paying
    the cold recompute. Each endpoint fn populates its own cache as a side effect.
    Safe to call anytime — it just primes; failures are logged, not fatal."""
    t0 = time.time()
    # Clear the probe memo first so fingerprints reflect the just-refreshed data.
    with _probe_lock:
        _probe_cache.clear()
    warmers = [
        ("swing-radar",          lambda: api_swing_radar()),
        ("swing-radar-history",  lambda: api_swing_radar_history(days=30)),
        ("stock-rs",             lambda: api_stock_rs()),
        ("tickers",              lambda: api_tickers(page=1, limit=9999)),
        ("ibd-rs weekly",        lambda: api_ibd_rs_rank_history(days=210, granularity="weekly")),
    ]
    for name, fn in warmers:
        try:
            fn()
        except Exception as e:
            logger.warning("cache warm-up: %s failed: %s", name, e)
    logger.info("Compute caches warmed in %.2fs", time.time() - t0)


# ── startup ───────────────────────────────────────────────────────────────────

@app.on_event("startup")
def startup():
    db.init_db()
    logger.info("DB initialised")
    threading.Thread(target=_warm_compute_caches, daemon=True).start()


# ── pages ─────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    resp = templates.TemplateResponse(request, "index.html")
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    resp.headers["Pragma"] = "no-cache"
    resp.headers["Expires"] = "0"
    return resp


# ── shutdown ──────────────────────────────────────────────────────────────────

@app.post("/shutdown")
def shutdown():
    def _kill():
        time.sleep(0.5)
        os.kill(os.getpid(), signal.SIGTERM)
    threading.Thread(target=_kill, daemon=True).start()
    return JSONResponse({"ok": True})


# ── US ticker universe refresh ───────────────────────────────────────────────

def _rebuild_sector_index_map() -> tuple[int, int, list[str]]:
    """Rebuild sector_index_map from each active ticker's GICS sector → Sector
    SPDR ETF (sector_etf_map.py) PLUS, where coverage exists, its industry →
    industry ETF (industry_etf_map.py) — a stock carries both rows. Returns
    (n_stocks, n_rows, unclassified) where unclassified = no sector mapping."""
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT symbol, sector, industry FROM tickers WHERE is_active=1")
            tickers = [{"symbol": r[0], "sector": r[1], "industry": r[2]} for r in cur.fetchall()]
    finally:
        db.release_conn(conn)
    db.clear_sector_index_map()
    rows = sector_etf_map.build_sector_index_map_rows(tickers)
    rows += industry_etf_map.build_industry_index_map_rows(tickers)
    db.upsert_sector_index_map(rows)
    mapped = {r[0] for r in rows}
    unclassified = [t["symbol"] for t in tickers if t["symbol"] not in mapped]
    return len(tickers), len(rows), unclassified


def _run_refresh():
    failed  = []
    total   = 0
    success = 0

    try:
        state.log_id = db.log_refresh_start()
    except Exception as e:
        logger.error("Could not create refresh log entry: %s", e)
        state.log_id = None

    try:
        db.mark_all_tickers_inactive()
    except Exception as e:
        logger.error("Could not mark tickers inactive: %s", e)

    try:
        for event in universe.run_full_refresh():
            etype = event.get("type")
            if etype == "batch":
                rows = event.get("rows", [])
                if rows:
                    try:
                        db.upsert_tickers(rows)
                    except Exception as e:
                        logger.error("DB upsert failed: %s", e)
                continue
            elif etype == "done":
                total   = event.get("total", 0)
                success = event.get("success", 0)
                failed  = event.get("failed", [])
            state.queue.put(event)
    except Exception as e:
        logger.exception("Ticker refresh crashed: %s", e)
        state.queue.put({"type": "error", "message": str(e)})
    finally:
        try:
            db.log_refresh_finish(state.log_id, total, success, failed)
        except Exception as e:
            logger.error("Could not write refresh log: %s", e)
        state.running = False
        # Rebuild sector index map so new stocks are mapped and delisted stocks hidden
        try:
            _rebuild_sector_index_map()
        except Exception as e:
            logger.error("Sector map rebuild failed: %s", e)
        state.queue.put(None)
        _invalidate_compute_caches()
        threading.Thread(target=_warm_compute_caches, daemon=True).start()



@app.post("/api/reset-state")
def api_reset_state():
    """Force-clear stuck running flags (use when a refresh crashed mid-run)."""
    state.running = False
    price_state.running = False
    while not state.queue.empty():
        try: state.queue.get_nowait()
        except queue.Empty: break
    while not price_state.queue.empty():
        try: price_state.queue.get_nowait()
        except queue.Empty: break
    return JSONResponse({"ok": True, "message": "State reset. You can now start a new refresh."})


@app.post("/api/clear-cache")
def api_clear_cache():
    """Force-clear the in-memory compute cache so next request rebuilds from DB."""
    _invalidate_compute_caches()
    threading.Thread(target=_warm_compute_caches, daemon=True).start()
    return JSONResponse({"ok": True, "message": "Cache cleared and warming in background."})


@app.post("/refresh")
def trigger_refresh():
    if state.running:
        return JSONResponse({"ok": False, "message": "Refresh already in progress."}, status_code=409)
    while not state.queue.empty():
        try:
            state.queue.get_nowait()
        except queue.Empty:
            break
    state.running = True
    threading.Thread(target=_run_refresh, daemon=True).start()
    return JSONResponse({"ok": True, "message": "Refresh started."})


@app.get("/refresh/stream")
async def refresh_stream():
    async def event_generator():
        while True:
            try:
                event = state.queue.get_nowait()
            except queue.Empty:
                await asyncio.sleep(0.1)
                continue
            if event is None:
                break
            try:
                payload = json.dumps(event)
            except (TypeError, ValueError):
                continue
            yield f"event: {event.get('type', 'message')}\ndata: {payload}\n\n"
    return StreamingResponse(event_generator(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ── Price refresh ─────────────────────────────────────────────────────────────

def _run_price_refresh(flash: bool = False):
    """
    Run the full 7-phase price/indicator/RS/scanner pipeline.

    flash=False → full weekend refresh: prices.run_prices_refresh() re-pulls the
                  whole history window (ground-truth resync).
    flash=True  → "Flash Price Refresh": flash_prices.run_flash_refresh() fetches
                  only new bars per symbol (incremental + small overlap), and the
                  RS backfill windows are narrowed. All 7 phases still run.

    Only P1's source generator and the P4/P5 backfill windows differ; every other
    phase is byte-for-byte shared so the two paths cannot diverge over time.
    """
    global _scanner_running
    import time as _time

    _p1_source = flash_prices_module.run_flash_refresh if flash else prices_module.run_prices_refresh
    _rs_backfill_days = 5 if flash else 30   # incremental RS window for Flash
    _label_prefix = "Flash " if flash else ""

    # ── Per-phase result tracking ─────────────────────────────────────────────
    phases = {}   # phase_key → {ok, detail, error, duration}

    def _phase(key, label):
        """Context: record start time and emit status message."""
        price_state.queue.put({"type": "status", "message": f"[{key}] {label}..."})
        return _time.time()

    def _phase_ok(key, detail, t0):
        dur = round(_time.time() - t0, 1)
        phases[key] = {"ok": True,  "detail": detail, "duration": dur}
        price_state.queue.put({"type": "status", "message": f"[{key}] OK — {detail} ({dur}s)"})
        logger.info("[%s] OK — %s (%.1fs)", key, detail, dur)

    def _phase_fail(key, error, t0):
        dur = round(_time.time() - t0, 1)
        phases[key] = {"ok": False, "detail": str(error), "duration": dur}
        price_state.queue.put({"type": "status", "message": f"[{key}] FAILED — {error}"})
        logger.error("[%s] FAILED — %s (%.1fs)", key, error, dur)

    price_ok      = False
    price_total   = 0
    price_success = 0
    price_failed  = []
    index_success = 0
    index_failed  = []
    scanner_result = None

    try:
        # ── Phase 1: Stock daily ──────────────────────────────────────────────
        t0 = _phase("P1-Stocks", f"{_label_prefix}Fetching stock prices (daily)")
        try:
            for event in _p1_source():
                etype = event.get("type")
                if etype == "price_batch":
                    rows = event.get("rows", [])
                    if rows:
                        try:
                            db.upsert_prices(rows)
                        except Exception as e:
                            logger.error("Price upsert failed: %s", e)
                    continue
                if etype == "price_done":
                    price_total   = event.get("total", 0)
                    price_success = event.get("success", 0)
                    price_failed  = event.get("failed", [])
                price_state.queue.put(event)
            _phase_ok("P1-Stocks",
                f"{price_success}/{price_total} symbols loaded"
                + (f", {len(price_failed)} failed" if price_failed else ""), t0)
        except Exception as e:
            _phase_fail("P1-Stocks", e, t0)
            raise

        # ── Phase 2: Index daily ──────────────────────────────────────────────
        t0 = _phase("P2-Index", f"Fetching index prices (daily, {len(index_data_module.SYMBOLS)} indices)")
        try:
            for event in index_data_module.run_index_refresh():
                etype = event.get("type")
                if etype == "done":
                    index_success = event.get("success", 0)
                    index_failed  = event.get("failed", [])
                price_state.queue.put(event)
            _phase_ok("P2-Index",
                f"{index_success} indices loaded"
                + (f", {len(index_failed)} failed: {index_failed}" if index_failed else ""), t0)
        except Exception as e:
            _phase_fail("P2-Index", e, t0)
            raise

        # ── Phase 3: EMA/SMA indicators + weekly/monthly OHLCV ───────────────
        t0 = _phase("P3-Indicators", "Computing EMA20/50, SMA200, RS line + weekly/monthly candles for all symbols")
        try:
            ind_result = indicators_module.run_incremental()
            if ind_result.get("ok"):
                c = ind_result.get("counts", {})
                _phase_ok("P3-Indicators",
                    f"{ind_result['symbols']} symbols — "
                    f"daily={c.get('indicator_daily',0):,} "
                    f"weekly={c.get('indicator_weekly',0):,} "
                    f"monthly={c.get('indicator_monthly',0):,} rows", t0)
            else:
                _phase_fail("P3-Indicators", ind_result.get("error") or ind_result.get("reason"), t0)
        except Exception as e:
            _phase_fail("P3-Indicators", e, t0)

        # ── Phase 4: Index IBD RS ratings ─────────────────────────────────────
        t0 = _phase("P4-IndexRS", f"Computing IBD RS ratings for {len(index_data_module.SYMBOLS)} indices (today + 30-day backfill)")
        try:
            rs_result = rs_module.compute_and_store_rs()
            bf_result = rs_module.backfill_rs(days=_rs_backfill_days)
            ms_result = rs_module.backfill_index_mid_short(days=_rs_backfill_days)
            if rs_result.get("ok"):
                _phase_ok("P4-IndexRS",
                    f"{rs_result.get('rated', 0)} indices rated today, "
                    f"{bf_result.get('computed', 0)} LT dates backfilled, "
                    f"{ms_result.get('upserted', 0)} mid/short rows upserted", t0)
            else:
                _phase_fail("P4-IndexRS", rs_result.get("error", "unknown"), t0)
        except Exception as e:
            _phase_fail("P4-IndexRS", e, t0)
            rs_result = {"ok": False}
            bf_result = {"computed": 0}

        # ── Phase 5: Stock RS ranks ───────────────────────────────────────────
        t0 = _phase("P5-StockRS", "Computing stock RS21/RS55/RS252 ranks (30-day backfill)")
        try:
            sr_dates = stock_rs_module.get_dates_to_compute(backfill_days=_rs_backfill_days)
            if sr_dates:
                sr_result = stock_rs_module.compute_and_store(sr_dates)
                _phase_ok("P5-StockRS",
                    f"{sr_result.get('computed', 0)} symbols across {len(sr_dates)} date(s)"
                    + (f", {len(sr_result.get('failed',[]))} failed" if sr_result.get('failed') else ""), t0)
            else:
                _phase_ok("P5-StockRS", "already up to date", t0)
                sr_result = {"computed": 0}
        except Exception as e:
            _phase_fail("P5-StockRS", e, t0)
            sr_result = {"computed": 0}

        # ── Phase 6: Daily Hunt EMA scanner ───────────────────────────────────
        with _scanner_lock:
            already = _scanner_running
        if already:
            phases["P6-DailyHunt"] = {"ok": False, "detail": "skipped — scanner already running", "duration": 0}
            price_state.queue.put({"type": "status", "message": "[P6-DailyHunt] skipped — scanner already running"})
        else:
            with _scanner_lock:
                _scanner_running = True
            try:
                t0 = _phase("P6-DailyHunt", "Running Daily Hunt EMA scanner (full US universe)")
                run_id = db.scanner_run_start()
                scanner_result = scanner_module.run_full_scan(run_id=run_id)
                _phase_ok("P6-DailyHunt",
                    f"{scanner_result.get('hits_bull', 0)} bull + {scanner_result.get('hits_bear', 0)} bear hits "
                    f"from {scanner_result.get('scanned', 0)} scanned", t0)
            except Exception as e:
                logger.error("Scanner failed: %s", e)
                _phase_fail("P6-DailyHunt", e, t0)
            finally:
                with _scanner_lock:
                    _scanner_running = False

        # ── Final summary event ────────────────────────────────────────────────
        price_state.queue.put({
            "type":          "all_done",
            "price_total":   price_total,
            "price_success": price_success,
            "price_failed":  price_failed,
            "index_success": index_success,
            "index_failed":  index_failed,
            "rs":            rs_result,
            "backfill":      bf_result,
            "phases":        phases,
        })
        price_ok = True

    except Exception as e:
        logger.exception("Price refresh crashed: %s", e)
        price_state.queue.put({"type": "error", "message": str(e)})

    # ── Finalise SSE stream ───────────────────────────────────────────────────
    price_state.running = False
    price_state.queue.put(None)
    _invalidate_compute_caches()
    threading.Thread(target=_warm_compute_caches, daemon=True).start()

    if price_ok:
        try:
            notifier.send_summary(
                price_total=price_total,
                price_success=price_success,
                price_failed=price_failed,
                scanner_result=scanner_result,
            )
        except Exception as e:
            logger.error("Notifier failed: %s", e)


@app.post("/refresh-prices")
def trigger_price_refresh():
    if price_state.running:
        return JSONResponse({"ok": False, "message": "Price refresh already running."}, status_code=409)
    if state.running:
        return JSONResponse({"ok": False, "message": "Ticker refresh is running — wait for it to finish."}, status_code=409)
    while not price_state.queue.empty():
        try:
            price_state.queue.get_nowait()
        except queue.Empty:
            break
    price_state.running = True
    threading.Thread(target=_run_price_refresh, daemon=True).start()
    return JSONResponse({"ok": True, "message": "Price refresh started."})


@app.post("/flash-refresh")
def trigger_flash_refresh():
    # Shares price_state with the full refresh, so the same guard prevents the two
    # from ever overlapping — Flash and the weekend full refresh are mutually exclusive.
    if price_state.running:
        return JSONResponse({"ok": False, "message": "A price refresh is already running."}, status_code=409)
    if state.running:
        return JSONResponse({"ok": False, "message": "Ticker refresh is running — wait for it to finish."}, status_code=409)
    while not price_state.queue.empty():
        try:
            price_state.queue.get_nowait()
        except queue.Empty:
            break
    price_state.running = True
    threading.Thread(target=_run_price_refresh, kwargs={"flash": True}, daemon=True).start()
    return JSONResponse({"ok": True, "message": "Flash price refresh started."})


@app.get("/prices/stream")
async def prices_stream():
    async def event_generator():
        while True:
            try:
                event = price_state.queue.get_nowait()
            except queue.Empty:
                await asyncio.sleep(0.1)
                continue
            if event is None:
                break
            try:
                payload = json.dumps(event)
            except (TypeError, ValueError):
                continue
            yield f"event: {event.get('type', 'message')}\ndata: {payload}\n\n"
    return StreamingResponse(event_generator(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ── API: status ───────────────────────────────────────────────────────────────

@app.get("/api/status")
def api_status():
    count = 0
    last  = None
    try:
        count = db.get_ticker_count()
        last  = db.get_last_refresh()
    except Exception as e:
        logger.error("api_status DB error: %s", e)

    last_dict = None
    if last:
        last_dict = dict(last)
        for k in ("started_at", "finished_at"):
            if last_dict.get(k):
                last_dict[k] = str(last_dict[k])
        # failed_symbols is stored as a JSON-encoded string column — parse it
        # to a real list so the frontend's .length reads the symbol count,
        # not the raw JSON string's character count.
        fs = last_dict.get("failed_symbols")
        if isinstance(fs, str):
            try:
                last_dict["failed_symbols"] = json.loads(fs)
            except Exception:
                last_dict["failed_symbols"] = []

    return {"running": state.running, "ticker_count": count, "last_refresh": last_dict}


@app.get("/api/prices-status")
def api_prices_status():
    info = db.get_prices_date_range(exclude_symbols=index_data_module.SYMBOLS)
    return {"running": price_state.running, **info}


# ── API: tickers ──────────────────────────────────────────────────────────────

@app.get("/api/tickers")
def api_tickers(search: str = "", page: int = 1, limit: int = 50,
                sector: str = "", macro: str = "",
                industry: str = "", basic_industry: str = ""):
    # Cache only the unfiltered full-page load that the Directory tab fires on open.
    # Any search/filter call carries extra params → different cache name → DB hit.
    _unfiltered = not (search or sector or macro or industry or basic_industry)
    _ck = _latest_tickers_key() if _unfiltered else None
    _cname = f"tickers:{page}:{limit}"
    if _unfiltered:
        _cached = _cache_get_response(_cname, _ck)
        if _cached is not None:
            return _cached

    offset     = (page - 1) * limit
    conditions = ["is_active = 1"]
    params: list = []

    if search:
        conditions.append("(symbol LIKE %s OR company_name LIKE %s)")
        params += [f"%{search}%", f"%{search}%"]
    if macro:
        conditions.append("macro = %s")
        params.append(macro)
    if sector:
        conditions.append("sector = %s")
        params.append(sector)
    if industry:
        conditions.append("industry = %s")
        params.append(industry)
    if basic_industry:
        conditions.append("basic_industry = %s")
        params.append(basic_industry)

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM tickers {where}", params)
            total = cur.fetchone()[0]
            cur.execute(
                f"""SELECT symbol, company_name,
                           macro, sector, industry, basic_industry,
                           index_list, refreshed_at
                    FROM tickers {where}
                    ORDER BY symbol LIMIT %s OFFSET %s""",
                params + [limit, offset],
            )
            cols = [d[0] for d in cur.description]
            rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        # Bulk load hawks_mappings from sector_index_map for all symbols on this page
        syms = [r["symbol"] for r in rows]
        hawks_map: dict[str, list[dict]] = {}
        if syms:
            hm_conn = db.get_conn()
            try:
                with hm_conn.cursor() as hm_cur:
                    ph2 = ",".join(["%s"] * len(syms))
                    hm_cur.execute(
                        f"""SELECT symbol, index_name, source, confidence
                            FROM sector_index_map
                            WHERE symbol IN ({ph2})
                            ORDER BY symbol,
                              CASE source WHEN 'official' THEN 0 ELSE 1 END,
                              CASE confidence WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END,
                              index_name""",
                        syms,
                    )
                    for sym, idx_name, src, conf in hm_cur.fetchall():
                        hawks_map.setdefault(sym, []).append(
                            {"name": idx_name, "source": src, "confidence": conf}
                        )
            finally:
                db.release_conn(hm_conn)

        for r in rows:
            if r.get("refreshed_at"):
                r["refreshed_at"] = str(r["refreshed_at"])
            # Restore plain index_list
            il = r.get("index_list")
            if isinstance(il, str):
                import json as _json
                try:
                    r["index_list"] = _json.loads(il)
                except Exception:
                    r["index_list"] = []
            elif il is None:
                r["index_list"] = []
            # New field: hawks_mappings — enriched {name, source, confidence} objects
            r["hawks_mappings"] = hawks_map.get(r["symbol"], [])
        _payload = {"total": total, "page": page, "limit": limit, "rows": rows}
        if _unfiltered:
            return _cache_put_response(_cname, _ck, _payload)
        return _payload
    except Exception as e:
        logger.error("api_tickers error: %s", e)
        return JSONResponse({"error": "Database error"}, status_code=500)
    finally:
        db.release_conn(conn)



# ── API: filter hierarchy (macro → sector → industry → basic_industry) ────────

@app.get("/api/filter-tree")
def api_filter_tree():
    """
    Returns full 4-level hierarchy used for cascading dropdowns.
    Shape: { macro: { sector: { industry: [basic_industry, ...] } } }
    """
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(NULLIF(macro,''),'—'),
                       COALESCE(NULLIF(sector,''),'—'),
                       COALESCE(NULLIF(industry,''),'—'),
                       COALESCE(NULLIF(basic_industry,''),'—')
                FROM tickers
                WHERE is_active=1 AND macro NOT IN ('','Uncategorized')
                ORDER BY macro, sector, industry, basic_industry
            """)
            tree: dict = {}
            for macro, sector, industry, bi in cur.fetchall():
                tree.setdefault(macro, {}).setdefault(sector, {}).setdefault(industry, set()).add(bi)
        # convert sets → sorted lists
        return {
            m: {s: {i: sorted(bis) for i, bis in inds.items()}
                for s, inds in secs.items()}
            for m, secs in tree.items()
        }
    except Exception as e:
        logger.error("api_filter_tree error: %s", e)
        return JSONResponse({"error": "Database error"}, status_code=500)
    finally:
        db.release_conn(conn)



# ── API: Scanner ──────────────────────────────────────────────────────────────

_scanner_running = False
_scanner_lock    = threading.Lock()


@app.post("/api/run-scanner")
async def api_run_scanner(request: Request):
    """Manually trigger the Daily Hunt scanner in a background thread."""
    global _scanner_running
    with _scanner_lock:
        if _scanner_running:
            return JSONResponse({"ok": False, "message": "Scanner already running."}, status_code=409)
        if price_state.running:
            return JSONResponse({"ok": False, "message": "Price refresh is running — wait for it to finish."}, status_code=409)
        _scanner_running = True

    try:
        body = await request.json()
    except Exception:
        body = {}

    # Reserve a run_id immediately so the frontend can poll the exact run
    run_id = db.scanner_run_start()

    atr_mult   = float(body.get("atr_mult",   scanner_module.DEFAULT_ATR_MULT))
    c_atr_mult = float(body.get("c_atr_mult", scanner_module.DEFAULT_ATR_MULT))

    def _run():
        global _scanner_running
        scanner_result = None
        try:
            logger.info("Manual scanner: Daily Hunt scan starting (run_id=%d, atr_mult=%.2f, c_atr_mult=%.2f)...", run_id, atr_mult, c_atr_mult)
            scanner_result = scanner_module.run_full_scan(run_id=run_id, atr_mult=atr_mult, c_atr_mult=c_atr_mult)
            if scanner_result and scanner_result.get("oldest_scan_date"):
                db.set_app_state("oldest_scan_date", scanner_result["oldest_scan_date"])
            logger.info("Manual scanner: Daily Hunt done — %s", scanner_result)
        except Exception as e:
            logger.error("Manual scanner failed: %s", e)
            db.scanner_run_finish(run_id, 0, 0, 0, 0, "error")
        finally:
            with _scanner_lock:
                _scanner_running = False
        try:
            notifier.send_summary(
                price_total=0,
                price_success=0,
                price_failed=[],
                scanner_result=scanner_result,
            )
        except Exception as e:
            logger.error("Notifier failed after manual scan: %s", e)

    threading.Thread(target=_run, daemon=True).start()
    return JSONResponse({"ok": True, "message": "Scanner started.", "run_id": run_id})


@app.get("/api/scanner-run/{run_id}")
def api_scanner_run(run_id: int):
    """Return progress for a specific scanner run by ID."""
    try:
        run = db.get_scanner_run_by_id(run_id)
        if not run:
            return JSONResponse({"error": "Run not found"}, status_code=404)
        return run
    except Exception as e:
        logger.error("scanner-run error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/scanner-status")
def api_scanner_status():
    """Return last scanner run info."""
    try:
        return db.get_scanner_last_run() or {"status": "never_run"}
    except Exception as e:
        logger.error("scanner-status error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/scanner-progress")
def api_scanner_progress():
    """Return live scanner progress (status, hits_bull, hits_bear, scanned)."""
    try:
        run = db.get_scanner_last_run()
        if not run:
            return {"status": "never_run", "hits_bull": 0, "hits_bear": 0}
        return {
            "status":      run.get("status", "unknown"),
            "scanned":     run.get("scanned", 0),
            "hits_bull":   run.get("hits_bull", 0) or 0,
            "hits_bear":   run.get("hits_bear", 0) or 0,
            "started_at":  run.get("started_at"),
            "finished_at": run.get("finished_at"),
        }
    except Exception as e:
        logger.error("scanner-progress error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/scanner")
async def api_scanner(request: Request):
    """
    Read cached Daily Hunt results from DB, filtered by direction + group selection.
    Body JSON:
    {
      "direction":        "bull" | "bear",
      "macros":           [...],
      "sectors":          [...],
      "industries":       [...],
      "basic_industries": [...]
    }
    Returns instantly — results were pre-computed during last scan.
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    direction        = body.get("direction", "bull")
    macros           = body.get("macros", [])
    sectors          = body.get("sectors", [])
    industries       = body.get("industries", [])
    basic_industries = body.get("basic_industries", [])
    indices          = body.get("indices") or None
    hawks            = body.get("hawks") or None
    symbols          = body.get("symbols") or None

    if direction not in ("bull", "bear"):
        return JSONResponse({"error": "direction must be 'bull' or 'bear'"}, status_code=400)

    try:
        result = scanner_module.get_cached_results(
            direction=direction,
            macros=macros,
            sectors=sectors,
            industries=industries,
            basic_industries=basic_industries,
            indices=indices,
            hawks=hawks,
            symbols=symbols,
        )
        oldest = db.get_app_state("oldest_scan_date")
        if oldest:
            result["oldest_scan_date"] = oldest
        return result
    except Exception as e:
        logger.error("Scanner error: %s", e)
        return JSONResponse({"error": f"Scanner failed: {e}"}, status_code=500)


@app.post("/api/scan-signals")
async def api_scan_signals(request: Request):
    """
    Return pre-computed scan_signals rows, filtered by direction + group/index/hawks.
    Same filter interface as /api/scanner so frontend can reuse the same logic.
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)

    direction        = body.get("direction", "bull")
    macros           = set(body.get("macros", []))
    sectors          = set(body.get("sectors", []))
    industries       = set(body.get("industries", []))
    basic_industries = set(body.get("basic_industries", []))
    indices          = body.get("indices") or None
    hawks            = body.get("hawks") or None
    symbols          = set(body.get("symbols") or []) or None

    try:
        # Reads from scanner_results — the table scanner.run_full_scan()
        # actually populates. This used to read db.get_scan_signals(), which
        # queries the scan_signals table: that table was exclusively written
        # by hourly_scanner.py's "hourly cycle enrichment" pass (see its
        # upsert_scan_signals() callers — there are none left), so since
        # hourly_scanner.py was removed, scan_signals has been a frozen
        # snapshot that nothing refreshes. Only the two signal types actually
        # in use (M-High/Low breakout + Daily Cross) matter here — pullback
        # (S-PB/C-PB) is old, unused code and deliberately not revived.
        rows = db.get_scanner_results(
            direction, list(macros), list(sectors), list(industries), list(basic_industries),
            indices=list(indices) if indices else None,
            hawks=list(hawks) if hawks else None,
            symbols=list(symbols) if symbols else None,
        )
        if not rows:
            return {"results": [], "direction": direction, "as_of": None}

        # Enrich with ticker meta (index_list, hawks, RS scores, 52W H/L)
        # Re-use existing enrichment — needs same shape as scanner_results
        enrich_rows = []
        for r in rows:
            enrich_rows.append({
                "symbol": r["symbol"],
                "direction": r["direction"],
                "m_date": r.get("m_date"),
                "m_price": r.get("m_price"),
                "bull_cross_date": r.get("bull_cross_date"),
                "above_200sma": r.get("above_200sma", 0),
                "_phase_end_date": None,
                "_daily_cross_date": r.get("bull_cross_date"),
            })

        scanner_module._enrich_with_ticker_meta(enrich_rows)

        # Apply group filters
        def _passes(r):
            # Explicit symbol scope — when the frontend passes a filtered set
            # (e.g. Stocks Lens Established results), restrict the scan to exactly
            # those symbols regardless of the group/index/hawks filters.
            if symbols and r.get("symbol") not in symbols: return False
            if macros and r.get("macro") not in macros: return False
            if sectors and r.get("sector") not in sectors: return False
            if industries and r.get("industry") not in industries: return False
            if basic_industries and r.get("basic_industry") not in basic_industries: return False
            if indices:
                idx_set = set(r.get("index_list") or [])
                if not idx_set.intersection(indices): return False
            if hawks:
                hk_names = {h["name"] for h in (r.get("hawks_mappings") or [])}
                if not hk_names.intersection(hawks): return False
            return True

        filtered = [r for r in enrich_rows if _passes(r)]

        # Sort by daily_cross_date DESC (most recent cross first)
        filtered.sort(key=lambda r: r.get("_daily_cross_date") or "", reverse=True)

        # Weekly EMA20 > EMA50 flag + avg daily dollar volume (liquidity filter)
        import sqlite3 as _sq3h
        import datetime as _dt52h
        _wema_map_h: dict = {}
        _avgvol_map_h: dict = {}
        try:
            _hcon = _sq3h.connect("local.db")
            _wema_rows_h = _hcon.execute(
                """SELECT symbol, ema20, ema50 FROM indicator_weekly
                   WHERE (symbol, week) IN (
                       SELECT symbol, MAX(week) FROM indicator_weekly GROUP BY symbol
                   )"""
            ).fetchall()
            for _wr in _wema_rows_h:
                e20, e50 = _wr[1], _wr[2]
                if e20 is not None and e50 is not None:
                    _wema_map_h[_wr[0]] = float(e20) > float(e50)

            _max_date_row = _hcon.execute("SELECT MAX(date) FROM ohlcv_daily").fetchone()
            _as_of_h = _max_date_row[0] if _max_date_row else None
            if _as_of_h:
                _cutoff20h = (
                    _dt52h.date.fromisoformat(str(_as_of_h)[:10]) - _dt52h.timedelta(days=28)
                ).isoformat()
                _vol_rows_h = _hcon.execute(
                    """SELECT symbol, AVG(volume * close) AS dvol FROM ohlcv_daily
                       WHERE date >= ? AND date <= ? AND close IS NOT NULL AND volume IS NOT NULL
                       GROUP BY symbol""",
                    (_cutoff20h, _as_of_h),
                ).fetchall()
                for _vr in _vol_rows_h:
                    if _vr[1] is not None:
                        _avgvol_map_h[_vr[0]] = round(float(_vr[1]), 0)
            _hcon.close()
        except Exception:
            pass

        # Shape final results
        results = []
        for r in filtered:
            results.append({
                "symbol":           r["symbol"],
                "company_name":     r.get("company_name", ""),
                "macro":            r.get("macro", ""),
                "sector":           r.get("sector", ""),
                "industry":         r.get("industry", ""),
                "basic_industry":   r.get("basic_industry", ""),
                "direction":        r["direction"],
                "m_date":           r.get("m_date"),
                "m_price":          r.get("m_price"),
                "daily_cross_date": r.get("_daily_cross_date"),
                "above_200sma":     r.get("above_200sma", 0),
                "phase_end_date":   r.get("_phase_end_date"),
                "index_list":       r.get("index_list", []),
                "hawks_mappings":   r.get("hawks_mappings", []),
                "rs21":             r.get("rs21"),
                "rs21_rank":        r.get("rs21_rank"),
                "rs55":             r.get("rs55"),
                "rs55_rank":        r.get("rs55_rank"),
                "rs252":            r.get("rs252"),
                "rs252_rank":       r.get("rs252_rank"),
                "w52_high":         r.get("w52_high"),
                "w52_low":          r.get("w52_low"),
                "last_close":       r.get("last_close"),
                "w_ema_bull":       _wema_map_h.get(r["symbol"], False),
                "avg_dollar_vol":   _avgvol_map_h.get(r["symbol"]),
            })

        import datetime as _dt
        as_of = _dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        oldest = db.get_app_state("oldest_scan_date")
        return {"results": results, "direction": direction, "as_of": as_of, "oldest_scan_date": oldest}

    except Exception as e:
        logger.error("Scan signals error: %s", e, exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


# ── API: Watchlist sync ───────────────────────────────────────────────────────


@app.get("/api/watchlist")
def api_watchlist_get():
    """Return all saved watchlist symbols."""
    try:
        return {"symbols": db.get_watchlist()}
    except Exception as e:
        logger.error("watchlist get error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/watchlist")
async def api_watchlist_add(request: Request):
    """Add a symbol to the watchlist. Body: { symbol }"""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)
    symbol = (body.get("symbol") or "").strip()
    if not symbol:
        return JSONResponse({"error": "symbol required"}, status_code=400)
    try:
        db.add_to_watchlist(symbol)
        return {"ok": True, "symbol": symbol, "symbols": db.get_watchlist()}
    except Exception as e:
        logger.error("watchlist add error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.delete("/api/watchlist/{symbol:path}")
def api_watchlist_remove(symbol: str):
    """Remove a symbol from the watchlist."""
    symbol = (symbol or "").strip()
    if not symbol:
        return JSONResponse({"error": "symbol required"}, status_code=400)
    try:
        db.remove_from_watchlist(symbol)
        return {"ok": True, "symbol": symbol, "symbols": db.get_watchlist()}
    except Exception as e:
        logger.error("watchlist remove error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


# ── Journal ───────────────────────────────────────────────────────────────────
# A journal entry freezes a one-time snapshot of everything tied to a stock at
# capture time (stock RS scores, 52W Zone, mapped indices, Hunt signals) plus a
# manually-edited trade plan (qty/entry/SL/exits/notes). The snapshot NEVER
# auto-updates with new market data — it is a photograph until deleted/recaptured.
# Index LT/Mid/ST scores are added client-side (that ranking logic lives in JS);
# this builder assembles the pure-SQL stock-level half.

def _build_journal_snapshot(symbol: str) -> dict:
    """Assemble the server-side (stock-level) half of a journal snapshot."""
    import json as _json
    symbol = (symbol or "").strip().upper()
    if not symbol:
        return {}

    snap: dict = {
        "symbol":       symbol,
        "captured_at":  time.strftime("%Y-%m-%dT%H:%M:%S"),
        "data_as_of":   None,
        "identity":     {},
        "stock_rs":     {},
        "w52_zone":     {},
        "mapped_indices": [],   # [{name, source, confidence}] — scores added client-side
        "hunt":         {"daily": None},
    }

    # 1) Stock RS row (latest) + data as-of date
    try:
        rs_rows = db.get_stock_rs_latest([symbol])
        if rs_rows:
            r = rs_rows[0]
            snap["data_as_of"] = r.get("date")
            snap["stock_rs"] = {
                k: (float(r[k]) if isinstance(r.get(k), (int, float)) and k not in (
                    "rs21_rank", "rs55_rank", "rs252_rank") else r.get(k))
                for k in ("rs21", "rs21_rank", "rs55", "rs55_rank", "rs252", "rs252_rank",
                          "day_return")
            }
    except Exception as e:
        logger.warning("journal snapshot rs error %s: %s", symbol, e)

    # 2) Identity + mapped indices, from tickers + sector_index_map
    try:
        conn = db.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT company_name, macro, sector, industry, basic_industry "
                    "FROM tickers WHERE symbol = %s", (symbol,),
                )
                row = cur.fetchone()
                if row:
                    snap["identity"] = {
                        "company_name": row[0], "macro": row[1], "sector": row[2],
                        "industry": row[3], "basic_industry": row[4],
                    }
                cur.execute(
                    """SELECT index_name, source, confidence FROM sector_index_map
                       WHERE symbol = %s
                       ORDER BY CASE source WHEN 'official' THEN 0 ELSE 1 END,
                                CASE confidence WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END,
                                index_name""",
                    (symbol,),
                )
                snap["mapped_indices"] = [
                    {"name": n, "source": s, "confidence": c} for n, s, c in cur.fetchall()
                ]
        finally:
            db.release_conn(conn)
    except Exception as e:
        logger.warning("journal snapshot meta error %s: %s", symbol, e)

    # 3) 52W Zone — max(high)/min(low)/last close over last 252 trading bars
    try:
        conn = db.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """SELECT MAX(high), MIN(low),
                              MAX(CASE WHEN rn=1 THEN close END)
                       FROM (
                           SELECT high, low, close,
                                  ROW_NUMBER() OVER (ORDER BY date DESC) AS rn
                           FROM ohlcv_daily WHERE symbol = %s AND close > 0
                       ) t WHERE rn <= 252""",
                    (symbol,),
                )
                z = cur.fetchone()
                if z and z[0] is not None:
                    h, l, c = float(z[0]), float(z[1]), float(z[2])
                    snap["w52_zone"] = {
                        "high": h, "low": l, "last_close": c,
                        "pct_above_low":  round((c / l - 1) * 100, 2) if l else None,
                        "pct_below_high": round((1 - c / h) * 100, 2) if h else None,
                        "pass_above_low":  (c >= l * 1.30) if l else None,
                        "pass_below_high": (c >= h * 0.75) if h else None,
                        "pass": (c >= l * 1.30 and c >= h * 0.75) if (l and h) else None,
                    }
        finally:
            db.release_conn(conn)
    except Exception as e:
        logger.warning("journal snapshot 52w error %s: %s", symbol, e)

    # 4) Hunt signals — is the stock currently in Daily Hunt?
    for direction in ("bull", "bear"):
        try:
            d_rows = db.get_scanner_results(direction, [], [], [], [], symbols=[symbol])
            if d_rows:
                dr = d_rows[0]
                snap["hunt"]["daily"] = {
                    "direction": direction, "m_date": dr.get("m_date"),
                    "s_pullback_date": dr.get("s_pullback_date"),
                    "c_pullback_date": dr.get("c_pullback_date"),
                    "above_200sma": bool(dr.get("above_200sma")),
                }
                break
        except Exception:
            pass

    return snap


@app.get("/api/journal/snapshot/{symbol:path}")
def api_journal_snapshot(symbol: str):
    """Build (but do NOT store) the server-side snapshot half for a symbol.
    The client enriches mapped_indices with LT/Mid/ST scores before saving."""
    try:
        snap = _build_journal_snapshot(symbol)
        if not snap.get("identity") and not snap.get("stock_rs"):
            return JSONResponse({"error": f"No data found for {symbol}"}, status_code=404)
        return snap
    except Exception as e:
        logger.error("journal snapshot error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/journal")
def api_journal_list():
    """Return all journal entries (newest first)."""
    try:
        return {"entries": db.list_journal_entries()}
    except Exception as e:
        logger.error("journal list error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/journal/{entry_id:int}")
def api_journal_get(entry_id: int):
    """Return a single journal entry by trade id."""
    try:
        e = db.get_journal_entry(entry_id)
        if not e:
            return JSONResponse({"error": "not found"}, status_code=404)
        return e
    except Exception as ex:
        logger.error("journal get error: %s", ex)
        return JSONResponse({"error": str(ex)}, status_code=500)


@app.post("/api/journal")
async def api_journal_create(request: Request):
    """Create a journal entry. Body: { symbol, snapshot, plan? }.
    snapshot is the client-assembled frozen blob (server half + index scores)."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)
    symbol = (body.get("symbol") or "").strip().upper()
    snapshot = body.get("snapshot")
    if not symbol or not isinstance(snapshot, dict):
        return JSONResponse({"error": "symbol and snapshot required"}, status_code=400)
    try:
        # Always create a NEW trade — a repeat symbol gets its own fresh entry,
        # never reopening a prior trade for that symbol.
        new_id = db.create_journal_entry(symbol, snapshot, body.get("plan") or {}, "open")
        return {"ok": True, "entry": db.get_journal_entry(new_id)}
    except Exception as e:
        logger.error("journal create error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.patch("/api/journal/{entry_id:int}")
async def api_journal_patch(entry_id: int, request: Request):
    """Update the editable trade plan (and optionally status). Snapshot untouched.
    Body: { plan, status? }"""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)
    if not db.get_journal_entry(entry_id):
        return JSONResponse({"error": "not found"}, status_code=404)
    try:
        db.update_journal_plan(entry_id, body.get("plan") or {}, body.get("status"))
        return {"ok": True, "entry": db.get_journal_entry(entry_id)}
    except Exception as e:
        logger.error("journal patch error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/journal/{entry_id:int}/recapture")
async def api_journal_recapture(entry_id: int, request: Request):
    """Deliberately overwrite the frozen snapshot. Body: { snapshot }"""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON body"}, status_code=400)
    snapshot = body.get("snapshot")
    if not isinstance(snapshot, dict):
        return JSONResponse({"error": "snapshot required"}, status_code=400)
    if not db.get_journal_entry(entry_id):
        return JSONResponse({"error": "not found"}, status_code=404)
    try:
        db.recapture_journal_snapshot(entry_id, snapshot)
        return {"ok": True, "entry": db.get_journal_entry(entry_id)}
    except Exception as e:
        logger.error("journal recapture error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.delete("/api/journal/{entry_id:int}")
def api_journal_delete(entry_id: int):
    """Delete a single journal entry by its trade id."""
    try:
        db.delete_journal_entry(entry_id)
        return {"ok": True, "id": entry_id}
    except Exception as e:
        logger.error("journal delete error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/ibd-rs-rank-history")
def api_ibd_rs_rank_history(days: int = 504, granularity: str = "monthly", scores_only: bool = False):
    """
    Return per-symbol RS raw history across the last `days` dates.
    granularity: "daily" | "weekly" | "monthly" (default: monthly)
    monthly = last trading day of each calendar month (~24 bars for 2Y)
    weekly  = last trading day of each ISO week    (~104 bars for 2Y)
    Shape: { dates: [...], raw: { symbol: [rs_raw|null, ...] } }
    When granularity=weekly, also includes:
      closes: { symbol: [weekly_close, ...] }  aligned to the same weekly dates,
      with enough warmup history (2200 days of daily data) for 200-week SMA.
    """
    import datetime as _dt
    _ck = _index_rs_fp()
    _cname = f"ibd_rs_rank_history:{days}:{granularity}:{'so' if scores_only else 'full'}"
    _cached = _cache_get(_cname, _ck)
    if _cached is not None:
        return _cached
    try:
        ind_rows = []   # indicator_weekly rows (weekly EMA20/50/SMA200/RS line)
        if _DB_MODE == "local":
            import sqlite3 as _sq3
            _lcon = _sq3.connect("local.db")
            dates = [r[0] for r in _lcon.execute(
                "SELECT DISTINCT date FROM index_rs WHERE rs_raw IS NOT NULL ORDER BY date DESC LIMIT ?", (days,)
            ).fetchall()]
            if not dates:
                return {"dates": [], "raw": {}}
            ph = ",".join(["?"] * len(dates))
            rows = _lcon.execute(
                f"SELECT symbol, date, rs_raw, group_lt_score FROM index_rs WHERE date IN ({ph})", dates
            ).fetchall()
            price_rows = []
            if granularity == "weekly" and not scores_only:
                price_rows = _lcon.execute(
                    """SELECT symbol, week, open, high, low, close, volume
                       FROM ohlcv_weekly
                       WHERE symbol IN (SELECT DISTINCT symbol FROM index_rs)
                       ORDER BY symbol, week ASC"""
                ).fetchall()
                ind_rows = _lcon.execute(
                    """SELECT symbol, week, ema20, ema50, sma200, rs_line
                       FROM indicator_weekly
                       WHERE symbol IN (SELECT DISTINCT symbol FROM index_rs)
                       ORDER BY symbol, week ASC"""
                ).fetchall()
            _lcon.close()
        else:
            conn = db.get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT DISTINCT date::text FROM index_rs WHERE rs_raw IS NOT NULL ORDER BY date DESC LIMIT %s",
                        (days,)
                    )
                    dates = [r[0] for r in cur.fetchall()]
                    if not dates:
                        return {"dates": [], "raw": {}}
                    cur.execute(
                        "SELECT symbol, date::text, rs_raw, group_lt_score FROM index_rs WHERE date::text = ANY(%s)",
                        (dates,)
                    )
                    rows = cur.fetchall()
                    price_rows = []
                    if granularity == "weekly" and not scores_only:
                        cur.execute(
                            """SELECT symbol, week::text, open, high, low, close, volume
                               FROM ohlcv_weekly
                               WHERE symbol IN (SELECT DISTINCT symbol FROM index_rs)
                               ORDER BY symbol, week ASC"""
                        )
                        price_rows = cur.fetchall()
                        cur.execute(
                            """SELECT symbol, week::text, ema20, ema50, sma200, rs_line
                               FROM indicator_weekly
                               WHERE symbol IN (SELECT DISTINCT symbol FROM index_rs)
                               ORDER BY symbol, week ASC"""
                        )
                        ind_rows = cur.fetchall()
            finally:
                db.release_conn(conn)

        by_sym: dict = {}
        grp_lt_by_sym: dict = {}
        for sym, date, raw, grp_lt in rows:
            if raw is not None:
                by_sym.setdefault(sym, {})[date] = float(raw)
            if grp_lt is not None:
                grp_lt_by_sym.setdefault(sym, {})[date] = int(grp_lt)

        dates_asc = list(reversed(dates))

        if granularity == "monthly":
            # Keep last trading day per calendar month (YYYY-MM)
            month_map: dict[str, str] = {}
            for d in dates_asc:
                key = d[:7]          # "YYYY-MM"
                month_map[key] = d   # later dates overwrite, so last day wins
            display_dates = [month_map[k] for k in sorted(month_map)]

        elif granularity == "weekly":
            # Skip phantom non-trading dates (written by compute_and_store_rs on
            # weekends/holidays) — they have rs_raw but no group_lt_score.
            # grp_lt_dates is already in memory — no extra DB query needed.
            grp_lt_dates = {d for dm in grp_lt_by_sym.values() for d in dm}
            week_map: dict[str, str] = {}
            for d in dates_asc:
                if d not in grp_lt_dates:
                    continue  # skip phantom dates that have no group_lt_score
                iso = _dt.date.fromisoformat(d).isocalendar()
                key = f"{iso[0]}-W{iso[1]:02d}"
                week_map[key] = d  # later real trading day in same week wins
            display_dates = [week_map[k] for k in sorted(week_map)]

        else:
            display_dates = dates_asc

        raw_out = {
            sym: [date_map.get(d) for d in display_dates]
            for sym, date_map in by_sym.items()
        }
        grp_lt_out = {
            sym: [date_map.get(d) for d in display_dates]
            for sym, date_map in grp_lt_by_sym.items()
        }
        result: dict = {"dates": display_dates, "raw": raw_out, "grp_lt": grp_lt_out}

        # Build weekly OHLCV arrays from pre-computed ohlcv_weekly
        if granularity == "weekly" and price_rows:
            from collections import defaultdict as _dd
            sym_weekly: dict = _dd(list)
            for sym, week_str, o, h, l, close, vol in price_rows:
                d = _dt.date.fromisoformat(str(week_str)[:10])
                iso = d.isocalendar()
                wk_key = f"{iso[0]}-W{iso[1]:02d}"
                sym_weekly[sym].append((wk_key, float(o or 0), float(h or 0), float(l or 0), float(close), float(vol or 0)))

            closes_out: dict = {}
            ohlcv_out:  dict = {}
            for sym, wk_rows in sym_weekly.items():
                wk_rows_sorted = sorted(wk_rows, key=lambda x: x[0])
                closes_out[sym] = [r[4] for r in wk_rows_sorted]
                ohlcv_out[sym]  = {
                    "o": [round(r[1], 2) for r in wk_rows_sorted],
                    "h": [round(r[2], 2) for r in wk_rows_sorted],
                    "l": [round(r[3], 2) for r in wk_rows_sorted],
                    "v": [int(r[5])       for r in wk_rows_sorted],
                }
            result["closes"] = closes_out
            result["ohlcv"]  = ohlcv_out

        if granularity == "weekly" and ind_rows:
            from collections import defaultdict as _dd2
            sym_ind: dict = _dd2(list)
            for sym, week_str, e20, e50, s200, rs in ind_rows:
                sym_ind[sym].append((str(week_str)[:10], e20, e50, s200, rs))
            indicators_out: dict = {}
            for sym, wrows in sym_ind.items():
                wrows_sorted = sorted(wrows, key=lambda x: x[0])
                indicators_out[sym] = {
                    "e20":  [r[1] for r in wrows_sorted],
                    "e50":  [r[2] for r in wrows_sorted],
                    "s200": [r[3] for r in wrows_sorted],
                    "rs":   [r[4] for r in wrows_sorted],
                }
            result["indicators"] = indicators_out

        _cache_put(_cname, _ck, result)
        return result
    except Exception as e:
        logger.error("ibd-rs-rank-history error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)

_SWING_BENCHMARK = prices_module.BENCHMARK_SYMBOL
_SWING_CHART_BARS = 52    # ~2.5 months of trading days for chart bars


@app.get("/api/swing-radar")
def api_swing_radar():
    """
    Swing Radar — latest swing scores for all sector/broad-market indices.
    Pure DB read: index_rs (scores + rs21/rs55/day_return/week_return/close),
    indicator_daily (ema20, rs_line), ohlcv_daily (OHLCV chart bars only).
    Zero computation at request time.
    """
    # Key on index_rs (group scores + RS values) not just ohlcv_daily, so that
    # a price refresh which updates group_short/mid/lt scores in index_rs but
    # doesn't add new OHLCV bars still invalidates the cache and shows fresh scores.
    _ck = _index_rs_fp()
    _cached = _cache_get("swing_radar", _ck)
    if _cached is not None:
        return _cached

    import sqlite3 as _sq
    try:
        con = _sq.connect("local.db")
        con.row_factory = _sq.Row

        # All pre-computed scores + metrics from index_rs.
        # Latest row per symbol. Resolve the "best" date for each symbol in Python
        # from two cheap single-row queries, then fetch only the matching rows by
        # exact (symbol, date) — avoids a COALESCE(MAX(CASE WHEN...)) full scan.
        grp_date = con.execute(
            "SELECT MAX(date) FROM index_rs WHERE group_short_score IS NOT NULL"
        ).fetchone()[0]
        abs_date = con.execute(
            "SELECT MAX(date) FROM index_rs"
        ).fetchone()[0]
        # Use whichever is later (normally the same date after backfill runs)
        best_date = grp_date if grp_date else abs_date
        rs_rows = con.execute(
            """SELECT symbol, date,
                      rs_raw, rs_rating, mid_score, short_score,
                      rs21, rs55, day_return, week_return, close,
                      group_short_score, group_mid_score, group_lt_score
               FROM index_rs WHERE date = ?""",
            (best_date,),
        ).fetchall()

        as_of = rs_rows[0]["date"] if rs_rows else None

        # Resolve the exact set of index symbols from the already-fetched rs_rows.
        # Using IN(~15 symbols) lets SQLite do per-symbol index seeks instead of a
        # full-table scan — 400ms → <15ms.
        from datetime import date as _date, timedelta as _td
        _idx_syms = [r["symbol"] for r in rs_rows]
        _ph = ",".join(["?"] * len(_idx_syms))

        # Latest EMA20: exact date lookup via IN
        ind_max_date = con.execute(
            f"SELECT MAX(date) FROM indicator_daily WHERE symbol IN ({_ph})", _idx_syms
        ).fetchone()[0]
        ind_rows = con.execute(
            f"SELECT symbol, ema20 FROM indicator_daily WHERE symbol IN ({_ph}) AND date = ?",
            _idx_syms + [ind_max_date],
        ).fetchall() if ind_max_date else []

        # RS line history — last 100 calendar days, IN lookup per symbol.
        # Display trims to 63 non-null values per symbol; data unchanged.
        _rs_cutoff = str((_date.fromisoformat(ind_max_date) - _td(days=100)).isoformat()) if ind_max_date else "2000-01-01"
        rs_line_rows = con.execute(
            f"""SELECT symbol, rs_line FROM indicator_daily
                WHERE symbol IN ({_ph})
                  AND rs_line IS NOT NULL
                  AND date >= ?
                ORDER BY symbol, date ASC""",
            _idx_syms + [_rs_cutoff],
        ).fetchall()

        # OHLCV chart bars — IN lookup, last 80 calendar days (covers 52 trading days).
        _ohlcv_max = con.execute(
            f"SELECT MAX(date) FROM ohlcv_daily WHERE symbol IN ({_ph})", _idx_syms
        ).fetchone()[0]
        _ohlcv_cutoff = str((_date.fromisoformat(_ohlcv_max) - _td(days=80)).isoformat()) if _ohlcv_max else "2000-01-01"
        ohlcv_rows = con.execute(
            f"""SELECT symbol, open, high, low, close, volume
                FROM ohlcv_daily
                WHERE symbol IN ({_ph})
                  AND date >= ?
                ORDER BY symbol, date ASC""",
            _idx_syms + [_ohlcv_cutoff],
        ).fetchall()

        con.close()
    except Exception as e:
        logger.error("swing-radar DB error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)

    from collections import defaultdict
    from index_data import get_info

    ind_map = {r["symbol"]: dict(r) for r in ind_rows}

    rs_line_by_sym: dict = defaultdict(list)
    for r in rs_line_rows:
        rs_line_by_sym[r["symbol"]].append(r["rs_line"])

    ohlcv_by_sym: dict = defaultdict(list)
    for r in ohlcv_rows:
        ohlcv_by_sym[r["symbol"]].append(r)

    out_indices = []
    for rs in rs_rows:
        sym  = rs["symbol"]
        info = get_info(sym)
        if not info:
            continue

        ind        = ind_map.get(sym, {})
        ohlcv_bars = ohlcv_by_sym.get(sym, [])
        chart_bars = ohlcv_bars[-_SWING_CHART_BARS:]

        chart = {
            "c":    [round(float(b["close"]), 2) for b in chart_bars],
            "o":    [round(float(b["open"]),  2) for b in chart_bars],
            "h":    [round(float(b["high"]),  2) for b in chart_bars],
            "l":    [round(float(b["low"]),   2) for b in chart_bars],
            "v":    [int(b["volume"] or 0)        for b in chart_bars],
            "e20":  [],
            "e50":  [],
            "s200": [],
        }

        swing_score = rs["short_score"] or 50

        out_indices.append({
            "symbol":      sym,
            "name":        info.name,
            "category":    info.category,
            "close":       round(float(rs["close"]), 2) if rs["close"] is not None else None,
            "close_21":    None,
            "close_55":    None,
            "day_return":  round(float(rs["day_return"]),  2) if rs["day_return"]  is not None else 0.0,
            "week_return": round(float(rs["week_return"]), 2) if rs["week_return"] is not None else 0.0,
            "ema20":       ind.get("ema20"),
            "ema_slope":   0.0,
            "ema_up":      False,
            "rs21":        round(float(rs["rs21"]), 3) if rs["rs21"] is not None else None,
            "rs55":        round(float(rs["rs55"]), 3) if rs["rs55"] is not None else None,
            "swing_score": swing_score,
            "chart":       chart,
            "rs_line":     rs_line_by_sym.get(sym, [])[-63:],
            "lt_score":         rs["rs_rating"],
            "mid_score":        rs["mid_score"],
            "short_score":      rs["short_score"],
            "group_short_score": rs["group_short_score"],
            "group_mid_score":   rs["group_mid_score"],
            "group_lt_score":    rs["group_lt_score"],
        })

    out_indices.sort(key=lambda x: x["swing_score"], reverse=True)

    _payload = {
        "as_of":      as_of or "",
        "bench_rs21": 0.0,
        "indices":    out_indices,
    }
    _cache_put("swing_radar", _ck, _payload)
    return _payload



@app.get("/api/swing-radar-history")
def api_swing_radar_history(days: int = 30):
    """
    Return per-symbol swing_score (short_score) and rs55 history for the last `days` trading dates.
    Reads directly from index_rs (pre-computed short_score/mid_score/rs_raw).
    Shape: { dates: [...], scores: { symbol: [score|null, ...] }, rs55: { symbol: [val|null, ...] } }
    """
    _ck = _index_rs_fp()
    _cname = f"swing_radar_history:{days}"
    _cached = _cache_get(_cname, _ck)
    if _cached is not None:
        return _cached

    import sqlite3 as _sq
    try:
        con = _sq.connect("local.db")
        con.row_factory = _sq.Row
        rows = con.execute(
            """SELECT symbol, date, rs21, rs55, group_short_score, group_mid_score, group_lt_score
               FROM index_rs
               WHERE rs21 IS NOT NULL
                 AND date >= (SELECT DATE(MAX(date), ? || ' days') FROM index_rs WHERE rs21 IS NOT NULL)
               ORDER BY symbol, date ASC""",
            (f"-{days * 2}",),
        ).fetchall()
        con.close()
    except Exception as e:
        logger.error("swing-radar-history DB error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)

    from collections import defaultdict
    rs21_by_sym:        dict = defaultdict(dict)
    rs55_by_sym:        dict = defaultdict(dict)
    grp_short_by_sym:   dict = defaultdict(dict)
    grp_mid_by_sym:     dict = defaultdict(dict)
    grp_lt_by_sym:      dict = defaultdict(dict)
    all_dates_set: set = set()

    for r in rows:
        sym = r["symbol"]
        d   = str(r["date"])[:10]
        all_dates_set.add(d)
        rs21_by_sym[sym][d]      = r["rs21"]
        rs55_by_sym[sym][d]      = r["rs55"]
        grp_short_by_sym[sym][d] = r["group_short_score"]
        grp_mid_by_sym[sym][d]   = r["group_mid_score"]
        grp_lt_by_sym[sym][d]    = r["group_lt_score"]

    # Only keep dates where at least some group scores are populated
    valid_dates = {d for d in all_dates_set
                   if any(grp_short_by_sym[sym].get(d) is not None for sym in grp_short_by_sym)}
    display_dates = sorted(valid_dates)[-days:]
    all_syms = list(rs21_by_sym.keys())

    def _to_series(by_sym, syms, dates):
        return {
            sym: [by_sym[sym].get(d) for d in dates]
            for sym in syms
            if any(by_sym[sym].get(d) is not None for d in dates)
        }

    _payload = {
        "dates":     display_dates,
        "scores":    _to_series(rs21_by_sym,     all_syms, display_dates),
        "rs55":      _to_series(rs55_by_sym,     all_syms, display_dates),
        "grp_short": _to_series(grp_short_by_sym, all_syms, display_dates),
        "grp_mid":   _to_series(grp_mid_by_sym,   all_syms, display_dates),
        "grp_lt":    _to_series(grp_lt_by_sym,    all_syms, display_dates),
    }
    _cache_put(_cname, _ck, _payload)
    return _payload


# ── Hawks Radar — stock RS21 / RS55 / RS252 from pre-computed cache ──────────

@app.get("/api/stock-rs")
def api_stock_rs(symbols: str = ""):
    """
    Return latest pre-computed RS scores from stock_rs_daily, joined with ticker meta
    and hawks_mappings. No OHLCV/chart data — charts fetched on-demand via /api/stock-rs-charts.
    """
    sym_list = [s.strip() for s in symbols.split(",") if s.strip()] if symbols else None

    # Cache only the full-universe load (no symbols filter) — the heavy default that
    # the Stocks Lens page fires every visit. Filtered calls stay uncached.
    # Key folds the RS table AND the meta tables (tickers + sector_index_map),
    # since the payload joins company/sector/hawks-mapping from them — so any change
    # to either source invalidates this cache.
    _ck = None
    if sym_list is None:
        _rsfp = _stock_rs_fp()
        _metafp = _latest_tickers_key()
        _ck = f"{_rsfp}::{_metafp}" if (_rsfp and _metafp) else None
    if sym_list is None:
        _cached = _cache_get_response("stock_rs", _ck)
        if _cached is not None:
            return _cached

    try:
        rows = db.get_stock_rs_latest(sym_list)
    except Exception as e:
        logger.error("api_stock_rs DB error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)

    if not rows:
        return {"as_of": None, "stocks": []}

    as_of = rows[0]["date"] if rows else None
    syms  = [r["symbol"] for r in rows]

    try:
        conn = db.get_conn()
        try:
            with conn.cursor() as cur:
                ph = ",".join(["%s"] * len(syms))
                cur.execute(
                    f"SELECT symbol, company_name, macro, sector, industry, basic_industry, index_list FROM tickers WHERE symbol IN ({ph}) AND is_active=1",
                    syms,
                )
                meta = {}
                import json as _json
                for r in cur.fetchall():
                    il = r[6]
                    if isinstance(il, str):
                        try: il = _json.loads(il)
                        except Exception: il = []
                    elif il is None:
                        il = []
                    meta[r[0]] = {"company_name": r[1], "macro": r[2], "sector": r[3], "industry": r[4], "basic_industry": r[5], "index_list": il}

                cur.execute(
                    f"""SELECT symbol, index_name, source, confidence
                        FROM sector_index_map WHERE symbol IN ({ph})
                        ORDER BY symbol,
                          CASE source WHEN 'official' THEN 0 ELSE 1 END,
                          CASE confidence WHEN 'high' THEN 0 WHEN 'medium' THEN 1 ELSE 2 END,
                          index_name""",
                    syms,
                )
                hawks_map: dict = {}
                for sym, idx_name, src, conf in cur.fetchall():
                    hawks_map.setdefault(sym, []).append(
                        {"name": idx_name, "source": src, "confidence": conf}
                    )
        finally:
            db.release_conn(conn)
    except Exception as e:
        logger.error("api_stock_rs meta fetch error: %s", e)
        meta = {}
        hawks_map = {}

    # Fetch last_close + 52W high/low from ohlcv_daily for the Lens 52W Zone filter.
    # last_close: exact date=as_of lookup (7ms). 52W range: date >= cutoff scan (850ms).
    # Both run only on cold cache (once per price refresh), not on every page load.
    import sqlite3 as _sq3
    import datetime as _dt52
    _ocon = _sq3.connect("local.db")
    _ocon.row_factory = _sq3.Row
    _close_map: dict = {}
    _w52_map:   dict = {}
    _w_ema_bull_map: dict = {}
    _avgvol_map: dict = {}
    try:
        _close_rows = _ocon.execute(
            "SELECT symbol, close FROM ohlcv_daily WHERE date = ?", (as_of,)
        ).fetchall()
        for _cr in _close_rows:
            if _cr["close"] is not None:
                _close_map[_cr["symbol"]] = round(float(_cr["close"]), 2)

        _cutoff52 = (
            _dt52.date.fromisoformat(str(as_of)[:10]) - _dt52.timedelta(days=365)
        ).isoformat()
        _w52_rows = _ocon.execute(
            "SELECT symbol, MAX(high) AS w52h, MIN(low) AS w52l FROM ohlcv_daily WHERE date >= ? GROUP BY symbol",
            (_cutoff52,),
        ).fetchall()
        for _wr in _w52_rows:
            if _wr["w52h"] is not None and _wr["w52l"] is not None:
                _w52_map[_wr["symbol"]] = (round(float(_wr["w52h"]), 2), round(float(_wr["w52l"]), 2))

        # Weekly EMA20 > EMA50 flag from latest week in indicator_weekly
        _wema_rows = _ocon.execute(
            """SELECT symbol, ema20, ema50 FROM indicator_weekly
               WHERE (symbol, week) IN (
                   SELECT symbol, MAX(week) FROM indicator_weekly GROUP BY symbol
               )"""
        ).fetchall()
        for _wr in _wema_rows:
            e20, e50 = _wr[1], _wr[2]
            if e20 is not None and e50 is not None:
                _w_ema_bull_map[_wr[0]] = float(e20) > float(e50)

        # Avg daily dollar volume (~20 trading days, approximated via a 28-calendar-day
        # window) — liquidity filter for swing trading. volume*close per bar, averaged.
        _cutoff20 = (
            _dt52.date.fromisoformat(str(as_of)[:10]) - _dt52.timedelta(days=28)
        ).isoformat()
        _vol_rows = _ocon.execute(
            """SELECT symbol, AVG(volume * close) AS dvol FROM ohlcv_daily
               WHERE date >= ? AND date <= ? AND close IS NOT NULL AND volume IS NOT NULL
               GROUP BY symbol""",
            (_cutoff20, as_of),
        ).fetchall()
        for _vr in _vol_rows:
            if _vr["dvol"] is not None:
                _avgvol_map[_vr["symbol"]] = round(float(_vr["dvol"]), 0)
    finally:
        _ocon.close()

    out_stocks = []
    for r in rows:
        sym  = r["symbol"]
        info = meta.get(sym)
        if info is None:
            continue  # inactive/delisted — excluded from tickers meta join above
        _w52 = _w52_map.get(sym)
        out_stocks.append({
            "symbol":         sym,
            "company_name":   info.get("company_name", sym),
            "macro":          info.get("macro", ""),
            "sector":         info.get("sector", ""),
            "industry":       info.get("industry", ""),
            "basic_industry": info.get("basic_industry", ""),
            "index_list":     info.get("index_list", []),
            "hawks_mappings": hawks_map.get(sym, []),
            "date":           r["date"],
            "rs21":           float(r["rs21"]) if r["rs21"] is not None else None,
            "rs21_rank":      r["rs21_rank"],
            "rs55":           float(r["rs55"]) if r["rs55"] is not None else None,
            "rs55_rank":      r["rs55_rank"],
            "day_return":     float(r["day_return"]) if r.get("day_return") is not None else None,
            "rs252":          float(r["rs252"]) if r.get("rs252") is not None else None,
            "rs252_rank":     r.get("rs252_rank"),
            "last_close":     _close_map.get(sym),
            "w52_high":       _w52[0] if _w52 else None,
            "w52_low":        _w52[1] if _w52 else None,
            "w_ema_bull":     _w_ema_bull_map.get(sym, False),
            "avg_dollar_vol": _avgvol_map.get(sym),
        })

    _payload = {"as_of": as_of, "stocks": out_stocks}
    if sym_list is None:
        return _cache_put_response("stock_rs", _ck, _payload)
    return _payload


@app.get("/api/stock-rs-charts")
def api_stock_rs_charts(symbols: str = ""):
    """
    Return daily chart OHLCV + pre-computed EMA20/50/SMA200 + RS line for up to 50 symbols.
    Reads indicators from indicator_daily; OHLCV from ohlcv_daily (display window only).
    """
    sym_list = [s.strip() for s in symbols.split(",") if s.strip()]
    if not sym_list:
        return JSONResponse({"error": "symbols parameter required"}, status_code=400)

    import sqlite3 as _sq

    try:
        con = _sq.connect("local.db")
        con.row_factory = _sq.Row
        ph = ",".join(["?"] * len(sym_list))

        price_rows = con.execute(
            f"""SELECT symbol, date, open, high, low, close, volume
                FROM ohlcv_daily
                WHERE symbol IN ({ph})
                ORDER BY symbol, date ASC""",
            sym_list,
        ).fetchall()

        ind_rows = con.execute(
            f"""SELECT symbol, date, ema20, ema50, sma200, rs_line
                FROM indicator_daily
                WHERE symbol IN ({ph})
                ORDER BY symbol, date ASC""",
            sym_list,
        ).fetchall()

        con.close()
    except Exception as e:
        logger.error("api_stock_rs_charts error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)

    from collections import defaultdict as _dd
    sym_prices: dict = _dd(list)
    for r in price_rows:
        sym_prices[r["symbol"]].append(r)

    sym_ind: dict = _dd(list)
    for r in ind_rows:
        sym_ind[r["symbol"]].append(r)

    out: dict = {}
    for sym in sym_list:
        prices = sym_prices.get(sym, [])
        inds   = sym_ind.get(sym, [])
        if not prices:
            out[sym] = {"chart": None, "rs_line": []}
            continue

        # Align indicators to price dates (both are sorted by date asc, same window)
        ind_by_date = {str(r["date"])[:10]: r for r in inds}

        dates  = [str(r["date"])[:10] for r in prices]
        chart  = {
            "o":     [round(float(r["open"]),   2) for r in prices],
            "h":     [round(float(r["high"]),   2) for r in prices],
            "l":     [round(float(r["low"]),    2) for r in prices],
            "c":     [round(float(r["close"]),  2) for r in prices],
            "v":     [int(r["volume"] or 0)        for r in prices],
            "dates": dates,
            "e20":   [round(ind_by_date[d]["ema20"],  2) if d in ind_by_date and ind_by_date[d]["ema20"]  is not None else None for d in dates],
            "e50":   [round(ind_by_date[d]["ema50"],  2) if d in ind_by_date and ind_by_date[d]["ema50"]  is not None else None for d in dates],
            "s200":  [round(ind_by_date[d]["sma200"], 2) if d in ind_by_date and ind_by_date[d]["sma200"] is not None else None for d in dates],
        }
        rs_line = [
            round(ind_by_date[d]["rs_line"], 6) if d in ind_by_date and ind_by_date[d]["rs_line"] is not None else None
            for d in dates
        ]
        out[sym] = {"chart": chart, "rs_line": rs_line}

    return out


@app.get("/api/stock-rs-history")
def api_stock_rs_history(symbols: str = "", days: int = 30):
    """
    Return per-symbol RS21/RS55/RS252 rank history for the last `days` trading dates.
    symbols: comma-separated list (required — don't load all 2100 for history).
    Shape: { dates: [...], rs21_rank: {sym: [...]}, rs55_rank: {sym: [...]}, rs252_rank: {sym: [...]} }
    """
    sym_list = [s.strip() for s in symbols.split(",") if s.strip()]
    if not sym_list:
        return JSONResponse({"error": "symbols parameter required"}, status_code=400)

    try:
        history = db.get_stock_rs_history(sym_list, days=days)
    except Exception as e:
        logger.error("api_stock_rs_history error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)

    all_dates: set = set()
    for sym_hist in history.values():
        all_dates.update(sym_hist.keys())
    sorted_dates = sorted(all_dates)[-days:]

    rs21_rank_out:  dict = {}
    rs55_rank_out:  dict = {}
    rs252_rank_out: dict = {}
    for sym in sym_list:
        sym_hist = history.get(sym, {})
        rs21_rank_out[sym]  = [sym_hist.get(d, {}).get("rs21_rank")  for d in sorted_dates]
        rs55_rank_out[sym]  = [sym_hist.get(d, {}).get("rs55_rank")  for d in sorted_dates]
        rs252_rank_out[sym] = [sym_hist.get(d, {}).get("rs252_rank") for d in sorted_dates]

    return {
        "dates":       sorted_dates,
        "rs21_rank":   rs21_rank_out,
        "rs55_rank":   rs55_rank_out,
        "rs252_rank":  rs252_rank_out,
    }


@app.get("/api/trading-date-cutoff")
def api_trading_date_cutoff(days: int = 45):
    """Return the oldest trading date for the last N trading days from the S&P 500 benchmark series."""
    days = max(1, min(90, days))
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT date FROM ohlcv_daily WHERE symbol=%s ORDER BY date DESC LIMIT %s",
                (prices_module.BENCHMARK_SYMBOL, days),
            )
            rows = cur.fetchall()
    finally:
        db.release_conn(conn)
    cutoff = str(rows[-1][0]) if rows else "1970-01-01"
    return {"cutoff": cutoff, "days": days}


@app.get("/api/settings")
async def api_get_settings():
    atr     = db.get_app_state("atr_mult")
    c_atr   = db.get_app_state("c_atr_mult")
    days    = db.get_app_state("scan_days")
    h_atr   = db.get_app_state("h_atr_mult")
    h_c_atr = db.get_app_state("h_c_atr_mult")
    h_days  = db.get_app_state("h_scan_days")
    return JSONResponse({
        "atr_mult":     float(atr)     if atr     else 0.5,
        "c_atr_mult":   float(c_atr)   if c_atr   else 0.5,
        "scan_days":    int(days)       if days     else 30,
        "h_atr_mult":   float(h_atr)   if h_atr   else 0.5,
        "h_c_atr_mult": float(h_c_atr) if h_c_atr else 0.5,
        "h_scan_days":  int(h_days)    if h_days   else 30,
    })


@app.post("/api/settings")
async def api_save_settings(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)
    if "atr_mult" in body:
        val = max(0.1, min(3.0, float(body["atr_mult"])))
        db.set_app_state("atr_mult", str(val))
    if "c_atr_mult" in body:
        val = max(0.1, min(3.0, float(body["c_atr_mult"])))
        db.set_app_state("c_atr_mult", str(val))
    if "scan_days" in body:
        val = max(1, min(90, int(body["scan_days"])))
        db.set_app_state("scan_days", str(val))  # stored for UI display only — scanner always uses 45
    if "h_atr_mult" in body:
        val = max(0.1, min(3.0, float(body["h_atr_mult"])))
        db.set_app_state("h_atr_mult", str(val))
    if "h_c_atr_mult" in body:
        val = max(0.1, min(3.0, float(body["h_c_atr_mult"])))
        db.set_app_state("h_c_atr_mult", str(val))
    if "h_scan_days" in body:
        val = max(1, min(90, int(body["h_scan_days"])))
        db.set_app_state("h_scan_days", str(val))
    return JSONResponse({"ok": True})


# ── IBD-style multi-timeframe chart endpoint ──────────────────────────────────

@app.get("/api/multi-chart")
def api_multi_chart(symbol: str = ""):
    """
    Return monthly / weekly / daily OHLCV + pre-computed EMA20/50/SMA200 + RS line for one symbol.
    Reads from ohlcv_weekly, ohlcv_monthly, indicator_daily, indicator_weekly,
    indicator_monthly — no live computation.
    """
    sym = symbol.strip().upper()
    if not sym:
        return JSONResponse({"error": "symbol required"}, status_code=400)

    import sqlite3 as _sq

    try:
        con = _sq.connect("local.db")
        con.row_factory = _sq.Row

        # ── Daily OHLCV ───────────────────────────────────────────────────
        # Excludes rows with any NULL OHLC value — isolated single-day gaps
        # yfinance occasionally leaves on thin/illiquid tickers (penny stocks
        # etc.). _pack_tf below has no NULL guard (float(None) crashes), so
        # without this filter one bad day took down the WHOLE chart with an
        # HTTP 500 instead of just skipping that one candle.
        daily_rows = con.execute(
            """SELECT date, open, high, low, close, volume FROM ohlcv_daily
               WHERE symbol=? AND open IS NOT NULL AND high IS NOT NULL
                     AND low IS NOT NULL AND close IS NOT NULL
               ORDER BY date ASC""",
            (sym,),
        ).fetchall()

        # ── Daily indicators (pre-computed) ───────────────────────────────
        daily_ind = con.execute(
            "SELECT date, ema20, ema50, sma200, rs_line FROM indicator_daily WHERE symbol=? ORDER BY date ASC",
            (sym,),
        ).fetchall()

        # ── Weekly OHLCV (pre-computed) ───────────────────────────────────
        weekly_rows = con.execute(
            "SELECT week, open, high, low, close, volume FROM ohlcv_weekly WHERE symbol=? ORDER BY week ASC",
            (sym,),
        ).fetchall()

        # ── Weekly indicators (pre-computed) ──────────────────────────────
        weekly_ind = con.execute(
            "SELECT week, ema20, ema50, sma200, rs_line FROM indicator_weekly WHERE symbol=? ORDER BY week ASC",
            (sym,),
        ).fetchall()

        # ── Monthly OHLCV (resampled from daily, pre-computed) ────────────
        monthly_rows = con.execute(
            "SELECT month, open, high, low, close, volume FROM ohlcv_monthly WHERE symbol=? ORDER BY month ASC",
            (sym,),
        ).fetchall()

        # ── Monthly indicators (pre-computed) ──────────────────────────────
        monthly_ind = con.execute(
            "SELECT month, ema20, ema50, sma200, rs_line FROM indicator_monthly WHERE symbol=? ORDER BY month ASC",
            (sym,),
        ).fetchall()

        con.close()
    except Exception as e:
        logger.error("api_multi_chart error: %s", e)
        return JSONResponse({"error": str(e)}, status_code=500)

    if not daily_rows:
        return JSONResponse({"error": f"No daily data for {sym}"}, status_code=404)

    def _pack_tf(ohlcv_rows, ind_rows, date_col, has_rs=False):
        """Return compact columnar arrays instead of per-row dicts.
        Reduces JSON payload by ~69% (679KB→200KB) with no data loss.
        The JS side expands these back to {time,open,...} objects via _expandTf().
        """
        im = {}
        for r in ind_rows:
            k = str(r[date_col])[:10] if date_col != "ts" else r[date_col]
            im[k] = r
        t = []; o = []; h = []; l = []; c = []; v = []
        e20 = []; e50 = []; s200 = []; rs = []
        for r in ohlcv_rows:
            # Skip rows with any NULL OHLC value — isolated single-day gaps
            # yfinance occasionally leaves on thin tickers. The daily SQL
            # query above already filters these out at the source, but this
            # guard protects weekly/monthly (and any future caller) too,
            # since float(None) would otherwise crash the whole response.
            if r["open"] is None or r["high"] is None or r["low"] is None or r["close"] is None:
                continue
            k  = str(r[date_col])[:10] if date_col != "ts" else r[date_col]
            rv = round; fv = float
            t.append(k)
            o.append(rv(fv(r["open"]),  2))
            h.append(rv(fv(r["high"]),  2))
            l.append(rv(fv(r["low"]),   2))
            c.append(rv(fv(r["close"]), 2))
            v.append(int(r["volume"] or 0))
            ind = im.get(k)
            e20.append( rv(ind["ema20"],  2) if ind and ind["ema20"]  is not None else None)
            e50.append( rv(ind["ema50"],  2) if ind and ind["ema50"]  is not None else None)
            s200.append(rv(ind["sma200"], 2) if ind and ind["sma200"] is not None else None)
            if has_rs:
                # RS line values for stocks are small floats (e.g. 0.0786) — must NOT
                # round to 2dp or they collapse to the same value and draw as flat lines.
                rs.append(ind["rs_line"] if ind and ind["rs_line"] is not None else None)
        out = {"t": t, "o": o, "h": h, "l": l, "c": c, "v": v,
               "e20": e20, "e50": e50, "s200": s200}
        if has_rs:
            out["rs"] = rs
        return out

    daily   = _pack_tf(daily_rows,   daily_ind,   "date",  has_rs=True)
    weekly  = _pack_tf(weekly_rows,  weekly_ind,  "week",  has_rs=True)
    monthly = _pack_tf(monthly_rows, monthly_ind, "month", has_rs=True) if monthly_rows else None

    return JSONResponse({"symbol": sym, "monthly": monthly, "weekly": weekly, "daily": daily})
