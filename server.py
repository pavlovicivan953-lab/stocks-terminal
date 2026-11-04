#!/usr/bin/env python3
"""Burzovni terminal - mali backend: kotacije (Yahoo, pa Finnhub, pa Stooq) + frontend + auto SL/TP."""
import csv, io, json, os, re, ssl, time, threading, urllib.request, urllib.parse, urllib.error
import http.cookiejar, http.client, hashlib, datetime, traceback
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from zoneinfo import ZoneInfo

BASE = os.path.dirname(os.path.abspath(__file__))
def _default_pf():
    for c in (os.environ.get("PORTFOLIO_PATH"), os.path.join(BASE, "portfolio.json"), os.path.join(os.path.dirname(BASE), "portfolio.json")):
        if c and os.path.exists(c): return c
    return os.path.join(BASE, "portfolio.json")
PORTFOLIO = _default_pf()
# Na Renderu: PORTFOLIO_URL = raw GitHub URL od portfolio.json. Tada se nista ne zapisuje na disk;
# SL/TP zatvaranja su samo u memoriji (prikaz), a sluzbeno zatvaranje radi tjedna rutina commitom u repo.
PORTFOLIO_URL = os.environ.get("PORTFOLIO_URL", "").strip()
REMOTE = bool(PORTFOLIO_URL) or os.environ.get("PERSIST", "1") == "0"
REMOTE_TTL = int(os.environ.get("PORTFOLIO_REFRESH_S", "600"))
_remote = {"p": None, "t": 0}
_mem_closed = {}  # trade id -> polja zatvaranja (samo u memoriji)
INFO = json.load(open(os.path.join(BASE, "info.json"), encoding="utf-8"))
PORT = int(os.environ.get("PORT", "8787"))
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130 Safari/537.36"
FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "").strip()
YAHOO_BACKOFF_S = int(os.environ.get("YAHOO_BACKOFF_S", "300"))
STOOQ_HIST_PAGES = int(os.environ.get("STOOQ_HIST_PAGES", "7"))
UPSTREAM_TIMEOUT = float(os.environ.get("UPSTREAM_TIMEOUT", "6"))
REFRESH_BUDGET_S = float(os.environ.get("REFRESH_BUDGET_S", "22"))
REFRESH_EVERY_S = float(os.environ.get("REFRESH_EVERY_S", "60"))
_snap_lock = threading.Lock()
_snap = {"body": None}

cj = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
opener.addheaders = [("User-Agent", UA), ("Accept", "application/json,text/plain,*/*")]
_crumb = {"v": None, "t": 0}
_lock = threading.Lock()
_pf_lock = threading.Lock()
_cache = {}
_inflight = {}
_cache_lock = threading.Lock()
_yahoo_down_until = 0.0
_yahoo_lock = threading.Lock()
_stooq_cookie = ""
_stooq_lock = threading.Lock()
_stooq_conn = None
_stooq_csv_quote_off = False
_stooq_csv_daily_off_until = 0.0
_ET = ZoneInfo("America/New_York")
_WAW = ZoneInfo("Europe/Warsaw")
_MONTHS = {m: i for i, m in enumerate(["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], 1)}
QUOTE_KEYS = ["symbol","longName","shortName","regularMarketPrice","regularMarketChange","regularMarketChangePercent",
              "regularMarketTime","regularMarketDayHigh","regularMarketDayLow","regularMarketOpen","regularMarketPreviousClose",
              "regularMarketVolume","averageDailyVolume3Month","fiftyTwoWeekLow","fiftyTwoWeekHigh","marketCap","trailingPE","forwardPE",
              "epsTrailingTwelveMonths","marketState","preMarketPrice","preMarketChangePercent","postMarketPrice","postMarketChangePercent",
              "postMarketTime","preMarketTime","currency","financialCurrency","fiftyDayAverage","twoHundredDayAverage","dividendYield","exchangeTimezoneName"]
_HIST_ROW = re.compile(
    r"<tr><td align=center id=t03>\d+</td><td nowrap>([^<]+)</td><td>([^<]*)</td><td>([^<]*)</td><td>([^<]*)</td><td>([^<]*)</td>"
    r"<td[^>]*>([^<]*)</td><td[^>]*>([^<]*)</td><td>([^<]*)</td></tr>")

def log(*a):
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), *a, flush=True)

def ttl(local, remote):
    return remote if REMOTE else local

def get(url, timeout=None):
    if timeout is None:
        timeout = UPSTREAM_TIMEOUT
    with opener.open(url, timeout=timeout) as r:
        return r.read()

def raw_get(url, headers=None, timeout=None, data=None):
    if timeout is None:
        timeout = UPSTREAM_TIMEOUT
    h = {"User-Agent": UA, "Accept": "text/html,application/json,text/csv,text/plain,*/*"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, data=data, headers=h)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read(), dict(r.headers)
    except urllib.error.HTTPError as e:
        return e.code, e.read() if e.fp else b"", dict(e.headers or {})

def yahoo_paused():
    return os.environ.get("FORCE_NO_YAHOO") == "1" or time.time() < _yahoo_down_until

def note_yahoo_limit(exc):
    global _yahoo_down_until
    code = getattr(exc, "code", None)
    text = str(exc)
    if code == 429 or "Too Many Requests" in text or re.search(r"\b429\b", text):
        with _yahoo_lock:
            _yahoo_down_until = time.time() + YAHOO_BACKOFF_S
        log("Yahoo 429, pauza", YAHOO_BACKOFF_S, "s")
        return True
    return False

def crumb(force=False):
    with _lock:
        if force or not _crumb["v"] or time.time() - _crumb["t"] > 3600:
            try: get("https://fc.yahoo.com", UPSTREAM_TIMEOUT)
            except Exception: pass  # vraca 404 ali postavi cookie
            _crumb["v"] = get("https://query1.finance.yahoo.com/v1/test/getcrumb").decode().strip()
            _crumb["t"] = time.time()
            log("novi crumb", _crumb["v"])
        return _crumb["v"]

def yget(url, need_crumb=False):
    for attempt in range(2):
        u = url
        if need_crumb:
            u += ("&" if "?" in u else "?") + "crumb=" + urllib.parse.quote(crumb(force=attempt > 0))
        try:
            return json.loads(get(u))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                note_yahoo_limit(e)
                raise
            if e.code in (401, 403) and attempt == 0 and need_crumb:
                continue
            raise

def cached(key, ttl_s, fn):
    now = time.time()
    with _cache_lock:
        c = _cache.get(key)
        if c and now - c[0] < ttl_s:
            return c[1]
        slot = _inflight.get(key)
        if slot is None:
            slot = {"ev": threading.Event(), "val": None, "err": None, "done": False}
            _inflight[key] = slot
            leader = True
        else:
            leader = False
    if not leader:
        slot["ev"].wait(180)
        if slot["done"] and slot["err"] is None:
            return slot["val"]
        with _cache_lock:
            c = _cache.get(key)
        if c:
            return c[1]
        if slot["err"]:
            raise slot["err"]
        raise RuntimeError("cache " + key)
    try:
        v = fn()
        with _cache_lock:
            _cache[key] = (time.time(), v)
        slot["val"] = v
        slot["done"] = True
        return v
    except Exception as e:
        log("greska", key, e)
        slot["err"] = e
        slot["done"] = True
        with _cache_lock:
            c = _cache.get(key)
        if c:
            return c[1]
        raise
    finally:
        slot["ev"].set()
        with _cache_lock:
            if _inflight.get(key) is slot:
                _inflight.pop(key, None)

def _read_local():
    with open(PORTFOLIO, encoding="utf-8") as f:
        return json.load(f)

def load_pf():
    if not REMOTE:
        return _read_local()
    if PORTFOLIO_URL and (_remote["p"] is None or time.time() - _remote["t"] > REMOTE_TTL):
        try:
            req = urllib.request.Request(PORTFOLIO_URL + ("&" if "?" in PORTFOLIO_URL else "?") + "t=" + str(int(time.time())), headers={"User-Agent": UA, "Cache-Control": "no-cache"})
            with urllib.request.urlopen(req, timeout=UPSTREAM_TIMEOUT) as r:
                _remote["p"] = json.loads(r.read()); _remote["t"] = time.time()
                log("portfolio.json ucitan s", PORTFOLIO_URL)
        except Exception as e:
            log("PORTFOLIO_URL greska, koristim lokalnu kopiju:", e)
            if _remote["p"] is None:
                _remote["p"] = _read_local(); _remote["t"] = time.time()
    if _remote["p"] is None:
        _remote["p"] = _read_local(); _remote["t"] = time.time()
    p = json.loads(json.dumps(_remote["p"]))
    # ponovno primijeni zatvaranja iz memorije na trejdove koji su u izvoru jos otvoreni
    for t in p.get("trades", []):
        m = _mem_closed.get(str(t.get("id", t["ticker"])))
        if m and t.get("status") == "open":
            t.update(m); p["cash"] = round(p.get("cash", 0) + m["exit_price"] * t["shares"], 2)
    p["_mode"] = "remote"
    return p

def save_pf(p):
    if REMOTE:
        for t in p.get("trades", []):
            if t.get("status") == "closed" and t.get("_mem"):
                _mem_closed[str(t.get("id", t["ticker"]))] = {k: t[k] for k in ("status","exit_price","exit_reason","exit_date","exit_time_utc","pnl_usd","_mem")}
        return
    tmp = PORTFOLIO + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(p, f, indent=2, ensure_ascii=False)
    os.replace(tmp, PORTFOLIO)

def symbols(p):
    s = list(p.get("watchlist", []))
    for t in p.get("trades", []):
        if t["ticker"] not in s: s.append(t["ticker"])
    return s

def blank_quote(sym):
    q = {k: None for k in QUOTE_KEYS}
    q["symbol"] = sym
    info = INFO.get(sym) or {}
    q["shortName"] = info.get("name")
    q["longName"] = info.get("name")
    q["currency"] = "USD"
    q["financialCurrency"] = "EUR" if info.get("cur") == "€" else "USD"
    q["exchangeTimezoneName"] = "America/New_York"
    q["marketState"] = infer_market_state()
    q["source"] = None
    return q

def infer_market_state(now=None):
    et = now or datetime.datetime.now(_ET)
    if et.weekday() >= 5:
        return "CLOSED"
    mins = et.hour * 60 + et.minute
    if 9 * 60 + 30 <= mins < 16 * 60:
        return "REGULAR"
    if 4 * 60 <= mins < 9 * 60 + 30:
        return "PRE"
    if 16 * 60 <= mins < 20 * 60:
        return "POST"
    return "CLOSED"

def num(s):
    if s is None or isinstance(s, bool):
        return None
    if isinstance(s, (int, float)):
        return float(s)
    t = str(s).strip().replace("\xa0", "").replace(" ", "").replace(",", "")
    if not t or t.upper() in ("N/D", "N/A", "NONE", "-"):
        return None
    t = t.replace("(", "").replace(")", "")
    if t.endswith("%"):
        t = t[:-1]
    mult = 1.0
    if t and t[-1] in "kKmMgG":
        mult = {"k": 1e3, "m": 1e6, "g": 1e9}[t[-1].lower()]
        t = t[:-1]
    try:
        return float(t) * mult
    except ValueError:
        return None

def source_note(qs):
    srcs = []
    for q in qs:
        s = q.get("source")
        if s and s not in srcs:
            srcs.append(s)
    if srcs == ["stooq"]:
        return "izvor: Stooq (odgoda ~15 min)"
    if srcs == ["yahoo"]:
        return "izvor: Yahoo"
    if srcs == ["finnhub"]:
        return "izvor: Finnhub"
    if not srcs:
        return ""
    names = {"yahoo": "Yahoo", "finnhub": "Finnhub", "stooq": "Stooq"}
    return "izvor: " + " + ".join(names.get(s, s) for s in srcs)

def _order(syms, rows):
    pos = {s: i for i, s in enumerate(syms)}
    rows.sort(key=lambda q: pos.get(q.get("symbol"), 999))
    return rows

def yahoo_quotes(syms):
    d = yget("https://query1.finance.yahoo.com/v7/finance/quote?symbols=" + ",".join(syms), True)
    out = []
    for item in d["quoteResponse"]["result"]:
        q = {k: item.get(k) for k in QUOTE_KEYS}
        q["source"] = "yahoo"
        out.append(q)
    return out

def quotes(syms):
    if not syms:
        return []
    def f():
        got = []
        if not yahoo_paused():
            try:
                got = yahoo_quotes(syms)
            except Exception as e:
                note_yahoo_limit(e)
                log("yahoo quotes", e)
                got = []
        have = {q["symbol"] for q in got if q.get("regularMarketPrice") is not None}
        missing = [s for s in syms if s not in have]
        if missing and FINNHUB_API_KEY:
            try:
                got.extend(finnhub_quotes(missing))
            except Exception as e:
                log("finnhub quotes", e)
        have = {q["symbol"] for q in got if q.get("regularMarketPrice") is not None}
        missing = [s for s in syms if s not in have]
        if missing:
            try:
                got.extend(stooq_quotes(missing))
            except Exception as e:
                log("stooq quotes", e)
        got = [q for q in got if q.get("regularMarketPrice") is not None]
        if not got:
            raise RuntimeError("nema kotacija ni s jednog izvora")
        log("kotacije", source_note(got), len(got))
        return _order(syms, got)
    return cached("q:" + ",".join(syms), ttl(12, 90), f)

def yahoo_spark(syms):
    out = {}
    for i in range(0, len(syms), 10):
        d = yget("https://query1.finance.yahoo.com/v8/finance/spark?range=1d&interval=5m&symbols=" + ",".join(syms[i:i+10]))
        for k, v in d.items():
            out[k] = {"c": [x for x in (v.get("close") or []) if x is not None], "prev": v.get("chartPreviousClose") or v.get("previousClose")}
    return out

def spark_from_bars(bars):
    closes = [b["c"] for b in bars if b.get("c") is not None]
    if len(closes) < 2:
        return None
    tail = closes[-20:]
    prev = closes[-21] if len(closes) > 20 else closes[0]
    return {"c": tail, "prev": prev}

def spark(syms):
    def f():
        out = {}
        if not yahoo_paused():
            try:
                out = yahoo_spark(syms)
            except Exception as e:
                note_yahoo_limit(e)
                log("yahoo spark", e)
                out = {}
        missing = [s for s in syms if s not in out or len((out.get(s) or {}).get("c") or []) < 2]
        if missing and FINNHUB_API_KEY:
            for s in missing:
                try:
                    sp = finnhub_spark(s)
                    if sp:
                        out[s] = sp
                except Exception as e:
                    log("finnhub spark", s, e)
        missing = [s for s in syms if s not in out or len((out.get(s) or {}).get("c") or []) < 2]
        for s in missing:
            try:
                bars = stooq_daily(s)
                sp = spark_from_bars(bars)
                if sp:
                    out[s] = sp
                    continue
            except Exception as e:
                log("stooq spark", s, e)
            try:
                q = stooq_html_quote(s)
                px, prev = q.get("regularMarketPrice"), q.get("regularMarketPreviousClose")
                if px is not None:
                    out[s] = {"c": [prev if prev is not None else px, px], "prev": prev if prev is not None else px}
            except Exception as e:
                log("stooq spark quote", s, e)
        return out
    return cached("spark:" + ",".join(syms), ttl(60, 300), f)

def et_date(ts):
    return datetime.datetime.fromtimestamp(ts, _ET).date().isoformat()

def check_sl_tp(qs):
    """Zatvori otvorene trejdove kad zivu cijenu dotakne SL ili TP (izlaz na razini SL/TP)."""
    qmap = {q["symbol"]: q for q in qs}
    with _pf_lock:
        p = load_pf(); changed = False
        for t in p.get("trades", []):
            if t.get("status") != "open": continue
            q = qmap.get(t["ticker"])
            if not q or q.get("regularMarketPrice") is None: continue
            px = q["regularMarketPrice"]; lo = hi = px
            # dnevni min/max uzimamo samo za dane nakon dana ulaza (ulaz je bio usred dana)
            if q.get("regularMarketTime") and et_date(q["regularMarketTime"]) > t["entry_date"]:
                lo = min(px, q.get("regularMarketDayLow") or px); hi = max(px, q.get("regularMarketDayHigh") or px)
            reason = None
            if lo <= t["stop_loss"]: reason, exitp = "SL", t["stop_loss"]
            elif hi >= t["take_profit"]: reason, exitp = "TP", t["take_profit"]
            if reason:
                t["status"] = "closed"; t["exit_price"] = exitp; t["exit_reason"] = reason
                t["exit_date"] = et_date(q["regularMarketTime"])
                t["exit_time_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
                t["pnl_usd"] = round((exitp - t["entry_price"]) * t["shares"] * (1 if t.get("side","long") == "long" else -1), 2)
                if REMOTE: t["_mem"] = True  # samo prikaz, nije spremljeno
                p["cash"] = round(p.get("cash", 0) + exitp * t["shares"], 2)
                changed = True
                log("ZATVOREN", t["ticker"], reason, exitp, t["pnl_usd"])
        if changed: save_pf(p)
        return p

def _info_public():
    return {k: {"name": v["name"], "why": v["why"]} for k, v in INFO.items()}

def cheap_spark(qs):
    out = {}
    for q in qs:
        px = q.get("regularMarketPrice")
        if px is None:
            continue
        prev = q.get("regularMarketPreviousClose")
        base = prev if prev is not None else px
        out[q["symbol"]] = {"c": [base, px], "prev": base}
    return out

def skeleton():
    try:
        p = _read_local()
    except Exception:
        p = {"watchlist": [], "trades": [], "cash": 0, "starting_capital": 10000}
    syms = symbols(p) if p.get("watchlist") is not None else []
    return {"serverTime": int(time.time()), "marketState": infer_market_state(), "sourceNote": "",
            "loading": True, "quotes": [blank_quote(s) for s in syms], "spark": {},
            "portfolio": p, "info": _info_public()}

def _assemble(p, qs, loading):
    ms = next((q.get("marketState") for q in qs if q.get("symbol") in ("MSFT", "AAPL", "NVDA") and q.get("marketState")), None)
    if not ms:
        ms = infer_market_state()
    return {"serverTime": int(time.time()), "marketState": ms, "sourceNote": source_note(qs),
            "loading": loading, "quotes": qs, "spark": cheap_spark(qs), "portfolio": p, "info": _info_public()}

def publish(body):
    with _snap_lock:
        _snap["body"] = body

def state():
    with _snap_lock:
        body = _snap["body"]
    if not body:
        return skeleton()
    out = json.loads(json.dumps(body))
    out["serverTime"] = int(time.time())
    return out

def quick_quotes(syms, deadline):
    got = []
    if syms and not yahoo_paused() and time.time() < deadline and os.environ.get("HANG_UPSTREAM") != "1":
        try:
            got = yahoo_quotes(syms)
        except Exception as e:
            note_yahoo_limit(e)
            log("yahoo quotes", e)
            got = []
    have = {q["symbol"] for q in got if q.get("regularMarketPrice") is not None}
    missing = [s for s in syms if s not in have]
    if missing and FINNHUB_API_KEY and time.time() < deadline and os.environ.get("HANG_UPSTREAM") != "1":
        try:
            got.extend(finnhub_quotes(missing))
        except Exception as e:
            log("finnhub quotes", e)
    have = {q["symbol"] for q in got if q.get("regularMarketPrice") is not None}
    missing = [s for s in syms if s not in have]
    if missing and time.time() < deadline:
        try:
            by = stooq_csv_quotes(missing)
            got.extend(by[s] for s in missing if s in by)
        except Exception as e:
            log("stooq csv", e)
    have = {q["symbol"] for q in got if q.get("regularMarketPrice") is not None}
    for s in syms:
        if s in have or time.time() >= deadline:
            continue
        try:
            got.append(stooq_html_quote(s))
            have.add(s)
        except Exception as e:
            log("stooq", s, e)
    return _order(syms, [q for q in got if q.get("regularMarketPrice") is not None])

def refresh_once():
    try:
        p = load_pf()
    except Exception as e:
        log("portfolio", e)
        p = _read_local()
    syms = symbols(p)
    deadline = time.time() + REFRESH_BUDGET_S
    t0 = time.time()
    qs = quick_quotes(syms, deadline)
    by = {q["symbol"]: q for q in qs}
    full = [by.get(s) or blank_quote(s) for s in syms]
    priced = sum(1 for q in full if q.get("regularMarketPrice") is not None)
    if priced == 0:
        with _snap_lock:
            prev = _snap["body"]
        if prev and any(q.get("regularMarketPrice") is not None for q in prev.get("quotes") or []):
            log("osvjezavanje prazno, zadrzavam cache")
            return
    if priced:
        try:
            p = check_sl_tp(full)
        except Exception as e:
            log("sl/tp greska", e)
    body = _assemble(p, full, priced < len(syms))
    publish(body)
    log("osvjezeno", body["sourceNote"] or "nema", priced, "/", len(syms), "za", round(time.time() - t0, 1), "s")

def refresh_loop():
    while True:
        t0 = time.time()
        try:
            refresh_once()
        except Exception as e:
            log("refresh", e)
        rest = REFRESH_EVERY_S - (time.time() - t0)
        time.sleep(rest if rest > 5 else 5)

# ---- Finnhub (samo uz FINNHUB_API_KEY) ----
def finnhub_get(path):
    sep = "&" if "?" in path else "?"
    url = "https://finnhub.io/api/v1" + path + sep + "token=" + urllib.parse.quote(FINNHUB_API_KEY)
    code, body, _ = raw_get(url, headers={"Accept": "application/json"}, timeout=15)
    if code != 200:
        raise RuntimeError("finnhub HTTP %s" % code)
    data = json.loads(body)
    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError("finnhub " + str(data.get("error")))
    return data

def _frac(v):
    """Finnhub marze i prinosi su postoci; Yahoo/frontend ocekuje razlomak."""
    if v is None:
        return None
    v = float(v)
    return v / 100.0 if abs(v) > 1.5 else v

def finnhub_quotes(syms):
    def one(sym):
        d = finnhub_get("/quote?symbol=" + urllib.parse.quote(sym))
        px = d.get("c")
        if not px:
            return None
        q = blank_quote(sym)
        q["source"] = "finnhub"
        q["regularMarketPrice"] = px
        q["regularMarketChange"] = d.get("d")
        q["regularMarketChangePercent"] = d.get("dp")
        q["regularMarketDayHigh"] = d.get("h") or None
        q["regularMarketDayLow"] = d.get("l") or None
        q["regularMarketOpen"] = d.get("o") or None
        q["regularMarketPreviousClose"] = d.get("pc") or None
        q["regularMarketTime"] = d.get("t") or int(time.time())
        try:
            prof = cached("fh-prof:" + sym, ttl(3600, 21600), lambda: finnhub_get("/stock/profile2?symbol=" + urllib.parse.quote(sym)))
            if prof.get("name"):
                q["shortName"] = q["shortName"] or prof.get("name")
                q["longName"] = q["longName"] or prof.get("name")
            if prof.get("marketCapitalization"):
                q["marketCap"] = float(prof["marketCapitalization"]) * 1e6
            if prof.get("currency"):
                q["currency"] = prof.get("currency")
        except Exception as e:
            log("finnhub profile", sym, e)
        try:
            met = cached("fh-met:" + sym, ttl(3600, 21600), lambda: finnhub_get("/stock/metric?symbol=" + urllib.parse.quote(sym) + "&metric=all"))
            m = met.get("metric") or {}
            q["fiftyTwoWeekHigh"] = m.get("52WeekHigh")
            q["fiftyTwoWeekLow"] = m.get("52WeekLow")
            q["trailingPE"] = m.get("peBasicExclExtraTTM") or m.get("peTTM")
            q["epsTrailingTwelveMonths"] = m.get("epsBasicExclExtraItemsTTM")
            q["dividendYield"] = _frac(m.get("currentDividendYieldTTM"))
        except Exception as e:
            log("finnhub metric", sym, e)
        return q
    out = []
    with ThreadPoolExecutor(max_workers=4) as ex:
        for q in ex.map(one, syms):
            if q:
                out.append(q)
    return out

def finnhub_candles(sym, resolution, start, end):
    d = finnhub_get("/stock/candle?symbol=%s&resolution=%s&from=%s&to=%s" % (urllib.parse.quote(sym), resolution, int(start), int(end)))
    if d.get("s") != "ok":
        return []
    pts = []
    for t, c in zip(d.get("t") or [], d.get("c") or []):
        if c is not None:
            pts.append([int(t), float(c)])
    return pts

def finnhub_spark(sym):
    now = time.time()
    pts = finnhub_candles(sym, "5", now - 5 * 86400, now)
    if len(pts) < 2:
        return None
    last_day = datetime.datetime.fromtimestamp(pts[-1][0], _ET).date()
    day = [p for p in pts if datetime.datetime.fromtimestamp(p[0], _ET).date() == last_day]
    use = day if len(day) >= 2 else pts[-40:]
    prev = None
    for p in reversed(pts):
        if datetime.datetime.fromtimestamp(p[0], _ET).date() < last_day:
            prev = p[1]
            break
    return {"c": [p[1] for p in use], "prev": prev if prev is not None else use[0][1]}

# ---- Stooq ----
def _is_challenge(body):
    return b"requires JavaScript to verify your browser" in body[:4000]

def _is_denied(body):
    t = body.strip().lower()[:80]
    return t.startswith(b"access denied") or t.startswith("odmowa".encode()) or t.startswith("odmowa dost".encode())

def _stooq_reset():
    global _stooq_conn
    if _stooq_conn is not None:
        try:
            _stooq_conn.close()
        except Exception:
            pass
    _stooq_conn = None

def _stooq_conn_get():
    global _stooq_conn
    if _stooq_conn is None:
        _stooq_conn = http.client.HTTPSConnection("stooq.com", timeout=UPSTREAM_TIMEOUT, context=ssl.create_default_context())
    return _stooq_conn

def _stooq_req(method, path, body=None, extra=None):
    headers = {"User-Agent": UA, "Accept": "text/html,text/csv,text/plain,*/*", "Connection": "keep-alive", "Referer": "https://stooq.com/"}
    if _stooq_cookie:
        headers["Cookie"] = _stooq_cookie
    if extra:
        headers.update(extra)
    err = None
    for _attempt in range(1):
        conn = _stooq_conn_get()
        try:
            conn.request(method, path, body=body, headers=headers)
            resp = conn.getresponse()
            data = resp.read()
            return resp.status, data, dict(resp.getheaders())
        except Exception as e:
            err = e
            _stooq_reset()
    raise RuntimeError("stooq veza %s" % err)

def _stooq_solve(challenge):
    """Isti dokaz rada koji stranica trazi od preglednika, na istoj vezi."""
    global _stooq_cookie
    m = re.search(br'const c="([^"]+)",d=(\d+)', challenge or b"")
    if not m:
        raise RuntimeError("stooq nema izazov")
    token, diff = m.group(1).decode(), int(m.group(2))
    target = "0" * diff
    n = 0
    raw = token.encode()
    while not hashlib.sha256(raw + str(n).encode()).hexdigest().startswith(target):
        n += 1
    form = urllib.parse.urlencode({"c": token, "n": str(n)}).encode()
    status, data, hdrs = _stooq_req("POST", "/__verify", form, {
        "Content-Type": "application/x-www-form-urlencoded", "Origin": "https://stooq.com", "Content-Length": str(len(form))})
    cookie = (hdrs.get("Set-Cookie") or "").split(";", 1)[0]
    if status != 200 or not data.startswith(b"ok") or not cookie.startswith("auth="):
        raise RuntimeError("stooq verify %s" % data[:80])
    _stooq_cookie = cookie
    log("stooq cookie")

def _stooq_good(code, body):
    return code == 200 and body and not _is_challenge(body) and not _is_denied(body)

def stooq_fetch(path):
    if os.environ.get("HANG_UPSTREAM") == "1":
        time.sleep(30)
        raise TimeoutError("hang")
    with _stooq_lock:
        code, body, _hdrs = _stooq_req("GET", path)
        if code == 404:
            raise FileNotFoundError(path)
        if not _stooq_good(code, body) and _is_challenge(body):
            _stooq_solve(body)
            code, body, _hdrs = _stooq_req("GET", path)
        if code == 404:
            raise FileNotFoundError(path)
        if not _stooq_good(code, body):
            raise RuntimeError("stooq odbio %s (%s)" % (path, (body or b"")[:40]))
        return body

def _parse_day(s):
    d, mon, y = s.split()
    return datetime.date(int(y), _MONTHS[mon], int(d))

def _bars_from_csv(text):
    rows = list(csv.DictReader(io.StringIO(text)))
    if not rows:
        return []
    bars = []
    for row in rows:
        low = {k.lower(): v for k, v in row.items() if k}
        ds = low.get("date") or ""
        if not re.match(r"\d{4}-\d{2}-\d{2}", ds):
            continue
        c = num(low.get("close"))
        if c is None:
            continue
        bars.append({"d": datetime.date.fromisoformat(ds[:10]), "o": num(low.get("open")), "h": num(low.get("high")),
                     "l": num(low.get("low")), "c": c, "v": num(low.get("volume"))})
    bars.sort(key=lambda b: b["d"])
    return bars

def _bars_from_html(html):
    bars = []
    for m in _HIST_ROW.finditer(html):
        try:
            day = _parse_day(m.group(1))
        except Exception:
            continue
        c = num(m.group(5))
        if c is None:
            continue
        bars.append({"d": day, "o": num(m.group(2)), "h": num(m.group(3)), "l": num(m.group(4)),
                     "c": c, "v": num(m.group(8))})
    return bars  # najnoviji prvi, kako ih stranica salje

def stooq_symbol(sym):
    return sym.lower() + ".us"

def stooq_csv_daily(sym):
    global _stooq_csv_daily_off_until
    if time.time() < _stooq_csv_daily_off_until:
        return None
    path = "/q/d/l/?s=%s&i=d" % stooq_symbol(sym)
    try:
        body = stooq_fetch(path)
    except FileNotFoundError:
        _stooq_csv_daily_off_until = time.time() + 1800
        return None
    except Exception as e:
        if "odbio" in str(e) or "denied" in str(e).lower():
            _stooq_csv_daily_off_until = time.time() + 1800
            log("stooq csv povijest nedostupna, koristim HTML tablicu")
            return None
        raise
    if _is_denied(body) or b"<html" in body[:200].lower():
        _stooq_csv_daily_off_until = time.time() + 1800
        return None
    return _bars_from_csv(body.decode("utf-8", "replace"))

def stooq_daily(sym):
    def f():
        bars = stooq_csv_daily(sym)
        if bars:
            return bars
        got = []
        for page in range(1, STOOQ_HIST_PAGES + 1):
            path = "/q/d/?s=%s&i=d" % stooq_symbol(sym)
            if page > 1:
                path += "&l=%d" % page
            try:
                html = stooq_fetch(path).decode("utf-8", "replace")
            except Exception as e:
                log("stooq stranica", sym, page, e)
                break
            rows = _bars_from_html(html)
            if not rows:
                break
            got.extend(rows)
            if len(rows) < 40:
                break
        got.reverse()  # stranice su od najnovijeg prema starijem
        seen = set()
        out = []
        for b in got:
            if b["d"] in seen:
                continue
            seen.add(b["d"])
            out.append(b)
        out.sort(key=lambda b: b["d"])
        if not out:
            raise RuntimeError("stooq nema povijest " + sym)
        return out
    return cached("stooq-d:" + sym, ttl(1800, 21600), f)

def _html_field(html, sid, suffix):
    sid_e = re.escape(sid)
    if suffix == "c":
        m = re.search(r"id=aq_%s_c\d+>([^<]*)" % sid_e, html)
    else:
        m = re.search(r"id=aq_%s_%s>([^<]*)" % (sid_e, suffix), html)
    return m.group(1).strip() if m else ""

def _quote_ts(date_s, time_s):
    if date_s and re.match(r"\d{4}-\d{2}-\d{2}", date_s) and time_s and re.match(r"\d{1,2}:\d{2}", time_s):
        hh, mm, *rest = [int(x) for x in time_s.split(":")]
        ss = rest[0] if rest else 0
        y, mo, d = [int(x) for x in date_s.split("-")]
        return int(datetime.datetime(y, mo, d, hh, mm, ss, tzinfo=_WAW).timestamp())
    if date_s and re.match(r"\d{4}-\d{2}-\d{2}", date_s):
        y, mo, d = [int(x) for x in date_s.split("-")]
        return int(datetime.datetime(y, mo, d, 16, 0, tzinfo=_ET).timestamp())
    return int(time.time())

def parse_html_quote(sym, html):
    sid = stooq_symbol(sym)
    px = num(_html_field(html, sid, "c"))
    if px is None:
        return None
    q = blank_quote(sym)
    q["source"] = "stooq"
    q["regularMarketPrice"] = px
    q["regularMarketOpen"] = num(_html_field(html, sid, "o"))
    q["regularMarketDayHigh"] = num(_html_field(html, sid, "h"))
    q["regularMarketDayLow"] = num(_html_field(html, sid, "l"))
    prev = num(_html_field(html, sid, "p"))
    q["regularMarketPreviousClose"] = prev
    ch = num(_html_field(html, sid, "m2"))
    chp = num(_html_field(html, sid, "m3"))
    if ch is None and prev:
        ch = px - prev
    if chp is None and prev:
        chp = (px / prev - 1) * 100
    q["regularMarketChange"] = ch
    q["regularMarketChangePercent"] = chp
    q["regularMarketVolume"] = num(_html_field(html, sid, "v2"))
    q["regularMarketTime"] = _quote_ts(_html_field(html, sid, "d2"), _html_field(html, sid, "t1"))
    return q

def enrich_quote(q, bars):
    if not bars:
        return q
    last, prevb = bars[-1], bars[-2] if len(bars) > 1 else None
    if q.get("regularMarketPreviousClose") is None and prevb:
        q["regularMarketPreviousClose"] = prevb["c"]
        if q.get("regularMarketPrice") is not None:
            q["regularMarketChange"] = q["regularMarketPrice"] - prevb["c"]
            q["regularMarketChangePercent"] = (q["regularMarketPrice"] / prevb["c"] - 1) * 100 if prevb["c"] else None
    if q.get("regularMarketVolume") is None:
        q["regularMarketVolume"] = last.get("v")
    window = bars[-252:]
    if len(window) >= 200:
        q["fiftyTwoWeekHigh"] = max(b["h"] for b in window if b.get("h") is not None)
        q["fiftyTwoWeekLow"] = min(b["l"] for b in window if b.get("l") is not None)
    if len(bars) >= 50:
        q["fiftyDayAverage"] = sum(b["c"] for b in bars[-50:]) / 50
    if len(bars) >= 200:
        q["twoHundredDayAverage"] = sum(b["c"] for b in bars[-200:]) / 200
    vols = [b["v"] for b in bars[-63:] if b.get("v")]
    if len(vols) >= 40:
        q["averageDailyVolume3Month"] = sum(vols) / len(vols)
    return q

def stooq_csv_quotes(syms):
    global _stooq_csv_quote_off
    if _stooq_csv_quote_off:
        return {}
    path = "/q/l/?s=%s&f=sd2t2ohlcv&h&e=csv" % "+".join(stooq_symbol(s) for s in syms)
    try:
        body = stooq_fetch(path)
    except FileNotFoundError:
        _stooq_csv_quote_off = True
        log("stooq csv kotacija 404, koristim HTML stranicu")
        return {}
    except Exception as e:
        log("stooq csv kotacija", e)
        return {}
    if _is_denied(body) or b"<html" in body[:300].lower():
        _stooq_csv_quote_off = True
        return {}
    text = body.decode("utf-8", "replace")
    rows = list(csv.DictReader(io.StringIO(text)))
    out = {}
    for row in rows:
        low = {(k or "").lower(): v for k, v in row.items()}
        raw_sym = (low.get("symbol") or "").upper()
        sym = raw_sym.split(".")[0]
        if sym not in syms:
            continue
        px = num(low.get("close"))
        if px is None:
            continue
        q = blank_quote(sym)
        q["source"] = "stooq"
        q["regularMarketPrice"] = px
        q["regularMarketOpen"] = num(low.get("open"))
        q["regularMarketDayHigh"] = num(low.get("high"))
        q["regularMarketDayLow"] = num(low.get("low"))
        q["regularMarketVolume"] = num(low.get("volume"))
        if low.get("name"):
            q["shortName"] = q["shortName"] or low.get("name")
            q["longName"] = q["longName"] or low.get("name")
        q["regularMarketTime"] = _quote_ts(low.get("date") or "", low.get("time") or "")
        out[sym] = q
    return out

def stooq_html_quote(sym):
    html = stooq_fetch("/q/?s=%s" % stooq_symbol(sym)).decode("utf-8", "replace")
    q = parse_html_quote(sym, html)
    if not q:
        raise RuntimeError("stooq nema cijenu " + sym)
    return q

def stooq_quotes(syms):
    by = {}
    try:
        by.update(stooq_csv_quotes(syms))
    except Exception as e:
        log("stooq csv", e)
    for s in syms:
        if s in by:
            continue
        try:
            by[s] = stooq_html_quote(s)
        except Exception as e:
            log("stooq", s, e)
            try:
                by[s] = stooq_html_quote(s)
            except Exception as e2:
                log("stooq retry", s, e2)
    return [by[s] for s in syms if s in by]

def bars_to_chart(sym, rng, bars):
    need = {"1d": 2, "5d": 5, "1mo": 22, "6mo": 130, "1y": 260}.get(rng, 130)
    seg = bars[-need:]
    if not seg:
        return None
    prev = bars[-need - 1]["c"] if len(bars) > need else (bars[-2]["c"] if len(bars) > 1 else seg[0]["c"])
    pts = [[int(datetime.datetime(b["d"].year, b["d"].month, b["d"].day, 16, 0, tzinfo=_ET).timestamp()), b["c"]] for b in seg]
    return {"symbol": sym, "range": rng, "points": pts, "prev": prev, "source": "stooq",
            "meta": {"regularMarketPrice": seg[-1]["c"], "currency": "USD", "exchangeTimezoneName": "America/New_York"}}

RANGES = {"1d": ("1d", "5m", 30, 180), "5d": ("5d", "15m", 60, 300), "1mo": ("1mo", "60m", 300, 900), "6mo": ("6mo", "1d", 900, 3600), "1y": ("1y", "1d", 900, 3600)}
_FH_RES = {"1d": ("5", 5), "5d": ("15", 12), "1mo": ("60", 40), "6mo": ("D", 220), "1y": ("D", 420)}

def yahoo_chart(sym, rng):
    r, iv, _local, _remote = RANGES.get(rng, RANGES["6mo"])
    d = yget(f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(sym)}?range={r}&interval={iv}&includePrePost=false")
    res = d["chart"]["result"][0]
    q = res["indicators"]["quote"][0]
    pts = [[t, c] for t, c in zip(res.get("timestamp", []), q["close"]) if c is not None]
    return {"symbol": sym, "range": rng, "points": pts, "prev": res["meta"].get("chartPreviousClose"), "source": "yahoo",
            "meta": {k: res["meta"].get(k) for k in ("regularMarketPrice", "currency", "exchangeTimezoneName")}}

def finnhub_chart(sym, rng):
    res, days = _FH_RES.get(rng, _FH_RES["6mo"])
    now = time.time()
    pts = finnhub_candles(sym, res, now - days * 86400, now)
    if len(pts) < 2:
        return None
    return {"symbol": sym, "range": rng, "points": pts, "prev": pts[0][1], "source": "finnhub",
            "meta": {"regularMarketPrice": pts[-1][1], "currency": "USD", "exchangeTimezoneName": "America/New_York"}}

def chart(sym, rng):
    r, iv, local_ttl, remote_ttl = RANGES.get(rng, RANGES["6mo"])
    def f():
        if not yahoo_paused():
            try:
                got = yahoo_chart(sym, rng)
                if got and got.get("points"):
                    return got
            except Exception as e:
                note_yahoo_limit(e)
                log("yahoo chart", sym, e)
        if FINNHUB_API_KEY:
            try:
                got = finnhub_chart(sym, rng)
                if got and got.get("points"):
                    return got
            except Exception as e:
                log("finnhub chart", sym, e)
        bars = stooq_daily(sym)
        got = bars_to_chart(sym, rng, bars)
        if not got or not got.get("points"):
            raise RuntimeError("nema grafa " + sym)
        return got
    try:
        return cached(f"c:{sym}:{rng}", ttl(local_ttl, remote_ttl), f)
    except Exception as e:
        log("chart", sym, rng, e)
        return {"symbol": sym, "range": rng, "points": [], "prev": None, "source": None,
                "meta": {"regularMarketPrice": None, "currency": "USD", "exchangeTimezoneName": "America/New_York"}}

def static_detail(sym):
    info = INFO.get(sym) or {}
    er = info.get("er") or {}
    news = []
    for n in info.get("news") or []:
        if isinstance(n, (list, tuple)) and len(n) >= 4:
            news.append({"title": n[0], "site": n[1], "ts": None, "href": n[3]})
    fund = {"revenue": None, "revenueGrowth": None, "grossMargins": None, "operatingMargins": None, "profitMargins": None,
            "targetMeanPrice": None, "recommendationKey": None, "analysts": None, "freeCashflow": None, "roe": None,
            "beta": None, "dividendYield": None, "financialCurrency": "EUR" if info.get("cur") == "€" else "USD",
            "sector": info.get("sector"), "industry": None, "website": None}
    return {"fund": fund, "eps": [], "nextEarnings": [er["next"]] if er.get("next") else [], "news": news}

def yahoo_detail(sym):
    mods = "summaryDetail,defaultKeyStatistics,financialData,earningsHistory,calendarEvents,assetProfile"
    d = yget(f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{urllib.parse.quote(sym)}?modules={mods}", True)
    s = d["quoteSummary"]["result"][0]
    def r(m, k):
        v = s.get(m, {}).get(k); return v.get("raw") if isinstance(v, dict) else v
    fund = {"revenue": r("financialData","totalRevenue"), "revenueGrowth": r("financialData","revenueGrowth"), "grossMargins": r("financialData","grossMargins"),
            "operatingMargins": r("financialData","operatingMargins"), "profitMargins": r("financialData","profitMargins"), "targetMeanPrice": r("financialData","targetMeanPrice"),
            "recommendationKey": s.get("financialData",{}).get("recommendationKey"), "analysts": r("financialData","numberOfAnalystOpinions"),
            "freeCashflow": r("financialData","freeCashflow"), "roe": r("financialData","returnOnEquity"), "beta": r("summaryDetail","beta"),
            "dividendYield": r("summaryDetail","dividendYield"), "financialCurrency": s.get("financialData",{}).get("financialCurrency"),
            "sector": s.get("assetProfile",{}).get("sector"), "industry": s.get("assetProfile",{}).get("industry"), "website": s.get("assetProfile",{}).get("website")}
    eh = [{"q": h.get("quarter",{}).get("fmt"), "act": r2(h,"epsActual"), "est": r2(h,"epsEstimate"), "surp": r2(h,"surprisePercent")} for h in s.get("earningsHistory",{}).get("history",[])]
    nxt = [x.get("fmt") for x in s.get("calendarEvents",{}).get("earnings",{}).get("earningsDate",[])]
    news = []
    try:
        n = yget(f"https://query1.finance.yahoo.com/v1/finance/search?q={urllib.parse.quote(sym)}&newsCount=8&quotesCount=0")
        news = [{"title": x["title"], "site": x.get("publisher"), "ts": x.get("providerPublishTime"), "href": x["link"]} for x in n.get("news", [])]
    except Exception as e:
        log("news", e)
    return {"fund": fund, "eps": eh, "nextEarnings": nxt, "news": news}

def finnhub_detail(sym):
    base = static_detail(sym)
    fund = base["fund"]
    try:
        prof = finnhub_get("/stock/profile2?symbol=" + urllib.parse.quote(sym))
        fund["industry"] = fund.get("industry") or prof.get("finnhubIndustry")
        fund["website"] = prof.get("weburl")
        if prof.get("currency"):
            fund["financialCurrency"] = prof.get("currency")
    except Exception as e:
        log("finnhub profile detail", sym, e)
    try:
        met = finnhub_get("/stock/metric?symbol=" + urllib.parse.quote(sym) + "&metric=all").get("metric") or {}
        fund["beta"] = met.get("beta")
        fund["dividendYield"] = _frac(met.get("currentDividendYieldTTM"))
        fund["grossMargins"] = _frac(met.get("grossMarginTTM") or met.get("grossMarginAnnual"))
        fund["operatingMargins"] = _frac(met.get("operatingMarginTTM") or met.get("operatingMarginAnnual"))
        fund["profitMargins"] = _frac(met.get("netProfitMarginTTM") or met.get("netProfitMarginAnnual"))
        fund["roe"] = _frac(met.get("roeTTM") or met.get("roeRfy"))
        fund["revenueGrowth"] = _frac(met.get("revenueGrowthTTMYoy") or met.get("revenueGrowth3Y"))
    except Exception as e:
        log("finnhub metric detail", sym, e)
    try:
        end = datetime.date.today()
        start = end - datetime.timedelta(days=21)
        rows = finnhub_get("/company-news?symbol=%s&from=%s&to=%s" % (urllib.parse.quote(sym), start.isoformat(), end.isoformat()))
        news = []
        for x in (rows or [])[:8]:
            if x.get("headline") and x.get("url"):
                news.append({"title": x["headline"], "site": x.get("source"), "ts": x.get("datetime"), "href": x["url"]})
        if news:
            base["news"] = news
    except Exception as e:
        log("finnhub news", sym, e)
    return base

def detail(sym):
    def f():
        if not yahoo_paused():
            try:
                return yahoo_detail(sym)
            except Exception as e:
                note_yahoo_limit(e)
                log("yahoo detail", sym, e)
        if FINNHUB_API_KEY:
            try:
                return finnhub_detail(sym)
            except Exception as e:
                log("finnhub detail", sym, e)
        return static_detail(sym)
    try:
        out = dict(cached("d:" + sym, ttl(600, 2400), f))
    except Exception as e:
        log("detail", sym, e)
        out = static_detail(sym)
    out["info"] = INFO.get(sym)
    return out

def r2(h, k):
    v = h.get(k); return v.get("raw") if isinstance(v, dict) else v

class H(BaseHTTPRequestHandler):
    def log_message(self, *a): pass
    def send(self, code, body, ctype="application/json; charset=utf-8", cache="no-store"):
        if isinstance(body, (dict, list)): body = json.dumps(body, ensure_ascii=False).encode()
        self.send_response(code); self.send_header("Content-Type", ctype); self.send_header("Cache-Control", cache)
        self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
    def do_GET(self):
        u = urllib.parse.urlparse(self.path); qs = urllib.parse.parse_qs(u.query)
        try:
            if u.path in ("/", "/index.html"):
                return self.send(200, open(os.path.join(BASE, "static", "index.html"), "rb").read(), "text/html; charset=utf-8")
            if u.path == "/api/state": return self.send(200, state())
            if u.path == "/api/chart": return self.send(200, chart(qs["s"][0].upper(), qs.get("r", ["6mo"])[0]))
            if u.path == "/api/detail": return self.send(200, detail(qs["s"][0].upper()))
            if u.path in ("/health", "/healthz"): return self.send(200, {"ok": True, "t": int(time.time()), "mode": "remote" if REMOTE else "local"})
            self.send(404, {"error": "not found"})
        except Exception as e:
            log("handler", u.path, e); traceback.print_exc()
            self.send(502, {"error": str(e)})

if __name__ == "__main__":
    threading.Thread(target=refresh_loop, daemon=True).start()
    log("Server na portu", PORT, "| portfolio:", PORTFOLIO_URL or PORTFOLIO, "| nacin:", "remote (samo memorija)" if REMOTE else "lokalno (zapisuje portfolio.json)")
    ThreadingHTTPServer(("0.0.0.0", PORT), H).serve_forever()
