#!/usr/bin/env python3
"""Burzovni terminal - mali backend: proxy za Yahoo Finance + posluzivanje frontenda + auto SL/TP."""
import json, os, time, threading, urllib.request, urllib.parse, http.cookiejar, datetime, traceback
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

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

cj = http.cookiejar.CookieJar()
opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(cj))
opener.addheaders = [("User-Agent", UA), ("Accept", "application/json,text/plain,*/*")]
_crumb = {"v": None, "t": 0}
_lock = threading.Lock()
_pf_lock = threading.Lock()
_cache = {}

def log(*a):
    print(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), *a, flush=True)

def get(url, timeout=15):
    with opener.open(url, timeout=timeout) as r:
        return r.read()

def crumb(force=False):
    with _lock:
        if force or not _crumb["v"] or time.time() - _crumb["t"] > 3600:
            try: get("https://fc.yahoo.com", 10)
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
            if e.code in (401, 403) and attempt == 0 and need_crumb:
                continue
            raise

def cached(key, ttl, fn):
    now = time.time()
    c = _cache.get(key)
    if c and now - c[0] < ttl:
        return c[1]
    try:
        v = fn()
        _cache[key] = (now, v)
        return v
    except Exception as e:
        log("greska", key, e)
        if c: return c[1]
        raise

def _read_local():
    with open(PORTFOLIO, encoding="utf-8") as f:
        return json.load(f)

def load_pf():
    if not REMOTE:
        return _read_local()
    if PORTFOLIO_URL and (_remote["p"] is None or time.time() - _remote["t"] > REMOTE_TTL):
        try:
            req = urllib.request.Request(PORTFOLIO_URL + ("&" if "?" in PORTFOLIO_URL else "?") + "t=" + str(int(time.time())), headers={"User-Agent": UA, "Cache-Control": "no-cache"})
            with urllib.request.urlopen(req, timeout=15) as r:
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

def quotes(syms):
    def f():
        d = yget("https://query1.finance.yahoo.com/v7/finance/quote?symbols=" + ",".join(syms), True)
        keep = ["symbol","longName","shortName","regularMarketPrice","regularMarketChange","regularMarketChangePercent",
                "regularMarketTime","regularMarketDayHigh","regularMarketDayLow","regularMarketOpen","regularMarketPreviousClose",
                "regularMarketVolume","averageDailyVolume3Month","fiftyTwoWeekLow","fiftyTwoWeekHigh","marketCap","trailingPE","forwardPE",
                "epsTrailingTwelveMonths","marketState","preMarketPrice","preMarketChangePercent","postMarketPrice","postMarketChangePercent",
                "postMarketTime","preMarketTime","currency","financialCurrency","fiftyDayAverage","twoHundredDayAverage","dividendYield","exchangeTimezoneName"]
        return [{k: q.get(k) for k in keep} for q in d["quoteResponse"]["result"]]
    return cached("q:" + ",".join(syms), 12, f)

def spark(syms):
    def f():
        out = {}
        for i in range(0, len(syms), 10):
            d = yget("https://query1.finance.yahoo.com/v8/finance/spark?range=1d&interval=5m&symbols=" + ",".join(syms[i:i+10]))
            for k, v in d.items():
                out[k] = {"c": [x for x in (v.get("close") or []) if x is not None], "prev": v.get("chartPreviousClose") or v.get("previousClose")}
        return out
    return cached("spark:" + ",".join(syms), 60, f)

def et_date(ts):
    # US/Eastern bez pytz: EST/EDT priblizno preko zoneinfo
    from zoneinfo import ZoneInfo
    return datetime.datetime.fromtimestamp(ts, ZoneInfo("America/New_York")).date().isoformat()

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

def state():
    p = load_pf()
    syms = symbols(p)
    qs = quotes(syms)
    try: p = check_sl_tp(qs)
    except Exception as e: log("sl/tp greska", e)
    try: sp = spark(syms)
    except Exception: sp = {}
    ms = next((q.get("marketState") for q in qs if q["symbol"] in ("MSFT","AAPL","NVDA")), qs[0].get("marketState") if qs else None)
    return {"serverTime": int(time.time()), "marketState": ms, "quotes": qs, "spark": sp, "portfolio": p,
            "info": {k: {"name": v["name"], "why": v["why"]} for k, v in INFO.items()}}

RANGES = {"1d": ("1d","5m",30), "5d": ("5d","15m",60), "1mo": ("1mo","60m",300), "6mo": ("6mo","1d",900), "1y": ("1y","1d",900)}

def chart(sym, rng):
    r, iv, ttl = RANGES.get(rng, RANGES["6mo"])
    def f():
        d = yget(f"https://query1.finance.yahoo.com/v8/finance/chart/{urllib.parse.quote(sym)}?range={r}&interval={iv}&includePrePost=false")
        res = d["chart"]["result"][0]; q = res["indicators"]["quote"][0]
        pts = [[t, c] for t, c in zip(res.get("timestamp", []), q["close"]) if c is not None]
        return {"symbol": sym, "range": rng, "points": pts, "prev": res["meta"].get("chartPreviousClose"), "meta": {k: res["meta"].get(k) for k in ("regularMarketPrice","currency","exchangeTimezoneName")}}
    return cached(f"c:{sym}:{rng}", ttl, f)

def detail(sym):
    def f():
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
        except Exception as e: log("news", e)
        return {"fund": fund, "eps": eh, "nextEarnings": nxt, "news": news}
    out = dict(cached("d:" + sym, 600, f))
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

def bg_loop():
    # SL/TP se provjerava i kad nitko nema otvorenu stranicu
    while True:
        try:
            p = load_pf()
            if any(t.get("status") == "open" for t in p.get("trades", [])):
                qs = quotes(symbols(p)); check_sl_tp(qs)
                ms = qs[0].get("marketState") if qs else None
                time.sleep(15 if ms == "REGULAR" else 120)
            else: time.sleep(120)
        except Exception as e:
            log("bg", e); time.sleep(60)

if __name__ == "__main__":
    threading.Thread(target=bg_loop, daemon=True).start()
    log("Server na portu", PORT, "| portfolio:", PORTFOLIO_URL or PORTFOLIO, "| nacin:", "remote (samo memorija)" if REMOTE else "lokalno (zapisuje portfolio.json)")
    ThreadingHTTPServer(("0.0.0.0", PORT), H).serve_forever()
