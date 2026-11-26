#!/usr/bin/env python3
"""
serve.py - nse.py ke upar ek chhota local web UI.

    python3 serve.py            # http://127.0.0.1:8765

Sirf stdlib. 127.0.0.1 par bind hota hai, to LAN/internet se koi nahi aa sakta.
Browser se cross-site request na aaye, is liye Origin/Referer check bhi hai
(warna koi website localhost par sync trigger kar sakti thi).

API:
  GET  /api/catalog                         local DB me kya hai
  GET  /api/series?symbol=&range=&adjust=   chart + stats ke liye
  GET  /api/compare?symbols=a,b&range=      indexed-to-100 comparison
  GET  /api/export?symbol=&adjust=          CSV download
  POST /api/sync  {"symbols":[],"years":5}  MCP se data bharo (network)
"""

from __future__ import annotations

import io
import json
import mimetypes
import os
import sys
import threading
import urllib.parse
from contextlib import redirect_stdout
from datetime import date, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import nse

HERE = os.path.dirname(os.path.abspath(__file__))
WEB_DIR = os.path.join(HERE, "web")
HOST = os.environ.get("NSE_UI_HOST", "127.0.0.1")
PORT = int(os.environ.get("NSE_UI_PORT", "8765"))

RANGES = {"1M": 31, "3M": 92, "6M": 183, "1Y": 366, "3Y": 1096, "5Y": 1827, "ALL": None}
_sync_lock = threading.Lock()          # ek waqt me ek hi sync (MCP server par narmi)


# ------------------------------------------------------------------ data layer


def catalog() -> dict:
    con = nse.db_open()
    eq = ",".join("'%s'" % x for x in nse.EQUITY_SERIES)
    stocks = [
        {"symbol": r["k"], "days": r["n"], "from": r["a"], "to": r["b"],
         "skip": r["skip"], "actions": r["ca"]}
        for r in con.execute(
            f"SELECT p.symbol k, COUNT(DISTINCT CASE WHEN p.series IN ({eq}) THEN p.date END) n,"
            f" MIN(CASE WHEN p.series IN ({eq}) THEN p.date END) a,"
            f" MAX(CASE WHEN p.series IN ({eq}) THEN p.date END) b,"
            f" SUM(p.series NOT IN ({eq})) skip,"
            "  (SELECT COUNT(*) FROM actions x WHERE x.symbol=p.symbol) ca "
            "FROM prices p GROUP BY p.symbol ORDER BY 1")
        if r["n"]
    ]
    indices = [
        {"name": r["k"], "days": r["n"], "from": r["a"], "to": r["b"]}
        for r in con.execute("SELECT index_name k, COUNT(*) n, MIN(date) a, MAX(date) b "
                             "FROM index_prices GROUP BY 1 ORDER BY 1")
    ]
    rows = con.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
    con.close()
    size = os.path.getsize(nse.DB_PATH) / 1e6 if os.path.exists(nse.DB_PATH) else 0
    return {"stocks": stocks, "indices": indices,
            "db": {"path": nse.DB_PATH, "rows": rows, "size_mb": round(size, 2)}}


def _cut(rng: str) -> str | None:
    days = RANGES.get((rng or "1Y").upper(), 366)
    return None if days is None else (date.today() - timedelta(days=days)).isoformat()


def _sma(closes: list[float], n: int):
    return round(sum(closes[-n:]) / n, 2) if len(closes) >= n else None


def _range_stats(rows: list[dict]) -> dict:
    closes = [r["close"] for r in rows if r["close"] is not None]
    dates = [r["date"] for r in rows if r["close"] is not None]
    if not closes:
        return {}
    peak, mdd, trough = closes[0], 0.0, dates[0]
    for c, d in zip(closes, dates):
        peak = max(peak, c)
        dd = (c / peak - 1) * 100
        if dd < mdd:
            mdd, trough = dd, d
    hi = max(zip(closes, dates))
    lo = min(zip(closes, dates))
    vols = [r["volume"] for r in rows[-20:] if r["volume"]]
    return {
        "first": closes[0], "first_date": dates[0], "last": closes[-1], "last_date": dates[-1],
        "ret": round((closes[-1] / closes[0] - 1) * 100, 2),
        "hi": {"v": hi[0], "d": hi[1]}, "lo": {"v": lo[0], "d": lo[1]},
        "mdd": round(mdd, 2), "mdd_date": trough,
        "avg_vol": int(sum(vols) / len(vols)) if vols else None,
        "days": len(closes),
    }


def series_payload(symbol: str, rng: str, adjust: bool) -> dict:
    con = nse.db_open()
    full = nse.series(con, symbol, adjust=adjust)
    if not full:
        con.close()
        return {"symbol": symbol, "rows": [], "error": "no local data for this symbol"}
    closes_full = [r["close"] for r in full if r["close"] is not None]
    cut = _cut(rng)
    rows = [r for r in full if cut is None or r["date"] >= cut] or full[-2:]
    actions = [
        {"ex": r["ex_date"], "type": r["action_type"], "purpose": r["purpose"],
         "factor": r["adj_factor"]}
        for r in con.execute("SELECT ex_date,action_type,purpose,adj_factor FROM actions "
                             "WHERE symbol=? ORDER BY ex_date DESC", (symbol,))
    ]
    skipped = nse.skipped_series(con, symbol)
    con.close()
    return {
        "symbol": symbol, "range": rng, "adjust": adjust,
        # compact: [date, open, high, low, close, volume]
        "rows": [[r["date"], r["open"], r["high"], r["low"], r["close"], r["volume"]] for r in rows],
        "stats": _range_stats(rows),
        # SMA poore local history se (definition hi "last N trading days" hai)
        "sma": {n: _sma(closes_full, n) for n in (20, 50, 200)},
        "full_span": {"from": full[0]["date"], "to": full[-1]["date"], "days": len(full)},
        "actions": actions,
        "skipped": [{"series": s, "n": n} for s, n in skipped],
    }


def compare_payload(symbols: list[str], rng: str, adjust: bool) -> dict:
    con = nse.db_open()
    cut = _cut(rng)
    out = []
    for sym in symbols[:4]:                       # 4 se zyada lines par labels tootne lagte hain
        full = nse.series(con, sym, adjust=adjust)
        rows = [r for r in full if cut is None or r["date"] >= cut]
        rows = [r for r in rows if r["close"]]
        if len(rows) < 2:
            continue
        base = rows[0]["close"]
        out.append({
            "symbol": sym,
            "points": [[r["date"], round(r["close"] / base * 100, 3)] for r in rows],
            "ret": round((rows[-1]["close"] / base - 1) * 100, 2),
            "from": rows[0]["date"], "to": rows[-1]["date"],
        })
    con.close()
    out.sort(key=lambda s: -s["ret"])
    return {"range": rng, "adjust": adjust, "series": out}


def index_payload(name: str, rng: str) -> dict:
    con = nse.db_open()
    cut = _cut(rng)
    rows = [
        dict(r) for r in con.execute(
            "SELECT date,open,high,low,close,volume,pe,pb,div_yield FROM index_prices "
            "WHERE index_name=? ORDER BY date", (name,))
        if cut is None or r["date"] >= cut
    ]
    con.close()
    return {
        "name": name, "range": rng,
        "rows": [[r["date"], r["open"], r["high"], r["low"], r["close"], r["volume"]] for r in rows],
        "stats": _range_stats(rows),
        "valuation": ({"pe": rows[-1]["pe"], "pb": rows[-1]["pb"], "div": rows[-1]["div_yield"]}
                      if rows else {}),
    }


def run_sync(symbols: list[str], years: float, indices: list[str]) -> dict:
    """nse.py ka sync chalao aur uska console output UI ko wapas do."""
    if not _sync_lock.acquire(blocking=False):
        return {"ok": False, "log": "A sync is already running — wait for it to finish."}
    buf = io.StringIO()
    try:
        mcp = nse.MCP()
        con = nse.db_open()
        with redirect_stdout(buf):
            for s in symbols:
                nse.sync_stock(mcp, con, s, years)
            for n in indices:
                nse.sync_index(mcp, con, n, years)
            print(f"\nnetwork calls: {mcp.calls}")
        con.close()
        return {"ok": True, "log": buf.getvalue(), "calls": mcp.calls}
    except Exception as exc:                       # UI ko error dikhna chahiye, crash nahi
        return {"ok": False, "log": buf.getvalue() + f"\nERROR: {exc}"}
    finally:
        _sync_lock.release()


# ------------------------------------------------------------------ http layer


class Handler(BaseHTTPRequestHandler):
    server_version = "nse-ui/1.0"
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):             # ek line, chhoti
        sys.stderr.write("  %s %s\n" % (self.command, self.path.split("?")[0]))

    # --- helpers -------------------------------------------------------
    def _send(self, code: int, body: bytes, ctype: str, extra: dict | None = None) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj, code: int = 200) -> None:
        self._send(code, json.dumps(obj, allow_nan=False).encode(), "application/json")

    def _same_site(self) -> bool:
        """Kisi doosri website ka page localhost par request na bhej sake."""
        origin = self.headers.get("Origin")
        if origin:
            host = urllib.parse.urlparse(origin).hostname
            return host in ("127.0.0.1", "localhost", "::1")
        return True                                 # seedha browser/curl - Origin nahi aata

    # --- routes --------------------------------------------------------
    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path, q = parsed.path, urllib.parse.parse_qs(parsed.query)
        one = lambda k, d="": (q.get(k) or [d])[0]
        try:
            if path == "/api/catalog":
                return self._json(catalog())
            if path == "/api/series":
                sym = one("symbol").upper()
                if not sym:
                    return self._json({"error": "symbol is required"}, 400)
                return self._json(series_payload(sym, one("range", "1Y"), one("adjust", "1") == "1"))
            if path == "/api/compare":
                syms = [s.strip().upper() for s in one("symbols").split(",") if s.strip()]
                return self._json(compare_payload(syms, one("range", "1Y"), one("adjust", "1") == "1"))
            if path == "/api/index":
                return self._json(index_payload(one("name"), one("range", "1Y")))
            if path == "/api/export":
                return self._export(one("symbol").upper(), one("adjust", "1") == "1")
            return self._static(path)
        except BrokenPipeError:
            pass
        except Exception as exc:
            self._json({"error": str(exc)}, 500)

    def do_POST(self) -> None:
        if not self._same_site():
            return self._json({"error": "cross-site request blocked"}, 403)
        if urllib.parse.urlparse(self.path).path != "/api/sync":
            return self._json({"error": "not found"}, 404)
        try:
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(n) or b"{}")
            syms = [str(s).strip().upper() for s in (body.get("symbols") or []) if str(s).strip()]
            idx = [str(s).strip() for s in (body.get("indices") or []) if str(s).strip()]
            years = float(body.get("years") or 5)
            if not syms and not idx:
                return self._json({"ok": False, "log": "Give at least one symbol or index"}, 400)
            res = run_sync(syms[:25], max(0.1, min(years, 5)), idx[:10])
            res["catalog"] = catalog()
            return self._json(res)
        except Exception as exc:
            self._json({"ok": False, "log": f"ERROR: {exc}"}, 500)

    def _export(self, symbol: str, adjust: bool) -> None:
        con = nse.db_open()
        rows = nse.series(con, symbol, adjust=adjust)
        con.close()
        if not rows:
            return self._json({"error": "no local data"}, 404)
        buf = io.StringIO()
        import csv as _csv
        w = _csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)
        name = f"{symbol}{'' if adjust else '-raw'}.csv"
        self._send(200, buf.getvalue().encode(), "text/csv; charset=utf-8",
                   {"Content-Disposition": f'attachment; filename="{name}"'})

    def _static(self, path: str) -> None:
        rel = "index.html" if path in ("/", "") else path.lstrip("/")
        target = os.path.realpath(os.path.join(WEB_DIR, rel))
        if not target.startswith(os.path.realpath(WEB_DIR) + os.sep) or not os.path.isfile(target):
            return self._send(404, b"not found", "text/plain")
        ctype = mimetypes.guess_type(target)[0] or "application/octet-stream"
        with open(target, "rb") as fh:
            self._send(200, fh.read(), ctype)


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def main() -> None:
    cat = catalog()
    srv = Server((HOST, PORT), Handler)
    print(f"  NSE Bhavcopy UI   http://{HOST}:{PORT}")
    print(f"  db: {cat['db']['path']}  ({cat['db']['rows']:,} rows, "
          f"{len(cat['stocks'])} symbols, {len(cat['indices'])} indices)")
    print("  Ctrl-C to stop\n")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\n  bye")
    finally:
        srv.server_close()


if __name__ == "__main__":
    main()
