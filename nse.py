#!/usr/bin/env python3
"""
nse.py - NSE Bhavcopy MCP server ka chhota local client + SQLite cache.

KYUN YEH SCRIPT:
  NSE ka MCP server CORS header me sirf NSE ke apne domains allow karta hai, aur
  region-locked bhi ho sakta hai. Isliye Claude ke cloud connector se jodna
  bharosemand nahi. CORS sirf browser ka rule hai - server-side HTTP call par
  laagu nahi hota. Yeh script wahi MCP server is laptop se seedha call karti hai
  (apne Indian connection se) aur data local SQLite me rakh leti hai.

3-MAHINE PER-CALL CAP KA TOD:
  get_stock_history ek call me max 3 mahine deta hai, par response me
  'next_end_date' bhi deta hai. Hum usi ko chain karke peeche-peeche chalte hain
  aur saare chunks ek hi table me jod dete hain -> 5 saal ka data, ek command.
  Data local aa gaya to cap khatam: analysis ke liye network hi nahi chahiye.
  Dusri baar run karne par sirf 1-2 call (naya chunk) - purana cached rehta hai.

Dependencies: koi nahi. Sirf Python 3.9+ stdlib.

NSE terms: data informational/educational use ke liye hai - real-time trading,
commercial use ya AI model training ke liye nahi. Prices unadjusted bhavcopy se
aate hain; split/bonus adjustment 'export --adjust' karta hai.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from datetime import date, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
BHAVCOPY_URL = os.environ.get("NSE_MCP_URL", "https://mcp.nseindia.in/bhavcopy/cm/mcp")
LIVE_URL = os.environ.get("NSE_LIVE_URL", "https://mcp.nseindia.in/cmmkt/mcp")
DB_PATH = os.environ.get("NSE_DB", os.path.join(HERE, "data", "nse.db"))
PAUSE = float(os.environ.get("NSE_PAUSE", "0.6"))   # chunks ke beech politeness gap
# Server kabhi-kabhi ek date par EQ row ki jagah doosri series ki row deta hai.
# W3 (warrant), N* / RE (rights) ka price poori tarah alag instrument ka hota hai -
# wo analysis me ghus gaya to return/drawdown jhoothe ho jaate hain. Yahan sirf wahi
# series rakhi hain jinka underlying wahi share hai:
#   EQ/BE/BZ = regular equity rolling segments, SM/ST = SME, BL = block deal, T0 = T+0
EQUITY_SERIES = ("EQ", "BE", "BZ", "SM", "ST", "BL", "T0")

STOCK_CHUNK_MONTHS = 3      # server ki hard limit
INDEX_CHUNK_MONTHS = 12     # index tool 12 mahine deta hai

# ----------------------------------------------------------------- MCP client


class MCPError(RuntimeError):
    """Transport / protocol level problem - retry karna theek hai."""


class ToolError(MCPError):
    """Tool ne khud error lautaya (galat symbol, galat date) - retry bekaar."""


class MCP:
    """Streamable-HTTP MCP client, utna hi jitna kaam ke liye chahiye."""

    def __init__(self, url: str = BHAVCOPY_URL, timeout: int = 120):
        self.url = url
        self.timeout = timeout
        self.sid: str | None = None
        self.calls = 0          # network calls counter (cache ka faida dikhane ke liye)
        self._id = 0

    def _next_id(self) -> int:
        self._id += 1
        return self._id

    def _post(self, body: dict) -> bytes:
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
            "Accept-Encoding": "gzip",
            "User-Agent": "nse-bhavcopy-local/1.0",
        }
        if self.sid:
            headers["Mcp-Session-Id"] = self.sid
        req = urllib.request.Request(
            self.url, data=json.dumps(body).encode("utf-8"), headers=headers, method="POST"
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            raw = resp.read()
            if (resp.headers.get("Content-Encoding") or "").lower() == "gzip":
                raw = gzip.decompress(raw)
            sid = resp.headers.get("Mcp-Session-Id")
            if sid:
                self.sid = sid
        return raw

    @staticmethod
    def _envelope(raw: bytes) -> dict:
        """Server plain JSON ya SSE ('data:' lines) dono bhej sakta hai."""
        text = raw.decode("utf-8", "replace").strip()
        if not text:
            return {}
        if text.startswith("{"):
            return json.loads(text)
        found = None
        for line in text.splitlines():
            line = line.strip()
            if line.startswith("data:"):
                chunk = line[5:].strip()
                if chunk:
                    found = json.loads(chunk)
        if found is None:
            raise MCPError("could not parse response: " + text[:200])
        return found

    def connect(self) -> dict:
        self.sid = None
        self.calls += 1
        env = self._envelope(
            self._post(
                {
                    "jsonrpc": "2.0",
                    "id": self._next_id(),
                    "method": "initialize",
                    "params": {
                        "protocolVersion": "2025-06-18",
                        "capabilities": {},
                        "clientInfo": {"name": "nse-bhavcopy-local", "version": "1.0"},
                    },
                }
            )
        )
        if "error" in env:
            raise MCPError(json.dumps(env["error"])[:300])
        self._post({"jsonrpc": "2.0", "method": "notifications/initialized"})
        return (env.get("result") or {}).get("serverInfo") or {}

    def list_tools(self) -> list[dict]:
        if self.sid is None:
            self.connect()
        self.calls += 1
        env = self._envelope(
            self._post({"jsonrpc": "2.0", "id": self._next_id(), "method": "tools/list", "params": {}})
        )
        return ((env.get("result") or {}).get("tools")) or []

    def call(self, tool: str, args: dict, retries: int = 3):
        """Tool chalao aur uska JSON payload dict/list ke roop me do."""
        last = None
        for attempt in range(1, retries + 1):
            try:
                if self.sid is None:
                    self.connect()
                self.calls += 1
                env = self._envelope(
                    self._post(
                        {
                            "jsonrpc": "2.0",
                            "id": self._next_id(),
                            "method": "tools/call",
                            "params": {"name": tool, "arguments": args},
                        }
                    )
                )
                if "error" in env:                       # session expire / protocol error
                    self.sid = None
                    raise MCPError(json.dumps(env["error"])[:300])
                result = env.get("result") or {}
                text = _result_text(result)
                if result.get("isError"):                # tool-level error: retry bekaar
                    raise ToolError(f"{tool}: {text[:300]}")
                try:
                    return json.loads(text) if text else {}
                except json.JSONDecodeError:
                    return {"text": text}
            except ToolError:
                raise
            except (MCPError, urllib.error.URLError, OSError, json.JSONDecodeError, TimeoutError) as exc:
                last = exc
                self.sid = None
            if attempt < retries:
                time.sleep(1.5 * attempt)
        raise MCPError(f"{tool} failed after {retries} attempts: {last}")


def _result_text(result: dict) -> str:
    parts = []
    for item in result.get("content") or []:
        if item.get("type") == "text":
            parts.append(item.get("text") or "")
    return "\n".join(parts)


# ----------------------------------------------------------------- local store

PRICES_DDL = """
CREATE TABLE IF NOT EXISTS prices(
  symbol TEXT NOT NULL, date TEXT NOT NULL, series TEXT NOT NULL DEFAULT 'EQ',
  open REAL, high REAL, low REAL, close REAL, prev_close REAL,
  volume INTEGER, traded_value REAL, delivery_qty INTEGER,
  PRIMARY KEY(symbol, date, series));
CREATE INDEX IF NOT EXISTS prices_date ON prices(date);
CREATE INDEX IF NOT EXISTS prices_sym_series ON prices(symbol, series);
"""

PRICES_COLS = ("symbol", "date", "series", "open", "high", "low", "close",
               "prev_close", "volume", "traded_value", "delivery_qty")

SCHEMA = PRICES_DDL + """

CREATE TABLE IF NOT EXISTS index_prices(
  index_name TEXT NOT NULL, date TEXT NOT NULL,
  open REAL, high REAL, low REAL, close REAL, points_change REAL, pct_change REAL,
  volume INTEGER, turnover_cr REAL, pe REAL, pb REAL, div_yield REAL,
  PRIMARY KEY(index_name, date));

CREATE TABLE IF NOT EXISTS actions(
  symbol TEXT NOT NULL, ex_date TEXT NOT NULL, action_type TEXT,
  purpose TEXT NOT NULL, adj_factor REAL,
  PRIMARY KEY(symbol, ex_date, purpose));

-- kahan tak maang liya hai: isse dobara wahi chunks maangne ki zarurat nahi padti
CREATE TABLE IF NOT EXISTS coverage(
  kind TEXT NOT NULL, key TEXT NOT NULL, asked_from TEXT, updated_at TEXT,
  PRIMARY KEY(kind, key));
"""


def db_open(path: str = DB_PATH) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.executescript(SCHEMA)
    _migrate_prices_pk(con)
    return con


def _migrate_prices_pk(con: sqlite3.Connection) -> None:
    """Purane DB me PK (symbol,date) tha - usme ek date ki ek hi row bachti thi."""
    info = list(con.execute("PRAGMA table_info(prices)"))
    if not info or "series" in [r["name"] for r in info if r["pk"]]:
        return
    cols = ",".join(PRICES_COLS)
    print("  (migrating prices -> PK (symbol,date,series))")
    con.executescript("ALTER TABLE prices RENAME TO prices_old;")
    con.executescript(PRICES_DDL)
    con.execute(
        f"INSERT OR REPLACE INTO prices({cols}) SELECT symbol,date,COALESCE(series,'EQ'),"
        "open,high,low,close,prev_close,volume,traded_value,delivery_qty FROM prices_old"
    )
    con.execute("DROP TABLE prices_old")
    con.executescript(SCHEMA)
    con.commit()


def _num(row: dict, key: str):
    val = row.get(key)
    return val if isinstance(val, (int, float)) else None


def put_prices(con: sqlite3.Connection, symbol: str, rows: list[dict]) -> int:
    data = [
        (
            (r.get("symbol") or symbol).upper(), r["date"], (r.get("series") or "EQ").upper(),
            _num(r, "open"), _num(r, "high"), _num(r, "low"), _num(r, "close"),
            _num(r, "prevClose"), _num(r, "volume"), _num(r, "totalTradedValue"),
            _num(r, "deliveryQty"),
        )
        for r in rows if r.get("date")
    ]
    con.executemany(
        "INSERT OR REPLACE INTO prices(symbol,date,series,open,high,low,close,"
        "prev_close,volume,traded_value,delivery_qty) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        data,
    )
    con.commit()
    return len(data)


def put_index(con: sqlite3.Connection, name: str, rows: list[dict]) -> int:
    data = [
        (
            r.get("indexName") or name, r["date"],
            _num(r, "open"), _num(r, "high"), _num(r, "low"), _num(r, "close"),
            _num(r, "pointsChange"), _num(r, "pctChange"), _num(r, "volume"),
            _num(r, "turnoverCr"), _num(r, "pe"), _num(r, "pb"), _num(r, "divYield"),
        )
        for r in rows if r.get("date")
    ]
    con.executemany(
        "INSERT OR REPLACE INTO index_prices(index_name,date,open,high,low,close,"
        "points_change,pct_change,volume,turnover_cr,pe,pb,div_yield) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        data,
    )
    con.commit()
    return len(data)


def span(con: sqlite3.Connection, table: str, col: str, key: str):
    extra = ""
    if table == "prices":                      # sirf asli equity series gino
        extra = " AND series IN (%s)" % ",".join("'%s'" % x for x in EQUITY_SERIES)
    row = con.execute(
        f"SELECT MIN(date) a, MAX(date) b, COUNT(DISTINCT date) n FROM {table} "
        f"WHERE {col}=?{extra}", (key,)
    ).fetchone()
    return row["a"], row["b"], row["n"]


def cov_get(con: sqlite3.Connection, kind: str, key: str):
    row = con.execute("SELECT asked_from FROM coverage WHERE kind=? AND key=?", (kind, key)).fetchone()
    return row["asked_from"] if row else None


def cov_set(con: sqlite3.Connection, kind: str, key: str, asked_from: str) -> None:
    con.execute(
        "INSERT INTO coverage(kind,key,asked_from,updated_at) VALUES(?,?,?,?) "
        "ON CONFLICT(kind,key) DO UPDATE SET "
        "asked_from=MIN(coverage.asked_from, excluded.asked_from), updated_at=excluded.updated_at",
        (kind, key, asked_from, date.today().isoformat()),
    )
    con.commit()


# ----------------------------------------------------------------- chunk chaining


def back_years(years: float) -> str:
    return (date.today() - timedelta(days=int(365.25 * years))).isoformat()


def prev_iso(iso: str) -> str:
    return (date.fromisoformat(iso) - timedelta(days=1)).isoformat()


def chain_back(fetch, store, end_date: str, target_start: str, max_chunks: int = 60,
               tag: str = "chunk", quiet: bool = False) -> int:
    """
    'next_end_date' ko chain karke peeche chalo, jab tak target_start cover na ho.
    fetch(end_date) -> server payload   |   store(rows) -> kitni rows likhi
    """
    end = end_date
    written = 0
    for i in range(1, max_chunks + 1):
        payload = fetch(end)
        rows = payload.get("data") or []
        if rows:
            written += store(rows)
        if not quiet:
            name = tag if max_chunks == 1 else f"{tag} {i}"
            print(
                f"    {name:<9} {payload.get('from_date') or '-':<12} -> "
                f"{payload.get('to_date') or '-':<12} {len(rows):>4} rows"
            )
        if not rows:                                   # itna purana data hi nahi hai
            break
        nxt = payload.get("next_end_date")
        got_from = payload.get("from_date")
        if not nxt:
            break
        if got_from and got_from <= target_start:      # target cover ho gaya
            break
        end = nxt
        time.sleep(PAUSE)
    return written


def sync_stock(mcp: MCP, con: sqlite3.Connection, symbol: str, years: float = 5, actions: bool = True) -> None:
    symbol = symbol.strip().upper()
    target = back_years(years)
    before = span(con, "prices", "symbol", symbol)[2]
    print(f"[{symbol}] want {target} -> today")

    fetch = lambda end: mcp.call(
        "get_stock_history", {"symbol": symbol, "months": STOCK_CHUNK_MONTHS, "endDate": end}
    )
    store = lambda rows: put_prices(con, symbol, rows)

    # 1) sabse naya chunk - hamesha (aaj/kal ka data taaza rakhne ke liye, 1 call)
    chain_back(fetch, store, "today", back_years(0), max_chunks=1, tag="latest")

    # 2) peeche ki taraf tabhi chalo jab pichhli baar target tak maanga nahi gaya
    asked = cov_get(con, "stock", symbol)
    oldest = span(con, "prices", "symbol", symbol)[0]
    if (asked is None or asked > target) and oldest:
        chain_back(fetch, store, prev_iso(oldest), target)
        cov_set(con, "stock", symbol, target)

    lo, hi, n = span(con, "prices", "symbol", symbol)
    if n == 0:
        print(f"[{symbol}] nothing found - the symbol looks wrong. Try:\n"
              f"           python3 nse.py call nse_lookup_symbol query={symbol[:4].lower()}")
        return

    if actions:
        sync_actions(mcp, con, symbol, years)

    print(f"[{symbol}] local: {n} trading days  ({lo} -> {hi})  +{n - before} new")


def sync_index(mcp: MCP, con: sqlite3.Connection, name: str, years: float = 5) -> None:
    target = back_years(years)
    before = span(con, "index_prices", "index_name", name)[2]
    print(f"[{name}] want {target} -> today")

    fetch = lambda end: mcp.call(
        "get_index_history", {"indexName": name, "months": INDEX_CHUNK_MONTHS, "endDate": end}
    )
    store = lambda rows: put_index(con, name, rows)

    chain_back(fetch, store, "today", back_years(0), max_chunks=1, tag="latest")
    asked = cov_get(con, "index", name)
    oldest = span(con, "index_prices", "index_name", name)[0]
    if (asked is None or asked > target) and oldest:
        chain_back(fetch, store, prev_iso(oldest), target)
        cov_set(con, "index", name, target)

    lo, hi, n = span(con, "index_prices", "index_name", name)
    print(f"[{name}] local: {n} days  ({lo} -> {hi})  +{n - before} new")


def sync_actions(mcp: MCP, con: sqlite3.Connection, symbol: str, years: float = 5) -> int:
    payload = mcp.call(
        "get_corporate_actions",
        {"symbol": symbol, "fromDate": back_years(years), "toDate": date.today().isoformat()},
    )
    rows = payload.get("actions") or []
    con.executemany(
        "INSERT OR REPLACE INTO actions(symbol,ex_date,action_type,purpose,adj_factor) VALUES(?,?,?,?,?)",
        [
            (symbol, a.get("exDate"), a.get("actionType"), a.get("purpose") or "", a.get("adjustmentFactor"))
            for a in rows if a.get("exDate")
        ],
    )
    con.commit()
    splits = [a for a in rows if (a.get("adjustmentFactor") or 1.0) != 1.0]
    print(f"    corporate actions: {len(rows)} (price-affecting: {len(splits)})")
    return len(rows)


# ----------------------------------------------------------------- analysis (offline)


def factor_list(con: sqlite3.Connection, symbol: str) -> list[tuple[str, float]]:
    """Sirf wahi actions jo price badalte hain (split/bonus), ex_date ke saath."""
    return [
        (r["ex_date"], float(r["adj_factor"]))
        for r in con.execute(
            "SELECT ex_date, adj_factor FROM actions WHERE symbol=? AND adj_factor IS NOT NULL "
            "AND (adj_factor < 0.9999 OR adj_factor > 1.0001) ORDER BY ex_date", (symbol,)
        )
    ]


def cum_factor(factors: list[tuple[str, float]], on: str) -> float:
    """'on' date ke baad aane wale saare factors ka product."""
    f = 1.0
    for ex_date, fac in factors:
        if ex_date > on:
            f *= fac
    return f


def skipped_series(con: sqlite3.Connection, symbol: str) -> list[tuple[str, int]]:
    """Jo rows analysis se bahar hain (warrant/rights jaisi series)."""
    marks = ",".join("?" * len(EQUITY_SERIES))
    return [
        (r["series"], r["n"]) for r in con.execute(
            f"SELECT series, COUNT(*) n FROM prices WHERE symbol=? AND series NOT IN ({marks}) "
            "GROUP BY series ORDER BY n DESC", (symbol, *EQUITY_SERIES))
    ]


def series(con: sqlite3.Connection, symbol: str, adjust: bool = True,
           all_series: bool = False) -> list[dict]:
    factors = factor_list(con, symbol) if adjust else []
    where, params = "symbol=?", [symbol]
    if not all_series:
        where += " AND series IN (%s)" % ",".join("?" * len(EQUITY_SERIES))
        params += list(EQUITY_SERIES)
    rank = " ".join(f"WHEN '{x}' THEN {i}" for i, x in enumerate(EQUITY_SERIES))
    out, seen = [], set()
    for r in con.execute(
        "SELECT date,series,open,high,low,close,prev_close,volume,traded_value FROM prices "
        f"WHERE {where} ORDER BY date, CASE series {rank} ELSE 99 END", params
    ):
        if r["date"] in seen:          # ek hi din ki doosri series - chhod do
            continue
        seen.add(r["date"])
        row = dict(r)
        f = cum_factor(factors, row["date"]) if factors else 1.0
        row["adj_factor"] = round(f, 6)
        if f != 1.0:
            for k in ("open", "high", "low", "close", "prev_close"):
                if row[k] is not None:
                    row[k] = round(row[k] * f, 4)
            if row["volume"]:
                row["volume"] = int(row["volume"] / f)
        out.append(row)
    return out


def stats(con: sqlite3.Connection, symbol: str, adjust: bool = True,
          all_series: bool = False) -> None:
    rows = series(con, symbol, adjust, all_series)
    if not rows:
        print(f"{symbol}: nothing in the local DB. First run:  python3 nse.py sync {symbol}")
        return
    closes = [r["close"] for r in rows if r["close"] is not None]
    dates = [r["date"] for r in rows if r["close"] is not None]
    last, first = closes[-1], closes[0]
    peak, mdd = closes[0], 0.0
    for c in closes:
        peak = max(peak, c)
        mdd = min(mdd, (c / peak - 1) * 100)
    win = [(c, d) for c, d in zip(closes, dates) if d >= back_years(1)] or list(zip(closes, dates))
    hi, lo = max(win), min(win)
    sma = lambda n: round(sum(closes[-n:]) / n, 2) if len(closes) >= n else None
    label = "adjusted (split/bonus)" if adjust and any(r["adj_factor"] != 1.0 for r in rows) else "raw bhavcopy"
    print(f"\n{symbol}  [{label}]  {dates[0]} -> {dates[-1]}  {len(closes)} trading days")
    print(f"  last close      {last:>12,.2f}")
    print(f"  period return   {((last / first) - 1) * 100:>12,.2f} %   (first close {first:,.2f})")
    print(f"  52w high        {hi[0]:>12,.2f}   {hi[1]}   ({(last / hi[0] - 1) * 100:+.2f} % away)")
    print(f"  52w low         {lo[0]:>12,.2f}   {lo[1]}   ({(last / lo[0] - 1) * 100:+.2f} % away)")
    print(f"  max drawdown    {mdd:>12,.2f} %")
    for n in (20, 50, 200):
        val = sma(n)
        extra = f"   ({(last / val - 1) * 100:+.2f} %)" if val else ""
        print(f"  SMA{n:<4}        {(f'{val:,.2f}' if val else 'n/a'):>12}{extra}")
    vols = [r["volume"] for r in rows[-20:] if r["volume"]]
    if vols:
        print(f"  avg vol (20d)   {int(sum(vols) / len(vols)):>12,}")
    dropped = skipped_series(con, symbol)
    if dropped and not all_series:
        detail = ", ".join(f"{s_}x{n}" for s_, n in dropped)
        print(f"  note: {sum(n for _, n in dropped)} rows excluded ({detail}) - "
              f"server returned a non-EQ series on those dates")
    print("  (network calls: 0 - all from the local cache)")


# ----------------------------------------------------------------- CLI


def parse_kv(pairs: list[str]) -> dict:
    args = {}
    for p in pairs:
        if "=" not in p:
            raise SystemExit(f"argument must be key=value: {p}")
        k, v = p.split("=", 1)
        try:
            args[k] = json.loads(v)
        except json.JSONDecodeError:
            args[k] = v
    return args


def symbols_from(args) -> list[str]:
    out = [s.strip().upper() for s in (args.symbols or []) if s.strip()]
    if args.file:
        with open(args.file) as fh:
            for line in fh:
                line = line.split("#", 1)[0].strip()
                if line:
                    out.append(line.upper())
    return list(dict.fromkeys(out))


def main() -> None:
    ap = argparse.ArgumentParser(
        prog="nse.py", description="NSE Bhavcopy MCP -> local SQLite (chunk-chaining lifts the 3-month per-call cap)"
    )
    ap.add_argument("--db", default=DB_PATH, help=f"SQLite file (default: {DB_PATH})")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("tools", help="list every tool the server exposes")
    p.add_argument("--live", action="store_true", help="tools on the cm-market (live) server")

    p = sub.add_parser("call", help="run any MCP tool directly")
    p.add_argument("tool")
    p.add_argument("args", nargs="*", help="key=value (the value may be JSON)")
    p.add_argument("--live", action="store_true", help="use the cm-market (today, live) server")

    p = sub.add_parser("sync", help="fill the local DB with stock history + corporate actions")
    p.add_argument("symbols", nargs="*")
    p.add_argument("--file", help="file with one symbol per line (watchlist.txt)")
    p.add_argument("--years", type=float, default=5)
    p.add_argument("--no-actions", action="store_true")

    p = sub.add_parser("sync-index", help="fill the local DB with index history")
    p.add_argument("names", nargs="+", help='e.g. "Nifty 50" "Nifty Bank"')
    p.add_argument("--years", type=float, default=5)

    p = sub.add_parser("update", help="top up everything already in the DB to today")
    p.add_argument("--years", type=float, default=5)

    sub.add_parser("status", help="what the local cache holds")

    p = sub.add_parser("stats", help="return/52w/SMA/drawdown from local data (0 network calls)")
    p.add_argument("symbol")
    p.add_argument("--raw", action="store_true", help="do not adjust for splits/bonuses")
    p.add_argument("--all-series", action="store_true",
                   help="include warrant/rights-style series too (mixes different instruments)")

    p = sub.add_parser("export", help="write a CSV")
    p.add_argument("symbol")
    p.add_argument("--out", help="default: data/<SYMBOL>.csv")
    p.add_argument("--raw", action="store_true", help="unadjusted bhavcopy prices")
    p.add_argument("--all-series", action="store_true", help="include non-equity series too")

    p = sub.add_parser("sql", help="read-only SELECT against the local DB")
    p.add_argument("query")
    p.add_argument("--csv", action="store_true")

    args = ap.parse_args()
    t0 = time.time()

    if args.cmd == "tools":
        url = LIVE_URL if args.live else BHAVCOPY_URL
        mcp = MCP(url)
        info = mcp.connect()
        print(f"server: {info.get('name')} v{info.get('version')}  <{url}>\n")
        for t in sorted(mcp.list_tools(), key=lambda x: x["name"]):
            schema = t.get("inputSchema") or {}
            props = list((schema.get("properties") or {}).keys())
            print(f"  {t['name']:<24} {','.join(props)}")
        return

    if args.cmd == "call":
        mcp = MCP(LIVE_URL if args.live else BHAVCOPY_URL)
        print(json.dumps(mcp.call(args.tool, parse_kv(args.args)), indent=2, ensure_ascii=False))
        return

    con = db_open(args.db)

    if args.cmd == "sync":
        syms = symbols_from(args)
        if not syms:
            raise SystemExit("give a symbol, or --file watchlist.txt")
        mcp = MCP()
        for s in syms:
            sync_stock(mcp, con, s, args.years, actions=not args.no_actions)
        print(f"\nnetwork calls: {mcp.calls}   time: {time.time() - t0:.1f}s   db: {args.db}")
        return

    if args.cmd == "sync-index":
        mcp = MCP()
        for n in args.names:
            sync_index(mcp, con, n, args.years)
        print(f"\nnetwork calls: {mcp.calls}   time: {time.time() - t0:.1f}s")
        return

    if args.cmd == "update":
        mcp = MCP()
        stocks = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM prices ORDER BY 1")]
        idx = [r[0] for r in con.execute("SELECT DISTINCT index_name FROM index_prices ORDER BY 1")]
        for s in stocks:
            sync_stock(mcp, con, s, args.years, actions=False)
        for n in idx:
            sync_index(mcp, con, n, args.years)
        print(f"\n{len(stocks)} stocks + {len(idx)} indices  |  network calls: {mcp.calls}  "
              f"time: {time.time() - t0:.1f}s")
        return

    if args.cmd == "status":
        print(f"db: {args.db}  ({os.path.getsize(args.db) / 1e6:.1f} MB)\n")
        eq = ",".join("'%s'" % x for x in EQUITY_SERIES)
        rows = list(con.execute(
            f"SELECT p.symbol k, COUNT(DISTINCT CASE WHEN p.series IN ({eq}) THEN p.date END) n, "
            f"MIN(CASE WHEN p.series IN ({eq}) THEN p.date END) a, "
            f"MAX(CASE WHEN p.series IN ({eq}) THEN p.date END) b, "
            f"SUM(p.series NOT IN ({eq})) skip, "
            "(SELECT COUNT(*) FROM actions x WHERE x.symbol=p.symbol) ca "
            "FROM prices p GROUP BY p.symbol ORDER BY 1"))
        if rows:
            print(f"  {'SYMBOL':<14}{'DAYS':>6}{'SKIP':>6}  {'FROM':<12}{'TO':<12}{'ACTIONS':>8}")
            for r in rows:
                print(f"  {r['k']:<14}{r['n']:>6}{r['skip']:>6}  "
                      f"{(r['a'] or '-'):<12}{(r['b'] or '-'):<12}{r['ca']:>8}")
            print("  SKIP = warrant/rights-style series rows (excluded from analysis)")
        idx = list(con.execute(
            "SELECT index_name k, COUNT(*) n, MIN(date) a, MAX(date) b FROM index_prices GROUP BY 1 ORDER BY 1"))
        if idx:
            print(f"\n  {'INDEX':<22}{'DAYS':>6}  {'FROM':<12}{'TO':<12}")
            for r in idx:
                print(f"  {r['k']:<22}{r['n']:>6}  {r['a']:<12}{r['b']:<12}")
        tot = con.execute("SELECT COUNT(*) FROM prices").fetchone()[0]
        print(f"\n  total price rows: {tot:,}")
        return

    if args.cmd == "stats":
        stats(con, args.symbol.upper(), adjust=not args.raw, all_series=args.all_series)
        return

    if args.cmd == "export":
        sym = args.symbol.upper()
        rows = series(con, sym, adjust=not args.raw, all_series=args.all_series)
        if not rows:
            raise SystemExit(f"{sym}: no local data. First run: python3 nse.py sync {sym}")
        out = args.out or os.path.join(os.path.dirname(args.db), f"{sym}.csv")
        with open(out, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        kind = "unadjusted" if args.raw else "split/bonus adjusted"
        print(f"{len(rows)} rows -> {out}  ({kind})")
        dropped = skipped_series(con, sym)
        if dropped and not args.all_series:
            print("  excluded: " + ", ".join(f"{s_}x{n}" for s_, n in dropped))
        return

    if args.cmd == "sql":
        q = args.query.strip()
        if not q.lower().startswith(("select", "with", "pragma")):
            raise SystemExit("only SELECT / WITH / PRAGMA is allowed")
        cur = con.execute(q)
        cols = [d[0] for d in cur.description]
        rows = cur.fetchall()
        if args.csv:
            w = csv.writer(sys.stdout)
            w.writerow(cols)
            w.writerows(rows)
        else:
            print("  ".join(c.upper() for c in cols))
            for r in rows:
                print("  ".join("" if v is None else str(v) for v in r))
            print(f"\n{len(rows)} rows")
        return


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
    except MCPError as exc:
        print(f"MCP error: {exc}", file=sys.stderr)
        sys.exit(1)
