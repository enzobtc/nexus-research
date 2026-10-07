"""NEXUS Multi-Exchange Bridge V1.4 — Liquidity V2.2.

Public-data collector only. No exchange API keys and no order endpoints.
Designed for a small always-on web service (one worker) and posts a compact
snapshot to NEXUS every second.

V1.4 keeps the existing payload fields and upgrades ``liquidity_v2``:
- multi-depth book pressure and wall concentration
- 3s sweep/burst detection
- price response to aggressive flow
- absorption and refill estimates
- wall persistence / spoof-risk estimate
- cross-exchange agreement, strength, quality, and confirmation eligibility
- explicit CONFIRMED / ABSORPTION / CONFLICT / WAIT states
- directional gate hints for Shadow V2 and reversal confirmation
- dynamic per-venue sweep baseline derived from recent 3-second flow

All Liquidity V2 outputs are support/research signals. They never place orders.
"""
from __future__ import annotations

import json
import math
import os
import statistics
import threading
import time
from collections import deque
from datetime import datetime, timezone

import requests
import websocket
from flask import Flask, jsonify

app = Flask(__name__)
VERSION = "1.4"
SYMBOL = "BTC"
POST_URL = os.getenv("NEXUS_MULTI_EXCHANGE_GATEWAY_URL", "").strip()
NEXUS_SECRET = os.getenv("NEXUS_SECRET", "").strip()
POST_INTERVAL = float(os.getenv("NEXUS_MULTI_EXCHANGE_POST_SECONDS", "1.0"))

# Detection knobs are environment-overridable, but defaults are intentionally
# conservative so Liquidity V2 is confirmation support, not a trigger by itself.
SWEEP_MIN_USD = float(os.getenv("NEXUS_LIQ_SWEEP_MIN_USD", "250000"))
WALL_MIN_USD = float(os.getenv("NEXUS_LIQ_WALL_MIN_USD", "100000"))
CONFIRM_STRENGTH = float(os.getenv("NEXUS_LIQ_CONFIRM_STRENGTH", "65"))
CONFIRM_QUALITY = float(os.getenv("NEXUS_LIQ_CONFIRM_QUALITY", "60"))
MIN_AGREE_VENUES = int(os.getenv("NEXUS_LIQ_MIN_AGREE_VENUES", "2"))
ABSORPTION_THRESHOLD = float(os.getenv("NEXUS_LIQ_ABSORPTION_THRESHOLD", "65"))
CONFLICT_COMPONENT_THRESHOLD = float(os.getenv("NEXUS_LIQ_CONFLICT_COMPONENT_THRESHOLD", "0.18"))
DYNAMIC_SWEEP_MIN_USD = float(os.getenv("NEXUS_LIQ_DYNAMIC_SWEEP_MIN_USD", "100000"))
DYNAMIC_SWEEP_MAX_USD = float(os.getenv("NEXUS_LIQ_DYNAMIC_SWEEP_MAX_USD", "750000"))
DYNAMIC_SWEEP_LOOKBACK_SEC = int(os.getenv("NEXUS_LIQ_DYNAMIC_SWEEP_LOOKBACK_SEC", "120"))
DYNAMIC_SWEEP_MIN_BUCKETS = int(os.getenv("NEXUS_LIQ_DYNAMIC_SWEEP_MIN_BUCKETS", "8"))
DYNAMIC_SWEEP_MULTIPLIER = float(os.getenv("NEXUS_LIQ_DYNAMIC_SWEEP_MULTIPLIER", "2.2"))

LOCK = threading.RLock()
EVENTS = {}
BOOKS = {}
LOCAL_BOOKS = {}
PRICES = {}
HEALTH = {}

# Liquidity V2 telemetry. In-memory only.
TRADE_TAPE = {}
PRICE_HISTORY = {}
BOOK_HISTORY = {}
WALL_STATE = {}
SPOOF_EVENTS = {}

STARTED = False
START_LOCK = threading.Lock()
THREADS = {}
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "NEXUS-MULTI-EXCHANGE/1.4"})


def now_ms():
    return int(time.time() * 1000)


def iso_now():
    return datetime.now(timezone.utc).isoformat()


def num(v, d=0.0):
    try:
        x = float(v)
        return x if math.isfinite(x) else float(d)
    except (TypeError, ValueError):
        return float(d)


def clamp(v, lo=0.0, hi=1.0):
    return max(lo, min(hi, float(v)))


def sign(v):
    return 1.0 if v > 0 else (-1.0 if v < 0 else 0.0)


def mark(venue, ok=True, error=""):
    with LOCK:
        row = HEALTH.setdefault(venue, {})
        row["healthy"] = bool(ok)
        row["updated_at"] = time.time()
        row["error"] = str(error)[:180]


def push_event(venue, kind, side, usd, ts=None, price=None):
    if usd <= 0:
        return
    event_ts = float(ts or time.time())
    event_side = str(side).upper()
    event_usd = float(usd)
    with LOCK:
        q = EVENTS.setdefault((venue, kind), deque(maxlen=12000))
        q.append((event_ts, event_side, event_usd))
        if kind == "trade":
            tq = TRADE_TAPE.setdefault(venue, deque(maxlen=20000))
            tq.append((event_ts, event_side, event_usd, num(price)))


def set_price(venue, price):
    p = num(price)
    if p <= 0:
        return
    ts = time.time()
    with LOCK:
        PRICES[venue] = (p, ts)
        q = PRICE_HISTORY.setdefault(venue, deque(maxlen=6000))
        # Avoid storing every single high-rate tick while retaining sub-second detail.
        if not q or ts - q[-1][0] >= 0.05:
            q.append((ts, p))


def _clean_book_rows(rows, reverse=False):
    out = []
    for row in rows or []:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        p, q = num(row[0]), num(row[1])
        if p > 0 and q > 0:
            out.append((p, q))
    out.sort(key=lambda x: x[0], reverse=reverse)
    return out[:20]


def _wall_update(venue, side_name, price, usd, ts):
    """Track persistence and flag large walls that vanish quickly.

    This is a conservative spoof *risk* estimate only. It does not accuse a venue
    or participant of spoofing; it merely lowers liquidity quality when large
    displayed walls appear/disappear too quickly.
    """
    state = WALL_STATE.setdefault(venue, {}).setdefault(side_name, {})
    prev_price = num(state.get("price"))
    prev_usd = num(state.get("usd"))
    first_seen = num(state.get("first_seen"), ts)
    observations = int(state.get("observations") or 0)

    same = False
    if prev_price > 0 and price > 0:
        same = abs(price - prev_price) / prev_price * 10000.0 <= 3.0

    if same and usd >= max(WALL_MIN_USD * 0.5, prev_usd * 0.30):
        state.update({
            "price": price,
            "usd": usd,
            "first_seen": first_seen,
            "last_seen": ts,
            "observations": observations + 1,
        })
    else:
        if prev_usd >= WALL_MIN_USD and prev_price > 0:
            persisted = max(0.0, ts - first_seen)
            if persisted < 2.5:
                q = SPOOF_EVENTS.setdefault(venue, deque(maxlen=200))
                q.append((ts, side_name.upper(), prev_usd, persisted))
        state.clear()
        state.update({
            "price": price,
            "usd": usd,
            "first_seen": ts,
            "last_seen": ts,
            "observations": 1,
        })
    return max(0.0, ts - num(state.get("first_seen"), ts))


def set_book(venue, bids, asks):
    bid_rows = _clean_book_rows(bids, reverse=True)
    ask_rows = _clean_book_rows(asks, reverse=False)

    def stats(rows):
        notionals = [max(0.0, p * q) for p, q in rows]
        total = sum(notionals)
        near5 = sum(notionals[:5])
        weighted = sum(n / math.sqrt(i + 1.0) for i, n in enumerate(notionals))
        if notionals:
            idx = max(range(len(notionals)), key=lambda i: notionals[i])
            wall_usd = notionals[idx]
            wall_price = rows[idx][0]
        else:
            wall_usd = wall_price = 0.0
        concentration = wall_usd / total if total > 0 else 0.0
        return total, near5, weighted, wall_usd, wall_price, concentration

    bid_usd, bid_near5, bid_weighted, bid_wall_usd, bid_wall_price, bid_conc = stats(bid_rows)
    ask_usd, ask_near5, ask_weighted, ask_wall_usd, ask_wall_price, ask_conc = stats(ask_rows)

    denom = bid_usd + ask_usd
    imbalance = (bid_usd - ask_usd) / denom if denom else 0.0
    wdenom = bid_weighted + ask_weighted
    weighted_imbalance = (bid_weighted - ask_weighted) / wdenom if wdenom else 0.0

    best_bid = bid_rows[0][0] if bid_rows else 0.0
    best_ask = ask_rows[0][0] if ask_rows else 0.0
    mid = (best_bid + best_ask) / 2.0 if best_bid and best_ask else max(best_bid, best_ask)
    spread_bps = ((best_ask - best_bid) / mid * 10000.0) if mid and best_bid and best_ask else 0.0
    bid_wall_distance_bps = ((mid - bid_wall_price) / mid * 10000.0) if mid and bid_wall_price else 0.0
    ask_wall_distance_bps = ((ask_wall_price - mid) / mid * 10000.0) if mid and ask_wall_price else 0.0

    ts = time.time()
    with LOCK:
        bid_persist = _wall_update(venue, "bid", bid_wall_price, bid_wall_usd, ts)
        ask_persist = _wall_update(venue, "ask", ask_wall_price, ask_wall_usd, ts)
        row = {
            "bid_usd": bid_usd,
            "ask_usd": ask_usd,
            "near5_bid_usd": bid_near5,
            "near5_ask_usd": ask_near5,
            "imbalance": imbalance,
            "weighted_imbalance": weighted_imbalance,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "spread_bps": spread_bps,
            "bid_wall_usd": bid_wall_usd,
            "ask_wall_usd": ask_wall_usd,
            "bid_wall_price": bid_wall_price,
            "ask_wall_price": ask_wall_price,
            "bid_wall_concentration": bid_conc,
            "ask_wall_concentration": ask_conc,
            "bid_wall_distance_bps": bid_wall_distance_bps,
            "ask_wall_distance_bps": ask_wall_distance_bps,
            "bid_wall_persistence_s": bid_persist,
            "ask_wall_persistence_s": ask_persist,
            "updated_at": ts,
        }
        BOOKS[venue] = row
        hq = BOOK_HISTORY.setdefault(venue, deque(maxlen=600))
        hq.append({
            "ts": ts,
            "bid_usd": bid_usd,
            "ask_usd": ask_usd,
            "bid_near5": bid_near5,
            "ask_near5": ask_near5,
            "imbalance": imbalance,
            "weighted_imbalance": weighted_imbalance,
        })


def apply_delta_book(venue, bids, asks, snapshot=False, bid_token="buy", ask_token="sell"):
    """Maintain a local price->size book and publish top-20 notional."""
    with LOCK:
        state = LOCAL_BOOKS.setdefault(venue, {"bids": {}, "asks": {}})
        if snapshot:
            state["bids"].clear()
            state["asks"].clear()
        for side_name, rows in (("bids", bids or []), ("asks", asks or [])):
            book = state[side_name]
            for row in rows:
                try:
                    price, size = num(row[0]), num(row[1])
                except Exception:
                    continue
                if price <= 0:
                    continue
                if size <= 0:
                    book.pop(price, None)
                else:
                    book[price] = size
        bid_rows = [[p, q] for p, q in sorted(state["bids"].items(), reverse=True)[:20]]
        ask_rows = [[p, q] for p, q in sorted(state["asks"].items())[:20]]
    set_book(venue, bid_rows, ask_rows)


def apply_coinbase_l2(d):
    typ = str(d.get("type") or "")
    if typ == "snapshot":
        apply_delta_book("coinbase", d.get("bids") or [], d.get("asks") or [], snapshot=True)
        return
    if typ != "l2update":
        return
    bids, asks = [], []
    for row in d.get("changes") or []:
        if not isinstance(row, (list, tuple)) or len(row) < 3:
            continue
        side, price, size = str(row[0]).lower(), row[1], row[2]
        (bids if side == "buy" else asks).append([price, size])
    apply_delta_book("coinbase", bids, asks, snapshot=False)


def ws_forever(name, url, on_open, on_message):
    delay = 1.0
    while True:
        try:
            def _open(ws):
                mark(name, True, "")
                on_open(ws)

            def _msg(ws, message):
                mark(name, True, "")
                on_message(json.loads(message))

            def _err(ws, error):
                mark(name, False, error)

            def _close(ws, code, reason):
                mark(name, False, f"closed {code}: {reason}")

            ws = websocket.WebSocketApp(
                url,
                on_open=_open,
                on_message=_msg,
                on_error=_err,
                on_close=_close,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as exc:
            mark(name, False, exc)
        time.sleep(delay)
        delay = min(15.0, delay * 1.5)


# ---- BYBIT ---------------------------------------------------------------
def bybit_open(ws):
    ws.send(json.dumps({
        "op": "subscribe",
        "args": ["allLiquidation.BTCUSDT", "publicTrade.BTCUSDT", "orderbook.50.BTCUSDT"],
    }))


def bybit_msg(msg):
    topic = str(msg.get("topic") or "")
    data = msg.get("data")
    if topic.startswith("allLiquidation."):
        rows = data if isinstance(data, list) else [data]
        for r in rows:
            if not isinstance(r, dict):
                continue
            p, q = num(r.get("p")), num(r.get("v"))
            usd = p * q
            # Bybit docs: S=Buy means a long position was liquidated.
            side = "LONG" if str(r.get("S")).upper() == "BUY" else "SHORT"
            push_event("bybit", "liq", side, usd, num(r.get("T")) / 1000.0)
            set_price("bybit", p)
    elif topic.startswith("publicTrade."):
        rows = data if isinstance(data, list) else []
        for r in rows:
            p, q = num(r.get("p")), num(r.get("v"))
            set_price("bybit", p)
            side = "BUY" if str(r.get("S")).upper() == "BUY" else "SELL"
            push_event("bybit", "trade", side, p * q, num(r.get("T")) / 1000.0, p)
    elif topic.startswith("orderbook.") and isinstance(data, dict):
        apply_delta_book(
            "bybit",
            data.get("b") or [],
            data.get("a") or [],
            snapshot=str(msg.get("type") or "").lower() == "snapshot",
        )


# ---- BINANCE USD-M -------------------------------------------------------
BINANCE_URL = os.getenv(
    "BINANCE_WS_URL",
    "wss://fstream.binance.com/stream?streams=btcusdt@aggTrade/btcusdt@forceOrder/btcusdt@depth20@100ms",
)


def binance_open(ws):
    pass


def binance_msg(msg):
    stream = str(msg.get("stream") or "")
    d = msg.get("data") if isinstance(msg.get("data"), dict) else msg
    if "@aggTrade" in stream or d.get("e") == "aggTrade":
        p, q = num(d.get("p")), num(d.get("q"))
        set_price("binance", p)
        # m=true => buyer is maker, therefore aggressive side is SELL.
        side = "SELL" if bool(d.get("m")) else "BUY"
        push_event("binance", "trade", side, p * q, num(d.get("T") or d.get("E")) / 1000.0, p)
    elif "@forceOrder" in stream or d.get("e") == "forceOrder":
        o = d.get("o") if isinstance(d.get("o"), dict) else {}
        p, q = num(o.get("ap") or o.get("p")), num(o.get("z") or o.get("q"))
        set_price("binance", p)
        # Liquidation order SELL closes a long; BUY closes a short.
        side = "LONG" if str(o.get("S")).upper() == "SELL" else "SHORT"
        push_event("binance", "liq", side, p * q, num(o.get("T") or d.get("E")) / 1000.0)
    elif "@depth" in stream or d.get("e") == "depthUpdate":
        set_book("binance", d.get("b") or d.get("bids") or [], d.get("a") or d.get("asks") or [])


# ---- OKX -----------------------------------------------------------------
OKX_WS = os.getenv("OKX_WS_URL", "wss://ws.okx.com/ws/v5/public")
# Spot is used by default so size units are directly BTC and comparable to the
# other venues. Override with OKX_INST_ID if desired.
OKX_INST_ID = os.getenv("OKX_INST_ID", "BTC-USDT")


def okx_open(ws):
    ws.send(json.dumps({
        "op": "subscribe",
        "args": [
            {"channel": "books5", "instId": OKX_INST_ID},
            {"channel": "trades", "instId": OKX_INST_ID},
        ],
    }))


def okx_msg(msg):
    arg = msg.get("arg") if isinstance(msg.get("arg"), dict) else {}
    channel = arg.get("channel")
    rows = msg.get("data") if isinstance(msg.get("data"), list) else []
    if channel == "books5" and rows:
        r = rows[0] if isinstance(rows[0], dict) else {}
        set_book("okx", r.get("bids") or [], r.get("asks") or [])
        b, a = r.get("bids") or [], r.get("asks") or []
        px = []
        if b:
            px.append(num(b[0][0]))
        if a:
            px.append(num(a[0][0]))
        if px:
            set_price("okx", sum(px) / len(px))
    elif channel == "trades":
        for r in rows:
            if not isinstance(r, dict):
                continue
            p, q = num(r.get("px")), num(r.get("sz"))
            set_price("okx", p)
            side = "BUY" if str(r.get("side")).lower() == "buy" else "SELL"
            push_event("okx", "trade", side, p * q, num(r.get("ts")) / 1000.0, p)


# ---- COINBASE EXCHANGE ---------------------------------------------------
COINBASE_WS = os.getenv("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com")


def coinbase_open(ws):
    ws.send(json.dumps({
        "type": "subscribe",
        "product_ids": ["BTC-USD"],
        "channels": ["ticker", "matches", "level2"],
    }))


def coinbase_msg(d):
    typ = str(d.get("type") or "")
    if typ == "ticker":
        set_price("coinbase", d.get("price"))
    elif typ in ("match", "last_match"):
        p, q = num(d.get("price")), num(d.get("size"))
        set_price("coinbase", p)
        # Coinbase match side is maker side; aggressive side is the opposite.
        maker = str(d.get("side") or "").lower()
        side = "SELL" if maker == "buy" else "BUY"
        push_event("coinbase", "trade", side, p * q, time.time(), p)
    elif typ in ("snapshot", "l2update"):
        apply_coinbase_l2(d)


def prune_and_sum(q, seconds, positive_side, negative_side):
    cutoff = time.time() - seconds
    pos = neg = 0.0
    while q and q[0][0] < cutoff:
        q.popleft()
    for _, side, usd in q:
        if side == positive_side:
            pos += usd
        elif side == negative_side:
            neg += usd
    return pos, neg


def _trade_window(venue, seconds):
    cutoff = time.time() - seconds
    with LOCK:
        rows = list(TRADE_TAPE.get(venue) or ())
    buy = sell = 0.0
    for ts, side, usd, _price in rows:
        if ts < cutoff:
            continue
        if side == "BUY":
            buy += usd
        elif side == "SELL":
            sell += usd
    return buy, sell


def _price_move_bps(venue, seconds):
    cutoff = time.time() - seconds
    with LOCK:
        rows = list(PRICE_HISTORY.get(venue) or ())
        current = num((PRICES.get(venue) or (0.0, 0.0))[0])
    if current <= 0 or not rows:
        return 0.0
    start = None
    for ts, px in rows:
        if ts >= cutoff:
            start = px
            break
    if not start:
        start = rows[0][1]
    return (current - start) / start * 10000.0 if start else 0.0


def _flow_persistence(venue, side_name, seconds=6):
    now = time.time()
    with LOCK:
        rows = list(TRADE_TAPE.get(venue) or ())
    buckets = []
    for i in range(seconds):
        lo = now - (i + 1)
        hi = now - i
        b = s = 0.0
        for ts, side, usd, _ in rows:
            if lo <= ts < hi:
                if side == "BUY":
                    b += usd
                elif side == "SELL":
                    s += usd
        if b + s > 0:
            buckets.append("BUY" if b >= s else "SELL")
    if not buckets:
        return 0.0
    return sum(1 for x in buckets if x == side_name) / len(buckets)


def _book_recovery(venue, side_name, seconds=5.0):
    """Estimate refill after recent depletion: 0-200%."""
    cutoff = time.time() - seconds
    key = "ask_usd" if side_name == "ASK" else "bid_usd"
    with LOCK:
        rows = [x for x in list(BOOK_HISTORY.get(venue) or ()) if num(x.get("ts")) >= cutoff]
        current = num((BOOKS.get(venue) or {}).get(key))
    if len(rows) < 3 or current <= 0:
        return 0.0
    vals = [num(r.get(key)) for r in rows if num(r.get(key)) > 0]
    if len(vals) < 3:
        return 0.0
    baseline = max(vals[0], statistics.median(vals[: max(1, len(vals)//3)]))
    low = min(vals)
    depleted = max(0.0, baseline - low)
    if depleted <= max(1000.0, baseline * 0.02):
        return 0.0
    recovered = max(0.0, current - low)
    return clamp(recovered / depleted, 0.0, 2.0) * 100.0


def _spoof_risk(venue, seconds=10.0):
    cutoff = time.time() - seconds
    with LOCK:
        q = SPOOF_EVENTS.setdefault(venue, deque(maxlen=200))
        while q and q[0][0] < cutoff:
            q.popleft()
        usd = sum(num(x[2]) for x in q)
        count = len(q)
    # $1M of quickly vanished walls in 10s is treated as maximum risk.
    score = clamp(usd / 1_000_000.0, 0.0, 1.0) * 100.0
    return score, count, usd



def _percentile(values, pct):
    rows = sorted(num(x) for x in values if num(x) > 0)
    if not rows:
        return 0.0
    if len(rows) == 1:
        return rows[0]
    pos = clamp(float(pct), 0.0, 1.0) * (len(rows) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return rows[lo]
    frac = pos - lo
    return rows[lo] * (1.0 - frac) + rows[hi] * frac


def _dynamic_sweep_baseline(venue):
    """Return a conservative per-venue 3-second sweep threshold.

    We compare the current 3-second dominant-side USD flow with recent completed
    3-second buckets. The threshold adapts to quiet/active sessions but is
    clamped so thin periods cannot make tiny prints look like real sweeps.
    """
    now = time.time()
    cutoff = now - max(30, DYNAMIC_SWEEP_LOOKBACK_SEC)
    with LOCK:
        rows = [
            (ts, side, usd)
            for ts, side, usd, _price in list(TRADE_TAPE.get(venue) or ())
            if cutoff <= ts < now - 3.0
        ]

    buckets = []
    # completed 3-second windows, newest first
    windows = max(1, int(DYNAMIC_SWEEP_LOOKBACK_SEC / 3))
    for i in range(1, windows + 1):
        hi = now - (i * 3.0)
        lo = hi - 3.0
        buy = sell = 0.0
        for ts, side, usd in rows:
            if lo <= ts < hi:
                if side == "BUY":
                    buy += usd
                elif side == "SELL":
                    sell += usd
        dominant = max(buy, sell)
        if dominant > 0:
            buckets.append(dominant)

    if len(buckets) < DYNAMIC_SWEEP_MIN_BUCKETS:
        return {
            "mode": "WARMUP",
            "threshold_usd": float(SWEEP_MIN_USD),
            "baseline_usd": 0.0,
            "median_usd": 0.0,
            "p75_usd": 0.0,
            "samples": len(buckets),
        }

    median = statistics.median(buckets)
    p75 = _percentile(buckets, 0.75)

    # The p75 term reacts to recent activity; median protects against a few
    # isolated bursts distorting the baseline.
    baseline = max(median * DYNAMIC_SWEEP_MULTIPLIER, p75 * 1.45)
    threshold = max(DYNAMIC_SWEEP_MIN_USD, min(DYNAMIC_SWEEP_MAX_USD, baseline))

    return {
        "mode": "DYNAMIC",
        "threshold_usd": float(threshold),
        "baseline_usd": float(baseline),
        "median_usd": float(median),
        "p75_usd": float(p75),
        "samples": len(buckets),
    }


def venue_liquidity_v2(venue):
    now = time.time()
    with LOCK:
        book = dict(BOOKS.get(venue) or {})
        h = dict(HEALTH.get(venue) or {})
        price, price_at = PRICES.get(venue, (0.0, 0.0))

    updated = max(num(h.get("updated_at")), num(book.get("updated_at")), num(price_at))
    age = max(0.0, now - updated) if updated else 9999.0
    healthy = bool(h.get("healthy")) and age < 20.0

    buy3, sell3 = _trade_window(venue, 3.0)
    buy15, sell15 = _trade_window(venue, 15.0)
    buy60, sell60 = _trade_window(venue, 60.0)
    total3 = buy3 + sell3
    total15 = buy15 + sell15
    total60 = buy60 + sell60

    flow15 = (buy15 - sell15) / total15 if total15 else 0.0
    dominant_side = "BUY" if buy3 >= sell3 else "SELL"
    dominant3 = max(buy3, sell3)
    dominant_share = dominant3 / total3 if total3 else 0.0
    sweep_baseline = _dynamic_sweep_baseline(venue)
    dynamic_threshold = max(1.0, num(sweep_baseline.get("threshold_usd"), SWEEP_MIN_USD))
    expected3 = max(dynamic_threshold, (total60 / 60.0) * 3.0) if total60 else dynamic_threshold
    burst_ratio = dominant3 / expected3 if expected3 > 0 else 0.0

    sweep_strength = 0.0
    if dominant3 >= dynamic_threshold and dominant_share >= 0.60:
        share_score = clamp((dominant_share - 0.50) / 0.50)
        size_score = clamp(math.log1p(dominant3 / dynamic_threshold) / math.log(5.0))
        burst_score = clamp(burst_ratio / 2.5)
        sweep_strength = (35.0 * share_score) + (35.0 * size_score) + (30.0 * burst_score)

    move3 = _price_move_bps(venue, 3.0)
    side_sign = 1.0 if dominant_side == "BUY" else -1.0
    signed_response = move3 * side_sign
    response_score = clamp(signed_response / 2.0, -1.0, 1.0)
    persistence = _flow_persistence(venue, dominant_side, 6)

    # Refill of the *opposing* wall after a sweep is a classic absorption clue.
    opposing_side = "ASK" if dominant_side == "BUY" else "BID"
    refill_pct = _book_recovery(venue, opposing_side, 5.0)
    opp_wall_conc = num(book.get("ask_wall_concentration" if dominant_side == "BUY" else "bid_wall_concentration"))
    opp_wall_persist = num(book.get("ask_wall_persistence_s" if dominant_side == "BUY" else "bid_wall_persistence_s"))
    poor_response = clamp((1.2 - signed_response) / 1.2) if sweep_strength >= 35 else 0.0
    absorption = 0.0
    if sweep_strength >= 35:
        absorption = 100.0 * (
            0.45 * poor_response
            + 0.25 * clamp(opp_wall_conc / 0.30)
            + 0.20 * clamp(refill_pct / 100.0)
            + 0.10 * clamp(opp_wall_persist / 4.0)
        )

    spoof_score, spoof_count, spoof_usd = _spoof_risk(venue)
    book_imb = num(book.get("weighted_imbalance", book.get("imbalance")))

    sweep_component = side_sign * (sweep_strength / 100.0)
    # Strong absorption reduces and can partially reverse apparent sweep pressure.
    absorption_penalty = side_sign * (absorption / 100.0) * 0.25
    score = (
        0.36 * flow15
        + 0.30 * book_imb
        + 0.20 * sweep_component
        + 0.14 * response_score
        - absorption_penalty
    )
    score = clamp(score, -1.0, 1.0)

    direction = "UP" if score >= 0.12 else ("DOWN" if score <= -0.12 else "NEUTRAL")
    strength = abs(score) * 100.0
    quality = 100.0 * (
        0.30 * (1.0 if healthy else 0.0)
        + 0.20 * persistence
        + 0.15 * clamp(max(num(book.get("bid_wall_persistence_s")), num(book.get("ask_wall_persistence_s"))) / 4.0)
        + 0.20 * (1.0 - spoof_score / 100.0)
        + 0.15 * clamp(total15 / 1_000_000.0)
    )

    return {
        "healthy": healthy,
        "age_seconds": round(age, 3),
        "direction": direction,
        "score": round(score, 4),
        "strength": round(strength, 1),
        "quality": round(clamp(quality, 0.0, 100.0), 1),
        "flow_15s_ratio": round(flow15, 4),
        "weighted_book_imbalance": round(book_imb, 4),
        "spread_bps": round(num(book.get("spread_bps")), 3),
        "sweep": {
            "side": dominant_side if sweep_strength > 0 else "NONE",
            "strength": round(sweep_strength, 1),
            "usd_3s": round(dominant3, 2),
            "share": round(dominant_share, 4),
            "burst_ratio": round(burst_ratio, 3),
            "price_response_bps": round(signed_response, 3),
            "persistence": round(persistence, 3),
            "threshold_usd": round(dynamic_threshold, 2),
            "threshold_mode": sweep_baseline.get("mode"),
            "baseline_usd": round(num(sweep_baseline.get("baseline_usd")), 2),
            "baseline_median_usd": round(num(sweep_baseline.get("median_usd")), 2),
            "baseline_p75_usd": round(num(sweep_baseline.get("p75_usd")), 2),
            "baseline_samples": int(num(sweep_baseline.get("samples"), 0)),
        },
        "absorption": {
            "score": round(clamp(absorption, 0.0, 100.0), 1),
            "opposing_side": opposing_side,
            "refill_pct": round(refill_pct, 1),
            "wall_concentration": round(opp_wall_conc, 4),
            "wall_persistence_s": round(opp_wall_persist, 2),
        },
        "walls": {
            "bid_usd": round(num(book.get("bid_wall_usd")), 2),
            "bid_price": round(num(book.get("bid_wall_price")), 2),
            "bid_persistence_s": round(num(book.get("bid_wall_persistence_s")), 2),
            "ask_usd": round(num(book.get("ask_wall_usd")), 2),
            "ask_price": round(num(book.get("ask_wall_price")), 2),
            "ask_persistence_s": round(num(book.get("ask_wall_persistence_s")), 2),
        },
        "spoof_risk": {
            "score": round(spoof_score, 1),
            "events_10s": spoof_count,
            "vanished_usd_10s": round(spoof_usd, 2),
        },
    }


def _dir_from_score(score, threshold=0.12):
    score = num(score)
    if score >= threshold:
        return "UP"
    if score <= -threshold:
        return "DOWN"
    return "NEUTRAL"


def _opposite(direction):
    return "DOWN" if direction == "UP" else ("UP" if direction == "DOWN" else "NEUTRAL")


def liquidity_v2_snapshot():
    """Aggregate Liquidity V2.1 into an explicit confirmation state.

    State meanings:
      CONFIRMED  - direction/quality/strength and venue agreement are sufficient.
      ABSORPTION - aggressive sweep is being absorbed; do not chase the sweep.
      CONFLICT   - meaningful components disagree; block fresh confirmation.
      WAIT       - not enough clean evidence yet.

    This is support-only telemetry. It never places an order.
    """
    with LOCK:
        venues = sorted(set(list(BOOKS) + list(PRICES) + list(HEALTH) + list(TRADE_TAPE)))

    details = {v: venue_liquidity_v2(v) for v in venues}
    healthy_items = [(v, x) for v, x in details.items() if x.get("healthy")]
    healthy = [x for _, x in healthy_items]
    healthy_names = [v for v, _ in healthy_items]

    if not healthy:
        return {
            "version": "2.2",
            "status": "OFFLINE",
            "state": "WAIT",
            "direction": "NEUTRAL",
            "confirmation_direction": "NEUTRAL",
            "absorption_bias": "NEUTRAL",
            "strength": 0.0,
            "quality": 0.0,
            "agreement": 0.0,
            "agreeing_venues": 0,
            "healthy_venues": 0,
            "confirmation_eligible": False,
            "gate": "HOLD",
            "reason": "no fresh healthy venues",
            "component_directions": {
                "aggregate": "NEUTRAL", "book": "NEUTRAL",
                "flow": "NEUTRAL", "sweep": "NEUTRAL",
            },
            "venues": details,
        }

    weights = [max(0.20, num(x.get("quality")) / 100.0) for x in healthy]
    wsum = sum(weights) or 1.0
    global_score = sum(num(x.get("score")) * w for x, w in zip(healthy, weights)) / wsum
    global_direction = _dir_from_score(global_score)

    # Cross-venue directional agreement.
    if global_direction == "NEUTRAL":
        agreeing = sum(1 for x in healthy if x.get("direction") == "NEUTRAL")
    else:
        agreeing = sum(1 for x in healthy if x.get("direction") == global_direction)
    agreement = agreeing / len(healthy)

    avg_quality = sum(num(x.get("quality")) for x in healthy) / len(healthy)
    avg_persistence = sum(num((x.get("sweep") or {}).get("persistence")) for x in healthy) / len(healthy)
    avg_spoof = sum(num((x.get("spoof_risk") or {}).get("score")) for x in healthy) / len(healthy)

    # Independent components used to detect internal conflict.
    book_score = sum(num(x.get("weighted_book_imbalance")) * w for x, w in zip(healthy, weights)) / wsum
    flow_score = sum(num(x.get("flow_15s_ratio")) * w for x, w in zip(healthy, weights)) / wsum
    book_direction = _dir_from_score(book_score, CONFLICT_COMPONENT_THRESHOLD)
    flow_direction = _dir_from_score(flow_score, CONFLICT_COMPONENT_THRESHOLD)

    strength = clamp(abs(global_score) * 0.78 + agreement * 0.22, 0.0, 1.0) * 100.0
    quality = clamp(
        0.45 * (avg_quality / 100.0)
        + 0.25 * agreement
        + 0.15 * avg_persistence
        + 0.15 * (1.0 - avg_spoof / 100.0),
        0.0,
        1.0,
    ) * 100.0

    sweeps = []
    absorptions = []
    for venue, x in healthy_items:
        sw = x.get("sweep") or {}
        ab = x.get("absorption") or {}
        if num(sw.get("strength")) > 0:
            sweeps.append((num(sw.get("strength")), venue, sw))
        if num(ab.get("score")) > 0:
            absorptions.append((num(ab.get("score")), venue, ab))
    sweeps.sort(reverse=True, key=lambda x: x[0])
    absorptions.sort(reverse=True, key=lambda x: x[0])

    strongest_sweep = ({"venue": sweeps[0][1], **sweeps[0][2]} if sweeps else None)
    strongest_absorption = ({"venue": absorptions[0][1], **absorptions[0][2]} if absorptions else None)

    sweep_direction = "NEUTRAL"
    if strongest_sweep and num(strongest_sweep.get("strength")) >= 45:
        sweep_direction = "UP" if str(strongest_sweep.get("side")).upper() == "BUY" else "DOWN"

    absorption_score = num((strongest_absorption or {}).get("score"))
    absorption_active = bool(
        strongest_sweep
        and strongest_absorption
        and absorption_score >= ABSORPTION_THRESHOLD
        and num(strongest_sweep.get("strength")) >= 45
    )
    # A BUY sweep being absorbed is bearish risk; a SELL sweep being absorbed is bullish risk.
    absorption_bias = _opposite(sweep_direction) if absorption_active else "NEUTRAL"

    component_directions = {
        "aggregate": global_direction,
        "book": book_direction,
        "flow": flow_direction,
        "sweep": sweep_direction,
    }
    directional_components = [
        d for d in component_directions.values() if d in ("UP", "DOWN")
    ]
    has_up = "UP" in directional_components
    has_down = "DOWN" in directional_components

    # Only call CONFLICT when both sides have meaningful independent evidence.
    conflict = has_up and has_down
    conflict_reasons = []
    if conflict:
        if book_direction not in ("NEUTRAL", global_direction):
            conflict_reasons.append(f"book {book_direction} vs aggregate {global_direction}")
        if flow_direction not in ("NEUTRAL", global_direction):
            conflict_reasons.append(f"flow {flow_direction} vs aggregate {global_direction}")
        if sweep_direction not in ("NEUTRAL", global_direction):
            conflict_reasons.append(f"sweep {sweep_direction} vs aggregate {global_direction}")
        if not conflict_reasons:
            conflict_reasons.append("directional components disagree")

    base_eligible = (
        len(healthy) >= 2
        and global_direction in ("UP", "DOWN")
        and strength >= CONFIRM_STRENGTH
        and quality >= CONFIRM_QUALITY
        and agreeing >= MIN_AGREE_VENUES
        and agreement >= 0.50
    )

    if absorption_active:
        state = "ABSORPTION"
        confirmation_direction = "NEUTRAL"
        gate = "BLOCK_NEW"
    elif conflict:
        state = "CONFLICT"
        confirmation_direction = "NEUTRAL"
        gate = "BLOCK_NEW"
    elif base_eligible:
        state = "CONFIRMED"
        confirmation_direction = global_direction
        gate = "ALLOW_UP" if global_direction == "UP" else "ALLOW_DOWN"
    else:
        state = "WAIT"
        confirmation_direction = "NEUTRAL"
        gate = "HOLD"

    confirmation_eligible = state == "CONFIRMED"

    reasons = []
    if state == "CONFIRMED":
        reasons.append(
            f"{confirmation_direction} confirmed: {agreeing}/{len(healthy)} venues agree"
        )
    elif state == "ABSORPTION":
        reasons.append(
            f"{sweep_direction} sweep absorbed; {absorption_bias} reversal risk"
        )
    elif state == "CONFLICT":
        reasons.extend(conflict_reasons)
    else:
        if len(healthy) < 2:
            reasons.append("need 2+ fresh venues")
        if global_direction == "NEUTRAL":
            reasons.append("direction neutral")
        if strength < CONFIRM_STRENGTH:
            reasons.append(f"strength {strength:.0f}<{CONFIRM_STRENGTH:.0f}")
        if quality < CONFIRM_QUALITY:
            reasons.append(f"quality {quality:.0f}<{CONFIRM_QUALITY:.0f}")
        if agreeing < MIN_AGREE_VENUES:
            reasons.append(f"need {MIN_AGREE_VENUES}+ agreeing venues")
        if agreement < 0.50:
            reasons.append("cross-venue disagreement")

    return {
        "version": "2.2",
        "status": "LIVE",
        "state": state,
        "direction": global_direction,
        "confirmation_direction": confirmation_direction,
        "absorption_bias": absorption_bias,
        "score": round(global_score, 4),
        "strength": round(strength, 1),
        "quality": round(quality, 1),
        "agreement": round(agreement, 3),
        "agreeing_venues": int(agreeing),
        "healthy_venues": len(healthy),
        "healthy_venue_names": healthy_names,
        "confirmation_eligible": bool(confirmation_eligible),
        "gate": gate,
        "confirmation_thresholds": {
            "strength": CONFIRM_STRENGTH,
            "quality": CONFIRM_QUALITY,
            "min_healthy_venues": 2,
            "min_agreeing_venues": MIN_AGREE_VENUES,
            "min_agreement": 0.50,
            "absorption_score": ABSORPTION_THRESHOLD,
        },
        "component_scores": {
            "aggregate": round(global_score, 4),
            "book": round(book_score, 4),
            "flow": round(flow_score, 4),
        },
        "component_directions": component_directions,
        "conflict": bool(conflict),
        "conflict_reasons": conflict_reasons,
        "reason": "; ".join(reasons) if reasons else state.lower(),
        "strongest_sweep": strongest_sweep,
        "strongest_absorption": strongest_absorption,
        "venues": details,
    }


def snapshot():
    now = time.time()
    out = {}
    with LOCK:
        venues = set([k[0] for k in EVENTS] + list(BOOKS) + list(PRICES) + list(HEALTH))
        for v in sorted(venues):
            h = dict(HEALTH.get(v) or {})
            price, price_at = PRICES.get(v, (0.0, 0.0))
            book = dict(BOOKS.get(v) or {})
            tq = EVENTS.setdefault((v, "trade"), deque(maxlen=12000))
            lq = EVENTS.setdefault((v, "liq"), deque(maxlen=12000))
            buy, sell = prune_and_sum(tq, 15.0, "BUY", "SELL")
            long_liq, short_liq = prune_and_sum(lq, 60.0, "LONG", "SHORT")
            total = buy + sell
            fr = (buy - sell) / total if total else 0.0
            updated = max(num(h.get("updated_at")), num(book.get("updated_at")), price_at)
            healthy = bool(h.get("healthy")) and now - updated < 20.0
            out[v] = {
                "healthy": healthy,
                "updated_at": updated,
                "price": round(price, 2) if price else None,
                "flow_15s": {
                    "buy_usd": round(buy, 2),
                    "sell_usd": round(sell, 2),
                    "ratio": round(fr, 5),
                },
                "liquidations_60s": {
                    "long_usd": round(long_liq, 2),
                    "short_usd": round(short_liq, 2),
                },
                # Original V1 fields preserved; richer values are additive.
                "book": {
                    "bid_usd": round(num(book.get("bid_usd")), 2),
                    "ask_usd": round(num(book.get("ask_usd")), 2),
                    "imbalance": round(num(book.get("imbalance")), 5),
                    "weighted_imbalance": round(num(book.get("weighted_imbalance")), 5),
                    "spread_bps": round(num(book.get("spread_bps")), 4),
                    "bid_wall_usd": round(num(book.get("bid_wall_usd")), 2),
                    "ask_wall_usd": round(num(book.get("ask_wall_usd")), 2),
                    "bid_wall_price": round(num(book.get("bid_wall_price")), 2),
                    "ask_wall_price": round(num(book.get("ask_wall_price")), 2),
                },
                "error": str(h.get("error") or "")[:160],
            }
    prices = [num(x.get("price")) for x in out.values() if num(x.get("price")) > 0]
    med = statistics.median(prices) if prices else None
    return {
        "version": VERSION,
        "generated_at": iso_now(),
        "generated_at_ms": now_ms(),
        "symbol": "BTC",
        "btc_price": round(med, 2) if med else None,
        "venues": out,
        "liquidity_v2": liquidity_v2_snapshot(),
        "paper_mode": True,
        "live_order_placed": False,
    }


def poster_loop():
    while True:
        try:
            if POST_URL:
                headers = {"Content-Type": "application/json"}
                if NEXUS_SECRET:
                    headers["X-NEXUS-SECRET"] = NEXUS_SECRET
                SESSION.post(POST_URL, json=snapshot(), headers=headers, timeout=4)
        except Exception:
            pass
        time.sleep(max(0.5, POST_INTERVAL))


def _spawn_thread(key, target, args=()):
    """Start a daemon worker only when its previous thread is missing/dead."""
    t = THREADS.get(key)
    if t is not None and t.is_alive():
        return False
    t = threading.Thread(target=target, args=args, daemon=True, name=key)
    THREADS[key] = t
    t.start()
    return True


def start_background():
    """Idempotently ensure every collector + poster worker is alive."""
    global STARTED
    with START_LOCK:
        STARTED = True
        jobs = [
            ("bybit", "wss://stream.bybit.com/v5/public/linear", bybit_open, bybit_msg),
            ("binance", BINANCE_URL, binance_open, binance_msg),
            ("okx", OKX_WS, okx_open, okx_msg),
            ("coinbase", COINBASE_WS, coinbase_open, coinbase_msg),
        ]
        for args in jobs:
            venue = args[0]
            if venue not in HEALTH:
                mark(venue, False, "starting")
            _spawn_thread(f"mx-{venue}", ws_forever, args)
        _spawn_thread("mx-poster", poster_loop)


def worker_status():
    with START_LOCK:
        return {name: bool(t and t.is_alive()) for name, t in THREADS.items()}


# Start on normal import, and also re-check on every health/API request.
start_background()


@app.get("/")
def root():
    start_background()
    return jsonify({
        "service": "NEXUS MULTI-EXCHANGE BRIDGE",
        "version": VERSION,
        "status": "ONLINE",
        "workers": worker_status(),
        "snapshot": snapshot(),
    })


@app.get("/healthz")
def healthz():
    start_background()
    snap = snapshot()
    return jsonify({
        "ok": True,
        "version": VERSION,
        "gateway_configured": bool(POST_URL),
        "secret_configured": bool(NEXUS_SECRET),
        "workers": worker_status(),
        "venues": snap.get("venues"),
        "liquidity_v2": snap.get("liquidity_v2"),
    })


@app.get("/api/snapshot")
def api_snapshot():
    start_background()
    return jsonify(snapshot())


@app.get("/api/liquidity")
def api_liquidity():
    start_background()
    return jsonify(liquidity_v2_snapshot())


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
