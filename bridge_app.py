"""NEXUS Multi-Exchange Bridge V1.

Public-data collector only. No exchange API keys and no order endpoints.
Designed for a small always-on web service (one worker) and posts a compact
snapshot to NEXUS every second.
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
VERSION = "1.0"
SYMBOL = "BTC"
POST_URL = os.getenv("NEXUS_MULTI_EXCHANGE_GATEWAY_URL", "").strip()
NEXUS_SECRET = os.getenv("NEXUS_SECRET", "").strip()
POST_INTERVAL = float(os.getenv("NEXUS_MULTI_EXCHANGE_POST_SECONDS", "1.0"))

LOCK = threading.RLock()
EVENTS = {}
BOOKS = {}
LOCAL_BOOKS = {}
PRICES = {}
HEALTH = {}
STARTED = False
START_LOCK = threading.Lock()
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "NEXUS-MULTI-EXCHANGE/1.0"})


def now_ms(): return int(time.time() * 1000)
def iso_now(): return datetime.now(timezone.utc).isoformat()
def num(v, d=0.0):
    try:
        x = float(v); return x if math.isfinite(x) else float(d)
    except (TypeError, ValueError): return float(d)


def mark(venue, ok=True, error=""):
    with LOCK:
        row = HEALTH.setdefault(venue, {})
        row["healthy"] = bool(ok)
        row["updated_at"] = time.time()
        row["error"] = str(error)[:180]


def push_event(venue, kind, side, usd, ts=None):
    if usd <= 0: return
    with LOCK:
        q = EVENTS.setdefault((venue, kind), deque(maxlen=12000))
        q.append((float(ts or time.time()), str(side).upper(), float(usd)))


def set_price(venue, price):
    p = num(price)
    if p > 0:
        with LOCK: PRICES[venue] = (p, time.time())


def set_book(venue, bids, asks):
    def side_notional(rows):
        total = 0.0
        for row in rows[:20]:
            try:
                p, q = num(row[0]), num(row[1]); total += max(0.0, p*q)
            except Exception: pass
        return total
    bid_usd, ask_usd = side_notional(bids), side_notional(asks)
    denom = bid_usd + ask_usd
    imbalance = (bid_usd - ask_usd) / denom if denom else 0.0
    with LOCK:
        BOOKS[venue] = {"bid_usd": bid_usd, "ask_usd": ask_usd, "imbalance": imbalance, "updated_at": time.time()}




def apply_delta_book(venue, bids, asks, snapshot=False, bid_token="buy", ask_token="sell"):
    """Maintain a local price->size book and publish top-20 notional."""
    with LOCK:
        state = LOCAL_BOOKS.setdefault(venue, {"bids": {}, "asks": {}})
        if snapshot:
            state["bids"].clear(); state["asks"].clear()
        for side_name, rows in (("bids", bids or []), ("asks", asks or [])):
            book = state[side_name]
            for row in rows:
                try:
                    price, size = num(row[0]), num(row[1])
                except Exception:
                    continue
                if price <= 0: continue
                if size <= 0: book.pop(price, None)
                else: book[price] = size
        bid_rows = [[p, q] for p, q in sorted(state["bids"].items(), reverse=True)[:20]]
        ask_rows = [[p, q] for p, q in sorted(state["asks"].items())[:20]]
    set_book(venue, bid_rows, ask_rows)


def apply_coinbase_l2(d):
    typ = str(d.get("type") or "")
    if typ == "snapshot":
        apply_delta_book("coinbase", d.get("bids") or [], d.get("asks") or [], snapshot=True)
        return
    if typ != "l2update": return
    bids, asks = [], []
    for row in d.get("changes") or []:
        if not isinstance(row, (list, tuple)) or len(row) < 3: continue
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
            def _err(ws, error): mark(name, False, error)
            def _close(ws, code, reason): mark(name, False, f"closed {code}: {reason}")
            ws = websocket.WebSocketApp(url, on_open=_open, on_message=_msg, on_error=_err, on_close=_close)
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as exc:
            mark(name, False, exc)
        time.sleep(delay)
        delay = min(15.0, delay * 1.5)


# ---- BYBIT ---------------------------------------------------------------
def bybit_open(ws):
    ws.send(json.dumps({"op":"subscribe","args":["allLiquidation.BTCUSDT","publicTrade.BTCUSDT","orderbook.50.BTCUSDT"]}))

def bybit_msg(msg):
    topic = str(msg.get("topic") or "")
    data = msg.get("data")
    if topic.startswith("allLiquidation."):
        rows = data if isinstance(data, list) else [data]
        for r in rows:
            if not isinstance(r, dict): continue
            p, q = num(r.get("p")), num(r.get("v")); usd = p*q
            # Bybit docs: S=Buy means a long position was liquidated.
            side = "LONG" if str(r.get("S")).upper()=="BUY" else "SHORT"
            push_event("bybit","liq",side,usd,num(r.get("T"))/1000.0)
            set_price("bybit", p)
    elif topic.startswith("publicTrade."):
        rows = data if isinstance(data, list) else []
        for r in rows:
            p, q = num(r.get("p")), num(r.get("v")); set_price("bybit", p)
            side = "BUY" if str(r.get("S")).upper()=="BUY" else "SELL"
            push_event("bybit","trade",side,p*q,num(r.get("T"))/1000.0)
    elif topic.startswith("orderbook.") and isinstance(data, dict):
        apply_delta_book("bybit", data.get("b") or [], data.get("a") or [], snapshot=str(msg.get("type") or "").lower()=="snapshot")


# ---- BINANCE USD-M -------------------------------------------------------
BINANCE_URL = os.getenv("BINANCE_WS_URL", "wss://fstream.binance.com/stream?streams=btcusdt@aggTrade/btcusdt@forceOrder/btcusdt@depth20@100ms")
def binance_open(ws): pass

def binance_msg(msg):
    stream = str(msg.get("stream") or "")
    d = msg.get("data") if isinstance(msg.get("data"), dict) else msg
    if "@aggTrade" in stream or d.get("e") == "aggTrade":
        p, q = num(d.get("p")), num(d.get("q")); set_price("binance", p)
        # m=true => buyer is maker, therefore aggressive side is SELL.
        side = "SELL" if bool(d.get("m")) else "BUY"
        push_event("binance","trade",side,p*q,num(d.get("T") or d.get("E"))/1000.0)
    elif "@forceOrder" in stream or d.get("e") == "forceOrder":
        o = d.get("o") if isinstance(d.get("o"), dict) else {}
        p, q = num(o.get("ap") or o.get("p")), num(o.get("z") or o.get("q")); set_price("binance", p)
        # Liquidation order SELL closes a long; BUY closes a short.
        side = "LONG" if str(o.get("S")).upper()=="SELL" else "SHORT"
        push_event("binance","liq",side,p*q,num(o.get("T") or d.get("E"))/1000.0)
    elif "@depth" in stream or d.get("e") == "depthUpdate":
        set_book("binance", d.get("b") or d.get("bids") or [], d.get("a") or d.get("asks") or [])


# ---- OKX -----------------------------------------------------------------
OKX_WS = os.getenv("OKX_WS_URL", "wss://ws.okx.com/ws/v5/public")
def okx_open(ws):
    ws.send(json.dumps({"op":"subscribe","args":[
        {"channel":"books5","instId":"BTC-USDT-SWAP"},
        {"channel":"trades","instId":"BTC-USDT-SWAP"}
    ]}))

def okx_msg(msg):
    arg = msg.get("arg") if isinstance(msg.get("arg"), dict) else {}
    channel = arg.get("channel")
    rows = msg.get("data") if isinstance(msg.get("data"), list) else []
    if channel == "books5" and rows:
        r=rows[0] if isinstance(rows[0],dict) else {}; set_book("okx", r.get("bids") or [], r.get("asks") or [])
        b=r.get("bids") or []; a=r.get("asks") or []
        px=[]
        if b: px.append(num(b[0][0]))
        if a: px.append(num(a[0][0]))
        if px: set_price("okx",sum(px)/len(px))
    elif channel == "trades":
        for r in rows:
            if not isinstance(r,dict): continue
            p,q=num(r.get("px")),num(r.get("sz")); set_price("okx",p)
            side="BUY" if str(r.get("side")).lower()=="buy" else "SELL"
            push_event("okx","trade",side,p*q,num(r.get("ts"))/1000.0)


# ---- COINBASE EXCHANGE ---------------------------------------------------
COINBASE_WS = os.getenv("COINBASE_WS_URL", "wss://ws-feed.exchange.coinbase.com")
def coinbase_open(ws):
    ws.send(json.dumps({"type":"subscribe","product_ids":["BTC-USD"],"channels":["ticker","matches","level2"]}))

def coinbase_msg(d):
    typ=str(d.get("type") or "")
    if typ=="ticker": set_price("coinbase",d.get("price"))
    elif typ in ("match","last_match"):
        p,q=num(d.get("price")),num(d.get("size")); set_price("coinbase",p)
        # Coinbase match side is maker side; aggressive side is the opposite.
        maker=str(d.get("side") or "").lower(); side="SELL" if maker=="buy" else "BUY"
        push_event("coinbase","trade",side,p*q,time.time())
    elif typ in ("snapshot","l2update"):
        apply_coinbase_l2(d)


def prune_and_sum(q, seconds, positive_side, negative_side):
    cutoff=time.time()-seconds; pos=neg=0.0
    while q and q[0][0] < cutoff: q.popleft()
    for _,side,usd in q:
        if side==positive_side: pos += usd
        elif side==negative_side: neg += usd
    return pos,neg


def snapshot():
    now=time.time(); out={}
    with LOCK:
        venues=set([k[0] for k in EVENTS] + list(BOOKS) + list(PRICES) + list(HEALTH))
        for v in sorted(venues):
            h=dict(HEALTH.get(v) or {})
            price, price_at = PRICES.get(v,(0.0,0.0))
            book=dict(BOOKS.get(v) or {})
            tq=EVENTS.setdefault((v,"trade"),deque(maxlen=12000)); lq=EVENTS.setdefault((v,"liq"),deque(maxlen=12000))
            buy,sell=prune_and_sum(tq,15.0,"BUY","SELL")
            long_liq,short_liq=prune_and_sum(lq,60.0,"LONG","SHORT")
            total=buy+sell; fr=(buy-sell)/total if total else 0.0
            # Momentum uses last price against oldest recent trade-derived price only
            # when available; the support scorer tolerates absent momentum.
            updated=max(num(h.get("updated_at")),num(book.get("updated_at")),price_at)
            healthy=bool(h.get("healthy")) and now-updated < 20.0
            out[v]={
                "healthy":healthy,"updated_at":updated,"price":round(price,2) if price else None,
                "flow_15s":{"buy_usd":round(buy,2),"sell_usd":round(sell,2),"ratio":round(fr,5)},
                "liquidations_60s":{"long_usd":round(long_liq,2),"short_usd":round(short_liq,2)},
                "book":{"bid_usd":round(num(book.get("bid_usd")),2),"ask_usd":round(num(book.get("ask_usd")),2),"imbalance":round(num(book.get("imbalance")),5)},
                "error":str(h.get("error") or "")[:160],
            }
    prices=[num(x.get("price")) for x in out.values() if num(x.get("price"))>0]
    med=statistics.median(prices) if prices else None
    return {"version":VERSION,"generated_at":iso_now(),"generated_at_ms":now_ms(),"symbol":"BTC","btc_price":round(med,2) if med else None,"venues":out,"paper_mode":True,"live_order_placed":False}


def poster_loop():
    while True:
        try:
            if POST_URL:
                headers={"Content-Type":"application/json"}
                if NEXUS_SECRET: headers["X-NEXUS-SECRET"]=NEXUS_SECRET
                SESSION.post(POST_URL,json=snapshot(),headers=headers,timeout=4)
        except Exception:
            pass
        time.sleep(max(0.5,POST_INTERVAL))


def start_background():
    global STARTED
    with START_LOCK:
        if STARTED: return
        STARTED=True
        jobs=[
            ("bybit","wss://stream.bybit.com/v5/public/linear",bybit_open,bybit_msg),
            ("binance",BINANCE_URL,binance_open,binance_msg),
            ("okx",OKX_WS,okx_open,okx_msg),
            ("coinbase",COINBASE_WS,coinbase_open,coinbase_msg),
        ]
        for args in jobs:
            threading.Thread(target=ws_forever,args=args,daemon=True,name=f"mx-{args[0]}").start()
        threading.Thread(target=poster_loop,daemon=True,name="mx-poster").start()

if os.getenv("NEXUS_BRIDGE_AUTOSTART", "1").strip() != "0":
    start_background()

@app.get("/")
def root(): return jsonify({"service":"NEXUS MULTI-EXCHANGE BRIDGE","version":VERSION,"status":"ONLINE","snapshot":snapshot()})
@app.get("/healthz")
def healthz(): return jsonify({"ok":True,"version":VERSION,"venues":snapshot().get("venues")})
@app.get("/api/snapshot")
def api_snapshot(): return jsonify(snapshot())

if __name__ == "__main__":
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","10000")))
