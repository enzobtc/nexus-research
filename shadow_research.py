"""Read-only NEXUS BTC 15m comparison against fixed shadow baselines.

Uses one observation per contract, the first snapshot with 330-360 seconds
remaining. Discovery ends at 2026-09-23T01:15:00Z; later markets are held out.
Never submits orders or changes NEXUS files.
"""
import collections
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import statistics

ROOT = Path(__file__).resolve().parent
CUTOFF = datetime.fromisoformat("2026-09-23T01:15:00+00:00")


def timestamp(value):
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def number(value):
    try:
        n = float(value)
        return n if math.isfinite(n) else None
    except (ValueError, TypeError):
        return None


def dollars(value):
    n = number(value)
    return n / 100.0 if n is not None and n > 1 else n


def fee(ask):
    # One-contract estimate following NEXUS's current general taker formula.
    return math.ceil(0.07 * ask * (1 - ask) * 100 - 1e-12) / 100


def grade(outcome, side, row):
    if side not in ("UP", "DOWN"):
        return None
    ask = dollars(row.get("kalshi_yes_ask" if side == "UP" else "kalshi_no_ask"))
    if ask is None or not 0 < ask < 1:
        return None
    win = side == outcome
    return {"win": win, "ask": ask, "pnl": round(float(win) - ask - fee(ask), 4)}


def summary(rows):
    if not rows:
        return "0 markets"
    wins = sum(row["win"] for row in rows)
    pnl = sum(row["pnl"] for row in rows)
    return f"{len(rows)} markets; {wins}W-{len(rows)-wins}L; " \
           f"{wins/len(rows):.1%} accuracy; ${pnl:+.2f} estimated for 1 contract each"


def evaluate():
    trades = json.loads((ROOT / "paper_trades.json").read_text())
    quotes = collections.defaultdict(list)
    with (ROOT / "research_data" / "nexus_v58_snapshots.jsonl").open() as handle:
        for line in handle:
            if line.strip():
                row = json.loads(line)
                quotes[row["contract_ticker"]].append(row)
    unique = {}
    for trade in trades:
        if (trade.get("settlement_source") == "KALSHI_OFFICIAL"
                and trade.get("market_outcome") in ("UP", "DOWN")
                and trade.get("features", {}).get("opening_policy") == "V5.8_FLOW_VALUE"):
            unique[trade.get("contract_ticker")] = trade
    groups = {"discovery": collections.defaultdict(list),
              "future_holdout": collections.defaultdict(list)}
    skipped = collections.Counter()
    for ticker, trade in sorted(unique.items(), key=lambda pair: pair[1]["market_open"]):
        stage = "discovery" if timestamp(trade["market_open"]) <= CUTOFF else "future_holdout"
        candidates = [r for r in quotes.get(ticker, ())
                      if 330 <= (number(r.get("seconds_remaining")) or -1) <= 360
                      and timestamp(r["observed_at"]) < timestamp(trade["market_close"])]
        if not candidates:
            skipped[stage] += 1
            continue
        row = min(candidates, key=lambda r: timestamp(r["observed_at"]))
        outcome = trade["market_outcome"]
        spot, strike = number(row.get("btc_price")), number(row.get("strike_price"))
        yes_bid, yes_ask = dollars(row.get("kalshi_yes_bid")), dollars(row.get("kalshi_yes_ask"))
        if spot is None or strike is None or spot == strike:
            skipped[stage] += 1
            continue
        sides = {
            "NEXUS opening": trade.get("signal"),
            "NEXUS live at 6 min": row.get("prediction"),
            "spot versus strike": "UP" if spot > strike else "DOWN",
            "Kalshi midpoint favorite": "UP" if yes_bid is not None and yes_ask is not None
                                        and (yes_bid + yes_ask) / 2 > .5 else "DOWN",
        }
        for name, side in sides.items():
            result = grade(outcome, side, row)
            if result is not None:
                groups[stage][name].append(result)
    print("Read-only, one contract per signal, at the first recorded quote with 330-360s left.")
    print("Estimated ask fills and general fee; no slippage or proof of execution.\n")
    for stage, rows in groups.items():
        print(stage.upper(), "skipped without fixed-time quote:", skipped[stage])
        for name in ("NEXUS opening", "NEXUS live at 6 min", "spot versus strike",
                     "Kalshi midpoint favorite"):
            print("  ", name, "â", summary(rows[name]))
        print()


if __name__ == "__main__":
    evaluate()
