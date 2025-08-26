#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, re, pathlib, sys, traceback
from datetime import datetime, timezone, timedelta
import requests

STATE_FILE = pathlib.Path("sosovalue_state.json")
PAYLOAD_DUMP = pathlib.Path("last_payload.json")

def log(*a): print(*a, flush=True)

def load_state():
    if STATE_FILE.exists():
        try: return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception: return {}
    return {}

def save_state(d):
    STATE_FILE.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")

def jst_yesterday_date():
    today_jst = datetime.now(timezone.utc) + timedelta(hours=9)
    return (today_jst - timedelta(days=1)).date()

def norm(s: str) -> str:
    return " ".join(str(s).replace("\xa0"," ").split()).strip()

def fnum(x) -> float:
    if x is None: return 0.0
    if isinstance(x, (int, float)): return float(x)
    s = norm(x).replace(",", "")
    if s in {"", "-", "–", "—"}: return 0.0
    if s.startswith("(") and s.endswith(")"):
        try: return -float(s[1:-1])
        except Exception: return 0.0
    m = re.match(r'^(-?\d+(?:\.\d+)?)([mbMB])?$', s)
    if m:
        val = float(m.group(1)); suf = m.group(2)
        if suf and suf.lower()=="b": val *= 1000.0
        return val
    try: return float(s)
    except Exception: return 0.0

def request_current_metrics(kind: str):
    base = os.getenv("SOSO_BASE", "https://api.sosovalue.xyz")
    url = f"{base}/openapi/v2/etf/currentEtfDataMetrics"
    api_key = os.getenv("SOSO_API_KEY")
    if not api_key: raise RuntimeError("SOSO_API_KEY not set")
    headers = {
        "x-soso-api-key": api_key,
        "accept": "application/json",
        "user-agent": "etf-flow-sentry/1.0"
    }
    log(f"[http] POST {url} kind={kind}")
    r = requests.post(url, json={"type": kind}, headers=headers, timeout=25)
    log(f"[http] status={r.status_code} content-type={r.headers.get('content-type')}")
    r.raise_for_status()
    return r.json()

def pick_series(payload):
    # ざっくり構造ログ
    if isinstance(payload, dict):
        log("[debug] top-level keys:", list(payload.keys())[:10])
    else:
        log("[debug] payload type:", type(payload).__name__)

    # まず全体を保存しておく（デバッグに使える）
    try:
        PAYLOAD_DUMP.write_text(json.dumps(payload, ensure_ascii=False, indent=2)[:400000], encoding="utf-8")
        log(f"[debug] payload dumped -> {PAYLOAD_DUMP}")
    except Exception as e:
        log(f"[warn] payload dump failed: {e}")

    rows=[]
    if isinstance(payload, dict):
        cand=None
        for k in ("data","result","items","list","rows"):
            if k in payload and isinstance(payload[k], list):
                cand = payload[k]; break
        if isinstance(cand, list):
            for rec in cand:
                if not isinstance(rec, dict): continue
                d = rec.get("date") or rec.get("tradingDay") or rec.get("day") or rec.get("statDate")
                d2=None
                for fmt in ("%Y-%m-%d","%Y/%m/%d","%d %b %Y"):
                    try:
                        d2 = datetime.strptime(d, fmt).date(); break
                    except Exception: pass
                items = rec.get("items") or rec.get("funds") or rec.get("etfs") or rec.get("records")
                out=[]
                if isinstance(items, list):
                    for it in items:
                        if not isinstance(it, dict): continue
                        name = it.get("ticker") or it.get("fund") or it.get("name") or "ETF"
                        val = it.get("net") or it.get("netFlow") or it.get("net_flow") or it.get("flow") or it.get("netUsd")
                        if val is None and "inflow" in it and "outflow" in it:
                            val = fnum(it.get("inflow")) - fnum(it.get("outflow"))
                        out.append({"name": str(name), "net": fnum(val)})
                if d2 and out:
                    rows.append({"date": d2, "items": out})
    log(f"[debug] parsed rows={len(rows)}; sample_dates={[r['date'].isoformat() for r in rows[:5]]}")
    return rows

def build_embed(title_day, flows):
    net = sum(v for _,v in flows)
    color = 0x2ecc71 if net>0 else 0xe74c3c if net<0 else 0x95a5a6
    shown = [(k,v) for k,v in flows if abs(v)>0.0] or flows[:6]
    fields=[{"name":k,"value":f"{'🟢' if v>0 else '🔴' if v<0 else '⚪'} {v:+,.1f} $m","inline":True} for k,v in shown]
    return {"title":f"{title_day} ETF Net Flows ($m)","color":color,"fields":fields,"footer":{"text":f"Net: {net:+,.1f} $m • Source: SoSoValue API"}}

def main():
    log("[boot] SoSoValue ETF Flow Sentry (direct API, verbose)")
    webhook = os.getenv("DISCORD_WEBHOOK"); 
    if not webhook: raise RuntimeError("DISCORD_WEBHOOK not set")
    send_eth = os.getenv("SEND_ETH","0")=="1"
    yday = jst_yesterday_date(); log(f"[info] yday(JST) = {yday.isoformat()}")

    state = load_state()
    embeds=[]

    # BTC
    btc_payload = request_current_metrics("us-btc-spot")
    btc_series = pick_series(btc_payload)
    btc_row = next((r for r in btc_series if r["date"]==yday), None)
    log(f"[debug] btc_row_found = {btc_row is not None}")
    if btc_row:
        btc_flows = [(it["name"], it["net"]) for it in btc_row["items"]]
        if yday.isoformat()!=state.get("last_btc_day"):
            embeds.append(build_embed(yday.strftime("%d %b %Y")+" (BTC)", btc_flows))
            state["last_btc_day"]=yday.isoformat()
        else:
            log("[info] BTC already sent for this day (dedup)")

    # ETH (optional)
    if send_eth:
        eth_payload = request_current_metrics("us-eth-spot")
        eth_series = pick_series(eth_payload)
        eth_row = next((r for r in eth_series if r["date"]==yday), None)
        log(f"[debug] eth_row_found = {eth_row is not None}")
        if eth_row:
            eth_flows = [(it["name"], it["net"]) for it in eth_row["items"]]
            if yday.isoformat()!=state.get("last_eth_day"):
                embeds.append(build_embed(yday.strftime("%d %b %Y")+" (ETH)", eth_flows))
                state["last_eth_day"]=yday.isoformat()
            else:
                log("[info] ETH already sent for this day (dedup)")

    if embeds:
        resp = requests.post(webhook, json={"embeds":embeds}, timeout=20)
        log(f"[discord] status={resp.status_code}")
        resp.raise_for_status()
        save_state(state)
        log(f"[ok] sent embeds x{len(embeds)} for {yday.isoformat()}")
    else:
        log("[info] No data yet (silent or already sent)")

if __name__=="__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(1)
