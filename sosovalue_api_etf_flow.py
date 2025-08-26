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


    data = r.json()
    # ★ 常時ダンプ（先頭40万文字まで）
    try:
        from pathlib import Path
        Path("last_payload.json").write_text(
            json.dumps(data, ensure_ascii=False, indent=2)[:400000],
            encoding="utf-8"
        )
        print("[debug] payload dumped -> last_payload.json", flush=True)
    except Exception as e:
        print("[warn] payload dump failed:", e, flush=True)
    return data

    return r.json()

def pick_series(payload):
    """
    JSON全体を再帰的に走査して、
    - 日付キー: date / tradingDay / day / statDate / dateStr など
    - アイテム配列: items / funds / etfs / records / list など
    を見つけ、{date: date, items: [{name, net}, ...]} の配列に正規化して返す。
    """
    from datetime import datetime

    print("[debug] type(payload) =", type(payload).__name__, flush=True)
    if isinstance(payload, dict):
        print("[debug] top-level keys:", list(payload.keys())[:20], flush=True)
        if "data" in payload and isinstance(payload["data"], dict):
            print("[debug] data keys:", list(payload["data"].keys())[:20], flush=True)


    DATE_KEYS = ("date", "tradingDay", "day", "statDate", "dateStr")
    ITEM_KEYS = ("items", "funds", "etfs", "records", "list", "rows", "data")
    NAME_KEYS = ("ticker", "fund", "name", "etf", "symbol")
    NET_KEYS  = ("net", "netFlow", "net_flow", "netUsd", "flow")
    INFLOW_KEYS  = ("inflow", "inFlowUsd", "spotInflow")
    OUTFLOW_KEYS = ("outflow", "outFlowUsd", "spotOutflow")

    def to_date(s):
        s = norm(s)
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d %b %Y"):
            try:
                return datetime.strptime(s, fmt).date()
            except Exception:
                pass
        return None

    rows = []

    def parse_item_dict(d: dict):
        name = None
        for k in NAME_KEYS:
            if k in d:
                name = d[k]; break
        # 値の候補
        val = None
        for k in NET_KEYS:
            if k in d:
                val = d[k]; break
        if val is None:
            inflow = None
            outflow = None
            for k in INFLOW_KEYS:
                if k in d: inflow = d[k]; break
            for k in OUTFLOW_KEYS:
                if k in d: outflow = d[k]; break
            if inflow is not None or outflow is not None:
                val = fnum(inflow) - fnum(outflow)
        if name is None and val is None:
            return None
        return {"name": str(name or "ETF"), "net": fnum(val)}

    # 親dictに date があり、かつ子に "アイテム配列" がいる形を拾う
    def try_make_row(obj):
        if not isinstance(obj, dict):
            return None
        d_val = None
        for dk in DATE_KEYS:
            if dk in obj:
                d_val = to_date(obj[dk]); 
                if d_val: break
        if not d_val:
            return None
        items = None
        for ik in ITEM_KEYS:
            if ik in obj and isinstance(obj[ik], list):
                items = obj[ik]; break
        if not items:
            return None
        out = []
        for it in items:
            if isinstance(it, dict):
                parsed = parse_item_dict(it)
                if parsed: out.append(parsed)
        if out:
            return {"date": d_val, "items": out}
        return None

    # 再帰走査
    def walk(x):
        if isinstance(x, dict):
            # まずこの dict 自体で行が作れるか
            row = try_make_row(x)
            if row: rows.append(row)
            # 子を辿る
            for v in x.values():
                walk(v)
        elif isinstance(x, list):
            for v in x:
                walk(v)

    walk(payload)
    # できた行を日付でまとめ直す（バラバラに見つかった場合用）
    by_date = {}
    for r in rows:
        by_date.setdefault(r["date"], []).extend(r["items"])
    merged = [{"date": k, "items": v} for k, v in sorted(by_date.items())]
    log(f"[debug] parsed rows={len(merged)}; sample_dates={[r['date'].isoformat() for r in merged[:5]]}")
    return merged


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
