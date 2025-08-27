#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SoSoValue ETF Flow Sentry (BTC+ETH, per-fund if available)
- 前日(JST)の US BTC/ETH Spot ETF のネットフローを Discord へ通知
- 銘柄別API(SOSO_FUNDS_API) が設定されていれば内訳を取得、無ければ集計APIにフォールバック
- 既送信日の重複送信ガード、payload ダンプ、月間コール上限の自己防衛つき
"""

import os, json, re, pathlib, sys, time, traceback
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Tuple
import requests

STATE_FILE   = pathlib.Path("sosovalue_state.json")
PAYLOAD_DUMP = pathlib.Path("last_payload.json")  # 直近レスポンスのダンプ
DEFAULT_BASE = "https://api.sosovalue.xyz"
FORCE_SEND = os.getenv("FORCE_SEND", "0") == "1"

# === ユーティリティ ===
def log(*a): print(*a, flush=True)

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
        val = float(m.group(1))
        if (m.group(2) or "").lower() == "b":
            val *= 1000.0
        return val
    try: return float(s)
    except Exception: return 0.0

def load_state() -> Dict[str, Any]:
    if STATE_FILE.exists():
        try: return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception: return {}
    return {}

def save_state(d: Dict[str, Any]):
    STATE_FILE.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")

# === 月間コール上限（自己防衛） ===
MAX_CALLS_PER_MONTH = int(os.getenv("MAX_CALLS_PER_MONTH", "1000"))

def _ym_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")

def can_use_api(state: Dict[str, Any], needed: int) -> bool:
    ym = _ym_utc()
    used = state.get("monthly_calls", {}).get(ym, 0)
    return (used + needed) <= MAX_CALLS_PER_MONTH

def add_api_calls(state: Dict[str, Any], used: int):
    ym = _ym_utc()
    calls = state.setdefault("monthly_calls", {})
    calls[ym] = calls.get(ym, 0) + used

# === HTTP（429/5xx リトライ & ダンプ） ===
def post_json(url: str, body: dict, api_key: str, max_retries: int = 3) -> dict:
    headers = {
        "x-soso-api-key": api_key,
        "accept": "application/json",
        "user-agent": "etf-flow-sentry/1.0"
    }
    backoff = 2.0
    last_resp = None
    for attempt in range(1, max_retries + 1):
        resp = requests.post(url, json=body, headers=headers, timeout=30)
        last_resp = resp
        log(f"[http] POST {url} -> {resp.status_code} ({resp.headers.get('content-type','')})")
        if resp.status_code == 200:
            data = resp.json()
            try:
                PAYLOAD_DUMP.write_text(json.dumps(data, ensure_ascii=False, indent=2)[:400000], encoding="utf-8")
                log(f"[debug] payload dumped -> {PAYLOAD_DUMP}")
            except Exception:
                pass
            return data
        if resp.status_code in (429, 500, 502, 503, 504) and attempt < max_retries:
            time.sleep(backoff); backoff *= 2
            continue
        resp.raise_for_status()
    # ここに来ない想定だが保険
    if last_resp is not None:
        last_resp.raise_for_status()
    raise RuntimeError("post_json failed without response")

# === APIラッパ ===
def request_aggregate(kind: str, api_key: str) -> Tuple[Any, Any, int]:
    base = os.getenv("SOSO_BASE", DEFAULT_BASE)
    url  = f"{base}/openapi/v2/etf/currentEtfDataMetrics"
    data = post_json(url, {"type": kind}, api_key)
    try:
        dn = (data.get("data") or {}).get("dailyNetInflow") or {}
        day = datetime.strptime(dn.get("lastUpdateDate"), "%Y-%m-%d").date()
        net_musd = fnum(dn.get("value")) / 1e6  # USD → $m
        return day, net_musd, 1
    except Exception:
        return None, None, 1

def request_fund_breakdown(kind: str, api_key: str) -> Tuple[List[Dict[str, Any]], int]:
    funds_api = os.getenv("SOSO_FUNDS_API", "").strip()
    if not funds_api:
        log("[info] SOSO_FUNDS_API not set -> skip per-fund (aggregate only)")
        return [], 0
    data = post_json(funds_api, {"type": kind}, api_key)
    return pick_series(data), 1

# === 再帰パーサ（銘柄別） ===
def pick_series(payload: Any) -> List[Dict[str, Any]]:
    from datetime import datetime
    DATE_KEYS = ("date","tradingDay","day","statDate","dateStr","date_time")
    ITEM_KEYS = ("items","funds","etfs","records","list","rows","data")
    NAME_KEYS = ("ticker","fund","name","etf","symbol")
    NET_KEYS  = ("net","netFlow","net_flow","netUsd","flow","net_usd")
    INFLOW_KEYS  = ("inflow","inFlowUsd","spotInflow")
    OUTFLOW_KEYS = ("outflow","outFlowUsd","spotOutflow")

    def to_date(s):
        s = norm(s)
        for fmt in ("%Y-%m-%d","%Y/%m/%d","%d %b %Y"):
            try: return datetime.strptime(s, fmt).date()
            except Exception: pass
        return None

    def parse_item_dict(d: dict):
        name = next((d[k] for k in NAME_KEYS if k in d), None)
        val  = next((d[k] for k in NET_KEYS  if k in d), None)
        if val is None:
            inflow  = next((d.get(k) for k in INFLOW_KEYS  if k in d), None)
            outflow = next((d.get(k) for k in OUTFLOW_KEYS if k in d), None)
            if inflow is not None or outflow is not None:
                val = fnum(inflow) - fnum(outflow)
        if name is None and val is None: return None
        return {"name": str(name or "ETF"), "net": fnum(val)}

    def try_make_row(obj):
        if not isinstance(obj, dict): return None
        d_val = next((to_date(obj[k]) for k in DATE_KEYS if k in obj), None)
        if not d_val: return None
        items = next((obj[k] for k in ITEM_KEYS if k in obj and isinstance(obj[k], list)), None)
        if not items: return None
        out=[]
        for it in items:
            if isinstance(it, dict):
                p = parse_item_dict(it)
                if p: out.append(p)
        return {"date": d_val, "items": out} if out else None

    rows=[]
    def walk(x):
        if isinstance(x, dict):
            r = try_make_row(x)
            if r: rows.append(r)
            for v in x.values(): walk(v)
        elif isinstance(x, list):
            for v in x: walk(v)
    walk(payload)

    by_date={}
    for r in rows:
        by_date.setdefault(r["date"], []).extend(r["items"])
    merged=[{"date": d, "items": its} for d,its in sorted(by_date.items())]
    log(f"[debug] parsed rows={len(merged)}; sample_dates={[r['date'].isoformat() for r in merged[:5]]}")
    return merged

# === Discord ===
def build_embed(title: str, flows: List[Tuple[str,float]]):
    top_n = int(os.getenv("SHOW_TOP_N", "24"))  # Discordのフィールド上限に配慮
    flows_sorted = sorted(flows, key=lambda x: abs(x[1]), reverse=True)[:top_n]
    net = sum(v for _,v in flows)
    color = 0x2ecc71 if net>0 else 0xe74c3c if net<0 else 0x95a5a6
    fields = [{"name": k, "value": f"{'🟢' if v>0 else '🔴' if v<0 else '⚪'} {v:+,.1f} $m", "inline": True}
              for k,v in flows_sorted]
    return {
        "title": title,
        "color": color,
        "fields": fields,
        "footer": {"text": f"Net: {net:+,.1f} $m • Source: SoSoValue API"}
    }

# === metrics 取得（v2/v1 両対応） ==========================================
def fetch_metrics(kind: str) -> dict:
    """
    /openapi/v2/etf/currentEtfDataMetrics を叩いて payload を返す。
    - v2: SOSO_CLIENT_ID / SOSO_CLIENT_SECRET があれば openapi.sosovalue.com
    - v1: それが無ければ api.sosovalue.xyz （x-soso-api-key）
    既存の post_json_full or post_json のどちらがあっても動くようにしてある。
    """
    base_v2 = os.getenv("SOSO_BASE", "https://openapi.sosovalue.com")
    base_v1 = os.getenv("SOSO_BASE", "https://api.sosovalue.xyz")  # デモ

    is_v2 = bool(os.getenv("SOSO_CLIENT_ID") and os.getenv("SOSO_CLIENT_SECRET"))
    if is_v2:
        path = "/openapi/v2/etf/currentEtfDataMetrics"
        # 新実装が入っている場合
        try:
            return post_json_full(path, {"type": kind})
        except NameError:
            # 念のため直URLもサポート
            url = f"{base_v2}{path}"
            headers = {
                "client-id": os.getenv("SOSO_CLIENT_ID"),
                "client-secret": os.getenv("SOSO_CLIENT_SECRET"),
                "accept": "application/json",
                "content-type": "application/json",
                "user-agent": "etf-flow-sentry/2.0"
            }
            r = requests.post(url, json={"type": kind}, headers=headers, timeout=30)
            r.raise_for_status()
            return r.json()
    else:
        # v1 デモキー（集計のみ）
        path = "/openapi/v2/etf/currentEtfDataMetrics"
        try:
            # post_json_full が無い環境もあるので、まずは post_json_full を試す
            return post_json_full(path, {"type": kind})
        except NameError:
            # 旧 post_json( url, body, api_key ) がある前提でフォールバック
            api_key = os.getenv("SOSO_API_KEY")
            url = f"{base_v1}{path}"
            return post_json(url, {"type": kind}, api_key)

# === metrics payload のパース補助 ========================================
from datetime import datetime

def parse_aggregate_from_metrics(payload):
    """aggregate（合計）値を (date, net_musd) で返す"""
    dn = (payload.get("data") or {}).get("dailyNetInflow") or {}
    day = None
    if dn.get("lastUpdateDate"):
        day = datetime.strptime(dn["lastUpdateDate"], "%Y-%m-%d").date()
    net_musd = fnum(dn.get("value")) / 1e6 if dn.get("value") is not None else 0.0
    return day, net_musd

def parse_funds_from_metrics(payload):
    """
    v2 のとき data.list に銘柄別が入る。
    戻り値: [{"name": ticker, "net": <USD>}, ...]
    """
    lst = (payload.get("data") or {}).get("list") or []
    out = []
    for rec in lst:
        name = rec.get("ticker") or rec.get("id") or rec.get("institute") or "ETF"
        dn = rec.get("dailyNetInflow") or {}
        # status==3 は未反映(null)なので除外
        if dn.get("status") == 3:
            continue
        val = fnum(dn.get("value"))
        out.append({"name": str(name), "net": val})
    return out



# === 1アセット実行 ===
def run_one(kind: str, tag: str, yday):
    used_calls = 1
    payload = fetch_metrics(kind)

    flows = []
    title = None
    dedup_date = None  # ← 実際に送る日付（重複判定用）

    # 銘柄別（v2なら同梱）
    items = parse_funds_from_metrics(payload)
    if items:
        flows = [(it["name"], it["net"]/1e6) for it in items]   # USD → $m
        title = yday.strftime("%d %b %Y") + f" ({tag})"
        dedup_date = yday.isoformat()
    else:
        # 集計のみ（v1デモなど）
        day, net_musd = parse_aggregate_from_metrics(payload)
        if day == yday or FORCE_SEND:
            target_day = day or yday   # dayが無ければydayで代用
            flows = [(f"Total (All {tag} ETFs)", net_musd)]
            title = target_day.strftime("%d %b %Y") + f" ({tag}, aggregate)"
            dedup_date = target_day.isoformat()

    if flows:
        return build_embed(title, flows), used_calls, dedup_date
    return None, used_calls, None

# === メイン ===
def main():
    log("[boot] SoSoValue ETF Flow Sentry (limits-aware)")
    webhook = os.getenv("DISCORD_WEBHOOK");  assert webhook, "DISCORD_WEBHOOK not set"
    api_key = os.getenv("SOSO_API_KEY");     assert api_key, "SOSO_API_KEY not set"
    send_eth = os.getenv("SEND_ETH", "1") == "1"

    yday = jst_yesterday_date()
    log(f"[info] yday(JST) = {yday.isoformat()}")

    state = load_state()
    embeds = []

    # 事前見積もり（最悪ケースでチェック：銘柄別APIが設定されていれば各2コール、無ければ各1）
    worst_per_asset = 2 if os.getenv("SOSO_FUNDS_API", "").strip() else 1
    assets = 1 + (1 if send_eth else 0)
    needed = worst_per_asset * assets

    if not can_use_api(state, needed):
        msg = f"⚠️ SoSoValue API monthly limit would exceed ({state.get('monthly_calls',{}).get(_ym_utc(),0)} + {needed} > {MAX_CALLS_PER_MONTH}). Skipping."
        log("[warn]", msg)
        try: requests.post(webhook, json={"content": msg}, timeout=15)
        except Exception: pass
        return

    # BTC
    emb, used, dt = run_one("us-btc-spot", "BTC", yday)
    add_api_calls(state, used)
    if emb and state.get("last_btc_day") != dt:
        embeds.append(emb)
        state["last_btc_day"] = dt


    # ETH
    if send_eth:
        emb, used, dt = run_one("us-eth-spot", "ETH", yday)
        add_api_calls(state, used)
        if emb and state.get("last_eth_day") != dt:
            embeds.append(emb)
            state["last_eth_day"] = dt

    if embeds:
        r = requests.post(webhook, json={"embeds": embeds}, timeout=20)
        log(f"[discord] status={r.status_code}")
        r.raise_for_status()

    
    save_state(state)
    log(f"[ok] done. monthly_calls[{_ym_utc()}]={state.get('monthly_calls',{}).get(_ym_utc(),0)}")



if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        sys.exit(1)
