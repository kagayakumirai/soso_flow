#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SoSoValue ETF Cumulative Flow Chart (BTC+ETH, last ~300d)
- /openapi/v2/etf/historicalInflowChart を使って累積ネットフローを取得
- 1枚のPNGに BTC/ETH の累積線を描画し、Discord Webhook に画像添付で送信
- v2(client-id/secret) も v1(x-soso-api-key) も自動判別
"""

import os, json, io, sys, time
from datetime import datetime, timezone, timedelta
import requests
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

TITLE = "Cumulative Net Inflow (US Spot ETFs)"
PNG_NAME = "etf_cum_flow.png"

def resolve_headers_and_base():
    # v2（本番） or v1（デモ）を自動判定
    cid  = os.getenv("SOSO_CLIENT_ID")
    csec = os.getenv("SOSO_CLIENT_SECRET")
    if cid and csec:
        base = os.getenv("SOSO_BASE", "https://openapi.sosovalue.com")
        headers = {
            "client-id": cid, "client-secret": csec,
            "accept": "application/json", "content-type": "application/json",
            "user-agent": "etf-cum-chart/2.0"
        }
        mode = "v2"
    else:
        base = os.getenv("SOSO_BASE", "https://api.sosovalue.xyz")
        api_key = os.getenv("SOSO_API_KEY")
        if not api_key:
            raise RuntimeError("Set SOSO_CLIENT_ID/SOSO_CLIENT_SECRET or SOSO_API_KEY")
        headers = {
            "x-soso-api-key": api_key,
            "accept": "application/json", "content-type": "application/json",
            "user-agent": "etf-cum-chart/1.x"
        }
        mode = "v1"
    return base, headers, mode

def post_json(path, body, max_retries=3):
    base, headers, mode = resolve_headers_and_base()
    url = f"{base}{path}" if not path.startswith("http") else path
    backoff = 2.0
    for i in range(max_retries):
        r = requests.post(url, json=body, headers=headers, timeout=30)
        print(f"[http] ({mode}) POST {url} -> {r.status_code}", flush=True)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 500, 502, 503, 504) and i < max_retries-1:
            time.sleep(backoff); backoff *= 2; continue
        r.raise_for_status()
    raise RuntimeError("HTTP failed")

def _extract_list(payload):
    # payload が dict でも list でも安全に list を返す
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        data = payload.get("data") or {}
        lst = data.get("list")
        if isinstance(lst, list):
            return lst
        # 念のためよくあるキーも見る
        for k in ("records", "items", "rows"):
            v = data.get(k) or payload.get(k)
            if isinstance(v, list):
                return v
    return []

def fetch_history(kind: str):
    payload = post_json("/openapi/v2/etf/historicalInflowChart", {"type": kind})
    lst = _extract_list(payload)

    dates, cum_b = [], []
    for row in lst:
        if not isinstance(row, dict):
            continue
        d = row.get("date")
        v = row.get("cumNetInflow")
        if not d or v is None:
            continue
        dates.append(datetime.strptime(d, "%Y-%m-%d").date())
        cum_b.append(float(v) / 1e9)  # USD -> $B
    return dates, cum_b





def make_chart(btc_dates, btc_b, eth_dates, eth_b, out_path):
    plt.figure(figsize=(10.5, 6))
    # 軸を共有するように単純に2系列を描画
    plt.plot(btc_dates, btc_b, label="BTC ETFs (cum $B)")
    plt.plot(eth_dates, eth_b, label="ETH ETFs (cum $B)")
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.title(TITLE)
    plt.xlabel("Date")
    plt.ylabel("Cumulative Net Inflow ($B)")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()

def send_to_discord(webhook: str, png_path: str, btc_last_b: float, eth_last_b: float, last_date: str):
    content = f"**ETF cumulative net inflow (up to {last_date})**\n" \
              f"BTC: {btc_last_b:,.2f} B | ETH: {eth_last_b:,.2f} B"
    embed = {
        "title": TITLE,
        "image": {"url": f"attachment://{PNG_NAME}"},
        "footer": {"text": "Source: SoSoValue API"}
    }
    payload = {"content": content, "embeds": [embed]}
    with open(png_path, "rb") as f:
        files = {"file": (PNG_NAME, f, "image/png")}
        r = requests.post(webhook, data={"payload_json": json.dumps(payload)}, files=files, timeout=60)
    print(f"[discord] status={r.status_code}", flush=True)
    r.raise_for_status()

def main():
    webhook = os.getenv("DISCORD_WEBHOOK")
    assert webhook, "DISCORD_WEBHOOK not set"

    # 取得
    btc_d, btc_b = fetch_history("us-btc-spot")
    eth_d, eth_b = fetch_history("us-eth-spot")
    assert btc_d and eth_d, "no history data"

    # 描画
    make_chart(btc_d, btc_b, eth_d, eth_b, PNG_NAME)

    # 送信
    last_date = max(btc_d[-1], eth_d[-1]).strftime("%Y-%m-%d")
    send_to_discord(webhook, PNG_NAME, btc_b[-1], eth_b[-1], last_date)
    print("[ok] chart sent")

if __name__ == "__main__":
    main()
