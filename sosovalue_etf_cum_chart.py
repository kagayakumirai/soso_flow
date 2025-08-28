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
from datetime import datetime, timezone, timedelta
import matplotlib.dates as mdates

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
    """
    payload から履歴の配列を取り出す。想定される形:
      1) [ {...}, ... ]
      2) {"data": {"list": [ ... ]}}
      3) {"data": [ ... ]}               <-- 今ここで落ちた
      4) {"list": [ ... ]} などのトップレベル
    """
    # 1) 配列そのもの
    if isinstance(payload, list):
        return payload

    # 2) dict の場合を網羅
    if isinstance(payload, dict):
        data = payload.get("data")

        # 2-a) data が配列
        if isinstance(data, list):
            return data

        # 2-b) data が dict
        if isinstance(data, dict):
            lst = data.get("list")
            if isinstance(lst, list):
                return lst
            # 他によくあるキー
            for k in ("records", "items", "rows"):
                v = data.get(k)
                if isinstance(v, list):
                    return v

        # 2-c) トップレベルに list 系キーがある
        for k in ("list", "records", "items", "rows"):
            v = payload.get(k)
            if isinstance(v, list):
                return v

    # どれにも当てはまらなければ空配列
    return []


def fetch_history(kind: str):
    payload = post_json("/openapi/v2/etf/historicalInflowChart", {"type": kind})

    def _extract_list(p):
        if isinstance(p, list): return p
        if isinstance(p, dict):
            data = p.get("data")
            if isinstance(data, list): return data
            if isinstance(data, dict):
                for k in ("list", "records", "items", "rows"):
                    v = data.get(k)
                    if isinstance(v, list): return v
            for k in ("list", "records", "items", "rows"):
                v = p.get(k)
                if isinstance(v, list): return v
        return []

    lst = _extract_list(payload)

    dates, cum_b, daily_b = [], [], []
    for row in lst:
        if not isinstance(row, dict):
            continue
        d = row.get("date")
        cum = row.get("cumNetInflow")
        day = row.get("totalNetInflow")
        if not d or cum is None or day is None:
            continue
        dates.append(datetime.strptime(d, "%Y-%m-%d").date())
        cum_b.append(float(cum) / 1e9)     # 累積 USD -> $B
        daily_b.append(float(day) / 1e9)   # 日次 USD -> $B
    return dates, cum_b, daily_b


def make_chart(btc_dates, btc_cum_b, btc_day_b, eth_dates, eth_cum_b, eth_day_b, out_path):
    plt.figure(figsize=(11, 6.2))

    # ---- 累積（左軸） ----
    ax = plt.gca()
    ax.plot(btc_dates, btc_cum_b, label="BTC ETFs (cum $B)")
    ax.plot(eth_dates, eth_cum_b, label="ETH ETFs (cum $B)")
    ax.set_ylabel("Cumulative Net Inflow ($B)")
    ax.grid(True, alpha=0.3)

    # ---- 日次（右軸、棒） ----
    ax2 = ax.twinx()
    # 日付を数値にして±0.4日ずらし（重なり回避）
    x_btc = mdates.date2num(btc_dates) - 0.4
    x_eth = mdates.date2num(eth_dates) + 0.4
    ax2.bar(x_btc, btc_day_b, width=0.8, alpha=0.25, align="center", label="BTC daily ($B/day)")
    ax2.bar(x_eth, eth_day_b, width=0.8, alpha=0.25, align="center", label="ETH daily ($B/day)")
    ax2.set_ylabel("Daily Net Inflow ($B/day)")

    # ---- 体裁 ----
    ax.set_title("Cumulative Net Inflow (US Spot ETFs)")
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax.xaxis.get_major_locator()))
    # 2軸の凡例を合体
    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc="upper left")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()

from datetime import datetime, timezone, timedelta

def is_confirmed_yday() -> tuple[bool, str, str]:
    """昨日(JST)が履歴に出たら True。戻り: (確定?, 昨日文字列, 最新確定日文字列)"""
    now_jst = datetime.now(timezone(timedelta(hours=9)))
    yday = (now_jst.date() - timedelta(days=1))
    yday_str = yday.strftime("%Y-%m-%d")

    btc_d, _, _ = fetch_history("us-btc-spot")
    eth_d, _, _ = fetch_history("us-eth-spot")
    last_hist = max(btc_d[-1], eth_d[-1]) if (btc_d and eth_d) else (btc_d[-1] if btc_d else eth_d[-1])
    last_hist_str = last_hist.strftime("%Y-%m-%d")

    return (last_hist >= yday), yday_str, last_hist_str



def send_to_discord(webhook: str, png_path: str, btc_last_b: float, eth_last_b: float, last_date: str):
    content = f"**ETF cumulative net inflow (up to {last_date})**\n" \
              f"BTC: {btc_last_b:,.2f} B | ETH: {eth_last_b:,.2f} B"
    embed = {
        "title": TITLE,
        "image": {"url": f"attachment://{PNG_NAME}"},
        "footer": {"text": "Source: SoSoValue API"}
    }
    # ← ここで実際に Discord POST してるはず

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
    btc_d, btc_cum, btc_day = fetch_history("us-btc-spot")
    eth_d, eth_cum, eth_day = fetch_history("us-eth-spot")
    
    # 描画
    make_chart(btc_d, btc_cum, btc_day, eth_d, eth_cum, eth_day, PNG_NAME)

    # 最終日付（listから直接取り出す）
    last_date = max(btc_d[-1], eth_d[-1]).strftime("%Y-%m-%d")
    
    # 送信（dates は list、cum は $B単位の list[float]）
    send_to_discord(webhook, PNG_NAME, btc_cum[-1], eth_cum[-1], last_date)
    
    print("[ok] chart sent")


if __name__ == "__main__":
    main()
