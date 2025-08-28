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
    payload から履歴の配列を取り出す（形のブレを吸収）
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for k in ("list", "records", "items", "rows"):
                v = data.get(k)
                if isinstance(v, list):
                    return v
        for k in ("list", "records", "items", "rows"):
            v = payload.get(k)
            if isinstance(v, list):
                return v
    return []

from datetime import datetime, timezone, timedelta

def _last_hist_date(kind: str):
    """historicalInflowChart からその種別の最新確定日(date)を返す"""
    payload = post_json("/openapi/v2/etf/historicalInflowChart", {"type": kind})
    lst = _extract_list(payload)
    dates = []
    for it in lst:
        d = it.get("date")
        if d:
            dates.append(datetime.strptime(d, "%Y-%m-%d").date())
    return max(dates) if dates else None

def is_confirmed_yday(send_eth: bool = True) -> tuple[bool, str, str]:
    """
    前日(JST)が確定していれば True。
    戻り値: (confirmed?, yday_str, last_hist_str)
    """
    now_jst = datetime.now(timezone(timedelta(hours=9)))
    yday = (now_jst.date() - timedelta(days=1))
    yday_str = yday.strftime("%Y-%m-%d")

    # BTC は必須、ETH は SEND_ETH=1 のときだけ見る
    ld_btc = _last_hist_date("us-btc-spot")
    ld_eth = _last_hist_date("us-eth-spot") if send_eth else None

    candidates = [d for d in (ld_btc, ld_eth) if d]
    if not candidates:
        return False, yday_str, "N/A"

    latest = max(candidates)
    return (latest >= yday), yday_str, latest.strftime("%Y-%m-%d")


def fetch_history(kind: str):
    payload = post_json("/openapi/v2/etf/historicalInflowChart", {"type": kind})
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

def fetch_history(kind: str):
    payload = post_json("/openapi/v2/etf/historicalInflowChart", {"type": kind})

    # ここはグローバルの _extract_list をそのまま使う
    lst = _extract_list(payload)

    dates, cum_b, daily_b = [], [], []
    for row in lst:
        if not isinstance(row, dict):
            continue
        d   = row.get("date")
        cum = row.get("cumNetInflow")
        day = row.get("totalNetInflow")
        if not d or cum is None or day is None:
            continue
        dates.append(datetime.strptime(d, "%Y-%m-%d").date())
        cum_b.append(float(cum) / 1e9)
        daily_b.append(float(day) / 1e9)
    return dates, cum_b, daily_b


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





def send_to_discord(webhook: str, png_path: str,
                    btc_cum_last_b: float, eth_cum_last_b: float,
                    btc_day_last_b: float, eth_day_last_b: float,
                    last_date: str):
    content = (
        f"**ETF cumulative net inflow (up to {last_date})**\n"
        f"BTC: {btc_cum_last_b:,.2f} B  (day {btc_day_last_b:+,.3f} B)\n"
        f"ETH: {eth_cum_last_b:,.2f} B  (day {eth_day_last_b:+,.3f} B)"
    )
    embed = {
        "title": "Cumulative Net Inflow (US Spot ETFs)",
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

    send_eth = os.getenv("SEND_ETH", "1") == "1"

    # === 未確定ならスキップ ===
    confirmed, yday_str, last_hist_str = is_confirmed_yday(send_eth)
    if not confirmed:
        print(f"[info] skip chart: yesterday({yday_str}) is not confirmed yet (latest={last_hist_str})", flush=True)
        return
    # ========================
   
    # 取得（履歴API）
    btc_d, btc_cum, btc_day = fetch_history("us-btc-spot")
    eth_d, eth_cum, eth_day = fetch_history("us-eth-spot")

    # 描画
    make_chart(btc_d, btc_cum, btc_day, eth_d, eth_cum, eth_day, PNG_NAME)

    # 最新確定日（履歴の末尾日付）
    last_date = max(btc_d[-1], eth_d[-1]).strftime("%Y-%m-%d")

    # 本文に使う数値（累計は $B、日次も $B/day）
    btc_cum_last_b = float(btc_cum[-1])
    eth_cum_last_b = float(eth_cum[-1])
    btc_day_last_b = float(btc_day[-1])
    eth_day_last_b = float(eth_day[-1])

    # 送信
    send_to_discord(
        webhook, PNG_NAME,
        btc_cum_last_b, eth_cum_last_b,
        btc_day_last_b, eth_day_last_b,
        last_date
    )
    
    print("[ok] chart sent")


if __name__ == "__main__":
    main()
