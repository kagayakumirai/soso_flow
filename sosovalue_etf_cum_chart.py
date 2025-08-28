#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SoSoValue ETF Cumulative Flow Chart (BTC+ETH)
- /openapi/v2/etf/historicalInflowChart から BTC/ETH の
  「累計(cumNetInflow)」と「日次(totalNetInflow)」を取得
- 未確定なら送信スキップ、確定後にグラフPNG＋本文をDiscordへ送信
- v2(client-id/secret) / v1(x-soso-api-key) を自動判別
"""

import os, json, time
from datetime import datetime, timezone, timedelta
import requests
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

TITLE    = "Cumulative Net Inflow (US Spot ETFs)"
PNG_NAME = "etf_cum_flow.png"

# ------------------ HTTP / Auth ------------------
def resolve_headers_and_base():
    cid  = os.getenv("SOSO_CLIENT_ID")
    csec = os.getenv("SOSO_CLIENT_SECRET")
    if cid and csec:
        base = os.getenv("SOSO_BASE", "https://openapi.sosovalue.com")
        headers = {
            "client-id": cid,
            "client-secret": csec,
            "accept": "application/json",
            "content-type": "application/json",
            "user-agent": "etf-cum-chart/2.0",
        }
        mode = "v2"
    else:
        base = os.getenv("SOSO_BASE", "https://api.sosovalue.xyz")
        api_key = os.getenv("SOSO_API_KEY")
        if not api_key:
            raise RuntimeError("Set SOSO_CLIENT_ID/SOSO_CLIENT_SECRET or SOSO_API_KEY")
        headers = {
            "x-soso-api-key": api_key,
            "accept": "application/json",
            "content-type": "application/json",
            "user-agent": "etf-cum-chart/1.x",
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
        if r.status_code in (429, 500, 502, 503, 504) and i < max_retries - 1:
            time.sleep(backoff); backoff *= 2; continue
        r.raise_for_status()
    raise RuntimeError("HTTP failed")

# ------------------ payload helpers ------------------
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

def _last_hist_date(kind: str):
    """historicalInflowChart から種別の最新確定日(date)を返す"""
    payload = post_json("/openapi/v2/etf/historicalInflowChart", {"type": kind})
    lst = _extract_list(payload)
    dates = []
    for it in lst:
        d = it.get("date")
        if d:
            dates.append(datetime.strptime(d, "%Y-%m-%d").date())
    return max(dates) if dates else None

def is_confirmed_yday(send_eth: bool = True):
    """
    前日(JST)が確定していれば True。
    戻り値: (confirmed?, yday_str, last_hist_str)
    """
    now_jst = datetime.now(timezone(timedelta(hours=9)))
    yday = (now_jst.date() - timedelta(days=1))
    yday_str = yday.strftime("%Y-%m-%d")

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
        d   = row.get("date")
        cum = row.get("cumNetInflow")
        day = row.get("totalNetInflow")
        if not d or cum is None or day is None:
            continue
        dates.append(datetime.strptime(d, "%Y-%m-%d").date())
        cum_b.append(float(cum) / 1e9)     # USD → $B
        daily_b.append(float(day) / 1e9)   # USD/day → $B/day

    # ←← ここを追加：日付でソートしてから返す
    if dates:
        packed = sorted(zip(dates, cum_b, daily_b), key=lambda x: x[0])
        dates, cum_b, daily_b = map(list, zip(*packed))

    return dates, cum_b, daily_b


# ------------------ drawing ------------------
def make_chart(btc_dates, btc_cum_b, btc_day_b, eth_dates, eth_cum_b, eth_day_b, out_path):
    plt.figure(figsize=(11, 6.2))

    # 累積（左軸）
    ax = plt.gca()
    ax.plot(btc_dates, btc_cum_b, label="BTC ETFs (cum $B)")
    ax.plot(eth_dates, eth_cum_b, label="ETH ETFs (cum $B)")
    ax.set_ylabel("Cumulative Net Inflow ($B)")
    ax.grid(True, alpha=0.3)

    # 日次（右軸、棒）
    ax2 = ax.twinx()
    x_btc = mdates.date2num(btc_dates) - 0.4
    x_eth = mdates.date2num(eth_dates) + 0.4
    ax2.bar(x_btc, btc_day_b, width=0.8, alpha=0.25, align="center", label="BTC daily ($B/day)")
    ax2.bar(x_eth, eth_day_b, width=0.8, alpha=0.25, align="center", label="ETH daily ($B/day)")
    ax2.set_ylabel("Daily Net Inflow ($B/day)")

    # 体裁
    ax.set_title(TITLE)
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax.xaxis.get_major_locator()))
    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc="upper left")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()

# ------------------ Discord ------------------
def send_to_discord(webhook: str, png_path: str,
                    btc_cum_last_b: float, eth_cum_last_b: float,
                    btc_day_last_b: float, eth_day_last_b: float,
                    last_date: str):
    content = (
        f"**ETF cumulative net inflow (up to {last_date})**\n"
        f"BTC: {btc_cum_last_b:,.2f} B  (day {btc_day_last_b:+,.3f} B)\n"
        f"ETH: {eth_cum_last_b:,.2f} B  (day {eth_day_last_b:+,.3f} B)"
        + (f"\n{extra_note}" if extra_note else "")
    )
    embed = {
        "title": TITLE,
        "image": {"url": f"attachment://{PNG_NAME}"},
        "footer": {"text": "Source: SoSoValue API"},
    }
    payload = {"content": content, "embeds": [embed]}
    with open(png_path, "rb") as f:
        files = {"file": (PNG_NAME, f, "image/png")}
        r = requests.post(
            webhook,
            data={"payload_json": json.dumps(payload)},
            files=files,
            timeout=60,
        )
    print(f"[discord] status={r.status_code}", flush=True)
    r.raise_for_status()
# ① ユーティリティ: アセットごとの最新確定日（<= yday）を返す
def last_hist_date(kind: str):
    payload = post_json("/openapi/v2/etf/historicalInflowChart", {"type": kind})
    lst = _extract_list(payload)
    ds = []
    for it in lst:
        d = it.get("date")
        if d:
            ds.append(datetime.strptime(d, "%Y-%m-%d").date())
    return max(ds) if ds else None

def jst_yesterday():
    return (datetime.now(timezone(timedelta(hours=9))).date() - timedelta(days=1))

# ② main の冒頭で BTC/ETH それぞれ確認
yday = jst_yesterday()
last_btc = last_hist_date("us-btc-spot")
last_eth = last_hist_date("us-eth-spot") if os.getenv("SEND_ETH","1")=="1" else None

btc_confirmed = (last_btc is not None and last_btc >= yday)
eth_confirmed = (last_eth is not None and last_eth >= yday)

# 完全に未確定ならスキップ（両方とも yday 未達）
if not (btc_confirmed or eth_confirmed):
    print(f"[info] skip: neither BTC nor ETH confirmed for yday={yday} "
          f"(latest_btc={last_btc}, latest_eth={last_eth})")
    return

# ③ 履歴取得
btc_d, btc_cum, btc_day = fetch_history("us-btc-spot")
eth_d, eth_cum, eth_day = fetch_history("us-eth-spot")

# ④ 確定日の行をピンポイントに拾う（見つからなければ末尾にフォールバック）
def pick_at(dates, series, target):
    idx = next((i for i, d in enumerate(dates) if d == target), None)
    return (series[idx] if idx is not None else (series[-1] if series else None))

target_btc = last_btc if last_btc and last_btc <= yday else (btc_d[-1] if btc_d else None)
target_eth = last_eth if last_eth and last_eth <= yday else (eth_d[-1] if eth_d else None)

btc_cum_last_b = float(pick_at(btc_d, btc_cum, target_btc) or 0.0)
btc_day_last_b = float(pick_at(btc_d, btc_day, target_btc) or 0.0)
eth_cum_last_b = float(pick_at(eth_d, eth_cum, target_eth) or 0.0)
eth_day_last_b = float(pick_at(eth_d, eth_day, target_eth) or 0.0)

# ⑤ 送信テキスト：それぞれの“実際の確定日”を明示。未確定は (stale) を付与
btc_tag = "" if (target_btc and target_btc == yday) else " (stale)"
eth_tag = "" if (target_eth and target_eth == yday) else " (stale)"

last_date_for_title = max(d for d in [target_btc, target_eth] if d).strftime("%Y-%m-%d")

send_to_discord(
    webhook, PNG_NAME,
    btc_cum_last_b, eth_cum_last_b,
    btc_day_last_b, eth_day_last_b,
    last_date_for_title,
    extra_note=f"BTC@{target_btc}{btc_tag}, ETH@{target_eth}{eth_tag}"
)



# ------------------ main ------------------
def main():
    webhook = os.getenv("DISCORD_WEBHOOK")
    assert webhook, "DISCORD_WEBHOOK not set"

    send_eth = os.getenv("SEND_ETH", "1") == "1"

    # 未確定ならスキップ
    confirmed, yday_str, last_hist_str = is_confirmed_yday(send_eth)
    if not confirmed:
        print(f"[info] skip chart: yesterday({yday_str}) is not confirmed yet (latest={last_hist_str})", flush=True)
        return

    # 取得
    btc_d, btc_cum, btc_day = fetch_history("us-btc-spot")
    eth_d, eth_cum, eth_day = fetch_history("us-eth-spot")

    # 描画
    make_chart(btc_d, btc_cum, btc_day, eth_d, eth_cum, eth_day, PNG_NAME)

    # 本文に使う最新値（確定分の末尾）
    last_date      = max(btc_d[-1], eth_d[-1]).strftime("%Y-%m-%d")
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
