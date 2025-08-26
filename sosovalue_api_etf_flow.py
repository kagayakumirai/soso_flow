#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SoSoValue ETF Flow Sentry (Direct API) → Discord
"""

import os, json, re, pathlib
from datetime import datetime, timezone, timedelta
import requests

STATE_FILE = pathlib.Path("sosovalue_state.json")

def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_state(d):
    STATE_FILE.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")

def jst_yesterday_date():
    today_jst = datetime.now(timezone.utc) + timedelta(hours=9)
    y = (today_jst - timedelta(days=1)).date()
    return y

def norm(s: str) -> str:
    return " ".join(str(s).replace("\xa0"," ").split()).strip()

def fnum(x) -> float:
    if x is None: return 0.0
    if isinstance(x, (int, float)): return float(x)
    s = norm(x).replace(",", "")
    if s in {"", "-", "–", "—"}: return 0.0
    if s.startswith("(") and s.endswith(")"):
        try: return -float(s[1:-1])
        except: return 0.0
    try: return float(s)
    except: return 0.0

def request_current_metrics(kind: str):
    base = os.getenv("SOSO_BASE", "https://api.sosovalue.xyz")
    url = f"{base}/openapi/v2/etf/currentEtfDataMetrics"
    api_key = os.getenv("SOSO_API_KEY")
    if not api_key:
        raise RuntimeError("SOSO_API_KEY not set")
    headers = {
        "x-soso-api-key": api_key,
        "accept": "application/json",
        "user-agent": "etf-flow-sentry/1.0"
    }
    r = requests.post(url, json={"type": kind}, headers=headers, timeout=25)
    r.raise_for_status()
    return r.json()

def pick_series(payload):
    rows = []
    if isinstance(payload, dict):
        cand = None
        for k in ("data","result","items","list","rows"):
            if k in payload and isinstance(payload[k], list):
                cand = payload[k];
