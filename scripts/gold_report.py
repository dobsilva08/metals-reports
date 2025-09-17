#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Relatório diário de Ouro — versão “no-LLM”
Gera narrativa automática via regras (sem chamadas a IA).
Inclui contador sequencial persistente em .sent/counter_golddaily.txt.
"""

import os
import json
import time
import datetime as dt
from textwrap import dedent
import requests

# ---------- Telegram ----------

def tg_send(token: str, chat_id: str, text: str, disable_web_page_preview=True):
    if not token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown",
        "disable_web_page_preview": disable_web_page_preview
    }
    try:
        r = requests.post(url, json=payload, timeout=30)
        r.raise_for_status()
    except Exception as e:
        print(f"[telegram] erro ao enviar: {e}")

def fetch_json(url, params=None, headers=None, timeout=30):
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[fetch_json] erro em {url}: {e}")
        return None

# ---------- Dados ----------

def get_dxy_from_fred(api_key: str):
    try:
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = dict(series_id="DTWEXBGS", api_key=api_key, file_type="json", sort_order="desc", limit=1)
        j = fetch_json(url, params=params)
        if j and j.get("observations"):
            o = j["observations"][0]
            return {"series_id": "DTWEXBGS", "last": {"date": o["date"], "value": float(o["value"]) if o["value"] != "." else None}}
    except Exception as e:
        print(f"[get_dxy_from_fred] {e}")
    return None

def get_world_reserves_worldbank():
    base = "https://api.worldbank.org/v2/country/WLD/indicator"
    out = {}
    try:
        j = fetch_json(f"{base}/FI.RES.TOTL.CD?format=json")
        k = fetch_json(f"{base}/FI.RES.XGLD.CD?format=json")
        def last_val(arr):
            if not arr or len(arr) < 2 or not isinstance(arr[1], list):
                return None
            for row in arr[1]:
                if row and row.get("value") is not None:
                    return {"date": row.get("date"), "value": row.get("value")}
            return None
        out["total_reserves"]  = last_val(j)
        out["gold_reserves"]   = last_val(k)
    except Exception as e:
        print(f"[worldbank] {e}")
    return out if out else None

def get_market_quotes(alpha_key: str, goldapi_key: str):
    headers = {}
    if goldapi_key:
        headers["x-access-token"] = goldapi_key

    out = {"xauusd_spot": None, "gc_futures": None, "gld": None, "iau": None}

    # spot via GoldAPI -> fallback Alpha Vantage
    try:
        if goldapi_key:
            j = fetch_json("https://www.goldapi.io/api/XAU/USD", headers=headers)
            if j and isinstance(j, dict) and j.get("price"):
                out["xauusd_spot"] = float(j["price"])
    except Exception as e:
        print(f"[goldapi spot] {e}")

    if out["xauusd_spot"] is None and alpha_key:
        try:
            url = "https://www.alphavantage.co/query"
            params = {"function": "CURRENCY_EXCHANGE_RATE", "from_currency": "XAU", "to_currency": "USD", "apikey": alpha_key}
            j = fetch_json(url, params=params)
            rate = (j or {}).get("Realtime Currency Exchange Rate", {}).get("5. Exchange Rate")
            out["xauusd_spot"] = float(rate) if rate else None
        except Exception as e:
            print(f"[alphavantage spot] {e}")

    # Yahoo CSV (sem chave) — último fechamento
    def yahoo_last_close_csv(ticker):
        try:
            url = f"https://query1.finance.yahoo.com/v7/finance/download/{ticker}"
            now = int(time.time())
            week = now - 7*24*3600
            params = dict(period1=week, period2=now, interval="1d", events="history", includeAdjustedClose="true")
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            lines = [L for L in r.text.splitlines() if L and not L.startswith("Date,") and ",,," not in L]
            if not lines:
                return None
            last = lines[-1].split(",")
            return float(last[4]) if len(last) >= 5 and last[4] not in ("", "null") else None
        except Exception as e:
            print(f"[yahoo {ticker}] {e}")
            return None

    out["gc_futures"] = yahoo_last_close_csv("GC=F")
    out["gld"]        = yahoo_last_close_csv("GLD")
    out["iau"]        = yahoo_last_close_csv("IAU")

    return out

def get_cftc_cot_gold():
    return None  # manter como placeholder até ligar a fonte real

# ---------- Narrativa sem IA ----------

def build_rule_based_summary(data: dict) -> str:
    today = dt.datetime.utcnow().date().strftime("%d/%m/%Y")
    lines = [f"*Resumo automático — Ouro ({today})*"]

    mq = data.get("market_quotes") or {}
    spot = mq.get("xauusd_spot")
    gc   = mq.get("gc_futures")
    gld  = mq.get("gld")
    iau  = mq.get("iau")

    bloco_mercado = []
    if spot is not None: bloco_mercado.append(f"- *XAUUSD (spot)*: **{spot:,.2f}** USD/oz")
    if gc   is not None: bloco_mercado.append(f"- *Futuro COMEX (GC=F)*: **{gc:,.2f}**")
    if gld  is not None: bloco_mercado.append(f"- *ETF GLD*: **{gld:,.2f}**")
    if iau  is not None: bloco_mercado.append(f"- *ETF IAU*: **{iau:,.2f}**")
    if bloco_mercado:
        lines.append("\n*1) Mercado / Cotações*\n" + "\n".join(bloco_mercado))

    dxy = data.get("fred_dxy")
    if dxy and dxy.get("last", {}).get("value") is not None:
        v = dxy["last"]["value"]
        d = dxy["last"].get("date")
        lines.append(f"\n*2) Dólar (DXY)*\n- Índice amplo (DTWEXBGS): **{v:,.2f}** (último: {d}). DXY forte tende a pressionar metais.")

    wb = data.get("central_banks_reserves_world") or {}
    tot, gold = wb.get("total_reserves"), wb.get("gold_reserves")
    if tot or gold:
        lines.append("\n*3) Reservas de Bancos Centrais*")
        if tot and tot.get("value") is not None:
            lines.append(f"- *Reservas internacionais totais (WLD)*: **${tot['value']:,.0f}** (ano: {tot.get('date')}).")
        else:
            lines.append("- Reservas totais: *sem dados recentes públicos*.")
        if gold and gold.get("value") is not None:
            lines.append(f"- *Reservas em ouro (WLD)*: **${gold['value']:,.0f}** (ano: {gold.get('date')}).")
        else:
            lines.append("- Reservas em ouro: *sem dados recentes públicos*.")

    cot = data.get("cftc_cot_gold")
    if cot:
        lines.append("\n*4) Posição CFTC (Ouro)*\n- Indicadores de especuladores/hedgers disponíveis.")
    else:
        lines.append("\n*4) Posição CFTC (Ouro)*\n- Sem dados disponíveis no momento (fonte pública inconsistente).")

    sintese = []
    if dxy and dxy.get("last", {}).get("value") is not None:
        dxy_v = dxy["last"]["value"]
        bias = "neutro"
        if dxy_v >= 105: bias = "pressão baixista para metais"
        elif dxy_v <= 100: bias = "ambiente relativamente favorável para metais"
        sintese.append(f"- Com o DXY em **{dxy_v:,.2f}**, o viés é *{bias}* para o ouro.")
    if spot is not None and gc is not None:
        diff = spot - gc
        if abs(diff) > 10:
            sentido = "acima" if diff > 0 else "abaixo"
            sintese.append(f"- *Spot* está **{abs(diff):,.2f}** USD/oz {sentido} do contrato futuro (GC=F).")
    if not sintese:
        sintese.append("- As métricas públicas disponíveis hoje foram limitadas; acompanhe preços spot, DXY e fluxos de ETFs.")

    lines.append("\n*5) Síntese*\n" + "\n".join(sintese))
    return "\n".join(lines)

# ---------- Contador sequencial ----------

COUNTER_FILE = ".sent/counter_golddaily.txt"

def load_and_bump_counter() -> int:
    """
    Lê o contador em .sent/counter_golddaily.txt, incrementa +1 e salva.
    Retorna o valor já incrementado (ex.: primeiro run -> 1).
    """
    try:
        os.makedirs(".sent", exist_ok=True)
        n = 0
        if os.path.exists(COUNTER_FILE):
            with open(COUNTER_FILE, "r", encoding="utf-8") as f:
                raw = f.read().strip()
                n = int(raw) if raw.isdigit() else 0
        n += 1
        with open(COUNTER_FILE, "w", encoding="utf-8") as f:
            f.write(str(n))
        return n
    except Exception as e:
        print(f"[counter] erro: {e}")
        # fallback: 1 (não persiste se falhar)
        return 1

# ---------- Main ----------

def main():
    TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TG_CHAT  = os.getenv("TELEGRAM_CHAT_ID_METALS")
    FRED_KEY = os.getenv("FRED_API_KEY")
    GOLDAPI  = os.getenv("GOLDAPI_KEY")
    ALPHAKEY = os.getenv("ALPHA_VANTAGE_API_KEY")

    data = {
        "market_quotes": get_market_quotes(ALPHAKEY, GOLDAPI),
        "central_banks_reserves_world": get_world_reserves_worldbank(),
        "fred_dxy": get_dxy_from_fred(FRED_KEY) if FRED_KEY else None,
        "cftc_cot_gold": get_cftc_cot_gold(),
    }

    # contador sequencial persistente
    report_no = load_and_bump_counter()

    run_date = dt.datetime.utcnow().strftime("%d de %B de %Y")

    # Título no formato solicitado
    title = f"📊 *Dados de Mercado — Ouro (XAU/USD) — {run_date} — Diário — Nº {report_no}*"

    # 1) dados (json) — opcional para debug
    pretty = json.dumps(data, ensure_ascii=False, indent=2)
    tg_send(TG_TOKEN, TG_CHAT, f"{title}\n\n```\n{pretty}\n```")

    # 2) resumo regra-baseada
    summary = build_rule_based_summary(data)
    tg_send(TG_TOKEN, TG_CHAT, summary)

if __name__ == "__main__":
    main()
