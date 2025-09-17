# -*- coding: utf-8 -*-
"""
Relatório Diário — Ouro (XAU/USD)
- Consulta múltiplas fontes públicas (Yahoo/FRED/WorldBank) para 10 tópicos.
- Gera resumo via IA (fallback: GROQ -> OpenAI -> DeepSeek).
- Evita duplicados (.sent/) e mantém contador persistente no título.
"""

import os
import json
import time
import math
import pathlib
from datetime import datetime, timezone, timedelta
import locale
from typing import Optional, Dict, Any

import requests
import pandas as pd
import yfinance as yf
from tenacity import retry, wait_exponential, stop_after_attempt

# ========== Locale pt-BR ==========
try:
    locale.setlocale(locale.LC_TIME, "pt_BR.UTF-8")
except locale.Error:
    try:
        locale.setlocale(locale.LC_TIME, "pt_BR")
    except locale.Error:
        pass

# ========== ENV ==========
FRED_API_KEY = os.getenv("FRED_API_KEY", "").strip()
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID_METALS = os.getenv("TELEGRAM_CHAT_ID_METALS", "").strip()

# IAs (fallback)
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "").strip()

# ========== Constantes ==========
SENT_ROOT = ".sent"
COUNTER_FILE = os.path.join(SENT_ROOT, "gold-daily-counter.txt")
SENT_FLAG_FILE = os.path.join(SENT_ROOT, f"done-gold-daily-{datetime.now(timezone.utc).strftime('%Y-%m-%d')}")
USER_AGENT = os.getenv("SEC_USER_AGENT", "metals-reports/1.0 (+github actions)")
HEADERS = {"User-Agent": USER_AGENT}

# ========== FS / contador ==========
def _ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d and not os.path.isdir(d):
        os.makedirs(d, exist_ok=True)

def _load_counter(path: str) -> int:
    if not os.path.exists(path):
        return 0
    try:
        with open(path, "r", encoding="utf-8") as f:
            return int((f.read() or "0").strip())
    except Exception:
        return 0

def _save_counter(path: str, value: int) -> None:
    _ensure_dir(path)
    with open(path, "w", encoding="utf-8") as f:
        f.write(str(value))

def get_title_with_counter(
    frequency: str = "Diário",
    pair_label: str = "Ouro (XAU/USD)",
    today: datetime | None = None,
    counter_file: str = COUNTER_FILE,
) -> str:
    dt = today or datetime.utcnow()
    date_str = dt.strftime("%d de %B de %Y")
    current = _load_counter(counter_file)
    nxt = current + 1
    _save_counter(counter_file, nxt)
    return f"📊 Dados de Mercado — {pair_label} — {date_str} — {frequency} — Nº {nxt}"

# ========== HTTP helpers ==========
@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(3))
def _get_json(url: str, params=None, headers=None, timeout=30):
    resp = requests.get(url, params=params, headers=headers or HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.json()

# ========== 1) Mercado / Cotações ==========
def fetch_yahoo_quotes() -> dict:
    out = {"xauusd_spot": None, "gc_futures": None, "gld": None, "iau": None}
    try:
        spot = yf.Ticker("XAUUSD=X").history(period="5d", interval="1d")
        if not spot.empty:
            out["xauusd_spot"] = round(float(spot["Close"].dropna().iloc[-1]), 2)
    except Exception as e:
        print("[WARN] Yahoo spot:", e)

    try:
        fut = yf.Ticker("GC=F").history(period="5d", interval="1d")
        if not fut.empty:
            out["gc_futures"] = round(float(fut["Close"].dropna().iloc[-1]), 2)
    except Exception as e:
        print("[WARN] Yahoo GC=F:", e)

    for etf_ticker, key in [("GLD", "gld"), ("IAU", "iau")]:
        try:
            etf = yf.Ticker(etf_ticker).history(period="5d", interval="1d")
            if not etf.empty:
                out[key] = round(float(etf["Close"].dropna().iloc[-1]), 2)
        except Exception as e:
            print(f"[WARN] Yahoo {etf_ticker}:", e)
    return out

# ========== 2) Dólar (DXY) — FRED ==========
def fetch_fred_series_last(series_id: str) -> Optional[dict]:
    if not FRED_API_KEY:
        return None
    try:
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": series_id,
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "sort_order": "desc",
            "limit": 1,
        }
        j = _get_json(url, params=params)
        obs = (j or {}).get("observations") or []
        if obs:
            o = obs[0]
            v = None if o.get("value") in (".", None) else float(o["value"])
            return {"series_id": series_id, "last": {"date": o.get("date"), "value": v}}
    except Exception as e:
        print(f"[FRED] {series_id}:", e)
    return None

def fetch_fred_dxy() -> dict | None:
    return fetch_fred_series_last("DTWEXBGS")

# ========== 6) Taxa de Juros Real (10y) & Nominal (10y) ==========
def fetch_real_yield_10y() -> dict | None:
    return fetch_fred_series_last("DFII10")  # 10-Year Treasury Inflation-Indexed Security, Constant Maturity

def fetch_nominal_yield_10y() -> dict | None:
    return fetch_fred_series_last("DGS10")   # 10-Year Treasury Constant Maturity

# ========== 7) Volatilidade (GVZ) ==========
def fetch_gvz_yahoo() -> dict | None:
    try:
        df = yf.Ticker("^GVZ").history(period="10d", interval="1d")
        if not df.empty:
            v = float(df["Close"].dropna().iloc[-1])
            d = df.index[-1].date().isoformat()
            return {"symbol": "^GVZ", "last": {"date": d, "value": v}}
    except Exception as e:
        print("[WARN] ^GVZ:", e)
    return None

# ========== 3) Reservas (World Bank) ==========
def fetch_world_reserves() -> dict | None:
    try:
        def last_val(arr):
            if not isinstance(arr, list) or len(arr) < 2 or not isinstance(arr[1], list):
                return None
            for row in arr[1]:
                if row and row.get("value") is not None:
                    return {"date": row.get("date"), "value": row.get("value")}
            return None

        total = _get_json("https://api.worldbank.org/v2/country/WLD/indicator/FI.RES.TOTL.CD",
                          params={"format": "json", "per_page": "5"})
        gold  = _get_json("https://api.worldbank.org/v2/country/WLD/indicator/FI.RES.XGLD.CD",
                          params={"format": "json", "per_page": "5"})
        return {
            "total_reserves": last_val(total),
            "gold_reserves": last_val(gold) if gold else None
        }
    except Exception as e:
        print("[WARN] WorldBank:", e)
        return None

# ========== 5) Fluxo em ETFs — proxy AUM (Yahoo info) ==========
def fetch_etf_aum_yahoo() -> dict:
    """AUM de GLD/IAU como proxy para fluxo. (shares outstanding diárias não têm API pública estável)."""
    out = {}
    for t in ("GLD", "IAU"):
        try:
            info = yf.Ticker(t).get_info()
            # Yahoo às vezes retorna 'totalAssets' em USD; se não vier, deixa None
            out[t] = {"totalAssets": info.get("totalAssets")}
        except Exception as e:
            print(f"[WARN] AUM {t}:", e)
            out[t] = {"totalAssets": None}
    return out

# ========== 8) Estrutura a Termo (COMEX) ==========
MONTH_CODES = "FGHJKMNQUVXZ"  # jan-dez

def _nearest_two_gc_symbols(today=None):
    """Tenta construir 2 vencimentos (mês atual/seguinte) no formato Yahoo: GCZ25.CMX"""
    d = today or datetime.utcnow()
    m = d.month
    y = d.year % 100  # YY
    # dois próximos meses com código
    months = []
    for k in range(2):
        mi = ((m - 1 + k) % 12) + 1
        yi = y + ((m - 1 + k) // 12)
        code = MONTH_CODES[mi - 1]
        sym = f"GC{code}{yi:02d}.CMX"
        months.append(sym)
    return months

def fetch_term_structure() -> dict:
    """Best-effort: preço de 2 vencimentos (se disponível) + spread; fallback: spot vs GC=F."""
    result = {"specific_contracts": None, "spot_vs_front": None}
    # 2 vencimentos específicos
    try:
        syms = _nearest_two_gc_symbols()
        vals = {}
        for s in syms:
            df = yf.Ticker(s).history(period="5d", interval="1d")
            if not df.empty:
                vals[s] = float(df["Close"].dropna().iloc[-1])
        if len(vals) == 2:
            keys = list(vals.keys())
            result["specific_contracts"] = {
                "contracts": vals,
                "spread": vals[keys[1]] - vals[keys[0]]
            }
    except Exception as e:
        print("[WARN] term structure:", e)

    # Fallback: spot vs front
    try:
        spot = yf.Ticker("XAUUSD=X").history(period="5d", interval="1d")
        fut  = yf.Ticker("GC=F").history(period="5d", interval="1d")
        if not spot.empty and not fut.empty:
            s = float(spot["Close"].dropna().iloc[-1])
            f = float(fut["Close"].dropna().iloc[-1])
            result["spot_vs_front"] = {"spot": s, "front": f, "spread": f - s}
    except Exception as e:
        print("[WARN] spot vs front:", e)
    return result

# ========== 9) Correlação (30 dias) ==========
def fetch_correlations_30d() -> dict:
    """Correlação 30d entre XAUUSD e ^DXY / ^GSPC."""
    res = {"xau_dxy_30d": None, "xau_spx_30d": None}
    try:
        tickers = ["XAUUSD=X", "^DXY", "^GSPC"]
        df = yf.download(tickers, period="3mo", interval="1d", progress=False)["Adj Close"]
        df = df.dropna(how="all").ffill().dropna()
        ret = df.pct_change().dropna()
        if ret.shape[0] >= 30:
            corr = ret.tail(30).corr()
            if "XAUUSD=X" in corr and "^DXY" in corr:
                res["xau_dxy_30d"] = float(corr.loc["XAUUSD=X", "^DXY"])
            if "XAUUSD=X" in corr and "^GSPC" in corr:
                res["xau_spx_30d"] = float(corr.loc["XAUUSD=X", "^GSPC"])
    except Exception as e:
        print("[WARN] correlations:", e)
    return res

# ========== 4) COT (placeholder) ==========
def fetch_cftc_cot_gold() -> dict | None:
    # Sem feed gratuito diário estável; manter None para não travar.
    return None

# ========== Prompt IA (10 tópicos) ==========
def build_summary_prompt_10topics(date_label: str, M: dict) -> str:
    """
    Gera instruções para a IA cobrir 10 tópicos fixos.
    'M' é o dicionário de métricas consolidado.
    """
    p = []
    p.append(f"Você é um analista sênior de commodities. Escreva em pt-BR, objetivo, para diretoria.")
    p.append(f"Data: {date_label}. Tema: OURO (XAU/USD).")
    p.append("Use exatamente os dados do JSON quando existirem; se um campo estiver ausente, descreva qualitativamente sem inventar números.")
    p.append("Estruture em 10 tópicos, nessa ordem e com estes títulos fixos:")
    p.append("1) Mercado / Cotações")
    p.append("2) Dólar (DXY)")
    p.append("3) Reservas Internacionais")
    p.append("4) Posição COT (CFTC)")
    p.append("5) Fluxo de ETFs de Ouro")
    p.append("6) Taxa de Juros Real (US 10Y - CPI)")
    p.append("7) Volatilidade (Índice GVZ)")
    p.append("8) Mineração e Produção")
    p.append("9) Correlação com Outros Ativos")
    p.append("10) Síntese / Viés do Dia")
    p.append("")
    p.append("JSON de métricas:")
    p.append(json.dumps(M, ensure_ascii=False, indent=2))
    p.append("")
    p.append("Diretrizes:")
    p.append("- Não use links. Não invente valores ausentes.")
    p.append("- Onde houver apenas AUM de ETFs, trate como proxy de fluxo.")
    p.append("- Para correlação, descreva o sinal e magnitude aproximada (ex.: leve, moderada, forte) com base no valor de correlação de 30 dias.")
    return "\n".join(p)

# ========== IA: chamadas ==========
@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(2))
def _post_json(url, headers, payload, timeout=60):
    r = requests.post(url, headers=headers, json=payload, timeout=timeout)
    r.raise_for_status()
    return r.json()

def _chat_extract(rj: dict) -> str:
    return (rj.get("choices") or [{}])[0].get("message", {}).get("content", "").strip()

def generate_via_groq(prompt: str) -> str:
    if not GROQ_API_KEY:
        raise RuntimeError("Sem GROQ_API_KEY")
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    payload = {"model": "llama3-8b-8192", "messages": [{"role": "user", "content": prompt}], "temperature": 0.3, "max_tokens": 900}
    return _chat_extract(_post_json(url, headers, payload))

def generate_via_openai(prompt: str) -> str:
    if not OPENAI_API_KEY:
        raise RuntimeError("Sem OPENAI_API_KEY")
    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {"model": "gpt-4o-mini", "messages": [{"role": "user", "content": prompt}], "temperature": 0.3, "max_tokens": 900}
    return _chat_extract(_post_json(url, headers, payload))

def generate_via_deepseek(prompt: str) -> str:
    if not DEEPSEEK_API_KEY:
        raise RuntimeError("Sem DEEPSEEK_API_KEY")
    url = "https://api.deepseek.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"}
    payload = {"model": "deepseek-chat", "messages": [{"role": "user", "content": prompt}], "temperature": 0.3, "max_tokens": 900}
    return _chat_extract(_post_json(url, headers, payload))

def generate_summary_with_fallback(prompt: str) -> str:
    for fn in (generate_via_groq, generate_via_openai, generate_via_deepseek):
        try:
            return fn(prompt)
        except Exception as e:
            print(f"[IA] provider falhou: {e}")
    return "Não foi possível gerar a interpretação automática agora. Use as métricas acima como base."

# ========== Telegram ==========
def send_telegram(text: str, parse_mode: str | None = None):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID_METALS:
        print("[WARN] Telegram env vars ausentes.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID_METALS, "text": text, "disable_web_page_preview": True}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    r = requests.post(url, json=payload, timeout=60)
    try:
        r.raise_for_status()
    except Exception as e:
        print("[WARN] Telegram:", e, r.text[:200])

# ========== Execução ==========
def main():
    _ensure_dir(SENT_ROOT)
    if os.path.exists(SENT_FLAG_FILE):
        print("[INFO] Já enviado hoje; encerrando.")
        return

    # Título com contador
    title = get_title_with_counter(
        frequency="Diário",
        pair_label="Ouro (XAU/USD)",
        counter_file=COUNTER_FILE,
    )

    # === Coletas ===
    market   = fetch_yahoo_quotes()                 # 1
    dxy      = fetch_fred_dxy()                     # 2
    reserves = fetch_world_reserves()               # 3
    cot      = fetch_cftc_cot_gold()                # 4 (placeholder)
    etf_aum  = fetch_etf_aum_yahoo()                # 5 (proxy)
    real10   = fetch_real_yield_10y()               # 6
    nom10    = fetch_nominal_yield_10y()            # 6 (apoio)
    gvz      = fetch_gvz_yahoo()                    # 7
    term     = fetch_term_structure()               # 8
    corrs    = fetch_correlations_30d()             # 9

    # JSON de métricas consolidado (para a IA)
    metrics = {
        "1_market_quotes": market,
        "2_dxy": dxy,
        "3_reserves": reserves,
        "4_cftc_cot_gold": cot,
        "5_etf_aum_proxy": etf_aum,
        "6_real_yield_10y": real10,
        "6b_nominal_yield_10y": nom10,
        "7_gvz": gvz,
        "8_term_structure": term,
        "9_correlations_30d": corrs,
    }

    # Envia cabeçalho + JSON (útil para auditoria)
    json_block = "```\n" + json.dumps(metrics, ensure_ascii=False, indent=2) + "\n```"
    send_telegram(f"{title}\n\n{json_block}", parse_mode="Markdown")

    # Monta prompt 10 tópicos e chama IA
    dtlabel = datetime.utcnow().strftime("%d/%m/%Y")
    prompt = build_summary_prompt_10topics(dtlabel, metrics)
    resumo = generate_summary_with_fallback(prompt)
    send_telegram(resumo)

    # Marca como enviado
    pathlib.Path(SENT_FLAG_FILE).touch()
    print("[OK] Relatório enviado.")

if __name__ == "__main__":
    main()