#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Relatório Diário — OURO (XAU)

- Coleta dados de múltiplas fontes públicas:
  * Yahoo Finance (cotações spot/futuros, ETFs)
  * FRED (DXY)
  * World Bank (reservas internacionais e reservas de ouro, série mundial)
  * Nasdaq Data Link (Quandl) — opcional para COT (se houver chave)
- Constrói um JSON de métricas
- Gera a interpretação executiva via IA (Groq → DeepSeek → OpenAI)
- Envia no Telegram
- Marca o dia em .sent/ para não duplicar

Variáveis de ambiente usadas (definidas pelo GitHub Actions):
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID_METALS
  FRED_API_KEY, NASDAQ_DATA_LINK_API_KEY, GOLDAPI_KEY, ALPHA_VANTAGE_API_KEY
  METALS_DEV_API, METAL_PRICE_API, SEC_USER_AGENT, AISC_TICKERS
  GROQ_API_KEY, DEEPSEEK_API_KEY, OPENAI_API_KEY
"""

import os, json, time, math, html, textwrap, datetime as dt
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List

import requests
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta

# ---------------------- Config / Env ----------------------

BRT = timezone(timedelta(hours=-3), name="BRT")

TELEGRAM_BOT_TOKEN      = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID        = os.getenv("TELEGRAM_CHAT_ID_METALS", "")

FRED_API_KEY            = os.getenv("FRED_API_KEY", "")
NASDAQ_DATA_LINK_API_KEY= os.getenv("NASDAQ_DATA_LINK_API_KEY", "")

# IA (fallback em cascata)
GROQ_API_KEY            = os.getenv("GROQ_API_KEY", "")
DEEPSEEK_API_KEY        = os.getenv("DEEPSEEK_API_KEY", "")
OPENAI_API_KEY          = os.getenv("OPENAI_API_KEY", "")

SEC_USER_AGENT          = os.getenv("SEC_USER_AGENT", "Mozilla/5.0 (compatible; HubRelatorios/1.0)")
AISC_TICKERS            = os.getenv("AISC_TICKERS", "NEM,GOLD")

SENT_DIR                = ".sent"

# ---------------------- Utilidades ----------------------

def today_brt_str() -> str:
    meses = ["janeiro","fevereiro","março","abril","maio","junho","julho","agosto","setembro","outubro","novembro","dezembro"]
    now = datetime.now(BRT)
    return f"{now.day} de {meses[now.month-1]} de {now.year}"

def ensure_sent_dir():
    os.makedirs(SENT_DIR, exist_ok=True)

def is_already_sent(kind: str, date_str: Optional[str] = None) -> bool:
    ensure_sent_dir()
    if not date_str:
        date_str = datetime.now(BRT).strftime("%Y-%m-%d")
    path = os.path.join(SENT_DIR, f"done-{kind}-{date_str}")
    return os.path.exists(path)

def mark_sent(kind: str, date_str: Optional[str] = None):
    ensure_sent_dir()
    if not date_str:
        date_str = datetime.now(BRT).strftime("%Y-%m-%d")
    path = os.path.join(SENT_DIR, f"done-{kind}-{date_str}")
    open(path, "w", encoding="utf-8").write("ok")

def chunk_message(text: str, limit: int = 3900) -> List[str]:
    """Divide mensagens longas em blocos para o Telegram (HTML)."""
    parts: List[str] = []
    for block in text.split("\n\n"):
        b = block.strip()
        if not b:
            if parts and not parts[-1].endswith("\n\n"):
                parts[-1] += "\n\n"
            continue
        if len(b) <= limit:
            if not parts:
                parts.append(b)
            elif len(parts[-1]) + 2 + len(b) <= limit:
                parts[-1] += "\n\n" + b
            else:
                parts.append(b)
        else:
            acc = ""
            for line in b.splitlines():
                if len(acc) + len(line) + 1 <= limit:
                    acc += (("\n" if acc else "") + line)
                else:
                    if acc:
                        parts.append(acc)
                    acc = line
            if acc:
                parts.append(acc)
    return parts if parts else ["(vazio)"]

def send_telegram(text: str, parse_mode: str = "HTML"):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("[telegram] skip: missing token/chat_id")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
        "parse_mode": parse_mode
    }
    r = requests.post(url, data=data, timeout=60)
    r.raise_for_status()

def ping_telegram_ok():
    send_telegram("✅ Conexão OK: Gold Daily inicializando...")

# ---------------------- Coletas ----------------------

def fetch_yahoo_price(ticker: str) -> Optional[float]:
    """Último preço de fechamento (ajustado) via Yahoo CSV endpoint."""
    try:
        end = int(time.time())
        start = end - 60*60*24*7  # última semana
        url = (
            f"https://query1.finance.yahoo.com/v7/finance/download/{ticker}"
            f"?period1={start}&period2={end}&interval=1d&events=history&includeAdjustedClose=true"
        )
        r = requests.get(url, timeout=30, headers={"User-Agent": SEC_USER_AGENT})
        r.raise_for_status()
        df = pd.read_csv(pd.compat.StringIO(r.text))
        if "Adj Close" in df.columns and not df["Adj Close"].dropna().empty:
            return float(df["Adj Close"].dropna().iloc[-1])
        if "Close" in df.columns and not df["Close"].dropna().empty:
            return float(df["Close"].dropna().iloc[-1])
    except Exception as e:
        print(f"[yahoo] {ticker} error: {e}")
    return None

def fetch_fred_series(series_id: str, api_key: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Busca último valor de uma série do FRED."""
    try:
        params = {"series_id": series_id, "file_type": "json"}
        if api_key:
            params["api_key"] = api_key
        url = "https://api.stlouisfed.org/fred/series/observations"
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        obs = data.get("observations", [])
        if obs:
            last = [o for o in obs if o.get("value") not in (".","")]  # remove missing
            if last:
                last = last[-1]
                return {
                    "series_id": series_id,
                    "last": {
                        "date": last.get("date"),
                        "value": float(last.get("value"))
                    }
                }
    except Exception as e:
        print(f"[fred] {series_id} error: {e}")
    return None

def fetch_worldbank_reserves() -> Optional[Dict[str, Any]]:
    """Total de reservas e reservas em ouro - World Bank (WLD)."""
    def get_indicator(ind):
        try:
            url = f"https://api.worldbank.org/v2/country/WLD/indicator/{ind}?format=json"
            r = requests.get(url, timeout=30)
            r.raise_for_status()
            arr = r.json()
            if isinstance(arr, list) and len(arr) == 2:
                series = arr[1]
                vals = [x for x in series if x.get("value") is not None]
                if vals:
                    last = vals[0]  # já vem ordenado desc
                    return {"indicator": ind, "date": last["date"], "value": float(last["value"])}
        except Exception as e:
            print(f"[worldbank] {ind} error: {e}")
        return None

    total = get_indicator("FI.RES.TOTL.CD")
    gold  = get_indicator("FI.RES.XGLD.CD")
    if total or gold:
        return {"total_reserves": total, "gold_reserves": gold}
    return None

def fetch_quandl_cot_gold(api_key: Optional[str]) -> Optional[Dict[str, Any]]:
    """
    Tenta buscar um COT de ouro via Nasdaq Data Link (Quandl).
    Obs: muitos conjuntos CFTC são pagos; mantemos tentativa graciosa.
    """
    if not api_key:
        return None
    try:
        # Exemplo: dataset público pode não existir; deixamos tentativa e fallback graceful.
        # Se você tiver um código específico que funciona na sua conta, troque abaixo:
        code = "CFTC/GC_F_ALL"  # placeholder; pode não existir em conta free
        url = f"https://data.nasdaq.com/api/v3/datasets/{code}.json?api_key={api_key}&limit=1"
        r = requests.get(url, timeout=20)
        if r.status_code == 200:
            j = r.json()
            # adaptador simples
            return {"dataset": code, "raw": j.get("dataset", {}).get("data", [])}
        else:
            print(f"[nasdaq] status {r.status_code}: {r.text[:120]}")
    except Exception as e:
        print(f"[nasdaq] error: {e}")
    return None

# ---------------------- IA (fallback) ----------------------

def call_groq(api_key: str, model: str, prompt: str) -> str:
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type":"application/json"}
    payload = {
        "model": model,
        "messages":[
            {"role":"system","content":"Você é um analista macro/commodities. Responda em pt-BR, claro e executivo."},
            {"role":"user","content": prompt}
        ],
        "temperature": 0.35,
        "max_tokens": 1400
    }
    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=60)
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"].strip()

def call_deepseek(api_key: str, model: str, prompt: str) -> str:
    url = "https://api.deepseek.com/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type":"application/json"}
    payload = {
        "model": model,
        "messages":[
            {"role":"system","content":"Você é um analista macro/commodities. Responda em pt-BR, claro e executivo."},
            {"role":"user","content": prompt}
        ],
        "temperature": 0.35,
        "max_tokens": 1400
    }
    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=60)
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"].strip()

def call_openai(api_key: str, model: str, prompt: str) -> str:
    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type":"application/json"}
    payload = {
        "model": model,
        "messages":[
            {"role":"system","content":"Você é um analista macro/commodities. Responda em pt-BR, claro e executivo."},
            {"role":"user","content": prompt}
        ],
        "temperature": 0.35,
        "max_tokens": 1400
    }
    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=60)
    r.raise_for_status()
    return r.json()["choices"][0]["message"]["content"].strip()

def llm_generate_with_fallback(prompt: str) -> Optional[str]:
    # 1) Groq
    if GROQ_API_KEY:
        try:
            return call_groq(GROQ_API_KEY, "llama-3.1-70b-versatile", prompt)
        except Exception as e:
            print(f"[LLM] Groq falhou: {e}")
    # 2) DeepSeek
    if DEEPSEEK_API_KEY:
        try:
            return call_deepseek(DEEPSEEK_API_KEY, "deepseek-chat", prompt)
        except Exception as e:
            print(f"[LLM] DeepSeek falhou: {e}")
    # 3) OpenAI (opcional)
    if OPENAI_API_KEY:
        try:
            return call_openai(OPENAI_API_KEY, "gpt-4o-mini", prompt)
        except Exception as e:
            print(f"[LLM] OpenAI falhou: {e}")
    return None

# ---------------------- Montagem do relatório ----------------------

def build_prompt(data_str: str, numero: int, metrics: Dict[str, Any]) -> str:
    header = f"Dados — Ouro — {data_str} — Diário — Nº {numero}"
    rules = (
        "Você é um analista macro e commodities. Escreva em português do Brasil, claro e institucional.\n"
        "TÍTULO (linha única):\n" + header + "\n\n"
        "REGRAS:\n"
        "- Use os dados do JSON exatamente como vierem; se algo estiver ausente, descreva qualitativamente.\n"
        "- Sem links; inclua a data completa no primeiro parágrafo.\n"
        "- Estrutura fixa (na ordem):\n"
        "  1) Fluxos em ETFs de Ouro (GLD, IAU etc.)\n"
        "  2) Posição Líquida em Futuros (CFTC)\n"
        "  3) Reservas de Bancos Centrais\n"
        "  4) Fluxos de Mineradoras & Bancos (produção, hedge, OTC)\n"
        "  5) Whale Ratio Institucional vs. Varejo (participação relativa)\n"
        "  6) Drivers Macro (taxa real, DXY, política monetária, geopolítica)\n"
        "  7) Custos de Produção & Oferta Física (AISC, supply)\n"
        "  8) Estrutura a Termo (contango/backwardation LBMA/COMEX)\n"
        "  9) Correlações Cruzadas (DXY, S&P500)\n"
        "  10) Interpretação Executiva & Conclusão — 5–8 bullets + síntese\n\n"
        "DADOS (JSON):\n"
    )
    return rules + json.dumps(metrics, ensure_ascii=False, indent=2)

def make_title(data_str: str, numero: int) -> str:
    return f"📊 <b>Dados — Ouro — {data_str} — Diário — Nº {numero}</b>"

# ---------------------- Main ----------------------

def main():
    # evita duplicidade
    if is_already_sent("gold-daily"):
        print("[sent] já enviado hoje; saindo.")
        return

    ping_telegram_ok()

    data_str = today_brt_str()
    numero   = int(time.time()) % 1_000_000  # id simples

    # ---- Coletas ----
    metrics: Dict[str, Any] = {}

    # 1) Cotações de mercado (Yahoo)
    market = {}
    market["xauusd_spot"] = fetch_yahoo_price("XAUUSD=X")     # spot USD/oz
    market["gc_futures"]  = fetch_yahoo_price("GC=F")          # futuro COMEX contínuo
    market["gld"]         = fetch_yahoo_price("GLD")
    market["iau"]         = fetch_yahoo_price("IAU")
    metrics["market_quotes"] = market

    # 2) Reservas WB
    metrics["central_banks_reserves_world"] = fetch_worldbank_reserves()

    # 3) FRED DXY
    metrics["fred_dxy"] = fetch_fred_series("DTWEXBGS", api_key=FRED_API_KEY)

    # 4) (Opcional) Nasdaq Data Link COT (placeholder gracioso)
    cot = fetch_quandl_cot_gold(NASDAQ_DATA_LINK_API_KEY)
    metrics["cftc_cot_gold"] = cot

    # ---- IA ----
    prompt = build_prompt(data_str, numero, metrics)
    llm_text = llm_generate_with_fallback(prompt)

    # ---- Telegram: JSON de métricas ----
    title = make_title(data_str, numero)
    body_json = html.escape(json.dumps(metrics, ensure_ascii=False, indent=2), quote=False)
    msg_json = f"{title}\n\n<code>{body_json}</code>"
    for part in chunk_message(msg_json):
        send_telegram(part)

    # ---- Telegram: Interpretação ----
    if llm_text:
        body = html.escape(llm_text, quote=False)
        for part in chunk_message(body):
            send_telegram(part)
    else:
        send_telegram("⚠️ Não foi possível gerar a interpretação automática agora.\nUse as métricas acima como base.\n\n(Falha nos provedores de IA)")

    # marca como enviado
    mark_sent("gold-daily")

if __name__ == "__main__":
    main()