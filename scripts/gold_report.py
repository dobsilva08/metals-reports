#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
scripts/gold_report.py
Relatório diário de OURO (XAU) com coleta de métricas, interpretação executiva
via LLM (fallback Groq -> OpenAI -> DeepSeek) e envio para Telegram.

Requer (via GitHub Secrets ou .env local):
- GROQ_API_KEY (obrigatório para 1º provedor)
- OPENAI_API_KEY (opcional; fallback)
- DEEPSEEK_API_KEY (opcional; fallback)
- FRED_API_KEY (opcional)
- NASDAQ_DATA_LINK_API_KEY (opcional)
- GOLDAPl_KEY (opcional, se tiver algum provedor extra)
- TELEGRAM_BOT_TOKEN (obrigatório para envio)
- TELEGRAM_CHAT_ID_METALS (obrigatório para envio)
- SEC_USER_AGENT (opcional; user-agent para HTTP)
- AISC_TICKERS (opcional; CSV p/ mineradoras: ex. "NEM,GOLD")

Obs.: se alguma chave estiver ausente, os respectivos blocos de dados
são pulados e o relatório continua. A interpretação executiva sempre
tenta os provedores em fallback, e se todos falharem, cai no texto de
contingência.
"""

import os
import json
import time
import html
import textwrap
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional

import requests

# ==========================
# Fuso horário e datas BRT
# ==========================
BRT = timezone(timedelta(hours=-3), name="BRT")

def today_brt_str() -> str:
    meses = ["janeiro","fevereiro","março","abril","maio","junho","julho","agosto","setembro","outubro","novembro","dezembro"]
    now = datetime.now(BRT)
    return f"{now.day} de {meses[now.month-1]} de {now.year}"

# ==========================
# HTTP helpers (com retry)
# ==========================
def get_json(url: str, headers: Optional[Dict[str, str]] = None, params: Optional[Dict[str, Any]] = None, timeout: int = 30) -> Optional[dict]:
    tries = 3
    headers = headers or {}
    if "User-Agent" not in headers:
        ua = os.getenv("SEC_USER_AGENT", "Mozilla/5.0 (compatible; Hub-Relatorios/1.0)")
        headers["User-Agent"] = ua
    for _ in range(tries):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            # 429/backoff simples
            if r.status_code in (429, 503):
                time.sleep(1.5)
            else:
                time.sleep(0.7)
        except Exception:
            time.sleep(0.7)
    return None

def _post_json(url, headers, payload, timeout=60) -> Optional[dict]:
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=timeout)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (401, 403, 429):
            return None
        return None
    except Exception:
        return None

# ==========================
# Coletas de dados (métricas)
# ==========================
def fetch_yahoo_quotes(symbols: list[str]) -> Dict[str, Any]:
    """
    Usa endpoint quote do Yahoo (sem chave) para pegar last price, var%, etc.
    """
    base = "https://query1.finance.yahoo.com/v7/finance/quote"
    q = ",".join(symbols)
    data = get_json(base, params={"symbols": q})
    out: Dict[str, Any] = {}
    if data and "quoteResponse" in data and "result" in data["quoteResponse"]:
        for it in data["quoteResponse"]["result"]:
            sym = it.get("symbol")
            out[sym] = {
                "price": it.get("regularMarketPrice"),
                "change": it.get("regularMarketChange"),
                "changePct": it.get("regularMarketChangePercent"),
                "currency": it.get("currency"),
            }
    return out

def fetch_fred_series(series_id: str) -> Optional[Dict[str, Any]]:
    """
    Busca série do FRED (se FRED_API_KEY existir).
    Retorna últimos valores.
    """
    fred_key = os.getenv("FRED_API_KEY")
    if not fred_key:
        return None
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "api_key": fred_key,
        "series_id": series_id,
        "file_type": "json",
        "observation_start": "2010-01-01"
    }
    data = get_json(url, params=params)
    if not data or "observations" not in data:
        return None
    obs = [o for o in data["observations"] if o.get("value") not in (".", None)]
    last = obs[-1] if obs else None
    return {"series_id": series_id, "last": last}

def fetch_worldbank_gold_reserves_world() -> Optional[Dict[str, Any]]:
    """
    World Bank - reservas (total e ouro, agregadas em 'WLD').
    Não precisa de chave.
    """
    total = get_json("https://api.worldbank.org/v2/country/WLD/indicator/FI.RES.TOTL.CD?format=json")
    gold  = get_json("https://api.worldbank.org/v2/country/WLD/indicator/FI.RES.XGLD.CD?format=json")
    if not total or not gold:
        return None
    try:
        tot_series = total[1]
        gold_series = gold[1]
    except Exception:
        return None
    def _last_valid(lst):
        for it in reversed(lst):
            if it.get("value") is not None:
                return {"date": it.get("date"), "value": it.get("value")}
        return None
    return {
        "total_reserves": _last_valid(tot_series),
        "gold_reserves": _last_valid(gold_series)
    }

def fetch_cftc_cot_gold() -> Optional[Dict[str, Any]]:
    """
    Posição líquida especulativa (COT) via Nasdaq Data Link (Quandl).
    Requer NASDAQ_DATA_LINK_API_KEY.
    Dataset exemplo: CFTC/088691_F_L_ALL  (COMEX Gold Futures, Financial Traders)
    """
    api_key = os.getenv("NASDAQ_DATA_LINK_API_KEY")
    if not api_key:
        return None
    dataset = "CFTC/088691_F_L_ALL"
    url = f"https://data.nasdaq.com/api/v3/datasets/{dataset}.json"
    params = {"api_key": api_key, "limit": 10}
    data = get_json(url, params=params)
    if not data or "dataset" not in data:
        return None
    ds = data["dataset"]
    cols = ds.get("column_names", [])
    rows = ds.get("data", [])
    if not rows:
        return None
    last = rows[0]
    row = dict(zip(cols, last))
    # campos comuns: "Net Position", "Noncommercial Long", "Noncommercial Short", etc (variam por tabela)
    out = {
        "as_of": row.get("As of Date") or row.get("Date"),
        "net_position": row.get("Net Position") or row.get("Net positions") or None,
        "noncomm_long": row.get("Noncommercial Long") or None,
        "noncomm_short": row.get("Noncommercial Short") or None,
    }
    return out

# ==========================
# LLM fallback (Groq -> OpenAI -> DeepSeek)
# ==========================
def llm_interpretation_with_fallback(metrics_dict: dict, title: str) -> Optional[str]:
    """
    Gera a interpretação executiva a partir das métricas usando fallback:
    Groq -> OpenAI -> DeepSeek. Retorna string ou None se todos falharem.
    Variáveis de ambiente usadas:
      - GROQ_API_KEY
      - OPENAI_API_KEY  (opcional)
      - DEEPSEEK_API_KEY (opcional)
      - LLM_TEMPERATURE (opcional; default 0.35)
      - LLM_MAXTOKENS   (opcional; default 900)
      - LLM_GROQ_MODEL  (opcional; default 'llama-3.1-70b-versatile')
      - LLM_OPENAI_MODEL (opcional; default 'gpt-4o-mini')
      - LLM_DEEPSEEK_MODEL (opcional; default 'deepseek-chat')
    """
    temperature = float(os.getenv("LLM_TEMPERATURE", "0.35"))
    max_tokens = int(os.getenv("LLM_MAXTOKENS", "900"))

    system_msg = (
        "Você é um analista sênior de metais preciosos. Escreva em português (Brasil), "
        "sintético, institucional e acionável. Foque em ouro (XAU)."
    )
    user_msg = (
        f"TÍTULO: {title}\n\n"
        "Tarefa: produzir 'Interpretação Executiva' (5–8 bullets) + uma síntese final "
        "a partir das métricas JSON abaixo. Não invente números; se algo estiver ausente, "
        "declare a limitação e indique impacto em risco/direção.\n\n"
        "MÉTRICAS JSON:\n" + json.dumps(metrics_dict, ensure_ascii=False, indent=2)
    )

    # 1) Groq
    groq_key = os.getenv("GROQ_API_KEY")
    if groq_key:
        groq_model = os.getenv("LLM_GROQ_MODEL", "llama-3.1-70b-versatile")
        data = _post_json(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
            payload={
                "model": groq_model,
                "messages": [
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": user_msg},
                ],
                "temperature": temperature,
                "max_tokens": max_tokens,
            },
            timeout=90,
        )
        if data and data.get("choices"):
            text = data["choices"][0]["message"]["content"].strip()
            if text:
                return text

    # 2) OpenAI
    openai_key = os.getenv("OPENAI_API_KEY")
    if openai_key:
        openai_model = os.getenv("LLM_OPENAI_MODEL", "gpt-4o-mini")
        data = _post_json(
            "https://api.openai.com/v1/chat/completions",
            headers={"Authorization": f"Bearer {openai_key}", "Content-Type": "application/json"},
            payload={
                "model": openai_model,
                "messages": [
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": user_msg},
                ],
                "temperature": temperature,
                "max_tokens": max_tokens,
            },
            timeout=90,
        )
        if data and data.get("choices"):
            text = data["choices"][0]["message"]["content"].strip()
            if text:
                return text

    # 3) DeepSeek
    deepseek_key = os.getenv("DEEPSEEK_API_KEY")
    if deepseek_key:
        deepseek_model = os.getenv("LLM_DEEPSEEK_MODEL", "deepseek-chat")
        data = _post_json(
            "https://api.deepseek.com/chat/completions",
            headers={"Authorization": f"Bearer {deepseek_key}", "Content-Type": "application/json"},
            payload={
                "model": deepseek_model,
                "messages": [
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": user_msg},
                ],
                "temperature": temperature,
                "max_tokens": max_tokens,
            },
            timeout=90,
        )
        if data and data.get("choices"):
            text = data["choices"][0]["message"]["content"].strip()
            if text:
                return text

    return None

# ==========================
# Telegram
# ==========================
def telegram_send_messages(token: str, chat_id: str, messages: list[str], parse_mode: Optional[str] = "HTML"):
    base = f"https://api.telegram.org/bot{token}/sendMessage"
    for msg in messages:
        data = {
            "chat_id": chat_id,
            "text": msg,
            "disable_web_page_preview": True,
        }
        if parse_mode:
            data["parse_mode"] = parse_mode
        r = requests.post(base, data=data, timeout=60)
        time.sleep(0.5)

# ==========================
# .sent guard (evitar duplicidade)
# ==========================
def already_sent_key(key: str) -> bool:
    path = os.path.join(".sent", key)
    return os.path.exists(path)

def mark_sent(key: str):
    os.makedirs(".sent", exist_ok=True)
    path = os.path.join(".sent", key)
    with open(path, "w", encoding="utf-8") as f:
        f.write(datetime.now().isoformat())

# ==========================
# Main
# ==========================
def main():
    # --- sanity ping (opcional) ---
    tg_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    tg_chat  = os.getenv("TELEGRAM_CHAT_ID_METALS", "")
    if tg_token and tg_chat:
        try:
            telegram_send_messages(tg_token, tg_chat, ["✅ Conexão OK: Gold Daily inicializando..."])
        except Exception:
            pass

    # --- chave de duplicidade (por dia/periodicidade) ---
    data_str = today_brt_str()
    # exemplo: 16 de setembro de 2025 -> 2025-09-16
    today_key = datetime.now(BRT).strftime("gold-daily-%Y-%m-%d.txt")
    if already_sent_key(today_key):
        return

    # ================
    # Coleta de dados
    # ================
    metrics: Dict[str, Any] = {}

    # 0) Preços/metas (Yahoo)
    try:
        q = fetch_yahoo_quotes(symbols=["GC=F", "^GSPC"])  # ouro futuro, S&P 500 (para correlação)
        metrics["market_quotes"] = q
    except Exception as e:
        metrics["market_quotes_error"] = str(e)

    # 1) World Bank: reservas (total e ouro)
    try:
        wb = fetch_worldbank_gold_reserves_world()
        metrics["central_banks_reserves_world"] = wb
    except Exception as e:
        metrics["central_banks_reserves_world_error"] = str(e)

    # 2) FRED: DXY (DTWEXBGS) e taxa real prox (usar TIPS proxy - DGS10 - T10YIE hipotético / simplificado)
    try:
        dxy = fetch_fred_series("DTWEXBGS")
        metrics["fred_dxy"] = dxy
    except Exception as e:
        metrics["fred_dxy_error"] = str(e)

    # 3) CFTC COT via Nasdaq Data Link
    try:
        cot = fetch_cftc_cot_gold()
        metrics["cftc_cot_gold"] = cot
    except Exception as e:
        metrics["cftc_cot_gold_error"] = str(e)

    # ================
    # Título e corpo
    # ================
    # contador simples em memória (ou poderia ler/gravar JSON counters como no seu padrão)
    numero = int(datetime.now().strftime("%H%M%S"))  # número simbólico do relatório
    title = f"📊 <b>Dados — Ouro — {data_str} — Diário — Nº {numero}</b>"

    # Interpretação executiva (LLM fallback)
    interpretacao = llm_interpretation_with_fallback(metrics, f"Dados — Ouro — {data_str} — Diário — Nº {numero}")
    if not interpretacao:
        interpretacao = (
            "⚠️ Não foi possível gerar a interpretação automática agora. "
            "Use as métricas acima como base. (Falha nos provedores de IA)"
        )

    # Mensagem (duas partes: cabeçalho + bloco de métricas + interpretação)
    m1 = title
    # bloco de métricas (compacto)
    metrics_compact = html.escape(json.dumps(metrics, ensure_ascii=False, indent=2), quote=False)
    m2 = f"<pre>{metrics_compact}</pre>\n\n{html.escape('---', quote=False)}"

    # interpretação (mantém formatação HTML simples)
    # Evitar tags não permitidas, manter texto puro aqui (Telegram aceita <b>, <i>, <code>, <pre>, etc.)
    m3 = interpretacao

    # Envio
    if tg_token and tg_chat:
        telegram_send_messages(tg_token, tg_chat, [m1, m2, m3], parse_mode="HTML")

    # marca .sent
    mark_sent(today_key)

if __name__ == "__main__":
    main()