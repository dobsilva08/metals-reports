# -*- coding: utf-8 -*-
"""
Gera e envia o relatório diário do Ouro (XAU/USD) para o Telegram.
- Busca cotações (Yahoo), DXY (FRED), reservas (World Bank; best-effort)
- Gera resumo automático via IA (fallback: GROQ -> OpenAI -> DeepSeek)
- Evita duplicados (.sent/)
- Título padrão com contador persistente ("— Nº X")
"""

import os
import json
import time
import math
import pathlib
from datetime import datetime, timezone
import locale

import requests
import pandas as pd
import yfinance as yf
from tenacity import retry, wait_exponential, stop_after_attempt

# ========== Localização (pt-BR) ==========
try:
    locale.setlocale(locale.LC_TIME, "pt_BR.UTF-8")
except locale.Error:
    try:
        locale.setlocale(locale.LC_TIME, "pt_BR")
    except locale.Error:
        pass

# ========== ENV / Secrets ==========
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
USER_AGENT = os.getenv("SEC_USER_AGENT", "metals-reports/1.0 (+https://github.com/dobsilva08)")

HEADERS = {"User-Agent": USER_AGENT}

# ========== Utilitários de arquivo / contador ==========
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

# ========== Retentativas HTTP ==========
@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(3))
def _get_json(url: str, params=None, headers=None, timeout=30):
    resp = requests.get(url, params=params, headers=headers or HEADERS, timeout=timeout)
    resp.raise_for_status()
    return resp.json()

# ========== Dados de mercado ==========
def fetch_yahoo_quotes() -> dict:
    """
    Retorna:
      {
        "xauusd_spot": 3683.32,
        "gc_futures": 2387.4,
        "gld": 193.2,
        "iau": 38.1
      }
    Campos podem vir None se indisponíveis.
    """
    out = {"xauusd_spot": None, "gc_futures": None, "gld": None, "iau": None}
    try:
        spot = yf.Ticker("XAUUSD=X").history(period="1d", interval="1d")
        if not spot.empty:
            out["xauusd_spot"] = round(float(spot["Close"].iloc[-1]), 2)
    except Exception as e:
        print("[WARN] Yahoo spot falhou:", e)

    try:
        fut = yf.Ticker("GC=F").history(period="1d", interval="1d")
        if not fut.empty:
            out["gc_futures"] = round(float(fut["Close"].iloc[-1]), 2)
    except Exception as e:
        print("[WARN] Yahoo GC=F falhou:", e)

    for etf_ticker, key in [("GLD", "gld"), ("IAU", "iau")]:
        try:
            etf = yf.Ticker(etf_ticker).history(period="1d", interval="1d")
            if not etf.empty:
                out[key] = round(float(etf["Close"].iloc[-1]), 2)
        except Exception as e:
            print(f"[WARN] Yahoo {etf_ticker} falhou:", e)

    return out

def fetch_fred_dxy() -> dict | None:
    """
    DXY Broad (DTWEXBGS) via FRED.
    Retorna { "series_id": "DTWEXBGS", "last": {"date":"YYYY-MM-DD","value":120.4905} } ou None
    """
    if not FRED_API_KEY:
        print("[INFO] Sem FRED_API_KEY; pulando FRED")
        return None
    try:
        url = "https://api.stlouisfed.org/fred/series/observations"
        params = {
            "series_id": "DTWEXBGS",
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "observation_start": (datetime.utcnow().replace(day=1)).strftime("%Y-%m-01"),
        }
        data = _get_json(url, params=params)
        obs = data.get("observations") or []
        if obs:
            last = obs[-1]
            date = last.get("date")
            value = last.get("value")
            try:
                value_f = float(value)
            except Exception:
                value_f = None
            return {
                "series_id": "DTWEXBGS",
                "last": {"date": date, "value": value_f},
            }
    except Exception as e:
        print("[WARN] FRED DXY falhou:", e)
    return None

def fetch_world_reserves() -> dict | None:
    """
    World Bank (best-effort). Pode retornar None se indisponível.
    """
    try:
        # Total reserves, current US$
        total = _get_json(
            "https://api.worldbank.org/v2/country/WLD/indicator/FI.RES.TOTL.CD",
            params={"format": "json", "per_page": "2"}
        )
        # Gold reserves (if available) – algumas séries relacionadas:
        # Muitas vezes "FI.RES.XGLD.CD" é inconsistente; deixamos best-effort
        gold = None
        try:
            gold = _get_json(
                "https://api.worldbank.org/v2/country/WLD/indicator/FI.RES.XGLD.CD",
                params={"format": "json", "per_page": "2"}
            )
        except Exception:
            gold = None

        def _last_value(wb_json):
            if not isinstance(wb_json, list) or len(wb_json) < 2:
                return None
            rows = wb_json[1] or []
            for row in rows:
                if row and row.get("value") is not None:
                    return {"date": row.get("date"), "value": row.get("value")}
            return None

        return {
            "total_reserves": _last_value(total),
            "gold_reserves": _last_value(gold) if gold else None,
        }
    except Exception as e:
        print("[WARN] World Bank falhou:", e)
        return None

def fetch_cftc_cot_gold() -> dict | None:
    """
    Placeholder (sem fonte gratuita direta confiável via API).
    Retorna None por padrão. Mantemos a estrutura.
    """
    return None

# ========== Prompt IA ==========
def build_summary_prompt(date_label: str, market: dict, dxy: dict | None, reserves: dict | None, cot: dict | None) -> str:
    """
    Constrói um prompt curto e objetivo em PT-BR.
    """
    xau = market.get("xauusd_spot")
    gcf = market.get("gc_futures")
    gld = market.get("gld")
    iau = market.get("iau")

    parts = []
    parts.append(f"Você é um analista macro. Resuma o mercado de OURO em {date_label} de forma curta, clara e útil para Telegram.")
    parts.append("Formato com títulos numerados: 1) Mercado 2) Dólar (DXY) 3) Reservas 4) COT (se houver) 5) Síntese.")
    parts.append("Sem emojis, sem linguagem promocional, foque em pontos chave.")

    # Mercado
    mercado_linha = []
    if xau is not None:
        mercado_linha.append(f"XAUUSD (spot): {xau} USD/oz")
    if gcf is not None:
        mercado_linha.append(f"Futuro GC=F: {gcf}")
    if gld is not None:
        mercado_linha.append(f"ETF GLD: {gld}")
    if iau is not None:
        mercado_linha.append(f"ETF IAU: {iau}")
    if mercado_linha:
        parts.append("Dados de mercado: " + " | ".join(mercado_linha))

    # DXY
    if dxy and dxy.get("last", {}).get("value") is not None:
        parts.append(f"DXY (DTWEXBGS): {dxy['last']['value']} (último: {dxy['last']['date']})")
    else:
        parts.append("DXY: sem dados hoje.")

    # Reservas
    if reserves:
        tr = reserves.get("total_reserves")
        gr = reserves.get("gold_reserves")
        r_parts = []
        if tr and tr.get("value") is not None:
            r_parts.append(f"Reservas globais (USD): {tr['value']} (ano {tr['date']})")
        if gr and gr.get("value") is not None:
            r_parts.append(f"Reservas em ouro (USD): {gr['value']} (ano {gr['date']})")
        if r_parts:
            parts.append("Reservas (World Bank): " + " | ".join(r_parts))
        else:
            parts.append("Reservas: sem dados consistentes no momento.")

    # COT
    if cot:
        parts.append("COT (ouro): dados parciais.")
    else:
        parts.append("COT (ouro): sem dados disponíveis gratuitamente no momento.")

    parts.append("Finalize com uma síntese direta (viés, riscos, drivers).")
    return "\n".join(parts)

# ========== IA: chamadas ==========
@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(2))
def _post_json(url, headers, payload, timeout=60):
    r = requests.post(url, headers=headers, json=payload, timeout=timeout)
    r.raise_for_status()
    return r.json()

def generate_via_groq(prompt: str) -> str:
    if not GROQ_API_KEY:
        raise RuntimeError("Sem GROQ_API_KEY")
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": "llama3-8b-8192",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3,
        "max_tokens": 600,
    }
    data = _post_json(url, headers, payload)
    return (data.get("choices") or [{}])[0].get("message", {}).get("content", "").strip()

def generate_via_openai(prompt: str) -> str:
    if not OPENAI_API_KEY:
        raise RuntimeError("Sem OPENAI_API_KEY")
    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": "gpt-4o-mini",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3,
        "max_tokens": 600,
    }
    data = _post_json(url, headers, payload)
    return (data.get("choices") or [{}])[0].get("message", {}).get("content", "").strip()

def generate_via_deepseek(prompt: str) -> str:
    if not DEEPSEEK_API_KEY:
        raise RuntimeError("Sem DEEPSEEK_API_KEY")
    url = "https://api.deepseek.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {DEEPSEEK_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": "deepseek-chat",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.3,
        "max_tokens": 600,
    }
    data = _post_json(url, headers, payload)
    return (data.get("choices") or [{}])[0].get("message", {}).get("content", "").strip()

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
    payload = {
        "chat_id": TELEGRAM_CHAT_ID_METALS,
        "text": text,
        "disable_web_page_preview": True,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode
    r = requests.post(url, json=payload, timeout=60)
    try:
        r.raise_for_status()
    except Exception as e:
        print("[WARN] Telegram falhou:", e, r.text[:200])

# ========== Execução ==========
def main():
    _ensure_dir(SENT_ROOT)

    # Guard de duplicidade diário
    if os.path.exists(SENT_FLAG_FILE):
        print("[INFO] Já enviado hoje; encerrando.")
        return

    # Título padrão com contador
    title = get_title_with_counter(
        frequency="Diário",
        pair_label="Ouro (XAU/USD)",
        counter_file=COUNTER_FILE,
    )

    # Coleta de dados
    market = fetch_yahoo_quotes()
    dxy = fetch_fred_dxy()
    reserves = fetch_world_reserves()
    cot = fetch_cftc_cot_gold()  # retorna None por enquanto

    # Bloco JSON “dados crus” (compacto)
    dados = {
        "market_quotes": market,
        "central_banks_reserves_world": reserves,
        "fred_dxy": dxy,
        "cftc_cot_gold": cot,
    }
    # Cabeçalho
    dtlabel = datetime.utcnow().strftime("%d/%m/%Y")
    header = f"{title}"

    # Mensagem 1: dados compactos (como JSON) – opcional, mas útil
    json_block = "```\n" + json.dumps(dados, ensure_ascii=False, indent=2) + "\n```"
    send_telegram(f"{header}\n\n{json_block}", parse_mode="Markdown")

    # Mensagem 2: resumo IA
    prompt = build_summary_prompt(dtlabel, market, dxy, reserves, cot)
    resumo = generate_summary_with_fallback(prompt)
    msg = f"{resumo}"
    send_telegram(msg)

    # Marca como enviado
    pathlib.Path(SENT_FLAG_FILE).touch()
    print("[OK] Relatório enviado.")

if __name__ == "__main__":
    main()
