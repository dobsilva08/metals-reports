#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Relatório Diário — Ouro (XAU/USD)
- 10 tópicos fixos
- Coleta factual via utils.market_data (APIs -> yfinance -> público)
- Usa LLMClient (ordem de fallback definida por env: LLM_FALLBACK_ORDER)
- Título com contador "Nº X" e data BRT
- Trava diária (.sent) para envio único por dia (ignorável com --force)
- Envio opcional ao Telegram

Requer .env/Secrets:
  # LLM
  LLM_PROVIDER=groq            # sugestão p/ reduzir custo
  LLM_FALLBACK_ORDER=groq,openrouter,piapi,deepseek
  GROQ_API_KEY=...
  OPENROUTER_API_KEY=...
  PIAPI_API_KEY=...
  DEEPSEEK_API_KEY=...

  # Dados (opcionais; o util tem fallbacks)
  GOLDAPI_KEY=...
  ALPHA_VANTAGE_API_KEY=...
  FRED_API_KEY=...
  NASDAQ_DATA_LINK_API_KEY=...
  METAL_PRICE_API=...
  METALS_DEV_API=...

  # Telegram
  TELEGRAM_BOT_TOKEN=...
  TELEGRAM_CHAT_ID_METALS=...
  TELEGRAM_CHAT_ID_TEST=... (opcional)
  TELEGRAM_MESSAGE_THREAD_ID=... (opcional)
"""

import os
import json
import argparse
import html
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any

# LLM unificado (Groq/OpenRouter/PIAPI/DeepSeek conforme env)
from providers.llm_client import LLMClient
# Dados reais com múltiplas camadas de fallback
from utils.market_data import (
    get_xauusd, get_dxy, get_us10y,
    fmt_line_price, fmt_line_macro
)

try:
    import requests
except Exception:
    requests = None

# ---------------- Config fuso BRT ----------------
BRT = timezone(timedelta(hours=-3), name="BRT")

# ---------------- Utilidades ----------------
def ensure_dir(path: str) -> None:
    base = os.path.dirname(path)
    if base:
        os.makedirs(base, exist_ok=True)

def today_brt_str() -> str:
    meses = ["janeiro","fevereiro","março","abril","maio","junho",
             "julho","agosto","setembro","outubro","novembro","dezembro"]
    now = datetime.now(BRT)
    return f"{now.day} de {meses[now.month-1]} de {now.year}"

def title_counter(counter_path: str, key: str = "diario_ouro") -> int:
    ensure_dir(counter_path)
    try:
        data = json.load(open(counter_path, "r", encoding="utf-8")) if os.path.exists(counter_path) else {}
    except Exception:
        data = {}
    data[key] = int(data.get(key, 0)) + 1
    json.dump(data, open(counter_path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    return data[key]

def sent_guard(path: str) -> bool:
    ensure_dir(path)
    today_tag = datetime.now(BRT).strftime("%Y-%m-%d")
    if os.path.exists(path):
        try:
            data = json.load(open(path, "r", encoding="utf-8"))
            if data.get("last_sent") == today_tag:
                return True
        except Exception:
            pass
    json.dump({"last_sent": today_tag}, open(path, "w", encoding="utf-8"))
    return False

# ---------------- Coleta de contexto (factual) ----------------
def build_context_block() -> str:
    xau = get_xauusd()
    dxy = get_dxy()
    us10 = get_us10y()

    partes = [
        "- GLD/IAU: (em breve dados reais de fluxos).",                         # 1 placeholder
        "- CFTC Net Position (GC): (em breve série real via FRED/CFTC).",       # 2 placeholder
        "- Reservas LBMA/COMEX: (em breve integração de estoques).",            # 3 placeholder
        fmt_line_price(xau, "XAUUSD spot"),                                     # 4 real
        fmt_line_macro(dxy, "DXY (broad)"),                                     # 5 real
        fmt_line_macro(us10, "Treasury 10Y", suffix=" % a.a."),                 # 6 real
        "- Bancos Centrais: compras líquidas modestas (tendência plurianual).", # 7 placeholder
        "- Mineração: custos pressionados; produção estável no agregado.",      # 8 placeholder
    ]
    return "\n".join(partes)

# ---------------- Geração do relatório (LLM) ----------------
def gerar_analise(contexto_textual: str, provider_hint: Optional[str] = None) -> Dict[str, Any]:
    system_msg = (
        "Você é um analista financeiro sênior. Escreva em PT-BR, objetivo e claro, "
        "ancorando-se no contexto factual fornecido. Não invente números."
    )
    user_msg = f"""
Gere um **Relatório Diário — Ouro (XAU/USD)** em **10 tópicos**, numerando exatamente de 1 a 10:

1) Fluxos em ETFs de Ouro (GLD/IAU)
2) Posição Líquida em Futuros (CFTC/CME)
3) Reservas (LBMA/COMEX) e Estoques
4) Fluxos de Bancos Centrais
5) Mercado de Mineração
6) Câmbio e DXY (Dollar Index)
7) Taxas de Juros e Treasuries
8) Notas de Instituições Financeiras / Research
9) Interpretação Executiva (até 5 bullets, objetivos)
10) Conclusão (1 parágrafo: curto e médio prazo)

Contexto factual para basear a análise:
{contexto_textual}
""".strip()

    llm = LLMClient(provider=provider_hint or None)
    texto = llm.generate(system_prompt=system_msg, user_prompt=user_msg, temperature=0.4, max_tokens=1800)
    return {"texto": texto, "provider": llm.active_provider}

# ---------------- Telegram ----------------
def send_to_telegram(text: str, preview: bool = False) -> None:
    if not requests:
        print("requests indisponível; envio ao Telegram pulado.")
        return
    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id_main = os.environ.get("TELEGRAM_CHAT_ID_METALS", "").strip()
    chat_id_test = os.environ.get("TELEGRAM_CHAT_ID_TEST", "").strip()
    thread_id = os.environ.get("TELEGRAM_MESSAGE_THREAD_ID", "").strip()

    chat_id = chat_id_test if (preview and chat_id_test) else chat_id_main
    if not bot_token or not chat_id:
        print("Telegram não configurado. Pulando envio.")
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if thread_id:
        payload["message_thread_id"] = thread_id
    try:
        r = requests.post(url, json=payload, timeout=30)
        r.raise_for_status()
        print("Telegram: mensagem enviada.")
    except Exception as e:
        print("Falha no envio ao Telegram:", e, getattr(r, "text", "")[:500])

# ---------------- Main ----------------
def _fmt_provider(p: str) -> str:
    name = (p or "?").strip().upper()
    aliases = {"PIAPI": "PIAPI", "GROQ": "Groq", "OPENROUTER": "OpenRouter", "DEEPSEEK": "DeepSeek"}
    return aliases.get(name, name.title())

def main():
    parser = argparse.ArgumentParser(description="Relatório Diário — Ouro (XAU/USD)")
    parser.add_argument("--send-telegram", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--preview", action="store_true")
    parser.add_argument("--counter-path", default="data/counters.json")
    parser.add_argument("--sent-path", default="data/sentinels/gold_daily.sent")
    parser.add_argument("--provider", default=None)
    args = parser.parse_args()

    if not args.force and sent_guard(args.sent_path):
        print("Já foi enviado hoje (trava .sent). Use --force para ignorar.")
        return

    numero = title_counter(args.counter_path, key="diario_ouro")
    titulo = f"📊 Dados de Mercado — Ouro (XAU/USD) — {today_brt_str()} — Diário — Nº {numero}"

    contexto = build_context_block()

    t0 = time.time()
    llm_out = gerar_analise(contexto_textual=contexto, provider_hint=args.provider)
    dt = time.time() - t0

    corpo = llm_out["texto"].strip()
    provider_usado = llm_out.get("provider", "?")
    provider_label = _fmt_provider(provider_usado)
    rodape = f"— <i>🧠 Provedor LLM: <b>{provider_label}</b> • {dt:.1f}s</i>"

    texto_final = f"<b>{html.escape(titulo)}</b>\n\n{corpo}\n\n{rodape}"
    print(f"[debug] provider={provider_usado} tempo={dt:.2f}s")
    print(texto_final)

    if args.send_telegram:
        send_to_telegram(texto_final, preview=args.preview)

if __name__ == "__main__":
    main()
