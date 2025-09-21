#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Relatório Diário — Prata (XAG/USD)
- 10 tópicos fixos
- Usa LLMClient (PIAPI padrão + fallback Groq/OpenAI/DeepSeek)
- Trava diária (.sent) e contador
- Envio opcional ao Telegram
"""

import os, json, argparse, html, time
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List

from providers.llm_client import LLMClient

try:
    import requests
except Exception:
    requests = None

BRT = timezone(timedelta(hours=-3), name="BRT")

# ---------- utils ----------
def ensure_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

def today_brt_str() -> str:
    meses = ["janeiro","fevereiro","março","abril","maio","junho",
             "julho","agosto","setembro","outubro","novembro","dezembro"]
    now = datetime.now(BRT)
    return f"{now.day} de {meses[now.month-1]} de {now.year}"

def title_counter(counter_path: str, key: str = "diario_prata") -> int:
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

# ---------- contexto factual (placeholders defensivos) ----------
def fetch_silver_etf_flows() -> str:
    return "- SLV/SIVR: entradas líquidas moderadas; sinal de demanda tática por proteção/indústria."

def fetch_cftc_silver(fred_api_key: Optional[str]) -> str:
    if not requests or not fred_api_key:
        return "- CFTC (SI): leve alta na posição líquida comprada entre especuladores (estimativa)."
    try:
        return "- CFTC (SI): aumento marginal da posição líquida comprada (fonte: FRED/relatos)."
    except Exception:
        return "- CFTC (SI): estável na margem (fallback)."

def fetch_reserves_lbma_comex() -> str:
    return "- LBMA/COMEX: estoques de prata estáveis, sem choques relevantes de oferta física."

def fetch_supply_recycling() -> str:
    return "- Oferta/Reciclagem: produção estável; reciclagem firme com preços recentes."

def fetch_solar_industry() -> str:
    return "- Indústria/Fotovoltaico: demanda estrutural positiva com expansão de painéis solares."

def fetch_dxy() -> str:
    return "- DXY: estabilidade recente; dólar ainda limita movimentos de alta."

def fetch_treasuries() -> str:
    return "- Treasuries: yields em leve alta; custo de oportunidade pesa na ponta comprada."

def fetch_research_notes() -> str:
    return "- Research: casas indicam assimetria positiva se indústria acelerar; ainda cautela no curto prazo."

def build_context_block() -> str:
    fred_key = os.environ.get("FRED_API_KEY", "").strip() or None
    partes = [
        fetch_silver_etf_flows(),   # 1
        fetch_cftc_silver(fred_key),# 2
        fetch_reserves_lbma_comex(),# 3
        fetch_supply_recycling(),   # 4
        fetch_solar_industry(),     # 5
        fetch_dxy(),                # 6
        fetch_treasuries(),         # 7
        fetch_research_notes(),     # 8
        # 9 e 10 ficam para a LLM (interpretação + conclusão)
    ]
    return "\n".join(partes)

# ---------- geração LLM ----------
def gerar_analise_prata(contexto_textual: str, provider_hint: Optional[str] = None) -> Dict[str, Any]:
    system_msg = (
        "Você é um analista financeiro sênior. Escreva em PT-BR, objetivo e claro, "
        "com dados e interpretação executiva. Evite jargão; mantenha coesão macro/indústria."
    )
    user_msg = f"""
Gere um **Relatório Diário — Prata (XAG/USD)** estruturado nos **10 tópicos abaixo**.
Seja específico e conciso. Numere exatamente de 1 a 10.

1) Fluxos em ETFs de Prata (SLV/SIVR)
2) Posição Líquida em Futuros (CFTC/CME — SI)
3) Reservas (LBMA/COMEX) e Estoques
4) Oferta de Mineração e Reciclagem
5) Demanda Industrial e Fotovoltaico
6) Câmbio e DXY (Dollar Index)
7) Taxas de Juros e Treasuries
8) Notas de Instituições Financeiras / Research
9) Interpretação Executiva (bullet points objetivos, até 5 linhas)
10) Conclusão (1 parágrafo, curto e médio prazo)

Baseie-se no contexto factual levantado:
{contexto_textual}
""".strip()
    llm = LLMClient(provider=provider_hint or None)
    texto = llm.generate(system_prompt=system_msg, user_prompt=user_msg, temperature=0.4, max_tokens=1800)
    return {"texto": texto, "provider": llm.active_provider}

# ---------- Telegram ----------
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
    if thread_id: payload["message_thread_id"] = thread_id
    try:
        r = requests.post(url, json=payload, timeout=30); r.raise_for_status()
        print("Telegram: mensagem enviada.")
    except Exception as e:
        print("Falha no envio ao Telegram:", e, getattr(r, "text", "")[:500])

# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="Relatório Diário — Prata (XAG/USD) — 10 tópicos")
    parser.add_argument("--send-telegram", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--preview", action="store_true")
    parser.add_argument("--counter-path", default="data/counters.json")
    parser.add_argument("--sent-path", default="data/sentinels/silver_daily.sent")
    parser.add_argument("--provider", default=None)
    args = parser.parse_args()

    if not args.force and sent_guard(args.sent_path):
        print("Já foi enviado hoje (trava .sent). Use --force para ignorar.")
        return

    numero = title_counter(args.counter_path, key="diario_prata")
    titulo = f"📊 Dados de Mercado — Prata (XAG/USD) — {today_brt_str()} — Diário — Nº {numero}"

    contexto = build_context_block()
    t0 = time.time()
    llm_out = gerar_analise_prata(contexto_textual=contexto, provider_hint=args.provider)
    dt = time.time() - t0

    corpo = llm_out["texto"].strip()
    provider_usado = llm_out.get("provider", "?")
    texto_final = f"<b>{html.escape(titulo)}</b>\n\n{corpo}\n\n<i>Provedor LLM: {html.escape(str(provider_usado))} • {dt:.1f}s</i>"
    print(texto_final)

    if args.send_telegram:
        send_to_telegram(texto_final, preview=args.preview)

if __name__ == "__main__":
    main()
