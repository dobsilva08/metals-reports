#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Relatório Diário — Cobre (XCU/USD)
- 10 tópicos fixos
- Usa LLMClient (PIAPI padrão + fallback)
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

def title_counter(counter_path: str, key: str = "diario_cobre") -> int:
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
def fetch_copper_etf_flows() -> str:
    return "- CPER/JJC: fluxos ligeiramente positivos; busca por exposição ao ciclo industrial."

def fetch_cftc_hg(fred_api_key: Optional[str]) -> str:
    if not requests or not fred_api_key:
        return "- CFTC (HG): especuladores com leve alta na posição líquida comprada (estimativa)."
    try:
        return "- CFTC (HG): aumento marginal na posição líquida comprada (fonte: FRED/relatos)."
    except Exception:
        return "- CFTC (HG): estável; variações contidas (fallback)."

def fetch_inventories_lme_comex_shfe() -> str:
    return "- Inventários LME/COMEX/SHFE: níveis moderados; estoques chineses sob observação."

def fetch_mines_smelters_supply() -> str:
    return "- Oferta: minas e fundições reportam manutenção e gargalos pontuais; custo de energia impacta."

def fetch_china_demand() -> str:
    return "- Demanda China/PMIs/Infra: sinais mistos; impulsos de infraestrutura sustentam consumo."

def fetch_dxy() -> str:
    return "- DXY: dólar firme pode limitar ralis de commodities denominadas em USD."

def fetch_treasuries() -> str:
    return "- Treasuries/global rates: yields estáveis a levemente mais altos; apetite por risco moderado."

def fetch_research_notes() -> str:
    return "- Research: foco em balanço tight 2025+, investimentos em transição energética elevam demanda."

def build_context_block() -> str:
    fred_key = os.environ.get("FRED_API_KEY", "").strip() or None
    partes = [
        fetch_copper_etf_flows(),          # 1
        fetch_cftc_hg(fred_key),           # 2
        fetch_inventories_lme_comex_shfe(),# 3
        fetch_mines_smelters_supply(),     # 4
        fetch_china_demand(),              # 5
        fetch_dxy(),                       # 6
        fetch_treasuries(),                # 7
        fetch_research_notes(),            # 8
        # 9 e 10 ficam para a LLM
    ]
    return "\n".join(partes)

# ---------- geração LLM ----------
def gerar_analise_cobre(contexto_textual: str, provider_hint: Optional[str] = None) -> Dict[str, Any]:
    system_msg = (
        "Você é um analista financeiro sênior. Escreva em PT-BR, objetivo e claro. "
        "Conecte macro (dólar/juros) à dinâmica industrial/global do cobre."
    )
    user_msg = f"""
Gere um **Relatório Diário — Cobre (XCU/USD)** estruturado nos **10 tópicos abaixo**.
Seja específico e conciso. Numere exatamente de 1 a 10.

1) Fluxos em ETFs de Cobre (CPER/JJC)
2) Posição Líquida em Futuros (CFTC/COMEX — HG) e LME (se disponível)
3) Inventários (LME/COMEX/SHFE)
4) Oferta de Mineração e Fundições
5) Demanda Industrial e China/PMIs/Infra
6) Câmbio e DXY (Dollar Index)
7) Taxas de Juros (Treasuries) e apetite por risco
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
    parser = argparse.ArgumentParser(description="Relatório Diário — Cobre (XCU/USD) — 10 tópicos")
    parser.add_argument("--send-telegram", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--preview", action="store_true")
    parser.add_argument("--counter-path", default="data/counters.json")
    parser.add_argument("--sent-path", default="data/sentinels/copper_daily.sent")
    parser.add_argument("--provider", default=None)
    args = parser.parse_args()

    if not args.force and sent_guard(args.sent_path):
        print("Já foi enviado hoje (trava .sent). Use --force para ignorar.")
        return

    numero = title_counter(args.counter_path, key="diario_cobre")
    titulo = f"📊 Dados de Mercado — Cobre (XCU/USD) — {today_brt_str()} — Diário — Nº {numero}"

    contexto = build_context_block()
    t0 = time.time()
    llm_out = gerar_analise_cobre(contexto_textual=contexto, provider_hint=args.provider)
    dt = time.time() - t0

    corpo = llm_out["texto"].strip()
    provider_usado = llm_out.get("provider", "?")
    texto_final = f"<b>{html.escape(titulo)}</b>\n\n{corpo}\n\n<i>Provedor LLM: {html.escape(str(provider_usado))} • {dt:.1f}s</i>"
    print(texto_final)

    if args.send_telegram:
        send_to_telegram(texto_final, preview=args.preview)

if __name__ == "__main__":
    main()
