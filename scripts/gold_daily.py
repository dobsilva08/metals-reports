#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Relatório Diário — Metais (Suporta: Prata XAG/USD e Ouro XAU/USD)
- 10 tópicos fixos
- Usa LLMClient (PIAPI padrão + fallback Groq/OpenAI/DeepSeek)
- Trava diária (.sent) e contador por metal
- Envio opcional ao Telegram

Como usar:
$ python relatorio_metais_diario.py --metal xau --send-telegram
$ python relatorio_metais_diario.py --metal xag --preview

Defaults: metal=xag (Prata)
"""

import os
import json
import argparse
import html
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any

from providers.llm_client import LLMClient

try:
    import requests
except Exception:
    requests = None

BRT = timezone(timedelta(hours=-3), name="BRT")

# ---------- utils ----------

def ensure_dir_for_file(path: str) -> None:
    """Create parent dir for a file path if needed."""
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def today_brt_str() -> str:
    meses = [
        "janeiro",
        "fevereiro",
        "março",
        "abril",
        "maio",
        "junho",
        "julho",
        "agosto",
        "setembro",
        "outubro",
        "novembro",
        "dezembro",
    ]
    now = datetime.now(BRT)
    return f"{now.day} de {meses[now.month-1]} de {now.year}"


def title_counter(counter_path: str, key: str) -> int:
    ensure_dir_for_file(counter_path)
    try:
        data = json.load(open(counter_path, "r", encoding="utf-8")) if os.path.exists(counter_path) else {}
    except Exception:
        data = {}
    data[key] = int(data.get(key, 0)) + 1
    json.dump(data, open(counter_path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    return data[key]


def sent_guard(path: str) -> bool:
    ensure_dir_for_file(path)
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
# Implementações simples que podem ser substituídas por queries reais (APIs/DB)


def _placeholder_for(metal: str) -> Dict[str, str]:
    if metal == "xau":
        return {
            "etf_flows": "- GLD/IAU: entradas/liquidas moderadas; demanda por proteção ainda presente.",
            "cftc": "- CFTC (GC): posição líquida dos especuladores ligeira inclinação comprada (estimativa).",
            "reserves": "- LBMA/COMEX: estoques de ouro estáveis; fluxos físicos discretos.",
            "supply": "- Mineração/Reciclagem: produção estável; reciclagem reduzida em relação ao ano anterior.",
            "industry": "- Indústria: demanda por joalheria e reserva; menos sensível a fotovoltaico (comparado à prata).",
            "dxy": "- DXY: dólar relativamente estável; influência negativa marginal em termos locais.",
            "treasuries": "- Treasuries: yields levemente em alta; custo de oportunidade pesa sobre posições em ouro.",
            "research": "- Research: casas seguem cautelosas; ouro mantido como hedge em carteiras.",
        }
    # default -> prata
    return {
        "etf_flows": "- SLV/SIVR: entradas líquidas moderadas; sinal de demanda tática por proteção/indústria.",
        "cftc": "- CFTC (SI): leve alta na posição líquida comprada entre especuladores (estimativa).",
        "reserves": "- LBMA/COMEX: estoques de prata estáveis, sem choques relevantes de oferta física.",
        "supply": "- Oferta/Reciclagem: produção estável; reciclagem firme com preços recentes.",
        "industry": "- Indústria/Fotovoltaico: demanda estrutural positiva com expansão de painéis solares.",
        "dxy": "- DXY: estabilidade recente; dólar ainda limita movimentos de alta.",
        "treasuries": "- Treasuries: yields em leve alta; custo de oportunidade pesa na ponta comprada.",
        "research": "- Research: casas indicam assimetria positiva se indústria acelerar; ainda cautela no curto prazo.",
    }


def build_context_block(metal: str = "xag") -> str:
    meta = _placeholder_for(metal)
    partes = [
        meta["etf_flows"],
        meta["cftc"],
        meta["reserves"],
        meta["supply"],
        meta["industry"],
        meta["dxy"],
        meta["treasuries"],
        meta["research"],
        # 9 e 10 ficam para a LLM (interpretação + conclusão)
    ]
    return "\n".join(partes)


# ---------- geração LLM ----------

def gerar_analise_metal(metal: str, contexto_textual: str, provider_hint: Optional[str] = None) -> Dict[str, Any]:
    metal_map = {"xag": ("Prata", "XAG/USD"), "xau": ("Ouro", "XAU/USD")}
    nome, symbol = metal_map.get(metal, ("Prata", "XAG/USD"))

    system_msg = (
        "Você é um analista financeiro sênior. Escreva em PT-BR, objetivo e claro, "
        "com dados e interpretação executiva. Evite jargão; mantenha coesão macro/indústria."
    )

    user_msg = f"""
Gere um **Relatório Diário — {nome} ({symbol})** estruturado nos **10 tópicos abaixo**.
Seja específico e conciso. Numere exatamente de 1 a 10.

1) Fluxos em ETFs ({'GLD/IAU' if metal=='xau' else 'SLV/SIVR'})
2) Posição Líquida em Futuros (CFTC/CME — {'GC' if metal=='xau' else 'SI'})
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
    if thread_id:
        payload["message_thread_id"] = thread_id
    try:
        r = requests.post(url, json=payload, timeout=30)
        r.raise_for_status()
        print("Telegram: mensagem enviada.")
    except Exception as e:
        print("Falha no envio ao Telegram:", e, getattr(r, "text", "")[:500])


# ---------- main ----------

def main():
    parser = argparse.ArgumentParser(description="Relatório Diário — Metais (XAU/XAG) — 10 tópicos")
    parser.add_argument("--metal", choices=["xag", "xau"], default="xag", help="Escolha 'xag' (Prata) ou 'xau' (Ouro). Default: xag")
    parser.add_argument("--send-telegram", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--preview", action="store_true")
    parser.add_argument("--counter-path", default="data/counters.json")
    parser.add_argument("--sent-path", default=None)
    parser.add_argument("--provider", default=None)
    args = parser.parse_args()

    metal = args.metal.lower()
    metal_map = {"xag": ("Prata", "XAG/USD", "diario_prata"), "xau": ("Ouro", "XAU/USD", "diario_ouro")}
    nome, symbol, counter_key = metal_map.get(metal, ("Prata", "XAG/USD", "diario_prata"))

    sent_path = args.sent_path or f"data/sentinels/{metal}_daily.sent"

    if not args.force and sent_guard(sent_path):
        print("Já foi enviado hoje (trava .sent). Use --force para ignorar.")
        return

    numero = title_counter(args.counter_path, key=counter_key)
    titulo = f"📊 Dados de Mercado — {nome} ({symbol}) — {today_brt_str()} — Diário — Nº {numero}"

    contexto = build_context_block(metal)
    t0 = time.time()
    llm_out = gerar_analise_metal(metal=metal, contexto_textual=contexto, provider_hint=args.provider)
    dt = time.time() - t0

    corpo = llm_out["texto"].strip()
    provider_usado = llm_out.get("provider", "?")
    texto_final = f"<b>{html.escape(titulo)}</b>\n\n{corpo}\n\n<i>Provedor LLM: {html.escape(str(provider_usado))} • {dt:.1f}s</i>"
    print(texto_final)

    if args.send_telegram:
        send_to_telegram(texto_final, preview=args.preview)


if __name__ == "__main__":
    main()
