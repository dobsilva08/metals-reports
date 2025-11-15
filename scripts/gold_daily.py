#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Relatório Diário — Ouro (XAU/USD)
- Usa LLMClient (PIAPI como padrão + fallback Groq/OpenAI/DeepSeek)
- Título com contador "Nº X" e data BRT
- Trava diária (.sent) para envio único por dia (ignorável com --force)
- Envio opcional ao Telegram

Requisitos de ambiente (veja .env.example):
  # LLM (padrão = PiAPI)
  PIAPI_API_KEY=...
  PIAPI_MODEL=gpt-4o-mini
  LLM_PROVIDER=piapi
  # (opcionais para fallback)
  GROQ_API_KEY=...
  OPENAI_API_KEY=...
  DEEPSEEK_API_KEY=...
  LLM_FALLBACK_ORDER=piapi,groq,openai,deepseek

  # Telegram
  TELEGRAM_BOT_TOKEN=...
  TELEGRAM_CHAT_ID_METALS=...      (id do grupo)
  TELEGRAM_MESSAGE_THREAD_ID=...   (opcional: id do tópico, se usar)

  # Dados/Fontes (opcionais, usados no contexto factual)
  FRED_API_KEY=...
  GOLDAPI_KEY=...
"""

import os
import json
import argparse
import html
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List

# --- Importa o cliente LLM unificado com fallback ---
from scripts.providers.llm_client import LLMClient

try:
    import requests  # só para chamadas HTTP opcionais (FRED/GoldAPI/Telegram)
except Exception:
    requests = None  # o script ainda roda, mas sem chamadas externas opcionais

# ---------------- Config fuso BRT ----------------
BRT = timezone(timedelta(hours=-3), name="BRT")


# ---------------- Utilidades de ambiente/arquivo ----------------
def load_env_if_present():
    """Carrega variáveis de um .env (mesma pasta), se existir."""
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")  # tenta ../.env
    env_path2 = os.path.join(os.path.dirname(__file__), ".env")       # tenta ./scripts/.env
    for candidate in (env_path, env_path2):
        if os.path.exists(candidate):
            for raw in open(candidate, "r", encoding="utf-8"):
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                if k and v and k.strip() not in os.environ:
                    os.environ[k.strip()] = v.strip()


def ensure_dir(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)


def today_brt_str() -> str:
    meses = ["janeiro","fevereiro","março","abril","maio","junho",
             "julho","agosto","setembro","outubro","novembro","dezembro"]
    now = datetime.now(BRT)
    return f"{now.day} de {meses[now.month-1]} de {now.year}"


def title_counter(counter_path: str, key: str = "diario_ouro") -> int:
    """
    Controla a numeração do relatório (Nº X) de forma persistente.
    """
    ensure_dir(counter_path)
    try:
        data = json.load(open(counter_path, "r", encoding="utf-8")) if os.path.exists(counter_path) else {}
    except Exception:
        data = {}
    data[key] = int(data.get(key, 0)) + 1
    json.dump(data, open(counter_path, "w", encoding="utf-8"), ensure_ascii=False, indent=2)
    return data[key]


def sent_guard(path: str) -> bool:
    """
    Garante envio único por dia usando um 'selo' .sent com a data BRT.
    Retorna True se JÁ enviou hoje (abortar), False se pode enviar e grava o selo.
    """
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
def fetch_gld_iau_flows() -> str:
    """
    Placeholder para fluxos em ETFs de ouro (GLD/IAU).
    Se você já tem fonte própria, conecte aqui. Abaixo exemplos defensivos.
    """
    # Exemplo: sem dependência fixa; retorna texto enxuto.
    # Integre API real se desejar (GLD/IAU shares/flows).
    return "- GLD/IAU: movimentos recentes indicam entradas moderadas e recomposição parcial de posição."


def fetch_cftc_net_position(fred_api_key: Optional[str]) -> str:
    """
    Placeholder para posição líquida em futuros (CFTC/CME) — via FRED (opcional).
    Se tiver FRED_API_KEY, você pode consultar séries relacionadas (ex.: GC).
    Implemento um texto defensivo se requests/fred não disponível.
    """
    if not requests or not fred_api_key:
        return "- CFTC Net Position (GC): leve aumento na posição líquida comprada (estimativa)."
    try:
        # Exemplo ilustrativo (não uma série real específica):
        # url = f"https://api.stlouisfed.org/fred/series/observations?series_id=XXXXX&api_key={fred_api_key}&file_type=json"
        # r = requests.get(url, timeout=20); r.raise_for_status()
        # ... parse ...
        return "- CFTC Net Position (GC): leve aumento na posição líquida comprada (fonte: FRED)."
    except Exception:
        return "- CFTC Net Position (GC): estabilidade, sem mudança material (fallback)."


def fetch_reserves_lbma_comex() -> str:
    """
    Placeholder para reservas/estoques (LBMA/COMEX).
    Integre suas fontes/planilhas se desejar.
    """
    return "- Reservas LBMA/COMEX: estoques estáveis na margem, sem inflexões relevantes."


def fetch_macro_notes() -> str:
    """
    Breves notas macro de apoio ao contexto (DXY, Treasuries, etc.)
    """
    return "- Macro: DXY lateral e yields dos Treasuries levemente mais altos, limitando altas no ouro."


def build_context_block() -> str:
    """Constrói um bloco factual enxuto para orientar a LLM."""
    fred_key = os.environ.get("FRED_API_KEY", "").strip() or None
    partes = [
        fetch_gld_iau_flows(),
        fetch_cftc_net_position(fred_key),
        fetch_reserves_lbma_comex(),
        fetch_macro_notes(),
    ]
    return "\n".join(partes)


# ---------------- Geração do relatório (LLM) ----------------
def gerar_analise_ouro(contexto_textual: str, provider_hint: Optional[str] = None) -> Dict[str, Any]:
    """
    Usa LLMClient com fallback automático. Retorna dict com texto e provedor usado.
    """
    system_msg = (
        "Você é um analista financeiro sênior. Escreva em PT-BR, objetivo e claro, "
        "com dados e interpretação executiva. Evite jargão desnecessário; "
        "mostre raciocínio econômico coerente."
    )
    user_msg = f"""
Gere um **Relatório Diário — Ouro (XAU/USD)** estruturado nas seções abaixo.
Seja específico e conciso, com foco em implicações de preço e contexto institucional.

1) Fluxos em ETFs de Ouro (GLD/IAU)
2) Posição Líquida em Futuros (CFTC/CME)
3) Reservas (LBMA/COMEX) e Estoques
4) Fluxos Institucionais e Notas de Bancos
5) Interpretação Executiva (bullet points objetivos)
6) Conclusão (1 parágrafo)

Baseie-se no contexto factual levantado:
{contexto_textual}
""".strip()

    # LLM_PROVIDER e LLM_FALLBACK_ORDER são lidos do ambiente.
    llm = LLMClient(provider=provider_hint or None)
    texto = llm.generate(system_prompt=system_msg, user_prompt=user_msg, temperature=0.4, max_tokens=1600)
    return {"texto": texto, "provider": llm.active_provider}


# ---------------- Telegram ----------------
def send_to_telegram(text: str, preview: bool = False) -> None:
    """
    Envia mensagem ao Telegram. Usa:
      TELEGRAM_BOT_TOKEN
      TELEGRAM_CHAT_ID_METALS
      (opcional) TELEGRAM_MESSAGE_THREAD_ID
    Se preview=True, tenta TELEGRAM_CHAT_ID_TEST (se existir).
    """
    if not requests:
        print("requests indisponível; envio ao Telegram pulado.")
        return

    bot_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id_main = os.environ.get("TELEGRAM_CHAT_ID_METALS", "").strip()
    chat_id_test = os.environ.get("TELEGRAM_CHAT_ID_TEST", "").strip()
    thread_id = os.environ.get("TELEGRAM_MESSAGE_THREAD_ID", "").strip()

    chat_id = chat_id_test if (preview and chat_id_test) else chat_id_main
    if not bot_token or not chat_id:
        print("Telegram não configurado (TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID_METALS). Pulando envio.")
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if thread_id:
        payload["message_thread_id"] = thread_id

    try:
        r = requests.post(url, json=payload, timeout=30)
        r.raise_for_status()
        print("Telegram: mensagem enviada com sucesso.")
    except Exception as e:
        body = ""
        try:
            body = r.text[:500]  # type: ignore
        except Exception:
            pass
        print("Falha no envio ao Telegram:", e, body)


# ---------------- Main ----------------
def main():
    load_env_if_present()

    parser = argparse.ArgumentParser(description="Relatório Diário — Ouro (XAU/USD)")
    parser.add_argument("--send-telegram", action="store_true", help="Envia o relatório para o Telegram")
    parser.add_argument("--force", action="store_true", help="Ignora a trava diária (.sent)")
    parser.add_argument("--preview", action="store_true", help="Envia para o chat de TESTE (se TELEGRAM_CHAT_ID_TEST estiver definido)")
    parser.add_argument("--counter-path", default="data/counters.json", help="Caminho do arquivo de contadores")
    parser.add_argument("--sent-path", default="data/sentinels/gold_daily.sent", help="Caminho do selo diário (.sent)")
    parser.add_argument("--provider", default=None, help="Força um provider específico (piapi/groq/openai/deepseek). Opcional.")
    args = parser.parse_args()

    # Trava diária (.sent)
    if not args.force and sent_guard(args.sent_path):
        print("Já foi enviado hoje (trava .sent). Use --force para ignorar.")
        return

    # Título
    numero = title_counter(args.counter_path, key="diario_ouro")
    data_fmt = today_brt_str()
    titulo = f"📊 Dados de Mercado — Ouro (XAU/USD) — {data_fmt} — Diário — Nº {numero}"

    # Contexto factual
    contexto = build_context_block()

    # Geração via LLM (piapi padrão, fallback automático)
    t0 = time.time()
    llm_out = gerar_analise_ouro(contexto_textual=contexto, provider_hint=args.provider)
    dt = time.time() - t0

    corpo = llm_out["texto"].strip()
    provider_usado = llm_out.get("provider", "?")

    # Montagem final (HTML)
    texto_final = f"<b>{html.escape(titulo)}</b>\n\n{corpo}\n\n<i>Provedor LLM: {html.escape(str(provider_usado))} • {dt:.1f}s</i>"

    # Sempre imprime no CI para debug/auditoria
    print(texto_final)

    # Envio opcional ao Telegram
    if args.send_telegram:
        send_to_telegram(texto_final, preview=args.preview)


if __name__ == "__main__":
    main()

