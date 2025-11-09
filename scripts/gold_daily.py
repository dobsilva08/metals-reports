#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Relatório Diário — Ouro (XAU/USD)
- Usa LLMClient (PIAPI como padrão + fallback Groq/OpenAI/DeepSeek)
- Título com contador "Nº X" e data BRT
- Trava diária (.sent) para envio único por dia (ignorável com --force)
- Envio opcional ao Telegram (parse_mode=HTML, com formatação limpa)

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
  TELEGRAM_CHAT_ID_TEST=...        (opcional: para preview)

  # Dados/Fontes (opcionais, usados no contexto factual)
  FRED_API_KEY=...
  GOLDAPI_KEY=...
"""

import os
import re
import json
import argparse
import html as html_escape
import time
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List

# --- Importa o cliente LLM unificado com fallback ---
from providers.llm_client import LLLMClient as LLMClient  # compat se seu módulo usa esse nome
# Se seu arquivo original for exatamente "LLMClient", use a linha abaixo e remova a de cima:
# from providers.llm_client import LLMClient

try:
    import requests  # chamadas HTTP opcionais (Telegram)
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


# ---------------- Coleta de contexto (factual - placeholders) ----------------
def fetch_gld_iau_flows() -> str:
    # Integre sua fonte real quando quiser (SPDR/BlackRock/Nasdaq Data Link).
    return "- GLD/IAU: entradas moderadas e recomposição parcial de posição. [SPDR/BlackRock]"


def fetch_cftc_net_position(fred_api_key: Optional[str]) -> str:
    # Integre FRED/CFTC quando desejar. Mantém texto defensivo.
    if not requests or not fred_api_key:
        return "- CFTC Net Position (GC): leve aumento na posição líquida comprada. [CFTC/FRED]"
    try:
        # Exemplo de integração (comentei porque depende da série exata):
        # url = f"https://api.stlouisfed.org/fred/series/observations?series_id=XXXXX&api_key={fred_api_key}&file_type=json"
        # r = requests.get(url, timeout=20); r.raise_for_status()
        # parse ...
        return "- CFTC Net Position (GC): leve aumento na posição líquida comprada. [FRED]"
    except Exception:
        return "- CFTC Net Position (GC): estabilidade, sem mudança material. [CFTC]"


def fetch_reserves_lbma_comex() -> str:
    return "- Reservas LBMA/COMEX: estoques estáveis, sem inflexões relevantes. [LBMA/COMEX]"


def fetch_macro_notes() -> str:
    return "- Macro: DXY lateral e yields dos Treasuries um pouco mais altos, limitando altas no ouro. [FRED]"


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
    Usa LLMClient com fallback automático. Retorna dict com texto (HTML Telegram-safe) e provedor usado.
    """
    system_msg = (
        "Você é o Head de Commodities Research de uma instituição global. "
        "Produza análise em PT-BR com precisão, concisão e foco em fluxo, risco e implicações de preço. "
        "Cite todos os números disponíveis e coloque a fonte entre colchetes (ex.: [CFTC], [LBMA], [SPDR], [FRED]). "
        "FORMATO DE SAÍDA OBRIGATÓRIO: SOMENTE HTML simples compatível com Telegram, usando apenas <b>, <i>, <u>, <code> e <br>. "
        "NÃO use Markdown (nada de **, _, listas automáticas). "
        "Cada seção deve iniciar com <b>N) Título</b><br> e o conteúdo deve usar frases curtas separadas por <br>. "
        "Para bullets, use o caractere '• ' seguido do texto e um <br> ao final."
    )

    user_msg = f"""
Gere o <b>Relatório Diário — Ouro (XAU/USD)</b> nas SEGUINTES 10 SEÇÕES, exatamente nesta ordem.
Limite total a ~250–350 palavras (conteúdo, sem contar título). Evite frases vagas. Trate curto prazo (dias/semanas) e médio prazo (1–3 meses).
Não inclua links. Não use Markdown. Não repita o título geral dentro do corpo.

<b>1) Fluxos em ETFs de Ouro (GLD/IAU)</b><br>
<b>2) Posição Líquida em Futuros (CFTC/CME)</b><br>
<b>3) Reservas (LBMA/COMEX) e Estoques</b><br>
<b>4) Fluxos de Bancos Centrais</b><br>
<b>5) Mercado de Mineração</b><br>
<b>6) Câmbio e DXY (Dollar Index)</b><br>
<b>7) Taxas de Juros e Treasuries (nominal e real)</b><br>
<b>8) Notas de Instituições Financeiras / Research</b><br>
<b>9) Interpretação Executiva (5 bullets, curtos)</b><br>
<b>10) Conclusão (1 parágrafo)</b><br>

Regras adicionais:
• Use números quando possível (ex.: variações %, níveis de yields).<br>
• Na seção 9, entregue EXATAMENTE 5 bullets, cada um iniciado com '• '.<br>
• Na seção 10, 1 parágrafo único (sem bullets).<br>

Contexto factual levantado (texto auxiliar — não reimprima literalmente):
{contexto_textual}
""".strip()

    llm = LLMClient(provider=provider_hint or None)
    texto = llm.generate(system_prompt=system_msg, user_prompt=user_msg, temperature=0.3, max_tokens=1600)
    # LLM já retorna HTML simples; apenas higienize pequenos deslizes comuns.
    texto_html = sanitize_llm_html(texto)
    return {"texto": texto_html, "provider": llm.active_provider}


def sanitize_llm_html(s: str) -> str:
    """
    Sanitiza pequenas violações: troca '**' por <b>, garante <br> padronizado,
    e remove backticks.
    """
    if not isinstance(s, str):
        return ""
    out = s

    # Converter **bold** para <b> (caso o modelo escape algo)
    def _bold_sub(m):
        return f"<b>{m.group(1)}</b>"
    out = re.sub(r"\*\*(.*?)\*\*", _bold_sub, out)

    # Remover backticks
    out = out.replace("```", "").replace("`", "")

    # Normalizar quebras de linha para <br>
    # Se vier com \n\n, troca por <br><br>; se \n simples, vira <br>
    out = out.replace("\r\n", "\n").replace("\r", "\n")
    out = out.replace("\n\n", "<br><br>").replace("\n", "<br>")

    # Evitar listas HTML não suportadas: troque <li> por bullet-texto
    out = re.sub(r"</?ul>|</?ol>", "", out, flags=re.I)
    out = re.sub(r"<li>\s*", "• ", out, flags=re.I)
    out = out.replace("</li>", "<br>")

    return out.strip()


# ---------------- Telegram ----------------
def split_for_telegram(text: str, max_len: int = 3900) -> List[str]:
    """
    Divide a mensagem em pedaços seguros para Telegram (limite ~4096).
    Corta preferindo quebras <br><br> ou <br>.
    """
    parts: List[str] = []
    t = text
    while len(t) > max_len:
        cut = t.rfind("<br><br>", 0, max_len)
        if cut == -1:
            cut = t.rfind("<br>", 0, max_len)
        if cut == -1:
            cut = max_len
        parts.append(t[:cut])
        t = t[cut:]
    if t:
        parts.append(t)
    return [p.strip() for p in parts if p.strip()]


def send_to_telegram(text_html: str, preview: bool = False) -> None:
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

    for chunk in split_for_telegram(text_html):
        payload = {
            "chat_id": chat_id,
            "text": chunk,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        if thread_id:
            payload["message_thread_id"] = thread_id

        try:
            r = requests.post(url, json=payload, timeout=30)
            r.raise_for_status()
        except Exception as e:
            body = ""
            try:
                body = r.text[:500]  # type: ignore
            except Exception:
                pass
            print("Falha no envio ao Telegram:", e, body)
            break


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

    corpo_html = llm_out["texto"].strip()
    provider_usado = llm_out.get("provider", "?")

    # Montagem final (HTML) — título escapado; corpo já é HTML seguro
    texto_final = (
        f"<b>{html_escape.escape(titulo)}</b><br><br>"
        f"{corpo_html}"
        f"<br><br><i>Provedor LLM: {html_escape.escape(str(provider_usado))} • {dt:.1f}s</i>"
    )

    # Sempre imprime no CI para debug/auditoria
    print(texto_final)

    # Envio opcional ao Telegram
    if args.send_telegram:
        send_to_telegram(texto_final, preview=args.preview)


if __name__ == "__main__":
    main()

