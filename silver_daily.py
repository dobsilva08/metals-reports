# silver_daily.py
# Relatorio Diario - Prata (XAG/USD)
# Coleta contexto macro, gera analise via LLM e envia ao Telegram

import os
import json
import argparse
from datetime import datetime, timezone, timedelta

from llm_client import LLMClient
from telegram_sender import send

BRT = timezone(timedelta(hours=-3), name="BRT")

COUNTER_FILE = "counter.json"
SENT_FILE    = "silver.sent"


def today_brt_str():
    meses = ["janeiro","fevereiro","marco","abril","maio","junho",
             "julho","agosto","setembro","outubro","novembro","dezembro"]
    now = datetime.now(BRT)
    return f"{now.day} de {meses[now.month-1]} de {now.year}"


def load_counter():
    if os.path.exists(COUNTER_FILE):
        try:
            return json.load(open(COUNTER_FILE, encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_counter(data):
    json.dump(data, open(COUNTER_FILE, "w", encoding="utf-8"), ensure_ascii=False, indent=2)


def increment_counter(key):
    data = load_counter()
    data[key] = int(data.get(key, 0)) + 1
    save_counter(data)
    return data[key]


def already_sent():
    today = datetime.now(BRT).strftime("%Y-%m-%d")
    if os.path.exists(SENT_FILE):
        try:
            data = json.load(open(SENT_FILE, encoding="utf-8"))
            if data.get("last_sent") == today:
                return True
        except Exception:
            pass
    json.dump({"last_sent": today}, open(SENT_FILE, "w", encoding="utf-8"))
    return False


def build_context():
    return """- SLV/PSLV: fluxos moderados; prata como hedge industrial e monetario.
- CFTC (SI): posicao liquida dos especuladores neutra a levemente comprada.
- LBMA/COMEX: estoques de prata estaveis; demanda fisica presente.
- Mineracao/Reciclagem: producao estavel; prata como subproduto de outros metais.
- Solar/Industrial: demanda industrial forte; setor solar impulsiona consumo.
- DXY: dolar influencia negativamente precificacao em USD.
- Treasuries: juros reais pressionam posicoes em metais preciosos.
- Research: analistas atentos ao ratio ouro/prata como indicador de ciclo."""


def gerar_relatorio(contexto):
    numero = increment_counter("diario_prata")
    data   = today_brt_str()

    system = (
        "Voce e um analista financeiro senior. Escreva em PT-BR, objetivo e claro, "
        "com dados e interpretacao executiva. Evite jargao; mantenha coesao macro/industria."
    )
    user = f"""Gere um Relatorio Diario - Prata (XAG/USD) estruturado nos 10 topicos abaixo.
Seja especifico e conciso. Numere exatamente de 1 a 10.

1) Fluxos em ETFs (SLV/PSLV)
2) Posicao Liquida em Futuros (CFTC/CME - SI)
3) Reservas (LBMA/COMEX) e Estoques
4) Oferta de Mineracao e Reciclagem
5) Demanda Solar e Industrial
6) Cambio e DXY (Dollar Index)
7) Taxas de Juros e Treasuries
8) Notas de Instituicoes Financeiras / Research
9) Interpretacao Executiva (bullet points objetivos, ate 5 linhas)
10) Conclusao (1 paragrafo, curto e medio prazo)

Contexto factual:
{contexto}
""".strip()

    llm  = LLMClient()
    body = llm.generate(system, user)

    header = (
        f"<b>METALS REPORTS - PRATA (XAG/USD)</b>\n"
        f"<b>Relatorio Diario - N {numero}</b>\n"
        f"<b>Data:</b> {data}\n"
        f"<b>Provider:</b> {llm.active_provider}\n"
        f"{'--'*15}\n\n"
    )
    return header + body


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--send-telegram", action="store_true")
    parser.add_argument("--preview",       action="store_true")
    parser.add_argument("--force",         action="store_true")
    args = parser.parse_args()

    if not args.force and already_sent():
        print("[silver_daily] Relatorio ja enviado hoje. Use --force para reenviar.")
        return

    print("[STEP 1] Coletando contexto de mercado...")
    contexto = build_context()

    print("[STEP 2] Gerando relatorio via LLM...")
    relatorio = gerar_relatorio(contexto)

    if args.preview:
        print("\n" + relatorio)

    if args.send_telegram:
        print("[STEP 3] Enviando ao Telegram...")
        send(relatorio)
        print("[silver_daily] Concluido com sucesso.")
    else:
        print("[silver_daily] Use --send-telegram para enviar.")


if __name__ == "__main__":
    main()
