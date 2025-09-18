#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, json, argparse, requests, time, textwrap, html
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List

# Fuso para datas legíveis no Brasil
BRT = timezone(timedelta(hours=-3), name="BRT")

# ---------------- utilidades de ambiente/tempo ----------------

def load_env_if_present():
    """Carrega variáveis de um .env (mesma pasta), se existir."""
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        for raw in open(env_path, "r", encoding="utf-8"):
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            if k and v and k not in os.environ:
                os.environ[k.strip()] = v.strip()

def today_brt_str() -> str:
    meses = ["janeiro","fevereiro","março","abril","maio","junho","julho","agosto","setembro","outubro","novembro","dezembro"]
    now = datetime.now(BRT)
    return f"{now.day} de {meses[now.month-1]} de {now.year}"

def iso_to_brt_human(iso_date: str) -> str:
    try:
        dt = datetime.strptime(iso_date, "%Y-%m-%d").replace(tzinfo=BRT)
        meses = ["janeiro","fevereiro","março","abril","maio","junho","julho","agosto","setembro","outubro","novembro","dezembro"]
        return f"{dt.day} de {meses[dt.month-1]} de {dt.year}"
    except Exception:
        return iso_date

def read_counter(counter_file: str, key: str, start_counter: int = 1) -> int:
    """
    Lê/atualiza contador por chave (ex.: diario/semanal/mensal) em counters.json.
    Retorna o Nº atual e já incrementa para o próximo.
    """
    try:
        data = json.load(open(counter_file, "r", encoding="utf-8")) if os.path.exists(counter_file) else {}
        val = int(data.get(key, start_counter))
        data[key] = val + 1
        with open(counter_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return val
    except Exception:
        return start_counter

# ---------------- prompt/estrutura ----------------

def build_prompt(data_str: str, numero: int, metrics: Optional[Dict[str, Any]], label: str) -> str:
    # Colocamos BTC no header pra diferenciar das demais moedas
    header = f"Dados On-Chain BTC — {data_str} — {label} — Nº {numero}"
    rules = (
        "Você é um analista on-chain sênior. Produza um relatório em português do Brasil, objetivo e profissional.\n"
        "TÍTULO (linha única):\n" + header + "\n\n"
        "REGRAS:\n"
        "- Se houver métricas (JSON), use-as; se não houver, NÃO invente números: descreva sinais qualitativos.\n"
        "- Sem links; inclua a data completa no primeiro parágrafo.\n"
        "- Estrutura fixa (na ordem):\n"
        "  1) Exchange Inflow (MA7)\n"
        "  2) Exchange Netflow (Total)\n"
        "  3) Reservas em Exchanges\n"
        "  4) Fluxos de Baleias — 2 parágrafos: (a) depósitos whales/miners; (b) Whale Ratio)\n"
        "  5) Resumo de Contexto Institucional\n"
        "  6) Interpretação Executiva — 5–8 bullets\n"
        "  7) Conclusão\n\n"
        "DADOS (JSON opcional):\n"
    )
    dados = json.dumps(metrics, ensure_ascii=False, indent=2) if metrics else "null"
    return rules + dados

def fallback_content(data_str: str, numero: int, motivo: str, label: str) -> str:
    return textwrap.dedent(f"""
    ⚠️ Não foi possível gerar o relatório automático hoje.
    Motivo: {motivo}
    Data: {data_str} — {label} — Nº {numero}

    Use o esqueleto abaixo para registro:

    1) Exchange Inflow (MA7) — leitura qualitativa.
    2) Exchange Netflow (Total) — leitura qualitativa.
    3) Reservas em Exchanges — leitura qualitativa.
    4) Fluxos de Baleias
       • Depósitos de whales/miners — leitura qualitativa.
       • Whale Ratio — leitura qualitativa.
    5) Resumo de Contexto Institucional — narrativa macro.
    6) Interpretação Executiva — 5–8 bullets curtos e acionáveis.
    7) Conclusão — enquadramento de risco.
    """).strip()

# ---------------- LLM providers ----------------

def _groq_chat(model: str, prompt: str, api_key: str) -> Optional[str]:
    """
    Faz chamada ao endpoint OpenAI-compatível da Groq com fallback de modelos.
    Retorna None para erros 401/403/429 (sem cota), levanta exceção para outros.
    """
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    fallbacks = [m for m in [model,
                             "llama-3.1-70b-versatile",
                             "llama-3.1-8b-instant",
                             "gemma2-9b-it"] if m]

    last_err = None
    for mdl in fallbacks:
        payload = {
            "model": mdl,
            "messages": [
                {"role": "system", "content": "Você é um analista on-chain sênior e escreve em português do Brasil."},
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.35,
            "max_tokens": 1800,
        }
        r = requests.post(url, headers=headers, json=payload, timeout=120)
        if r.status_code in (401, 403, 429):
            return None
        if r.status_code == 200:
            try:
                return r.json()["choices"][0]["message"]["content"]
            except Exception as e:
                last_err = f"parse error: {e} / body={r.text[:200]}"
        else:
            last_err = f"HTTP {r.status_code} — {r.text[:200]}"

    if last_err:
        raise RuntimeError(f"GROQ error: {last_err}")
    return None

def llm_generate(provider: str, model: str, prompt: str, keys: Dict[str, str]) -> Optional[str]:
    """
    Retorna texto do LLM ou None se o erro for de quota/limite (para cair no fallback).
    provider: "groq" | "openai" | "openrouter" | "anthropic"
    """
    p = (provider or "groq").lower()

    if p == "groq":
        return _groq_chat(model or "llama-3.1-70b-versatile", prompt, keys.get("GROQ_API_KEY", ""))

    if p == "openai":
        url = "https://api.openai.com/v1/chat/completions"
        headers = {"Authorization": "Bearer " + keys.get("OPENAI_API_KEY",""), "Content-Type": "application/json"}
        payload = {"model": model or "gpt-4o",
                   "messages":[{"role":"system","content":"Você é um analista on-chain sênior e escreve em português do Brasil."},
                               {"role":"user","content":prompt}],
                   "temperature":0.35,"max_tokens":1800}
        r = requests.post(url, headers=headers, json=payload, timeout=120)
        if r.status_code in (401,403,429):
            return None
        if r.status_code != 200:
            raise RuntimeError(f"OpenAI error: HTTP {r.status_code} — {r.text}")
        return r.json()["choices"][0]["message"]["content"]

    if p == "openrouter":
        url = "https://openrouter.ai/api/v1/chat/completions"
        headers = {"Authorization": "Bearer " + keys.get("OPENROUTER_API_KEY",""), "Content-Type": "application/json"}
        payload = {"model": model or "meta-llama/llama-3.1-70b-instruct",
                   "messages":[{"role":"system","content":"Você é um analista on-chain sênior e escreve em português do Brasil."},
                               {"role":"user","content":prompt}],
                   "temperature":0.35,"max_tokens":1800}
        r = requests.post(url, headers=headers, json=payload, timeout=120)
        if r.status_code in (401,403,429):
            return None
        if r.status_code != 200:
            raise RuntimeError(f"OpenRouter error: HTTP {r.status_code} — {r.text}")
        return r.json()["choices"][0]["message"]["content"]

    if p == "anthropic":
        url = "https://api.anthropic.com/v1/messages"
        headers = {"x-api-key": keys.get("ANTHROPIC_API_KEY",""),
                   "anthropic-version": "2023-06-01",
                   "content-type": "application/json"}
        payload = {"model": model or "claude-3-5-sonnet-20240620",
                   "max_tokens": 1800,
                   "temperature": 0.35,
                   "messages":[{"role":"user","content":prompt}]}
        r = requests.post(url, headers=headers, json=payload, timeout=120)
        if r.status_code in (401,403,429):
            return None
        if r.status_code != 200:
            raise RuntimeError(f"Anthropic error: HTTP {r.status_code} — {r.text}")
        data = r.json()
        parts = data.get("content", [])
        text = "".join(p.get("text","") for p in parts if isinstance(p, dict))
        return text

    raise RuntimeError(f"Provider desconhecido: {provider}")

# ---------------- Telegram helpers ----------------

def _chunk_message(text: str, limit: int = 3900) -> List[str]:
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

def telegram_send_messages(token: str, chat_id: str, messages: List[str], parse_mode: Optional[str] = "HTML"):
    base = f"https://api.telegram.org/bot{token}/sendMessage"
    for msg in messages:
        data = {"chat_id": chat_id, "text": msg, "disable_web_page_preview": True}
        if parse_mode:
            data["parse_mode"] = parse_mode
        r = requests.post(base, data=data, timeout=120)
        if r.status_code != 200:
            raise RuntimeError(f"Telegram error: HTTP {r.status_code} — {r.text}")
        time.sleep(0.6)

# --------------------------- main --------------------------------------

def main():
    load_env_if_present()
    ap = argparse.ArgumentParser(description="Relatório on-chain (BTC) → Telegram.")
    ap.add_argument("--date", help="YYYY-MM-DD (opcional)")
    ap.add_argument("--start-counter", type=int, default=1)
    ap.add_argument("--counter-file", default=os.path.join(os.path.dirname(__file__), "counters.json"))
    ap.add_argument("--metrics")
    ap.add_argument("--provider", choices=["groq","openai","openrouter","anthropic"], default=os.environ.get("PROVIDER","groq"))
    ap.add_argument("--model")  # depende do provider
    ap.add_argument("--period", choices=["daily","weekly","monthly"], default="daily")
    ap.add_argument("--send-as", choices=["message","pdf","both"], default="message")
    args = ap.parse_args()

    # labels/keys por período
    label_map = {"daily": "Diário", "weekly": "Semanal", "monthly": "Mensal"}
    key_map   = {"daily": "diario", "weekly": "semanal", "monthly": "mensal"}
    label     = label_map.get(args.period, "Diário")
    key       = key_map.get(args.period, "diario")

    # Chaves por provider (use a pertinente)
    keys = {
        "OPENAI_API_KEY": os.environ.get("OPENAI_API_KEY",""),
        "GROQ_API_KEY": os.environ.get("GROQ_API_KEY",""),
        "OPENROUTER_API_KEY": os.environ.get("OPENROUTER_API_KEY",""),
        "ANTHROPIC_API_KEY": os.environ.get("ANTHROPIC_API_KEY",""),
    }

    tg_token = os.environ.get("TELEGRAM_BOT_TOKEN") or ""
    tg_chat  = os.environ.get("TELEGRAM_CHAT_ID") or ""
    if args.send_as in ("message","both") and (not tg_token or not tg_chat):
        raise SystemExit("Defina TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID para envio por mensagem.")

    data_str = iso_to_brt_human(args.date) if args.date else today_brt_str()
    numero   = read_counter(args.counter_file, key=f"btc_{key}", start_counter=args.start_counter)

    metrics = None
    if args.metrics and os.path.exists(args.metrics):
        metrics = json.load(open(args.metrics, "r", encoding="utf-8"))

    prompt  = build_prompt(data_str, numero, metrics, label)

    try:
        content = llm_generate(args.provider, args.model, prompt, keys)
        motivo  = None if content else f"sem cota/limite em {args.provider.upper()} ou chave ausente."
    except Exception as e:
        content = None
        motivo  = f"erro no provedor {args.provider.upper()}: {e}"

    # título com BTC para diferenciar
    titulo = f"📊 <b>Dados On-Chain BTC — {data_str} — {label} — Nº {numero}</b>"
    corpo  = content.strip() if content else fallback_content(data_str, numero, motivo, label)

    # Evita erro de parse no Telegram
    corpo_seguro = html.escape(corpo, quote=False)
    full = f"{titulo}\n\n{corpo_seguro}"

    if args.send_as in ("message","both"):
        msgs = _chunk_message(full, limit=3900)
        telegram_send_messages(tg_token, tg_chat, msgs, parse_mode="HTML")
        print(f"[ok] Mensagem enviada em {len(msgs)} parte(s).")

    if args.send_as in ("pdf","both"):
        out_dir = os.path.join(os.path.dirname(__file__), "out")
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, f"Dados On-Chain BTC — {data_str} — {label} — Nº {numero}.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write(full)
        print("[ok] Texto salvo em:", path)

if __name__ == "__main__":
    main()