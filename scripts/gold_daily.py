#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Relatório Diário — Ouro (XAU/USD) — com fallback de IA (OpenAI) para preencher contexto
- Envio ao Telegram é OPCIONAL (não quebra se não houver chat).
- Trava "uma vez por dia" via .sent.
- APIs > texto IA: números só vêm de APIs; IA complementa seções com narrativa curta e SEM inventar valores.
"""

import os, sys, json, math, urllib.request, urllib.parse, textwrap
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple

# ========================= util/tempo/env =========================

BRT = timezone(timedelta(hours=-3), name="BRT")

def _env(k: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(k)
    return v if (v is not None and str(v).strip() != "") else default

def _now_brt() -> datetime:
    return datetime.now(BRT)

def _today_ptbr() -> str:
    meses = ["janeiro","fevereiro","março","abril","maio","junho","julho","agosto","setembro","outubro","novembro","dezembro"]
    d = _now_brt()
    return f"{d.day} de {meses[d.month-1]} de {d.year}"

def _http_get_json(url: str, headers: Optional[Dict[str,str]] = None, timeout: int = 25) -> Optional[Dict[str,Any]]:
    try:
        req = urllib.request.Request(url, headers=headers or {})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"[http] GET falhou {url}: {e}")
        return None

def _http_post_json(url: str, payload: Dict[str,Any], headers: Optional[Dict[str,str]] = None, timeout: int = 25) -> Optional[Dict[str,Any]]:
    try:
        data = json.dumps(payload).encode("utf-8")
        base_headers = {"Content-Type":"application/json"}
        if headers: base_headers.update(headers)
        req = urllib.request.Request(url, data=data, headers=base_headers)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"[http] POST falhou {url}: {e}")
        return None

# ========================= trava diária ==========================

def _sent_flag_path(prefix: str = "gold_daily") -> str:
    os.makedirs(".sent", exist_ok=True)
    return os.path.join(".sent", f"{_now_brt().strftime('%Y-%m-%d')}_{prefix}.sent")

SEND_ONCE_PER_DAY = True

def _abort_if_already_sent():
    if SEND_ONCE_PER_DAY and os.path.exists(_sent_flag_path()):
        print("[gold] Já enviado hoje. Abortando para evitar duplicidade.")
        sys.exit(0)

def _mark_sent_ok():
    with open(_sent_flag_path(), "w", encoding="utf-8") as f:
        f.write("ok")

# ====================== fontes de preço do ouro ==================

def _gold_spot_any() -> Tuple[Optional[float], Optional[str]]:
    """Tenta obter XAU/USD por diversas fontes; retorna (preco, fonte)."""
    ua = _env("SEC_USER_AGENT") or "HubMetalsBot/1.0 (contact: your-email@example.com)"

    # 1) GoldAPI.io
    goldapi = _env("GOLDAPI_KEY")
    if goldapi:
        js = _http_get_json("https://www.goldapi.io/api/XAU/USD", headers={"x-access-token": goldapi, "User-Agent": ua})
        if js and isinstance(js.get("price"), (int,float)) and js["price"] > 0:
            return float(js["price"]), "GoldAPI.io"

    # 2) Metals.dev
    metals_dev = _env("METALS_DEV_API")
    if metals_dev:
        url = metals_dev
        if "metals=" not in url:
            url += ("&" if "?" in url else "?") + "metals=XAU&currency=USD"
        js = _http_get_json(url, headers={"User-Agent": ua})
        if js:
            price = None
            if isinstance(js.get("metals"), dict):
                xau = js["metals"].get("XAU") or {}
                price = xau.get("price")
            if price is None and isinstance(js.get("rates"), dict):
                price = js["rates"].get("XAU")
            if isinstance(price, (int,float)) and price > 0:
                return float(price), "Metals.dev"

    # 3) MetalPriceAPI
    metalprice = _env("METAL_PRICE_API")
    if metalprice:
        url = metalprice
        if "symbols=" not in url:
            url += ("&" if "?" in url else "?") + "base=USD&symbols=XAU"
        js = _http_get_json(url, headers={"User-Agent": ua})
        if js and isinstance(js.get("rates"), dict):
            price = js["rates"].get("XAU")
            if isinstance(price, (int,float)) and price > 0:
                # algumas vezes é XAU por USD → se <1, inverter
                if price < 1: price = 1.0 / price
                return float(price), "MetalPriceAPI"

    return None, None

def _fred_series(days: int = 90) -> List[Tuple[str, float]]:
    """FRED GOLDAMGBD228NLBM (London AM Fix). Retorna [(data_iso, valor_usd)]."""
    key = _env("FRED_API_KEY")
    if not key:
        return []
    end = _now_brt().strftime("%Y-%m-%d")
    start = (_now_brt() - timedelta(days=days*2)).strftime("%Y-%m-%d")
    url = ("https://api.stlouisfed.org/fred/series/observations?" +
           urllib.parse.urlencode({
               "series_id": "GOLDAMGBD228NLBM",
               "api_key": key,
               "file_type": "json",
               "observation_start": start,
               "observation_end": end,
           }))
    js = _http_get_json(url)
    out: List[Tuple[str,float]] = []
    if js and isinstance(js.get("observations"), list):
        for o in js["observations"]:
            try:
                out.append((o["date"], float(o["value"])))
            except Exception:
                pass
    return [x for x in out if isinstance(x[1], (int,float))][-days:]

def _pct(a: Optional[float], b: Optional[float]) -> Optional[float]:
    try:
        if a is None or b is None or b == 0:
            return None
        return (a/b - 1.0) * 100.0
    except Exception:
        return None

def _fmt_pct(x: Optional[float]) -> str:
    if x is None: return "—"
    return f"{'+' if x>=0 else ''}{x:.2f}%"

# ============================ telegram opcional ===================

def _telegram_send(text: str, parse_mode: Optional[str] = "Markdown") -> bool:
    token = _env("TELEGRAM_BOT_TOKEN")
    # destino pode ser TELEGRAM_CHAT_ID, TELEGRAM_CHAT_ID_METALS ou TELEGRAM_TO (@canal)
    to = _env("TELEGRAM_CHAT_ID") or _env("TELEGRAM_CHAT_ID_METALS") or _env("TELEGRAM_TO")
    if not token or not to:
        print("[telegram] Sem destino/token — envio pulado.")
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": to, "text": text, "disable_web_page_preview": True}
    if parse_mode: payload["parse_mode"] = parse_mode
    resp = _http_post_json(url, payload)
    ok = bool(resp and resp.get("ok"))
    print("[telegram] Mensagem enviada." if ok else f"[telegram] Falha: {resp}")
    return ok

# ============================ IA (OpenAI) =========================

def _openai_chat(system: str, user: str) -> Optional[str]:
    """
    Chama OpenAI Chat Completions (gpt-4o-mini por padrão). Usa variável OPENAI_API_KEY.
    Retorna texto ou None.
    """
    key = _env("OPENAI_API_KEY")
    if not key:
        return None
    model = _env("OPENAI_MODEL") or "gpt-4o-mini"
    url = "https://api.openai.com/v1/chat/completions"
    payload = {
        "model": model,
        "temperature": 0.2,
        "messages": [
            {"role":"system","content":system},
            {"role":"user","content":user},
        ],
    }
    headers = {"Authorization": f"Bearer {key}"}
    js = _http_post_json(url, payload, headers=headers)
    try:
        return js["choices"][0]["message"]["content"].strip()
    except Exception:
        return None

def _ia_fill_section(title: str, guidance: str, known_numbers: Dict[str, Any]) -> str:
    """
    Pede para a IA gerar um parágrafo conciso SEM inventar números.
    - known_numbers: dicionário com strings para a IA referenciar (ex.: spot_pretty, var_5d_pretty).
    """
    sysmsg = (
        "Você é um analista de mercados de metais preciosos. "
        "Sua tarefa é escrever 2–4 frases concisas, em português do Brasil, "
        "SEM inventar números e SEM datas específicas se não foram fornecidas. "
        "Use termos qualitativos (alta/baixa/estável). "
        "Se algum número conhecido for passado, você pode citá-lo explicitamente; "
        "caso contrário, use travessão (—) para valores. "
        "Finalize sem emojis e sem links."
    )
    # Passa números já conhecidos para evitar alucinação
    facts = "\n".join([f"- {k}: {v}" for k,v in known_numbers.items() if v is not None])
    usermsg = textwrap.dedent(f"""
    Título da seção: {title}
    Diretriz: {guidance}

    Números conhecidos (se algum):
    {facts if facts else "- (nenhum número conhecido)"}

    Responda APENAS com o texto final da seção, sem Markdown extra além de negritos se fizer sentido.
    """).strip()
    out = _openai_chat(sysmsg, usermsg)
    return out or "—"

# =============================== relatório =======================

def _build_report() -> str:
    """
    Gera SEMPRE 10 tópicos:
      1) Preço Spot
      2) Variações (D/D-1, 5d, 30d)
      3) Fluxos em ETFs (GLD/IAU)
      4) Futuros (CFTC/CME)
      5) Reservas em ETFs (toneladas)
      6) Miners (preço intradiário)  [placeholder sem números]
      7) Estrutura a termo (contango/backwardation)
      8) Volatilidade implícita (opções)
      9) Drivers macro (DXY, US10Y)
     10) Interpretação Executiva (IA)
    """

    # --- Título com contador ---
    numero = _next_counter()
    titulo = f"📊 **Dados de Mercado — Ouro (XAU/USD) — {_today_ptbr()} — Diário — Nº {numero}**"

    # ---------- 1) Preço Spot ----------
    spot, fonte = _gold_spot_any()
    if spot:
        spot_line = f"Preço spot atual: **US$ {spot:,.2f}**" + (f" _(fonte: {fonte})_" if fonte else "")
        spot_pretty = f"US$ {spot:,.2f}"
    else:
        spot_line = "Preço spot atual: **—**"
        spot_pretty = None

    # ---------- 2) Variações (FRED) ----------
    fred = _fred_series(90)
    vals = [v for _, v in fred]
    last = vals[-1] if len(vals) >= 1 else None
    d1   = vals[-2] if len(vals) >= 2 else None
    d5   = vals[-6] if len(vals) >= 6 else None
    d30  = vals[-31] if len(vals) >= 31 else None
    var_d1  = _fmt_pct(_pct(last, d1))
    var_5d  = _fmt_pct(_pct(last, d5))
    var_30d = _fmt_pct(_pct(last, d30))
    sec2 = [
        f"- Variação **D/D-1**: {var_d1}",
        f"- Variação **5d**: {var_5d}",
        f"- Variação **30d**: {var_30d}",
    ]

    # ---------- 3) Fluxos em ETFs (GLD/IAU) ----------
    # Sem API nesta versão → IA escreve contexto (sem números).
    sec3_txt = _ia_fill_section(
        "Fluxos em ETFs de Ouro (GLD/IAU)",
        "Explique, qualitativamente, como os fluxos de ETFs tendem a reagir a movimentos do spot e a juros reais. "
        "Não invente números; se não houver dados, use generalidades e observe que números do dia dependem das divulgações oficiais.",
        {"spot": spot_pretty, "var_5d": var_5d, "var_30d": var_30d}
    )

    # ---------- 4) Futuros (CFTC/CME) ----------
    sec4_txt = _ia_fill_section(
        "Posição Líquida em Futuros (CFTC/CME)",
        "Faça 2–3 frases sobre o comportamento típico da posição líquida de não-comerciais e o que isso sinaliza. "
        "Sem números específicos.",
        {}
    )

    # ---------- 5) Reservas em ETFs ----------
    sec5_txt = _ia_fill_section(
        "Reservas em ETFs (toneladas)",
        "Contextualize a relação entre reservas dos maiores ETFs (GLD/IAU) e o sentimento de longo prazo. "
        "Sem citar números.",
        {}
    )

    # ---------- 6) Miners (preço intradiário) ----------
    sec6_txt = _ia_fill_section(
        "Acompanhamento de Miners",
        "Explique rapidamente como miners tendem a alavancar movimentos do ouro e por que podem divergir no curto prazo.",
        {}
    )

    # ---------- 7) Estrutura a termo ----------
    sec7_txt = _ia_fill_section(
        "Estrutura a Termo",
        "Descreva o que é contango/backwardation no ouro e o que normalmente indica. Sem números.",
        {}
    )

    # ---------- 8) Volatilidade implícita ----------
    sec8_txt = _ia_fill_section(
        "Volatilidade Implícita (opções)",
        "Explique em 2–3 frases como a vol implícita de 30d influencia a interpretação do risco no curto prazo. Sem números.",
        {}
    )

    # ---------- 9) Drivers macro ----------
    sec9_txt = _ia_fill_section(
        "Drivers Macro (DXY, US10Y)",
        "Relacione o papel do dólar (DXY) e dos juros longos dos EUA (10y) nos movimentos do ouro. Sem números.",
        {}
    )

    # ---------- 10) Interpretação Executiva ----------
    interp_txt = _ia_fill_section(
        "Interpretação Executiva",
        "Produza 3–5 bullets com conclusões acionáveis, referindo-se ao spot e às variações (5d/30d) já calculadas. "
        "Sem inventar números além dos já fornecidos.",
        {"spot": spot_pretty, "var_5d": var_5d, "var_30d": var_30d}
    )

    # ---------- Montagem final ----------
    parts = [
        titulo, "",
        "**1. Preço Spot (USD/oz)**",
        spot_line, "",
        "**2. Variações (London AM Fix — FRED)**",
        *sec2, "",
        "**3. Fluxos em ETFs de Ouro (GLD/IAU)**",
        sec3_txt, "",
        "**4. Posição Líquida em Futuros (CFTC/CME)**",
        sec4_txt, "",
        "**5. Reservas em ETFs (toneladas)**",
        sec5_txt, "",
        "**6. Acompanhamento de Miners (preço intradiário)**",
        sec6_txt, "",
        "**7. Estrutura a Termo (Spreads/Regime)**",
        sec7_txt, "",
        "**8. Volatilidade Implícita (opções)**",
        sec8_txt, "",
        "**9. Drivers Macro (DXY, US10Y)**",
        sec9_txt, "",
        "**10. Interpretação Executiva**",
        interp_txt, "",
        "_Este relatório foi gerado automaticamente._",
    ]
    return "\n".join(parts)

# ================ contador diário persistente ====================

def _counter_path() -> str:
    os.makedirs("counters", exist_ok=True)
    return os.path.join("counters", "gold_daily.txt")

def _read_counter() -> int:
    try:
        with open(_counter_path(), "r", encoding="utf-8") as f:
            return int(f.read().strip())
    except Exception:
        return 0

def _write_counter(n: int):
    with open(_counter_path(), "w", encoding="utf-8") as f:
        f.write(str(n))

def _next_counter() -> int:
    n = _read_counter() + 1
    _write_counter(n)
    return n

# ================================ main ===========================

def main():
    _abort_if_already_sent()

    report = _build_report()
    print(report)  # sempre loga o texto

    if _telegram_send(report, parse_mode="Markdown") and SEND_ONCE_PER_DAY:
        _mark_sent_ok()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[erro] Execução falhou: {e}")
        sys.exit(1)