#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Relatório Diário — Ouro (XAU/USD)
- Envio ao Telegram é OPCIONAL (não quebra se não houver chat).
- Trava de "uma vez por dia" via arquivo .sent.
- Busca preço spot por múltiplas fontes quando disponíveis.
"""

import os, sys, json, math, urllib.request, urllib.parse
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

def _http_post_json(url: str, payload: Dict[str,Any], timeout: int = 25) -> Optional[Dict[str,Any]]:
    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, headers={"Content-Type":"application/json"})
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
                # muitas vezes é XAU per USD → se <1, inverter
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

# =============================== relatório =======================

def _build_report() -> str:
    numero = _next_counter()
    titulo = f"📊 **Dados de Mercado — Ouro (XAU/USD) — {_today_ptbr()} — Diário — Nº {numero}**"

    spot, fonte = _gold_spot_any()
    spot_line = f"Preço spot atual: **US$ {spot:,.2f}**" if spot else "Preço spot atual: **indisponível**"
    if fonte: spot_line += f" _(fonte: {fonte})_"

    fred = _fred_series(90)
    vals = [v for _, v in fred]
    last = vals[-1] if len(vals) >= 1 else None
    d1   = vals[-2] if len(vals) >= 2 else None
    d5   = vals[-6] if len(vals) >= 6 else None
    d30  = vals[-31] if len(vals) >= 31 else None

    var_lines = [
        f"- Variação **D/D-1**: {_fmt_pct(_pct(last, d1))}",
        f"- Variação **5d**: {_fmt_pct(_pct(last, d5))}",
        f"- Variação **30d**: {_fmt_pct(_pct(last, d30))}",
    ]

    parts = [
        titulo, "",
        "**1. Preço Spot (USD/oz)**",
        spot_line, "",
        "**2. Variações (London AM Fix — FRED)**",
        *var_lines, "",
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