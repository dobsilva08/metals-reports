#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Relatório Diário — Ouro (XAU/USD)
- Gera texto em PT-BR com preço spot e variações (5d/30d) quando possível.
- Envio ao Telegram é opcional (sem obrigar chat_id).
- Trava de "uma vez por dia" via arquivo .sent.
- Contador diário persistente em counters/gold_daily.txt
"""

import os, sys, json, time, math, textwrap
import urllib.request, urllib.parse
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple

# ========================= Config/Data helpers ===============================

BRT = timezone(timedelta(hours=-3), name="BRT")

def load_env_if_present():
    """Carrega variáveis de um .env (mesma pasta), se existir."""
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k, v = k.strip(), v.strip()
                if k and v and k not in os.environ:
                    os.environ[k] = v

def env(k: str, default: Optional[str] = None) -> Optional[str]:
    v = os.environ.get(k)
    return v if (v is not None and str(v).strip() != "") else default

def now_brt() -> datetime:
    return datetime.now(BRT)

def today_brt_str() -> str:
    meses = ["janeiro","fevereiro","março","abril","maio","junho","julho","agosto","setembro","outubro","novembro","dezembro"]
    d = now_brt()
    return f"{d.day} de {meses[d.month-1]} de {d.year}"

def pct(a: float, b: float) -> Optional[float]:
    try:
        if b == 0 or a is None or b is None:
            return None
        return (a/b - 1.0) * 100.0
    except Exception:
        return None

def http_get_json(url: str, headers: Optional[Dict[str, str]] = None, timeout: int = 25) -> Optional[Dict[str, Any]]:
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"[http] GET falhou {url}: {e}")
        return None

def http_post_json(url: str, payload: Dict[str, Any], headers: Optional[Dict[str, str]] = None, timeout: int = 25) -> Optional[Dict[str, Any]]:
    data = json.dumps(payload).encode("utf-8")
    base_headers = {"Content-Type": "application/json"}
    if headers:
        base_headers.update(headers)
    req = urllib.request.Request(url, data=data, headers=base_headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print(f"[http] POST falhou {url}: {e}")
        return None

# ============================ Telegram (opcional) ============================

def sent_flag_path(prefix: str = "gold_daily") -> str:
    d = now_brt().strftime("%Y-%m-%d")
    os.makedirs(".sent", exist_ok=True)
    return os.path.join(".sent", f"{prefix}_{d}.sent")

SEND_ONCE_PER_DAY = True

def check_once_per_day(prefix: str = "gold_daily"):
    if SEND_ONCE_PER_DAY and os.path.exists(sent_flag_path(prefix)):
        print("[gold] Já enviado hoje. Abortando para evitar duplicidade.")
        sys.exit(0)

def mark_sent_today(prefix: str = "gold_daily"):
    p = sent_flag_path(prefix)
    with open(p, "w", encoding="utf-8") as f:
        f.write("ok")

def telegram_send(text: str, parse_mode: Optional[str] = "Markdown") -> bool:
    token = env("TELEGRAM_BOT_TOKEN")
    # Destino pode ser: TELEGRAM_CHAT_ID, TELEGRAM_CHAT_ID_METALS, TELEGRAM_TO (@canal)
    to = env("TELEGRAM_CHAT_ID") or env("TELEGRAM_CHAT_ID_METALS") or env("TELEGRAM_TO")
    if not token or not to:
        print("[telegram] Sem destino/token — envio pulado.")
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": to, "text": text, "disable_web_page_preview": True}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    resp = http_post_json(url, payload)
    ok = bool(resp and resp.get("ok"))
    if ok:
        print("[telegram] Mensagem enviada.")
    else:
        print(f"[telegram] Falha no envio: {resp}")
    return ok

# =============================== Data Sources ================================

def get_gold_spot_any() -> Tuple[Optional[float], Optional[str]]:
    """
    Tenta obter XAU/USD (1 troy ounce) a partir de diversas APIs.
    Retorna (preco_usd, fonte).
    """
    ua = env("SEC_USER_AGENT") or "HubMetalsBot/1.0 (contact: your-email@example.com)"

    # 1) GoldAPI.io
    goldapi_key = env("GOLDAPI_KEY")
    if goldapi_key:
        url = "https://www.goldapi.io/api/XAU/USD"
        headers = {"x-access-token": goldapi_key, "User-Agent": ua}
        js = http_get_json(url, headers)
        if js:
            price = js.get("price")
            if isinstance(price, (int, float)) and price > 0:
                return float(price), "GoldAPI.io"

    # 2) Metals.dev
    metals_dev = env("METALS_DEV_API")  # se já contiver ?api_key=... deixa como está
    if metals_dev:
        # aceita: METALS_DEV_API=https://api.metals.dev/v1/spot?metals=XAU&currency=USD&api_key=XXXX
        url = metals_dev
        if "metals=" not in url:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}metals=XAU&currency=USD"
        js = http_get_json(url, headers={"User-Agent": ua})
        if js:
            # formatos possíveis:
            # {"metals":{"XAU":{"price":xxxx}}}  OU  {"rates":{"XAU":xxxx}}
            price = None
            if isinstance(js.get("metals"), dict):
                price = (((js["metals"].get("XAU") or {}).get("price")))
            if price is None and isinstance(js.get("rates"), dict):
                price = js["rates"].get("XAU")
            if isinstance(price, (int, float)) and price > 0:
                return float(price), "Metals.dev"

    # 3) Metal Price API
    metalprice = env("METAL_PRICE_API")
    if metalprice:
        # aceita: https://api.metalpriceapi.com/v1/latest?base=USD&symbols=XAU&api_key=....
        url = metalprice
        if "symbols=" not in url:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}base=USD&symbols=XAU"
        js = http_get_json(url, headers={"User-Agent": ua})
        if js and isinstance(js.get("rates"), dict):
            price = js["rates"].get("XAU")
            if isinstance(price, (int, float)) and price > 0:
                # Algumas APIs retornam XAU como "quantas onças por USD" → inverter
                # Metalpriceapi costuma ser "1 USD = rates[XAU] XAU". Precisamos USD por XAU:
                if price < 1:
                    price = 1.0 / price
                return float(price), "MetalPriceAPI"

    return None, None

def fred_gold_series(days: int = 60) -> List[Tuple[str, float]]:
    """
    FRED: série GOLDAMGBD228NLBM (London AM Fix, USD/oz).
    Retorna lista [(date_iso, price)] dos últimos 'days'.
    """
    key = env("FRED_API_KEY")
    if not key:
        return []
    end = now_brt().strftime("%Y-%m-%d")
    start = (now_brt() - timedelta(days=days*2)).strftime("%Y-%m-%d")
    url = (
        "https://api.stlouisfed.org/fred/series/observations?"
        + urllib.parse.urlencode({
            "series_id": "GOLDAMGBD228NLBM",
            "api_key": key,
            "file_type": "json",
            "observation_start": start,
            "observation_end": end,
        })
    )
    js = http_get_json(url)
    out: List[Tuple[str, float]] = []
    if js and isinstance(js.get("observations"), list):
        for obs in js["observations"]:
            d = obs.get("date")
            v = obs.get("value")
            try:
                val = float(v)
                if not math.isnan(val):
                    out.append((d, val))
            except Exception:
                continue
    # mantém somente últimos 'days' valores não nulos
    out = [x for x in out if isinstance(x[1], (int,float))]
    return out[-days:]

def alpha_vantage_latest(symbol: str) -> Optional[float]:
    """
    Alpha Vantage: pega último preço intradiário (close mais recente).
    """
    key = env("ALPHA_VANTAGE_API_KEY")
    if not key:
        return None
    url = (
        "https://www.alphavantage.co/query?"
        + urllib.parse.urlencode({
            "function":"TIME_SERIES_INTRADAY",
            "symbol": symbol,
            "interval":"60min",
            "apikey": key
        })
    )
    js = http_get_json(url)
    if not js:
        return None
    ts = js.get("Time Series (60min)") or js.get("Time Series (5min)") or {}
    if not isinstance(ts, dict) or not ts:
        return None
    latest_ts = sorted(ts.keys())[-1]
    close = ts[latest_ts].get("4. close")
    try:
        return float(close)
    except Exception:
        return None

# =============================== Report logic =================================

def compute_changes_from_fred(hist: List[Tuple[str, float]]) -> Dict[str, Optional[float]]:
    """
    A partir da série diária do FRED, calcula:
      - ultimo (close mais recente)
      - D-1 (anterior)
      - D-5 (aprox 5 pregões atrás)
      - D-30 (aprox 30 pregões)
    """
    if not hist:
        return {"last": None, "d1": None, "d5": None, "d30": None,
                "pct_d1": None, "pct_5d": None, "pct_30d": None}
    vals = [v for (_, v) in hist if isinstance(v, (int, float))]
    if not vals:
        return {"last": None, "d1": None, "d5": None, "d30": None,
                "pct_d1": None, "pct_5d": None, "pct_30d": None}
    last = vals[-1]
    d1 = vals[-2] if len(vals) >= 2 else None
    d5 = vals[-6] if len(vals) >= 6 else None
    d30 = vals[-31] if len(vals) >= 31 else None
    return {
        "last": last,
        "d1": d1,
        "d5": d5,
        "d30": d30,
        "pct_d1": pct(last, d1) if d1 else None,
        "pct_5d": pct(last, d5) if d5 else None,
        "pct_30d": pct(last, d30) if d30 else None,
    }

def format_pct(v: Optional[float]) -> str:
    if v is None:
        return "—"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.2f}%"

def read_counter(path: str) -> int:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return int(f.read().strip())
    except Exception:
        return 0

def write_counter(path: str, val: int):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(str(val))

def next_daily_counter() -> int:
    """Contador persistente simples para o relatório diário."""
    path = os.path.join("counters", "gold_daily.txt")
    n = read_counter(path) + 1
    write_counter(path, n)
    return n

def build_report() -> str:
    # Título
    numero = next_daily_counter()
    data_legivel = today_brt_str()
    titulo = f"📊 **Dados de Mercado — Ouro (XAU/USD) — {data_legivel} — Diário — Nº {numero}**"

    # Seção 1 — Preço spot (tentativa por múltiplas fontes)
    spot, fonte_spot = get_gold_spot_any()
    spot_line = f"Preço spot atual: **US$ {spot:,.2f}**" if spot else "Preço spot atual: **indisponível**"
    if fonte_spot:
        spot_line += f" _(fonte: {fonte_spot})_"

    # Seção 2 — Variações (FRED)
    fred_hist = fred_gold_series(days=90)
    ch = compute_changes_from_fred(fred_hist)
    var_lines = [
        f"- Variação **D/D-1**: {format_pct(ch['pct_d1'])}",
        f"- Variação **5d**: {format_pct(ch['pct_5d'])}",
        f"- Variação **30d**: {format_pct(ch['pct_30d'])}",
    ]

    # Seção 3 — Miners (opcional)
    miners = []
    tickers = (env("AISC_TICKERS") or "NEM,GOLD").split(",")
    tickers = [t.strip().upper() for t in tickers if t.strip()]
    for t in tickers[:4]:
        px = alpha_vantage_latest(t)
        if px:
            miners.append(f"{t}: **${px:,.2f}**")
    miners_block = " | ".join(miners) if miners else "—"

    # Montagem final
    parts = [
        titulo,
        "",
        "**1. Preço Spot (USD/oz)**",
        spot_line,
        "",
        "**2. Variações (London AM Fix — FRED)**",
        *var_lines,
        "",
        "**3. Acompanhamento de Miners (preço intradiário)**",
        miners_block,
        "",
        "_Este relatório foi gerado automaticamente._",
    ]
    return "\n".join(parts)

# =============================== Main ========================================

def main():
    load_env_if_present()
    check_once_per_day(prefix="gold_daily")

    report = build_report()

    # Log sempre
    print(report)

    # Envio opcional ao Telegram
    sent = telegram_send(report, parse_mode="Markdown")
    if sent and SEND_ONCE_PER_DAY:
        mark_sent_today(prefix="gold_daily")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Não explode silenciosamente: mostra erro e sai com 1
        print(f"[erro] Execução falhou: {e}")
        sys.exit(1)