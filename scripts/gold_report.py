#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, json, argparse, requests, time, html, textwrap, hashlib, math, statistics
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple

# =========================
# FUSO & DATA (BRT)
# =========================
BRT = timezone(timedelta(hours=-3), name="BRT")
MESES = ["janeiro","fevereiro","março","abril","maio","junho","julho","agosto","setembro","outubro","novembro","dezembro"]

def today_brt_str() -> str:
    now = datetime.now(BRT); return f"{now.day} de {MESES[now.month-1]} de {now.year}"

def iso_to_brt_human(iso_date: str) -> str:
    try:
        dt = datetime.strptime(iso_date, "%Y-%m-%d").replace(tzinfo=BRT)
        return f"{dt.day} de {MESES[dt.month-1]} de {dt.year}"
    except Exception:
        return iso_date

def load_env_if_present():
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    if os.path.exists(env_path):
        for raw in open(env_path, "r", encoding="utf-8"):
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line: continue
            k, v = line.split("=", 1)
            if k and v and k not in os.environ: os.environ[k.strip()] = v.strip()

# =========================
# CONTADOR & TRAVA
# =========================
def read_counter(counter_file: str, key: str, start_counter: int = 1) -> int:
    try:
        data = json.load(open(counter_file, "r", encoding="utf-8")) if os.path.exists(counter_file) else {}
        val = int(data.get(key, start_counter))
        data[key] = val + 1
        with open(counter_file, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return val
    except Exception:
        return start_counter

def ensure_not_already_sent(sent_dir: str, date_iso: Optional[str], period: str):
    os.makedirs(sent_dir, exist_ok=True)
    key = f"XAU-{date_iso or datetime.now(BRT).strftime('%Y-%m-%d')}-{period.lower()}"
    h = hashlib.sha1(key.encode("utf-8")).hexdigest()[:10]
    path = os.path.join(sent_dir, f"{key}.{h}.lock")
    if os.path.exists(path):
        raise SystemExit(f"[skip] Já enviado hoje para XAU/{period}")
    with open(path, "w", encoding="utf-8") as f: f.write(key+"\n")
    return path

# =========================
# UTILS
# =========================
def _maybe_float(x):
    try: return float(x)
    except: return None

# =========================
# COLETORES DE DADOS
# =========================

# FRED (DFII10 = real 10y, DTWEXBGS = Dollar Broad)
def fred_series_latest(series_id: str, api_key: str) -> Optional[Tuple[str, float]]:
    if not api_key: return None
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {"series_id": series_id, "api_key": api_key, "file_type": "json", "sort_order": "desc", "limit": 1}
    r = requests.get(url, params=params, timeout=30)
    if r.status_code != 200: return None
    obs = r.json().get("observations", [])
    if not obs: return None
    date = obs[0].get("date"); val = _maybe_float(obs[0].get("value"))
    return (date, val) if (date and val is not None) else None

# CFTC COT (Gold COMEX 088691 — Futures Only Legacy)
def cftc_gold_legacy_latest() -> Optional[Dict[str, Any]]:
    url = "https://www.cftc.gov/dea/futures/deacmxlf.htm"
    r = requests.get(url, timeout=30)
    if r.status_code != 200: return None
    txt = r.text
    m = re.search(r"GOLD\s*-\s*COMMODITY EXCHANGE INC\.\s*Code-0?88?691(.*?)(?:MICRO GOLD|COBALT|$)", txt, flags=re.S|re.I)
    if not m: return None
    block = m.group(1)
    d = re.search(r"Commitments of Traders - Futures Only,\s*([A-Za-z]+\s+\d{1,2},\s*\d{4})", block)
    asof = d.group(1) if d else None
    nums = re.findall(r"\b(\d[\d,]*)\b", block)
    nums = [int(x.replace(",","")) for x in nums[:40]] if nums else []
    noncomm_long = nums[0] if len(nums)>1 else None
    noncomm_short= nums[1] if len(nums)>1 else None
    net = (noncomm_long - noncomm_short) if (noncomm_long is not None and noncomm_short is not None) else None
    return {"as_of": asof, "noncomm_long": noncomm_long, "noncomm_short": noncomm_short, "noncomm_net": net}

# GLD / IAU shares outstanding (páginas oficiais)
def gld_shares_outstanding() -> Optional[float]:
    urls = [
        "https://www.ssga.com/us/en/intermediary/etfs/spdr-gold-shares-gld",
        "https://www.spdrgoldshares.com/usa/financial-information/",
    ]
    pat_m = re.compile(r"Shares Outstanding[^0-9]*([\d,.]+)\s*M", flags=re.I)
    pat_n = re.compile(r"Total Shares Outstanding[^0-9]*([\d,]+)", flags=re.I)
    for u in urls:
        r = requests.get(u, timeout=30)
        if r.status_code != 200: continue
        t = r.text
        m = pat_m.search(t)
        if m:
            millions = float(m.group(1).replace(",",""))
            return millions * 1_000_000.0
        m2 = pat_n.search(t)
        if m2:
            num = float(m2.group(1).replace(",",""))
            return num
    return None

def iau_shares_outstanding() -> Optional[float]:
    u = "https://www.ishares.com/us/products/239561/ishares-gold-trust-fund"
    r = requests.get(u, timeout=30)
    if r.status_code != 200: return None
    t = r.text
    m = re.search(r"Shares Outstanding[^0-9]*([\d,]+)", t, flags=re.I)
    if not m: return None
    return float(m.group(1).replace(",",""))

# Spot ouro (GoldAPI opcional; fallback Yahoo Finance)
def spot_xauusd(goldapi_key: Optional[str]) -> Optional[float]:
    if goldapi_key:
        try:
            r = requests.get("https://www.goldapi.io/api/XAU/USD",
                             headers={"x-access-token": goldapi_key}, timeout=30)
            if r.status_code == 200:
                return _maybe_float(r.json().get("price"))
        except Exception:
            pass
    r2 = requests.get("https://query1.finance.yahoo.com/v7/finance/quote?symbols=XAUUSD%3DX", timeout=30)
    if r2.status_code == 200:
        try: return float(r2.json()["quoteResponse"]["result"][0]["regularMarketPrice"])
        except Exception: return None
    return None

# Estrutura a termo: front GC=F vs contrato +6m (Yahoo Finance)
MONTH_CODE = "FGHJKMNQUVXZ"
def cme_gold_symbol_for_month(year:int, month:int) -> str:
    return f"GC{MONTH_CODE[month-1]}{str(year)[-2:]}"

def gold_contango_6m() -> Optional[Dict[str, Any]]:
    now = datetime.now(BRT)
    rf = requests.get("https://query1.finance.yahoo.com/v7/finance/quote?symbols=GC%3DF", timeout=30)
    if rf.status_code != 200: return None
    jf = rf.json(); front = jf["quoteResponse"]["result"]
    if not front: return None
    front_price = _maybe_float(front[0].get("regularMarketPrice"))
    target = now + timedelta(days=182)
    sym = cme_gold_symbol_for_month(target.year, target.month)
    r2 = requests.get(f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={sym}", timeout=30)
    fut_price = None
    if r2.status_code == 200:
        j2 = r2.json(); fut = j2["quoteResponse"]["result"]
        if fut: fut_price = _maybe_float(fut[0].get("regularMarketPrice"))
    if front_price is None or fut_price is None: return None
    return {"front_symbol":"GC=F","front_price":front_price,"fut6m_symbol":sym,"fut6m_price":fut_price,"contango_usd":fut_price-front_price}

# Correlação GLD vs DXY 30d (opcional)
def simple_corr(xs: List[float], ys: List[float]) -> Optional[float]:
    if len(xs)!=len(ys) or len(xs)<5: return None
    mx, my = statistics.mean(xs), statistics.mean(ys)
    cov = sum((x-mx)*(y-my) for x,y in zip(xs,ys)) / (len(xs)-1)
    sx = math.sqrt(sum((x-mx)**2 for x in xs)/(len(xs)-1)); sy = math.sqrt(sum((y-my)**2 for y in ys)/(len(ys)-1))
    if sx==0 or sy==0: return None
    return cov/(sx*sy)

def last_n_days_series_yahoo(symbol: str, n: int=35) -> List[float]:
    u = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?range={n}d&interval=1d"
    r = requests.get(u, timeout=30)
    if r.status_code!=200: return []
    try:
        closes = r.json()["chart"]["result"][0]["indicators"]["quote"][0]["close"]
        return [float(x) for x in closes if x is not None]
    except Exception:
        return []

def last_n_days_series_fred(series_id: str, n: int, api_key: str) -> List[float]:
    if not api_key: return []
    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {"series_id": series_id, "api_key": api_key, "file_type": "json", "sort_order": "desc", "limit": n}
    r = requests.get(url, params=params, timeout=30)
    if r.status_code!=200: return []
    obs = r.json().get("observations", [])
    vals = []
    for o in obs:
        v = _maybe_float(o.get("value"))
        if v is not None: vals.append(v)
    return list(reversed(vals))

# =========================
# PROMPT — 10 SEÇÕES
# =========================
def build_prompt_xau(data_str: str, numero: int, metrics: Dict[str, Any], label: str) -> str:
    header = f"Dados de Mercado — Ouro (XAU/USD) — {data_str} — {label} — Nº {numero}"
    rules = (
        "Você é um analista sênior de macro e commodities. Escreva em português do Brasil, claro e institucional.\n"
        "TÍTULO (linha única):\n" + header + "\n\n"
        "REGRAS:\n"
        "- Use os dados do JSON exatamente como vierem; se algum campo estiver ausente, não invente números — descreva qualitativamente.\n"
        "- Sem links; inclua a data completa no primeiro parágrafo.\n"
        "- Estrutura fixa (na ordem):\n"
        "  1) Fluxos em ETFs de Ouro (GLD, IAU, etc.)\n"
        "  2) Posição Líquida em Futuros (CFTC/CME)\n"
        "  3) Reservas de Bancos Centrais\n"
        "  4) Fluxos de Mineradoras & Bancos (produção, hedge, OTC)\n"
        "  5) Whale Ratio Institucional vs. Varejo (participação relativa)\n"
        "  6) Drivers Macro (taxa real, DXY, política monetária, geopolítica)\n"
        "  7) Custos de Produção & Oferta Física (AISC, supply)\n"
        "  8) Estrutura a Termo (contango/backwardation LBMA/COMEX)\n"
        "  9) Correlações Cruzadas (DXY, S&P500, BTC)\n"
        "  10) Interpretação Executiva & Conclusão — 5–8 bullets + síntese\n\n"
        "DADOS (JSON):\n"
    )
    return rules + json.dumps(metrics, ensure_ascii=False, indent=2)

# =========================
# LLM (Groq)
# =========================
def llm_generate_groq(model: str, prompt: str, api_key: str) -> Optional[str]:
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    for mdl in [model, "llama-3.1-70b-versatile", "llama-3.1-8b-instant", "gemma2-9b-it"]:
        if not mdl: continue
        payload = {"model": mdl,
                   "messages":[{"role":"system","content":"Você escreve relatórios institucionais em português do Brasil."},
                               {"role":"user","content":prompt}],
                   "temperature":0.35,"max_tokens":1800}
        r = requests.post(url, headers=headers, json=payload, timeout=120)
        if r.status_code in (401,403,429): return None
        if r.status_code == 200:
            try: return r.json()["choices"][0]["message"]["content"]
            except Exception: pass
    return None

# =========================
# TELEGRAM
# =========================
def _chunk_message(text: str, limit: int = 3900) -> List[str]:
    parts: List[str] = []
    for block in text.split("\n\n"):
        b = block.strip()
        if not b:
            if parts and not parts[-1].endswith("\n\n"): parts[-1] += "\n\n"
            continue
        if len(b) <= limit:
            if not parts: parts.append(b)
            elif len(parts[-1]) + 2 + len(b) <= limit: parts[-1] += "\n\n" + b
            else: parts.append(b)
        else:
            acc = ""
            for line in b.splitlines():
                if len(acc) + len(line) + 1 <= limit: acc += (("\n" if acc else "") + line)
                else:
                    if acc: parts.append(acc)
                    acc = line
            if acc: parts.append(acc)
    return parts if parts else ["(vazio)"]

def telegram_send_messages(token: str, chat_id: str, messages: List[str], topic_id: Optional[int]=None):
    base = f"https://api.telegram.org/bot{token}/sendMessage"
    for msg in messages:
        data = {"chat_id": chat_id, "text": msg, "disable_web_page_preview": True, "parse_mode": "HTML"}
        if topic_id: data["message_thread_id"] = topic_id
        r = requests.post(base, data=data, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"Telegram error: HTTP {r.status_code} — {r.text[:200]}")
        time.sleep(0.5)

# =========================
# MAIN
# =========================
def main():
    load_env_if_present()
    ap = argparse.ArgumentParser(description="Relatório OURO (XAU/USD) — dados reais + Groq → Telegram")
    ap.add_argument("--date", help="YYYY-MM-DD (opcional)")
    ap.add_argument("--start-counter", type=int, default=1)
    ap.add_argument("--counter-file", default=os.path.join(os.path.dirname(__file__), "counters-xau.json"))
    ap.add_argument("--period", choices=["daily","weekly","monthly"], default="daily")
    ap.add_argument("--send-as", choices=["message","both"], default="message")
    ap.add_argument("--chat-id", help="Override do chat id (Telegram)")
    ap.add_argument("--topic-id", type=int, help="message_thread_id (opcional)")
    ap.add_argument("--model", default=os.environ.get("MODEL","llama-3.1-70b-versatile"))
    ap.add_argument("--hist-corr", action="store_true", help="Calcula correlação GLD vs DXY (30d)")
    args = ap.parse_args()

    tg_token = os.environ.get("TELEGRAM_BOT_TOKEN","")
    tg_chat  = args.chat_id or os.environ.get("TELEGRAM_CHAT_ID_METALS","")
    tg_topic = args.topic_id

    if args.send_as in ("message","both") and (not tg_token or not tg_chat):
        raise SystemExit("Defina TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID_METALS (ou passe --chat-id).")

    label_map = {"daily": "Diário", "weekly": "Semanal", "monthly": "Mensal"}
    key_map   = {"daily": "diario", "weekly": "semanal", "monthly": "mensal"}
    label     = label_map[args.period]
    key       = key_map[args.period]

    data_str  = iso_to_brt_human(args.date) if args.date else today_brt_str()
    numero    = read_counter(args.counter_file, key=f"xau_{key}", start_counter=args.start_counter)

    groq_key  = os.environ.get("GROQ_API_KEY","")
    fred_key  = os.environ.get("FRED_API_KEY","")
    gold_key  = os.environ.get("GOLDAPI_KEY","")

    # ===== Coleta =====
    metrics: Dict[str, Any] = {}

    metrics["spot"] = {"xauusd": spot_xauusd(gold_key), "unit": "USD/oz"}

    gld_sh = gld_shares_outstanding()
    iau_sh = iau_shares_outstanding()
    metrics["etf_flows"] = {"gld_shares": gld_sh, "iau_shares": iau_sh, "unit": "shares"}

    cot = cftc_gold_legacy_latest()
    if cot:
        metrics["cftc_futures_position"] = {
            "as_of": cot.get("as_of"),
            "speculators_net_long": cot.get("noncomm_net"),
            "noncomm_long": cot.get("noncomm_long"),
            "noncomm_short": cot.get("noncomm_short"),
            "unit": "contratos"
        }

    if fred_key:
        ry = fred_series_latest("DFII10", fred_key)
        dx = fred_series_latest("DTWEXBGS", fred_key)
        metrics["macro_drivers"] = {
            "real_yield_10y": ry[1] if ry else None,
            "real_yield_10y_date": ry[0] if ry else None,
            "dxy_broad": dx[1] if dx else None,
            "dxy_broad_date": dx[0] if dx else None,
        }
    else:
        metrics["macro_drivers"] = {"real_yield_10y": None, "dxy_broad": None, "note": "Informe FRED_API_KEY para números oficiais"}

    term = gold_contango_6m()
    metrics["term_structure"] = term if term else {"contango_usd": None, "note": "Falha em GC=F ou contrato +6m"}

    # placeholders (sem fonte pública unificada)
    metrics["central_bank_reserves"] = {"monthly_change_tonnes": None, "ytd_tonnes": None}
    metrics["miners_banks"] = {"hedge_ratio": None, "production_qoq_pct": None}
    metrics["institution_vs_retail"] = {"institutional_share": None, "retail_share": None}
    metrics["production_cost"] = {"aisc_avg_usd_oz": None}

    if args.hist_corr and fred_key:
        gl = last_n_days_series_yahoo("GLD", 35)
        dxy = last_n_days_series_fred("DTWEXBGS", 35, fred_key)
        c = simple_corr(gl[-30:], dxy[-30:]) if len(gl)>=30 and len(dxy)>=30 else None
        metrics["correlations"] = {"gold_dxy_30d": c}
    else:
        metrics["correlations"] = {"gold_dxy_30d": None}

    # ===== LLM =====
    prompt  = build_prompt_xau(data_str, numero, metrics, label)
    ensure_not_already_sent(os.path.join(os.path.dirname(__file__), ".sent_xau"), args.date, args.period)

    content = llm_generate_groq(args.model, prompt, groq_key)
    if not content:
        corpo = textwrap.dedent(f"""
        ⚠️ Não foi possível gerar o relatório automático hoje.
        Data: {data_str} — {label} — Nº {numero} — XAU

        Métricas coletadas (JSON):
        {json.dumps(metrics, ensure_ascii=False, indent=2)}
        """).strip()
    else:
        corpo = content.strip()

    titulo = f"📊 <b>Dados de Mercado — Ouro (XAU/USD) — {data_str} — {label} — Nº {numero}</b>"
    full = f"{titulo}\n\n{html.escape(corpo, quote=False)}"

    if args.send_as in ("message","both"):
        msgs = _chunk_message(full, 3900)
        telegram_send_messages(os.environ.get("TELEGRAM_BOT_TOKEN",""), tg_chat, msgs, topic_id=tg_topic)
        print(f"[ok] Enviado para chat {tg_chat}")

if __name__ == "__main__":
    main()
