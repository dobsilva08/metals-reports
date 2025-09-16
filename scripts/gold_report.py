#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
gold_report.py — Metals Reports (Ouro)
Coleta dados de múltiplas fontes (APIs e endpoints públicos), consolida métricas,
gera interpretação executiva via LLM (Groq) e envia o relatório ao Telegram.

DEPENDÊNCIAS:
  pip install -r requirements.txt
  (requests, python-dateutil, pandas, openpyxl, beautifulsoup4, lxml)

ENV ESPERADAS (GitHub Secrets):
  GROQ_API_KEY
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID_METALS
  FRED_API_KEY
  NASDAQ_DATA_LINK_API_KEY
  GOLDAPI_KEY
  ALPHA_VANTAGE_API_KEY
  METALS_DEV_API
  METAL_PRICE_API
  SEC_USER_AGENT              # recomendado pela SEC para EDGAR; um e-mail é suficiente
  AISC_TICKERS                # opcional: ex. "NEM,GOLD,AEM,KGC"
"""

import os, json, time, html, re, io
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple

# ------------------------ Config & util ------------------------

BRT = timezone(timedelta(hours=-3), name="BRT")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SENT_DIR = os.path.join(ROOT, ".sent")
os.makedirs(SENT_DIR, exist_ok=True)

def today_brt() -> datetime:
    return datetime.now(BRT)

def today_brt_str_long() -> str:
    meses = ["janeiro","fevereiro","março","abril","maio","junho","julho","agosto","setembro","outubro","novembro","dezembro"]
    now = today_brt()
    return f"{now.day} de {meses[now.month-1]} de {now.year}"

def ymd(date: datetime) -> str:
    return date.strftime("%Y-%m-%d")

def guard_already_sent(period_key: str) -> bool:
    """Evita envios duplicados: cria/checa marcador .sent/"""
    tag = f"done-gold-{period_key}-{ymd(today_brt())}"
    path = os.path.join(SENT_DIR, tag)
    if os.path.exists(path):
        return True
    open(path, "w").close()
    return False

def safe_float(x) -> Optional[float]:
    try:
        if x is None: return None
        return float(x)
    except Exception:
        return None

def http_get(url: Optional[str], params: dict = None, headers: dict = None, timeout: int = 30) -> Optional[requests.Response]:
    if not url:
        return None
    try:
        r = requests.get(url, params=params, headers=headers, timeout=timeout)
        if r.status_code == 200:
            return r
    except Exception:
        return None
    return None

# ------------------------ Spot XAU/USD (cadeia de fallbacks) ------------------------

def spot_from_nasdaq_lbma() -> Optional[float]:
    key = os.environ.get("NASDAQ_DATA_LINK_API_KEY","")
    if not key:
        return None
    url = "https://data.nasdaq.com/api/v3/datasets/LBMA/GOLD.json"
    r = http_get(url, params={"api_key": key})
    if not r:
        return None
    try:
        data = r.json().get("dataset", {})
        rows = data.get("data", [])
        for row in rows:
            usd_pm = safe_float(row[2] if len(row) > 2 else None)
            usd_am = safe_float(row[1] if len(row) > 1 else None)
            if usd_pm: return usd_pm
            if usd_am: return usd_am
    except Exception:
        return None
    return None

def spot_from_alpha_vantage() -> Optional[float]:
    key = os.environ.get("ALPHA_VANTAGE_API_KEY","")
    if not key:
        return None
    base = "https://www.alphavantage.co/query"
    r = http_get(base, params={
        "function": "CURRENCY_EXCHANGE_RATE",
        "from_currency": "XAU",
        "to_currency": "USD",
        "apikey": key
    })
    if r:
        try:
            j = r.json().get("Realtime Currency Exchange Rate", {})
            val = safe_float(j.get("5. Exchange Rate"))
            if val: return val
        except Exception:
            pass
    r2 = http_get(base, params={
        "function": "FX_DAILY",
        "from_symbol": "XAU",
        "to_symbol": "USD",
        "outputsize": "compact",
        "apikey": key
    })
    if r2:
        try:
            ts = r2.json().get("Time Series FX (Daily)", {})
            if ts:
                last_day = sorted(ts.keys())[-1]
                close = safe_float(ts[last_day].get("4. close"))
                if close: return close
        except Exception:
            pass
    return None

def spot_from_goldapi() -> Optional[float]:
    key = os.environ.get("GOLDAPI_KEY","")
    if not key:
        return None
    url = "https://www.goldapi.io/api/XAU/USD"
    r = http_get(url, headers={"x-access-token": key})
    if not r:
        return None
    try:
        j = r.json()
        return safe_float(j.get("price"))
    except Exception:
        return None

def spot_from_metals_dev() -> Optional[float]:
    key = os.environ.get("METALS_DEV_API","")
    if not key:
        return None
    url = "https://api.metals.dev/v1/latest"
    r = http_get(url, params={"api_key": key, "symbols": "XAU", "base": "USD"})
    if not r:
        return None
    try:
        j = r.json()
        return safe_float(((j.get("data") or {}).get("XAU") or {}).get("price"))
    except Exception:
        return None

def spot_from_metalpriceapi() -> Optional[float]:
    key = os.environ.get("METAL_PRICE_API","")
    if not key:
        return None
    url = "https://api.metalpriceapi.com/v1/latest"
    r = http_get(url, params={"api_key": key, "base": "USD", "symbols": "XAU"})
    if not r:
        return None
    try:
        j = r.json()
        xau_per_usd = safe_float(((j.get("rates") or {}).get("XAU")))
        if xau_per_usd and xau_per_usd > 0:
            return 1.0 / xau_per_usd
    except Exception:
        return None
    return None

def spot_from_yahoo() -> Optional[float]:
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    r = http_get(url, params={"symbols": "XAUUSD%3DX,GC%3DF"})
    if not r:
        return None
    try:
        res = r.json().get("quoteResponse", {}).get("result", [])
        for it in res:
            if it.get("symbol") == "XAUUSD=X":
                val = safe_float(it.get("regularMarketPrice")) or safe_float(it.get("postMarketPrice"))
                if val: return val
        for it in res:
            if it.get("symbol") == "GC=F":
                val = safe_float(it.get("regularMarketPrice")) or safe_float(it.get("postMarketPrice"))
                if val: return val
    except Exception:
        return None
    return None

def get_spot_xauusd() -> Tuple[Optional[float], str]:
    chain = [
        ("Nasdaq LBMA", spot_from_nasdaq_lbma),
        ("Alpha Vantage", spot_from_alpha_vantage),
        ("GoldAPI", spot_from_goldapi),
        ("Metals.dev", spot_from_metals_dev),
        ("MetalpriceAPI", spot_from_metalpriceapi),
        ("Yahoo", spot_from_yahoo),
    ]
    for name, fn in chain:
        val = fn()
        if val:
            return val, name
    return None, "none"

# ------------------------ ETFs (GLD/IAU/SLV) ------------------------

def yahoo_quote_symbols(symbols: List[str]) -> Dict[str, dict]:
    url = "https://query1.finance.yahoo.com/v7/finance/quote"
    r = http_get(url, params={"symbols": ",".join(symbols)})
    out = {}
    if not r:
        return out
    try:
        data = r.json().get("quoteResponse", {}).get("result", [])
        for it in data:
            out[it.get("symbol")] = it
    except Exception:
        return out
    return out

def etf_flows_proxy() -> Dict[str, Any]:
    info = yahoo_quote_symbols(["GLD","IAU","SLV"])
    def pick(sym):
        x = info.get(sym, {})
        return {
            "price": safe_float(x.get("regularMarketPrice")),
            "shares_outstanding": safe_float(x.get("sharesOutstanding")),
            "total_assets": safe_float(x.get("totalAssets")),
            "nav_price": safe_float(x.get("navPrice")),
            "source": "Yahoo"
        }
    return {"GLD": pick("GLD"), "IAU": pick("IAU"), "SLV": pick("SLV")}

# ------------------------ COT (CFTC) via Nasdaq Data Link ------------------------

def normalize_key(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")

def cftc_cot_gold_net() -> Optional[Dict[str, Any]]:
    """
    Nasdaq Data Link — Legacy Futures Only (ouro COMEX):
      CFTC/088691_FO_L_ALL
    Calcula Non-Commercial Net, Commercial Net, OI e % de participação (aprox.).
    """
    key = os.environ.get("NASDAQ_DATA_LINK_API_KEY","")
    if not key:
        return None
    url = "https://data.nasdaq.com/api/v3/datasets/CFTC/088691_FO_L_ALL.json"
    r = http_get(url, params={"api_key": key})
    if not r:
        return None
    try:
        ds = r.json().get("dataset", {})
        cols = ds.get("column_names", [])
        data = ds.get("data", [])
        if not data:
            return None
        latest = data[0]
        colmap = {normalize_key(c): i for i, c in enumerate(cols)}

        def getv(*candidates):
            for c in candidates:
                i = colmap.get(c)
                if i is not None and i < len(latest):
                    v = safe_float(latest[i])
                    if v is not None:
                        return v
            return None

        nc_long = getv("noncommercial_long", "non_commercial_long", "noncommercial_positions_long_all")
        nc_short = getv("noncommercial_short", "non_commercial_short", "noncommercial_positions_short_all")
        c_long  = getv("commercial_long", "commercial_positions_long_all")
        c_short = getv("commercial_short", "commercial_positions_short_all")
        spreads = getv("noncommercial_spreads", "spreads", "noncommercial_positions_spreading_all", "spreading")
        oi_all  = getv("open_interest_all", "open_interest__all")

        nc_net = (nc_long or 0) - (nc_short or 0)
        com_net = (c_long or 0) - (c_short or 0)

        part_nc = None
        if oi_all and (nc_long or nc_short or spreads):
            part_nc = 100.0 * ((nc_long or 0) + (nc_short or 0) + (spreads or 0)) / oi_all

        return {
            "available": True,
            "date": latest[0] if isinstance(latest[0], str) else None,
            "noncommercial_long": nc_long,
            "noncommercial_short": nc_short,
            "noncommercial_net": nc_net,
            "commercial_long": c_long,
            "commercial_short": c_short,
            "commercial_net": com_net,
            "spreads": spreads,
            "open_interest_all": oi_all,
            "noncommercial_participation_pct_approx": part_nc,
            "source": "Nasdaq Data Link CFTC/088691_FO_L_ALL"
        }
    except Exception:
        return None

# ------------------------ World Bank: Macro & Reservas ------------------------

def wb_latest_indicator(indicator: str, country: str = "WLD", start_year: int = 2000, end_year: int = 2100):
    url = f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"
    params = {"format": "json", "per_page": 1, "date": f"{start_year}:{end_year}"}
    r = http_get(url, params=params, timeout=30)
    if not r:
        return None
    try:
        data = r.json()
        if not isinstance(data, list) or len(data) < 2 or not data[1]:
            return None
        entry = data[1][0]
        date_year = entry.get("date")
        val = entry.get("value")
        if val is None:
            return None
        date_iso = f"{date_year}-12-31"
        return (date_iso, float(val))
    except Exception:
        return None

def wb_global_macro_drivers() -> Dict[str, Any]:
    gdp = wb_latest_indicator("NY.GDP.MKTP.KD.ZG","WLD")
    cpi = wb_latest_indicator("FP.CPI.TOTL.ZG","WLD")
    return {
        "global_gdp_growth_pct": gdp[1] if gdp else None,
        "global_gdp_growth_date": gdp[0] if gdp else None,
        "global_inflation_cpi_pct": cpi[1] if cpi else None,
        "global_inflation_cpi_date": cpi[0] if cpi else None,
        "source": "World Bank"
    }

def wb_central_bank_gold_value_usd() -> Dict[str, Any]:
    tot = wb_latest_indicator("FI.RES.TOTL.CD","WLD")
    exg = wb_latest_indicator("FI.RES.XGLD.CD","WLD")
    if not tot or not exg:
        return {"gold_value_usd": None, "date_total": tot[0] if tot else None, "date_exgold": exg[0] if exg else None, "source": "World Bank"}
    gold_val = max(0.0, float(tot[1]) - float(exg[1]))
    return {"gold_value_usd": gold_val, "date_total": tot[0], "date_exgold": exg[0], "source": "World Bank"}

def wb_gold_value_to_tonnes(gold_value_usd: float, xauusd_spot: float) -> Optional[float]:
    if not gold_value_usd or not xauusd_spot or xauusd_spot <= 0:
        return None
    troy_oz = gold_value_usd / xauusd_spot
    tonnes = troy_oz / 32150.7466
    return tonnes

# ------------------------ FRED: Macro drivers ------------------------

def fred_series_last(series_id: str) -> Optional[Tuple[str,float]]:
    key = os.environ.get("FRED_API_KEY","")
    if not key:
        return None
    url = "https://api.stlouisfed.org/fred/series/observations"
    r = http_get(url, params={"series_id": series_id, "file_type": "json", "api_key": key})
    if not r:
        return None
    try:
        obs = r.json().get("observations", [])
        for row in reversed(obs):
            val = row.get("value")
            if val not in (None, ".", ""):
                return (row.get("date"), float(val))
    except Exception:
        return None
    return None

def macro_from_fred() -> Dict[str, Any]:
    out = {}
    pairs = {
        "real_yield_10y": "DFII10",
        "dollar_dtwexbgs": "DTWEXBGS",
        "fed_funds": "FEDFUNDS",
    }
    for k, sid in pairs.items():
        res = fred_series_last(sid)
        if res:
            out[k] = {"date": res[0], "value": res[1], "series": sid, "source": "FRED"}
        else:
            out[k] = {"date": None, "value": None, "series": sid, "source": "FRED"}
    return out

# ------------------------ Estrutura a termo (GC=F) ------------------------

def yahoo_gc_future() -> Dict[str, Any]:
    q = yahoo_quote_symbols(["GC=F"])
    gc = q.get("GC=F", {})
    return {
        "last": safe_float(gc.get("regularMarketPrice") or gc.get("postMarketPrice")),
        "change": safe_float(gc.get("regularMarketChange")),
        "change_pct": safe_float(gc.get("regularMarketChangePercent")),
        "source": "Yahoo"
    }

# ------------------------ Correlações: metais (SI=F, HG=F) ------------------------

def yahoo_correlation_inputs() -> Dict[str, Any]:
    """
    Correlações mais pertinentes a metais:
      - Prata (SI=F)
      - Cobre (HG=F)
    Obs.: DXY e juros reais já vêm do FRED (macro_from_fred).
    """
    q = yahoo_quote_symbols(["SI=F", "HG=F"])
    si = q.get("SI=F", {})
    hg = q.get("HG=F", {})
    return {
        "silver": {"symbol": "SI=F", "last": safe_float(si.get("regularMarketPrice")), "source": "Yahoo"},
        "copper": {"symbol": "HG=F", "last": safe_float(hg.get("regularMarketPrice")), "source": "Yahoo"},
    }

# ------------------------ SEC EDGAR: AISC amostra ------------------------

SEC_TICKER_INDEX_URL = "https://www.sec.gov/files/company_tickers.json"

def _sec_headers() -> dict:
    ua = os.environ.get("SEC_USER_AGENT") or "metals-reports (contact: you@example.com)"
    return {"User-Agent": ua, "Accept-Encoding":"gzip, deflate", "Host":"data.sec.gov"}

def _get_ticker_index() -> Dict[str,str]:
    r = http_get(SEC_TICKER_INDEX_URL, headers=_sec_headers(), timeout=30)
    if not r:
        return {}
    try:
        data = r.json()
        return {v["ticker"].upper(): f'{int(v["cik_str"]):010d}' for v in data.values()}
    except Exception:
        return {}

def _sec_company_submissions(cik10: str) -> Optional[dict]:
    url = f"https://data.sec.gov/submissions/CIK{cik10}.json"
    r = http_get(url, headers=_sec_headers(), timeout=30)
    if not r:
        return None
    try:
        return r.json()
    except Exception:
        return None

def _build_filing_url(cik10: str, accession_no: str, primary_doc: str) -> str:
    return (
        f"https://www.sec.gov/Archives/edgar/data/{int(cik10)}/"
        f"{accession_no.replace('-', '')}/"
        f"{primary_doc}"
    )

_AISC_PATTERNS = [
    r"all[- ]in sustaining (?:costs?|AISC)[^$%]{0,80}\$?\s*([0-9]{3,5})(?:\s*/?\s*oz|\s*per\s*oz)",
    r"AISC[^$%]{0,80}\$?\s*([0-9]{3,5})(?:\s*/?\s*oz|\s*per\s*oz)"
]

def _extract_aisc_usd_per_oz_from_text(text: str) -> Optional[float]:
    t = re.sub(r"\s+", " ", text, flags=re.S)
    for pat in _AISC_PATTERNS:
        m = re.search(pat, t, flags=re.I)
        if m:
            try:
                val = int(m.group(1).replace(",", ""))
                if 300 <= val <= 3000:
                    return float(val)
            except Exception:
                pass
    return None

def _fetch_text(url: str) -> Optional[str]:
    h = _sec_headers().copy()
    h["Accept"] = "text/html,*/*"
    r = http_get(url, headers=h, timeout=30)
    if not r:
        return None
    return r.text

def edgar_latest_aisc_for_tickers(tickers=("NEM","GOLD"), forms=("10-Q","10-K"), max_check=8) -> Dict[str, Any]:
    out = {}
    idx = _get_ticker_index()
    for tk in tickers:
        cik10 = idx.get(tk.upper())
        if not cik10:
            out[tk.upper()] = {"aisc_usd_oz": None, "source_url": None}
            continue
        subs = _sec_company_submissions(cik10)
        if not subs or "filings" not in subs or "recent" not in subs["filings"]:
            out[tk.upper()] = {"aisc_usd_oz": None, "source_url": None}
            continue
        recent = subs["filings"]["recent"]
        forms_list   = recent.get("form", [])
        acc_nums     = recent.get("accessionNumber", [])
        primary_docs = recent.get("primaryDocument", [])
        got = False
        for i, f in enumerate(forms_list[:max_check]):
            if f not in forms: 
                continue
            if i >= len(acc_nums) or i >= len(primary_docs):
                continue
            url = _build_filing_url(cik10, acc_nums[i], primary_docs[i])
            html_txt = _fetch_text(url)
            if not html_txt:
                continue
            val = _extract_aisc_usd_per_oz_from_text(html_txt)
            if val:
                out[tk.upper()] = {"aisc_usd_oz": val, "source_url": url}
                got = True
                break
        if not got:
            out[tk.upper()] = {"aisc_usd_oz": None, "source_url": None}
    return out

# ------------------------ USGS: Produção mundial (MCS XLSX) ------------------------

USGS_GOLD_PAGE = "https://www.usgs.gov/centers/nmic/gold-statistics-and-information"

def find_latest_usgs_gold_xlsx() -> Optional[str]:
    r = http_get(USGS_GOLD_PAGE, timeout=40)
    if not r:
        return None
    soup = BeautifulSoup(r.text, "lxml")
    candidates = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        href_l = href.lower()
        if href_l.endswith(".xlsx") and ("gold" in href_l or "au" in href_l) and ("mcs" in href_l or "commodity" in href_l):
            candidates.append(href)
    norm = []
    for h in candidates:
        if h.startswith("http"):
            norm.append(h)
        else:
            norm.append("https://www.usgs.gov" + h if h.startswith("/") else "https://www.usgs.gov/" + h)
    def extract_year(s: str) -> int:
        m = re.search(r"(20\d{2})", s)
        return int(m.group(1)) if m else 0
    norm.sort(key=lambda s: extract_year(s), reverse=True)
    return norm[0] if norm else None

def parse_world_production_from_xlsx(xlsx_bytes: bytes) -> Optional[Dict[str, Any]]:
    try:
        xl = pd.ExcelFile(io.BytesIO(xlsx_bytes))
    except Exception:
        return None
    target_patterns = [r"world.*mine.*production", r"world.*production", r"world.*total"]
    for sheet in xl.sheet_names:
        try:
            df = xl.parse(sheet, header=None)
        except Exception:
            continue
        df_str = df.astype(str).applymap(lambda x: x.strip().lower())
        for ridx in range(df_str.shape[0]):
            row_text = " ".join(df_str.iloc[ridx, :].tolist())
            if any(re.search(p, row_text) for p in target_patterns):
                row_vals = df.iloc[ridx, :].tolist()
                for val in reversed(row_vals):
                    try:
                        num = float(str(val).replace(",", "").replace(" ", ""))
                        if num > 0:
                            return {"sheet": sheet, "row": ridx, "world_mine_production_tonnes": num}
                    except Exception:
                        continue
    return None

def usgs_world_gold_production() -> Optional[Dict[str, Any]]:
    try:
        xlsx_url = find_latest_usgs_gold_xlsx()
        if not xlsx_url:
            return None
        r = http_get(xlsx_url, timeout=60)
        if not r:
            return None
        parsed = parse_world_production_from_xlsx(r.content)
        if parsed:
            parsed["xlsx_url"] = xlsx_url
            parsed["source"] = "USGS MCS (XLSX)"
            return parsed
    except Exception:
        return None
    return None

# ------------------------ LLM (Groq) & Telegram ------------------------

def build_prompt(data_str: str, numero: int, metrics: Dict[str, Any], label: str) -> str:
    header = f"Dados — Ouro (XAU) — {data_str} — {label} — Nº {numero}"
    rules = (
        "Você é um analista sênior de metais. Escreva em português (Brasil), tom institucional, conciso e acionável.\n"
        "TÍTULO (linha única):\n" + header + "\n\n"
        "REGRAS:\n"
        "- Use APENAS os dados fornecidos em JSON (não invente números). Se faltar dado, descreva qualitativamente.\n"
        "- Estrutura fixa (10 seções, nesta ordem):\n"
        "  1) Preço Spot (XAU/USD) & Origem do Dado\n"
        "  2) Fluxos em ETFs (GLD/IAU/SLV)\n"
        "  3) Posição em Futuros (CFTC COT)\n"
        "  4) Reservas de Bancos Centrais\n"
        "  5) Produção/Oferta (World Bank/USGS)\n"
        "  6) Whale Ratio (proxy via COT + ETFs)\n"
        "  7) Drivers Macro — real yield, DXY, PIB/inflação globais\n"
        "  8) Estrutura a Termo (GC=F)\n"
        "  9) Correlações Cruzadas — DXY (FRED), prata (SI=F) e cobre (HG=F)\n"
        " 10) Conclusão — 4–6 bullets executivos\n\n"
        "DADOS (JSON):\n"
    )
    return rules + json.dumps(metrics, ensure_ascii=False, indent=2)

def groq_generate(prompt: str) -> Optional[str]:
    key = os.environ.get("GROQ_API_KEY","")
    if not key:
        return None
    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    payload = {
        "model": "llama-3.1-70b-versatile",
        "messages": [
            {"role":"system","content":"Você é um analista financeiro sênior e escreve em português do Brasil, com precisão e sobriedade."},
            {"role":"user","content": prompt}
        ],
        "temperature": 0.35,
        "max_tokens": 1800
    }
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=120)
        if r.status_code == 200:
            return r.json()["choices"][0]["message"]["content"]
    except Exception:
        return None
    return None

def chunk_text(text: str, limit: int = 3900) -> List[str]:
    parts: List[str] = []
    for block in text.split("\n\n"):
        b = block.strip()
        if not b:
            continue
        if len(b) <= limit:
            if not parts: parts.append(b)
            elif len(parts[-1]) + 2 + len(b) <= limit: parts[-1] += "\n\n" + b
            else: parts.append(b)
        else:
            acc = ""
            for line in b.splitlines():
                if len(acc) + len(line) + 1 <= limit:
                    acc += (("\n" if acc else "") + line)
                else:
                    if acc: parts.append(acc)
                    acc = line
            if acc: parts.append(acc)
    return parts if parts else ["(vazio)"]

def telegram_send(html_text: str) -> bool:
    token = os.environ.get("TELEGRAM_BOT_TOKEN","")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID_METALS","")
    if not token or not chat_id:
        return False
    base = f"https://api.telegram.org/bot{token}/sendMessage"
    parts = chunk_text(html_text, limit=3900)
    ok = True
    for msg in parts:
        data = {"chat_id": chat_id, "text": msg, "disable_web_page_preview": True, "parse_mode": "HTML"}
        try:
            resp = requests.post(base, data=data, timeout=60)
            ok = ok and (resp.status_code == 200)
            time.sleep(0.6)
        except Exception:
            ok = False
    return ok

# ------------------------ MAIN ------------------------

def main():
    import argparse
    ap = argparse.ArgumentParser(description="Relatório Metals — OURO (diário/semanal/mensal)")
    ap.add_argument("--period", choices=["daily","weekly","monthly"], default="daily")
    ap.add_argument("--send", choices=["yes","no"], default="yes")
    args = ap.parse_args()

    # Anti-duplicação por período
    if guard_already_sent(args.period):
        print("[skip] Relatório já enviado hoje para", args.period)
        return

    data_str = today_brt_str_long()
    label_map = {"daily":"Diário","weekly":"Semanal","monthly":"Mensal"}
    label = label_map.get(args.period, "Diário")

    # 1) Spot XAUUSD com fallback
    spot, spot_source = get_spot_xauusd()

    # 2) ETFs GLD/IAU/SLV (proxy de fluxos)
    etfs = etf_flows_proxy()

    # 3) COT (Nasdaq Data Link)
    cot = cftc_cot_gold_net() or {"available": False, "note": "Sem leitura (COT) nesta execução."}

    # 4) Reservas & tonelagem (World Bank)
    wb_res = wb_central_bank_gold_value_usd()
    gold_tonnes = wb_gold_value_to_tonnes(wb_res.get("gold_value_usd"), spot) if spot and wb_res.get("gold_value_usd") else None

    # 5) Produção/Oferta — USGS (MCS XLSX)
    usgs_prod = usgs_world_gold_production() or {"world_mine_production_tonnes": None, "source": "USGS (indisponível nesta execução)"}

    # 6) Whale Ratio (proxy via COT + ETFs)
    whale_proxy = {
        "method": "Proxy COT (non-commercial vs commercial) + direção de ETFs (GLD/IAU)",
        "available_cot": bool(cot and cot.get("available", False)),
        "source": ["CFTC/Nasdaq DL", "Yahoo"]
    }

    # 7) Drivers macro (FRED + WB)
    fred = macro_from_fred()
    wb_macro = wb_global_macro_drivers()

    # 8) Estrutura a termo (GC=F)
    term = yahoo_gc_future()

    # 9) Correlações (inputs) — metais
    corr = yahoo_correlation_inputs()

    # 10) AISC (amostra via EDGAR)
    try:
        tickers = [t.strip() for t in os.environ.get("AISC_TICKERS","NEM,GOLD").split(",") if t.strip()]
        aisc = edgar_latest_aisc_for_tickers(tickers=tickers)
        aisc_vals = [v["aisc_usd_oz"] for v in aisc.values() if v.get("aisc_usd_oz")]
        aisc_avg = round(sum(aisc_vals)/len(aisc_vals),2) if aisc_vals else None
    except Exception:
        aisc, aisc_avg = {}, None

    metrics = {
        "as_of_date_brt": ymd(today_brt()),
        "spot": {"xauusd": spot, "unit": "USD/oz", "source": spot_source},
        "etf_flows_proxy": etfs,
        "futures_cot": cot,
        "central_bank_reserves": {
            "gold_value_usd": wb_res.get("gold_value_usd"),
            "gold_value_usd_date_total": wb_res.get("date_total"),
            "gold_value_usd_date_exgold": wb_res.get("date_exgold"),
            "gold_implied_tonnes": gold_tonnes,
            "source": wb_res.get("source")
        },
        "supply_proxy": {
            "world_mine_production_tonnes": usgs_prod.get("world_mine_production_tonnes"),
            "usgs_sheet": usgs_prod.get("sheet"),
            "usgs_xlsx_url": usgs_prod.get("xlsx_url"),
            "source": usgs_prod.get("source", "USGS")
        },
        "whale_ratio_proxy": whale_proxy,
        "macro_fred": fred,
        "macro_world_bank": wb_macro,
        "term_structure_gc": term,
        "correlation_inputs": corr,
        "production_cost": {
            "aisc_samples": aisc,
            "aisc_avg_usd_oz": aisc_avg,
            "source": "SEC EDGAR (10-Q/10-K, regex)"
        }
    }

    numero = int(datetime.now().timestamp()) % 100000
    prompt = build_prompt(data_str, numero, metrics, label)
    content = groq_generate(prompt) or "⚠️ Não foi possível gerar a interpretação automática hoje. Utilize as métricas acima como base."

    titulo = f"📊 <b>Dados — Ouro — {data_str} — {label} — Nº {numero}</b>"
    corpo_seguro = html.escape(content, quote=False)
    full_msg = f"{titulo}\n\n{corpo_seguro}"

    if args.send == "yes":
        ok = telegram_send(full_msg)
        print("[telegram]", "ok" if ok else "falhou")
    else:
        print(full_msg)

if __name__ == "__main__":
    main()