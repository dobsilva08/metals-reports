# collector_unified.py
# Coleta unificada de metais via Yahoo Finance, com validações e timestamps UTC.
import json
from datetime import datetime, timezone

import yfinance as yf

METALS = {
    "OURO": "GC=F",
    "PRATA": "SI=F",
    "COBRE": "HG=F",
    "PLATINA": "PL=F",
    "PALADIO": "PA=F",
    "NIQUEL": "NIY=F",
    # LIT é ETF de empresas de lítio/baterias, não preço spot do lítio.
    "ETF_LITIO": "LIT",
}


def safe_round(value, digits=2):
    try:
        result = round(float(value), digits)
        return result if result == result and abs(result) != float("inf") else None
    except (TypeError, ValueError, OverflowError):
        return None


def fetch_metal_data(symbol, ticker):
    hist = yf.Ticker(ticker).history(period="7d", auto_adjust=False)
    if hist is None or hist.empty or "Close" not in hist:
        print(f"[AVISO] Sem histórico para {symbol} ({ticker})")
        return None

    hist = hist.dropna(subset=["Close"])
    if hist.empty:
        return None

    current = hist.iloc[-1]
    first = hist.iloc[0]["Close"]
    current_price = safe_round(current["Close"])
    first_price = safe_round(first)

    if current_price is None or first_price in (None, 0):
        return None

    change_7d = safe_round(((current_price - first_price) / first_price) * 100)
    open_value = safe_round(current.get("Open"))
    change_24h = safe_round(((current_price - open_value) / open_value) * 100) if open_value else None

    return {
        "ticker": ticker,
        "price": current_price,
        "change_24h": change_24h,
        "change_7d": change_7d,
        "high": safe_round(current.get("High")),
        "low": safe_round(current.get("Low")),
        "volume": safe_round(current.get("Volume"), 0),
    }


def collect_all():
    now = datetime.now(timezone.utc)
    snapshot = {
        "date": now.strftime("%d/%m/%Y"),
        "time": now.strftime("%H:%M UTC"),
        "metals": {},
    }

    print("[COLETA] Iniciando coleta de metais...")
    for name, ticker in METALS.items():
        try:
            data = fetch_metal_data(name, ticker)
            if data:
                snapshot["metals"][name] = data
                print(f"[OK] {name} coletado")
            else:
                print(f"[AVISO] {name}: sem dados válidos")
        except Exception as exc:
            print(f"[ERRO] {name}: {exc}")

    with open("snapshot.json", "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)

    return snapshot


if __name__ == "__main__":
    print(json.dumps(collect_all(), indent=2, ensure_ascii=False))
