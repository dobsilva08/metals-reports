# collector_unified.py
# Coleta unificada de metais via Yahoo Finance

import json
from datetime import datetime, timezone

import yfinance as yf


# --------------------------------------------------
# CONFIG
# --------------------------------------------------

METALS = {
    "OURO": "GC=F",
    "PRATA": "SI=F",
    "COBRE": "HG=F",
    "PLATINA": "PL=F",
    "PALADIO": "PA=F",
    "NIQUEL": "NIY=F",
    "LITIO": "LIT"
}


# --------------------------------------------------
# FORMATADORES
# --------------------------------------------------

def safe_round(value, digits=2):

    try:
        return round(float(value), digits)
    except Exception:
        return None


# --------------------------------------------------
# COLETA
# --------------------------------------------------

def fetch_metal_data(symbol, ticker):

    asset = yf.Ticker(ticker)

    hist = asset.history(period="7d")

    if hist.empty:
        return None

    current = hist.iloc[-1]

    first = hist.iloc[0]["Close"]

    current_price = safe_round(current["Close"])

    change_7d = safe_round(
        ((current_price - first) / first) * 100
    )

    prev_close = current.get("Open")

    if prev_close:
        change_24h = safe_round(
            ((current_price - prev_close) / prev_close) * 100
        )
    else:
        change_24h = None

    return {
        "ticker": ticker,
        "price": current_price,
        "change_24h": change_24h,
        "change_7d": change_7d,
        "high": safe_round(current.get("High")),
        "low": safe_round(current.get("Low")),
        "volume": safe_round(current.get("Volume"), 0)
    }


# --------------------------------------------------
# SNAPSHOT COMPLETO
# --------------------------------------------------

def collect_all():

    snapshot = {
        "date": datetime.now(
            timezone.utc
        ).strftime("%d/%m/%Y"),

        "time": datetime.now(
            timezone.utc
        ).strftime("%H:%M UTC"),

        "metals": {}
    }

    print("[1/1] Coletando metais...")

    for name, ticker in METALS.items():

        try:

            data = fetch_metal_data(
                name,
                ticker
            )

            if data:
                snapshot["metals"][name] = data

                print(
                    f"[OK] {name} coletado"
                )

        except Exception as e:

            print(
                f"[ERRO] {name}: {e}"
            )

    with open(
        "snapshot.json",
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            snapshot,
            f,
            indent=2,
            ensure_ascii=False
        )

    return snapshot


# --------------------------------------------------
# TESTE LOCAL
# --------------------------------------------------

if __name__ == "__main__":

    data = collect_all()

    print(
        json.dumps(
            data,
            indent=2,
            ensure_ascii=False
        )
    )
