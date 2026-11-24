# Coleta unificada de metais via Yahoo Finance, com retries e validação.
import json
import math
import time
from datetime import datetime, timezone

import yfinance as yf

METALS = {
    "OURO": "GC=F",
    "PRATA": "SI=F",
    "COBRE": "HG=F",
    "PLATINA": "PL=F",
    "PALADIO": "PA=F",
    "NIQUEL": "NIY=F",
    "ETF_LITIO": "LIT",
}

RETRIES = 3
RETRY_DELAY = 2


def safe_round(value, digits=2):
    try:
        value = float(value)
        if not math.isfinite(value):
            return None
        return round(value, digits)
    except (TypeError, ValueError, OverflowError):
        return None


def fetch_metal_data(symbol, ticker):
    last_error = None
    for attempt in range(1, RETRIES + 1):
        try:
            hist = yf.Ticker(ticker).history(period="10d", auto_adjust=False, timeout=20)
            if hist is None or hist.empty or "Close" not in hist.columns:
                raise ValueError("Yahoo Finance retornou histórico vazio")
            hist = hist.dropna(subset=["Close"])
            if hist.empty:
                raise ValueError("Histórico sem preços de fechamento válidos")

            current = hist.iloc[-1]
            current_price = safe_round(current["Close"])
            first_price = safe_round(hist.iloc[0]["Close"])
            if current_price is None or first_price in (None, 0):
                raise ValueError("Preço atual ou base histórica inválida")

            open_value = safe_round(current.get("Open"))
            change_24h = (
                safe_round(((current_price - open_value) / open_value) * 100)
                if open_value not in (None, 0) else None
            )
            change_7d = safe_round(((current_price - first_price) / first_price) * 100)

            return {
                "ticker": ticker,
                "price": current_price,
                "change_24h": change_24h,
                "change_7d": change_7d,
                "high": safe_round(current.get("High")),
                "low": safe_round(current.get("Low")),
                "volume": safe_round(current.get("Volume"), 0),
            }
        except Exception as exc:
            last_error = exc
            print(f"[AVISO] {symbol} tentativa {attempt}/{RETRIES}: {exc}")
            if attempt < RETRIES:
                time.sleep(RETRY_DELAY * attempt)

    raise RuntimeError(f"Falha final em {symbol} ({ticker}): {last_error}")


def collect_all():
    now = datetime.now(timezone.utc)
    snapshot = {
        "date": now.strftime("%d/%m/%Y"),
        "time": now.strftime("%H:%M UTC"),
        "metals": {},
        "collection_errors": {},
    }

    print("[COLETA] Iniciando coleta de metais...")
    for name, ticker in METALS.items():
        try:
            data = fetch_metal_data(name, ticker)
            snapshot["metals"][name] = data
            print(f"[OK] {name} coletado: {data['price']}")
        except Exception as exc:
            snapshot["collection_errors"][name] = str(exc)
            print(f"[ERRO] {name}: {exc}")

    print(f"[COLETA] Sucesso: {len(snapshot['metals'])}/{len(METALS)}")
    print(f"[COLETA] Falhas: {len(snapshot['collection_errors'])}")

    # Permite relatório parcial, mas evita que uma coleta quase totalmente vazia passe.
    if len(snapshot["metals"]) < 2:
        raise RuntimeError(
            f"Coleta insuficiente: somente {len(snapshot['metals'])} ativo(s) válido(s). "
            f"Falhas: {snapshot['collection_errors']}"
        )

    with open("snapshot.json", "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)

    return snapshot


if __name__ == "__main__":
    print(json.dumps(collect_all(), indent=2, ensure_ascii=False))
