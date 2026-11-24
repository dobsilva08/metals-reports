# telegram_sender.py
# Envio Telegram via Bot API com validação da resposta.
import os
import requests


def send(message: str, parse_mode: str = "HTML"):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID_METALS")
    if not token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN não definido")
    if not chat_id:
        raise RuntimeError("TELEGRAM_CHAT_ID_METALS não definido")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    if len(message) > 4096:
        message = message[:4090] + "\n[...]"
    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }

    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()
    except requests.RequestException as exc:
        raise RuntimeError(f"Erro de conexão com Telegram: {exc}") from exc
    except ValueError as exc:
        raise RuntimeError("Telegram retornou resposta inválida (não JSON).") from exc

    if not result.get("ok"):
        raise RuntimeError(f"Telegram não confirmou o envio: {result}")
    return result
