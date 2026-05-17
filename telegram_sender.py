# telegram_sender.py
# Envio Telegram via Bot API

import os
import requests


def send(message: str, parse_mode: str = "HTML"):

    token = os.getenv("TELEGRAM_BOT_TOKEN")

    chat_id = os.getenv(
        "TELEGRAM_CHAT_ID_METALS"
    )

    if not token:

        raise RuntimeError(
            "TELEGRAM_BOT_TOKEN nao definido"
        )

    if not chat_id:

        raise RuntimeError(
            "TELEGRAM_CHAT_ID_METALS nao definido"
        )

    url = (
        f"https://api.telegram.org/bot"
        f"{token}/sendMessage"
    )

    # Limite Telegram
    if len(message) > 4096:
        message = message[:4090] + "\n[...]"

    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True
    }

    response = requests.post(
        url,
        json=payload,
        timeout=30
    )

    if response.status_code != 200:

        raise RuntimeError(
            f"Erro Telegram: "
            f"{response.status_code} - "
            f"{response.text}"
        )

    return response.json()
