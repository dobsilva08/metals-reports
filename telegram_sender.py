# telegram_sender.py
# Envia mensagens para o Telegram via Bot API

import os
import requests


def send(message: str, parse_mode: str = "HTML"):

      token = os.getenv("TELEGRAM_BOT_TOKEN")
      chat_id = os.getenv("TELEGRAM_CHAT_ID")

    if not token:
              raise RuntimeError("TELEGRAM_BOT_TOKEN nao definido nos secrets do GitHub")

    if not chat_id:
              raise RuntimeError("TELEGRAM_CHAT_ID nao definido nos secrets do GitHub")

    url = f"https://api.telegram.org/bot{token}/sendMessage"

    # Telegram possui limite de 4096 caracteres
    if len(message) > 4096:
              message = message[:4090] + "\n[...]"

    payload = {
              "chat_id": chat_id,
              "text": message,
              "parse_mode": parse_mode,
              "disable_web_page_preview": True
    }

    response = requests.post(url, json=payload, timeout=30)

    if response.status_code != 200:
              raise RuntimeError(
                            f"Erro ao enviar ao Telegram: {response.status_code} - {response.text}"
              )

    print(f"  [Telegram] Mensagem enviada com sucesso ({len(message)} chars)")
    return response.json()
