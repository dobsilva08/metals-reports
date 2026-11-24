# Envio Telegram com diagnóstico detalhado da API.
import os
import requests


def send(message: str, parse_mode: str = "HTML"):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID_METALS")
    if not token or not chat_id:
        raise RuntimeError("TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID_METALS devem estar configurados")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    if len(message) > 4096:
        raise ValueError(f"Mensagem excede o limite do Telegram ({len(message)} caracteres)")

    payload = {
        "chat_id": chat_id,
        "text": message,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }

    try:
        response = requests.post(url, json=payload, timeout=30)
    except requests.RequestException as exc:
        raise RuntimeError(f"Erro de conexão com Telegram: {exc}") from exc

    # Leia o corpo antes de levantar HTTPError; a API normalmente explica o motivo do 400.
    try:
        result = response.json()
    except ValueError:
        result = {"ok": False, "description": response.text[:1000]}

    if not response.ok or not result.get("ok"):
        description = result.get("description", response.text[:1000])
        error_code = result.get("error_code", response.status_code)
        raise RuntimeError(
            f"Telegram API rejeitou a mensagem. HTTP={response.status_code}; "
            f"error_code={error_code}; description={description}"
        )

    return result
