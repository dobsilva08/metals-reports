# Envio Telegram com validação e tratamento de falhas.
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
        "chat_id": chat_id, "text": message, "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }
    try:
        response = requests.post(url, json=payload, timeout=30)
        response.raise_for_status()
        result = response.json()
    except requests.RequestException as exc:
        raise RuntimeError(f"Erro de conexão com Telegram: {exc}") from exc
    except ValueError as exc:
        raise RuntimeError("Telegram retornou resposta inválida (não JSON)") from exc

    if not result.get("ok"):
        raise RuntimeError(f"Telegram não confirmou o envio: {result}")
    return result
