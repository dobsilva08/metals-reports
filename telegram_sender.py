import os
import re
import html
import requests


def _plain_text(message: str) -> str:
    # Convert report HTML markup into safe Telegram plain text fallback.
    text = re.sub(r"</?(?:b|strong|i|em|u|s|strike|code|pre)>", "", message, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    return html.unescape(text)


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

    def request_send(body):
        try:
            response = requests.post(url, json=body, timeout=30)
        except requests.RequestException as exc:
            raise RuntimeError(f"Erro de conexão com Telegram: {exc}") from exc
        try:
            result = response.json()
        except ValueError:
            result = {"ok": False, "description": response.text[:1000]}
        return response, result

    response, result = request_send(payload)
    if response.ok and result.get("ok"):
        return result

    description = result.get("description", response.text[:1000])
    error_code = result.get("error_code", response.status_code)
    print(f"[Telegram] Primeiro envio rejeitado: HTTP={response.status_code}; code={error_code}; {description}")

    # If HTML markup is rejected, retry once as plain text, preserving the report content.
    if parse_mode and parse_mode.upper() == "HTML":
        fallback = {
            "chat_id": chat_id,
            "text": _plain_text(message),
            "disable_web_page_preview": True,
        }
        response, result = request_send(fallback)
        if response.ok and result.get("ok"):
            print("[Telegram] Envio de fallback plain text concluído.")
            return result
        description = result.get("description", response.text[:1000])
        error_code = result.get("error_code", response.status_code)

    raise RuntimeError(
        f"Telegram API rejeitou a mensagem. HTTP={response.status_code}; "
        f"error_code={error_code}; description={description}"
    )
