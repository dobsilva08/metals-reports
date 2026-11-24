import os
import re
import html
import requests

_MIGRATED_CHAT_ID = None


def _plain_text(message: str) -> str:
    text = re.sub(r"</?(?:b|strong|i|em|u|s|strike|code|pre)>", "", message, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    return html.unescape(text)


def send(message: str, parse_mode: str = "HTML"):
    global _MIGRATED_CHAT_ID
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    original_chat_id = os.getenv("TELEGRAM_CHAT_ID_METALS")
    chat_id = _MIGRATED_CHAT_ID or original_chat_id
    if not token or not original_chat_id:
        raise RuntimeError("TELEGRAM_BOT_TOKEN e TELEGRAM_CHAT_ID_METALS devem estar configurados")

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    if len(message) > 4096:
        raise ValueError(f"Mensagem excede o limite do Telegram ({len(message)} caracteres)")

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

    def payload(target, text, mode):
        p = {"chat_id": target, "text": text, "disable_web_page_preview": True}
        if mode:
            p["parse_mode"] = mode
        return p

    response, result = request_send(payload(chat_id, message, parse_mode))
    if response.ok and result.get("ok"):
        return result

    migrate_id = (result.get("parameters") or {}).get("migrate_to_chat_id")
    if migrate_id:
        _MIGRATED_CHAT_ID = str(migrate_id)
        print("[Telegram] Migração para supergrupo detectada; ID atualizado em memória.")
        response, result = request_send(payload(_MIGRATED_CHAT_ID, message, parse_mode))
        if response.ok and result.get("ok"):
            print("[Telegram] Envio para supergrupo migrado concluído.")
            return result

    description = result.get("description", response.text[:1000])
    error_code = result.get("error_code", response.status_code)
    print(f"[Telegram] Envio rejeitado: HTTP={response.status_code}; code={error_code}; {description}")

    if parse_mode and parse_mode.upper() == "HTML":
        response, result = request_send(payload(chat_id, _plain_text(message), None))
        if response.ok and result.get("ok"):
            print("[Telegram] Envio fallback plain text concluído.")
            return result

    description = result.get("description", response.text[:1000])
    error_code = result.get("error_code", response.status_code)
    raise RuntimeError(f"Telegram API rejeitou a mensagem. HTTP={response.status_code}; error_code={error_code}; description={description}")
