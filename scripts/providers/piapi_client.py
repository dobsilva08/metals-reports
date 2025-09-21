# -*- coding: utf-8 -*-
import os
import requests
from typing import List, Dict, Any, Optional

PIAPI_URL = "https://api.piapi.ai/v1/chat/completions"

class PiAPIClient:
    """
    Cliente mínimo para a PiAPI (Chat Completions compatível).
    Usa:
      - Header: Authorization: Bearer <PIAPI_API_KEY>
      - Body: { model, messages=[{role, content}, ...], ... }
    """
    def __init__(self,
                 api_key: Optional[str] = None,
                 model: Optional[str] = None,
                 timeout: int = 60):
        self.api_key = (api_key or os.environ.get("PIAPI_API_KEY", "")).strip()
        self.model = (model or os.environ.get("PIAPI_MODEL", "gpt-4o-mini")).strip()
        self.timeout = timeout
        if not self.api_key:
            raise RuntimeError("PIAPI_API_KEY não definido. Configure .env ou Secrets do GitHub.")

    def chat(self,
             messages: List[Dict[str, str]],
             temperature: float = 0.3,
             max_tokens: Optional[int] = None,
             stream: bool = False,
             extra: Optional[Dict[str, Any]] = None) -> str:
        """
        messages = [
          {"role": "system", "content": "..."},
          {"role": "user",   "content": "..."}
        ]
        """
        payload: Dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": float(temperature),
        }
        if max_tokens:
            payload["max_tokens"] = int(max_tokens)
        if stream:
            payload["stream"] = True
        if extra:
            payload.update(extra)

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        resp = requests.post(PIAPI_URL, headers=headers, json=payload, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()

        # Espera formato compatível com OpenAI:
        # { "choices": [ { "message": { "role": "assistant", "content": "..." } } ] }
        try:
            return data["choices"][0]["message"]["content"]
        except Exception:
            # Fallback defensivo para debug
            return str(data)
