# -*- coding: utf-8 -*-
import os
import requests
from typing import List, Dict, Any, Optional

class OpenAICompatClient:
    """
    Cliente base para provedores com API compatível com OpenAI Chat Completions.
    Exige:
      - endpoint (ex.: https://api.openai.com/v1/chat/completions)
      - header Authorization: Bearer <API_KEY>
      - body: { model, messages=[{role, content}], ... }
    """
    def __init__(self,
                 api_key: str,
                 model: str,
                 endpoint: str,
                 timeout: int = 60):
        if not api_key:
            raise RuntimeError("API key ausente para cliente compatível com OpenAI.")
        self.api_key = api_key.strip()
        self.model = model.strip()
        self.endpoint = endpoint.strip()
        self.timeout = timeout

    def chat(self,
             messages: List[Dict[str, str]],
             temperature: float = 0.3,
             max_tokens: Optional[int] = None,
             extra: Optional[Dict[str, Any]] = None) -> str:
        payload: Dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": float(temperature),
        }
        if max_tokens:
            payload["max_tokens"] = int(max_tokens)
        if extra:
            payload.update(extra)

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        resp = requests.post(self.endpoint, headers=headers, json=payload, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        try:
            return data["choices"][0]["message"]["content"]
        except Exception:
            return str(data)
