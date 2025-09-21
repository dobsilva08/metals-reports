# -*- coding: utf-8 -*-
import os
from typing import List, Dict, Optional

from .piapi_client import PiAPIClient

class LLMClient:
    """
    Cliente unificado de LLM:
      - Seleciona o provedor via env LLM_PROVIDER (ex.: 'piapi') ou pela presença de chaves.
      - Para este setup, focamos em 'piapi'.
    """
    def __init__(self, provider: Optional[str] = None):
        self.provider = (provider or os.environ.get("LLM_PROVIDER", "")).strip().lower()
        self._client = None

        # Seleção automática se provider não especificado:
        if not self.provider:
            if os.environ.get("PIAPI_API_KEY"):
                self.provider = "piapi"

        if self.provider == "piapi":
            self._client = PiAPIClient()
        else:
            raise RuntimeError(
                f"Provedor LLM não suportado ou não configurado: '{self.provider}'. "
                f"Defina LLM_PROVIDER=piapi e PIAPI_API_KEY nos Secrets."
            )

    def generate(self,
                 system_prompt: str,
                 user_prompt: str,
                 temperature: float = 0.3,
                 max_tokens: Optional[int] = None) -> str:
        messages: List[Dict[str, str]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_prompt})
        return self._client.chat(messages, temperature=temperature, max_tokens=max_tokens)
