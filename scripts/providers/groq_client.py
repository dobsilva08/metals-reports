# -*- coding: utf-8 -*-
import os
from typing import Optional
from .base_openai_compat import OpenAICompatClient

GROQ_ENDPOINT = "https://api.groq.com/openai/v1/chat/completions"

class GroqClient(OpenAICompatClient):
    def __init__(self,
                 api_key: Optional[str] = None,
                 model: Optional[str] = None,
                 timeout: int = 60):
        api_key = (api_key or os.environ.get("GROQ_API_KEY", "")).strip()
        model = (model or os.environ.get("GROQ_MODEL", "llama-3.1-70b-versatile")).strip()
        if not api_key:
            raise RuntimeError("GROQ_API_KEY ausente.")
        super().__init__(api_key=api_key, model=model, endpoint=GROQ_ENDPOINT, timeout=timeout)
