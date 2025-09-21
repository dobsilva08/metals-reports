# -*- coding: utf-8 -*-
import os
from typing import Optional
from .base_openai_compat import OpenAICompatClient

OPENAI_ENDPOINT = "https://api.openai.com/v1/chat/completions"

class OpenAIClient(OpenAICompatClient):
    def __init__(self,
                 api_key: Optional[str] = None,
                 model: Optional[str] = None,
                 timeout: int = 60):
        api_key = (api_key or os.environ.get("OPENAI_API_KEY", "")).strip()
        model = (model or os.environ.get("OPENAI_MODEL", "gpt-4o-mini")).strip()
        if not api_key:
            raise RuntimeError("OPENAI_API_KEY ausente.")
        super().__init__(api_key=api_key, model=model, endpoint=OPENAI_ENDPOINT, timeout=timeout)
