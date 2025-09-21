# -*- coding: utf-8 -*-
import os
from typing import Optional
from .base_openai_compat import OpenAICompatClient

DEEPSEEK_ENDPOINT = "https://api.deepseek.com/v1/chat/completions"

class DeepSeekClient(OpenAICompatClient):
    def __init__(self,
                 api_key: Optional[str] = None,
                 model: Optional[str] = None,
                 timeout: int = 60):
        api_key = (api_key or os.environ.get("DEEPSEEK_API_KEY", "")).strip()
        model = (model or os.environ.get("DEEPSEEK_MODEL", "deepseek-chat")).strip()
        if not api_key:
            raise RuntimeError("DEEPSEEK_API_KEY ausente.")
        super().__init__(api_key=api_key, model=model, endpoint=DEEPSEEK_ENDPOINT, timeout=timeout)
