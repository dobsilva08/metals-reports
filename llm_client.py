# llm_client.py
# Cliente LLM unificado com fallback: PiAPI -> Groq -> OpenAI -> DeepSeek

import os
import requests
from typing import Optional, List, Dict, Any

SUPPORTED = ["piapi", "groq", "openai", "deepseek"]

ENDPOINTS = {
    "piapi":    "https://api.piapi.ai/v1/chat/completions",
    "groq":     "https://api.groq.com/openai/v1/chat/completions",
    "openai":   "https://api.openai.com/v1/chat/completions",
    "deepseek": "https://api.deepseek.com/v1/chat/completions",
}

ENV_KEYS = {
    "piapi":    ("PIAPI_API_KEY",    "PIAPI_MODEL",    "gpt-4o-mini"),
    "groq":     ("GROQ_API_KEY",     "GROQ_MODEL",     "llama-3.1-70b-versatile"),
    "openai":   ("OPENAI_API_KEY",   "OPENAI_MODEL",   "gpt-4o-mini"),
    "deepseek": ("DEEPSEEK_API_KEY", "DEEPSEEK_MODEL", "deepseek-chat"),
}


def _call_provider(provider, messages, temperature=0.3, max_tokens=None):
    key_env, model_env, default_model = ENV_KEYS[provider]
    api_key = os.environ.get(key_env, "").strip()
    model = os.environ.get(model_env, default_model).strip()
    if not api_key:
        raise RuntimeError(f"[{provider}] API key ausente ({key_env})")
    payload = {"model": model, "messages": messages, "temperature": temperature}
    if max_tokens:
        payload["max_tokens"] = max_tokens
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    resp = requests.post(ENDPOINTS[provider], json=payload, headers=headers, timeout=90)
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]


class LLMClient:
    def __init__(self, provider=None):
        self._order = [provider] if provider else list(SUPPORTED)
        self._current = 0

    @property
    def active_provider(self):
        return self._order[self._current]

    def generate(self, system, user, temperature=0.3, max_tokens=None):
        messages = [{"role": "system", "content": system}, {"role": "user", "content": user}]
        errors = []
        for i, provider in enumerate(self._order):
            try:
                self._current = i
                result = _call_provider(provider, messages, temperature, max_tokens)
                print(f"  [LLM] Provider usado: {provider}")
                return result
            except Exception as e:
                print(f"  [LLM] {provider} falhou: {e}")
                errors.append(f"{provider}: {e}")
        raise RuntimeError("Todos os providers falharam:\n" + "\n".join(errors))
