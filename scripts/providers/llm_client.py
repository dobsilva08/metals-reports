# -*- coding: utf-8 -*-
import os
from typing import List, Dict, Optional, Callable, Tuple

from .piapi_client import PiAPIClient
from .groq_client import GroqClient
from .openai_client import OpenAIClient
from .deepseek_client import DeepSeekClient

SUPPORTED = {"piapi", "groq", "openai", "deepseek"}

def _build_client(provider: str):
    provider = provider.lower().strip()
    if provider == "piapi":
        return PiAPIClient()
    elif provider == "groq":
        return GroqClient()
    elif provider == "openai":
        return OpenAIClient()
    elif provider == "deepseek":
        return DeepSeekClient()
    else:
        raise RuntimeError(f"Provider desconhecido: {provider}")

class LLMClient:
    """
    Estratégia:
      1) Usa LLM_PROVIDER (env) como preferência inicial (default: 'piapi').
      2) Fallback em ordem definida por LLM_FALLBACK_ORDER (env),
         ex.: "piapi,groq,openai,deepseek".
      3) Se um provider da ordem não tem chave/env, é ignorado.
      4) Em tempo de execução, se uma chamada levantar erro HTTP/timeout,
         tenta o próximo da fila.
    """
    def __init__(self,
                 provider: Optional[str] = None,
                 fallback_order: Optional[str] = None):
        preferred = (provider or os.environ.get("LLM_PROVIDER", "piapi")).strip().lower()
        if preferred and preferred not in SUPPORTED:
            raise RuntimeError(f"LLM_PROVIDER inválido: {preferred}. Opções: {sorted(SUPPORTED)}")

        order_str = (fallback_order or os.environ.get("LLM_FALLBACK_ORDER", "")).strip()
        if not order_str:
            # ordem padrão
            order = ["piapi", "groq", "openai", "deepseek"]
        else:
            order = [p.strip().lower() for p in order_str.split(",") if p.strip()]

        # Garante que o preferido venha primeiro:
        if preferred in order:
            order.remove(preferred)
        self.providers_order: List[str] = [preferred] + order

        # Filtra por chaves disponíveis (não derruba aqui; deixa para runtime).
        self.available_providers: List[str] = []
        for p in self.providers_order:
            if p == "piapi" and os.environ.get("PIAPI_API_KEY"):
                self.available_providers.append(p)
            elif p == "groq" and os.environ.get("GROQ_API_KEY"):
                self.available_providers.append(p)
            elif p == "openai" and os.environ.get("OPENAI_API_KEY"):
                self.available_providers.append(p)
            elif p == "deepseek" and os.environ.get("DEEPSEEK_API_KEY"):
                self.available_providers.append(p)

        if not self.available_providers:
            raise RuntimeError(
                "Nenhuma chave encontrada para provedores. "
                "Defina ao menos PIAPI_API_KEY (recomendado) ou as demais."
            )

        self._active_name: Optional[str] = None
        self._active_client = None

    @property
    def active_provider(self) -> Optional[str]:
        return self._active_name

    def _ensure_client(self):
        if self._active_client is not None:
            return
        # Inicializa com o primeiro disponível da lista
        for p in self.available_providers:
            try:
                self._active_client = _build_client(p)
                self._active_name = p
                break
            except Exception:
                self._active_client = None
                self._active_name = None
        if self._active_client is None:
            raise RuntimeError("Falha ao inicializar qualquer provedor LLM.")

    def _rotate(self) -> bool:
        """
        Troca para o próximo provedor disponível.
        Retorna True se conseguiu trocar, False se acabou a lista.
        """
        if self._active_name is None:
            return False
        try:
            idx = self.available_providers.index(self._active_name)
        except ValueError:
            idx = -1
        for j in range(idx + 1, len(self.available_providers)):
            candidate = self.available_providers[j]
            try:
                self._active_client = _build_client(candidate)
                self._active_name = candidate
                return True
            except Exception:
                continue
        return False

    def generate(self,
                 system_prompt: str,
                 user_prompt: str,
                 temperature: float = 0.3,
                 max_tokens: Optional[int] = None) -> str:
        """
        Tenta gerar via provider ativo; se falhar (HTTP/timeout/etc.), rota para o próximo.
        """
        messages: List[Dict[str, str]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_prompt})

        self._ensure_client()

        last_err = None
        tried = set()
        while True:
            if self._active_name in tried:
                # evita loop infinito
                break
            tried.add(self._active_name)
            try:
                return self._active_client.chat(messages, temperature=temperature, max_tokens=max_tokens)
            except Exception as e:
                last_err = e
                # tenta próximo provedor
                rotated = self._rotate()
                if not rotated:
                    break

        raise RuntimeError(f"Todos os provedores falharam. Último erro: {last_err}")
