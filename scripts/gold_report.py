# --- topo do arquivo ---
import os
...
GROQ_API_KEY       = os.getenv("GROQ_API_KEY", "")
DEEPSEEK_API_KEY   = os.getenv("DEEPSEEK_API_KEY", "")
OPENAI_API_KEY     = os.getenv("OPENAI_API_KEY", "")

def llm_generate_with_fallback(prompt: str) -> Optional[str]:
    """
    Tenta Groq -> DeepSeek -> OpenAI. Se todos falharem, retorna None.
    """
    # 1) Groq
    if GROQ_API_KEY:
        try:
            return call_groq(
                api_key=GROQ_API_KEY,
                model="llama-3.1-70b-versatile",
                prompt=prompt
            )
        except Exception as e:
            print(f"[LLM] Groq falhou: {e}")

    # 2) DeepSeek
    if DEEPSEEK_API_KEY:
        try:
            return call_deepseek(
                api_key=DEEPSEEK_API_KEY,
                model="deepseek-chat",
                prompt=prompt
            )
        except Exception as e:
            print(f"[LLM] DeepSeek falhou: {e}")

    # 3) OpenAI (opcional)
    if OPENAI_API_KEY:
        try:
            return call_openai(
                api_key=OPENAI_API_KEY,
                model="gpt-4o-mini",
                prompt=prompt
            )
        except Exception as e:
            print(f"[LLM] OpenAI falhou: {e}")

    return None