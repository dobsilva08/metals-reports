# ============================ IA (OpenAI/DeepSeek/Groq) ======================

def _prov_openai(system: str, user: str) -> Optional[str]:
    key = _env("OPENAI_API_KEY")
    if not key: return None
    model = _env("OPENAI_MODEL") or "gpt-4o-mini"
    url = "https://api.openai.com/v1/chat/completions"
    payload = {
        "model": model,
        "temperature": 0.2,
        "messages": [{"role":"system","content":system},
                     {"role":"user","content":user}],
    }
    headers = {"Authorization": f"Bearer {key}"}
    js = _http_post_json(url, payload, headers=headers)
    try:
        return js["choices"][0]["message"]["content"].strip()
    except Exception:
        return None

def _prov_deepseek(system: str, user: str) -> Optional[str]:
    key = _env("DEEPSEEK_API_KEY")
    if not key: return None
    model = _env("DEEPSEEK_MODEL") or "deepseek-chat"
    url = "https://api.deepseek.com/chat/completions"
    payload = {
        "model": model,
        "temperature": 0.2,
        "messages": [{"role":"system","content":system},
                     {"role":"user","content":user}],
    }
    headers = {"Authorization": f"Bearer {key}"}
    js = _http_post_json(url, payload, headers=headers)
    try:
        return js["choices"][0]["message"]["content"].strip()
    except Exception:
        return None

def _prov_groq(system: str, user: str) -> Optional[str]:
    key = _env("GROQ_API_KEY")
    if not key: return None
    model = _env("GROQ_MODEL") or "llama-3.1-70b-versatile"
    url = "https://api.groq.com/openai/v1/chat/completions"
    payload = {
        "model": model,
        "temperature": 0.2,
        "messages": [{"role":"system","content":system},
                     {"role":"user","content":user}],
    }
    headers = {"Authorization": f"Bearer {key}"}
    js = _http_post_json(url, payload, headers=headers)
    try:
        return js["choices"][0]["message"]["content"].strip()
    except Exception:
        return None

def _llm_chat(system: str, user: str) -> Optional[str]:
    """
    Orquestra provedores conforme LLM_PROVIDER_ORDER (csv).
    Ex.: 'openai,groq,deepseek'. Defaults seguros se não definido.
    """
    order = (_env("LLM_PROVIDER_ORDER") or "openai,groq,deepseek").split(",")
    order = [p.strip().lower() for p in order if p.strip()]
    tried, providers = [], {
        "openai": _prov_openai,
        "deepseek": _prov_deepseek,
        "groq": _prov_groq,
    }
    for p in order:
        fn = providers.get(p)
        if not fn: continue
        tried.append(p)
        try:
            out = fn(system, user)
            if out: 
                print(f"[llm] provider={p} ok")
                return out
            else:
                print(f"[llm] provider={p} sem resposta")
        except Exception as e:
            print(f"[llm] provider={p} erro: {e}")
    print(f"[llm] nenhum provedor respondeu (tentados: {', '.join(tried) or 'nenhum'})")
    return None

def _ia_fill_section(title: str, guidance: str, known_numbers: Dict[str, Any]) -> str:
    """
    Gera 2–4 frases concisas em PT-BR, SEM inventar números.
    Usa _llm_chat() com fallback entre OpenAI/DeepSeek/Groq.
    """
    sysmsg = (
        "Você é um analista de metais preciosos. "
        "Escreva 2–4 frases concisas, em PT-BR, SEM inventar números. "
        "Se nenhum número foi fornecido, use termos qualitativos (alta/baixa/estável) "
        "e mantenha travessão (—) para valores. Sem links e sem emojis."
    )
    facts = "\n".join([f"- {k}: {v}" for k,v in known_numbers.items() if v])
    usermsg = (
        f"Título da seção: {title}\n"
        f"Diretriz: {guidance}\n\n"
        f"Números conhecidos:\n{facts if facts else '- (nenhum)'}\n\n"
        "Responda APENAS com o texto final da seção."
    )
    out = _llm_chat(sysmsg, usermsg)
    return out or "—"