# metals-reports  
Automação de relatórios diários, semanais e mensais para metais preciosos (Ouro, Prata, Cobre etc.), com geração de análises via LLM (Groq, PiAPI, OpenAI, DeepSeek) e envio automatizado para Telegram.

<p align="left">
  <img src="https://img.shields.io/badge/LLM-Powered-blue?style=flat-square" />
  <img src="https://img.shields.io/badge/Automation-GitHub%20Actions-green?style=flat-square" />
  <img src="https://img.shields.io/badge/Language-Python%203.11-yellow?style=flat-square" />
  <img src="https://img.shields.io/badge/Reports-Automatic-orange?style=flat-square" />
</p>

---

## ✅ Visão Geral

O projeto **metals-reports** automatiza relatórios de mercado com qualidade institucional ("desk de research") para metais preciosos.  
Diariamente, o sistema coleta contexto macro, aciona uma IA analítica, formata a saída em HTML para Telegram e envia automaticamente o relatório para um grupo ou tópico específico.

Todos os relatórios seguem um padrão profissional, com:

- Estrutura fixa de tópicos
- Referências institucionais (CFTC, LBMA, COMEX, FRED, GLD/IAU)
- Tom de Head de Commodities Research
- HTML limpo para Telegram
- Fallback de provedores LLM (PiAPI → Groq → OpenAI → DeepSeek)

---

## ✅ Funcionalidades Principais

### 📌 Relatórios Diários
- Ouro (XAU/USD) – **gold_daily.py**  
- Prata (XAG/USD) – **silver_daily.py**  
- Cobre (HG) – **copper_daily.py**  

Incluem:
- Fluxos em ETFs (GLD/IAU)
- Posição líquida (CFTC/CME)
- Reservas físicas (LBMA/COMEX)
- Compras de bancos centrais
- Produção de mineração
- Dólar (DXY)
- Juros (Treasuries nominais e reais)
- Notas de Research (GS, JPM, UBS, Citi etc.)
- Interpretação executiva (5 bullets)
- Conclusão clara com cenário de curto e médio prazo

---

## ✅ Fluxo de Execução (GitHub Actions)

### 🌅 **Daily — Relatório Principal**
Horário padrão: **06:00 BRT**  
Workflow: `.github/workflows/gold_daily.yml`

- Gera o relatório completo  
- Atualiza contador  
- Cria/atualiza trava diária `.sent`  
- Envia para o Telegram  

### 🧭 **Watchdog — Backup**
Horário: **06:30 BRT**  
Workflow: `.github/workflows/gold_watchdog.yml`

- Só envia **se o Daily falhar**  
- Usa a mesma trava `.sent`  
- Evita completamente duplicações  

---

## ✅ Trava Diária — `.sent`

Para garantir **somente 1 envio por dia**, o script cria automaticamente:


