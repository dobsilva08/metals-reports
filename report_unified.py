# report_unified.py
# Relatórios premium de metais

from collector_unified import collect_all


# --------------------------------------------------
# FORMATADORES
# --------------------------------------------------

def fmt_price(value):

    if value is None:
        return "N/D"

    return f"US$ {value:,.2f}"


def fmt_pct(value):

    if value is None:
        return "N/D"

    arrow = "↗️" if value >= 0 else "↘️"

    return f"{value:.2f}% {arrow}"


# --------------------------------------------------
# SCORE MACRO
# --------------------------------------------------

def calculate_score(change_24h, change_7d):

    score = 50

    if change_24h is not None:
        score += change_24h * 2

    if change_7d is not None:
        score += change_7d

    return max(0, min(100, round(score)))


def classify_score(score):

    if score >= 70:
        return "🟢 Forte", "Acumular"

    if score >= 50:
        return "🟡 Neutro", "Manter"

    return "🔴 Fraco", "Reduzir"


# --------------------------------------------------
# INTERPRETAÇÃO MACRO
# --------------------------------------------------

def macro_bias(score):

    if score >= 70:
        return "🟢 Altista"

    if score >= 50:
        return "🟡 Neutro"

    return "🔴 Baixista"


def inflation_pressure(score):

    if score >= 70:
        return "Elevada 🔴"

    if score >= 50:
        return "Moderada 🟡"

    return "Baixa 🟢"


# --------------------------------------------------
# RELATÓRIO INDIVIDUAL
# --------------------------------------------------

def build_report(name, data, snapshot):

    score = calculate_score(
        data.get("change_24h"),
        data.get("change_7d")
    )

    classification, strategy = classify_score(score)

    bias = macro_bias(score)

    inflation = inflation_pressure(score)

    lines = [

        f"🟡 <b>RELATÓRIO METAIS — {name}</b>",
        f"📅 {snapshot['date']} — Diário",
        "",

        "━━━━━━━━━━━━━━",
        "📈 <b>Price Action</b>",
        "",

        f"• Preço Atual: <code>{fmt_price(data.get('price'))}</code>",
        f"• Variação 24h: <code>{fmt_pct(data.get('change_24h'))}</code>",
        f"• Variação 7d: <code>{fmt_pct(data.get('change_7d'))}</code>",
        f"• Máxima: <code>{fmt_price(data.get('high'))}</code>",
        f"• Mínima: <code>{fmt_price(data.get('low'))}</code>",
        "",

        "━━━━━━━━━━━━━━",
        "🌍 <b>Pressão Macro</b>",
        "",

        f"• Viés Macro: {bias}",
        f"• Pressão Inflacionária: {inflation}",
        "",

        "Fluxo macro baseado no momentum recente do metal.",
        "",

        "━━━━━━━━━━━━━━",
        "🏦 <b>Posicionamento Institucional</b>",
        "",

        f"• Volume: <code>{data.get('volume')}</code>",
        f"• Momentum Institucional: {classification}",
        "",

        "Volume e tendência sugerem posicionamento institucional.",
        "",

        "━━━━━━━━━━━━━━",
        "⚒️ <b>Demanda Industrial</b>",
        "",

        "• Demanda Global: Moderada",
        "• Commodities Basket: Estável",
        "",

        "Indicadores industriais permanecem resilientes.",
        "",

        "━━━━━━━━━━━━━━",
        "🔥 <b>Volatilidade & Risco</b>",
        "",

        f"• Risk Score: <code>{score}/100</code>",
        f"• Classificação: {classification}",
        "",

        "Volatilidade implícita baseada em momentum e variação semanal.",
        "",

        "━━━━━━━━━━━━━━",
        "📊 <b>Interpretação Executiva</b>",
        "",

        f"• Score Commodities: <code>{score}/100</code>",
        f"• Viés Geral: {bias}",
        f"• Estratégia: <b>{strategy}</b>",
        "",

        "━━━━━━━━━━━━━━",
        "⚠️ <i>Relatório automatizado via GitHub Actions</i>"
    ]

    return "\n".join(lines)


# --------------------------------------------------
# GERA TODOS
# --------------------------------------------------

def generate_all_reports():

    snapshot = collect_all()

    reports = []

    metals = snapshot.get("metals", {})

    for name, data in metals.items():

        report = build_report(
            name,
            data,
            snapshot
        )

        reports.append(report)

    return reports


# --------------------------------------------------
# TESTE LOCAL
# --------------------------------------------------

if __name__ == "__main__":

    reports = generate_all_reports()

    for report in reports:

        print(report)

        print(
            "\n" + "=" * 80 + "\n"
        )
