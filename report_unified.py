# report_unified.py
# Relatórios premium de metais usando snapshot único.

from collector_unified import collect_all


def fmt_price(value):
    if value is None:
        return "N/D"
    return f"US$ {value:,.2f}"


def fmt_pct(value):
    if value is None:
        return "N/D"
    arrow = "↗️" if value >= 0 else "↘️"
    return f"{value:.2f}% {arrow}"


def calculate_score(change_24h, change_7d):
    score = 50
    if change_24h is not None:
        score += max(-20, min(20, change_24h * 2))
    if change_7d is not None:
        score += max(-25, min(25, change_7d))
    return max(0, min(100, round(score)))


def classify_score(score):
    if score >= 70:
        return "🟢 Forte", "Acumular"
    if score >= 50:
        return "🟡 Neutro", "Manter"
    return "🔴 Fraco", "Reduzir"


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


def build_report(name, data, snapshot):
    score = calculate_score(data.get("change_24h"), data.get("change_7d"))
    classification, strategy = classify_score(score)
    bias = macro_bias(score)
    inflation = inflation_pressure(score)

    lines = [
        f"🟡 <b>RELATÓRIO METAIS — {name}</b>",
        f"📅 {snapshot['date']} — Diário",
        "",
        "━━━━━━━━━━━━━━", "📈 <b>Price Action</b>", "",
        f"• Preço Atual: <code>{fmt_price(data.get('price'))}</code>",
        f"• Variação 24h: <code>{fmt_pct(data.get('change_24h'))}</code>",
        f"• Variação 7d: <code>{fmt_pct(data.get('change_7d'))}</code>",
        f"• Máxima: <code>{fmt_price(data.get('high'))}</code>",
        f"• Mínima: <code>{fmt_price(data.get('low'))}</code>", "",
        "━━━━━━━━━━━━━━", "🌍 <b>Pressão Macro</b>", "",
        f"• Viés Macro: {bias}",
        f"• Pressão Inflacionária (proxy de momentum): {inflation}", "",
        "Proxy calculada a partir do momentum de preço; não representa uma medida direta de inflação.",
        "",
        "━━━━━━━━━━━━━━", "🏦 <b>Volume & Momentum</b>", "",
        f"• Volume informado pelo Yahoo Finance: <code>{data.get('volume', 'N/D')}</code>",
        f"• Momentum: {classification}", "",
        "Volume isolado não comprova posicionamento institucional.", "",
        "━━━━━━━━━━━━━━", "⚒️ <b>Demanda Industrial</b>", "",
        "• Demanda Global: <i>Não avaliada por este pipeline</i>",
        "• Commodities Basket: <i>Não avaliado</i>", "",
        "━━━━━━━━━━━━━━", "🔥 <b>Volatilidade & Risco</b>", "",
        f"• Score de Momentum: <code>{score}/100</code>",
        f"• Classificação: {classification}", "",
        "Score baseado nas variações de preço; não é volatilidade implícita.", "",
        "━━━━━━━━━━━━━━", "📊 <b>Interpretação Executiva</b>", "",
        f"• Score Commodities: <code>{score}/100</code>",
        f"• Viés Geral: {bias}",
        f"• Estratégia indicativa: <b>{strategy}</b>", "",
        "━━━━━━━━━━━━━━",
        "⚠️ <i>Relatório automatizado via GitHub Actions; não constitui recomendação financeira.</i>",
    ]
    return "\n".join(lines)


def generate_all_reports(snapshot=None):
    if snapshot is None:
        snapshot = collect_all()
    reports = []
    for name, data in snapshot.get("metals", {}).items():
        reports.append(build_report(name, data, snapshot))
    return reports


if __name__ == "__main__":
    for report in generate_all_reports():
        print(report)
        print("\n" + "=" * 80 + "\n")
