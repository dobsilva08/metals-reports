# Relatórios de metais usando snapshot único.

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
    return "🟢 Altista" if score >= 70 else "🟡 Neutro" if score >= 50 else "🔴 Baixista"


def build_report(name, data, snapshot):
    score = calculate_score(data.get("change_24h"), data.get("change_7d"))
    classification, strategy = classify_score(score)
    bias = macro_bias(score)
    label = "ETF relacionado ao setor de lítio" if name == "ETF_LITIO" else name

    lines = [
        f"🟡 <b>RELATÓRIO METAIS — {label}</b>",
        f"📅 {snapshot['date']} — Diário", "",
        "━━━━━━━━━━━━━━", "📈 <b>Price Action</b>", "",
        f"• Preço Atual: <code>{fmt_price(data.get('price'))}</code>",
        f"• Variação 24h: <code>{fmt_pct(data.get('change_24h'))}</code>",
        f"• Variação 7d: <code>{fmt_pct(data.get('change_7d'))}</code>",
        f"• Máxima: <code>{fmt_price(data.get('high'))}</code>",
        f"• Mínima: <code>{fmt_price(data.get('low'))}</code>", "",
        "━━━━━━━━━━━━━━", "🌍 <b>Contexto de Momentum</b>", "",
        f"• Viés: {bias}",
        f"• Momentum: {classification}", "",
        "Score baseado exclusivamente nas variações de preço coletadas; não é medida direta de inflação, demanda industrial ou volatilidade implícita.", "",
        "━━━━━━━━━━━━━━", "🏦 <b>Volume</b>", "",
        f"• Volume informado pelo Yahoo Finance: <code>{data.get('volume', 'N/D')}</code>",
        "• Volume não comprova posicionamento institucional.", "",
        "━━━━━━━━━━━━━━", "📊 <b>Interpretação Executiva</b>", "",
        f"• Score de Momentum: <code>{score}/100</code>",
        f"• Viés Geral: {bias}",
        f"• Estratégia indicativa: <b>{strategy}</b>", "",
        "━━━━━━━━━━━━━━",
        "⚠️ <i>Relatório automatizado; não constitui recomendação financeira.</i>",
    ]
    return "\n".join(lines)


def generate_all_reports(snapshot):
    reports = []
    for name, data in snapshot.get("metals", {}).items():
        reports.append(build_report(name, data, snapshot))
    return reports


if __name__ == "__main__":
    import json
    with open("snapshot.json", encoding="utf-8") as f:
        snapshot = json.load(f)
    for report in generate_all_reports(snapshot):
        print(report)
        print("\n" + "=" * 80 + "\n")
