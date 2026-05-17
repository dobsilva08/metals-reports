# run_all.py
# Pipeline principal de metais

import sys

from collector_unified import collect_all
from report_unified import generate_all_reports
from telegram_sender import send


# --------------------------------------------------
# MAIN
# --------------------------------------------------

def main():

    print(
        "== METALS REPORTER INICIANDO =="
    )

    # -----------------------------
    # Coleta snapshot
    # -----------------------------

    snapshot = collect_all()

    print(
        "[OK] Snapshot coletado"
    )

    # -----------------------------
    # Geração relatórios
    # -----------------------------

    reports = generate_all_reports()

    print(
        f"[OK] {len(reports)} relatórios gerados"
    )

    # -----------------------------
    # Envio Telegram
    # -----------------------------

    for report in reports:

        send(report)

        first_line = report.split("\n")[0]

        print(
            f"[OK] Enviado: {first_line}"
        )

    print(
        "== CONCLUÍDO =="
    )


# --------------------------------------------------
# EXECUÇÃO
# --------------------------------------------------

if __name__ == "__main__":

    try:

        main()

    except Exception as e:

        print(
            f"[ERRO] {e}",
            file=sys.stderr
        )

        sys.exit(1)
