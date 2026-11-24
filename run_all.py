# run_all.py
# Pipeline principal de metais - coleta unica, validação e envio robusto.
import sys

from collector_unified import collect_all
from report_unified import generate_all_reports
from telegram_sender import send


def main():
    print("== METALS REPORTER INICIANDO ==")

    snapshot = collect_all()
    metals = snapshot.get("metals", {})
    if not metals:
        raise RuntimeError("Coleta não retornou nenhum metal; pipeline abortado.")

    print(f"[OK] Snapshot coletado: {len(metals)} ativos")

    reports = generate_all_reports(snapshot)
    if not reports:
        raise RuntimeError("Nenhum relatório foi gerado a partir do snapshot.")

    print(f"[OK] {len(reports)} relatórios gerados")

    failures = []
    for report in reports:
        try:
            result = send(report)
            if not result.get("ok", False):
                raise RuntimeError(f"Telegram não confirmou envio: {result}")
            first_line = report.split("\n")[0]
            print(f"[OK] Enviado: {first_line}")
        except Exception as exc:
            failures.append(str(exc))
            print(f"[ERRO ENVIO] {exc}", file=sys.stderr)

    if failures:
        raise RuntimeError(f"{len(failures)} relatório(s) falharam no envio ao Telegram.")

    print("== CONCLUÍDO ==")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERRO] {e}", file=sys.stderr)
        sys.exit(1)
