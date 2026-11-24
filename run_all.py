# Pipeline principal de metais - validação preventiva, coleta e envio.
import os
import sys

from collector_unified import collect_all
from report_unified import generate_all_reports
from telegram_sender import send


def validate_secrets():
    missing = [
        name for name in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID_METALS")
        if not os.getenv(name)
    ]
    if missing:
        raise RuntimeError("Secrets ausentes: " + ", ".join(missing))


def main():
    print("== METALS REPORTER INICIANDO ==")
    validate_secrets()
    print("[OK] Secrets necessários presentes")

    snapshot = collect_all()
    metals = snapshot.get("metals", {})
    if len(metals) < 2:
        raise RuntimeError(f"Coleta insuficiente para relatório: {len(metals)} ativo(s)")

    reports = generate_all_reports(snapshot)
    if not reports:
        raise RuntimeError("Nenhum relatório foi gerado a partir do snapshot.")
    print(f"[OK] {len(reports)} relatórios gerados")

    failures = []
    for report in reports:
        first_line = report.split("\n")[0]
        try:
            result = send(report)
            if not result.get("ok"):
                raise RuntimeError(f"Telegram retornou ok=false: {result}")
            print(f"[OK] Enviado: {first_line}")
        except Exception as exc:
            failures.append(f"{first_line}: {exc}")
            print(f"[ERRO ENVIO] {first_line}: {exc}", file=sys.stderr)

    if failures:
        raise RuntimeError("Falhas de envio: " + " | ".join(failures))

    if snapshot.get("collection_errors"):
        print("[AVISO] Pipeline concluído com coleta parcial.")
        for name, error in snapshot["collection_errors"].items():
            print(f"[AVISO COLETA] {name}: {error}")

    print("== CONCLUÍDO ==")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"[ERRO FATAL] {e}", file=sys.stderr)
        sys.exit(1)
