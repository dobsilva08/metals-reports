# run_all.py
# Orquestrador principal - Metais Preciosos
# Pipeline: Ouro + Prata + Cobre -> LLM -> Telegram

import sys

import gold_daily
import silver_daily
import copper_daily


def main():
    print("=" * 50)
    print("METALS REPORTS - Pipeline Diario")
    print("=" * 50)

    reports = [
        ("OURO (XAU/USD)",   gold_daily),
        ("PRATA (XAG/USD)",  silver_daily),
        ("COBRE (HG/COMEX)", copper_daily),
    ]

    errors = []

    for name, module in reports:
        print(f"\n[STEP] Processando {name}...")
        try:
            # Simula --send-telegram --force para o pipeline automatizado
            import sys as _sys
            _sys.argv = ["run_all.py", "--send-telegram", "--force"]
            module.main()
            print(f"  [OK] {name} concluido.")
        except Exception as e:
            print(f"  [ERRO] {name}: {e}")
            errors.append(f"{name}: {e}")

    print("\n" + "=" * 50)
    if errors:
        print(f"Pipeline concluido com {len(errors)} erro(s):")
        for err in errors:
            print(f"  - {err}")
        sys.exit(1)
    else:
        print("Pipeline concluido com sucesso! Todos os relatorios enviados.")


if __name__ == "__main__":
    main()
