"""Habilita la fuente "sena" en el config de las empresas existentes (2026-09-01).

Las empresas creadas ANTES de la fuente sena tienen `config.fuentes_habilitadas`
persistido sin ella (el default viejo era [manifiestos_rndc, procuraduria, runt,
simit]). Este script la agrega con $addToSet (idempotente, sin carrera). El gate
real del consumo sigue siendo el PLAN: agregar la fuente al config es inofensivo
mientras ningún plan la incluya (`cobro.fuentes_con_plan` la filtra), así que
se puede correr antes o después del deploy sin interrumpir a nadie.

Empresas sin `fuentes_habilitadas` persistido (heredan FUENTES en runtime) no
necesitan nada.

Uso:
    python scripts/habilitar_fuente_sena.py             # dry-run
    python scripts/habilitar_fuente_sena.py --ejecutar  # real
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bd.bd_cliente import bd_cliente  # noqa: E402

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

db = bd_cliente["integra"]
col_empresas = db["empresas_seguridad"]


def main(ejecutar: bool) -> None:
    pendientes = []
    for empresa in col_empresas.find({"activo": True}):
        fuentes = (empresa.get("config") or {}).get("fuentes_habilitadas")
        if fuentes is None:
            print(f"{empresa.get('nombre')}: sin config persistido (hereda el catálogo en runtime) — nada que hacer")
            continue
        if "sena" in fuentes:
            print(f"{empresa.get('nombre')}: ya tiene sena ✓")
            continue
        pendientes.append(empresa)

    if not pendientes:
        print("\nNinguna empresa pendiente.")
        return

    print()
    for empresa in pendientes:
        actual = (empresa.get("config") or {}).get("fuentes_habilitadas") or []
        print(f"{empresa.get('nombre')}: {actual} → {actual + ['sena']}")
        if ejecutar:
            col_empresas.update_one(
                {"_id": empresa["_id"]},
                {"$addToSet": {"config.fuentes_habilitadas": "sena"}},
            )
            print("  ✔ habilitada")

    if not ejecutar:
        print(f"\n(dry-run: {len(pendientes)} empresa(s); repita con --ejecutar para aplicar)")
    else:
        print(f"\nListo: {len(pendientes)} empresa(s) habilitadas con la fuente sena.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Habilitar la fuente sena en empresas existentes")
    parser.add_argument("--ejecutar", action="store_true", help="Aplicar de verdad (default: dry-run)")
    args = parser.parse_args()
    main(args.ejecutar)
