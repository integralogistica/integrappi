"""Backfill plan → planes (multi-plan por fuente) en empresas_seguridad.

Cada empresa con el subdoc `plan` viejo (una empresa = un plan) y sin el array
`planes` pasa a tener una entrada por fuente incluida en ese plan, con el cupo
clonado por fuente. El subdoc `plan` se elimina SOLO en modo --ejecutar (el
código nuevo lo tolera como fallback; el viejo no soporta el array).

Idempotente: empresas ya migradas (con `planes`) se ignoran.

Uso:
    python scripts/migrar_planes_por_fuente.py             # dry-run
    python scripts/migrar_planes_por_fuente.py --ejecutar   # real
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bd.bd_cliente import bd_cliente  # noqa: E402

db = bd_cliente["integra"]
col_empresas = db["empresas_seguridad"]
col_planes = db["planes_seguridad"]


def migrar(ejecutar: bool) -> None:
    pendientes = [e for e in col_empresas.find({}) if not (e.get("planes") or []) and (e.get("plan") or {}).get("plan_id")]
    if not pendientes:
        print("Nada por migrar: todas las empresas con plan ya tienen el array `planes`.")
        return
    for empresa in pendientes:
        viejo = empresa["plan"]
        plan_doc = col_planes.find_one({"_id": viejo["plan_id"]}) or {}
        fuentes = [f for f in plan_doc.get("fuentes_incluidas", []) if f] or ["todas"]
        entradas = [
            {
                "plan_id": viejo["plan_id"],
                "plan_nombre": plan_doc.get("nombre", viejo.get("plan_nombre", "")),
                "fuente": f,
                "precio_congelado": int(plan_doc.get("precio_por_estudio") or 0),
                "cupo_autorizado": viejo.get("cupo_autorizado"),
                "cupo_disponible": viejo.get("cupo_disponible"),
                "cupo_consumido": int(viejo.get("cupo_consumido") or 0),
                "asignado_por": viejo.get("asignado_por", ""),
                "asignado_en": viejo.get("asignado_en"),
            }
            for f in fuentes
        ]
        print(f"{empresa.get('nombre')}: plan '{plan_doc.get('nombre', '?')}' → {len(entradas)} entrada(s) "
              f"[{', '.join(f'{e['fuente']}: cupo {e['cupo_autorizado'] if e['cupo_autorizado'] is not None else '∞'}' for e in entradas)}]")
        if ejecutar:
            col_empresas.update_one(
                {"_id": empresa["_id"]},
                {"$set": {"planes": entradas}, "$unset": {"plan": ""}},
            )
            print(f"  ✔ migrada (subdoc `plan` eliminado)")
    if not ejecutar:
        print("\nDRY-RUN: nada escrito. Corra con --ejecutar para aplicar.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill plan→planes por fuente")
    parser.add_argument("--ejecutar", action="store_true", help="Aplicar cambios (default: dry-run)")
    args = parser.parse_args()
    migrar(args.ejecutar)
