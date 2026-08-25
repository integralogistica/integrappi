"""
Migración one-time: backfill de `conductores.cedula` cruzando los vehículos
donde `condCorreo` coincide con el correo del conductor (la cédula vive en el
documento del vehículo como condCedulaCiudadania, pero la cuenta no la tenía).

Dry-run por defecto; `--ejecutar` escribe. Estilo de migrar_claves_bcrypt.py.

Uso:
    python scripts/migrar_cedula_conductores.py            # dry-run
    python scripts/migrar_cedula_conductores.py --ejecutar
"""
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bd.bd_cliente import bd_cliente  # noqa: E402

bd = bd_cliente["integra"]
conductores = bd["conductores"]
vehiculos = bd["vehiculos"]


def main() -> None:
    ejecutar = "--ejecutar" in sys.argv

    lista = list(conductores.find({}, {"correo": 1, "cedula": 1, "nombre": 1}))
    print(f"Conductores: {len(lista)}")

    por_actualizar = []
    sin_cedula = []
    cedulas_vistas: dict = {}

    for cond in lista:
        correo = (cond.get("correo") or "").strip().upper()
        if cond.get("cedula"):
            cedulas_vistas.setdefault(cond["cedula"], []).append(correo)
            continue

        # Buscar cédula en vehículos con el correo del conductor.
        veh = vehiculos.find_one(
            {"condCorreo": {"$regex": f"^{re.escape(correo)}$", "$options": "i"}},
            {"condCedulaCiudadania": 1, "placa": 1},
        )
        cedula = (veh or {}).get("condCedulaCiudadania") or ""
        cedula = re.sub(r"\D", "", str(cedula))
        if cedula:
            por_actualizar.append((cond["_id"], correo, cedula, veh["placa"]))
        else:
            sin_cedula.append(correo)

    print(f"\nA actualizar con cedula: {len(por_actualizar)}")
    for _id, correo, cedula, placa in por_actualizar:
        marca = f"[{placa}]"
        print(f"  {correo}: {cedula} {marca}")
        if ejecutar:
            conductores.update_one({"_id": _id}, {"$set": {"cedula": cedula}})
            cedulas_vistas.setdefault(cedula, []).append(correo)

    print(f"\nSin cedula encontrable (quedan en null): {len(sin_cedula)}")
    for correo in sin_cedula:
        print(f"  {correo}")

    # Duplicados: señal de saneamiento pendiente (la cédula NO es unique aún).
    duplicados = {c: cs for c, cs in cedulas_vistas.items() if len(cs) > 1}
    print(f"\nCedulas duplicadas: {len(duplicados)}")
    for cedula, correos in duplicados.items():
        print(f"  {cedula}: {', '.join(correos)}")

    modo = "EJECUTADO" if ejecutar else "DRY-RUN (usa --ejecutar para escribir)"
    print(f"\n{modo}. Total escrituras: {len(por_actualizar) if ejecutar else 0}")


if __name__ == "__main__":
    main()
