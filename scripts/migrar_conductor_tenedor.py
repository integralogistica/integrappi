"""
Migración one-time (2026-10-14, pedido del usuario): las cuentas con perfil
CONDUCTOR que son DUEÑAS de una ficha de vehículo (idUsuario) — es decir,
conducen SU propio vehículo y a la vez hacen de tenedor/responsable — pasan
a perfil TENEDOR, que es lo que ya son en la práctica, y así pueden registrar
más placas (regla CONDUCTOR=1 placa / TENEDOR=N de /crear y
/asignar-responsable).

NADA pasa automáticamente al crear la ficha: el perfil se fija en el
registro (toggle «Yo conduzco / Soy el dueño») y nunca cambia solo — este
script corrige los históricos.

Criterio: perfil CONDUCTOR (o sin perfil, default CONDUCTOR) + ser idUsuario
de ≥1 vehículo NO rechazado/borrador sin responsable. Ser `idConductor`
invitado NO cuenta (siguen siendo CONDUCTOR).

Dry-run por defecto; `--ejecutar` escribe (con backup JSON de las cuentas
tocadas + auditoría en cada vehículo afectado).

Uso:
    python scripts/migrar_conductor_tenedor.py             # dry-run
    python scripts/migrar_conductor_tenedor.py --ejecutar
"""
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from bd.bd_cliente import bd_cliente  # noqa: E402

bd = bd_cliente["integra"]
conductores = bd["conductores"]
vehiculos = bd["vehiculos"]


def main() -> None:
    ejecutar = "--ejecutar" in sys.argv

    # Dueños de ficha por id de cuenta (excluye rechazados y borradores del
    # alta sin responsable — no son placas operando).
    duenos: dict = {}
    for veh in vehiculos.find(
        {"idUsuario": {"$nin": [None, ""]}, "estadoIntegra": {"$ne": "rechazado"}},
        {"idUsuario": 1, "placa": 1, "estadoIntegra": 1},
    ):
        duenos.setdefault(str(veh.get("idUsuario")), []).append(veh["placa"])

    print(f"Cuentas dueñas de al menos una ficha: {len(duenos)}")

    a_promover = []  # (doc cuenta, placas)
    for cid, placas in duenos.items():
        try:
            cuenta = conductores.find_one({"_id": cid}) or conductores.find_one({"_id": _a_oid(cid)})
        except Exception:
            cuenta = None
        if not cuenta:
            print(f"  ⚠ dueño sin cuenta en `conductores`: id {cid} ({', '.join(placas)})")
            continue
        perfil = (cuenta.get("perfil") or "CONDUCTOR").upper()
        if perfil == "TENEDOR":
            continue  # ya está bien
        if perfil != "CONDUCTOR":
            print(f"  ⚠ perfil inesperado {perfil!r}: {cuenta.get('correo')} — se deja igual")
            continue
        a_promover.append((cuenta, placas))

    print(f"\nA promover CONDUCTOR → TENEDOR: {len(a_promover)}")
    for cuenta, placas in a_promover:
        print(f"  {cuenta.get('correo', '?')}: dueño de {', '.join(placas)}")

    if not ejecutar:
        print("\nDRY-RUN (usa --ejecutar para escribir). Total escrituras: 0")
        return

    # Backup de las cuentas antes de tocarlas.
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    ruta_backup = Path(f"backup_perfil_tenedor_{stamp}.json")
    ruta_backup.write_text(
        json.dumps(
            [{"_id": str(c["_id"]), "correo": c.get("correo"), "perfil": c.get("perfil")}
             for c, _ in a_promover],
            indent=2, ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    print(f"\nBackup: {ruta_backup}")

    ahora = datetime.utcnow()
    escrituras = 0
    for cuenta, placas in a_promover:
        conductores.update_one({"_id": cuenta["_id"]}, {"$set": {"perfil": "TENEDOR"}})
        escrituras += 1
        # Trazabilidad en cada ficha que posee (misma bitácora de /revision).
        for placa in placas:
            try:
                vehiculos.update_one(
                    {"placa": placa},
                    {"$push": {"auditoriaVehiculo": {"$each": [{
                        "fecha": ahora,
                        "actor": "migracion-tenedor",
                        "via": "sistema",
                        "accion": "perfil_tenedor",
                        "detalle": f"Cuenta dueña promovida a TENEDOR ({cuenta.get('correo', '')})",
                    }], "$slice": -200}}},
                )
            except Exception as e:
                print(f"  ⚠ auditoría de {placa}: {e}")

    print(f"\nEJECUTADO. Perfiles actualizados: {escrituras}")


def _a_oid(valor):
    from bson import ObjectId
    try:
        return ObjectId(valor)
    except Exception:
        return valor


if __name__ == "__main__":
    main()
