#!/usr/bin/env python3
"""
Migración one-time: hashear con bcrypt las claves guardadas en claro en las
colecciones `conductores` y `baseusuarios` (DB `integra`).

El backend verifica en dual-mode (Funciones/claves.py), así que este script
puede ejecutarse antes o después del deploy del backend sin romper logins.

Uso (seco por defecto — no escribe nada):
    python scripts/migrar_claves_bcrypt.py            # dry-run: reporta qué haría
    python scripts/migrar_claves_bcrypt.py --ejecutar # hashea de verdad

ANTES de ejecutar contra producción:
    1. Backup:  mongodump --uri "$MONGO_URI" --db integra --collection baseusuarios
                mongodump --uri "$MONGO_URI" --db integra --collection conductores
    2. Deploy del backend primero (el dual-mode hace seguro cualquier orden).
    3. Correr dry-run y revisar el reporte (sin-correo / duplicados).
    4. Correr con --ejecutar y verificar logins inmediatamente.

El reporte lista además:
    - Usuarios de baseusuarios SIN correo (el login ahora es por correo
      estricto: estos usuarios NO podrán entrar hasta asignarles correo).
    - Correos DUPLICADOS en baseusuarios (no hay índice único en `correo`).
    - Conductores sin correo (su login también es por correo).
"""

import sys
import os

# Consolas Windows (cp1252) no soportan unicode del reporte; forzar UTF-8.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Agregar el directorio raíz al path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from bd.bd_cliente import bd_cliente  # noqa: E402
from Funciones.claves import crear_hash, es_hash  # noqa: E402

bd = bd_cliente["integra"]


def migrar_coleccion(coleccion, nombre, ejecutar):
    hasheados = ya_hasheados = vacios = errores = 0

    cursor = coleccion.find(
        {"clave": {"$exists": True}},
        {"clave": 1, "correo": 1, "usuario": 1, "perfil": 1, "activo": 1},
    )

    for doc in cursor:
        clave = doc.get("clave")
        clave_str = str(clave or "").strip()

        if not clave_str:
            vacios += 1
            continue
        if es_hash(clave_str):
            ya_hasheados += 1
            continue

        if not ejecutar:
            hasheados += 1
            continue

        try:
            # Filtro condicional sobre el valor original: idempotente y seguro
            # ante carreras (si otro proceso cambió la clave, no se pisa).
            resultado = coleccion.update_one(
                {"_id": doc["_id"], "clave": clave},
                {"$set": {"clave": crear_hash(clave_str)}},
            )
            if resultado.matched_count:
                hasheados += 1
            else:
                print(f"  ⚠️ {nombre} {doc['_id']}: la clave cambió durante la migración, se omite")
        except Exception as e:
            errores += 1
            print(f"  ❌ {nombre} {doc.get('usuario') or doc['_id']}: {e}")

    verbo = "Se hashearian" if not ejecutar else "Se hashearon"
    print(f"\n{nombre}: {verbo} {hasheados} · ya hasheadas {ya_hasheados} · vacías {vacios} · errores {errores}")
    return errores


def reportar_sin_correo(coleccion, nombre):
    """Usuarios sin correo: con login estricto por correo quedan bloqueados."""
    print(f"\n{'─' * 60}\n⚠️  {nombre}: usuarios SIN correo (no podrán entrar por correo):")
    sin_correo = coleccion.find(
        {"$or": [{"correo": {"$exists": False}}, {"correo": None}, {"correo": ""}]},
        {"usuario": 1, "perfil": 1, "activo": 1},
    )
    total = 0
    for doc in sin_correo:
        total += 1
        print(f"   - {doc.get('usuario', '?')} | perfil={doc.get('perfil', '?')} | activo={doc.get('activo', True)} | _id={doc['_id']}")
    if not total:
        print("   (ninguno ✅)")


def reportar_correos_duplicados(coleccion, nombre):
    """Correos duplicados (case-insensitive): login ambiguo."""
    print(f"\n{'─' * 60}\n⚠️  {nombre}: correos DUPLICADOS (login ambiguo):")
    pipeline = [
        {"$match": {"correo": {"$exists": True, "$nin": [None, ""]}}},
        {"$group": {"_id": {"$toUpper": "$correo"}, "total": {"$sum": 1}}},
        {"$match": {"total": {"$gt": 1}}},
    ]
    duplicados = list(coleccion.aggregate(pipeline))
    if not duplicados:
        print("   (ninguno ✅)")
        return
    for dup in duplicados:
        print(f"   - {dup['_id']} × {dup['total']}")


def main():
    ejecutar = "--ejecutar" in sys.argv
    modo = "EJECUTAR (escribe en BD)" if ejecutar else "DRY-RUN (no escribe nada; usar --ejecutar para aplicar)"
    print("=" * 60)
    print(f"Migración de claves a bcrypt — {modo}")
    print("=" * 60)

    errores = 0
    errores += migrar_coleccion(bd["conductores"], "conductores", ejecutar)
    errores += migrar_coleccion(bd["baseusuarios"], "baseusuarios", ejecutar)

    reportar_sin_correo(bd["baseusuarios"], "baseusuarios")
    reportar_correos_duplicados(bd["baseusuarios"], "baseusuarios")
    reportar_sin_correo(bd["conductores"], "conductores")

    print("\n" + "=" * 60)
    if errores:
        print(f"❌ Terminado con {errores} errores. Revisar los mensajes de arriba.")
        sys.exit(1)
    print("✅ Terminado." if ejecutar else "✅ Dry-run terminado. Revisa el reporte y vuelve a correr con --ejecutar.")
    if not ejecutar:
        print("   Recuerda: backup con mongodump ANTES de --ejecutar contra producción.")


if __name__ == "__main__":
    main()
