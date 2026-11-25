# Migración F8 (2026-11-25): desactivar las cuentas CONDUCTOR de baseusuarios.
#
# La auditoría (auditar_perfiles.py) encontró 3 cuentas perfil CONDUCTOR que
# NO existen en la colección `conductores` (donde viven los conductores reales
# con login propio) — legado de pruebas. Decisión del usuario: DESACTIVAR las
# 2 activas (activo=false, reversible; nada se borra). PRUEBA123 ya estaba
# inactiva y no se toca. CONDUCTOR sale de PERFILES_VALIDOS (mismo commit).
#
# Uso:  python scripts/desactivar_conductores_legado.py
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bd.bd_cliente import bd_cliente  # noqa: E402

db = bd_cliente["integra"]
usuarios = db["baseusuarios"]

OBJETIVOS = ["RICHARD223015@GMAIL.COM", "YAMIDLEITON84@GMAIL.COM"]

import json

respaldo = [dict(r, _id=str(r["_id"])) for r in usuarios.find({"perfil": "CONDUCTOR"}, {"clave": 0})]
with open("respaldo_conductores_baseusuarios_2026-11-25.json", "w", encoding="utf-8") as f:
    json.dump(respaldo, f, ensure_ascii=False, indent=2, default=str)

actualizados = 0
for objetivo in OBJETIVOS:
    r = usuarios.update_one(
        {"usuario": objetivo, "perfil": "CONDUCTOR"},
        {"$set": {"activo": False, "desactivado_legado_f8": datetime.now(timezone.utc).isoformat()}},
    )
    print(f"  {objetivo}: matched={r.matched_count} modified={r.modified_count}")
    actualizados += r.modified_count

print(f"\nDesactivadas {actualizados} cuentas. Respaldo (sin claves): respaldo_conductores_baseusuarios_2026-11-25.json")
