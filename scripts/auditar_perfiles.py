# Auditoría de perfiles de baseusuarios (2026-11-25, F8) — SOLO LECTURA.
#
# Antes de limpiar la matriz de perfiles (p. ej. eliminar CONDUCTOR de
# PERFILES_VALIDOS) hay que saber qué hay en la BD:
#   1. Conteo de usuarios por perfil (y por perfiles_adicionales).
#   2. Cuentas CONDUCTOR de baseusuarios: ¿existen? ¿son legado (su correo
#      también vive en la colección `conductores` con login propio)?
#   3. Cuentas CLIENTE_FMC (esperadas: clave SIN_ACCESO, solo notificaciones).
#   4. Perfiles "fantasma" que no están en PERFILES_VALIDOS (creados cuando
#      el PATCH no validaba).
#
# Uso:  python scripts/auditar_perfiles.py
# (usa MONGO_URI del entorno, igual que el resto del backend)
import os
import sys
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bd.bd_cliente import bd_cliente  # noqa: E402

PERFILES_VALIDOS = {
    'ADMIN', 'ANALISTA', 'CLIENTE', 'CLIENTE_ESTUDIOS', 'CLIENTE_FMC', 'CONDUCTOR',
    'CONTROL', 'COORDINADOR', 'DESPACHADOR', 'FINANCIERO', 'OPERADOR', 'OPERATIVO',
    'PADRINO', 'SEGURIDAD', 'VISUALIZADOR',
}

db = bd_cliente["integra"]
usuarios = db["baseusuarios"]
conductores = db["conductores"]

todos = list(usuarios.find({}, {"usuario": 1, "nombre": 1, "correo": 1, "perfil": 1,
                                "perfiles_adicionales": 1, "activo": 1, "clientes": 1}))

print(f"Total usuarios: {len(todos)}")
print("\n== Conteo por perfil ==")
for perfil, n in Counter((u.get("perfil") or "«sin perfil»") for u in todos).most_common():
    marca = "" if perfil in PERFILES_VALIDOS else "   ⚠️ PERFIL FANTASMA (no está en PERFILES_VALIDOS)"
    print(f"  {perfil:<20} {n:>4}{marca}")

print("\n== perfiles_adicionales ==")
for perfil, n in Counter(p for u in todos for p in (u.get("perfiles_adicionales") or [])).most_common():
    print(f"  {perfil:<20} {n:>4}")

# ── CONDUCTOR en baseusuarios: legado ──
conductores_bu = [u for u in todos if (u.get("perfil") or "").upper() == "CONDUCTOR"]
print(f"\n== Cuentas perfil CONDUCTOR en baseusuarios: {len(conductores_bu)} ==")
if conductores_bu:
    correos_cond = {c.get("correo", "").upper() for c in conductores.find({}, {"correo": 1})}
    for u in conductores_bu:
        correo = (u.get("correo") or "").upper()
        en_conductores = "SÍ existe en colección `conductores` (cuenta duplicada legado)" if correo and correo in correos_cond else "NO está en `conductores`"
        print(f"  - {u.get('usuario')} | {u.get('nombre')} | {u.get('correo')} | activo={u.get('activo', True)} | {en_conductores}")

# ── CLIENTE_FMC ──
fmc = [u for u in todos if (u.get("perfil") or "").upper() == "CLIENTE_FMC"]
print(f"\n== Cuentas perfil CLIENTE_FMC (solo notificaciones): {len(fmc)} ==")
for u in fmc[:30]:
    print(f"  - {u.get('usuario')} | {u.get('nombre')} | {u.get('correo')} | activo={u.get('activo', True)}")
if len(fmc) > 30:
    print(f"  … y {len(fmc) - 30} más")

# ── Fantasma ──
fantasma = [u for u in todos if (u.get("perfil") or "") not in PERFILES_VALIDOS]
print(f"\n== Usuarios con perfil FUERA de la matriz válida: {len(fantasma)} ==")
for u in fantasma:
    print(f"  - {u.get('usuario')} | perfil='{u.get('perfil')}' | {u.get('nombre')} | activo={u.get('activo', True)}")

print("\n(Script de SOLO LECTURA: no modifica nada. Con este resultado se decide la migración de F8.)")
