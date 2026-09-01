#!/usr/bin/env python3
"""
Inicialización one-time del módulo de Estudios de Seguridad (multi-tenant).

Hace tres cosas idempotentes:
  1. Crea (upsert por slug) la empresa INTEGRA en `empresas_seguridad`.
  2. Asigna `empresa_id` y `rol_seguridad` en `baseusuarios`:
       - perfil ADMIN     → rol_seguridad ADMIN_INTEGRA (sin empresa obligatoria)
       - perfil SEGURIDAD → empresa INTEGRA + rol_seguridad CONSULTADOR
         (flag --admin-empresa usuario1,usuario2 para marcar ADMIN_EMPRESA)
     Nunca pisa un usuario que ya tenga empresa_id/rol_seguridad asignados.
  3. Crea los índices de las colecciones nuevas (incluido TTL de retención).

Uso (seco por defecto — no escribe nada):
    python scripts/inicializar_seguridad_estudios.py
    python scripts/inicializar_seguridad_estudios.py --ejecutar
    python scripts/inicializar_seguridad_estudios.py --ejecutar --admin-empresa JSUAREZ,MLOPEZ

ANTES de ejecutar contra producción:
    1. Backup:  mongodump --uri "$MONGO_URI" --db integra --collection baseusuarios
    2. Correr dry-run y revisar el reporte (usuarios SEGURIDAD sin correo, etc.).
"""

import argparse
import sys
import os
from datetime import datetime, timezone

# Consolas Windows (cp1252) no soportan unicode del reporte; forzar UTF-8.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from bson import ObjectId  # noqa: E402

from bd.bd_cliente import bd_cliente  # noqa: E402

bd = bd_cliente["integra"]
col_empresas = bd["empresas_seguridad"]
col_estudios = bd["estudios_seguridad"]
col_eventos = bd["eventos_seguridad"]
col_usuarios = bd["baseusuarios"]

EMPRESA_INICIAL = {
    "nit": "900164363",
    "nombre": "INTEGRA LOGISTICA",
    "slug": "integra",
    "logo_url": None,
    "activo": True,
    "config": {
        "retencion_dias": 730,
        "aislamiento_usuario": False,
        "consultas_por_minuto": 10,
        # "policia" (antecedentes judiciales) NO va aquí: portal de autoconsulta
        # del titular (Decreto 019 de 2012) — se activa por empresa vía admin
        # con autorización documentada del titular (Ley 1581 de 2012).
        # "runt"/"simit"/"sena" van (portales públicos). ⚠️ Desde 2026-09-01
        # el gate real es el PLAN (fuentes_habilitadas_efectivas): las fuentes
        # default corren para toda empresa SIN tocar este config — este
        # listado solo persiste las opt-in y documenta el estado inicial.
        "fuentes_habilitadas": ["manifiestos_rndc", "procuraduria", "runt", "simit", "sena"],
    },
}


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def crear_empresa(ejecutar: bool) -> ObjectId | None:
    existente = col_empresas.find_one({"slug": EMPRESA_INICIAL["slug"]})
    if existente:
        print(f"[empresa] ya existe: {existente['nombre']} (_id={existente['_id']})")
        return existente["_id"]
    doc = {**EMPRESA_INICIAL, "creado_en": _utcnow(), "actualizado_en": _utcnow()}
    if not ejecutar:
        print(f"[empresa] (dry-run) crearía: {doc['nombre']} slug={doc['slug']}")
        return None
    resultado = col_empresas.insert_one(doc)
    print(f"[empresa] creada: {doc['nombre']} _id={resultado.inserted_id}")
    return resultado.inserted_id


def crear_indices(ejecutar: bool) -> None:
    planes = [
        (
            col_empresas,
            [
                ([("nombre", 1)], {"name": "idx_empseg_nombre", "unique": True}),
                ([("slug", 1)], {"name": "idx_empseg_slug", "unique": True}),
            ],
        ),
        (
            col_estudios,
            [
                ([("consulta_id", 1)], {"name": "idx_estseg_consulta", "unique": True}),
                ([("empresa_id", 1), ("creado_en", -1)], {"name": "idx_estseg_empresa_fecha"}),
                ([("empresa_id", 1), ("cedula", 1), ("creado_en", -1)], {"name": "idx_estseg_empresa_cedula"}),
                ([("empresa_id", 1), ("estado", 1), ("creado_en", -1)], {"name": "idx_estseg_empresa_estado"}),
                # TTL REAL: Mongo borra el doc cuando retencion_expira_en pasa.
                ([("retencion_expira_en", 1)], {"name": "idx_estseg_retencion_ttl", "expireAfterSeconds": 0}),
            ],
        ),
        (
            col_eventos,
            [
                ([("empresa_id", 1), ("creado_en", -1)], {"name": "idx_evtseg_empresa_fecha"}),
                ([("consulta_id", 1), ("creado_en", 1)], {"name": "idx_evtseg_consulta"}),
                ([("evento", 1), ("creado_en", -1)], {"name": "idx_evtseg_evento_fecha"}),
            ],
        ),
    ]
    for coleccion, indices in planes:
        for keys, opts in indices:
            nombre = opts.get("name", "?")
            if not ejecutar:
                print(f"[índice] (dry-run) {coleccion.name}.{nombre} {keys}")
                continue
            try:
                coleccion.create_index(keys, **opts)
                print(f"[índice] ok {coleccion.name}.{nombre}")
            except Exception as exc:
                print(f"[índice] ERROR {coleccion.name}.{nombre}: {exc}")


def asignar_usuarios(empresa_id: ObjectId | None, admin_empresa: list[str], ejecutar: bool) -> None:
    if empresa_id is None and ejecutar:
        print("[usuarios] no hay empresa_id (dry-run); se omite asignación")
        return

    admins = [u.strip().upper() for u in admin_empresa if u.strip()]

    for doc in col_usuarios.find(
        {"perfil": {"$in": ["ADMIN", "SEGURIDAD"]}},
        {"usuario": 1, "perfil": 1, "correo": 1, "activo": 1, "empresa_id": 1, "rol_seguridad": 1},
    ):
        usuario = doc.get("usuario", "?")
        perfil = (doc.get("perfil") or "").upper()
        ya_tiene = doc.get("empresa_id") or doc.get("rol_seguridad")
        if ya_tiene:
            print(f"[usuario] {usuario} ({perfil}) ya tiene empresa/rol — se omite")
            continue
        if not doc.get("correo"):
            print(f"[usuario] ⚠️ {usuario} ({perfil}) SIN correo: no podrá iniciar sesión")
        if perfil == "ADMIN":
            cambios = {"rol_seguridad": "ADMIN_INTEGRA"}
            detalle = "rol ADMIN_INTEGRA (sin empresa obligatoria)"
        else:
            rol = "ADMIN_EMPRESA" if usuario in admins else "CONSULTADOR"
            cambios = {"empresa_id": empresa_id, "rol_seguridad": rol}
            detalle = f"empresa INTEGRA + rol {rol}"
        if not ejecutar:
            print(f"[usuario] (dry-run) {usuario} ({perfil}) → {detalle}")
            continue
        col_usuarios.update_one({"_id": doc["_id"]}, {"$set": cambios})
        print(f"[usuario] {usuario} ({perfil}) → {detalle}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Inicializa el módulo de estudios de seguridad")
    parser.add_argument("--ejecutar", action="store_true", help="Escribe cambios (default: dry-run)")
    parser.add_argument(
        "--admin-empresa",
        default="",
        help="Usuarios SEGURIDAD a marcar ADMIN_EMPRESA (separados por coma)",
    )
    args = parser.parse_args()

    print(f"=== inicializar_seguridad_estudios ({'EJECUTAR' if args.ejecutar else 'DRY-RUN'}) ===")
    empresa_id = crear_empresa(args.ejecutar)
    crear_indices(args.ejecutar)
    asignar_usuarios(empresa_id, args.admin_empresa.split(","), args.ejecutar)
    print("=== listo ===")


if __name__ == "__main__":
    main()
