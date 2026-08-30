"""API de administración del cobro de Estudios de Seguridad.

Solo ADMIN_INTEGRA (perfil ADMIN de baseusuarios). Prefijo /seguridad/admin/cobro.

Gestiona: planes (catálogo armable), asignación plan+cupo por empresa,
pagos/ajustes/reembolsos manuales, movimientos con filtros y cierres de mes
con cuenta de cobro PDF (GCS privado). El consumo automático vive en
Funciones/cobro_seguridad.py y se gancha en crear_estudio (seguridad_estudios).
"""
from __future__ import annotations

import logging
import os
import re
from datetime import datetime

from bson import ObjectId
from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from pydantic import BaseModel

from bd.bd_cliente import bd_cliente
from Funciones import cobro_seguridad as cobro
from Funciones import storage_seguridad
from Funciones.auth_seguridad import ROL_ADMIN_INTEGRA
from Funciones.orquestador_estudios import FUENTES, enmascarar_cedula
from Funciones.pdf_cuenta_cobro import generar_pdf_cuenta
from rutas.seguridad_estudios import _requiere_admin_integra, _utcnow, registrar_evento

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/seguridad/admin/cobro", tags=["Seguridad - Cobro"])

db = bd_cliente["integra"]
col_planes = db["planes_seguridad"]
col_movimientos = db["movimientos_cobro_seguridad"]
col_periodos = db["periodos_cobro_seguridad"]
col_empresas = db["empresas_seguridad"]
col_estudios = db["estudios_seguridad"]
col_api_keys = db["api_keys_seguridad"]

cobro.asegurar_indices_cobro()


def _oid(valor: str, campo: str) -> ObjectId:
    if not re.fullmatch(r"[0-9a-fA-F]{24}", valor or ""):
        raise HTTPException(status_code=422, detail=f"{campo} inválido")
    return ObjectId(valor)


def _serializar(doc: dict) -> dict:
    doc = {k: v for k, v in doc.items() if k != "_id"}
    for campo in ("empresa_id", "plan_id", "consumo_id", "cierre_id"):
        if isinstance(doc.get(campo), ObjectId):
            doc[campo] = str(doc[campo])
    if doc.get("fecha_pago") and isinstance(doc["fecha_pago"], datetime):
        pass  # datetime nativo serializa solo
    return doc


def _empresa_o_404(empresa_id: str) -> dict:
    doc = col_empresas.find_one({"_id": _oid(empresa_id, "empresa_id")})
    if not doc:
        raise HTTPException(status_code=404, detail="Empresa no encontrada")
    return doc


# ═══════════════════════════ Planes ═══════════════════════════

class PlanCrear(BaseModel):
    nombre: str
    descripcion: str | None = None
    precio_por_estudio: int
    fuentes_incluidas: list[str]
    vigencia_dias: int | None = None
    activo: bool = True


class PlanActualizar(BaseModel):
    nombre: str | None = None
    descripcion: str | None = None
    precio_por_estudio: int | None = None
    fuentes_incluidas: list[str] | None = None
    vigencia_dias: int | None = None
    activo: bool | None = None


def _validar_plan(nombre=None, precio=None, fuentes=None, vigencia=None):
    if nombre is not None:
        nombre = nombre.strip().upper()
        if len(nombre) < 3:
            raise HTTPException(status_code=422, detail="El nombre debe tener al menos 3 caracteres")
    if precio is not None and precio <= 0:
        raise HTTPException(status_code=422, detail="El precio debe ser mayor que 0")
    if fuentes is not None:
        if not fuentes:
            raise HTTPException(status_code=422, detail="El plan debe incluir al menos una fuente")
        desconocidas = [f for f in fuentes if f not in FUENTES]
        if desconocidas:
            raise HTTPException(status_code=422, detail=f"Fuentes desconocidas: {desconocidas}. Válidas: {list(FUENTES)}")
    if vigencia is not None and vigencia is not False and vigencia < 1:
        raise HTTPException(status_code=422, detail="vigencia_dias debe ser ≥ 1 o null (sin vencimiento)")


@router.get("/planes")
def listar_planes(actor: dict = Depends(_requiere_admin_integra)):
    items = []
    for doc in col_planes.find().sort("nombre", 1):
        doc["id"] = str(doc.pop("_id"))
        items.append(doc)
    return {"total": len(items), "items": items}


@router.post("/planes", status_code=201)
def crear_plan(datos: PlanCrear, request: Request, actor: dict = Depends(_requiere_admin_integra)):
    _validar_plan(datos.nombre, datos.precio_por_estudio, datos.fuentes_incluidas, datos.vigencia_dias)
    nombre = datos.nombre.strip().upper()
    if col_planes.find_one({"nombre": nombre}):
        raise HTTPException(status_code=409, detail=f"Ya existe un plan llamado {nombre}")
    ahora = _utcnow()
    doc = {
        "nombre": nombre,
        "descripcion": datos.descripcion or "",
        "precio_por_estudio": int(datos.precio_por_estudio),
        "fuentes_incluidas": list(datos.fuentes_incluidas),
        "vigencia_dias": datos.vigencia_dias,
        "activo": bool(datos.activo),
        "creado_en": ahora,
        "actualizado_en": ahora,
    }
    resultado = col_planes.insert_one(doc)
    registrar_evento("plan_creado", actor=actor, detalle=f"{nombre} · ${datos.precio_por_estudio} · {datos.fuentes_incluidas}", request=request)
    # insert_one añade _id (ObjectId) al dict: excluirlo de la respuesta.
    return {"id": str(resultado.inserted_id), **{k: v for k, v in doc.items() if k != "_id"}}


@router.patch("/planes/{plan_id}")
def actualizar_plan(plan_id: str, datos: PlanActualizar, request: Request, actor: dict = Depends(_requiere_admin_integra)):
    doc = col_planes.find_one({"_id": _oid(plan_id, "plan_id")})
    if not doc:
        raise HTTPException(status_code=404, detail="Plan no encontrado")
    cambios: dict = {}
    if datos.nombre is not None:
        nombre = datos.nombre.strip().upper()
        _validar_plan(nombre=nombre)
        if col_planes.find_one({"nombre": nombre, "_id": {"$ne": doc["_id"]}}):
            raise HTTPException(status_code=409, detail=f"Ya existe un plan llamado {nombre}")
        cambios["nombre"] = nombre
    if datos.descripcion is not None:
        cambios["descripcion"] = datos.descripcion
    if datos.precio_por_estudio is not None:
        _validar_plan(precio=datos.precio_por_estudio)
        cambios["precio_por_estudio"] = int(datos.precio_por_estudio)
    if datos.fuentes_incluidas is not None:
        _validar_plan(fuentes=datos.fuentes_incluidas)
        cambios["fuentes_incluidas"] = list(datos.fuentes_incluidas)
    if datos.vigencia_dias is not None:
        if datos.vigencia_dias is not False:
            _validar_plan(vigencia=datos.vigencia_dias)
        cambios["vigencia_dias"] = datos.vigencia_dias or None
    if datos.activo is not None:
        cambios["activo"] = bool(datos.activo)
    if not cambios:
        raise HTTPException(status_code=422, detail="Nada que actualizar")
    cambios["actualizado_en"] = _utcnow()
    col_planes.update_one({"_id": doc["_id"]}, {"$set": cambios})
    # Auditoría del diff de fuentes: las NUEVAS se propagarán solas a cada
    # empresa en su próxima consulta (sync read-time); las RETIRADAS dejan de
    # ser consumibles al instante (validación de membresía en el motor).
    sufijo = ""
    if "fuentes_incluidas" in cambios:
        viejas = set(doc.get("fuentes_incluidas") or [])
        nuevas = set(cambios["fuentes_incluidas"])
        agregadas, retiradas = sorted(nuevas - viejas), sorted(viejas - nuevas)
        partes = []
        if agregadas:
            partes.append(f"+{agregadas}")
        if retiradas:
            partes.append(f"-{retiradas}")
        if partes:
            sufijo = f" · fuentes: {' '.join(partes)} (se propaga a empresas en su próxima consulta)"
    registrar_evento("plan_actualizado", actor=actor, detalle=f"{doc.get('nombre')} → {cambios}{sufijo}", request=request)
    actualizado = col_planes.find_one({"_id": doc["_id"]})
    actualizado["id"] = str(actualizado.pop("_id"))
    return actualizado


@router.delete("/planes/{plan_id}")
def desactivar_plan(plan_id: str, request: Request, actor: dict = Depends(_requiere_admin_integra)):
    """Soft delete: los movimientos históricos referencian el plan."""
    doc = col_planes.find_one({"_id": _oid(plan_id, "plan_id")})
    if not doc:
        raise HTTPException(status_code=404, detail="Plan no encontrado")
    col_planes.update_one({"_id": doc["_id"]}, {"$set": {"activo": False, "actualizado_en": _utcnow()}})
    afectadas = [e.get("nombre") for e in col_empresas.find({"planes.plan_id": doc["_id"]}, {"nombre": 1})]
    registrar_evento("plan_actualizado", actor=actor, detalle=f"{doc.get('nombre')} desactivado", request=request)
    return {"id": plan_id, "activo": False, "empresas_afectadas": afectadas}


# ═══════════════════════ Empresas: planes por fuente ═══════════════════════

def _vence_en(plan_doc: dict, asignado_en):
    if plan_doc and plan_doc.get("vigencia_dias") and asignado_en:
        from datetime import timedelta, timezone

        base = asignado_en
        if base.tzinfo is None:
            base = base.replace(tzinfo=timezone.utc)
        return base + timedelta(days=int(plan_doc["vigencia_dias"]))
    return None


class EmpresaCrear(BaseModel):
    nit: str
    nombre: str
    slug: str | None = None  # opcional: se deriva del nombre si viene vacío
    logo_url: str | None = None


def _slug_de(nombre: str) -> str:
    """Slug estable: minúsculas, sin acentos ni caracteres no alfanuméricos."""
    import unicodedata

    base = unicodedata.normalize("NFKD", nombre.strip().lower())
    base = "".join(c for c in base if not unicodedata.combining(c))
    base = re.sub(r"[^a-z0-9]+", "-", base).strip("-")
    if not base:
        raise HTTPException(status_code=422, detail="No se pudo derivar un slug del nombre; escríbalo manualmente")
    return base


@router.post("/empresas", status_code=201)
def crear_empresa(datos: EmpresaCrear, request: Request, actor: dict = Depends(_requiere_admin_integra)):
    """Crea una empresa cliente en empresas_seguridad (sin planes todavía)."""
    nombre = datos.nombre.strip().upper()
    if len(nombre) < 3:
        raise HTTPException(status_code=422, detail="El nombre debe tener al menos 3 caracteres")
    nit = re.sub(r"[\s.-]", "", datos.nit.strip())
    if not nit or not nit[0].isdigit():
        raise HTTPException(status_code=422, detail="NIT inválido")
    if col_empresas.find_one({"$or": [{"nombre": nombre}, {"nit": nit}]}):
        raise HTTPException(status_code=409, detail=f"Ya existe una empresa con ese nombre o NIT")

    slug = (datos.slug or _slug_de(nombre)).strip().lower()
    if not re.fullmatch(r"[a-z0-9-]+", slug):
        raise HTTPException(status_code=422, detail="El slug solo admite letras minúsculas, números y guiones")
    if col_empresas.find_one({"slug": slug}):
        raise HTTPException(status_code=409, detail=f"El slug '{slug}' ya está en uso")

    ahora = _utcnow()
    doc = {
        "nit": nit,
        "nombre": nombre,
        "slug": slug,
        "logo_url": datos.logo_url or None,
        "activo": True,
        # Config por defecto = misma de INTEGRA (inicializar_seguridad_estudios.py).
        "config": {
            "retencion_dias": 730,
            "aislamiento_usuario": False,
            "consultas_por_minuto": 10,
            "fuentes_habilitadas": list(FUENTES),
        },
        "creado_en": ahora,
        "actualizado_en": ahora,
    }
    resultado = col_empresas.insert_one(doc)
    registrar_evento(
        "empresa_creada", actor=actor,
        detalle=f"{nombre} · NIT {nit} · slug {slug}",
        request=request,
    )
    return {"id": str(resultado.inserted_id), **{k: v for k, v in doc.items() if k != "_id"}}


@router.get("/empresas")
def listar_empresas_cobro(actor: dict = Depends(_requiere_admin_integra)):
    """Empresas con sus planes por fuente, consumo del mes y saldo pendiente."""
    periodo_actual = cobro.periodo_colombia()
    items = []
    for empresa in col_empresas.find().sort("nombre", 1):
        # El catálogo manda: materializar fuentes nuevas antes de listar.
        empresa = cobro.sincronizar_fuentes_planes(empresa, col_planes, col_empresas)
        consumo_mes = cobro.totales_periodo(empresa["_id"], periodo_actual)
        saldo = 0
        for cierre in col_periodos.find({"empresa_id": empresa["_id"], "estado": "PENDIENTE_COBRO"}):
            saldo += int((cierre.get("totales") or {}).get("total_cop") or 0)
        planes_salida = []
        for n, entrada in enumerate(cobro._planes_efectivos(empresa, col_planes)):
            plan_doc = col_planes.find_one({"_id": entrada.get("plan_id")}) if entrada.get("plan_id") else None
            if not plan_doc:
                continue
            planes_salida.append({
                "id": str(plan_doc["_id"]),
                "entrada_id": f"{plan_doc['_id']}:{entrada.get('fuente')}",  # único por (plan, fuente)
                "nombre": plan_doc.get("nombre", ""),
                "precio_por_estudio": plan_doc.get("precio_por_estudio", 0),
                "fuentes_incluidas": plan_doc.get("fuentes_incluidas", []),
                "fuente": entrada.get("fuente"),
                # Entrada de fuente retirada del catálogo: sobrevive (historial)
                # pero ya no es consumible — el panel la pinta gris.
                "retirada": entrada.get("fuente") not in (plan_doc.get("fuentes_incluidas") or []),
                "ilimitado": entrada.get("cupo_autorizado") is None,
                "cupo_autorizado": entrada.get("cupo_autorizado"),
                "cupo_consumido": entrada.get("cupo_consumido", 0),
                "cupo_disponible": entrada.get("cupo_disponible"),
                "vence_en": _vence_en(plan_doc, entrada.get("asignado_en")),
            })
        items.append({
            "id": str(empresa["_id"]),
            "nombre": empresa.get("nombre", ""),
            "activo": empresa.get("activo", True),
            "planes": planes_salida,
            "consumo_mes_actual": {
                "periodo": periodo_actual,
                "unidades": consumo_mes.get("unidades", 0),
                "cop": consumo_mes.get("subtotal_cop", 0),
            },
            "saldo_pendiente_cop": saldo,
        })
    return {"total": len(items), "items": items}


class AsignarPlanCompleto(BaseModel):
    plan_id: str
    cupo_autorizado: int | None = None  # None = sin tope; se aplica a TODAS las fuentes del plan


@router.put("/empresas/{empresa_id}/plan", status_code=200)
def asignar_plan_completo(
    empresa_id: str,
    datos: AsignarPlanCompleto,
    request: Request,
    actor: dict = Depends(_requiere_admin_integra),
):
    """Agrega UN plan a la empresa cubriendo TODAS las fuentes que incluye.

    Planes ACUMULABLES: si la fuente ya tenía otro plan, este se suma (el
    consumo gasta FIFO por fecha de asignación). Si ya tenía ESTE mismo plan
    en una fuente, actualiza su cupo (resetea al valor nuevo, conservando
    nada) — es el "reasignar mismo plan" del admin.
    """
    empresa = _empresa_o_404(empresa_id)
    plan = col_planes.find_one({"_id": _oid(datos.plan_id, "plan_id")})
    if not plan:
        raise HTTPException(status_code=404, detail="Plan no encontrado")
    if not plan.get("activo", True):
        raise HTTPException(status_code=422, detail="El plan está inactivo; actívelo primero")
    fuentes_plan = [f for f in (plan.get("fuentes_incluidas") or []) if f in FUENTES]
    if not fuentes_plan:
        raise HTTPException(status_code=422, detail=f"El plan {plan.get('nombre')} no incluye fuentes válidas ({list(FUENTES)})")
    if datos.cupo_autorizado is not None and datos.cupo_autorizado < 0:
        raise HTTPException(status_code=422, detail="El cupo no puede ser negativo")

    # Backfill implícito si la empresa aún vive en el subdoc viejo.
    if not (empresa.get("planes") or []):
        derivados = cobro._planes_efectivos(empresa, col_planes)
        col_empresas.update_one(
            {"_id": empresa["_id"]},
            {"$set": {"planes": derivados}, "$unset": {"plan": ""}},
        )
        empresa["planes"] = derivados

    planes_actuales = list(empresa.get("planes") or [])
    ahora = _utcnow()
    ya_estaba = False
    for fuente in fuentes_plan:
        # Mismo plan ya asignado a esta fuente → reemplaza su entrada (update).
        idx = next(
            (i for i, e in enumerate(planes_actuales)
             if e.get("fuente") == fuente and e.get("plan_id") == plan["_id"]),
            None,
        )
        nueva_entrada = {
            "plan_id": plan["_id"],
            "plan_nombre": plan.get("nombre", ""),
            "fuente": fuente,
            "precio_congelado": int(plan.get("precio_por_estudio") or 0),
            "cupo_autorizado": None if datos.cupo_autorizado is None else int(datos.cupo_autorizado),
            "cupo_disponible": None if datos.cupo_autorizado is None else int(datos.cupo_autorizado),
            "cupo_consumido": 0,
            "asignado_por": actor.get("usuario", ""),
            "asignado_en": ahora,
        }
        if idx is not None:
            ya_estaba = True
            planes_actuales[idx] = nueva_entrada
            col_empresas.update_one(
                {"_id": empresa["_id"], f"planes.{idx}.fuente": fuente},
                {"$set": {f"planes.{idx}": nueva_entrada}},
            )
        else:
            planes_actuales.append(nueva_entrada)
            col_empresas.update_one({"_id": empresa["_id"]}, {"$push": {"planes": nueva_entrada}})

    registrar_evento(
        "plan_asignado", actor=actor,
        detalle=f"{empresa.get('nombre')} · plan {plan.get('nombre')} → {fuentes_plan} · "
                f"cupo {datos.cupo_autorizado if datos.cupo_autorizado is not None else 'ilimitado'}"
                f"{' (actualizado)' if ya_estaba else ''}",
        request=request,
    )
    return {
        "empresa": empresa.get("nombre"),
        "plan": plan.get("nombre", ""),
        "fuentes": fuentes_plan,
        "cupo_autorizado": datos.cupo_autorizado,
        "actualizado": ya_estaba,
    }


@router.delete("/empresas/{empresa_id}/plan/{plan_id}")
def quitar_plan_completo(empresa_id: str, plan_id: str, request: Request, actor: dict = Depends(_requiere_admin_integra)):
    """Quita TODAS las entradas de un plan de la empresa (todas sus fuentes)."""
    empresa = _empresa_o_404(empresa_id)
    pid = _oid(plan_id, "plan_id")
    entradas = [e for e in (empresa.get("planes") or []) if e.get("plan_id") == pid]
    if not entradas:
        raise HTTPException(status_code=404, detail="La empresa no tiene ese plan asignado")
    consumido = sum(int(e.get("cupo_consumido") or 0) for e in entradas)
    if consumido > 0:
        # No romper trazabilidad del consumo ya facturado: rechazar y sugerir
        # quitar una vez cerrado el período, o dejar cupo 0.
        raise HTTPException(
            status_code=422,
            detail=f"El plan ya tiene {consumido} consumo(s) registrados. Cierre el período antes de retirarlo, o asigne cupo 0.",
        )
    col_empresas.update_one({"_id": empresa["_id"]}, {"$pull": {"planes": {"plan_id": pid}}})
    registrar_evento(
        "plan_retirado", actor=actor,
        detalle=f"{empresa.get('nombre')} · plan {plan_id} ({len(entradas)} fuente(s))",
        request=request,
    )
    return {"empresa": empresa.get("nombre"), "fuentes_retiradas": [e.get("fuente") for e in entradas]}


# ═══════════════════════════ API keys (integraciones) ═══════════════════════════
# Credenciales de máquina a máquina para que un cliente consuma el servicio por
# API (`Authorization: Bearer sek_…`). El actor derivado es CONSULTADOR de la
# empresa (jamás admin) y sus consultas/movimientos quedan con canal="api".

class ApiKeyCrear(BaseModel):
    nombre: str


@router.get("/empresas/{empresa_id}/api-keys")
def listar_api_keys(empresa_id: str, actor: dict = Depends(_requiere_admin_integra)):
    """API keys de la empresa (sin hash; el prefijo identifica cada una)."""
    empresa = _empresa_o_404(empresa_id)
    items = []
    for doc in col_api_keys.find({"empresa_id": empresa["_id"]}).sort("creado_en", -1):
        items.append(_serializar(doc) | {"id": str(doc["_id"])})
    return {"empresa": empresa.get("nombre"), "items": items}


@router.post("/empresas/{empresa_id}/api-keys", status_code=201)
def crear_api_key(empresa_id: str, datos: ApiKeyCrear, request: Request, actor: dict = Depends(_requiere_admin_integra)):
    """Crea una API key. La clave completa se devuelve ESTA ÚNICA vez: guardarla
    de inmediato (en BD solo queda su hash; no hay forma de recuperarla)."""
    empresa = _empresa_o_404(empresa_id)
    if not empresa.get("activo", True):
        raise HTTPException(status_code=422, detail="La empresa está inactiva")
    nombre = datos.nombre.strip()
    if not 3 <= len(nombre) <= 40:
        raise HTTPException(status_code=422, detail="El nombre debe tener entre 3 y 40 caracteres")
    if col_api_keys.count_documents({"empresa_id": empresa["_id"], "activo": True}) >= 10:
        raise HTTPException(status_code=422, detail="Máximo 10 API keys activas por empresa")

    from Funciones.auth_seguridad import generar_api_key

    clave, doc = generar_api_key(nombre, empresa["_id"], actor.get("usuario", ""))
    resultado = col_api_keys.insert_one(doc)
    registrar_evento(
        "api_key_creada", actor=actor,
        detalle=f"{empresa.get('nombre')} · {nombre} · {doc['prefijo']}",
        request=request,
    )
    return {
        "id": str(resultado.inserted_id),
        "nombre": nombre,
        "prefijo": doc["prefijo"],
        # ÚNICO momento en que la clave existe en claro.
        "api_key": clave,
        "scopes": doc["scopes"],
        "creado_en": doc["creado_en"],
    }


@router.delete("/empresas/{empresa_id}/api-keys/{key_id}")
def revocar_api_key(empresa_id: str, key_id: str, request: Request, actor: dict = Depends(_requiere_admin_integra)):
    """Revoca (soft: activo=False) una API key. Las consultas en vuelo con esa
    clave fallan en el PRÓXIMO request (cada uso relee la BD)."""
    empresa = _empresa_o_404(empresa_id)
    doc = col_api_keys.find_one({"_id": _oid(key_id, "key_id"), "empresa_id": empresa["_id"]})
    if not doc:
        raise HTTPException(status_code=404, detail="API key no encontrada en esta empresa")
    if not doc.get("activo", True):
        raise HTTPException(status_code=409, detail="La API key ya estaba revocada")
    col_api_keys.update_one({"_id": doc["_id"]}, {"$set": {"activo": False, "revocada_en": _utcnow()}})
    registrar_evento(
        "api_key_revocada", actor=actor,
        detalle=f"{empresa.get('nombre')} · {doc.get('nombre')} · {doc.get('prefijo')}",
        request=request,
    )
    return {"empresa": empresa.get("nombre"), "revocada": doc.get("nombre"), "prefijo": doc.get("prefijo")}


@router.get("/dashboard")
def dashboard(actor: dict = Depends(_requiere_admin_integra)):
    periodo_actual = cobro.periodo_colombia()
    empresas = list(col_empresas.find({"activo": True}))
    con_plan = sum(1 for e in empresas if (e.get("planes") or e.get("plan")))
    # Catálogo una sola vez: excluir del cupo global las entradas cuya fuente
    # fue retirada del plan (cupos no consumibles que sobrerreportarían).
    catalogo = {p["_id"]: p for p in col_planes.find()}

    def _cupo_consumible(entrada: dict) -> bool:
        if entrada.get("fuente") == "todas":
            return True
        plan_doc = catalogo.get(entrada.get("plan_id"))
        return bool(plan_doc) and entrada.get("fuente") in (plan_doc.get("fuentes_incluidas") or [])

    cupo_global = sum(
        int(p.get("cupo_disponible") or 0)
        for e in empresas
        for p in (e.get("planes") or [])
        if p.get("cupo_disponible") is not None and _cupo_consumible(p)
    )
    consumo_mes = 0
    cartera = 0
    pendientes = 0
    for e in empresas:
        consumo_mes += int(cobro.totales_periodo(e["_id"], periodo_actual).get("subtotal_cop") or 0)
    for cierre in col_periodos.find({"estado": "PENDIENTE_COBRO"}):
        cartera += int((cierre.get("totales") or {}).get("total_cop") or 0)
        pendientes += 1
    return {
        "empresas_activas": len(empresas),
        "con_plan": con_plan,
        "cupo_global_disponible": cupo_global,
        "periodo_actual": periodo_actual,
        "consumo_mes_cop": consumo_mes,
        "cartera_pendiente_cop": cartera,
        "periodos_pendientes": pendientes,
    }


# ═══════════════════ Pagos, ajustes, reembolsos ═══════════════════

class PagoCrear(BaseModel):
    monto_cop: int
    fecha_pago: str
    metodo: str
    referencia: str = ""
    nota: str = ""
    periodo: str | None = None


class AjusteCrear(BaseModel):
    monto_cop: int
    motivo: str
    periodo: str | None = None


class ReembolsoCrear(BaseModel):
    consulta_id: str
    motivo: str


@router.post("/empresas/{empresa_id}/pagos", status_code=201)
def registrar_pago(empresa_id: str, datos: PagoCrear, request: Request, actor: dict = Depends(_requiere_admin_integra)):
    if datos.monto_cop <= 0:
        raise HTTPException(status_code=422, detail="El monto del pago debe ser mayor que 0")
    metodo = (datos.metodo or "").strip().upper()
    if metodo not in cobro.METODOS_PAGO:
        raise HTTPException(status_code=422, detail=f"método inválido; válidos: {sorted(cobro.METODOS_PAGO)}")
    empresa = _empresa_o_404(empresa_id)
    mov = cobro.registrar_pago(
        empresa, actor, datos.monto_cop, datos.fecha_pago, metodo,
        referencia=datos.referencia, nota=datos.nota, periodo=datos.periodo,
    )
    registrar_evento(
        "pago_registrado", actor=actor,
        detalle=f"{empresa.get('nombre')} · ${datos.monto_cop} · {metodo} · período {mov.get('periodo')}",
        request=request,
    )
    if mov.get("periodo_pagado"):
        registrar_evento("periodo_pagado", actor=actor, detalle=f"{empresa.get('nombre')} {mov['periodo_pagado']}", request=request)
    return _serializar(mov)


@router.post("/empresas/{empresa_id}/ajustes", status_code=201)
def registrar_ajuste(empresa_id: str, datos: AjusteCrear, request: Request, actor: dict = Depends(_requiere_admin_integra)):
    if datos.monto_cop == 0:
        raise HTTPException(status_code=422, detail="El ajuste debe ser distinto de 0 (use signo − para descontar)")
    if not (datos.motivo or "").strip():
        raise HTTPException(status_code=422, detail="El motivo es obligatorio")
    empresa = _empresa_o_404(empresa_id)
    mov = cobro.registrar_ajuste(empresa, actor, datos.monto_cop, datos.motivo.strip(), periodo=datos.periodo)
    registrar_evento("ajuste_manual", actor=actor, detalle=f"{empresa.get('nombre')} {datos.monto_cop} · {datos.motivo}", request=request)
    return _serializar(mov)


@router.post("/empresas/{empresa_id}/reembolsos", status_code=201)
def reembolso_manual(empresa_id: str, datos: ReembolsoCrear, request: Request, actor: dict = Depends(_requiere_admin_integra)):
    if not (datos.motivo or "").strip():
        raise HTTPException(status_code=422, detail="El motivo es obligatorio")
    empresa = _empresa_o_404(empresa_id)
    consulta_id = datos.consulta_id.strip().upper()
    consumo = cobro.buscar_consumo(consulta_id, empresa["_id"])
    if not consumo:
        raise HTTPException(status_code=404, detail=f"No hay consumo de {consulta_id} para esta empresa")
    if consumo.get("reembolsado"):
        raise HTTPException(status_code=409, detail=f"El consumo {consulta_id} ya fue reembolsado")
    mov = cobro.reembolsar_consumo(empresa, actor, consumo, datos.motivo.strip(), automatico=False)
    registrar_evento("reembolso", actor=actor, consulta_id=consulta_id, detalle=f"manual · {datos.motivo}", request=request)
    if mov is None:
        return {"consulta_id": consulta_id, "reembolsado": True, "nota": "consumo exento: sin efecto monetario"}
    return _serializar(mov)


# ═══════════════════════════ Movimientos ═══════════════════════════

@router.get("/movimientos")
def listar_movimientos(
    request: Request,
    actor: dict = Depends(_requiere_admin_integra),
    empresa_id: str | None = None,
    tipo: str | None = None,
    periodo: str | None = None,
    fecha_desde: str | None = None,
    fecha_hasta: str | None = None,
    consulta_id: str | None = None,
    limit: int = Query(25, ge=1, le=200),
    skip: int = Query(0, ge=0),
):
    query: dict = {}
    if empresa_id:
        query["empresa_id"] = _oid(empresa_id, "empresa_id")
    if tipo:
        if tipo not in cobro.TIPOS_MOVIMIENTO:
            raise HTTPException(status_code=422, detail=f"tipo inválido; válidos: {sorted(cobro.TIPOS_MOVIMIENTO)}")
        query["tipo"] = tipo
    if periodo:
        query["periodo"] = periodo
    if consulta_id:
        query["consulta_id"] = consulta_id.strip().upper()
    from datetime import timedelta

    try:
        if fecha_desde:
            query.setdefault("creado_en", {})["$gte"] = datetime.fromisoformat(fecha_desde)
        if fecha_hasta:
            query.setdefault("creado_en", {})["$lte"] = datetime.fromisoformat(fecha_hasta) + timedelta(days=1)
    except ValueError:
        raise HTTPException(status_code=422, detail="Fechas deben ser YYYY-MM-DD")

    total = col_movimientos.count_documents(query)
    # Datos del estudio (estado + cédula) para la tabla de la cuenta/factura.
    items = []
    for mov in col_movimientos.find(query).sort("creado_en", -1).skip(skip).limit(limit):
        mov = _serializar(mov)
        if mov.get("consulta_id"):
            estudio = col_estudios.find_one(
                {"consulta_id": mov["consulta_id"]}, {"estado": 1, "cedula": 1}
            )
            if estudio:
                mov["estado_estudio"] = estudio.get("estado")
                mov["cedula"] = enmascarar_cedula(estudio.get("cedula", ""))
        items.append(mov)
    return {"total": total, "items": items}


# ═══════════════════════════ Períodos ═══════════════════════════

class CerrarPeriodo(BaseModel):
    empresa_id: str
    periodo: str
    permitir_vacio: bool = False


class ReabrirPeriodo(BaseModel):
    motivo: str


class CambiarEstadoPeriodo(BaseModel):
    estado: str


@router.get("/periodos")
def listar_periodos(
    actor: dict = Depends(_requiere_admin_integra),
    empresa_id: str | None = None,
    estado: str | None = None,
):
    query: dict = {}
    if empresa_id:
        query["empresa_id"] = _oid(empresa_id, "empresa_id")
    if estado:
        query["estado"] = estado
    items = []
    for doc in col_periodos.find(query).sort([("periodo", -1)]):
        doc = _serializar(doc)
        items.append(doc)
    return {"total": len(items), "items": items}


def _generar_y_subir_pdf_cuenta(empresa: dict, cierre: dict) -> dict:
    """Genera el PDF de la cuenta desde los movimientos CONGELADOS del cierre
    y lo sube a GCS privado. Retorna el bloque pdf para el doc de cierre."""
    movimientos = []
    for mov in col_movimientos.find({"empresa_id": empresa["_id"], "periodo": cierre["periodo"]}):
        movimientos.append(mov)
        if mov.get("consulta_id"):
            estudio = col_estudios.find_one({"consulta_id": mov["consulta_id"]}, {"estado": 1, "cedula": 1})
            if estudio:
                mov["estado_estudio"] = estudio.get("estado")
                mov["cedula"] = estudio.get("cedula")
    contenido = generar_pdf_cuenta(empresa, cierre, movimientos)
    ruta = storage_seguridad.ruta_blob_cuenta(str(empresa["_id"]), cierre["periodo"])
    subido = storage_seguridad.subir_pdf(contenido, ruta, cedula="", content_type="application/pdf")
    return subido


@router.post("/periodos/cerrar", status_code=201)
def cerrar_periodo(datos: CerrarPeriodo, request: Request, actor: dict = Depends(_requiere_admin_integra)):
    empresa = _empresa_o_404(datos.empresa_id)
    cierre = cobro.cerrar_periodo(empresa, actor, datos.periodo, permitir_vacio=datos.permitir_vacio)
    try:
        pdf_info = _generar_y_subir_pdf_cuenta(empresa, cierre)
        cierre["pdf"] = pdf_info
        col_periodos.update_one({"_id": cierre["_id"]}, {"$set": {"pdf": pdf_info}})
    except Exception as exc:
        logger.error("PDF de cuenta %s/%s no se pudo generar: %s", empresa.get("nombre"), datos.periodo, exc)
        cierre["pdf"] = None
    registrar_evento(
        "periodo_cerrado", actor=actor,
        detalle=f"{empresa.get('nombre')} {datos.periodo} · total {(cierre.get('totales') or {}).get('total_cop', 0)} COP",
        request=request,
    )
    cierre["id"] = str(cierre.pop("_id"))
    return _serializar(cierre)


def _cierre_o_404(cierre_id: str) -> dict:
    doc = col_periodos.find_one({"_id": _oid(cierre_id, "cierre_id")})
    if not doc:
        raise HTTPException(status_code=404, detail="Período no encontrado")
    return doc


@router.post("/periodos/{cierre_id}/reabrir")
def reabrir_periodo(cierre_id: str, datos: ReabrirPeriodo, request: Request, actor: dict = Depends(_requiere_admin_integra)):
    if not (datos.motivo or "").strip():
        raise HTTPException(status_code=422, detail="El motivo es obligatorio")
    cierre = _cierre_o_404(cierre_id)
    empresa = col_empresas.find_one({"_id": cierre["empresa_id"]}) or {"_id": cierre["empresa_id"], "nombre": "?"}
    cobro.reabrir_periodo(cierre, actor, datos.motivo.strip())
    registrar_evento(
        "periodo_reabierto", actor=actor,
        detalle=f"{empresa.get('nombre')} {cierre.get('periodo')} · {datos.motivo}",
        request=request,
    )
    return {"id": cierre_id, "reabierto": True}


@router.patch("/periodos/{cierre_id}")
def cambiar_estado_periodo(cierre_id: str, datos: CambiarEstadoPeriodo, request: Request, actor: dict = Depends(_requiere_admin_integra)):
    if datos.estado not in {"PENDIENTE_COBRO", "PAGADA"}:
        raise HTTPException(status_code=422, detail="estado debe ser PENDIENTE_COBRO o PAGADA")
    cierre = _cierre_o_404(cierre_id)
    cambios = {"estado": datos.estado}
    if datos.estado == "PAGADA":
        cambios["pagada_en"] = _utcnow()
    col_periodos.update_one({"_id": cierre["_id"]}, {"$set": cambios})
    registrar_evento("periodo_pagado" if datos.estado == "PAGADA" else "periodo_reabierto", actor=actor, detalle=f"{cierre.get('periodo')} → {datos.estado}", request=request)
    return {"id": cierre_id, "estado": datos.estado}


@router.get("/periodos/{cierre_id}/pdf")
def descargar_pdf_cuenta(
    cierre_id: str,
    request: Request,
    descarga: bool = Query(False),
    url_firmada: bool = Query(False),
    actor: dict = Depends(_requiere_admin_integra),
):
    cierre = _cierre_o_404(cierre_id)
    empresa = col_empresas.find_one({"_id": cierre["empresa_id"]}) or {"_id": cierre["empresa_id"], "nombre": "?"}

    # Si el cierre quedó sin PDF (fallo al cerrar), se genera on-demand.
    if not cierre.get("pdf") or not cierre["pdf"].get("gcs_ruta"):
        pdf_info = _generar_y_subir_pdf_cuenta(empresa, cierre)
        col_periodos.update_one({"_id": cierre["_id"]}, {"$set": {"pdf": pdf_info}})
        cierre["pdf"] = pdf_info

    registrar_evento("pdf_cuenta_descargado", actor=actor, detalle=f"{cierre.get('periodo')}", request=request)
    if url_firmada:
        return {"url": storage_seguridad.generar_url_firmada(cierre["pdf"]["gcs_ruta"])}
    try:
        contenido = storage_seguridad.descargar_blob(cierre["pdf"]["gcs_ruta"])
    except Exception as exc:
        logger.error("Cuenta %s no se pudo descargar: %s", cierre_id, exc)
        raise HTTPException(status_code=502, detail="No fue posible recuperar la cuenta de cobro")
    nombre = f"cuenta_cobro_{(cierre.get('empresa_nombre') or 'empresa').replace(' ', '_')}_{cierre.get('periodo')}.pdf"
    return Response(
        content=contenido,
        media_type="application/pdf",
        headers={"Content-Disposition": f"{'attachment' if descarga else 'inline'}; filename={nombre}"},
    )


@router.post("/periodos/{cierre_id}/pdf/regenerar")
def regenerar_pdf_cuenta(cierre_id: str, request: Request, actor: dict = Depends(_requiere_admin_integra)):
    """Reconstruye el PDF con los TOTALES congelados del cierre (no recalcula)."""
    cierre = _cierre_o_404(cierre_id)
    empresa = col_empresas.find_one({"_id": cierre["empresa_id"]}) or {"_id": cierre["empresa_id"], "nombre": "?"}
    pdf_info = _generar_y_subir_pdf_cuenta(empresa, cierre)
    col_periodos.update_one({"_id": cierre["_id"]}, {"$set": {"pdf": pdf_info}})
    registrar_evento("pdf_cuenta_descargado", actor=actor, detalle=f"regenerado {cierre.get('periodo')}", request=request)
    return {"id": cierre_id, "pdf": pdf_info}
