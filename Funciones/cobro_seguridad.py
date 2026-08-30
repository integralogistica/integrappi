"""Motor de cobro postpago de los Estudios de Seguridad (sin HTTP).

Modelo (ver plan 2026-08-29):
  - La empresa tiene un `plan` (de `planes_seguridad`) con `cupo_autorizado`.
  - Cada estudio consume 1 unidad: `find_one_and_update` ATÓMICO con condición
    `cupo_disponible > 0` — dos consultas simultáneas jamás se pasan del cupo.
  - El precio se congela (snapshot) en el movimiento CONSUMO: cambiar el plan
    después no recalcula lo ya consumido.
  - Estudio con estado global ERROR → reembolso automático (devuelve unidad y
    COP), idempotente por índice único parcial (consulta_id, tipo=REEMBOLSO).
  - UNA colección de movimientos con tipo y signo:
      CONSUMO  (+unidades, +cop)   REEMBOLSO (−unidades, −cop)
      PAGO     ( 0, −cop)          AJUSTE   ( 0, ±cop)
  - Cierre de mes: congela totales por `periodo` ("YYYY-MM" en hora Colombia)
    y genera la cuenta de cobro; los períodos cerrados son inmutables (409).

ADMIN_INTEGRA con empresa sin plan: consumo EXENTO (monto 0) para trazabilidad.
Válvula de despliegue: SEGURIDAD_COBRO_EXIGIR_PLAN=false desactiva la exigencia.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from bson import ObjectId
from fastapi import HTTPException
from pymongo.errors import DuplicateKeyError

from bd.bd_cliente import bd_cliente

logger = logging.getLogger(__name__)

EXIGIR_PLAN = os.getenv("SEGURIDAD_COBRO_EXIGIR_PLAN", "true").lower() in ("1", "true", "yes")

# El período de cobro se define en hora Colombia (UTC−5, sin DST).
_TZ_CO = timezone(timedelta(hours=-5))

TIPO_CONSUMO = "CONSUMO"
TIPO_REEMBOLSO = "REEMBOLSO"
TIPO_PAGO = "PAGO"
TIPO_AJUSTE = "AJUSTE"
TIPOS_MOVIMIENTO = {TIPO_CONSUMO, TIPO_REEMBOLSO, TIPO_PAGO, TIPO_AJUSTE}
METODOS_PAGO = {"TRANSFERENCIA", "EFECTIVO", "OTRO"}

db = bd_cliente["integra"]
col_planes = db["planes_seguridad"]
col_movimientos = db["movimientos_cobro_seguridad"]
col_periodos = db["periodos_cobro_seguridad"]
col_empresas = db["empresas_seguridad"]

_indices_creados = False


def asegurar_indices_cobro() -> None:
    """Índices idempotentes (se llama al importar el router; tolera fallo)."""
    global _indices_creados
    if _indices_creados:
        return
    try:
        col_planes.create_index([("nombre", 1)], name="idx_planseg_nombre", unique=True)
        col_movimientos.create_index(
            [("empresa_id", 1), ("periodo", 1), ("tipo", 1)], name="idx_mov_empresa_periodo_tipo"
        )
        col_movimientos.create_index(
            [("empresa_id", 1), ("creado_en", -1)], name="idx_mov_empresa_fecha"
        )
        # Anti doble reembolso: un solo REEMBOLSO por (consulta, consumo). El
        # índice viejo (consulta_id, tipo) asumía 1 consumo por consulta; con
        # cobro por fuente hay N. Dropear el viejo si existe.
        col_movimientos.create_index(
            [("consulta_id", 1), ("consumo_id", 1), ("tipo", 1)],
            name="idx_mov_reembolso_unico_v2",
            unique=True,
            partialFilterExpression={"tipo": TIPO_REEMBOLSO},
        )
        try:
            col_movimientos.drop_index("idx_mov_reembolso_unico")
        except Exception:
            pass  # no existía (deploy fresco)
        col_periodos.create_index(
            [("empresa_id", 1), ("periodo", 1)], name="idx_perseg_empresa_periodo", unique=True
        )
        _indices_creados = True
    except Exception as exc:
        logger.warning("Índices de cobro no se pudieron crear: %s", exc)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def periodo_colombia(dt: datetime | None = None) -> str:
    """'YYYY-MM' en hora Colombia. Cruce de medianoche: 2026-09-01 02:00 UTC
    sigue siendo 2026-08-31 en Colombia → '2026-08'."""
    dt = dt or _utcnow()
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(_TZ_CO).strftime("%Y-%m")


def periodo_actual_es(periodo: str) -> bool:
    return periodo >= periodo_colombia()


# ── Validación de plan ────────────────────────────────────────────────────────

def _plan_vigente(plan_doc: dict | None, asignado_en: datetime | None = None) -> tuple[dict | None, str | None]:
    """(plan, motivo_de_rechazo). motivo None = plan OK para consumir.
    La vigencia cuenta desde la ASIGNACIÓN del plan a la empresa."""
    if not plan_doc or not plan_doc.get("activo", True):
        return None, "Su empresa no tiene un plan activo. Contacte a Integra Logística para activar su servicio de consultas."
    vigencia = plan_doc.get("vigencia_dias")
    if vigencia:
        base = asignado_en or plan_doc.get("creado_en")
        if base:
            if base.tzinfo is None:
                base = base.replace(tzinfo=timezone.utc)
            vence = (base + timedelta(days=int(vigencia))).astimezone(_TZ_CO)
            if _utcnow().replace(tzinfo=timezone.utc) >= vence:
                return None, f"Su plan venció el {vence.strftime('%d/%m/%Y')}. Contacte a Integra Logística para renovarlo."
    return plan_doc, None


# ── Planes por fuente ─────────────────────────────────────────────────────────

def _planes_efectivos(empresa: dict, col_pla: Any = None) -> list[dict]:
    """Entradas de plan de la empresa NORMALIZADAS a la forma array `planes`
    (una por fuente): [{plan_id, plan_nombre, fuente, cupo_*, asignado_en}].

    Fallback transitorio: si la empresa aún tiene el subdoc `plan` viejo (una
    empresa = un plan) y no el array, sintetiza una entrada por fuente incluida
    en ese plan — así el deploy del código nuevo puede ir antes del backfill.
    """
    col_pla = col_pla if col_pla is not None else col_planes
    entradas = list(empresa.get("planes") or [])
    if entradas:
        return entradas
    viejo = empresa.get("plan") or {}
    if not viejo.get("plan_id"):
        return []
    plan_doc = col_pla.find_one({"_id": viejo["plan_id"]}) or {}
    fuentes = [f for f in plan_doc.get("fuentes_incluidas", []) if f] or ["todas"]
    return [
        {
            **viejo,
            "fuente": f,
            "plan_nombre": plan_doc.get("nombre", viejo.get("plan_nombre", "")),
        }
        for f in fuentes
    ]


def _posicion_plan(planes: list[dict], fuente: str) -> int | None:
    """Índice del elemento del array `planes` de esa fuente (None si no está)."""
    for i, entrada in enumerate(planes):
        if entrada.get("fuente") == fuente:
            return i
    return None


def _entrada_consumible(entrada: dict, plan_doc: dict | None) -> tuple[bool, dict | None, str | None]:
    """(consumible, plan_doc, motivo). Unifica vigencia + membresía de fuente.

    Además de _plan_vigente, exige que la fuente de la entrada SIGA en
    `plan_doc.fuentes_incluidas`: si el admin retiró la fuente del catálogo,
    la entrada de la empresa sobrevive (historial de cobro) pero deja de ser
    consumible. La fuente sintética "todas" (fallback pre-backfill del subdoc
    viejo) nunca se bloquea: no representa una fuente real del catálogo.
    """
    plan_doc, rechazo = _plan_vigente(plan_doc, asignado_en=entrada.get("asignado_en"))
    if rechazo or not plan_doc:
        return False, plan_doc, rechazo
    fuente = entrada.get("fuente")
    if fuente and fuente != "todas" and fuente not in (plan_doc.get("fuentes_incluidas") or []):
        return False, plan_doc, "fuente retirada del plan"
    return True, plan_doc, None


def sincronizar_fuentes_planes(
    empresa: dict,
    col_pla: Any = None,
    col_emp: Any = None,
) -> dict:
    """Propaga las fuentes NUEVAS del catálogo a las entradas de la empresa.

    El catálogo manda: si un plan pasó de 2 a 3 fuentes, toda empresa con ese
    plan gana la 3ª entrada aquí — sin reasignar y sin tocar nada existente
    (nada se resetea; para el cliente es transparente). Fuente RETIRADA del
    catálogo no se escribe: la entrada sobrevive intacta y pasa a ser no
    consumible vía _entrada_consumible.

    Reglas de la entrada nueva:
      - precio_congelado = precio ACTUAL del catálogo (decisión de negocio).
      - cupo_autorizado/cupo_disponible clonados de la hermana más antigua
        del mismo plan (menor asignado_en; None = hermana ilimitada).
      - cupo_consumido 0, asignado_en ahora ⇒ ventana de vigencia completa
        (heredar asignado_en crearía entradas nacidas vencidas).
      - asignado_por "sync_catalogo" como marcador de auditoría.

    Concurrencia: $push con guard $not $elemMatch — si dos consultas ven el
    mismo hueco, solo el primer update modifica; el otro ve modified_count 0
    y continúa. Sin duplicados, sin índice extra.

    ⚠️ $push NUNCA reordena el array: los índices posicionales `planes.{pos}`
    del $inc de reservar_consumos siguen válidos durante una carrera. NO
    cambiar por $set del array completo: perdería $inc concurrentes.

    Degradación: fallo de escritura → logger.warning y doc sin sincronizar
    (un read path jamás propaga 5xx por el sync). Devuelve el doc FRESCO.
    """
    col_pla = col_pla if col_pla is not None else col_planes
    col_emp = col_emp if col_emp is not None else col_empresas
    entradas = list(empresa.get("planes") or [])
    if not entradas:
        # Subdoc viejo: ya deriva las fuentes del catálogo en runtime.
        return empresa

    cache: dict[Any, dict | None] = {}
    ahora = _utcnow()
    try:
        for pid in {e.get("plan_id") for e in entradas if e.get("plan_id")}:
            plan_doc = cache.get(pid) or col_pla.find_one({"_id": pid})
            cache[pid] = plan_doc
            if not plan_doc or not plan_doc.get("activo", True):
                continue  # no agrega nada, no borra nada
            hermanas = sorted(
                (e for e in entradas if e.get("plan_id") == pid),
                key=lambda e: e.get("asignado_en") or _utcnow(),
            )
            if not hermanas:
                continue
            molde = hermanas[0]  # la más antigua define el cupo del plan
            for fuente in plan_doc.get("fuentes_incluidas") or []:
                if any(e.get("fuente") == fuente for e in entradas):
                    continue  # ya existe (de este u otro plan: acumulable)
                nueva = {
                    "plan_id": pid,
                    "plan_nombre": plan_doc.get("nombre", ""),
                    "fuente": fuente,
                    "precio_congelado": int(plan_doc.get("precio_por_estudio") or 0),
                    "cupo_autorizado": molde.get("cupo_autorizado"),
                    "cupo_disponible": molde.get("cupo_autorizado"),
                    "cupo_consumido": 0,
                    "asignado_por": "sync_catalogo",
                    "asignado_en": ahora,
                }
                resultado = col_emp.update_one(
                    {"_id": empresa["_id"],
                     "planes": {"$not": {"$elemMatch": {"plan_id": pid, "fuente": fuente}}}},
                    {"$push": {"planes": nueva}},
                )
                if getattr(resultado, "modified_count", 0):
                    entradas.append(nueva)  # reflejar localmente para las siguientes fuentes
    except Exception as exc:
        logger.warning("sync fuentes de %s degradado (%s); se continúa con las entradas actuales",
                       empresa.get("nombre"), exc)
    fresco = col_emp.find_one({"_id": empresa["_id"]})
    return fresco if fresco else empresa


def fuentes_con_plan(empresa: dict, fuentes: list[str], col_pla: Any = None) -> list[str]:
    """De las fuentes pedidas, cuáles tienen plan VIGENTE asignado a la empresa.

    Subdoc viejo sin array `planes`: si el plan viejo está vigente, pasan todas
    las fuentes que incluya (comportamiento pre-multi-plan).
    """
    col_pla = col_pla if col_pla is not None else col_planes
    if not (empresa.get("planes") or []):
        viejo = empresa.get("plan") or {}
        if not viejo.get("plan_id"):
            return []
        plan_doc = col_pla.find_one({"_id": viejo["plan_id"]})
        plan_doc, rechazo = _plan_vigente(plan_doc, asignado_en=viejo.get("asignado_en"))
        if rechazo or not plan_doc:
            return []
        incluidas = [f for f in plan_doc.get("fuentes_incluidas", []) if f in fuentes]
        return incluidas or list(fuentes)
    resultado = []
    for entrada in empresa["planes"]:
        fuente = entrada.get("fuente")
        if fuente not in fuentes or fuente in resultado:
            continue
        plan_doc = col_pla.find_one({"_id": entrada.get("plan_id")}) if entrada.get("plan_id") else None
        consumible, plan_doc, _ = _entrada_consumible(entrada, plan_doc)
        if consumible:
            resultado.append(fuente)
    return resultado


def reservar_consumos(
    empresa: dict,
    actor: dict,
    consulta_id: str,
    fuentes_a_correr: list[str],
    plan_preferido_id: Any = None,
    col_mov: Any = None,
    col_emp: Any = None,
    col_pla: Any = None,
) -> list[dict]:
    """Reserva 1 unidad por cada FUENTE a correr y registra un CONSUMO por una.

    Multi-plan: cada fuente consume del plan asignado a esa fuente (cupo propio
    o None = sin tope). Planes acumulables: varias entradas por fuente; el
    consumo gasta FIFO por asignado_en, SALVO que el usuario haya elegido un
    plan (plan_preferido_id): entonces se gasta ese primero (y si se agota,
    cae al siguiente como fallback). Compensación en cascada: si la reserva de
    la fuente N falla, se devuelven las ya hechas y se lanza 402 nombrando la
    fuente.

    ADMIN_INTEGRA sin planes vigentes → 1 movimiento exento (trazabilidad).
    Válvula EXIGIR_PLAN=false → no bloquea ni descuenta.
    """
    col_mov = col_mov if col_mov is not None else col_movimientos
    col_emp = col_emp if col_emp is not None else col_empresas
    col_pla = col_pla if col_pla is not None else col_planes

    # Empresa aún en el subdoc viejo (deploy antes del backfill): delegar en la
    # reserva de subdoc único (1 unidad, 1 precio) sobre plan.*.
    if not (empresa.get("planes") or []):
        consumo = reservar_consumo(empresa, actor, consulta_id, col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        return [consumo] if consumo else []

    planes = _planes_efectivos(empresa, col_pla)
    ahora = _utcnow()
    periodo = periodo_colombia(ahora)

    # Resolver entradas vigentes por fuente. Multi-plan: una fuente puede
    # tener VARIAS entradas (planes acumulables); se ordenan FIFO por
    # asignado_en y el consumo gasta la primera con cupo.
    resueltos: list[dict] = []  # {fuente, candidatos: [{entrada, plan_doc, pos}]}
    for fuente in fuentes_a_correr:
        candidatos = []
        for i, entrada in enumerate(planes):
            if entrada.get("fuente") != fuente:
                continue
            plan_doc = col_pla.find_one({"_id": entrada.get("plan_id")}) if entrada.get("plan_id") else None
            consumible, plan_doc, _ = _entrada_consumible(entrada, plan_doc)
            if not consumible:
                continue
            candidatos.append({"entrada": entrada, "plan_doc": plan_doc, "pos": i})
        # FIFO: la asignación más antigua se consume primero. Si el usuario
        # eligió un plan, ese plan pasa al frente (elección explícita > FIFO).
        candidatos.sort(key=lambda c: c["entrada"].get("asignado_en") or _utcnow())
        if plan_preferido_id is not None:
            elegido = [c for c in candidatos if c["entrada"].get("plan_id") == plan_preferido_id]
            resto = [c for c in candidatos if c["entrada"].get("plan_id") != plan_preferido_id]
            if elegido:
                candidatos = elegido + resto  # fallback si el elegido se agota
        if candidatos:
            resueltos.append({"fuente": fuente, "candidatos": candidatos})

    if not resueltos:
        # Ninguna fuente con plan vigente.
        if actor.get("rol") == "ADMIN_INTEGRA":
            mov = _nuevo_movimiento(
                empresa, actor, TIPO_CONSUMO, unidades=0, monto_cop=0, consulta_id=consulta_id,
                plan=None, periodo=periodo, exento=True,
            )
            col_mov.insert_one(mov)
            return [mov]
        if not EXIGIR_PLAN:
            return []
        raise HTTPException(
            status_code=402,
            detail="Su empresa no tiene un plan activo para ninguna fuente. "
                   "Contacte a Integra Logística para activar su servicio de consultas.",
        )

    movimientos: list[dict] = []
    reservados: list[dict] = []  # para compensación en cascada

    def _compensar():
        # La consulta NO se va a ejecutar: revertir cupos y borrar los
        # movimientos ya insertados (no puede quedar consumo de algo que no corrió).
        for r in reservados:
            entrada, pos = r["entrada"], r.get("pos", _posicion_plan(planes, r["entrada"].get("fuente")) or 0)
            reversa = {f"planes.{pos}.cupo_consumido": -1}
            if entrada.get("cupo_autorizado") is not None:
                reversa[f"planes.{pos}.cupo_disponible"] = 1
            try:
                col_emp.update_one(
                    {"_id": empresa["_id"], f"planes.{pos}.fuente": entrada.get("fuente")},
                    {"$inc": reversa},
                )
            except Exception as exc:
                logger.critical(
                    "Cupo de %s (fuente %s) quedó inconsistente tras compensación: %s",
                    empresa.get("nombre"), entrada.get("fuente"), exc,
                )
        for mov in movimientos:
            try:
                col_mov.delete_one({"_id": mov["_id"]})
            except Exception as exc:
                logger.critical("Movimiento huérfano %s tras compensación: %s", mov.get("_id"), exc)

    for r in resueltos:
        fuente = r["fuente"]
        reservado = None
        agotados: list[str] = []
        for cand in r["candidatos"]:
            entrada, plan_doc, pos = cand["entrada"], cand["plan_doc"], cand["pos"]
            ilimitado = entrada.get("cupo_autorizado") is None
            filtro: dict = {"_id": empresa["_id"], f"planes.{pos}.fuente": entrada.get("fuente")}
            cambios: dict = {"$inc": {f"planes.{pos}.cupo_consumido": 1}}
            if not ilimitado:
                filtro[f"planes.{pos}.cupo_disponible"] = {"$gt": 0}
                cambios["$inc"][f"planes.{pos}.cupo_disponible"] = -1
            actualizada = col_emp.find_one_and_update(filtro, cambios, return_document=True)
            if actualizada is not None:
                reservado = cand
                break
            # Este plan no tiene cupo (o hubo carrera del posicional): releer
            # la posición por si el array se reordenó, y probar el siguiente.
            fresca = col_emp.find_one({"_id": empresa["_id"]}) or {}
            planes_frescos = _planes_efectivos(fresca, col_pla)
            pos_fresca = None
            for i, ent in enumerate(planes_frescos):
                if ent.get("fuente") == entrada.get("fuente") and ent.get("plan_id") == entrada.get("plan_id"):
                    pos_fresca = i
                    break
            if pos_fresca is not None and pos_fresca != pos:
                cand["pos"] = pos = pos_fresca
                filtro = {"_id": empresa["_id"], f"planes.{pos}.fuente": entrada.get("fuente")}
                cambios = {"$inc": {f"planes.{pos}.cupo_consumido": 1}}
                if not ilimitado:
                    filtro[f"planes.{pos}.cupo_disponible"] = {"$gt": 0}
                    cambios["$inc"][f"planes.{pos}.cupo_disponible"] = -1
                actualizada = col_emp.find_one_and_update(filtro, cambios, return_document=True)
                if actualizada is not None:
                    reservado = cand
                    break
            agotados.append(entrada.get("plan_nombre") or str(entrada.get("plan_id")))

        if reservado is None:
            _compensar()
            detalle = f" ({', '.join(agotados)} agotado(s))" if agotados else ""
            raise HTTPException(
                status_code=402,
                detail=f"Cupo agotado para la fuente {fuente}{detalle}. Contacte a Integra Logística.",
            )

        entrada, plan_doc, pos = reservado["entrada"], reservado["plan_doc"], reservado["pos"]
        reservados.append(reservado)
        ilimitado = entrada.get("cupo_autorizado") is None

        # Precio: el CONGELADO de la asignación (no el del catálogo actual).
        precio = int(entrada.get("precio_congelado") or plan_doc.get("precio_por_estudio") or 0)
        mov = _nuevo_movimiento(
            empresa, actor, TIPO_CONSUMO, unidades=1, monto_cop=precio, consulta_id=consulta_id,
            plan=plan_doc, periodo=periodo, fuente=fuente,
        )
        try:
            col_mov.insert_one(mov)
            movimientos.append(mov)
        except Exception as exc:
            # Revertir ESTA reserva (el movimiento no existe) y las previas.
            logger.error("Movimiento de consumo %s (%s) no se insertó: %s — compensando", consulta_id, fuente, exc)
            reversa = {f"planes.{pos}.cupo_consumido": -1}
            if not ilimitado:
                reversa[f"planes.{pos}.cupo_disponible"] = 1
            try:
                col_emp.update_one(
                    {"_id": empresa["_id"], f"planes.{pos}.fuente": fuente},
                    {"$inc": reversa},
                )
            except Exception as exc2:
                logger.critical("Cupo de %s inconsistente tras fallo de insert: %s", empresa.get("nombre"), exc2)
            _compensar()
            raise HTTPException(status_code=503, detail="No se pudo registrar el consumo. Intente de nuevo.")
    return movimientos


def reembolsar_consumos_consulta(
    consulta_id: str,
    empresa: dict,
    actor: dict,
    motivo: str,
    automatico: bool = False,
    col_mov: Any = None,
    col_emp: Any = None,
    col_pla: Any = None,
) -> list[dict]:
    """Reembolsa TODOS los consumos no reembolsados de una consulta (ERROR
    global). Idempotente: cada consumo tiene flag `reembolsado` + índice único
    por (consulta_id, consumo_id, REEMBOLSO)."""
    col_mov = col_mov if col_mov is not None else col_movimientos
    col_emp = col_emp if col_emp is not None else col_empresas
    col_pla = col_pla if col_pla is not None else col_planes
    reembolsos = []
    for consumo in col_mov.find({
        "consulta_id": consulta_id.strip().upper(),
        "empresa_id": empresa["_id"],
        "tipo": TIPO_CONSUMO,
        "reembolsado": {"$ne": True},
    }):
        mov = reembolsar_consumo(empresa, actor, consumo, motivo, automatico=automatico, col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        if mov:
            reembolsos.append(mov)
    return reembolsos


# ── Consumo atómico (interfaz vieja de 1 plan — compatibilidad) ──────────────

def reservar_consumo(
    empresa: dict,
    actor: dict,
    consulta_id: str,
    col_mov: Any = None,
    col_emp: Any = None,
    col_pla: Any = None,
) -> dict:
    """Reserva 1 unidad del cupo de la empresa y registra el CONSUMO.

    Retorna el movimiento creado. Levanta HTTPException 402 si no hay plan,
    está vencido o no hay cupo. ADMIN_INTEGRA sin plan → consumo exento.
    Plan con cupo_autorizado None (ilimitado): solo registra el consumo COP,
    nunca bloquea por unidades (pospago puro).
    """
    col_mov = col_mov if col_mov is not None else col_movimientos
    col_emp = col_emp if col_emp is not None else col_empresas
    col_pla = col_pla if col_pla is not None else col_planes

    plan_ref = empresa.get("plan") or {}
    plan_doc = col_pla.find_one({"_id": plan_ref.get("plan_id")}) if plan_ref.get("plan_id") else None
    plan_doc, rechazo = _plan_vigente(plan_doc, asignado_en=plan_ref.get("asignado_en"))
    ahora = _utcnow()
    periodo = periodo_colombia(ahora)

    # ADMIN_INTEGRA con empresa sin plan vigente: exento (trazabilidad sin costo).
    if rechazo and actor.get("rol") == "ADMIN_INTEGRA":
        mov = _nuevo_movimiento(
            empresa, actor, TIPO_CONSUMO, unidades=0, monto_cop=0, consulta_id=consulta_id,
            plan=None, periodo=periodo, exento=True,
        )
        col_mov.insert_one(mov)
        return mov
    if rechazo:
        if not EXIGIR_PLAN:
            # Válvula de despliegue: sin exigir plan no se bloquea (no hay cupo que descontar).
            return {}
        raise HTTPException(status_code=402, detail=rechazo)
    if not EXIGIR_PLAN:
        # Con la válvula cerrada se consume de todas formas (comportamiento activado).
        pass

    ilimitado = plan_ref.get("cupo_autorizado") is None

    # Descuento atómico: condición y contador en el mismo documento.
    # Plan ilimitado: sin condición de cupo (solo contador de consumo).
    filtro: dict = {"_id": empresa["_id"]}
    cambios: dict = {"$inc": {"plan.cupo_consumido": 1}}
    if not ilimitado:
        filtro["plan.cupo_disponible"] = {"$gt": 0}
        cambios["$inc"]["plan.cupo_disponible"] = -1
    actualizada = col_emp.find_one_and_update(filtro, cambios, return_document=True)
    if actualizada is None:
        disponible = int((empresa.get("plan") or {}).get("cupo_disponible") or 0)
        if disponible > 0:
            # El cupo cambió entre la lectura de `empresa` y el update: releer para el mensaje.
            fresca = col_emp.find_one({"_id": empresa["_id"]}) or {}
            disponible = int((fresca.get("plan") or {}).get("cupo_disponible") or 0)
        autorizado = int((empresa.get("plan") or {}).get("cupo_autorizado") or 0)
        raise HTTPException(
            status_code=402,
            detail=f"Cupo de consultas agotado: {disponible} de {autorizado} disponibles. "
                   "Contacte a Integra Logística para recargar.",
        )

    precio = int(plan_doc.get("precio_por_estudio") or 0)
    mov = _nuevo_movimiento(
        empresa, actor, TIPO_CONSUMO, unidades=1, monto_cop=precio, consulta_id=consulta_id,
        plan=plan_doc, periodo=periodo,
    )
    try:
        col_mov.insert_one(mov)
    except Exception as exc:
        # La consulta no se ejecutará: devolver la unidad reservada (best-effort).
        logger.error("Movimiento de consumo %s no se pudo insertar: %s — compensando cupo", consulta_id, exc)
        compensar: dict = {"plan.cupo_consumido": -1}
        if not ilimitado:
            compensar["plan.cupo_disponible"] = 1
        try:
            col_emp.update_one({"_id": empresa["_id"]}, {"$inc": compensar})
        except Exception as exc2:
            logger.critical("Cupo de %s quedó inconsistente tras fallo de insert: %s", empresa.get("nombre"), exc2)
        raise HTTPException(status_code=503, detail="No se pudo registrar el consumo. Intente de nuevo.")
    return mov


def reembolsar_consumo(
    empresa: dict,
    actor: dict,
    consumo: dict,
    motivo: str,
    automatico: bool = False,
    col_mov: Any = None,
    col_emp: Any = None,
    col_pla: Any = None,
) -> dict | None:
    """Compensa un CONSUMO (unidades y COP) si el estudio terminó en ERROR.

    Idempotente por índice único parcial (consulta_id, tipo=REEMBOLSO): un
    segundo intento retorna None sin crear nada. Devuelve cupo SOLO si la
    empresa sigue en el mismo plan_id del consumo. Un consumo de período ya
    cerrado se reembolsa en el período ACTUAL (el cierre es inmutable).
    """
    col_mov = col_mov if col_mov is not None else col_movimientos
    col_emp = col_emp if col_emp is not None else col_empresas
    col_pla = col_pla if col_pla is not None else col_planes

    if not consumo or consumo.get("tipo") != TIPO_CONSUMO:
        raise HTTPException(status_code=422, detail="El movimiento a reembolsar no es un consumo")
    if consumo.get("exento"):
        # Exento no descontó nada: solo marcar para no reintentar.
        col_mov.update_one({"_id": consumo["_id"]}, {"$set": {"reembolsado": True}})
        return None

    # Flag anti-doble en el consumo original (condicionado).
    marcado = col_mov.update_one(
        {"_id": consumo["_id"], "reembolsado": {"$ne": True}},
        {"$set": {"reembolsado": True}},
    )
    if getattr(marcado, "modified_count", 1) == 0:
        return None  # ya fue reembolsado por otra vía

    consulta_id = consumo.get("consulta_id") or ""
    try:
        fresca = col_emp.find_one({"_id": empresa["_id"]}) or {}
    except Exception:
        fresca = {}
    fuente_consumo = consumo.get("fuente")

    if not consumo.get("cierre_id"):
        if fresca.get("planes"):
            # Formato nuevo: array por fuente; multi-plan → localizar por
            # (fuente, plan_id) porque puede haber varias entradas de la fuente.
            pos = None
            for i, ent in enumerate(fresca["planes"]):
                if ent.get("fuente") == fuente_consumo and ent.get("plan_id") == consumo.get("plan_id"):
                    pos = i
                    break
            if pos is not None:
                entrada = fresca["planes"][pos]
                ilimitado = entrada.get("cupo_autorizado") is None
                compensar = {f"planes.{pos}.cupo_consumido": -1}
                if not ilimitado:
                    compensar[f"planes.{pos}.cupo_disponible"] = 1
                col_emp.update_one(
                    {"_id": empresa["_id"], f"planes.{pos}.fuente": fuente_consumo},
                    {"$inc": compensar},
                )
        else:
            # Formato viejo (subdoc único): reintegro al subdoc si sigue el mismo plan.
            plan_actual = fresca.get("plan") or {}
            if plan_actual.get("plan_id") is not None and plan_actual.get("plan_id") == consumo.get("plan_id"):
                ilimitado = plan_actual.get("cupo_autorizado") is None
                compensar = {"plan.cupo_consumido": -1}
                if not ilimitado:
                    compensar["plan.cupo_disponible"] = 1
                col_emp.update_one(
                    {"_id": empresa["_id"], "plan.plan_id": consumo.get("plan_id")},
                    {"$inc": compensar},
                )

    # Unidades negativas del consumo; COP negado. Período: el ACTUAL si el
    # original ya está congelado en un cierre.
    periodo = periodo_colombia() if consumo.get("cierre_id") else consumo.get("periodo") or periodo_colombia()
    mov = _nuevo_movimiento(
        empresa, actor, TIPO_REEMBOLSO,
        unidades=-int(consumo.get("unidades") or 0),
        monto_cop=-int(consumo.get("monto_cop") or 0),
        consulta_id=consulta_id,
        plan={"_id": consumo.get("plan_id"), "nombre": consumo.get("plan_nombre"), "precio_por_estudio": consumo.get("precio_unitario_cop")},
        periodo=periodo,
        consumo_id=consumo["_id"],
        motivo=motivo,
        automatico=automatico,
        fuente=consumo.get("fuente"),
    )
    try:
        col_mov.insert_one(mov)
    except DuplicateKeyError:
        logger.warning("Reembolso duplicado ignorado para %s", consulta_id)
        return None
    return mov


# ── Pagos y ajustes ───────────────────────────────────────────────────────────

def registrar_pago(
    empresa: dict,
    actor: dict,
    monto_cop: int,
    fecha_pago: str,
    metodo: str,
    referencia: str = "",
    nota: str = "",
    periodo: str | None = None,
    col_mov: Any = None,
    col_per: Any = None,
) -> dict:
    """Registra un PAGO (−COP). Si no llega periodo, se aplica al cierre
    PENDIENTE_COBRO más antiguo; si cubre su total, lo marca PAGADA.

    Un pago SÍ puede aplicarse a un período cerrado: pagar la cuenta de cobro
    de un mes pasado es el flujo natural (lo que no se edita en cerrado son
    ajustes/reembolsos que cambian lo cobrado)."""
    col_mov = col_mov if col_mov is not None else col_movimientos
    col_per = col_per if col_per is not None else col_periodos

    if not periodo:
        antiguo = col_per.find_one(
            {"empresa_id": empresa["_id"], "estado": "PENDIENTE_COBRO"},
            sort=[("periodo", 1)],
        )
        periodo = antiguo["periodo"] if antiguo else periodo_colombia()

    try:
        fecha = datetime.strptime(fecha_pago, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=422, detail="fecha_pago debe ser YYYY-MM-DD")

    mov = _nuevo_movimiento(
        empresa, actor, TIPO_PAGO, unidades=0, monto_cop=-int(monto_cop), periodo=periodo,
        motivo="", extra={
            "fecha_pago": fecha,
            "metodo": metodo,
            "referencia": referencia,
            "nota": nota,
        },
    )
    col_mov.insert_one(mov)

    # ¿El pago acumulado cubre el total congelado del cierre PENDIENTE_COBRO?
    periodo_pagado = None
    cierre = col_per.find_one({"empresa_id": empresa["_id"], "periodo": periodo, "estado": "PENDIENTE_COBRO"})
    if cierre:
        total = int((cierre.get("totales") or {}).get("total_cop") or 0)
        pagos = sum(
            -int(m.get("monto_cop") or 0)
            for m in col_mov.find({"empresa_id": empresa["_id"], "periodo": periodo, "tipo": TIPO_PAGO})
        )
        if pagos >= total:
            col_per.update_one(
                {"_id": cierre["_id"]},
                {"$set": {"estado": "PAGADA", "pagada_en": _utcnow()}},
            )
            periodo_pagado = periodo
    mov["periodo_pagado"] = periodo_pagado
    return mov


def registrar_ajuste(
    empresa: dict,
    actor: dict,
    monto_cop: int,
    motivo: str,
    periodo: str | None = None,
    col_mov: Any = None,
    col_per: Any = None,
) -> dict:
    """AJUSTE manual ±COP (descuentos, sanciones, correcciones)."""
    col_mov = col_mov if col_mov is not None else col_movimientos
    col_per = col_per if col_per is not None else col_periodos
    periodo = periodo or periodo_colombia()
    if col_per.find_one({"empresa_id": empresa["_id"], "periodo": periodo}):
        raise HTTPException(status_code=409, detail=f"El período {periodo} está cerrado; reábrela para editarlo.")
    mov = _nuevo_movimiento(
        empresa, actor, TIPO_AJUSTE, unidades=0, monto_cop=int(monto_cop), periodo=periodo,
        motivo=motivo,
    )
    col_mov.insert_one(mov)
    return mov


# ── Totales y cierres ─────────────────────────────────────────────────────────

def totales_periodo(empresa_id, periodo: str, col_mov: Any = None) -> dict:
    """Totales por tipo del período (se congelan al cerrar)."""
    col_mov = col_mov if col_mov is not None else col_movimientos
    totales = {
        "consumos": 0, "unidades": 0, "reembolsos_unidades": 0,
        "subtotal_cop": 0, "reembolsos_cop": 0, "ajustes_cop": 0, "pagos_cop": 0, "total_cop": 0,
    }
    for mov in col_mov.find({"empresa_id": empresa_id, "periodo": periodo}):
        tipo = mov.get("tipo")
        monto = int(mov.get("monto_cop") or 0)
        unidades = int(mov.get("unidades") or 0)
        if tipo == TIPO_CONSUMO:
            totales["consumos"] += 1
            if not mov.get("exento"):
                totales["subtotal_cop"] += monto
        elif tipo == TIPO_REEMBOLSO:
            totales["reembolsos_unidades"] += -unidades
            totales["reembolsos_cop"] += monto  # ya viene negativo
        elif tipo == TIPO_PAGO:
            totales["pagos_cop"] += monto       # negativo
        elif tipo == TIPO_AJUSTE:
            totales["ajustes_cop"] += monto
    totales["unidades"] = totales["consumos"] - totales["reembolsos_unidades"]
    totales["total_cop"] = totales["subtotal_cop"] + totales["reembolsos_cop"] + totales["ajustes_cop"] + totales["pagos_cop"]
    return totales


def cerrar_periodo(
    empresa: dict,
    actor: dict,
    periodo: str,
    permitir_vacio: bool = False,
    col_mov: Any = None,
    col_per: Any = None,
) -> dict:
    """Congela los totales del período pasado, marca los movimientos y crea
    el doc de cierre (el PDF lo genera/adjorna el router)."""
    col_mov = col_mov if col_mov is not None else col_movimientos
    col_per = col_per if col_per is not None else col_periodos

    if not periodo or len(periodo) != 7 or periodo[4] != "-":
        raise HTTPException(status_code=422, detail="periodo debe ser YYYY-MM")
    if periodo_actual_es(periodo):
        raise HTTPException(status_code=422, detail="Solo se pueden cerrar períodos pasados (el mes en curso sigue abierto).")
    if col_per.find_one({"empresa_id": empresa["_id"], "periodo": periodo}):
        raise HTTPException(status_code=409, detail=f"El período {periodo} ya está cerrado.")

    movimientos = list(col_mov.find({"empresa_id": empresa["_id"], "periodo": periodo}))
    if not movimientos and not permitir_vacio:
        raise HTTPException(status_code=422, detail=f"Sin movimientos en {periodo} (use permitir_vacio para cerrar de todas formas).")

    totales = totales_periodo(empresa["_id"], periodo, col_mov=col_mov)
    anio, mes = int(periodo[:4]), int(periodo[5:7])
    desde = datetime(anio, mes, 1, 0, 0) - timedelta(hours=5)
    hasta = (datetime(anio + 1, 1, 1) if mes == 12 else datetime(anio, mes + 1, 1)) - timedelta(hours=5)

    cierre = {
        "empresa_id": empresa["_id"],
        "empresa_nombre": empresa.get("nombre", ""),
        "periodo": periodo,
        "desde": desde,
        "hasta": hasta,
        "totales": totales,
        "estado": "PENDIENTE_COBRO",
        "pdf": None,
        "cerrado_por": actor.get("usuario", ""),
        "cerrado_en": _utcnow(),
        "pagada_en": None,
        "reabierto_en": None,
        "reabierto_por": None,
        "motivo_reapertura": "",
    }
    resultado = col_per.insert_one(cierre)
    cierre["_id"] = resultado.inserted_id
    col_mov.update_many(
        {"empresa_id": empresa["_id"], "periodo": periodo, "cierre_id": None},
        {"$set": {"cierre_id": resultado.inserted_id}},
    )
    return cierre


def reabrir_periodo(
    cierre: dict,
    actor: dict,
    motivo: str,
    col_mov: Any = None,
    col_per: Any = None,
) -> None:
    """Elimina el cierre y descongela sus movimientos (el PDF queda en GCS
    como historial)."""
    col_mov = col_mov if col_mov is not None else col_movimientos
    col_per = col_per if col_per is not None else col_periodos
    col_mov.update_many(
        {"cierre_id": cierre["_id"]},
        {"$set": {"cierre_id": None}},
    )
    col_per.delete_one({"_id": cierre["_id"]})


# ── Helpers ───────────────────────────────────────────────────────────────────

def _nuevo_movimiento(
    empresa: dict,
    actor: dict,
    tipo: str,
    *,
    unidades: int,
    monto_cop: int,
    periodo: str,
    consulta_id: str = "",
    plan: dict | None = None,
    consumo_id=None,
    motivo: str = "",
    exento: bool = False,
    automatico: bool = False,
    fuente: str | None = None,
    extra: dict | None = None,
) -> dict:
    mov = {
        "empresa_id": empresa["_id"],
        "empresa_nombre": empresa.get("nombre", ""),
        "tipo": tipo,
        "unidades": unidades,
        "monto_cop": monto_cop,
        "precio_unitario_cop": (plan or {}).get("precio_por_estudio") if tipo == TIPO_CONSUMO else None,
        "plan_id": (plan or {}).get("_id") if plan else None,
        "plan_nombre": (plan or {}).get("nombre", "") if plan else "",
        "fuente": fuente,
        "consulta_id": consulta_id or None,
        "consumo_id": consumo_id,
        "exento": exento,
        "reembolsado": False if tipo == TIPO_CONSUMO else None,
        "periodo": periodo,
        "motivo": motivo,
        "nota": "",
        "actor_usuario": actor.get("usuario", ""),
        "actor_nombre": actor.get("usuario_nombre", actor.get("nombre", "")),
        "automatico": automatico,
        "cierre_id": None,
        "creado_en": _utcnow(),
    }
    if extra:
        mov.update(extra)
    return mov


def buscar_consumo(consulta_id: str, empresa_id, col_mov: Any = None) -> dict | None:
    """CONSUMO (no exento-saltar) de una consulta para su empresa."""
    col_mov = col_mov if col_mov is not None else col_movimientos
    return col_mov.find_one({
        "consulta_id": consulta_id.strip().upper(),
        "empresa_id": empresa_id,
        "tipo": TIPO_CONSUMO,
    })
