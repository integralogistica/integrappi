"""
Módulo «Otros Costos» — Integrapp.

Gestiona los costos adicionales que algunos vehículos generan después de prestado
el servicio (parqueadero, peaje, cargue, horas adicionales, etc.). El equipo
operativo registra una solicitud asociada a uno o varios pedidos de Vulcano
(consultados en `pedidos_medical_historico`), y esta sigue un flujo de aprobación
por monto → pago → histórico, con trazabilidad completa.

Colecciones (base de datos `integra`):
  - `otros_costos`            ciclo activo (borrador, pendiente_aprobacion, devuelto, rechazado, aprobado)
  - `historico_otros_costos`  solicitudes pagadas
  - `anulados_otros_costos`   solicitudes anuladas

Seguridad: el perfil NO se confía del frontend. Cada endpoint recibe `usuario` y
lo resuelve contra `baseusuarios` para autorizar con el perfil real.
"""

import logging
import re
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from bson import ObjectId
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field
from pymongo.errors import DuplicateKeyError

from bd.bd_cliente import bd_cliente

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/otros-costos", tags=["Otros Costos"])

# Colombia = UTC-5. El servidor corre en UTC y Mongo guarda los datetime como
# instantes UTC; los límites por "día Colombia" se alinean sumando 5 h.
_OFFSET_COLOMBIA = timedelta(hours=5)
LIMITE_COORDINADOR = 500000  # Coordinador aprueba hasta este valor inclusive

TIPOS_COSTO = [
    "Parqueadero", "Peaje", "Cargue", "Descargue", "Stand by", "Horas adicionales",
    "Ayudante", "Devolución", "Reexpedición", "Reparación", "Alimentación",
    "Hospedaje", "Otro",
]
BANCOS = [
    "NEQUI", "BANCO DE BOGOTÁ", "BANCOLOMBIA", "DAVIVIENDA", "DAVIPLATA",
    "CMR FALABELLA", "BANCO CAJA SOCIAL", "BANCO AV VILLAS", "COLPATRIA",
]
TIPOS_CUENTA = ["Ahorros", "Corriente", "Depósito electrónico"]
ESTADOS_VALIDOS = [
    "borrador", "pendiente_aprobacion", "devuelto", "rechazado",
    "aprobado", "pagado", "anulado",
]

# ── Conexión MongoDB ──────────────────────────────────────────────────────────
client = bd_cliente
db = client["integra"]
col_activos = db["otros_costos"]
col_historico = db["historico_otros_costos"]
col_anulados = db["anulados_otros_costos"]
col_historico_pedidos = db["pedidos_medical_historico"]   # solo lectura (lookup)
col_usuarios = db["baseusuarios"]                          # resolución de identidad

# Índices (idempotentes al importar, igual patrón que siscore_consultas.py).
# `consecutivo` unique en activos → anti-colisión en la generación del consecutivo.
try:
    col_activos.create_index([("consecutivo", 1)], unique=True, name="idx_oc_consecutivo")
except Exception as _e:
    logger.warning(f"[OTROS_COSTOS] No se pudo crear índice unique 'consecutivo' en activos: {_e}")
for _col, _nombre in (
    (col_historico, "idx_oc_hist_consecutivo"),
    (col_anulados, "idx_oc_anul_consecutivo"),
):
    try:
        _col.create_index([("consecutivo", 1)], name=_nombre)
    except Exception as _e:
        logger.warning(f"[OTROS_COSTOS] No se pudo crear índice 'consecutivo': {_e}")
for _col in (col_activos, col_historico, col_anulados):
    for _fld in ("estado", "usuario_registro", "pedidos_normalizados"):
        try:
            _col.create_index(_fld)
        except Exception:
            pass
    try:
        _col.create_index([("created_at", -1)], name=f"idx_oc_created_{_col.name}")
    except Exception:
        pass


# ── Utilidades de tiempo ──────────────────────────────────────────────────────
def _ahora_utc() -> datetime:
    """UTC actual como datetime naive (igualconvención que el resto del sistema)."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _hoy_colombia() -> datetime:
    """Fecha/hora actual en zona Colombia (UTC-5) como datetime naive."""
    return _ahora_utc() - _OFFSET_COLOMBIA


# ── Resolución de identidad / autorización ────────────────────────────────────
def _resolver_usuario(usuario: str) -> dict:
    """Devuelve el perfil/region/nombre REALES del usuario desde baseusuarios.

    El backend NO confía en el perfil que envía el frontend: lo deriva de la BD.
    Lanza 401 si no llega usuario / no existe, 403 si está inactivo.
    """
    if not usuario or not str(usuario).strip():
        raise HTTPException(status_code=401, detail="No autenticado")
    u = str(usuario).strip().upper()
    doc = col_usuarios.find_one({"usuario": u})
    if not doc:
        raise HTTPException(status_code=401, detail="Usuario no válido")
    if not doc.get("activo", True):
        raise HTTPException(status_code=403, detail="Usuario inactivo")
    return {
        "usuario": doc["usuario"],
        "perfil": (doc.get("perfil") or "").strip().upper(),
        "regional": doc.get("regional", ""),
        "nombre": doc.get("nombre", ""),
        "id": str(doc["_id"]),
    }


def _requiere(info: dict, perfiles: set[str], accion: str):
    if info["perfil"] not in perfiles:
        raise HTTPException(
            status_code=403,
            detail=f"Su perfil ({info['perfil']}) no tiene permiso para {accion}.",
        )


# ── Normalización de pedidos Vulcano (spec §4) ────────────────────────────────
def _normalizar_pedidos(texto: str) -> List[str]:
    """Normaliza una lista de pedidos (string) a una lista de strings sin ceros
    a la izquierda, sin duplicados, separando por coma/guion/puntoycoma/espacios."""
    if not texto:
        return []
    partes = re.split(r"[,\-;\s/]+", str(texto))
    out: List[str] = []
    seen: set[str] = set()
    for p in partes:
        digits = re.sub(r"\D", "", p)          # sólo dígitos (descarta caracteres inválidos)
        if not digits:
            continue
        norm = digits.lstrip("0") or "0"        # comparar sin ceros a la izquierda
        if norm not in seen:
            seen.add(norm)
            out.append(norm)
    return out


def _proyeccion_pedido(doc: dict) -> dict:
    """Proyecta un documento de pedidos_medical_historico al formato que necesita
    el módulo de Otros Costos (datos_servicio + referencia al origen)."""
    return {
        "_id_origen": str(doc["_id"]),
        "pedido_vulcano": str(doc.get("pedido_vulcano") or doc.get("codigo_pedido") or ""),
        "cliente": str(doc.get("cliente_origen") or ""),
        "centro_distribucion": str(doc.get("regional") or doc.get("centro_costo") or ""),
        "fecha_servicio": _fecha_a_str(doc.get("fecha_movimiento_historico"))
        or _fecha_a_str(doc.get("fecha_aprobacion"))
        or _fecha_a_str(doc.get("fecha_creacion")),
        "piezas": _a_numero(doc.get("piezas")),
        "peso_real": _a_numero(doc.get("peso_real")),
        "tipo_vehiculo": str(doc.get("tipo_vehiculo") or doc.get("tipo_veh_sicetac") or ""),
        "placa": str(doc.get("placa") or ""),
        "municipio_destino": str(doc.get("municipio_destino") or ""),
        "departamento_destino": str(doc.get("departamento_destino") or ""),
        "transportador": str(doc.get("transportador") or ""),
        "manifiesto": str(doc.get("manifiesto") or ""),
        "total_solicitado": _a_numero(doc.get("total_solicitado")),
        "regional": str(doc.get("regional") or ""),
        "estado_pedido": str(doc.get("estado") or ""),
    }


def _buscar_pedidos_historico(pedidos_norm: List[str]) -> List[dict]:
    """Busca en pedidos_medical_historico los docs cuyo pedido_vulcano/codigo_pedido
    coincide (tolerante a ceros a la izquierda y separadores) con alguno de los
    pedidos normalizados. Regex armado solo con dígitos escapados (sin inyección)."""
    if not pedidos_norm:
        return []
    condiciones = []
    for n in pedidos_norm:
        patron = rf"(^|[^0-9])0*{re.escape(n)}([^0-9]|$)"
        condiciones.append({"pedido_vulcano": {"$regex": patron}})
        condiciones.append({"codigo_pedido": {"$regex": patron}})
    encontrados: List[dict] = []
    vistos: set[str] = set()
    set_norm = set(pedidos_norm)
    for doc in col_historico_pedidos.find({"$or": condiciones}):
        almacenados = _normalizar_pedidos(
            f"{doc.get('pedido_vulcano','')},{doc.get('codigo_pedido','')}"
        )
        if set_norm & set(almacenados):                  # confirmar intersección real
            oid = str(doc["_id"])
            if oid in vistos:
                continue
            vistos.add(oid)
            encontrados.append(_proyeccion_pedido(doc))
    return encontrados


# ── Consecutivo OC-AAAAMMDD-NNNN ──────────────────────────────────────────────
def _generar_consecutivo_oc(fecha_col: datetime) -> str:
    prefijo = f"OC-{fecha_col.strftime('%Y%m%d')}-"
    regex_prefijo = re.compile(rf"^{re.escape(prefijo)}(\d{{4}})$")
    max_n = 0
    for col in (col_activos, col_historico, col_anulados):
        for d in col.find({"consecutivo": {"$regex": f"^{re.escape(prefijo)}"}}, {"consecutivo": 1}):
            m = regex_prefijo.match(str(d.get("consecutivo", "")))
            if m:
                max_n = max(max_n, int(m.group(1)))
    return f"{prefijo}{max_n + 1:04d}"


def _insertar_con_reintento(doc: dict) -> str:
    """Inserta el doc generando consecutivo con reintento ante colisión (concurrencia)."""
    fecha_col = _hoy_colombia()
    ultimo_error = None
    for _ in range(3):
        doc["consecutivo"] = _generar_consecutivo_oc(fecha_col)
        try:
            col_activos.insert_one(doc)
            return doc["consecutivo"]
        except DuplicateKeyError as e:
            ultimo_error = e
            continue
    raise HTTPException(status_code=500, detail="No se pudo generar un consecutivo único")


# ── Trazabilidad (spec §11) ───────────────────────────────────────────────────
def _nuevo_movimiento(
    accion: str,
    estado_anterior: Optional[str],
    estado_nuevo: Optional[str],
    info: dict,
    observacion: str = "",
    ip: str = "",
) -> dict:
    return {
        "accion": accion,
        "estado_anterior": estado_anterior,
        "estado_nuevo": estado_nuevo,
        "usuario": info.get("usuario", ""),
        "nombre_usuario": info.get("nombre", ""),
        "rol": info.get("perfil", ""),
        "fecha": _ahora_utc(),
        "observacion": observacion or "",
        "ip": ip or "",
    }


# ── Serialización / visibilidad de datos sensibles ────────────────────────────
def _fecha_a_str(v) -> Optional[str]:
    if isinstance(v, datetime):
        return v.isoformat()
    return v if v is not None else None


def _a_numero(v) -> float:
    try:
        if v is None or v == "":
            return 0.0
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def _jsonable(v):
    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, list):
        return [_jsonable(x) for x in v]
    if isinstance(v, dict):
        return {k: _jsonable(x) for k, x in v.items()}
    return v


def _aplicar_visibilidad(doc: dict, perfil: str) -> dict:
    """Enmascara datos bancarios sensibles para perfiles que no los necesitan."""
    if perfil in {"FINANCIERO", "ADMIN"}:
        return doc
    db_ = doc.get("datos_bancarios")
    if isinstance(db_, dict):
        db_ = dict(db_)
        db_["numero_cuenta"] = _enmascarar(db_.get("numero_cuenta", ""))
        db_["cedula_titular"] = _enmascarar(db_.get("cedula_titular", ""))
        doc["datos_bancarios"] = db_
    return doc


def _enmascarar(valor: str) -> str:
    s = str(valor or "")
    if len(s) <= 4:
        return "*" * len(s)
    return "*" * (len(s) - 4) + s[-4:]


def _serializar(doc: Optional[dict], perfil: str) -> Optional[dict]:
    if doc is None:
        return None
    return _aplicar_visibilidad(_jsonable(doc), perfil)


# ── Validaciones (spec §8) ────────────────────────────────────────────────────
def _validar_solicitud(
    datos_servicio: "DatosServicio",
    costos: List["CostoConcepto"],
    datos_bancarios: "DatosBancarios",
    conductor: "Conductor",
    pedido_encontrado: bool,
) -> float:
    """Valida los campos obligatorios. Devuelve valor_total calculado."""
    if not datos_servicio.manifiesto or not str(datos_servicio.manifiesto).strip():
        raise HTTPException(status_code=422, detail="El manifiesto es obligatorio.")
    if not pedido_encontrado and not (datos_servicio.placa or "").strip():
        raise HTTPException(
            status_code=422,
            detail="La placa es obligatoria cuando el pedido no se encuentra en el histórico.",
        )

    if not costos:
        raise HTTPException(status_code=422, detail="Debe agregar al menos un concepto de costo.")
    valor_total = 0.0
    for c in costos:
        if not (c.tipo_costo or "").strip():
            raise HTTPException(status_code=422, detail="Cada concepto debe tener un tipo de costo.")
        if c.tipo_costo == "Otro" and not (c.concepto or "").strip():
            raise HTTPException(
                status_code=422,
                detail="Cuando el tipo de costo es 'Otro' debe indicar el concepto.",
            )
        if not (c.concepto or "").strip():
            raise HTTPException(status_code=422, detail="El concepto del costo es obligatorio.")
        if not (c.descripcion or "").strip():
            raise HTTPException(status_code=422, detail="La descripción del costo es obligatoria.")
        if _a_numero(c.valor) <= 0:
            raise HTTPException(status_code=422, detail="El valor de cada costo debe ser mayor que cero.")
        valor_total += _a_numero(c.valor)

    if valor_total <= 0:
        raise HTTPException(status_code=422, detail="El valor solicitado debe ser mayor que cero.")

    # Bancarios
    if not (datos_bancarios.banco or "").strip():
        raise HTTPException(status_code=422, detail="El banco es obligatorio.")
    if (datos_bancarios.banco or "").strip().upper() not in [b.upper() for b in BANCOS]:
        raise HTTPException(status_code=422, detail="El banco seleccionado no es válido.")
    if not (datos_bancarios.numero_cuenta or "").strip():
        raise HTTPException(status_code=422, detail="El número de cuenta es obligatorio.")
    ced = re.sub(r"\D", "", datos_bancarios.cedula_titular or "")
    if not ced:
        raise HTTPException(status_code=422, detail="La cédula del titular es obligatoria.")
    if not (datos_bancarios.nombre_titular or "").strip():
        raise HTTPException(status_code=422, detail="El nombre del titular de la cuenta es obligatorio.")
    if not (conductor.nombre or "").strip():
        raise HTTPException(status_code=422, detail="El nombre del conductor es obligatorio.")

    return round(valor_total, 2)


def _verificar_duplicado_interno(
    pedidos_norm: List[str],
    manifiesto: str,
    costos: List["CostoConcepto"],
    excluir_consecutivo: Optional[str] = None,
) -> List[dict]:
    """Advertencia (no bloqueante) de solicitudes que coincidan en pedido,
    manifiesto, tipo de costo y valor en los últimos 30 días."""
    if not pedidos_norm or not manifiesto:
        return []
    desde = _ahora_utc() - timedelta(days=30)
    tipos = {(c.tipo_costo or "").strip().upper() for c in costos}
    coincidencias: List[dict] = []
    set_norm = set(pedidos_norm)
    for col in (col_activos, col_historico):
        for d in col.find(
            {
                "manifiesto": str(manifiesto).strip(),
                "created_at": {"$gte": desde},
            }
        ):
            if excluir_consecutivo and d.get("consecutivo") == excluir_consecutivo:
                continue
            if not (set_norm & set(d.get("pedidos_normalizados") or [])):
                continue
            doc_tipos = {(c.get("tipo_costo") or "").strip().upper() for c in (d.get("costos") or [])}
            if tipos & doc_tipos:
                coincidencias.append({
                    "consecutivo": d.get("consecutivo"),
                    "estado": d.get("estado"),
                    "created_at": _fecha_a_str(d.get("created_at")),
                })
    return coincidencias


def _doc_por_consecutivo(consecutivo: str) -> tuple[Optional[dict], Optional[object]]:
    """Busca un documento por consecutivo en activos (y devuelve la colección).
    Retorna (doc, coleccion). Si no existe en activos, busca en histórico/anulados."""
    doc = col_activos.find_one({"consecutivo": consecutivo})
    if doc:
        return doc, col_activos
    for col in (col_historico, col_anulados):
        d = col.find_one({"consecutivo": consecutivo})
        if d:
            return d, col
    return None, None


def _scope_lectura(filtro: dict, info: dict):
    """Aplica el alcance de lectura según el perfil real."""
    perfil = info["perfil"]
    if perfil == "FINANCIERO":
        filtro["estado"] = "aprobado"
    elif perfil == "OPERATIVO":
        filtro["usuario_registro"] = info["usuario"]
    # COORDINADOR / CONTROL / ADMIN: sin restricción


# ════════════════════════════════════════════════════════════════════════════
# Modelos Pydantic
# ════════════════════════════════════════════════════════════════════════════
class CostoConcepto(BaseModel):
    tipo_costo: str = ""
    concepto: str = ""
    descripcion: str = ""
    valor: float = 0


class DatosServicio(BaseModel):
    cliente: str = ""
    centro_distribucion: str = ""
    fecha_servicio: Optional[str] = None
    piezas: float = 0
    peso_real: float = 0
    tipo_vehiculo: str = ""
    placa: str = ""
    municipio_destino: str = ""
    departamento_destino: str = ""
    transportador: str = ""
    manifiesto: str = ""


class DatosBancarios(BaseModel):
    banco: str = ""
    tipo_cuenta: str = ""
    numero_cuenta: str = ""
    cedula_titular: str = ""
    nombre_titular: str = ""


class Conductor(BaseModel):
    nombre: str = ""
    telefono: str = ""


class BuscarPedidosRequest(BaseModel):
    usuario: str
    pedido_vulcano: str


class CrearOtroCostoRequest(BaseModel):
    usuario: str
    enviar: bool = False
    pedido_vulcano_original: str
    pedidos_normalizados: List[str] = Field(default_factory=list)
    pedido_encontrado: bool = True
    motivo_no_encontrado: str = ""
    datos_servicio: DatosServicio
    costos: List[CostoConcepto]
    datos_bancarios: DatosBancarios
    conductor: Conductor = Field(default_factory=Conductor)
    observaciones: str = ""


class EditarOtroCostoRequest(BaseModel):
    consecutivo: str
    usuario: str
    pedido_vulcano_original: Optional[str] = None
    pedidos_normalizados: Optional[List[str]] = None
    pedido_encontrado: Optional[bool] = None
    motivo_no_encontrado: Optional[str] = None
    datos_servicio: Optional[DatosServicio] = None
    costos: Optional[List[CostoConcepto]] = None
    datos_bancarios: Optional[DatosBancarios] = None
    conductor: Optional[Conductor] = None
    observaciones: Optional[str] = None


class AccionConObservacionRequest(BaseModel):
    consecutivo: str
    usuario: str
    observacion: str = ""


class RegistrarPagoRequest(BaseModel):
    consecutivo: str
    usuario: str
    estado_pago: str = "PAGADO"
    fecha_pago: Optional[str] = None
    referencia: str = ""
    observaciones: str = ""


class AnularRequest(BaseModel):
    consecutivo: str
    usuario: str
    motivo: str = ""


class VerificarDuplicadoRequest(BaseModel):
    usuario: str
    pedido_vulcano: str
    manifiesto: str = ""
    tipo_costo: str = ""
    valor: float = 0


class ExportarExcelRequest(BaseModel):
    usuario: str
    estado: Optional[str] = None
    fecha_inicio: Optional[str] = None
    fecha_fin: Optional[str] = None
    pedido: Optional[str] = None
    placa: Optional[str] = None
    manifiesto: Optional[str] = None
    cliente: Optional[str] = None
    origen: str = "historico"   # "historico" | "activos"


# ════════════════════════════════════════════════════════════════════════════
# Endpoints
# ════════════════════════════════════════════════════════════════════════════
@router.get("/tipos-costo")
async def tipos_costo():
    return TIPOS_COSTO


@router.get("/bancos")
async def bancos():
    return BANCOS


@router.get("/tipos-cuenta")
async def tipos_cuenta():
    return TIPOS_CUENTA


@router.post("/buscar-pedidos")
async def buscar_pedidos(req: BuscarPedidosRequest):
    _resolver_usuario(req.usuario)  # valida sesión (cualquier perfil puede buscar)
    pedidos_norm = _normalizar_pedidos(req.pedido_vulcano)
    encontrados = _buscar_pedidos_historico(pedidos_norm)
    encontrados_norm = set()
    for e in encontrados:
        encontrados_norm |= set(_normalizar_pedidos(e.get("pedido_vulcano", "")))
    no_encontrados = [p for p in pedidos_norm if p not in encontrados_norm]

    totales = {
        "piezas": sum(_a_numero(e.get("piezas")) for e in encontrados),
        "peso_real": sum(_a_numero(e.get("peso_real")) for e in encontrados),
        "total_solicitado": sum(_a_numero(e.get("total_solicitado")) for e in encontrados),
    }

    # Diferencias entre los pedidos encontrados (cliente, destino, placa, vehículo)
    diferencias = {}
    advertencia = False
    if len(encontrados) > 1:
        for campo in ("cliente", "municipio_destino", "departamento_destino", "placa", "tipo_vehiculo"):
            valores = {str(e.get(campo, "")).strip() for e in encontrados if e.get(campo)}
            valores.discard("")
            if len(valores) > 1:
                diferencias[campo] = sorted(valores)
                advertencia = True

    return {
        "pedido_vulcano_original": req.pedido_vulcano,
        "pedidos_normalizados": pedidos_norm,
        "pedidos_encontrados": encontrados,
        "pedidos_no_encontrados": no_encontrados,
        "pedido_encontrado": len(encontrados) > 0,
        "totales": totales,
        "diferencias": diferencias,
        "advertencia_servicios_diferentes": advertencia,
    }


@router.post("/verificar-duplicado")
async def verificar_duplicado(req: VerificarDuplicadoRequest):
    _resolver_usuario(req.usuario)
    pedidos_norm = _normalizar_pedidos(req.pedido_vulcano)
    costo = CostoConcepto(tipo_costo=req.tipo_costo, valor=req.valor)
    coincidencias = _verificar_duplicado_interno(pedidos_norm, req.manifiesto, [costo])
    return {"posible_duplicado": len(coincidencias) > 0, "coincidencias": coincidencias}


@router.post("/crear")
async def crear_solicitud(req: CrearOtroCostoRequest, request: Request):
    info = _resolver_usuario(req.usuario)
    _requiere(info, {"OPERATIVO", "ADMIN"}, "crear solicitudes")

    if not str(req.pedido_vulcano_original or "").strip():
        raise HTTPException(status_code=422, detail="El pedido de Vulcano es obligatorio.")

    pedidos_norm = req.pedidos_normalizados or _normalizar_pedidos(req.pedido_vulcano_original)
    valor_total = _validar_solicitud(req.datos_servicio, req.costos, req.datos_bancarios, req.conductor, req.pedido_encontrado)
    requiere_control = valor_total > LIMITE_COORDINADOR
    ahora = _ahora_utc()
    estado = "pendiente_aprobacion" if req.enviar else "borrador"

    doc = {
        "pedido_vulcano_original": str(req.pedido_vulcano_original).strip(),
        "pedidos_normalizados": pedidos_norm,
        "pedido_encontrado": bool(req.pedido_encontrado),
        "motivo_no_encontrado": req.motivo_no_encontrado or "",
        "datos_servicio": req.datos_servicio.model_dump(),
        "costos": [c.model_dump() for c in req.costos],
        "valor_total": valor_total,
        "requiere_aprobacion_control": requiere_control,
        "datos_bancarios": req.datos_bancarios.model_dump(),
        "conductor": req.conductor.model_dump(),
        "observaciones": req.observaciones or "",
        "manifiesto": (req.datos_servicio.manifiesto or "").strip(),
        "estado": estado,
        "usuario_registro": info["usuario"],
        "perfil_registro": info["perfil"],
        "regional_registro": info["regional"],
        "creado_por": {
            "usuario": info["usuario"], "nombre": info["nombre"],
            "rol": info["perfil"], "fecha": ahora,
        },
        "aprobacion": {},
        "pago": {},
        "historial_movimientos": [
            _nuevo_movimiento("creacion", None, estado, info, "Solicitud creada", _ip(request))
        ],
        "created_at": ahora,
        "updated_at": ahora,
    }

    consecutivo = _insertar_con_reintento(doc)

    posible_dup = _verificar_duplicado_interno(pedidos_norm, doc["manifiesto"], req.costos, excluir_consecutivo=consecutivo)

    return {
        "mensaje": "Solicitud creada",
        "consecutivo": consecutivo,
        "estado": estado,
        "valor_total": valor_total,
        "requiere_aprobacion_control": requiere_control,
        "posible_duplicado": len(posible_dup) > 0,
        "duplicados": posible_dup,
        "solicitud": _serializar(doc, info["perfil"]),
    }


@router.put("/editar")
async def editar_solicitud(req: EditarOtroCostoRequest, request: Request):
    info = _resolver_usuario(req.usuario)
    doc, col = _doc_por_consecutivo(req.consecutivo)
    if not doc or col is not col_activos:
        raise HTTPException(status_code=404, detail="Solicitud no encontrada o ya no está activa.")

    estado_actual = doc.get("estado")
    if estado_actual in {"aprobado", "pagado", "anulado"}:
        raise HTTPException(
            status_code=422,
            detail="La solicitud está aprobada/pagada/anulada y no se puede editar.",
        )
    # Permiso: ADMIN, o OPERATIVO dueño
    if info["perfil"] != "ADMIN":
        if info["perfil"] != "OPERATIVO" or doc.get("usuario_registro") != info["usuario"]:
            raise HTTPException(status_code=403, detail="Solo el creador o un administrador pueden editar.")

    datos_servicio = req.datos_servicio or DatosServicio(**doc.get("datos_servicio", {}))
    costos = req.costos if req.costos is not None else [CostoConcepto(**c) for c in doc.get("costos", [])]
    datos_bancarios = req.datos_bancarios or DatosBancarios(**doc.get("datos_bancarios", {}))
    conductor = req.conductor or Conductor(**doc.get("conductor", {}))

    valor_total = _validar_solicitud(datos_servicio, costos, datos_bancarios, conductor, doc.get("pedido_encontrado", True))
    requiere_control = valor_total > LIMITE_COORDINADOR

    set_fields = {
        "datos_servicio": datos_servicio.model_dump(),
        "costos": [c.model_dump() for c in costos],
        "valor_total": valor_total,
        "requiere_aprobacion_control": requiere_control,
        "datos_bancarios": datos_bancarios.model_dump(),
        "conductor": conductor.model_dump(),
        "manifiesto": (datos_servicio.manifiesto or "").strip(),
        "updated_at": _ahora_utc(),
    }
    if req.pedido_vulcano_original is not None:
        set_fields["pedido_vulcano_original"] = str(req.pedido_vulcano_original).strip()
        set_fields["pedidos_normalizados"] = req.pedidos_normalizados or _normalizar_pedidos(req.pedido_vulcano_original)
    if req.pedido_encontrado is not None:
        set_fields["pedido_encontrado"] = bool(req.pedido_encontrado)
    if req.motivo_no_encontrado is not None:
        set_fields["motivo_no_encontrado"] = req.motivo_no_encontrado
    if req.observaciones is not None:
        set_fields["observaciones"] = req.observaciones

    mov = _nuevo_movimiento("edicion", estado_actual, estado_actual, info, "Edición de la solicitud", _ip(request))
    col_activos.update_one(
        {"consecutivo": req.consecutivo},
        {"$set": set_fields, "$push": {"historial_movimientos": mov}},
    )
    actualizado = col_activos.find_one({"consecutivo": req.consecutivo})
    return {"mensaje": "Solicitud actualizada", "solicitud": _serializar(actualizado, info["perfil"])}


@router.post("/enviar-aprobacion")
async def enviar_aprobacion(req: AccionConObservacionRequest, request: Request):
    info = _resolver_usuario(req.usuario)
    _requiere(info, {"OPERATIVO", "ADMIN"}, "enviar solicitudes a aprobación")
    doc = col_activos.find_one({"consecutivo": req.consecutivo})
    if not doc:
        raise HTTPException(status_code=404, detail="Solicitud no encontrada.")
    if doc.get("estado") not in {"borrador", "devuelto"}:
        raise HTTPException(status_code=422, detail="Solo se pueden enviar solicitudes en borrador o devueltas.")

    # Validar que esté completa antes de enviar
    _validar_solicitud(
        DatosServicio(**doc.get("datos_servicio", {})),
        [CostoConcepto(**c) for c in doc.get("costos", [])],
        DatosBancarios(**doc.get("datos_bancarios", {})),
        Conductor(**doc.get("conductor", {})),
        doc.get("pedido_encontrado", True),
    )

    estado_prev = doc.get("estado")
    mov = _nuevo_movimiento("envio_aprobacion", estado_prev, "pendiente_aprobacion", info, req.observacion, _ip(request))
    col_activos.update_one(
        {"consecutivo": req.consecutivo, "estado": estado_prev},
        {"$set": {"estado": "pendiente_aprobacion", "updated_at": _ahora_utc()},
         "$push": {"historial_movimientos": mov}},
    )
    return {"mensaje": "Solicitud enviada a aprobación", "estado": "pendiente_aprobacion"}


def _obtener_y_validar_aprobacion(req, info) -> dict:
    doc = col_activos.find_one({"consecutivo": req.consecutivo})
    if not doc:
        raise HTTPException(status_code=404, detail="Solicitud no encontrada.")
    if doc.get("estado") == "aprobado":
        raise HTTPException(status_code=409, detail="La solicitud ya está aprobada.")
    if doc.get("estado") != "pendiente_aprobacion":
        raise HTTPException(status_code=422, detail="La solicitud no está pendiente de aprobación.")
    valor_total = _a_numero(doc.get("valor_total"))
    if valor_total > LIMITE_COORDINADOR and info["perfil"] not in {"CONTROL", "ADMIN"}:
        raise HTTPException(
            status_code=403,
            detail=f"El valor (${valor_total:,.0f}) supera el límite del coordinador "
                   f"(${LIMITE_COORDINADOR:,.0f}). Requiere aprobación de Control.",
        )
    if info["perfil"] not in {"COORDINADOR", "CONTROL", "ADMIN"}:
        raise HTTPException(status_code=403, detail="Su perfil no puede aprobar solicitudes.")
    return doc


@router.post("/aprobar")
async def aprobar_solicitud(req: AccionConObservacionRequest, request: Request):
    info = _resolver_usuario(req.usuario)
    doc = _obtener_y_validar_aprobacion(req, info)
    ahora = _ahora_utc()
    aprobacion = {
        "usuario": info["usuario"], "nombre": info["nombre"], "rol": info["perfil"],
        "fecha": ahora, "observacion": req.observacion or "",
    }
    mov = _nuevo_movimiento("aprobacion", doc.get("estado"), "aprobado", info, req.observacion, _ip(request))
    res = col_activos.update_one(
        {"consecutivo": req.consecutivo, "estado": "pendiente_aprobacion"},
        {"$set": {"estado": "aprobado", "aprobacion": aprobacion, "updated_at": ahora},
         "$push": {"historial_movimientos": mov}},
    )
    if res.matched_count == 0:
        raise HTTPException(status_code=409, detail="La solicitud cambió de estado (acción simultánea).")
    return {"mensaje": "Solicitud aprobada", "estado": "aprobado"}


@router.post("/devolver")
async def devolver_solicitud(req: AccionConObservacionRequest, request: Request):
    info = _resolver_usuario(req.usuario)
    _requiere(info, {"COORDINADOR", "CONTROL", "ADMIN"}, "devolver solicitudes")
    doc = col_activos.find_one({"consecutivo": req.consecutivo})
    if not doc:
        raise HTTPException(status_code=404, detail="Solicitud no encontrada.")
    if doc.get("estado") != "pendiente_aprobacion":
        raise HTTPException(status_code=422, detail="Solo se pueden devolver solicitudes pendientes.")
    estado_prev = doc.get("estado")
    mov = _nuevo_movimiento("devolucion", estado_prev, "devuelto", info, req.observacion, _ip(request))
    col_activos.update_one(
        {"consecutivo": req.consecutivo, "estado": estado_prev},
        {"$set": {"estado": "devuelto", "updated_at": _ahora_utc()},
         "$push": {"historial_movimientos": mov}},
    )
    return {"mensaje": "Solicitud devuelta", "estado": "devuelto"}


@router.post("/rechazar")
async def rechazar_solicitud(req: AccionConObservacionRequest, request: Request):
    info = _resolver_usuario(req.usuario)
    _requiere(info, {"COORDINADOR", "CONTROL", "ADMIN"}, "rechazar solicitudes")
    doc = col_activos.find_one({"consecutivo": req.consecutivo})
    if not doc:
        raise HTTPException(status_code=404, detail="Solicitud no encontrada.")
    if doc.get("estado") in {"rechazado", "pagado", "anulado"}:
        raise HTTPException(status_code=422, detail="La solicitud no se puede rechazar en su estado actual.")
    estado_prev = doc.get("estado")
    mov = _nuevo_movimiento("rechazo", estado_prev, "rechazado", info, req.observacion, _ip(request))
    col_activos.update_one(
        {"consecutivo": req.consecutivo, "estado": estado_prev},
        {"$set": {"estado": "rechazado", "updated_at": _ahora_utc()},
         "$push": {"historial_movimientos": mov}},
    )
    return {"mensaje": "Solicitud rechazada", "estado": "rechazado"}


def _mover_documento(doc: dict, col_destino) -> None:
    """Mueve el doc de col_activos a col_destino (patrón delete-first + insert idempotente)."""
    col_destino.delete_one({"_id": doc["_id"]})
    col_destino.insert_one(doc)
    col_activos.delete_one({"_id": doc["_id"]})


@router.post("/registrar-pago")
async def registrar_pago(req: RegistrarPagoRequest, request: Request):
    info = _resolver_usuario(req.usuario)
    _requiere(info, {"FINANCIERO", "ADMIN"}, "registrar pagos")

    doc = col_activos.find_one({"consecutivo": req.consecutivo})
    if not doc:
        # ¿ya está pagada en el histórico?
        if col_historico.find_one({"consecutivo": req.consecutivo, "estado": "pagado"}):
            raise HTTPException(status_code=409, detail="La solicitud ya está pagada.")
        raise HTTPException(status_code=404, detail="Solicitud no encontrada.")
    if doc.get("estado") == "pagado":
        raise HTTPException(status_code=409, detail="La solicitud ya está pagada.")
    if doc.get("estado") != "aprobado":
        raise HTTPException(status_code=422, detail="Solo se puede pagar una solicitud aprobada.")

    ahora = _ahora_utc()
    estado_prev = doc.get("estado")
    pago = {
        "usuario": info["usuario"], "nombre": info["nombre"], "rol": info["perfil"],
        "estado_pago": (req.estado_pago or "PAGADO").strip().upper(),
        "fecha_pago": ahora,
        "fecha_pago_ingresada": req.fecha_pago or None,
        "referencia": req.referencia or "",
        "observaciones": req.observaciones or "",
    }
    mov_pago = _nuevo_movimiento("registro_pago", estado_prev, "pagado", info, req.observaciones, _ip(request))
    # Guardar atómicamente el pago SOLO si sigue aprobada (anti-doble-pago).
    res = col_activos.update_one(
        {"consecutivo": req.consecutivo, "estado": "aprobado"},
        {"$set": {"estado": "pagado", "pago": pago, "updated_at": ahora},
         "$push": {"historial_movimientos": mov_pago}},
    )
    if res.matched_count == 0:
        raise HTTPException(status_code=409, detail="La solicitud cambió de estado (acción simultánea).")

    doc_final = col_activos.find_one({"consecutivo": req.consecutivo})
    # Registrar el paso al histórico y mover
    mov_hist = _nuevo_movimiento("paso_historico", "pagado", "pagado", info, "Paso al histórico", _ip(request))
    col_activos.update_one({"consecutivo": req.consecutivo}, {"$push": {"historial_movimientos": mov_hist}})
    doc_final = col_activos.find_one({"consecutivo": req.consecutivo})
    _mover_documento(doc_final, col_historico)
    return {"mensaje": "Pago registrado y solicitud movida al histórico", "estado": "pagado"}


@router.post("/anular")
async def anular_solicitud(req: AnularRequest, request: Request):
    info = _resolver_usuario(req.usuario)
    _requiere(info, {"ADMIN"}, "anular solicitudes")
    doc, col = _doc_por_consecutivo(req.consecutivo)
    if not doc:
        raise HTTPException(status_code=404, detail="Solicitud no encontrada.")
    if doc.get("estado") == "anulado":
        raise HTTPException(status_code=409, detail="La solicitud ya está anulada.")

    estado_prev = doc.get("estado")
    ahora = _ahora_utc()
    mov = _nuevo_movimiento("anulacion", estado_prev, "anulado", info, req.motivo, _ip(request))
    if col is col_activos:
        col_activos.update_one(
            {"consecutivo": req.consecutivo},
            {"$set": {"estado": "anulado", "motivo_anulacion": req.motivo or "",
                      "anulado_por": info["usuario"], "fecha_anulacion": ahora, "updated_at": ahora},
             "$push": {"historial_movimientos": mov}},
        )
        doc_final = col_activos.find_one({"consecutivo": req.consecutivo})
        _mover_documento(doc_final, col_anulados)
    else:
        # Ya estaba en histórico/anulados: sólo se marca (no se mueve de colección)
        col.update_one(
            {"consecutivo": req.consecutivo},
            {"$set": {"estado": "anulado", "motivo_anulacion": req.motivo or "",
                      "anulado_por": info["usuario"], "fecha_anulacion": ahora, "updated_at": ahora},
             "$push": {"historial_movimientos": mov}},
        )
    return {"mensaje": "Solicitud anulada", "estado": "anulado"}


def _ip(request: Request) -> str:
    try:
        return request.client.host if request and request.client else ""
    except Exception:
        return ""


def _construir_filtro_listado(
    info: dict,
    estado: Optional[str],
    fecha_inicio: Optional[str],
    fecha_fin: Optional[str],
    pedido: Optional[str],
    placa: Optional[str],
    manifiesto: Optional[str],
    cliente: Optional[str],
) -> dict:
    filtro: dict = {}
    # Descartar documentos huérfanos/incompletos (sin consecutivo) que pudieran existir.
    filtro["consecutivo"] = {"$exists": True}
    _scope_lectura(filtro, info)

    if estado:
        filtro["estado"] = estado
    # FINANCIERO siempre ve 'aprobado' en activos (el scope ya lo impone)
    if pedido:
        pnorm = _normalizar_pedidos(pedido)
        if len(pnorm) == 1:
            filtro["pedidos_normalizados"] = pnorm[0]
        elif pnorm:
            filtro["pedidos_normalizados"] = {"$in": pnorm}
    if placa:
        filtro["datos_servicio.placa"] = str(placa).strip().upper()
    if manifiesto:
        filtro["manifiesto"] = str(manifiesto).strip()
    if cliente:
        filtro["datos_servicio.cliente"] = {"$regex": re.escape(str(cliente).strip()), "$options": "i"}
    if fecha_inicio or fecha_fin:
        rango = {}
        if fecha_inicio:
            rango["$gte"] = datetime.strptime(fecha_inicio, "%Y-%m-%d") + _OFFSET_COLOMBIA
        if fecha_fin:
            rango["$lt"] = datetime.strptime(fecha_fin, "%Y-%m-%d") + timedelta(days=1) + _OFFSET_COLOMBIA
        filtro["created_at"] = rango
    return filtro


@router.get("/")
async def listar_activos(
    request: Request,
    usuario: str = Query(...),
    estado: Optional[str] = Query(None),
    fecha_inicio: Optional[str] = Query(None),
    fecha_fin: Optional[str] = Query(None),
    pedido: Optional[str] = Query(None),
    placa: Optional[str] = Query(None),
    manifiesto: Optional[str] = Query(None),
    cliente: Optional[str] = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
):
    info = _resolver_usuario(usuario)
    filtro = _construir_filtro_listado(info, estado, fecha_inicio, fecha_fin, pedido, placa, manifiesto, cliente)
    total = col_activos.count_documents(filtro)
    cursor = col_activos.find(filtro).sort("created_at", -1).skip(skip).limit(limit)
    items = [_serializar(d, info["perfil"]) for d in cursor]
    return {"total": total, "skip": skip, "limit": limit, "items": items}


@router.get("/historico")
async def listar_historico(
    request: Request,
    usuario: str = Query(...),
    estado: Optional[str] = Query(None),
    fecha_inicio: Optional[str] = Query(None),
    fecha_fin: Optional[str] = Query(None),
    pedido: Optional[str] = Query(None),
    placa: Optional[str] = Query(None),
    manifiesto: Optional[str] = Query(None),
    cliente: Optional[str] = Query(None),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
):
    info = _resolver_usuario(usuario)
    # FINANCIERO ve todo el histórico (pagadas); OPERATIVO, las propias; el resto, todas.
    filtro = _construir_filtro_listado(info, estado, fecha_inicio, fecha_fin, pedido, placa, manifiesto, cliente)
    # Reaplicar scope sobre histórico (FINANCIERO sin restricción aquí; OPERATIVO propias)
    if info["perfil"] == "OPERATIVO":
        filtro["usuario_registro"] = info["usuario"]
    total = col_historico.count_documents(filtro)
    cursor = col_historico.find(filtro).sort("created_at", -1).skip(skip).limit(limit)
    items = [_serializar(d, info["perfil"]) for d in cursor]
    return {"total": total, "skip": skip, "limit": limit, "items": items}


def _obtener_detalle(consecutivo: str, info: dict, col):
    doc = col.find_one({"consecutivo": consecutivo})
    if not doc:
        raise HTTPException(status_code=404, detail="Solicitud no encontrada.")
    # Scope de detalle
    perfil = info["perfil"]
    if perfil == "OPERATIVO" and doc.get("usuario_registro") != info["usuario"]:
        raise HTTPException(status_code=403, detail="No tiene acceso a esta solicitud.")
    if perfil == "FINANCIERO" and doc.get("estado") not in {"aprobado", "pagado"}:
        raise HTTPException(status_code=403, detail="No tiene acceso a esta solicitud.")
    return _serializar(doc, perfil)


@router.get("/{consecutivo}")
async def obtener_detalle_activo(consecutivo: str, usuario: str = Query(...)):
    info = _resolver_usuario(usuario)
    return _obtener_detalle(consecutivo, info, col_activos)


@router.get("/historico/{consecutivo}")
async def obtener_detalle_historico(consecutivo: str, usuario: str = Query(...)):
    info = _resolver_usuario(usuario)
    return _obtener_detalle(consecutivo, info, col_historico)


@router.post("/exportar-excel")
async def exportar_excel(req: ExportarExcelRequest):
    import io
    from openpyxl import Workbook
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side

    info = _resolver_usuario(req.usuario)
    col = col_historico if req.origen != "activos" else col_activos
    filtro = _construir_filtro_listado(
        info, req.estado, req.fecha_inicio, req.fecha_fin, req.pedido, req.placa, req.manifiesto, req.cliente
    )
    if col is col_historico and info["perfil"] == "OPERATIVO":
        filtro["usuario_registro"] = info["usuario"]

    docs = list(col.find(filtro).sort("created_at", -1).limit(5000))

    wb = Workbook()
    ws = wb.active
    ws.title = "Otros Costos"

    columnas = [
        "Consecutivo", "Pedido Vulcano", "Cliente", "Placa", "Manifiesto",
        "Tipo de Costo", "Valor Total", "Usuario Creación", "Usuario Aprobación",
        "Rol Aprobación", "Usuario Pago", "Estado Final", "Fecha Creación",
        "Fecha Aprobación", "Fecha Pago",
    ]
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="004d40", end_color="004d40", fill_type="solid")
    thin = Border(*(Side(style="thin"),) * 4)
    for i, c in enumerate(columnas, 1):
        cell = ws.cell(row=1, column=i, value=c)
        cell.font = header_font
        cell.fill = header_fill
        cell.border = thin
        cell.alignment = Alignment(horizontal="center")

    enmascarar = info["perfil"] not in {"FINANCIERO", "ADMIN"}
    for r, d in enumerate(docs, start=2):
        ds = d.get("datos_servicio", {}) or {}
        costos = d.get("costos", []) or []
        tipos = ", ".join((c.get("tipo_costo") or "") for c in costos)
        aprob = d.get("aprobacion", {}) or {}
        pago = d.get("pago", {}) or {}
        fila = [
            d.get("consecutivo", ""),
            d.get("pedido_vulcano_original", ""),
            ds.get("cliente", ""),
            ds.get("placa", ""),
            d.get("manifiesto", ""),
            tipos,
            d.get("valor_total", 0),
            (d.get("creado_por", {}) or {}).get("usuario", ""),
            aprob.get("usuario", ""),
            aprob.get("rol", ""),
            pago.get("usuario", ""),
            d.get("estado", ""),
            _fecha_a_str(d.get("created_at")),
            _fecha_a_str(aprob.get("fecha")),
            _fecha_a_str(pago.get("fecha_pago")),
        ]
        for i, v in enumerate(fila, 1):
            ws.cell(row=r, column=i, value=v).border = thin

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return Response(
        content=output.read(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=otros_costos.xlsx"},
    )
