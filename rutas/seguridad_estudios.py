"""API de Estudios de Seguridad (multi-tenant): cédula → estudio → PDF.

Flujo: login (correo+clave, perfil SEGURIDAD/ADMIN con empresa) → token Bearer
→ POST /seguridad/estudios {cedula} → RNDC + Procuraduría en paralelo →
estado global honesto (COMPLETADA / COMPLETADA_CON_ADVERTENCIAS / PARCIAL /
ERROR — nunca éxito con fuente fallida) → PDF consolidado en GCS privado →
servido solo por endpoint autenticado (cada descarga queda auditada).

Aislamiento entre empresas: TODO find/count filtra por empresa_id del actor
(ADMIN_INTEGRA ve todas); acceso cross-tenant responde 404 (no revela
existencia) y deja evento de acceso_denegado.
"""
from __future__ import annotations

import logging
import os
import re
import secrets
from collections import defaultdict, deque
from datetime import datetime, timedelta

from bson import ObjectId
from fastapi import APIRouter, BackgroundTasks, Body, Depends, HTTPException, Query, Request, Response
from fastapi.security import OAuth2PasswordRequestForm
from pydantic import BaseModel

from bd.bd_cliente import bd_cliente
from Funciones import personas_seguridad as personas
from Funciones.claves import crear_hash, verificar_clave
from Funciones.auth_seguridad import (
    ROL_ADMIN_EMPRESA,
    ROL_ADMIN_INTEGRA,
    ROL_CONSULTADOR,
    actor_actual,
    autenticar,
    crear_token_estudios,
)
from Funciones.orquestador_estudios import (
    FUENTES,
    calcular_estado_global,
    crear_documento_estudio,
    codigo_verificacion,
    detectar_nombres,
    enmascarar_cedula,
    ejecutar_estudio,
    fuentes_habilitadas_efectivas,
    fuentes_pendientes_estudio,
    mayoria_fuentes_fallidas,
    nuevo_consulta_id,
    reintentar_fuentes_estudio,
)
from rutas.seguridad import _normalizar_cedula, _normalizar_placa

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/seguridad/estudios", tags=["Seguridad - Estudios"])

db = bd_cliente["integra"]
col_estudios = db["estudios_seguridad"]
col_eventos = db["eventos_seguridad"]
col_empresas = db["empresas_seguridad"]
col_usuarios = db["baseusuarios"]

# Índices de la memoria de personas (idempotentes; patrón cobro_seguridad).
personas.asegurar_indices_personas()

CONSULTAS_MIN_DEFAULT = int(os.getenv("SEGURIDAD_CONSULTAS_MIN", "10"))
# Rate limit por empresa: ventana móvil de 60 s en memoria (una instancia).
_RATE: dict[str, deque] = defaultdict(lambda: deque())

ESTADOS_ESTUDIO = {"EN_PROGRESO", "COMPLETADA", "COMPLETADA_CON_ADVERTENCIAS", "PARCIAL", "ERROR"}


# --- Helpers -------------------------------------------------------------------

def _utcnow() -> datetime:
    return datetime.utcnow()


def registrar_evento(
    evento: str,
    *,
    actor: dict | None = None,
    consulta_id: str | None = None,
    fuente: str | None = None,
    detalle: str = "",
    request: Request | None = None,
) -> None:
    """Log best-effort: un fallo de auditoría nunca rompe el request."""
    doc = {
        "evento": evento,
        "empresa_id": ObjectId(actor["empresa_id"]) if actor and actor.get("empresa_id") else None,
        # API keys no tienen usuario humano detrás (usuario_id None).
        "usuario_id": ObjectId(actor["usuario_id"]) if actor and actor.get("usuario_id") else None,
        "usuario": (actor or {}).get("usuario"),
        "canal": (actor or {}).get("canal", "portal"),
        "consulta_id": consulta_id,
        "fuente": fuente,
        "ip": (request.client.host if request and request.client else (actor or {}).get("ip", "")),
        "user_agent": request.headers.get("user-agent", "") if request else "",
        "detalle": detalle[:300],
        "creado_en": _utcnow(),
    }
    try:
        col_eventos.insert_one(doc)
    except Exception as exc:
        logger.warning("Evento %s no se pudo registrar: %s", evento, exc)


def _requiere_rol(actor: dict, roles: set[str], accion: str) -> None:
    if actor["rol"] not in roles:
        raise HTTPException(
            status_code=403,
            detail=f"Su rol ({actor['rol']}) no tiene permiso para {accion}.",
        )


def _filtro_empresa(actor: dict) -> dict:
    """Filtro de aislamiento multi-tenant para TODAS las consultas de estudios.

    Acepta empresa_id/usuario_id tanto ObjectId (lo correcto) como string —
    documentos escritos antes del fix de tipos siguen siendo visibles.
    """
    if actor["rol"] == ROL_ADMIN_INTEGRA:
        return {}
    filtro: dict = {"empresa_id": {"$in": [ObjectId(actor["empresa_id"]), actor["empresa_id"]]}}
    if (
        actor["rol"] == ROL_CONSULTADOR
        and actor.get("canal") != "api"  # una API key ve TODA la empresa (integración)
        and (actor.get("empresa_config") or {}).get("aislamiento_usuario")
        and actor.get("usuario_id")
    ):
        filtro["usuario_id"] = {"$in": [ObjectId(actor["usuario_id"]), actor["usuario_id"]]}
    return filtro


def _obtener_estudio(consulta_id: str, actor: dict, request: Request | None = None) -> dict:
    """Busca el estudio DENTRO del scope del actor; cross-tenant → 404 + evento."""
    doc = col_estudios.find_one({"consulta_id": consulta_id.strip().upper(), **_filtro_empresa(actor)})
    if not doc:
        registrar_evento(
            "acceso_denegado",
            actor=actor,
            consulta_id=consulta_id,
            detalle="Estudio inexistente o de otra empresa",
            request=request,
        )
        raise HTTPException(status_code=404, detail="Estudio no encontrado")
    return doc


def _auditoria_request(request: Request) -> dict:
    return {
        "ip": request.client.host if request.client else "",
        "user_agent": request.headers.get("user-agent", "")[:300],
        "esquema_auth": "bearer",
    }


def _verificar_rate_limit(actor: dict) -> None:
    """Ventana móvil de 60 s por empresa (no aplica a ADMIN_INTEGRA)."""
    if actor["rol"] == ROL_ADMIN_INTEGRA:
        return
    limite = int((actor.get("empresa_config") or {}).get("consultas_por_minuto") or CONSULTAS_MIN_DEFAULT)
    ahora = datetime.utcnow().timestamp()
    cola = _RATE[actor["empresa_id"]]
    while cola and cola[0] < ahora - 60:
        cola.popleft()
    if len(cola) >= limite:
        raise HTTPException(
            status_code=429,
            detail=f"Límite de {limite} estudios/minuto alcanzado para su empresa. Intente en unos segundos.",
        )
    cola.append(ahora)


def _empresa_del_actor(actor: dict) -> dict:
    """Doc de la empresa del actor (ADMIN_INTEGRA necesita una para crear: usa
    la que indique el body o la primera activa — ver endpoint crear)."""
    if actor.get("empresa_id"):
        doc = col_empresas.find_one({"_id": ObjectId(actor["empresa_id"])})
        if doc:
            return doc
    return {
        "_id": None,
        "nombre": "INTEGRA (admin)",
        "config": {"fuentes_habilitadas": list(FUENTES)},
    }


def _respuesta_estudio(doc: dict) -> dict:
    """Vista de respuesta del estudio: sin _id ni campos internos, con
    ObjectId convertidos a string (FastAPI no los serializa)."""
    doc = {k: v for k, v in doc.items() if k != "_id"}
    for campo in ("empresa_id", "usuario_id"):
        if isinstance(doc.get(campo), ObjectId):
            doc[campo] = str(doc[campo])
    for fuente in (doc.get("fuentes") or {}).values():
        if not isinstance(fuente, dict):
            continue  # p.ej. fuentes.error_global (str) tras fallo inesperado
        fuente.pop("_pdf_bytes", None)
        if isinstance(fuente.get("cache_id"), ObjectId):
            fuente["cache_id"] = str(fuente["cache_id"])
    return doc


# === 1. Login ==================================================================

class LoginEstudios(BaseModel):
    correo: str
    clave: str


@router.post("/login")
def login_estudios(datos: LoginEstudios, request: Request):
    """Login del módulo: correo + clave → Bearer token con empresa/rol."""
    try:
        usuario_doc, rol, empresa_id, empresa_doc = autenticar(datos.correo, datos.clave)
    except HTTPException as exc:
        registrar_evento(
            "login_fallido",
            detalle=f"{exc.status_code}: {exc.detail}",
            request=request,
        )
        raise
    token = crear_token_estudios(usuario_doc, str(empresa_id) if empresa_id else None, rol)
    registrar_evento("login_exitoso", actor={
        "usuario_id": str(usuario_doc["_id"]),
        "usuario": usuario_doc.get("usuario", ""),
        "empresa_id": str(empresa_id) if empresa_id else None,
    }, request=request)
    return {
        "mensaje": "Autenticado",
        "access_token": token,
        "token_type": "bearer",
        "expira_en_min": int(os.getenv("SEGURIDAD_TOKEN_MINUTES", "480")),
        "usuario": {
            "id": str(usuario_doc["_id"]),
            "usuario": usuario_doc.get("usuario", ""),
            "nombre": usuario_doc.get("nombre", ""),
            "correo": usuario_doc.get("correo", ""),
            "perfil": usuario_doc.get("perfil", ""),
            "rol": rol,
            "empresa": (
                {"id": str(empresa_doc["_id"]), "nombre": empresa_doc["nombre"], "slug": empresa_doc.get("slug", "")}
                if empresa_doc
                else None
            ),
        },
    }


@router.post("/token")
def token_estudios(
    request: Request,
    formulario=Depends(OAuth2PasswordRequestForm),
):
    """Variante OAuth2 (formulario username/password) para el botón Authorize
    de Swagger — reusa el mismo login del módulo."""
    try:
        usuario_doc, rol, empresa_id, empresa_doc = autenticar(formulario.username, formulario.password)
    except HTTPException as exc:
        registrar_evento("login_fallido", detalle=f"{exc.status_code}: {exc.detail}", request=request)
        raise
    token = crear_token_estudios(usuario_doc, str(empresa_id) if empresa_id else None, rol)
    registrar_evento("login_exitoso", actor={
        "usuario_id": str(usuario_doc["_id"]),
        "usuario": usuario_doc.get("usuario", ""),
        "empresa_id": str(empresa_id) if empresa_id else None,
    }, request=request)
    return {"access_token": token, "token_type": "bearer"}


# === 1b. Claves: cambio autenticado y recuperación ==============================

class CambiarClaveIn(BaseModel):
    clave_actual: str
    clave_nueva: str


class RecuperarSolicitarIn(BaseModel):
    correo: str


class RecuperarConfirmarIn(BaseModel):
    correo: str
    codigo: str
    clave_nueva: str


RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
MAIL_FROM = os.getenv("MAIL_FROM", "no-reply@integralogistica.com")
EXPIRE_MIN_RECUPERACION = int(os.getenv("RESET_TOKEN_EXPIRE_MINUTES", "15"))
MAX_INTENTOS_CODIGO = 8


def _enviar_correo_codigo(email_destino: str, codigo: str):
    """Envía el código de recuperación por Resend (best-effort, background)."""
    if not RESEND_API_KEY:
        logger.warning("RESEND_API_KEY no configurada: código de recuperación no enviado por correo")
        return
    try:
        import resend

        resend.api_key = RESEND_API_KEY
        resend.Emails.send({
            "from": MAIL_FROM,
            "to": [email_destino],
            "subject": "seguriDatia · código para restablecer su contraseña",
            "html": (
                "<div style='font-family:Segoe UI,Arial,sans-serif;color:#22313f'>"
                "<h2 style='color:#00a5b5;margin-bottom:4px'>seguriDatia</h2>"
                "<p>Recibimos una solicitud para restablecer la contraseña de su cuenta "
                f"de Consultas de Seguridad (<b>{email_destino}</b>).</p>"
                f"<p style='font-size:1.6rem;letter-spacing:8px;font-weight:700'>{codigo}</p>"
                f"<p>Este código vence en {EXPIRE_MIN_RECUPERACION} minutos. Si no lo solicitó, ignore este mensaje.</p>"
                "</div>"
            ),
        })
    except Exception as exc:
        logger.error("Correo de recuperación a %s falló: %s", email_destino, exc)


@router.post("/cambiar-clave")
def cambiar_clave_estudios(datos: CambiarClaveIn, request: Request, actor: dict = Depends(actor_actual)):
    """Cambio de clave AUTENTICADO (menú del avatar del portal). Verifica la
    clave actual contra `baseusuarios` (bcrypt dual-mode de Funciones/claves)
    y guarda la nueva hasheada."""
    if len(datos.clave_nueva) < 6:
        raise HTTPException(status_code=422, detail="La clave nueva debe tener al menos 6 caracteres")
    if not actor.get("usuario_id"):
        # Las API keys no son usuarios humanos: nada que cambiar.
        raise HTTPException(status_code=403, detail="El cambio de clave aplica a usuarios del portal, no a API keys")
    usuario = col_usuarios.find_one({"_id": ObjectId(actor["usuario_id"])})
    if not usuario:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    if not verificar_clave(datos.clave_actual, usuario.get("clave") or ""):
        registrar_evento("clave_cambio_fallido", actor=actor, request=request)
        raise HTTPException(status_code=400, detail="La clave actual no es correcta")
    col_usuarios.update_one(
        {"_id": usuario["_id"]},
        {"$set": {"clave": crear_hash(datos.clave_nueva)}},
    )
    registrar_evento("clave_cambiada", actor=actor, request=request)
    return {"mensaje": "Clave actualizada correctamente"}


@router.post("/recuperar/solicitar")
def recuperar_solicitar(datos: RecuperarSolicitarIn, request: Request, background_tasks: BackgroundTasks):
    """Paso 1 del 'olvidé mi contraseña' del PORTAL: genera un código de 6
    dígitos de un solo uso (vence 15 min) y lo envía por correo vía Resend.
    Respuesta NEUTRA: no revela si el correo existe (anti-enumeración)."""
    correo = (datos.correo or "").strip().lower()
    usuario = col_usuarios.find_one({"email": correo, "activo": True})
    if usuario:
        codigo = f"{secrets.randbelow(1_000_000):06d}"
        col_usuarios.update_one(
            {"_id": usuario["_id"]},
            {"$set": {
                "reset_codigo_hash": crear_hash(codigo),
                "reset_codigo_exp": _utcnow() + timedelta(minutes=EXPIRE_MIN_RECUPERACION),
                "reset_codigo_intentos": 0,
            }},
        )
        background_tasks.add_task(_enviar_correo_codigo, correo, codigo)
        registrar_evento(
            "recuperacion_solicitada",
            actor={"usuario_id": str(usuario["_id"]), "usuario": usuario.get("usuario", ""), "empresa_id": None},
            request=request,
        )
    return {"mensaje": f"Si el correo está registrado, enviamos un código (vence en {EXPIRE_MIN_RECUPERACION} minutos)"}


@router.post("/recuperar/confirmar")
def recuperar_confirmar(datos: RecuperarConfirmarIn, request: Request):
    """Paso 2: correo + código de 6 dígitos + clave nueva. Límite de intentos
    por código (8) para frenar fuerza bruta; al acertar se cambia la clave y
    el código queda inservible."""
    if len(datos.clave_nueva) < 6:
        raise HTTPException(status_code=422, detail="La clave nueva debe tener al menos 6 caracteres")
    correo = (datos.correo or "").strip().lower()
    usuario = col_usuarios.find_one({"email": correo, "activo": True})
    codigo_hash = (usuario or {}).get("reset_codigo_hash")
    exp = (usuario or {}).get("reset_codigo_exp")
    valido = (
        usuario and codigo_hash and exp
        and isinstance(exp, datetime)
        and exp > _utcnow()
        and verificar_clave((datos.codigo or "").strip(), codigo_hash)
    )
    if not valido:
        if usuario and codigo_hash:
            intentos = int(usuario.get("reset_codigo_intentos") or 0) + 1
            col_usuarios.update_one(
                {"_id": usuario["_id"]},
                {"$set": {"reset_codigo_intentos": intentos}},
            )
            if intentos >= MAX_INTENTOS_CODIGO:
                col_usuarios.update_one(
                    {"_id": usuario["_id"]},
                    {"$set": {"reset_codigo_hash": None, "reset_codigo_exp": None}},
                )
        raise HTTPException(status_code=400, detail="Código inválido o expirado")
    col_usuarios.update_one(
        {"_id": usuario["_id"]},
        {"$set": {"clave": crear_hash(datos.clave_nueva), "reset_codigo_hash": None, "reset_codigo_exp": None}},
    )
    registrar_evento(
        "clave_restablecida",
        actor={"usuario_id": str(usuario["_id"]), "usuario": usuario.get("usuario", ""), "empresa_id": None},
        request=request,
    )
    return {"mensaje": "Su contraseña fue restablecida"}


# === 2. Identidad ==============================================================

@router.get("/me")
def quien_soy(actor: dict = Depends(actor_actual)):
    """Verificación de token para el frontend."""
    return {
        "id": actor["usuario_id"],
        "usuario": actor["usuario"],
        "nombre": actor["usuario_nombre"],
        "correo": actor["usuario_correo"],
        "perfil": actor["perfil"],
        "rol": actor["rol"],
        "empresa": (
            {"id": actor["empresa_id"], "nombre": actor["empresa_nombre"]}
            if actor["empresa_id"]
            else None
        ),
    }


@router.get("/cupo")
def consultar_cupo(
    request: Request,
    actor: dict = Depends(actor_actual),
    empresa_id: str | None = Query(None, description="Solo ADMIN_INTEGRA: empresa a consultar"),
):
    """Cupo y plan de la empresa del actor (contrato del futuro portal cliente).

    ADMIN_INTEGRA debe indicar empresa_id (o se reporta su propia empresa si
    la tiene). No expone movimientos.
    """
    _requiere_rol(actor, {ROL_CONSULTADOR, ROL_ADMIN_EMPRESA, ROL_ADMIN_INTEGRA}, "consultar el cupo")
    from Funciones import cobro_seguridad as cobro

    objetivo = actor.get("empresa_id")
    if actor["rol"] == ROL_ADMIN_INTEGRA:
        objetivo = empresa_id or actor.get("empresa_id")
        if not objetivo:
            raise HTTPException(status_code=422, detail="Indique empresa_id para consultar su cupo")
    empresa = col_empresas.find_one({"_id": ObjectId(objetivo)})
    if not empresa:
        raise HTTPException(status_code=404, detail="Empresa no encontrada")

    # El catálogo manda: materializar fuentes nuevas antes de consolidar.
    empresa = cobro.sincronizar_fuentes_planes(empresa, db["planes_seguridad"], col_empresas)

    periodo = cobro.periodo_colombia()
    consumo_mes = cobro.totales_periodo(empresa["_id"], periodo)

    # Multi-plan: desglose por fuente consolidando los planes de cada una
    # (una fuente puede tener varios planes acumulados; los cupos se suman).
    # `planes[]` = planes INDIVIDUALES asignados (para que el usuario elija
    # bajo cuál consultar); `fuentes[]` = consolidado por fuente (cupos totales).
    col_planes = db["planes_seguridad"]
    por_fuente: dict[str, dict] = {}
    planes_individuales: dict[str, dict] = {}  # plan_id → datos agregados del plan
    for entrada in cobro._planes_efectivos(empresa, col_planes):
        fuente = entrada.get("fuente")
        if fuente not in FUENTES and fuente != "todas":
            continue
        doc = col_planes.find_one({"_id": entrada.get("plan_id")}) if entrada.get("plan_id") else None
        valido, _ = cobro._plan_vigente(doc, asignado_en=entrada.get("asignado_en"))
        if not valido:
            continue
        # Fuente retirada del catálogo: la entrada sobrevive en la empresa pero
        # no se ofrece (rompería precio=min e "ilimitado contagia" del consolidado).
        if fuente != "todas" and fuente not in (valido.get("fuentes_incluidas") or []):
            continue
        nombres = [n for n in (valido.get("nombre", ""),) if n]
        ilimitado = entrada.get("cupo_autorizado") is None
        precio_entrada = int(entrada.get("precio_congelado") or valido.get("precio_por_estudio") or 0)

        pid = str(entrada.get("plan_id"))
        if pid not in planes_individuales:
            planes_individuales[pid] = {
                "plan_id": pid,
                "nombre": valido.get("nombre", ""),
                "precio_por_estudio": precio_entrada,
                "fuentes": [fuente],
                "ilimitado": ilimitado,
                "cupo_autorizado": None if ilimitado else int(entrada.get("cupo_autorizado") or 0),
                "cupo_consumido": int(entrada.get("cupo_consumido") or 0),
                "cupo_disponible": None if ilimitado else int(entrada.get("cupo_disponible") or 0),
            }
        else:
            pind = planes_individuales[pid]
            if fuente not in pind["fuentes"]:
                pind["fuentes"].append(fuente)
            if ilimitado:
                pind["ilimitado"] = True
                pind["cupo_autorizado"] = None
                pind["cupo_disponible"] = None
            elif not pind["ilimitado"]:
                # v3.5: el cupo del plan cuenta CONSULTAS y sus entradas drenan
                # en LOCKSTEP → el cupo del plan es el de SUS entradas (mínimo
                # por seguridad), NO la suma (4 fuentes × 50 no son 200
                # consultas, son 50).
                pind["cupo_autorizado"] = min(
                    pind["cupo_autorizado"], int(entrada.get("cupo_autorizado") or 0)
                )
                pind["cupo_disponible"] = min(
                    pind["cupo_disponible"], int(entrada.get("cupo_disponible") or 0)
                )
            pind["cupo_consumido"] = max(
                pind["cupo_consumido"], int(entrada.get("cupo_consumido") or 0)
            )

        if fuente not in por_fuente:
            por_fuente[fuente] = {
                "fuente": fuente,
                "planes_nombres": nombres,
                "ilimitado": ilimitado,
                "cupo_autorizado": None if ilimitado else int(entrada.get("cupo_autorizado") or 0),
                "cupo_consumido": int(entrada.get("cupo_consumido") or 0),
                "cupo_disponible": None if ilimitado else int(entrada.get("cupo_disponible") or 0),
                "precio_por_estudio": precio_entrada,
            }
        else:
            agg = por_fuente[fuente]
            for n in nombres:
                if n not in agg["planes_nombres"]:
                    agg["planes_nombres"].append(n)
            if ilimitado:
                # Un plan sin tope hace que toda la fuente quede sin tope.
                agg["ilimitado"] = True
                agg["cupo_autorizado"] = None
                agg["cupo_disponible"] = None
            elif not agg["ilimitado"]:
                # Ambos con cupo: se suman.
                agg["cupo_autorizado"] = (agg["cupo_autorizado"] or 0) + int(entrada.get("cupo_autorizado") or 0)
                agg["cupo_disponible"] = (agg["cupo_disponible"] or 0) + int(entrada.get("cupo_disponible") or 0)
            agg["cupo_consumido"] += int(entrada.get("cupo_consumido") or 0)
            # Precio mostrado: el plan más barato (el primero que se consume FIFO no
            # se puede prometer; el mínimo es el mejor caso para el cliente).
            agg["precio_por_estudio"] = min(agg["precio_por_estudio"], precio_entrada)
    fuentes_cupo = []
    for fuente, agg in por_fuente.items():
        fuentes_cupo.append({
            **agg,
            "plan_nombre": " + ".join(agg["planes_nombres"]),
        })

    return {
        "empresa": empresa.get("nombre", ""),
        "vigente": bool(fuentes_cupo),
        "fuentes": fuentes_cupo,
        # Planes individuales para el selector de "bajo qué plan consultar".
        # Solo planes con cupo restante (o sin tope) son elegibles.
        "planes": [
            p for p in planes_individuales.values()
            if p["ilimitado"] or (p["cupo_disponible"] or 0) > 0
        ],
        "consumo_mes": {
            "periodo": periodo,
            "unidades": consumo_mes.get("unidades", 0),
            "cop": consumo_mes.get("subtotal_cop", 0),
        },
    }


# === 3. Crear estudio ==========================================================

_TILDES_NOMBRE = str.maketrans("áéíóúüÁÉÍÓÚÜ", "aeiouuAEIOUU")


def _normalizar_nombre(valor: str | None) -> str | None:
    """Nombres/apellidos del consultante → MAYÚSCULAS SIN tildes, solo letras
    y espacios (2026-09-01): así los espera el captcha de la PGN ("¿cuál es
    el primer nombre…?"). La Ñ se conserva (es letra, no tilde). None/vacío
    → None (sin pista)."""
    texto = (valor or "").translate(_TILDES_NOMBRE).upper()
    texto = "".join(c for c in texto if c.isalpha() or c == " ")
    texto = " ".join(texto.split())
    return texto or None


def _normalizar_nit_sin_dv(valor: str | None) -> str:
    """NIT base: elimina el DV si viene separado por guion."""
    crudo = (valor or "").strip()
    crudo = re.sub(r"-\s*\d\s*$", "", crudo)
    return re.sub(r"\D", "", crudo)


def _completar_desde_memoria(
    cedula: str, empresa_id: str,
    nombres: str | None, apellidos: str | None, fecha_expedicion: str | None,
) -> tuple[str | None, str | None, str]:
    """Completa los campos que el body NO trajo con la memoria de personas
    de ESTA empresa (2026-09-25). Lo que el usuario envió SIEMPRE gana; la
    memoria solo rellena vacíos (nombres/apellidos para rama_judicial y el
    captcha de la PGN; fecha de expedición para delitos_sexuales)."""
    persona = personas.buscar_persona(cedula, empresa_id) if cedula else None
    if not persona:
        return nombres, apellidos, fecha_expedicion or ""
    if not nombres:
        nombres = _normalizar_nombre(persona.get("nombres"))
    if not apellidos:
        apellidos = _normalizar_nombre(persona.get("apellidos"))
    if not fecha_expedicion:
        fecha_expedicion = (persona.get("fecha_expedicion") or "").strip()
    return nombres, apellidos, fecha_expedicion


def _nombres_verificados_del_estudio(estudio: dict | None) -> tuple[str | None, str | None]:
    """Nombres/apellidos VERIFICADOS por un portal oficial (2026-09-25).

    situacion_militar y sisconmp devuelven `nombres`/`apellidos` SEPARADOS
    del certificado oficial (Ejército / Mintransporte) — son la fuente de
    verdad de la persona evaluada: si el consultante escribió el nombre mal,
    el verificado lo REEMPLAZA en la memoria. El nombre completo de la
    cascada PGN→Policía viene en un solo string (apellidos+nombres) sin
    split confiable, así que queda como `nombre_consultado` (mostrar) y NO
    alimenta los campos separados. Retorna (None, None) si ninguna fuente
    verificada respondió: la memoria conserva lo escrito por el consultante.
    """
    fuentes = (estudio or {}).get("fuentes") or {}
    for fuente in ("situacion_militar", "sisconmp"):
        seccion = fuentes.get(fuente) or {}
        if seccion.get("estado") not in {"EXITO", "ADVERTENCIA"}:
            continue
        nombres = _normalizar_nombre(seccion.get("nombres"))
        apellidos = _normalizar_nombre(seccion.get("apellidos"))
        if nombres or apellidos:
            return (nombres or None, apellidos or None)
    return (None, None)


class CrearEstudio(BaseModel):
    cedula: str | None = None
    nit: str | None = None  # fuente empresarial ofac_nit; separado de la cédula
    forzar: bool = False
    empresa_id: str | None = None  # solo ADMIN_INTEGRA (attribución del estudio)
    fuentes: list[str] | None = None  # fuentes a consultar; None = todas las del plan
    plan_id: str | None = None  # plan con el que cobrar la consulta (elegido por el usuario)
    placa: str | None = None  # fuentes de vehículo (runt/simit)
    # cédula del PROPIETARIO del vehículo (solo runt): el RUNT valida contra
    # el propietario ACTIVO de la placa, que muchas veces NO es el conductor
    # evaluado. Vacía → se usa la cédula consultada (comportamiento previo).
    cedula_propietario: str | None = None
    # Nombres y apellidos de la persona evaluada que informa el CONSULTANTE
    # (2026-09-01): el portal los PIDE cuando el plan incluye procuraduria
    # porque el captcha de la PGN pregunta por ellos ("¿cuál es el primer
    # nombre de la persona que está consultando?"). Se normalizan SIN tildes
    # y en mayúsculas. Opcionales para no romper integraciones API: sin ellos
    # esas variantes del captcha caen a Gemini/reintento.
    nombres: str | None = None
    apellidos: str | None = None
    # Fecha de EXPEDICIÓN de la cédula (DD/MM/AAAA o aaaa-mm-dd, 2026-09-14):
    # el portal de inhabilidades (Ley 1918) la PIDE junto con la cédula.
    # Obligatoria cuando el plan incluye delitos_sexuales (422 accionable).
    fecha_expedicion: str | None = None


@router.post("", status_code=201)
async def crear_estudio(
    datos: CrearEstudio,
    request: Request,
    actor: dict = Depends(actor_actual),
):
    """Cédula → RNDC + Procuraduría en paralelo → estado global honesto → PDF.

    HTTP 201 significa que el estudio se creó y auditó; el resultado real de
    las fuentes está en `estado` (COMPLETADA / COMPLETADA_CON_ADVERTENCIAS /
    PARCIAL / ERROR). Nunca se reporta éxito si una fuente falló.
    """
    _requiere_rol(actor, {ROL_CONSULTADOR, ROL_ADMIN_EMPRESA, ROL_ADMIN_INTEGRA}, "crear estudios de seguridad")
    _verificar_rate_limit(actor)

    cedula = _normalizar_cedula(datos.cedula) if (datos.cedula or "").strip() else ""

    from Funciones import cobro_seguridad as cobro

    # ADMIN_INTEGRA sin empresa: puede atribuir el estudio a una empresa vía body.
    empresa = _empresa_del_actor(actor)
    if actor["rol"] == ROL_ADMIN_INTEGRA and not actor.get("empresa_id"):
        if datos.empresa_id:
            empresa = col_empresas.find_one({"_id": ObjectId(datos.empresa_id), "activo": True})
            if not empresa:
                raise HTTPException(status_code=422, detail="empresa_id inválido o inactivo")
        else:
            raise HTTPException(
                status_code=422,
                detail="Como ADMIN_INTEGRA debe indicar empresa_id para atribuir el estudio",
            )
    if not empresa.get("_id"):
        raise HTTPException(status_code=403, detail="Su usuario no tiene empresa asignada")

    # El catálogo manda: materializar fuentes nuevas del plan antes de decidir
    # qué correr (idempotente; para el cliente es transparente).
    empresa = cobro.sincronizar_fuentes_planes(empresa, db["planes_seguridad"], col_empresas)

    # EL PLAN ES EL GATE (2026-09-01): fuentes default habilitadas de facto
    # para toda empresa — editar un plan se refleja INMEDIATAMENTE en todos
    # los clientes; solo policia (opt-in) y las exclusiones explícitas pasan
    # por el config de la empresa.
    habilitadas = fuentes_habilitadas_efectivas(empresa)
    # Fuentes con plan: multi-plan por fuente — la consulta corre solo las
    # fuentes que la empresa tenga con plan vigente (ej. solo compró RNDC).
    # ADMIN_INTEGRA ve todo, salvo fuentes con entrada RETIRADA del catálogo
    # (dejaría correr sin CONSUMO una fuente que el plan ya no cubre).
    if actor["rol"] != ROL_ADMIN_INTEGRA:
        con_plan = cobro.fuentes_con_plan(empresa, habilitadas, db["planes_seguridad"])
        habilitadas = [f for f in habilitadas if f in con_plan]
    else:
        entradas_admin = cobro._planes_efectivos(empresa, db["planes_seguridad"])
        retiradas = {
            e.get("fuente") for e in entradas_admin
            if e.get("fuente") in habilitadas
            and (pd := db["planes_seguridad"].find_one({"_id": e.get("plan_id")}))
            and (e.get("fuente") not in (pd.get("fuentes_incluidas") or []) or not pd.get("activo", True))
        }
        habilitadas = [f for f in habilitadas if f not in retiradas]
    # El usuario ELIGE qué fuentes consultar (debe tener plan para cada una).
    fuentes_pedidas = getattr(datos, "fuentes", None)
    if fuentes_pedidas is not None:
        if not fuentes_pedidas:
            raise HTTPException(status_code=422, detail="fuentes no puede estar vacía (omítala para consultar todas)")
        invalidas = [f for f in fuentes_pedidas if f not in FUENTES]
        if invalidas:
            raise HTTPException(status_code=422, detail=f"Fuentes inválidas: {invalidas}. Válidas: {list(FUENTES)}")
        sin_plan = [f for f in fuentes_pedidas if f not in habilitadas]
        if sin_plan:
            raise HTTPException(
                status_code=422,
                detail=f"Su empresa no tiene plan activo para: {sin_plan}. "
                       f"Fuentes disponibles: {habilitadas}",
            )
        habilitadas = list(fuentes_pedidas)
    if not any(f in habilitadas for f in FUENTES):
        raise HTTPException(
            status_code=503,
            detail="Su empresa no tiene plan activo para ninguna fuente. Contacte a Integra Logística.",
        )

    # El usuario elige BAJO QUÉ PLAN consultar: el PLAN define las fuentes a
    # correr (sus entradas ∩ fuentes habilitadas), no al revés. Sin plan_id
    # se corren todas las fuentes habilitadas (comportamiento previo).
    plan_preferido = None
    plan_pedido = getattr(datos, "plan_id", None)
    if plan_pedido:
        if not re.fullmatch(r"[0-9a-fA-F]{24}", plan_pedido or ""):
            raise HTTPException(status_code=422, detail="plan_id inválido")
        entradas_empresa = cobro._planes_efectivos(empresa, db["planes_seguridad"])
        plan_doc_pedido = db["planes_seguridad"].find_one({"_id": ObjectId(plan_pedido)})
        fuentes_incluidas = (plan_doc_pedido or {}).get("fuentes_incluidas") or []
        fuentes_del_plan = [
            f for f in habilitadas
            if f in fuentes_incluidas  # fuente retirada del plan no corre
            and any(e.get("fuente") == f and str(e.get("plan_id")) == plan_pedido for e in entradas_empresa)
        ]
        if not fuentes_del_plan:
            raise HTTPException(
                422,
                detail="Ese plan no está asignado a su empresa o no cubre ninguna fuente habilitada",
            )
        habilitadas = fuentes_del_plan
        plan_preferido = {"plan_id": ObjectId(plan_pedido), "fuente": fuentes_del_plan[0]}

    # Las fuentes empresariales (OFAC/BDME/RUES) usan NIT, nunca reutilizan la
    # cédula. Las demás fuentes continúan siendo consultas de la persona evaluada.
    if any(f not in {"ofac_nit", "bdme_nit", "rama_judicial", "rues"} for f in habilitadas) and not cedula:
        raise HTTPException(status_code=422, detail="Las fuentes personales requieren el campo cedula")
    nit: str | None = None
    if any(f in {"ofac_nit", "bdme_nit", "rues"} for f in habilitadas):
        nit = _normalizar_nit_sin_dv(getattr(datos, "nit", None))
        if len(nit) < 6 or len(nit) > 15:
            raise HTTPException(
                status_code=422,
                detail="Las fuentes empresariales requieren el NIT sin dígito de verificación (campo nit)",
            )

    # Las fuentes de vehículo (runt, simit) consultan por PLACA: es OBLIGATORIA
    # si alguna va a correr (con `habilitadas` ya definitiva), y se ignora/limpia
    # si no (no se persiste nada del vehículo en ese caso).
    # La cédula del propietario es SOLO de runt (valida contra el PROPIETARIO
    # ACTIVO; puede ser DISTINTA de la del conductor evaluado: sin
    # `cedula_propietario` se asume que el evaluado es el propietario —
    # comportamiento previo a 2026-08-30). simit NO usa cédula: si solo corre
    # simit, el campo se ignora.
    placa: str | None = None
    cedula_propietario: str | None = None
    if "runt" in habilitadas or "simit" in habilitadas:
        # getattr: SimpleNamespace de tests/integraciones puede omitir campos.
        if not (getattr(datos, "placa", None) or "").strip():
            fuentes_placa = " y ".join(
                f for f in ("RUNT", "SIMIT") if f.lower() in habilitadas
            )
            raise HTTPException(
                status_code=422,
                detail=f"La fuente {fuentes_placa} requiere la placa del vehículo (campo placa)",
            )
        placa = _normalizar_placa(datos.placa)
        if "runt" in habilitadas:
            if (getattr(datos, "cedula_propietario", None) or "").strip():
                cedula_propietario = _normalizar_cedula(datos.cedula_propietario)
            else:
                cedula_propietario = cedula

    # Nombres/apellidos del consultante (captcha de la PGN): SIN tildes, en
    # mayúsculas, solo letras y espacios — es la forma en que el portal de la
    # PGN espera la respuesta ("primer nombre de la persona que está
    # consultando").
    nombres = _normalizar_nombre(getattr(datos, "nombres", None))
    apellidos = _normalizar_nombre(getattr(datos, "apellidos", None))

    # Fecha de expedición de la cédula: la exige el portal de inhabilidades
    # de la Ley 1918 (valida el par cédula+fecha). Se acepta DD/MM/AAAA o
    # ISO; el bot la normaliza.
    fecha_expedicion = (getattr(datos, "fecha_expedicion", None) or "").strip()

    # Memoria de personas (2026-09-25): lo que falte del body se completa con
    # lo que ESTA empresa ya consultó de la persona — evita repetir la fecha
    # de expedición y los nombres a mano, y salva el 422 de delitos_sexuales.
    nombres, apellidos, fecha_expedicion = _completar_desde_memoria(
        cedula, str(empresa["_id"]), nombres, apellidos, fecha_expedicion
    )

    # Cascada de nombres (decisión del usuario 2026-09-25): si el plan
    # necesita nombres (captcha de la PGN / búsqueda por nombre de la Rama
    # Judicial) y ni el body ni la memoria los tienen, se DETECTAN solos desde
    # fuentes que devuelven el nombre por cédula ($0: situacion_militar →
    # sisconmp) — el usuario ya no digita nada. Lo que trae el body SIEMPRE
    # gana; el 422 solo queda como última barrera.
    if ("procuraduria" in habilitadas or "rama_judicial" in habilitadas) and (not nombres or not apellidos):
        nombres_detectados, apellidos_detectados = await detectar_nombres(
            cedula, {**actor, "empresa_id": str(empresa["_id"])}
        )
        nombres = nombres or nombres_detectados
        apellidos = apellidos or apellidos_detectados

    if "rama_judicial" in habilitadas and (not nombres or not apellidos):
        raise HTTPException(
            status_code=422,
            detail="La fuente Rama Judicial requiere nombres y apellidos completos: no fue "
                   "posible obtenerlos automáticamente (sin memoria previa ni registro en las "
                   "fuentes oficiales). Intente de nuevo más tarde o comuníquese con Integra "
                   "Logística; por API puede enviarlos en los campos nombres/apellidos",
        )
    if "delitos_sexuales" in habilitadas and not fecha_expedicion:
        raise HTTPException(
            status_code=422,
            detail="La fuente Inhabilidades Ley 1918 requiere la fecha de expedición de la cédula "
                   "(campo fecha_expedicion, formato DD/MM/AAAA)",
        )

    # Actor efectivo para el doc: la empresa de atribución (ADMIN_INTEGA puede
    # actuar sobre otra empresa sin perder su identidad).
    actor_doc = {**actor, "empresa_id": str(empresa["_id"])}

    consulta_id = nuevo_consulta_id()

    # Consumo por fuente (postpago): un CONSUMO por fuente corrida; atómico y
    # con compensación en cascada; sin plan/cupo → 402 antes de ejecutar.
    # plan_id: el usuario eligió bajo qué plan cobrar (si no, FIFO).
    consumos = cobro.reservar_consumos(
        empresa, actor_doc, consulta_id, habilitadas,
        plan_preferido_id=(plan_preferido or {}).get("plan_id"),
    )
    for consumo in consumos:
        if consumo.get("monto_cop", 0) != 0:
            registrar_evento(
                "consumo_registrado",
                actor=actor,
                consulta_id=consulta_id,
                fuente=consumo.get("fuente"),
                detalle=f"{empresa.get('nombre')} · {consumo.get('fuente')} · "
                        f"{consumo.get('plan_nombre')} · ${consumo.get('precio_unitario_cop', 0)}",
                request=request,
            )

    crear_documento_estudio(
        consulta_id=consulta_id,
        cedula=cedula,
        actor=actor_doc,
        empresa=empresa,
        forzar=datos.forzar,
        auditoria=_auditoria_request(request),
        placa=placa,
        cedula_propietario=cedula_propietario,
        nombres=nombres,
        apellidos=apellidos,
        nit=nit,
        fecha_expedicion=fecha_expedicion or None,
    )

    try:
        estudio = await ejecutar_estudio(
            consulta_id=consulta_id,
            cedula=cedula,
            actor=actor_doc,
            empresa=empresa,
            forzar=datos.forzar,
            auditoria=_auditoria_request(request),
            registrar_evento=lambda *a, **k: registrar_evento(*a, request=request, **k),
            fuentes=habilitadas,
            placa=placa,
            cedula_propietario=cedula_propietario,
            nombres=nombres,
            apellidos=apellidos,
            nit=nit,
            fecha_expedicion=fecha_expedicion or None,
        )
    except Exception as exc:
        logger.exception("Estudio %s falló de forma inesperada", consulta_id)
        col_estudios.update_one(
            {"consulta_id": consulta_id},
            {"$set": {"estado": "ERROR", "finalizado_en": _utcnow(), "fuentes.error_global": str(exc)[:300]}},
        )
        estudio = col_estudios.find_one({"consulta_id": consulta_id})

    # Reembolso automático (regla 2026-09-01): se devuelve la consulta SOLO si
    # MÁS del 51% de las fuentes corridas fallaron. Con la mitad o menos caídas
    # lo entregado sigue siendo valioso y se cobra — que la Procuraduría (o
    # cualquier portal) esté caída no invalida el resto del informe. Nada
    # corrido (falla catastrófica del proceso) también reembolsa.
    if consumos and mayoria_fuentes_fallidas(estudio.get("fuentes") or {}):
        try:
            cobro.reembolsar_consumos_consulta(
                consulta_id, empresa, actor_doc,
                motivo="Más del 51% de las fuentes corridas fallaron (o no corrió ninguna)", automatico=True,
            )
            registrar_evento(
                "reembolso", actor=actor, consulta_id=consulta_id,
                detalle="automático por mayoría de fuentes fallidas", request=request,
            )
        except Exception as exc:
            logger.error("Reembolso automático de %s falló: %s", consulta_id, exc)

    # Memoria de personas (2026-09-25): registrar/actualizar la persona para
    # que la próxima consulta de esta empresa autollene nombres, fecha de
    # expedición y el nombre VERIFICADO por los portales. El nombre verificado
    # (situacion_militar/sisconmp separados) REEMPLAZA al escrito a mano —
    # un nombre mal digitado la primera vez no envenena la memoria. Best-effort
    # dentro del módulo (un fallo jamás tumba el estudio).
    if cedula:
        try:
            nombres_verificados, apellidos_verificados = _nombres_verificados_del_estudio(estudio)
            personas.registrar_consulta_persona(
                cedula,
                str(empresa["_id"]),
                nombres=nombres_verificados or nombres or None,
                apellidos=apellidos_verificados or apellidos or None,
                nombre_consultado=(estudio or {}).get("nombre_consultado") or None,
                fecha_expedicion=fecha_expedicion or None,
            )
        except Exception as exc:
            logger.warning("Memoria de personas (%s) no registrada: %s", cedula, exc)

    # PDF consolidado (desde el doc persistido → reproducible) + subida a GCS.
    try:
        from Funciones import storage_seguridad
        from Funciones.pdf_estudio_seguridad import generar_pdf_estudio

        contenido = generar_pdf_estudio(estudio, empresa)
        ruta = storage_seguridad.ruta_blob(str(empresa["_id"]), _utcnow().year, consulta_id)
        subido = storage_seguridad.subir_pdf(contenido, ruta, cedula)
        info_pdf = {
            **subido,
            "version": 1,
            "generado_en": _utcnow(),
            "regeneraciones": 0,
            "historial": [],
        }
        col_estudios.update_one(
            {"consulta_id": consulta_id},
            {"$set": {"pdf": info_pdf}},
        )
        estudio["pdf"] = info_pdf
        registrar_evento(
            "pdf_generado",
            actor=actor,
            consulta_id=consulta_id,
            detalle=f"v1 · {subido['tamano']} bytes",
            request=request,
        )
    except Exception as exc:
        logger.error("PDF del estudio %s no se pudo generar/subir: %s", consulta_id, exc)
        registrar_evento(
            "pdf_generado",
            actor=actor,
            consulta_id=consulta_id,
            detalle=f"ERROR: {str(exc)[:200]}",
            request=request,
        )

    estudio = _respuesta_estudio(estudio)
    estudio["pdf_endpoint"] = f"/seguridad/estudios/{consulta_id}/pdf"
    return estudio


# === 4-5. Listado y detalle ====================================================

@router.get("")
def listar_estudios(
    request: Request,
    actor: dict = Depends(actor_actual),
    cedula: str | None = Query(None, description="Filtrar por cédula"),
    estado: str | None = Query(None, description="Filtrar por estado"),
    usuario_id: str | None = Query(None, description="Filtrar por usuario responsable"),
    fecha_desde: str | None = Query(None, description="YYYY-MM-DD"),
    fecha_hasta: str | None = Query(None, description="YYYY-MM-DD"),
    empresa_id: str | None = Query(None, description="Solo ADMIN_INTEGRA"),
    limit: int = Query(25, ge=1, le=100),
    skip: int = Query(0, ge=0),
):
    """Historial de estudios del scope del actor (sin payloads de fuentes)."""
    query = _filtro_empresa(actor)
    if actor["rol"] == ROL_ADMIN_INTEGRA and empresa_id:
        query = {"empresa_id": ObjectId(empresa_id)}
    if cedula:
        query["cedula"] = _normalizar_cedula(cedula)
    if estado:
        if estado not in ESTADOS_ESTUDIO:
            raise HTTPException(status_code=422, detail=f"Estado inválido: {estado}")
        query["estado"] = estado
    if usuario_id:
        if not re.fullmatch(r"[0-9a-fA-F]{24}", usuario_id):
            raise HTTPException(status_code=422, detail="usuario_id inválido")
        query["usuario_id"] = ObjectId(usuario_id)
    try:
        if fecha_desde:
            query["creado_en"] = {"$gte": datetime.fromisoformat(fecha_desde)}
        if fecha_hasta:
            query.setdefault("creado_en", {})
            query["creado_en"]["$lte"] = datetime.fromisoformat(fecha_hasta) + timedelta(days=1)
    except ValueError:
        raise HTTPException(status_code=422, detail="Fechas deben ser YYYY-MM-DD")

    total = col_estudios.count_documents(query)
    cursor = (
        col_estudios.find(query, {"fuentes": 0, "auditoria": 0})
        .sort("creado_en", -1)
        .skip(skip)
        .limit(limit)
    )
    items = []
    for doc in cursor:
        doc.pop("_id", None)
        doc["empresa_id"] = str(doc.get("empresa_id")) if doc.get("empresa_id") else None
        doc["usuario_id"] = str(doc.get("usuario_id")) if doc.get("usuario_id") else None
        items.append(doc)

    # Costo de cada consulta (lo que el cliente quiere ver en su historial):
    # suma de sus CONSUMOs − REEMBOLSOS (monto_cop ya viene CON SIGNO: consumo
    # +, reembolso −). Una sola aggregation con $in sobre los consulta_id de la
    # página (no N finds). Fallo de cálculo ≠ fallo del listado: sin costo.
    ids = [it["consulta_id"] for it in items if it.get("consulta_id")]
    costos: dict[str, int] = {}
    if ids:
        try:
            pipeline = [
                {"$match": {"consulta_id": {"$in": ids}, "tipo": {"$in": ["CONSUMO", "REEMBOLSO"]}}},
                {"$group": {"_id": "$consulta_id", "total_cop": {"$sum": "$monto_cop"}}},
            ]
            for fila in db["movimientos_cobro_seguridad"].aggregate(pipeline):
                costos[str(fila["_id"])] = int(fila.get("total_cop") or 0)
        except Exception as exc:
            logger.warning("Costos del historial no se pudieron calcular: %s", exc)
    for it in items:
        it["costo_cop"] = costos.get(it.get("consulta_id"), 0)

    # Fuentes PENDIENTES por consulta (2026-09-25): las que quedaron sin
    # respuesta (NO_DISPONIBLE/ERROR) — el historial del portal muestra la
    # cantidad y despliega el listado al hacer clic en la fila. El doc de la
    # página ya viene SIN `fuentes` (payload pesado): el cálculo va por
    # aggregation ($objectToArray sobre la clave dinámica), igual que los
    # costos: fallo ≠ fallo del listado.
    pendientes_por_id: dict[str, list[str]] = {}
    if ids:
        try:
            pipeline = [
                {"$match": {"consulta_id": {"$in": ids}}},
                {"$project": {
                    "consulta_id": 1,
                    "pendientes": {
                        "$map": {
                            "input": {
                                "$filter": {
                                    "input": {"$objectToArray": {"$ifNull": ["$fuentes", {}]}},
                                    "as": "f",
                                    "cond": {"$in": ["$$f.v.estado", ["NO_DISPONIBLE", "ERROR"]]},
                                }
                            },
                            "as": "f",
                            "in": "$$f.k",
                        }
                    },
                }},
            ]
            for fila in col_estudios.aggregate(pipeline):
                pendientes_por_id[str(fila.get("consulta_id"))] = list(fila.get("pendientes") or [])
        except Exception as exc:
            logger.warning("Fuentes pendientes del historial no se pudieron calcular: %s", exc)
    for it in items:
        it["fuentes_pendientes"] = pendientes_por_id.get(it.get("consulta_id"), [])

    return {"total": total, "items": items}


@router.get("/personas")
def buscar_persona_consultada(
    request: Request,
    actor: dict = Depends(actor_actual),
    cedula: str = Query(..., min_length=3, max_length=15, description="Cédula de la persona"),
    empresa_id: str | None = Query(None, description="Solo ADMIN_INTEGRA sin empresa propia"),
):
    """Memoria de personas consultadas por la empresa del actor (2026-09-25).

    Retorna los datos que la EMPRESA ya envió/verificó en consultas previas
    de esa cédula (nombres, apellidos, nombre verificado por los portales,
    fecha de expedición) para autollenar el formulario. Aislamiento: solo ve
    personas que SU empresa consultó antes; otra empresa → encontrada=false
    (no revela que un tercero la consultó).
    """
    _requiere_rol(actor, {ROL_CONSULTADOR, ROL_ADMIN_EMPRESA, ROL_ADMIN_INTEGRA}, "consultar personas")
    if actor["rol"] == ROL_ADMIN_INTEGRA and not actor.get("empresa_id"):
        if not (empresa_id or "").strip():
            raise HTTPException(status_code=422, detail="Indique empresa_id para consultar su memoria de personas")
        if not re.fullmatch(r"[0-9a-fA-F]{24}", empresa_id):
            raise HTTPException(status_code=422, detail="empresa_id inválido")
        eid = empresa_id
    else:
        eid = actor.get("empresa_id") or ""
    persona = personas.buscar_persona(_normalizar_cedula(cedula), eid)
    if not persona:
        return {"encontrada": False}
    ultima = persona.get("ultima_consulta_en")
    return {
        "encontrada": True,
        "nombres": persona.get("nombres") or "",
        "apellidos": persona.get("apellidos") or "",
        "nombre_consultado": persona.get("nombre_consultado") or "",
        "fecha_expedicion": persona.get("fecha_expedicion") or "",
        "total_consultas": persona.get("total_consultas") or 0,
        "ultima_consulta_en": ultima.isoformat() + "Z" if ultima else None,
    }


@router.get("/verificar/{consulta_id}")
def verificar_estudio(consulta_id: str, codigo: str = Query(..., min_length=4, max_length=20), request: Request = None):
    """Destino público del QR: valida consulta_id + código de verificación.

    Solo expone datos mínimos (estado, fecha, empresa, cédula enmascarada) y
    responde siempre 200 con valido:false para no permitir enumeración.
    """
    doc = col_estudios.find_one({"consulta_id": consulta_id.strip().upper()})
    valido = bool(doc) and doc.get("codigo_verificacion", "") == codigo.strip().upper()
    registrar_evento(
        "verificacion_qr",
        consulta_id=consulta_id,
        detalle="válido" if valido else "código incorrecto",
        request=request,
    )
    if not valido:
        return {"valido": False}
    etiquetas = {
        "manifiestos_rndc": "RNDC — Historial de viajes",
        "procuraduria": "Procuraduría — Antecedentes disciplinarios",
        "contraloria": "Contraloría — Antecedentes fiscales",
        "delitos_sexuales": "Policía — Inhabilidades Ley 1918 (delitos sexuales contra menores)",
        "policia": "Policía — Antecedentes judiciales",
        "runt": "RUNT — Información del vehículo",
        "simit": "SIMIT — Estado de cuenta de la placa",
        "sena": "SENA — Certificados de formación",
        "sisconmp": "SISCONMP — Capacitaciones Mercancías Peligrosas",
        "situacion_militar": "Ejército — Situación militar (libreta)",
        "ofac": "OFAC — Personas por cédula",
        "ofac_nit": "OFAC — Empresas por NIT",
        "onu_ue": "ONU/UE — Listas internacionales de sanciones",
        "bdme": "BDME — Persona por cédula",
        "bdme_nit": "BDME — Empresa por NIT",
        "rama_judicial": "Rama Judicial — Procesos por nombre",
        "rues": "RUES — Registro Mercantil por NIT",
    }
    fuentes = []
    for clave, nombre in etiquetas.items():
        estado_fuente = ((doc.get("fuentes") or {}).get(clave) or {}).get("estado")
        if estado_fuente and estado_fuente != "DESHABILITADA":
            fuentes.append({"codigo": clave, "nombre": nombre, "estado": estado_fuente})
    return {
        "valido": True,
        "consulta_id": doc.get("consulta_id"),
        "estado": doc.get("estado"),
        "fecha": doc.get("creado_en"),
        "empresa": doc.get("empresa_nombre"),
        "cedula": enmascarar_cedula(doc.get("cedula", "")),
        "fuentes": fuentes,
        "huella_documento": ((doc.get("pdf") or {}).get("sha256") or ""),
    }


@router.get("/{consulta_id}")
def detalle_estudio(consulta_id: str, request: Request, actor: dict = Depends(actor_actual)):
    """Doc completo del estudio (scope del actor)."""
    doc = _obtener_estudio(consulta_id, actor, request)
    registrar_evento("estudio_visto", actor=actor, consulta_id=consulta_id, request=request)
    return _respuesta_estudio(doc)


# === 6-8. PDFs =================================================================

@router.get("/{consulta_id}/pdf")
def descargar_pdf(
    consulta_id: str,
    request: Request,
    descarga: bool = Query(False, description="attachment en vez de inline"),
    url_firmada: bool = Query(False, description="URL firmada de 15 min en vez de stream"),
    actor: dict = Depends(actor_actual),
):
    """Informe PDF (stream desde GCS privado; cada descarga queda auditada)."""
    doc = _obtener_estudio(consulta_id, actor, request)
    if not doc.get("pdf"):
        raise HTTPException(status_code=404, detail="El estudio no tiene PDF generado")
    from Funciones import storage_seguridad

    registrar_evento(
        "pdf_descargado",
        actor=actor,
        consulta_id=consulta_id,
        detalle="stream" if not url_firmada else "url_firmada",
        request=request,
    )
    if url_firmada:
        return {"url": storage_seguridad.generar_url_firmada(doc["pdf"]["gcs_ruta"])}
    try:
        contenido = storage_seguridad.descargar_blob(doc["pdf"]["gcs_ruta"])
    except Exception as exc:
        logger.error("PDF %s no se pudo descargar de GCS: %s", consulta_id, exc)
        raise HTTPException(status_code=502, detail="No fue posible recuperar el PDF del almacenamiento")
    nombre = f"estudio_seguridad_{consulta_id}.pdf"
    headers = {
        "Content-Disposition": f"{'attachment' if descarga else 'inline'}; filename={nombre}"
    }
    return Response(content=contenido, media_type="application/pdf", headers=headers)


@router.get("/{consulta_id}/runt.pdf")
def descargar_anexo_runt(consulta_id: str, request: Request, actor: dict = Depends(actor_actual)):
    """Certificado/descarga oficial del RUNT si el portal llegó a entregarlo
    (hoy no genera PDF consolidado: el anexo queda listo por si cambia)."""
    doc = _obtener_estudio(consulta_id, actor, request)
    if not doc.get("anexo_runt"):
        raise HTTPException(status_code=404, detail="El estudio no tiene anexo del RUNT")
    from Funciones import storage_seguridad

    registrar_evento("pdf_descargado", actor=actor, consulta_id=consulta_id, detalle="anexo runt", request=request)
    try:
        contenido = storage_seguridad.descargar_blob(doc["anexo_runt"]["gcs_ruta"])
    except Exception as exc:
        logger.error("Anexo runt %s no se pudo descargar de GCS: %s", consulta_id, exc)
        raise HTTPException(status_code=502, detail="No fue posible recuperar el anexo del almacenamiento")
    return Response(
        content=contenido,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename=consulta_runt_{consulta_id}.pdf'},
    )


@router.post("/{consulta_id}/pdf/regenerar")
def regenerar_pdf(consulta_id: str, request: Request, actor: dict = Depends(actor_actual)):
    """Reconstruye el PDF desde el doc persistido (SIN consultar portales)."""
    _requiere_rol(actor, {ROL_ADMIN_EMPRESA, ROL_ADMIN_INTEGRA}, "regenerar PDFs")
    doc = _obtener_estudio(consulta_id, actor, request)
    if doc.get("estado") == "EN_PROGRESO":
        raise HTTPException(status_code=409, detail="El estudio sigue en progreso")

    from Funciones import storage_seguridad
    from Funciones.pdf_estudio_seguridad import generar_pdf_estudio

    empresa = col_empresas.find_one({"_id": ObjectId(doc["empresa_id"])}) if doc.get("empresa_id") else None
    contenido = generar_pdf_estudio(doc, empresa)
    anterior = doc.get("pdf") or {}
    ruta = anterior.get("gcs_ruta") or storage_seguridad.ruta_blob(str(doc["empresa_id"]), doc["creado_en"].year, consulta_id)
    subido = storage_seguridad.subir_pdf(contenido, ruta, doc.get("cedula", ""))
    version = int(anterior.get("version", 0)) + 1
    historial = list(anterior.get("historial") or [])
    if anterior.get("sha256"):
        historial.append({
            "version": anterior.get("version", 1),
            "sha256": anterior["sha256"],
            "generado_en": anterior.get("generado_en"),
            "por_usuario": anterior.get("por_usuario", doc.get("usuario")),
        })
    info_pdf = {
        **subido,
        "version": version,
        "generado_en": _utcnow(),
        "regeneraciones": int(anterior.get("regeneraciones", 0)) + 1,
        "historial": historial,
        "por_usuario": actor["usuario"],
    }
    col_estudios.update_one({"consulta_id": consulta_id}, {"$set": {"pdf": info_pdf}})
    registrar_evento(
        "pdf_regenerado",
        actor=actor,
        consulta_id=consulta_id,
        detalle=f"v{version}",
        request=request,
    )
    return {"consulta_id": consulta_id, "version": version, "sha256": subido["sha256"], "generado_en": info_pdf["generado_en"]}


def _reemitir_pdf(doc: dict, empresa: dict | None, actor: dict, request: Request | None = None) -> dict:
    """Regenera y sube el PDF del estudio SOBRE la misma ruta de GCS (versión
    +1 con historial). Usado por el completado de fuentes pendientes: el
    informe final reemplaza al parcial dejando rastro de versiones."""
    from Funciones import storage_seguridad
    from Funciones.pdf_estudio_seguridad import generar_pdf_estudio

    consulta_id = doc["consulta_id"]
    contenido = generar_pdf_estudio(doc, empresa)
    anterior = doc.get("pdf") or {}
    ruta = anterior.get("gcs_ruta") or storage_seguridad.ruta_blob(str(doc["empresa_id"]), doc["creado_en"].year, consulta_id)
    subido = storage_seguridad.subir_pdf(contenido, ruta, doc.get("cedula", ""))
    version = int(anterior.get("version", 0)) + 1
    historial = list(anterior.get("historial") or [])
    if anterior.get("sha256"):
        historial.append({
            "version": anterior.get("version", 1),
            "sha256": anterior["sha256"],
            "generado_en": anterior.get("generado_en"),
            "por_usuario": anterior.get("por_usuario", doc.get("usuario")),
        })
    info_pdf = {
        **subido,
        "version": version,
        "generado_en": _utcnow(),
        "regeneraciones": int(anterior.get("regeneraciones", 0)) + 1,
        "historial": historial,
        "por_usuario": actor["usuario"],
    }
    col_estudios.update_one({"consulta_id": consulta_id}, {"$set": {"pdf": info_pdf}})
    registrar_evento(
        "pdf_regenerado",
        actor=actor,
        consulta_id=consulta_id,
        detalle=f"v{version} (completado de fuentes pendientes)",
        request=request,
    )
    return info_pdf


MAX_COMPLETAR_INTENTOS = 5


def _consumos_del_completado(
    doc: dict, empresa: dict, actor_doc: dict, actor: dict, request: Request | None = None,
) -> list[dict]:
    """Cobro del completado de fuentes pendientes (decisión del usuario
    2026-09-25): una consulta REEMBOLSADA (>51% de fuentes fallidas al crear)
    que se completa estaba entregando el estudio completo SIN costo → al
    completarla se cobra de nuevo el plan (misma reserva atómica que una
    consulta nueva, con el plan de los CONSUMO originales como preferido).

    Las consultas ya cobradas (≤51% caídas → PARCIAL cobrado) completan
    GRATIS: esas fuentes ya se pagaron. Devuelve [] en ese caso.
    """
    from Funciones import cobro_seguridad as cobro

    consulta_id = doc["consulta_id"]
    col_mov = db["movimientos_cobro_seguridad"]
    reembolsada = col_mov.find_one(
        {"consulta_id": consulta_id, "tipo": "REEMBOLSO"}, {"_id": 1}
    ) is not None
    if not reembolsada:
        return []

    empresa = cobro.sincronizar_fuentes_planes(empresa, db["planes_seguridad"], col_empresas)
    # Fuentes que la consulta original cobró: todas las que NO fueron
    # decisión del plan (DESHABILITADA no se cobró).
    fuentes_originales = [
        nombre for nombre in FUENTES
        if isinstance((doc.get("fuentes") or {}).get(nombre), dict)
        and doc["fuentes"][nombre].get("estado") != "DESHABILITADA"
    ]
    # Plan preferido: el de los CONSUMO originales si fueron de UN solo plan
    # (reproduce la elección del usuario); multi-plan → FIFO como en la
    # creación sin plan_id.
    planes_originales = {
        str(m.get("plan_id"))
        for m in col_mov.find(
            {"consulta_id": consulta_id, "tipo": "CONSUMO", "plan_id": {"$ne": None}},
            {"plan_id": 1},
        )
    }
    plan_preferido = ObjectId(next(iter(planes_originales))) if len(planes_originales) == 1 else None
    consumos = cobro.reservar_consumos(
        empresa, actor_doc, consulta_id, fuentes_originales,
        plan_preferido_id=plan_preferido,
    )
    for consumo in consumos:
        if consumo.get("monto_cop", 0) != 0:
            registrar_evento(
                "consumo_registrado",
                actor=actor,
                consulta_id=consulta_id,
                fuente=consumo.get("fuente"),
                detalle=f"Completado · {empresa.get('nombre')} · {consumo.get('plan_nombre')} · "
                        f"${consumo.get('precio_unitario_cop', 0)}",
                request=request,
            )
    return consumos


@router.post("/{consulta_id}/completar")
async def completar_estudio(consulta_id: str, request: Request, actor: dict = Depends(actor_actual)):
    """Vuelve a consultar SOLO las fuentes que no respondieron del estudio y
    re-emite el PDF (mismo consulta_id, versión +1) — completar el informe,
    no una consulta nueva (pedido 2026-09-25). SIN consumo de cobro: la
    consulta original ya se cobró (o se reembolsó si falló la mayoría).
    """
    _requiere_rol(actor, {ROL_CONSULTADOR, ROL_ADMIN_EMPRESA, ROL_ADMIN_INTEGRA}, "completar estudios")
    _verificar_rate_limit(actor)
    doc = _obtener_estudio(consulta_id, actor, request)
    if doc.get("estado") == "EN_PROGRESO":
        raise HTTPException(status_code=409, detail="El estudio sigue en progreso")
    pendientes = fuentes_pendientes_estudio(doc)
    if not pendientes:
        raise HTTPException(
            status_code=422,
            detail="El estudio no tiene fuentes pendientes: todas las fuentes consultadas respondieron",
        )
    if int(doc.get("completar_intentos") or 0) >= MAX_COMPLETAR_INTENTOS:
        raise HTTPException(
            status_code=429,
            detail="Este estudio agotó los reintentos de completado. Genere una consulta nueva.",
        )

    empresa = col_empresas.find_one({"_id": ObjectId(doc["empresa_id"])}) if doc.get("empresa_id") else {}
    actor_doc = {**actor, "empresa_id": str(doc["empresa_id"])}

    # Cobro (solo si la consulta fue REEMBOLSADA; ver helper): 402 accionable
    # ANTES de tocar portales si ya no hay cupo.
    from Funciones import cobro_seguridad as cobro

    consumos = _consumos_del_completado(doc, empresa, actor_doc, actor, request)

    try:
        estudio, corridas = await reintentar_fuentes_estudio(
            consulta_id=consulta_id,
            actor=actor_doc,
            empresa=empresa or {},
            registrar_evento=lambda *a, **k: registrar_evento(*a, request=request, **k),
        )
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except Exception as exc:
        logger.exception("Completado del estudio %s falló de forma inesperada", consulta_id)
        raise HTTPException(status_code=502, detail=f"El completado falló: {str(exc)[:200]}")

    # Reembolso con el MISMO criterio de la creación (>51% de las fuentes
    # corridas fallidas): si el completado tampoco logró entregar, la consulta
    # vuelve a quedar sin costo. Solo toca los consumos recién hechos (los
    # originales ya tienen `reembolsado: True`).
    cobrado = bool(consumos)
    if consumos and mayoria_fuentes_fallidas(estudio.get("fuentes") or {}):
        try:
            cobro.reembolsar_consumos_consulta(
                consulta_id, empresa, actor_doc,
                motivo="Completado: seguía fallando la mayoría de las fuentes",
                automatico=True,
            )
            registrar_evento(
                "reembolso", actor=actor, consulta_id=consulta_id,
                detalle="automático tras completado (mayoría de fuentes fallidas)",
                request=request,
            )
            cobrado = False
        except Exception as exc:
            logger.error("Reembolso post-completado de %s falló: %s", consulta_id, exc)

    # Memoria de personas: las fuentes recién corridas pueden traer el nombre
    # VERIFICADO (situacion_militar/sisconmp) que actualiza la memoria. Best-effort.
    if estudio.get("cedula"):
        try:
            nombres_verificados, apellidos_verificados = _nombres_verificados_del_estudio(estudio)
            if nombres_verificados or apellidos_verificados:
                personas.registrar_consulta_persona(
                    estudio["cedula"], str(doc["empresa_id"]),
                    nombres=nombres_verificados or None,
                    apellidos=apellidos_verificados or None,
                    nombre_consultado=estudio.get("nombre_consultado") or None,
                    fecha_expedicion=estudio.get("fecha_expedicion"),
                )
        except Exception as exc:
            logger.warning("Memoria de personas (%s) no actualizada al completar: %s", consulta_id, exc)

    # Re-emitir el PDF consolidado (mismo estudio, versión +1).
    try:
        info_pdf = _reemitir_pdf(estudio, empresa, actor, request)
        estudio["pdf"] = info_pdf
    except Exception as exc:
        logger.error("PDF del completado %s no se pudo generar/subir: %s", consulta_id, exc)
        registrar_evento(
            "pdf_generado", actor=actor, consulta_id=consulta_id,
            detalle=f"ERROR tras completado: {str(exc)[:200]}", request=request,
        )

    estudio = _respuesta_estudio(estudio)
    estudio["pdf_endpoint"] = f"/seguridad/estudios/{consulta_id}/pdf"
    estudio["fuentes_completadas"] = corridas
    estudio["fuentes_pendientes"] = fuentes_pendientes_estudio({"fuentes": estudio.get("fuentes") or {}})
    estudio["cobrado"] = cobrado
    return estudio


# === 10-11. Estadísticas y eventos =============================================

@router.get("/estadisticas/estudios")
def estadisticas_estudios(
    request: Request,
    actor: dict = Depends(actor_actual),
    desde: str | None = Query(None, description="YYYY-MM-DD"),
    hasta: str | None = Query(None, description="YYYY-MM-DD"),
    empresa_id: str | None = Query(None, description="Solo ADMIN_INTEGRA"),
):
    _requiere_rol(actor, {ROL_ADMIN_EMPRESA, ROL_ADMIN_INTEGRA}, "ver estadísticas")
    match: dict = _filtro_empresa(actor)
    if actor["rol"] == ROL_ADMIN_INTEGRA and empresa_id:
        match = {"empresa_id": ObjectId(empresa_id)}
    try:
        if desde:
            match["creado_en"] = {"$gte": datetime.fromisoformat(desde)}
        if hasta:
            match.setdefault("creado_en", {})
            match["creado_en"]["$lte"] = datetime.fromisoformat(hasta) + timedelta(days=1)
    except ValueError:
        raise HTTPException(status_code=422, detail="Fechas deben ser YYYY-MM-DD")

    pipeline = [
        {"$match": match},
        {"$group": {
            "_id": None,
            "total": {"$sum": 1},
            "por_estado": {"$push": "$estado"},
            "usuarios": {"$push": {"usuario": "$usuario", "nombre": "$usuario_nombre"}},
            "duracion": {"$avg": "$duracion_s"},
            "cache_hits": {"$sum": {"$cond": [
                {"$or": [
                    {"$eq": ["$fuentes.manifiestos_rndc.origen", "cache"]},
                    {"$eq": ["$fuentes.procuraduria.origen", "cache"]},
                    {"$eq": ["$fuentes.policia.origen", "cache"]},
                    {"$eq": ["$fuentes.runt.origen", "cache"]},
                    {"$eq": ["$fuentes.simit.origen", "cache"]},
                    {"$eq": ["$fuentes.sena.origen", "cache"]},
                    {"$eq": ["$fuentes.sisconmp.origen", "cache"]},
                    {"$eq": ["$fuentes.situacion_militar.origen", "cache"]},
                ]},
                1, 0,
            ]}},
        }},
    ]
    try:
        fila = next(col_estudios.aggregate(pipeline), None) or {}
    except Exception as exc:
        logger.error("Estadísticas fallaron: %s", exc)
        raise HTTPException(status_code=502, detail="No fue posible calcular estadísticas")

    por_estado: dict[str, int] = {}
    for estado in fila.get("por_estado", []):
        por_estado[estado] = por_estado.get(estado, 0) + 1
    por_usuario: dict[str, int] = {}
    for u in fila.get("usuarios", []):
        clave = f"{u.get('nombre', '')} ({u.get('usuario', '')})"
        por_usuario[clave] = por_usuario.get(clave, 0) + 1
    return {
        "total_estudios": fila.get("total", 0),
        "por_estado": por_estado,
        "por_usuario": por_usuario,
        "tiempo_promedio_s": round(fila.get("duracion") or 0, 1),
        "con_cache": fila.get("cache_hits", 0),
    }


@router.get("/eventos/auditoria")
def listar_eventos(
    request: Request,
    actor: dict = Depends(actor_actual),
    consulta_id: str | None = None,
    evento: str | None = None,
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
):
    """Log de eventos/auditoría (ADMIN_INTEGRA global, ADMIN_EMPRESA su tenant)."""
    _requiere_rol(actor, {ROL_ADMIN_EMPRESA, ROL_ADMIN_INTEGRA}, "ver eventos de auditoría")
    query: dict = _filtro_empresa(actor)
    if consulta_id:
        query["consulta_id"] = consulta_id.strip().upper()
    if evento:
        query["evento"] = evento
    total = col_eventos.count_documents(query)
    items = []
    for doc in col_eventos.find(query).sort("creado_en", -1).skip(skip).limit(limit):
        doc.pop("_id", None)
        for campo in ("empresa_id", "usuario_id"):
            doc[campo] = str(doc[campo]) if doc.get(campo) else None
        items.append(doc)
    return {"total": total, "items": items}


# === 12-13. Administración (solo ADMIN_INTEGRA) ================================

admin_router = APIRouter(prefix="/seguridad/admin", tags=["Seguridad - Admin"])


def _requiere_admin_integra(actor: dict = Depends(actor_actual)) -> dict:
    if actor["rol"] != ROL_ADMIN_INTEGRA:
        raise HTTPException(status_code=403, detail="Solo ADMIN_INTEGRA puede administrar el módulo")
    return actor


class EmpresaCrear(BaseModel):
    nombre: str
    nit: str | None = None
    slug: str | None = None
    logo_url: str | None = None
    config: dict | None = None


class EmpresaActualizar(BaseModel):
    nombre: str | None = None
    nit: str | None = None
    logo_url: str | None = None
    activo: bool | None = None
    config: dict | None = None


CONFIG_DEFAULT_EMPRESA = {
    "retencion_dias": 730,
    "aislamiento_usuario": False,
    "consultas_por_minuto": 10,
    # ⚠️ SEMÁNTICA 2026-09-01 (ver `fuentes_habilitadas_efectivas` del
    # orquestador): EL PLAN ES EL GATE — las fuentes default (portales
    # públicos) corren para TODA empresa sin tocar config, y editar un plan
    # se refleja inmediatamente en todos los clientes. Este listado SOLO
    # persiste las opt-in (policia: autoconsulta del titular, Decreto 019 de
    # 2012 — requiere autorización documentada del titular, Ley 1581) y
    # documenta el estado inicial; para apagar una fuente puntual por empresa
    # usar `config.fuentes_excluidas`.
    "fuentes_habilitadas": ["manifiestos_rndc", "procuraduria", "contraloria", "delitos_sexuales", "runt", "simit", "sena", "sisconmp", "ofac", "ofac_nit", "onu_ue", "bdme", "bdme_nit", "rama_judicial", "situacion_militar"],
}


def _respuesta_empresa(doc: dict) -> dict:
    doc.pop("_id", None)
    return doc


@admin_router.get("/empresas")
def listar_empresas(actor: dict = Depends(_requiere_admin_integra)):
    empresas = []
    for doc in col_empresas.find().sort("nombre", 1):
        doc["id"] = str(doc.pop("_id"))
        empresas.append(doc)
    return {"total": len(empresas), "items": empresas}


@admin_router.post("/empresas", status_code=201)
def crear_empresa(datos: EmpresaCrear, request: Request, actor: dict = Depends(_requiere_admin_integra)):
    nombre = datos.nombre.strip().upper()
    if len(nombre) < 3:
        raise HTTPException(status_code=422, detail="El nombre debe tener al menos 3 caracteres")
    slug = (datos.slug or re.sub(r"[^a-z0-9]+", "-", nombre.lower())).strip("-").lower()
    if not slug:
        raise HTTPException(status_code=422, detail="No se pudo derivar un slug válido")
    for campo, valor in (("nombre", nombre), ("slug", slug)):
        if col_empresas.find_one({campo: valor}):
            raise HTTPException(status_code=409, detail=f"Ya existe una empresa con ese {campo}")
    config = {**CONFIG_DEFAULT_EMPRESA, **(datos.config or {})}
    ahora = _utcnow()
    doc = {
        "nombre": nombre, "slug": slug, "nit": datos.nit, "logo_url": datos.logo_url,
        "activo": True, "config": config, "creado_en": ahora, "actualizado_en": ahora,
    }
    resultado = col_empresas.insert_one(doc)
    registrar_evento("empresa_creada", actor=actor, detalle=f"{nombre} ({slug})", request=request)
    return {"id": str(resultado.inserted_id), **_respuesta_empresa(doc)}


@admin_router.patch("/empresas/{empresa_id}")
def actualizar_empresa(empresa_id: str, datos: EmpresaActualizar, request: Request, actor: dict = Depends(_requiere_admin_integra)):
    if not re.fullmatch(r"[0-9a-fA-F]{24}", empresa_id):
        raise HTTPException(status_code=422, detail="empresa_id inválido")
    doc = col_empresas.find_one({"_id": ObjectId(empresa_id)})
    if not doc:
        raise HTTPException(status_code=404, detail="Empresa no encontrada")
    cambios: dict = {}
    if datos.nombre is not None:
        nombre = datos.nombre.strip().upper()
        if len(nombre) < 3:
            raise HTTPException(status_code=422, detail="El nombre debe tener al menos 3 caracteres")
        if col_empresas.find_one({"nombre": nombre, "_id": {"$ne": doc["_id"]}}):
            raise HTTPException(status_code=409, detail="Ya existe una empresa con ese nombre")
        cambios["nombre"] = nombre
    if datos.nit is not None:
        cambios["nit"] = datos.nit
    if datos.logo_url is not None:
        cambios["logo_url"] = datos.logo_url
    if datos.activo is not None:
        cambios["activo"] = bool(datos.activo)
    if datos.config is not None:
        cambios["config"] = {**CONFIG_DEFAULT_EMPRESA, **(doc.get("config") or {}), **datos.config}
    if not cambios:
        raise HTTPException(status_code=422, detail="Nada que actualizar")
    cambios["actualizado_en"] = _utcnow()
    col_empresas.update_one({"_id": doc["_id"]}, {"$set": cambios})
    registrar_evento(
        "empresa_actualizada",
        actor=actor,
        detalle=f"{doc.get('nombre')} → {cambios}",
        request=request,
    )
    actualizada = col_empresas.find_one({"_id": doc["_id"]})
    actualizada["id"] = str(actualizada.pop("_id"))
    return actualizada


@admin_router.get("/usuarios")
def listar_usuarios_modulo(
    actor: dict = Depends(_requiere_admin_integra),
    empresa_id: str | None = Query(None, description="Filtrar por empresa asignada"),
    perfil: str | None = Query(None, description="Filtrar por perfil baseusuarios"),
):
    """Usuarios baseusuarios con su asignación de empresa/rol del módulo."""
    query: dict = {}
    if empresa_id:
        if not re.fullmatch(r"[0-9a-fA-F]{24}", empresa_id):
            raise HTTPException(status_code=422, detail="empresa_id inválido")
        query["empresa_id"] = ObjectId(empresa_id)
    if perfil:
        query["perfil"] = perfil.upper()
    else:
        query["perfil"] = {"$in": ["SEGURIDAD", "CLIENTE_ESTUDIOS", "ADMIN"]}
    usuarios = []
    for doc in col_usuarios.find(query).sort("usuario", 1):
        usuarios.append({
            "id": str(doc["_id"]),
            "usuario": doc.get("usuario", ""),
            "nombre": doc.get("nombre", ""),
            "correo": doc.get("correo", ""),
            "perfil": doc.get("perfil", ""),
            "activo": doc.get("activo", True),
            "empresa_id": str(doc["empresa_id"]) if doc.get("empresa_id") else None,
            "rol_seguridad": doc.get("rol_seguridad"),
        })
    return {"total": len(usuarios), "items": usuarios}


class AsignarEmpresa(BaseModel):
    empresa_id: str | None = None   # None → desasignar
    rol_seguridad: str | None = None


@admin_router.patch("/usuarios/{usuario_id}/empresa")
def asignar_empresa_usuario(
    usuario_id: str,
    datos: AsignarEmpresa,
    request: Request,
    actor: dict = Depends(_requiere_admin_integra),
):
    """Asigna/quita empresa y rol del módulo a un usuario baseusuarios."""
    if not re.fullmatch(r"[0-9a-fA-F]{24}", usuario_id):
        raise HTTPException(status_code=422, detail="usuario_id inválido")
    usuario = col_usuarios.find_one({"_id": ObjectId(usuario_id)})
    if not usuario:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    empresa_obj_id = None
    if datos.empresa_id:
        if not re.fullmatch(r"[0-9a-fA-F]{24}", datos.empresa_id):
            raise HTTPException(status_code=422, detail="empresa_id inválido")
        empresa = col_empresas.find_one({"_id": ObjectId(datos.empresa_id)})
        if not empresa:
            raise HTTPException(status_code=404, detail="Empresa no encontrada")
        empresa_obj_id = empresa["_id"]

    rol = datos.rol_seguridad
    if rol is not None:
        rol = rol.strip().upper()
        if rol and rol not in {ROL_ADMIN_EMPRESA, ROL_CONSULTADOR, ROL_ADMIN_INTEGRA}:
            raise HTTPException(status_code=422, detail=f"Rol inválido: {rol}")

    perfil = str(usuario.get("perfil") or "").upper()
    if perfil not in {"SEGURIDAD", "CLIENTE_ESTUDIOS", "ADMIN"}:
        raise HTTPException(
            status_code=422,
            detail=f"El perfil {perfil} no participa del módulo de seguridad",
        )
    if perfil in ("SEGURIDAD", "CLIENTE_ESTUDIOS") and not empresa_obj_id:
        raise HTTPException(
            status_code=422,
            detail=f"Un usuario {perfil} requiere empresa asignada (no se puede desasignar)",
        )

    cambios = {}
    if datos.empresa_id is not None or empresa_obj_id:
        cambios["empresa_id"] = empresa_obj_id
    if rol is not None:
        cambios["rol_seguridad"] = rol or None
    if not cambios:
        raise HTTPException(status_code=422, detail="Nada que actualizar")
    col_usuarios.update_one({"_id": usuario["_id"]}, {"$set": cambios})
    registrar_evento(
        "usuario_asignado",
        actor=actor,
        detalle=f"{usuario.get('usuario')} → empresa {datos.empresa_id} rol {rol}",
        request=request,
    )
    actualizado = col_usuarios.find_one({"_id": usuario["_id"]})
    return {
        "id": str(actualizado["_id"]),
        "usuario": actualizado.get("usuario", ""),
        "empresa_id": str(actualizado["empresa_id"]) if actualizado.get("empresa_id") else None,
        "rol_seguridad": actualizado.get("rol_seguridad"),
    }

