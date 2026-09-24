# ============================================================
# TUSDATOS.CO — API puente (wrapper) — SIN auth, experimental
# ============================================================
# Expone localmente la API comercial de TusDatos.co (validación
# de antecedentes de personas, empresas y vehículos) para ponerla
# a prueba desde /docs. Es INDEPENDIENTE del módulo /seguridad:
# no comparte colecciones, cobro ni auditoría; si alguna fuente
# de aquí resulta útil, ese módulo la implementa con sus bots.
#
# Documentación oficial: https://docs.tusdatos.co/redoc
# (spec completo copiado en TUSDATOS/referencia/openapi_tusdatos.json)
#
# Auth upstream: HTTP Basic (email:password de la cuenta de
# dash-board.tusdatos.co) leído de las variables de entorno
# TUSDATOS_USER / TUSDATOS_PASS (integrappi/.env).
#
# Flujo de una consulta en TusDatos:
#   1. POST /api/launch            -> jobid (vigencia 2 horas)
#   2. GET  /api/results/{jobid}   -> estado: procesando|finalizado|error
#      (tarda ~1 min en promedio; aquí los endpoints ".../completa"
#       hacen el polling automáticamente)
#   3. GET  /api/v2/report*/{id}   -> reporte html/pdf/json; el `id`
#      aparece en los resultados SOLO cuando la consulta finaliza
#
# Endpoints (prefijo /tusdatos):
#   GET    /health                          → credenciales + conectividad
#   POST   /consultas/lanzar                → inicia consulta, devuelve jobid
#   GET    /consultas/{jobid}               → estado/resultado del sondeo
#   POST   /consultas/completa              → lanza + espera el final
#   POST   /vehiculos/completa              → consulta de vehículo + espera
#   POST   /nit/verificar                   → validación de empresa por NIT
#   GET    /reportes/{id}/json|html|pdf|pdf-nit   → reportes por id
#   GET    /reportes/{id}/reintentar        → relanza fuentes en error
#   GET    /plan · /historial               → plan/consumo de la cuenta
#   POST   /token                           → API token Bearer (api_td_...)
#   ANY    /proxy/{ruta}                    → passthrough crudo (batches,
#                                             webhooks, endpoints nuevos)
# ============================================================
import asyncio
import logging
import os
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

ruta_tusdatos = APIRouter(prefix="/tusdatos", tags=["TusDatos"])

TUSDATOS_BASE_URL = os.getenv("TUSDATOS_BASE_URL", "https://dash-board.tusdatos.co").rstrip("/")
TUSDATOS_USER = os.getenv("TUSDATOS_USER", "")
TUSDATOS_PASS = os.getenv("TUSDATOS_PASS", "")
TUSDATOS_TIMEOUT_LECTURA = 180.0  # las consultas de antecedentes tardan ~1 min

_cliente: Optional[httpx.AsyncClient] = None


def _configurado() -> bool:
    return bool(TUSDATOS_USER and TUSDATOS_PASS)


async def _obtener_cliente() -> httpx.AsyncClient:
    global _cliente
    if _cliente is None or _cliente.is_closed:
        if not _configurado():
            raise HTTPException(
                status_code=503,
                detail="Faltan credenciales de TusDatos: define TUSDATOS_USER y "
                "TUSDATOS_PASS en integrappi/.env y reinicia.",
            )
        _cliente = httpx.AsyncClient(
            auth=(TUSDATOS_USER, TUSDATOS_PASS),
            base_url=TUSDATOS_BASE_URL,
            timeout=httpx.Timeout(30.0, read=TUSDATOS_TIMEOUT_LECTURA),
            # Los endpoints reales responden 308 hacia la misma ruta con
            # barra final (/api/plans -> /api/plans/); sin esto llega HTML.
            follow_redirects=True,
        )
    return _cliente


async def _pedido(metodo: str, ruta: str, **kw) -> httpx.Response:
    cliente = await _obtener_cliente()
    respuesta = await cliente.request(metodo, ruta, **kw)
    if respuesta.status_code >= 400:
        try:
            detalle = respuesta.json()
        except Exception:
            detalle = respuesta.text[:500]
        raise HTTPException(status_code=respuesta.status_code, detail=detalle)
    return respuesta


async def _pedido_json(metodo: str, ruta: str, **kw) -> dict:
    return (await _pedido(metodo, ruta, **kw)).json()


# --- Modelos ------------------------------------------------

TIPOS_DOCUMENTO = ("CC", "CE", "INT", "NIT", "PP", "PPT", "NOMBRE")


class ConsultaIn(BaseModel):
    """Espeja LaunchIn de TusDatos (POST /api/launch)."""
    typedoc: str = Field(description="CC, CE, INT, NIT, PP, PPT o NOMBRE")
    doc: Optional[str] = Field(default=None, description="Documento sin puntos ni comas; omitir si es por nombre")
    nombre: Optional[str] = Field(default=None, description="Nombre de la persona o empresa (consultas por NOMBRE)")
    fechaE: Optional[str] = Field(default=None, description="Fecha de expedición dd/mm/yyyy; obligatoria para CE y PPT")
    force: bool = Field(default=False, description="True para relanzar una cédula consultada previamente")


class ConsultaCompletaIn(ConsultaIn):
    esperar_maximo: int = Field(default=300, ge=10, le=3500, description="Segundos máximos de espera")
    intervalo: int = Field(default=5, ge=2, le=60, description="Segundos entre sondeos")


class VehiculoCompletaIn(BaseModel):
    """Espeja LaunchCarIn de TusDatos (POST /api/launch/car)."""
    placa: str = Field(min_length=6, max_length=6, description="Placa del vehículo")
    doc: int = Field(description="Documento del propietario, sin puntos ni comas")
    tdoc: str = Field(default="CC", description="CC, CE, NIT o TI")
    esperar_maximo: int = Field(default=300, ge=10, le=3500)
    intervalo: int = Field(default=5, ge=2, le=60)


class NitIn(BaseModel):
    """POST /api/launch/verify/nit — validación de empresa."""
    nit: int = Field(description="NIT de la empresa (solo números)")
    nombre: Optional[str] = Field(default=None, description="Nombre de la empresa (opcional)")


# --- Helpers ------------------------------------------------

def _limpiar_payload(consulta: ConsultaIn) -> dict:
    payload = consulta.model_dump(exclude_none=True)
    if payload.get("doc") is not None:
        payload["doc"] = str(payload["doc"]).replace(".", "").replace(",", "")
    if payload.get("typedoc") not in TIPOS_DOCUMENTO:
        raise ValueError(f"typedoc debe ser uno de {TIPOS_DOCUMENTO}")
    if payload["typedoc"] == "NOMBRE" and not payload.get("nombre"):
        raise ValueError("typedoc=NOMBRE requiere el campo 'nombre'")
    return payload


async def _esperar(jobid: str, esperar_maximo: int, intervalo: int) -> dict:
    """Sondea /api/results hasta estado final o vencimiento del tiempo."""
    transcurrido = 0
    while True:
        resultado = await _pedido_json("GET", f"/api/results/{jobid}")
        estado = str(resultado.get("estado", "")).lower()
        if estado in ("finalizado", "error"):
            return resultado
        if transcurrido >= esperar_maximo:
            resultado["_aviso"] = (
                f"Se agotó el tiempo de espera ({esperar_maximo}s); el jobid sigue "
                "vigente 2 h, vuelve a sondear /tusdatos/consultas/{jobid}"
            )
            return resultado
        await asyncio.sleep(intervalo)
        transcurrido += intervalo


# --- Infraestructura ----------------------------------------

@ruta_tusdatos.get("/health")
async def health():
    """Verifica credenciales y conectividad consultando el plan de la cuenta."""
    if not _configurado():
        return JSONResponse(
            status_code=503,
            content={"ok": False, "detalle": "Faltan credenciales: define TUSDATOS_USER "
                     "y TUSDATOS_PASS en integrappi/.env y reinicia."},
        )
    try:
        plan = await _pedido_json("GET", "/api/plans", params={"exclude": "checks"})
        return {"ok": True, "base_url": TUSDATOS_BASE_URL, "plan": plan}
    except HTTPException as exc:
        return JSONResponse(status_code=502, content={"ok": False, "detalle": exc.detail})
    except Exception as exc:  # red caída, DNS, timeout...
        return JSONResponse(status_code=502, content={"ok": False, "detalle": str(exc)})


# --- Consultas (personas) -----------------------------------

@ruta_tusdatos.post("/consultas/lanzar")
async def lanzar_consulta(consulta: ConsultaIn):
    """POST /api/launch — inicia la consulta y devuelve el `jobid`."""
    try:
        payload = _limpiar_payload(consulta)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    return await _pedido_json("POST", "/api/launch", json=payload)


@ruta_tusdatos.get("/consultas/{jobid}")
async def resultados_consulta(jobid: str):
    """GET /api/results/{jobkey} — estado (procesando/finalizado/error) y hallazgos."""
    return await _pedido_json("GET", f"/api/results/{jobid}")


@ruta_tusdatos.post("/consultas/completa")
async def consulta_completa(consulta: ConsultaCompletaIn):
    """Lanza la consulta y espera el resultado final en una sola llamada."""
    try:
        payload = _limpiar_payload(consulta)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    lanzamiento = await _pedido_json("POST", "/api/launch", json=payload)
    jobid = lanzamiento.get("jobid")
    if not jobid:
        raise HTTPException(status_code=502, detail={"detalle": "TusDatos no entregó jobid",
                                                     "lanzamiento": lanzamiento})
    resultado = await _esperar(jobid, consulta.esperar_maximo, consulta.intervalo)
    return {"lanzamiento": lanzamiento, "resultado": resultado}


# --- Vehículos ----------------------------------------------

@ruta_tusdatos.post("/vehiculos/completa")
async def consulta_vehiculo(vehiculo: VehiculoCompletaIn):
    """POST /api/launch/car (doc del propietario) + espera del resultado."""
    lanzamiento = await _pedido_json(
        "POST", "/api/launch/car",
        json={"doc": vehiculo.doc, "tdoc": vehiculo.tdoc, "placa": vehiculo.placa.upper()},
    )
    jobid = lanzamiento.get("jobid")
    if not jobid:
        raise HTTPException(status_code=502, detail={"detalle": "TusDatos no entregó jobid",
                                                     "lanzamiento": lanzamiento})
    resultado = await _esperar(jobid, vehiculo.esperar_maximo, vehiculo.intervalo)
    return {"lanzamiento": lanzamiento, "resultado": resultado}


# --- Empresas (NIT) -----------------------------------------

@ruta_tusdatos.post("/nit/verificar")
async def verificar_nit(nit: NitIn):
    """POST /api/launch/verify/nit — validación de empresa por NIT."""
    payload = {"nit": nit.nit}
    if nit.nombre:
        payload["nombre"] = nit.nombre
    return await _pedido_json("POST", "/api/launch/verify/nit", json=payload)


# --- Reportes -----------------------------------------------

@ruta_tusdatos.get("/reportes/{id_reporte}/json")
async def reporte_json(id_reporte: str):
    """JSON completo del reporte (toda la información de las fuentes)."""
    return await _pedido_json("GET", f"/api/report_json/{id_reporte}")


@ruta_tusdatos.get("/reportes/{id_reporte}/html")
async def reporte_html(id_reporte: str):
    """HTML del reporte (se abre directo en el navegador)."""
    respuesta = await _pedido("GET", f"/api/v2/report/{id_reporte}")
    return Response(content=respuesta.content, media_type="text/html")


@ruta_tusdatos.get("/reportes/{id_reporte}/pdf")
async def reporte_pdf(id_reporte: str):
    """PDF del reporte de persona; para empresas usar /pdf-nit."""
    respuesta = await _pedido("GET", f"/api/v2/report_pdf/{id_reporte}")
    return Response(content=respuesta.content, media_type="application/pdf",
                    headers={"Content-Disposition": f'inline; filename="reporte_{id_reporte}.pdf"'})


@ruta_tusdatos.get("/reportes/{id_reporte}/pdf-nit")
async def reporte_nit_pdf(id_reporte: str):
    """PDF del reporte de empresa (NIT)."""
    respuesta = await _pedido("GET", f"/api/v2/report_nit_pdf/{id_reporte}")
    return Response(content=respuesta.content, media_type="application/pdf",
                    headers={"Content-Disposition": f'inline; filename="reporte_nit_{id_reporte}.pdf"'})


@ruta_tusdatos.get("/reportes/{id_reporte}/reintentar")
async def reintentar_fuentes(id_reporte: str, typedoc: str = "CC"):
    """GET /api/retry/{id} — relanza las fuentes que quedaron en error."""
    return await _pedido_json("GET", f"/api/retry/{id_reporte}", params={"typedoc": typedoc})


# --- Cuenta -------------------------------------------------

@ruta_tusdatos.get("/plan")
async def plan():
    """GET /api/plans — plan actual y consumo de la cuenta."""
    return await _pedido_json("GET", "/api/plans")


@ruta_tusdatos.get("/historial")
async def historial():
    """GET /api/querys — número de consultas previas del usuario."""
    return await _pedido_json("GET", "/api/querys", params={"parameter_list": "true"})


@ruta_tusdatos.post("/token")
async def crear_token():
    """POST /api/v1/auth/token — crea un API token Bearer (api_td_...).

    El token solo se muestra en la respuesta; guárdalo de inmediato
    (máx 10 tokens activos por usuario).
    """
    return await _pedido_json("POST", "/api/v1/auth/token")


# --- Passthrough (batches, webhooks y lo que falte) ---------

@ruta_tusdatos.api_route("/proxy/{ruta:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy(ruta: str, request: Request):
    """Reenvía crudo cualquier endpoint de la API de TusDatos.

    La ruta es relativa a la raíz de la API, p. ej. `/api/fuentes-colombia`
    o `/api/v1/batches/progress/{job_id}`.
    """
    cuerpo = await request.body()
    respuesta = await _pedido(
        request.method.upper(),
        "/" + ruta,
        params=dict(request.query_params),
        content=cuerpo if cuerpo else None,
        headers={"Content-Type": request.headers.get("content-type", "application/json")},
    )
    return Response(content=respuesta.content,
                    media_type=respuesta.headers.get("content-type", "application/json"))
