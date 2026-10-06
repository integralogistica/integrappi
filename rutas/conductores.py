import os
import random
import re
import secrets
from datetime import datetime, timedelta, timezone
from typing import Optional

import resend
from fastapi import APIRouter, BackgroundTasks, Body, File, Form, HTTPException, Request, UploadFile, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pytz

from bd.bd_cliente import bd_cliente
from Funciones.claves import crear_hash, verificar_clave
from Funciones.whatsapp_utils_integra import enviar_template_sync

# ==============================================================================
# 🔗 CONFIGURACIÓN DE BASE DE DATOS
# ==============================================================================
# Los conductores viven en `conductores`. `baseusuarios` solo se consulta para
# permitir el acceso de ADMIN al portal de conductores.
bd = bd_cliente["integra"]
coleccion_conductores = bd["conductores"]
coleccion_baseusuarios = bd["baseusuarios"]
# Vehículos: se escriben desde aquí (invitación/vinculación) para evitar import
# circular con rutas/vehiculos.py, que importa Mongo por su cuenta.
coleccion_vehiculos = bd["vehiculos"]
# Políticas de tratamiento de datos (Habeas Data): versionadas en `politicas_datos`
# (una sola activa) y evidencia append-only de aceptaciones en `aceptaciones_politica`.
coleccion_politicas = bd["politicas_datos"]
coleccion_aceptaciones = bd["aceptaciones_politica"]
# Autorización de SUJETOS sin cuenta (2026-10-05, plan aprobado): tokens de los
# links enviados por correo a propietario/tenedor/dueño de remolque sin cuenta
# en el portal. En aceptaciones_politica, esas personas quedan con
# conductor_id=null + sujeto_cedula (una entrada por declaración, canales
# "vinculo_correo" y "papel").
coleccion_tokens_aut = bd["tokens_autorizacion"]
# Auditoría append-only de impersonaciones (Seguridad entra al panel de un
# conductor existente SIN tocar su clave — módulo Alta de vehículo).
coleccion_impersonaciones = bd["impersonaciones"]

# Índice único (el correo se guarda en MAYÚSCULAS → unicidad case-insensitive).
try:
    coleccion_conductores.create_index("correo", unique=True)
    coleccion_politicas.create_index("version", unique=True)
    coleccion_aceptaciones.create_index([("conductor_id", 1), ("version", 1)])
    # Cédula: sparse (cuentas legacy no la tienen); NO unique hasta sanear datos.
    coleccion_conductores.create_index("cedula", sparse=True)
    coleccion_vehiculos.create_index("idConductor")
    # Sujetos SIN cuenta: evidencia de autorización por cédula (sparse).
    coleccion_aceptaciones.create_index("sujeto_cedula", sparse=True)
    coleccion_tokens_aut.create_index("cedula")
except Exception:
    pass


# ==============================================================================
# 🚦 CONFIGURACIÓN DEL ROUTER
# ==============================================================================
ruta_conductores = APIRouter(
    prefix="/conductores",
    tags=["Conductores"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}},
)


# ==============================================================================
# 🔑 CONFIGURACIÓN RESEND
# ==============================================================================
resend.api_key = os.getenv("RESEND_API_KEY", "re_TuApiKeyAqui...")
MAIL_FROM = os.getenv("MAIL_FROM", "no-reply@integralogistica.com")
FRONTEND_URL_VERIFICAR = os.getenv(
    "FRONTEND_URL_VERIFICAR",
    "https://integralogistica.com/integrapp/VerificarCorreo",
)
# Página de aceptación de invitación de conductor (la crea el tenedor).
FRONTEND_URL_INVITACION = os.getenv(
    "FRONTEND_URL_INVITACION",
    "https://integralogistica.com/integrapp/AceptarInvitacion",
)
# Login del portal de conductores (para el correo de credenciales del alta).
FRONTEND_URL_LOGIN = os.getenv(
    "FRONTEND_URL_LOGIN",
    "https://integralogistica.com/integrapp/LoginConductores",
)
# Plantillas WA del alta — DOBLE mensaje (2026-10-02, decisión del usuario):
# ① UTILIDAD `enruta_clave_cuenta` (APROBADA): bienvenida + {{1}} = correo +
#    botón «Abrir IntegrApp» — texto ultra-limpio, sin lenguaje de acceso
#    (cualquier mención de usuario/clave/ingresar dispara el rechazo).
# ② AUTENTICACIÓN `enruta_codigo_acceso`: cuerpo fijo de Meta con {{1}} = la
#    clave como código de un solo uso + botón «Copiar código» (es la única
#    categoría que acepta credenciales, y su cuerpo ya no es editable).
# La clave JAMÁS va dentro de la plantilla de Utilidad (Meta la rechaza y
# esconderla en una variable = variable-abuse que arriesga el WABA de las
# notificaciones oc_*). Creadas por el usuario en WhatsApp Manager.
PLANTILLA_ENRUTA_CUENTA = ("enruta_clave_cuenta", "es_CO")
PLANTILLA_ENRUTA_CLAVE = ("enruta_codigo_acceso", "es_CO")
EXPIRA_HORAS_VERIFICACION = int(os.getenv("VERIFICACION_EXPIRE_HORAS", "48"))


# ==============================================================================
# 📜 POLÍTICAS DE TRATAMIENTO DE DATOS
# ==============================================================================
# Primera versión sembrada automáticamente si `politicas_datos` está vacía
# (patrón de /tipos-costo en otros_costos.py). Editable desde Mongo o desde los
# endpoints de admin sin redeploy; nuevas versiones van con version+1.
POLITICA_DATOS_TITULO_V1 = "Política de Tratamiento de Datos Personales — Habeas Data"
POLITICA_DATOS_V1_HTML = """
<p><strong>Responsable del tratamiento:</strong> Integra Cadena de Servicios S.A.S.
(nit 901.442.833-5), en adelante <em>Integra</em>.</p>
<p>En cumplimiento de la <strong>Ley Estatutaria 1581 de 2012</strong>, el
<strong>Decreto 1074 de 2015 (art. 2.2.4.2)</strong> y demás normas concordantes
de protección de datos personales en Colombia, Integra informa lo siguiente:</p>

<h4>1. Datos que se recolectan</h4>
<p>Nombre completo, número de cédula de ciudadanía, número de celular, correo
electrónico, regional, información del vehículo (placa, línea, documentos
soporte) y documentos personales asociados a la hoja de vida del conductor.</p>

<h4>2. Finalidad del tratamiento</h4>
<ul>
  <li>Registro, verificación y aprobación de conductores y vehículos para la
      prestación del servicio de transporte.</li>
  <li>Verificación de seguridad y documental del conductor y su vehículo.</li>
  <li>Gestión contractual, operativa y de pagos del servicio prestado.</li>
  <li>Comunicaciones asociadas al servicio y a la plataforma IntegrApp.</li>
</ul>

<h4>3. Derechos del titular (ARCO)</h4>
<p>Como titular de los datos usted tiene derecho a conocer, actualizar,
rectificar y suprimir sus datos personales, así como a revocar la autorización
otorgada, en los términos de la Ley 1581 de 2012. Estos derechos pueden
ejercerse escribiendo al correo del responsable de tratamiento de datos.</p>

<h4>4. Autorización</h4>
<p>Al marcar la casilla de aceptación, el titular autoriza de forma previa,
expresa e inequívoca el tratamiento de sus datos personales para las
finalidades descritas anteriormente.</p>

<h4>5. Vigencia</h4>
<p>La presente política rige desde su publicación y puede ser actualizada;
cada actualización genera una nueva versión que le será notificada cuando
sea requerido por la ley.</p>
"""

# ── v2: DECLARACIONES DE VINCULACIÓN (2026-08-27) ─────────────────────────────
# Cada declaración se acepta INDIVIDUALMENTE (checkbox por declaración) y deja
# evidencia propia en `aceptaciones_politica` (una entrada por declaración).
# Si la política activa no tiene `declaraciones`, se auto-publica esta versión.
DECLARACIONES_V2 = [
    {
        "id": "origen_fondos",
        "titulo": "Declaración 1 — Origen de Fondos",
        "texto_html": (
            "<p>Declaro que los recursos que entrego y/o recibiré en desarrollo de mi "
            "vinculación con ORION TRANSPORTADORA DE CARGA S.A.S. provienen de actividades "
            "lícitas y que no me encuentro incluido en listas vinculantes o restrictivas "
            "relacionadas con el lavado de activos, la financiación del terrorismo u otros "
            "delitos asociados.</p>"
        ),
    },
    {
        "id": "sarlaft",
        "titulo": "Declaración 2 — SARLAFT",
        "texto_html": (
            "<p>Declaro que he sido informado(a) sobre las políticas y lineamientos del "
            "Sistema de Administración del Riesgo de Lavado de Activos y de la Financiación "
            "del Terrorismo (SARLAFT) adoptados por ORION TRANSPORTADORA DE CARGA S.A.S., y "
            "me comprometo a cumplir las disposiciones que me sean aplicables y a reportar "
            "cualquier situación inusual o sospechosa de la que tenga conocimiento en el "
            "desarrollo de mis actividades.</p>"
        ),
    },
    {
        "id": "ptee",
        "titulo": "Declaración 3 — PTEE",
        "texto_html": (
            "<p>Declaro que he leído la Política del Programa de Transparencia y Ética "
            "Empresarial (PTEE) de ORION TRANSPORTADORA DE CARGA S.A.S. y me comprometo a "
            "cumplir sus lineamientos, actuando con integridad y reportando cualquier "
            "situación que pueda constituir fraude, corrupción, soborno o cualquier conducta "
            "contraria a la ley o a las políticas de la organización.</p>"
        ),
    },
    {
        "id": "informacion_veraz",
        "titulo": "Declaración 4 — Información Veraz",
        "texto_html": (
            "<p>Declaro que la información suministrada es veraz y autorizo su verificación "
            "ante cualquier entidad pública o privada y me comprometo a actualizar los datos "
            "y documentos entregados.</p>"
        ),
    },
    {
        "id": "tratamiento_datos",
        "titulo": "Declaración 5 — Tratamiento de Datos Personales",
        "texto_html": (
            "<p>Autorizo de manera voluntaria, previa, expresa, informada e inequívoca a "
            "ORION TRANSPORTADORA DE CARGA S.A.S., identificada con NIT 800047876, para "
            "recolectar, almacenar, usar, procesar, actualizar, transferir, transmitir, "
            "circular y, en general, tratar mis datos personales de conformidad con la "
            "Ley 1581 de 2012, el Decreto 1377 de 2013 y demás normas que los modifiquen, "
            "adicionen o sustituyan, así como con la Política de Protección de Datos "
            "Personales de la organización.</p>"
            "<p>Declaro que he sido informado(a) de mis derechos como titular de los datos "
            "personales, entre ellos: conocer, actualizar y rectificar mis datos; solicitar "
            "prueba de la autorización otorgada; conocer el uso dado a mis datos; revocar la "
            "autorización y/o solicitar la supresión de los datos cuando sea procedente; "
            "acceder gratuitamente a mis datos personales; y ejercer los demás derechos "
            "consagrados en el artículo 8 de la Ley 1581 de 2012.</p>"
            "<p>Entiendo que mis datos personales podrán ser tratados para fines relacionados "
            "con la actualización de información, conocimiento de contrapartes, validación de "
            "identidad, verificación de antecedentes legales, penales y financieros, procesos "
            "de debida diligencia y consultas en bases de datos públicas y privadas, así como "
            "para las demás finalidades descritas en la Política de Protección de Datos "
            "Personales.</p>"
            "<p>Asimismo, manifiesto que conozco que puedo ejercer mis derechos mediante "
            "comunicación dirigida a la Carrera 68A No. 19-80 o al correo electrónico "
            "oficialdecumplimiento@transorion.com.co, y que la Política de Protección de "
            "Datos Personales se encuentra disponible para consulta en el siguiente enlace: "
            "Política de Protección de Datos Personales, documento que declaro conocer y me "
            "comprometo a consultar.</p>"
        ),
    },
    {
        "id": "seguridad_salud",
        "titulo": "Declaración 6 — Seguridad y Salud",
        "texto_html": (
            "<p>Me comprometo a cumplir las normas de Seguridad y Salud en el Trabajo y a "
            "reportar de manera inmediata cualquier acto o condición insegura, incidente, "
            "accidente, novedad en mi estado de salud o situación que pueda poner en riesgo "
            "mi integridad o la de terceros.</p>"
        ),
    },
    {
        "id": "pesv",
        "titulo": "Declaración 7 — PESV",
        "texto_html": (
            "<p>Me comprometo a realizar las inspecciones preoperacionales del vehículo, "
            "reportar de manera inmediata cualquier falla o condición que afecte su "
            "operación segura, así como cualquier novedad en mi estado de salud, condición "
            "de fatiga o situación que pueda poner en riesgo mi seguridad, la de los demás "
            "actores viales o la integridad de la carga.</p>"
        ),
    },
]

POLITICA_DATOS_TITULO_V2 = "Declaraciones de Vinculación y Autorización de Tratamiento de Datos Personales"

# Declaraciones que NO se exigen para activar la cuenta (2026-08-31, orden del
# usuario): si el titular no las marca, el flujo continúa igual; si las marca,
# la evidencia se registra como con las demás. En la UI NO se comunican como
# opcionales (se ven iguales a las exigidas).
DECLARACIONES_NO_EXIGIDAS = {"tratamiento_datos"}


def _politica_vigente() -> Optional[dict]:
    """
    Política activa. Auto-siembra la v1 si la colección está vacía y, si la
    activa no tiene `declaraciones` (modelo v1 de política única), auto-publica
    la v2 con las 7 declaraciones de vinculación individuales.
    """
    if coleccion_politicas.count_documents({}) == 0:
        coleccion_politicas.insert_one({
            "version": 1,
            "titulo": POLITICA_DATOS_TITULO_V1,
            "texto_html": POLITICA_DATOS_V1_HTML,
            "activo": True,
            "publicado_en": datetime.now(timezone.utc),
            "publicado_por": "SISTEMA",
        })
    politica = coleccion_politicas.find_one({"activo": True}, sort=[("version", -1)])
    if politica and not politica.get("declaraciones"):
        # Upgrade a v2: declaraciones individuales (Origen de Fondos, SARLAFT,
        # PTEE, Información Veraz, Tratamiento de Datos, SST, PESV).
        ultima = coleccion_politicas.find_one({}, sort=[("version", -1)])
        nueva_version = (ultima or {}).get("version", 1) + 1
        coleccion_politicas.update_many({"activo": True}, {"$set": {"activo": False}})
        coleccion_politicas.insert_one({
            "version": nueva_version,
            "titulo": POLITICA_DATOS_TITULO_V2,
            "declaraciones": DECLARACIONES_V2,
            "activo": True,
            "publicado_en": datetime.now(timezone.utc),
            "publicado_por": "SISTEMA",
            "auto_upgrade": True,
        })
        politica = coleccion_politicas.find_one({"activo": True}, sort=[("version", -1)])
    return politica


def _politica_publica(doc: dict) -> dict:
    """Proyección de la política para respuestas públicas (sin metadatos internos)."""
    return {
        "version": doc.get("version"),
        "titulo": doc.get("titulo", ""),
        "texto_html": doc.get("texto_html", ""),
        "declaraciones": doc.get("declaraciones", []),
        "publicado_en": doc.get("publicado_en"),
    }


# ==============================================================================
# 📌 ESQUEMAS DE DATOS
# ==============================================================================
class RegistrarConductorInput(BaseModel):
    # `usuario` se conserva por compatibilidad con el front desplegado; el
    # identificador real del conductor es `correo` (login solo por correo).
    # El nombre YA NO se pide en el registro (2026-10-06, mismo criterio del
    # alta de Seguridad): lo aporta la IA al leer cédula/RUT en el paso 2 y
    # se propaga a la cuenta desde /vehiculos/actualizar-informacion.
    nombre: Optional[str] = None
    usuario: Optional[str] = None  # ignorado; se usa `correo`
    correo: str
    clave: str
    cedula: Optional[str] = None
    celular: Optional[str] = None
    regional: Optional[str] = None
    perfil: Optional[str] = None  # CONDUCTOR (default) o TENEDOR (dueño del vehículo)


class VerificarInput(BaseModel):
    usuario: str  # contiene el correo (compatibilidad de body con el front)
    perfil: Optional[str] = None


class ValidarCodigoInput(BaseModel):
    usuario: str  # contiene el correo
    codigo: str
    perfil: Optional[str] = None


class CambioClaveInput(BaseModel):
    usuario: str  # contiene el correo
    nuevaClave: str
    codigo: str
    perfil: Optional[str] = None


# ==============================================================================
# 🛠️ HELPERS
# ==============================================================================
def enviar_correo_codigo(destinatario: str, codigo: str):
    """Envía el código de verificación usando Resend de forma silenciosa."""
    if not resend.api_key or "TuApiKeyAqui" in resend.api_key:
        print("⚠️ ERROR: Falta API KEY de Resend.")
        return

    html_simple = f"""
    <p>Hola,</p>
    <p>Tu código de verificación es: <strong>{codigo}</strong></p>
    <p><small>Si no solicitaste este código, ignora este mensaje.</small></p>
    """
    try:
        resend.Emails.send({
            "from": MAIL_FROM,
            "to": [destinatario],
            "subject": f"Código de verificación: {codigo}",
            "html": html_simple,
        })
    except Exception as e:
        print(f"❌ Error crítico enviando correo: {e}")


def enviar_correo_credenciales(destinatario: str, clave: str, perfil: str, creado_por: str):
    """
    Correo con las CREDENCIALES de la cuenta creada por Seguridad (alta de
    vehículo, 2026-10-02): el conductor recibe su usuario y su clave temporal
    sin esperar a que alguien se los haga llegar. Fire-and-forget: si falla,
    la clave ya quedó mostrada en pantalla UNA vez como respaldo.
    """
    if not resend.api_key or "TuApiKeyAqui" in resend.api_key:
        print("⚠️ ERROR: Falta API KEY de Resend; no se envió el correo de credenciales.")
        return
    rol = "tenedor del vehículo" if perfil == "TENEDOR" else "conductor"
    html = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 520px; margin: 0 auto;">
      <h2 style="color: #0f1928;">Tu cuenta de IntegrApp En Ruta</h2>
      <p>Hola{f', {creado_por} de Seguridad creó' if creado_por else 'Crearon'} tu cuenta
      como {rol}. Con ella ingresas al portal para completar el registro de tu
      vehículo y ofrecer tu disponibilidad.</p>
      <p>Tus datos de ingreso son:</p>
      <div style="background: #f4f6f8; border-radius: 10px; padding: 16px 20px; margin: 16px 0;">
        <p style="margin: 4px 0;"><strong>Usuario:</strong> {destinatario}</p>
        <p style="margin: 4px 0;"><strong>Clave:</strong> {clave}</p>
      </div>
      <p style="text-align: center; margin: 28px 0;">
        <a href="{FRONTEND_URL_LOGIN}"
           style="background: #0f1928; color: #fff; padding: 12px 28px; border-radius: 10px;
                  text-decoration: none; font-weight: bold;">
          Ingresar al portal
        </a>
      </p>
      <p>O copia y pega este enlace en tu navegador:</p>
      <p><a href="{FRONTEND_URL_LOGIN}">{FRONTEND_URL_LOGIN}</a></p>
      <p>En tu primer ingreso deberás leer y aceptar las declaraciones de
      vinculación (Habeas Data); ese paso es personal y no puede hacerlo nadie
      por ti. Guarda este correo: la clave no se vuelve a mostrar.</p>
      <p><small>Si no reconoces esta cuenta, comunícate con Integra Logística.</small></p>
    </div>
    """
    try:
        resend.Emails.send({
            "from": MAIL_FROM,
            "to": [destinatario],
            "subject": "Tu usuario y clave — IntegrApp En Ruta",
            "html": html,
        })
        print(f"📧 Correo de credenciales enviado a {destinatario}")
    except Exception as e:
        print(f"❌ Error enviando correo de credenciales: {e}")


def _celular_whatsapp(celular: str) -> Optional[str]:
    """
    Normaliza el celular del alta al formato de la API de WhatsApp (solo
    dígitos con indicativo de país). El PhoneField del formulario guarda:
    +57 → solo dígitos locales (10, empiezan por 3); otra región →
    "+<código> <número>". Devuelve None si no alcanza para un número útil.
    """
    digitos = re.sub(r"\D", "", celular or "")
    if len(digitos) == 10 and digitos.startswith("3"):
        return f"57{digitos}"          # celular colombiano sin indicativo
    if len(digitos) >= 11:             # ya trae indicativo (57 u otro país)
        return digitos
    return None


def _enviar_wa_credenciales(celular: str, usuario: str, clave: str):
    """
    WhatsApp DOBLE de la cuenta nueva: ① Utilidad (aviso + usuario + botón al
    portal) y ② Autenticación (la clave como código con «Copiar código»).
    Cada envío es independiente (el fallo de uno no tapa el otro) y todo es
    fire-and-forget: si una plantilla sigue pendiente en Meta o el número no
    sirve, solo queda el log — jamás rompe el alta.
    """
    destino = _celular_whatsapp(celular)
    if not destino:
        print(f"[alta-seguridad] Sin celular útil ({celular!r}); no se envió WhatsApp.")
        return
    # Prints SIN emoji: en consolas sin UTF-8 (Windows/cp1252) un print con
    # emoji lanza UnicodeEncodeError DENTRO del try y mataba el segundo envío.
    # El helper devuelve None si Meta rechaza (no lanza): el log solo dice
    # "enviado" cuando fue 200 de verdad.
    try:
        ok = enviar_template_sync(
            destino, PLANTILLA_ENRUTA_CUENTA[0], PLANTILLA_ENRUTA_CUENTA[1],
            [usuario])  # {{1}} = correo (el usuario de la cuenta)
        if ok:
            print(f"[alta-seguridad] WhatsApp de aviso de cuenta enviado a +{destino}")
    except Exception as e:
        print(f"[alta-seguridad] Error enviando WhatsApp de aviso: {e}")
    try:
        ok = enviar_template_sync(
            destino, PLANTILLA_ENRUTA_CLAVE[0], PLANTILLA_ENRUTA_CLAVE[1],
            [clave],  # {{1}} = clave (código de un solo uso)
            # La plantilla de Autenticación con «Copiar código» EXIGE el código
            # también como parámetro del botón, y el botón viaja por la API
            # como sub_type "url" (no "copy_code": Meta responde
            # "Button at index 0 must be of type Url").
            botones=[{"sub_type": "url", "parameters": [clave]}])
        if ok:
            print(f"[alta-seguridad] WhatsApp de la clave enviado a +{destino}")
    except Exception as e:
        print(f"[alta-seguridad] Error enviando WhatsApp de la clave: {e}")


def _existe_correo(correo: str) -> bool:
    if not correo:
        return False
    patron = {"$regex": f"^{re.escape(correo.strip())}$", "$options": "i"}
    return coleccion_conductores.find_one({"correo": patron}) is not None


def _buscar_por_usuario(usuario_o_correo: str):
    """Conductor por correo exacto (case-insensitive), para recuperación de clave."""
    if not usuario_o_correo:
        return None
    patron = {"$regex": f"^{re.escape(usuario_o_correo.strip())}$", "$options": "i"}
    return coleccion_conductores.find_one({"correo": patron})


def _generar_token_verificacion(doc_id) -> str:
    """Token plano de un solo uso; en BD queda solo su hash (patrón de aut2.py)."""
    token_plano = secrets.token_urlsafe(32)
    coleccion_conductores.update_one(
        {"_id": doc_id},
        {"$set": {
            "verificacion_token_hash": crear_hash(token_plano),
            "verificacion_expira": datetime.now(timezone.utc) + timedelta(hours=EXPIRA_HORAS_VERIFICACION),
        }},
    )
    return token_plano


def _verificar_token_verificacion(doc: dict, token_plano: str) -> bool:
    token_hash = doc.get("verificacion_token_hash")
    expira = doc.get("verificacion_expira")
    if not token_hash or not expira:
        return False
    # Fechas Mongo naive = UTC (convención del proyecto).
    if expira.tzinfo is None:
        expira = expira.replace(tzinfo=timezone.utc)
    if datetime.now(timezone.utc) > expira:
        return False
    return verificar_clave(token_plano, token_hash)


def _buscar_conductor_por_token(token_plano: str) -> Optional[dict]:
    """
    Conductor cuyo token de verificación coincide (hash + expiración).
    Comparte el bucle entre /verificar-correo (GET) y /aceptar-politica (POST).
    """
    if not token_plano:
        return None
    for doc in coleccion_conductores.find(
        {"verificacion_token_hash": {"$exists": True}},
    ).limit(200):
        if _verificar_token_verificacion(doc, token_plano):
            return doc
    return None


def enviar_correo_verificacion(destinatario: str, enlace: str, nombre: str):
    """Envía el correo con el enlace de verificación usando Resend (fire-and-forget)."""
    if not resend.api_key or "TuApiKeyAqui" in resend.api_key:
        print("⚠️ ERROR: Falta API KEY de Resend; no se envió el correo de verificación.")
        return
    # El registro ya no pide nombre (lo llena la IA en el paso 2): sin nombre
    # el saludo no deja una coma colgando.
    saludo = f"¡Bienvenido a IntegrApp, {nombre}!" if (nombre or "").strip() else "¡Bienvenido a IntegrApp!"
    html = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 520px; margin: 0 auto;">
      <h2 style="color: #0f1928;">{saludo}</h2>
      <p>Para activar tu cuenta de conductor y continuar con el registro de tu vehículo,
      confirma tu correo electrónico con el siguiente enlace:</p>
      <p>Al verificar tu correo deberás leer y aceptar nuestras Políticas de
      Tratamiento de Datos Personales (Habeas Data).</p>
      <p style="text-align: center; margin: 28px 0;">
        <a href="{enlace}"
           style="background: #0f1928; color: #fff; padding: 12px 28px; border-radius: 10px;
                  text-decoration: none; font-weight: bold;">
          Verificar mi correo
        </a>
      </p>
      <p>O copia y pega este enlace en tu navegador:</p>
      <p><a href="{enlace}">{enlace}</a></p>
      <p><small>El enlace vence en {EXPIRA_HORAS_VERIFICACION} horas. Si no solicitaste esta
      cuenta, ignora este mensaje.</small></p>
    </div>
    """
    try:
        resend.Emails.send({
            "from": MAIL_FROM,
            "to": [destinatario],
            "subject": "Verifica tu correo — IntegrApp Conductores",
            "html": html,
        })
        print(f"📧 Correo de verificación enviado a {destinatario}")
    except Exception as e:
        print(f"❌ Error enviando correo de verificación: {e}")


# ==============================================================================
# 📝 REGISTRO
# ==============================================================================
@ruta_conductores.post("/registrar", response_model=dict)
async def registrar_conductor(data: RegistrarConductorInput, background_tasks: BackgroundTasks):
    correo_norm = (data.correo or "").strip()
    clave_plana = (data.clave or "").strip()

    if _existe_correo(correo_norm):
        raise HTTPException(status_code=400, detail="El usuario ya existe")

    if len(clave_plana) < 6:
        raise HTTPException(status_code=400, detail="La clave debe tener al menos 6 caracteres")

    try:
        clave_hash = crear_hash(clave_plana)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # Perfil: CONDUCTOR (default) o TENEDOR (dueño del vehículo con flota).
    perfil_solicitado = (data.perfil or "CONDUCTOR").strip().upper()
    if perfil_solicitado not in ("CONDUCTOR", "TENEDOR"):
        perfil_solicitado = "CONDUCTOR"

    cedula = re.sub(r"\D", "", data.cedula or "") or None

    nuevo = {
        "nombre": (data.nombre or "").upper(),
        "correo": correo_norm.upper() if correo_norm else None,
        "cedula": cedula,
        "regional": (data.regional or "N/A").upper(),
        "celular": (data.celular or "").upper() if data.celular else None,
        "perfil": perfil_solicitado,
        "clave": clave_hash,
        "clientes": [],
        "activo": True,
        # El correo se verifica con el enlace enviado al registrarse.
        "correo_verificado": False,
    }

    insertado = coleccion_conductores.insert_one(nuevo).inserted_id

    # Enviar correo de verificación (token hasheado en BD, plano solo en el enlace).
    token = _generar_token_verificacion(insertado)
    enlace = f"{FRONTEND_URL_VERIFICAR}?token={token}"
    background_tasks.add_task(enviar_correo_verificacion, correo_norm, enlace, (data.nombre or "").strip())

    return {
        "mensaje": "Conductor registrado. Revisa tu correo para verificar la cuenta.",
        "usuario": {"id": str(insertado), "correo": nuevo["correo"], "perfil": "CONDUCTOR"},
    }


# ==============================================================================
# 🛡️ ALTA POR SEGURIDAD (2026-09-28)
# Para las placas históricas que ya trabajan con Integra y entregaron la
# hoja de vida FÍSICA firmada (autorización de datos): Seguridad crea la
# cuenta del conductor con su correo, le entrega usuario+clave, y el
# conductor acepta las políticas digitales en su primer ingreso.
# ==============================================================================
class AltaSeguridadInput(BaseModel):
    correo: str
    # El nombre YA NO se pide en el alta (2026-10-01): lo aporta la IA al
    # leer cédula/RUT en el panel y se propaga a la cuenta desde
    # /vehiculos/actualizar-informacion mientras esté vacío.
    nombre: Optional[str] = None
    perfil: Optional[str] = "CONDUCTOR"   # CONDUCTOR | TENEDOR
    clave: Optional[str] = None           # generada si no viene
    cedula: Optional[str] = None
    celular: Optional[str] = None
    creado_por: Optional[str] = None      # nombre del usuario de Seguridad


def _generar_clave_legible() -> str:
    """Clave temporal legible (el conductor la cambia después)."""
    import secrets
    import string
    alfabeto = string.ascii_letters + string.digits
    return "Integra" + "".join(secrets.choice(alfabeto) for _ in range(6))


@ruta_conductores.post("/alta-seguridad", response_model=dict)
async def alta_por_seguridad(data: AltaSeguridadInput, background_tasks: BackgroundTasks):
    correo_norm = (data.correo or "").strip()
    if not correo_norm or "@" not in correo_norm:
        raise HTTPException(status_code=400, detail="El correo del conductor es obligatorio.")

    if _existe_correo(correo_norm):
        raise HTTPException(status_code=400, detail="Ya existe una cuenta con ese correo.")

    clave_plana = (data.clave or "").strip() or _generar_clave_legible()
    if len(clave_plana) < 6:
        raise HTTPException(status_code=400, detail="La clave debe tener al menos 6 caracteres.")

    try:
        clave_hash = crear_hash(clave_plana)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    perfil_solicitado = (data.perfil or "CONDUCTOR").strip().upper()
    if perfil_solicitado not in ("CONDUCTOR", "TENEDOR"):
        perfil_solicitado = "CONDUCTOR"

    nuevo = {
        # Vacío cuando el alta no lo trae: la IA lo llena al leer los
        # documentos y actualizar-informacion lo propaga a la cuenta.
        "nombre": (data.nombre or "").strip().upper(),
        "correo": correo_norm.upper(),
        "cedula": re.sub(r"\D", "", data.cedula or "") or None,
        "celular": (data.celular or "").strip() or None,
        "regional": "N/A",
        "perfil": perfil_solicitado,
        "clave": clave_hash,
        "clientes": [],
        "activo": True,
        # Seguridad verificó la identidad EN PERSONE (hoja de vida física
        # firmada = autorización de datos): el correo no requiere verificación.
        "correo_verificado": True,
        # La aceptación DIGITAL de políticas la hace el conductor en su
        # primer ingreso (banner bloqueante del panel).
        "pendiente_aceptacion_politica": True,
        "alta_por_seguridad": True,
        "alta_seguridad_en": datetime.now(timezone.utc),
        "alta_por": (data.creado_por or "Seguridad").strip() or "Seguridad",
    }
    insertado = coleccion_conductores.insert_one(nuevo).inserted_id
    print(f"[alta-seguridad] Conductor {nuevo['correo']} creado por {nuevo['alta_por']}")

    # Correo automático con las credenciales (2026-10-02): fire-and-forget —
    # la clave igual se muestra UNA vez abajo como respaldo si el correo falla.
    background_tasks.add_task(
        enviar_correo_credenciales, correo_norm, clave_plana,
        perfil_solicitado, nuevo["alta_por"])
    # WhatsApp con las credenciales (plantilla enruta_cuenta_creada), solo si
    # el alta trae celular útil — también fire-and-forget.
    if _celular_whatsapp(data.celular):
        background_tasks.add_task(
            _enviar_wa_credenciales, data.celular, nuevo["correo"], clave_plana)

    return {
        "mensaje": ("Conductor creado. Le enviamos sus credenciales por correo"
                    "%s; guárdate esta clave por si no llegan."
                    % (" y WhatsApp" if _celular_whatsapp(data.celular) else "")),
        "clave": clave_plana,  # se muestra UNA sola vez
        "credenciales_enviadas": True,
        "usuario": {"id": str(insertado), "correo": nuevo["correo"], "perfil": perfil_solicitado},
    }


@ruta_conductores.get("/alta-seguridad/listar", response_model=dict)
async def listar_alta_seguridad():
    """
    Cuentas creadas por Seguridad (para el módulo Alta conductor): quién las
    creó, cuándo y si el conductor ya aceptó políticas. Sin claves JAMÁS
    (solo hasheadas en BD) — si se pierde, se regenera con nueva-clave.
    """
    cuentas = []
    for doc in coleccion_conductores.find(
            {"alta_por_seguridad": True},
            {"clave": 0}).sort("alta_seguridad_en", -1).limit(200):
        cuentas.append({
            "id": str(doc.get("_id")),
            "nombre": doc.get("nombre", ""),
            "correo": doc.get("correo", ""),
            "perfil": doc.get("perfil", ""),
            "celular": doc.get("celular") or "",
            "creado_por": doc.get("alta_por", "Seguridad"),
            "creado_en": doc.get("alta_seguridad_en"),
            "politicas_pendientes": bool(doc.get("pendiente_aceptacion_politica")),
        })
    return {"cuentas": cuentas}


@ruta_conductores.post("/alta-seguridad/{conductor_id}/nueva-clave", response_model=dict)
async def regenerar_clave_alta(conductor_id: str):
    """
    Regenera la clave de una cuenta creada por Seguridad (rescate cuando la
    clave mostrada se pierde o el conductor la olvida): genera una nueva,
    la hashea y la devuelve UNA sola vez para reenviársela.
    """
    from bson import ObjectId as _ObjectId
    try:
        oid = _ObjectId(conductor_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Cuenta no encontrada.")

    doc = coleccion_conductores.find_one({"_id": oid, "alta_por_seguridad": True})
    if not doc:
        raise HTTPException(status_code=404, detail="Cuenta no encontrada (o no fue creada por Seguridad).")

    clave_plana = _generar_clave_legible()
    coleccion_conductores.update_one(
        {"_id": oid}, {"$set": {"clave": crear_hash(clave_plana)}})
    print(f"[alta-seguridad] Clave regenerada para {doc.get('correo')}")
    return {
        "mensaje": "Nueva clave generada. Envíasela al conductor (invalida la anterior).",
        "correo": doc.get("correo", ""),
        "clave": clave_plana,  # se muestra UNA sola vez
    }


# ==============================================================================
# 🚚 ALTA DE VEHÍCULO (2026-10-01)
# El alta es del VEHÍCULO (la placa), no de la cuenta: Seguridad primero valida
# la placa y luego la vincula a un dueño que ya tiene cuenta (búsqueda) o crea
# una cuenta nueva. Para cuentas existentes, el panel se abre por
# impersonación SIN tocar la clave del conductor.
# ==============================================================================
@ruta_conductores.get("/buscar", response_model=dict)
async def buscar_conductores(q: str = ""):
    """
    Búsqueda de cuentas CONDUCTOR/TENEDOR para VINCULAR una placa nueva a un
    dueño que ya tiene cuenta (módulo Alta de vehículo): por correo, cédula o
    nombre. Nunca devuelve claves.
    """
    texto = (q or "").strip()
    if len(texto) < 3:
        return {"cuentas": []}

    patron = re.escape(texto)
    condiciones = [
        {"correo": {"$regex": patron, "$options": "i"}},
        {"nombre": {"$regex": patron, "$options": "i"}},
    ]
    cedula_digitos = re.sub(r"\D", "", texto)
    if cedula_digitos:
        condiciones.append({"cedula": {"$regex": cedula_digitos, "$options": "i"}})

    cuentas = []
    for doc in coleccion_conductores.find({"$or": condiciones}, {"clave": 0}).limit(20):
        cid = str(doc.get("_id"))
        cuentas.append({
            "id": cid,
            "nombre": doc.get("nombre", ""),
            "correo": doc.get("correo", ""),
            "perfil": (doc.get("perfil") or "CONDUCTOR").upper(),
            "cedula": doc.get("cedula") or "",
            "celular": doc.get("celular") or "",
            "activo": bool(doc.get("activo", True)),
            "correo_verificado": bool(doc.get("correo_verificado")),
            "alta_por_seguridad": bool(doc.get("alta_por_seguridad")),
            "politicas_pendientes": bool(
                doc.get("pendiente_aceptacion_politica")
                and not doc.get("aceptacion_politica")),
            # Placas propias: un CONDUCTOR con 1 placa ya no puede recibir otra.
            "placas_propias": coleccion_vehiculos.count_documents({"idUsuario": cid}),
        })
    return {"cuentas": cuentas}


# ==============================================================================
# ⚖️ HABEAS DATA — EVIDENCIA DE AUTORIZACIÓN POR CÉDULA (2026-10-05)
# Consulta de SOLO LECTURA para auditorías: dadas las cédulas de los sujetos de
# un estudio de seguridad, resuelve la cuenta del portal y TODO su historial de
# aceptaciones de políticas (fecha, canal, versión, declaración, IP,
# user-agent). La evidencia vive en `aceptaciones_politica` (append-only) y
# sobrevive a ediciones del doc del conductor.
# ==============================================================================
@ruta_conductores.get("/habeas-data", response_model=dict)
async def consultar_habeas_data(cedulas: str = ""):
    """
    Evidencia de autorización de tratamiento de datos por cédula (consumido por
    las tarjetas de estudio de /revision y el módulo «Estudios por antigüedad»).
    `cedulas` = lista separada por comas. Nunca devuelve claves.
    """
    lista = [re.sub(r"\D", "", c) for c in (cedulas or "").split(",")]
    lista = [c for c in lista if len(c) >= 4][:15]
    if not lista:
        return {"personas": []}

    personas = []
    for cedula in lista:
        # La cédula puede venir guardada con puntos/espacios en cuentas viejas:
        # se busca por dígitos anclados (mismo criterio del /buscar).
        cuenta = coleccion_conductores.find_one(
            {"cedula": {"$regex": f"^{re.escape(cedula)}$", "$options": "i"}},
            {"clave": 0, "verificacion_token_hash": 0},
        )

        entrada = {
            "cedula": cedula,
            "tiene_cuenta": bool(cuenta),
            "nombre": (cuenta or {}).get("nombre", ""),
            "correo": (cuenta or {}).get("correo", ""),
            "perfil": ((cuenta or {}).get("perfil") or "CONDUCTOR").upper() if cuenta else "",
            "aceptacion_politica": (cuenta or {}).get("aceptacion_politica"),
            "declaraciones_aceptadas": (cuenta or {}).get("declaraciones_aceptadas", []),
            "politicas_pendientes": bool(
                cuenta and cuenta.get("pendiente_aceptacion_politica")
                and not cuenta.get("aceptacion_politica")),
            "aceptaciones": [],
        }

        if cuenta:
            # Historial completo (append-only): una entrada POR declaración.
            for acep in coleccion_aceptaciones.find(
                {"conductor_id": cuenta["_id"]}
            ).sort("aceptado_en", -1).limit(50):
                entrada["aceptaciones"].append({
                    "version": acep.get("version"),
                    "declaracion_id": acep.get("declaracion_id", ""),
                    "declaracion_titulo": acep.get("declaracion_titulo", ""),
                    "aceptado_en": acep.get("aceptado_en"),
                    "canal": acep.get("canal", ""),
                    "ip": acep.get("ip", ""),
                    "user_agent": (acep.get("user_agent") or "")[:120],
                })

        # Sujetos SIN cuenta (2026-10-05): evidencia por cédula — link por
        # correo o firma en papel (con el documento de respaldo).
        for acep in coleccion_aceptaciones.find(
            {"sujeto_cedula": cedula}
        ).sort("aceptado_en", -1).limit(50):
            entrada["aceptaciones"].append({
                "version": acep.get("version"),
                "declaracion_id": acep.get("declaracion_id", ""),
                "declaracion_titulo": acep.get("declaracion_titulo", ""),
                "aceptado_en": acep.get("aceptado_en"),
                "canal": acep.get("canal", ""),
                "ip": acep.get("ip", ""),
                "user_agent": (acep.get("user_agent") or "")[:120],
                "documento_ruta": acep.get("documento_ruta"),
                "registrado_por": acep.get("registrado_por", ""),
            })
        # Link vigente sin usar → el front muestra «pendiente (enviado el …)».
        try:
            pendiente = coleccion_tokens_aut.find_one({
                "cedula": cedula, "usado_en": None,
                "expira": {"$gt": datetime.utcnow()}})
            creado = pendiente.get("creado_en") if pendiente else None
            # Solo si es un datetime real (defensa: un mock/valor raro del
            # almacenamiento no debe romper la serialización del response).
            if isinstance(creado, datetime):
                entrada["token_pendiente"] = creado
        except Exception:
            pass

        personas.append(entrada)

    # El doc embebido `aceptacion_politica` guarda ObjectIds (politica_id,
    # aceptacion_id) y fechas datetime: sin sanitizar, la serialización de
    # FastAPI revienta con 500 (bug encontrado en runtime 2026-10-06 — la
    # pestaña «Cambios» de /revision no mostraba NADA en autorizaciones).
    from rutas.vehiculos import _json_seguro
    return JSONResponse(content={"personas": _json_seguro(personas)})


# ==============================================================================
# ⚖️ AUTORIZACIÓN DE SUJETOS SIN CUENTA (2026-10-05, plan aprobado)
# La autorización es por PERSONA (cédula): cubre TODOS sus roles (conductor,
# propietario, tenedor, dueño de remolque) en todos los vehículos. Los actores
# SIN cuenta en el portal autorizan por:
#   - LINK POR CORREO: token 48 h → página pública /AutorizacionDatos →
#     acepta las declaraciones → evidencia canal "vinculo_correo".
#   - PAPEL: Seguridad sube la firma física → evidencia canal "papel" con el
#     documento en el bucket privado.
# Las empresas (NIT) quedan exentas (autorización vía contrato/tenedor).
# ==============================================================================
FRONTEND_URL_AUTORIZACION = os.getenv(
    "FRONTEND_URL_AUTORIZACION",
    "https://integralogistica.com/integrapp/AutorizacionDatos")
# Vencimiento del link de autorización (2026-10-06: 48 h → 30 días — pedido
# del usuario: los actores suelen tardar CASI UN MES o más en abrir el correo;
# el link muerto obligaba a reenviar a mano). Ajustable por env sin deploy.
EXPIRA_HORAS_AUTORIZACION = int(os.getenv("AUTORIZACION_EXPIRE_HORAS", "720"))
_TZ_BOGOTA_AUT = pytz.timezone("America/Bogota")


def _digitos_aut(valor) -> str:
    return re.sub(r"\D", "", str(valor or ""))


def _sujeto_ya_autorizado(cedula: str) -> bool:
    """La cédula ya tiene evidencia de autorización: cuenta con aceptación
    registrada o entradas de sujeto (link por correo / papel)."""
    cuenta = coleccion_conductores.find_one(
        {"cedula": {"$regex": f"^{re.escape(cedula)}$", "$options": "i"}})
    if cuenta and cuenta.get("aceptacion_politica"):
        return True
    return coleccion_aceptaciones.find_one({"sujeto_cedula": cedula}) is not None


def _buscar_token_aut(token_plano: str) -> Optional[dict]:
    """Token de autorización VÁLIDO (hash correcto + sin usar + no expirado).
    Igual que el token de verificación: solo el hash vive en BD (bcrypt), así
    que se recorren los candidatos y se verifica (tope 200)."""
    if not token_plano:
        return None
    ahora = datetime.utcnow()
    for doc in coleccion_tokens_aut.find({"usado_en": None}).limit(200):
        expira = doc.get("expira")
        token_hash = doc.get("token_hash")
        if not expira or not token_hash:
            continue
        if expira.tzinfo is not None:  # fechas Mongo naive = UTC (convención)
            expira = expira.replace(tzinfo=None)
        if ahora > expira:
            continue
        if verificar_clave(token_plano, doc.get("token_hash", "")):
            return doc
    return None


def _vencimiento_legible() -> str:
    """Vencimiento del link en texto humano (días si ≥72 h, horas si no)."""
    if EXPIRA_HORAS_AUTORIZACION >= 72:
        return f"{EXPIRA_HORAS_AUTORIZACION // 24} días"
    return f"{EXPIRA_HORAS_AUTORIZACION} horas"


def enviar_correo_autorizacion(destinatario: str, enlace: str, nombre: str, placa: str):
    """Correo con el link público de autorización de tratamiento de datos."""
    if not resend.api_key or "TuApiKeyAqui" in resend.api_key:
        print("⚠️ Falta API KEY de Resend; no se envió el correo de autorización "
              f"(destinatario {destinatario}).")
        return
    html = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 520px; margin: 0 auto;">
      <h2 style="color: #0f1928;">Autorización de tratamiento de datos</h2>
      <p>Hola {nombre or 'usuario'}:</p>
      <p>Estás siendo vinculado(a) como actor del vehículo <b>{placa}</b> en la
      plataforma <b>IntegrApp</b> de ORION TRANSPORTADORA DE CARGA S.A.S.
      Para consultar tu información en fuentes públicas (estudio de seguridad)
      necesitamos tu autorización expresa de tratamiento de datos personales
      (Ley 1581 de 2012).</p>
      <p style="text-align: center; margin: 28px 0;">
        <a href="{enlace}"
           style="background: #0f1928; color: #fff; padding: 12px 28px; border-radius: 10px;
                  text-decoration: none; font-weight: bold;">
          Revisar y autorizar
        </a>
      </p>
      <p>O copia y pega este enlace en tu navegador:</p>
      <p><a href="{enlace}">{enlace}</a></p>
      <p><small>El enlace vence en {_vencimiento_legible()}. Si no esperabas
      este mensaje, ignóralo.</small></p>
    </div>
    """
    try:
        resend.Emails.send({
            "from": MAIL_FROM,
            "to": [destinatario],
            "subject": "Autorización de tratamiento de datos — IntegrApp",
            "html": html,
        })
        print(f"📧 Correo de autorización enviado a {destinatario} ({placa})")
    except Exception as e:
        print(f"❌ Error enviando correo de autorización a {destinatario}: {e}")


def _solicitar_autorizacion(placa: str, cedula: str, correo: str,
                            nombre: str = "", solicitado_por: str = "") -> dict:
    """Crea/regenera el token y envía el correo. Compartido por el endpoint
    manual (botón de /revision) y el envío automático al pasar a revisión.
    Idempotente: si ya hay link vigente no se re-envía; si la persona ya
    autorizó, no se envía nada."""
    correo = (correo or "").strip()
    if not correo or "@" not in correo:
        raise HTTPException(status_code=400,
                            detail="El correo es obligatorio para enviar la autorización.")
    if _sujeto_ya_autorizado(cedula):
        return {"estado": "ya_autorizado"}
    ahora = datetime.utcnow()
    pendiente = coleccion_tokens_aut.find_one({
        "cedula": cedula, "usado_en": None, "expira": {"$gt": ahora}})
    if pendiente:
        return {"estado": "pendiente", "enviado_en": pendiente.get("creado_en"),
                "expira": pendiente.get("expira")}
    # Invalidar tokens previos sin usar y crear el nuevo (hash en BD).
    token_plano = secrets.token_urlsafe(32)
    try:
        coleccion_tokens_aut.update_many(
            {"cedula": cedula, "usado_en": None},
            {"$set": {"usado_en": ahora, "resultado": "reemplazado"}})
    except Exception:
        pass
    coleccion_tokens_aut.insert_one({
        "cedula": cedula, "correo": correo.upper(),
        "placa": (placa or "").strip().upper(), "nombre": (nombre or "").strip(),
        "token_hash": crear_hash(token_plano),
        "creado_en": ahora,
        "expira": ahora + timedelta(hours=EXPIRA_HORAS_AUTORIZACION),
        "solicitado_por": (solicitado_por or "").strip() or "seguridad",
    })
    enviar_correo_autorizacion(correo, f"{FRONTEND_URL_AUTORIZACION}?token={token_plano}",
                               (nombre or "").strip(), (placa or "").strip().upper())
    return {"estado": "enviado", "enviado_en": ahora,
            "expira": ahora + timedelta(hours=EXPIRA_HORAS_AUTORIZACION)}


class SolicitarAutorizacionInput(BaseModel):
    placa: str
    cedula: str
    correo: str
    nombre: Optional[str] = None
    solicitado_por: Optional[str] = None


class AceptarAutorizacionInput(BaseModel):
    token: str
    # ids de las declaraciones marcadas (deben ser TODAS las exigidas).
    declaraciones_aceptadas: Optional[list] = None


@ruta_conductores.post("/autorizacion/solicitar", response_model=dict)
async def solicitar_autorizacion(data: SolicitarAutorizacionInput):
    """Botón «✉️ Enviar autorización» de /revision: link por correo a un actor
    del estudio sin cuenta. Idempotente (link vigente → no duplica correo)."""
    cedula = _digitos_aut(data.cedula)
    if len(cedula) < 4:
        raise HTTPException(status_code=400, detail="Cédula no válida.")
    return _solicitar_autorizacion(data.placa, cedula, data.correo,
                                   data.nombre or "", data.solicitado_por or "")


@ruta_conductores.get("/autorizacion/verificar", response_model=dict)
def verificar_token_autorizacion(token: str = ""):
    """Valida el link (sin consumirlo) y entrega la política a mostrar en la
    página pública /AutorizacionDatos (mismo shape que verificar-correo)."""
    doc = _buscar_token_aut(token)
    if not doc:
        raise HTTPException(
            status_code=400,
            detail="El enlace no es válido o ya fue usado/expiró. Pide uno nuevo a Seguridad.")
    politica = _politica_vigente()
    return {
        "estado": "pendiente",
        "nombre": doc.get("nombre", ""),
        "correo": doc.get("correo", ""),
        "placa": doc.get("placa", ""),
        "politica": _politica_publica(politica),
    }


@ruta_conductores.post("/autorizacion/aceptar", response_model=dict)
async def aceptar_autorizacion(data: AceptarAutorizacionInput, request: Request):
    """Aceptación desde la página pública: una entrada append-only POR
    declaración en aceptaciones_politica (canal vinculo_correo, campos
    sujeto_*) y el token queda usado."""
    doc = _buscar_token_aut(data.token)
    if not doc:
        raise HTTPException(
            status_code=400,
            detail="El enlace no es válido o ya fue usado/expiró. Pide uno nuevo a Seguridad.")
    politica = _politica_vigente()
    marcadas = _validar_declaraciones_completas(politica, data.declaraciones_aceptadas)

    ahora = datetime.utcnow()
    ip = request.client.host if request.client else ""
    user_agent = (request.headers.get("user-agent") or "")[:300]
    declaraciones = politica.get("declaraciones") or []
    base = {
        "conductor_id": None,
        "sujeto_cedula": doc["cedula"],
        "sujeto_nombre": doc.get("nombre", ""),
        "sujeto_correo": doc.get("correo", ""),
        "politica_id": politica["_id"],
        "version": politica.get("version"),
        "aceptado_en": ahora,
        "canal": "vinculo_correo",
        "ip": ip,
        "user_agent": user_agent,
    }
    if declaraciones:
        for decl in declaraciones:
            if marcadas is not None and decl["id"] not in marcadas:
                continue
            entrada = dict(base)
            entrada.update({
                "declaracion_id": decl["id"],
                "declaracion_titulo": decl.get("titulo", ""),
            })
            coleccion_aceptaciones.insert_one(entrada)
    else:  # política v1 (sin declaraciones)
        coleccion_aceptaciones.insert_one(dict(base))

    coleccion_tokens_aut.update_one(
        {"_id": doc["_id"]}, {"$set": {"usado_en": ahora, "resultado": "aceptado"}})
    return {"estado": "aceptado", "declaraciones_aceptadas": marcadas or []}


@ruta_conductores.post("/autorizacion/papel")
async def registrar_autorizacion_papel(
    archivo: UploadFile = File(...),
    placa: str = Form(...),
    cedula: str = Form(...),
    registrado_por: Optional[str] = Form(None),
):
    """Fallback en PAPEL: Seguridad sube la autorización firmada a mano; queda
    en el bucket privado y genera las entradas de evidencia canal «papel»."""
    import asyncio
    from rutas.vehiculos import subir_a_google_storage, _url_para_cliente

    cedula = _digitos_aut(cedula)
    if len(cedula) < 4:
        raise HTTPException(status_code=400, detail="Cédula no válida.")
    tipo = archivo.content_type or ""
    if not (tipo.startswith("image/") or tipo == "application/pdf"):
        raise HTTPException(status_code=400, detail="Sube una imagen o un PDF de la autorización firmada.")
    placa_limpia = placa.strip().upper()
    ext = "pdf" if tipo == "application/pdf" else "webp"
    fecha = datetime.now(_TZ_BOGOTA_AUT).strftime("%Y-%m-%d")
    # Nomenclatura del módulo: {PLACA}/{fecha}/autorizacionFisica_{cedula}_{placa}.{ext}
    nombre_blob = f"{placa_limpia}/{fecha}/autorizacionFisica_{cedula}_{placa_limpia.lower()}.{ext}"
    ruta = await asyncio.to_thread(subir_a_google_storage, archivo, nombre_blob)

    politica = _politica_vigente()
    ahora = datetime.utcnow()
    base = {
        "conductor_id": None,
        "sujeto_cedula": cedula,
        "sujeto_nombre": "",
        "sujeto_correo": "",
        "politica_id": politica["_id"],
        "version": politica.get("version"),
        "aceptado_en": ahora,
        "canal": "papel",
        "ip": "",
        "user_agent": "",
        "documento_ruta": ruta,
        "registrado_por": (registrado_por or "").strip(),
    }
    declaraciones = politica.get("declaraciones") or []
    if declaraciones:
        # El papel firma la política COMPLETA: una entrada por declaración con
        # el mismo documento de respaldo.
        for decl in declaraciones:
            entrada = dict(base)
            entrada.update({
                "declaracion_id": decl["id"],
                "declaracion_titulo": decl.get("titulo", ""),
            })
            coleccion_aceptaciones.insert_one(entrada)
    else:
        coleccion_aceptaciones.insert_one(dict(base))

    return {"estado": "registrado", "documento_ruta": ruta,
            "documento_url": _url_para_cliente(ruta)}


def enviar_autorizaciones_pendientes(vehiculo: dict, solicitado_por: str = "sistema") -> dict:
    """Envío AUTOMÁTICO al pasar el vehículo a revisión (2026-10-05): por cada
    PERSONA del estudio SIN autorización y CON correo en el formulario, un
    link (idempotente — no duplica correos en re-revisiones). Las empresas
    (NIT) no aplican; la dedup de personas ya la hace sujetos_estudio."""
    from Funciones import estudios_automaticos as ea

    correos: dict = {}
    for campo_doc, campo_correo in (
        ("condCedulaCiudadania", "condCorreo"),
        ("propDocumento", "propCorreo"),
        ("tenedDocumento", "tenedCorreo"),
        ("RemolDuenoDocumento", "RemolDuenoCorreo"),
    ):
        ced = _digitos_aut(vehiculo.get(campo_doc))
        correo = str(vehiculo.get(campo_correo) or "").strip()
        if ced and correo and ced not in correos:
            correos[ced] = correo

    resumen: dict = {"enviado": 0, "pendiente": 0, "ya_autorizado": 0, "sin_correo": []}
    for sujeto in ea.sujetos_estudio(vehiculo):
        if sujeto.get("tipo") != "persona":
            continue
        cedula = sujeto["cedula"]
        try:
            if _sujeto_ya_autorizado(cedula):
                resumen["ya_autorizado"] += 1
                continue
            correo = correos.get(cedula)
            if not correo:
                resumen["sin_correo"].append(cedula)
                continue
            r = _solicitar_autorizacion(vehiculo.get("placa", ""), cedula, correo,
                                        "", solicitado_por)
            estado = r.get("estado", "enviado")
            resumen[estado] = resumen.get(estado, 0) + 1
        except Exception as e:
            print(f"[autorizacion] No se pudo enviar a {cedula}: {e}")
    return resumen


class LoginComoInput(BaseModel):
    solicitante: Optional[str] = None   # nombre del usuario de Seguridad (cookie seguridadNombre)


@ruta_conductores.post("/login-como/{conductor_id}", response_model=dict)
async def login_como(conductor_id: str, data: Optional[LoginComoInput] = None, request: Request = None):
    """
    Impersonación de Seguridad (módulo Alta de vehículo): entra al panel de un
    conductor con cuenta EXISTENTE sin conocer ni tocar su clave. Devuelve el
    mismo shape que /login y deja auditoría append-only en `impersonaciones`.
    """
    from bson import ObjectId as _ObjectId
    try:
        oid = _ObjectId(conductor_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Cuenta no encontrada.")

    doc = coleccion_conductores.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=404, detail="Cuenta no encontrada.")

    perfil = (doc.get("perfil") or "CONDUCTOR").upper()
    # Cuentas stub de invitación pendiente: no tienen panel que abrir.
    if not doc.get("activo", True):
        raise HTTPException(
            status_code=403,
            detail="La cuenta está pendiente de activación (invitación de tenedor sin aceptar).",
        )

    solicitante = ((data.solicitante if data else None) or "Seguridad").strip() or "Seguridad"
    coleccion_impersonaciones.insert_one({
        "conductor_id": str(oid),
        "correo": doc.get("correo", ""),
        "perfil": perfil,
        "solicitante": solicitante,
        "fecha": datetime.now(timezone.utc),
        "ip": (request.client.host if request and request.client else ""),
        "user_agent": (request.headers.get("user-agent", "") if request else ""),
    })
    print(f"[login-como] {solicitante} entró al panel de {doc.get('correo')}")

    return {
        "mensaje": "Login como conductor exitoso",
        # Nombre de Seguridad que entró: el front lo usa para marcar la sesión
        # como impersonada (banner + editado_por en TODAS las mutaciones).
        "impersonado_por": solicitante,
        "politicas_pendientes": bool(
            doc.get("pendiente_aceptacion_politica")
            and not doc.get("aceptacion_politica")),
        "usuario": {
            "id": str(doc["_id"]),
            "correo": doc.get("correo", ""),
            "perfil": perfil,
            "primerNombre": (doc.get("nombre", "").strip() or "Conductor").split(" ")[0],
        },
    }


@ruta_conductores.get("/politica-actual", response_model=dict)
async def politica_actual():
    """Política vigente (pública) para el banner de aceptación del panel."""
    politica = _politica_vigente()
    if not politica:
        raise HTTPException(status_code=503, detail="No hay política vigente configurada.")
    return _politica_publica(politica)


class AceptarPoliticaSesionInput(BaseModel):
    conductor_id: str
    declaraciones_aceptadas: Optional[list] = None


@ruta_conductores.post("/aceptar-politica-sesion", response_model=dict)
async def aceptar_politica_sesion(data: AceptarPoliticaSesionInput, request: Request):
    """Aceptación de políticas DENTRO de la sesión (primer ingreso de una
    cuenta creada por Seguridad): misma evidencia trazable que la
    verificación por correo, canal 'alta_seguridad'."""
    from bson import ObjectId as _ObjectId
    try:
        oid = _ObjectId(data.conductor_id)
    except Exception:
        raise HTTPException(status_code=401, detail="Sesión inválida.")

    doc = coleccion_conductores.find_one({"_id": oid})
    if not doc:
        raise HTTPException(status_code=401, detail="Sesión inválida.")
    if not doc.get("pendiente_aceptacion_politica"):
        return {"estado": "ya_aceptada", "mensaje": "Las políticas ya fueron aceptadas."}

    politica = _politica_vigente()
    if not politica:
        raise HTTPException(status_code=503, detail="No hay política vigente configurada.")

    ids_declaraciones = _validar_declaraciones_completas(politica, data.declaraciones_aceptadas)
    _registrar_aceptacion(
        doc, politica, request, datetime.now(timezone.utc),
        canal="alta_seguridad", declaraciones_aceptadas=ids_declaraciones,
    )
    coleccion_conductores.update_one(
        {"_id": oid}, {"$set": {"pendiente_aceptacion_politica": False}})
    return {"estado": "aceptada", "mensaje": "Políticas aceptadas."}


# ==============================================================================
# 🔓 LOGIN
# ==============================================================================
@ruta_conductores.post("/login", response_model=dict)
async def login_conductor(usuario: str = Body(..., embed=True), clave: str = Body(..., embed=True)):
    usuario_ingresado = usuario.strip()
    clave_ingresada = clave.strip()
    # Regex anclado (^...$): antes matcheaba por subcadena y un correo
    # podía autenticarse como prefijo de otro.
    query_correo = {"correo": {"$regex": f"^{re.escape(usuario_ingresado)}$", "$options": "i"}}

    # 1. Conductor o Tenedor en la colección conductores.
    encontrado = coleccion_conductores.find_one(query_correo)
    perfil = (encontrado.get("perfil") or "CONDUCTOR").upper() if encontrado else "CONDUCTOR"

    # 2. Si no es conductor/tenedor, intentar ADMIN en baseusuarios (acceso de soporte al portal).
    if not encontrado:
        encontrado = coleccion_baseusuarios.find_one({**query_correo, "perfil": "ADMIN"})
        perfil = "ADMIN"

    if not encontrado:
        raise HTTPException(status_code=401, detail="Usuario o clave incorrectos")

    clave_almacenada = str(encontrado.get("clave", "")).strip()
    if not verificar_clave(clave_ingresada, clave_almacenada):
        raise HTTPException(status_code=401, detail="Usuario o clave incorrectos")

    # Cuentas stub de invitación pendiente (sin aceptar el enlace del tenedor).
    if perfil in ("CONDUCTOR", "TENEDOR") and not encontrado.get("activo", True):
        raise HTTPException(
            status_code=403,
            detail="Tu cuenta está pendiente de activación: abre el enlace que te enviamos por correo para elegir tu contraseña.",
        )

    # Exigir correo verificado a conductores y tenedores (los ADMIN de soporte no).
    if perfil in ("CONDUCTOR", "TENEDOR") and not encontrado.get("correo_verificado", False):
        raise HTTPException(
            status_code=403,
            detail="Tu correo aún no está verificado. Revisa tu bandeja de entrada (y spam) y abre el enlace de verificación.",
        )

    nombre_completo = encontrado.get("nombre", "").strip()
    primer_nombre = nombre_completo.split(" ")[0]

    # Cuentas creadas por Seguridad: el conductor debe aceptar las políticas
    # digitales en su primer ingreso (banner bloqueante del panel).
    politicas_pendientes = bool(
        perfil in ("CONDUCTOR", "TENEDOR")
        and encontrado.get("pendiente_aceptacion_politica")
        and not encontrado.get("aceptacion_politica")
    )

    return {
        "mensaje": "Login Conductor exitoso",
        "politicas_pendientes": politicas_pendientes,
        "usuario": {
            "id": str(encontrado["_id"]),
            "correo": encontrado.get("correo", ""),
            "perfil": perfil,
            "primerNombre": primer_nombre,
        },
    }


# ==============================================================================
# ✉️ VERIFICACIÓN DE CORREO
# ==============================================================================
@ruta_conductores.get("/verificar-correo", response_model=dict)
async def verificar_correo(token: str = ""):
    """
    Valida el token del enlace del correo SIN efectos en BD (idempotente):
    la cuenta solo se habilita en POST /aceptar-politica, tras aceptar la
    política de tratamiento de datos vigente.
    """
    token_plano = token.strip()
    if not token_plano:
        raise HTTPException(status_code=400, detail="Token de verificación vacío")

    doc = _buscar_conductor_por_token(token_plano)
    if not doc:
        raise HTTPException(
            status_code=400,
            detail="Enlace de verificación inválido, expirado o ya utilizado. Puedes reenviarlo desde el login.",
        )

    correo = doc.get("correo", "")
    if doc.get("correo_verificado"):
        return {
            "estado": "ya_verificado",
            "mensaje": "Tu correo ya fue verificado. Puedes iniciar sesión.",
            "correo": correo,
        }

    politica = _politica_vigente()
    if not politica:
        raise HTTPException(
            status_code=503,
            detail="No hay política de tratamiento de datos vigente configurada. Intenta más tarde.",
        )
    return {
        "estado": "pendiente_aceptacion",
        "correo": correo,
        "politica": _politica_publica(politica),
    }


class AceptarPoliticaInput(BaseModel):
    token: str
    version_politica: int
    acepta: bool
    # Modelo declaraciones: ids de las declaraciones marcadas como aceptadas.
    # Deben ser todas las EXIGIDAS de la política vigente (validado en el
    # endpoint; las de DECLARACIONES_NO_EXIGIDAS no bloquean).
    declaraciones_aceptadas: Optional[list] = None


def _validar_declaraciones_completas(politica: dict, declaraciones_aceptadas: Optional[list]):
    """
    Con el modelo de declaraciones (v2+): exige que el usuario haya aceptado
    todas las declaraciones EXIGIDAS de la política vigente (las de
    DECLARACIONES_NO_EXIGIDAS no bloquean). Retorna la lista saneada de ids
    MARCADOS (solo las realmente aceptadas, para evidencia honesta), o None
    si la política no usa declaraciones (modelo v1).
    """
    declaraciones = politica.get("declaraciones") or []
    if not declaraciones:
        return None
    ids_todas = [d["id"] for d in declaraciones]
    ids_exigidas = [i for i in ids_todas if i not in DECLARACIONES_NO_EXIGIDAS]
    marcadas = set(declaraciones_aceptadas or [])
    faltantes = [i for i in ids_exigidas if i not in marcadas]
    if faltantes:
        raise HTTPException(
            status_code=400,
            detail="Debes aceptar todas las declaraciones para continuar. Faltan: " + ", ".join(faltantes),
        )
    return [i for i in ids_todas if i in marcadas]


def _registrar_aceptacion(doc: dict, politica: dict, request: Request, ahora, canal: str, declaraciones_aceptadas: Optional[list] = None):
    """
    Registra la evidencia append-only de aceptación de política y habilita la
    cuenta (correo_verificado + activo). Compartido entre /aceptar-politica
    (registro propio) y /aceptar-invitacion (cuenta creada por un tenedor).

    Con el modelo de declaraciones (v2+): una entrada de evidencia POR
    declaración aceptada (declaracion_id + titulo) y en el conductor queda
    `declaraciones_aceptadas: [ids]` además del resumen de aceptación.
    """
    declaraciones = politica.get("declaraciones") or []
    ip = request.client.host if request.client else ""
    user_agent = (request.headers.get("user-agent") or "")[:300]

    if declaraciones:
        aceptacion_ids = []
        for decl in declaraciones:
            # Solo las que el usuario marcó; por diseño el endpoint valida que
            # estén todas las EXIGIDAS antes de llegar aquí (las no exigidas
            # solo dejan evidencia si se marcaron).
            if declaraciones_aceptadas is not None and decl["id"] not in declaraciones_aceptadas:
                continue
            evidencia = {
                "conductor_id": doc["_id"],
                "conductor_usuario": doc.get("correo", ""),
                "conductor_correo": doc.get("correo", ""),
                "conductor_nombre": doc.get("nombre", ""),
                "politica_id": politica["_id"],
                "version": politica.get("version"),
                "declaracion_id": decl["id"],
                "declaracion_titulo": decl.get("titulo", ""),
                "aceptado_en": ahora,
                "canal": canal,
                "ip": ip,
                "user_agent": user_agent,
            }
            aceptacion_ids.append(coleccion_aceptaciones.insert_one(evidencia).inserted_id)

        coleccion_conductores.update_one(
            {"_id": doc["_id"]},
            {"$set": {
                "correo_verificado": True,
                "activo": True,
                "aceptacion_politica": {
                    "version": politica.get("version"),
                    "politica_id": politica["_id"],
                    "aceptado_en": ahora,
                    "declaraciones_aceptadas": declaraciones_aceptadas or [d["id"] for d in declaraciones],
                },
                "declaraciones_aceptadas": declaraciones_aceptadas or [d["id"] for d in declaraciones],
            }},
        )
        return

    # Modelo v1 (política única, sin declaraciones): comportamiento original.
    aceptacion = {
        "conductor_id": doc["_id"],
        "conductor_usuario": doc.get("correo", ""),
        "conductor_correo": doc.get("correo", ""),
        "conductor_nombre": doc.get("nombre", ""),
        "politica_id": politica["_id"],
        "version": politica.get("version"),
        "aceptado_en": ahora,
        "canal": canal,
        "ip": ip,
        "user_agent": user_agent,
    }
    aceptacion_id = coleccion_aceptaciones.insert_one(aceptacion).inserted_id

    coleccion_conductores.update_one(
        {"_id": doc["_id"]},
        {"$set": {
            "correo_verificado": True,
            "activo": True,
            "aceptacion_politica": {
                "version": politica.get("version"),
                "politica_id": politica["_id"],
                "aceptado_en": ahora,
                "aceptacion_id": aceptacion_id,
            },
        }},
    )


@ruta_conductores.post("/aceptar-politica", response_model=dict)
async def aceptar_politica(data: AceptarPoliticaInput, request: Request):
    """
    Registra la aceptación de la política de datos (evidencia trazable) y
    habilita la cuenta. Único punto que escribe la verificación.
    """
    token_plano = (data.token or "").strip()
    if not token_plano:
        raise HTTPException(status_code=400, detail="Token de verificación vacío")

    doc = _buscar_conductor_por_token(token_plano)
    if not doc:
        raise HTTPException(
            status_code=400,
            detail="Enlace de verificación inválido, expirado o ya utilizado. Puedes reenviarlo desde el login.",
        )

    correo = doc.get("correo", "")

    # Consentimiento afirmado en el servidor, no solo en el front.
    if data.acepta is not True:
        raise HTTPException(
            status_code=400,
            detail="Debes aceptar las políticas de tratamiento de datos para continuar.",
        )

    # Idempotente: token ya aceptado no genera segunda evidencia.
    if doc.get("correo_verificado") and doc.get("aceptacion_politica"):
        return {"estado": "ya_verificado", "mensaje": "Tu correo ya fue verificado.", "correo": correo}

    politica = _politica_vigente()
    if not politica:
        raise HTTPException(
            status_code=503,
            detail="No hay política de tratamiento de datos vigente configurada. Intenta más tarde.",
        )

    # La política cambió entre el GET y este POST → devolver la nueva para re-aceptar.
    if data.version_politica != politica.get("version"):
        raise HTTPException(
            status_code=400,
            detail={
                "mensaje": "La política fue actualizada. Revísala y acéptala nuevamente.",
                "politica": _politica_publica(politica),
            },
        )

    # Modelo declaraciones: TODAS las de la política vigente deben venir marcadas.
    ids_declaraciones = _validar_declaraciones_completas(politica, data.declaraciones_aceptadas)

    ahora = datetime.now(timezone.utc)
    _registrar_aceptacion(
        doc, politica, request, ahora, canal="verificacion_correo",
        declaraciones_aceptadas=ids_declaraciones,
    )

    return {
        "estado": "verificado",
        "mensaje": "Correo verificado y declaraciones aceptadas. Ya puedes iniciar sesión.",
        "correo": correo,
        "version_politica": politica.get("version"),
    }


class ReenviarVerificacionInput(BaseModel):
    correo: str


@ruta_conductores.post("/reenviar-verificacion", response_model=dict)
async def reenviar_verificacion(data: ReenviarVerificacionInput, background_tasks: BackgroundTasks):
    """
    Reenvía el correo de verificación. Respuesta neutra: no revela si la cuenta
    existe ni si ya está verificada (evita enumeración de correos).
    """
    correo_norm = (data.correo or "").strip()
    patron = {"$regex": f"^{re.escape(correo_norm)}$", "$options": "i"}
    doc = coleccion_conductores.find_one(patron)

    if doc and not doc.get("correo_verificado", False):
        # Regenerar token (invalida el anterior) y reenviar.
        token = _generar_token_verificacion(doc["_id"])
        enlace = f"{FRONTEND_URL_VERIFICAR}?token={token}"
        correo_destino = doc.get("correo") or correo_norm
        background_tasks.add_task(
            enviar_correo_verificacion, correo_destino, enlace, (doc.get("nombre") or "").strip()
        )

    return {"mensaje": "Si el correo está pendiente de verificación, se envió un nuevo enlace."}


# ==============================================================================
# 👥 INVITACIÓN DE CONDUCTOR POR PARTE DEL TENEDOR
# ==============================================================================
class InvitarConductorInput(BaseModel):
    id_tenedor: str
    placa: str
    correo_conductor: str
    nombre_conductor: Optional[str] = None
    # Celular (WhatsApp) del conductor invitado (2026-10-02): lo pide el popup
    # de invitación en el paso 2; queda en la cuenta stub para notificaciones.
    celular_conductor: Optional[str] = None


def _correo_patron(correo: str) -> dict:
    """Query de correo exacto case-insensitive (anclada, con el campo incluido)."""
    return {"correo": {"$regex": f"^{re.escape((correo or '').strip())}$", "$options": "i"}}


def enviar_correo_invitacion(destinatario: str, enlace: str, nombre: str, placa: str, tenedor: str):
    """Correo al conductor invitado: link para activar cuenta y aceptar política."""
    if not resend.api_key or "TuApiKeyAqui" in resend.api_key:
        print("⚠️ ERROR: Falta API KEY de Resend; no se envió la invitación.")
        return
    html = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 520px; margin: 0 auto;">
      <h2 style="color: #0f1928;">¡Hola{(', ' + nombre) if nombre else ''}!</h2>
      <p><strong>{tenedor}</strong> te invitó a operar el vehículo de placa
      <strong>{placa}</strong> en IntegrApp.</p>
      <p>Para activar tu cuenta de conductor, elige tu contraseña y acepta nuestras
      Políticas de Tratamiento de Datos Personales (Habeas Data):</p>
      <p style="text-align: center; margin: 28px 0;">
        <a href="{enlace}"
           style="background: #0f1928; color: #fff; padding: 12px 28px; border-radius: 10px;
                  text-decoration: none; font-weight: bold;">
          Activar mi cuenta
        </a>
      </p>
      <p>O copia y pega este enlace en tu navegador:</p>
      <p><a href="{enlace}">{enlace}</a></p>
      <p><small>El enlace vence en {EXPIRA_HORAS_VERIFICACION} horas. Si no esperabas esta
      invitación, ignora este mensaje.</small></p>
    </div>
    """
    try:
        resend.Emails.send({
            "from": MAIL_FROM,
            "to": [destinatario],
            "subject": f"Te invitaron a conducir el vehículo {placa} — IntegrApp",
            "html": html,
        })
        print(f"📧 Correo de invitación enviado a {destinatario}")
    except Exception as e:
        print(f"❌ Error enviando invitación: {e}")


@ruta_conductores.post("/invitar-conductor", response_model=dict)
async def invitar_conductor(data: InvitarConductorInput, background_tasks: BackgroundTasks):
    """
    El tenedor invita a un conductor para su placa:
    - Si ya tiene cuenta CONDUCTOR activa → se vincula directo.
    - Si no → se crea cuenta stub (inactiva, sin clave usable) y se envía
      el correo de activación; al aceptar queda vinculada.
    """
    placa = (data.placa or "").strip().upper()
    correo_invitado = (data.correo_conductor or "").strip()
    if not correo_invitado or "@" not in correo_invitado:
        raise HTTPException(status_code=400, detail="Correo del conductor inválido.")

    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    if str(vehiculo.get("idUsuario", "")) != str(data.id_tenedor):
        raise HTTPException(status_code=403, detail="El vehículo no pertenece a este tenedor.")

    existente = coleccion_conductores.find_one(_correo_patron(correo_invitado))

    if existente:
        perfil_existente = (existente.get("perfil") or "CONDUCTOR").upper()
        if perfil_existente == "TENEDOR":
            raise HTTPException(status_code=400, detail="Ese correo pertenece a un tenedor; no puede ser conductor.")
        if existente.get("activo", True) and existente.get("correo_verificado", False):
            # Cuenta viva → vinculación directa.
            coleccion_vehiculos.update_one(
                {"placa": placa},
                {"$set": {
                    "idConductor": str(existente["_id"]),
                    "invitacionConductor": {
                        "correo": existente.get("correo", ""),
                        "estado": "aceptada",
                        "creado_en": datetime.now(timezone.utc),
                    },
                }},
            )
            return {
                "estado": "vinculado",
                "mensaje": "El conductor ya tenía cuenta: quedó vinculado al vehículo.",
            }
        # Cuenta stub previa de otra invitación → regenerar token y reenviar.

    # Crear (o reusar) cuenta stub: sin clave usable hasta que el conductor
    # la elija en la página de aceptación.
    ahora = datetime.now(timezone.utc)
    celular_invitado = (data.celular_conductor or "").strip() or None
    if not existente:
        doc_stub = {
            "nombre": (data.nombre_conductor or correo_invitado.split("@")[0]).upper(),
            "correo": correo_invitado.upper(),
            "cedula": None,
            "regional": "N/A",
            "celular": celular_invitado,
            "perfil": "CONDUCTOR",
            "clave": crear_hash(secrets.token_urlsafe(24)),  # aleatoria: nadie la conoce
            "clientes": [],
            "activo": False,          # login bloqueado hasta aceptar
            "correo_verificado": False,
            "invitado_por": str(data.id_tenedor),
        }
        stub_id = coleccion_conductores.insert_one(doc_stub).inserted_id
    else:
        stub_id = existente["_id"]
        cambios_stub = {"invitado_por": str(data.id_tenedor)}
        if celular_invitado:  # actualiza el celular si la invitación lo trae
            cambios_stub["celular"] = celular_invitado
        coleccion_conductores.update_one(
            {"_id": stub_id},
            {"$set": cambios_stub},
        )

    token = _generar_token_verificacion(stub_id)
    enlace = f"{FRONTEND_URL_INVITACION}?token={token}&placa={placa}"

    # Nombre del tenedor para el correo (si existe su cuenta).
    doc_tenedor = None
    try:
        from bson import ObjectId as _ObjectId
        doc_tenedor = coleccion_conductores.find_one({"_id": _ObjectId(data.id_tenedor)})
    except Exception:
        doc_tenedor = coleccion_conductores.find_one({"_id": data.id_tenedor})
    nombre_tenedor = (doc_tenedor or {}).get("nombre", "") or "Integra"

    coleccion_vehiculos.update_one(
        {"placa": placa},
        {"$set": {
            "invitacionConductor": {
                "correo": correo_invitado.upper(),
                "estado": "pendiente",
                "creado_en": ahora,
                "expira": datetime.now(timezone.utc) + timedelta(hours=EXPIRA_HORAS_VERIFICACION),
            },
        }},
    )

    background_tasks.add_task(
        enviar_correo_invitacion, correo_invitado, enlace,
        (data.nombre_conductor or "").strip(), placa, nombre_tenedor,
    )
    return {
        "estado": "invitado",
        "mensaje": f"Invitación enviada a {correo_invitado}. El conductor quedará vinculado al aceptar.",
    }


class ReenviarInvitacionInput(BaseModel):
    id_tenedor: str
    placa: str


@ruta_conductores.post("/reenviar-invitacion", response_model=dict)
async def reenviar_invitacion(data: ReenviarInvitacionInput, background_tasks: BackgroundTasks):
    """Regenera el token de la invitación pendiente de una placa y reenvía el correo."""
    placa = (data.placa or "").strip().upper()
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    if str(vehiculo.get("idUsuario", "")) != str(data.id_tenedor):
        raise HTTPException(status_code=403, detail="El vehículo no pertenece a este tenedor.")

    invitacion = vehiculo.get("invitacionConductor") or {}
    if vehiculo.get("idConductor"):
        raise HTTPException(status_code=400, detail="Esa placa ya tiene un conductor vinculado.")
    if not invitacion.get("correo"):
        raise HTTPException(status_code=400, detail="No hay invitación pendiente para esta placa.")

    stub = coleccion_conductores.find_one(_correo_patron(invitacion["correo"]))
    if not stub:
        raise HTTPException(status_code=404, detail="No se encontró la cuenta del conductor invitado.")

    token = _generar_token_verificacion(stub["_id"])
    enlace = f"{FRONTEND_URL_INVITACION}?token={token}&placa={placa}"
    coleccion_vehiculos.update_one(
        {"placa": placa},
        {"$set": {"invitacionConductor.expira": datetime.now(timezone.utc) + timedelta(hours=EXPIRA_HORAS_VERIFICACION)}},
    )
    background_tasks.add_task(
        enviar_correo_invitacion, invitacion["correo"], enlace, "", placa, "Integra",
    )
    return {"mensaje": "Invitación reenviada."}


class AceptarInvitacionInput(BaseModel):
    token: str
    placa: str
    clave: str
    version_politica: int
    acepta: bool
    declaraciones_aceptadas: Optional[list] = None
    celular: Optional[str] = None
    cedula: Optional[str] = None


@ruta_conductores.post("/aceptar-invitacion", response_model=dict)
async def aceptar_invitacion(data: AceptarInvitacionInput, request: Request):
    """
    El conductor invitado activa su cuenta: valida el token, registra la
    aceptación de política (evidencia), fija su clave y queda vinculado a la placa.
    """
    token_plano = (data.token or "").strip()
    placa = (data.placa or "").strip().upper()
    if not token_plano:
        raise HTTPException(status_code=400, detail="Token de invitación vacío")

    doc = _buscar_conductor_por_token(token_plano)
    if not doc:
        raise HTTPException(
            status_code=400,
            detail="Enlace de invitación inválido o expirado. Pide al tenedor que te reenvíe la invitación.",
        )

    if data.acepta is not True:
        raise HTTPException(status_code=400, detail="Debes aceptar las políticas de tratamiento de datos para continuar.")

    clave_plana = (data.clave or "").strip()
    if len(clave_plana) < 6:
        raise HTTPException(status_code=400, detail="La clave debe tener al menos 6 caracteres")

    politica = _politica_vigente()
    if not politica:
        raise HTTPException(status_code=503, detail="No hay política de tratamiento de datos vigente configurada.")
    if data.version_politica != politica.get("version"):
        raise HTTPException(
            status_code=400,
            detail={
                "mensaje": "La política fue actualizada. Revísala y acéptala nuevamente.",
                "politica": _politica_publica(politica),
            },
        )

    # Modelo declaraciones: TODAS las de la política vigente deben venir marcadas.
    ids_declaraciones = _validar_declaraciones_completas(politica, data.declaraciones_aceptadas)

    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")

    ahora = datetime.now(timezone.utc)

    # Evidencia de política + cuenta habilitada.
    _registrar_aceptacion(
        doc, politica, request, ahora, canal="invitacion_tenedor",
        declaraciones_aceptadas=ids_declaraciones,
    )

    # Clave elegida por el conductor + datos adicionales.
    updates: dict = {"clave": crear_hash(clave_plana)}
    if data.celular:
        updates["celular"] = re.sub(r"\D", "", data.celular)
    if data.cedula:
        updates["cedula"] = re.sub(r"\D", "", data.cedula)
    coleccion_conductores.update_one({"_id": doc["_id"]}, {"$set": updates})

    # Vinculación al vehículo.
    coleccion_vehiculos.update_one(
        {"placa": placa},
        {"$set": {
            "idConductor": str(doc["_id"]),
            "invitacionConductor": {
                "correo": doc.get("correo", ""),
                "estado": "aceptada",
                "creado_en": (vehiculo.get("invitacionConductor") or {}).get("creado_en", ahora),
                "aceptada_en": ahora,
            },
        }},
    )

    return {
        "estado": "aceptada",
        "mensaje": "Cuenta activada y vehículo vinculado. Ya puedes iniciar sesión.",
        "correo": doc.get("correo", ""),
    }


class DesvincularConductorInput(BaseModel):
    id_tenedor: str
    placa: str


@ruta_conductores.put("/desvincular-conductor", response_model=dict)
async def desvincular_conductor(data: DesvincularConductorInput):
    """El tenedor quita al conductor (o la invitación pendiente) de una placa."""
    placa = (data.placa or "").strip().upper()
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    if str(vehiculo.get("idUsuario", "")) != str(data.id_tenedor):
        raise HTTPException(status_code=403, detail="El vehículo no pertenece a este tenedor.")
    if not vehiculo.get("idConductor") and not (vehiculo.get("invitacionConductor") or {}).get("correo"):
        raise HTTPException(status_code=400, detail="Esa placa no tiene conductor ni invitación.")

    coleccion_vehiculos.update_one(
        {"placa": placa},
        {"$set": {"idConductor": None, "invitacionConductor": None}},
    )
    return {"mensaje": "Conductor desvinculado del vehículo."}


# ==============================================================================
# 📜 POLÍTICA DE DATOS — CONSULTA PÚBLICA Y ADMINISTRACIÓN
# ==============================================================================
@ruta_conductores.get("/politica-datos", response_model=dict)
async def obtener_politica_datos():
    """Política vigente (auto-siembra la v1 si la colección está vacía)."""
    politica = _politica_vigente()
    if not politica:
        raise HTTPException(status_code=503, detail="No hay política de datos vigente configurada")
    return _politica_publica(politica)


def _requiere_admin(usuario: str) -> dict:
    """Valida que `usuario` exista en baseusuarios con perfil ADMIN (patrón del proyecto)."""
    user = coleccion_baseusuarios.find_one({"usuario": (usuario or "").strip().upper()})
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    if (user.get("perfil") or "").upper() != "ADMIN":
        raise HTTPException(
            status_code=403,
            detail=f"Su perfil ({user.get('perfil')}) no tiene permiso para administrar políticas.",
        )
    return user


class NuevaPoliticaInput(BaseModel):
    usuario: str
    titulo: str
    texto_html: str


@ruta_conductores.post("/politica-datos", response_model=dict, status_code=201)
async def crear_politica_datos(data: NuevaPoliticaInput):
    """Publica una nueva versión (version = max+1) y la deja como vigente."""
    _requiere_admin(data.usuario)

    titulo = (data.titulo or "").strip()
    texto = (data.texto_html or "").strip()
    if not titulo or not texto:
        raise HTTPException(status_code=400, detail="titulo y texto_html son obligatorios")

    ultima = coleccion_politicas.find_one(sort=[("version", -1)])
    nueva_version = (ultima.get("version", 0) or 0) + 1 if ultima else 1

    # Invariante: una sola versión activa.
    coleccion_politicas.update_many({"activo": True}, {"$set": {"activo": False}})
    doc = {
        "version": nueva_version,
        "titulo": titulo,
        "texto_html": texto,
        "activo": True,
        "publicado_en": datetime.now(timezone.utc),
        "publicado_por": (data.usuario or "").strip().upper(),
    }
    coleccion_politicas.insert_one(doc)
    return {"version": nueva_version, "titulo": titulo, "activo": True}


@ruta_conductores.get("/politica-datos/historial", response_model=list)
async def historial_politicas(usuario: str = ""):
    """Versiones publicadas (sin texto_html, es pesado). Solo ADMIN."""
    _requiere_admin(usuario)
    docs = coleccion_politicas.find({}, {"texto_html": 0}).sort("version", -1)
    return [
        {
            "version": d.get("version"),
            "titulo": d.get("titulo", ""),
            "activo": d.get("activo", False),
            "publicado_en": d.get("publicado_en"),
            "publicado_por": d.get("publicado_por", ""),
        }
        for d in docs
    ]


class ActivarPoliticaInput(BaseModel):
    usuario: str
    version: int


@ruta_conductores.put("/politica-datos/activar", response_model=dict)
async def activar_politica_datos(data: ActivarPoliticaInput):
    """Reactiva una versión histórica (desactivando la vigente). Solo ADMIN."""
    _requiere_admin(data.usuario)

    doc = coleccion_politicas.find_one({"version": data.version})
    if not doc:
        raise HTTPException(status_code=404, detail=f"No existe la versión {data.version}")

    coleccion_politicas.update_many({"activo": True}, {"$set": {"activo": False}})
    coleccion_politicas.update_one({"_id": doc["_id"]}, {"$set": {"activo": True}})
    return {"version": data.version, "activo": True}



@ruta_conductores.post("/recuperar/verificar", response_model=dict)
async def recuperar_verificar(data: VerificarInput, background_tasks: BackgroundTasks):
    # El front envía el correo en el campo `usuario` (compatibilidad de body).
    doc = _buscar_por_usuario(data.usuario)
    if not doc:
        return {"existe": False}

    codigo = str(random.randint(1000, 9999))
    coleccion_conductores.update_one({"_id": doc["_id"]}, {"$set": {"recovery_code": codigo}})

    correo_destino = doc.get("correo") or data.usuario.strip()
    background_tasks.add_task(enviar_correo_codigo, correo_destino, codigo)

    return {"existe": True, "mensaje": "Código generado"}


@ruta_conductores.post("/recuperar/validar", response_model=dict)
async def recuperar_validar(data: ValidarCodigoInput):
    doc = _buscar_por_usuario(data.usuario)
    if not doc:
        return {"valido": False}

    codigo_guardado = doc.get("recovery_code")
    es_valido = (codigo_guardado is not None) and (codigo_guardado == data.codigo)
    return {"valido": es_valido}


@ruta_conductores.post("/recuperar/cambiar", response_model=dict)
async def recuperar_cambiar(data: CambioClaveInput):
    doc = _buscar_por_usuario(data.usuario)
    if not doc:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")

    codigo_guardado = doc.get("recovery_code")
    if not codigo_guardado or codigo_guardado != data.codigo:
        raise HTTPException(status_code=403, detail="Código inválido o expirado")

    coleccion_conductores.update_one(
        {"_id": doc["_id"]},
        {"$set": {"clave": crear_hash(data.nuevaClave.strip())}, "$unset": {"recovery_code": ""}},
    )
    return {"mensaje": "Clave actualizada correctamente"}
