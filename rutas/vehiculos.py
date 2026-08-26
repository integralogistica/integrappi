import os
import json
import asyncio
from datetime import datetime
from io import BytesIO
from typing import List, Optional
import resend
from dotenv import load_dotenv
from fastapi import APIRouter, File, Form, HTTPException,Response, UploadFile, status
from fastapi.responses import JSONResponse
from google.cloud import storage
from PIL import Image
from pymongo import MongoClient
from bson import ObjectId
import re
import requests
import base64
import pdfplumber
import pytz
from PIL import Image
import io

# ==========================================
# Carga de variables de entorno
# ==========================================
load_dotenv()

# Configuración MongoDB
MONGO_URI = os.getenv("MONGO_URI")
if not MONGO_URI:
    raise ValueError("La variable de entorno MONGO_URI no está configurada.")

# Configuración Google Cloud
BUCKET_NAME = "integrapp"
CARPETA_STORAGE = "Vehiculos"
GOOGLE_CREDENTIALS_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
# Zona Colombia para las fechas de la nomenclatura del bucket (el server es UTC).
_TZ_BOGOTA = pytz.timezone("America/Bogota")

if GOOGLE_CREDENTIALS_PATH:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_CREDENTIALS_PATH

# --- CONFIGURACIÓN RESEND ---
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
MAIL_FROM = os.getenv("MAIL_FROM", "no-reply@integralogistica.com")

if RESEND_API_KEY:
    resend.api_key = RESEND_API_KEY
else:
    print("⚠️ ADVERTENCIA: RESEND_API_KEY no encontrada en .env")

# --- CONFIGURACIÓN LLM (lectura de documentos con Gemini) ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.6-flash")
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"

# ==========================================
# Configuración base de datos
# ==========================================
from bd.bd_cliente import bd_cliente
bd = bd_cliente['integra']

# --- COLECCIONES ---
coleccion_vehiculos = bd['vehiculos']
coleccion_disponibilidades = bd['disponibilidades']
# Cuentas de conductor/tenedor (para propagar la cédula leída por IA).
coleccion_conductores_cuenta = bd['conductores']
coleccion_usuarios = bd['usuarios']         # Conductores / Usuarios app
coleccion_baseusuarios = bd['baseusuarios'] # <--- AQUÍ ESTÁN LOS PERFILES DE SEGURIDAD

# Router
ruta_vehiculos = APIRouter(
    prefix="/vehiculos",
    tags=['Vehiculos'],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}}
)

# ==========================================
# 2. FUNCIONES AUXILIARES
# ==========================================

def optimizar_imagen(archivo: UploadFile, formato: str = "WEBP", max_width: int = 1200, max_height: int = 800) -> BytesIO:
    try:
        imagen = Image.open(archivo.file)
        imagen.thumbnail((max_width, max_height), Image.Resampling.LANCZOS)
        buffer = BytesIO()
        imagen.save(buffer, format=formato, optimize=True, quality=75)
        buffer.seek(0)
        return buffer
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error al optimizar la imagen: {str(e)}")

def subir_a_google_storage(archivo: UploadFile, nombre_archivo: str) -> str:
    try:
        cliente = storage.Client()
        bucket = cliente.bucket(BUCKET_NAME)
        ruta_archivo = f"{CARPETA_STORAGE}/{nombre_archivo}"

        if archivo.content_type.startswith("image/"):
            archivo_optimizado = optimizar_imagen(archivo)
            blob = bucket.blob(ruta_archivo)
            blob.upload_from_file(archivo_optimizado, content_type="image/webp")
        else:
            blob = bucket.blob(ruta_archivo)
            archivo.file.seek(0)
            blob.upload_from_file(archivo.file, content_type=archivo.content_type)

        return f"https://storage.googleapis.com/{BUCKET_NAME}/{ruta_archivo}"
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al subir el archivo a Google Storage: {str(e)}")


def _nombre_doc_bucket(placa: str, tipo: str, extension: str, vehiculo: dict = None, sufijo: str = "") -> str:
    """
    Nomenclatura estándar de archivos en el bucket (2026-08-27):
        {PLACA}/{AAAA-MM-DD}/{tipo}{_cedula}{sufijo}.{ext}
    Ej: Vehiculos/MX48E/2026-08-27/soat_1020304050.pdf
    — Agrupado por placa → fecha → documento; la cédula (si ya se conoce)
    identifica al conductor; si no, se omite (ej. la primera subida de la
    cédula se hace ANTES de que la IA lea el número). Re-subir el mismo doc
    el mismo día pisa el archivo (sin duplicados); otro día crea versión
    nueva y Mongo queda con la URL vigente (la anterior queda como histórico).
    """
    fecha = datetime.now(_TZ_BOGOTA).strftime("%Y-%m-%d")
    cedula = re.sub(r"\D", "", str((vehiculo or {}).get("condCedulaCiudadania") or ""))
    identificador = f"_{cedula}" if cedula else ""
    return f"{placa.strip().upper()}/{fecha}/{tipo}{identificador}{sufijo}.{extension}"

def eliminar_de_google_storage(url: str):
    try:
        cliente = storage.Client()
        bucket = cliente.bucket(BUCKET_NAME)
        nombre_archivo = url.split(f"https://storage.googleapis.com/{BUCKET_NAME}/")[-1]
        blob = bucket.blob(nombre_archivo)
        blob.delete()
    except Exception as e:
        print(f"Advertencia al eliminar archivo: {str(e)}")


# ==========================================
# 2b. LECTURA DE DOCUMENTOS CON LLM (Gemini)
# ==========================================
# Esquemas por tipo de documento: el LLM devuelve SOLO estas claves.
# Extensible: agregar entrada aquí + campo en el prompt = nuevo documento soportado.
ESQUEMAS_EXTRACCION = {
    "cedula": {
        "campos": {
            "numero": "Número de la cédula (solo dígitos)",
            "nombres": "Nombres de pila (como aparecen, en MAYÚSCULAS)",
            "apellidos": "Apellidos (como aparecen, en MAYÚSCULAS)",
            "fecha_nacimiento": "Fecha de nacimiento en formato YYYY-MM-DD",
            "fecha_expedicion": "Fecha de expedición en formato YYYY-MM-DD",
            "lugar_expedicion": "Municipio/Ciudad de expedición (solo el nombre)",
            "departamento_expedicion": "Departamento de expedición (solo el nombre)",
            "rh": "Grupo sanguíneo con RH (ej: O+, A-)",
        },
        "descripcion": (
            "Cédula de Ciudadanía colombiana. Dos formatos posibles: "
            "(a) azul (posterior a 2020), una sola cara con todos los datos incluidos fecha y lugar de expedición; "
            "(b) amarilla hologramada (anterior a 2020), los datos de identidad están en el anverso (frente) y la fecha/lugar de expedición en el reverso. "
            "Puede llegar 1 imagen (frente, o frente de la azul) o 2 imágenes (frente y reverso de la amarilla)."
        ),
    },
    "rut": {
        "campos": {
            "tipo_persona": "PERSONA NATURAL o PERSONA JURIDICA",
            "razon_social": "Razón social completa en MAYÚSCULAS (solo si es persona jurídica)",
            "nombres": "Nombres de pila tal como aparecen (solo persona natural)",
            "apellidos": "Primer y segundo apellido tal como aparecen (solo persona natural)",
            "numero_documento": "NIT sin dígito de verificación o cédula, SOLO dígitos",
            "digito_verificacion": "Dígito de verificación del NIT (un dígito) o null",
            "direccion": "Dirección principal (campo 41)",
            "ciudad": "Municipio (campo 40)",
            "departamento": "Departamento (campo 39)",
            "correo": "Correo electrónico (campo 42)",
            "telefono": "Teléfono, SOLO dígitos (campo 44)",
            "responsabilidades": "Lista de códigos DIAN de responsabilidades (ej: [\"05\",\"22\",\"49\"])",
        },
        "descripcion": (
            "Registro Único Tributario (RUT) colombiano de la DIAN. "
            "Puede llegar como PDF digital (texto), PDF escaneado o foto. "
            "Los números pueden venir impresos con espacios entre dígitos."
        ),
    },
    "certificado_bancario": {
        "campos": {
            "banco": "Nombre del banco emisor",
            "tipo_cuenta": "AHORROS o CORRIENTE",
            "numero_cuenta": "Número de cuenta, SOLO dígitos",
            "titular": "Nombre del titular de la cuenta",
            "documento_titular": "Número de documento (cédula o NIT) del titular, SOLO dígitos",
        },
        "descripcion": (
            "Certificación bancaria colombiana (constancia de cuenta activa emitida por el banco). "
            "Puede llegar como PDF o foto. El número de cuenta suele aparecer como 'No. Cuenta' o 'Número de cuenta'."
        ),
    },
    "licencia": {
        "campos": {
            "numero": "Número de la licencia de conducción, SOLO dígitos",
            "categoria": "Categoría (A1, A2, B1, B2, B3, C1, C2 o C3)",
            "fecha_vencimiento": "Fecha de vencimiento en formato YYYY-MM-DD",
            "nombre_completo": "Nombre completo del conductor tal como aparece",
            "cedula": "Número de cédula del conductor, SOLO dígitos",
        },
        "descripcion": (
            "Licencia de conducción colombiana. Tiene DOS caras: los datos "
            "principales (número, categoría, vencimiento) están en el FRENTE; "
            "el reverso aporta datos complementarios. Puede llegar 1 imagen "
            "(frente) o 2 (frente y reverso)."
        ),
    },
    "tarjeta_propiedad": {
        "campos": {
            "placa": "Placa del vehículo, SOLO letras, números y guiones",
            "marca": "Marca del vehículo",
            "linea": "Línea o referencia del vehículo",
            "modelo": "Año del modelo (4 dígitos)",
            "color": "Color del vehículo",
            "numero_serie": "Número de serie/chasis, SOLO caracteres alfanuméricos",
            "propietario_nombre": "Nombre o razón social del propietario registrado",
            "propietario_documento": "Cédula o NIT del propietario, SOLO dígitos",
        },
        "descripcion": (
            "Tarjeta de propiedad (registro RUNT) de un vehículo colombiano. "
            "Tiene DOS caras: los datos del vehículo en el FRENTE; el reverso "
            "tiene información del propietario y restricciones. Puede llegar "
            "1 imagen (frente) o 2 (frente y reverso)."
        ),
    },
    "soat": {
        "campos": {
            "aseguradora": "Nombre de la aseguradora emisor",
            "numero_poliza": "Número de póliza SOAT",
            "fecha_vencimiento": "Fecha de vencimiento en formato YYYY-MM-DD",
            "placa": "Placa del vehículo asegurado, SOLO letras, números y guiones",
        },
        "descripcion": "SOAT (Seguro Obligatorio de Accidentes de Tránsito) colombiano. PDF o foto.",
    },
}


def _llamar_gemini(parts: list, instruction: str) -> str:
    """Llama a Gemini con las parts (texto/imagen) y devuelve el texto de respuesta."""
    if not GEMINI_API_KEY:
        raise HTTPException(status_code=503, detail="Servicio de lectura de documentos no configurado (falta API key).")

    cuerpo = {
        "systemInstruction": {"parts": [{"text": instruction}]},
        "contents": [{"role": "user", "parts": parts}],
        "generationConfig": {
            "temperature": 0,          # lectura determinista
            "responseMimeType": "application/json",
            # OJO: en Gemini 3.x los tokens de "thinking" cuentan dentro de este
            # presupuesto (una lectura puede pensar ~1800 tokens). Con 2048 el JSON
            # salía truncado a mitad. 8192 cubre thinking + respuesta sobrado.
            "maxOutputTokens": 8192,
        },
    }
    try:
        respuesta = requests.post(
            f"{GEMINI_URL}?key={GEMINI_API_KEY}",
            json=cuerpo,
            timeout=45,
        )
    except requests.RequestException:
        raise HTTPException(status_code=504, detail="El servicio de lectura no respondió. Intenta de nuevo.")

    if respuesta.status_code == 429:
        raise HTTPException(status_code=429, detail="El servicio de lectura está saturado o sin crédito. Intenta más tarde.")
    if respuesta.status_code != 200:
        print(f"⚠️ Gemini error {respuesta.status_code}: {respuesta.text[:300]}")
        raise HTTPException(status_code=502, detail="Error del servicio de lectura de documentos.")

    candidatos = respuesta.json().get("candidates", [])
    if not candidatos:
        raise HTTPException(status_code=502, detail="El servicio no devolvió resultados para este documento.")
    return "".join(p.get("text", "") for p in candidatos[0].get("content", {}).get("parts", []))


def _prompt_extraccion(tipo_doc: str) -> tuple[str, dict]:
    """Arma el prompt estricto para el tipo de documento. Retorna (instruction, esquema)."""
    esquema = ESQUEMAS_EXTRACCION[tipo_doc]
    lista_campos = "\n".join(f'  "{k}": {v}' for k, v in esquema["campos"].items())
    instruction = f"""Eres un extractor de datos de documentos colombianos. Recibes la imagen de: {esquema["descripcion"]}

Devuelve EXCLUSIVAMENTE un objeto JSON con estas claves (nada más, sin markdown, sin explicaciones):
{{
{lista_campos}
  "documento_valido": true si la imagen corresponde realmente a {tipo_doc}; false si es cualquier otra cosa (foto personal, meme, factura, otro documento, imagen sin relación)
}}

Reglas estrictas:
- Si un dato NO se ve o no es legible en la imagen, su valor debe ser null. NUNCA inventes ni completes con datos plausibles.
- No incluyas claves adicionales.
- Corrige obvios errores de OCR (ej: l vs 1, O vs 0) usando el contexto del documento.
- Fechas siempre YYYY-MM-DD.
- Los números pueden venir impresos con espacios o guiones entre dígitos (ej: "1 1 2 0 0 4 2 7 1"): únelos y devuelve el número continuo (ej: "112004271")."""
    return instruction, esquema


def _extraer_texto_pdf(datos: bytes) -> str:
    """
    Extrae el texto de un PDF digital con pdfplumber (hasta 3 páginas, ~8000 chars).
    Retorna "" si el PDF no tiene texto sustancial (escaneado).
    Errores de parseo → "" (el llamador cae a mandar el PDF inline al LLM).
    """
    try:
        textos = []
        with pdfplumber.open(BytesIO(datos)) as pdf:
            for pagina in pdf.pages[:3]:
                texto = pagina.extract_text() or ""
                if texto.strip():
                    textos.append(texto.strip())
        return "\n".join(textos)[:8000]
    except Exception as e:
        # Sin emoji: en consolas Windows (cp1252) el print con emoji revienta y
        # rompería el propio except. ASCII siempre es seguro.
        print(f"[pdfplumber] No pudo leer el PDF ({e}); se envia inline al LLM.")
        return ""


def _reencodear_imagen_para_llm(datos: bytes) -> bytes:
    """
    Re-encodea una imagen muy pesada (JPEG q80, máx 1600px) para que quepa en
    el límite de 6 MB de Gemini. Si falla, devuelve los bytes originales
    (el llamador decidirá si rechaza con 400).
    """
    try:
        imagen = Image.open(io.BytesIO(datos))
        imagen.thumbnail((1600, 1600), Image.Resampling.LANCZOS)
        buffer = io.BytesIO()
        imagen.save(buffer, format="JPEG", optimize=True, quality=80)
        return buffer.getvalue()
    except Exception as e:
        print(f"[lecturaIA] No se pudo re-encodear la imagen para el LLM: {e}")
        return datos


def extraer_datos_con_llm(tipo_doc: str, archivos: list[UploadFile]) -> dict:
    """
    Envía 1 o 2 archivos (imagen o PDF) al LLM y retorna el JSON de datos.
    Los PDF digitales van como texto (más barato); los escaneados inline como imagen.
    La respuesta siempre se trata como sugerencia: el conductor corrige en el front.
    """
    if tipo_doc not in ESQUEMAS_EXTRACCION:
        raise HTTPException(status_code=400, detail=f"Tipo de documento no soportado para lectura: {tipo_doc}")

    instruction, esquema = _prompt_extraccion(tipo_doc)

    parts = []
    for archivo in archivos:
        datos = archivo.file.read()
        archivo.file.seek(0)
        content_type = archivo.content_type or "image/jpeg"
        if len(datos) > 6 * 1024 * 1024:
            # Imágenes muy pesadas: re-encodear para que Gemini las acepte
            # (la foto ORIGINAL ya quedó guardada; esto es solo para lectura).
            if content_type.startswith("image/"):
                datos = _reencodear_imagen_para_llm(datos)
            if len(datos) > 6 * 1024 * 1024:
                raise HTTPException(status_code=400, detail="Cada archivo debe pesar menos de 6 MB.")
        if content_type == "application/pdf":
            texto = _extraer_texto_pdf(datos)
            if len(texto.strip()) >= 200:
                # PDF digital: el texto basta y es mucho más barato que el inline.
                parts.append({"text": f"Contenido textual del documento (PDF digital):\n{texto}"})
            else:
                # PDF escaneado: se manda inline (Gemini acepta application/pdf base64).
                parts.append({
                    "inline_data": {
                        "mime_type": "application/pdf",
                        "data": base64.b64encode(datos).decode("utf-8"),
                    }
                })
        else:
            parts.append({
                "inline_data": {
                    "mime_type": content_type,
                    "data": base64.b64encode(datos).decode("utf-8"),
                }
            })
    parts.append({"text": f"Extrae los datos. Claves permitidas: {', '.join(esquema['campos'].keys())}."})

    texto = _llamar_gemini(parts, instruction)

    try:
        datos = json.loads(texto)
    except json.JSONDecodeError:
        # Defensa: si el modelo envuelve el JSON en texto ("```json ... ```" o
        # prosa), se recupera el primer objeto balanceado {...}.
        inicio = texto.find("{")
        if inicio == -1:
            raise HTTPException(status_code=502, detail="No se pudo interpretar la lectura del documento. Intenta con una foto más nítida.")
        profundidad = 0
        fin = -1
        for i, ch in enumerate(texto[inicio:], start=inicio):
            if ch == "{":
                profundidad += 1
            elif ch == "}":
                profundidad -= 1
                if profundidad == 0:
                    fin = i
                    break
        if fin == -1:
            raise HTTPException(status_code=502, detail="No se pudo interpretar la lectura del documento. Intenta con una foto más nítida.")
        try:
            datos = json.loads(texto[inicio:fin + 1])
        except json.JSONDecodeError:
            raise HTTPException(status_code=502, detail="No se pudo interpretar la lectura del documento. Intenta con una foto más nítida.")

    # documentación_valida: veredicto del LLM sobre si la imagen ES el documento
    # esperado (control anti-archivos ajenos: memes, facturas, fotos personales).
    # Se SEPARA de los datos para no contaminar lecturasIA ni los mapeos del front.
    # 409 (no 422): el front distingue "ilegible, guarda igual" de "documento
    # equivocado, NO guardar".
    documento_valido = datos.pop("documento_valido", None)
    if documento_valido is False:
        raise HTTPException(
            status_code=409,
            detail="Esto no parece ser el documento esperado. Sube una foto o PDF del documento indicado.",
        )

    # Normalizar: solo claves del esquema (defensa ante respuestas con claves extra).
    datos = {k: datos.get(k) for k in esquema["campos"] if datos.get(k) is not None}
    if not datos:
        raise HTTPException(status_code=422, detail="No se logró leer ningún dato legible. Intenta con una foto más nítida y con buena luz.")
    return datos


# ==========================================
# 2c. AVISOS DE CONSISTENCIA POST-LECTURA
# ==========================================
def _normalizar_placa(valor: str) -> str:
    """Mayúsculas sin espacios ni guiones, para comparar placas leídas vs reales."""
    return re.sub(r"[\s\-]", "", str(valor or "")).upper()


def _generar_avisos(tipo_doc: str, datos: dict, contexto: dict) -> list:
    """
    Validaciones de consistencia entre lo leído y el vehículo en curso.
    `contexto`: {placa_vehiculo, cedula_conductor} (opcionales, los manda el front).
    Retorna lista de avisos legibles; vacía si todo cuadra.
    """
    avisos = []
    hoy = datetime.utcnow().strftime("%Y-%m-%d")

    placa_vehiculo = _normalizar_placa(contexto.get("placa_vehiculo", ""))
    cedula_conductor = re.sub(r"\D", "", str(contexto.get("cedula_conductor", "")))

    if tipo_doc in ("tarjeta_propiedad", "soat"):
        placa_leida = _normalizar_placa(datos.get("placa", ""))
        if placa_leida and placa_vehiculo and placa_leida != placa_vehiculo:
            avisos.append(
                f"⚠️ La placa del documento ({placa_leida}) no coincide con la placa del vehículo ({contexto.get('placa_vehiculo')}). Verifica que sea el documento correcto."
            )

    if tipo_doc == "licencia":
        cedula_leida = re.sub(r"\D", "", str(datos.get("cedula", "")))
        if cedula_leida and cedula_conductor and cedula_leida != cedula_conductor:
            avisos.append(
                f"⚠️ La cédula de la licencia ({cedula_leida}) no coincide con la cédula del conductor ({cedula_conductor})."
            )

    if tipo_doc in ("licencia", "soat"):
        vence = str(datos.get("fecha_vencimiento") or "")
        if vence and vence < hoy:
            etiqueta = "La licencia" if tipo_doc == "licencia" else "El SOAT"
            avisos.append(f"⚠️ {etiqueta} está vencido ({vence}).")

    return avisos

# ==========================================
# 2d. EXTRACCIÓN IA AL SUBIR DOCUMENTOS
# ==========================================
# Tipos de documento de /subir-documento que la IA puede leer → esquema de
# ESQUEMAS_EXTRACCION. Al subirlos, se extraen los datos y se dejan en
# `lecturasIA.<tipo_subida>` para que el formulario los aplique (paso 2).
TIPOS_SUBIDA_LEIBLES = {
    "documentoIdentidadConductor": "cedula",
    "rutTenedor": "rut",
    "rutPropietario": "rut",
    "condCertificacionBancaria": "certificado_bancario",
    "propCertificacionBancaria": "certificado_bancario",
    "tenedCertificacionBancaria": "certificado_bancario",
    "licencia": "licencia",
    "tarjetaPropiedad": "tarjeta_propiedad",
    "soat": "soat",
}


# ==========================================
# 2e. FIGURAS Y OBLIGATORIEDAD DE DOCUMENTOS
# ==========================================
# Documentos por familia de figura: cuando Propietario=Conductor y/o
# Tenedor=Propietario, el mismo archivo satisface a las figuras gemelas
# (misma semántica que el front en documentConstants.tsx).
FAMILIAS_FIGURA = {
    "identidad": {
        "conductor": "documentoIdentidadConductor",
        "propietario": "documentoIdentidadPropietario",
        "tenedor": "documentoIdentidadTenedor",
    },
    "bancaria": {
        "conductor": "condCertificacionBancaria",
        "propietario": "propCertificacionBancaria",
        "tenedor": "tenedCertificacionBancaria",
    },
    "rut": {
        "propietario": "rutPropietario",
        "tenedor": "rutTenedor",
    },
}

# Documentos exigidos al pasar a completado_revision (los 18 del paso 3).
# Los de figura se satisfacen por gemelo cuando las figuras coinciden.
DOCUMENTOS_REQUERIDOS = [
    "tarjetaPropiedad", "soat", "revisionTecnomecanica", "tarjetaRemolque",
    "polizaResponsabilidad", "documentoIdentidadConductor", "documentoIdentidadPropietario",
    "documentoIdentidadTenedor", "licencia", "planillaEpsArl", "condFoto",
    "condCertificacionBancaria", "propCertificacionBancaria", "tenedCertificacionBancaria",
    "documentoAcreditacionTenedor", "rutTenedor", "rutPropietario", "fotos",
]

ETIQUETAS_DOCUMENTO = {
    "tarjetaPropiedad": "Tarjeta de Propiedad",
    "soat": "SOAT",
    "revisionTecnomecanica": "Revisión Tecnomecánica",
    "tarjetaRemolque": "Tarjeta de Remolque",
    "polizaResponsabilidad": "Póliza de Responsabilidad Civil",
    "documentoIdentidadConductor": "Documento de Identidad del Conductor",
    "documentoIdentidadPropietario": "Documento de Identidad del Propietario",
    "documentoIdentidadTenedor": "Documento de Identidad del Tenedor",
    "licencia": "Licencia de Conducción Vigente",
    "planillaEpsArl": "Planilla de EPS y ARL",
    "condFoto": "Foto del Conductor",
    "condCertificacionBancaria": "Certificación Bancaria del Conductor",
    "propCertificacionBancaria": "Certificación Bancaria del Propietario",
    "tenedCertificacionBancaria": "Certificación Bancaria del Tenedor",
    "documentoAcreditacionTenedor": "Documento que lo acredite como Tenedor",
    "rutTenedor": "RUT del Tenedor",
    "rutPropietario": "RUT del Propietario",
    "fotos": "Fotos del vehículo",
}


def _solo_digitos(valor) -> str:
    return re.sub(r"\D", "", str(valor or ""))


def _figuras_iguales(vehiculo: dict) -> dict:
    """
    Propietario=Conductor y Tenedor=Propietario. Prioriza los flags persistidos
    (`propietarioIgualConductor` / `tenedorIgualPropietario`); si no existen
    (históricos), infiere comparando los dígitos de los documentos.
    """
    flag_prop = vehiculo.get("propietarioIgualConductor")
    flag_tened = vehiculo.get("tenedorIgualPropietario")

    cedula_cond = _solo_digitos(vehiculo.get("condCedulaCiudadania"))
    doc_prop = _solo_digitos(vehiculo.get("propDocumento"))
    doc_tened = _solo_digitos(vehiculo.get("tenedDocumento"))

    prop_igual_cond = flag_prop if isinstance(flag_prop, bool) else (bool(doc_prop) and doc_prop == cedula_cond)
    tened_igual_prop = flag_tened if isinstance(flag_tened, bool) else (bool(doc_tened) and doc_tened == doc_prop)
    return {"prop_igual_cond": prop_igual_cond, "tened_igual_prop": tened_igual_prop}


def _gemelos_documento(tipo: str, vehiculo: dict) -> list:
    """Campos de figuras iguales de la misma familia donde replicar la URL."""
    fig = _figuras_iguales(vehiculo)
    prop_igual_cond = fig["prop_igual_cond"]
    tened_igual_prop = fig["tened_igual_prop"]
    tened_igual_cond = tened_igual_prop and prop_igual_cond

    gemelos = []
    for familia in FAMILIAS_FIGURA.values():
        cond = familia.get("conductor")
        prop = familia.get("propietario")
        tened = familia.get("tenedor")

        if tipo == prop:
            # prop → cond (si son iguales) y prop → tened (si son iguales).
            if cond and prop_igual_cond:
                gemelos.append(cond)
            if tened and tened_igual_prop:
                gemelos.append(tened)
        elif tipo == tened:
            # tened → prop (si son iguales) y, por transitividad, → cond.
            if prop and tened_igual_prop:
                gemelos.append(prop)
            if cond and tened_igual_cond:
                gemelos.append(cond)
        elif tipo == cond and cond:
            # cond → prop y, por transitividad, → tened.
            if prop and prop_igual_cond:
                gemelos.append(prop)
            if tened and tened_igual_cond:
                gemelos.append(tened)
    return [g for g in gemelos if g != tipo]


def _doc_lleno(vehiculo: dict, campo: str) -> bool:
    """Un documento está 'lleno' si su campo tiene una URL (o array no vacío)."""
    valor = vehiculo.get(campo)
    if isinstance(valor, list):
        return len([u for u in valor if u and str(u).strip()]) > 0
    return bool(valor and str(valor).strip() and str(valor) not in ("null", "undefined"))


def _documentos_faltantes(vehiculo: dict) -> list:
    """
    Documentos obligatorios que faltan para pasar a completado_revision.
    Un documento de figura se satisface si su campo está lleno O si el de una
    figura igual (gemelo de la misma familia) lo está.
    """
    faltantes = []
    for campo in DOCUMENTOS_REQUERIDOS:
        if _doc_lleno(vehiculo, campo):
            continue
        if any(_doc_lleno(vehiculo, gemelo) for gemelo in _gemelos_documento(campo, vehiculo)):
            continue
        faltantes.append(campo)
    return faltantes


def _registrar_cambio_aprobado(vehiculo: dict, editado_por: str, seccion: str, campos: list) -> None:
    """
    Si un vehículo APROBADO es editado por el conductor/tenedor: baja a revisión,
    registra el diff en `historialCambios`, cancela su disponibilidad del día y
    notifica a Seguridad. No-op para vehículos no aprobados.
    """
    if vehiculo.get("estadoIntegra") != "aprobado":
        return
    if not campos:
        return

    placa = vehiculo["placa"]
    ahora = datetime.utcnow()
    coleccion_vehiculos.update_one(
        {"placa": placa},
        {
            "$set": {"estadoIntegra": "completado_revision"},
            "$push": {
                "historialCambios": {
                    "fecha": ahora,
                    "usuario": editado_por or "desconocido",
                    "seccion": seccion,
                    "campos": campos,
                }
            },
        },
    )
    # Sacar la placa de la bolsa de disponibilidad del día (si tenía check-in activo).
    try:
        coleccion_disponibilidades.update_many(
            {"placa": placa, "estado": "activa"},
            {"$set": {"estado": "cancelada", "actualizado_en": ahora}},
        )
    except Exception as e:
        print(f"[disponibilidad] No se pudo cancelar el check-in de {placa}: {e}")

    nombre_conductor = vehiculo.get("condNombres") or "Conductor"
    try:
        enviar_notificacion_seguridad(placa, nombre_conductor)
    except Exception as e:
        print(f"[seguridad] No se pudo notificar el cambio de {placa}: {e}")


def enviar_notificacion_seguridad(placa: str, nombre_conductor_busqueda: str):
    """
    1. Busca usuarios con perfil 'SEGURIDAD' para enviar el correo.
    2. Busca al conductor por su NOMBRE (no por ID).
    """
    if not RESEND_API_KEY:
        print("[RESEND] ⚠️ No hay API Key. No se enviará correo.")
        return

    try:
        # 1. Buscar destinatarios (Usuarios de Seguridad)
        cursor_seguridad = coleccion_baseusuarios.find({"perfil": "SEGURIDAD"})
        destinatarios = [u.get("correo") for u in cursor_seguridad if u.get("correo")]

        if not destinatarios:
            print(f"[RESEND] ⚠️ No se encontraron usuarios SEGURIDAD para la placa {placa}.")
            return

        # 2. Obtener datos del conductor
        nombre_final_para_email = nombre_conductor_busqueda 

        try:
            filtro_nombre = {
                "nombre": {
                    "$regex": f"^{re.escape(nombre_conductor_busqueda)}$", 
                    "$options": "i"
                }
            }
            conductor_doc = coleccion_usuarios.find_one(filtro_nombre)
            
            if conductor_doc:
                nombre_final_para_email = conductor_doc.get("nombre", nombre_conductor_busqueda)
            
        except Exception as e:
            print(f"[DEBUG] Error en consulta de conductor: {e}")


        # 3. HTML del Correo
        cuerpo_html = f"""
        <div style="font-family: Arial, sans-serif; color: #333; max-width: 600px;">
            <h2 style="color: #0056b3;">Nueva Solicitud de Revisión</h2>
            <p>El conductor <strong>{nombre_final_para_email}</strong> ha completado la carga de documentos.</p>
            
            <div style="background-color: #f0f8ff; padding: 15px; border-radius: 8px; margin: 20px 0; border: 1px solid #cce5ff;">
                <p style="margin: 0; font-size: 14px; color: #555;">Vehículo a revisar:</p>
                <h1 style="margin: 5px 0 0 0; color: #004085; font-size: 28px;">{placa}</h1>
            </div>

            <p>Por favor ingresa a la plataforma <b>IntegraApp</b> para validar la documentación.</p>
            <hr style="border: 0; border-top: 1px solid #eee; margin-top: 30px;">
            <p style="font-size: 12px; color: #999;">Notificación automática.</p>
        </div>
        """

        # 4. Enviar
        params = {
            "from": MAIL_FROM,
            "to": destinatarios,
            "subject": f"🚨 Revisión Pendiente: {placa}",
            "html": cuerpo_html,
        }

        email = resend.Emails.send(params)
        print(f"[RESEND] ✅ Correo enviado a {destinatarios}. ID: {email}")

    except Exception as e:
        print(f"[RESEND] ❌ Error enviando correo: {str(e)}")

# ==========================================
# 3. ENDPOINTS
# ==========================================

@ruta_vehiculos.post("/crear")
async def crear_vehiculo(id_usuario: str = Form(...), placa: str = Form(...)):
    placa_limpia = placa.strip().upper()
    if coleccion_vehiculos.find_one({"placa": placa_limpia}):
        raise HTTPException(status_code=400, detail="La placa ya está registrada.")
    
    nuevo_vehiculo = {
        "idUsuario": id_usuario,
        "placa": placa_limpia,
        "estadoIntegra": "registro_incompleto",
        "estudioSeguridad": None,
        "usuarioIntegra": None,
        "fotos": [],
        "tarjetaPropiedad": None,
        "soat": None,
        "revisionTecnomecanica": None,
        "tarjetaRemolque": None,
        "polizaResponsabilidad": None,
        "documentoIdentidadConductor": None,
        "condFoto": None,
        "licencia": None,
        "planillaEpsArl": None,
        "documentoIdentidadTenedor": None,
        "condCertificacionBancaria": None,
        "propCertificacionBancaria": None,
        "tenedCertificacionBancaria": None,
        "documentoAcreditacionTenedor": None,
        "rutTenedor": None,
        "documentoIdentidadPropietario": None,
        "rutPropietario": None,
        "vehMarca": None,
        "firmaUrl": None, # Inicializamos campo firma
        # Roles/vinculación (Fase tenedor-conductor): idUsuario = dueño de la
        # ficha (tenedor o conductor legacy); idConductor = conductor invitado.
        "idConductor": None,
        "invitacionConductor": None,   # {correo, estado: pendiente|aceptada, creado_en, expira}
        # Auditoría de ediciones sobre vehículos aprobados (re-revisión).
        "historialCambios": [],
        # Datos extraídos por IA al subir documentos, por tipo de subida:
        # {rutTenedor: {datos, avisos, fecha}, ...} — el paso 2 los aplica.
        "lecturasIA": {},
    }
    
    coleccion_vehiculos.insert_one(nuevo_vehiculo)
    print(f" Vehículo creado: {placa_limpia} para usuario {id_usuario}")
    return JSONResponse(status_code=status.HTTP_201_CREATED, content={"message": "Vehículo registrado exitosamente"})


@ruta_vehiculos.get("/obtener-vehiculos")
def obtener_vehiculos(id_usuario: str, estadoIntegra: Optional[str] = None):
    # $or: vehículos propios (idUsuario = dueño de la ficha, conductor legacy o
    # tenedor) + vehículos donde soy el conductor invitado (idConductor).
    filtro = {"$or": [{"idUsuario": id_usuario}, {"idConductor": id_usuario}]}
    if estadoIntegra:
        filtro = {"$and": [filtro, {"estadoIntegra": estadoIntegra}]}

    vehiculos = list(coleccion_vehiculos.find(filtro, {"_id": 0}))
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"message": "Búsqueda finalizada", "vehiculos": vehiculos}
    )


@ruta_vehiculos.get("/obtener-vehiculo/{placa}")
async def obtener_vehiculo(placa: str):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa}, {"_id": 0})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")

    return JSONResponse(status_code=status.HTTP_200_OK, content={"message": "Vehículo encontrado", "data": vehiculo})

@ruta_vehiculos.put("/actualizar-estado")
async def actualizar_estado(
    placa: str = Form(...),
    nuevo_estado: str = Form(...),
    usuario_id: str = Form(...),
    observaciones: Optional[str] = Form(None),
    nombre_conductor: str = Form("Conductor") 
):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")

    # Al pasar a completado_revision el conductor declara la documentación
    # completa: validar server-side ( Seguridad usa este endpoint con otros
    # estados y NO se le exige nada).
    if nuevo_estado == "completado_revision":
        faltantes = _documentos_faltantes(vehiculo)
        if faltantes:
            nombres = ", ".join(ETIQUETAS_DOCUMENTO.get(f, f) for f in faltantes)
            raise HTTPException(
                status_code=400,
                detail=f"Faltan documentos obligatorios: {nombres}.",
            )

    datos_actualizar = {
        "estadoIntegra": nuevo_estado,
        "usuarioIntegra": usuario_id
    }

    if observaciones:
        datos_actualizar["observaciones"] = observaciones

    coleccion_vehiculos.update_one(
        {"placa": placa},
        {"$set": datos_actualizar}
    )

    if nuevo_estado == "completado_revision":
        enviar_notificacion_seguridad(placa, nombre_conductor)

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "message": f"Estado actualizado a '{nuevo_estado}'"
        }
    )

# Claves internas del documento que el front NUNCA debe pisar con
# actualizar-informacion (estado, vinculación, auditoría, archivos de Seguridad).
CLAVES_PROTEGIDAS = {
    "_id", "placa", "idUsuario", "idConductor", "estadoIntegra",
    "invitacionConductor", "historialCambios", "usuarioIntegra",
    "estudioSeguridad", "fotoconductorseguridad", "lecturasIA", "fotos",
}

# URLs de documentos: sus dueños son subir-documento/eliminar-documento.
# actualizar-informacion NUNCA las toca (un form con valores null/vacíos del
# montaje pisaría la URL recién subida — bug real del autoguardado, 2026-08-27).
CAMPOS_DOCUMENTO_PROTEGIDOS = {
    "tarjetaPropiedad", "soat", "revisionTecnomecanica", "tarjetaRemolque",
    "polizaResponsabilidad", "documentoIdentidadConductor", "documentoIdentidadPropietario",
    "documentoIdentidadTenedor", "licencia", "planillaEpsArl", "condFoto",
    "condCertificacionBancaria", "propCertificacionBancaria", "tenedCertificacionBancaria",
    "documentoAcreditacionTenedor", "rutTenedor", "rutPropietario", "firmaUrl",
}


@ruta_vehiculos.put("/actualizar-informacion/{placa}")
async def actualizar_informacion_vehiculo(placa: str, datos: dict, editado_por: Optional[str] = None):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")

    # Defensa: el endpoint acepta cualquier clave, pero las internas se ignoran
    # y los campos de documentos también (sus dueños son los endpoints de subida).
    datos_limpios = {
        k: v for k, v in datos.items()
        if k not in CLAVES_PROTEGIDAS and k not in CAMPOS_DOCUMENTO_PROTEGIDOS
    }
    if not datos_limpios:
        return JSONResponse(status_code=200, content={"message": "Información actualizada"})

    # Diff contra el documento actual: SOLO los campos cuyo valor cambia.
    # Un re-guardado sin cambios reales no baja un aprobado a revisión.
    cambios = [
        {"campo": k, "antes": vehiculo.get(k) if vehiculo.get(k) is not None else "(vacío)", "despues": v}
        for k, v in datos_limpios.items()
        if vehiculo.get(k) != v
    ]

    coleccion_vehiculos.update_one({"placa": placa}, {"$set": datos_limpios})

    if cambios:
        _registrar_cambio_aprobado(vehiculo, editado_por or "", "datos", cambios)

    # La cédula del conductor (normalmente leída por IA de la cédula/licencia)
    # se propaga a las cuentas vinculadas que aún no la tengan: la del dueño
    # de la ficha (idUsuario, si es conductor) y la del conductor invitado.
    cedula = str(datos_limpios.get("condCedulaCiudadania") or "").strip()
    if cedula:
        cedula = re.sub(r"\D", "", cedula)
        for id_cuenta in {vehiculo.get("idUsuario"), vehiculo.get("idConductor")}:
            if not id_cuenta:
                continue
            try:
                coleccion_conductores_cuenta.update_one(
                    {"_id": _a_objectid(id_cuenta), "cedula": {"$in": [None, ""]}},
                    {"$set": {"cedula": cedula}},
                )
            except Exception as e:
                print(f"[cedula] No se pudo propagar la cedula a la cuenta {id_cuenta}: {e}")

    return JSONResponse(status_code=200, content={"message": "Información actualizada"})


def _a_objectid(valor: str):
    """Convierte a ObjectId si es válido; si no, devuelve el valor tal cual."""
    try:
        from bson import ObjectId
        return ObjectId(valor)
    except Exception:
        return valor


@ruta_vehiculos.put("/subir-estudio-seguridad")
async def subir_estudio_seguridad(
    archivo: UploadFile = File(...),
    placa: str = Form(...)
):
    placa_limpia = placa.strip().upper()
    vehiculo = coleccion_vehiculos.find_one({"placa": placa_limpia})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")

    if archivo.content_type == "application/pdf":
        extension = "pdf"
    elif archivo.content_type.startswith("image/"):
        extension = "webp"
    else:
        raise HTTPException(status_code=400, detail="Solo se permiten archivos PDF o Imágenes.")

    nombre_archivo = _nombre_doc_bucket(placa_limpia, "estudioSeguridad", extension, vehiculo)

    try:
        url_archivo = subir_a_google_storage(archivo, nombre_archivo)
        coleccion_vehiculos.update_one(
            {"placa": placa_limpia},
            {"$set": {"estudioSeguridad": url_archivo}}
        )
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"message": "Estudio de seguridad subido correctamente", "url": url_archivo}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al procesar el archivo: {str(e)}")



@ruta_vehiculos.put("/subir-foto-seguridad")
async def subir_foto_seguridad(
    archivo: UploadFile = File(...),
    placa: str = Form(...)
):
    placa_limpia = placa.strip().upper()
    vehiculo = coleccion_vehiculos.find_one({"placa": placa_limpia})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")

    if not archivo.content_type.startswith("image/"):
         raise HTTPException(status_code=400, detail="Solo se permiten archivos de imagen para la foto del conductor.")

    nombre_archivo = _nombre_doc_bucket(placa_limpia, "fotoConductorSeguridad", "webp", vehiculo)

    try:
        url_archivo = subir_a_google_storage(archivo, nombre_archivo)
        coleccion_vehiculos.update_one(
            {"placa": placa_limpia},
            {"$set": {"fotoconductorseguridad": url_archivo}}
        )
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"message": "Foto de seguridad subida correctamente", "url": url_archivo}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al procesar la foto de seguridad: {str(e)}")


@ruta_vehiculos.put("/subir-documento")
async def subir_documento(
    archivo: UploadFile,
    placa: str = Form(...),
    tipo: str = Form(...),
    editado_por: Optional[str] = Form(None),
    extraer: Optional[str] = Form("true"),
    replicar_en: Optional[str] = Form(None),
    lectura_datos: Optional[str] = Form(None),
    lectura_avisos: Optional[str] = Form(None),
    reverso: Optional[UploadFile] = File(None),
):
    # Documentos de dos caras: el reverso se guarda junto al frente.
    TIPOS_DOS_CARAS = {"documentoIdentidadConductor", "licencia", "tarjetaPropiedad"}
    if reverso and tipo not in TIPOS_DOS_CARAS:
        raise HTTPException(status_code=400, detail="Ese tipo de documento no admite reverso.")
    tipos_validos = [
        "tarjetaPropiedad", "soat", "revisionTecnomecanica", "tarjetaRemolque",
        "polizaResponsabilidad", "documentoIdentidadConductor", "documentoIdentidadPropietario",
        "documentoIdentidadTenedor", "licencia", "planillaEpsArl", "condFoto",
        "condCertificacionBancaria", "propCertificacionBancaria", "tenedCertificacionBancaria",
        "documentoAcreditacionTenedor", "rutTenedor", "rutPropietario"
    ]
    if tipo not in tipos_validos:
        raise HTTPException(status_code=400, detail="Tipo de documento no válido.")

    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")

    if archivo.content_type.startswith("image/"):
        extension = "webp"
    elif archivo.content_type == "application/pdf":
        extension = "pdf"
    else:
        raise HTTPException(status_code=400, detail="Solo se permiten archivos de imagen o PDF.")

    # Gemelos (figuras iguales): los que manda el front, filtrados por válidos,
    # + los que el propio backend calcula por si el front no los manda.
    gemelos_front = [g.strip() for g in (replicar_en or "").split(",") if g.strip()]
    gemelos = list(dict.fromkeys(
        [g for g in gemelos_front if g in tipos_validos and g != tipo]
        + [g for g in _gemelos_documento(tipo, vehiculo) if g in tipos_validos]
    ))

    # Para tipos leíbles: leer con IA ANTES de subir. Si Gemini determina que
    # NO es el documento esperado (409), se rechaza la subida completa (nada
    # en el bucket ni en Mongo). Ilegible (422) u otros fallos: se sube igual.
    # `lectura_datos`/`lectura_avisos` (JSON): lectura YA hecha por el front
    # (flujo de dos caras: cédula/licencia/tarjeta leyeron frente+reverso por
    # separado) — se persiste en lecturasIA sin re-pagar Gemini.
    lectura_ia = None
    esquema_ia = TIPOS_SUBIDA_LEIBLES.get(tipo)
    if lectura_datos:
        try:
            lectura_ia = {
                "datos": json.loads(lectura_datos),
                "avisos": json.loads(lectura_avisos) if lectura_avisos else [],
            }
        except (ValueError, TypeError):
            lectura_ia = None
    elif esquema_ia and (extraer or "true").lower() != "false":
        try:
            contexto = {
                "placa_vehiculo": placa,
                "cedula_conductor": vehiculo.get("condCedulaCiudadania") or "",
            }
            datos_leidos = await asyncio.to_thread(extraer_datos_con_llm, esquema_ia, [archivo])
            archivo.file.seek(0)
            avisos = _generar_avisos(esquema_ia, datos_leidos, contexto)
            lectura_ia = {"datos": datos_leidos, "avisos": avisos}
        except HTTPException as e:
            if e.status_code == 409:
                # Documento equivocado (meme/factura/otro doc): NO guardar nada.
                raise
            # Ilegible o servicio caído: el archivo SÍ se guarda, campos a mano.
            print(f"[lecturaIA] Fallo leyendo {tipo} de {placa}: {e.detail}")
            lectura_ia = None
            archivo.file.seek(0)
        except Exception as e:
            print(f"[lecturaIA] Error inesperado leyendo {tipo} de {placa}: {e}")
            lectura_ia = None
            archivo.file.seek(0)

    nombre_archivo = _nombre_doc_bucket(placa, tipo, extension, vehiculo)
    url_archivo = subir_a_google_storage(archivo, nombre_archivo)

    set_inicial = {tipo: url_archivo}
    set_inicial.update({g: url_archivo for g in gemelos})

    # Reverso (solo docs de dos caras): se guarda con su propio campo y URL.
    url_reverso = None
    if reverso is not None:
        if reverso.content_type.startswith("image/"):
            extension_rev = "webp"
        elif reverso.content_type == "application/pdf":
            extension_rev = "pdf"
        else:
            raise HTTPException(status_code=400, detail="El reverso solo puede ser imagen o PDF.")
        nombre_reverso = _nombre_doc_bucket(placa, f"{tipo}Reverso", extension_rev, vehiculo)
        url_reverso = subir_a_google_storage(reverso, nombre_reverso)
        set_inicial[f"{tipo}Reverso"] = url_reverso

    coleccion_vehiculos.update_one({"placa": placa}, {"$set": set_inicial})

    # Edición de un aprobado por el conductor/tenedor → baja a re-revisión.
    campos_diff = [{"campo": tipo, "antes": vehiculo.get(tipo) or "(ninguno)", "despues": url_archivo}]
    campos_diff += [
        {"campo": g, "antes": vehiculo.get(g) or "(ninguno)", "despues": url_archivo}
        for g in gemelos
    ]
    if url_reverso:
        campos_diff.append({"campo": f"{tipo}Reverso", "antes": vehiculo.get(f"{tipo}Reverso") or "(ninguno)", "despues": url_reverso})
    _registrar_cambio_aprobado(vehiculo, editado_por or "", "documentos", campos_diff)

    if lectura_ia:
        lecturas_set = {f"lecturasIA.{tipo}": {**lectura_ia, "fecha": datetime.utcnow()}}
        # La lectura también queda disponible para los gemelos (autollenado
        # de la otra figura en el próximo montaje del formulario).
        for g in gemelos:
            lecturas_set[f"lecturasIA.{g}"] = {**lectura_ia, "fecha": datetime.utcnow()}
        coleccion_vehiculos.update_one({"placa": placa}, {"$set": lecturas_set})

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "message": f"{tipo} subido correctamente",
            "url": url_archivo,
            "lectura_ia": lectura_ia,
            "replicado_en": gemelos,
            "url_reverso": url_reverso,
        }
    )


@ruta_vehiculos.put("/subir-fotos")
async def subir_fotos(archivos: List[UploadFile], placa: str = Form(...)):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    
    urls_fotos = []
    for indice, archivo in enumerate(archivos, start=1):
        nombre_archivo = _nombre_doc_bucket(placa, "foto", "webp", vehiculo, sufijo=f"_{indice:03d}")
        url_archivo = subir_a_google_storage(archivo, nombre_archivo)
        urls_fotos.append(url_archivo)

    coleccion_vehiculos.update_one({"placa": placa}, {"$push": {"fotos": {"$each": urls_fotos}}})
    return JSONResponse(status_code=status.HTTP_200_OK, content={"message": "Fotos subidas correctamente", "urls": urls_fotos})


@ruta_vehiculos.post("/extraer-datos-documento")
async def extraer_datos_documento(
    tipo: str = Form(...),
    anverso: UploadFile = File(...),
    reverso: Optional[UploadFile] = File(None),
    placa_vehiculo: Optional[str] = Form(None),
    cedula_conductor: Optional[str] = Form(None),
):
    """
    Lee un documento con LLM (Gemini) y devuelve los datos estructurados.
    `tipo` define el esquema (cedula, rut, certificado_bancario, licencia,
    tarjeta_propiedad, soat). El anverso es obligatorio, el reverso opcional.
    Acepta imágenes y PDF (los PDF digitales van como texto al LLM).
    `placa_vehiculo`/`cedula_conductor` (opcionales) habilitan avisos de
    consistencia (placa distinta, licencia vencida, cédula cruzada).
    Los datos son una SUGERENCIA: el conductor los confirma/edita en el formulario.
    """
    archivos = [anverso] + ([reverso] if reverso else [])
    datos = await asyncio.to_thread(extraer_datos_con_llm, tipo, archivos)
    contexto = {"placa_vehiculo": placa_vehiculo or "", "cedula_conductor": cedula_conductor or ""}
    avisos = _generar_avisos(tipo, datos, contexto)
    return {
        "tipo": tipo,
        "datos": datos,
        "aviso": "Verifica y corrige los datos antes de continuar.",
        "avisos": avisos,
    }



@ruta_vehiculos.put("/subir-firma")
async def subir_firma(
    archivo: UploadFile = File(...),
    placa: str = Form(...),
    tipo_documento: Optional[str] = Form(None),
    editado_por: Optional[str] = Form(None),
):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")

    try:
        # Nomenclatura estándar del bucket (placa/fecha/firma_cedula.webp).
        nombre_archivo = _nombre_doc_bucket(placa, "firma", "webp", vehiculo)

        # Reutilizamos la lógica existente de Google Cloud
        url_archivo = subir_a_google_storage(archivo, nombre_archivo)

        # Actualizamos campo firmaUrl
        coleccion_vehiculos.update_one(
            {"placa": placa},
            {"$set": {"firmaUrl": url_archivo}}
        )

        _registrar_cambio_aprobado(
            vehiculo, editado_por or "", "documentos",
            [{"campo": "firmaUrl", "antes": vehiculo.get("firmaUrl") or "(ninguna)", "despues": url_archivo}],
        )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"message": "Firma subida correctamente", "url": url_archivo}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error subiendo firma: {str(e)}")

@ruta_vehiculos.get("/obtener-firma")
async def obtener_firma(placa: str):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa}, {"firmaUrl": 1, "_id": 0})
    
    if not vehiculo or not vehiculo.get("firmaUrl"):
        raise HTTPException(status_code=404, detail="Firma no encontrada")
    
    url_firma = vehiculo.get("firmaUrl")

    try:
        respuesta_imagen = requests.get(url_firma)
        
        if respuesta_imagen.status_code == 200:
            
            try:
                # 1. Abrir la imagen binaria (sin importar el formato original)
                imagen_pil = Image.open(io.BytesIO(respuesta_imagen.content))
                
                # 2. Convertir y guardar en un buffer como PNG
                buffer = io.BytesIO()
                imagen_pil.save(buffer, format="PNG")
                
                # 3. Codificar el buffer PNG a Base64
                b64_data = base64.b64encode(buffer.getvalue()).decode('utf-8')
                
                # 4. Crear la Data URL completa
                imagen_final_base64 = f"data:image/png;base64,{b64_data}"

                # Retornar el Base64 dentro de un JSON
                return {"firma_b64": imagen_final_base64}
            
            except Exception as convert_error:
                print(f"Error al convertir la firma a Base64/PNG: {convert_error}")
                # Si falla la conversión
                raise HTTPException(status_code=500, detail="Error al procesar y codificar la imagen de firma")

        else:
            raise HTTPException(status_code=404, detail="No se pudo descargar la imagen remota")
            
    except Exception as e:
        print(f"Error general en el proxy de firma: {e}")
        raise HTTPException(status_code=500, detail="Error al procesar la imagen")


@ruta_vehiculos.delete("/eliminar-documento")
async def eliminar_documento(placa: str, tipo: str, editado_por: Optional[str] = None):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo or not vehiculo.get(tipo):
        raise HTTPException(status_code=404, detail="Documento no encontrado.")

    url_previa = vehiculo[tipo]
    eliminar_de_google_storage(url_previa)
    unset_campos = {tipo: None}

    # Reverso del documento de dos caras (si existe): se borra junto al frente.
    campo_reverso = f"{tipo}Reverso"
    if vehiculo.get(campo_reverso):
        eliminar_de_google_storage(vehiculo[campo_reverso])
        unset_campos[campo_reverso] = None

    # Gemelos replicados: si otra figura de la misma familia tiene EXACTAMENTE
    # la misma URL (el archivo se replicó), se limpia también.
    gemelos = _gemelos_documento(tipo, vehiculo)
    gemelos_eliminados = [g for g in gemelos if vehiculo.get(g) == url_previa]
    for g in gemelos_eliminados:
        unset_campos[g] = None
    coleccion_vehiculos.update_one({"placa": placa}, {"$unset": unset_campos})

    campos_diff = [{"campo": tipo, "antes": url_previa, "despues": "(eliminado)"}]
    if campo_reverso in unset_campos:
        campos_diff.append({"campo": campo_reverso, "antes": vehiculo.get(campo_reverso), "despues": "(eliminado)"})
    campos_diff += [{"campo": g, "antes": url_previa, "despues": "(eliminado)"} for g in gemelos_eliminados]
    _registrar_cambio_aprobado(
        vehiculo, editado_por or "", "documentos",
        campos_diff,
    )

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"message": f"{tipo} eliminado correctamente", "gemelos_eliminados": gemelos_eliminados},
    )


@ruta_vehiculos.delete("/eliminar-foto")
async def eliminar_foto(placa: str, url: str, editado_por: Optional[str] = None):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo or url not in vehiculo["fotos"]:
        raise HTTPException(status_code=404, detail="Foto no encontrada.")

    eliminar_de_google_storage(url)
    coleccion_vehiculos.update_one({"placa": placa}, {"$pull": {"fotos": url}})

    _registrar_cambio_aprobado(
        vehiculo, editado_por or "", "documentos",
        [{"campo": "fotos", "antes": url, "despues": "(eliminada)"}],
    )

    return JSONResponse(status_code=status.HTTP_200_OK, content={"message": "Foto eliminada correctamente"})


@ruta_vehiculos.get("/obtener-vehiculos-incompletos")
def obtener_vehiculos_incompletos(id_usuario: Optional[str] = None):
    filtro = {
        "estadoIntegra": {
            "$in": ["registro_incompleto", "completado_revision", "aprobado", "rechazado"]
        }
    }
    vehiculos_raw = list(coleccion_vehiculos.find(filtro))

    if not vehiculos_raw:
        return JSONResponse(
            status_code=status.HTTP_200_OK, 
            content={"message": "No hay vehículos", "vehicles": []}
        )

    vehiculos_final = []
    for veh in vehiculos_raw:
        veh["_id"] = str(veh["_id"])
        documentos = {
            k: v for k, v in veh.items()
            # SE HA MODIFICADO AQUÍ PARA QUE NO DEVUELVA estudioSeguridad NI fotoconductorseguridad 
            # SI NO QUIERES QUE EL CONDUCTOR LOS VEA (Opcional, pero recomendado por seguridad)
            if isinstance(v, str) and v.startswith("https://storage.googleapis.com") and k not in ["estudioSeguridad", "fotoconductorseguridad"]
        }
        veh["documentos"] = documentos
        vehiculos_final.append(veh)

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"message": "Vehículos encontrados", "vehicles": vehiculos_final}
    )


@ruta_vehiculos.get("/obtener-aprobados-paginados")
def obtener_aprobados_paginados(search: Optional[str] = None, limit: int = 10):
    filtro = {"estadoIntegra": "aprobado"}
    if search and search.strip():
        search_regex = {"$regex": search.strip(), "$options": "i"} 
        filtro["$or"] = [
            {"placa": search_regex},
            {"condCedulaCiudadania": search_regex}
        ]
    
    vehiculos_cursor = coleccion_vehiculos.find(filtro).sort("_id", -1).limit(limit)
    vehiculos_final = []
    
    for veh in vehiculos_cursor:
        veh["_id"] = str(veh["_id"])
        documentos = {
            k: v for k, v in veh.items()
            if isinstance(v, str) and v.startswith("https://storage.googleapis.com") and k != "estudioSeguridad"
        }
        veh["documentos"] = documentos
        vehiculos_final.append(veh)

    return JSONResponse(
        status_code=status.HTTP_200_OK, 
        content={"vehiculos": vehiculos_final}
    )