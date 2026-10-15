import os
import json
import re
import asyncio
import hashlib
from datetime import datetime, date, timedelta
from io import BytesIO
from typing import List, Optional
import resend
from dotenv import load_dotenv
from fastapi import APIRouter, File, Form, HTTPException, Request, Response, UploadFile, status
from fastapi.responses import JSONResponse, RedirectResponse
from google.cloud import storage
from PIL import Image
from pymongo import MongoClient
from bson import ObjectId
import re
import requests
import base64
import pdfplumber
# Excepción de pdfminer (motor de pdfplumber) cuando el PDF exige contraseña
# de apertura: se usa para detectar PDFs con clave y rechazarlos con un
# mensaje accionable en vez de mandarlos cifrados a Gemini.
from pdfminer.pdfdocument import PDFPasswordIncorrect
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
# 2026-08-31: los documentos del vehículo (cédulas de 3 figuras con ambas
# caras, RUT, licencia, tarjeta de propiedad, certificados bancarios, firma
# electrónica...) son PII pesada y van al bucket PRIVADO (mismo criterio que
# los estudios de seguridad). Mongo guarda la RUTA del blob (no URL pública)
# y los endpoints de lectura la convierten en URL firmada temporal.
BUCKET_NAME = os.getenv("VEHICULOS_BUCKET", "integrapp-privado")
CARPETA_STORAGE = os.getenv("VEHICULOS_CARPETA", "Vehiculos")
# Vigencia de las URLs firmadas que reciben los fronts (panel, revisión, HV).
VEHICULOS_URL_FIRMADA_MIN = int(os.getenv("VEHICULOS_URL_FIRMADA_MIN", "60"))
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
# Evidencias de firma electrónica (append-only, como aceptaciones_politica):
# cada firma del conductor sella un registro que sobrevive a ediciones del
# documento del vehículo.
coleccion_firmas = bd['firmas_conductor']


def _json_seguro(valor):
    """
    Convierte un documento Mongo a tipos JSON-serializables de forma recursiva:
    datetime → ISO string (UTC naive), ObjectId → str. Los endpoints que
    devuelven el documento crudo reventaban con 500 cuando el doc tenía
    lecturasIA.{tipo}.fecha / historialCambios[].fecha (bug real 2026-08-27:
    un vehículo con lecturas IA rompía obtener-vehiculos y el conductor
    dejaba de ver sus placas).
    """
    if isinstance(valor, dict):
        return {k: _json_seguro(v) for k, v in valor.items()}
    if isinstance(valor, list):
        return [_json_seguro(v) for v in valor]
    if isinstance(valor, ObjectId):
        return str(valor)
    if isinstance(valor, (datetime, date)):
        return valor.isoformat()
    return valor

# ==========================================
# Firma electrónica — helpers de sellado
# ==========================================

# SEGUNDO CONDUCTOR (2026-10-14, pedido del usuario): el slot 2 es OPCIONAL y
# tiene hoja de vida COMPLETA — sus datos (`cond2*`, espejo de los `cond*`) y
# sus propios documentos. Catálogo ÚNICO de contenido declarado del slot 2: de
# aquí se derivan la volatilidad de la firma, el hash de SU firma
# (_hash_datos_firmados_slot2) y el gate de documentos. La VINCULACIÓN
# (`idConductor2`/`invitacionConductor2`) y la firma (`firmaUrl2`/
# `firmaEvidencia2`) son bookkeeping y viven aparte.
CAMPOS_CONDUCTOR_2 = frozenset({
    "cond2PrimerApellido", "cond2SegundoApellido", "cond2Nombres",
    "cond2CedulaCiudadania", "cond2FechaNacimiento", "cond2ExpedidaEn",
    "cond2FechaExpedicion", "cond2DeptoCiudad", "cond2Ciudad", "cond2Direccion",
    "cond2Celular", "cond2Correo", "cond2Eps", "cond2Arl", "cond2NoLicencia",
    "cond2FechaVencimientoLic", "cond2CategoriaLic", "cond2GrupoSanguineo",
    "cond2Banco", "cond2TipoCuenta", "cond2NumeroCuenta",
    "cond2NombreEmergencia", "cond2CelularEmergencia", "cond2ParentescoEmergencia",
    "cond2EmpresaRef", "cond2CelularRef", "cond2DeptoCiudadRef", "cond2CiudadRef",
    "cond2NroViajesRef", "cond2AntiguedadRef", "cond2MercTransportada",
    # Documentos del slot 2 (otra persona: NO entran en FAMILIAS_FIGURA ni
    # tienen gemelos con propietario/tenedor/conductor 1).
    "documentoIdentidadConductor2", "documentoIdentidadConductor2Reverso",
    "licencia2", "licencia2Reverso", "cond2Foto", "cond2CertificacionBancaria",
})

# Campos EXCLUIDOS del hash de datos firmados: bookkeeping interno, estado
# administrativo o la propia evidencia. Todo lo demás (datos del formulario y
# URLs de los documentos cargados) queda amarrado criptográficamente a la firma.
CAMPOS_VOLATILES_FIRMA = {
    "_id", "firmaUrl", "firmaEvidencia", "lecturasIA", "historialCambios",
    "invitacionConductor", "estadoIntegra", "observaciones", "usuarioIntegra",
    "estudioSeguridad", "fotoconductorseguridad", "estudioSeguridadFecha",
    # Estudios automáticos (TusDatos): resultado de un proceso en background,
    # no un dato declarado por el conductor al firmar.
    "estudiosSeguridadAuto", "historialEstudios", "documentosEstudioSeguridad",
    "estudiosVigencia",
    # Historial de planillas de seguridad social (2026-10-07): se acumula
    # mensualmente DESPUÉS de firmado — no debe romper la verificación.
    "documentosPlanillaSegSocial",
    # Historial universal de documentos (2026-10-07): cada subida de
    # cualquier documento se registra DESPUÉS de firmado.
    "historialDocumentos",
    # Bitácora de auditoría: se escribe DESPUÉS de cada mutación; jamás debe
    # romper la verificación del hash de datos firmados.
    "auditoriaVehiculo", "historialInactivacion",
    # Catálogo de ángulos de las fotos del vehículo (2026-10-14): lo escribe
    # subir-fotos, jamás el front ni la firma (el espejo plano `fotos` ya está
    # amparado por el hash).
    "fotosVehiculo",
    # Sello temporal del último cambio de estado (2026-10-14): bookkeeping
    # administrativo como estadoIntegra — sin esto CADA transición posterior
    # a la firma rompía la verificación del hash en falso.
    "fechaEstado",
    # Estado al que volver cuando un vehículo en_actualizacion por documento
    # eliminado vuelva a estar completo (2026-10-14).
    "actualizacionOrigen",
    # SEGUNDO CONDUCTOR (2026-10-14): TODO su contenido es volátil para la
    # firma del conductor 1 — el slot 1 firmó SUS datos; la HV del 2º se llena
    # después y el 2º tiene SU PROPIA firma sobre estos mismos campos
    # (_hash_datos_firmados_slot2). Sin esto, llenar la HV del 2º rompería la
    # verificación de TODAS las firmas del slot 1.
    "idConductor2", "invitacionConductor2", "firmaUrl2", "firmaEvidencia2",
    *CAMPOS_CONDUCTOR_2,
    # PADRINOS que adoptaron la placa (2026-10-15): suscripción de avisos, se
    # gestiona con adoptar/desadoptar-placa DESPUÉS de firmar — jamás debe
    # romper la verificación del hash.
    "padrinos",
}

def _hash_datos_firmados(vehiculo: dict) -> str:
    """
    SHA-256 canónico del contenido declarado del vehículo en el momento de
    firmar: normaliza (datetime/ObjectId → JSON), ordena claves y excluye los
    campos volátiles. Si cualquier dato o documento cambia después de firmado,
    el hash recalculado ya no coincide → integridad verificable.
    """
    datos = {k: v for k, v in _json_seguro(vehiculo).items() if k not in CAMPOS_VOLATILES_FIRMA}
    canonico = json.dumps(datos, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonico.encode("utf-8")).hexdigest()

def _hash_datos_firmados_slot2(vehiculo: dict) -> str:
    """
    SHA-256 canónico de los datos declarados del SEGUNDO conductor (2026-10-14):
    su HV (`CAMPOS_CONDUCTOR_2`) anclada a la placa — la placa impide reutilizar
    la misma firma en otra ficha. La vinculación y la propia evidencia de firma
    quedan fuera (bookkeeping, misma convención del slot 1).
    """
    datos = {"placa": vehiculo.get("placa")}
    datos.update({k: v for k, v in _json_seguro(vehiculo).items() if k in CAMPOS_CONDUCTOR_2})
    canonico = json.dumps(datos, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonico.encode("utf-8")).hexdigest()

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

_cliente_storage = None

def _obtener_cliente_storage():
    """Cliente GCS perezoso y compartido (uno por request es caro; patrón de
    storage_seguridad.py)."""
    global _cliente_storage
    if _cliente_storage is None:
        _cliente_storage = storage.Client()
    return _cliente_storage


def subir_a_google_storage(archivo: UploadFile, nombre_archivo: str) -> str:
    """
    Sube un archivo al bucket PRIVADO y devuelve la RUTA del blob
    (Vehiculos/{nombre_archivo}). La ruta — nunca una URL pública — es lo que
    persiste en Mongo; los endpoints de lectura la firman (URL v4 temporal).
    """
    try:
        cliente = _obtener_cliente_storage()
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

        return ruta_archivo
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al subir el archivo a Google Storage: {str(e)}")


def _nombre_doc_bucket(placa: str, tipo: str, extension: str, vehiculo: dict = None, sufijo: str = "") -> str:
    """
    Nomenclatura estándar de archivos en el bucket (2026-08-27; SIN cédula
    desde 2026-08-31 — minimización: las rutas llegan a logs de GCS y proxies,
    mismo criterio que los estudios de seguridad; CON placa al final del
    nombre desde 2026-10-02, pedido del usuario: cada archivo identifica su
    vehículo aunque se descargue o se mueva fuera de la carpeta):
        {PLACA}/{AAAA-MM-DD}/{tipo}{sufijo}_{placa}.{ext}
    Ej: Vehiculos/MX48E/2026-08-27/soat_mx48e.pdf
    — Agrupado por placa → fecha → documento. Re-subir el mismo doc el mismo
    día pisa el archivo (sin duplicados); otro día crea versión nueva y Mongo
    queda con la ruta vigente (la anterior queda como histórico). Los archivos
    previos al cambio (sin el sufijo de placa) siguen sirviendo tal cual: Mongo
    guarda la ruta completa y no se migra nada.
    (El parámetro `vehiculo` se mantiene por compatibilidad de firma y ya no
    aporta nada a la nomenclatura.)
    """
    fecha = datetime.now(_TZ_BOGOTA).strftime("%Y-%m-%d")
    placa_limpia = placa.strip().upper()
    return f"{placa_limpia}/{fecha}/{tipo}{sufijo}_{placa_limpia.lower()}.{extension}"


_RE_URL_PUBLICA_GCS = re.compile(r"^https://storage\.googleapis\.com/[^/]+/")


def _nombre_blob_de_ruta(valor: str) -> str:
    """
    Normaliza lo guardado en Mongo a nombre de blob. Lo nuevo son rutas
    (`Vehiculos/...`); lo histórico (URLs públicas del bucket viejo) se
    tolera recortando el prefijo https://storage.googleapis.com/{bucket}/.
    """
    return _RE_URL_PUBLICA_GCS.sub("", str(valor or ""))


def _es_ruta_documento(valor) -> bool:
    """True si el valor es una ruta de blob de este módulo (Vehiculos/...)."""
    return isinstance(valor, str) and valor.startswith(f"{CARPETA_STORAGE}/")


def _url_firmada_documento(ruta: str) -> str:
    """
    URL firmada v4 temporal (VEHICULOS_URL_FIRMADA_MIN) para que el
    navegador abra el documento privado sin exponerlo permanentemente.
    Defensiva: si el signing falla (GCS caído, entorno sin credenciales),
    devuelve la ruta plana — jamás tumba el endpoint que la llama.
    """
    try:
        blob = _obtener_cliente_storage().bucket(BUCKET_NAME).blob(_nombre_blob_de_ruta(ruta))
        return blob.generate_signed_url(
            version="v4",
            expiration=timedelta(minutes=VEHICULOS_URL_FIRMADA_MIN),
            method="GET",
        )
    except Exception as e:
        print(f"[storage] No se pudo firmar {ruta}: {e}")
        return ruta


def _url_para_cliente(ruta: str):
    """URL para entregar al front: firma las rutas de blob de este módulo y
    deja pasar cualquier otro valor tal cual (URLs históricas, mocks)."""
    return _url_firmada_documento(ruta) if _es_ruta_documento(ruta) else ruta


def _firmar_documentos(valor):
    """
    Recorre un documento Mongo y convierte TODAS las rutas de blobs de
    documentos (Vehiculos/...) en URLs firmadas temporales. Se aplica a los
    payloads de lectura; Mongo siempre queda con la ruta plana (el hash de la
    firma electrónica se calcula sobre las rutas, así que no cambia).
    """
    if isinstance(valor, dict):
        return {k: _firmar_documentos(v) for k, v in valor.items()}
    if isinstance(valor, list):
        return [_firmar_documentos(v) for v in valor]
    if _es_ruta_documento(valor):
        return _url_firmada_documento(valor)
    return valor


def _descargar_blob(ruta: str) -> bytes:
    """Descarga el contenido de un blob privado (server-side)."""
    blob = _obtener_cliente_storage().bucket(BUCKET_NAME).blob(_nombre_blob_de_ruta(ruta))
    return blob.download_as_bytes()


def eliminar_de_google_storage(ruta: str):
    try:
        cliente = _obtener_cliente_storage()
        bucket = cliente.bucket(BUCKET_NAME)
        blob = bucket.blob(_nombre_blob_de_ruta(ruta))
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
            "tipo_documento": "Tipo de documento del campo 25 (Cédula de Ciudadanía, NIT, Cédula de Extranjería, Pasaporte o Tarjeta de Identidad), tal como aparece",
            "razon_social": "Razón social completa en MAYÚSCULAS (solo si es persona jurídica)",
            "nombres": "Nombres de pila tal como aparecen (solo persona natural)",
            "apellidos": "Primer y segundo apellido tal como aparecen (solo persona natural)",
            "numero_documento": "Número de identificación del campo 26 (NIT sin dígito de verificación o cédula), SOLO dígitos",
            "digito_verificacion": "Dígito de verificación del NIT (un dígito) o null",
            "direccion": "Dirección principal (campo 41)",
            "ciudad": "Ciudad/Municipio (campo 40) — en el RUT el campo se llama 'Ciudad/Municipio'",
            "departamento": "Departamento (campo 39)",
            "correo": "Correo electrónico (campo 42)",
            "telefono": "Teléfono, SOLO dígitos (campo 44)",
            "fecha_inicio_actividad": "Fecha de inicio de la actividad económica (campo 47) en formato YYYY-MM-DD",
            "fecha_expedicion_rut": "Fecha de generación del documento PDF (línea 'Fecha generación documento PDF') en formato YYYY-MM-DD",
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
            "categorias": (
                "Lista con TODAS las categorías que la licencia autoriza, entre "
                "A1, A2, B1, B2, B3, C1, C2 y C3 (ej: [\"C1\",\"C2\"]). Una licencia "
                "puede tener varias categorías: inclúyelas TODAS"
            ),
            "fecha_vencimiento": "Fecha de vencimiento en formato YYYY-MM-DD",
            "nombre_completo": "Nombre completo del conductor tal como aparece",
            "cedula": "Número de cédula del conductor, SOLO dígitos",
        },
        "descripcion": (
            "Licencia de conducción colombiana. Tiene DOS caras: los datos "
            "principales (número, categorías, vencimiento) están en el FRENTE; "
            "el reverso aporta datos complementarios. Puede llegar 1 imagen "
            "(frente) o 2 (frente y reverso)."
        ),
    },
    "tarjeta_propiedad": {
        "campos": {
            "numero_licencia_transito": "Número de la licencia de tránsito (esquina superior), SOLO dígitos",
            "placa": "Placa del vehículo, SOLO letras, números y guiones",
            "marca": "Marca del vehículo",
            "linea": "Línea o referencia del vehículo",
            "modelo": "Año del modelo (4 dígitos)",
            "color": "Color del vehículo",
            "clase_vehiculo": "Clase de vehículo (ej: Motocicleta, Automóvil, Camión, Bus)",
            "capacidad_carga": "Capacidad de carga en kg del campo 'Capacidad Kg' o 'Capacidad Carga', SOLO dígitos",
            "cilindraje": "Cilindraje en c.c., SOLO dígitos",
            "servicio": "Servicio (ej: Particular, Público, Comercial)",
            "combustible": "Combustible (ej: Gasolina, Diesel, GNV, Híbrido, Eléctrico)",
            "capacidad_pasajeros": "Capacidad de pasajeros (número entero)",
            "potencia": "Potencia (ej: 15 HP)",
            "vin": "Número VIN, SOLO caracteres alfanuméricos",
            "numero_chasis": "Número de chasis, SOLO caracteres alfanuméricos",
            "numero_motor": "Número de motor, SOLO caracteres alfanuméricos y guiones",
            "numero_puertas": "Número de puertas (entero)",
            "fecha_matricula": "Fecha de matrícula en formato YYYY-MM-DD",
            "organismo_transito": "Organismo de tránsito emisor",
            "blindaje": "Sí/No según tenga blindaje",
            "limitacion_propiedad": "Sí/No según tenga limitación a la propiedad",
            "codigo_licencia": "Código inferior de la licencia (empieza por LT)",
            "propietario_nombre": "Nombre o razón social del propietario registrado",
            "propietario_documento": "Cédula o NIT del propietario, SOLO dígitos",
        },
        "descripcion": (
            "Tarjeta de propiedad / licencia de tránsito (registro RUNT) de un "
            "vehículo colombiano. Tiene DOS caras: el FRENTE tiene placa, "
            "número de licencia de tránsito, marca, línea, modelo, color, clase, "
            "cilindraje, servicio, combustible, VIN/chasis/motor y fechas; el "
            "REVERSO tiene el propietario, el organismo de tránsito, el código "
            "inferior (LT...) y las anotaciones (blindaje, limitaciones). "
            "Puede llegar 1 imagen (frente) o 2 (frente y reverso)."
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
    # Planilla de Seguridad Social (2026-10-07, pedido del usuario): la IA lee
    # la fecha de VENCIMIENTO — con ella (o con la que digite quien sube) el
    # vehículo queda INHABILITADO de la bolsa al vencerse, hasta que suban una
    # planilla nueva.
    "planilla_seguridad_social": {
        "campos": {
            "fecha_vencimiento": "Fecha de VENCIMIENTO de la planilla en formato YYYY-MM-DD (el día hasta el cual la afiliación a EPS/ARL está vigente; si el documento muestra un periodo mensual, es el ÚLTIMO día del periodo)",
            "eps": "Nombre de la EPS si aparece",
            "arl": "Nombre de la ARL si aparece",
            "nombre_titular": "Nombre del conductor/titular de la afiliación",
            "documento_titular": "Número de documento del titular, solo dígitos",
        },
        "descripcion": "Planilla o certificado de afiliación/pago de Seguridad Social (EPS y ARL) de un conductor colombiano. PDF o foto.",
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
    except PDFPasswordIncorrect:
        # PDF protegido con contraseña de apertura (típico en certificados
        # bancarios): ni pdfplumber ni Gemini pueden descifrarlo. Mensaje
        # accionable en vez del 409 engañoso de "documento equivocado".
        raise HTTPException(
            status_code=400,
            detail=(
                "El PDF está protegido con contraseña, por eso no pudimos leerlo con IA. "
                "Ábrelo con la clave y guárdalo/exportalo de nuevo como PDF sin contraseña "
                "(o tómale una foto) y vuelve a subirlo."
            ),
        )
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
    # Cédulas de propietario/tenedor: en el paso 2 la tarjeta IA las pide EN
    # VEZ de los RUT (los RUT se cargan en el paso 3 de documentación).
    "documentoIdentidadPropietario": "cedula",
    "documentoIdentidadTenedor": "cedula",
    # Cédula del dueño del remolque (2026-10-05): misma lectura que las demás
    # figuras — llena los campos RemolDueno* del paso 2 y entra al estudio.
    "documentoIdentidadRemolque": "cedula",
    "rutTenedor": "rut",
    "rutPropietario": "rut",
    "condCertificacionBancaria": "certificado_bancario",
    "propCertificacionBancaria": "certificado_bancario",
    "tenedCertificacionBancaria": "certificado_bancario",
    "licencia": "licencia",
    "tarjetaPropiedad": "tarjeta_propiedad",
    "soat": "soat",
    # Planilla de Seguridad Social: la IA lee la fecha de VENCIMIENTO (2026-10-07).
    "planillaEpsArl": "planilla_seguridad_social",
    # SEGUNDO CONDUCTOR (2026-10-14): mismos esquemas — el contexto de cédula
    # para los avisos sale de cond2CedulaCiudadania (ver subir-documento).
    "documentoIdentidadConductor2": "cedula",
    "licencia2": "licencia",
    "cond2CertificacionBancaria": "certificado_bancario",
}

# Tipos leíbles que pertenecen al SEGUNDO conductor: la IA valida contra SU
# cédula (cond2CedulaCiudadania) y no contra la del conductor 1.
TIPOS_LEIBLES_CONDUCTOR2 = {
    "documentoIdentidadConductor2", "licencia2", "cond2CertificacionBancaria",
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

# Documentos exigidos al pasar a completado_revision (los del paso 3).
# Los de figura se satisfacen por gemelo cuando las figuras coinciden.
# Reversos OBLIGATORIOS (2026-08-27): licencia y tarjeta de propiedad tienen
# dos caras y ambas se exigen; el de la cédula queda opcional (solo amarilla).
# La Tarjeta de Remolque es OPCIONAL (2026-08-27, orden del usuario: no todo
# vehículo arrastra remolque).
# FOTOS del vehículo (2026-10-14, pedido del usuario): se exigen CUATRO
# ÁNGULOS — FRONTAL, LATERAL IZQUIERDA, LATERAL DERECHA y POSTERIOR — en el
# array `fotosVehiculo` (ver ANGULOS_FOTO_VEHICULO abajo; "fotos" deja de
# estar en esta lista y queda como espejo plano de TODAS las fotos, tope 10).
# TODAS las cédulas exigen reverso (2026-08-27, orden del usuario: siempre
# dos caras, ya no solo la cédula amarilla del conductor).
# OPCIONALES (2026-08-31, orden del usuario): la Póliza de Responsabilidad
# Civil deja de exigirse (sigue cargable en el paso 3, no bloquea «Finalizar»).
# ELIMINADOS DEL PEDIDO por completo (2026-08-31): Certificación Bancaria del
# PROPIETARIO y RUT del PROPIETARIO — ninguna UI los pide; los tipos siguen
# válidos en subir-documento por compatibilidad con históricos.
# "documentoAcreditacionTenedor" SOLO se exige cuando el tenedor NO es el
# propietario (ver _documentos_faltantes): si tenedor=propietario, la propia
# tarjeta de propiedad lo acredita.
DOCUMENTOS_REQUERIDOS = [
    "tarjetaPropiedad", "tarjetaPropiedadReverso", "soat", "revisionTecnomecanica",
    "documentoIdentidadConductor", "documentoIdentidadConductorReverso",
    "documentoIdentidadPropietario", "documentoIdentidadPropietarioReverso",
    "documentoIdentidadTenedor", "documentoIdentidadTenedorReverso",
    "licencia", "licenciaReverso", "planillaEpsArl", "condFoto",
    "condCertificacionBancaria", "tenedCertificacionBancaria",
    "documentoAcreditacionTenedor", "rutTenedor",
    # (2026-10-01) La Hoja de vida FÍSICA salió de los requeridos: NO se le pide
    # al conductor en el paso 3 — la sube SEGURIDAD desde /revision solo en los
    # casos históricos que la entregaron en papel (sigue siendo tipo válido en
    # subir-documento y campo blindado).
    # (2026-10-14) "fotos" salió de la lista: los 4 ÁNGULOS se exigen aparte.
]

# Tope de fotos del vehículo por placa (4 ángulos + adicionales, en total).
MAX_FOTOS_VEHICULO = 10

# ÁNGULOS de foto del vehículo EXIGIDOS al finalizar (2026-10-14, pedido del
# usuario): «que se suban al menos 4 fotos — FRONTAL, LATERAL IZQ Y DERECHO,
# POSTERIOR». Cada ángulo vive en `fotosVehiculo` [{tipo, ruta, fecha}] (una
# por ángulo; re-subirla la REEMPLAZA) y `fotos` queda como espejo plano de
# todas (compat: carrusel, /revision, Excel, hash de la firma).
ANGULOS_FOTO_VEHICULO = ["frontal", "lateral_izq", "lateral_der", "posterior"]

# Documentos del SEGUNDO CONDUCTOR (2026-10-14): se exigen SOLO cuando el
# slot 2 está VINCULADO (`idConductor2`) — el 2º conductor es opcional y un
# vehículo con un solo conductor no debe exigir nada de él. Sin gemelos: es
# otra persona, ningún documento de otra figura lo cubre.
DOCUMENTOS_REQUERIDOS_CONDUCTOR2 = (
    "documentoIdentidadConductor2", "documentoIdentidadConductor2Reverso",
    "licencia2", "licencia2Reverso", "cond2Foto", "cond2CertificacionBancaria",
)
ETIQUETAS_FOTO_VEHICULO = {
    "frontal": "Foto Frontal del Vehículo",
    "lateral_izq": "Foto Lateral Izquierda del Vehículo",
    "lateral_der": "Foto Lateral Derecha del Vehículo",
    "posterior": "Foto Posterior del Vehículo",
}

ETIQUETAS_DOCUMENTO = {
    "tarjetaPropiedad": "Tarjeta de Propiedad",
    "tarjetaPropiedadReverso": "Tarjeta de Propiedad (Reverso)",
    "soat": "SOAT",
    "revisionTecnomecanica": "Revisión Tecnomecánica",
    "tarjetaRemolque": "Tarjeta de Remolque",
    "polizaResponsabilidad": "Póliza de Responsabilidad Civil",
    "documentoIdentidadConductor": "Documento de Identidad del Conductor",
    "documentoIdentidadConductorReverso": "Documento de Identidad del Conductor (Reverso)",
    "documentoIdentidadPropietario": "Documento de Identidad del Propietario",
    "documentoIdentidadPropietarioReverso": "Documento de Identidad del Propietario (Reverso)",
    "documentoIdentidadTenedor": "Documento de Identidad del Tenedor",
    "documentoIdentidadTenedorReverso": "Documento de Identidad del Tenedor (Reverso)",
    "documentoIdentidadRemolque": "Cédula del Dueño del Remolque",
    "documentoIdentidadRemolqueReverso": "Cédula del Dueño del Remolque (Reverso)",
    "licencia": "Licencia de Conducción Vigente",
    "licenciaReverso": "Licencia de Conducción (Reverso)",
    "planillaEpsArl": "Planilla de Seguridad Social",
    "condFoto": "Foto del Conductor",
    "condCertificacionBancaria": "Certificación Bancaria del Conductor",
    "propCertificacionBancaria": "Certificación Bancaria del Propietario",
    "tenedCertificacionBancaria": "Certificación Bancaria del Tenedor",
    "documentoAcreditacionTenedor": "Documento que lo acredite como Tenedor",
    "rutTenedor": "RUT del Tenedor",
    "rutPropietario": "RUT del Propietario",
    "fotos": "Fotos del vehículo",
    # SEGUNDO CONDUCTOR (2026-10-14): mismos documentos, persona distinta.
    "documentoIdentidadConductor2": "Documento de Identidad del Conductor 2",
    "documentoIdentidadConductor2Reverso": "Documento de Identidad del Conductor 2 (Reverso)",
    "licencia2": "Licencia de Conducción del Conductor 2",
    "licencia2Reverso": "Licencia de Conducción del Conductor 2 (Reverso)",
    "cond2Foto": "Foto del Conductor 2",
    "cond2CertificacionBancaria": "Certificación Bancaria del Conductor 2",
    # Ángulos de foto exigidos (2026-10-14): los devuelve _documentos_faltantes
    # y así el gate de «Finalizar» los lista con nombre legible.
    **ETIQUETAS_FOTO_VEHICULO,
    "hojaVidaFisica": "Hoja de Vida Física (autorización firmada)",
}

# Documentos de DOS CARAS aceptados por subir-documento (referencia
# compartida; antes vivía adentro del endpoint).
TIPOS_DOS_CARAS_SUBIDA = {
    "documentoIdentidadConductor", "documentoIdentidadPropietario",
    "documentoIdentidadTenedor", "documentoIdentidadRemolque",
    "licencia", "tarjetaPropiedad",
    # SEGUNDO CONDUCTOR (2026-10-14): cédula y licencia del slot 2 también
    # suben reverso (misma regla de dos caras).
    "documentoIdentidadConductor2", "licencia2",
}

# Tipos aceptados por subir-documento (referencia compartida).
TIPOS_SUBIDA_VALIDOS = [
    "tarjetaPropiedad", "soat", "revisionTecnomecanica", "tarjetaRemolque",
    "polizaResponsabilidad", "documentoIdentidadConductor", "documentoIdentidadPropietario",
    "documentoIdentidadTenedor", "licencia", "planillaEpsArl", "condFoto",
    "condCertificacionBancaria", "propCertificacionBancaria", "tenedCertificacionBancaria",
    "documentoAcreditacionTenedor", "rutTenedor", "rutPropietario",
    # Cédula del dueño del remolque (2026-10-05): figura opcional, sube
    # frente (+reverso vía `reverso`) igual que las demás cédulas.
    "documentoIdentidadRemolque",
    # Reversos como tipo directo: el paso 3 los sube como ítem propio.
    "licenciaReverso", "tarjetaPropiedadReverso",
    # (2026-09-28) Hoja de vida física firmada (alta por Seguridad).
    "hojaVidaFisica",
    # SEGUNDO CONDUCTOR (2026-10-14): sus documentos son tipos de subida
    # propios (reverso de la licencia como ítem; el de la cédula vía `reverso`
    # en la subida del frente, igual que el slot 1).
    "documentoIdentidadConductor2", "licencia2", "licencia2Reverso",
    "cond2Foto", "cond2CertificacionBancaria",
]

# Campos que SEGURIDAD puede RECORTAR en /revision (2026-10-07, pedido del
# usuario: fotos tomadas muy lejos que muestran cosas de sobra): documentos
# de UNA imagen, frente o reverso. FUERA: `firmaUrl` (el hash de la firma
# electrónica se calcula sobre la ruta), `fotos` (array) y `planillaEpsArl`
# (espejo del historial `documentosPlanillaSegSocial` — recortarla los
# desincronizaría).
CAMPOS_RECORTABLES = (
    set(TIPOS_SUBIDA_VALIDOS)
    | {f"{t}Reverso" for t in TIPOS_DOS_CARAS_SUBIDA}
    | {"fotoconductorseguridad"}
) - {"planillaEpsArl"}


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


def _es_empresa_figura(vehiculo: dict, campo_tipo: str) -> bool:
    """La figura (propietario/tenedor) es una EMPRESA: su tipo de documento
    es NIT (leído del RUT por la IA o elegido en el formulario)."""
    return "NIT" in str(vehiculo.get(campo_tipo) or "").upper()


# Datos del remolque guardados en la ficha (los que dispara el checkbox
# «mi vehículo tiene remolque» del paso 2).
CAMPOS_REMOLQUE = (
    "RemolPlaca", "RemolModelo", "RemolClase", "RemolTipoCarroceria",
    "RemolAlto", "RemolLargo", "RemolAncho",
    "RemolDuenoNombre", "RemolDuenoDocumento", "RemolDuenoCorreo",
)


def _tiene_remolque(vehiculo: dict) -> bool:
    """El vehículo tiene remolque declarado (algún dato de la sección)."""
    for campo in CAMPOS_REMOLQUE:
        valor = str(vehiculo.get(campo) or "").strip()
        if valor and valor not in ("null", "undefined"):
            return True
    return False


def _exento_tecnomecanica(vehiculo: dict) -> bool:
    """Vehículo de modelo con MENOS de 2 años (incluye modelos del año
    siguiente que ya vende el comercio): los vehículos nuevos NO requieren
    revisión tecnomecánica (pedido del usuario 2026-10-07)."""
    modelo = re.sub(r"\D", "", str(vehiculo.get("vehModelo") or ""))[:4]
    if len(modelo) != 4:
        return False
    try:
        anio = int(modelo)
    except ValueError:
        return False
    return anio >= datetime.now().year - 1


def _documentos_faltantes(vehiculo: dict) -> list:
    """
    Documentos obligatorios que faltan para pasar a completado_revision.
    Un documento de figura se satisface si su campo está lleno O si el de una
    figura igual (gemelo de la misma familia) lo está.
    La acreditación como Tenedor NO se exige cuando el tenedor ES el
    propietario (2026-08-31): la tarjeta de propiedad ya lo acredita.
    Figuras EMPRESA (NIT, 2026-09-28): la cédula no existe — se reemplaza
    por el RUT de la empresa (propietario); el tenedor ya exige RUT.
    """
    figuras = _figuras_iguales(vehiculo)
    prop_empresa = _es_empresa_figura(vehiculo, "propTipoDocumento")
    tened_empresa = _es_empresa_figura(vehiculo, "tenedTipoDocumento")

    faltantes = []
    for campo in DOCUMENTOS_REQUERIDOS:
        if campo == "documentoAcreditacionTenedor" and figuras["tened_igual_prop"]:
            continue
        # Empresa: la cédula de la figura no se exige.
        if prop_empresa and campo.startswith("documentoIdentidadPropietario"):
            continue
        if tened_empresa and campo.startswith("documentoIdentidadTenedor"):
            continue
        # Vehículo nuevo (modelo < 2 años): la tecnomecánica no se exige.
        if campo == "revisionTecnomecanica" and _exento_tecnomecanica(vehiculo):
            continue
        if _doc_lleno(vehiculo, campo):
            continue
        if any(_doc_lleno(vehiculo, gemelo) for gemelo in _gemelos_documento(campo, vehiculo)):
            continue
        faltantes.append(campo)

    # Propietario empresa: en lugar de la cédula, el RUT de la empresa.
    if prop_empresa and not _doc_lleno(vehiculo, "rutPropietario"):
        faltantes.append("rutPropietario")
    # Tarjeta de Remolque (2026-10-06, pedido del usuario): con remolque
    # declarado es OBLIGATORIA (sin remolque sigue siendo opcional y ni se
    # pide).
    if _tiene_remolque(vehiculo) and not _doc_lleno(vehiculo, "tarjetaRemolque"):
        faltantes.append("tarjetaRemolque")
    # FOTOS del vehículo por ÁNGULO (2026-10-14, pedido del usuario): se
    # exigen las 4 — frontal, lateral izquierda, lateral derecha y posterior.
    # `fotosVehiculo` las cataloga; un histórico con fotos sin catalogar
    # (solo el array plano) no pasa: hay que subir los 4 ángulos.
    angulos_fotos = {str(f.get("tipo") or "").strip() for f in (vehiculo.get("fotosVehiculo") or [])
                     if isinstance(f, dict) and str(f.get("ruta") or "").strip()}
    for angulo in ANGULOS_FOTO_VEHICULO:
        if angulo not in angulos_fotos:
            faltantes.append(angulo)
    # SEGUNDO CONDUCTOR (2026-10-14): sus 6 documentos, SOLO si está vinculado.
    if vehiculo.get("idConductor2"):
        for campo in DOCUMENTOS_REQUERIDOS_CONDUCTOR2:
            if not _doc_lleno(vehiculo, campo):
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
            "$set": {"estadoIntegra": "completado_revision", "fechaEstado": ahora},
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

    # Re-revisión: los estudios de seguridad se re-disparan SOLO si las
    # cédulas o la placa cambiaron (si no, se conservan los anteriores).
    _disparar_estudios_seguridad(placa, re_revision=True)


def _via_de_actor(actor: Optional[str], via: Optional[str] = None) -> str:
    """Canal de la mutación para la bitácora: el explícito si viene;
    si hay actor (Seguridad impersonando), 'impersonacion'; si no, el
    titular con su propia cuenta."""
    if via in ("conductor", "impersonacion", "seguridad"):
        return via
    return "impersonacion" if (actor or "").strip() else "conductor"


# ── Etiquetas legibles de campos para la bitácora (2026-10-14) ─────────────
# La auditoría de "datos_actualizados" decía solo "N campo(s)" y no se sabía
# QUÉ había cambiado (reporte del usuario con LCM768: filas "3 campo(s)" sin
# sentido). Las etiquetas espejan las de la pestaña Datos de /revision; los
# campos fuera del mapa caen a la regla de prefijos por figura.
ETIQUETAS_CAMPO_AUDITORIA = {
    "condNombres": "Nombres Conductor", "condPrimerApellido": "1er Apellido Conductor",
    "condSegundoApellido": "2do Apellido Conductor", "condCedulaCiudadania": "Cédula Conductor",
    "condExpedidaEn": "Ciudad Exp. Cédula", "condFechaExpedicion": "Fecha Expedición Cédula",
    "condFechaNacimiento": "Fecha Nacimiento Conductor", "condDireccion": "Dirección Conductor",
    "condCiudad": "Ciudad Residencia", "condDeptoCiudad": "Depto Residencia",
    "condCelular": "Celular Conductor", "condCorreo": "Correo Conductor",
    "condEps": "EPS", "condArl": "ARL", "condGrupoSanguineo": "Grupo Sanguíneo",
    "condNoLicencia": "No. Licencia", "condFechaVencimientoLic": "Vencimiento Licencia",
    "condCategoriaLic": "Categoría Licencia",
    "condNombreEmergencia": "Nombre Emergencia", "condCelularEmergencia": "Celular Emergencia",
    "condParentescoEmergencia": "Parentesco Emergencia",
    "condEmpresaRef": "Empresa Referencia", "condCelularRef": "Celular Referencia",
    "condCiudadRef": "Ciudad Referencia", "condDeptoCiudadRef": "Depto Referencia",
    "condNroViajesRef": "Nro. Viajes Ref", "condAntiguedadRef": "Antigüedad Ref",
    "condMercTransportada": "Mercancía Transportada",
    "condBanco": "Banco Conductor", "condTipoCuenta": "Tipo Cuenta Conductor",
    "condNumeroCuenta": "No. Cuenta Conductor",
    "propNombre": "Nombre Propietario", "propDocumento": "Doc. Propietario",
    "propTipoDocumento": "Tipo Doc. Propietario", "propCiudadExpDoc": "Ciudad Exp. Doc Prop",
    "propCorreo": "Correo Propietario", "propCelular": "Celular Propietario",
    "propDireccion": "Dirección Propietario", "propCiudad": "Ciudad Propietario",
    "propDeptoCiudad": "Depto Propietario", "propBanco": "Banco Propietario",
    "propTipoCuenta": "Tipo Cuenta Propietario", "propNumeroCuenta": "No. Cuenta Propietario",
    "propFechaInicioActividad": "Inicio Actividad RUT Prop.", "propFechaExpedicionRut": "Fecha Expedición RUT Prop.",
    "tenedNombre": "Nombre Tenedor", "tenedDocumento": "Doc. Tenedor",
    "tenedTipoDocumento": "Tipo Doc. Tenedor", "tenedCiudadExpDoc": "Ciudad Exp. Doc Tenedor",
    "tenedCorreo": "Correo Tenedor", "tenedCelular": "Celular Tenedor",
    "tenedDireccion": "Dirección Tenedor", "tenedCiudad": "Ciudad Tenedor",
    "tenedDeptoCiudad": "Depto Tenedor", "tenedBanco": "Banco Tenedor",
    "tenedTipoCuenta": "Tipo Cuenta Tenedor", "tenedNumeroCuenta": "No. Cuenta Tenedor",
    "tenedFechaInicioActividad": "Inicio Actividad RUT Tenedor", "tenedFechaExpedicionRut": "Fecha Expedición RUT Tenedor",
    "vehMarca": "Marca Vehículo", "vehLinea": "Línea Vehículo", "vehModelo": "Modelo Vehículo",
    "vehColor": "Color Vehículo", "vehTipoCarroceria": "Carrocería Vehículo",
    "vehCapacidadCarga": "Capacidad de Carga (kg)", "vehAno": "Año Repotenciación",
    "vehRepotenciado": "Repotenciado", "vehNoLicTransito": "Nº Licencia Tránsito",
    "vehCodigoLicTransito": "Código Licencia (LT)", "vehClase": "Clase Vehículo",
    "vehServicio": "Servicio Vehículo", "vehCilindraje": "Cilindraje",
    "vehCombustible": "Combustible", "vehCapPasajeros": "Capacidad Pasajeros",
    "vehVin": "VIN", "vehChasis": "Chasis", "vehMotor": "Motor",
    "vehAseguradoraSoat": "Aseguradora SOAT", "vehPolizaSoat": "Póliza SOAT",
    "vehVencimientoSoat": "Vencimiento SOAT",
    "vehEmpresaSat": "Empresa Satelital", "vehUsuarioSat": "Usuario Satelital", "vehClaveSat": "Clave Satelital",
    "RemolPlaca": "Placa Remolque", "RemolModelo": "Modelo Remolque", "RemolClase": "Clase Remolque",
    "RemolTipoCarroceria": "Carrocería Remolque", "RemolAlto": "Alto Remolque",
    "RemolLargo": "Largo Remolque", "RemolAncho": "Ancho Remolque",
    "RemolDuenoNombre": "Nombre Dueño Remolque", "RemolDuenoDocumento": "Doc. Dueño Remolque",
    "RemolDuenoTipoDocumento": "Tipo Doc. Dueño Remolque", "RemolDuenoCorreo": "Correo Dueño Remolque",
    "referenciasAdicionales": "Referencias Adicionales",
    "idConductor": "Conductor Vinculado", "idConductor2": "Conductor 2 Vinculado",
    "invitacionConductor2": "Invitación Conductor 2",
}
# SEGUNDO CONDUCTOR (2026-10-14): etiquetas de los campos del 2º derivadas de
# las del conductor 1 (cond2X → «<etiqueta> (2º Conductor)»).
ETIQUETAS_CAMPO_AUDITORIA.update({
    f"cond2{k[4:]}": f"{v} (2º Conductor)"
    for k, v in list(ETIQUETAS_CAMPO_AUDITORIA.items())
    if k.startswith("cond") and k != "condCorreo"
})
ETIQUETAS_CAMPO_AUDITORIA["cond2Correo"] = "Correo (2º Conductor)"

_PREFIJOS_FIGURA = (
    ("cond", "Conductor"), ("prop", "Propietario"), ("tened", "Tenedor"),
    ("Remol", "Remolque"), ("veh", "Vehículo"),
)


def _etiqueta_campo_auditoria(campo: str) -> str:
    """Etiqueta legible de un campo de la ficha (mapa curado → regla de
    prefijos por figura → nombre crudo como último recurso)."""
    if campo in ETIQUETAS_CAMPO_AUDITORIA:
        return ETIQUETAS_CAMPO_AUDITORIA[campo]
    for prefijo, figura in _PREFIJOS_FIGURA:
        if campo.startswith(prefijo):
            resto = re.sub(r"(?<!^)(?=[A-Z])", " ", campo[len(prefijo):]).strip()
            return f"{figura} · {resto}" if resto else figura
    return campo


def _detalle_campos_auditoria(cambios: list) -> str:
    """Detalle legible para la bitácora de datos_actualizados: cuántos campos
    y CUÁLES (etiquetas legibles). Con muchos campos se resume para no golpear
    el tope de 300 chars de la entrada."""
    etiquetas = [_etiqueta_campo_auditoria(c.get("campo", "")) for c in cambios]
    if len(etiquetas) > 6:
        resumen = ", ".join(etiquetas[:6]) + f" y {len(etiquetas) - 6} más"
    else:
        resumen = ", ".join(etiquetas)
    return f"{len(cambios)} campo(s) — {resumen}"


def _registrar_auditoria(placa: str, accion: str, actor: Optional[str] = None,
                         via: Optional[str] = None, detalle: Optional[str] = None) -> None:
    """
    Bitácora append-only del vehículo (auditoría): quién hizo cada mutación,
    cuándo y por qué canal — conductor con su cuenta | Seguridad impersonando
    (actor = su nombre) | Seguridad directa. BEST-EFFORT: un fallo JAMÁS
    tumba el endpoint que la llama. Tope 200 entradas (las más recientes).
    """
    entrada = {
        "fecha": datetime.utcnow(),
        "actor": (actor or "").strip() or None,
        "via": _via_de_actor(actor, via),
        "accion": accion,
    }
    if detalle:
        entrada["detalle"] = str(detalle)[:300]
    try:
        coleccion_vehiculos.update_one(
            {"placa": placa},
            {"$push": {"auditoriaVehiculo": {"$each": [entrada], "$slice": -200}}},
        )
    except Exception as e:
        print(f"[auditoria] No se pudo registrar '{accion}' de {placa}: {e}")


def _disparar_estudios_seguridad(placa: str, re_revision: bool = False,
                                 forzar: bool = False,
                                 manual: bool = False) -> None:
    """
    Lanza en background los estudios de seguridad automáticos del
    vehículo (cédulas de conductor/tenedor/propietario deduplicadas +
    placa) contra el proveedor configurado (hoy TusDatos). Fire-and-forget:
    un fallo JAMÁS tumba el endpoint que lo dispara.
    `forzar=True` → force del proveedor (re-consulta real, no su caché).
    `manual=True` → ignora el switch (acción EXPLÍCITA del usuario: botón
    «Volver a consultar» de /revision y módulo de antigüedad).
    Import local para evitar el ciclo vehiculos ↔ estudios_automaticos.
    SWITCH (2026-10-05): si el disparo automático está APAGADO (switch en
    config_estudios / env ESTUDIOS_AUTO_DISPARAR=false), no se lanza nada
    por los hooks automáticos. BUG corregido 2026-10-07: el endpoint manual
    `/disparar` también pasaba por el switch (lo llamó "no me hizo nada"
    con el switch apagado) — ahora marca manual=True.
    """
    try:
        from Funciones import estudios_automaticos
        if not manual and not estudios_automaticos.auto_disparo_habilitado():
            print(f"[estudios-auto] Disparo automático de {placa} OMITIDO (switch apagado)")
            return
        asyncio.create_task(
            estudios_automaticos.disparar_estudios(
                placa, re_revision=re_revision, forzar=forzar))
    except Exception as e:
        print(f"[estudios-auto] No se pudieron disparar los estudios de {placa}: {e}")


def enviar_notificacion_seguridad(placa: str, nombre_conductor_busqueda: str):
    """
    1. Busca usuarios con perfil 'SEGURIDAD' para enviar el correo.
    2. Busca al conductor por su NOMBRE (no por ID).
    (2026-10-07) Además envía WHATSAPP a los SEGURIDAD con celular —
    fire-and-forget e independiente del correo (plantilla
    `enruta_revision_pendiente`, ver _enviar_wa_seguridad).
    """
    # WhatsApp primero: no depende de la config de Resend.
    _enviar_wa_seguridad(placa, nombre_conductor_busqueda)

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
        print(f"[RESEND] Correo enviado a {destinatarios}. ID: {email}")

    except Exception as e:
        print(f"[RESEND] [ERROR] Error enviando correo: {str(e)}")


# ── WHATSAPP A SEGURIDAD (2026-10-07, pedido del usuario) ────────────────────
# Misma notificación del correo pero por el canal que el equipo sí mira:
# el conductor dejó un vehículo pendiente de revisión. Plantilla Meta
# `enruta_revision_pendiente` (Utilidad, es_CO) con {{1}}=nombre de Seguridad,
# {{2}}=conductor, {{3}}=placa y botón URL «Abrir IntegrApp».
# APROBADA en Meta desde 2026-10-08. ⚠️ El language.code debe ser «es_CO»
# EXACTO: la plantilla se creó en español (Colombia) y con «es» Meta responde
# 404 «Template name does not exist in the translation» aunque esté aprobada.
# Los celulares viven en `baseusuarios.celular` (mismo patrón de Otros Costos).
PLANTILLA_WA_REVISION = ("enruta_revision_pendiente", "es_CO")

# WhatsApp al CONDUCTOR/TENEDOR cuando Seguridad devuelve el vehículo con
# observaciones (2026-10-07, pedido del usuario): plantilla Meta
# `enruta_vehiculo_devuelto` (Utilidad, es) con {{1}}=nombre del conductor,
# {{2}}=placa, {{3}}=observaciones y botón URL «Abrir IntegrApp».
# ⚠️ REQUIERE crear y aprobar la plantilla en Meta Business Manager; sin
# aprobar, el envío falla en silencio (fire-and-forget, queda en el log).
PLANTILLA_WA_DEVOLUCION = ("enruta_vehiculo_devuelto", "es_CO")


def _normalizar_celular_co(celular) -> Optional[str]:
    """Celular a formato internacional Colombia: solo dígitos con 57."""
    if not celular:
        return None
    limpio = "".join(c for c in str(celular) if c.isdigit())
    if not limpio:
        return None
    if not limpio.startswith("57"):
        limpio = "57" + limpio
    return limpio


def _enviar_wa_seguridad(placa: str, nombre_conductor: str = "") -> None:
    """WhatsApp a los usuarios SEGURIDAD activos con celular. Fire-and-forget:
    un fallo (sin celular, plantilla sin aprobar, red) JAMÁS tumba el flujo."""
    try:
        from Funciones.whatsapp_utils_integra import enviar_template_sync
        usuarios = list(coleccion_baseusuarios.find({
            "perfil": "SEGURIDAD",
            "$or": [{"activo": True}, {"activo": {"$exists": False}}],
        }))
        if not usuarios:
            print("[WA-Seguridad] No hay usuarios SEGURIDAD activos; no se notifica.")
            return
        enviados = 0
        for u in usuarios:
            celular = _normalizar_celular_co(u.get("celular"))
            if not celular:
                print(f"[WA-Seguridad] {u.get('usuario') or u.get('correo') or '?'} sin celular valido; se saltea.")
                continue
            nombre = (u.get("nombre") or "Equipo").strip()
            res = enviar_template_sync(
                to=celular,
                template_name=PLANTILLA_WA_REVISION[0],
                language_code=PLANTILLA_WA_REVISION[1],
                body_params=[nombre, nombre_conductor or "Conductor", placa],
            )
            if res:
                enviados += 1
                print(f"[WA-Seguridad] OK -> {celular} ({nombre}) | {placa}")
            else:
                print(f"[WA-Seguridad] NO enviado a {celular} ({nombre}) — revisar plantilla '{PLANTILLA_WA_REVISION[0]}' y aprobacion en Meta.")
        print(f"[WA-Seguridad] {PLANTILLA_WA_REVISION[0]} {placa}: {enviados}/{len(usuarios)} notificados.")
    except Exception as e:
        print(f"[WA-Seguridad] Error: {e}")


def _celular_conductor_vehiculo(vehiculo: dict) -> Optional[str]:
    """Celular del responsable de la ficha: primero el campo de la ficha
    (`condCelular`, llenado por la IA o a mano) y si no, la cuenta vinculada
    (`conductores`, por idUsuario o idConductor — el alta propaga el celular
    de la ficha a la cuenta). Devuelve el celular normalizado 57… o None."""
    celular = _normalizar_celular_co(vehiculo.get("condCelular"))
    if celular:
        return celular
    # 2026-10-14: el 2º conductor también es responsable de la ficha.
    for campo in ("idUsuario", "idConductor", "idConductor2"):
        valor = vehiculo.get(campo)
        if not valor:
            continue
        try:
            cuenta_id = ObjectId(valor) if not isinstance(valor, ObjectId) else valor
        except Exception:
            continue  # id no válido (p. ej. cuentas de prueba) — siguiente fuente
        try:
            cuenta = coleccion_conductores_cuenta.find_one({"_id": cuenta_id}) or {}
        except Exception:
            cuenta = {}
        celular = _normalizar_celular_co(cuenta.get("celular") or cuenta.get("condCelular"))
        if celular:
            return celular
    return None


def _enviar_wa_devolucion(placa: str, vehiculo: dict, observaciones: str = "") -> None:
    """WhatsApp al conductor/tenedor cuando Seguridad DEVUELVE el vehículo con
    observaciones. Fire-and-forget: un fallo (sin celular, plantilla sin
    aprobar, red) JAMÁS tumba la devolución — queda en el log."""
    try:
        celular = _celular_conductor_vehiculo(vehiculo)
        if not celular:
            print(f"[WA-Devolucion] {placa}: sin celular del conductor; no se notifica.")
            return
        nombre = ((vehiculo.get("condNombres") or "").strip()) or "Conductor"
        texto_obs = (observaciones or "").strip()
        if not texto_obs:
            texto_obs = "Sin observaciones específicas"
        elif len(texto_obs) > 250:
            texto_obs = texto_obs[:250].rstrip() + "…"
        from Funciones.whatsapp_utils_integra import enviar_template_sync
        res = enviar_template_sync(
            to=celular,
            template_name=PLANTILLA_WA_DEVOLUCION[0],
            language_code=PLANTILLA_WA_DEVOLUCION[1],
            body_params=[nombre, placa, texto_obs],
        )
        if res:
            print(f"[WA-Devolucion] OK -> {celular} | {placa}")
        else:
            print(f"[WA-Devolucion] NO enviado a {celular} | {placa} — revisar plantilla "
                  f"'{PLANTILLA_WA_DEVOLUCION[0]}' y aprobacion en Meta.")
    except Exception as e:
        print(f"[WA-Devolucion] Error: {e}")


# ── PADRINOS: adopción de placas + avisos dirigidos (2026-10-15) ─────────────
# El padrino se AUTO-ASIGNA las placas que quiere apadrinar desde /revision
# (botón «Adoptar esta placa»); los avisos de cada placa le llegan SOLO a los
# padrinos que la adoptaron — no a todos los padrinos.
# Plantilla Meta `enruta_aviso_padrino` (Utilidad, es_CO) con {{1}}=nombre del
# padrino, {{2}}=placa, {{3}}=mensaje y botón URL «Abrir IntegrApp».
# ⚠️ REQUIERE crear/aprobar la plantilla en Meta Business Manager; sin aprobar,
# el WA falla en silencio (fire-and-forget) y el CORREO sigue llegando.
PLANTILLA_WA_PADRINO = ("enruta_aviso_padrino", "es_CO")

# Mensajes de cambio de estado para los padrinos (actualizar-estado).
ETIQUETAS_ESTADO_PADRINO = {
    "registro_incompleto": "La placa {placa} fue DEVUELTA al conductor para correcciones: {detalle}",
    "completado_revision": "El conductor finalizó el registro de {placa}: quedó PENDIENTE DE REVISIÓN.",
    "aprobado": "¡La placa {placa} fue APROBADA!",
    "en_actualizacion": "Se solicitó al conductor actualizar datos de {placa}: {detalle}",
    "inactivo": "La placa {placa} fue INACTIVADA: {detalle}",
    "rechazado": "La placa {placa} fue RECHAZADA de forma definitiva: {detalle}",
}


def _padrinos_de_placa(placa_o_vehiculo) -> list:
    """Padrinos que adoptaron la placa: acepta la placa (str) o el doc del
    vehículo (evita re-consultar cuando el llamador ya lo tiene)."""
    vehiculo = placa_o_vehiculo
    if isinstance(placa_o_vehiculo, str):
        vehiculo = coleccion_vehiculos.find_one({"placa": placa_o_vehiculo}, {"padrinos": 1})
    return [p for p in ((vehiculo or {}).get("padrinos") or [])
            if isinstance(p, dict) and (p.get("correo") or p.get("celular"))]


def _notificar_padrinos(placa: str, mensaje: str, titulo: Optional[str] = None) -> None:
    """
    Correo (Resend) + WhatsApp (plantilla entra_aviso_padrino) a los PADRINOS
    que adoptaron la placa. Fire-and-forget: un fallo JAMÁS tumba el flujo del
    que lo dispara (mismo patrón de _enviar_wa_seguridad).
    """
    try:
        padrinos = _padrinos_de_placa(placa)
        if not padrinos:
            return
        asunto = titulo or f"Actualización de la placa {placa}"
        # 1. Correo: canal que no depende de plantillas de Meta.
        if RESEND_API_KEY:
            destinatarios = [p.get("correo") for p in padrinos if p.get("correo")]
            if destinatarios:
                cuerpo_html = f"""
                <div style="font-family: Arial, sans-serif; color: #333; max-width: 600px;">
                    <h2 style="color: #006f7a;">{asunto}</h2>
                    <p>Como padrino de la placa <strong>{placa}</strong>, esta
                    actualización también te concierne:</p>
                    <div style="background-color: #f0f8f8; padding: 15px; border-radius: 8px;
                                margin: 20px 0; border: 1px solid #cce5e8;">
                        <p style="margin: 0; font-size: 15px;">{mensaje}</p>
                    </div>
                    <p>Ingresa a <b>IntegraApp</b> para ver el detalle.</p>
                    <hr style="border: 0; border-top: 1px solid #eee; margin-top: 30px;">
                    <p style="font-size: 12px; color: #999;">Notificación automática —
                    eres padrino de esta placa.</p>
                </div>"""
                params = {"from": MAIL_FROM, "to": destinatarios,
                          "subject": f"🤝 {asunto}", "html": cuerpo_html}
                email = resend.Emails.send(params)
                print(f"[padrinos] Correo enviado a {len(destinatarios)} padrino(s) de {placa}. ID: {email}")
        # 2. WhatsApp (fire-and-forget, plantilla pendiente de aprobación).
        from Funciones.whatsapp_utils_integra import enviar_template_sync
        mensaje_wa = (mensaje or "").strip()
        if len(mensaje_wa) > 250:
            mensaje_wa = mensaje_wa[:250].rstrip() + "…"
        for p in padrinos:
            celular = _normalizar_celular_co(p.get("celular"))
            if not celular:
                continue
            res = enviar_template_sync(
                to=celular,
                template_name=PLANTILLA_WA_PADRINO[0],
                language_code=PLANTILLA_WA_PADRINO[1],
                body_params=[(p.get("nombre") or "Padrino").strip(), placa, mensaje_wa],
            )
            if res:
                print(f"[padrinos] WA OK -> {celular} | {placa}")
            else:
                print(f"[padrinos] WA NO enviado a {celular} | {placa} — revisar plantilla "
                      f"'{PLANTILLA_WA_PADRINO[0]}' y aprobación en Meta.")
    except Exception as e:
        print(f"[padrinos] No se pudo notificar a los padrinos de {placa}: {e}")


# ==========================================
# 3. ENDPOINTS
# ==========================================

@ruta_vehiculos.post("/crear")
async def crear_vehiculo(
    id_usuario: Optional[str] = Form(None),  # ausente = BORRADOR del alta (placa-primero): la placa existe desde que se valida
    placa: str = Form(...),
    creado_por: Optional[str] = Form(None),   # nombre de Seguridad (módulo Alta de vehículo)
):
    placa_limpia = placa.strip().upper()
    if coleccion_vehiculos.find_one({"placa": placa_limpia}):
        raise HTTPException(status_code=400, detail="La placa ya está registrada.")

    # Alta placa-primero: sin id_usuario se crea un BORRADOR (registro_incompleto
    # sin responsable). Se building sobre la placa desde ya y el responsable se
    # vincula después con /asignar-responsable (regla CONDUCTOR=1/TENEDOR=N allá).
    es_borrador = not (id_usuario or "").strip()
    if not es_borrador:
        # Regla de negocio: el CONDUCTOR solo puede registrar UN vehículo propio
        # (varias placas es privilegio del TENEDOR, que además invita conductores).
        # No cuentan los vehículos donde llega como conductor invitado (idConductor).
        try:
            usuario = coleccion_conductores_cuenta.find_one({"_id": ObjectId(id_usuario)})
        except Exception:
            usuario = None
        perfil_usuario = (usuario or {}).get("perfil", "CONDUCTOR")
        if (perfil_usuario or "CONDUCTOR").strip().upper() != "TENEDOR":
            propios = coleccion_vehiculos.count_documents({"idUsuario": id_usuario})
            if propios >= 1:
                # (2026-10-14, pedido del usuario) Ya es dueño de su(s) ficha(s)
                # y conduce → ES tenedor de facto: se promueve automáticamente
                # en vez de bloquear (la regla CONDUCTOR=1 placa quedaba corta
                # para el dueño-conductor que quiere otra placa).
                try:
                    coleccion_conductores_cuenta.update_one(
                        {"_id": _a_objectid(id_usuario)}, {"$set": {"perfil": "TENEDOR"}})
                    print(f"[perfil] {id_usuario} promovido a TENEDOR al registrar "
                          f"su placa #{propios + 1} (era dueño-conductor)")
                except Exception as e:
                    print(f"[perfil] No se pudo promover a TENEDOR a {id_usuario}: {e}")

    nuevo_vehiculo = {
        "idUsuario": None if es_borrador else id_usuario,
        # True mientras el borrador del alta no tiene dueño de ficha.
        "responsable_pendiente": es_borrador,
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
        # SEGUNDO CONDUCTOR (2026-10-14): slot 2 OPCIONAL con HV completa
        # (cond2* + documentos propios + firma propia).
        "idConductor2": None,
        "invitacionConductor2": None,
        # Auditoría de ediciones sobre vehículos aprobados (re-revisión).
        "historialCambios": [],
        # Histórico de inactivaciones/reactivaciones por Seguridad.
        "historialInactivacion": [],
        # Fecha del último cambio de estado (para "tiempo esperando").
        "fechaEstado": datetime.utcnow(),
        # Datos extraídos por IA al subir documentos, por tipo de subida:
        # {rutTenedor: {datos, avisos, fecha}, ...} — el paso 2 los aplica.
        "lecturasIA": {},
        # Bitácora de auditoría (quién creó/modificó cada cosa): append-only.
        "auditoriaVehiculo": [],
    }

    coleccion_vehiculos.insert_one(nuevo_vehiculo)
    print(f" Vehículo creado: {placa_limpia} para usuario {id_usuario or '(borrador de alta)'}")
    _registrar_auditoria(
        placa_limpia, "vehiculo_creado", actor=creado_por,
        via="seguridad" if (creado_por or "").strip() else "conductor",
        detalle="Alta de la placa" + (" (borrador sin responsable)" if es_borrador else ""),
    )
    return JSONResponse(status_code=status.HTTP_201_CREATED, content={"message": "Vehículo registrado exitosamente"})


@ruta_vehiculos.get("/consultar-placa/{placa}")
def consultar_placa(placa: str):
    """
    Paso 1 del módulo Alta de vehículo (Seguridad): ¿la placa ya existe?
    Reporta su estado y quién es el dueño de la ficha (para NO duplicar ni
    crear cuentas de más). Sin documentos ni PII pesada.
    """
    placa_limpia = placa.strip().upper()
    doc = coleccion_vehiculos.find_one(
        {"placa": placa_limpia},
        {"estadoIntegra": 1, "idUsuario": 1, "responsable_pendiente": 1, "_id": 0},
    )
    if not doc:
        return {"existe": False, "estadoIntegra": None, "duenio": None, "borrador": False}

    duenio = None
    id_usuario = doc.get("idUsuario")
    if id_usuario:
        try:
            cuenta = coleccion_conductores_cuenta.find_one(
                {"_id": ObjectId(id_usuario)}, {"nombre": 1, "correo": 1, "perfil": 1, "_id": 0})
        except Exception:
            cuenta = None
        if cuenta:
            duenio = {
                "nombre": cuenta.get("nombre", ""),
                "correo": cuenta.get("correo", ""),
                "perfil": (cuenta.get("perfil") or "CONDUCTOR").upper(),
            }
    return {
        "existe": True,
        "estadoIntegra": doc.get("estadoIntegra"),
        "duenio": duenio,
        # Borrador del alta (placa-primero): existe pero sin responsable —
        # el módulo de alta ofrece CONTINUAR en vez de un error.
        "borrador": bool(doc.get("responsable_pendiente")),
    }


@ruta_vehiculos.put("/asignar-responsable/{placa}")
def asignar_responsable(
    placa: str,
    id_usuario: str = Form(...),
    asignado_por: Optional[str] = Form(None),  # nombre de Seguridad (trazabilidad)
):
    """
    Paso 2→3 del alta placa-primero: vincula el responsable de la ficha a un
    BORRADOR creado al validar la placa (POST /crear sin id_usuario). Aplica
    aquí la regla CONDUCTOR=1 placa / TENEDOR=N y deja auditoría.
    """
    placa_limpia = placa.strip().upper()
    vehiculo = coleccion_vehiculos.find_one({"placa": placa_limpia}, {"idUsuario": 1, "_id": 0})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    actual = vehiculo.get("idUsuario")
    if actual and actual != id_usuario:
        raise HTTPException(
            status_code=409,
            detail="La placa ya tiene un responsable distinto. Usa «Cuenta existente» con esa cuenta o inactiva el vehículo.",
        )
    if actual == id_usuario:
        return {"message": "El responsable ya estaba vinculado."}  # idempotente

    try:
        cuenta = coleccion_conductores_cuenta.find_one(
            {"_id": ObjectId(id_usuario)}, {"nombre": 1, "correo": 1, "perfil": 1, "_id": 0})
    except Exception:
        cuenta = None
    if not cuenta:
        raise HTTPException(status_code=404, detail="Cuenta no encontrada.")

    # Regla de negocio (misma de /crear): el TENEDOR tiene flota. El
    # CONDUCTOR dueño de otra ficha se PROMUEVE automáticamente a TENEDOR
    # (2026-10-14, pedido del usuario): ya es tenedor de facto.
    perfil = (cuenta.get("perfil") or "CONDUCTOR").strip().upper()
    if perfil != "TENEDOR":
        propios = coleccion_vehiculos.count_documents({"idUsuario": id_usuario})
        if propios >= 1:
            try:
                coleccion_conductores_cuenta.update_one(
                    {"_id": cuenta["_id"]}, {"$set": {"perfil": "TENEDOR"}})
                print(f"[perfil] {id_usuario} promovido a TENEDOR al asignarle "
                      f"su placa #{propios + 1} (era dueño-conductor)")
            except Exception as e:
                print(f"[perfil] No se pudo promover a TENEDOR a {id_usuario}: {e}")

    coleccion_vehiculos.update_one(
        {"placa": placa_limpia},
        {"$set": {"idUsuario": id_usuario, "responsable_pendiente": False}},
    )
    _registrar_auditoria(
        placa_limpia, "responsable_asignado", actor=asignado_por,
        via="seguridad" if (asignado_por or "").strip() else "conductor",
        detalle=f"Ficha asignada a {cuenta.get('correo', '')}",
    )
    return {"message": "Responsable vinculado.", "correo": cuenta.get("correo", "")}


@ruta_vehiculos.get("/obtener-vehiculos")
def obtener_vehiculos(id_usuario: str, estadoIntegra: Optional[str] = None):
    # $or: vehículos propios (idUsuario = dueño de la ficha, conductor legacy o
    # tenedor) + vehículos donde soy el conductor invitado (idConductor /
    # idConductor2 — el 2º conductor también ve la placa, 2026-10-14).
    filtro = {"$or": [
        {"idUsuario": id_usuario}, {"idConductor": id_usuario}, {"idConductor2": id_usuario},
    ]}
    if estadoIntegra:
        filtro = {"$and": [filtro, {"estadoIntegra": estadoIntegra}]}

    vehiculos = list(coleccion_vehiculos.find(filtro, {"_id": 0}))
    # La bitácora de auditoría es de /revision: el conductor no la necesita.
    for v in vehiculos:
        v.pop("auditoriaVehiculo", None)
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"message": "Búsqueda finalizada", "vehiculos": _json_seguro(_firmar_documentos(vehiculos))}
    )


@ruta_vehiculos.get("/obtener-vehiculo/{placa}")
async def obtener_vehiculo(placa: str):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa}, {"_id": 0})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")

    # La bitácora de auditoría es de /revision: el conductor no la necesita.
    vehiculo.pop("auditoriaVehiculo", None)
    return JSONResponse(status_code=status.HTTP_200_OK, content={"message": "Vehículo encontrado", "data": _json_seguro(_firmar_documentos(vehiculo))})

def _tiene_estudio_seguridad(vehiculo: dict) -> bool:
    """True si el vehículo tiene AL MENOS un estudio de seguridad en PDF
    (pedido del usuario 2026-10-06: requisito para aprobar): un adjunto
    cargado por Seguridad (`documentosEstudioSeguridad`, o el espejo legacy
    `estudioSeguridad`) o un estudio automático finalizado con reporte del
    proveedor (corrida vigente o histórica — ambos dejan PDF abrible)."""
    if vehiculo.get("documentosEstudioSeguridad"):
        return True
    if (vehiculo.get("estudioSeguridad") or "").strip():
        return True
    corridas = [(vehiculo.get("estudiosSeguridadAuto") or [])] + [
        (c.get("estudios") or []) for c in (vehiculo.get("historialEstudios") or [])
    ]
    for corrida in corridas:
        for est in corrida:
            if isinstance(est, dict) and est.get("estado") == "finalizado" \
                    and est.get("reporte_id"):
                return True
    return False


def _parsear_capacidad_kg(valor) -> Optional[int]:
    """Capacidad de carga (kg) → int, tolerante al formato colombiano.

    La IA (o el conductor) puede dejar el valor con separador de miles
    ('2.415' = 2415 kg): float() lo leería como 2.4 y el gate de
    completado_revision lo rechazaría por fuera del rango (bug real en
    QTZ352, 2026-10-08). Reglas: puntos/comas de MILES con grupos de 3
    dígitos se eliminan; coma decimal se asume solo si NO encaja en miles.
    Devuelve None si no hay número utilizable."""
    texto = str(valor or "").strip().replace(" ", "")
    if not texto:
        return None
    if re.fullmatch(r"\d{1,3}(\.\d{3})+", texto):        # 2.415 / 12.345.678
        texto = texto.replace(".", "")
    elif re.fullmatch(r"\d{1,3}(,\d{3})+", texto):       # 2,415 / 12,345
        texto = texto.replace(",", "")
    else:
        texto = texto.replace(",", ".")                  # coma decimal
    try:
        return int(round(float(texto)))
    except ValueError:
        return None


# Transiciones permitidas de estadoIntegra (2026-08-27): todo pasa por
# actualizar-estado, que antes aceptaba cualquier string. `inactivo` es un
# aprobado pausado por Seguridad (motivo obligatorio); reactivar vuelve a
# `aprobado` SIN re-revisión.
# `rechazado` (2026-10-05, pedido del usuario): NO se puede corregir — es un
# rechazo DEFINITIVO con motivo; el vehículo queda CANDADO (nadie lo edita:
# ni el conductor/tenedor ni Seguridad) y no tiene transiciones de salida.
TRANSICIONES_VALIDAS = {
    "registro_incompleto": {"completado_revision", "rechazado"},
    "completado_revision": {"aprobado", "registro_incompleto", "rechazado"},
    "aprobado": {"inactivo", "registro_incompleto", "en_actualizacion"},
    "inactivo": {"aprobado"},
    # Vehículo enviado a actualizar datos (2026-09-28): el conductor corrige
    # y «Finaliza» → revisión (con estudios re-disparados); Seguridad también
    # puede cancelar la actualización y devolverlo a aprobado.
    "en_actualizacion": {"completado_revision", "aprobado"},
    # Sin salidas: rechazado es final.
    "rechazado": set(),
}

def _perfil_de_actor(nombre: Optional[str]) -> Optional[str]:
    """
    Perfil REAL en `baseusuarios` del actor declarado (2026-10-14, patrón de
    `_resolver_usuario_flota`): busca por `usuario` o `nombre`. Best-effort:
    si no resuelve (actor de prueba, nombre libre) devuelve None y el llamador
    deja pasar — mismo nivel de confianza declarada del resto del sistema.

    (2026-10-15) Perfil ADICIONAL PADRINO: un usuario con otro perfil
    principal (p. ej. OPERATIVO) que tiene PADRINO en `perfiles_adicionales`
    solo entra a las mutaciones de vehículos por el portal simplificado → se
    le aplican los mismos límites del padrino. Los SEGURIDAD/ADMIN no se
    ven afectados (su perfil principal ya tiene más permisos).
    """
    texto = (nombre or "").strip()
    if not texto:
        return None
    try:
        doc = coleccion_baseusuarios.find_one({"usuario": texto.upper()})
        if not doc:
            doc = coleccion_baseusuarios.find_one({"nombre": {"$regex": f"^{re.escape(texto)}$", "$options": "i"}})
        if not doc:
            return None
        perfil = str(doc.get("perfil") or "").upper()
        if perfil not in ("SEGURIDAD", "ADMIN", "PADRINO"):
            adicionales = doc.get("perfiles_adicionales") or []
            if "PADRINO" in [str(p).upper() for p in adicionales]:
                return "PADRINO"
        return perfil
    except Exception as e:
        print(f"[perfil-actor] No se pudo resolver el perfil de {texto!r}: {e}")
        return None


@ruta_vehiculos.put("/actualizar-estado")
async def actualizar_estado(
    placa: str = Form(...),
    nuevo_estado: str = Form(...),
    usuario_id: str = Form(...),
    observaciones: Optional[str] = Form(None),
    motivo: Optional[str] = Form(None),
    nombre_conductor: str = Form("Conductor"),
    editado_por: Optional[str] = Form(None),   # actor real (Seguridad impersonando o directa)
    via: Optional[str] = Form(None),           # conductor | impersonacion | seguridad
):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")

    # PADRINO (2026-10-14): impersona placas SOLO para cargar documentos y
    # datos — NO autoriza estados. El front del padrino no ofrece los botones;
    # este guard tapa la llamada directa al endpoint.
    if (via or "") == "seguridad" and _perfil_de_actor(editado_por) == "PADRINO":
        raise HTTPException(
            status_code=403,
            detail="El perfil PADRINO no puede cambiar estados de vehículos: solo cargar documentos y datos. Pide a Seguridad que autorice.",
        )

    # Whitelist de transiciones: evita estados basura y saltos inválidos
    # (ej. registro_incompleto → aprobado directo, o re-revisión de un inactivo).
    estado_actual = vehiculo.get("estadoIntegra")
    if nuevo_estado not in TRANSICIONES_VALIDAS.get(estado_actual, set()):
        raise HTTPException(
            status_code=400,
            detail=f"Transición inválida: {estado_actual} → {nuevo_estado}.",
        )

    # Al pasar a completado_revision el conductor declara la documentación
    # completa: validar server-side ( Seguridad usa este endpoint con otros
    # estados y NO se le exige nada).
    capacidad_saneada = None
    if nuevo_estado == "completado_revision":
        faltantes = _documentos_faltantes(vehiculo)
        if faltantes:
            nombres = ", ".join(ETIQUETAS_DOCUMENTO.get(f, f) for f in faltantes)
            raise HTTPException(
                status_code=400,
                detail=f"Faltan documentos obligatorios: {nombres}.",
            )
        # Capacidad de carga (kg): obligatoria y dentro del rango operativo
        # (300–50.000). Si la IA leyó 0/ilegible, el conductor la digita a mano.
        # Parser tolerante al formato colombiano: '2.415' son 2415 kg, no 2.4
        # (bug real QTZ352 2026-10-08: el gate lo rechazaba por '2.4' < 300).
        capacidad_num = _parsear_capacidad_kg(vehiculo.get("vehCapacidadCarga"))
        if capacidad_num is None or capacidad_num < 300 or capacidad_num > 50000:
            raise HTTPException(
                status_code=400,
                detail="La Capacidad de Carga (kg) es obligatoria y debe estar entre 300 y 50.000 kg. Diligénciala en Datos del Vehículo.",
            )
        # Auto-sanear el valor guardado ('2.415' → '2415') para que el dato
        # quede limpio de raíz y los próximos gates/exports no lo re-padezcan.
        capacidad_saneada = str(capacidad_num) \
            if str(vehiculo.get("vehCapacidadCarga") or "").strip() != str(capacidad_num) else None

    # Inactivar un aprobado exige SIEMPRE un motivo (quedar en la base sin
    # operar debe ser explicable).
    if nuevo_estado == "inactivo" and not (motivo or "").strip():
        raise HTTPException(
            status_code=400,
            detail="El motivo de inactivación es obligatorio.",
        )

    # Rechazar es DEFINITIVO (2026-10-05): exige motivo y el vehículo queda
    # candado para todos (sin transiciones de salida ni ediciones).
    if nuevo_estado == "rechazado" and not ((motivo or "").strip() or (observaciones or "").strip()):
        raise HTTPException(
            status_code=400,
            detail="El motivo del rechazo es obligatorio.",
        )

    # Enviar a actualización de datos exige la observación de QUÉ actualizar
    # (es lo que ve el conductor en su panel).
    if nuevo_estado == "en_actualizacion" and not ((observaciones or "").strip() or (motivo or "").strip()):
        raise HTTPException(
            status_code=400,
            detail="La observación de qué debe actualizar el conductor es obligatoria.",
        )

    # Aprobar exige AL MENOS un estudio de seguridad en PDF (2026-10-06,
    # pedido del usuario): adjunto cargado por Seguridad o automático con
    # reporte. Solo aplica a completado_revision→aprobado (la acción real de
    # «Aprobar»): reactivar un inactivo y cancelar una actualización NO
    # re-evalúan (el vehículo ya fue aprobado una vez).
    if nuevo_estado == "aprobado" and estado_actual == "completado_revision":
        if not _tiene_estudio_seguridad(vehiculo):
            raise HTTPException(
                status_code=400,
                detail="Para aprobar debe existir al menos un archivo PDF de estudio de "
                       "seguridad. Cárgalo o consúltalo desde la pestaña «Estudios».",
            )

    ahora = datetime.utcnow()
    datos_actualizar = {
        "estadoIntegra": nuevo_estado,
        "usuarioIntegra": usuario_id,
        # Sello temporal del último cambio de estado (para "tiempo esperando").
        "fechaEstado": ahora,
    }
    # Capacidad saneada por el gate de completado_revision ('2.415' → '2415').
    if capacidad_saneada:
        datos_actualizar["vehCapacidadCarga"] = capacidad_saneada

    if observaciones:
        datos_actualizar["observaciones"] = observaciones
    elif nuevo_estado == "rechazado" and (motivo or "").strip():
        # El motivo del rechazo queda visible para el conductor como
        # observación (no hay camino de corrección — es informativo).
        datos_actualizar["observaciones"] = motivo.strip()
    elif nuevo_estado in ("aprobado", "completado_revision"):
        # Aprobar o re-enviar SIN comentario nuevo limpia la observación
        # anterior (p. ej. el motivo de una devolución): dejarla pegada
        # confundiría a quien consulte el estado del vehículo.
        datos_actualizar["observaciones"] = ""

    operacion = {"$set": datos_actualizar}
    # El origen de una actualización automática (documento eliminado, 2026-10-14)
    # deja de aplicar en cuanto el vehículo sale de en_actualizacion.
    if nuevo_estado != "en_actualizacion":
        operacion["$unset"] = {"actualizacionOrigen": ""}

    # Histórico de inactivación (append-only): quién, cuándo y por qué.
    if nuevo_estado == "inactivo":
        operacion["$push"] = {"historialInactivacion": {
            "fecha": ahora, "usuario": usuario_id,
            "motivo": motivo.strip(), "accion": "inactivo",
        }}
    elif nuevo_estado == "rechazado":
        # El rechazo viaja en el MISMO histórico (timeline de PestanaCambios)
        # con su acción propia; el motivo también queda en `observaciones`
        # para que el conductor lo vea.
        operacion["$push"] = {"historialInactivacion": {
            "fecha": ahora, "usuario": usuario_id,
            "motivo": ((motivo or "").strip() or (observaciones or "").strip()),
            "accion": "rechazado",
        }}
    elif nuevo_estado == "aprobado" and estado_actual == "inactivo":
        operacion["$push"] = {"historialInactivacion": {
            "fecha": ahora, "usuario": usuario_id,
            "motivo": (motivo or "").strip() or "Reactivado por Seguridad",
            "accion": "reactivado",
        }}

    coleccion_vehiculos.update_one({"placa": placa}, operacion)
    _registrar_auditoria(
        placa, f"estado_{nuevo_estado}", actor=editado_por, via=via,
        detalle=f"{estado_actual} → {nuevo_estado}"
        + (f" · motivo: {motivo.strip()}" if (motivo or "").strip() else "")
        # El comentario de aprobar/devolver también viaja en la auditoría para
        # que la timeline lo muestre. Se excluye rechazo/actualización porque
        # ahí el comentario YA es el motivo (vendría duplicado).
        + (f" · comentario: {observaciones.strip()}" if (observaciones or "").strip()
           and nuevo_estado not in ("rechazado", "en_actualizacion") else ""),
    )

    # WhatsApp al conductor/tenedor cuando Seguridad DEVUELVE el vehículo con
    # observaciones (2026-10-07): solo la devolución real de revisión
    # (completado_revision → registro_incompleto) — «Actualizar datos» desde
    # aprobado es otro flujo con su propia observación.
    if nuevo_estado == "registro_incompleto" and estado_actual == "completado_revision":
        _enviar_wa_devolucion(placa, vehiculo, observaciones or motivo or "")

    # AVISO A LOS PADRINOS que adoptaron la placa (2026-10-15): correo + WA
    # SOLO a quienes la adoptaron. Mismo cambio de estado que ve el conductor.
    _notificar_padrinos(
        placa,
        ETIQUETAS_ESTADO_PADRINO.get(nuevo_estado, f"Cambio de estado: {nuevo_estado}").format(
            placa=placa,
            detalle=(observaciones or motivo or "").strip() or "sin observaciones"),
        titulo=f"La placa {placa} cambió de estado",
    )

    # Al inactivar, el carro sale de la bolsa del día aunque tuviera check-in
    # activo (mismo patrón de _registrar_cambio_aprobado; la /bolsa además
    # filtra por estado como segunda barrera).
    if nuevo_estado == "inactivo":
        try:
            coleccion_disponibilidades.update_many(
                {"placa": placa, "estado": "activa"},
                {"$set": {"estado": "cancelada", "actualizado_en": ahora}},
            )
        except Exception as e:
            print(f"[disponibilidad] No se pudo cancelar el check-in al inactivar {placa}: {e}")

    if nuevo_estado == "completado_revision":
        enviar_notificacion_seguridad(placa, nombre_conductor)
        # Estudios de seguridad automáticos (TusDatos) para las cédulas de
        # las 3 figuras (deduplicadas) + la placa, en background.
        # SOLO cuando el registro lo finaliza el propio conductor/tenedor
        # (2026-10-01, pedido del usuario): si finaliza SEGURIDAD —
        # impersonando al conductor (via=impersonacion) o actuando directa
        # (via=seguridad) — los estudios NO se disparan solos; los dispara
        # Seguridad con «Volver a consultar» cuando evalue la placa.
        if (via or "conductor") not in ("seguridad", "impersonacion"):
            _disparar_estudios_seguridad(placa, re_revision=False)
        # Correos de AUTORIZACIÓN DE DATOS (2026-10-05) a los actores del
        # estudio sin evidencia (propietario/tenedor/dueño remolque sin
        # cuenta): link público + declaraciones. Va SIEMPRE (es un correo, no
        # gasto de TusDatos — independiente del switch de estudios) y es
        # idempotente por persona.
        try:
            from rutas import conductores as _conductores
            asyncio.create_task(asyncio.to_thread(
                _conductores.enviar_autorizaciones_pendientes, vehiculo))
        except Exception as e:
            print(f"[autorizacion] No se pudieron enviar los correos de {placa}: {e}")

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
    # SEGUNDO CONDUCTOR (2026-10-14): la vinculación la gestionan los
    # endpoints de /conductores (invitar/desvincular), jamás el front.
    # Los `cond2*` SÍ fluyen por aquí (son datos de formulario).
    "idConductor2", "invitacionConductor2",
    # Borrador del alta (placa-primero): lo escribe /asignar-responsable.
    "responsable_pendiente",
    "estudioSeguridad", "fotoconductorseguridad", "estudioSeguridadFecha", "lecturasIA", "fotos",
    # Sello de la firma electrónica: solo /vehiculos/firmar lo escribe.
    "firmaEvidencia",
    # Sello de la firma del SEGUNDO conductor (2026-10-14): igual, /firmar.
    "firmaEvidencia2",
    # Estudios automáticos (TusDatos): los escribe el disparador en
    # background, jamás el front.
    "estudiosSeguridadAuto", "historialEstudios", "documentosEstudioSeguridad",
    "estudiosVigencia",
    # Historial de planillas de seguridad social (2026-10-07): lo escribe
    # subir-documento (acumulación mensual), jamás el front.
    "documentosPlanillaSegSocial",
    # Historial universal de documentos (2026-10-07): lo escriben
    # subir-documento/reutilizar-documento, jamás el front.
    "historialDocumentos",
    # Bitácora de auditoría (quién creó/modificó cada cosa): la escriben los
    # endpoints del backend, jamás el front.
    "auditoriaVehiculo", "historialInactivacion",
    # Origen de una actualización automática por documento eliminado
    # (2026-10-14): lo escriben eliminar-documento/subir-documento.
    "actualizacionOrigen",
    # Catálogo de ángulos de fotos (2026-10-14): lo escribe subir-fotos.
    "fotosVehiculo",
    # PADRINOS que adoptaron la placa (2026-10-15): lo escriben los endpoints
    # adoptar/desadoptar-placa, jamás el front.
    "padrinos",
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
    "hojaVidaFisica",
    # Vencimiento de la planilla (2026-10-07): lo valida y escribe SOLO
    # subir-documento (tope 31 días, nunca vencida al subir).
    "planillaVencimiento",
    # Historial de planillas (2026-10-07): acumulación mensual server-side.
    "documentosPlanillaSegSocial",
    # Historial universal de documentos (2026-10-07): server-side.
    "historialDocumentos",
    # Reversos de documentos de dos caras (mismo blindaje que sus frentes).
    "documentoIdentidadConductorReverso", "documentoIdentidadPropietarioReverso",
    "documentoIdentidadTenedorReverso", "licenciaReverso", "tarjetaPropiedadReverso",
    # Cédula del dueño del remolque (2026-10-05): figura opcional aparte.
    "documentoIdentidadRemolque", "documentoIdentidadRemolqueReverso",
    # Catálogo de ángulos de las fotos (2026-10-14): lo escribe subir-fotos.
    "fotosVehiculo",
    # SEGUNDO CONDUCTOR (2026-10-14): sus documentos y su imagen de firma
    # (mismo blindaje que los del slot 1).
    "documentoIdentidadConductor2", "documentoIdentidadConductor2Reverso",
    "licencia2", "licencia2Reverso", "cond2Foto", "cond2CertificacionBancaria",
    "firmaUrl2",
}

# Referencias laborales adicionales (2026-08-31): la #1 son los campos planos
# cond*Ref; las extra viven en el array `referenciasAdicionales` — opcionales,
# tope 10, claves y tipos controlados (el endpoint acepta cualquier clave).
CLAVES_REF_ADICIONAL = {
    "empresa", "celular", "departamento", "ciudad", "nroViajes", "antiguedad", "mercancia",
}
MAX_REFERENCIAS_ADICIONALES = 10


def _sanear_refs_adicionales(valor):
    """Normaliza referenciasAdicionales: lista de dicts con claves permitidas y
    valores string; descarta vacías y recorta al tope. None si no aplica."""
    if not isinstance(valor, list):
        return None
    saneadas = []
    for entrada in valor[:MAX_REFERENCIAS_ADICIONALES]:
        if not isinstance(entrada, dict):
            continue
        limpia = {
            k: str(v).strip()[:120]
            for k, v in entrada.items()
            if k in CLAVES_REF_ADICIONAL and v is not None
        }
        # Solo referencias con algo diligenciado (empresa o celular).
        if limpia.get("empresa") or limpia.get("celular"):
            saneadas.append(limpia)
    return saneadas


def _asegurar_editable(vehiculo: dict):
    """Un vehículo RECHAZADO es de SOLO LECTURA para todos (2026-10-05, orden
    del usuario): ni el conductor/tenedor ni Seguridad pueden modificar su
    contenido (datos, documentos, fotos, firma). El estado no tiene
    transiciones de salida — el rechazo es definitivo."""
    if (vehiculo or {}).get("estadoIntegra") == "rechazado":
        raise HTTPException(
            status_code=403,
            detail="El vehículo está RECHAZADO y no puede editarse (rechazo definitivo de Seguridad).",
        )


@ruta_vehiculos.put("/actualizar-informacion/{placa}")
async def actualizar_informacion_vehiculo(placa: str, datos: dict, editado_por: Optional[str] = None):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    _asegurar_editable(vehiculo)

    # Defensa: el endpoint acepta cualquier clave, pero las internas se ignoran
    # y los campos de documentos también (sus dueños son los endpoints de subida).
    datos_limpios = {
        k: v for k, v in datos.items()
        if k not in CLAVES_PROTEGIDAS and k not in CAMPOS_DOCUMENTO_PROTEGIDOS
    }

    # Referencias adicionales: saneadas (claves permitidas, tope, sin vacías).
    # Un array vacío en un doc que NUNCA tuvo referencias extra no se escribe
    # (evita regar `referenciasAdicionales: []` por todos los históricos); si
    # el doc SÍ tenía, el [] vacío ES el borrado (persistir la eliminación).
    if "referenciasAdicionales" in datos_limpios:
        refs = _sanear_refs_adicionales(datos_limpios["referenciasAdicionales"])
        if refs is None or (not refs and "referenciasAdicionales" not in vehiculo):
            datos_limpios.pop("referenciasAdicionales", None)
        else:
            datos_limpios["referenciasAdicionales"] = refs

    if not datos_limpios:
        return JSONResponse(status_code=200, content={"message": "Información actualizada"})

    # Diff contra el documento actual: SOLO los campos cuyo valor cambia.
    # Un re-guardado sin cambios reales no baja un aprobado a revisión.
    cambios = [
        {"campo": k, "antes": vehiculo.get(k) if vehiculo.get(k) is not None else "(vacío)", "despues": v}
        for k, v in datos_limpios.items()
        if vehiculo.get(k) != v
        # «Sin referencias adicionales» es lo mismo que «campo ausente»: el
        # front manda el array SIEMPRE (vacío incluido) para que quitar
        # referencias persista; eso no debe contar como cambio.
        and not (k == "referenciasAdicionales" and not v and not vehiculo.get(k))
    ]

    coleccion_vehiculos.update_one({"placa": placa}, {"$set": datos_limpios})

    if cambios:
        _registrar_auditoria(
            placa, "datos_actualizados", actor=editado_por,
            detalle=_detalle_campos_auditoria(cambios),
        )
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

    # La cédula del SEGUNDO conductor (2026-10-14) va SOLO a SU cuenta
    # (idConductor2) — jamás a las del dueño/conductor 1.
    cedula2 = str(datos_limpios.get("cond2CedulaCiudadania") or "").strip()
    if cedula2:
        cedula2 = re.sub(r"\D", "", cedula2)
        id_cuenta2 = vehiculo.get("idConductor2")
        if id_cuenta2:
            try:
                coleccion_conductores_cuenta.update_one(
                    {"_id": _a_objectid(id_cuenta2), "cedula": {"$in": [None, ""]}},
                    {"$set": {"cedula": cedula2}},
                )
            except Exception as e:
                print(f"[cedula] No se pudo propagar la cedula a la cuenta {id_cuenta2}: {e}")

    # El NOMBRE llega igual a las cuentas que no lo tengan (el alta de
    # Seguridad pide solo correo + celular: el nombre lo aporta la IA al
    # leer la cédula del conductor o el RUT del tenedor). La cuenta TENEDOR
    # recibe el nombre del tenedor de la ficha; las demás, el del conductor.
    claves_nombre = {"condNombres", "condPrimerApellido", "condSegundoApellido", "tenedNombre"}
    if any(k in datos_limpios for k in claves_nombre):
        def _val(clave):
            return str(datos_limpios.get(clave) or vehiculo.get(clave) or "").strip()
        nombre_cond = " ".join(filter(None, [
            _val("condNombres"), _val("condPrimerApellido"), _val("condSegundoApellido"),
        ])).upper() or None
        nombre_tened = _val("tenedNombre").upper() or None
        for id_cuenta in {vehiculo.get("idUsuario"), vehiculo.get("idConductor")}:
            if not id_cuenta:
                continue
            try:
                cuenta_doc = coleccion_conductores_cuenta.find_one(
                    {"_id": _a_objectid(id_cuenta)}, {"perfil": 1, "_id": 0})
                if not cuenta_doc:
                    continue
                nombre_para = (nombre_tened
                               if (cuenta_doc.get("perfil") or "").upper() == "TENEDOR"
                               else nombre_cond)
                if not nombre_para:
                    continue
                coleccion_conductores_cuenta.update_one(
                    {"_id": _a_objectid(id_cuenta), "nombre": {"$in": [None, ""]}},
                    {"$set": {"nombre": nombre_para}},
                )
            except Exception as e:
                print(f"[nombre] No se pudo propagar el nombre a la cuenta {id_cuenta}: {e}")

    # El CELULAR del conductor llega igual a las cuentas que no lo tengan
    # (el registro ya no lo pide — 2026-10-06, mismo criterio del nombre).
    celular_cond = str(datos_limpios.get("condCelular") or "").strip()
    if celular_cond:
        for id_cuenta in {vehiculo.get("idUsuario"), vehiculo.get("idConductor")}:
            if not id_cuenta:
                continue
            try:
                coleccion_conductores_cuenta.update_one(
                    {"_id": _a_objectid(id_cuenta), "celular": {"$in": [None, ""]}},
                    {"$set": {"celular": celular_cond}},
                )
            except Exception as e:
                print(f"[celular] No se pudo propagar el celular a la cuenta {id_cuenta}: {e}")

    # SEGUNDO CONDUCTOR (2026-10-14): nombre y celular del 2º → SU cuenta.
    id_cuenta2 = vehiculo.get("idConductor2")
    if id_cuenta2:
        claves_nombre2 = {"cond2Nombres", "cond2PrimerApellido", "cond2SegundoApellido"}
        if any(k in datos_limpios for k in claves_nombre2):
            def _val2(clave):
                return str(datos_limpios.get(clave) or vehiculo.get(clave) or "").strip()
            nombre2 = " ".join(filter(None, [
                _val2("cond2Nombres"), _val2("cond2PrimerApellido"), _val2("cond2SegundoApellido"),
            ])).upper() or None
            if nombre2:
                try:
                    coleccion_conductores_cuenta.update_one(
                        {"_id": _a_objectid(id_cuenta2), "nombre": {"$in": [None, ""]}},
                        {"$set": {"nombre": nombre2}},
                    )
                except Exception as e:
                    print(f"[nombre] No se pudo propagar el nombre a la cuenta {id_cuenta2}: {e}")
        celular2 = str(datos_limpios.get("cond2Celular") or "").strip()
        if celular2:
            try:
                coleccion_conductores_cuenta.update_one(
                    {"_id": _a_objectid(id_cuenta2), "celular": {"$in": [None, ""]}},
                    {"$set": {"celular": celular2}},
                )
            except Exception as e:
                print(f"[celular] No se pudo propagar el celular a la cuenta {id_cuenta2}: {e}")

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
    placa: str = Form(...),
    editado_por: Optional[str] = Form(None),   # usuario de Seguridad que lo cargó
):
    placa_limpia = placa.strip().upper()
    vehiculo = coleccion_vehiculos.find_one({"placa": placa_limpia})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    _asegurar_editable(vehiculo)

    if archivo.content_type == "application/pdf":
        extension = "pdf"
    elif archivo.content_type.startswith("image/"):
        extension = "webp"
    else:
        raise HTTPException(status_code=400, detail="Solo se permiten archivos PDF o Imágenes.")

    nombre_archivo = _nombre_doc_bucket(placa_limpia, "estudioSeguridad", extension, vehiculo)

    try:
        url_archivo = subir_a_google_storage(archivo, nombre_archivo)
        fecha_carga = datetime.utcnow()
        # La carga ACUMULA en el historial del vehículo (nada se reemplaza):
        # `documentosEstudioSeguridad` es la fuente de la tabla de /revision.
        # `estudioSeguridad`/`estudioSeguridadFecha` se mantienen como espejo
        # del ÚLTIMO cargado (compat: el flujo de aprobación los exige).
        coleccion_vehiculos.update_one(
            {"placa": placa_limpia},
            {
                "$set": {
                    "estudioSeguridad": url_archivo,
                    "estudioSeguridadFecha": fecha_carga,
                },
                "$push": {
                    "documentosEstudioSeguridad": {
                        "$each": [{
                            "ruta": url_archivo,
                            "fecha": fecha_carga,
                            "nombre": archivo.filename or "",
                        }],
                        "$position": 0,
                        "$slice": 20,
                    }
                },
            }
        )
        _registrar_auditoria(
            placa_limpia, "estudio_seguridad_cargado", actor=editado_por,
            via="seguridad", detalle=archivo.filename or "",
        )
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": "Estudio de seguridad subido correctamente",
                "ruta": url_archivo, "url": _url_para_cliente(url_archivo),
                "fecha": fecha_carga.isoformat(),
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al procesar el archivo: {str(e)}")



@ruta_vehiculos.put("/subir-foto-seguridad")
async def subir_foto_seguridad(
    archivo: UploadFile = File(...),
    placa: str = Form(...),
    editado_por: Optional[str] = Form(None),   # usuario de Seguridad que la cargó
):
    placa_limpia = placa.strip().upper()
    vehiculo = coleccion_vehiculos.find_one({"placa": placa_limpia})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    _asegurar_editable(vehiculo)

    if not archivo.content_type.startswith("image/"):
         raise HTTPException(status_code=400, detail="Solo se permiten archivos de imagen para la foto del conductor.")

    nombre_archivo = _nombre_doc_bucket(placa_limpia, "fotoConductorSeguridad", "webp", vehiculo)

    try:
        url_archivo = subir_a_google_storage(archivo, nombre_archivo)
        coleccion_vehiculos.update_one(
            {"placa": placa_limpia},
            {"$set": {"fotoconductorseguridad": url_archivo}}
        )
        _registrar_auditoria(
            placa_limpia, "foto_seguridad_cargada", actor=editado_por,
            via="seguridad",
        )
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"message": "Foto de seguridad subida correctamente", "ruta": url_archivo, "url": _url_para_cliente(url_archivo)}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al procesar la foto de seguridad: {str(e)}")


# Tope de la fecha de vencimiento de la planilla de seguridad social contado
# desde HOY (pedido del usuario 2026-10-07: la planilla se renueva cada mes).
PLANILLA_MAX_DIAS = 31


def _fecha_vencimiento_planilla(manual, leida_ia) -> tuple[str, str]:
    """
    Resuelve y valida la fecha de VENCIMIENTO de la planilla de seguridad
    social (2026-10-07, pedido del usuario). Gana la que DIGITA quien sube
    (por si la IA no leyó); si no viene, la leída por la IA; si ninguna → 400.
    Reglas: fecha válida (YYYY-MM-DD o dd/mm/yyyy), NO vencida y con tope de
    31 días desde hoy (zona Bogotá). Devuelve (YYYY-MM-DD, origen).
    """
    hoy = datetime.now(_TZ_BOGOTA).date()
    tope = hoy + timedelta(days=PLANILLA_MAX_DIAS)

    valor = str(manual or "").strip()
    origen = "indicada"
    if not valor:
        valor = str(leida_ia or "").strip()
        origen = "leída por la IA"
    if not valor:
        raise HTTPException(
            status_code=400,
            detail="Indica la fecha de VENCIMIENTO de la planilla: la IA no la "
                   "pudo leer y es obligatoria.",
        )
    # La IA a veces devuelve dd/mm/yyyy — se normaliza.
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", valor)
    if m:
        valor = f"{m.group(3)}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"
    try:
        fecha = date.fromisoformat(valor[:10])
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"La fecha de vencimiento '{valor}' no es válida (usa una fecha real).",
        )
    if fecha < hoy:
        raise HTTPException(
            status_code=400,
            detail=f"La fecha de vencimiento ({fecha.isoformat()}) ya pasó: sube "
                   "una planilla VIGENTE.",
        )
    if fecha > tope:
        raise HTTPException(
            status_code=400,
            detail=f"La fecha de vencimiento ({fecha.isoformat()}) no puede ser "
                   f"superior a {PLANILLA_MAX_DIAS} días desde hoy (tope: "
                   f"{tope.isoformat()}).",
        )
    return fecha.isoformat(), origen


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
    fecha_vencimiento: Optional[str] = Form(None),
    reverso: Optional[UploadFile] = File(None),
):
    # Documentos de dos caras: el reverso se guarda junto al frente.
    # Cédulas de propietario/tenedor (2026-08-27): la tarjeta IA también pide
    # su reverso opcional (cédula amarilla), como la del conductor.
    if reverso and tipo not in TIPOS_DOS_CARAS_SUBIDA:
        raise HTTPException(status_code=400, detail="Ese tipo de documento no admite reverso.")
    tipos_validos = TIPOS_SUBIDA_VALIDOS
    if tipo not in tipos_validos:
        raise HTTPException(status_code=400, detail="Tipo de documento no válido.")

    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    _asegurar_editable(vehiculo)

    # Vehículo nuevo (modelo < 2 años): la tecnomecánica NO se requiere y NO
    # se deja subir (2026-10-07, pedido del usuario — coherente con
    # _documentos_faltantes, que tampoco la exige).
    if tipo == "revisionTecnomecanica" and _exento_tecnomecanica(vehiculo):
        raise HTTPException(
            status_code=400,
            detail=(
                "Este vehículo es nuevo (modelo "
                f"{str(vehiculo.get('vehModelo') or '').strip()}) y NO requiere "
                "Revisión Tecnomecánica — no la subas."
            ),
        )

    if archivo.content_type.startswith("image/"):
        extension = "webp"
    elif archivo.content_type == "application/pdf":
        # PDF protegido con contraseña (2026-10-15, pedido del usuario): antes
        # se guardaba «bien» y después nadie podía abrirlo. Se detecta el
        # diccionario /Encrypt del trailer y se rechaza ANTES de guardar nada.
        datos_pdf = await archivo.read()
        archivo.file.seek(0)
        if b"/Encrypt" in datos_pdf:
            raise HTTPException(
                status_code=400,
                detail=(
                    "El PDF está protegido con contraseña y NO se puede guardar: "
                    "ábrelo con la clave y expórtalo/guárdalo de nuevo SIN contraseña "
                    "(o tómale una foto) antes de subirlo."
                ),
            )
        extension = "pdf"
    else:
        raise HTTPException(status_code=400, detail="Solo se permiten archivos de imagen o PDF.")

    # El REVERSO también puede ser un PDF con clave: mismo rechazo.
    if reverso is not None and (reverso.content_type or "") == "application/pdf":
        datos_reverso = await reverso.read()
        reverso.file.seek(0)
        if b"/Encrypt" in datos_reverso:
            raise HTTPException(
                status_code=400,
                detail="El PDF del REVERSO está protegido con contraseña. Expórtalo sin clave y vuelve a subirlo.",
            )

    # Gemelos (figuras iguales): los que manda el front, filtrados por válidos,
    # + los que el propio backend calcula por si el front no los manda.
    gemelos_front = [g.strip() for g in (replicar_en or "").split(",") if g.strip()]
    gemelos = list(dict.fromkeys(
        [g for g in gemelos_front if g in tipos_validos and g != tipo]
        + [g for g in _gemelos_documento(tipo, vehiculo) if g in tipos_validos]
    ))

    # Para tipos leíbles: leer con IA ANTES de subir. Si Gemini determina que
    # NO es el documento esperado (409), que es un PDF con clave (400) o que no
    # logró leer NADA legible (422), se RECHAZA la subida completa (nada en el
    # bucket ni en Mongo) — 2026-10-15, pedido del usuario: "no subir basura";
    # el front ofrece «Adjuntar de todos modos» reenviando con extraer=false.
    # Fallos del PROVEEDOR (429/503/504 u otros): el archivo SÍ se sube.
    # `lectura_datos`/`lectura_avisos` (JSON): lectura YA hecha por el front
    # (flujo de dos caras: cédula/licencia/tarjeta leyeron frente+reverso por
    # separado) — se persiste en lecturasIA sin re-pagar Gemini.
    # Planilla de Seguridad Social (2026-10-11): si quien sube ya DIGITÓ la
    # fecha de vencimiento (obligatoria en ambas UIs: conductor y Seguridad),
    # NO se paga la lectura IA — Gemini tardaba 30-120 s con el request abierto
    # y el modal del conductor quedaba "pensando" eternamente aunque el
    # documento SÍ se guardaba (la gente cerraba creyendo que falló). La
    # lectura queda solo como fallback cuando no viene fecha manual, y sus
    # datos (eps/arl) no se consumen en ningún formulario del front.
    saltar_lectura = (
        tipo == "planillaEpsArl" and str(fecha_vencimiento or "").strip()
    )
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
    elif esquema_ia and not saltar_lectura and (extraer or "true").lower() != "false":
        try:
            # SEGUNDO CONDUCTOR (2026-10-14): sus documentos se validan contra
            # SU cédula — si no, la IA marcaría la cédula legítima del 2º como
            # "documento ajeno" (409) y bloquearía la subida completa.
            campo_cedula = ("cond2CedulaCiudadania" if tipo in TIPOS_LEIBLES_CONDUCTOR2
                            else "condCedulaCiudadania")
            contexto = {
                "placa_vehiculo": placa,
                "cedula_conductor": vehiculo.get(campo_cedula) or "",
            }
            datos_leidos = await asyncio.to_thread(extraer_datos_con_llm, esquema_ia, [archivo])
            archivo.file.seek(0)
            avisos = _generar_avisos(esquema_ia, datos_leidos, contexto)
            lectura_ia = {"datos": datos_leidos, "avisos": avisos}
        except HTTPException as e:
            if e.status_code in (409, 400, 422):
                # Documento equivocado (409, meme/factura/otro doc), PDF con
                # clave (400) o ilegible total (422): NO guardar nada — antes
                # el 422/400 se guardaban "por si acaso" y llenaban el bucket
                # de basura (2026-10-15). El conductor puede reintentar con
                # mejor foto o adjuntar de todos modos (extraer=false).
                raise
            else:
                # 429/503/504 (servicio saturado o caído): el archivo SÍ se
                # sube, campos a mano — no bloqueamos la operación por un
                # problema del proveedor de IA.
                print(f"[lecturaIA] Fallo leyendo {tipo} de {placa}: {e.detail}")
                lectura_ia = None
                archivo.file.seek(0)
        except Exception as e:
            print(f"[lecturaIA] Error inesperado leyendo {tipo} de {placa}: {e}")
            lectura_ia = None
            archivo.file.seek(0)

    # Historial universal de documentos (2026-10-07, pedido del usuario:
    # "todos los documentos son sujetos a actualización, nada se pierde"):
    # cada subida de CUALQUIER documento queda en `historialDocumentos` con su
    # ruta, y el blob lleva sufijo de versión `_v{N}` para que re-subir NUNCA
    # pise el archivo anterior (antes re-subir el mismo día lo sobrescribía).
    # La 1ª subida mantiene el nombre base (compat con lo existente).
    def _sufijo_version(tipo_doc: str, es_reverso: bool = False) -> str:
        previas = sum(
            1 for h in (vehiculo.get("historialDocumentos") or [])
            if h.get("tipo") == tipo_doc and bool(h.get("reverso")) == es_reverso)
        return f"_v{previas + 1}" if previas else ""

    # Planilla de Seguridad Social: la fecha de VENCIMIENTO es OBLIGATORIA
    # (2026-10-07) — gana la digitada, cae a la leída por la IA, sin ninguna no
    # se sube NADA (ni bucket ni Mongo). Con ella el vehículo queda
    # inhabilitado de la bolsa al vencer.
    fecha_planilla = None
    if tipo == "planillaEpsArl":
        fecha_planilla, _origen_fecha = _fecha_vencimiento_planilla(
            fecha_vencimiento,
            (lectura_ia or {}).get("datos", {}).get("fecha_vencimiento"))

    nombre_archivo = _nombre_doc_bucket(
        placa, tipo, extension, vehiculo, sufijo=_sufijo_version(tipo))
    url_archivo = subir_a_google_storage(archivo, nombre_archivo)

    set_inicial = {tipo: url_archivo}
    if fecha_planilla:
        set_inicial["planillaVencimiento"] = fecha_planilla
    set_inicial.update({g: url_archivo for g in gemelos})

    # Reverso (solo docs de dos caras): se guarda con su propio campo y URL.
    # También se replica a los gemelos de figura (identidad): si prop==cond,
    # el reverso de la cédula cubre a ambas figuras igual que el frente.
    url_reverso = None
    if reverso is not None:
        if reverso.content_type.startswith("image/"):
            extension_rev = "webp"
        elif reverso.content_type == "application/pdf":
            extension_rev = "pdf"
        else:
            raise HTTPException(status_code=400, detail="El reverso solo puede ser imagen o PDF.")
        nombre_reverso = _nombre_doc_bucket(
            placa, f"{tipo}Reverso", extension_rev, vehiculo,
            sufijo=_sufijo_version(tipo, es_reverso=True))
        url_reverso = subir_a_google_storage(reverso, nombre_reverso)
        set_inicial[f"{tipo}Reverso"] = url_reverso
        set_inicial.update({f"{g}Reverso": url_reverso for g in gemelos})

    # Planilla de Seguridad Social (2026-10-07): se ACTUALIZA MENSUALMENTE →
    # cada carga ACUMULA en `documentosPlanillaSegSocial` (nada se reemplaza,
    # mismo patrón de los PDFs de estudio) y `planillaEpsArl` queda como
    # espejo de la ÚLTIMA (compat: gate de documentos, paso 3 del conductor
    # e históricos). Además, TODA subida de CUALQUIER documento entra al
    # `historialDocumentos` universal (ver _sufijo_version arriba).
    fecha_carga = datetime.utcnow()
    entradas_historial = [{
        "tipo": tipo,
        "etiqueta": ETIQUETAS_DOCUMENTO.get(tipo, tipo),
        "ruta": url_archivo,
        "fecha": fecha_carga,
        "nombre": archivo.filename or "",
        "actor": editado_por or "",
    }]
    if url_reverso:
        entradas_historial.append({
            "tipo": tipo,
            "etiqueta": ETIQUETAS_DOCUMENTO.get(tipo, tipo) + " (reverso)",
            "reverso": True,
            "ruta": url_reverso,
            "fecha": fecha_carga,
            "nombre": reverso.filename or "",
            "actor": editado_por or "",
        })
    pushes = {
        "historialDocumentos": {
            "$each": entradas_historial, "$position": 0, "$slice": -200,
        }
    }
    if tipo == "planillaEpsArl":
        pushes["documentosPlanillaSegSocial"] = {
            "$each": [{
                "ruta": url_archivo,
                "fecha": fecha_carga,
                "nombre": archivo.filename or "",
                "vence": fecha_planilla,
            }],
            "$position": 0,
            "$slice": 24,
        }
    update_doc: dict = {"$set": set_inicial, "$push": pushes}
    coleccion_vehiculos.update_one({"placa": placa}, update_doc)
    _registrar_auditoria(
        placa, "documento_subido", actor=editado_por,
        detalle=ETIQUETAS_DOCUMENTO.get(tipo, tipo) + (" (+ reverso)" if url_reverso else ""),
    )

    # Edición de un aprobado por el conductor/tenedor → baja a re-revisión.
    # EXCEPCIONES: documentos que son ADJUNTOS de actualización periódica, no
    # datos del conductor — NO sacan un aprobado de la bolsa:
    #  - Hoja de Vida FÍSICA (2026-10-01): la sube Seguridad (casos históricos).
    #  - Planilla de Seguridad Social (2026-10-07): se renueva CADA MES —
    #    bajar el vehículo a re-revisión por subirla lo inhabilitaba sin razón.
    TIPOS_SIN_REVISION = {"hojaVidaFisica", "planillaEpsArl"}
    if tipo not in TIPOS_SIN_REVISION:
        campos_diff = [{"campo": tipo, "antes": vehiculo.get(tipo) or "(ninguno)", "despues": url_archivo}]
        campos_diff += [
            {"campo": g, "antes": vehiculo.get(g) or "(ninguno)", "despues": url_archivo}
            for g in gemelos
        ]
        if url_reverso:
            campos_diff.append({"campo": f"{tipo}Reverso", "antes": vehiculo.get(f"{tipo}Reverso") or "(ninguno)", "despues": url_reverso})
            campos_diff += [
                {"campo": f"{g}Reverso", "antes": vehiculo.get(f"{g}Reverso") or "(ninguno)", "despues": url_reverso}
                for g in gemelos
            ]
        _registrar_cambio_aprobado(vehiculo, editado_por or "", "documentos", campos_diff)

    # (2026-10-14) La planilla es el documento que se elimina/re-sube sin
    # generar re-revisión: si el vehículo estaba inhabilitado en
    # en_actualizacion POR eliminarla y ya no falta nada, se rehabilita solo.
    if tipo == "planillaEpsArl":
        _rehabilitar_tras_planilla(placa, editado_por)

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
            "ruta": url_archivo,
            "url": _url_para_cliente(url_archivo),
            "lectura_ia": lectura_ia,
            "replicado_en": gemelos,
            "ruta_reverso": url_reverso,
            "url_reverso": _url_para_cliente(url_reverso) if url_reverso else None,
        }
    )


@ruta_vehiculos.put("/subir-documento-adicional")
async def subir_documento_adicional(
    archivo: UploadFile = File(...),
    placa: str = Form(...),
    nombre: str = Form(...),
    editado_por: Optional[str] = Form(None),
):
    """
    Documento ADICIONAL con NOMBRE libre (2026-10-15, pedido del usuario):
    Seguridad/Admin adjuntan desde /revision cualquier otro documento
    (contratos, certificados, comunicaciones…) con una etiqueta propia que
    ellos digitan. Acumula en `documentosAdicionales` (nada se reemplaza) y
    entra al `historialDocumentos` universal. NO baja un aprobado a
    re-revisión: es un adjunto de archivo, no un dato del conductor (misma
    excepción de la Hoja de Vida física). Solo SEGURIDAD/ADMIN: el perfil
    PADRINO no puede (403 best-effort por actor, patrón del resto del módulo).
    """
    etiqueta = (nombre or "").strip()[:80]
    if not etiqueta:
        raise HTTPException(status_code=400, detail="Escribe un nombre para el documento adicional.")

    if _perfil_de_actor(editado_por) == "PADRINO":
        raise HTTPException(
            status_code=403,
            detail="El perfil PADRINO no puede cargar documentos adicionales (solo Seguridad).",
        )

    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    _asegurar_editable(vehiculo)

    if archivo.content_type.startswith("image/"):
        extension = "webp"
    elif archivo.content_type == "application/pdf":
        # Mismo chequeo anti-clave que subir-documento (2026-10-15).
        datos_pdf = await archivo.read()
        archivo.file.seek(0)
        if b"/Encrypt" in datos_pdf:
            raise HTTPException(
                status_code=400,
                detail=(
                    "El PDF está protegido con contraseña y NO se puede guardar: "
                    "expórtalo de nuevo SIN contraseña (o tómale una foto) antes de subirlo."
                ),
            )
        extension = "pdf"
    else:
        raise HTTPException(status_code=400, detail="Solo se permiten archivos de imagen o PDF.")

    # Numeración adicional_NNN por vehículo: cada adjunto lleva su blob propio.
    previos = sum(
        1 for h in (vehiculo.get("historialDocumentos") or [])
        if h.get("tipo") == "adicional"
    )
    nombre_archivo = _nombre_doc_bucket(
        placa, "adicional", extension, vehiculo, sufijo=f"_{previos + 1:03d}")
    url_archivo = subir_a_google_storage(archivo, nombre_archivo)

    fecha_carga = datetime.utcnow()
    entrada_historial = {
        "tipo": "adicional",
        "etiqueta": etiqueta,
        "ruta": url_archivo,
        "fecha": fecha_carga,
        "nombre": archivo.filename or "",
        "actor": editado_por or "",
    }
    coleccion_vehiculos.update_one(
        {"placa": placa},
        {"$push": {
            "historialDocumentos": {"$each": [entrada_historial], "$position": 0, "$slice": -200},
            "documentosAdicionales": {"$each": [{
                "nombre": etiqueta,
                "ruta": url_archivo,
                "fecha": fecha_carga,
                "actor": editado_por or "",
            }], "$position": 0},
        }},
    )
    _registrar_auditoria(placa, "documento_subido", actor=editado_por, detalle=f"Adicional: {etiqueta}")

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "message": f"Documento adicional «{etiqueta}» cargado",
            "ruta": url_archivo,
            "url": _url_para_cliente(url_archivo),
            "nombre": etiqueta,
        }
    )


def _padrinos_para_respuesta(padrinos: list) -> list:
    """`fecha` es datetime en Mongo: a ISO para la respuesta JSON."""
    return [{**p, "fecha": p["fecha"].isoformat()} if isinstance(p.get("fecha"), datetime) else p
            for p in (padrinos or [])]


@ruta_vehiculos.put("/adoptar-placa")
async def adoptar_placa(
    placa: str = Form(...),
    padrino_id: str = Form(...),
    nombre: Optional[str] = Form(None),
    correo: Optional[str] = Form(None),
):
    """
    El PADRINO se auto-asigna la placa que quiere apadrinar (2026-10-15,
    pedido del usuario): desde el panel de la placa en /revision. Los avisos
    de la placa (cambios de estado, invitaciones, documentos eliminados…)
    les llegan SOLO a los padrinos que la adoptaron.
    Datos de contacto AUTORITATIVOS desde baseusuarios por id (correo y
    celular para las notificaciones); los pasados por el front son fallback.
    Idempotente: adoptar dos veces no duplica.
    """
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    _asegurar_editable(vehiculo)

    # Resolver la cuenta del padrino (por id: el front manda seguridadId).
    cuenta = None
    try:
        cuenta = coleccion_baseusuarios.find_one({"_id": ObjectId(padrino_id)})
    except Exception:
        cuenta = None
    entrada = {
        "id": padrino_id,
        "nombre": ((cuenta or {}).get("nombre") or nombre or "Padrino").strip(),
        "correo": str((cuenta or {}).get("correo") or correo or "").strip().lower() or None,
        "celular": _normalizar_celular_co((cuenta or {}).get("celular")),
        "fecha": datetime.utcnow(),
    }
    # $pull + $push no pueden ir en el mismo update (conflicto de ruta): dos pasos.
    coleccion_vehiculos.update_one({"placa": placa}, {"$pull": {"padrinos": {"id": padrino_id}}})
    coleccion_vehiculos.update_one({"placa": placa}, {"$push": {"padrinos": entrada}})
    _registrar_auditoria(placa, "placa_adoptada", actor=entrada["nombre"],
                         detalle="El padrino comenzará a recibir los avisos de esta placa")
    actualizado = coleccion_vehiculos.find_one({"placa": placa}, {"padrinos": 1}) or {}
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"message": f"Ahora eres padrino de {placa}", "padrinos": _padrinos_para_respuesta(actualizado.get("padrinos"))},
    )


@ruta_vehiculos.put("/desadoptar-placa")
async def desadoptar_placa(placa: str = Form(...), padrino_id: str = Form(...)):
    """El padrino deja de apadrinar la placa: deja de recibir sus avisos."""
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    _asegurar_editable(vehiculo)
    coleccion_vehiculos.update_one({"placa": placa}, {"$pull": {"padrinos": {"id": padrino_id}}})
    try:
        cuenta = coleccion_baseusuarios.find_one({"_id": ObjectId(padrino_id)}) or {}
    except Exception:
        cuenta = {}
    _registrar_auditoria(placa, "placa_desadoptada",
                         actor=(cuenta.get("nombre") or padrino_id),
                         detalle="El padrino dejó de recibir los avisos de esta placa")
    actualizado = coleccion_vehiculos.find_one({"placa": placa}, {"padrinos": 1}) or {}
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"message": f"Dejaste de ser padrino de {placa}", "padrinos": _padrinos_para_respuesta(actualizado.get("padrinos"))},
    )


# ─────────────────────────────────────────────────────────────────────────────
# RECORTE de documentos por SEGURIDAD (2026-10-07, pedido del usuario: hay
# fotos tomadas muy lejos que muestran cosas de sobra y no quieren que las
# imágenes queden así). Seguridad dibuja un recuadro sobre la imagen en
# /revision y guarda la copia recortada como versión NUEVA del documento:
# el ORIGINAL jamás se pierde (queda en `historialDocumentos`), no se
# re-lee con IA (el contenido no cambió, solo el encuadre) y NO baja un
# aprobado a re-revisión (es una edición cosmética de Seguridad, no un
# cambio de datos del conductor).
# ─────────────────────────────────────────────────────────────────────────────

_MEDIA_TYPES_BLOB = {
    "webp": "image/webp", "png": "image/png", "jpg": "image/jpeg",
    "jpeg": "image/jpeg", "pdf": "application/pdf",
}


@ruta_vehiculos.get("/documento-bruto/{placa}")
def documento_bruto(placa: str, campo: str):
    """
    Bytes del documento ACTUAL de la placa, descargados SERVER-SIDE. Lo usa el
    recorte de /revision para pintar la imagen en un canvas SIN problema de
    CORS (las URLs firmadas de GCS no cargan en canvas desde otro origen si el
    bucket no tiene CORS configurado; por acá viaja same-origin).
    """
    if campo not in CAMPOS_RECORTABLES:
        raise HTTPException(status_code=400, detail="Campo de documento no válido.")
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    ruta = vehiculo.get(campo)
    if not isinstance(ruta, str) or not ruta.strip():
        raise HTTPException(status_code=404, detail="El vehículo no tiene ese documento cargado.")
    try:
        datos = _descargar_blob(ruta)
    except Exception as e:
        print(f"[recorte] No se pudo descargar {campo} de {placa}: {e}")
        raise HTTPException(status_code=502, detail="No se pudo descargar el documento del almacenamiento.")
    extension = str(ruta).rsplit(".", 1)[-1].lower()
    return Response(content=datos, media_type=_MEDIA_TYPES_BLOB.get(extension, "application/octet-stream"))


@ruta_vehiculos.put("/recortar-documento")
async def recortar_documento(
    archivo: UploadFile,
    placa: str = Form(...),
    campo: str = Form(...),
    editado_por: Optional[str] = Form(None),
):
    """
    Guarda la imagen RECORTADA por Seguridad como versión nueva del documento
    (`_v{N}` en el bucket + entrada en `historialDocumentos` con marca
    `recorte`). Los campos gemelos que compartían la ruta anterior también se
    actualizan (la réplica por figura es la misma imagen).
    """
    if campo not in CAMPOS_RECORTABLES:
        raise HTTPException(status_code=400, detail="Campo de documento no válido.")
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    _asegurar_editable(vehiculo)
    if not archivo.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="El recorte debe ser una imagen.")
    anterior = vehiculo.get(campo)
    if not isinstance(anterior, str) or not anterior.strip():
        raise HTTPException(status_code=404, detail="El vehículo no tiene ese documento cargado.")

    # Versión del blob: mismas reglas del historial universal (cuenta las
    # subidas previas del documento, sea como tipo directo o como reverso de
    # su base — la 1ª vez mantiene el nombre base).
    previas = 0
    for h in (vehiculo.get("historialDocumentos") or []):
        if h.get("tipo") == campo and not h.get("reverso"):
            previas += 1
        elif campo.endswith("Reverso") and h.get("tipo") == campo[: -len("Reverso")] and h.get("reverso"):
            previas += 1
    sufijo = f"_v{previas + 1}" if previas else ""

    nombre = _nombre_doc_bucket(placa, campo, "webp", vehiculo, sufijo=sufijo)
    ruta_nueva = subir_a_google_storage(archivo, nombre)

    # El campo apunta a la nueva versión; los gemelos que compartían la ruta
    # anterior también (misma imagen replicada por figura).
    set_campos = {campo: ruta_nueva}
    for g in CAMPOS_RECORTABLES:
        if g != campo and vehiculo.get(g) == anterior:
            set_campos[g] = ruta_nueva

    fecha = datetime.utcnow()
    coleccion_vehiculos.update_one(
        {"placa": placa},
        {
            "$set": set_campos,
            "$push": {
                "historialDocumentos": {
                    "$each": [{
                        "tipo": campo,
                        "etiqueta": ETIQUETAS_DOCUMENTO.get(campo, campo) + " (recorte)",
                        "recorte": True,
                        "ruta": ruta_nueva,
                        "fecha": fecha,
                        "nombre": archivo.filename or "recorte.webp",
                        "actor": editado_por or "",
                    }],
                    "$position": 0,
                    "$slice": -200,
                }
            },
        },
    )
    _registrar_auditoria(
        placa, "documento_recortado", actor=editado_por, via="seguridad",
        detalle=ETIQUETAS_DOCUMENTO.get(campo, campo),
    )
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "message": "Documento recortado",
            "ruta": ruta_nueva,
            "url": _url_para_cliente(ruta_nueva),
        },
    )


def _copiar_blob_bucket(ruta_origen: str, nombre_destino: str) -> str:
    """Copia un blob ya existente del bucket a otra ruta (server-side, sin
    bajar/subir por el cliente). Devuelve la RUTA del blob destino."""
    nombre_origen = _nombre_blob_de_ruta(ruta_origen)
    bucket = _obtener_cliente_storage().bucket(BUCKET_NAME)
    bucket.copy_blob(bucket.blob(nombre_origen), bucket, f"{CARPETA_STORAGE}/{nombre_destino}")
    return f"{CARPETA_STORAGE}/{nombre_destino}"


# Documentos reutilizables del conductor hacia propietario/tenedor («es la
# misma persona»): copia el blob server-side SIN re-leer con IA (ahorra
# Gemini). 2026-08-31: además de la cédula, el certificado bancario.
DOCUMENTOS_REUTILIZABLES = {
    # Campos por FIGURA (2026-10-01: el origen ya no es siempre el conductor —
    # puede copiarse ENTRE cualquier par de figuras que ya tengan el documento).
    "cedula": {
        "campos": {
            "conductor": "documentoIdentidadConductor",
            "propietario": "documentoIdentidadPropietario",
            "tenedor": "documentoIdentidadTenedor",
            # Dueño del remolque (2026-10-05): figura opcional — puede copiarse
            # la cédula de cualquiera de las otras tres figuras.
            "remolque": "documentoIdentidadRemolque",
        },
        "dos_caras": True,
        "nombre": "Cédula",
    },
    "certificado_bancario": {
        "campos": {
            "conductor": "condCertificacionBancaria",
            "propietario": "propCertificacionBancaria",
            "tenedor": "tenedCertificacionBancaria",
        },
        "dos_caras": False,  # una sola cara
        "nombre": "Certificado bancario",
    },
}

_ETIQUETA_FIGURA = {
    "conductor": "el conductor",
    "propietario": "el propietario",
    "tenedor": "el tenedor",
    "remolque": "el dueño del remolque",
}


@ruta_vehiculos.put("/reutilizar-documento")
@ruta_vehiculos.put("/reutilizar-cedula")
async def reutilizar_documento(
    placa: str = Form(...),
    figura: str = Form(...),
    documento: str = Form("cedula"),
    origen: Optional[str] = Form(None),  # figura de la que se copia (default conductor, compat)
    editado_por: Optional[str] = Form(None),
):
    """«Es la misma persona»: copia un documento ya cargado de UNA figura
    (cédula de conductor/propietario/tenedor, certificado bancario) a OTRA
    SIN volver a leer con IA (ahorra Gemini). `documento` = cedula (default)
    | certificado_bancario; `figura` = destino; `origen` = de quién se copia
    (default conductor — compat con el front desplegado). Puede subirse en
    CUALQUIER orden: si el conductor sube primero la del propietario, la del
    conductor puede copiarse de esa.

    Copia el blob (frente + reverso si aplica) con la nomenclatura PROPIA de
    la figura destino — cada figura conserva su archivo y su campo en Mongo;
    NO es la replicación por gemelos, que comparte la ruta).
    La lecturasIA del origen se copia también al tipo destino para que el
    formulario pueda autollenar los datos de esa figura al montar.
    (La ruta /reutilizar-cedula se mantiene por compatibilidad con el front
    desplegado; el nuevo nombre es /reutilizar-documento.)"""
    figura = figura.strip().lower()
    origen = (origen or "conductor").strip().lower()
    spec = DOCUMENTOS_REUTILIZABLES.get((documento or "cedula").strip().lower())
    if not spec:
        raise HTTPException(
            status_code=400,
            detail="Documento no válido: use cedula o certificado_bancario.",
        )
    campos = spec["campos"]
    tipo_destino = campos.get(figura)
    if not tipo_destino:
        raise HTTPException(
            status_code=400,
            detail="Figura no válida: use conductor, propietario, tenedor o remolque.",
        )
    tipo_origen = campos.get(origen)
    if not tipo_origen:
        raise HTTPException(
            status_code=400,
            detail="Origen no válido: use conductor, propietario, tenedor o remolque.",
        )
    if origen == figura:
        raise HTTPException(
            status_code=400,
            detail="El origen y el destino de la copia son la misma figura.",
        )

    # Normalización server-side + fallback insensible a mayúsculas/espacios:
    # el 404 real de prod (2026-08-31, MVX48E) solo puede venir de un valor
    # que no coincide byte a byte con la placa del doc; se loguea el valor
    # RECIBIDO para diagnosticarlo desde el log de Render.
    placa_norm = (placa or "").strip().upper()
    vehiculo = coleccion_vehiculos.find_one({"placa": placa_norm})
    if not vehiculo:
        try:
            vehiculo = coleccion_vehiculos.find_one({
                "placa": {"$regex": f"^{re.escape(placa_norm)}$", "$options": "i"}
            })
        except Exception as e:
            print(f"[reutilizar-cedula] Fallback de placa falló: {e}")
    if not vehiculo:
        print(f"[reutilizar-cedula] 404: placa recibida={placa!r} (normalizada={placa_norm!r})")
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    _asegurar_editable(vehiculo)

    url_cond = vehiculo.get(tipo_origen)
    if not url_cond or not (_es_ruta_documento(url_cond) or str(url_cond).startswith("https://")):
        raise HTTPException(
            status_code=409,
            detail=f"{_ETIQUETA_FIGURA[origen].capitalize()} no tiene {spec['nombre'].lower()} cargado todavía.",
        )

    try:
        # Frente: mismo tipo de archivo que el original (webp o pdf).
        extension = "pdf" if str(url_cond).lower().endswith(".pdf") else "webp"
        nombre_frente = _nombre_doc_bucket(placa, tipo_destino, extension, vehiculo)
        ruta_destino = await asyncio.to_thread(_copiar_blob_bucket, url_cond, nombre_frente)

        # Reverso (solo docs de dos caras, si el conductor lo subió): campo propio.
        ruta_reverso_destino = None
        if spec["dos_caras"]:
            ruta_rev_cond = vehiculo.get(f"{tipo_origen}Reverso")
            if ruta_rev_cond and (_es_ruta_documento(ruta_rev_cond) or str(ruta_rev_cond).startswith("https://")):
                ext_rev = "pdf" if str(ruta_rev_cond).lower().endswith(".pdf") else "webp"
                nombre_rev = _nombre_doc_bucket(placa, f"{tipo_destino}Reverso", ext_rev, vehiculo)
                ruta_reverso_destino = await asyncio.to_thread(_copiar_blob_bucket, ruta_rev_cond, nombre_rev)

        # Placa CANÓNICA del doc (no la recibida): si el fallback fue quien
        # encontró el vehículo, los $set deben filtrar por la placa real.
        placa_doc = vehiculo["placa"]
        set_doc = {tipo_destino: ruta_destino}
        if ruta_reverso_destino:
            set_doc[f"{tipo_destino}Reverso"] = ruta_reverso_destino
        # Historial universal (2026-10-07): la reutilización también es una
        # carga de documento (copia server-side) y queda registrada.
        entrada_hist = {
            "tipo": tipo_destino,
            "etiqueta": f"{spec['nombre']} de {origen} (♻️ reutilizado)",
            "ruta": ruta_destino,
            "fecha": datetime.utcnow(),
            "nombre": f"reutilizado de {tipo_origen}",
            "actor": editado_por or "",
        }
        coleccion_vehiculos.update_one(
            {"placa": placa_doc},
            {"$set": set_doc,
             "$push": {"historialDocumentos": {
                 "$each": [entrada_hist], "$position": 0, "$slice": -200}}},
        )

        # La lectura IA de la figura ORIGEN queda disponible también para la
        # destino (autollenado de identidad/bancario en el próximo montaje).
        lectura_cond = (vehiculo.get("lecturasIA") or {}).get(tipo_origen)
        if lectura_cond and lectura_cond.get("datos"):
            coleccion_vehiculos.update_one(
                {"placa": placa_doc},
                {"$set": {f"lecturasIA.{tipo_destino}": {
                    **lectura_cond,
                    "reutilizada_de": tipo_origen,
                    "fecha": datetime.utcnow(),
                }}},
            )

        # Edición de un aprobado → baja a re-revisión con diff del documento.
        campos_diff = [{"campo": tipo_destino, "antes": vehiculo.get(tipo_destino) or "(ninguno)", "despues": ruta_destino}]
        if ruta_reverso_destino:
            campos_diff.append({"campo": f"{tipo_destino}Reverso", "antes": vehiculo.get(f"{tipo_destino}Reverso") or "(ninguno)", "despues": ruta_reverso_destino})
        _registrar_auditoria(
            placa_doc, "documento_reutilizado", actor=editado_por,
            detalle=f"{spec['nombre']} de {origen} → {figura}",
        )
        _registrar_cambio_aprobado(vehiculo, editado_por or "", "documentos", campos_diff)

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": f"{spec['nombre']} de {origen} reutilizado como {figura}",
                "ruta": ruta_destino,
                "url": _url_para_cliente(ruta_destino),
                "url_reverso": _url_para_cliente(ruta_reverso_destino) if ruta_reverso_destino else None,
                # _json_seguro: lecturasIA.{tipo}.fecha es datetime en Mongo y
                # JSONResponse no lo serializa (bug real 2026-08-31 en MVX48E:
                # 500 "object of type datetime is not json serializable" y el
                # front mostraba "no pudimos reutilizar la cédula" aunque la
                # copia del blob y el $set YA habían quedado hechos).
                "lectura_ia": _json_seguro(lectura_cond) or None,
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al reutilizar el documento: {str(e)}")


@ruta_vehiculos.put("/subir-fotos")
async def subir_fotos(
    archivos: List[UploadFile],
    placa: str = Form(...),
    editado_por: Optional[str] = Form(None),   # actor real (el front ya lo enviaba)
    # Ángulo de la foto (2026-10-14): frontal | lateral_izq | lateral_der |
    # posterior — UNA foto por ángulo (re-subirla la reemplaza). Sin el parámetro
    # (o "otra") son fotos adicionales: comportamiento histórico (acumulan).
    foto_tipo: Optional[str] = Form(None),
):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    _asegurar_editable(vehiculo)

    es_angulo = (foto_tipo or "").strip() in ANGULOS_FOTO_VEHICULO
    if es_angulo and len(archivos) != 1:
        raise HTTPException(
            status_code=400,
            detail=f"El ángulo «{ETIQUETAS_FOTO_VEHICULO.get(foto_tipo, foto_tipo)}» lleva UNA foto (llevas {len(archivos)}).",
        )

    # Tope de fotos por vehículo: máximo 10 en total (las ya subidas + estas).
    # Se deduplican primero (sanear docs contaminados por el bug de numeración:
    # la misma URL repetida N veces cuenta como UNA foto).
    actuales = [u for u in (vehiculo.get("fotos") or []) if u and str(u).strip()]
    actuales = list(dict.fromkeys(actuales))
    # Re-subir un ÁNGULO reemplaza su foto: la anterior sale del espejo plano
    # (el blob queda en el bucket) para no ocupar cupo del tope.
    angulos_previos = [f for f in (vehiculo.get("fotosVehiculo") or []) if isinstance(f, dict)]
    ruta_anterior_angulo = None
    if es_angulo:
        previa = next((f for f in angulos_previos if f.get("tipo") == foto_tipo
                       and str(f.get("ruta") or "").strip()), None)
        if previa:
            ruta_anterior_angulo = str(previa["ruta"])
    # La numeración se calcula sobre TODAS las fotos existentes (ANTES de
    # sacar la reemplazada): si no, la nueva reutilizaría su nombre foto_00N
    # del mismo día y PISARÍA el blob que acaba de reemplazar. El número se
    # extrae con regex del segmento foto_NN (desde 2026-10-02 el nombre lleva
    # sufijo _{placa} al final — el parser viejo rsplit("_") leía "test01.webp",
    # int() reventaba en silencio y CADA tanda del día reiniciaba en 001
    # pisando los blobs de las tandas anteriores).
    fecha_hoy = datetime.now(_TZ_BOGOTA).strftime("%Y-%m-%d")
    prefijo_hoy = f"{placa.strip().upper()}/{fecha_hoy}/foto_"
    maximo_hoy = 0
    for url_existente in actuales:
        s = str(url_existente)
        if prefijo_hoy in s:
            m = re.search(r"foto_(\d+)", s)
            if m:
                maximo_hoy = max(maximo_hoy, int(m.group(1)))
    if ruta_anterior_angulo:
        actuales = [u for u in actuales if u != ruta_anterior_angulo]
    if len(actuales) + len(archivos) > MAX_FOTOS_VEHICULO:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Máximo {MAX_FOTOS_VEHICULO} fotos del vehículo. "
                f"Ya tienes {len(actuales)}; puedes subir hasta {MAX_FOTOS_VEHICULO - len(actuales)} más."
            ),
        )

    urls_fotos = []
    # (La numeración CONTINÚA desde el mayor sufijo _NNN usado HOY — ver el
    # bloque de arriba: evita que re-subir pise el blob de foto_001.)
    for indice, archivo in enumerate(archivos, start=maximo_hoy + 1):
        nombre_archivo = _nombre_doc_bucket(placa, "foto", "webp", vehiculo, sufijo=f"_{indice:03d}")
        url_archivo = subir_a_google_storage(archivo, nombre_archivo)
        urls_fotos.append(url_archivo)

    operacion: dict = {
        # $set (no $push): escribe el array saneado (sin duplicados) + las
        # nuevas, reparando de paso los docs con URLs repetidas del bug de
        # numeración.
        "$set": {"fotos": actuales + urls_fotos}
    }
    if es_angulo:
        # Catálogo de ángulos: la foto nueva REEMPLAZA a la anterior del mismo
        # ángulo (una por ángulo, orden fijo de ANGULOS_FOTO_VEHICULO).
        nuevos_angulos = [f for f in angulos_previos if f.get("tipo") != foto_tipo]
        nuevos_angulos.append({
            "tipo": foto_tipo,
            "ruta": urls_fotos[0],
            "fecha": datetime.utcnow(),
        })
        nuevos_angulos.sort(key=lambda f: ANGULOS_FOTO_VEHICULO.index(f.get("tipo")))
        operacion["$set"]["fotosVehiculo"] = nuevos_angulos
    coleccion_vehiculos.update_one({"placa": placa}, operacion)
    _registrar_auditoria(
        placa, "fotos_subidas", actor=editado_por,
        detalle=(f"{ETIQUETAS_FOTO_VEHICULO.get(foto_tipo, foto_tipo)} (reemplazada)"
                 if es_angulo and ruta_anterior_angulo else
                 (ETIQUETAS_FOTO_VEHICULO.get(foto_tipo, foto_tipo) if es_angulo
                  else f"{len(urls_fotos)} foto(s)")),
    )
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"message": "Fotos subidas correctamente", "rutas": urls_fotos, "urls": _firmar_documentos(urls_fotos)}
    )


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
    _asegurar_editable(vehiculo)

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
        _registrar_auditoria(placa, "firma_subida", actor=editado_por)

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={"message": "Firma subida correctamente", "ruta": url_archivo, "url": _url_para_cliente(url_archivo)}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error subiendo firma: {str(e)}")


@ruta_vehiculos.put("/firmar")
async def firmar(
    request: Request,
    archivo: UploadFile = File(...),
    placa: str = Form(...),
    id_usuario: Optional[str] = Form(None),
    editado_por: Optional[str] = Form(None),
    slot: int = Form(1),  # 1|2: conductor principal | segundo conductor (2026-10-14)
):
    """
    Firma ELECTRÓNICA del conductor (nivel de evidencia reforzada, Ley 1955
    art. 76 / Decreto 1499 de 2020): en UN solo acto
      1. sube la imagen de la firma al bucket (nomenclatura estándar),
      2. calcula el SHA-256 de los datos declarados del vehículo EN ESE
         MOMENTO y de la propia imagen,
      3. sella un registro append-only en `firmas_conductor` con fecha UTC,
         IP y user-agent (mismo patrón de evidencia que aceptaciones_politica).
    El registro es inmutable y sobrevive a ediciones posteriores del vehículo;
    re-firmar agrega un registro nuevo (version 2, 3, …), nunca reemplaza.
    `slot=2` (2026-10-14): firma del SEGUNDO conductor — exige `idConductor2`
    vinculado, hashea SOLO sus datos (`_hash_datos_firmados_slot2`) y escribe
    los punteros firmaUrl2/firmaEvidencia2. Los registros llevan `slot`.
    """
    if slot not in (1, 2):
        raise HTTPException(status_code=400, detail="slot inválido: debe ser 1 o 2.")
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    _asegurar_editable(vehiculo)
    if slot == 2 and not vehiculo.get("idConductor2"):
        raise HTTPException(
            status_code=400,
            detail="Este vehículo no tiene un segundo conductor vinculado.",
        )

    contenido = await archivo.read()
    if not contenido:
        raise HTTPException(status_code=400, detail="La firma está vacía: dibújala antes de firmar.")
    hash_firma = hashlib.sha256(contenido).hexdigest()

    # El hash amarra la firma a los datos EXACTOS declarados en este momento.
    if slot == 2:
        hash_datos = _hash_datos_firmados_slot2(vehiculo)
    else:
        hash_datos = _hash_datos_firmados(vehiculo)

    try:
        archivo.file.seek(0)
        nombre_archivo = _nombre_doc_bucket(placa, "firma" if slot == 1 else "firma2", "webp", vehiculo)
        url_archivo = subir_a_google_storage(archivo, nombre_archivo)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error subiendo la firma: {str(e)}")

    ahora = datetime.utcnow()
    ip = request.client.host if request.client else ""
    user_agent = (request.headers.get("user-agent") or "")[:300]

    if slot == 2:
        prefijo = "cond2"
        campos_nombre = ("cond2Nombres", "cond2PrimerApellido", "cond2SegundoApellido")
        cedula = re.sub(r"\D", "", str(vehiculo.get("cond2CedulaCiudadania") or ""))
        nombre = " ".join(str(vehiculo.get(k) or "").strip() for k in campos_nombre).strip()
        correo = (vehiculo.get("cond2Correo") or "").upper()
        id_usuario = id_usuario or vehiculo.get("idConductor2")
    else:
        prefijo = "cond"
        cedula = re.sub(r"\D", "", str(vehiculo.get("condCedulaCiudadania") or ""))
        nombre = " ".join(
            str(vehiculo.get(k) or "").strip()
            for k in ("condNombres", "condPrimerApellido", "condSegundoApellido")
        ).strip()
        correo = (vehiculo.get("condCorreo") or "").upper()
        id_usuario = id_usuario or vehiculo.get("idUsuario")

    # Versiones POR SLOT: las del 1 son los registros históricos (sin `slot`)
    # más los nuevos slot=1; las del 2 cuentan solo los slot=2.
    filtro_version = {"placa": placa, "slot": 2} if slot == 2 else {"placa": placa, "slot": {"$ne": 2}}
    version = 1 + coleccion_firmas.count_documents(filtro_version)
    registro = {
        "placa": placa,
        "slot": slot,
        "version": version,
        "id_usuario": id_usuario,
        "cedula": cedula,
        "nombre": nombre,
        "correo": correo,
        "hash_datos": hash_datos,
        "firma_url": url_archivo,
        "hash_firma": hash_firma,
        "firmado_en": ahora,
        "ip": ip,
        "user_agent": user_agent,
    }
    resultado = coleccion_firmas.insert_one(registro)

    # Puntero de conveniencia en el vehículo (la evidencia vive en la colección
    # append-only; esto es solo para mostrar el sello en panel/revisión/HV).
    campos_puntero = ("firmaUrl", "firmaEvidencia") if slot == 1 else ("firmaUrl2", "firmaEvidencia2")
    coleccion_vehiculos.update_one(
        {"placa": placa},
        {"$set": {
            campos_puntero[0]: url_archivo,
            campos_puntero[1]: {
                "hash_datos": hash_datos,
                "hash_firma": hash_firma,
                "firmado_en": ahora,
                "version": version,
                "registro_id": str(resultado.inserted_id),
                "slot": slot,
            },
        }},
    )

    sufijo_detalle = "Conductor 2 — " if slot == 2 else ""
    _registrar_cambio_aprobado(
        vehiculo, editado_por or "", "documentos",
        [{"campo": campos_puntero[0],
          "antes": vehiculo.get(campos_puntero[0]) or "(ninguna)",
          "despues": url_archivo}],
    )
    _registrar_auditoria(
        placa, "firma_sellada", actor=editado_por,
        detalle=f"{sufijo_detalle}Versión {version} — hash {hash_datos[:12]}…",
    )

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "message": "Firma registrada y sellada",
            "ruta": url_archivo,
            "url": _url_para_cliente(url_archivo),
            "firmado_en": ahora.isoformat(),
            "hash_datos": hash_datos,
            "version": version,
        },
    )


@ruta_vehiculos.get("/verificar-firma/{placa}")
async def verificar_firma(placa: str, slot: int = 1):
    """
    Verificación de integridad de la firma electrónica: recalcula el hash de
    los datos actuales del vehículo y lo compara con el sellado en la última
    firma. `coincide=false` = el documento cambió después de firmado (el diff
    de qué cambió vive en `historialCambios`).
    `slot=2` (2026-10-14): verifica la firma del SEGUNDO conductor (sus datos
    y SUS registros `slot=2`). Los históricos sin `slot` se leen como slot 1.
    """
    if slot not in (1, 2):
        raise HTTPException(status_code=400, detail="slot inválido: debe ser 1 o 2.")
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    filtro = {"placa": placa, "slot": 2} if slot == 2 else {"placa": placa, "slot": {"$ne": 2}}
    evidencia = coleccion_firmas.find_one(filtro, sort=[("firmado_en", -1)])
    if not evidencia:
        articulo = "segundo conductor" if slot == 2 else "vehículo"
        raise HTTPException(status_code=404, detail=f"Este {articulo} no tiene firmas registradas.")
    hash_actual = _hash_datos_firmados_slot2(vehiculo) if slot == 2 else _hash_datos_firmados(vehiculo)
    return {
        "placa": placa,
        "slot": slot,
        "coincide": hash_actual == evidencia.get("hash_datos"),
        "hash_actual": hash_actual,
        "evidencia": _json_seguro(_firmar_documentos({k: v for k, v in evidencia.items() if k != "_id"})),
    }

@ruta_vehiculos.get("/obtener-firma")
async def obtener_firma(placa: str, slot: int = 1):
    if slot not in (1, 2):
        raise HTTPException(status_code=400, detail="slot inválido: debe ser 1 o 2.")
    campo_firma = "firmaUrl2" if slot == 2 else "firmaUrl"
    vehiculo = coleccion_vehiculos.find_one({"placa": placa}, {campo_firma: 1, "_id": 0})

    if not vehiculo or not vehiculo.get(campo_firma):
        raise HTTPException(status_code=404, detail="Firma no encontrada")

    url_firma = vehiculo.get(campo_firma)

    try:
        # El blob es privado: se descarga server-side (antes era una URL
        # pública levantada con requests.get).
        contenido_firma = _descargar_blob(url_firma)
        try:
            # 1. Abrir la imagen binaria (sin importar el formato original)
            imagen_pil = Image.open(io.BytesIO(contenido_firma))

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

    except HTTPException:
        raise
    except Exception as e:
        print(f"Error general en el proxy de firma: {e}")
        raise HTTPException(status_code=500, detail="Error al procesar la imagen")


# ==========================================
# ELIMINAR DOC REQUERIDO → en_actualizacion (2026-10-14)
# ==========================================

def _mover_a_actualizacion_por_eliminacion(placa: str, actor: Optional[str] = None,
                                           estado_previo: Optional[str] = None):
    """(2026-10-14, caso real QVK730 + pedido del usuario) El gate de
    documentación solo vive en la TRANSICIÓN a completado_revision: eliminar
    un documento DESPUÉS dejaba el vehículo anclado en la bandeja de revisión
    (o en la bolsa de oferta) con avance < 100 % para siempre. Regla: si tras
    la eliminación falta documentación EXIGIDA y el vehículo estaba en
    revisión o aprobado, pasa a `en_actualizacion` — el estado que existe para
    eso — y queda INHABILITADO hasta que el documento vuelva a subirse.

    `actualizacionOrigen` recuerda a dónde volver cuando la documentación
    vuelva a estar completa (la planilla lo hace sola; el resto exige
    «Finalizar» → revisión) y distingue esta actualización automática de la
    que pide Seguridad con «Actualizar datos», que NO se auto-restaura.
    """
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo:
        return
    estado_actual = vehiculo.get("estadoIntegra")
    if estado_actual not in ("completado_revision", "aprobado"):
        return
    faltantes = _documentos_faltantes(vehiculo)
    if not faltantes:
        return
    # El origen es el estado REAL antes de eliminar: si la eliminación bajó
    # un aprobado a re-revisión (_registrar_cambio_aprobado corre antes), el
    # origen sigue siendo el aprobado original.
    origen = estado_previo if estado_previo in ("aprobado", "completado_revision") else estado_actual
    nombres = ", ".join(ETIQUETAS_DOCUMENTO.get(f, f) for f in faltantes)
    ahora = datetime.utcnow()
    coleccion_vehiculos.update_one(
        {"placa": placa},
        {"$set": {
            "estadoIntegra": "en_actualizacion",
            "fechaEstado": ahora,
            "actualizacionOrigen": origen,
            "observaciones": (
                f"Documento eliminado: {nombres}. El vehículo queda inhabilitado "
                f"hasta cargar de nuevo la documentación faltante."
            ),
        }},
    )
    # Sacarlo de la bolsa del día por si tenía check-in activo (la /bolsa
    # además filtra por estado — misma defensa doble del inactivo).
    try:
        coleccion_disponibilidades.update_many(
            {"placa": placa, "estado": "activa"},
            {"$set": {"estado": "cancelada", "actualizado_en": ahora}},
        )
    except Exception as e:
        print(f"[disponibilidad] No se pudo cancelar el check-in de {placa}: {e}")
    _registrar_auditoria(
        placa, "estado_en_actualizacion", actor=actor,
        detalle=f"{estado_actual} → en_actualizacion · faltan: {nombres}",
    )
    # AVISO a los PADRINOS que adoptaron la placa (2026-10-15): el vehículo
    # quedó inhabilitado por documento eliminado (no pasa por actualizar-estado).
    _notificar_padrinos(
        placa,
        f"Se eliminó documentación exigida ({nombres}): la placa {placa} quedó INHABILITADA "
        "en actualización hasta que se suba de nuevo.",
        titulo=f"Documento eliminado — {placa}",
    )


def _rehabilitar_tras_planilla(placa: str, actor: Optional[str] = None):
    """(2026-10-14, pedido del usuario: "inhabilitado hasta que se lo vuelvan
    a subir") Re-subir la Planilla de Seguridad Social rehabilita SOLO al
    vehículo que pasó a en_actualizacion POR HABERLA ELIMINADO
    (`actualizacionOrigen` existe) y cuya documentación vuelve a estar
    completa — es el documento de renovación mensual que por diseño no genera
    re-revisión (TIPOS_SIN_REVISION). La actualización pedida por Seguridad
    (sin origen) la cierra ella con «Aprobar»/«Cancelar actualización».
    """
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo or vehiculo.get("estadoIntegra") != "en_actualizacion":
        return
    origen = vehiculo.get("actualizacionOrigen")
    if origen not in ("aprobado", "completado_revision"):
        return
    if _documentos_faltantes(vehiculo):
        return  # aún falta otra documentación: sigue inhabilitado
    ahora = datetime.utcnow()
    coleccion_vehiculos.update_one(
        {"placa": placa},
        {"$set": {"estadoIntegra": origen, "fechaEstado": ahora, "observaciones": ""},
         "$unset": {"actualizacionOrigen": ""}},
    )
    _registrar_auditoria(
        placa, f"estado_{origen}", actor=actor,
        detalle=f"en_actualizacion → {origen} · rehabilitado al subir la planilla",
    )


@ruta_vehiculos.delete("/eliminar-documento")
async def eliminar_documento(placa: str, tipo: str, editado_por: Optional[str] = None):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo or not vehiculo.get(tipo):
        raise HTTPException(status_code=404, detail="Documento no encontrado.")
    _asegurar_editable(vehiculo)
    # Estado ANTES de cualquier mutación (_registrar_cambio_aprobado puede
    # bajar un aprobado a re-revisión: el origen que se recuerda debe ser el
    # original, no el intermedio).
    estado_previo = vehiculo.get("estadoIntegra")

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

    operacion: dict = {"$unset": unset_campos}
    # Planilla de Seguridad Social (2026-10-14, caso QVK730): su entrada del
    # historial apunta a un blob recién borrado y el vencimiento dejó de
    # aplicar — sin esto el bloque de /revision la seguía listando como
    # VENCIDA para siempre pese a NO existir.
    if tipo == "planillaEpsArl":
        operacion["$pull"] = {"documentosPlanillaSegSocial": {"ruta": url_previa}}
        unset_campos["planillaVencimiento"] = None
    coleccion_vehiculos.update_one({"placa": placa}, operacion)

    campos_diff = [{"campo": tipo, "antes": url_previa, "despues": "(eliminado)"}]
    if campo_reverso in unset_campos:
        campos_diff.append({"campo": campo_reverso, "antes": vehiculo.get(campo_reverso), "despues": "(eliminado)"})
    campos_diff += [{"campo": g, "antes": url_previa, "despues": "(eliminado)"} for g in gemelos_eliminados]
    _registrar_auditoria(
        placa, "documento_eliminado", actor=editado_por,
        detalle=ETIQUETAS_DOCUMENTO.get(tipo, tipo)
        + (f" (+ {len(gemelos_eliminados)} gemelo(s))" if gemelos_eliminados else ""),
    )
    _registrar_cambio_aprobado(
        vehiculo, editado_por or "", "documentos",
        campos_diff,
    )

    # (2026-10-14) Eliminar documentación EXIGIDA inhabilita el vehículo
    # (en_actualizacion) hasta que se cargue de nuevo — sin esto quedaba
    # anclado en revisión/aprobado con avance < 100 % (caso QVK730).
    _mover_a_actualizacion_por_eliminacion(
        placa, actor=editado_por, estado_previo=estado_previo)

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"message": f"{tipo} eliminado correctamente", "gemelos_eliminados": gemelos_eliminados},
    )


@ruta_vehiculos.delete("/eliminar-foto")
async def eliminar_foto(placa: str, url: str, editado_por: Optional[str] = None):
    vehiculo = coleccion_vehiculos.find_one({"placa": placa})
    if not vehiculo or url not in vehiculo["fotos"]:
        raise HTTPException(status_code=404, detail="Foto no encontrada.")
    _asegurar_editable(vehiculo)

    eliminar_de_google_storage(url)
    # El espejo plano Y el catálogo de ángulos (2026-10-14): si era una foto
    # frontal/lateral/posterior, su ángulo vuelve a faltar.
    coleccion_vehiculos.update_one(
        {"placa": placa},
        {"$pull": {"fotos": url, "fotosVehiculo": {"ruta": url}}},
    )
    _registrar_auditoria(placa, "foto_eliminada", actor=editado_por)

    _registrar_cambio_aprobado(
        vehiculo, editado_por or "", "documentos",
        [{"campo": "fotos", "antes": url, "despues": "(eliminada)"}],
    )

    return JSONResponse(status_code=status.HTTP_200_OK, content={"message": "Foto eliminada correctamente"})


@ruta_vehiculos.get("/exportar-excel")
def exportar_excel_vehiculos():
    """
    Exporta TODAS las placas con la información detallada almacenada de cada
    vehículo (datos de las 3 figuras + vehículo + documentos + estudios +
    tracking). Botón de /revision (bandeja Aprobados). PII pesada: cédulas
    completas por diseño (documento interno de Seguridad).
    """
    import io
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment

    def _v(doc, clave, default=""):
        valor = doc.get(clave)
        return default if valor in (None, "") else valor

    def _fecha(valor):
        if isinstance(valor, datetime):
            return valor.strftime("%d/%m/%Y")
        return str(valor)[:10] if valor else ""

    COLUMNAS = [
        ("Placa", lambda d: d.get("placa", "")),
        ("Estado", lambda d: ETIQUETA_ESTADO_EXPORT.get(d.get("estadoIntegra"), d.get("estadoIntegra", ""))),
        ("Fecha estado", lambda d: _fecha(d.get("fechaEstado"))),
        ("Creado", lambda d: _fecha(d["_id"].generation_time if hasattr(d.get("_id"), "generation_time") else None)),
        # Conductor
        ("Cond. Nombres", lambda d: _v(d, "condNombres")),
        ("Cond. Apellido 1", lambda d: _v(d, "condPrimerApellido")),
        ("Cond. Apellido 2", lambda d: _v(d, "condSegundoApellido")),
        ("Cond. Cédula", lambda d: _v(d, "condCedulaCiudadania")),
        ("Cond. Fecha nac.", lambda d: _fecha(d.get("condFechaNacimiento"))),
        ("Cond. RH", lambda d: _v(d, "condGrupoSanguineo")),
        ("Cond. Expedida en", lambda d: _v(d, "condExpedidaEn")),
        ("Cond. Licencia", lambda d: _v(d, "condNoLicencia")),
        ("Cond. Categorías", lambda d: _v(d, "condCategoriaLic")),
        ("Cond. Vence lic.", lambda d: _fecha(d.get("condFechaVencimientoLic"))),
        ("Cond. Celular", lambda d: _v(d, "condCelular")),
        ("Cond. Correo", lambda d: _v(d, "condCorreo")),
        ("Cond. Dirección", lambda d: _v(d, "condDireccion")),
        ("Cond. Ciudad", lambda d: _v(d, "condCiudad")),
        ("Cond. EPS", lambda d: _v(d, "condEps")),
        ("Cond. ARL", lambda d: _v(d, "condArl")),
        ("Cond. Banco", lambda d: _v(d, "condBanco")),
        ("Cond. Tipo cuenta", lambda d: _v(d, "condTipoCuenta")),
        ("Cond. Nº cuenta", lambda d: _v(d, "condNumeroCuenta")),
        # Propietario
        ("Prop. Nombre/Razón", lambda d: _v(d, "propNombre")),
        ("Prop. Tipo doc.", lambda d: _v(d, "propTipoDocumento")),
        ("Prop. Documento", lambda d: _v(d, "propDocumento")),
        ("Prop. Correo", lambda d: _v(d, "propCorreo")),
        ("Prop. Celular", lambda d: _v(d, "propCelular")),
        ("Prop. Ciudad", lambda d: _v(d, "propCiudad")),
        # Tenedor
        ("Tened. Nombre/Razón", lambda d: _v(d, "tenedNombre")),
        ("Tened. Tipo doc.", lambda d: _v(d, "tenedTipoDocumento")),
        ("Tened. Documento", lambda d: _v(d, "tenedDocumento")),
        ("Tened. Correo", lambda d: _v(d, "tenedCorreo")),
        ("Tened. Celular", lambda d: _v(d, "tenedCelular")),
        ("Tened. Ciudad", lambda d: _v(d, "tenedCiudad")),
        # Vehículo
        ("Veh. Marca", lambda d: _v(d, "vehMarca")),
        ("Veh. Línea", lambda d: _v(d, "vehLinea")),
        ("Veh. Modelo", lambda d: _v(d, "vehModelo")),
        ("Veh. Color", lambda d: _v(d, "vehColor")),
        ("Veh. Clase", lambda d: _v(d, "vehClase")),
        ("Veh. Capacidad (kg)", lambda d: _v(d, "vehCapacidadCarga")),
        ("Veh. SOAT aseguradora", lambda d: _v(d, "vehAseguradoraSoat")),
        ("Veh. Póliza SOAT", lambda d: _v(d, "vehPolizaSoat")),
        ("Veh. Vence SOAT", lambda d: _fecha(d.get("vehVencimientoSoat"))),
        ("Remolque", lambda d: _v(d, "remolqueMarca") or _v(d, "remMarca")),
        # Documentos y tracking
        # El total sube a +1 cuando el vehículo tiene remolque (la tarjeta de
        # remolque es obligatoria adicional, fuera de DOCUMENTOS_REQUERIDOS).
        ("Docs. cargados", lambda d: (
            lambda total: f"{total - len(_documentos_faltantes(d))}/{total}"
        )(len(DOCUMENTOS_REQUERIDOS) + (1 if _tiene_remolque(d) else 0))),
        ("Fotos", lambda d: len(d.get("fotos") or [])),
        ("Estudios vigencia vence", lambda d: _fecha((d.get("estudiosVigencia") or {}).get("vence"))),
        ("Última corrida", lambda d: _resumen_ultima_corrida(d)),
        ("Cambios editados", lambda d: len(d.get("historialCambios") or [])),
        ("Inactivaciones", lambda d: len(d.get("historialInactivacion") or [])),
        ("Observaciones", lambda d: _v(d, "observaciones")),
    ]

    ETIQUETA_ESTADO_EXPORT = {
        "registro_incompleto": "Pendiente conductor",
        "completado_revision": "En revisión",
        "aprobado": "Aprobado",
        "inactivo": "Inactivo",
        "en_actualizacion": "En actualización",
    }

    def _resumen_ultima_corrida(doc):
        corrida = doc.get("estudiosSeguridadAuto") or []
        if not corrida:
            return "sin estudios"
        finalizados = sum(1 for e in corrida if isinstance(e, dict) and e.get("estado") == "finalizado")
        errores = sum(1 for e in corrida if isinstance(e, dict) and e.get("estado") == "error")
        return f"{finalizados}/{len(corrida)} finalizados" + (f" · {errores} con error" if errores else "")

    wb = Workbook()
    ws = wb.active
    ws.title = "Vehículos"

    fill = PatternFill("solid", fgColor="006F7A")
    ws.append([titulo for titulo, _ in COLUMNAS])
    for celda in ws[1]:
        celda.font = Font(bold=True, color="FFFFFF")
        celda.fill = fill
        celda.alignment = Alignment(vertical="center")

    documentos = sorted(coleccion_vehiculos.find(), key=lambda d: str(d.get("placa", "")))
    for doc in documentos:
        ws.append([fn(doc) for _, fn in COLUMNAS])

    # Ancho razonable por columna (título + algo de aire).
    for idx, (titulo, _) in enumerate(COLUMNAS, start=1):
        ws.column_dimensions[ws.cell(row=1, column=idx).column_letter].width = \
            max(12, min(34, len(titulo) + 4))
    ws.freeze_panes = "A2"

    buffer = io.BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    return Response(
        content=buffer.getvalue(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="vehiculos_integrapp.xlsx"'},
    )


@ruta_vehiculos.get("/estudios-seguridad/{placa}")
def estudios_seguridad(placa: str):
    """
    Estudios de seguridad automáticos del vehículo (TusDatos): lo consume
    /revision con polling mientras hay estudios pendiente/en_curso.
    `estudios` = corrida vigente; `historico` = corridas anteriores
    (aplanadas — cada estudio conserva fecha y reporte_id); `documentos` =
    PDFs del estudio cargados manualmente por Seguridad (acumulativos).
    Sin estudios (vehículos históricos o no disparados) → listas vacías.
    """
    doc = coleccion_vehiculos.find_one(
        {"placa": placa},
        {"estudiosSeguridadAuto": 1, "historialEstudios": 1,
         "documentosEstudioSeguridad": 1, "estudiosVigencia": 1})
    if not doc:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    def _con_pdf_url(est):
        """Adjunta la URL firmada del PDF ARCHIVADO en el bucket (si existe);
        sin archivo, el front sirve el reporte en vivo del proveedor."""
        if not isinstance(est, dict):
            return est
        pdf_gcs = est.get("pdf_gcs")
        if isinstance(pdf_gcs, dict) and pdf_gcs.get("ruta"):
            est = dict(est)
            est["pdf_url"] = _url_para_cliente(pdf_gcs["ruta"])
        return est

    historico = []
    for corrida in doc.get("historialEstudios") or []:
        for est in (corrida.get("estudios") or []):
            if isinstance(est, dict):
                historico.append(_con_pdf_url(est))
    documentos = []
    for d in doc.get("documentosEstudioSeguridad") or []:
        if isinstance(d, dict) and d.get("ruta"):
            documentos.append({
                "url": _url_para_cliente(d["ruta"]),
                "fecha": d.get("fecha"),
                "nombre": d.get("nombre") or "",
            })
    return JSONResponse(status_code=status.HTTP_200_OK, content={
        "placa": placa,
        "estudios": _json_seguro([_con_pdf_url(e) for e in
                                  (doc.get("estudiosSeguridadAuto") or [])]),
        "historico": _json_seguro(historico),
        "documentos": _json_seguro(documentos),
        "vigencia": _json_seguro(doc.get("estudiosVigencia")),
    })


@ruta_vehiculos.get("/estudios-seguridad/{placa}/pdf/{estudio_id}")
async def pdf_estudio(placa: str, estudio_id: str):
    """
    PDF de un estudio automático, A PRUEBA DE EXPIRACIÓN: prioriza el archivo
    del bucket privado (pdf_gcs); si no existe, lo baja del proveedor y lo
    ARCHIVA (los reportes de TusDatos expiran — 410 — y el histórico no debe
    morirse con ellos). 410 amigable si el proveedor ya no lo sirve y no hay
    archivo: la salida es «Volver a consultar».
    """
    from Funciones import estudios_automaticos as ea

    placa_limpia = placa.strip().upper()
    doc = coleccion_vehiculos.find_one(
        {"placa": placa_limpia},
        {"estudiosSeguridadAuto": 1, "historialEstudios": 1})
    if not doc:
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")

    def _buscar(estudios):
        return next((e for e in (estudios or [])
                     if isinstance(e, dict) and e.get("id") == estudio_id), None)

    estudio = _buscar(doc.get("estudiosSeguridadAuto"))
    if not estudio:
        for corrida in doc.get("historialEstudios") or []:
            estudio = _buscar((corrida or {}).get("estudios"))
            if estudio:
                break
    if not estudio:
        raise HTTPException(status_code=404, detail="Estudio no encontrado.")

    pdf_gcs = estudio.get("pdf_gcs")
    if isinstance(pdf_gcs, dict) and pdf_gcs.get("ruta"):
        return RedirectResponse(_url_para_cliente(pdf_gcs["ruta"]))

    reporte_id = estudio.get("reporte_id")
    if not reporte_id:
        raise HTTPException(
            status_code=409,
            detail="El estudio no tiene reporte del proveedor (¿falló la consulta?). "
                   "Usa «Volver a consultar» para regenerarlo.")

    try:
        # (2026-10-09) Descarga VALIDADA: el proveedor genera el PDF de forma
        # asíncrona y responde 200 + JSON "processing" mientras tanto — servir
        # o archivar ese cuerpo corrompe el estudio (bug real NNM001). El
        # helper espera su retry_after (~1 min) y solo entrega PDFs reales.
        contenido = await ea._bajar_pdf_reporte_validado(
            estudio.get("tipo"), reporte_id)
    except HTTPException as e:
        raise HTTPException(
            status_code=410,
            detail=f"El reporte ya no está disponible en el proveedor (expiran con el "
                   f"tiempo). Usa «Volver a consultar» para regenerarlo — consume una "
                   f"consulta. (Detalle: {e.detail})")
    except Exception as e:
        raise HTTPException(
            status_code=502,
            detail=f"No se pudo descargar el reporte del proveedor: {e}")
    if not contenido:
        raise HTTPException(
            status_code=425,
            detail="El proveedor sigue generando el PDF de este reporte (tarda ~1 min "
                   "tras finalizar la consulta). Espera un momento y vuelve a abrirlo; "
                   "si persiste, usa «Volver a consultar».")

    # Archivar best-effort: la próxima vez el PDF sale directo del bucket
    # (se le pasa el contenido YA validado para no descargarlo dos veces).
    try:
        sujeto = {"tipo": estudio.get("tipo"), "cedula": estudio.get("cedula"),
                  "nit": estudio.get("nit"), "placa": estudio.get("placa")}
        archivado = await ea._archivar_pdf_reporte(placa_limpia, sujeto, reporte_id,
                                                   contenido=contenido)
        if archivado:
            coleccion_vehiculos.update_one(
                {"placa": placa_limpia},
                {"$set": {"estudiosSeguridadAuto.$[e].pdf_gcs": archivado}},
                array_filters=[{"e.id": estudio_id}],
            )
    except Exception as e:
        print(f"[estudios-pdf] No se pudo archivar el PDF de {placa_limpia}: {e}")

    return Response(content=contenido, media_type="application/pdf")


@ruta_vehiculos.get("/estudios-config")
def obtener_config_estudios():
    """
    Estado del SWITCH de disparo automático de estudios (2026-10-05):
    `auto_disparo=true` (default) → cuando el conductor envía a revisión se
    consultan los estudios solos; `false` → el vehículo llega a revisión SIN
    estudios y Seguridad los corre manualmente («Volver a consultar» o el
    módulo «Estudios por antigüedad»). Pensado para pausas TEMPORALES.
    """
    from Funciones import estudios_automaticos
    return JSONResponse(status_code=status.HTTP_200_OK, content={
        "auto_disparo": estudios_automaticos.auto_disparo_habilitado(),
    })


@ruta_vehiculos.put("/estudios-config")
def actualizar_config_estudios(auto_disparo: str = Form(...)):
    """Cambia el switch de disparo automático (botón de /revision)."""
    from Funciones import estudios_automaticos
    valor = str(auto_disparo).strip().lower() in ("1", "true", "si", "sí", "on")
    efectivo = estudios_automaticos.fijar_auto_disparo(valor)
    _log_switch = "encendido" if efectivo else "apagado"
    return JSONResponse(status_code=status.HTTP_200_OK, content={
        "auto_disparo": efectivo,
        "message": f"Disparo automático de estudios {_log_switch}.",
    })


@ruta_vehiculos.post("/estudios-seguridad/{placa}/reintentar-estudio")
async def reintentar_estudio(placa: str, estudio_id: str = Form(...)):
    """
    Reintenta UN estudio fallido (2026-10-09, botón «Reintentar esta
    consulta» de la tarjeta en error): relanza SOLO ese sujeto, sin force
    (si el proveedor ya tiene el resultado, sale barato) y SIN tocar los
    demás estudios de la corrida. Bloquea mientras corre (~1 min).
    """
    from Funciones import estudios_automaticos as ea
    estudio = await ea.reintentar_estudio_unico(placa, estudio_id)
    if estudio.get("estado") == "finalizado":
        mensaje = "Estudio re-consultado y finalizado."
    else:
        # El error ya viene legible (con el rechazo crudo del proveedor
        # cuando lo hay — caso WDN489); sin coletillas que tapen.
        mensaje = f"El estudio sigue fallando: {estudio.get('error', 'sin detalle')}"
    return JSONResponse(status_code=status.HTTP_200_OK, content={
        "message": mensaje,
        "estudio": _json_seguro(estudio),
    })


@ruta_vehiculos.post("/estudios-seguridad/{placa}/disparar")
async def disparar_estudios_seguridad(placa: str):
    """
    Dispara MANUALMENTE los estudios de seguridad automáticos (botón de
    /revision): re-consulta cédulas deduplicadas + placa sin importar la
    corrida anterior (pisa el array). Consume consultas del proveedor.
    """
    placa_limpia = placa.strip().upper()
    if not coleccion_vehiculos.find_one({"placa": placa_limpia}, {"_id": 1}):
        raise HTTPException(status_code=404, detail="Vehículo no encontrado.")
    # force=True: el botón es "volver a consultar" de verdad (re-consulta
    # real, no la caché por cédula del proveedor). manual=True: el switch de
    # disparo automático NO aplica a una acción explícita del usuario (bug
    # 2026-10-07: con el switch apagado el botón respondía 200 en silencio).
    _disparar_estudios_seguridad(placa_limpia, re_revision=False, forzar=True,
                                 manual=True)
    return JSONResponse(status_code=status.HTTP_200_OK, content={
        "message": "Estudios de seguridad disparados; consulte el avance en unos minutos.",
    })


@ruta_vehiculos.post("/estudios-seguridad/{placa}/reintentar-fuentes")
async def reintentar_fuentes_estudio(placa: str, estudio_id: str = Form(...)):
    """
    Reintenta SOLO las fuentes que quedaron en «Error» de un estudio
    finalizado (botón de la pestaña Estudios de /revision): el proveedor
    relanza esas fuentes sobre el MISMO reporte. Espera el resultado
    (≤180 s) y devuelve el estudio actualizado.
    """
    from Funciones import estudios_automaticos
    cambios = await estudios_automaticos.reintentar_fuentes_estudio(placa, estudio_id)
    return JSONResponse(status_code=status.HTTP_200_OK, content={
        "message": "Fuentes fallidas reintentadas.",
        "estudio": _json_seguro(cambios),
    })


def _autorizado_seguro(cedula: str) -> bool:
    """Best-effort: ¿la cédula ya tiene evidencia de autorización de datos?
    Si la consulta falla (caída de la colección), reporta autorizado para no
    llenar el módulo de falsos «pendientes»."""
    from rutas.conductores import _sujeto_ya_autorizado
    try:
        return bool(_sujeto_ya_autorizado(cedula))
    except Exception as e:
        print(f"[estudios-antiguedad] No se pudo verificar autorización de {cedula}: {e}")
        return True


@ruta_vehiculos.get("/estudios-antiguedad")
def estudios_por_antiguedad(
    placa: Optional[str] = None,
    solo_vencidas: bool = False,
    solo_pendientes: bool = False,
):
    """
    Módulo «Estudios por antigüedad» de /revision (2026-10-05, reemplaza la
    renovación automática eliminada): TODAS las placas con estudios (corrida
    vigente, histórico, PDFs manuales o estudio legacy), ORDENADAS por la
    fecha del último estudio (más antiguo primero) para que el usuario decida
    MANUALMENTE cuál actualizar (botón → /estudios-seguridad/{placa}/disparar).

    Los FILTROS los procesa el BACKEND (2026-10-06, pedido del usuario — el
    front solo pasa los criterios): `placa` (contiene, i), `solo_vencidas`
    y `solo_pendientes` (solo placas con actores persona SIN autorización de
    tratamiento de datos). Cada fila reporta además cuántos actores persona
    faltan por autorizar (`pendientes_autorizacion`).
    """
    from Funciones import estudios_automaticos as ea

    ahora = datetime.utcnow()
    filas = []
    placa_filtro = (placa or "").strip().upper()
    proyeccion = {
        "placa": 1, "estadoIntegra": 1,
        "condCedulaCiudadania": 1, "condNombres": 1, "condApellidos": 1,
        "propDocumento": 1, "propTipoDocumento": 1, "propNombre": 1,
        "tenedDocumento": 1, "tenedTipoDocumento": 1, "tenedNombre": 1,
        "RemolDuenoDocumento": 1, "RemolDuenoTipoDocumento": 1, "RemolDuenoNombre": 1,
        "estudiosSeguridadAuto": 1, "historialEstudios": 1,
        "documentosEstudioSeguridad": 1, "estudiosVigencia": 1,
        "estudioSeguridadFecha": 1,
    }
    consulta: dict = {"$or": [
        {"estudiosSeguridadAuto.0": {"$exists": True}},
        {"historialEstudios.0": {"$exists": True}},
        {"documentosEstudioSeguridad.0": {"$exists": True}},
        {"estudioSeguridadFecha": {"$ne": None}},
    ]}
    if placa_filtro:
        consulta["placa"] = {"$regex": re.escape(placa_filtro), "$options": "i"}

    for doc in coleccion_vehiculos.find(consulta, proyeccion):
        # Última fecha de estudio: corrida vigente → vigencia → PDF manual.
        fechas = [e.get("finalizado_en") for e in (doc.get("estudiosSeguridadAuto") or [])
                  if isinstance(e, dict) and e.get("finalizado_en")]
        vigencia = doc.get("estudiosVigencia") or {}
        if not fechas and vigencia.get("desde"):
            fechas = [vigencia["desde"]]
        if not fechas and doc.get("estudioSeguridadFecha"):
            fechas = [doc["estudioSeguridadFecha"]]
        if not fechas:
            for d in (doc.get("documentosEstudioSeguridad") or []):
                if isinstance(d, dict) and d.get("fecha"):
                    fechas.append(d["fecha"])
        if not fechas:
            continue  # sin ninguna fecha de estudio → no hay nada que listar
        ultimo = max(fechas)

        sujetos = []
        # Correo de contacto por cédula (para el botón «✉️ Enviar autorización»
        # del módulo, prellenado desde el formulario del vehículo).
        correos_por_ced = {}
        for campo_doc, campo_correo in (
            ("condCedulaCiudadania", "condCorreo"),
            ("propDocumento", "propCorreo"),
            ("tenedDocumento", "tenedCorreo"),
            ("RemolDuenoDocumento", "RemolDuenoCorreo"),
        ):
            ced = re.sub(r"\D", "", str(doc.get(campo_doc) or ""))
            correo = str(doc.get(campo_correo) or "").strip()
            if ced and correo and ced not in correos_por_ced:
                correos_por_ced[ced] = correo
        for s in ea.sujetos_estudio(doc):
            if s["tipo"] == "persona":
                sujetos.append({"tipo": "persona", "documento": s["cedula"],
                                "roles": s["roles"],
                                "correo": correos_por_ced.get(s["cedula"], ""),
                                # Autorización de tratamiento de datos (2026-10-06):
                                # la persona ya tiene evidencia (cuenta o canal
                                # vinculo_correo/papel) — best-effort por si la
                                # colección de aceptaciones falla.
                                "autorizado": _autorizado_seguro(s["cedula"])})
            elif s["tipo"] == "empresa":
                sujetos.append({"tipo": "empresa", "documento": s["nit"],
                                "roles": s["roles"]})
            else:
                sujetos.append({"tipo": "vehiculo", "documento": s["placa"],
                                "roles": ["vehículo"]})

        personas = [s for s in sujetos if s["tipo"] == "persona"]
        pendientes_aut = sum(1 for s in personas if not s["autorizado"])
        vence = vigencia.get("vence")
        fila = {
            "placa": doc.get("placa", ""),
            "estadoIntegra": doc.get("estadoIntegra", ""),
            "sujetos": sujetos,
            "ultimo_estudio": ultimo,
            "antiguedad_dias": max(0, (ahora - ultimo).days),
            "vigencia_desde": vigencia.get("desde"),
            "vigencia_vence": vence,
            "vencida": bool(vence and vence < ahora),
            "tiene_corrida_vigente": bool(doc.get("estudiosSeguridadAuto")),
            "total_estudios": len(doc.get("estudiosSeguridadAuto") or []),
            "personas": len(personas),
            "pendientes_autorizacion": pendientes_aut,
        }
        if solo_vencidas and not fila["vencida"]:
            continue
        if solo_pendientes and pendientes_aut == 0:
            continue
        filas.append(fila)

    filas.sort(key=lambda f: f["ultimo_estudio"])
    return JSONResponse(status_code=status.HTTP_200_OK, content={
        "vehiculos": _json_seguro(filas),
        "vencidas": sum(1 for f in filas if f["vencida"]),
        # Placas con al menos un actor persona sin autorización (2026-10-06):
        # alimenta el contador del filtro «solo pendientes» del front.
        "con_pendientes": sum(1 for f in filas if f["pendientes_autorizacion"] > 0),
    })


# Filtro por bandeja de /revision (2026-10-13): el front carga SOLO la bandeja
# que está viendo — antes este endpoint devolvía TODOS los vehículos en TODOS
# los estados con cada ruta de documento firmada (payload gigante + miles de
# firmas v4 con la flota creciendo). Sin el param mantiene el comportamiento
# completo (compat con el front desplegado); «aprobados» sigue yendo por
# obtener-aprobados-paginados, que ya es paginado.
BANDEJAS_REVISION = {
    "pendientes": ["registro_incompleto"],
    "revision": ["completado_revision", "en_revision"],
    "inactivos": ["inactivo"],
    "actualizacion": ["en_actualizacion"],
    "rechazados": ["rechazado"],
}


@ruta_vehiculos.get("/obtener-vehiculos-incompletos")
def obtener_vehiculos_incompletos(id_usuario: Optional[str] = None, bandeja: Optional[str] = None,
                                  placa: Optional[str] = None):
    filtro = {
        "estadoIntegra": {
            "$in": BANDEJAS_REVISION.get(bandeja, ["registro_incompleto", "completado_revision",
                                                    "aprobado", "inactivo", "rechazado",
                                                    "en_actualizacion"])
        },
        # Borradores del alta (placa validada, responsable sin vincular): se
        # retoman desde el módulo de alta, no desde las bandejas de revisión.
        "responsable_pendiente": {"$ne": True},
    }
    # Filtro por placa (2026-10-14): restaurar el panel de UN vehículo en
    # /revision (?placa= tras un atrás/adelante o F5) NO debe traer la bandeja
    # completa — cada vehículo firma todas sus rutas de documento y con la
    # flota creciendo eso era lo que se sentía lento. Sin el param (o vacío)
    # el filtro queda igual que siempre (compat con el front desplegado).
    if placa and placa.strip():
        filtro["placa"] = placa.strip().upper()
    vehiculos_raw = list(coleccion_vehiculos.find(filtro))

    if not vehiculos_raw:
        return JSONResponse(
            status_code=status.HTTP_200_OK, 
            content={"message": "No hay vehículos", "vehicles": []}
        )

    vehiculos_final = []
    for veh in vehiculos_raw:
        veh["_id"] = str(veh["_id"])
        # Los estudios automáticos son insumo de SEGURIDAD (/revision),
        # no del conductor.
        veh.pop("estudiosSeguridadAuto", None)
        veh.pop("historialEstudios", None)
        veh.pop("documentosEstudioSeguridad", None)
        veh.pop("estudiosVigencia", None)
        documentos = {
            k: v for k, v in veh.items()
            # SE HA MODIFICADO AQUÍ PARA QUE NO DEVUELVA estudioSeguridad NI fotoconductorseguridad
            # SI NO QUIERES QUE EL CONDUCTOR LOS VEA (Opcional, pero recomendado por seguridad)
            if (isinstance(v, str) and (
                _es_ruta_documento(v) or v.startswith("https://storage.googleapis.com")
            )) and k not in ["estudioSeguridad", "fotoconductorseguridad"]
        }
        veh["documentos"] = documentos
        vehiculos_final.append(veh)

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"message": "Vehículos encontrados", "vehicles": _json_seguro(_firmar_documentos(vehiculos_final))}
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
    # Total real de la consulta (sin el limit): el contador de la bandeja
    # Aprobados del front lo muestra junto al título (2026-10-13).
    try:
        total = coleccion_vehiculos.count_documents(filtro)
    except Exception:
        total = None
    vehiculos_final = []

    for veh in vehiculos_cursor:
        veh["_id"] = str(veh["_id"])
        # Los estudios automáticos son insumo de SEGURIDAD (/revision).
        veh.pop("estudiosSeguridadAuto", None)
        veh.pop("historialEstudios", None)
        veh.pop("documentosEstudioSeguridad", None)
        veh.pop("estudiosVigencia", None)
        documentos = {
            k: v for k, v in veh.items()
            if isinstance(v, str) and (
                _es_ruta_documento(v) or v.startswith("https://storage.googleapis.com")
            ) and k != "estudioSeguridad"
        }
        veh["documentos"] = documentos
        vehiculos_final.append(veh)

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"vehiculos": _json_seguro(_firmar_documentos(vehiculos_final)), "total": total}
    )


# ─────────────────────────────────────────────────────────────────────────────
# Indicadores de /revision (2026-10-14): placas registradas POR DÍA y en qué
# estado están HOY. La fecha de registro es el timestamp del _id (ObjectId.
# generation_time, ya UTC naive): NO existe un campo fechaCreacion y
# fechaEstado cambia con cada transición. El día agrupado es el de Colombia
# (UTC−5, misma regla del _MS_5H de indicadores_costo_operacion.py).
# ─────────────────────────────────────────────────────────────────────────────

# Orden de apilado del gráfico (verde abajo → grises arriba) + etiqueta y
# color; el front renderiza un <Bar> por entrada en este orden, así la
# leyenda y el apilado nunca divergen entre backend y front.
# Paleta VALIDADA (dataviz/validate_palette, pares adyacentes): dentro de cada
# familia cálida la luminosidad ALTERNNA (claro/oscuro) para que protanopes
# separen los segmentos que se tocan — los tonos rojo/naranja solos colapsan
# bajo CVD. Los grises finales son intencionales (estados «muertos»: inactivo/
# rechazado), con leyenda+tooltip como encoding secundario.
ESTADOS_REGISTRO_DIA = [
    ("aprobado", "Aprobado", "#15803d"),
    ("en_revision", "En revisión (histórico)", "#f59e0b"),
    ("completado_revision", "En revisión", "#9a3412"),
    ("devuelto", "Devuelto", "#f87171"),
    ("registro_incompleto", "Pendiente conductor", "#b91c1c"),
    ("en_actualizacion", "En actualización", "#fb923c"),
    ("inactivo", "Inactivo", "#94a3b8"),
    ("rechazado", "Rechazado", "#475569"),
]

_OFFSET_COLOMBIA = timedelta(hours=5)


@ruta_vehiculos.get("/registros-por-dia")
def registros_por_dia(dias: int = 30):
    if dias < 1 or dias > 365:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="dias debe estar entre 1 y 365",
        )
    hoy_colombia = (datetime.utcnow() - _OFFSET_COLOMBIA).date()
    desde = hoy_colombia - timedelta(days=dias - 1)

    # La flota es pequeña (cientos): proyección mínima + agrupación en Python
    # en vez de aggregate (mismo resultado, testeable con el FakeColeccion).
    conteos: dict = {}
    for doc in coleccion_vehiculos.find({}, {"estadoIntegra": 1}):
        _id = doc.get("_id")
        if not isinstance(_id, ObjectId):
            continue  # sin ObjectId no hay fecha de registro confiable
        dia = (_id.generation_time - _OFFSET_COLOMBIA).date()
        if dia < desde:
            continue
        estado = doc.get("estadoIntegra") or "sin_estado"
        conteos.setdefault((dia, estado), 0)
        conteos[(dia, estado)] += 1

    # Estados conocidos en su orden de apilado + cualquiera otro que aparezca
    # (p. ej. «sin_estado» en fichas viejas) al final, para no perder placas.
    # etiquetas conserva el orden de inserción → orden de apilado del gráfico.
    extra = sorted({
        estado for (_, estado) in conteos
        if estado not in {e for e, _, _ in ESTADOS_REGISTRO_DIA}
    })
    etiquetas = {e: etiqueta for e, etiqueta, _ in ESTADOS_REGISTRO_DIA}
    colores = {e: color for e, _, color in ESTADOS_REGISTRO_DIA}
    for estado in extra:
        etiquetas[estado] = ESTADOS_FLOTA_LEGIBLES.get(estado, estado or "sin estado")
        colores[estado] = "#cbd5e1"

    series = []
    dia = desde
    while dia <= hoy_colombia:
        fecha = dia.isoformat()
        conteo_dia = {estado: conteos.get((dia, estado), 0) for estado in etiquetas}
        series.append({
            "fecha": fecha,
            "total": sum(conteo_dia.values()),
            "conteos": conteo_dia,
        })
        dia += timedelta(days=1)

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "message": "Registros por día",
            "dias": dias,
            "desde": desde.isoformat(),
            # [{estado, etiqueta, color}] en orden de apilado del gráfico.
            "estados": [
                {"estado": estado, "etiqueta": etiqueta, "color": colores[estado]}
                for estado, etiqueta in etiquetas.items()
            ],
            "series": series,
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# Estado de la flota — vista de SOLO LECTURA para el portal Medical Care
# (2026-10-07). Muestra en qué estadoIntegra está cada vehículo con un buscador
# por placa procesado EN EL BACKEND. Perfiles: OPERATIVO, CONTROL, COORDINADOR
# y ADMIN (derivados de baseusuarios; el perfil que envía el front NO se
# confía). No expone documentos ni PII pesada: solo lo necesario para
# visualizar el estado.
# ─────────────────────────────────────────────────────────────────────────────

PERFILES_ESTADO_FLOTA = {"OPERATIVO", "CONTROL", "COORDINADOR", "ADMIN"}

ESTADOS_FLOTA_LEGIBLES = {
    "registro_incompleto": "Pendiente conductor",
    "completado_revision": "En revisión",
    "aprobado": "Aprobado",
    "devuelto": "Devuelto",
    "inactivo": "Inactivo",
    "rechazado": "Rechazado",
    "en_actualizacion": "En actualización",
}


def _resolver_usuario_flota(usuario: str) -> dict:
    """Perfil REAL del usuario desde baseusuarios (patrón _resolver_usuario de
    otros_costos.py: el backend no confía en lo que envía el front)."""
    if not usuario or not str(usuario).strip():
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="No autenticado")
    u = str(usuario).strip().upper()
    doc = coleccion_baseusuarios.find_one({"usuario": u})
    if not doc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Usuario no válido")
    if not doc.get("activo", True):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Usuario inactivo")
    return {"usuario": doc["usuario"], "perfil": (doc.get("perfil") or "").strip().upper()}


@ruta_vehiculos.get("/estado-flota")
def obtener_estado_flota(
    usuario: str = "",
    placa: Optional[str] = None,
    estado: Optional[str] = None,
    skip: int = 0,
    limit: int = 50,
):
    user = _resolver_usuario_flota(usuario)
    if user["perfil"] not in PERFILES_ESTADO_FLOTA:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No tienes permisos para ver el estado de la flota",
        )

    if limit < 1 or limit > 200:
        limit = 50
    if skip < 0:
        skip = 0

    filtro: dict = {
        # Borradores del alta (sin responsable vinculado) no son flota visible.
        "responsable_pendiente": {"$ne": True},
    }
    if placa and placa.strip():
        filtro["placa"] = {"$regex": re.sub(r"[^A-Za-z0-9]", "", placa.strip()), "$options": "i"}
    if estado and estado.strip():
        estado_limpio = estado.strip()
        if estado_limpio not in ESTADOS_FLOTA_LEGIBLES:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Estado no válido: {estado_limpio}",
            )
        filtro["estadoIntegra"] = estado_limpio

    # Conteos por estado sobre el MISMO filtro de búsqueda (sin el de estado,
    # para que los chips sigan mostrando el panorama completo de lo buscado).
    filtro_conteo = {k: v for k, v in filtro.items() if k != "estadoIntegra"}
    conteos: dict = {}
    for doc in coleccion_vehiculos.aggregate([
        {"$match": filtro_conteo},
        {"$group": {"_id": "$estadoIntegra", "n": {"$sum": 1}}},
    ]):
        clave = doc["_id"] or "sin_estado"
        conteos[clave] = doc["n"]

    cursor = (
        coleccion_vehiculos.find(filtro, {
            "placa": 1, "estadoIntegra": 1, "fechaEstado": 1,
            "condNombres": 1, "condApellidos": 1, "condCedulaCiudadania": 1,
            "condCelular": 1, "vehMarca": 1, "vehLinea": 1, "vehModelo": 1,
            "observaciones": 1, "_id": 1,
        })
        .sort("placa", 1)
        .skip(skip)
        .limit(limit)
    )

    filas = []
    for veh in cursor:
        est = veh.get("estadoIntegra") or "sin_estado"
        filas.append({
            "id": str(veh["_id"]),
            "placa": veh.get("placa", ""),
            "estado": est,
            "estado_legible": ESTADOS_FLOTA_LEGIBLES.get(est, est),
            "fecha_estado": veh.get("fechaEstado"),
            "conductor": " ".join(x for x in [veh.get("condNombres", ""), veh.get("condApellidos", "")] if x).strip(),
            "cedula": veh.get("condCedulaCiudadania", ""),
            "celular": veh.get("condCelular", ""),
            "marca": veh.get("vehMarca", ""),
            "linea": veh.get("vehLinea", ""),
            "modelo": veh.get("vehModelo", ""),
            "observaciones": veh.get("observaciones", ""),
        })

    total = sum(conteos.values()) if not (estado and estado.strip()) else coleccion_vehiculos.count_documents(filtro)

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=_json_seguro({
            "vehiculos": filas,
            "total": total,
            "skip": skip,
            "limit": limit,
            "conteos": conteos,
        })
    )


# ─────────────────────────────────────────────────────────────────────────────
# Estado de la flota — DETALLE por placa (2026-10-07): al hacer clic en una
# placa se abre la historia del vehículo (qué ha pasado, por qué está devuelto,
# si ya está activo para operar). Timeline unificada construida EN EL BACKEND
# a partir de auditoriaVehiculo + historialInactivacion + historialCambios.
# ─────────────────────────────────────────────────────────────────────────────

# Qué SIGNIFICA cada estado para quien consulta (no solo la etiqueta).
EXPLICACION_ESTADO_FLOTA = {
    "registro_incompleto": "El conductor aún está registrando su información y documentos. Todavía no está disponible para operar.",
    "completado_revision": "El conductor terminó la carga y está en espera de revisión de Seguridad.",
    "aprobado": "Aprobado por Seguridad: activo para operar y habilitado para la bolsa de flota.",
    "devuelto": "Devuelto por Seguridad con observaciones: el conductor debe corregir lo indicado y volver a enviarlo.",
    "rechazado": "Rechazado definitivamente por Seguridad: no puede volver al flujo.",
    "inactivo": "Inactivado por Seguridad: sale de la bolsa de flota hasta que se reactive.",
    "en_actualizacion": "Aprobado, pero en proceso de actualización de datos solicitado por Seguridad.",
}

ACCIONES_AUDITORIA_LEGIBLES = {
    "vehiculo_creado": "Vehículo creado",
    "responsable_asignado": "Responsable asignado",
    "datos_actualizados": "Datos actualizados",
    "documento_subido": "Documento subido",
    "documento_reutilizado": "Documento reutilizado",
    "documento_eliminado": "Documento eliminado",
    "fotos_subidas": "Fotos del vehículo subidas",
    "foto_eliminada": "Foto eliminada",
    "firma_subida": "Firma subida",
    "firma_sellada": "Firma electrónica sellada",
    "estudio_seguridad_cargado": "Estudio de seguridad cargado",
    "foto_seguridad_cargada": "Foto del conductor cargada",
    "estudios_consultados": "Estudios de seguridad consultados",
    # Cambios de conductor (2026-10-14): vinculación/desvinculación de los
    # dos slots — los escribe /conductores (invitar·aceptar·desvincular).
    "conductor_vinculado": "Conductor vinculado",
    "conductor_desvinculado": "Conductor desvinculado",
    "invitacion_conductor_enviada": "Invitación de conductor enviada",
}

# Cambios de estado (accion=estado_X de la auditoría) con etiqueta legible.
ACCIONES_AUDITORIA_LEGIBLES.update({
    "estado_registro_incompleto": "Enviado a registro del conductor",
    "estado_completado_revision": "Enviado a revisión de Seguridad",
    "estado_aprobado": "Aprobado por Seguridad",
    "estado_devuelto": "Devuelto por Seguridad",
    "estado_rechazado": "Rechazado por Seguridad",
    "estado_inactivo": "Inactivado por Seguridad",
    "estado_en_actualizacion": "Enviado a actualización de datos",
})

VIAS_AUDITORIA_LEGIBLES = {
    "conductor": "Conductor",
    "impersonacion": "Seguridad (como conductor)",
    "seguridad": "Seguridad",
    "sistema": "Automático (sistema)",
}


def _entrada_timeline(fecha, tipo: str, accion: str, actor, via=None, detalle=None, extra=None) -> dict:
    return {
        "fecha": fecha,
        "tipo": tipo,           # auditoria | estado | edicion
        "accion": accion,
        "accion_legible": ACCIONES_AUDITORIA_LEGIBLES.get(accion, accion),
        "actor": actor or "",
        "via": via or "",
        "via_legible": VIAS_AUDITORIA_LEGIBLES.get(via or "", via or ""),
        "detalle": detalle or "",
        "extra": extra or "",
    }


@ruta_vehiculos.get("/estado-flota/{placa}")
def obtener_detalle_estado_flota(placa: str, usuario: str = ""):
    user = _resolver_usuario_flota(usuario)
    if user["perfil"] not in PERFILES_ESTADO_FLOTA:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="No tienes permisos para ver el estado de la flota",
        )

    placa_limpia = placa.strip().upper()
    veh = coleccion_vehiculos.find_one({"placa": placa_limpia})
    if not veh:
        # Fallback tolerante a mayúsculas (mismo patrón de reutilizar-documento).
        veh = coleccion_vehiculos.find_one({"placa": {"$regex": f"^{re.escape(placa_limpia)}$", "$options": "i"}})
    if not veh:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Vehículo no encontrado")

    est = veh.get("estadoIntegra") or "sin_estado"

    # ¿Está disponible HOY en la bolsa de flota? (check-in activo del día, zona Bogotá)
    hoy = datetime.now(_TZ_BOGOTA).strftime("%Y-%m-%d")
    checkin = coleccion_disponibilidades.find_one(
        {"placa": veh.get("placa", placa_limpia), "fecha": hoy, "estado": "activa"},
        {"_id": 1, "origen": 1, "departamentos_destino": 1},
    )

    # Timeline unificada (más reciente primero) desde las 3 fuentes del doc.
    timeline = []
    for entrada in veh.get("auditoriaVehiculo") or []:
        timeline.append(_entrada_timeline(
            entrada.get("fecha"), "auditoria",
            entrada.get("accion", ""), entrada.get("actor"), entrada.get("via"),
            entrada.get("detalle"),
        ))
    for entrada in veh.get("historialInactivacion") or []:
        timeline.append(_entrada_timeline(
            entrada.get("fecha"), "estado",
            entrada.get("accion", "inactivado"), entrada.get("usuario"),
            "seguridad", entrada.get("motivo"),
        ))
    for corrida in veh.get("historialCambios") or []:
        campos = corrida.get("campos") or []
        detalle_cambios = "; ".join(
            f"{c.get('campo', '')}: {c.get('antes', '')} → {c.get('despues', '')}" for c in campos[:6]
        ) + ("…" if len(campos) > 6 else "")
        timeline.append(_entrada_timeline(
            corrida.get("fecha"), "edicion",
            "datos_actualizados", corrida.get("usuario"), None, detalle_cambios,
            extra=f"{len(campos)} campo(s) · {corrida.get('seccion', '')}",
        ))

    def _clave_orden(e):
        f = e.get("fecha")
        return f if isinstance(f, datetime) else datetime.min.replace(tzinfo=None)

    timeline.sort(key=_clave_orden, reverse=True)

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content=_json_seguro({
            "placa": veh.get("placa", placa_limpia),
            "estado": est,
            "estado_legible": ESTADOS_FLOTA_LEGIBLES.get(est, est),
            "explicacion": EXPLICACION_ESTADO_FLOTA.get(est, ""),
            "fecha_estado": veh.get("fechaEstado"),
            "observaciones": veh.get("observaciones", ""),
            "conductor": " ".join(x for x in [veh.get("condNombres", ""), veh.get("condApellidos", "")] if x).strip(),
            "cedula": veh.get("condCedulaCiudadania", ""),
            "celular": veh.get("condCelular", ""),
            "correo": veh.get("condCorreo", ""),
            "marca": veh.get("vehMarca", ""),
            "linea": veh.get("vehLinea", ""),
            "modelo": veh.get("vehModelo", ""),
            "tenedor": veh.get("tenedNombre", ""),
            "disponible_hoy": bool(checkin),
            "disponibilidad_origen": (checkin or {}).get("origen", ""),
            "disponibilidad_destinos": (checkin or {}).get("departamentos_destino", []),
            "timeline": timeline[:200],
        })
    )