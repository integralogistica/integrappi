# ============================================================
# Inventario Escaner (PRUEBAS) — planilla AUDITORIA CONTEO escaneada → IA → Excel
# ============================================================
# Recibe uno o varios PDFs escaneados (sin capa de texto) de la planilla
# "PLANILLA AUDITORIA CONTEO" (Rhenus/Henkel), los lee con Gemini (visión)
# y devuelve las filas en JSON (/leer) o un Excel de 2 hojas (/leer-excel):
#   1) "Auditoria Conteo": tabla principal, cada fila con el "Lote de Conteo"
#      del encabezado de SU hoja (identifica la planilla).
#   2) "Anexos": registros MANUSCRITOS de los recuadros bajo el subtítulo
#      "Anexos" (máx ~2 por hoja): Ubicacion, Referencia, lote, auditada.
#
# SESIONES de auditoría (/sesiones): la operación va trayendo hojas a medida
# que los operarios terminan de contar (~1 cada 5 min, hasta ~50); cada una se
# AGREGA a la sesión (persistida en Mongo `inventario_sesiones`, dedupe por
# hash de archivo) y el Excel CONSOLIDADO se descarga cuando se quiera.
#
# Formato fijo de la hoja (solo cambia la cantidad de registros):
#   Encabezado con "Lote de Conteo: I2510.HENKEL.PF-..." +
#   tabla Ubicacion | Referencia | EAN 13 | Descripcion | Atr 1 (lote, prefijo
#   "lotnum:") | Atr 2 | UOM | Cantidad Inventario | Cantidad Conteo 1 |
#   Cantidad Conteo 2 | Cantidad Auditada (MANUSCRITA) +
#   recuadros "Anexos" manuscritos al pie.
#
# La página llega rotada 90°: Gemini la lee igual, no hace falta prerotar.
#
# Endpoint de pruebas: SIN auth y SIN persistencia (todo en memoria).
# ============================================================
import asyncio
import base64
import hashlib
import json
import os
import re
import uuid
from datetime import datetime, timezone
from io import BytesIO
from typing import List

import requests
from fastapi import APIRouter, File, HTTPException, UploadFile
from fastapi.responses import StreamingResponse
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

from bd.bd_cliente import bd_cliente

ruta_inventario_escaner = APIRouter(prefix="/inventario-escaner", tags=["Inventario Escaner"])

# Sesiones de auditoría: una por jornada/lote de conteo; cada hoja escaneada se
# acumula como una página del doc (persistencia en Mongo — sin sesiones no hay
# forma de ir sumando hoja a hoja en un solo Excel).
bd = bd_cliente['integra']
coleccion_sesiones = bd['inventario_sesiones']

# --- CONFIGURACIÓN LLM (mismo patrón que rutas/vehiculos.py) ---
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.6-flash")
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"

XLSX_MEDIA_TYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
MAX_ARCHIVOS = 10
MAX_ARCHIVO_BYTES = 8 * 1024 * 1024   # 8 MB por archivo (los escaneos pesan ~450 KB)
GEMINI_TIMEOUT_S = 120                # el PDF escaneado + handwriting tarda más que un documento suelto

# Prefijo del lote en "Atr 1": impreso como "lotnum:" (a veces OCR-eado "lothum:").
# Se limpia en el backend, no en el modelo.
_RE_PREFIJO_LOTE = re.compile(r"^\s*lo[tnh]\w*\s*num\s*[:：]?\s*", re.IGNORECASE)

INSTRUCTION = """Eres un extractor de tablas de planillas de auditoría de inventario escaneadas \
(planilla "PLANILLA AUDITORIA CONTEO" de operador logístico, encabezado Rhenus/Henkel).

La página puede venir ROTADA 90° (impresa apaisada en hoja carta). La hoja tiene DOS zonas de datos:

A) TABLA PRINCIPAL, con columnas (de izquierda a derecha): Ubicacion, Referencia, EAN 13, \
Descripcion, Atr 1, Atr 2, UOM, Cantidad Inventario, Cantidad Conteo 1, Cantidad Conteo 2, \
Cantidad Auditada. Sus filas van en "filas".

B) Sección "ANEXOS" al pie: recuadros de diligenciamiento con las MISMAS etiquetas de columna \
impresas, donde una persona escribe A MANO (esfero) hasta 2 registros adicionales. \
Sus filas (SOLO las escritas a mano, no las vacías) van en "anexos" — JAMÁS las mezcles con "filas".

Devuelve EXCLUSIVAMENTE un objeto JSON con esta forma (nada más, sin markdown, sin explicaciones):
{
  "lote_de_conteo": "I2510.HENKEL.PF-1384-RADN1-11",
  "filas": [
    {"ubicacion": "AH47111", "referencia": "2600617", "art1": "lotnum: 123456", "auditada": 168}
  ],
  "anexos": [
    {"ubicacion": "AD4G3122", "referencia": "2496137", "art1": "29062EC340", "auditada": 8}
  ]
}

Reglas estrictas:
- "lote_de_conteo": el valor EXACTO que acompaña la etiqueta "Lote de Conteo:" en el ENCABEZADO de \
la hoja (ej: I2510.HENKEL.PF-1384-RADN1-11). null si no aparece.
- "ubicacion": valor EXACTO de la columna Ubicacion (ej: AH47111; en anexos es manuscrito).
- "referencia": valor de la columna Referencia, solo el código (suele ser numérico).
- "art1": valor EXACTO de la columna "Atr 1", INCLUIDO cualquier prefijo tipo "lotnum:" o "lothum:" \
(no lo traduzcas ni lo interpretes; si la celda trae solo guiones "----", devuélvelo tal cual; \
en anexos es manuscrito y suele venir SIN prefijo).
- "auditada": el número ESCRITO A MANO en la columna "Cantidad Auditada". Es manuscrito, distinto \
del texto impreso. null si la celda está vacía o solo hay una marca/check (✓) sin número. \
NUNCA uses Cantidad Inventario, Conteo 1 ni Conteo 2 para llenarla.
- En "anexos": solo Ubicacion, Referencia, art1 y auditada (ignora Descripcion/UOM). \
Filas del recuadro totalmente vacías: omítelas.
- NO incluyas en ninguna lista la fila de encabezados ni el bloque de firmas del pie \
(Auditor de Conteo, Usuario Digitador, Lider Inventario, Auditor Cliente).
- Si un dato no se ve o no es legible, null. NUNCA inventes filas ni valores.
- Corrige errores de OCR obvios (l vs 1, O vs 0) usando el contexto.
- Filas completamente vacías de la tabla principal: omítelas."""


def _llamar_gemini(parts: list, instruction: str) -> str:
    """Llama a Gemini con las parts (PDF/imagen inline) y devuelve el texto de respuesta."""
    if not GEMINI_API_KEY:
        raise HTTPException(status_code=503, detail="Servicio de lectura no configurado (falta GEMINI_API_KEY).")

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
            timeout=GEMINI_TIMEOUT_S,
        )
    except requests.RequestException:
        raise HTTPException(status_code=504, detail="El servicio de lectura no respondió. Intenta de nuevo.")

    if respuesta.status_code == 429:
        raise HTTPException(status_code=429, detail="El servicio de lectura está saturado o sin crédito. Intenta más tarde.")
    if respuesta.status_code != 200:
        # Sin emoji: en consolas Windows (cp1252) el print con emoji revienta y
        # rompería el propio except. ASCII siempre es seguro (patrón vehiculos.py).
        print(f"[inventario-escaner] Gemini error {respuesta.status_code}: {respuesta.text[:300]}")
        raise HTTPException(status_code=502, detail="Error del servicio de lectura de documentos.")

    candidatos = respuesta.json().get("candidates", [])
    if not candidatos:
        raise HTTPException(status_code=502, detail="El servicio no devolvió resultados para este archivo.")
    return "".join(p.get("text", "") for p in candidatos[0].get("content", {}).get("parts", []))


def _primer_objeto_json(texto: str) -> dict:
    """
    Parsea el JSON de respuesta de forma defensiva: si el modelo lo envolvió en
    prosa o ```json ...```, recupera el primer objeto balanceado {...}.
    (Mismo espíritu que el parser de rutas/vehiculos.py.)
    """
    try:
        return json.loads(texto)
    except json.JSONDecodeError:
        pass
    inicio = texto.find("{")
    if inicio == -1:
        raise HTTPException(status_code=502, detail="No se pudo interpretar la lectura de la planilla.")
    profundidad = 0
    for i, ch in enumerate(texto[inicio:], start=inicio):
        if ch == "{":
            profundidad += 1
        elif ch == "}":
            profundidad -= 1
            if profundidad == 0:
                try:
                    return json.loads(texto[inicio:i + 1])
                except json.JSONDecodeError:
                    break
    raise HTTPException(status_code=502, detail="No se pudo interpretar la lectura de la planilla.")


def _leer_archivo(nombre: str, datos: bytes, content_type: str) -> dict:
    """Lee UN archivo (página) con Gemini y devuelve {lote_de_conteo, filas, anexos} normalizado."""
    if not (content_type == "application/pdf" or content_type.startswith("image/")):
        raise HTTPException(status_code=400, detail=f"'{nombre}': tipo no soportado ({content_type}). Sube PDF o imagen.")
    if len(datos) == 0:
        raise HTTPException(status_code=400, detail=f"'{nombre}': archivo vacío.")

    mime = "application/pdf" if content_type == "application/pdf" else content_type
    parts = [
        {"inline_data": {"mime_type": mime, "data": base64.b64encode(datos).decode("utf-8")}},
        {"text": "Extrae el lote de conteo, las filas de la tabla y los anexos manuscritos."},
    ]
    try:
        resultado = _primer_objeto_json(_llamar_gemini(parts, INSTRUCTION))
    except HTTPException as e:
        # Nombrar el archivo problemático hace el error accionable con varios a la vez.
        e.detail = f"'{nombre}': {e.detail}"
        raise

    lote_conteo = (resultado.get("lote_de_conteo") or "").strip() or None

    def _limpiar(lista_cruda: list) -> list:
        return [
            _normalizar_fila(f, lote_conteo)
            for f in lista_cruda
            if isinstance(f, dict)
            # Un anexo/fila sin NINGÚN campo no aporta nada (evita rellenos vacíos).
            and any((f.get(k) or "").strip() if isinstance(f.get(k), str) else f.get(k) for k in ("ubicacion", "referencia", "art1", "auditada"))
        ]

    filas = _limpiar(resultado.get("filas") if isinstance(resultado.get("filas"), list) else [])
    anexos = _limpiar(resultado.get("anexos") if isinstance(resultado.get("anexos"), list) else [])
    # Cada fila de la tabla principal lleva el # de anexos que trajo SU hoja:
    # el analista ve en "Auditoria Conteo" qué hojas físicas tuvieron registros
    # manuscritos aparte (los detalles están en la hoja "Anexos").
    for fila in filas:
        fila["anexos_en_hoja"] = len(anexos)
    return {"lote_de_conteo": lote_conteo, "filas": filas, "anexos": anexos}


def _normalizar_fila(fila: dict, lote_conteo: str | None) -> dict:
    """Limpia una fila cruda del modelo: strings, lote sin prefijo, auditada numérica."""
    ubicacion = (fila.get("ubicacion") or "").strip() or None
    referencia = (fila.get("referencia") or "").strip() or None
    art1 = (fila.get("art1") or "").strip() or None
    lote = _RE_PREFIJO_LOTE.sub("", art1).strip() if art1 else None

    auditada = fila.get("auditada")
    if isinstance(auditada, str):
        auditada = auditada.strip()
        auditada = int(auditada) if auditada.isdigit() else (auditada or None)

    return {
        "lote_conteo": lote_conteo,
        "ubicacion": ubicacion,
        "referencia": referencia,
        "lote": lote or None,
        "auditada": auditada,
    }


async def _leer_archivos(archivos: List[UploadFile]) -> tuple:
    """
    Valida y lee todos los archivos (uno por request de Gemini: aísla fallos,
    preserva el orden de páginas y evita truncar el output con hojas largas).
    Un archivo sin filas no es error (puede ser una página de firmas); el 422
    se decide al final si NADIE trajo filas ni anexos.
    """
    if not archivos:
        raise HTTPException(status_code=422, detail="No se recibió ningún archivo.")
    if len(archivos) > MAX_ARCHIVOS:
        raise HTTPException(status_code=422, detail=f"Máximo {MAX_ARCHIVOS} archivos por consulta.")

    filas = []
    anexos = []
    avisos = []
    for archivo in archivos:
        datos = await archivo.read(MAX_ARCHIVO_BYTES + 1)
        if len(datos) > MAX_ARCHIVO_BYTES:
            raise HTTPException(status_code=413, detail=f"'{archivo.filename}': supera los 8 MB.")
        resultado = await asyncio.to_thread(
            _leer_archivo, archivo.filename or "(sin nombre)", datos, archivo.content_type or ""
        )
        if not resultado["filas"] and not resultado["anexos"]:
            avisos.append(f"'{archivo.filename}' no aportó filas (¿página sin tabla?).")
        filas.extend(resultado["filas"])
        anexos.extend(resultado["anexos"])

    if not filas and not anexos:
        raise HTTPException(
            status_code=422,
            detail="La IA no encontró filas en ningún archivo. Verifica que sean planillas de auditoría de conteo escaneadas.",
        )
    return filas, anexos, avisos


def _marca_anexos(fila: dict) -> str:
    """'Sí (N)' si la hoja de esa fila trajo N anexos manuscritos; '' si no (o página previa al campo)."""
    n = fila.get("anexos_en_hoja")
    return f"Sí ({n})" if n else ""


def _hoja_excel(wb: Workbook, titulo: str, filas: list, con_anexos: bool = False) -> None:
    """
    Escribe una hoja con la estructura estándar del módulo:
    Lote de Conteo | Ubicacion | Referencia | Lote (Art 1) | Cantidad Auditada
    (+ "Anexos en hoja" al final cuando con_anexos=True — solo "Auditoria Conteo").
    """
    ws = wb.create_sheet(titulo)
    cabecera = ["Lote de Conteo", "Ubicacion", "Referencia", "Lote (Art 1)", "Cantidad Auditada"]
    anchos = [30, 14, 14, 22, 18]
    if con_anexos:
        cabecera.append("Anexos en hoja")
        anchos.append(14)

    ws.append(cabecera)
    for celda in ws[1]:
        celda.font = Font(bold=True, color="FFFFFF")
        celda.fill = PatternFill("solid", fgColor="1F4E78")
        celda.alignment = Alignment(horizontal="center", vertical="center")

    for fila in filas:
        valores = [fila["lote_conteo"], fila["ubicacion"], fila["referencia"], fila["lote"], fila["auditada"]]
        if con_anexos:
            valores.append(_marca_anexos(fila))
        ws.append(valores)

    for i, ancho in enumerate(anchos, start=1):
        ws.column_dimensions[get_column_letter(i)].width = ancho
    ws.freeze_panes = "A2"


def _excel_filas(filas: list, anexos: list) -> BytesIO:
    """
    Construye el Excel (openpyxl) con DOS hojas:
    1) "Auditoria Conteo": filas de la tabla principal de todas las páginas, en orden,
       cada una con el Lote de Conteo de SU hoja (la identifica) y la marca
       "Anexos en hoja" (Sí (N)) para saber qué hojas físicas trajeron anexos.
    2) "Anexos": registros MANUSCRITOS de los recuadros bajo el subtítulo "Anexos"
       de cada hoja (máx ~2 por hoja), mismos encabezados. Se crea siempre para que
       el formato del archivo sea estable.
    """
    wb = Workbook()
    wb.remove(wb.active)                       # la hoja default sobra
    _hoja_excel(wb, "Auditoria Conteo", filas, con_anexos=True)
    _hoja_excel(wb, "Anexos", anexos)

    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    return buffer


@ruta_inventario_escaner.post("/leer")
async def leer_planilla(archivos: List[UploadFile] = File(...)):
    """Lee las planillas escaneadas con IA y devuelve las filas y anexos en JSON (para depurar)."""
    filas, anexos, avisos = await _leer_archivos(archivos)
    return {
        "total_filas": len(filas),
        "filas": filas,
        "anexos": anexos,
        "avisos": avisos,
    }


@ruta_inventario_escaner.post("/leer-excel")
async def leer_planilla_excel(archivos: List[UploadFile] = File(...)):
    """Lee las planillas escaneadas con IA y devuelve un Excel con 2 hojas (tabla + anexos manuscritos)."""
    filas, anexos, _ = await _leer_archivos(archivos)
    nombre = f"auditoria_conteo_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    return StreamingResponse(
        _excel_filas(filas, anexos),
        media_type=XLSX_MEDIA_TYPE,
        headers={"Content-Disposition": f'attachment; filename="{nombre}"'},
    )


# ============================================================
# SESIONES DE AUDITORÍA — acumulado hoja a hoja en un solo Excel
#
# La operación real: los operarios van trayendo planillas escaneadas a medida
# que terminan de contar (~1 hoja cada 5 min, hasta ~50 por auditoría). Cada
# hoja se agrega a la sesión y el Excel consolidado se descarga cuando se quiera
# (siempre con TODO lo acumulado). La página queda persistida en Mongo con el
# hash del archivo → un re-escaneo no duplica filas.
# ============================================================


def _sesion_o_404(sesion_id: str) -> dict:
    sesion = coleccion_sesiones.find_one({"_id": sesion_id})
    if not sesion:
        raise HTTPException(status_code=404, detail=f"Sesión '{sesion_id}' no existe.")
    return sesion


def _resumen_sesion(sesion: dict) -> dict:
    paginas = sesion.get("paginas", [])
    filas = [f for p in paginas for f in p.get("filas", [])]
    anexos = [a for p in paginas for a in p.get("anexos", [])]
    return {
        "sesion_id": sesion["_id"],
        "paginas": len(paginas),
        "total_filas": len(filas),
        "total_anexos": len(anexos),
        "lotes_de_conteo": sorted({p.get("lote_de_conteo") for p in paginas if p.get("lote_de_conteo")}),
        "creada_en": sesion.get("creada_en"),
        "ultima_adicion_en": sesion.get("ultima_adicion_en"),
    }


@ruta_inventario_escaner.post("/sesiones", status_code=201)
async def crear_sesion():
    """Crea una sesión de auditoría (una por jornada/lote de conteo)."""
    sesion_id = f"INV-{uuid.uuid4().hex[:10]}"
    ahora = datetime.now(timezone.utc)
    coleccion_sesiones.insert_one({
        "_id": sesion_id,
        "creada_en": ahora,
        "ultima_adicion_en": ahora,
        "paginas": [],
    })
    return {"sesion_id": sesion_id, "creada_en": ahora}


@ruta_inventario_escaner.get("/sesiones")
async def listar_sesiones():
    """Sesiones existentes con su resumen (cuántas páginas/filas/anexos acumula cada una)."""
    return [_resumen_sesion(s) for s in coleccion_sesiones.find().sort("creada_en", -1)]


@ruta_inventario_escaner.get("/sesiones/{sesion_id}")
async def detalle_sesion(sesion_id: str):
    """Detalle de una sesión: resumen + páginas agregadas (sin las filas completas)."""
    sesion = _sesion_o_404(sesion_id)
    detalle = _resumen_sesion(sesion)
    detalle["paginas_detalle"] = [
        {
            "nombre": p.get("nombre"),
            "lote_de_conteo": p.get("lote_de_conteo"),
            "filas": len(p.get("filas", [])),
            "anexos": len(p.get("anexos", [])),
            "agregada_en": p.get("agregada_en"),
        }
        for p in sesion.get("paginas", [])
    ]
    return detalle


@ruta_inventario_escaner.post("/sesiones/{sesion_id}/agregar")
async def agregar_a_sesion(sesion_id: str, archivos: List[UploadFile] = File(...)):
    """
    Agrega una o más hojas escaneadas a la sesión. Cada hoja se lee con IA y
    queda persistida; una hoja ya agregada antes (mismo contenido) se omite
    con aviso — el re-escaneo no duplica filas.
    """
    _sesion_o_404(sesion_id)                       # 404 antes de gastar Gemini
    if len(archivos) > MAX_ARCHIVOS:
        raise HTTPException(status_code=422, detail=f"Máximo {MAX_ARCHIVOS} archivos por consulta.")

    hashes_previos = {
        p["hash"] for p in coleccion_sesiones.find_one({"_id": sesion_id}).get("paginas", [])
    }

    agregadas = 0
    avisos = []
    for archivo in archivos:
        datos = await archivo.read(MAX_ARCHIVO_BYTES + 1)
        if len(datos) > MAX_ARCHIVO_BYTES:
            raise HTTPException(status_code=413, detail=f"'{archivo.filename}': supera los 8 MB.")

        nombre = archivo.filename or "(sin nombre)"
        hash_archivo = hashlib.sha256(datos).hexdigest()
        if hash_archivo in hashes_previos:
            # El mismo archivo ya está en la sesión: re-escaneo/doble envío.
            avisos.append(f"'{nombre}': ya estaba en la sesión, se omite (no se duplica).")
            continue

        resultado = await asyncio.to_thread(
            _leer_archivo, nombre, datos, archivo.content_type or ""
        )
        if not resultado["filas"] and not resultado["anexos"]:
            avisos.append(f"'{nombre}' no aportó filas (¿página sin tabla?) — no se agrega.")
            continue

        pagina = {
            "hash": hash_archivo,
            "nombre": nombre,
            "lote_de_conteo": resultado["lote_de_conteo"],
            "filas": resultado["filas"],
            "anexos": resultado["anexos"],
            "agregada_en": datetime.now(timezone.utc),
        }
        # $push por página: dos operarios agregando a la vez no se pisan.
        coleccion_sesiones.update_one(
            {"_id": sesion_id},
            {"$push": {"paginas": pagina}, "$set": {"ultima_adicion_en": pagina["agregada_en"]}},
        )
        hashes_previos.add(hash_archivo)
        agregadas += 1

    if agregadas == 0:
        raise HTTPException(
            status_code=422,
            detail=f"Ningún archivo se agregó a la sesión: {'; '.join(avisos) or 'sin archivos'}",
        )
    return {"agregadas": agregadas, "avisos": avisos, **_resumen_sesion(_sesion_o_404(sesion_id))}


@ruta_inventario_escaner.get("/sesiones/{sesion_id}/excel")
async def excel_sesion(sesion_id: str):
    """
    Excel CONSOLIDADO de la sesión (2 hojas) con todo lo acumulado hasta ahora.
    Se puede descargar cuantas veces se quiera: cada descarga refleja el estado actual.
    """
    sesion = _sesion_o_404(sesion_id)
    paginas = sesion.get("paginas", [])
    filas = [f for p in paginas for f in p.get("filas", [])]
    anexos = [a for p in paginas for a in p.get("anexos", [])]
    return StreamingResponse(
        _excel_filas(filas, anexos),
        media_type=XLSX_MEDIA_TYPE,
        headers={"Content-Disposition": f'attachment; filename="auditoria_conteo_{sesion_id}.xlsx"'},
    )


@ruta_inventario_escaner.delete("/sesiones/{sesion_id}")
async def eliminar_sesion(sesion_id: str):
    """Elimina la sesión completa (sus páginas quedan fuera del consolidado)."""
    resultado = _sesion_o_404(sesion_id)
    coleccion_sesiones.delete_one({"_id": resultado["_id"]})
    return {"eliminada": sesion_id}
