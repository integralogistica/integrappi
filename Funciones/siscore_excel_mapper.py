# Funciones/siscore_excel_mapper.py
"""
Mapeo del Excel descargado del portal Siscore ("DESCARGAR ASIGNADOS ACTUALMENTE")
al contrato que consume el frontend de SolicitudVehiculos.

Es un módulo PURAMENTE FUNCIONAL (sin navegador) para poder testearlo con una
muestra del .xlsx sin levantar Chromium.

Columnas que trae el Excel: Entidad, Guia, Nombre, Direccion, Destino, Placa,
Manifiesto, Peso, Piezas.

El frontend (handleBuscar) lee claves EXACTAS:
  Planilla, Piezas, 'Peso Real', Ruta, 'Codigo Pedido', 'Cliente Origen',
  'Municipio Destino', 'Departamento Destino', 'Centro Costo', 'Bodega Origen'

NOTA: El mapeo de 'Guia' -> 'Codigo Pedido' y 'Entidad' -> 'Cliente Origen' es
TENTATIVO y debe confirmarse contra una muestra real del Excel enviada por el
usuario. Mientras tanto, la normalización de columnas es robusta a variaciones
de mayúsculas/tildes/sinónimos.
"""
import logging
import unicodedata
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Utilidades de texto
# ---------------------------------------------------------------------------

def normalizar_texto(s: Any) -> str:
    """Mayúsculas, sin espacios laterales y sin tildes."""
    if s is None:
        return ""
    s = str(s).strip().upper()
    # Quitar tildes/diacríticos
    s = "".join(
        c for c in unicodedata.normalize("NFKD", s)
        if not unicodedata.combining(c)
    )
    # Colapsar espacios internos
    return " ".join(s.split())


# Sinónimos de columnas: cualquier variante normalizada -> nombre canónico.
# Las claves y valores ya van en forma normalizada (MAYÚS, sin tildes).
SINONIMOS_COLUMNAS: Dict[str, str] = {
    # Peso
    "PESO": "PESO", "PESO REAL": "PESO", "PESOREAL": "PESO",
    "PESO REAL KG": "PESO", "PESO KG": "PESO", "KG": "PESO",
    "PESO REAL (KG)": "PESO",
    # Guia
    "GUIA": "GUIA", "NUMERO GUIA": "GUIA", "N GUIA": "GUIA",
    "NO GUIA": "GUIA", "GUIA NUMERO": "GUIA", "NUMGUIA": "GUIA",
    # Piezas
    "PIEZAS": "PIEZAS", "PIEZA": "PIEZAS", "CANTIDAD": "PIEZAS",
    "NUMERO PIEZAS": "PIEZAS", "N PIEZAS": "PIEZAS", "UNIDADES": "PIEZAS",
    # Placa
    "PLACA": "PLACA", "PLACA VEHICULO": "PLACA", "VEHICULO": "PLACA",
    "VEHICULO PLACA": "PLACA",
    # Manifiesto
    "MANIFIESTO": "MANIFIESTO", "NO MANIFIESTO": "MANIFIESTO",
    "N MANIFIESTO": "MANIFIESTO", "MANIFIESTO NUMERO": "MANIFIESTO",
    "NUMMANIFIESTO": "MANIFIESTO",
    # Entidad
    "ENTIDAD": "ENTIDAD", "CLIENTE": "ENTIDAD", "RAZON SOCIAL": "ENTIDAD",
    "ENTIDAD CLIENTE": "ENTIDAD",
    # Nombre (destinatario)
    "NOMBRE": "NOMBRE", "NOMBRE DESTINATARIO": "NOMBRE",
    "DESTINATARIO": "NOMBRE", "CONTACTO": "NOMBRE", "RAZON SOCIAL DESTINO": "NOMBRE",
    # Dirección
    "DIRECCION": "DIRECCION", "DIRECCION DESTINO": "DIRECCION",
    "DIRECCION DESTINATARIO": "DIRECCION", "DIR": "DIRECCION",
    # Destino (municipio)
    "DESTINO": "DESTINO", "MUNICIPIO": "DESTINO",
    "MUNICIPIO DESTINO": "DESTINO", "CIUDAD": "DESTINO",
    "CIUDAD DESTINO": "DESTINO", "POBLACION": "DESTINO",
    # Otras columnas que trae el Excel del portal: se reconocen (identidad) para
    # no generar ruido en logs, pero NO se mapean al contrato del frontend.
    "CEDULA": "CEDULA",
    "ORIGEN": "ORIGEN",          # OJO: 'Origen' del Excel (JUAN MINA) NO es Cliente Origen
    "PRODUCTO": "PRODUCTO",
    "CODIGO": "CODIGO",
    "MENSAJERO": "MENSAJERO",
    "USUARIO": "USUARIO",
    "CONDUCTOR": "CONDUCTOR",
    "FECHA": "FECHA",
    "ESTADO": "ESTADO",
    "VALOR DECLARADO": "VALOR DECLARADO",
    "VALOR DECLARADO ": "VALOR DECLARADO",
}


def normalizar_columnas_excel(df: pd.DataFrame) -> pd.DataFrame:
    """
    Renombra las columnas del DataFrame a sus nombres canónicos usando SINONIMOS_COLUMNAS.
    Las columnas no reconocidas se conservan pero se loguean como advertencia.
    """
    nuevo_nombres: Dict[str, str] = {}
    no_reconocidas: List[str] = []
    for col in df.columns:
        col_norm = normalizar_texto(col)
        canonico = SINONIMOS_COLUMNAS.get(col_norm)
        if canonico:
            nuevo_nombres[col] = canonico
        else:
            # Conservar la columna original por si acaso
            nuevo_nombres[col] = col_norm
            no_reconocidas.append(str(col))

    if no_reconocidas:
        logger.warning(
            f"[MAPPER] Columnas del Excel NO reconocidas (se conservan tal cual): {no_reconocidas}"
        )

    return df.rename(columns=nuevo_nombres)


# ---------------------------------------------------------------------------
# Enriquecimiento con divipolas
# ---------------------------------------------------------------------------

def construir_lookup_divipolas(coleccion_divipolas: Any) -> Dict[str, Dict[str, str]]:
    """
    Construye un diccionario {poblacion_normalizada: {ruta, departamento}}
    a partir de la colección MongoDB `divipolas`.
    Mismo patrón que exportar-planillas-excel (líneas ~2080-2087).
    """
    lookup: Dict[str, Dict[str, str]] = {}
    try:
        for doc in coleccion_divipolas.find({}):
            pob = normalizar_texto(doc.get("poblacion", ""))
            if pob and pob not in lookup:
                lookup[pob] = {
                    "ruta": (doc.get("ruta") or "").strip(),
                    "departamento": (doc.get("departamento") or "").strip(),
                }
    except Exception as e:
        logger.error(f"[MAPPER] Error construyendo lookup de divipolas: {e}")
    logger.info(f"[MAPPER] Lookup divipolas construido con {len(lookup)} poblaciones")
    return lookup


# ---------------------------------------------------------------------------
# Conversión de valores numéricos
# ---------------------------------------------------------------------------

def _a_float(valor: Any) -> float:
    """
    Convierte a float de forma robusta. Soporta formatos es-CO ('1.234,56')
    y en-US ('1,234.56'), además de números puros. Devuelve 0.0 si no parsea.
    """
    if valor is None:
        return 0.0
    if isinstance(valor, (int, float)):
        return float(valor)
    s = str(valor).strip()
    if not s:
        return 0.0
    # Quitar caracteres no numéricos excepto . , -
    s = s.replace(" ", "")
    try:
        if "," in s and "." in s:
            # Asumir formato es-CO: punto=miles, coma=decimal
            s = s.replace(".", "").replace(",", ".")
        elif "," in s:
            # Solo coma -> decimal
            s = s.replace(",", ".")
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def _a_int(valor: Any) -> int:
    return int(round(_a_float(valor)))


def _txt(valor: Any, fallback: str = "-") -> str:
    if valor is None:
        return fallback
    s = str(valor).strip()
    return s if s else fallback


# ---------------------------------------------------------------------------
# Mapeo fila -> registro del contrato frontend
# ---------------------------------------------------------------------------

def mapear_fila_a_registro(
    planilla: str,
    fila: Dict[str, Any],
    lookup_divipolas: Dict[str, Dict[str, str]],
) -> Dict[str, Any]:
    """
    Convierte una fila (dict) del Excel en un registro con las claves EXACTAS
    que lee handleBuscar, más los campos extra (Placa, Manifiesto, Guia).
    """
    destino_txt = _txt(fila.get("DESTINO"), "-")
    destino_norm = normalizar_texto(destino_txt)

    div = lookup_divipolas.get(destino_norm) or {}
    ruta = div.get("ruta") or "-"
    departamento = div.get("departamento") or "-"
    if destino_norm and not div:
        logger.warning(
            f"[MAPPER] Destino '{destino_txt}' (planilla {planilla}) "
            f"no encontrado en divipolas; Ruta/Departamento quedan en '-'."
        )

    guia = _txt(fila.get("GUIA"), "-")
    entidad = _txt(fila.get("ENTIDAD"), "-")

    registro: Dict[str, Any] = {
        # ---- Claves que lee el frontend (handleBuscar) ----
        "Planilla": str(planilla).strip(),
        "Piezas": _a_int(fila.get("PIEZAS")),
        "Peso Real": _a_float(fila.get("PESO")),
        "Codigo Pedido": guia,            # TENTATIVO: confirmar con muestra
        "Cliente Origen": entidad,        # TENTATIVO: confirmar con muestra
        "Municipio Destino": destino_txt,
        "Departamento Destino": departamento,
        "Ruta": ruta,
        "Centro Costo": "FMC",            # default; el frontend hace fallback a FMC
        "Bodega Origen": "FMC",
        # ---- Campos extra (para uso futuro / exportación) ----
        "Placa": _txt(fila.get("PLACA"), ""),
        "Manifiesto": _txt(fila.get("MANIFIESTO"), ""),
        "Guia": guia,
        "Nombre": _txt(fila.get("NOMBRE"), ""),
        "Direccion": _txt(fila.get("DIRECCION"), ""),
        # ---- Campos detallados por guía (para auditoría en MongoDB) ----
        "Cedula": _txt(fila.get("CEDULA"), ""),
        "Origen": _txt(fila.get("ORIGEN"), ""),
        "Producto": _txt(fila.get("PRODUCTO"), ""),
        "Codigo": _txt(fila.get("CODIGO"), ""),
        "Mensajero": _txt(fila.get("MENSAJERO"), ""),
        "Usuario": _txt(fila.get("USUARIO"), ""),
        "Conductor": _txt(fila.get("CONDUCTOR"), ""),
        "Fecha": _txt(fila.get("FECHA"), ""),
        "Estado": _txt(fila.get("ESTADO"), ""),
        "Valor Declarado": _txt(fila.get("VALOR DECLARADO"), ""),
    }
    return registro


# ---------------------------------------------------------------------------
# Lectura del Excel completo
# ---------------------------------------------------------------------------

def _leer_dataframe(path_excel: str) -> pd.DataFrame:
    """
    Lee el archivo descargado del portal detectando su formato real:
      - HTML disfrazado de .xls (lo que realmente entrega el portal) -> pd.read_html
      - .xlsx real (ZIP 'PK') -> openpyxl
      - .xls BIFF (OLE2/CFB) -> xlrd (si está instalado)
    Devuelve un DataFrame (vacío si no se pudo leer).
    """
    try:
        with open(path_excel, "rb") as f:
            head = f.read(512)
    except Exception as e:
        logger.error(f"[MAPPER] No se pudo abrir el archivo {path_excel}: {e}")
        return pd.DataFrame()

    head_text = head.decode("latin-1", errors="ignore").lstrip().lower()

    # HTML / XML disfrazado de .xls (caso real del portal Siscore)
    if head_text.startswith("<") or "<table" in head_text or "<html" in head_text:
        logger.info(f"[MAPPER] Archivo detectado como HTML-as-xls: {path_excel}")
        try:
            dfs = pd.read_html(path_excel, header=0)
        except Exception as e:
            logger.error(f"[MAPPER] read_html falló: {e}")
            return pd.DataFrame()
        if not dfs:
            return pd.DataFrame()
        # Elegir la tabla que más se parece al informe (cabeceras conocidas)
        def _score(d: pd.DataFrame) -> int:
            cols = " ".join(str(c).upper() for c in d.columns)
            return int("GUIA" in cols) + int("ENTIDAD" in cols) + int("PIEZAS" in cols) + len(d)
        return max(dfs, key=_score)

    # .xlsx real (ZIP)
    if head[:2] == b"PK":
        try:
            return pd.read_excel(path_excel, engine="openpyxl", dtype=str)
        except Exception as e:
            logger.error(f"[MAPPER] openpyxl falló: {e}")
            return pd.DataFrame()

    # .xls BIFF (OLE2/CFB)
    if head[:4] == b"\xd0\xcf\x11\xe0":
        try:
            return pd.read_excel(path_excel, engine="xlrd", dtype=str)
        except Exception as e:
            logger.warning(f"[MAPPER] xlrd falló (¿instalado?): {e}")
            return pd.DataFrame()

    # Último recurso
    try:
        return pd.read_html(path_excel, header=0)[0]
    except Exception:
        try:
            return pd.read_excel(path_excel, dtype=str)
        except Exception as e:
            logger.error(f"[MAPPER] No se pudo leer el archivo en ningún formato: {e}")
            return pd.DataFrame()


# Subcadenas que identifican la fila de pie de página que el portal agrega al final
_PIE_PAGINA = ("informacion", "total registros", "planilla:", "tabla:")


def _es_fila_pie_pagina(fila: Dict[str, Any]) -> bool:
    """Detecta la fila de pie/sumario del informe del portal."""
    for v in fila.values():
        s = str(v).lower()
        if any(marca in s for marca in _PIE_PAGINA):
            return True
    return False


def leer_excel_a_registros(
    path_excel: str,
    planilla: str,
    lookup_divipolas: Dict[str, Dict[str, str]],
) -> List[Dict[str, Any]]:
    """
    Lee el Excel descargado para una planilla (HTML-as-xls u otros formatos) y
    devuelve la lista de registros (uno por fila de datos) con el contrato del
    frontend.
    """
    df = _leer_dataframe(path_excel)
    if df is None or df.empty:
        logger.warning(f"[MAPPER] Archivo vacío o ilegible para planilla {planilla}: {path_excel}")
        return []

    # Descartar columnas 'Unnamed' (artefacto de read_html por colspan/rowspan)
    df = df.loc[:, [not str(c).startswith("Unnamed") for c in df.columns]]

    # Uniformizar a string y limpiar nulos
    df = df.astype(str)
    df = df.replace({"nan": "", "NaN": "", "None": "", "NaT": "", "<NA>": ""})

    df = normalizar_columnas_excel(df)

    registros: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        fila = row.to_dict()
        # Saltar fila de pie de página del portal y filas sin datos relevantes
        if _es_fila_pie_pagina(fila):
            continue
        if not any(
            _txt(fila.get(k), "") for k in ("GUIA", "DESTINO", "PESO", "PIEZAS", "PLACA")
        ):
            continue
        registros.append(mapear_fila_a_registro(planilla, fila, lookup_divipolas))

    logger.info(f"[MAPPER] Planilla {planilla}: {len(registros)} registros extraídos de {path_excel}")
    return registros
