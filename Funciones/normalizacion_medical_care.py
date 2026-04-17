"""
Funciones de normalización de datos para Medical Care
Portadas desde Power Query (logs.txt) a Python
"""

from typing import Optional, List, Tuple
import re
from collections import Counter


def fx_reemplazar_lista(txt: Optional[str], reemplazos: List[Tuple[str, str]]) -> Optional[str]:
    """Reemplaza múltiples patrones en un texto"""
    if txt is None:
        return None
    resultado = txt
    for patron, reemplazo in reemplazos:
        resultado = resultado.replace(patron, reemplazo)
    return resultado


def fx_compactar_espacios(txt: Optional[str]) -> Optional[str]:
    """Elimina espacios múltiples y recorta el texto"""
    if txt is None:
        return None
    return ' '.join(txt.strip().split())


def fx_primeras_n_palabras(txt: Optional[str], n: int) -> Optional[str]:
    """Retorna las primeras n palabras de un texto"""
    if txt is None:
        return None
    palabras = [p for p in txt.split() if p]
    return ' '.join(palabras[:n])


def fx_separar_via_numero(txt: Optional[str]) -> Optional[str]:
    """Corrige problemas específicos de vías y números"""
    if txt is None:
        return None
    reemplazos = [
        ("CARRERA12", "CARRERA 12"),
        ("CARRERA6TA", "CARRERA 6"),
        ("CALLE4TA", "CALLE 4")
    ]
    return fx_reemplazar_lista(txt, reemplazos)


def fx_normalizar_base(txt: Optional[str]) -> Optional[str]:
    """Normalización base: mayúsculas, trim, clean caracteres especiales"""
    if txt is None:
        return None

    # Convertir a mayúsculas y recortar
    t0 = txt.strip().upper()

    # Eliminar caracteres de control (similar a Text.Clean)
    t0 = ''.join(char for char in t0 if ord(char) >= 32 or char in '\n\r\t')

    # Reemplazos de codificación (incluyendo caracteres mal codificados UTF-8)
    reemplazos_codificacion = [
        ('\xa0', ' '),  # Non-breaking space
        ('–', '-'),
        # Caracteres correctamente codificados
        ('Ñ', 'N'), ('ÓN', 'ON'), ('Ó', 'O'), ('É', 'E'), ('Ú', 'U'), ('Í', 'I'), ('Á', 'A'),
        ('À', 'A'), ('È', 'E'), ('Ì', 'I'), ('Ò', 'O'), ('Ù', 'U'),
        ('Ü', 'U'), ('ñ', 'N'), ('á', 'A'), ('é', 'E'), ('í', 'I'), ('ó', 'O'), ('ú', 'U'),
        # Caracteres mal codificados (UTF-8 leído como Latin-1)
        ('Ã‘', 'N'), ('Ã"ON', 'ON'), ('Ã"', 'O'), ('Ã‰', 'E'), ('Ãš', 'U'), ('Ã', 'I'), ('Ã', 'A'),
        ('Ã€', 'A'), ('Ãˆ', 'E'), ('ÃŒ', 'I'), ('Ã"', 'O'), ('Ã™', 'U'),
        ('Ãœ', 'U'), ('Ã±', 'N'), ('Ã¡', 'A'), ('Ã©', 'E'), ('Ã­', 'I'), ('Ã³', 'O'), ('Ãº', 'U'),
        ('FÃ', 'FA'),
        ('Â', ''), ('ª', 'A'), ('º', 'O')
    ]
    t1 = fx_reemplazar_lista(t0, reemplazos_codificacion)

    # Reemplazos de tildes adicionales
    reemplazos_tildes = [
        ('Á', 'A'), ('É', 'E'), ('Í', 'I'), ('Ó', 'O'), ('Ú', 'U'), ('Ü', 'U'), ('Ñ', 'N'),
        ('À', 'A'), ('È', 'E'), ('Ì', 'I'), ('Ò', 'O'), ('Ù', 'U')
    ]
    t2 = fx_reemplazar_lista(t1, reemplazos_tildes)

    # Compactar espacios
    return fx_compactar_espacios(t2)


def fx_normalizar_paciente(txt: Optional[str]) -> Optional[str]:
    """Normaliza nombre de paciente: elimina caracteres especiales, orden alfabético, max 2 ocurrencias por palabra, primeras 6 palabras"""
    t0 = fx_normalizar_base(txt)

    # Reemplazos específicos para nombres
    reemplazos_paciente = [
        (',', ' '), ('.', ' '), (';', ' '), (':', ' '), ('/', ' '), ('\\', ' '),
        ('-', ' '), ('(', ' '), (')', ' '), ('[', ' '), (']', ' '), ('{', ' '), ('}', ' '),
        ("'", ' '), ('"', ' '), ('|', ' '), ('_', ' '), ('*', ' '), ('+', ' '), ('=', ' ')
    ]

    t1 = fx_reemplazar_lista(t0, reemplazos_paciente)
    t2 = fx_compactar_espacios(t1)

    # Obtener todas las palabras
    palabras = [p for p in t2.split() if p]

    # Reordenar alfabéticamente
    palabras_ordenadas = sorted(palabras)

    # Eliminar palabras repetidas más de 2 veces (mantener máximo 2 ocurrencias)
    conteo = Counter(palabras_ordenadas)

    palabras_sin_repetir = []
    for palabra in palabras_ordenadas:
        if conteo[palabra] > 2:
            if palabras_sin_repetir.count(palabra) < 2:
                palabras_sin_repetir.append(palabra)
        else:
            palabras_sin_repetir.append(palabra)

    # Primeras 6 palabras (sin repeticiones excesivas)
    return ' '.join(palabras_sin_repetir[:6])


def fx_normalizar_direccion(txt: Optional[str]) -> Optional[str]:
    """Normalización completa de direcciones con correcciones de errores comunes"""
    t0 = fx_normalizar_base(txt)

    # Eliminar signos de puntuación
    reemplazos_signos = [
        (',', ' '), ('.', ' '), (';', ' '), (':', ' '), ('/', ' '), ('\\', ' '),
        ('(', ' '), (')', ' '), ('[', ' '), (']', ' '), ('{', ' '), ('}', ' '),
        ("'", ' '), ('"', ' '), ('|', ' '), ('_', ' '), ('*', ' '), ('=', ' ')
    ]

    t1 = fx_reemplazar_lista(t0, reemplazos_signos)
    t1b = fx_compactar_espacios(t1)

    # Reemplazos de errores comunes
    reemplazos_errores = [
        (" CAKLE ", " CALLE "),
        (" CALKE ", " CALLE "),
        (" CAKLLE ", " CALLE "),
        (" CARREA ", " CARRERA "),
        (" CARRRA ", " CARRERA "),
        (" CARRERRA ", " CARRERA "),
        (" CARRRE ", " CARRERA "),
        (" TRASVERSAL ", " TRANSVERSAL "),
        (" TRANVERSAL ", " TRANSVERSAL "),
        (" TRANV ", " TRANSVERSAL "),
        (" TRANSV ", " TRANSVERSAL "),
        (" VEREEDA ", " VEREDA "),
        (" VERENA ", " VEREDA "),
        (" RECIDENCIAL ", " RESIDENCIAL "),
        (" COJUNTO ", " CONJUNTO "),
        (" MAZANA ", " MANZANA "),
        (" BARIO ", " BARRIO "),
        (" BARIRO ", " BARRIO "),
        (" BARRRIO ", " BARRIO "),
        (" BAARRIO ", " BARRIO "),
        (" ETAPTA ", " ETAPA "),
        (" SUPERMANZA ", " SUPERMANZANA "),
        (" SERRO ", " CERRO "),
        (" KLOMETRO ", " KILOMETRO "),
        (" KILOMETROS ", " KILOMETRO "),
        (" KILOMETROO ", " KILOMETRO "),
        (" APTO ", " APARTAMENTO "),
        (" AP ", " APARTAMENTO "),
        (" INT ", " INTERIOR "),
        (" TO ", " TORRE "),
        (" NUMERO ERO ", " NUMERO ")
    ]

    t2 = fx_reemplazar_lista(" " + t1b + " ", reemplazos_errores)

    # Reemplazos de abreviaturas de vías
    reemplazos_vias = [
        (" KRA ", " CARRERA "),
        (" KR ", " CARRERA "),
        (" CRA ", " CARRERA "),
        (" CR ", " CARRERA "),
        (" CLL ", " CALLE "),
        (" CL ", " CALLE "),
        (" TV ", " TRANSVERSAL "),
        ("TRV ", " TRANSVERSAL "),
        (" DG ", " DIAGONAL "),
        (" DIAG ", " DIAGONAL "),
        (" DIG ", " DIAGONAL "),
        (" AV ", " AVENIDA "),
        (" AVDA ", " AVENIDA ")
    ]

    t3 = fx_reemplazar_lista(t2, reemplazos_vias)

    # Reemplazos de abreviaturas de número
    reemplazos_numero = [
        (" N° ", " NUMERO "),
        (" Nº ", " NUMERO "),
        (" NO ", " NUMERO "),
        (" NRO ", " NUMERO "),
        (" NRO. ", " NUMERO "),
        (" NUM ", " NUMERO "),
        (" NUMER ", " NUMERO "),
        (" NUM ERO ", " NUMERO "),
        (" N0 ", " NUMERO "),
        (" # ", " NUMERO "),
        ("#", " NUMERO ")
    ]

    t4 = fx_reemplazar_lista(t3, reemplazos_numero)

    # Reemplazos de ubicación
    reemplazos_ubicacion = [
        (" BRR ", " BARRIO "),
        (" BR ", " BARRIO "),
        (" VDA ", " VEREDA "),
        (" VRDA ", " VEREDA "),
        (" SECT ", " SECTOR "),
        (" MZ ", " MANZANA "),
        (" MZA ", " MANZANA "),
        (" CS ", " CASA "),
        (" CASA LOTE ", " LOTE "),
        (" LT ", " LOTE "),
        (" ET ", " ETAPA "),
        (" URB ", " URBANIZACION "),
        (" CONJ ", " CONJUNTO "),
        (" RES ", " RESIDENCIAL "),
        ("BLOQ ", " BLOQUE "),
        (" BLQ ", " BLOQUE "),
        (" TOR ", " TORRE ")
    ]

    t5 = fx_reemplazar_lista(t4, reemplazos_ubicacion)

    # Reemplazos finales (eliminar duplicados)
    reemplazos_finales = [
        (" DIRECCION ", " "),
        (" DIRECION ", " "),
        (" DIREC ", " "),
        (" MUNICIPIO ", " "),
        (" NUMERO NUMERO ", " NUMERO "),
        (" NUMERO ERO ", " NUMERO "),
        (" CALLE CALLE ", " CALLE "),
        (" CARRERA CARRERA ", " CARRERA "),
        (" TRANSVERSAL TRANSVERSAL ", " TRANSVERSAL "),
        (" DIAGONAL DIAGONAL ", " DIAGONAL "),
        (" BARRIO BARRIO ", " BARRIO "),
        (" VEREDA VEREDA ", " VEREDA "),
        (" SECTOR SECTOR ", " SECTOR "),
        (" MANZANA MANZANA ", " MANZANA "),
        (" LOTE LOTE ", " LOTE ")
    ]

    t6 = fx_reemplazar_lista(t5, reemplazos_finales)
    t7 = fx_compactar_espacios(t6)
    t8 = fx_separar_via_numero(t7)
    t9 = fx_compactar_espacios(t8)

    # Reordenar alfabéticamente
    palabras = [p for p in t9.split() if p]
    palabras_ordenadas = sorted(palabras)
    t10 = ' '.join(palabras_ordenadas)

    return t10


def fx_normalizar_celular(txt: Optional[str]) -> Optional[str]:
    """Normaliza celular: solo dígitos, últimos 10 dígitos"""
    if txt is None:
        return None

    # Convertir a string
    t0 = str(txt)

    # Solo mantener dígitos
    solo_digitos = re.sub(r'\D', '', t0)

    # Retornar los últimos 10 dígitos
    if len(solo_digitos) <= 10:
        return solo_digitos if solo_digitos else None
    else:
        return solo_digitos[-10:]


def fx_separar_telefonos(txt: Optional[str]) -> tuple:
    """
    Separa un campo de celular en hasta dos números normalizados.
    Soporta separadores: ' - ', '/', ',', ';', ' y ', '|'.
    Limpia caracteres no numéricos al inicio/fin antes de partir.
    Retorna (telefono1, telefono2) donde telefono2 puede ser '' si no hay segundo número.
    """
    if not txt:
        return ('', '')

    # Limpiar caracteres no numéricos al inicio y al fin (ej: "-3123418728")
    texto = re.sub(r'^[^\d]+', '', str(txt).strip())
    texto = re.sub(r'[^\d]+$', '', texto)

    if not texto:
        return ('', '')

    partes = re.split(r'\s*[-/,;|]\s*|\s+y\s+', texto, maxsplit=1)

    def _norm(val: str) -> str:
        digits = re.sub(r'\D', '', val)
        if not digits:
            return ''
        return digits[-10:] if len(digits) > 10 else digits

    # Descartar partes vacías: si tel1 queda vacío pero tel2 tiene valor, promoverlo
    tel1 = _norm(partes[0]) if len(partes) > 0 else ''
    tel2 = _norm(partes[1]) if len(partes) > 1 else ''
    if not tel1 and tel2:
        tel1, tel2 = tel2, ''
    return (tel1, tel2)


def fx_normalizar_municipio(txt: Optional[str]) -> Optional[str]:
    """Normalización básica de municipio"""
    if txt is None:
        return None

    # Aplicar normalización base
    return fx_normalizar_base(txt)


def fx_normalizar_cedula(txt: Optional[str]) -> Optional[str]:
    """Normalización básica de cédula"""
    if txt is None:
        return None

    # Convertir a string
    t0 = str(txt)

    # Solo mantener dígitos
    solo_digitos = re.sub(r'\D', '', t0)

    return solo_digitos if solo_digitos else None
