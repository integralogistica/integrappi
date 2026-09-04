"""Consulta del RUES (Registro Único Empresarial, Confecámaras) por NIT.

El portal www.rues.org.co es una SPA React que no consulta backend propio:
llama a un API Elasticsearch (elasticprd.rues.org.co) cuyos payloads viajan
CIFRADOS con CryptoJS AES-256-CBC (formato OpenSSL "Salted__", KDF
EVP_BytesToKey/MD5) y una passphrase embebida en el bundle ("qwerty",
REACT_APP_SECRET_KEY del chunk services/127.*.js — hallazgo 2026-09-03).
Este bot replica el cifrado y consulta el API directamente con requests:
~1-2 s por consulta, costo $0, sin captcha y sin navegador.

Contrato del API (descifrado en vivo):
- POST /query {"term": NIT, "offset": 0, "type": 2, "filter": {...}} → hits
  Elasticsearch (_source: razon_social, desc_matricula, nit, dv, camara,
  matricula, fecha_matricula, id, municipio...).
- POST /api/Expediente/DetalleRM {"id": <codigo_camara+matricula>} → detalle
  completo (estado, renovaciones, tipo societario, CIIU con descripciones).
- POST /api/ConsultFacultadesXCamYMatricula {"codigo_camara", "matricula"} →
  HTML del representante legal ("REPRESENTACION LEGAL (PRINCIPALES)").

Trampas calibradas con la sonda:
- Un NIT que no existe NO devuelve 0 hits: el buscador lista TODO (total
  10000). El bot exige coincidencia EXACTA de numero_identificacion (con
  padding de ceros a la izquierda) antes de dar por encontrado el NIT.
- El filtro de estado del portal default es solo ["ACTIVA"] y OCULTA las
  matrículas canceladas/inactivas: siempre se envía ["ACTIVA", "CANCELADA"]
  y el estado real se lee del hit (el vocabulario incluye CANCELADA,
  INACTIVA, "MATRICULA INACTIVA POR PERDIDA DE CALIDAD DE COMERCIANTE"...).
- Si Confecámaras rota la passphrase, la fuente cae a NO_DISPONIBLE
  accionable: rotarla en SEGURIDAD_RUES_SECRET_KEY (sin redeploy).
"""
from __future__ import annotations

import base64
import hashlib
import html
import json
import os
import re
import threading
import time
from typing import Any, Dict, List, Optional

import requests
from Crypto.Cipher import AES

RUES_API_URL = os.getenv("SEGURIDAD_RUES_API_URL", "https://elasticprd.rues.org.co")
RUES_SECRET_KEY = os.getenv("SEGURIDAD_RUES_SECRET_KEY", "qwerty")
RUES_TIMEOUT_S = float(os.getenv("SEGURIDAD_RUES_TIMEOUT_S", "30"))

PORTAL_URL = "https://www.rues.org.co/"

# Bloqueo para serializar la sesión HTTP compartida entre hilos.
_LOCK = threading.Lock()

# El buscador del portal acepta términos de hasta 10 caracteres (11+ → 400).
_RE_NIT = re.compile(r"^\d{6,10}$")
_RE_FECHA_COMPACTA = re.compile(r"^(\d{4})(\d{2})(\d{2})$")
# "1010213062 - ZARATE PEÑA EDWIN MISAEL" dentro del HTML del representante.
_RE_REPRESENTANTE = re.compile(r"(\d{6,12})\s*-\s*([A-ZÁÉÍÓÚÑÜa-záéíóúñü0-9 .']+)")
_MAX_REPRESENTANTES = 5


class BotRuesError(RuntimeError):
    """Error del bot de consulta del RUES."""


class BotRuesSinResultado(BotRuesError):
    """El API respondió sin un resultado determinante (anti-envenenamiento)."""


def _cifrar(obj: Any) -> str:
    """Replica CryptoJS.AES.encrypt(JSON.stringify(obj), passphrase).

    AES-256-CBC con formato OpenSSL "Salted__" + salt de 8 bytes y derivación
    EVP_BytesToKey (MD5, 1 iteración): key 32 bytes + iv 16 bytes.
    """
    plano = json.dumps(obj, separators=(",", ":")).encode("utf-8")
    salt = os.urandom(8)
    derivado = b""
    previo = b""
    while len(derivado) < 48:
        previo = hashlib.md5(previo + RUES_SECRET_KEY.encode("utf-8") + salt).digest()
        derivado += previo
    key, iv = derivado[:32], derivado[32:48]
    pad = 16 - len(plano) % 16
    cuerpo = AES.new(key, AES.MODE_CBC, iv).encrypt(plano + bytes([pad]) * pad)
    return base64.b64encode(b"Salted__" + salt + cuerpo).decode("ascii")


def _normalizar_nit(valor: str) -> str:
    """Quita el dígito de verificación (-X) y todo lo que no sea dígito."""
    return re.sub(r"\D", "", str(valor or "").split("-")[0])


def _fecha_iso(valor: Any) -> Optional[str]:
    """'20250303' → '2025-03-03' (formato compacto AAAAMMDD del API)."""
    texto = str(valor or "").strip()
    m = _RE_FECHA_COMPACTA.match(texto)
    if not m:
        return None
    a, mes, d = m.group(1), m.group(2), m.group(3)
    if (a, mes, d) == ("9999", "12", "31"):  # vigencia indefinida
        return None
    try:
        return f"{a}-{mes}-{d}"
    except ValueError:
        return None


def _sesion() -> requests.Session:
    sesion = requests.Session()
    # El API valida el origen de la SPA: sin estos headers rechaza.
    sesion.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Content-Type": "application/json",
        "Origin": "https://www.rues.org.co",
        "Referer": PORTAL_URL,
        "app-name": "RuesFront",
    })
    return sesion


def _post(sesion: requests.Session, ruta: str, payload: dict) -> dict:
    try:
        respuesta = sesion.post(
            RUES_API_URL + ruta,
            json={"dataBody": _cifrar(payload)},
            timeout=RUES_TIMEOUT_S,
        )
    except requests.RequestException as exc:
        raise BotRuesError(f"RUES no respondió ({ruta}): {exc}") from exc
    if respuesta.status_code != 200:
        raise BotRuesError(f"RUES respondió {respuesta.status_code} en {ruta}")
    try:
        return respuesta.json()
    except ValueError as exc:
        raise BotRuesError(f"RUES devolvió una respuesta no JSON en {ruta}") from exc


def _buscar_hits(sesion: requests.Session, nit: str) -> List[dict]:
    """Hits de Registro Mercantil cuyo NIT coincida EXACTO con el consultado.

    El buscador lista todo cuando el término no matchea (total 10000): la
    coincidencia exacta de numero_identificacion (sin padding de ceros) es
    la única evidencia de que el NIT está registrado.
    """
    consulta = {
        "term": nit,
        "offset": 0,
        "type": 2,
        "filter": {
            "Category": ["JURIDICA", "NATURAL", "COMERCIO", "SUCURSAL", "AGENCIA"],
            # Solo ACTIVA ocultaría las canceladas/inactivas (filtro default
            # del portal): se piden ambas y el estado real se lee del hit.
            "Status": ["ACTIVA", "CANCELADA"],
            "advanced": False,
            "tipoRegistro": "RM",
        },
    }
    data = _post(sesion, "/query", consulta)
    hits = data.get("hits") or []
    coincidentes = []
    for hit in hits:
        fuente = hit.get("_source") or {}
        if (fuente.get("numero_identificacion") or "").lstrip("0") == nit:
            coincidentes.append(fuente)
    return coincidentes


def _normalizar_estado(valor: Any) -> str:
    """'A C T I V A' / 'ACTIVA ' → 'ACTIVA' (el API espacia las letras)."""
    return re.sub(r"\s+", "", str(valor or "")).upper()


def _parsear_representantes(texto: str) -> List[dict]:
    """Extrae cédula+nombre del HTML de representación legal (sin facultades)."""
    # El HTML llega con &nbsp; entre tokens y embebido en el JSON de respuesta.
    # unescape los convierte en \xa0: se normaliza a espacio para el regex.
    texto = html.unescape(str(texto or "")).replace("\xa0", " ")
    representantes: List[dict] = []
    for m in _RE_REPRESENTANTE.finditer(texto):
        documento = m.group(1).lstrip("0") or m.group(1)
        nombre = " ".join(m.group(2).split())
        representantes.append({"documento": documento, "nombre": nombre})
        if len(representantes) >= _MAX_REPRESENTANTES:
            break
    return representantes


def consultar_rues_sync(nit: str) -> Dict[str, Any]:
    """Consulta el estado de la matrícula mercantil de un NIT (sin DV).

    Retorna {nit, nit_con_dv, razon_social, estado, no_registra, camara,
    matricula, fechas, tipo societario, ciiu, representantes[], mensaje}.
    Un NIT sin registro es una respuesta determinante (no_registra=True),
    análoga a "placa sin información" del RUNT.
    """
    inicio = time.monotonic()
    nit_norm = _normalizar_nit(nit)
    if not _RE_NIT.match(nit_norm):
        raise BotRuesError(f"NIT inválido para RUES: {nit!r}")

    with _LOCK:
        sesion = _sesion()
        hits = _buscar_hits(sesion, nit_norm)

        if not hits:
            return {
                "nit": nit_norm,
                "no_registra": True,
                "estado": None,
                "mensaje": "NIT sin registro en el Registro Mercantil del RUES.",
                "duracion_s": round(time.monotonic() - inicio, 2),
            }

        # Prefiere la matrícula ACTIVA si hay varias (renovaciones/historial).
        hits_ordenados = sorted(
            hits,
            key=lambda h: 0 if _normalizar_estado(h.get("desc_matricula")) == "ACTIVA" else 1,
        )
        hit = hits_ordenados[0]

        detalle = {}
        expediente = str(hit.get("id") or "").strip()
        if expediente:
            data = _post(sesion, "/api/Expediente/DetalleRM", {"id": expediente})
            detalle = data.get("registros") or {}

        # Representante legal: enriquecimiento best-effort (su fallo NO tumba
        # la fuente — análogo a los paneles perezosos del RUNT). El endpoint
        # devuelve HTML plano (no JSON) con la representación y facultades.
        representantes: List[dict] = []
        codigo_camara = str(detalle.get("cod_camara") or hit.get("codigo_camara") or "").strip()
        matricula = str(detalle.get("matricula") or hit.get("matricula") or "").strip()
        if codigo_camara and matricula:
            try:
                try:
                    respuesta = sesion.post(
                        RUES_API_URL + "/api/ConsultFacultadesXCamYMatricula",
                        json={"dataBody": _cifrar({"codigo_camara": codigo_camara, "matricula": matricula})},
                        timeout=RUES_TIMEOUT_S,
                    )
                    texto_rep = respuesta.text if respuesta.status_code == 200 else ""
                except requests.RequestException:
                    texto_rep = ""
                representantes = _parsear_representantes(texto_rep)
            except BotRuesError:
                representantes = []

    estado = _normalizar_estado(detalle.get("estado") or hit.get("desc_matricula"))
    razon_social = " ".join(str(detalle.get("razon_social") or hit.get("razon_social") or "").split())
    dv = str(detalle.get("dv") or hit.get("dv") or "").strip()
    ciiu = {
        "principal": {
            "codigo": str(detalle.get("cod_ciiu_act_econ_pri") or "").strip(),
            "descripcion": " ".join(str(detalle.get("desc_ciiu_act_econ_pri") or "").split()),
        },
        "secundaria": {
            "codigo": str(detalle.get("cod_ciiu_act_econ_sec") or "").strip(),
            "descripcion": " ".join(str(detalle.get("desc_ciiu_act_econ_sec") or "").split()),
        },
        "terciaria": {
            "codigo": str(detalle.get("ciiu3") or "").strip(),
            "descripcion": " ".join(str(detalle.get("desc_ciiu3") or "").split()),
        },
    }
    for entrada in ciiu.values():
        if not entrada["codigo"] and not entrada["descripcion"]:
            entrada.clear()

    if not estado and not razon_social:
        # Ni estado ni razón social: el API no entregó un resultado usable.
        raise BotRuesSinResultado("RUES respondió sin estado ni razón social de la matrícula")

    nit_con_dv = f"{nit_norm}-{dv}" if dv else nit_norm
    mensaje = (
        f"Matrícula {estado or 'sin estado'} — {razon_social} (cámara {str(detalle.get('camara') or hit.get('camara') or '').strip()})."
        if estado or razon_social
        else "RUES respondió sin datos de la matrícula."
    )

    return {
        "nit": nit_norm,
        "nit_con_dv": nit_con_dv,
        "razon_social": razon_social,
        "estado": estado or None,
        "no_registra": False,
        "camara": " ".join(str(detalle.get("camara") or hit.get("camara") or "").split()),
        "codigo_camara": codigo_camara,
        "matricula": matricula.lstrip("0") or matricula,
        "fecha_matricula": _fecha_iso(detalle.get("fecha_matricula") or hit.get("fecha_matricula")),
        "fecha_renovacion": _fecha_iso(detalle.get("fecha_renovacion")),
        "ultimo_ano_renovado": str(detalle.get("ultimo_ano_renovado") or "").strip() or None,
        "fecha_cancelacion": _fecha_iso(detalle.get("fecha_cancelacion")),
        "tipo_sociedad": " ".join(str(detalle.get("tipo_sociedad") or "").split()),
        "organizacion_juridica": " ".join(str(detalle.get("organizacion_juridica") or "").split()),
        "categoria_matricula": " ".join(str(detalle.get("categoria_matricula") or hit.get("desc_categoria_matricula") or "").split()),
        "ciiu": ciiu,
        "municipio": " ".join(str(hit.get("desc_municipio") or "").split()),
        "departamento": " ".join(str(hit.get("desc_dpto") or "").split()),
        "sigla": " ".join(str(detalle.get("sigla") or hit.get("sigla") or "").split()),
        "representantes": representantes,
        "fecha_actualizacion": _fecha_iso(detalle.get("fecha_actualizacion")),
        "mensaje": mensaje[:300],
        "duracion_s": round(time.monotonic() - inicio, 2),
    }


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Uso: python Funciones/bot_rues.py <NIT sin dígito de verificación>")
        raise SystemExit(1)
    print(json.dumps(consultar_rues_sync(sys.argv[1]), ensure_ascii=False, indent=2, default=str))
