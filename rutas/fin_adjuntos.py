"""Adjuntos del Módulo Financiero en Google Cloud Storage.

Adaptación de app/routers/adjuntos.py del módulo externo: allá los archivos
iban a disco local (STORAGE_DIR — se perdería en cada deploy de Render);
aquí van al bucket PRIVADO `integrapp-privado` (mismo patrón que
rutas/otros_costos.py y Funciones/storage_seguridad.py):
  - En fin.adjuntos solo vive la RUTA del blob + sha256 + metadatos.
  - La descarga es una URL firmada v4 temporal (nunca pública).
"""
import hashlib
import logging
import re
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, status
from sqlalchemy.orm import Session

from financiero.constantes import (
    FIN_ADJUNTO_MAX_MB,
    FIN_BUCKET,
    FIN_CARPETA_GCS,
    FIN_URL_FIRMADA_MIN,
)
from financiero.database import get_db_fin
from financiero.dependencies import get_usuario_fin, require_not_consulta
from financiero.models.adjunto import Adjunto
from financiero.models.usuario import RolUsuario, Usuario
from financiero.services.permisos import es_admin

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/fin/adjuntos", tags=["Financiero · Adjuntos"])

MAX_TAMANO = FIN_ADJUNTO_MAX_MB * 1024 * 1024
TIPOS_PERMITIDOS = {"application/pdf", "image/jpeg", "image/jpg", "image/png"}

# Tipado opcional del documento (proveedores nuevos, cotizaciones de requisición,
# circuito de cajas menores y órdenes de compra de facturas DIAN)
TIPOS_DOCUMENTO = {"", "rut", "cert_bancaria", "camara_comercio", "cotizacion",
                   "comprobante_fc", "cruce", "egreso_auxiliar", "reembolso", "soporte_caja", "orden_compra"}

# Roles que además ven adjuntos de soportes (modulo_origen empieza por "soporte")
ROLES_SOPORTES = {RolUsuario.CALIDAD, RolUsuario.CONSULTA, RolUsuario.CONTABILIDAD, RolUsuario.TESORERIA}

_cliente_storage = None


def _obtener_cliente_storage():
    """Cliente GCS perezoso (mismo patrón que otros_costos.py)."""
    global _cliente_storage
    if _cliente_storage is None:
        from google.cloud import storage
        _cliente_storage = storage.Client()
    return _cliente_storage


def _sanitizar_nombre(nombre: str) -> str:
    nombre = re.sub(r"[^\w.\- ]", "_", nombre or "archivo")
    return nombre[:120] or "archivo"


def _ruta_blob(modulo: str, referencia_id: str) -> str:
    hoy = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"{FIN_CARPETA_GCS}/{modulo}/{referencia_id}/{hoy}_{uuid.uuid4().hex[:8]}"


def _url_firmada(ruta: str) -> str:
    bucket = _obtener_cliente_storage().bucket(FIN_BUCKET)
    return bucket.blob(ruta).generate_signed_url(
        version="v4",
        expiration=FIN_URL_FIRMADA_MIN * 60,
        method="GET",
    )


def _puede_ver(adj: Adjunto, u: Usuario) -> bool:
    """# puerto de app/routers/adjuntos.py::_puede_ver."""
    if es_admin(u) or u.rol in {
        RolUsuario.TESORERIA, RolUsuario.CONTABILIDAD, RolUsuario.COMPRAS,
        RolUsuario.CONTROLLER, RolUsuario.GERENTE_FINANCIERO, RolUsuario.PRESIDENCIA,
    }:
        return True
    if adj.subido_por_id and str(adj.subido_por_id) == str(u.id):
        return True
    if (adj.modulo_origen or "").startswith("soporte") and u.rol in ROLES_SOPORTES:
        return True
    return False


def _serializar(adj: Adjunto) -> dict:
    return {
        "id": str(adj.id),
        "nombre_archivo": adj.nombre_archivo,
        "tipo_mime": adj.tipo_mime,
        "tamano_bytes": adj.tamano_bytes,
        "modulo_origen": adj.modulo_origen,
        "referencia_id": adj.referencia_id,
        "tipo_documento": getattr(adj, "tipo_documento", None),
        "subido_por_id": str(adj.subido_por_id) if adj.subido_por_id else None,
        "created_at": adj.created_at.isoformat() if adj.created_at else None,
    }


@router.post("/subir")
def subir_adjunto(
    archivo: UploadFile = File(...),
    modulo_origen: str = Form(...),
    referencia_id: str = Form(...),
    tipo_documento: str = Form(""),
    db: Session = Depends(get_db_fin),
    usuario: Usuario = Depends(require_not_consulta),
):
    if (modulo_origen or "").strip() == "" or (referencia_id or "").strip() == "":
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "modulo_origen y referencia_id son obligatorios.")
    if (tipo_documento or "").strip().lower() not in TIPOS_DOCUMENTO:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY,
                            f"tipo_documento no válido. Permitidos: {', '.join(sorted(t for t in TIPOS_DOCUMENTO if t))}.")
    if (archivo.content_type or "") not in TIPOS_PERMITIDOS:
        raise HTTPException(status.HTTP_415_UNSUPPORTED_MEDIA_TYPE, f"Tipo no permitido ({archivo.content_type}): PDF, JPG o PNG.")

    # Lectura por bloques con tope duro y hash en el mismo paso
    sha = hashlib.sha256()
    pedazos: list[bytes] = []
    leido = 0
    while True:
        bloque = archivo.file.read(1024 * 1024)
        if not bloque:
            break
        leido += len(bloque)
        if leido > MAX_TAMANO:
            raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, f"El archivo supera {FIN_ADJUNTO_MAX_MB} MB.")
        sha.update(bloque)
        pedazos.append(bloque)
    contenido = b"".join(pedazos)
    if not contenido:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Archivo vacío.")

    adj = subir_bytes_gcs(
        contenido=contenido,
        nombre=archivo.filename or "archivo",
        tipo_mime=archivo.content_type or "application/octet-stream",
        modulo_origen=modulo_origen.strip(),
        referencia_id=referencia_id.strip(),
        db=db,
        usuario=usuario,
        tipo_documento=tipo_documento.strip().lower() or None,
    )
    return _serializar(adj)


def subir_bytes_gcs(contenido: bytes, nombre: str, tipo_mime: str, modulo_origen: str,
                    referencia_id: str, db: Session, usuario: Usuario, tipo_documento: str = None) -> Adjunto:
    """Sube bytes ya en memoria a GCS y registra el adjunto. La usan el
    endpoint /subir y la carga de ZIPs DIAN (PDFs que venían dentro)."""
    if not contenido:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, "Archivo vacío.")
    if len(contenido) > MAX_TAMANO:
        raise HTTPException(status.HTTP_413_REQUEST_ENTITY_TOO_LARGE, f"El archivo supera {FIN_ADJUNTO_MAX_MB} MB.")
    sha = hashlib.sha256(contenido).hexdigest()
    nombre = _sanitizar_nombre(nombre)
    ruta = _ruta_blob(modulo_origen, referencia_id)
    try:
        bucket = _obtener_cliente_storage().bucket(FIN_BUCKET)
        bucket.blob(ruta).upload_from_string(contenido, content_type=tipo_mime)
    except Exception as exc:
        logger.error("[FIN] Error subiendo adjunto a GCS: %s", exc)
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, "No se pudo subir el archivo al almacenamiento.")
    adj = Adjunto(
        nombre_archivo=nombre,
        tipo_mime=tipo_mime,
        tamano_bytes=len(contenido),
        hash_sha256=sha,
        ruta_almacenamiento=ruta,
        modulo_origen=modulo_origen[:50],
        referencia_id=referencia_id[:100],
        tipo_documento=(tipo_documento or None),
        subido_por_id=usuario.id,
        created_at=datetime.now(timezone.utc),
    )
    db.add(adj)
    db.flush()
    return adj


@router.get("")
def listar_adjuntos(
    modulo_origen: str = "",
    referencia_id: str = "",
    db: Session = Depends(get_db_fin),
    usuario: Usuario = Depends(get_usuario_fin),
):
    q = db.query(Adjunto)
    if modulo_origen:
        q = q.filter(Adjunto.modulo_origen == modulo_origen)
    if referencia_id:
        q = q.filter(Adjunto.referencia_id == referencia_id)
    return [_serializar(a) for a in q.order_by(Adjunto.created_at.desc()).all() if _puede_ver(a, usuario)]


@router.get("/{adjunto_id}/descargar")
def descargar_adjunto(adjunto_id: uuid.UUID, db: Session = Depends(get_db_fin), usuario: Usuario = Depends(get_usuario_fin)):
    """Devuelve la URL firmada en JSON: el front la abre con window.open
    (un <a href> no puede llevar el Authorization Bearer)."""
    adj = db.get(Adjunto, adjunto_id)
    if not adj or not _puede_ver(adj, usuario):
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Adjunto no encontrado.")
    try:
        return {"url": _url_firmada(adj.ruta_almacenamiento), "expira_min": FIN_URL_FIRMADA_MIN}
    except Exception as exc:
        logger.error("[FIN] Error firmando URL de adjunto: %s", exc)
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, "No se pudo generar el enlace de descarga.")


@router.delete("/{adjunto_id}")
def eliminar_adjunto(adjunto_id: uuid.UUID, db: Session = Depends(get_db_fin), usuario: Usuario = Depends(get_usuario_fin)):
    adj = db.get(Adjunto, adjunto_id)
    if not adj:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Adjunto no encontrado.")
    puede_borrar = (
        es_admin(usuario)
        or (adj.subido_por_id and str(adj.subido_por_id) == str(usuario.id))
        or ((adj.modulo_origen or "").startswith("soporte") and usuario.rol in {RolUsuario.TESORERIA, RolUsuario.CONTABILIDAD})
    )
    if not puede_borrar:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "No puedes eliminar este adjunto.")
    try:  # best-effort: el blob huérfano no rompe nada
        _obtener_cliente_storage().bucket(FIN_BUCKET).blob(adj.ruta_almacenamiento).delete()
    except Exception as exc:
        logger.warning("[FIN] Blob no se pudo borrar de GCS (%s): %s", adj.ruta_almacenamiento, exc)
    db.delete(adj)
    db.commit()
    return {"mensaje": "Adjunto eliminado."}
