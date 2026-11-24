"""Notificaciones por correo de las transiciones del Módulo Financiero.

Fire-and-forget (BackgroundTasks): si Resend falla o el dominio no está en la
whitelist, el flujo NUNCA se cae — solo queda en log (mismo criterio que los
WhatsApp de Otros Costos).
"""
import logging
from typing import Optional

from fastapi import BackgroundTasks
from sqlalchemy import select
from sqlalchemy.orm import Session

from financiero.models.usuario import RolUsuario, Usuario
from financiero.services import correo as correo_srv

logger = logging.getLogger("financiero.notificaciones")


def _despachar(destinatarios: list[str], asunto: str, lineas: list[str], usuario_rate: Optional[str] = None):
    """Se ejecuta DESPUÉS de responder la petición (background)."""
    try:
        if not destinatarios:
            return
        html = (
            "<div style='font-family:Segoe UI,Arial,sans-serif;max-width:560px'>"
            + "".join(f"<p style='margin:4px 0'>{l}</p>" for l in lineas)
            + "<p style='color:#6b7280;font-size:12px'>Mensaje automático del Módulo Financiero de IntegrApp.</p></div>"
        )
        correo_srv.enviar_correo(destinatarios, asunto, html, usuario_rate=usuario_rate)
    except Exception as exc:  # inclusive 403 de whitelist: no bloquea el flujo
        logger.warning("[FIN] Notificación no enviada a %s: %s", destinatarios, exc)


def _correos_por_rol(db: Session, *roles: RolUsuario) -> list[str]:
    if not roles:
        return []
    usuarios = db.scalars(
        select(Usuario).where(Usuario.rol.in_(roles), Usuario.activo.is_(True))
    ).all()
    return [u.correo for u in usuarios if u.correo]


def _correo_de(db: Session, usuario_id) -> Optional[str]:
    u = db.get(Usuario, usuario_id)
    return u.correo if u and u.correo else None


def avisar(background: BackgroundTasks, db: Session, destinatarios: list[str], asunto: str, lineas: list[str]):
    if not destinatarios:
        return
    background.add_task(_despachar, list(set(destinatarios)), asunto, lineas)


# ── Requisiciones ─────────────────────────────────────────────────────────

def req_pendiente_jefe(background: BackgroundTasks, db: Session, jefe_id, req):
    dest = [_correo_de(db, jefe_id)] if jefe_id else []
    avisar(background, db, dest, f"Requisición {req.consecutivo} espera tu visto bueno",
           [f"<b>{req.consecutivo}</b> de {req.solicitante_nombre} (área {req.area}).",
            f"Total: <b>${req.total:,.0f}</b> COP. Revísala en el Módulo Financiero de IntegrApp."])


def req_pendiente_compras(background: BackgroundTasks, db: Session, req):
    avisar(background, db, _correos_por_rol(db, RolUsuario.COMPRAS),
           f"Requisición {req.consecutivo} lista para cotizar",
           [f"<b>{req.consecutivo}</b> de {req.solicitante_nombre} (área {req.area}).",
            f"Total: <b>${req.total:,.0f}</b> COP."])


def req_pendiente_financiera(background: BackgroundTasks, db: Session, req, tope):
    rol = RolUsuario.GERENTE_FINANCIERO if req.total > tope else RolUsuario.CONTROLLER
    avisar(background, db, _correos_por_rol(db, rol),
           f"Requisición {req.consecutivo} espera aprobación financiera",
           [f"<b>{req.consecutivo}</b> de {req.solicitante_nombre} — total <b>${req.total:,.0f}</b> COP.",
            ("Supera el tope del Controller: la aprueba el Gerente financiero." if req.total > tope
             else "Dentro del tope del Controller.")])


def req_rechazada(background: BackgroundTasks, db: Session, req, motivo: str, por: str):
    avisar(background, db, [_correo_de(db, req.solicitante_id)] if req.solicitante_id else [],
           f"Requisición {req.consecutivo} rechazada",
           [f"<b>{req.consecutivo}</b> fue rechazada por {por}.",
            f"Motivo: {motivo or '(sin motivo)'}. Corrígela y reenvíala en el Módulo Financiero."])


def req_aprobada(background: BackgroundTasks, db: Session, req):
    dest = [_correo_de(db, req.solicitante_id)] if req.solicitante_id else []
    dest += _correos_por_rol(db, RolUsuario.COMPRAS)
    avisar(background, db, dest, f"Requisición {req.consecutivo} aprobada · OC {req.oc_numero}",
           [f"<b>{req.consecutivo}</b> quedó aprobada y generó la orden de compra <b>{req.oc_numero}</b>.",
            f"Total: <b>${req.total:,.0f}</b> COP."])


# ── Traslados / Viajes ────────────────────────────────────────────────────

def traslado_pendiente(background: BackgroundTasks, db: Session, tr):
    avisar(background, db, _correos_por_rol(db, RolUsuario.PRESIDENCIA),
           f"Traslado {tr.consecutivo} espera aprobación",
           [f"<b>{tr.consecutivo}</b> de {tr.solicitante_nombre} (área {tr.area}).",
            f"Total: <b>${tr.total:,.0f}</b> COP."])


def viaje_pendiente(background: BackgroundTasks, db: Session, vj):
    dest = [_correo_de(db, vj.autorizador_id)] if vj.autorizador_id else []
    avisar(background, db, dest, f"Viaje {vj.consecutivo} espera tu autorización",
           [f"<b>{vj.consecutivo}</b> de {vj.solicitante_nombre}: {vj.origen} → {vj.destino}.",
            f"Objetivo: {vj.objetivo}."])
