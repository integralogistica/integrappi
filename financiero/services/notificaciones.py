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


def req_entregada(background: BackgroundTasks, db: Session, req):
    """Compras marcó la entrega: el solicitante debe confirmar la recepción y calificar al proveedor."""
    avisar(background, db, [_correo_de(db, req.solicitante_id)] if req.solicitante_id else [],
           f"Tu compra fue entregada · {req.consecutivo}",
           [f"Compras marcó como entregada la <b>OC {req.oc_numero}</b> ({req.proveedor_sugerido or req.consecutivo}).",
            "Confirma la recepción y califica al proveedor (9 criterios) en el Módulo Financiero de IntegrApp."])


# ── Proveedores nuevos (terceros de Compras) ──────────────────────────────

def tercero_pendiente_compras(background: BackgroundTasks, db: Session, tercero):
    avisar(background, db, _correos_por_rol(db, RolUsuario.COMPRAS),
           f"Proveedor nuevo pendiente: {tercero.razon_social}",
           [f"<b>{tercero.razon_social}</b> (NIT {tercero.nit}) quedó pendiente de aprobación en «Prov. nuevos».",
            f"Registrado por {tercero.creado_por_nombre or tercero.creado_por or 'un solicitante'}"
            + (f" desde la requisición <b>{tercero.consecutivo_req}</b>." if tercero.consecutivo_req else "."),
            "Adjuntó RUT, certificación bancaria y cámara de comercio."])


def tercero_aprobado(background: BackgroundTasks, db: Session, tercero):
    avisar(background, db, _correos_por_rol(db, RolUsuario.CONTABILIDAD),
           f"Proveedor nuevo aprobado: crear en software contable — {tercero.razon_social}",
           [f"Compras aprobó a <b>{tercero.razon_social}</b> (NIT {tercero.nit}).",
            f"Cuenta: {tercero.banco or '—'} · {tercero.tipo_cuenta or '—'} · Nº {tercero.no_cuenta or '—'}.",
            "Créalo en el software contable (Vulcano) y márcalo como cargado en la bandeja «Prov. nuevos» del Módulo Financiero."])


# ── Traslados / Viajes ────────────────────────────────────────────────────

def traslado_pendiente(background: BackgroundTasks, db: Session, tr):
    avisar(background, db, _correos_por_rol(db, RolUsuario.PRESIDENCIA),
           f"Traslado {tr.consecutivo} espera aprobación",
           [f"<b>{tr.consecutivo}</b> de {tr.solicitante_nombre} (área {tr.area}).",
            f"Total: <b>${tr.total:,.0f}</b> COP."])


def traslado_aprobado(background: BackgroundTasks, db: Session, tr):
    """Presidencia remite lo aprobado al Controller y al Gerente financiero."""
    avisar(background, db, _correos_por_rol(db, RolUsuario.CONTROLLER, RolUsuario.GERENTE_FINANCIERO),
           f"Traslado {tr.consecutivo} aprobado",
           [f"Presidencia aprobó el traslado <b>{tr.consecutivo}</b> de {tr.solicitante_nombre} (área {tr.area}).",
            f"Total: <b>${tr.total:,.0f}</b> COP."])


def traslado_rechazado(background: BackgroundTasks, db: Session, tr, motivo: str):
    avisar(background, db, [_correo_de(db, tr.solicitante_id)] if tr.solicitante_id else [],
           f"Traslado {tr.consecutivo} rechazado",
           [f"<b>{tr.consecutivo}</b> fue rechazado por Presidencia.",
            f"Motivo: {motivo or '(sin motivo)'}. Corrígelo y reenvíalo en el Módulo Financiero."])


def viaje_pendiente(background: BackgroundTasks, db: Session, vj):
    dest = [_correo_de(db, vj.autorizador_id)] if vj.autorizador_id else []
    avisar(background, db, dest, f"Viaje {vj.consecutivo} espera tu autorización",
           [f"<b>{vj.consecutivo}</b> de {vj.solicitante_nombre}: {vj.origen} → {vj.destino}.",
            f"Objetivo: {vj.objetivo}."])


def viaje_autorizado(background: BackgroundTasks, db: Session, vj, por: str):
    """Al autorizar, el viaje pasa en paralelo a Compras (tiquetes) y al Controller (presupuesto)."""
    avisar(background, db, _correos_por_rol(db, RolUsuario.COMPRAS, RolUsuario.CONTROLLER),
           f"Viaje {vj.consecutivo} autorizado",
           [f"<b>{vj.consecutivo}</b> de {vj.solicitante_nombre} ({vj.origen} → {vj.destino}) fue autorizado por {por}.",
            "Controller: generar el presupuesto de viáticos. Compras: gestión de tiquetes."])


def viaje_rechazado(background: BackgroundTasks, db: Session, vj, motivo: str):
    avisar(background, db, [_correo_de(db, vj.solicitante_id)] if vj.solicitante_id else [],
           f"Viaje {vj.consecutivo} rechazado",
           [f"<b>{vj.consecutivo}</b> fue rechazado.",
            f"Motivo: {motivo or '(sin motivo)'}. Corrígelo y reenvíalo en el Módulo Financiero."])


def viaje_anticipo_pagado(background: BackgroundTasks, db: Session, vj):
    avisar(background, db, [_correo_de(db, vj.solicitante_id)] if vj.solicitante_id else [],
           f"Anticipo de tu viaje {vj.consecutivo} gestionado",
           [f"El anticipo del viaje <b>{vj.consecutivo}</b> ya fue procesado.",
            "Cuando regreses, recuerda legalizar el viaje en el Módulo Financiero."])


# ── Cajas menores (circuito FORM-FIN-007) ──────────────────────────────────

def _caja_titulo(c) -> str:
    return c.consecutivo or f"{c.sede} {c.periodo}"


def _correo_jefe_por_nombre(db: Session, jefe_nombre: str | None) -> Optional[str]:
    """El jefe inmediato de la sede se declara por NOMBRE en el maestro: se
    mapea al usuario de fin.usuarios cuyo nombre lo contiene."""
    if not jefe_nombre:
        return None
    primos = jefe_nombre.strip().upper().split()
    for u in db.scalars(select(Usuario).where(Usuario.activo.is_(True))).all():
        n = (u.nombre or "").upper()
        if n == jefe_nombre.upper() or (primos and all(p in n for p in primos[:2])):
            return u.correo
    return None


def caja_radicada(background: BackgroundTasks, db: Session, c, jefe_nombre: str | None):
    ident = _caja_titulo(c)
    dest = [x for x in [_correo_jefe_por_nombre(db, jefe_nombre)] if x]
    dest += _correos_por_rol(db, RolUsuario.CONTABILIDAD)
    avisar(background, db, dest, f"Nueva caja menor radicada: {ident}",
           [f"<b>{ident}</b> de {c.responsable} (sede {c.sede}).",
            f"Total a reembolsar: <b>${float(c.total_reembolso or 0):,.0f}</b> COP · {len(c.movimientos or [])} folio(s).",
            "Pendiente de la aprobación del jefe inmediato."])


def caja_aprobada_jefe(background: BackgroundTasks, db: Session, c):
    avisar(background, db, _correos_por_rol(db, RolUsuario.CONTROLLER, RolUsuario.GERENTE_FINANCIERO),
           f"Caja menor {_caja_titulo(c)} aprobada por Jefe",
           [f"<b>{_caja_titulo(c)}</b> ({c.sede}) aprobada por el jefe inmediato.",
            f"Total: <b>${float(c.total_reembolso or 0):,.0f}</b> COP. Pendiente de la aprobación del Controller."])


def caja_aprobada_controller(background: BackgroundTasks, db: Session, c):
    dest = _correos_por_rol(db, RolUsuario.CONTABILIDAD, RolUsuario.TESORERIA)
    avisar(background, db, dest, f"Caja menor {_caja_titulo(c)} pasa a legalización",
           [f"<b>{_caja_titulo(c)}</b> ({c.sede}) aprobada por Controller.",
            "Contabilidad: causar comprobante FC. Tesorería: cruce de facturas electrónicas (cuenta 110510)."])


def caja_contabilizada(background: BackgroundTasks, db: Session, c):
    avisar(background, db, _correos_por_rol(db, RolUsuario.TESORERIA),
           f"Caja menor {_caja_titulo(c)} contabilizada (FC {(c.datos or {}).get('no_comprobante_fc')})",
           [f"<b>{_caja_titulo(c)}</b> ({c.sede}) ya tiene comprobante FC.",
            "Tesorería: proceder con el cruce de facturas y cuentas de cobro."])


def caja_aviso_responsable(background: BackgroundTasks, db: Session, c, etapa: str):
    avisar(background, db, [u.correo for u in db.scalars(
        select(Usuario).where(Usuario.rol.in_([RolUsuario.CONTABILIDAD]), Usuario.activo.is_(True))).all() if u.correo],
        f"Caja menor {_caja_titulo(c)}: {etapa}",
        [f"<b>{_caja_titulo(c)}</b> ({c.sede}) — {etapa}."])
