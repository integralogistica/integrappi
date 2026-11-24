"""Auditoría inmutable del módulo.

# puerto LITERAL de app/services/audit_service.py del módulo externo.
"""
from typing import Any, Dict, Optional

from sqlalchemy.orm import Session

from financiero.models.usuario import Usuario
from financiero.models.auditoria import RegistroAuditoria


class AuditService:
    @staticmethod
    def registrar(
        db: Session,
        modulo: str,
        accion: str,
        entidad_id: Optional[str] = None,
        usuario: Optional[Usuario] = None,
        ip_cliente: Optional[str] = None,
        detalles: Optional[Dict[str, Any]] = None
    ):
        """Registra un evento de auditoría inmutable en la base de datos."""
        evento = RegistroAuditoria(
            usuario_id=usuario.id if usuario else None,
            usuario_username=usuario.username if usuario else "SISTEMA",
            usuario_rol=usuario.rol.value if usuario else "SISTEMA",
            modulo=modulo,
            accion=accion,
            entidad_id=str(entidad_id) if entidad_id else None,
            ip_cliente=ip_cliente,
            detalles=detalles or {}
        )
        db.add(evento)
        db.commit()
