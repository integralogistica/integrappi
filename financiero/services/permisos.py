"""Predicados de permisos compartidos del Módulo Financiero.

Parte de app/dependencies.py + lógica dispersa en routers del módulo externo;
la visibilidad por módulo se porta con cada router (F1+).
"""
from financiero.models.usuario import RolUsuario, Usuario

# Roles financieros globales (ven todo en su dominio)
ROLES_GLOBALES_FIN = {
    RolUsuario.ADMIN,
    RolUsuario.TESORERIA,
    RolUsuario.CONTABILIDAD,
    RolUsuario.COMPRAS,
    RolUsuario.CONTROLLER,
    RolUsuario.GERENTE_FINANCIERO,
    RolUsuario.PRESIDENCIA,
}

# Roles que pueden crear/enviar requisiciones
ROLES_REQUISICION = {
    RolUsuario.REQUISICION,
    RolUsuario.COMPRAS,
}


def es_admin(usuario: Usuario) -> bool:
    return usuario.rol == RolUsuario.ADMIN or usuario.es_admin_adicional


def puede_ver_todos_los_modulos(usuario: Usuario) -> bool:
    """Visibilidad global (admin y roles financieros)."""
    return es_admin(usuario) or usuario.rol in ROLES_GLOBALES_FIN


def puede_ver_tarifa_viaje(usuario: Usuario) -> bool:
    """La tarifa A/B/C es confidencial (enmascaramiento del serializer)."""
    return (
        es_admin(usuario)
        or usuario.rol in {
            RolUsuario.CONTROLLER,
            RolUsuario.GERENTE_FINANCIERO,
            RolUsuario.PRESIDENCIA,
        }
        or usuario.es_autorizador_viajes
    )


# ── Permisos finos por usuario (secciones) ────────────────────────────────
# Puerta de la matriz que la demo configuraba por pantalla. Aquí:
#   1. Cada ROL tiene sus secciones por defecto (DEFAULT_SECCIONES_POR_ROL).
#   2. fin.usuarios.permisos guarda SOLO los overrides {seccion: true|false}.
#   3. El backend lo respeta en las lecturas de cada módulo (require_seccion).

SECCIONES_FIN = (
    "requisiciones", "traslados", "viajes", "facturas",
    "pagos", "servicios", "cajas", "administracion", "proveedores",
)

_TODAS = set(SECCIONES_FIN)
DEFAULT_SECCIONES_POR_ROL: dict[RolUsuario, set[str]] = {
    RolUsuario.ADMIN: _TODAS,
    # Tesorería: todo menos traslados y administración
    RolUsuario.TESORERIA: {"requisiciones", "viajes", "facturas", "pagos", "servicios", "cajas"},
    RolUsuario.CONTABILIDAD: {"facturas", "servicios", "cajas", "viajes", "pagos", "proveedores"},
    RolUsuario.COMPRAS: {"requisiciones", "facturas", "pagos", "viajes", "proveedores"},
    RolUsuario.CONTROLLER: {"requisiciones", "traslados", "viajes"},
    RolUsuario.GERENTE_FINANCIERO: {"requisiciones", "traslados", "viajes"},
    RolUsuario.PRESIDENCIA: {"traslados", "viajes"},
    RolUsuario.REQUISICION: {"requisiciones", "viajes"},
    RolUsuario.JEFE_AREA: {"requisiciones", "traslados", "viajes"},
    # Calidad solo audita servicios públicos (como en la app original)
    RolUsuario.CALIDAD: {"servicios"},
    RolUsuario.AUTORIZADOR_VIAJES: {"viajes"},
    RolUsuario.CONSULTA: set(),
}


def secciones_efectivas(usuario: Usuario) -> set[str]:
    """Secciones que el usuario puede ver: override personal sobre el rol."""
    if es_admin(usuario):
        return _TODAS  # el admin siempre ve todo (los permisos no lo recortan)
    base = set(DEFAULT_SECCIONES_POR_ROL.get(usuario.rol, set()))
    overrides = usuario.permisos or {}
    for seccion, permitido in overrides.items():
        if seccion in SECCIONES_FIN:
            if permitido:
                base.add(seccion)
            else:
                base.discard(seccion)
    return base


def puede_ver_seccion(usuario: Usuario, seccion: str) -> bool:
    return seccion in secciones_efectivas(usuario)
