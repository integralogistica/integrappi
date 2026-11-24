"""Máquinas de estado: transiciones legales e ilegales por módulo.

# puerto ampliado de tests/test_flujos.py del módulo externo.
"""
import pytest
from fastapi import HTTPException

from financiero.models.usuario import RolUsuario, Usuario
from financiero.models.requisicion import EstadoRequisicion, Requisicion
from financiero.models.traslado import EstadoTraslado, Traslado
from financiero.models.viaje import EstadoViaje, Viaje
from financiero.services.state_machines import StateMachineService, TOPE_APROBACION_CONTROLLER_COP
from decimal import Decimal


def usuario(rol, **kwargs):
    return Usuario(username=f"u_{rol.value}", nombre=f"U {rol.value}",
                   correo=f"{rol.value}@integralogistica.com", rol=rol, activo=True, **kwargs)


def requisicion(**kwargs):
    base = dict(
        solicitante_id="00000000-0000-0000-0000-000000000001",
        area="Tecnología", estado=EstadoRequisicion.PENDIENTE_JEFE,
        total=Decimal("1000"),
    )
    base.update(kwargs)
    return Requisicion(**base)


def traslado(**kwargs):
    base = dict(
        solicitante_id="00000000-0000-0000-0000-000000000001",
        estado=EstadoTraslado.PENDIENTE_PRESIDENCIA,
    )
    base.update(kwargs)
    return Traslado(**base)


def viaje(**kwargs):
    base = dict(
        solicitante_id="00000000-0000-0000-0000-000000000001",
        estado=EstadoViaje.SOLICITADO,
    )
    base.update(kwargs)
    return Viaje(**base)


SID = "00000000-0000-0000-0000-000000000001"
OTRO = "00000000-0000-0000-0000-000000000002"


# ── Requisiciones ───────────────────────────────────────────────────────

class TestRequisiciones:
    def test_jefe_del_area_puede_aprobar(self):
        req = requisicion()
        jefe = usuario(RolUsuario.JEFE_AREA, area="Tecnología")
        StateMachineService.validar_transicion_requisicion(
            req, EstadoRequisicion.PENDIENTE_COMPRAS, jefe)

    def test_jefe_de_otra_area_no_puede(self):
        req = requisicion()
        jefe = usuario(RolUsuario.JEFE_AREA, area="Compras")
        with pytest.raises(HTTPException) as e:
            StateMachineService.validar_transicion_requisicion(
                req, EstadoRequisicion.PENDIENTE_COMPRAS, jefe)
        assert e.value.status_code == 403

    def test_compras_pasa_a_financiera(self):
        req = requisicion(estado=EstadoRequisicion.PENDIENTE_COMPRAS)
        StateMachineService.validar_transicion_requisicion(
            req, EstadoRequisicion.PENDIENTE_FINANCIERA, usuario(RolUsuario.COMPRAS))

    def test_controller_aprueba_hasta_el_tope(self):
        req = requisicion(estado=EstadoRequisicion.PENDIENTE_FINANCIERA,
                          total=TOPE_APROBACION_CONTROLLER_COP)
        StateMachineService.validar_transicion_requisicion(
            req, EstadoRequisicion.APROBADA, usuario(RolUsuario.CONTROLLER))

    def test_controller_NO_aprueba_sobre_el_tope(self):
        req = requisicion(estado=EstadoRequisicion.PENDIENTE_FINANCIERA,
                          total=TOPE_APROBACION_CONTROLLER_COP + 1)
        with pytest.raises(HTTPException) as e:
            StateMachineService.validar_transicion_requisicion(
                req, EstadoRequisicion.APROBADA, usuario(RolUsuario.CONTROLLER))
        assert e.value.status_code == 403

    def test_gerente_aprueba_sobre_el_tope(self):
        req = requisicion(estado=EstadoRequisicion.PENDIENTE_FINANCIERA,
                          total=Decimal("999999999"))
        StateMachineService.validar_transicion_requisicion(
            req, EstadoRequisicion.APROBADA, usuario(RolUsuario.GERENTE_FINANCIERO))

    def test_solo_solicitante_confirma_recepcion(self):
        import uuid
        sid = uuid.UUID(SID)
        req = requisicion(estado=EstadoRequisicion.EN_ORDEN_COMPRA, solicitante_id=sid)
        StateMachineService.validar_transicion_requisicion(
            req, EstadoRequisicion.RECIBIDO_TOTAL,
            usuario(RolUsuario.REQUISICION, id=sid))
        with pytest.raises(HTTPException):
            StateMachineService.validar_transicion_requisicion(
                requisicion(estado=EstadoRequisicion.EN_ORDEN_COMPRA,
                            solicitante_id=uuid.UUID(OTRO)),
                EstadoRequisicion.RECIBIDO_TOTAL,
                usuario(RolUsuario.REQUISICION, id=sid))

    def test_solicitante_anula_antes_de_financiera(self):
        import uuid
        StateMachineService.validar_transicion_requisicion(
            requisicion(estado=EstadoRequisicion.PENDIENTE_JEFE, solicitante_id=uuid.UUID(SID)),
            EstadoRequisicion.ANULADA, usuario(RolUsuario.REQUISICION, id=uuid.UUID(SID)))

    def test_solicitante_NO_anula_con_orden_de_compra(self):
        with pytest.raises(HTTPException) as e:
            StateMachineService.validar_transicion_requisicion(
                requisicion(estado=EstadoRequisicion.EN_ORDEN_COMPRA),
                EstadoRequisicion.ANULADA, usuario(RolUsuario.REQUISICION))
        assert e.value.status_code == 403

    def test_admin_anula_casi_todo(self):
        StateMachineService.validar_transicion_requisicion(
            requisicion(estado=EstadoRequisicion.EN_ORDEN_COMPRA),
            EstadoRequisicion.ANULADA, usuario(RolUsuario.ADMIN))

    def test_transicion_inexistente_rechazada(self):
        with pytest.raises(HTTPException) as e:
            StateMachineService.validar_transicion_requisicion(
                requisicion(estado=EstadoRequisicion.BORRADOR),
                EstadoRequisicion.APROBADA, usuario(RolUsuario.ADMIN))
        assert e.value.status_code == 400

    def test_admin_adicional_pasa_como_admin(self):
        casilla = usuario(RolUsuario.TESORERIA, es_admin_adicional=True)
        StateMachineService.validar_transicion_requisicion(
            requisicion(estado=EstadoRequisicion.PENDIENTE_COMPRAS),
            EstadoRequisicion.PENDIENTE_FINANCIERA, casilla)


# ── Traslados ───────────────────────────────────────────────────────────

class TestTraslados:
    def test_presidencia_aprueba(self):
        StateMachineService.validar_transicion_traslado(
            traslado(), EstadoTraslado.APROBADO, usuario(RolUsuario.PRESIDENCIA))

    def test_controller_NO_decide_traslados(self):
        with pytest.raises(HTTPException) as e:
            StateMachineService.validar_transicion_traslado(
                traslado(), EstadoTraslado.APROBADO, usuario(RolUsuario.CONTROLLER))
        assert e.value.status_code == 403

    def test_creador_anula_pendiente(self):
        import uuid
        StateMachineService.validar_transicion_traslado(
            traslado(solicitante_id=uuid.UUID(SID)), EstadoTraslado.ANULADO,
            usuario(RolUsuario.JEFE_AREA, id=uuid.UUID(SID)))

    def test_ya_decidido_no_cambia(self):
        with pytest.raises(HTTPException) as e:
            StateMachineService.validar_transicion_traslado(
                traslado(estado=EstadoTraslado.APROBADO),
                EstadoTraslado.RECHAZADO, usuario(RolUsuario.PRESIDENCIA))
        assert e.value.status_code == 400


# ── Viajes ──────────────────────────────────────────────────────────────

class TestViajes:
    def test_autorizadores_oficiales(self):
        for rol in (RolUsuario.CONTROLLER, RolUsuario.GERENTE_FINANCIERO, RolUsuario.PRESIDENCIA):
            StateMachineService.validar_transicion_viaje(
                viaje(), EstadoViaje.AUTORIZADO, usuario(rol))

    def test_casilla_autorizador(self):
        StateMachineService.validar_transicion_viaje(
            viaje(), EstadoViaje.AUTORIZADO,
            usuario(RolUsuario.TESORERIA, es_autorizador_viajes=True))

    def test_cualquiera_no_autoriza(self):
        with pytest.raises(HTTPException) as e:
            StateMachineService.validar_transicion_viaje(
                viaje(), EstadoViaje.AUTORIZADO, usuario(RolUsuario.REQUISICION))
        assert e.value.status_code == 403

    def test_flujo_completo_pasos(self):
        v = viaje()
        comprador = usuario(RolUsuario.COMPRAS)
        tesoro = usuario(RolUsuario.TESORERIA, es_aprobador_pagos=True)
        # AUTORIZADO → PRESUPUESTO_ASIGNADO (Controller/Gerente)
        v.estado = EstadoViaje.AUTORIZADO
        StateMachineService.validar_transicion_viaje(
            v, EstadoViaje.PRESUPUESTO_ASIGNADO, usuario(RolUsuario.CONTROLLER))
        # → TIQUETES (Compras)
        v.estado = EstadoViaje.PRESUPUESTO_ASIGNADO
        StateMachineService.validar_transicion_viaje(v, EstadoViaje.TIQUETES_GESTIONADOS, comprador)
        # → ANTICIPO (Tesorería/aprobador pagos)
        v.estado = EstadoViaje.TIQUETES_GESTIONADOS
        StateMachineService.validar_transicion_viaje(v, EstadoViaje.ANTICIPO_PAGADO, tesoro)
        # → LEGALIZADO (solicitante)
        v.estado = EstadoViaje.ANTICIPO_PAGADO
        import uuid
        v.solicitante_id = uuid.UUID(SID)
        StateMachineService.validar_transicion_viaje(
            v, EstadoViaje.LEGALIZADO, usuario(RolUsuario.REQUISICION, id=uuid.UUID(SID)))
        # → CERRADO (Contabilidad/Controller/Tesorería)
        v.estado = EstadoViaje.LEGALIZADO
        StateMachineService.validar_transicion_viaje(v, EstadoViaje.CERRADO, usuario(RolUsuario.CONTABILIDAD))

    def test_compras_no_paga_anticipo(self):
        with pytest.raises(HTTPException) as e:
            StateMachineService.validar_transicion_viaje(
                viaje(estado=EstadoViaje.TIQUETES_GESTIONADOS),
                EstadoViaje.ANTICIPO_PAGADO, usuario(RolUsuario.COMPRAS))
        assert e.value.status_code == 403

    def test_no_se_anula_cerrado(self):
        with pytest.raises(HTTPException) as e:
            StateMachineService.validar_transicion_viaje(
                viaje(estado=EstadoViaje.CERRADO), EstadoViaje.ANULADO, usuario(RolUsuario.ADMIN))
        assert e.value.status_code == 400
