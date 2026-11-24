"""Máquinas de estado de requisiciones, traslados y viajes.

# puerto LITERAL de app/services/state_machines.py del módulo externo (solo
cambian los imports a financiero.*).
"""
from decimal import Decimal

from fastapi import HTTPException, status

from financiero.models.usuario import Usuario, RolUsuario
from financiero.models.requisicion import Requisicion, EstadoRequisicion
from financiero.models.traslado import Traslado, EstadoTraslado
from financiero.models.viaje import Viaje, EstadoViaje

TOPE_APROBACION_CONTROLLER_COP = Decimal("100000000.00")  # $100 M COP


class StateMachineService:
    @staticmethod
    def es_admin(usuario: Usuario) -> bool:
        return usuario.rol == RolUsuario.ADMIN or usuario.es_admin_adicional

    # -------------------------------------------------------------------------
    # MÁQUINA DE ESTADOS: REQUISICIONES
    # -------------------------------------------------------------------------
    @classmethod
    def validar_transicion_requisicion(
        cls,
        req: Requisicion,
        nuevo_estado: EstadoRequisicion,
        usuario: Usuario,
        tope: Decimal = TOPE_APROBACION_CONTROLLER_COP
    ):
        actual = req.estado
        admin = cls.es_admin(usuario)

        # Regla 1: Anulación. El solicitante solo mientras no esté en aprobación final ni tenga orden de compra;
        # el administrador en cualquier estado no terminal.
        if nuevo_estado == EstadoRequisicion.ANULADA:
            terminales = [EstadoRequisicion.ANULADA, EstadoRequisicion.RECIBIDO_TOTAL]
            if actual in terminales:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, f"No se puede anular una requisición en estado '{actual.value}'.")
            if admin:
                return
            anulable_por_solicitante = [
                EstadoRequisicion.BORRADOR, EstadoRequisicion.PENDIENTE_JEFE, EstadoRequisicion.PENDIENTE_COMPRAS,
                EstadoRequisicion.RECHAZADA_JEFE, EstadoRequisicion.RECHAZADA_FINANCIERA,
            ]
            if str(req.solicitante_id) != str(usuario.id) or actual not in anulable_por_solicitante:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Solo el Administrador puede anular una requisición en aprobación final o con orden de compra; el solicitante puede hacerlo antes."
                )
            return

        # De BORRADOR a PENDIENTE_JEFE
        if actual == EstadoRequisicion.BORRADOR and nuevo_estado == EstadoRequisicion.PENDIENTE_JEFE:
            if str(req.solicitante_id) != str(usuario.id) and not admin:
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Solo el solicitante puede enviar a aprobación.")
            return

        # De PENDIENTE_JEFE a PENDIENTE_COMPRAS o RECHAZADA_JEFE
        if actual == EstadoRequisicion.PENDIENTE_JEFE and nuevo_estado in [EstadoRequisicion.PENDIENTE_COMPRAS, EstadoRequisicion.RECHAZADA_JEFE]:
            es_jefe_del_area = (usuario.rol == RolUsuario.JEFE_AREA and usuario.area == req.area)
            if not es_jefe_del_area and not admin:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Solo el Jefe del área '{req.area}' o Administrador puede aprobar/rechazar en esta fase."
                )
            return

        # De PENDIENTE_COMPRAS a PENDIENTE_FINANCIERA
        if actual == EstadoRequisicion.PENDIENTE_COMPRAS and nuevo_estado == EstadoRequisicion.PENDIENTE_FINANCIERA:
            if usuario.rol != RolUsuario.COMPRAS and not admin:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Solo el área de Compras puede cotizar y remitir a aprobación financiera."
                )
            return

        # De PENDIENTE_FINANCIERA a APROBADA o RECHAZADA_FINANCIERA
        if actual == EstadoRequisicion.PENDIENTE_FINANCIERA and nuevo_estado in [EstadoRequisicion.APROBADA, EstadoRequisicion.RECHAZADA_FINANCIERA]:
            if req.total <= tope:
                if usuario.rol != RolUsuario.CONTROLLER and not admin:
                    raise HTTPException(status.HTTP_403_FORBIDDEN, "Para requisiciones de hasta el tope del Controller aprueba el Controller.")
            else:
                if usuario.rol != RolUsuario.GERENTE_FINANCIERO and not admin:
                    raise HTTPException(status.HTTP_403_FORBIDDEN, "Requisición mayor al tope del Controller: la aprueba el Gerente financiero.")
            return

        # De APROBADA a EN_ORDEN_COMPRA (o RECIBIDO)
        if actual in [EstadoRequisicion.APROBADA, EstadoRequisicion.EN_ORDEN_COMPRA] and nuevo_estado in [EstadoRequisicion.RECIBIDO_PARCIAL, EstadoRequisicion.RECIBIDO_TOTAL]:
            # Solo quien hizo la requisición confirma la recepción (Compras marca la entrega).
            permitido = (str(req.solicitante_id) == str(usuario.id)) or admin
            if not permitido:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Solo quien hizo la requisición puede confirmar la recepción."
                )
            return

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Transición de estado no válida de '{actual.value}' a '{nuevo_estado.value}'."
        )

    # -------------------------------------------------------------------------
    # MÁQUINA DE ESTADOS: TRASLADOS DE PRESUPUESTO
    # Jefe de área solicita -> Presidencia aprueba o rechaza (un solo paso). Lo aprobado lo ven Gerente financiero y Controller.
    # -------------------------------------------------------------------------
    @classmethod
    def validar_transicion_traslado(cls, tr: Traslado, nuevo_estado: EstadoTraslado, usuario: Usuario):
        actual = tr.estado
        admin = cls.es_admin(usuario)

        if actual != EstadoTraslado.PENDIENTE_PRESIDENCIA:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"La solicitud ya no está pendiente (estado: '{actual.value}').")

        if nuevo_estado == EstadoTraslado.ANULADO:
            if not admin and str(tr.solicitante_id) != str(usuario.id):
                raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo quien creó la solicitud o el Administrador pueden anularla.")
            return

        if nuevo_estado in (EstadoTraslado.APROBADO, EstadoTraslado.RECHAZADO):
            if usuario.rol != RolUsuario.PRESIDENCIA and not admin:
                raise HTTPException(status.HTTP_403_FORBIDDEN, "Solo Presidencia aprueba o rechaza traslados de presupuesto.")
            return

        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"Transición no permitida de '{actual.value}' a '{nuevo_estado.value}'.")

    # -------------------------------------------------------------------------
    # MÁQUINA DE ESTADOS: VIAJES Y ANTICIPOS
    # -------------------------------------------------------------------------
    @classmethod
    def validar_transicion_viaje(
        cls,
        vj: Viaje,
        nuevo_estado: EstadoViaje,
        usuario: Usuario
    ):
        actual = vj.estado
        admin = cls.es_admin(usuario)

        if nuevo_estado == EstadoViaje.ANULADO:
            if actual in [EstadoViaje.CERRADO, EstadoViaje.ANULADO]:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, f"No se puede anular un viaje en estado '{actual.value}'.")
            if not admin and (str(vj.solicitante_id) != str(usuario.id) or actual not in [EstadoViaje.SOLICITADO, EstadoViaje.RECHAZADO]):
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No autorizado para anular la solicitud de viaje.")
            return

        # Autorización de viaje
        if actual == EstadoViaje.SOLICITADO and nuevo_estado in [EstadoViaje.AUTORIZADO, EstadoViaje.RECHAZADO]:
            es_autorizador_oficial = (
                usuario.rol in [RolUsuario.CONTROLLER, RolUsuario.GERENTE_FINANCIERO, RolUsuario.PRESIDENCIA]
                or usuario.es_autorizador_viajes
                or (vj.autorizador_id and str(vj.autorizador_id) == str(usuario.id))
                or admin
            )
            if not es_autorizador_oficial:
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="No tienes privilegios para autorizar solicitudes de viaje.")
            return

        # FLUJO: Solicitante -> Autorizador -> Controller (presupuesto) -> Compras (tiquetes) -> Anticipo
        # 1) Presupuesto y tarifas A/B/C por el Controller, DESPUÉS de la autorización
        if actual == EstadoViaje.AUTORIZADO and nuevo_estado == EstadoViaje.PRESUPUESTO_ASIGNADO:
            if usuario.rol not in [RolUsuario.CONTROLLER, RolUsuario.GERENTE_FINANCIERO] and not admin:
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Solo Controller puede fijar presupuesto y tarifas A/B/C.")
            return

        # 2) Tiquetes por Compras, DESPUÉS del presupuesto del Controller
        if actual == EstadoViaje.PRESUPUESTO_ASIGNADO and nuevo_estado == EstadoViaje.TIQUETES_GESTIONADOS:
            if usuario.rol != RolUsuario.COMPRAS and not admin:
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Solo el área de Compras puede registrar la emisión de tiquetes.")
            return

        # 3) Anticipo: Tesorería o aprobador de pagos, con presupuesto y tiquetes ya gestionados
        if actual == EstadoViaje.TIQUETES_GESTIONADOS and nuevo_estado == EstadoViaje.ANTICIPO_PAGADO:
            permitido = usuario.rol == RolUsuario.TESORERIA or usuario.es_aprobador_pagos or admin
            if not permitido:
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Solo Tesorería o un aprobador de pagos pueden registrar el anticipo.")
            return

        # 4) Legalización: solicitante, Contabilidad o Tesorería
        if actual == EstadoViaje.ANTICIPO_PAGADO and nuevo_estado == EstadoViaje.LEGALIZADO:
            permitido = (
                str(vj.solicitante_id) == str(usuario.id)
                or usuario.rol in [RolUsuario.CONTABILIDAD, RolUsuario.TESORERIA]
                or admin
            )
            if not permitido:
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Solo el solicitante, Contabilidad o Tesorería pueden legalizar el viaje.")
            return

        if actual == EstadoViaje.LEGALIZADO and nuevo_estado == EstadoViaje.CERRADO:
            permitido = usuario.rol in [RolUsuario.CONTABILIDAD, RolUsuario.CONTROLLER, RolUsuario.TESORERIA] or admin
            if not permitido:
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Solo Contabilidad o Controller pueden cerrar el ciclo del viaje.")
            return

        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Transición no permitida de '{actual.value}' a '{nuevo_estado.value}'."
        )
