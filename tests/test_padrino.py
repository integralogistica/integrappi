"""
Tests del perfil PADRINO (2026-10-14): como Seguridad pero SOLO impersona
placas para cargar documentos y datos — sin autorizar estados ni gestionar
conductores. Guards best-effort por NOMBRE del actor contra baseusuarios.
"""
import unittest
from unittest.mock import patch

from bson import ObjectId
from fastapi import HTTPException
from fastapi.testclient import TestClient

from rutas import conductores, vehiculos, baseusuarios


class FakeColeccion:
    def __init__(self, documentos=None):
        self.documents = list(documentos or [])

    def find(self, query=None, *a, **k):
        return iter([d for d in self.documents])

    def find_one(self, query=None, *a, **k):
        if query is None:
            return self.documents[0] if self.documents else None
        for d in self.documents:
            if self._match(d, query):
                return d
        return None

    @staticmethod
    def _match(doc, query):
        import re as _re
        for k, v in query.items():
            valor = doc.get(k)
            if isinstance(v, dict) and "$regex" in v:
                flags = _re.IGNORECASE if "i" in v.get("$options", "") else 0
                if not _re.match(v["$regex"], str(valor or ""), flags):
                    return False
            elif valor != v:
                return False
        return True

    def update_one(self, filtro, cambio):
        for d in self.documents:
            if self._match(d, filtro):
                d.update(cambio.get("$set", {}))

    def count_documents(self, query=None):
        return len([d for d in self.documents if self._match(d, query or {})])


def _cliente(router):
    from fastapi import FastAPI
    app = FastAPI()
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


CUENTA_PADRINO = {"_id": ObjectId(), "usuario": "PADRINO JUAN", "nombre": "Padrino Juan", "perfil": "PADRINO"}
CUENTA_SEGURIDAD = {"_id": ObjectId(), "usuario": "DIANA SEGURIDAD", "nombre": "Diana Seguridad", "perfil": "SEGURIDAD"}


class PerfilDisponibleTests(unittest.TestCase):
    def test_padrino_en_perfiles_disponibles(self):
        self.assertIn("PADRINO", baseusuarios.PERFILES_VALIDOS)


class GuardEstadoTests(unittest.TestCase):
    """actualizar-estado: via=seguridad + actor PADRINO → 403; Seguridad pasa."""

    def _vehiculo(self):
        return {"placa": "XYZ789", "estadoIntegra": "completado_revision",
                "estudiosVigencia": {"hasta": "2999-01-01"},
                "condCedulaCiudadania": "123", "placa_doc": "x"}

    def _llamar(self, actor, cuentas):
        veh = FakeColeccion([self._vehiculo()])
        with patch.object(vehiculos, "coleccion_vehiculos", veh), \
             patch.object(vehiculos, "coleccion_baseusuarios", cuentas), \
             patch.object(vehiculos, "_registrar_auditoria"), \
             patch.object(vehiculos, "_disparar_estudios_seguridad"), \
             patch.object(vehiculos, "enviar_notificacion_seguridad"), \
             patch.object(vehiculos, "_enviar_wa_seguridad"):
            c = _cliente(vehiculos.ruta_vehiculos)
            return c.put("/vehiculos/actualizar-estado", data={
                "placa": "XYZ789", "nuevo_estado": "aprobado",
                "usuario_id": "seg-1", "editado_por": actor, "via": "seguridad",
            })

    def test_padrino_no_cambia_estados(self):
        r = self._llamar("Padrino Juan", FakeColeccion([CUENTA_PADRINO]))
        self.assertEqual(r.status_code, 403)
        self.assertIn("PADRINO", r.json()["detail"])

    def test_seguridad_si_puede(self):
        # aprobado exige estudio vigente: parchear el gate para aislar el guard.
        with patch.object(vehiculos, "_tiene_estudio_seguridad", return_value=True):
            r = self._llamar("Diana Seguridad", FakeColeccion([CUENTA_SEGURIDAD]))
        self.assertEqual(r.status_code, 200)

    def test_actor_desconocido_pasa(self):
        """Compat: el actor que no resuelve en baseusuarios pasa como hoy."""
        with patch.object(vehiculos, "_tiene_estudio_seguridad", return_value=True):
            r = self._llamar("Alguien Random", FakeColeccion([CUENTA_SEGURIDAD]))
        self.assertEqual(r.status_code, 200)

    def test_padrino_sin_via_seguridad_no_toca_el_guard(self):
        """via=impersonacion (acciones del conductor impersonado): sin guard."""
        veh = FakeColeccion([self._vehiculo()])
        with patch.object(vehiculos, "coleccion_vehiculos", veh), \
             patch.object(vehiculos, "coleccion_baseusuarios", FakeColeccion([CUENTA_PADRINO])), \
             patch.object(vehiculos, "_registrar_auditoria"), \
             patch.object(vehiculos, "_disparar_estudios_seguridad"), \
             patch.object(vehiculos, "enviar_notificacion_seguridad"), \
             patch.object(vehiculos, "_enviar_wa_seguridad"):
            c = _cliente(vehiculos.ruta_vehiculos)
            r = c.put("/vehiculos/actualizar-estado", data={
                "placa": "XYZ789", "nuevo_estado": "completado_revision",
                "usuario_id": "u", "editado_por": "Padrino Juan",
                "via": "impersonacion",
            })
        self.assertNotEqual(r.status_code, 403)


class GuardConductoresTests(unittest.TestCase):
    """invitar/reenviar/desvincular con solicitante PADRINO → 403."""

    def setUp(self):
        self.vehiculo = {"placa": "XYZ789", "idUsuario": "ten-1", "estadoIntegra": "registro_incompleto",
                         "idConductor": "cond-1",
                         "invitacionConductor": {"correo": "UNO@correo.com", "estado": "aceptada"},
                         "idConductor2": None, "invitacionConductor2": None}

    def _parches(self):
        return [
            patch.object(conductores, "coleccion_conductores", FakeColeccion()),
            patch.object(conductores, "coleccion_vehiculos", FakeColeccion([dict(self.vehiculo)])),
            patch.object(conductores, "coleccion_baseusuarios", FakeColeccion([CUENTA_PADRINO, CUENTA_SEGURIDAD])),
            patch.object(conductores, "_pos_cambio_conductor"),
        ]

    def test_invitar_con_solicitante_padrino_rechaza(self):
        import asyncio
        from rutas.conductores import InvitarConductorInput
        from contextlib import ExitStack
        datos = InvitarConductorInput(
            id_tenedor="cualquiera", placa="XYZ789",
            correo_conductor="dos@correo.com", slot=2,
            solicitante="Padrino Juan")
        with ExitStack() as stack:
            for p in self._parches():
                stack.enter_context(p)
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(conductores.invitar_conductor(datos, _TareasFake()))
        self.assertEqual(ctx.exception.status_code, 403)

    def test_desvincular_con_solicitante_padrino_rechaza(self):
        import asyncio
        from rutas.conductores import DesvincularConductorInput
        from contextlib import ExitStack
        datos = DesvincularConductorInput(
            id_tenedor="ten-1", placa="XYZ789", slot=2, solicitante="Padrino Juan")
        with ExitStack() as stack:
            for p in self._parches():
                stack.enter_context(p)
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(conductores.desvincular_conductor(datos))
        self.assertEqual(ctx.exception.status_code, 403)

    def test_reenviar_con_solicitante_padrino_rechaza(self):
        import asyncio
        from rutas.conductores import ReenviarInvitacionInput
        from contextlib import ExitStack
        self.vehiculo["idConductor2"] = None
        self.vehiculo["invitacionConductor2"] = {"correo": "DOS@correo.com", "estado": "pendiente"}
        datos = ReenviarInvitacionInput(
            id_tenedor="ten-1", placa="XYZ789", slot=2, solicitante="Padrino Juan")
        with ExitStack() as stack:
            parches = self._parches()
            parches[1] = patch.object(conductores, "coleccion_vehiculos",
                                      FakeColeccion([dict(self.vehiculo)]))
            for p in parches:
                stack.enter_context(p)
            with self.assertRaises(HTTPException) as ctx:
                asyncio.run(conductores.reenviar_invitacion(datos, _TareasFake()))
        self.assertEqual(ctx.exception.status_code, 403)

    def test_solicitante_seguridad_pasa_el_guard(self):
        """Seguridad llega hasta la validación de NEGOCIO (409 slot ocupado),
        no al 403 de padrino — prueba de que el guard no la bloquea."""
        import asyncio
        from rutas.conductores import DesvincularConductorInput
        from contextlib import ExitStack
        datos = DesvincularConductorInput(
            id_tenedor="ten-1", placa="XYZ789", slot=1, solicitante="Diana Seguridad")
        with ExitStack() as stack:
            for p in self._parches():
                stack.enter_context(p)
            resultado = asyncio.run(conductores.desvincular_conductor(datos))
        self.assertIn("desvinculado", resultado["mensaje"].lower())


class _TareasFake:
    def add_task(self, func, *args, **kwargs):
        pass


if __name__ == "__main__":
    unittest.main()
