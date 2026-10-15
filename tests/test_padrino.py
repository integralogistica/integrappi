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
                if "$set" in cambio:
                    d.update(cambio["$set"])
                # $pull/$push (adoptar/desadoptar-placa): soporte mínimo.
                if "$pull" in cambio:
                    for campo, cond in cambio["$pull"].items():
                        lista = d.get(campo) or []
                        if isinstance(cond, dict):
                            d[campo] = [x for x in lista
                                        if not isinstance(x, dict)
                                        or any(x.get(k) != v for k, v in cond.items())]
                        else:
                            d[campo] = [x for x in lista if x != cond]
                if "$push" in cambio:
                    for campo, valor in cambio["$push"].items():
                        d.setdefault(campo, []).append(valor)

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


# ── (2026-10-15) Perfil ADICIONAL PADRINO ────────────────────────────────────
# Un usuario con otro perfil principal (p. ej. OPERATIVO) que tenga PADRINO en
# `perfiles_adicionales` entra al portal de Seguridad en modo simplificado y
# queda sujeto a los MISMOS guards que un padrino de perfil principal.

CUENTA_OPERATIVO_PADRINO = {
    "_id": ObjectId(), "usuario": "MARIA OPERATIVA", "nombre": "Maria Operativa",
    "perfil": "OPERATIVO", "perfiles_adicionales": ["PADRINO"],
    "regional": "BOGOTA", "correo": "maria@test.com",
    "clientes": ["KABI"], "clave": "x",
}
CUENTA_OPERATIVO_SIMPLE = {
    "_id": ObjectId(), "usuario": "PEDRO OPERATIVO", "nombre": "Pedro Operativo",
    "perfil": "OPERATIVO", "regional": "BOGOTA", "correo": "pedro@test.com",
    "clientes": ["KABI"], "clave": "x",
}


class NormalizarPerfilesAdicionalesTests(unittest.TestCase):
    def test_operativo_con_padrino(self):
        self.assertEqual(
            baseusuarios._normalizar_perfiles_adicionales("operativo", ["padrino"]),
            ["PADRINO"])

    def test_redundantes_se_colapsan(self):
        for principal in ("SEGURIDAD", "ADMIN", "PADRINO"):
            self.assertEqual(
                baseusuarios._normalizar_perfiles_adicionales(principal, ["PADRINO"]), [])

    def test_solo_validos_y_sin_duplicados(self):
        self.assertEqual(
            baseusuarios._normalizar_perfiles_adicionales("OPERATIVO", ["padrino", "PADRINO", "ADMIN", ""]),
            ["PADRINO"])


class PerfilDeActorAdicionalTests(unittest.TestCase):
    def test_operativo_con_padrino_adicional_resuelve_padrino(self):
        cuentas = FakeColeccion([CUENTA_OPERATIVO_PADRINO])
        with patch.object(vehiculos, "coleccion_baseusuarios", cuentas):
            self.assertEqual(vehiculos._perfil_de_actor("Maria Operativa"), "PADRINO")

    def test_operativo_simple_resuelve_operativo(self):
        cuentas = FakeColeccion([CUENTA_OPERATIVO_SIMPLE])
        with patch.object(vehiculos, "coleccion_baseusuarios", cuentas):
            self.assertEqual(vehiculos._perfil_de_actor("Pedro Operativo"), "OPERATIVO")

    def test_seguridad_con_adicional_no_degrada(self):
        cuenta = dict(CUENTA_SEGURIDAD, perfiles_adicionales=["PADRINO"])
        with patch.object(vehiculos, "coleccion_baseusuarios", FakeColeccion([cuenta])):
            self.assertEqual(vehiculos._perfil_de_actor("Diana Seguridad"), "SEGURIDAD")


class GuardEstadoAdicionalTests(unittest.TestCase):
    """actualizar-estado via=seguridad: el operativo-con-padrino también 403."""

    def test_operativo_padrino_adicional_no_cambia_estados(self):
        veh = FakeColeccion([{"placa": "XYZ789", "estadoIntegra": "completado_revision"}])
        with patch.object(vehiculos, "coleccion_vehiculos", veh), \
             patch.object(vehiculos, "coleccion_baseusuarios", FakeColeccion([CUENTA_OPERATIVO_PADRINO])):
            c = _cliente(vehiculos.ruta_vehiculos)
            r = c.put("/vehiculos/actualizar-estado", data={
                "placa": "XYZ789", "nuevo_estado": "aprobado",
                "usuario_id": "seg-1", "editado_por": "Maria Operativa", "via": "seguridad",
            })
        self.assertEqual(r.status_code, 403)


class RechazarPadrinoAdicionalTests(unittest.TestCase):
    def test_operativo_padrino_adicional_no_gestiona_conductores(self):
        with patch.object(conductores, "coleccion_baseusuarios",
                          FakeColeccion([CUENTA_OPERATIVO_PADRINO])):
            with self.assertRaises(HTTPException) as ctx:
                conductores._rechazar_padrino("Maria Operativa")
        self.assertEqual(ctx.exception.status_code, 403)

    def test_operativo_simple_pasa(self):
        with patch.object(conductores, "coleccion_baseusuarios",
                          FakeColeccion([CUENTA_OPERATIVO_SIMPLE])):
            conductores._rechazar_padrino("Pedro Operativo")  # no lanza


class PerfilesAdicionalesEndpointTests(unittest.TestCase):
    def setUp(self):
        self.client = _cliente(baseusuarios.ruta_baseusuarios)

    def test_patch_asigna_y_normaliza(self):
        cuentas = FakeColeccion([dict(CUENTA_OPERATIVO_SIMPLE)])
        with patch.object(baseusuarios, "coleccion_usuarios", cuentas):
            r = self.client.patch(
                f"/baseusuarios/{CUENTA_OPERATIVO_SIMPLE['_id']}/perfiles-adicionales",
                json={"perfiles_adicionales": ["padrino"]})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["perfiles_adicionales"], ["PADRINO"])
        self.assertEqual(cuentas.documents[0]["perfiles_adicionales"], ["PADRINO"])

    def test_patch_sobre_seguridad_se_colapsa(self):
        cuentas = FakeColeccion([dict(CUENTA_SEGURIDAD)])
        with patch.object(baseusuarios, "coleccion_usuarios", cuentas):
            r = self.client.patch(
                f"/baseusuarios/{CUENTA_SEGURIDAD['_id']}/perfiles-adicionales",
                json={"perfiles_adicionales": ["PADRINO"]})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["perfiles_adicionales"], [])

    def test_patch_404_si_no_existe(self):
        with patch.object(baseusuarios, "coleccion_usuarios", FakeColeccion([])):
            r = self.client.patch(
                f"/baseusuarios/{ObjectId()}/perfiles-adicionales",
                json={"perfiles_adicionales": ["PADRINO"]})
        self.assertEqual(r.status_code, 404)


class LoginAdicionalTests(unittest.TestCase):
    def setUp(self):
        self.client = _cliente(baseusuarios.ruta_baseusuarios)

    def test_login_devuelve_perfiles_adicionales(self):
        cuenta = dict(CUENTA_OPERATIVO_PADRINO, clave="secreta")
        # Se parchea la búsqueda (usa .find().limit(): el Fake no lo soporta):
        # lo que se verifica es que la RESPUESTA del login incluya los
        # perfiles adicionales, que es lo que el front usa para habilitar el
        # portal de Seguridad en modo Padrino.
        with patch.object(baseusuarios, "_buscar_baseusuario_activo", return_value=cuenta):
            r = self.client.post("/baseusuarios/login", json={"usuario": "maria@test.com", "clave": "secreta"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(r.json()["usuario"]["perfiles_adicionales"], ["PADRINO"])


# ── (2026-10-15) ADOPCIÓN DE PLACAS + AVISOS DIRIGIDOS ────────────────────────
class AdoptarPlacaTests(unittest.TestCase):
    def setUp(self):
        self.client = _cliente(vehiculos.ruta_vehiculos)
        self.cuenta = dict(CUENTA_OPERATIVO_PADRINO,
                           celular="3123456789", correo="maria@test.com")

    def _adoptar(self, veh, cuentas=None, **extra):
        with patch.object(vehiculos, "coleccion_vehiculos", veh), \
             patch.object(vehiculos, "coleccion_baseusuarios", cuentas or FakeColeccion([self.cuenta])), \
             patch.object(vehiculos, "_registrar_auditoria"):
            data = {"placa": "ABC123", "padrino_id": str(self.cuenta["_id"]),
                    "nombre": "Fallback", "correo": "fallback@x.com"}
            data.update(extra)
            return self.client.put("/vehiculos/adoptar-placa", data=data)

    def test_adopta_con_datos_de_la_cuenta(self):
        veh = FakeColeccion([{"placa": "ABC123", "estadoIntegra": "pendiente"}])
        r = self._adoptar(veh)
        self.assertEqual(r.status_code, 200, r.text)
        padrinos = veh.documents[0]["padrinos"]
        self.assertEqual(len(padrinos), 1)
        self.assertEqual(padrinos[0]["nombre"], "Maria Operativa")  # baseusuarios manda
        self.assertEqual(padrinos[0]["correo"], "maria@test.com")
        self.assertEqual(padrinos[0]["celular"], "573123456789")    # normalizado 57…

    def test_idempotente_no_duplica(self):
        veh = FakeColeccion([{"placa": "ABC123", "estadoIntegra": "pendiente"}])
        self._adoptar(veh)
        self._adoptar(veh)
        self.assertEqual(len(veh.documents[0]["padrinos"]), 1)

    def test_404_si_no_existe(self):
        r = self._adoptar(FakeColeccion([]))
        self.assertEqual(r.status_code, 404)

    def test_rechazado_es_solo_lectura(self):
        veh = FakeColeccion([{"placa": "ABC123", "estadoIntegra": "rechazado"}])
        r = self._adoptar(veh)
        self.assertEqual(r.status_code, 403)

    def test_desadoptar_quita_solo_al_padrino(self):
        otro = {"id": "otro-id", "nombre": "Otro", "correo": "otro@x.com"}
        veh = FakeColeccion([{"placa": "ABC123", "estadoIntegra": "pendiente",
                              "padrinos": [otro, {"id": str(self.cuenta["_id"]), "nombre": "Maria"}]}])
        with patch.object(vehiculos, "coleccion_vehiculos", veh), \
             patch.object(vehiculos, "coleccion_baseusuarios", FakeColeccion([self.cuenta])), \
             patch.object(vehiculos, "_registrar_auditoria"):
            r = self.client.put("/vehiculos/desadoptar-placa", data={
                "placa": "ABC123", "padrino_id": str(self.cuenta["_id"])})
        self.assertEqual(r.status_code, 200)
        restantes = veh.documents[0]["padrinos"]
        self.assertEqual(len(restantes), 1)
        self.assertEqual(restantes[0]["id"], "otro-id")


class NotificarPadrinosTests(unittest.TestCase):
    def test_sin_padrinos_no_envia(self):
        with patch.object(vehiculos, "coleccion_vehiculos",
                          FakeColeccion([{"placa": "ABC123"}])), \
             patch.object(vehiculos, "RESEND_API_KEY", "k"), \
             patch.object(vehiculos.resend.Emails, "send") as mock_send:
            vehiculos._notificar_padrinos("ABC123", "algo")
        mock_send.assert_not_called()

    def test_correo_a_los_que_adoptaron(self):
        veh = FakeColeccion([{"placa": "ABC123", "padrinos": [
            {"id": "1", "nombre": "A", "correo": "a@x.com"},
            {"id": "2", "nombre": "B", "correo": "b@x.com"},
            {"id": "3", "nombre": "Sin contacto"},  # sin correo/celular: fuera
        ]}])
        with patch.object(vehiculos, "coleccion_vehiculos", veh), \
             patch.object(vehiculos, "RESEND_API_KEY", "k"), \
             patch.object(vehiculos.resend.Emails, "send") as mock_send, \
             patch("Funciones.whatsapp_utils_integra.enviar_template_sync", return_value=True) as mock_wa:
            vehiculos._notificar_padrinos("ABC123", "Fue aprobada", titulo="Prueba")
        mock_send.assert_called_once()
        # El envío es posicional: params = args[0].
        self.assertEqual(mock_send.call_args.args[0]["to"], ["a@x.com", "b@x.com"])
        self.assertEqual(mock_wa.call_count, 0)  # ninguno tiene celular

    def test_error_no_tumba_el_flujo(self):
        with patch.object(vehiculos, "coleccion_vehiculos", FakeColeccion([{"placa": "X"}])), \
             patch.object(vehiculos, "RESEND_API_KEY", "k"), \
             patch.object(vehiculos.resend.Emails, "send", side_effect=Exception("boom")):
            vehiculos._notificar_padrinos("X", "msg")  # no lanza


class AvisoEstadoAPadrinosTests(unittest.TestCase):
    """actualizar-estado notifica a los padrinos de la placa."""

    def test_cambio_de_estado_avisa(self):
        veh = FakeColeccion([{"placa": "XYZ789", "estadoIntegra": "completado_revision",
                              "estudiosVigencia": {"hasta": "2999-01-01"},
                              "padrinos": [{"id": "p1", "nombre": "P", "correo": "p@x.com"}]}])
        with patch.object(vehiculos, "coleccion_vehiculos", veh), \
             patch.object(vehiculos, "coleccion_baseusuarios", FakeColeccion([CUENTA_SEGURIDAD])), \
             patch.object(vehiculos, "_registrar_auditoria"), \
             patch.object(vehiculos, "_disparar_estudios_seguridad"), \
             patch.object(vehiculos, "enviar_notificacion_seguridad"), \
             patch.object(vehiculos, "_enviar_wa_seguridad"), \
             patch.object(vehiculos, "_tiene_estudio_seguridad", return_value=True), \
             patch.object(vehiculos, "_notificar_padrinos") as mock_aviso:
            c = _cliente(vehiculos.ruta_vehiculos)
            r = c.put("/vehiculos/actualizar-estado", data={
                "placa": "XYZ789", "nuevo_estado": "aprobado",
                "usuario_id": "seg-1", "editado_por": "Diana Seguridad", "via": "seguridad",
            })
        self.assertEqual(r.status_code, 200)
        mock_aviso.assert_called_once()
        self.assertEqual(mock_aviso.call_args.args[0], "XYZ789")
        self.assertIn("APROBADA", mock_aviso.call_args.args[1])
