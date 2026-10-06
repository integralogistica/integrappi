"""Tests de la BITÁCORA DE AUDITORÍA del vehículo (2026-10-01): toda mutación
deja registro de quién la hizo realmente (conductor con su cuenta | Seguridad
impersonando | Seguridad directa), cuándo y por qué canal."""
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

if "bd.bd_cliente" not in sys.modules:
    _stub = types.ModuleType("bd.bd_cliente")

    class _BDFake(dict):
        def __getitem__(self, clave):
            return self.setdefault(clave, MagicMock())

    _stub.bd_cliente = _BDFake()
    sys.modules["bd.bd_cliente"] = _stub

from fastapi import FastAPI
from fastapi.testclient import TestClient

from rutas import conductores, vehiculos
from tests.test_alta_seguridad import FakeColeccion


def cliente_de_prueba(router) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


def _veh(placa="ABC123", estado="registro_incompleto", **extra):
    doc = {"_id": "v1", "placa": placa, "estadoIntegra": estado,
           "idUsuario": "507f1f77bcf86cd799439012", "fotos": []}
    doc.update(extra)
    return doc


class BlindajesTests(unittest.TestCase):
    """El campo no lo escribe el front ni rompe la verificación de firma."""

    def test_blindajes(self):
        self.assertIn("auditoriaVehiculo", vehiculos.CLAVES_PROTEGIDAS)
        self.assertIn("auditoriaVehiculo", vehiculos.CAMPOS_VOLATILES_FIRMA)


class CrearTests(unittest.TestCase):

    def test_crear_con_creado_por_queda_via_seguridad(self):
        veh_fake = FakeColeccion()
        cuentas = FakeColeccion([{"_id": "507f1f77bcf86cd799439012",
                                  "perfil": "TENEDOR"}])
        cliente = cliente_de_prueba(vehiculos.ruta_vehiculos)
        with patch.object(vehiculos, "coleccion_vehiculos", veh_fake), \
             patch.object(vehiculos, "coleccion_conductores_cuenta", cuentas), \
             patch.object(vehiculos, "_registrar_auditoria",
                          wraps=vehiculos._registrar_auditoria) as spy:
            r = cliente.post("/vehiculos/crear", data={
                "id_usuario": "507f1f77bcf86cd799439012",
                "placa": "XYZ789", "creado_por": "EDWIN"})
        self.assertEqual(r.status_code, 201)
        # El doc nace con la bitácora inicializada y la entrada de creación.
        bitacora = veh_fake.documents[0]["auditoriaVehiculo"]
        self.assertEqual(len(bitacora), 1)
        self.assertEqual(bitacora[0]["accion"], "vehiculo_creado")
        spy.assert_called_once()
        _, kwargs = spy.call_args
        self.assertEqual(kwargs["actor"], "EDWIN")
        self.assertEqual(kwargs["via"], "seguridad")


class AuditoriaHelperTests(unittest.TestCase):

    def test_registrar_auditoria_appends_con_via(self):
        veh_fake = FakeColeccion([_veh()])
        cliente = None  # helper directo
        with patch.object(vehiculos, "coleccion_vehiculos", veh_fake):
            vehiculos._registrar_auditoria("ABC123", "datos_actualizados",
                                           actor="EDWIN", detalle="3 campo(s)")
            vehiculos._registrar_auditoria("ABC123", "fotos_subidas",
                                           detalle="2 foto(s)")
        bitacora = veh_fake.documents[0]["auditoriaVehiculo"]
        self.assertEqual(len(bitacora), 2)
        # Con actor (Seguridad impersonando) → via impersonacion.
        self.assertEqual(bitacora[0]["actor"], "EDWIN")
        self.assertEqual(bitacora[0]["via"], "impersonacion")
        self.assertEqual(bitacora[0]["accion"], "datos_actualizados")
        # Sin actor → el titular con su cuenta.
        self.assertIsNone(bitacora[1]["actor"])
        self.assertEqual(bitacora[1]["via"], "conductor")
        self.assertIn("fecha", bitacora[0])


class MutacionesTests(unittest.TestCase):

    def test_subir_documento_audita_en_cualquier_estado(self):
        """Antes el actor se DESCARTABA si el vehículo no era aprobado
        (editado_por solo alimentaba la re-revisión): ahora siempre queda."""
        veh_fake = FakeColeccion([_veh(estado="registro_incompleto")])
        cliente = cliente_de_prueba(vehiculos.ruta_vehiculos)
        with patch.object(vehiculos, "coleccion_vehiculos", veh_fake), \
             patch.object(vehiculos, "subir_a_google_storage",
                          lambda *a, **k: "Vehiculos/X/2026-10-01/soat.pdf"), \
             patch.object(vehiculos, "_registrar_cambio_aprobado") as spy_rr:
            r = cliente.put(
                "/vehiculos/subir-documento",
                data={"placa": "ABC123", "tipo": "soat",
                      "editado_por": "EDWIN", "extraer": "false"},
                files={"archivo": ("soat.pdf", b"%PDF", "application/pdf")})
        self.assertEqual(r.status_code, 200)
        bitacora = veh_fake.documents[0]["auditoriaVehiculo"]
        self.assertEqual(len(bitacora), 1)
        self.assertEqual(bitacora[0]["accion"], "documento_subido")
        self.assertEqual(bitacora[0]["actor"], "EDWIN")
        self.assertEqual(bitacora[0]["via"], "impersonacion")
        # registro_incompleto NO baja a re-revisión (early-return interno).
        self.assertEqual(veh_fake.documents[0]["estadoIntegra"], "registro_incompleto")

    def test_actualizar_estado_audita_transicion(self):
        veh_fake = FakeColeccion([_veh(estado="completado_revision", hojaVidaFisica="u",
                                       vehCapacidadCarga="10000", fotos=["f1"])])
        # Vehículo con TODOS los documentos para pasar la validación.
        completo = {
            "tarjetaPropiedad": "u1", "tarjetaPropiedadReverso": "u1r", "soat": "u2",
            "revisionTecnomecanica": "u3",
            "documentoIdentidadConductor": "u6", "documentoIdentidadConductorReverso": "u6r",
            "documentoIdentidadPropietario": "u7", "documentoIdentidadPropietarioReverso": "u7r",
            "documentoIdentidadTenedor": "u8", "documentoIdentidadTenedorReverso": "u8r",
            "licencia": "u9", "licenciaReverso": "u9r", "planillaEpsArl": "u10",
            "condFoto": "u11", "condCertificacionBancaria": "u12",
            "tenedCertificacionBancaria": "u14", "documentoAcreditacionTenedor": "u15",
            "rutTenedor": "u16", "hojaVidaFisica": "u17",
        }
        veh_fake.documents[0].update(completo)
        cliente = cliente_de_prueba(vehiculos.ruta_vehiculos)
        with patch.object(vehiculos, "coleccion_vehiculos", veh_fake), \
             patch.object(vehiculos, "_disparar_estudios_seguridad"), \
             patch.object(vehiculos, "enviar_notificacion_seguridad"):
            r = cliente.put("/vehiculos/actualizar-estado", data={
                "placa": "ABC123", "nuevo_estado": "aprobado",
                "usuario_id": "seg1", "editado_por": "EDWIN", "via": "seguridad"})
        self.assertEqual(r.status_code, 200)
        bitacora = veh_fake.documents[0]["auditoriaVehiculo"]
        self.assertEqual(bitacora[-1]["accion"], "estado_aprobado")
        self.assertEqual(bitacora[-1]["via"], "seguridad")
        self.assertIn("completado_revision → aprobado", bitacora[-1]["detalle"])

    def test_obtener_vehiculo_oculta_auditoria_al_conductor(self):
        doc = _veh()
        doc["auditoriaVehiculo"] = [
            {"fecha": "2026-10-01T00:00:00", "actor": "EDWIN",
             "via": "impersonacion", "accion": "datos_actualizados"}]
        veh_fake = FakeColeccion([doc])
        cliente = cliente_de_prueba(vehiculos.ruta_vehiculos)
        with patch.object(vehiculos, "coleccion_vehiculos", veh_fake):
            r = cliente.get("/vehiculos/obtener-vehiculo/ABC123")
        self.assertEqual(r.status_code, 200)
        self.assertNotIn("auditoriaVehiculo", r.json()["data"])


class AltaSinNombreTests(unittest.TestCase):
    """El alta de Seguridad pide SOLO correo + celular: el nombre lo aporta
    la IA al leer los documentos y se propaga a la cuenta desde
    actualizar-informacion mientras esté vacío."""

    def test_alta_sin_nombre(self):
        from tests.test_alta_seguridad import FakeColeccion as FC
        fake = FC()
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", fake):
            r = cliente.post("/conductores/alta-seguridad", json={
                "correo": "nuevo@x.com", "perfil": "TENEDOR",
                "celular": "3001234567"})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(fake.documents[0]["nombre"], "")  # vacío, no error
        self.assertEqual(fake.documents[0]["celular"], "3001234567")

    def test_propagar_nombre_a_cuenta_vacia(self):
        """La IA lee la cédula → guardar el formulario propaga el nombre del
        conductor/tenedor a las cuentas vinculadas que no lo tengan."""
        oid = "507f1f77bcf86cd799439012"
        cuentas_fake = FakeColeccion([
            {"_id": oid, "correo": "TEN@X.COM", "perfil": "TENEDOR", "nombre": ""},
            {"_id": "507f1f77bcf86cd799439013", "correo": "COND@X.COM",
             "perfil": "CONDUCTOR", "nombre": "YA TENGO NOMBRE"},
        ])
        veh = _veh(idUsuario=oid, idConductor="507f1f77bcf86cd799439013")
        veh_fake = FakeColeccion([veh])
        cliente = cliente_de_prueba(vehiculos.ruta_vehiculos)
        with patch.object(vehiculos, "coleccion_vehiculos", veh_fake), \
             patch.object(vehiculos, "coleccion_conductores_cuenta", cuentas_fake):
            r = cliente.put("/vehiculos/actualizar-informacion/ABC123", json={
                "condNombres": "JUAN", "condPrimerApellido": "PEREZ",
                "condSegundoApellido": "GOMEZ",
                "tenedNombre": "TRANSPORTES SAS",
            })
        self.assertEqual(r.status_code, 200, r.text)
        # TENEDOR recibió el nombre del TENEDOR de la ficha (estaba vacío).
        self.assertEqual(cuentas_fake.documents[0]["nombre"], "TRANSPORTES SAS")
        # La cuenta CONDUCTOR YA tenía nombre → no se pisa.
        self.assertEqual(cuentas_fake.documents[1]["nombre"], "YA TENGO NOMBRE")


class RegistroMinimoTests(unittest.TestCase):
    """El registro del conductor pide SOLO correo + clave (+ perfil): el
    nombre y el celular los aporta la IA en el paso 2 y se propagan a la
    cuenta desde actualizar-informacion mientras estén vacíos (2026-10-06)."""

    def test_registrar_sin_nombre_ni_celular(self):
        fake = FakeColeccion()
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", fake):
            r = cliente.post("/conductores/registrar", json={
                "correo": "nuevo@x.com", "clave": "clave123",
                "perfil": "CONDUCTOR"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(fake.documents[0]["nombre"], "")  # vacío, no error
        self.assertIsNone(fake.documents[0]["celular"])

    def test_propagar_celular_a_cuenta_vacia(self):
        """El celular del formulario del paso 2 llega a las cuentas vinculadas
        que no lo tengan (la cuenta CONDUCTOR ya tiene → no se pisa)."""
        oid = "507f1f77bcf86cd799439012"
        cuentas_fake = FakeColeccion([
            {"_id": oid, "correo": "TEN@X.COM", "perfil": "TENEDOR", "celular": None},
            {"_id": "507f1f77bcf86cd799439013", "correo": "COND@X.COM",
             "perfil": "CONDUCTOR", "celular": "3101112223"},
        ])
        veh_fake = FakeColeccion([_veh(idConductor="507f1f77bcf86cd799439013")])
        cliente = cliente_de_prueba(vehiculos.ruta_vehiculos)
        with patch.object(vehiculos, "coleccion_vehiculos", veh_fake), \
             patch.object(vehiculos, "coleccion_conductores_cuenta", cuentas_fake):
            r = cliente.put("/vehiculos/actualizar-informacion/ABC123", json={
                "condCelular": "3001234567"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(cuentas_fake.documents[0]["celular"], "3001234567")
        self.assertEqual(cuentas_fake.documents[1]["celular"], "3101112223")


class LoginComoTests(unittest.TestCase):

    def test_login_como_devuelve_impersonado_por(self):
        from tests.test_alta_seguridad import FakeColeccion as FC
        oid_hex = "507f1f77bcf86cd799439012"
        doc = {"_id": oid_hex, "correo": "A@X.COM", "nombre": "PEDRO PEREZ",
               "perfil": "CONDUCTOR", "activo": True}
        fake = FC([doc])
        imp = FC()
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", fake), \
             patch.object(conductores, "coleccion_impersonaciones", imp):
            r = cliente.post(f"/conductores/login-como/{oid_hex}",
                             json={"solicitante": "EDWIN"})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["impersonado_por"], "EDWIN")


if __name__ == "__main__":
    unittest.main()
