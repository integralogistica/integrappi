"""Tests de las CUENTAS SIN VEHÍCULO (2026-10-13): listado de cuentas
activas sin ninguna placa inscrita (ni propia ni como conductor invitado)
+ reenvío de verificación explícito + recordatorio a verificados."""
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

if "bd.bd_cliente" not in sys.modules:
    _stub = types.ModuleType("bd.bd_cliente")

    class _BDFake(dict):
        def __getitem__(self, clave):
            from unittest.mock import MagicMock
            return self.setdefault(clave, MagicMock())

    _stub.bd_cliente = _BDFake()
    sys.modules["bd.bd_cliente"] = _stub

from bson import ObjectId
from fastapi import FastAPI
from fastapi.testclient import TestClient

from rutas import conductores
from tests.test_alta_seguridad import FakeColeccion


def cliente_de_prueba(router) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


def _cuenta(correo, **extra):
    doc = {"_id": ObjectId(), "correo": correo, "nombre": "PEDRO PEREZ",
           "perfil": "CONDUCTOR", "activo": True, "clave": "SECRETO",
           "correo_verificado": False}
    doc.update(extra)
    return doc


class SinVehiculoTests(unittest.TestCase):

    def test_lista_solo_las_que_no_tienen_placa(self):
        """Dueño (idUsuario) y conductor invitado (idConductor) SÍ cuentan
        como placa inscrita: solo la cuenta huérfana queda en la lista."""
        sola = _cuenta("SOLA@X.COM", correo_verificado=True)
        duenhia = _cuenta("DUENHO@X.COM")
        invitado = _cuenta("INVITADO@X.COM")
        conductores_fake = FakeColeccion([sola, duenhia, invitado])
        vehiculos_fake = FakeColeccion([
            {"_id": "v1", "placa": "ABC123", "idUsuario": str(duenhia["_id"])},
            {"_id": "v2", "placa": "DEF456", "idConductor": str(invitado["_id"])},
        ])
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", conductores_fake), \
             patch.object(conductores, "coleccion_vehiculos", vehiculos_fake):
            r = cliente.get("/conductores/sin-vehiculo")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["total"], 1)
        cuenta = r.json()["cuentas"][0]
        self.assertEqual(cuenta["correo"], "SOLA@X.COM")
        self.assertTrue(cuenta["correo_verificado"])
        # La fecha viene del ObjectId (generation_time, UTC) y sin claves JAMÁS.
        self.assertIn("creado", cuenta)
        self.assertIn("+00:00", cuenta["creado"])
        self.assertNotIn("clave", cuenta)

    def test_excluye_stubs_de_invitacion(self):
        """Las cuentas stub (activo=False, esperando invitación) no cuentan."""
        stub = _cuenta("STUB@X.COM", activo=False)
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", FakeColeccion([stub])), \
             patch.object(conductores, "coleccion_vehiculos", FakeColeccion()):
            r = cliente.get("/conductores/sin-vehiculo")
        self.assertEqual(r.json()["total"], 0)

    def test_sin_verificar_primero(self):
        """Orden: pendientes de verificación primero (los que necesitan reenvío)."""
        verificada = _cuenta("V@X.COM", correo_verificado=True)
        pendiente = _cuenta("P@X.COM", correo_verificado=False)
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores",
                          FakeColeccion([verificada, pendiente])), \
             patch.object(conductores, "coleccion_vehiculos", FakeColeccion()):
            r = cliente.get("/conductores/sin-vehiculo")
        correos = [c["correo"] for c in r.json()["cuentas"]]
        self.assertEqual(correos, ["P@X.COM", "V@X.COM"])


class ReenvioVerificacionSeguridadTests(unittest.TestCase):

    def test_reenvia_y_regenera_token(self):
        doc = _cuenta("PEND@X.COM")
        fake = FakeColeccion([doc])
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", fake):
            r = cliente.post(f"/conductores/{doc['_id']}/reenviar-verificacion")
        self.assertEqual(r.status_code, 200)
        self.assertIn("PEND@X.COM", r.json()["mensaje"])
        # El token quedó regenerado (sha nuevo para el lookup O(1)).
        self.assertIn("verificacion_token_sha", doc)
        self.assertIn("verificacion_expira", doc)

    def test_409_si_ya_verificada(self):
        doc = _cuenta("OK@X.COM", correo_verificado=True)
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", FakeColeccion([doc])):
            r = cliente.post(f"/conductores/{doc['_id']}/reenviar-verificacion")
        self.assertEqual(r.status_code, 409)
        self.assertIn("recordatorio", r.json()["detail"].lower())

    def test_corrige_correo_xom_y_manda_whatsapp(self):
        # (2026-11-17) El caso GMAIL.XOM: el reenvío corrige el correo en la
        # MISMA petición (si no, el WhatsApp rescataría la activación pero la
        # cuenta quedaría con un correo muerto) y dispara el WhatsApp.
        doc = _cuenta("OMARANGULOMIRANDA@GMAIL.XOM")
        doc["nombre"] = "OMAR ANGULO"
        fake = FakeColeccion([doc])
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        was = []
        def wa(destino, plantilla, idioma, params, botones=None):
            was.append({"to": destino, "plantilla": plantilla})
            return True
        with patch.object(conductores, "coleccion_conductores", fake), \
             patch.object(conductores, "enviar_template_sync", side_effect=wa):
            r = cliente.post(f"/conductores/{doc['_id']}/reenviar-verificacion",
                             json={"correo": "omarangulomiranda@gmail.com",
                                   "celular": "312 544 3396"})
        self.assertEqual(r.status_code, 200, r.text)
        cuerpo = r.json()
        self.assertTrue(cuerpo["whatsapp"])
        self.assertEqual(cuerpo["correo"], "OMARANGULOMIRANDA@GMAIL.COM")
        # La cuenta quedó con el correo corregido y el celular guardado.
        self.assertEqual(doc["correo"], "OMARANGULOMIRANDA@GMAIL.COM")
        self.assertEqual(doc["celular"], "3125443396")
        # El mensaje de éxito muestra el correo NUEVO y el WhatsApp.
        self.assertIn("OMARANGULOMIRANDA@GMAIL.COM", cuerpo["mensaje"])
        self.assertEqual(was[0]["to"], "573125443396")
        self.assertEqual(was[0]["plantilla"], "enruta_continuar_registro")

    def test_correo_nuevo_invalido_o_ocupado(self):
        doc = _cuenta("MAL@X.XOM")
        otra = _cuenta("TOMADO@GMAIL.COM")
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        # Dominio sin punto → 400 con mensaje que guía al typo.
        with patch.object(conductores, "coleccion_conductores", FakeColeccion([doc])):
            r = cliente.post(f"/conductores/{doc['_id']}/reenviar-verificacion",
                             json={"correo": "omar@gmail"})
        self.assertEqual(r.status_code, 400)
        self.assertIn("no parece válido", r.json()["detail"])
        # Correo de OTRA cuenta → 409.
        with patch.object(conductores, "coleccion_conductores", FakeColeccion([doc, otra])):
            r = cliente.post(f"/conductores/{doc['_id']}/reenviar-verificacion",
                             json={"correo": "tomado@gmail.com"})
        self.assertEqual(r.status_code, 409)
        self.assertIn("otra cuenta", r.json()["detail"])

    def test_404_inexistente(self):
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", FakeColeccion()):
            r = cliente.post(f"/conductores/{ObjectId()}/reenviar-verificacion")
        self.assertEqual(r.status_code, 404)


class RecordatorioVehiculoTests(unittest.TestCase):

    def test_envia_a_verificada_sin_placa(self):
        doc = _cuenta("LISTO@X.COM", correo_verificado=True)
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", FakeColeccion([doc])), \
             patch.object(conductores, "coleccion_vehiculos", FakeColeccion()):
            r = cliente.post(f"/conductores/{doc['_id']}/recordatorio-vehiculo")
        self.assertEqual(r.status_code, 200)
        self.assertIn("LISTO@X.COM", r.json()["mensaje"])

    def test_409_sin_verificar(self):
        doc = _cuenta("NOVER@X.COM", correo_verificado=False)
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", FakeColeccion([doc])), \
             patch.object(conductores, "coleccion_vehiculos", FakeColeccion()):
            r = cliente.post(f"/conductores/{doc['_id']}/recordatorio-vehiculo")
        self.assertEqual(r.status_code, 409)

    def test_409_con_placas_vinculadas(self):
        doc = _cuenta("CONPLACA@X.COM", correo_verificado=True)
        vehiculos_fake = FakeColeccion(
            [{"_id": "v1", "placa": "ABC123", "idUsuario": str(doc["_id"])}])
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", FakeColeccion([doc])), \
             patch.object(conductores, "coleccion_vehiculos", vehiculos_fake):
            r = cliente.post(f"/conductores/{doc['_id']}/recordatorio-vehiculo")
        self.assertEqual(r.status_code, 409)


class CambiarPerfilTests(unittest.TestCase):
    """(2026-10-19) Corrección del PERFIL elegido al registrarse. (2026-11-25)
    RELAJADO y CON AUTORIZACIÓN: el `solicitante` (SEGURIDAD/ADMIN en
    baseusuarios) es obligatorio; un CONDUCTOR con placas PROPIAS (idUsuario)
    SÍ puede pasar a TENEDOR para registrar varias placas (cada ficha propia
    queda auditada), pero con placas INVITADAS (idConductor/idConductor2 en
    fichas ajenas) → 409 (un tenedor no puede ser conductor invitado) y
    TENEDOR→CONDUCTOR sigue exigiendo 0 placas."""

    SEGURIDAD = {"_id": "u1", "usuario": "SEGUSER", "nombre": "Laura Seguridad",
                 "perfil": "SEGURIDAD"}
    ADMIN = {"_id": "u2", "usuario": "ADMINUSER", "nombre": "Ana Admin",
             "perfil": "ADMIN"}
    OPERATIVO = {"_id": "u3", "usuario": "OPUSER", "nombre": "Oscar Operativo",
                 "perfil": "OPERATIVO"}
    PADRINO = {"_id": "u4", "usuario": "PADUSER", "nombre": "Pablo Padrino",
               "perfil": "PADRINO"}

    def baseusuarios_fake(self):
        return FakeColeccion([dict(self.SEGURIDAD), dict(self.ADMIN),
                              dict(self.OPERATIVO), dict(self.PADRINO)])

    def cliente_cambiar(self, doc, vehiculos_fake, body):
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", FakeColeccion([doc])), \
             patch.object(conductores, "coleccion_vehiculos", vehiculos_fake), \
             patch.object(conductores, "coleccion_baseusuarios", self.baseusuarios_fake()):
            r = cliente.post(f"/conductores/{doc['_id']}/cambiar-perfil", json=body)
        return r

    def test_cambia_tenedor_a_conductor_sin_placas(self):
        doc = _cuenta("EQUIVOCADO@X.COM", perfil="TENEDOR", correo_verificado=True)
        r = self.cliente_cambiar(doc, FakeColeccion(),
                                 {"perfil": "conductor", "solicitante": "SEGUSER"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(doc["perfil"], "CONDUCTOR")       # quedó corregido
        self.assertIn("CONDUCTOR", r.json()["mensaje"])
        self.assertEqual(r.json()["placas_auditoria"], 0)

    def test_409_con_placas_inscritas_no_toca_el_perfil(self):
        # TENEDOR→CONDUCTOR sigue exigiendo 0 placas (propias ni invitadas).
        doc = _cuenta("CONFICHA@X.COM", perfil="TENEDOR")
        vehiculos_fake = FakeColeccion(
            [{"_id": "v1", "placa": "ABC123", "idUsuario": str(doc["_id"])}])
        r = self.cliente_cambiar(doc, vehiculos_fake,
                                 {"perfil": "CONDUCTOR", "solicitante": "SEGUSER"})
        self.assertEqual(r.status_code, 409)
        self.assertEqual(doc["perfil"], "TENEDOR")          # intacto

    def test_400_perfil_fuera_de_catalogo(self):
        # ADMIN/SEGURIDAD/PADRUNO… no son perfiles de portal: se rechazan.
        doc = _cuenta("X@X.COM")
        r = self.cliente_cambiar(doc, FakeColeccion(),
                                 {"perfil": "ADMIN", "solicitante": "SEGUSER"})
        self.assertEqual(r.status_code, 400)

    def test_mismo_perfil_es_idempotente(self):
        doc = _cuenta("YA@X.COM", perfil="CONDUCTOR")
        r = self.cliente_cambiar(doc, FakeColeccion(),
                                 {"perfil": "CONDUCTOR", "solicitante": "SEGUSER"})
        self.assertEqual(r.status_code, 200)

    # ── (2026-11-25) CONDUCTOR con placa propia → TENEDOR ──

    def test_conductor_con_placa_propia_pasa_a_tenedor_y_audita(self):
        """El caso pedido: se registró como CONDUCTOR e inscribió SU placa →
        Seguridad lo promueve a TENEDOR para que registre más placas. El
        cambio queda auditado en la ficha (accion perfil_tenedor)."""
        doc = _cuenta("CONPLACA@X.COM", correo_verificado=True)
        vehiculo = {"_id": "v1", "placa": "ABC123", "idUsuario": str(doc["_id"])}
        r = self.cliente_cambiar(doc, FakeColeccion([vehiculo]),
                                 {"perfil": "TENEDOR", "solicitante": "SEGUSER"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(doc["perfil"], "TENEDOR")
        self.assertEqual(r.json()["placas_auditoria"], 1)
        entrada = vehiculo["auditoriaVehiculo"][0]
        self.assertEqual(entrada["accion"], "perfil_tenedor")
        self.assertEqual(entrada["via"], "seguridad")
        self.assertEqual(entrada["actor"], "SEGUSER")
        self.assertIn("CONPLACA@X.COM", entrada["detalle"])

    def test_conductor_invitado_no_pasa_a_tenedor(self):
        """Es conductor INVITADO (idConductor) en la ficha de otro → 409: un
        tenedor no puede ser conductor invitado; el perfil queda intacto."""
        doc = _cuenta("INVITADO@X.COM")
        vehiculos_fake = FakeColeccion(
            [{"_id": "v9", "placa": "ZZZ999", "idUsuario": "otro",
              "idConductor": str(doc["_id"])}])
        r = self.cliente_cambiar(doc, vehiculos_fake,
                                 {"perfil": "TENEDOR", "solicitante": "SEGUSER"})
        self.assertEqual(r.status_code, 409)
        self.assertIn("invitado", r.json()["detail"].lower())
        self.assertEqual(doc["perfil"], "CONDUCTOR")

    def test_conductor_propia_e_invitada_no_pasa_a_tenedor(self):
        doc = _cuenta("MIXTO@X.COM")
        vehiculos_fake = FakeColeccion([
            {"_id": "v1", "placa": "AAA111", "idUsuario": str(doc["_id"])},
            {"_id": "v2", "placa": "BBB222", "idUsuario": "otro",
             "idConductor2": str(doc["_id"])},
        ])
        r = self.cliente_cambiar(doc, vehiculos_fake,
                                 {"perfil": "TENEDOR", "solicitante": "SEGUSER"})
        self.assertEqual(r.status_code, 409)
        self.assertIn("BBB222", r.json()["detail"])
        self.assertEqual(doc["perfil"], "CONDUCTOR")

    def test_admin_tambien_puede_promover(self):
        doc = _cuenta("ADMINPROMUEVE@X.COM")
        r = self.cliente_cambiar(doc, FakeColeccion(),
                                 {"perfil": "TENEDOR", "solicitante": "ADMINUSER"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(doc["perfil"], "TENEDOR")

    # ── (2026-11-25) Autorización del solicitante ──

    def test_sin_solicitante_rechaza(self):
        doc = _cuenta("X@X.COM")
        r = self.cliente_cambiar(doc, FakeColeccion(), {"perfil": "TENEDOR"})
        self.assertEqual(r.status_code, 403)
        self.assertEqual(doc["perfil"], "CONDUCTOR")

    def test_solicitante_operativo_rechaza(self):
        doc = _cuenta("X@X.COM")
        r = self.cliente_cambiar(doc, FakeColeccion(),
                                 {"perfil": "TENEDOR", "solicitante": "OPUSER"})
        self.assertEqual(r.status_code, 403)
        self.assertIn("OPERATIVO", r.json()["detail"])

    def test_solicitante_desconocido_404(self):
        doc = _cuenta("X@X.COM")
        r = self.cliente_cambiar(doc, FakeColeccion(),
                                 {"perfil": "TENEDOR", "solicitante": "NADIE"})
        self.assertEqual(r.status_code, 404)

    def test_solicitante_por_nombre_resuelve(self):
        """El front envía seguridadNombre (= nombre || usuario): el helper
        resuelve por nombre cuando el usuario exacto no matchea."""
        doc = _cuenta("NOMBRE@X.COM")
        r = self.cliente_cambiar(doc, FakeColeccion(),
                                 {"perfil": "TENEDOR", "solicitante": "Laura Seguridad"})
        self.assertEqual(r.status_code, 200, r.text)
        self.assertEqual(doc["perfil"], "TENEDOR")

    def test_solicitante_padrino_rechaza(self):
        doc = _cuenta("X@X.COM")
        r = self.cliente_cambiar(doc, FakeColeccion(),
                                 {"perfil": "TENEDOR", "solicitante": "PADUSER"})
        self.assertEqual(r.status_code, 403)
        self.assertEqual(doc["perfil"], "CONDUCTOR")


class BuscarPlacasInvitadoTests(unittest.TestCase):
    """(2026-11-25) /buscar expone placas_invitado para que el front
    deshabilite la promoción a TENEDOR con explicación (no 409 al vuelo)."""

    def test_buscar_devuelve_placas_invitado(self):
        doc = _cuenta("BUSCADA@X.COM", correo_verificado=True)
        vehiculos_fake = FakeColeccion([
            {"_id": "v1", "placa": "AAA111", "idUsuario": str(doc["_id"])},
            {"_id": "v2", "placa": "BBB222", "idUsuario": "otro",
             "idConductor": str(doc["_id"])},
        ])
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", FakeColeccion([doc])), \
             patch.object(conductores, "coleccion_vehiculos", vehiculos_fake):
            r = cliente.get("/conductores/buscar", params={"q": "BUSCADA"})
        self.assertEqual(r.status_code, 200)
        cuenta = r.json()["cuentas"][0]
        self.assertEqual(cuenta["placas_propias"], 1)
        self.assertEqual(cuenta["placas_invitado"], 1)


class EliminarCuentaTests(unittest.TestCase):
    """(2026-10-19) Limpieza de cuentas BASURA (correo mal digitado, p.ej.
    PEREZ.FABIO@90): DESACTIVACIÓN (activo=False + sello), no borrado físico
    — desaparecen del listado (filtra inactivas) y el login las rechaza.
    Solo sin placas inscritas."""

    def test_desactiva_y_marca(self):
        doc = _cuenta("BASURA@90")
        fake = FakeColeccion([doc])
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", fake), \
             patch.object(conductores, "coleccion_vehiculos", FakeColeccion()):
            r = cliente.post(f"/conductores/{doc['_id']}/eliminar")
        self.assertEqual(r.status_code, 200, r.text)
        self.assertFalse(doc["activo"])                     # desactivada
        self.assertTrue(doc["eliminada_por_seguridad"])     # con sello
        self.assertIn("eliminada_fecha", doc)
        # Y ya no aparece en el listado de cuentas sin vehículo.
        with patch.object(conductores, "coleccion_conductores", fake), \
             patch.object(conductores, "coleccion_vehiculos", FakeColeccion()):
            r2 = cliente.get("/conductores/sin-vehiculo")
        self.assertEqual(r2.json()["total"], 0)

    def test_409_con_placas_inscritas(self):
        doc = _cuenta("CONFICHA@X.COM")
        vehiculos_fake = FakeColeccion(
            [{"_id": "v1", "placa": "ABC123", "idUsuario": str(doc["_id"])}])
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", FakeColeccion([doc])), \
             patch.object(conductores, "coleccion_vehiculos", vehiculos_fake):
            r = cliente.post(f"/conductores/{doc['_id']}/eliminar")
        self.assertEqual(r.status_code, 409)
        self.assertTrue(doc.get("activo", True))            # intacta

    def test_idempotente_si_ya_inactiva(self):
        doc = _cuenta("YA@X.COM", activo=False)
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", FakeColeccion([doc])), \
             patch.object(conductores, "coleccion_vehiculos", FakeColeccion()):
            r = cliente.post(f"/conductores/{doc['_id']}/eliminar")
        self.assertEqual(r.status_code, 200)


if __name__ == "__main__":
    unittest.main()
