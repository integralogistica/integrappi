"""Tests del ALTA POR SEGURIDAD (2026-09-28): crear conductores de las
placas históricas con hoja de vida física firmada, credenciales para
enviar, y aceptación de políticas en el primer ingreso."""
import sys
import types
import unittest
from datetime import datetime
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


def cliente_de_prueba(router) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


class _CursorFake(list):
    """Cursor mínimo: soporta sort/limit encadenados del listado."""

    def sort(self, clave, direccion=-1):
        try:
            return _CursorFake(sorted(self, key=lambda d: str(d.get(clave, "")),
                                      reverse=direccion < 0))
        except Exception:
            return self

    def limit(self, _n):
        return self


def _match_documento(d, filtro):
    """Match genérico: igualdad, $regex (con $options i) y $or (para /buscar)."""
    import re as _re
    for campo, esperado in (filtro or {}).items():
        if campo == "$or":
            if not any(_match_documento(d, cond) for cond in esperado):
                return False
            continue
        if campo == "_id":
            if str(d.get("_id")) != str(esperado):
                return False
            continue
        actual = d.get(campo)
        if isinstance(esperado, dict) and "$regex" in esperado:
            opciones = _re.IGNORECASE if "i" in esperado.get("$options", "") else 0
            if not _re.search(esperado["$regex"], str(actual or ""), opciones):
                return False
        elif isinstance(esperado, dict) and "$in" in esperado:
            if actual not in esperado["$in"]:
                return False
        elif isinstance(esperado, dict) and "$ne" in esperado:
            if actual == esperado["$ne"]:
                return False
        elif actual != esperado:
            return False
    return True


class FakeColeccion:
    def __init__(self, documentos=None):
        self.documents = list(documentos or [])
        self.contador = 0

    def find(self, filtro=None, *a, **k):
        return _CursorFake([d for d in self.documents if _match_documento(d, filtro)])

    def find_one(self, query, *a, **k):
        for d in self.documents:
            if _match_documento(d, query):
                return d
        return None

    def count_documents(self, filtro=None):
        return len([d for d in self.documents if _match_documento(d, filtro)])

    def insert_one(self, doc):
        self.contador += 1
        doc["_id"] = f"id-{self.contador}"
        self.documents.append(doc)
        return MagicMock(inserted_id=doc["_id"])

    def update_one(self, filtro, cambio):
        for d in self.documents:
            if _match_documento(d, filtro):
                if "$set" in cambio:
                    d.update(cambio["$set"])
                if "$push" in cambio:
                    for campo, valor in cambio["$push"].items():
                        items = (valor["$each"]
                                 if isinstance(valor, dict) and "$each" in valor
                                 else [valor])
                        actual = d.setdefault(campo, [])
                        if isinstance(actual, list):
                            actual.extend(items)
                            # $slice negativo: conserva solo los últimos N.
                            if isinstance(valor, dict) and isinstance(valor.get("$slice"), int) \
                                    and valor["$slice"] < 0:
                                del actual[:valor["$slice"]]


class AltaSeguridadTests(unittest.TestCase):

    def test_alta_crea_cuenta_verificada_con_clave(self):
        fake = FakeColeccion()
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", fake):
            r = cliente.post("/conductores/alta-seguridad", json={
                "correo": "nuevo@correo.com", "nombre": "PEDRO PEREZ",
                "perfil": "CONDUCTOR",
            })
        self.assertEqual(r.status_code, 200)
        cuerpo = r.json()
        self.assertTrue(cuerpo["clave"])  # clave para enviarle
        doc = fake.documents[0]
        self.assertTrue(doc["correo_verificado"])       # identidad verificada en persona
        self.assertTrue(doc["pendiente_aceptacion_politica"])  # él acepta al entrar
        self.assertTrue(doc["alta_por_seguridad"])

    def test_alta_envia_correo_con_credenciales(self):
        """Al crear la cuenta se le envía AL CONDUCTOR un correo con su
        usuario y clave (fire-and-forget; la clave igual se muestra una vez
        en pantalla como respaldo)."""
        fake = FakeColeccion()
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", fake), \
             patch.object(conductores, "enviar_correo_credenciales") as correo:
            r = cliente.post("/conductores/alta-seguridad", json={
                "correo": "nuevo@correo.com", "perfil": "TENEDOR",
                "creado_por": "EDWIN"})
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json()["credenciales_enviadas"])
        correo.assert_called_once()  # background task ejecutada por TestClient
        destino, clave = correo.call_args[0][0], correo.call_args[0][1]
        self.assertEqual(destino, "nuevo@correo.com")
        self.assertEqual(clave, r.json()["clave"])  # la MISMA que se muestra
        self.assertIn("TENEDOR", correo.call_args[0][2:])

    def test_alta_envia_whatsapp_con_credenciales(self):
        """Si el alta trae celular útil salen DOS plantillas: Utilidad
        ({{1}} = correo, el usuario) y Autenticación ({{1}} = la clave como
        código) — la clave nunca viaja dentro de la plantilla de Utilidad."""
        fake = FakeColeccion()
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", fake), \
             patch.object(conductores, "enviar_correo_credenciales"), \
             patch.object(conductores, "enviar_template_sync") as wa:
            r = cliente.post("/conductores/alta-seguridad", json={
                "correo": "nuevo@correo.com", "perfil": "CONDUCTOR",
                "celular": "+57 310 456 7890"})
        self.assertEqual(r.status_code, 200)
        self.assertIn("WhatsApp", r.json()["mensaje"])
        self.assertEqual(wa.call_count, 2)
        destino1, plantilla1, _, params1 = wa.call_args_list[0][0]
        destino2, plantilla2, _, params2 = wa.call_args_list[1][0]
        # ① Utilidad (bienvenida): aviso + usuario + botón al portal
        self.assertEqual((destino1, plantilla1), ("573104567890", "enruta_clave_cuenta"))
        self.assertEqual(params1, ["NUEVO@CORREO.COM"])
        # ② Autenticación: la clave como código de un solo uso — el botón
        # «Copiar código» viaja por la API como sub_type "url" con la clave
        # como parámetro (copy_code → 400 "must be of type Url").
        self.assertEqual((destino2, plantilla2), ("573104567890", "enruta_codigo_acceso"))
        self.assertEqual(params2, [r.json()["clave"]])
        botones = wa.call_args_list[1][1]["botones"]
        self.assertEqual(botones, [{"sub_type": "url",
                                    "parameters": [r.json()["clave"]]}])

    def test_alta_sin_celular_no_envia_whatsapp(self):
        fake = FakeColeccion()
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", fake), \
             patch.object(conductores, "enviar_correo_credenciales"), \
             patch.object(conductores, "enviar_template_sync") as wa:
            r = cliente.post("/conductores/alta-seguridad", json={
                "correo": "nuevo@correo.com"})
        self.assertEqual(r.status_code, 200)
        wa.assert_not_called()

    def test_normalizacion_celular_whatsapp(self):
        n = conductores._celular_whatsapp
        self.assertEqual(n("3104567890"), "573104567890")      # local CO
        self.assertEqual(n("+57 310 456 7890"), "573104567890")
        self.assertEqual(n("+1 555 123 4567"), "15551234567")  # otro país
        self.assertIsNone(n("12345"))                          # muy corto
        self.assertIsNone(None)                                # ausente

    def test_alta_registra_creador_y_se_lista(self):
        """El alta guarda quién creó la cuenta; el listado lo muestra (sin
        claves) con el estado de políticas."""
        fake = FakeColeccion()
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", fake):
            r = cliente.post("/conductores/alta-seguridad", json={
                "correo": "a@x.com", "nombre": "PEDRO", "creado_por": "EDWIN"})
            self.assertEqual(r.status_code, 200)
            self.assertEqual(fake.documents[0]["alta_por"], "EDWIN")

            fake.documents.append({  # cuenta NO creada por Seguridad: fuera
                "_id": "otro", "correo": "VIEJO@X.COM", "nombre": "VIEJO",
                "perfil": "CONDUCTOR"})
            r2 = cliente.get("/conductores/alta-seguridad/listar")
        cuentas = r2.json()["cuentas"]
        self.assertEqual(len(cuentas), 1)  # solo la del alta
        self.assertEqual(cuentas[0]["creado_por"], "EDWIN")
        self.assertTrue(cuentas[0]["politicas_pendientes"])
        self.assertNotIn("clave", cuentas[0])  # JAMÁS la clave

    def test_regenerar_clave(self):
        """La clave perdida se rescata generando una NUEVA (invalida la
        anterior); solo aplica a cuentas de alta por Seguridad."""
        from rutas.conductores import verificar_clave, crear_hash
        oid_hex = "507f1f77bcf86cd799439012"
        doc = {"_id": oid_hex, "correo": "A@X.COM", "nombre": "PEDRO",
               "perfil": "CONDUCTOR", "clave": crear_hash("vieja123"),
               "alta_por_seguridad": True}
        fake = FakeColeccion([doc])
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", fake):
            r = cliente.post(f"/conductores/alta-seguridad/{oid_hex}/nueva-clave")
        self.assertEqual(r.status_code, 200)
        nueva = r.json()["clave"]
        self.assertNotEqual(nueva, "vieja123")
        self.assertTrue(verificar_clave(nueva, fake.documents[0]["clave"]))
        # Cuenta ajena al alta → 404.
        with patch.object(conductores, "coleccion_conductores", fake):
            r2 = cliente.post("/conductores/alta-seguridad/no-existe/nueva-clave")
        self.assertEqual(r2.status_code, 404)

    def test_alta_correo_duplicado_400(self):
        fake = FakeColeccion([{"_id": "1", "correo": "YA@X.COM", "clave": "h"}])
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", fake), \
             patch.object(conductores, "_existe_correo", lambda c: True):
            r = cliente.post("/conductores/alta-seguridad", json={
                "correo": "ya@x.com", "nombre": "X"})
        self.assertEqual(r.status_code, 400)

    def test_login_reporta_politicas_pendientes(self):
        doc = {"_id": "1", "correo": "A@X.COM", "clave": conductores.crear_hash("clave123"),
               "perfil": "CONDUCTOR", "activo": True, "correo_verificado": True,
               "nombre": "PEDRO", "pendiente_aceptacion_politica": True}
        fake = FakeColeccion([doc])
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", fake):
            r = cliente.post("/conductores/login",
                             json={"usuario": "a@x.com", "clave": "clave123"})
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json()["politicas_pendientes"])

    def test_aceptar_politica_sesion(self):
        oid_hex = "507f1f77bcf86cd799439011"  # ObjectId válido (el endpoint convierte)
        doc = {"_id": oid_hex, "correo": "A@X.COM", "clave": "h", "nombre": "PEDRO",
               "perfil": "CONDUCTOR", "activo": True, "correo_verificado": True,
               "pendiente_aceptacion_politica": True}
        fake = FakeColeccion([doc])
        politica = {"_id": "p1", "version": 2,
                    "declaraciones": [{"id": "sarlaft", "titulo": "SARLAFT", "texto_html": "x"}]}
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", fake), \
             patch.object(conductores, "coleccion_aceptaciones", FakeColeccion()), \
             patch.object(conductores, "_politica_vigente", lambda: politica):
            r = cliente.post("/conductores/aceptar-politica-sesion", json={
                "conductor_id": oid_hex, "declaraciones_aceptadas": ["sarlaft"]})
        self.assertEqual(r.status_code, 200)
        self.assertFalse(fake.documents[0]["pendiente_aceptacion_politica"])
        self.assertTrue(fake.documents[0]["aceptacion_politica"])


class AltaVehiculoTests(unittest.TestCase):
    """Alta de VEHÍCULO (2026-10-01): buscar cuentas para vincular,
    impersonación sin tocar clave y consulta de placa antes del alta."""

    def test_buscar_por_correo_nombre_o_cedula_sin_clave(self):
        docs = [
            {"_id": "1", "nombre": "PEDRO PEREZ", "correo": "PEDRO@X.COM",
             "perfil": "TENEDOR", "cedula": "1020304050", "clave": "secreto"},
            {"_id": "2", "nombre": "JUAN LOPEZ", "correo": "JUAN@X.COM",
             "perfil": "CONDUCTOR", "cedula": "9876543210"},
        ]
        fake = FakeColeccion(docs)
        veh_fake = FakeColeccion([{"idUsuario": "1"}, {"idUsuario": "1"}])
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", fake), \
             patch.object(conductores, "coleccion_vehiculos", veh_fake):
            r1 = cliente.get("/conductores/buscar", params={"q": "pedro"})
            r2 = cliente.get("/conductores/buscar", params={"q": "1020"})
            r3 = cliente.get("/conductores/buscar", params={"q": "lopez"})
            r4 = cliente.get("/conductores/buscar", params={"q": "pe"})
        cuentas = r1.json()["cuentas"]
        self.assertEqual(len(cuentas), 1)               # por correo Y nombre, sin duplicar
        self.assertEqual(cuentas[0]["correo"], "PEDRO@X.COM")
        self.assertEqual(cuentas[0]["placas_propias"], 2)
        self.assertNotIn("clave", cuentas[0])           # JAMÁS la clave
        self.assertEqual(len(r2.json()["cuentas"]), 1)  # por cédula
        self.assertEqual(len(r3.json()["cuentas"]), 1)  # por nombre
        self.assertEqual(r4.json()["cuentas"], [])      # < 3 caracteres

    def test_login_como_audita_y_no_toca_clave(self):
        oid_hex = "507f1f77bcf86cd799439012"
        doc = {"_id": oid_hex, "correo": "A@X.COM", "nombre": "PEDRO PEREZ",
               "perfil": "TENEDOR", "activo": True, "clave": "hash",
               "pendiente_aceptacion_politica": True}
        fake = FakeColeccion([doc])
        imp = FakeColeccion()
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", fake), \
             patch.object(conductores, "coleccion_impersonaciones", imp):
            r = cliente.post(f"/conductores/login-como/{oid_hex}",
                             json={"solicitante": "EDWIN"})
        self.assertEqual(r.status_code, 200)
        cuerpo = r.json()
        self.assertEqual(cuerpo["usuario"]["correo"], "A@X.COM")   # shape de /login
        self.assertEqual(cuerpo["usuario"]["primerNombre"], "PEDRO")
        self.assertTrue(cuerpo["politicas_pendientes"])
        self.assertEqual(fake.documents[0]["clave"], "hash")       # clave INTACTA
        self.assertEqual(imp.documents[0]["solicitante"], "EDWIN")  # auditoría
        self.assertEqual(imp.documents[0]["conductor_id"], oid_hex)

    def test_login_como_rechaza_stub_y_404(self):
        oid_hex = "507f1f77bcf86cd799439013"
        stub = {"_id": oid_hex, "correo": "S@X.COM", "perfil": "CONDUCTOR",
                "activo": False}
        fake = FakeColeccion([stub])
        imp = FakeColeccion()
        cliente = cliente_de_prueba(conductores.ruta_conductores)
        with patch.object(conductores, "coleccion_conductores", fake), \
             patch.object(conductores, "coleccion_impersonaciones", imp):
            r_stub = cliente.post(f"/conductores/login-como/{oid_hex}",
                                  json={"solicitante": "EDWIN"})
            r_404 = cliente.post("/conductores/login-como/no-existe",
                                 json={"solicitante": "EDWIN"})
        self.assertEqual(r_stub.status_code, 403)
        self.assertEqual(r_404.status_code, 404)
        self.assertEqual(imp.documents, [])  # nada auditado en rechazos

    def test_consultar_placa(self):
        oid_hex = "507f1f77bcf86cd799439014"
        veh = FakeColeccion([
            {"_id": "v1", "placa": "ABC123", "estadoIntegra": "aprobado",
             "idUsuario": oid_hex},
        ])
        cuentas = FakeColeccion([
            {"_id": oid_hex, "nombre": "PEDRO PEREZ", "correo": "PEDRO@X.COM",
             "perfil": "TENEDOR"},
        ])
        cliente = cliente_de_prueba(vehiculos.ruta_vehiculos)
        with patch.object(vehiculos, "coleccion_vehiculos", veh), \
             patch.object(vehiculos, "coleccion_conductores_cuenta", cuentas):
            r1 = cliente.get("/vehiculos/consultar-placa/abc123")  # normaliza
            r2 = cliente.get("/vehiculos/consultar-placa/ZZZ999")
        d1 = r1.json()
        self.assertTrue(d1["existe"])
        self.assertEqual(d1["estadoIntegra"], "aprobado")
        self.assertEqual(d1["duenio"]["correo"], "PEDRO@X.COM")
        d2 = r2.json()
        self.assertFalse(d2["existe"])
        self.assertIsNone(d2["duenio"])


class HojaVidaFisicaTests(unittest.TestCase):

    def test_hoja_de_vida_NO_requerida(self):
        completo = {
            "tarjetaPropiedad": "u1", "tarjetaPropiedadReverso": "u1r", "soat": "u2",
            "revisionTecnomecanica": "u3",
            "documentoIdentidadConductor": "u6", "documentoIdentidadConductorReverso": "u6r",
            "documentoIdentidadPropietario": "u7", "documentoIdentidadPropietarioReverso": "u7r",
            "documentoIdentidadTenedor": "u8", "documentoIdentidadTenedorReverso": "u8r",
            "licencia": "u9", "licenciaReverso": "u9r", "planillaEpsArl": "u10",
            "condFoto": "u11", "condCertificacionBancaria": "u12",
            "tenedCertificacionBancaria": "u14", "documentoAcreditacionTenedor": "u15",
            "rutTenedor": "u16", "fotos": ["f1"],
        }
        # (2026-10-01) La HV física la sube Seguridad en /revision solo en los
        # casos históricos: SIN ella el vehículo ya puede finalizar.
        self.assertEqual(vehiculos._documentos_faltantes(dict(completo)), [])

    def test_tipo_valido_en_subida(self):
        self.assertIn("hojaVidaFisica", vehiculos.ETIQUETAS_DOCUMENTO)
        self.assertIn("Hoja de Vida Física",
                      vehiculos.ETIQUETAS_DOCUMENTO["hojaVidaFisica"])
        self.assertIn("hojaVidaFisica", vehiculos.CAMPOS_DOCUMENTO_PROTEGIDOS)


if __name__ == "__main__":
    unittest.main()
