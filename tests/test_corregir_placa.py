"""
Tests de CORREGIR PLACA por Seguridad/Admin (2026-10-15): renombrar la placa
de un vehículo con error de digitación sin perder historial ni romper la
verificación de la firma electrónica (el sello guarda `placa_firmada`).
"""
import sys
import types
import unittest
from contextlib import ExitStack
from datetime import datetime
from unittest.mock import patch

from bson import ObjectId
from fastapi.testclient import TestClient

# Stub de BD (patrón del repo): correr standalone sin Atlas.
if "bd.bd_cliente" not in sys.modules:
    _stub = types.ModuleType("bd.bd_cliente")

    class _BDFake(dict):
        def __getitem__(self, clave):
            from unittest.mock import MagicMock
            return self.setdefault(clave, MagicMock())

    _stub.bd_cliente = _BDFake()
    sys.modules["bd.bd_cliente"] = _stub

from rutas import conductores, vehiculos


class FakeColeccion:
    """Igualdad, $ne, $or, $regex + update_one/update_many con $set."""

    def __init__(self, documentos=None):
        self.documents = list(documentos or [])

    def find_one(self, query=None, *a, **k):
        if query is None:
            return self.documents[0] if self.documents else None
        for d in self.documents:
            if self._match(d, query):
                return d
        return None

    @classmethod
    def _match(cls, doc, query):
        for k, v in query.items():
            if k == "$or":
                if not any(cls._match(doc, sub) for sub in v):
                    return False
                continue
            if isinstance(v, dict):
                import re as _re
                valor = doc.get(k)
                if "$regex" in v:
                    flags = _re.IGNORECASE if "i" in v.get("$options", "") else 0
                    if not _re.match(v["$regex"], str(valor or ""), flags):
                        return False
                elif "$ne" in v:
                    if valor == v["$ne"]:
                        return False
                else:
                    return False
            elif doc.get(k) != v:
                return False
        return True

    def _aplicar_set(self, filtro, cambio):
        for d in self.documents:
            if self._match(d, filtro):
                for k, v in cambio.get("$set", {}).items():
                    if "." in k:
                        raiz, hijo = k.split(".", 1)
                        d.setdefault(raiz, {})[hijo] = v
                    else:
                        d[k] = v

    def insert_one(self, doc):
        doc = dict(doc)
        doc.setdefault("_id", ObjectId())
        self.documents.append(doc)

        class R:
            inserted_id = doc["_id"]
        return R()

    def update_one(self, filtro, cambio):
        self._aplicar_set(filtro, cambio)

    def update_many(self, filtro, cambio):
        self._aplicar_set(filtro, cambio)

    def count_documents(self, query=None):
        return len([d for d in self.documents if self._match(d, query or {})])


def _cliente():
    from fastapi import FastAPI
    app = FastAPI()
    app.include_router(vehiculos.ruta_vehiculos)
    return TestClient(app, raise_server_exceptions=False)


VIEJA, NUEVA = "SPL76", "SPL276"
CUENTA_SEGURIDAD = {"_id": ObjectId(), "usuario": "DIANA SEGURIDAD",
                    "nombre": "Diana Seguridad", "perfil": "SEGURIDAD"}
CUENTA_PADRINO = {"_id": ObjectId(), "usuario": "PADRINO JUAN",
                  "nombre": "Padrino Juan", "perfil": "PADRINO"}
CUENTA_OPERATIVO = {"_id": ObjectId(), "usuario": "PEDRO OP",
                    "nombre": "Pedro Op", "perfil": "OPERATIVO"}


def _vehiculo():
    return {
        "placa": VIEJA, "estadoIntegra": "aprobado", "idUsuario": "ten-1",
        "idConductor": "cond-1", "condNombres": "ANA",
        "condCedulaCiudadania": "111", "condCorreo": "ana@correo.com",
        "invitacionConductor": {"correo": "UNO@correo.com", "estado": "aceptada"},
        "invitacionConductor2": None,
    }


class CorregirPlacaTests(unittest.TestCase):

    def setUp(self):
        self.vehiculos = FakeColeccion([_vehiculo()])
        self.firmas = FakeColeccion([
            {"placa": VIEJA, "slot": 1, "version": 1, "hash_datos": "h1",
             "firmado_en": datetime(2026, 10, 1)},
        ])
        self.disponibilidades = FakeColeccion([
            {"placa": VIEJA, "fecha": "2026-10-15", "estado": "activa"},
        ])
        self.asignaciones = FakeColeccion([
            {"placa": VIEJA, "fecha": "2026-10-10", "estado": "devuelta"},
        ])

    def _llamar(self, placa_nueva=NUEVA, actor="Diana Seguridad", via="seguridad",
                placa=VIEJA, extra_vehiculos=None, cuentas=None):
        docs = list(self.vehiculos.documents) + list(extra_vehiculos or [])
        auditoria, padrinos, aviso = [], [], []
        with ExitStack() as stack:
            for p in [
                patch.object(vehiculos, "coleccion_vehiculos", FakeColeccion(docs)),
                patch.object(vehiculos, "coleccion_firmas", self.firmas),
                patch.object(vehiculos, "coleccion_disponibilidades", self.disponibilidades),
                patch.object(vehiculos, "coleccion_asignaciones", self.asignaciones),
                patch.object(vehiculos, "coleccion_baseusuarios",
                             cuentas if cuentas is not None else FakeColeccion([CUENTA_SEGURIDAD])),
                patch.object(vehiculos, "_registrar_auditoria", side_effect=lambda *a, **k: auditoria.append((a, k))),
                patch.object(vehiculos, "_notificar_padrinos", side_effect=lambda *a, **k: padrinos.append((a, k))),
                patch.object(vehiculos, "_avisar_correccion_placa", side_effect=lambda *a, **k: aviso.append(a)),
            ]:
                stack.enter_context(p)
            r = _cliente().put("/vehiculos/corregir-placa", data={
                "placa": placa, "placa_nueva": placa_nueva,
                "editado_por": actor, "via": via,
            })
            self._auditoria, self._padrinos = auditoria, padrinos
            # Recuperar el documento renombrado para las aserciones.
            self._veh = next((d for d in docs if d.get("placaAnterior") == VIEJA
                              or d.get("placa") == VIEJA), None)
            self._docs = docs
            return r

    # ── Caso feliz ──────────────────────────────────────────────────────────

    def test_corrige_y_deja_trazabilidad(self):
        r = self._llamar()
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["placa"], NUEVA)
        self.assertEqual(self._veh["placa"], NUEVA)
        self.assertEqual(self._veh["placaAnterior"], VIEJA)
        self.assertIn("placaCorregidaEn", self._veh)
        # Colecciones hermanas re-etiquetadas; firmas sellan la placa vieja.
        self.assertEqual(self.firmas.documents[0]["placa"], NUEVA)
        self.assertEqual(self.firmas.documents[0]["placa_firmada"], VIEJA)
        self.assertEqual(self.disponibilidades.documents[0]["placa"], NUEVA)
        self.assertEqual(self.asignaciones.documents[0]["placa"], NUEVA)
        # Auditoría sobre la placa NUEVA con el detalle vieja → nueva.
        args, kwargs = self._auditoria[0]
        self.assertEqual(args[0], NUEVA)
        self.assertEqual(args[1], "placa_corregida")
        self.assertEqual(args[3] if len(args) > 3 else kwargs.get("detalle"), f"{VIEJA} → {NUEVA}")
        # Avisos a padrinos con la placa nueva.
        self.assertEqual(self._padrinos[0][0][0], NUEVA)

    def test_mantiene_estado(self):
        """Corregir un typo NO baja a re-revisión: el aprobado sigue aprobado."""
        self._llamar()
        self.assertEqual(self._veh["estadoIntegra"], "aprobado")
        self.assertNotIn("fechaEstado", self._veh)

    def test_minusculas_se_normalizan(self):
        r = self._llamar(placa_nueva="spl276")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["placa"], NUEVA)

    # ── Validaciones ────────────────────────────────────────────────────────

    def test_rechazos_400_404_409(self):
        self.assertEqual(self._llamar(placa_nueva="AB 123").status_code, 400)   # espacio
        self.assertEqual(self._llamar(placa_nueva="ABCDEFGHIJ").status_code, 400)  # > 7
        self.assertEqual(self._llamar(placa_nueva=VIEJA).status_code, 400)      # igual
        self.assertEqual(self._llamar(placa="NADIE00").status_code, 404)        # no existe
        ocupada = self._llamar(extra_vehiculos=[{"placa": NUEVA, "estadoIntegra": "aprobado"}])
        self.assertEqual(ocupada.status_code, 409)
        self.assertIn("ya está registrada", ocupada.json()["detail"])

    def test_invitacion_pendiente_en_respuesta(self):
        self.vehiculos.documents[0]["invitacionConductor2"] = {
            "correo": "dos@correo.com", "estado": "pendiente"}
        self.assertTrue(self._llamar().json()["invitacion_pendiente"])
        # Sin invitaciones pendientes → False.
        self.setUp()
        self.assertFalse(self._llamar().json()["invitacion_pendiente"])

    # ── Guards ──────────────────────────────────────────────────────────────

    def test_guards_de_perfil(self):
        # Sin via=seguridad → 403 siempre.
        self.assertEqual(self._llamar(via="conductor").status_code, 403)
        # PADRINO → 403.
        r = self._llamar(actor="Padrino Juan", cuentas=FakeColeccion([CUENTA_PADRINO]))
        self.assertEqual(r.status_code, 403)
        # Un perfil de operación (p. ej. OPERATIVO) tampoco puede.
        r = self._llamar(actor="Pedro Op", cuentas=FakeColeccion([CUENTA_OPERATIVO]))
        self.assertEqual(r.status_code, 403)
        # Actor que no resuelve en baseusuarios pasa (identidad declarada).
        self.assertEqual(self._llamar(actor="Alguien Random").status_code, 200)


class VerificarFirmaTrasCorreccionTests(unittest.TestCase):
    """La placa forma parte del hash de la firma → tras corregirla, la
    verificación debe recalcular con la placa SELLADA (`placa_firmada`)."""

    def setUp(self):
        self.vehiculo = _vehiculo()
        # Slot 2 con su HV para el hash del segundo conductor.
        self.vehiculo.update({"idConductor2": "cond-2", "cond2Nombres": "BETO",
                              "cond2CedulaCiudadania": "222"})

    def _corregir(self):
        firmas = FakeColeccion(self._firmas_docs)
        with ExitStack() as stack:
            for p in [
                patch.object(vehiculos, "coleccion_vehiculos", FakeColeccion([self.vehiculo])),
                patch.object(vehiculos, "coleccion_firmas", firmas),
                patch.object(vehiculos, "coleccion_disponibilidades", FakeColeccion()),
                patch.object(vehiculos, "coleccion_asignaciones", FakeColeccion()),
                patch.object(vehiculos, "coleccion_baseusuarios", FakeColeccion([CUENTA_SEGURIDAD])),
                patch.object(vehiculos, "_registrar_auditoria"),
                patch.object(vehiculos, "_notificar_padrinos"),
                patch.object(vehiculos, "_avisar_correccion_placa"),
            ]:
                stack.enter_context(p)
            r = _cliente().put("/vehiculos/corregir-placa", data={
                "placa": VIEJA, "placa_nueva": NUEVA,
                "editado_por": "Diana Seguridad", "via": "seguridad",
            })
        self.assertEqual(r.status_code, 200)

    def _verificar(self, slot, firmas):
        import asyncio
        with patch.object(vehiculos, "coleccion_vehiculos", FakeColeccion([self.vehiculo])), \
             patch.object(vehiculos, "coleccion_firmas", firmas), \
             patch.object(vehiculos, "_firmar_documentos", side_effect=lambda x: x):
            return asyncio.run(vehiculos.verificar_firma(NUEVA, slot=slot))

    def test_slot1_sigue_coincidiendo(self):
        hash_firmado = vehiculos._hash_datos_firmados(self.vehiculo)
        self._firmas_docs = [{"placa": VIEJA, "slot": 1, "hash_datos": hash_firmado,
                              "firmado_en": datetime(2026, 10, 1)}]
        self._corregir()
        self.assertEqual(self.vehiculo["placa"], NUEVA)
        resultado = self._verificar(slot=1, firmas=FakeColeccion(self._firmas_docs))
        self.assertTrue(resultado["coincide"])

    def test_slot2_sigue_coincidiendo(self):
        hash_firmado = vehiculos._hash_datos_firmados_slot2(self.vehiculo)
        self._firmas_docs = [{"placa": VIEJA, "slot": 2, "hash_datos": hash_firmado,
                              "firmado_en": datetime(2026, 10, 1)}]
        self._corregir()
        resultado = self._verificar(slot=2, firmas=FakeColeccion(self._firmas_docs))
        self.assertTrue(resultado["coincide"])

    def test_sin_sello_la_verificacion_romperia(self):
        """Documenta el PORQUÉ del sello: una evidencia SIN `placa_firmada`
        (escenario hipotético sin el fix) reporta «datos cambiaron» en falso
        tras corregir la placa, aunque nada más haya cambiado."""
        hash_firmado = vehiculos._hash_datos_firmados(self.vehiculo)
        self._firmas_docs = []  # el rename no necesita evidencias
        self._corregir()  # renombra el documento del vehículo
        firmas = FakeColeccion([{"placa": NUEVA, "slot": 1,
                                 "hash_datos": hash_firmado,
                                 "firmado_en": datetime(2026, 10, 1)}])  # sin sello
        resultado = self._verificar(slot=1, firmas=firmas)
        self.assertFalse(resultado["coincide"])


class AceptarInvitacionPlacaAnteriorTests(unittest.TestCase):
    """El link de invitación lleva ?placa= del momento del envío: si Seguridad
    corrigió la placa después, aceptar resuelve por `placaAnterior`."""

    def setUp(self):
        self.stub_id = ObjectId()
        self.cuentas = FakeColeccion([{
            "_id": self.stub_id, "correo": "DOS@correo.com", "perfil": "CONDUCTOR",
            "nombre": "BETO", "activo": False, "correo_verificado": False,
        }])
        self.vehiculo = _vehiculo()
        self.vehiculo.update({
            "placa": NUEVA, "placaAnterior": VIEJA,
            "idConductor2": None,
            "invitacionConductor2": {"correo": "DOS@correo.com", "estado": "pendiente"},
        })
        politicas = FakeColeccion([{
            "_id": ObjectId(), "version": 1, "titulo": "T", "texto_html": "<p>x</p>",
            "activo": True, "declaraciones": conductores.DECLARACIONES_V2,
        }])
        self.patches = [
            patch.object(conductores, "coleccion_conductores", self.cuentas),
            patch.object(conductores, "coleccion_vehiculos", FakeColeccion([self.vehiculo])),
            patch.object(conductores, "coleccion_politicas", politicas),
            patch.object(conductores, "coleccion_aceptaciones", FakeColeccion()),
            patch.object(conductores, "_pos_cambio_conductor"),
            patch.object(vehiculos, "_disparar_estudios_seguridad"),
        ]

    def _aceptar(self, placa):
        import asyncio
        from rutas.conductores import AceptarInvitacionInput
        ids = [d["id"] for d in conductores.DECLARACIONES_V2]
        datos = AceptarInvitacionInput(
            token="tok", placa=placa, clave="secreta1", version_politica=1,
            acepta=True, declaraciones_aceptadas=ids, slot=2)
        with patch.object(conductores, "_buscar_conductor_por_token",
                          return_value=self.cuentas.documents[0]):
            with ExitStack() as stack:
                for p in self.patches:
                    stack.enter_context(p)
                return asyncio.run(conductores.aceptar_invitacion(datos, _FakeRequest()))

    def test_link_con_placa_vieja_resuelve(self):
        resultado = self._aceptar(placa=VIEJA)
        self.assertEqual(resultado["slot"], 2)
        self.assertEqual(self.vehiculo["idConductor2"], str(self.stub_id))

    def test_placa_inexistente_sigue_404(self):
        from fastapi import HTTPException
        with self.assertRaises(HTTPException) as ctx:
            self._aceptar(placa="NADIE00")
        self.assertEqual(ctx.exception.status_code, 404)


class _FakeRequest:
    def __init__(self):
        self.client = type("C", (), {"host": "127.0.0.1"})()
        self.headers = {"user-agent": "test-agent"}


if __name__ == "__main__":
    unittest.main()
