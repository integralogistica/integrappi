# -*- coding: utf-8 -*-
"""Tests de la regla 2026-10-14 (caso real QVK730 + pedido del usuario):
eliminar un documento REQUERIDO inhabilita el vehículo (en_actualizacion)
hasta que se vuelva a subir.

1. Eliminar la planilla limpia SUS residuos (entrada del historial con blob
   muerto + planillaVencimiento stale) — antes quedaba listada como VENCIDA
   para siempre aunque ya no existiera.
2. Eliminar doc exigido con el vehículo en completado_revision/aprobado →
   en_actualizacion + actualizacionOrigen (el estado al que volver) +
   observación visible para el conductor.
3. Re-subir la PLANILLA rehabilita solo (es el doc de renovación mensual sin
   re-revisión) y SOLO si la actualización fue por eliminación (la que pide
   Seguridad no se auto-restaura) y ya no falta nada más.
4. actualizar-estado limpia actualizacionOrigen al salir de en_actualizacion.
"""
import sys
import types
import unittest
from datetime import date, timedelta
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

from rutas import vehiculos


def cliente_de_prueba() -> TestClient:
    app = FastAPI()
    app.include_router(vehiculos.ruta_vehiculos)
    return TestClient(app, raise_server_exceptions=False)


TODOS_DOCS = {
    "tarjetaPropiedad": "https://x/1", "tarjetaPropiedadReverso": "https://x/1r", "soat": "https://x/2",
    "revisionTecnomecanica": "https://x/3",
    "documentoIdentidadConductor": "https://x/6",
    "documentoIdentidadConductorReverso": "https://x/6r",
    "documentoIdentidadPropietario": "https://x/7", "documentoIdentidadPropietarioReverso": "https://x/7r",
    "documentoIdentidadTenedor": "https://x/8", "documentoIdentidadTenedorReverso": "https://x/8r",
    "licencia": "https://x/9", "licenciaReverso": "https://x/9r", "planillaEpsArl": "https://x/10",
    "condFoto": "https://x/11",
    "condCertificacionBancaria": "https://x/12",
    "tenedCertificacionBancaria": "https://x/14", "documentoAcreditacionTenedor": "https://x/15",
    "rutTenedor": "https://x/16",
    "fotos": ["https://x/f1"],
    # Fotos del vehículo POR ÁNGULO (2026-10-14): los 4 exigidos.
    "fotosVehiculo": [
        {"tipo": t, "ruta": f"https://x/foto_{t}"}
        for t in ("frontal", "lateral_izq", "lateral_der", "posterior")
    ],
}


def vehiculo_completo(**extra):
    doc = {
        "placa": "TEST01", "estadoIntegra": "completado_revision",
        "condCedulaCiudadania": "123", "propDocumento": "123", "tenedDocumento": "123",
        "vehCapacidadCarga": "8000", "vehModelo": "2015",
        **TODOS_DOCS,
    }
    doc.update(extra)
    return doc


class FakeColeccion:
    """Falsa colección vehiculos: find_one por placa + update_one con
    $set/$unset/$push ($each/$position/$slice — el orden lo verifican las
    rutas, no el índice)/$pull."""

    def __init__(self, documentos=None):
        self.documents = list(documentos or [])

    def find_one(self, query, *a, **k):
        for d in self.documents:
            if d.get("placa") == query.get("placa"):
                return d
        return None

    def update_one(self, filtro, cambio, upsert=False):
        for d in self.documents:
            if d.get("placa") != filtro.get("placa"):
                continue
            for k, v in (cambio.get("$set") or {}).items():
                d[k] = v
            for k in (cambio.get("$unset") or {}):
                d.pop(k, None)
            for campo, valor in (cambio.get("$push") or {}).items():
                entradas = valor["$each"] if isinstance(valor, dict) and "$each" in valor else [valor]
                d.setdefault(campo, []).extend(entradas)
            for campo, cond in (cambio.get("$pull") or {}).items():
                d[campo] = [e for e in d.get(campo, []) if not self._matchea(e, cond)]
            return

    @staticmethod
    def _matchea(entrada, cond):
        return all(entrada.get(k) == v for k, v in cond.items()) if isinstance(entrada, dict) else entrada == cond


class FakeDisponibilidades:
    def __init__(self):
        self.updates = []

    def update_many(self, filtro, cambio):
        self.updates.append((filtro, cambio))


class EliminacionTests(unittest.TestCase):

    def setUp(self):
        self.client = cliente_de_prueba()

    def _eliminar(self, tipo):
        return self.client.delete(f"/vehiculos/eliminar-documento?placa=TEST01&tipo={tipo}")

    def _run(self, fake, accion):
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "coleccion_disponibilidades", FakeDisponibilidades()), \
             patch.object(vehiculos, "eliminar_de_google_storage") as mock_del, \
             patch.object(vehiculos, "enviar_notificacion_seguridad"), \
             patch.object(vehiculos, "_disparar_estudios_seguridad"):
            resp = accion()
        return resp, mock_del

    def test_eliminar_planilla_limpia_historial_y_vencimiento(self):
        """Caso QVK730: la entrada del historial apuntaba a un blob borrado y
        planillaVencimiento quedaba stale (VENCIDA para siempre)."""
        fake = FakeColeccion([vehiculo_completo(
            planillaVencimiento="2026-10-09",
            documentosPlanillaSegSocial=[
                {"ruta": "https://x/10", "fecha": "vieja", "nombre": "octubre.pdf", "vence": "2026-10-09"},
                {"ruta": "https://x/previa", "fecha": "mas vieja", "nombre": "septiembre.pdf", "vence": "2026-09-09"},
            ],
        )])
        resp, _ = self._run(fake, lambda: self._eliminar("planillaEpsArl"))
        self.assertEqual(resp.status_code, 200, resp.text)
        doc = fake.documents[0]
        self.assertNotIn("planillaEpsArl", doc)            # espejo fuera
        self.assertNotIn("planillaVencimiento", doc)       # vencimiento stale fuera
        rutas = [h["ruta"] for h in doc["documentosPlanillaSegSocial"]]
        self.assertEqual(rutas, ["https://x/previa"])      # SOLO la entrada muerta se va

    def test_eliminar_doc_requerido_en_revision_pasa_a_actualizacion(self):
        fake = FakeColeccion([vehiculo_completo()])
        resp, _ = self._run(fake, lambda: self._eliminar("soat"))
        self.assertEqual(resp.status_code, 200, resp.text)
        doc = fake.documents[0]
        self.assertEqual(doc["estadoIntegra"], "en_actualizacion")
        self.assertEqual(doc["actualizacionOrigen"], "completado_revision")
        self.assertIn("SOAT", doc["observaciones"])        # el conductor ve qué falta
        auditoria = [a for a in doc.get("auditoriaVehiculo", []) if a["accion"] == "estado_en_actualizacion"]
        self.assertEqual(len(auditoria), 1)

    def test_eliminar_doc_requerido_aprobado_recuerda_el_origen(self):
        """Aunque la eliminación baja el aprobado a re-revisión
        (_registrar_cambio_aprobado corre antes), el origen que se recuerda es
        el APROBADO original."""
        fake = FakeColeccion([vehiculo_completo(estadoIntegra="aprobado")])
        resp, _ = self._run(fake, lambda: self._eliminar("soat"))
        self.assertEqual(resp.status_code, 200, resp.text)
        doc = fake.documents[0]
        self.assertEqual(doc["estadoIntegra"], "en_actualizacion")
        self.assertEqual(doc["actualizacionOrigen"], "aprobado")

    def test_eliminar_doc_opcional_NO_pasa_a_actualizacion(self):
        """Sin documentación exigida faltante no hay inhabilitación: el
        opcional solo genera la re-revisión de siempre (si era aprobado)."""
        fake = FakeColeccion([vehiculo_completo(
            estadoIntegra="aprobado", polizaResponsabilidad="https://x/5")])
        resp, _ = self._run(fake, lambda: self._eliminar("polizaResponsabilidad"))
        self.assertEqual(resp.status_code, 200, resp.text)
        doc = fake.documents[0]
        self.assertEqual(doc["estadoIntegra"], "completado_revision")  # re-revisión, no actualización
        self.assertNotIn("actualizacionOrigen", doc)

    def test_eliminar_doc_en_registro_incompleto_no_toca_estado(self):
        fake = FakeColeccion([vehiculo_completo(estadoIntegra="registro_incompleto")])
        resp, _ = self._run(fake, lambda: self._eliminar("soat"))
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(fake.documents[0]["estadoIntegra"], "registro_incompleto")


class RehabilitacionPorPlanillaTests(unittest.TestCase):
    """Re-subir la planilla rehabilita al vehículo inhabilitado POR ELIMINARLA
    (pedido del usuario: "inhabilitado hasta que se lo vuelvan a subir")."""

    def setUp(self):
        self.client = cliente_de_prueba()
        self.vence = (date.today() + timedelta(days=10)).isoformat()

    def _subir_planilla(self):
        return self.client.put(
            "/vehiculos/subir-documento",
            data={"placa": "TEST01", "tipo": "planillaEpsArl", "extraer": "false",
                  "fecha_vencimiento": self.vence},
            files={"archivo": ("planilla.pdf", b"pdf", "application/pdf")},
        )

    def _run_subida(self, fake):
        with patch.object(vehiculos, "coleccion_vehiculos", fake), \
             patch.object(vehiculos, "coleccion_disponibilidades", FakeDisponibilidades()), \
             patch.object(vehiculos, "subir_a_google_storage",
                          side_effect=lambda a, n: f"Vehiculos/{n}"), \
             patch.object(vehiculos, "enviar_notificacion_seguridad"), \
             patch.object(vehiculos, "_disparar_estudios_seguridad"):
            return self._subir_planilla()

    def test_rehabilita_al_aprobado_que_elimino_la_planilla(self):
        doc = vehiculo_completo(estadoIntegra="en_actualizacion", actualizacionOrigen="aprobado")
        del doc["planillaEpsArl"]  # lo que falta es JUSTO la planilla
        fake = FakeColeccion([doc])
        resp = self._run_subida(fake)
        self.assertEqual(resp.status_code, 200, resp.text)
        doc = fake.documents[0]
        self.assertEqual(doc["estadoIntegra"], "aprobado")   # vuelve a la bolsa
        self.assertEqual(doc["observaciones"], "")           # sin observación pegada
        self.assertNotIn("actualizacionOrigen", doc)
        auditoria = [a for a in doc.get("auditoriaVehiculo", []) if a["accion"] == "estado_aprobado"]
        self.assertEqual(len(auditoria), 1)

    def test_NO_rehabilita_la_actualizacion_pedida_por_seguridad(self):
        """«Actualizar datos» de Seguridad (sin actualizacionOrigen) la cierra
        ella — subir la planilla no debe saltárselo."""
        doc = vehiculo_completo(estadoIntegra="en_actualizacion",
                                observaciones="Actualizar datos del tenedor")
        del doc["planillaEpsArl"]
        fake = FakeColeccion([doc])
        resp = self._run_subida(fake)
        self.assertEqual(resp.status_code, 200, resp.text)
        doc = fake.documents[0]
        self.assertEqual(doc["estadoIntegra"], "en_actualizacion")
        self.assertIn("tenedor", doc["observaciones"])       # su observación queda intacta

    def test_NO_rehabilita_si_falta_otra_documentacion(self):
        doc = vehiculo_completo(estadoIntegra="en_actualizacion", actualizacionOrigen="aprobado")
        del doc["planillaEpsArl"]
        del doc["soat"]                                      # falta MÁS que la planilla
        fake = FakeColeccion([doc])
        resp = self._run_subida(fake)
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(fake.documents[0]["estadoIntegra"], "en_actualizacion")


class BlindajesTests(unittest.TestCase):

    def test_origen_blindado(self):
        self.assertIn("actualizacionOrigen", vehiculos.CLAVES_PROTEGIDAS)
        self.assertIn("actualizacionOrigen", vehiculos.CAMPOS_VOLATILES_FIRMA)

    def test_fecha_estado_no_rompe_la_firma(self):
        """fechaEstado es bookkeeping administrativo: sin estar excluido del
        hash, CADA transición posterior a la firma reportaba 'datos cambiaron'
        en falso (hueco preexistente detectado con QVK730)."""
        self.assertIn("fechaEstado", vehiculos.CAMPOS_VOLATILES_FIRMA)


if __name__ == "__main__":
    unittest.main()
