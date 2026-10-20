# -*- coding: utf-8 -*-
"""Tests del INFORME FINANCIERO por vehículo (2026-10-16): PDF descargable
SOLO para FINANCIERO/ADMIN desde /EstadoVehiculos — datos bancarios de las
4 figuras + LINKS firmados a las certificaciones bancarias (vigente +
versiones históricas; el usuario cambió de opinión: embebido → enlaces).

1. Guard: FINANCIERO y ADMIN 200 / OPERATIVO 403 / placa inexistente 404 /
   usuario inválido 401.
2. El PDF se genera (empieza por %PDF) con y SIN certificados cargados, y la
   tabla usa celdas Paragraph (wrap — el texto no desborda los recuadros).
3. Los enlaces van FIRMADOS: vigente + versión histórica del historial.
"""
import io
import unittest
from unittest.mock import patch

from bson import ObjectId

from rutas import vehiculos


class FakeColeccion:
    def __init__(self, documentos=None):
        self.documents = list(documentos or [])

    def find_one(self, query=None, *a, **k):
        for d in self.documents:
            if all(d.get(k) == v for k, v in (query or {}).items()):
                return d
        return None

    def find(self, query=None, *a, **k):
        return iter([d for d in self.documents
                     if all(d.get(k) == v for k, v in (query or {}).items())])

    def update_one(self, filtro, cambio, upsert=False):
        for d in self.documents:
            if all(d.get(k) == v for k, v in (filtro or {}).items()):
                d.update(cambio.get("$set") or {})
                for campo in (cambio.get("$unset") or {}):
                    d.pop(campo, None)
                return
        if upsert:
            nuevo = dict(filtro)
            nuevo.update(cambio.get("$set") or {})
            self.documents.append(nuevo)


CUENTA_FINANCIERO = {"_id": ObjectId(), "usuario": "FIN ROSA", "perfil": "FINANCIERO", "activo": True}
CUENTA_ADMIN = {"_id": ObjectId(), "usuario": "AD SOFI", "perfil": "ADMIN", "activo": True}
CUENTA_OPERATIVO = {"_id": ObjectId(), "usuario": "OP PEDRO", "perfil": "OPERATIVO", "activo": True}


def _vehiculo():
    return {
        "placa": "FIN123", "estadoIntegra": "aprobado",
        "condNombres": "PERICO", "condPrimerApellido": "PEREZ",
        "condCedulaCiudadania": "123", "condCelular": "300",
        "condBanco": "BANCO DE BOGOTA", "condTipoCuenta": "AHORROS", "condNumeroCuenta": "456",
        "tenedNombre": "TRANSPORTADORA XYZ", "tenedDocumento": "900123456",
        "tenedBanco": "DAVIVIENDA", "tenedTipoCuenta": "CORRIENTE", "tenedNumeroCuenta": "789",
        "condCertificacionBancaria": "Vehiculos/FIN123/2026-10-16/condCertificacionBancaria_fin123.webp",
        "documentoIdentidadConductor": "Vehiculos/FIN123/2026-10-16/documentoIdentidadConductor_fin123.webp",
    }


def _texto_pdf(contenido: bytes) -> str:
    from pypdf import PdfReader
    return "\n".join(p.extract_text() or "" for p in PdfReader(io.BytesIO(contenido)).pages)


def _uris_pdf(contenido: bytes) -> list:
    """URLs de los ENLACES del PDF (anotaciones /Annots) — desde 2026-10-16
    la URL ya no va como texto visible sino como link «Abrir documento»."""
    from pypdf import PdfReader
    uris = []
    for pagina in PdfReader(io.BytesIO(contenido)).pages:
        for anot in (pagina.get("/Annots") or []):
            obj = anot.get_object()
            accion = obj.get("/A")
            if accion and accion.get("/URI"):
                uris.append(str(accion["/URI"]))
    return uris


class InformeFinancieroTests(unittest.TestCase):

    def setUp(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        app = FastAPI()
        app.include_router(vehiculos.ruta_vehiculos)
        self.client = TestClient(app, raise_server_exceptions=False)

    def _get(self, placa="FIN123", usuario="FIN ROSA", veh=None):
        fake_veh = FakeColeccion([veh if veh is not None else _vehiculo()])
        with patch.object(vehiculos, "coleccion_vehiculos", fake_veh), \
             patch.object(vehiculos, "coleccion_baseusuarios",
                          FakeColeccion([CUENTA_FINANCIERO, CUENTA_ADMIN, CUENTA_OPERATIVO])), \
             patch.object(vehiculos, "_url_firmada_documento",
                          side_effect=lambda r: f"https://firma.test/{r.split('/')[-1]}"):
            return self.client.get(f"/vehiculos/financiero/{placa}?usuario={usuario}")

    def test_financiero_descarga_pdf(self):
        r = self._get()
        self.assertEqual(r.status_code, 200, r.text)
        self.assertTrue(r.content.startswith(b"%PDF"))
        self.assertIn("application/pdf", r.headers["content-type"])
        self.assertIn("Financiero_FIN123", r.headers.get("content-disposition", ""))
        texto = _texto_pdf(r.content)
        # La tabla lleva los datos bancarios de las figuras diligenciadas.
        self.assertIn("BANCO DE BOGOTA", texto)
        self.assertIn("DAVIVIENDA", texto)
        # El certificado vigente va como LINK «Abrir documento» apuntando a
        # la URL firmada (anotación, no texto crudo — no se amontona).
        self.assertIn("Abrir documento", texto)
        uris = _uris_pdf(r.content)
        self.assertTrue(any("condCertificacionBancaria_fin123.webp" in u for u in uris), uris)

    def test_admin_tambien_descarga(self):
        """(2026-10-16) ADMIN también puede descargar el informe (pedido del
        usuario tras la primera versión FINANCIERO-only)."""
        r = self._get(usuario="AD SOFI")
        self.assertEqual(r.status_code, 200, r.text)
        self.assertTrue(r.content.startswith(b"%PDF"))

    def test_otro_perfil_403(self):
        """El informe bancario es SOLO de FINANCIERO y ADMIN: el resto de
        perfiles de la página (OPERATIVO, CONTROL, COORDINADOR) → 403."""
        r = self._get(usuario="OP PEDRO")
        self.assertEqual(r.status_code, 403)
        self.assertIn("FINANCIERO", r.json()["detail"])

    def test_usuario_invalido_401(self):
        r = self._get(usuario="NADIE")
        self.assertEqual(r.status_code, 401)

    def test_placa_inexistente_404(self):
        r = self._get(placa="NOPE99")
        self.assertEqual(r.status_code, 404)

    def test_vehiculo_sin_certificados_tambien_genera(self):
        v = _vehiculo()
        v.pop("condCertificacionBancaria")
        r = self._get(veh=v)
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.content.startswith(b"%PDF"))

    def test_solo_certificaciones_vigentes_y_cedulas_en_links(self):
        """(2026-10-16, feedback) La tabla de enlaces trae SOLO la certificación
        VIGENTE (las versiones históricas ya no interesan) y las CÉDULAS/NIT
        cargados de cada actor."""
        v = _vehiculo()
        v["historialDocumentos"] = [{
            "tipo": "condCertificacionBancaria",
            "ruta": "Vehiculos/FIN123/2026-09-01/condCertificacionBancaria_v1_fin123.webp",
            "fecha": "2026-09-01", "actor": "PERICO PEREZ",
        }]
        r = self._get(veh=v)
        self.assertEqual(r.status_code, 200)
        texto = _texto_pdf(r.content)
        uris = _uris_pdf(r.content)
        # Vigente y cédula del conductor: con link.
        self.assertTrue(any("condCertificacionBancaria_fin123.webp" in u for u in uris), uris)
        self.assertTrue(any("documentoIdentidadConductor_fin123.webp" in u for u in uris), uris)
        self.assertIn("Cédula del conductor", texto)
        # La versión HISTÓRICA del certificado ya NO aparece.
        self.assertFalse(any("_v1_fin123" in u for u in uris), uris)
        self.assertNotIn("Anterior", texto)


class NotificacionFinancierosTests(unittest.TestCase):
    """(2026-10-16) Al APROBAR una placa: correo a FINANCIERO con la info
    financiera en el cuerpo y ADJUNTOS (informe PDF + certificados)."""

    def test_correo_con_adjuntos(self):
        enviado = {}

        def _enviar(params):
            enviado.update(params)

        fake_base = FakeColeccion([
            {"usuario": "FIN ROSA", "correo": "rosa@integra.com", "perfil": "FINANCIERO", "activo": True},
        ])
        with patch.object(vehiculos, "coleccion_baseusuarios", fake_base), \
             patch.object(vehiculos, "_pdf_informe_financiero", return_value=b"%PDF-informe"), \
             patch.object(vehiculos, "_descargar_blob", return_value=b"imagen-certificado"), \
             patch.object(vehiculos.resend.Emails, "send", side_effect=_enviar):
            vehiculos._notificar_financieros_aprobacion(_vehiculo())
        self.assertIn("rosa@integra.com", enviado["to"])
        self.assertIn("FIN123", enviado["subject"])
        # Cuerpo: tabla con las cuentas diligenciadas.
        self.assertIn("BANCO DE BOGOTA", enviado["html"])
        # Adjuntos: informe PDF + certificación del conductor (nombre legible).
        nombres = [a["filename"] for a in enviado["attachments"]]
        self.assertIn("Informe_Financiero_FIN123.pdf", nombres)
        # La certificación del conductor va adjunta con nombre legible + placa.
        self.assertTrue(any("Bancaria" in n and "Conductor" in n and "FIN123" in n
                            for n in nombres), nombres)

    def test_sin_financieros_no_reventar(self):
        with patch.object(vehiculos, "coleccion_baseusuarios", FakeColeccion()), \
             patch.object(vehiculos.resend.Emails, "send",
                          side_effect=AssertionError("no debía enviar")):
            vehiculos._notificar_financieros_aprobacion(_vehiculo())  # no raise

    def test_aprobacion_real_dispara_y_reactivacion_no(self):
        import asyncio

        async def _aprobar(estado_inicial, nuevo):
            veh = _vehiculo()
            veh["estadoIntegra"] = estado_inicial
            veh["estudiosVigencia"] = {"hasta": "2999-01-01"}
            llamadas = []
            with patch.object(vehiculos, "coleccion_vehiculos", FakeColeccion([veh])), \
                 patch.object(vehiculos, "coleccion_baseusuarios", FakeColeccion()), \
                 patch.object(vehiculos, "_tiene_estudio_seguridad", return_value=True), \
                 patch.object(vehiculos, "_registrar_auditoria"), \
                 patch.object(vehiculos, "_notificar_padrinos"), \
                 patch.object(vehiculos, "_enviar_wa_seguridad"), \
                 patch.object(vehiculos, "_notificar_financieros_aprobacion",
                              side_effect=lambda v: llamadas.append(v["placa"])):
                await vehiculos.actualizar_estado(
                    placa="FIN123", nuevo_estado=nuevo, usuario_id="seg-1",
                    nombre_conductor="X", via="seguridad",
                    # Los defaults son Form(None): pasarlos explícitos evita
                    # que el `or` del handler reciba el objeto Form.
                    observaciones=None, motivo=None, editado_por=None)
                await asyncio.sleep(0.1)  # dejar correr la tarea background
            return llamadas

        # Revisión → aprobado: SÍ dispara el correo a FINANCIERO.
        self.assertIn("FIN123", asyncio.run(_aprobar("completado_revision", "aprobado")))
        # Reactivar un inactivo NO es aprobación de revisión: no re-notifica.
        self.assertEqual(asyncio.run(_aprobar("inactivo", "aprobado")), [])


class _FakeColeccionVeh:
    """Falsa colección de vehículos que soporta el filtro $ne del Excel
    (exclusión de borradores del alta)."""

    def __init__(self, documentos=None):
        self.documents = list(documentos or [])

    def find(self, query=None, *a, **k):
        docs = self.documents
        for campo, esperado in (query or {}).items():
            if isinstance(esperado, dict) and "$ne" in esperado:
                docs = [d for d in docs if d.get(campo) != esperado["$ne"]]
            else:
                docs = [d for d in docs if d.get(campo) == esperado]
        return iter(list(docs))


class ExcelFinancieroTests(unittest.TestCase):
    """(2026-10-20) Excel con los datos financieros de los actores de los
    vehículos APROBADOS — SOLO FINANCIERO/ADMIN, SIN enlaces (solo datos)."""

    def setUp(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        app = FastAPI()
        app.include_router(vehiculos.ruta_vehiculos)
        self.client = TestClient(app, raise_server_exceptions=False)

    def _get(self, usuario="FIN ROSA", vehiculos_docs=None):
        if vehiculos_docs is None:
            vehiculos_docs = [_vehiculo()]
        with patch.object(vehiculos, "coleccion_vehiculos",
                          _FakeColeccionVeh(vehiculos_docs)), \
             patch.object(vehiculos, "coleccion_baseusuarios",
                          FakeColeccion([CUENTA_FINANCIERO, CUENTA_ADMIN, CUENTA_OPERATIVO])):
            return self.client.get(f"/vehiculos/financiero-excel?usuario={usuario}")

    def _celdas(self, contenido: bytes):
        import io as _io
        from openpyxl import load_workbook
        wb = load_workbook(_io.BytesIO(contenido))
        ws = wb.active
        return [list(fila) for fila in ws.iter_rows(values_only=True)]

    def test_financiero_descarga_excel_con_datos(self):
        r = self._get()
        self.assertEqual(r.status_code, 200, r.text)
        self.assertIn("spreadsheetml", r.headers["content-type"])
        self.assertIn("Datos_financieros_flota_", r.headers.get("content-disposition", ""))
        filas = self._celdas(r.content)
        encabezados = filas[0]
        self.assertEqual(encabezados[0], "Placa")
        self.assertIn("Número de cuenta", encabezados)
        textos = [str(c) for fila in filas for c in fila if c]
        # Los datos bancarios de las figuras diligenciadas viajan completos.
        self.assertIn("BANCO DE BOGOTA", textos)
        self.assertIn("DAVIVIENDA", textos)
        self.assertIn("TRANSPORTADORA XYZ", textos)
        self.assertIn("456", textos)   # nº cuenta conductor
        self.assertIn("789", textos)   # nº cuenta tenedor
        # SIN enlaces: ninguna celda contiene una URL.
        self.assertFalse(any("http" in t or "Vehiculos/" in t for t in textos), textos)

    def test_admin_tambien_descarga(self):
        self.assertEqual(self._get(usuario="AD SOFI").status_code, 200)

    def test_otro_perfil_403(self):
        r = self._get(usuario="OP PEDRO")
        self.assertEqual(r.status_code, 403)
        self.assertIn("FINANCIERO", r.json()["detail"])

    def test_usuario_invalido_401(self):
        self.assertEqual(self._get(usuario="NADIE").status_code, 401)

    def test_vehiculo_sin_actores_queda_en_la_base(self):
        vacio = {"placa": "VAC111", "estadoIntegra": "aprobado"}
        r = self._get(vehiculos_docs=[_vehiculo(), vacio])
        self.assertEqual(r.status_code, 200)
        filas = self._celdas(r.content)
        # El vehículo sin actores tiene su fila (con guiones) y el estado legible.
        fila_vacia = [f for f in filas if f and f[0] == "VAC111"]
        self.assertEqual(len(fila_vacia), 1)
        self.assertEqual(fila_vacia[0][2], "-")

    def test_solo_aprobados_borradores_y_otros_estados_excluidos(self):
        """(Pedido del usuario 2026-10-20) El Excel baja ÚNICAMENTE los
        vehículos APROBADOS: borradores del alta y otros estados fuera."""
        borrador = {"placa": "BOR222", "estadoIntegra": "registro_incompleto",
                    "responsable_pendiente": True, "condNombres": "FANTASMA"}
        en_revision = {"placa": "REV333", "estadoIntegra": "completado_revision",
                       "condNombres": "EN CURSO"}
        r = self._get(vehiculos_docs=[_vehiculo(), borrador, en_revision])
        self.assertEqual(r.status_code, 200)
        textos = [str(c) for fila in self._celdas(r.content) for c in fila if c]
        self.assertNotIn("BOR222", textos)
        self.assertNotIn("REV333", textos)
        # El aprobado del fixture SÍ está.
        self.assertIn("FIN123", textos)


if __name__ == "__main__":
    unittest.main()
