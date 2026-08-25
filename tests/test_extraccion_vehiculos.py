"""Tests de la lectura IA de documentos (esquemas, PDF, avisos de consistencia)."""
import unittest
from io import BytesIO
from unittest.mock import patch

from fastapi import HTTPException

from rutas import vehiculos


class EsquemasExtraccionTests(unittest.TestCase):
    """Los esquemas nuevos existen con sus claves y el prompt los incluye."""

    TIPOS_ESPERADOS = {"cedula", "rut", "certificado_bancario", "licencia", "tarjeta_propiedad", "soat"}

    def test_tipos_definidos(self):
        self.assertEqual(set(vehiculos.ESQUEMAS_EXTRACCION.keys()), self.TIPOS_ESPERADOS)

    def test_prompt_incluye_claves_del_esquema(self):
        for tipo in self.TIPOS_ESPERADOS:
            with self.subTest(tipo=tipo):
                instruction, esquema = vehiculos._prompt_extraccion(tipo)
                for clave in esquema["campos"]:
                    self.assertIn(clave, instruction)
                # Regla de dígitos espaciados (crítica para RUT digital).
                self.assertIn("espacios", instruction)

    def test_tipo_desconocido_rechazado(self):
        with self.assertRaises(HTTPException) as ctx:
            vehiculos.extraer_datos_con_llm("dni_argentino", [])
        self.assertEqual(ctx.exception.status_code, 400)


class ExtraerTextoPdfTests(unittest.TestCase):
    """El PDF digital entrega texto; el escaneado/corrupto entrega ''."""

    def test_pdf_digital(self):
        pdf = self._pdf_con_texto("RUT colombiano. " * 30)  # >200 chars
        texto = vehiculos._extraer_texto_pdf(pdf)
        self.assertGreaterEqual(len(texto.strip()), 200)
        self.assertIn("RUT", texto)

    def test_pdf_sin_texto(self):
        # PDF válido pero sin texto extraíble (una página en blanco).
        pdf = self._pdf_con_texto("")
        self.assertEqual(vehiculos._extraer_texto_pdf(pdf), "")

    def test_bytes_corruptos(self):
        # No debe lanzar: cae a "" y el llamador manda el archivo inline.
        self.assertEqual(vehiculos._extraer_texto_pdf(b"no soy un pdf"), "")

    @staticmethod
    def _pdf_con_texto(texto: str) -> bytes:
        """PDF con una página que contiene `texto`; si fpdf no está, salta el test."""
        try:
            from fpdf import FPDF
        except ImportError:
            raise unittest.SkipTest("fpdf no instalado; cubierto por test_pdf_minimo_manual")
        doc = FPDF()
        doc.add_page()
        doc.set_font("Helvetica", size=12)
        doc.multi_cell(0, 8, texto)
        return bytes(doc.output())

    def test_pdf_minimo_manual(self):
        """Sin fpdf: genera un PDF de una página vacía a mano (válido para pdfplumber)."""
        pdf = (
            b"%PDF-1.4\n"
            b"1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
            b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
            b"3 0 obj<</Type/Page/Parent 2 0 R/MediaBox[0 0 200 200]>>endobj\n"
            b"xref\n0 4\ntrailer<</Size 4/Root 1 0 R>>\nstartxref\n0\n%%EOF"
        )
        # Aunque pdfplumber no lo indexe por xref roto, no debe lanzar.
        try:
            vehiculos._extraer_texto_pdf(pdf)
        except Exception as e:  # pragma: no cover
            self.fail(f"_extraer_texto_pdf lanzó {e}")


class GenerarAvisosTests(unittest.TestCase):
    """Avisos de consistencia entre lo leído y el vehículo en curso."""

    def test_placa_tarjeta_distinta(self):
        avisos = vehiculos._generar_avisos(
            "tarjeta_propiedad", {"placa": "ABC-123"}, {"placa_vehiculo": "XYZ456"}
        )
        self.assertEqual(len(avisos), 1)
        self.assertIn("ABC123", avisos[0])

    def test_placa_soat_coincide_sin_aviso(self):
        avisos = vehiculos._generar_avisos(
            "soat", {"placa": "XYZ 456"}, {"placa_vehiculo": "XYZ456"}
        )
        self.assertEqual(avisos, [])

    def test_licencia_cedula_distinta(self):
        avisos = vehiculos._generar_avisos(
            "licencia", {"cedula": "112004271"}, {"cedula_conductor": "52345678"}
        )
        self.assertEqual(len(avisos), 1)
        self.assertIn("112004271", avisos[0])

    def test_licencia_vencida(self):
        avisos = vehiculos._generar_avisos(
            "licencia", {"fecha_vencimiento": "2020-01-01"}, {}
        )
        self.assertEqual(len(avisos), 1)
        self.assertIn("vencid", avisos[0].lower())

    def test_soat_vencido(self):
        avisos = vehiculos._generar_avisos("soat", {"fecha_vencimiento": "2020-06-06"}, {})
        self.assertEqual(len(avisos), 1)
        self.assertIn("SOAT", avisos[0])
        self.assertIn("vencid", avisos[0].lower())

    def test_licencia_al_dia_sin_aviso(self):
        avisos = vehiculos._generar_avisos("licencia", {"fecha_vencimiento": "2099-12-31"}, {})
        self.assertEqual(avisos, [])

    def test_contexto_vacio_sin_aviso_de_placa(self):
        # Sin placa de contexto no hay con qué comparar → sin aviso.
        avisos = vehiculos._generar_avisos("tarjeta_propiedad", {"placa": "ABC123"}, {})
        self.assertEqual(avisos, [])


class ExtraerDatosConLlmTests(unittest.TestCase):
    """El armado de parts: imágenes inline, PDF digital como texto, límite de tamaño."""

    class FakeUpload:
        def __init__(self, datos: bytes, content_type: str):
            from io import BytesIO as _BIO

            self.file = _BIO(datos)
            self.content_type = content_type

    def _con_respuesta(self, respuesta_json: str, captura: dict):
        """Parcha _llamar_gemini y retorna el resultado de extraer_datos_con_llm."""

        def fake_llamar(parts, instruction):
            captura["parts"] = parts
            captura["instruction"] = instruction
            return respuesta_json

        with patch.object(vehiculos, "_llamar_gemini", side_effect=fake_llamar):
            with patch.object(
                vehiculos, "_extraer_texto_pdf", return_value="RUT " * 100
            ):
                archivos = [
                    self.FakeUpload(b"contenido-pdf", "application/pdf"),
                ]
                return vehiculos.extraer_datos_con_llm("rut", archivos)

    def test_pdf_digital_va_como_texto(self):
        captura = {}
        datos = self._con_respuesta('{"numero_documento": "112004271"}', captura)
        self.assertEqual(datos["numero_documento"], "112004271")
        # Ninguna part inline: todo texto.
        self.assertTrue(
            all("inline_data" not in p for p in captura["parts"]),
            "El PDF digital no debe ir inline al LLM.",
        )

    def test_imagen_va_inline(self):
        captura = {}

        def fake_llamar(parts, instruction):
            captura["parts"] = parts
            return '{"numero": "1020304050"}'

        with patch.object(vehiculos, "_llamar_gemini", side_effect=fake_llamar):
            archivos = [self.FakeUpload(b"imagen-fake", "image/jpeg")]
            datos = vehiculos.extraer_datos_con_llm("cedula", archivos)
        self.assertEqual(datos["numero"], "1020304050")
        self.assertTrue(
            any("inline_data" in p for p in captura["parts"]),
            "La imagen debe ir inline al LLM.",
        )

    def test_archivo_demasiado_grande(self):
        grande = self.FakeUpload(b"x" * (6 * 1024 * 1024 + 1), "image/jpeg")
        with self.assertRaises(HTTPException) as ctx:
            vehiculos.extraer_datos_con_llm("cedula", [grande])
        self.assertEqual(ctx.exception.status_code, 400)

    def test_respuesta_sin_datos_legibles(self):
        captura = {}
        with self.assertRaises(HTTPException) as ctx:
            self._con_respuesta('{"numero_documento": null}', captura)
        self.assertEqual(ctx.exception.status_code, 422)

    def test_respuesta_con_claves_extra_se_normaliza(self):
        captura = {}
        datos = self._con_respuesta('{"numero_documento": "1", "inventada": "x"}', captura)
        self.assertNotIn("inventada", datos)
        self.assertEqual(datos["numero_documento"], "1")


if __name__ == "__main__":
    unittest.main()
