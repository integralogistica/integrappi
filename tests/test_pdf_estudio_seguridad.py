"""Tests del generador de PDF del estudio de seguridad (reportlab).

Valida: bytes %PDF válidos, multi-página con viajes, marca de agua con
empresa/usuario/consulta_id, footer "Página X de Y", y reproducibilidad
(mismo doc → mismo contenido de secciones).

Ejecutar:  python -m unittest tests.test_pdf_estudio_seguridad -v
"""
import unittest
from datetime import datetime

from Funciones.pdf_estudio_seguridad import generar_pdf_estudio


def estudio_fixture(n_viajes=3):
    viajes = [
        {
            "Nro. de Radicado": f"1230358{i:02d}",
            "Tipo Doc.": "Manifiesto",
            "Consecutivo": "00147783",
            "Fecha Hora Radicación": "2026/08/21 20:29:31",
            "Nombre Empresa Transportadora": "INTEGRA CADENA DE SERVICIOS S.A.S.",
            "Origen": "FUNZA CUNDINAMARCA",
            "Destino": "PEREIRA RISARALDA",
            "Cedula Conductor": "1033688842",
            "Placa": "QVK013",
            "Placa Remolque": "",
            "Fecha Exped": "2026/08/21",
            "Estado": "AC",
        }
        for i in range(n_viajes)
    ]
    return {
        "consulta_id": "ES-TEST0001",
        "codigo_verificacion": "ABC123DEF4",
        "empresa_id": "507f1f77bcf86cd799439011",
        "empresa_nombre": "EMPRESA DE PRUEBA",
        "usuario_id": "507f1f77bcf86cd799439022",
        "usuario": "JPEREZ",
        "usuario_nombre": "JUAN PEREZ",
        "usuario_correo": "jperez@prueba.com",
        "cedula": "1033688842",
        "nombre_consultado": "JHOAM ORLANDO AMAYA TOVAR",
        "estado": "COMPLETADA",
        "creado_en": datetime(2026, 8, 29, 15, 0, 0),
        "finalizado_en": datetime(2026, 8, 29, 15, 0, 40),
        "duracion_s": 40.2,
        "forzado": False,
        "fuentes": {
            "manifiestos_rndc": {
                "estado": "EXITO",
                "origen": "portal",
                "desde": "2025/08/29",
                "hasta": "2026/08/29",
                "total": n_viajes,
                "viajes": viajes,
                "columnas": list(viajes[0].keys()),
                "intentos": 1,
                "duraciones_s": [18.3],
                "error": None,
            },
            "procuraduria": {
                "estado": "EXITO",
                "origen": "portal",
                "no_registra": True,
                "mensaje": "NO REGISTRA SANCIONES NI INHABILIDADES VIGENTES",
                "nombre_certificado": "JHOAM ORLANDO AMAYA TOVAR",
                "pdf_sha256": "ab12" * 16,
                "pdf_tamano": 81234,
                "intentos": 1,
                "duraciones_s": [22.1],
                "error": None,
            },
        },
        "pdf": {
            "gcs_ruta": "SeguridadEstudios/x/2026/ES-TEST0001.pdf",
            "sha256": "cd34" * 16,
            "tamano": 145230,
            "version": 1,
            "generado_en": datetime(2026, 8, 29, 15, 0, 45),
        },
        "anexo_procuraduria": {
            "gcs_ruta": "SeguridadEstudios/x/2026/ES-TEST0001_procuraduria.pdf",
            "sha256": "ab12" * 16,
            "tamano": 81234,
        },
        "auditoria": {"ip": "190.85.1.2", "user_agent": "test-agent", "esquema_auth": "bearer"},
    }


_GRIS_MARCA = (0.501961, 0.501961, 0.501961)


def _es_marca_agua(char: dict) -> bool:
    """La marca de agua se dibuja con fill gris uniforme y tamaño ~11 (la
    rotación está en el text matrix, pdfplumber no la expone como upright)."""
    return char.get("non_stroking_color") == _GRIS_MARCA and char.get("size", 0) > 9


def _texto_plano(contenido: bytes) -> str:
    """Texto de todas las páginas sin espacios/saltos, EXCLUYENDO la marca de
    agua (pdfplumber la intercalaría en las mismas líneas al extraer)."""
    import io

    import pdfplumber

    with pdfplumber.open(io.BytesIO(contenido)) as pdf:
        partes = []
        for pagina in pdf.pages:
            chars = [c for c in pagina.chars if not _es_marca_agua(c)]
            # Reconstruir por líneas (top redondeado) y de izquierda a derecha.
            chars.sort(key=lambda c: (round(c["top"], 1), c["x0"]))
            partes.append("".join(c["text"] for c in chars))
        return "".join(partes).replace(" ", "").replace("\n", "")


def _texto_marca_agua(contenido: bytes) -> str:
    """Solo los caracteres de la marca de agua (gris ~11pt)."""
    import io

    import pdfplumber

    with pdfplumber.open(io.BytesIO(contenido)) as pdf:
        partes = []
        for pagina in pdf.pages:
            chars = [c for c in pagina.chars if _es_marca_agua(c)]
            partes.append("".join(c["text"] for c in chars))
        return "".join(partes).replace(" ", "").replace("\n", "")


class TestGenerarPDF(unittest.TestCase):
    def test_bytes_pdf_validos(self):
        contenido = generar_pdf_estudio(estudio_fixture())
        self.assertTrue(contenido.startswith(b"%PDF"))
        self.assertGreater(len(contenido), 1024)

    def test_multipagina_con_muchos_viajes(self):
        import io

        import pdfplumber

        estudio = estudio_fixture(n_viajes=60)
        contenido = generar_pdf_estudio(estudio)
        with pdfplumber.open(io.BytesIO(contenido)) as pdf:
            paginas = len(pdf.pages)
        self.assertGreater(paginas, 1)
        # Footer "Página X de Y" (canvas de dos pasadas).
        self.assertIn("Página1de", _texto_plano(contenido))

    def test_marca_de_agua_identifica_origen(self):
        marca = _texto_marca_agua(generar_pdf_estudio(estudio_fixture()))
        # La marca de agua (rotada 45°) lleva empresa | usuario | fecha | consulta_id.
        self.assertIn("EMPRESADEPRUEBA", marca)
        self.assertIn("JPEREZ", marca)
        self.assertIn("ES-TEST0001", marca)

    def test_secciones_presentes(self):
        texto = _texto_plano(generar_pdf_estudio(estudio_fixture()))
        for esperado in (
            "ESTUDIODESEGURIDAD",
            "Manifiestosdecarga",
            "Procuraduría",
            "NOREGISTRASANCIONES",
            "Trazabilidad",
            "Ley1581",
            "JUANPEREZ",
            "1033688842",
        ):
            self.assertIn(esperado, texto)

    def test_estado_error_no_muestra_veredicto_positivo(self):
        estudio = estudio_fixture()
        estudio["estado"] = "PARCIAL"
        estudio["fuentes"]["procuraduria"] = {
            "estado": "NO_DISPONIBLE",
            "origen": None,
            "intentos": 2,
            "duraciones_s": [60.0, 60.0],
            "error": {"tipo": "TimeoutError", "mensaje": "sin respuesta"},
        }
        texto = _texto_plano(generar_pdf_estudio(estudio))
        self.assertIn("NODISPONIBLE", texto)
        self.assertNotIn("NOREGISTRASANCIONES", texto)

    def test_reproducibilidad(self):
        """Mismo doc → mismo PDF byte a byte (sin timestamps del entorno)."""
        estudio = estudio_fixture()
        primero = generar_pdf_estudio(estudio)
        segundo = generar_pdf_estudio(estudio)
        self.assertEqual(primero, segundo)


class TestTablaViajes(unittest.TestCase):
    """Regresión del bug 2026-08-29: la tabla de manifiestos usaba texto plano
    (reportlab no lo parte) y los nombres largos de transportadora INVADÍAN la
    columna siguiente. Ahora cada celda es un Paragraph que hace wrap."""

    def test_celdas_son_paragraph(self):
        from reportlab.platypus import Paragraph, Table

        from Funciones.pdf_estudio_seguridad import _tabla_viajes

        viajes = [{
            "Nro. de Radicado": "123408537",
            "Fecha Hora Radicación": "2026/08/28 15:15:21",
            "Nombre Empresa Transportadora": "CORPORACION COLOMBIANA DE LOGISTICA S.A. C.C.L S.A.",
            "Origen": "YUMBO VALLE DEL CAUCA",
            "Destino": "DUITAMA BOYACA",
            "Placa": "JUY439",
            "Tipo Doc.": "Manifiesto",
            "Estado": "CE",
        }]
        tabla = _tabla_viajes(viajes, list(viajes[0].keys()))
        self.assertIsInstance(tabla, Table)
        celdas = tabla._cellvalues
        for fila in celdas:
            for celda in fila:
                self.assertIsInstance(celda, Paragraph, "toda celda debe ser Paragraph (wrap)")

    def test_anchos_respetan_el_ancho_util(self):
        from Funciones.pdf_estudio_seguridad import ANCHO, MARGEN, _tabla_viajes

        viajes = [{
            "Nro. de Radicado": "1", "Fecha Hora Radicación": "2",
            "Nombre Empresa Transportadora": "3", "Origen": "4", "Destino": "5",
            "Placa": "6", "Tipo Doc.": "7", "Estado": "8",
        }]
        tabla = _tabla_viajes(viajes, list(viajes[0].keys()))
        self.assertAlmostEqual(sum(tabla._colWidths), ANCHO - 2 * MARGEN, places=1)


if __name__ == "__main__":
    unittest.main()
