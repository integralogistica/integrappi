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


def _texto_por_pagina(contenido: bytes) -> list[str]:
    """Texto normalizado por página, excluyendo la marca de agua."""
    import io

    import pdfplumber

    with pdfplumber.open(io.BytesIO(contenido)) as pdf:
        paginas = []
        for pagina in pdf.pages:
            chars = [c for c in pagina.chars if not _es_marca_agua(c)]
            chars.sort(key=lambda c: (round(c["top"], 1), c["x0"]))
            paginas.append("".join(c["text"] for c in chars).replace(" ", "").replace("\n", ""))
        return paginas


class TestGenerarPDF(unittest.TestCase):
    def test_fuentes_compactas_maximo_tres_por_pagina_sin_titulo_huerfano(self):
        """Una cuarta fuente no queda colgada al final de la página."""
        estudio = estudio_fixture()
        estudio["placa"] = "MVX48E"
        estudio["fuentes"]["policia"] = _fuente_policia()
        estudio["fuentes"]["runt"] = _fuente_runt()
        estudio["fuentes"]["simit"] = _fuente_simit(
            total_comparendos=88, total_multas=17, total_deuda=40_257_438.0,
        )
        estudio["fuentes"]["sena"] = _fuente_sena()

        paginas = _texto_por_pagina(generar_pdf_estudio(estudio))
        # Procuraduría SIEMPRE es la última fuente del informe (2026-09-15);
        # su sección compacta al final empaqueta el informe en 4 páginas.
        self.assertEqual(4, len(paginas))

        titulos = (
            "Manifiestosdecarga—RNDC",
            "Antecedentesjudiciales—Policía",
            "Vehículo—RUNT",
            "Comparendos—SIMIT",
            "FormaciónSENA—Certificados",
            "Antecedentesdisciplinarios—Procuraduría",
        )
        fuentes_por_pagina = [
            [titulo for titulo in titulos if titulo in pagina]
            for pagina in paginas
        ]
        self.assertEqual(
            [[], list(titulos[:3]), list(titulos[3:5]), [titulos[5]]],
            fuentes_por_pagina,
        )
        # Procuraduría es la última fuente y abre la página final, junto a la
        # trazabilidad (nunca queda huérfana a mitad de página).
        self.assertIn("Trazabilidadyauditoría", paginas[3])
        self.assertLessEqual(max(map(len, fuentes_por_pagina)), 3)
        # Cada página que ABRE con una fuente conserva primero el encabezado
        # fijo (~40 caracteres); el título debe aparecer inmediatamente después.
        # (La página 2 abre con la continuación de la tabla RTM del RUNT, así
        # que SIMIT queda a media página — continuación legítima, no huérfano.)
        self.assertLess(paginas[1].index(titulos[0]), 55)
        self.assertLess(paginas[3].index(titulos[5]), 55)

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
        # La fuente caída: aviso gris en el detalle + mención en el cuadro
        # aparte (el estado "NO DISPONIBLE" ya no se imprime por fuente).
        self.assertIn("Lafuentedeconsultapresentaindisponibilidad", texto)
        self.assertIn("Fuentessinrespuestaenestaconsulta", texto)
        self.assertNotIn("PARCIALFUENTESNODISPONIBLES", texto)
        self.assertNotIn("NOREGISTRASANCIONES", texto)

    def test_reproducibilidad(self):
        """Mismo doc → mismo PDF byte a byte (sin timestamps del entorno)."""
        estudio = estudio_fixture()
        primero = generar_pdf_estudio(estudio)
        segundo = generar_pdf_estudio(estudio)
        self.assertEqual(primero, segundo)


class TestSeccionRamaJudicial(unittest.TestCase):
    def test_imprime_demandante_y_demandado(self):
        estudio = estudio_fixture()
        estudio["fuentes"]["rama_judicial"] = {
            "estado": "ADVERTENCIA",
            "nombre_completo": "JHOAM ORLANDO AMAYA TOVAR",
            "total_procesos": 1,
            "procesos": [{
                "llaveProceso": "11001418903620220056800",
                "despacho": "JUZGADO 036 DE PEQUEÑAS CAUSAS",
                "fechaProceso": "2022-05-05T00:00:00",
                "sujetosProcesales": (
                    "Demandante: SYSTEMGROUP S.A.S. | "
                    "Demandado: JHOAM ORLANDO AMAYA TOVAR"
                ),
            }],
            "intentos": 1,
            "duraciones_s": [5.0],
        }
        texto = _texto_plano(generar_pdf_estudio(estudio))
        self.assertIn("Sujetosprocesales", texto)
        self.assertIn("Demandante:SYSTEMGROUPS.A.S.", texto)
        self.assertIn("Demandado:JHOAMORLANDOAMAYATOVAR", texto)


class TestSeccionContraloria(unittest.TestCase):
    def test_veredicto_y_codigo_de_verificacion(self):
        estudio = estudio_fixture()
        estudio["fuentes"]["contraloria"] = {
            "estado": "EXITO",
            "no_registra": True,
            "mensaje": "No se encuentra reportado como responsable fiscal (SIBOR)",
            "codigo_verificacion": "1033688842260902160211",
            "intentos": 1,
            "duraciones_s": [30.0],
        }
        texto = _texto_plano(generar_pdf_estudio(estudio))
        self.assertIn("Antecedentesfiscales", texto)
        self.assertIn("NOSEENCUENTRAREPORTADOCOMORESPONSABLEFISCAL", texto)
        self.assertIn("1033688842260902160211", texto)
        # El párrafo legal solo cubre fuentes corridas.
        self.assertIn("SIBOR", texto)

    def test_reportado_es_advertencia(self):
        estudio = estudio_fixture()
        estudio["fuentes"]["contraloria"] = {
            "estado": "ADVERTENCIA",
            "no_registra": False,
            "mensaje": "SE ENCUENTRA REPORTADO COMO RESPONSABLE FISCAL",
            "codigo_verificacion": "",
            "intentos": 2,
            "duraciones_s": [30.0, 30.0],
        }
        texto = _texto_plano(generar_pdf_estudio(estudio))
        self.assertIn("SEENCUENTRAREPORTADOCOMORESPONSABLEFISCAL", texto)
        self.assertIn("Reportadocomoresponsablefiscal", texto)


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


def _fuente_policia(estado="EXITO", no_registra=True, mensaje=None, nombre="AMAYA TOVAR JHOAM ORLANDO"):
    return {
        "estado": estado,
        "origen": "portal",
        "no_registra": no_registra,
        "mensaje": mensaje or "NO TIENE ASUNTOS PENDIENTES CON LAS AUTORIDADES JUDICIALES",
        "nombre_consultado": nombre,
        "pdf_sha256": None,
        "pdf_tamano": 0,
        "intentos": 1,
        "duraciones_s": [20.5],
        "error": None,
    }


class TestSeccionPolicia(unittest.TestCase):
    """Fuente "policia" en el PDF: fila de resumen, banner semaforizado,
    detalle con leyenda y nombre, y disposición legal honesta (sin norma
    habilitante de terceros — el portal es de autoconsulta del titular)."""

    def _con_policia(self, **kw):
        estudio = estudio_fixture()
        estudio["fuentes"]["policia"] = _fuente_policia(**kw)
        return estudio

    def test_exito_no_registra_banner_verde(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_policia()))
        self.assertIn("Antecedentesjudiciales", texto)
        self.assertIn("NOREGISTRAANTECEDENTESJUDICIALES", texto)
        self.assertIn("AMAYATOVARJHOAMORLANDO", texto)

    def test_leyenda_oficial_su458_completa(self):
        """2026-09-01: la leyenda oficial COMPLETA de la SU-458 se imprime en
        la sección Policía (texto fijo — el `mensaje` del bot es solo la línea
        del veredicto)."""
        import unicodedata

        texto = _texto_plano(generar_pdf_estudio(self._con_policia()))
        self.assertIn("Leyendaoficial", texto.replace(" ", ""))
        plano = "".join(c for c in unicodedata.normalize("NFD", texto) if unicodedata.category(c) != "Mn")
        self.assertIn("EncumplimientodelaSentenciaSU-458del21de juniode2012".replace(" ", ""), plano.replace(" ", ""))
        self.assertIn("extinciondelacondenaolaprescripciondelapena".replace(" ", ""), plano.replace(" ", ""))
        self.assertIn("soloaplicaparaelterritoriocolombiano".replace(" ", ""), plano.replace(" ", ""))

    def test_registra_banner_rojo(self):
        estudio = self._con_policia(
            no_registra=False,
            mensaje="ACTUALMENTE NO ES REQUERIDO POR AUTORIDAD JUDICIAL",
        )
        texto = _texto_plano(generar_pdf_estudio(estudio))
        self.assertIn("REGISTRAREQUERIMIENTOJUDICIAL", texto)
        self.assertIn("ACTUALMENTENOESREQUERIDO", texto)

    def test_no_conclusivo_advertencia(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_policia(estado="ADVERTENCIA", no_registra=None, mensaje="", nombre="")))
        self.assertIn("VEREDICTONOCONCLUSIVO", texto)

    def test_fuente_fallida_muestra_estado(self):
        estudio = self._con_policia(
            estado="NO_DISPONIBLE", mensaje=None,
        )
        estudio["fuentes"]["policia"]["error"] = {"tipo": "portal_inconsistente", "mensaje": "El portal de la Policía no entregó veredicto"}
        texto = _texto_plano(generar_pdf_estudio(estudio))
        # El detalle de la fuente caída dice SOLO el aviso gris discreto
        # (2026-09-25) y el cuadro aparte SOLO menciona la fuente: ni el
        # motivo técnico ni el estado repetido llegan al informe.
        self.assertIn("Lafuentedeconsultapresentaindisponibilidad", texto)
        self.assertNotIn("noentregóveredicto", texto)
        self.assertNotIn("NODISPONIBLE", texto)

    def test_resumen_con_tres_fuentes(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_policia()))
        # Las 3 fuentes en la tabla "Resumen por fuente".
        for frag in ("ManifiestosRNDC", "ProcuraduríaGeneraldelaNación", "PolicíaNacional"):
            self.assertIn(frag, texto)

    def test_disposiciones_legales_honestas(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_policia()))
        # Autoconsulta del titular + autorización Ley 1581.
        self.assertIn("autoconsulta", texto)
        # Tolerante al wrap (la leyenda SU-458 movió los saltos de línea):
        # comparar sin espacios/saltos.
        denso = "".join(texto.split()).replace("|", "")
        # Una letra de la marca de agua diagonal puede quedar intercalada por
        # pdfplumber entre "Decreto" y "019"; el contenido legal no cambia.
        self.assertTrue(
            "Decreto019de2012" in denso or "Decretoi019de2012" in denso,
            denso,
        )
        # La cita de la 1238 sigue presente pero SOLO para la Procuraduría.
        self.assertIn("Ley1238de2008", texto)


class TestResumenFuentesSeparado(unittest.TestCase):
    """(2026-09-25) El cuadro 'Resumen por fuente' solo lista fuentes con
    respuesta (EXITO/ADVERTENCIA); NO_DISPONIBLE/ERROR van al cuadro aparte
    'Fuentes sin respuesta en esta consulta' con motivo limpio."""

    def _con_policia_caida(self, estado="NO_DISPONIBLE", mensaje="La fuente no respondió en 150 s", tipo="TimeoutError"):
        estudio = estudio_fixture()
        estudio["fuentes"]["policia"] = {
            "estado": estado,
            "origen": "portal",
            "intentos": 2,
            "duraciones_s": [75.0, 75.0],
            "error": {"tipo": tipo, "mensaje": mensaje},
        }
        return estudio

    def test_cuadro_aparte_para_fuentes_caidas(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_policia_caida()))
        self.assertIn("Fuentessinrespuestaenestaconsulta", texto)
        # SOLO se menciona la fuente (pedido 2026-09-25): ni estado repetido
        # ni motivo técnico en este cuadro.
        self.assertIn("PolicíaNacional", texto)
        self.assertNotIn("Lafuentenorespondióen150s", texto)

    def test_resumen_principal_sin_fuentes_fallidas(self):
        """La fila de la fuente caída NO va en 'Resumen por fuente': la
        portada no muestra estados de fallo por fuente — la caída solo se
        MENCIONA (nombre) en el cuadro aparte."""
        paginas = _texto_por_pagina(generar_pdf_estudio(self._con_policia_caida()))
        portada = paginas[0]
        self.assertIn("Resumenporfuente", portada)
        self.assertIn("Fuentessinrespuestaenestaconsulta", portada)
        # Ningún estado de fallo por fuente en la portada (el cuadro aparte
        # solo lista nombres).
        self.assertNotIn("NODISPONIBLE", portada)
        self.assertNotIn("ERROR", portada)

    def test_stacktrace_playwright_no_aparece(self):
        estudio = self._con_policia_caida(
            estado="ERROR", tipo="Error",
            mensaje=(
                "Page.evaluate: TypeError: Failed to fetch at eval "
                "(eval at evaluate (:234:30), :2:37) at UtilityScript.evaluate "
                "(:241:19) at UtilityScript. (:1:"
            ),
        )
        texto = _texto_plano(generar_pdf_estudio(estudio))
        # El detalle de fuente caída es el aviso gris fijo y el cuadro aparte
        # SOLO menciona la fuente: el stacktrace del bot no llega jamás al PDF.
        self.assertNotIn("UtilityScript", texto)
        self.assertNotIn("evalatevaluate", texto)
        self.assertNotIn("TypeError:Failedtofetch", texto)
        self.assertIn("Lafuentedeconsultapresentaindisponibilidad", texto)

    def test_limpiar_mensaje_error_unitario(self):
        from Funciones.pdf_estudio_seguridad import _limpiar_mensaje_error
        self.assertEqual(_limpiar_mensaje_error(""), "")
        self.assertEqual(
            _limpiar_mensaje_error("Page.evaluate: TypeError: Failed to fetch at eval (x) at UtilityScript.y"),
            "TypeError: Failed to fetch",
        )
        # Mensajes normales pasan intactos.
        self.assertEqual(_limpiar_mensaje_error("La fuente no respondió en 150 s"), "La fuente no respondió en 150 s")


def _fuente_runt(estado="EXITO", no_registra=None, soat=None, datos=None, polizas=None, mensaje="", rtm=None, revisiones=None):
    return {
        "estado": estado,
        "origen": "portal",
        "no_registra": no_registra,
        "mensaje": mensaje,
        "placa": "MVX48E",
        "datos_vehiculo": datos if datos is not None else {
            "placa": "MVX48E", "marca": "HONDA", "linea": "CB 160F DLX", "modelo": "2018",
            "clase": "MOTOCICLETA", "numero_motor": "KC23E-7-3006584",
            "numero_vin": "9FMKC2325JF002733", "cilindraje": "162",
        },
        "soat": soat if soat is not None else {
            "numero": "3453028900", "aseguradora": "AXA COLPATRIA SEGUROS SA",
            "fecha_inicio_vigencia": "2025-10-23", "fecha_fin_vigencia": "2099-10-22",
            "estado_portal": "VIGENTE", "vigente": True,
        },
        "polizas": polizas if polizas is not None else [
            {
                "numero": "3453028900", "fecha_expedicion": "2025-10-04",
                "fecha_inicio_vigencia": "2025-10-23", "fecha_fin_vigencia": "2099-10-22",
                "aseguradora": "AXA COLPATRIA SEGUROS SA", "codigo_tarifa": "112", "estado": "VIGENTE",
            }
        ],
        "rtm": rtm if rtm is not None else {
            "numero_certificado": "184404264",
            "cda": "CENTRO DE DIAGNOSTICO AUTOMOTOR LA AGUACATALA",
            "fecha_expedicion": "2025-10-04", "fecha_vigencia": "2026-10-04",
            "vigente_portal": True, "vigente": True,
        },
        "revisiones": revisiones if revisiones is not None else [
            {"numero_certificado": "184404264", "fecha_vigencia": "2026-10-04",
             "cda": "CENTRO DE DIAGNOSTICO AUTOMOTOR LA AGUACATALA", "vigente_portal": True},
            {"numero_certificado": "176330660", "fecha_vigencia": "2025-10-04",
             "cda": "CENTRO DE DIAGNOSTICO AUTOMOTOR LA AGUACATALA", "vigente_portal": False},
        ],
        "intentos": 1,
        "duraciones_s": [15.2],
        "error": None,
    }


class TestSeccionRunt(unittest.TestCase):
    """Fuente "runt" en el PDF: fila de resumen con placa, banner semaforizado
    por SOAT, tabla de datos del vehículo, historial de pólizas y disposición
    legal honesta (portal público, sin norma habilitante específica)."""

    def _con_runt(self, **kw):
        estudio = estudio_fixture()
        estudio["placa"] = "MVX48E"
        estudio["fuentes"]["runt"] = _fuente_runt(**kw)
        return estudio

    def test_soat_vigente_banner_verde(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_runt()))
        self.assertIn("Vehículo—RUNT", texto)
        self.assertIn("SOATVIGENTE", texto)
        self.assertIn("HONDA", texto)
        self.assertIn("CB160FDLX", texto)
        self.assertIn("MVX48E", texto)

    def test_soat_vencido_banner_rojo(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_runt(
            estado="ADVERTENCIA",
            soat={
                "numero": "3306307200", "aseguradora": "AXA",
                "fecha_inicio_vigencia": "2020-10-23", "fecha_fin_vigencia": "2021-10-22",
                "estado_portal": "NO VIGENTE", "vigente": False,
            },
        )))
        self.assertIn("SOATVENCIDO", texto)

    # ── RTM del vehículo (2026-09-14) ──────────────────────────────────────

    def test_rtm_vigente_en_banner_y_historial(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_runt()))
        plano = texto.replace(" ", "")
        self.assertIn("RTMVIGENTE", plano)  # banner verde la incluye
        self.assertIn("184404264", plano)   # certificado vigente
        self.assertIn("AGUACATALA", plano)  # CDA
        self.assertIn("Historialderevisionestécnico-mecánicas", plano)

    def test_rtm_vencida_banner_rojo_con_soat_al_dia(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_runt(
            estado="ADVERTENCIA",
            rtm={
                "numero_certificado": "176330660",
                "cda": "CENTRO DE DIAGNOSTICO AUTOMOTOR LA AGUACATALA",
                "fecha_expedicion": "2024-10-04", "fecha_vigencia": "2025-10-04",
                "vigente_portal": False, "vigente": False,
            },
        )))
        plano = texto.replace(" ", "")
        self.assertIn("RTM)VENCIDA", plano)
        self.assertIn("NOALDÍAENREVISIÓN", plano)
        # La fila resumen también lo nombra.
        self.assertIn("RTMvencida", plano)

    def test_placa_sin_informacion_neutro(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_runt(
            no_registra=True, soat=None, polizas=[], datos={}, mensaje="La placa no registra información en el RUNT",
        )))
        self.assertIn("PLACASININFORMACIÓN", texto)

    def test_no_propietario_activo(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_runt(
            no_registra=False, soat=None, polizas=[], datos={},
            mensaje="La cédula no corresponde a un propietario activo del vehículo",
        )))
        self.assertIn("PROPIETARIOACTIVO", texto)

    # ── Propietario ≠ persona evaluada (2026-08-30) ────────────────────────

    def _con_vehiculo(self, propietario_es_evaluado, cedula_propietario=None):
        estudio = self._con_runt()
        estudio["vehiculos"] = [{
            "placa": "MVX48E",
            "cedula_propietario": cedula_propietario
                or ("1033688842" if propietario_es_evaluado else "1010213062"),
            "propietario_es_evaluado": propietario_es_evaluado,
        }]
        return estudio

    def test_propietario_distinto_badge_y_fila_persona(self):
        """El dueño del vehículo NO es el evaluado: badge ámbar en la sección
        RUNT + fila 'Propietario del vehículo' en los datos de la persona."""
        texto = _texto_plano(generar_pdf_estudio(self._con_vehiculo(False)))
        self.assertIn("ESDISTINTODELAPERSONAEVALUADA", texto)
        self.assertIn("DISTINTA delapersonaevaluada".replace(" ", ""), texto)
        self.assertIn("1010213062", texto)  # cédula propietario COMPLETA (2026-08-30)
        self.assertIn("Propietariodelvehículo", texto)

    def test_propietario_distinto_en_trazabilidad(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_vehiculo(False)))
        self.assertIn("Vehículo/propietario", texto)
        self.assertIn("(DISTINTAdelapersonaevaluada)", texto.replace(" ", ""))

    def test_propietario_es_el_evaluado_sin_badge(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_vehiculo(True)))
        self.assertIn("eslapersonaevaluada", texto)
        self.assertNotIn("ESDISTINTODELAPERSONAEVALUADA", texto)

    def test_doc_viejo_sin_vehiculos_no_rompe(self):
        """Docs previos a 2026-08-30 (solo placa top-level): se asume que el
        propietario es el evaluado y el PDF se genera igual."""
        texto = _texto_plano(generar_pdf_estudio(self._con_runt()))
        self.assertIn("SOATVIGENTE", texto)
        self.assertIn("Propietariodelvehículo", texto)
        self.assertIn("eslapersonaevaluada", texto)

    def test_rechazo_propietario_explica_cedula_consultada(self):
        """'No propietario activo' debe decir CON QUÉ cédula se consultó para
        no leerse como antecedente del vehículo ni del conductor."""
        estudio = self._con_vehiculo(False)
        estudio["fuentes"]["runt"] = _fuente_runt(
            no_registra=False, soat=None, polizas=[], datos={},
            mensaje="La cédula no corresponde a un propietario activo del vehículo",
        )
        texto = _texto_plano(generar_pdf_estudio(estudio))
        # El label se parte con el wrap de la celda ("Cédula consultada
        # (propietario)" → "(propietario)" cae tras el valor): afirmar los
        # fragmentos estables, sin tildes (pdfplumber las extrae como mojibake).
        self.assertIn("dulaconsultada", texto)
        self.assertIn("elportalvalid", texto)
        self.assertIn("1010213062", texto)

    def test_historial_polizas(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_runt()))
        self.assertIn("HistorialdepólizasSOAT", texto)
        self.assertIn("AXACOLPATRIASEGUROSSA", texto.replace(" ", ""))

    def test_fuente_fallida_muestra_estado(self):
        estudio = self._con_runt(estado="NO_DISPONIBLE", soat=None, polizas=[], datos={})
        estudio["fuentes"]["runt"]["error"] = {"tipo": "portal_inconsistente", "mensaje": "El portal del RUNT no entregó datos"}
        texto = _texto_plano(generar_pdf_estudio(estudio))
        self.assertIn("RUNT", texto)
        # Aviso gris fijo + mención simple en el cuadro aparte (2026-09-25):
        # el mensaje técnico del bot NO llega al informe.
        self.assertIn("Lafuentedeconsultapresentaindisponibilidad", texto)
        self.assertNotIn("noentregódatos", texto)

    def test_resumen_con_cuatro_fuentes(self):
        """2026-09-01: el resumen (y el informe) muestra SOLO las fuentes que
        corrieron — Policía no está en este estudio (clave ausente) y ya NO
        aparece como fila fantasma 'no consultada'."""
        texto = _texto_plano(generar_pdf_estudio(self._con_runt()))
        for frag in ("ManifiestosRNDC", "ProcuraduríaGeneraldelaNación", "RUNT—VehículoMVX48E"):
            self.assertIn(frag, texto)
        self.assertNotIn("PolicíaNacional", texto)

    def test_disposicion_legal_runt(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_runt()))
        self.assertIn("PortalPúblicodeConsultaCiudadana", texto)
        # La extracción de pdfplumber trae los acentos como mojibake según la
        # codificación de la fuente: comparar sin tildes.
        import unicodedata

        plano = "".join(c for c in unicodedata.normalize("NFD", texto) if unicodedata.category(c) != "Mn")
        self.assertIn("noconstituyecertificaciondeaseguramiento", plano)


def _fuente_simit(
    estado="EXITO", total_comparendos=0, total_multas=0, total_acuerdos=0,
    total_deuda=0.0, total_a_pagar=0.0, comparendos=None, mensaje="",
):
    return {
        "estado": estado,
        "origen": "portal",
        "no_registra": None,
        "mensaje": mensaje,
        "placa": "MVX48E",
        "total_comparendos": total_comparendos,
        "total_multas": total_multas,
        "total_acuerdos": total_acuerdos,
        "total_deuda": total_deuda,
        "total_a_pagar": total_a_pagar,
        "comparendos": comparendos if comparendos is not None else ([] if not total_comparendos else [{
            "numero": "130289A", "tipo": "Comparendo", "fecha_imposicion": "2000-04-11",
            "notificacion": "No aplica", "placa": "MVX48E", "secretaria": "Villavicencio",
            "infraccion": "No respetar las señales de tránsito", "estado": "Pendiente",
            "estado_nota": "No tiene curso", "valor": 260130.0, "valor_a_pagar": 260130.0,
        }]),
        "intentos": 1,
        "duraciones_s": [9.8],
        "error": None,
    }


class TestSeccionSimit(unittest.TestCase):
    """Fuente "simit" en el PDF: fila de resumen con placa, banner semaforizado
    por saldo EXIGIBLE (no por deuda histórica), tabla de comparendos y
    disposición legal honesta (consulta sobre la PLACA, no antecedente
    personal)."""

    def _con_simit(self, **kw):
        estudio = estudio_fixture()
        estudio["placa"] = "MVX48E"
        estudio["fuentes"]["simit"] = _fuente_simit(**kw)
        return estudio

    def test_limpio_banner_verde(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_simit(
            mensaje="No tienes comparendos ni multas registradas en Simit",
        )))
        self.assertIn("Comparendos—SIMIT", texto)
        self.assertIn("SINCOMPARENDOSNIMULTASREGISTRADAS", texto)

    def test_saldo_exigible_banner_ambar(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_simit(
            estado="ADVERTENCIA", total_comparendos=1,
            total_deuda=260130.0, total_a_pagar=260130.0,
        )))
        self.assertIn("COMPARENDOSPENDIENTES", texto)
        self.assertIn("SALDOEXIGIBLE", texto)
        self.assertIn("$260.130", texto.replace(" ", ""))

    def test_deuda_historica_sin_saldo_es_neutro(self):
        # ZZZ999 real: 105 pendientes de 1999-2000, agregado "Total a pagar: $0"
        # → neutro (NO verde, NO rojo): no es deuda vigente pero tampoco limpio.
        texto = _texto_plano(generar_pdf_estudio(self._con_simit(
            total_comparendos=88, total_multas=17, total_deuda=40257438.0, total_a_pagar=0.0,
        )))
        self.assertIn("SINSALDOEXIGIBLE", texto)
        self.assertNotIn("SINCOMPARENDOSNIMULTASREGISTRADAS", texto)
        self.assertNotIn("COMPARENDOSPENDIENTES", texto)

    def test_tabla_de_comparendos(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_simit(
            total_comparendos=88, total_multas=17, total_deuda=40257438.0, total_a_pagar=0.0,
        )))
        self.assertIn("130289A", texto)
        self.assertIn("Villavicencio", texto)
        self.assertIn("Pendiente", texto)
        self.assertIn("Detalledecomparendosymultas", texto.replace(" ", ""))

    def test_solo_simit_sin_filas_de_propietario(self):
        """Estudio SIN runt (DESHABILITADA por el plan): la placa es de simit —
        no hay fila de propietario (simit no valida propiedad) ni badge de
        propietario distinto."""
        estudio = self._con_simit()
        estudio["fuentes"]["runt"] = {"estado": "DESHABILITADA", "origen": None, "intentos": 0, "duraciones_s": [], "error": None}
        estudio["fuentes"]["policia"] = {"estado": "DESHABILITADA", "origen": None, "intentos": 0, "duraciones_s": [], "error": None}
        texto = _texto_plano(generar_pdf_estudio(estudio))
        self.assertIn("Placaconsultada(SIMIT)", texto)
        self.assertNotIn("Propietariodelvehículo", texto)
        # Badge de propietario distinto: en mayúsculas (sección RUNT) — con
        # solo simit no hay tríada y NO debe dispararse con None.
        self.assertNotIn("ESDISTINTODELAPERSONAEVALUADA", texto)
        self.assertIn("consultadaenSIMIT", texto)  # trazabilidad

    def test_solo_las_fuentes_del_plan(self):
        """2026-09-01: un plan que SOLO consulta algunas fuentes produce un
        PDF con SOLO esas secciones — las DESHABILITADAS (fuera del plan, no
        corridas ni cobradas) no aparecen ni en el resumen ni como sección
        'no consultada'."""
        estudio = estudio_fixture()
        estudio["placa"] = "QVK013"
        # Plan imaginario: solo procuraduria + simt (el caso reportado).
        estudio["fuentes"]["manifiestos_rndc"] = {"estado": "DESHABILITADA", "origen": None, "intentos": 0, "duraciones_s": [], "error": None}
        estudio["fuentes"]["policia"] = {"estado": "DESHABILITADA", "origen": None, "intentos": 0, "duraciones_s": [], "error": None}
        estudio["fuentes"]["runt"] = {"estado": "DESHABILITADA", "origen": None, "intentos": 0, "duraciones_s": [], "error": None}
        estudio["fuentes"]["simit"] = _fuente_simit(mensaje="No tienes comparendos ni multas registradas en Simit")
        texto = _texto_plano(generar_pdf_estudio(estudio))
        # Procuraduría (corrió, EXITO) y SIMIT sí.
        self.assertIn("Antecedentesdisciplinarios", texto)
        self.assertIn("Comparendos—SIMIT", texto)
        # Las excluidas por el plan NO: ni sección ni fila de resumen.
        self.assertNotIn("Manifiestosdecarga", texto)
        self.assertNotIn("ManifiestosRNDC", texto)
        self.assertNotIn("Antecedentesjudiciales", texto)
        self.assertNotIn("PolicíaNacional", texto)
        self.assertNotIn("Vehículo—RUNT", texto)
        self.assertNotIn("RUNT—Vehículo", texto)
        self.assertNotIn("Noconsultada", texto.replace(" ", ""))
        # La placa es de SIMIT (runt no corrió).
        self.assertIn("Placaconsultada(SIMIT)", texto)

    def test_disposicion_legal_simit(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_simit()))
        self.assertIn("FederaciónColombianadeMunicipios".replace(" ", ""), texto.replace(" ", ""))
        # La consulta es sobre el VEHÍCULO: jamás antecedente personal.
        import unicodedata

        plano = "".join(c for c in unicodedata.normalize("NFD", texto) if unicodedata.category(c) != "Mn")
        self.assertIn("noconstituyeantecedentepersonal", plano)

    def test_resumen_con_cinco_fuentes(self):
        estudio = estudio_fixture()
        estudio["placa"] = "MVX48E"
        estudio["fuentes"]["runt"] = _fuente_runt()
        estudio["fuentes"]["simit"] = _fuente_simit()
        texto = _texto_plano(generar_pdf_estudio(estudio))
        for frag in ("ManifiestosRNDC", "ProcuraduríaGeneraldelaNación", "RUNT—VehículoMVX48E", "SIMIT—ComparendosplacaMVX48E"):
            self.assertIn(frag, texto)
        # Policía no corrió en este estudio: sin fila fantasma.
        self.assertNotIn("PolicíaNacional", texto)


def _fuente_sena(estado="EXITO", no_registra=False, certificados=None, mensaje=""):
    return {
        "estado": estado,
        "origen": "portal",
        "no_registra": no_registra,
        "mensaje": mensaje,
        "total_certificados": len(certificados) if certificados is not None else (0 if no_registra else 2),
        "certificados": certificados if certificados is not None else ([] if no_registra else [
            {
                "registro": "921100151013CC1010213062A",
                "titulo": "TECNÓLOGO EN",
                "tipo": "Acta",
                "programa": "GESTIÓN DE LA PRODUCCIÓN INDUSTRIAL",
                "fecha_certificacion": "2013-02-09",
                "fecha_firma": "2013-02-11",
            },
            {
                "registro": "9303002878307CC1010213062C",
                "titulo": "CURSO ESPECIAL EN",
                "tipo": "Certificado Aprobación",
                "programa": "HIGIENE Y MANIPULACION DE ALIMENTOS.",
                "fecha_certificacion": "2023-11-14",
                "fecha_firma": "2023-11-30",
            },
        ]),
        "intentos": 1,
        "duraciones_s": [12.3],
        "error": None,
    }


def _fuente_sisconmp(estado="EXITO", no_registra=False, capacitaciones=None, mensaje=""):
    return {
        "estado": estado,
        "origen": "portal",
        "no_registra": no_registra,
        "mensaje": mensaje,
        "apellidos": "GOMEZ GOMEZ",
        "nombres": "MARIO",
        "total_capacitaciones": len(capacitaciones) if capacitaciones is not None else (0 if no_registra else 2),
        "capacitaciones": capacitaciones if capacitaciones is not None else ([] if no_registra else [
            {
                "tipo_capacitacion": "CURSO BASICO",
                "nombre": "Curso Básico para el Transporte de Mercancías Peligrosas",
                "entidad_certificadora": "MEN",
                "institucion_educativa": "ACADEMIA DE CONDUCCION INTEGRA",
                "fecha_expedicion": "2020-01-10",
                "fecha_vencimiento": "2099-12-31",  # vigente (fecha lejana: el test nunca caduca)
                "fecha_registro": "2020-01-12",
                "clase": "",
                "descripcion_clase": "",
                "tipo_vehiculo": "",
                "vigente": True,
            },
            {
                "tipo_capacitacion": "TITULACION NCL",
                "nombre": "Titulación en la Norma de Competencia Laboral TMR",
                "entidad_certificadora": "SENA",
                "institucion_educativa": "SENA REGIONAL CUNDINAMARCA",
                "fecha_expedicion": "2001-01-10",
                "fecha_vencimiento": "2001-01-10",  # vencida hace décadas
                "fecha_registro": "2001-01-12",
                "clase": "3",
                "descripcion_clase": "Líquidos inflamables",
                "tipo_vehiculo": "TRACTOCAMION",
                "vigente": False,
            },
        ]),
        "intentos": 1,
        "duraciones_s": [15.1],
        "error": None,
    }


class TestSeccionSena(unittest.TestCase):
    """Fuente "sena" en el PDF: fila de resumen, sección Formación SENA con
    banner informativo (con certificados / sin certificados), tabla de
    certificados y disposición legal honesta (formación, no credencial
    verificada)."""

    def _con_sena(self, **kw):
        estudio = estudio_fixture()
        estudio["fuentes"]["sena"] = _fuente_sena(**kw)
        return estudio

    def test_con_certificados_banner_y_tabla(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_sena()))
        self.assertIn("FormaciónSENA—Certificados", texto.replace(" ", ""))
        self.assertIn("REGISTRA2CERTIFICADO(S)DEFORMACIÓN", texto.replace(" ", ""))
        self.assertIn("Detalledecertificados", texto.replace(" ", ""))
        # (wrap-tolerante: el programa largo se parte entre líneas de la celda)
        self.assertIn("PRODUCCIÓN", texto)
        self.assertIn("ALIMENTOS", texto)
        self.assertIn("TECNÓLOGOEN", texto.replace(" ", ""))
        # Las fechas van unificadas a dd/mm/aaaa (2026-09-04) y caben enteras
        # en su columna (23 mm): sin wrap de la barra.
        self.assertIn("09/02/2013", texto)
        self.assertIn("30/11/2023", texto)
        self.assertNotIn("2013-02-09", texto)

    def test_sin_certificados_banner_verde(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_sena(
            no_registra=True,
            mensaje="La cédula no registra certificados disponibles en el SENA",
        )))
        # (wrap-tolerante: el banner puede partirse entre líneas del PDF)
        self.assertIn("SINCERTIFICADOS", texto.replace(" ", ""))
        self.assertIn("REGISTRADOS", texto.replace(" ", ""))
        self.assertNotIn("Detalledecertificados", texto.replace(" ", ""))
        self.assertIn("noregistracertificados", texto.replace(" ", ""))

    def test_fila_de_resumen(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_sena()))
        self.assertIn("SENA—Certificadosdeformación".replace(" ", ""), texto.replace(" ", ""))
        self.assertIn("2certificado(s)deformación", texto.replace(" ", ""))

    def test_resumen_solo_fuentes_corridas(self):
        """Un estudio SIN sena (clave ausente, fuente posterior): su sección no
        aparece — sin fila fantasma ni sección vacía."""
        estudio = estudio_fixture()
        texto = _texto_plano(generar_pdf_estudio(estudio))
        self.assertNotIn("FormaciónSENA", texto.replace(" ", ""))
        self.assertNotIn("SENA—Certificados", texto.replace(" ", ""))

    def test_disposicion_legal_sena(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_sena()))
        import unicodedata

        plano = "".join(c for c in unicodedata.normalize("NFD", texto) if unicodedata.category(c) != "Mn")
        self.assertIn("NacionaldeAprendizaje", plano.replace(" ", ""))
        # Formación ≠ credencial verificada: el informe NO promete validación de títulos.
        self.assertIn("constituyeverificaciondetitulos", plano.replace(" ", ""))

    def test_fuente_fallida_muestra_estado(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_sena(
            estado="NO_DISPONIBLE", no_registra=None, certificados=[],
        )))
        # La fuente fallida SIGUE mostrándose (honestidad): aviso gris en su
        # sección + mención en el cuadro aparte "Fuentes sin respuesta"
        # (2026-09-25, sin estado ni motivo en el cuadro).
        self.assertIn("FormaciónSENA", texto.replace(" ", ""))
        self.assertIn("Lafuentedeconsultapresentaindisponibilidad", texto)
        self.assertIn("Fuentessinrespuestaenestaconsulta", texto.replace(" ", ""))


class TestSeccionSisconmp(unittest.TestCase):
    """Fuente "sisconmp" en el PDF: fila de resumen, sección Capacitaciones
    Mercancías Peligrosas con banner de vigencia (alguna vigente / todas
    vencidas / sin capacitaciones), tabla de capacitaciones y disposición
    legal honesta (Resolución 1223 de 2014, informativo)."""

    def _con_sis(self, **kw):
        estudio = estudio_fixture()
        estudio["fuentes"]["sisconmp"] = _fuente_sisconmp(**kw)
        return estudio

    def test_con_capacitativas_banner_y_tabla(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_sis()))
        self.assertIn("CapacitacionesenMercancíasPeligrosas".replace(" ", ""), texto)
        self.assertIn("ALMENOSUNAVIGENTE", texto)
        self.assertIn("Detalledecapacitaciones", texto)
        # (wrap-tolerante: la celda ancha parte los textos largos entre líneas)
        self.assertIn("ACADEMIADE", texto)
        self.assertIn("CURSOBASICO", texto)
        self.assertIn("VENCIDA", texto)

    def test_todas_vencidas_banner_ambar(self):
        caps = [
            {
                "tipo_capacitacion": "CURSO BASICO",
                "nombre": "Curso Básico TMR",
                "entidad_certificadora": "MEN",
                "institucion_educativa": "ACADEMIA X",
                "fecha_expedicion": "2001-01-10",
                "fecha_vencimiento": "2001-01-10",
                "clase": "", "descripcion_clase": "", "tipo_vehiculo": "",
                "vigente": False,
            },
        ]
        texto = _texto_plano(generar_pdf_estudio(self._con_sis(capacitaciones=caps)))
        self.assertIn("NINGUNAVIGENTE(VENCIDAS)", texto)

    def test_sin_capacitaciones_banner_verde(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_sis(
            no_registra=True,
            mensaje="No se encontrarón registros sobre el ciudadano.",
        )))
        self.assertIn("SINCAPACITACIONES", texto)
        self.assertIn("REGISTRADAS", texto)
        self.assertNotIn("Detalledecapacitaciones", texto)
        # El mensaje del portal aparece en el detalle.
        self.assertIn("Noseencontrarónregistros".replace(" ", ""), texto)

    def test_fila_de_resumen(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_sis()))
        # (wrap-tolerante: el nombre largo de la fuente se parte en dos líneas
        # de la celda y la extracción intercala las otras columnas)
        self.assertIn("SISCONMP—CapacitacionesMercancías", texto)
        self.assertIn("Peligrosas", texto)
        # (la celda se parte en dos líneas y la extracción intercala la
        # columna Estado: "almenosuna" … "Peligrosas" … "vigente")
        self.assertIn("almenosuna", texto)

    def test_resumen_solo_fuentes_corridas(self):
        """Un estudio SIN sisconmp (clave ausente): sin fila fantasma ni sección."""
        estudio = estudio_fixture()
        texto = _texto_plano(generar_pdf_estudio(estudio))
        self.assertNotIn("SISCONMP", texto)

    def test_disposicion_legal_sisconmp(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_sis()))
        import unicodedata

        plano = "".join(c for c in unicodedata.normalize("NFD", texto) if unicodedata.category(c) != "Mn")
        self.assertIn("Resolucion1223de2014".replace(" ", ""), plano.replace(" ", ""))
        # Informativo: no promete inhabilidad por ausencia de capacitación.
        self.assertIn("caracterinformativo".replace(" ", ""), plano.replace(" ", ""))

    def test_fuente_fallida_muestra_estado(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_sis(
            estado="NO_DISPONIBLE", no_registra=None, capacitaciones=[],
        )))
        self.assertIn("SISCONMP", texto)
        self.assertIn("Lafuentedeconsultapresentaindisponibilidad", texto)
        # Cuadro aparte de fuentes caídas (2026-09-25), solo el nombre.
        self.assertIn("Fuentessinrespuestaenestaconsulta".replace(" ", ""), texto)
        self.assertNotIn("NODISPONIBLE", texto)


def _fuente_situacion_militar(estado="EXITO", no_registra=False, estado_tarjeta=None, mensaje=""):
    return {
        "estado": estado,
        "origen": "portal",
        "no_registra": no_registra,
        "mensaje": mensaje,
        "nombres": "EDWIN MISAEL",
        "apellidos": "ZARATE PEÑA",
        "nombre_completo": "EDWIN MISAEL ZARATE PEÑA",
        "estado_tarjeta_militar": estado_tarjeta if estado_tarjeta is not None else (
            "" if no_registra else "RESERVISTA - 2DA CLASE"),
        "fecha_expedicion": None if no_registra else "2026-09-24",
        "intentos": 1,
        "duraciones_s": [1.8],
        "error": None,
    }


class TestSeccionSituacionMilitar(unittest.TestCase):
    """Fuente "situacion_militar" en el PDF: fila de resumen, sección
    Situación Militar con banner (estado certificado / sin definir / sin
    registro), datos del certificado y disposición legal honesta (Ley
    1581 art. 10, Leyes 1861/1184, Decreto 977)."""

    def _con_sm(self, **kw):
        estudio = estudio_fixture()
        estudio["fuentes"]["situacion_militar"] = _fuente_situacion_militar(**kw)
        return estudio

    def test_reservista_banner_verde(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_sm()))
        self.assertIn("SituaciónMilitar—LibretaMilitar".replace(" ", ""), texto)
        self.assertIn("SITUACIÓNMILITAR:RESERVISTA-2DACLASE".replace(" ", ""), texto)
        self.assertIn("EDWINMISAELZARATEPEÑA".replace(" ", ""), texto)
        self.assertIn("24/09/2026", texto)

    def test_pendiente_banner_ambar(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_sm(
            estado="ADVERTENCIA", estado_tarjeta="PENDIENTE DE DEFINIR",
        )))
        self.assertIn("SINDEFINIR", texto)

    def test_sin_registro_banner_verde(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_sm(
            no_registra=True,
            mensaje="El ciudadano no registra situación militar con cédula de ciudadanía",
        )))
        self.assertIn("SINREGISTRODESITUACIÓNMILITAR".replace(" ", ""), texto)

    def test_fila_de_resumen(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_sm()))
        self.assertIn("Ejército—Situaciónmilitar(libreta)".replace(" ", ""), texto)
        self.assertIn("RESERVISTA-2DACLASE", texto)

    def test_resumen_solo_fuentes_corridas(self):
        estudio = estudio_fixture()
        texto = _texto_plano(generar_pdf_estudio(estudio))
        self.assertNotIn("LibretaMilitar".replace(" ", ""), texto)

    def test_disposicion_legal(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_sm()))
        import unicodedata

        plano = "".join(c for c in unicodedata.normalize("NFD", texto) if unicodedata.category(c) != "Mn")
        self.assertIn("Ley1581de2012".replace(" ", ""), plano.replace(" ", ""))
        self.assertIn("Ley1861de2017".replace(" ", ""), plano.replace(" ", ""))

    def test_fuente_fallida_muestra_estado(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_sm(
            estado="NO_DISPONIBLE", no_registra=None, estado_tarjeta="",
        )))
        self.assertIn("Lafuentedeconsultapresentaindisponibilidad", texto)
        self.assertIn("Fuentessinrespuestaenestaconsulta", texto)


class TestSeccionOfac(unittest.TestCase):
    def _con_ofac(self, aplica=False):
        estudio = estudio_fixture()
        estudio["fuentes"]["ofac"] = {
            "estado": "ADVERTENCIA" if aplica else "EXITO",
            "origen": "portal",
            "aplica": aplica,
            "no_registra": not aplica,
            "total_coincidencias": 1 if aplica else 0,
            "fecha_publicacion": "08/28/2026",
            "total_registros_lista": 19321,
            "sha256_dataset": "a" * 64,
            "coincidencias": ([{
                "uid": "56062",
                "nombre": "Gustavo Francisco PETRO URREGO",
                "programas": ["ILLICIT-DRUGS-EO14059"],
            }] if aplica else []),
            "intentos": 1,
            "duraciones_s": [0.1],
            "error": None,
        }
        return estudio

    def test_coincidencia_exacta_exige_revision_humana(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_ofac(aplica=True)))
        plano = texto.replace(" ", "")
        self.assertIn("COINCIDENCIAEXACTADEIDENTIFICACIÓN", plano)
        self.assertIn("REQUIEREREVISIÓNHUMANA", plano)
        self.assertIn("PETROURREGO", plano)
        self.assertIn("56062", plano)
        self.assertIn("ILLICIT-DRUGS-EO14059", plano)
        self.assertIn("OFACcédula:1intento(s)", plano)

    def test_ficha_sdn_completa_en_la_coincidencia(self):
        """2026-09-25 (patrón TusDatos): la coincidencia pinta la FICHA
        completa del registro SDN — nacimiento, ciudadanías, dirección y el
        link oficial de sanctionssearch."""
        estudio = self._con_ofac(aplica=True)
        estudio["fuentes"]["ofac"]["coincidencias"][0].update({
            "tipo_documento": "Cedula No.", "numero_documento": "208079",
            "pais_documento": "Colombia",
            "fecha_nacimiento": "19 Apr 1960",
            "lugar_nacimiento": "Zipaquira, Colombia",
            "nacionalidades": ["Colombia"],
            "ciudadanias": ["Colombia", "Italy"],
            "direcciones": ["Bogota, Colombia"],
            "alias": ["Gustavo PETRO"],
            "observaciones": "Member, ELN.",
            "fuente_url": "https://sanctionssearch.ofac.treas.gov/Details.aspx?id=56062",
        })
        plano = _texto_plano(generar_pdf_estudio(estudio)).replace(" ", "")
        self.assertIn("Fechadenacimiento", plano)
        self.assertIn("19Apr1960", plano)  # formato SDN original, pasa intacto
        self.assertIn("Zipaquira,Colombia", plano)
        self.assertIn("Ciudadanía", plano)
        self.assertIn("Colombia,Italy", plano)
        self.assertIn("Bogota,Colombia", plano)
        self.assertIn("GustavoPETRO", plano)
        self.assertIn("sanctionssearch.ofac.treas.gov/Details.aspx?id=56062", plano)
        self.assertIn("FichaoficialOFAC", plano)

    def test_sin_coincidencia_exacta(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_ofac()))
        self.assertIn("SINCOINCIDENCIAEXACTADEIDENTIFICACIÓN", texto.replace(" ", ""))
        self.assertNotIn("REQUIEREREVISIÓNHUMANA", texto.replace(" ", ""))

    def test_ofac_nit_es_seccion_empresarial_separada(self):
        estudio = self._con_ofac(aplica=True)
        estudio["nit"] = "9001234567"
        estudio["cedula"] = ""
        estudio["fuentes"]["ofac_nit"] = estudio["fuentes"].pop("ofac")
        estudio["fuentes"]["ofac_nit"]["coincidencias"][0]["nombre"] = "EMPRESA DE PRUEBA S.A.S."
        texto = _texto_plano(generar_pdf_estudio(estudio)).replace(" ", "")
        self.assertIn("NITconsultado9001234567", texto)
        self.assertIn("OFAC—EmpresaporNIT", texto)
        self.assertIn("COINCIDENCIAEXACTADENIT", texto)
        self.assertIn("EMPRESADEPRUEBA", texto)


class TestSeccionSanciones(unittest.TestCase):
    """Sección 4g del PDF: listas internacionales ONU/UE (molde OFAC)."""

    def _con_onu_ue(self, aplica=False, no_disponibles=None):
        estudio = estudio_fixture()
        estudio["fuentes"]["onu_ue"] = {
            "estado": "ADVERTENCIA" if aplica else "EXITO",
            "origen": "portal",
            "aplica": aplica,
            "no_registra": not aplica,
            "total_coincidencias": 1 if aplica else 0,
            "listas": {
                "ONU": {"fecha_publicacion": "2026-09-12", "total_registros_lista": 736,
                        "sha256_dataset": "a" * 64},
                "UE": {"fecha_publicacion": "2026-08-05", "total_registros_lista": 4462,
                       "sha256_dataset": "b" * 64},
            },
            "listas_no_disponibles": no_disponibles or [],
            "coincidencias": ([{
                "lista": "ONU", "uid": "6907993", "nombre": "ERIC BADEGE",
                "programas": ["DRC"], "referencia": "CDi.001",
            }] if aplica else []),
            "intentos": 1,
            "duraciones_s": [0.1],
            "error": None,
        }
        return estudio

    def test_coincidencia_exacta_exige_revision_humana(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_onu_ue(aplica=True)))
        plano = texto.replace(" ", "")
        self.assertIn("Listasinternacionalesdesanciones", plano)
        self.assertIn("COINCIDENCIAEXACTADEIDENTIFICACIÓN", plano)
        self.assertIn("REQUIEREREVISIÓNHUMANA", plano)
        self.assertIn("ERICBADEGE", plano)
        self.assertIn("DRC", plano)
        self.assertIn("listaONU", plano)
        # Metadatos POR LISTA: publicaciones de ambas.
        self.assertIn("12/09/2026", plano)  # fecha ONU legible dd/mm/aaaa
        self.assertIn("05/08/2026", plano)  # fecha UE legible dd/mm/aaaa
        self.assertIn("ONU/UE:1intento(s)", plano)

    def test_sin_coincidencia_y_lista_caida(self):
        texto = _texto_plano(
            generar_pdf_estudio(self._con_onu_ue(aplica=False, no_disponibles=["UE"]))
        )
        plano = texto.replace(" ", "")
        self.assertIn("SINCOINCIDENCIAEXACTADEIDENTIFICACIÓN", plano)
        self.assertNotIn("REQUIEREREVISIÓNHUMANA", plano)
        self.assertIn("Listasnodisponibles", plano)
        self.assertIn("UE", plano)
        # Fila resumen: sin coincidencia + UE no disponible.
        self.assertIn("Sincoincidenciaexactadeidentificación", plano)


class TestSeccionDelitos(unittest.TestCase):
    """Sección 3c del PDF: inhabilidades Ley 1918 (calibrada con caso real)."""

    def _con_delitos(self, no_registra=True):
        estudio = estudio_fixture()
        estudio["fuentes"]["delitos_sexuales"] = {
            "estado": "EXITO" if no_registra else "ADVERTENCIA",
            "origen": "portal",
            "no_registra": no_registra,
            "mensaje": (
                "No registra inhabilidad por delitos sexuales contra menores (Ley 1918 de 2018)"
                if no_registra else "…REGISTRA INHABILIDAD…"
            ),
            "fecha_consulta": "14/09/2026 19:15:28",
            "empresa_consultante": "GLAMPEROS S.A.S.",
            "intentos": 1,
            "duraciones_s": [20.1],
            "error": None,
        }
        return estudio

    def test_no_registra_banner_verde(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_delitos())).replace(" ", "")
        self.assertIn("Inhabilidades—Delitossexualescontramenores(Ley1918)", texto)
        self.assertIn("NOREGISTRAINHABILIDAD(LEY1918DE2018)", texto)
        self.assertIn("ConsultaantelaDIJIN", texto)
        self.assertIn("14/09/202619:15:28", texto)
        self.assertIn("Empresaconsultante", texto)
        self.assertIn("GLAMPEROSS.A.S.", texto)
        self.assertIn("Inhabilidades1918:1intento(s)", texto)
        self.assertIn("Noregistrainhabilidad(Ley1918)", texto)  # fila resumen

    def test_registra_banner_rojo(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_delitos(no_registra=False))).replace(" ", "")
        self.assertIn("REGISTRAINHABILIDAD—REVISIÓNHUMANAOBLIGATORIA", texto)
        self.assertIn("Decreto753de2019", texto)  # párrafo legal honesto


def _fuente_rues(
    estado="EXITO", estado_matricula="ACTIVA", no_registra=False,
    razon_social="GLAMPEROS S.A.S.", representantes=None,
    fecha_renovacion="2026-03-09", ultimo_ano_renovado="2026",
):
    return {
        "estado": estado,
        "origen": "portal",
        "nit": "901923029",
        "nit_con_dv": "901923029-2",
        "razon_social": razon_social,
        "estado_matricula": estado_matricula,
        "no_registra": no_registra,
        "mensaje": f"Matrícula {estado_matricula}" if estado_matricula else "NIT sin registro",
        "camara": "ABURRA SUR",
        "codigo_camara": "55",
        "matricula": "281773",
        "fecha_matricula": "2025-03-03",
        "fecha_renovacion": fecha_renovacion,
        "ultimo_ano_renovado": ultimo_ano_renovado,
        "fecha_cancelacion": None,
        "tipo_sociedad": "SOCIEDAD COMERCIAL",
        "organizacion_juridica": "SOCIEDADES POR ACCIONES SIMPLIFICADAS SAS",
        "categoria_matricula": "SOCIEDAD — PERSONA JURIDICA PRINCIPAL — ESAL",
        "ciiu": {"principal": {"codigo": "6312", "descripcion": "Portales web"}},
        "municipio": "ITAGUI",
        "departamento": "ANTIOQUIA",
        "representantes": representantes if representantes is not None else [
            {"documento": "1010213062", "nombre": "ZARATE PEÑA EDWIN MISAEL"},
        ],
        "fecha_actualizacion": "2026-03-09",
        "intentos": 1,
        "duraciones_s": [0.1],
        "error": None,
    }


class TestSeccionRues(unittest.TestCase):
    def _con_rues(self, **kwargs):
        estudio = estudio_fixture()
        estudio["nit"] = "901923029"
        estudio["fuentes"]["rues"] = _fuente_rues(**kwargs)
        return estudio

    def test_matricula_activa_banner_verde_y_datos(self):
        texto = _texto_plano(generar_pdf_estudio(self._con_rues())).replace(" ", "")
        self.assertIn("RUES—RegistroMercantil(Confecámaras)", texto)
        self.assertIn("MATRÍCULAACTIVA", texto)
        self.assertIn("RENOVADAHASTA09/03/2026", texto)
        self.assertIn("GLAMPEROSS.A.S.", texto)
        self.assertIn("901923029-2", texto)
        self.assertIn("ABURRASUR", texto)
        self.assertIn("Portalesweb", texto)
        self.assertIn("Representantelegal", texto)
        self.assertIn("ZARATEPEÑAEDWINMISAEL", texto)
        self.assertIn("MatrículamercantilACTIVA", texto)  # fila resumen
        self.assertIn("NOconstituyeelCertificadodeExistencia", texto)  # legal honesto

    def test_matricula_distinta_de_activa_es_advertencia(self):
        # Caso real de la sonda: cancelada por Ley 1429 de 2010.
        estudio = self._con_rues(
            estado="ADVERTENCIA", estado_matricula="MATRICULACANCELADALEY1429",
            fecha_renovacion=None, ultimo_ano_renovado=None,
        )
        texto = _texto_plano(generar_pdf_estudio(estudio)).replace(" ", "")
        self.assertIn("LAEMPRESANOESTÁACTIVAENREGISTROMERCANTIL", texto)
        self.assertIn("MatrículaMATRICULACANCELADALEY1429", texto)  # fila resumen

    def test_nit_sin_registro_neutro(self):
        estudio = self._con_rues(estado_matricula=None, no_registra=True, razon_social="")
        estudio["fuentes"]["rues"]["mensaje"] = "NIT sin registro en el Registro Mercantil del RUES."
        texto = _texto_plano(generar_pdf_estudio(estudio)).replace(" ", "")
        self.assertIn("NITSINREGISTROENELREGISTROMERCANTIL", texto)

    def test_fuente_fallida_muestra_estado_no_disponible(self):
        estudio = self._con_rues(estado="NO_DISPONIBLE", estado_matricula=None, razon_social="")
        estudio["fuentes"]["rues"]["error"] = {"tipo": "rues_no_disponible", "mensaje": "El API no respondió"}
        texto = _texto_plano(generar_pdf_estudio(estudio)).replace(" ", "")
        self.assertIn("RUES—RegistroMercantil(Confecámaras)", texto)
        self.assertIn("Lafuentedeconsultapresentaindisponibilidad", texto)
        # El cuadro aparte solo MENCIONA la fuente (2026-09-25): sin motivo.
        self.assertIn("Fuentessinrespuestaenestaconsulta", texto)
        self.assertNotIn("ElAPInorespondió", texto)


class TestFechaLegible(unittest.TestCase):
    """2026-09-04: TODAS las fechas del informe van en dd/mm/aaaa vengan como
    vengan de la fuente (cada portal tiene su formato)."""

    def test_iso_de_runt_simit_sena_rues(self):
        from Funciones.pdf_estudio_seguridad import _fecha_legible
        self.assertEqual(_fecha_legible("2026-10-22"), "22/10/2026")
        self.assertEqual(_fecha_legible("2000-04-11"), "11/04/2000")

    def test_iso_con_hora_de_rama_judicial(self):
        from Funciones.pdf_estudio_seguridad import _fecha_legible
        self.assertEqual(_fecha_legible("2022-05-05T00:00:00"), "05/05/2022")

    def test_slash_del_rndc(self):
        from Funciones.pdf_estudio_seguridad import _fecha_legible
        self.assertEqual(_fecha_legible("2026/08/28 15:15:21"), "28/08/2026 15:15:21")
        self.assertEqual(_fecha_legible("2026/08/21"), "21/08/2026")

    def test_us_de_ofac(self):
        from Funciones.pdf_estudio_seguridad import _fecha_legible
        self.assertEqual(_fecha_legible("08/28/2026"), "28/08/2026")

    def test_ya_legible_pasa_intacto(self):
        from Funciones.pdf_estudio_seguridad import _fecha_legible
        self.assertEqual(_fecha_legible("21/10/2026"), "21/10/2026")

    def test_texto_libre_y_vacios(self):
        from Funciones.pdf_estudio_seguridad import _fecha_legible
        # Lo que no es fecha NO se toca (celdas de datos de portales).
        self.assertEqual(_fecha_legible("VIGENTE"), "VIGENTE")
        self.assertEqual(_fecha_legible(None), "—")
        self.assertEqual(_fecha_legible(""), "—")

    def test_fecha_imposible_no_se_reescribe(self):
        from Funciones.pdf_estudio_seguridad import _fecha_legible
        self.assertEqual(_fecha_legible("2026-13-45"), "2026-13-45")

    def test_en_pdf_completo_no_queda_fecha_iso(self):
        """El informe fixture trae fechas ISO en runt/simit/sena/rues: ninguna
        debe sobrevivir sin convertir en el PDF final."""
        from Funciones.pdf_estudio_seguridad import _fecha_legible

        estudio = estudio_fixture()
        estudio["placa"] = "MVX48E"
        estudio["nit"] = "901923029"
        estudio["fuentes"]["runt"] = _fuente_runt()
        estudio["fuentes"]["simit"] = _fuente_simit(total_comparendos=1)
        estudio["fuentes"]["sena"] = _fuente_sena()
        estudio["fuentes"]["rues"] = _fuente_rues()
        texto = _texto_plano(generar_pdf_estudio(estudio)).replace(" ", "")
        for fecha_iso in (
            "2025-10-23", "2099-10-22",  # SOAT runt
            "2000-04-11",               # simit
            "2013-02-09",               # sena
            "2025-03-03", "2026-03-09",  # rues
        ):
            self.assertNotIn(fecha_iso, texto, f"{fecha_iso} debió ir como {_fecha_legible(fecha_iso)}")


def _jpeg_prueba(ancho=200, alto=120) -> bytes:
    """JPEG real de tamaño conocido (Pillow ya es dependencia del proyecto)."""
    import io

    from PIL import Image as ImagenPIL

    buffer = io.BytesIO()
    ImagenPIL.new("RGB", (ancho, alto), color=(30, 60, 120)).save(buffer, format="JPEG")
    return buffer.getvalue()


class TestSeccionEvidencias(unittest.TestCase):
    """Sección final 'Evidencias de consulta' (pantallazos por fuente, patrón
    TusDatos): una página por fuente con la imagen de GCS; si el blob no se
    puede recuperar, placeholder honesto en vez de romper el informe."""

    def _fixture_con_evidencias(self):
        estudio = estudio_fixture()
        estudio["evidencias"] = {
            "manifiestos_rndc": {
                "gcs_ruta": "SeguridadEstudios/x/2026/ES-TEST0001_captura_manifiestos_rndc.jpg",
                "sha256": "ab12" * 16,
                "tamano": 20480,
                "capturado_en": datetime(2026, 9, 24, 14, 30, 0),
            },
            "procuraduria": {
                "gcs_ruta": "SeguridadEstudios/x/2026/ES-TEST0001_captura_procuraduria.jpg",
                "sha256": "cd34" * 16,
                "tamano": 18432,
                "capturado_en": datetime(2026, 9, 24, 14, 31, 0),
            },
        }
        return estudio

    def test_con_evidencias_agrega_paginas_con_titulo_por_fuente(self):
        from unittest.mock import patch

        from Funciones import storage_seguridad

        jpeg = _jpeg_prueba()
        estudio = self._fixture_con_evidencias()
        paginas_base = len(_texto_por_pagina(generar_pdf_estudio(estudio_fixture())))
        with patch.object(storage_seguridad, "descargar_blob", return_value=jpeg):
            contenido = generar_pdf_estudio(estudio)
        self.assertTrue(contenido.startswith(b"%PDF"))
        paginas = _texto_por_pagina(contenido)
        self.assertEqual(len(paginas), paginas_base + 1 + len(estudio["evidencias"]))
        texto = _texto_plano(contenido)
        self.assertIn("Evidenciasdeconsulta", texto)
        # Una página por fuente con su título legible (orden canónico, PGN última).
        self.assertIn("ManifiestosRNDC", texto)
        self.assertIn("ProcuraduríaGeneraldelaNación", texto)

    def test_sin_evidencias_el_pdf_queda_igual(self):
        # Docs previos (o fuentes sin navegador): la sección no aparece.
        texto = _texto_plano(generar_pdf_estudio(estudio_fixture()))
        self.assertNotIn("Evidenciasdeconsulta", texto)

    def test_blob_irrecuperable_deja_placeholder_honesto(self):
        from unittest.mock import patch

        from Funciones import storage_seguridad

        estudio = self._fixture_con_evidencias()
        with patch.object(storage_seguridad, "descargar_blob", side_effect=RuntimeError("404")):
            contenido = generar_pdf_estudio(estudio)
        texto = _texto_plano(contenido)
        self.assertIn("Evidenciasdeconsulta", texto)
        self.assertIn("nodisponible", texto)
        self.assertEqual(texto.count("nodisponible"), len(estudio["evidencias"]))

    def test_evidencias_reproducible_byte_a_byte(self):
        from unittest.mock import patch

        from Funciones import storage_seguridad

        jpeg = _jpeg_prueba()
        estudio = self._fixture_con_evidencias()
        with patch.object(storage_seguridad, "descargar_blob", return_value=jpeg):
            primero = generar_pdf_estudio(estudio)
            segundo = generar_pdf_estudio(estudio)
        self.assertEqual(primero, segundo)


if __name__ == "__main__":
    unittest.main()
