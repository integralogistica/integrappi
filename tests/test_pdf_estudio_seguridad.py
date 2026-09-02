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
        self.assertEqual(4, len(paginas))

        titulos = (
            "Manifiestosdecarga—RNDC",
            "Antecedentesdisciplinarios—Procuraduría",
            "Antecedentesjudiciales—Policía",
            "Vehículo—RUNT",
            "Comparendos—SIMIT",
            "FormaciónSENA—Certificados",
        )
        fuentes_por_pagina = [
            [titulo for titulo in titulos if titulo in pagina]
            for pagina in paginas
        ]
        self.assertEqual(
            [[], list(titulos[:3]), list(titulos[3:5]), [titulos[5]]],
            fuentes_por_pagina,
        )
        self.assertLessEqual(max(map(len, fuentes_por_pagina)), 3)
        # Cada página conserva primero el encabezado fijo (~40 caracteres);
        # el título de la fuente debe aparecer inmediatamente después.
        self.assertLess(paginas[1].index(titulos[0]), 55)
        self.assertLess(paginas[2].index(titulos[3]), 55)
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
        self.assertIn("NODISPONIBLE", texto)
        self.assertNotIn("PARCIALFUENTESNODISPONIBLES", texto)
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
        self.assertIn("noentregóveredicto", texto.replace("á", "a").replace("é", "e"))

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


def _fuente_runt(estado="EXITO", no_registra=None, soat=None, datos=None, polizas=None, mensaje=""):
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
        self.assertIn("noentregódatos", texto.replace("á", "a"))

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
        # Las fechas caben enteras en su columna (23 mm): sin wrap del guion.
        self.assertIn("2013-02-09", texto)
        self.assertIn("2023-11-30", texto)

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
        # La fuente fallida SIGUE mostrándose (honestidad): párrafo de estado.
        self.assertIn("FormaciónSENA", texto.replace(" ", ""))
        self.assertIn("NODISPONIBLE", texto.replace(" ", ""))
        self.assertIn("Noconsultada", texto.replace(" ", ""))


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


if __name__ == "__main__":
    unittest.main()
