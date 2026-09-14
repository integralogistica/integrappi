import unittest
from unittest.mock import Mock, patch

import requests

from Funciones import bot_sanciones


XML_ONU = b'''<?xml version="1.0" encoding="UTF-8"?>
<CONSOLIDATED_LIST dateGenerated="2026-09-12T23:00:05.988Z">
  <INDIVIDUALS>
    <INDIVIDUAL>
      <DATAID>6907993</DATAID>
      <FIRST_NAME>ERIC</FIRST_NAME>
      <SECOND_NAME>BADEGE</SECOND_NAME>
      <UN_LIST_TYPE>DRC</UN_LIST_TYPE>
      <REFERENCE_NUMBER>CDi.001</REFERENCE_NUMBER>
      <INDIVIDUAL_DOCUMENT>
        <TYPE_OF_DOCUMENT>National Identification Number</TYPE_OF_DOCUMENT>
        <NUMBER>123.456.789</NUMBER>
        <ISSUING_COUNTRY>Democratic Republic of the Congo</ISSUING_COUNTRY>
      </INDIVIDUAL_DOCUMENT>
    </INDIVIDUAL>
    <INDIVIDUAL>
      <DATAID>6907995</DATAID>
      <FIRST_NAME>PASAPORTE</FIRST_NAME>
      <UN_LIST_TYPE>SO SOMALIA</UN_LIST_TYPE>
      <INDIVIDUAL_DOCUMENT>
        <TYPE_OF_DOCUMENT>Passport</TYPE_OF_DOCUMENT>
        <NUMBER>123456789</NUMBER>
      </INDIVIDUAL_DOCUMENT>
    </INDIVIDUAL>
  </INDIVIDUALS>
</CONSOLIDATED_LIST>'''

XML_UE = b'''<?xml version="1.0" encoding="UTF-8"?>
<export xmlns="http://eu.europa.ec/fpi/fsd/export" generationDate="2026-08-05T16:47:04.449+02:00">
  <sanctionEntity euReferenceNumber="EU.101.1" logicalId="42">
    <subjectType code="person" classificationCode="P"/>
    <nameAlias firstName="Ali" lastName="TEST UE" wholeName="Ali TEST UE" strong="true"/>
    <identification number="987.654.321" identificationTypeCode="id"
        identificationTypeDescription="National identification card" countryDescription="AFGHANISTAN"/>
  </sanctionEntity>
  <sanctionEntity euReferenceNumber="EU.102.2" logicalId="43">
    <subjectType code="entity" classificationCode="E"/>
    <nameAlias wholeName="EMPRESA UE S.A.S." strong="true"/>
    <identification number="987654321" identificationTypeCode="taxid"
        identificationTypeDescription="Tax identification number"/>
  </sanctionEntity>
</export>'''


class TestBotSanciones(unittest.TestCase):
    def setUp(self):
        bot_sanciones._INDICE = {}
        bot_sanciones._METADATA = {}
        bot_sanciones._CARGADO_EN = 0.0

    def _respuesta(self, contenido: bytes):
        respuesta = Mock(content=contenido)
        respuesta.raise_for_status.return_value = None
        return respuesta

    @patch.object(bot_sanciones.requests, "get")
    def test_documento_onu_coincide_exactamente(self, get):
        get.side_effect = [self._respuesta(XML_ONU), self._respuesta(XML_UE)]
        resultado = bot_sanciones.consultar_sanciones_sync("123456789")
        self.assertTrue(resultado["aplica"])
        self.assertEqual(resultado["total_coincidencias"], 1)
        coincidencia = resultado["coincidencias"][0]
        self.assertEqual(coincidencia["lista"], "ONU")
        self.assertEqual(coincidencia["nombre"], "ERIC BADEGE")
        self.assertEqual(coincidencia["programas"], ["DRC"])
        self.assertEqual(resultado["listas"]["ONU"]["fecha_publicacion"], "2026-09-12")

    @patch.object(bot_sanciones.requests, "get")
    def test_documento_ue_coincide_y_tributario_no(self, get):
        get.side_effect = [self._respuesta(XML_ONU), self._respuesta(XML_UE)]
        resultado = bot_sanciones.consultar_sanciones_sync("987654321")
        self.assertTrue(resultado["aplica"])
        self.assertEqual(resultado["coincidencias"][0]["lista"], "UE")
        self.assertEqual(resultado["coincidencias"][0]["nombre"], "Ali TEST UE")
        self.assertEqual(resultado["coincidencias"][0]["referencia"], "EU.101.1")
        self.assertEqual(resultado["listas"]["UE"]["fecha_publicacion"], "2026-08-05")

    @patch.object(bot_sanciones.requests, "get")
    def test_documento_sin_coincidencia(self, get):
        get.side_effect = [self._respuesta(XML_ONU), self._respuesta(XML_UE)]
        resultado = bot_sanciones.consultar_sanciones_sync("1033688842")
        self.assertFalse(resultado["aplica"])
        self.assertTrue(resultado["no_registra"])
        self.assertEqual(resultado["listas_no_disponibles"], [])

    @patch.object(bot_sanciones.requests, "get")
    def test_pasaporte_y_taxid_no_se_indexan(self, get):
        # El pasaporte ONU y el taxid UE usan los MISMOS números del test, pero
        # sus tipos no son identificadores de cédula: no deben aparecer.
        get.side_effect = [self._respuesta(XML_ONU), self._respuesta(XML_UE)]
        onu = bot_sanciones.consultar_sanciones_sync("123456789")
        ue = bot_sanciones.consultar_sanciones_sync("987654321")
        self.assertEqual(len(onu["coincidencias"]), 1)  # solo el ID del Congo
        self.assertEqual(len(ue["coincidencias"]), 1)   # solo la persona, no la entidad

    @patch.object(bot_sanciones.requests, "get")
    def test_dataset_se_reutiliza_en_memoria(self, get):
        get.side_effect = [self._respuesta(XML_ONU), self._respuesta(XML_UE)]
        bot_sanciones.consultar_sanciones_sync("123456789")
        bot_sanciones.consultar_sanciones_sync("1033688842")
        self.assertEqual(get.call_count, 2)  # una descarga por lista, nada más

    @patch.object(bot_sanciones.requests, "get")
    def test_una_lista_caida_degrade_honesta(self, get):
        get.side_effect = [
            self._respuesta(XML_ONU),
            requests.RequestException("webgate caído"),
        ]
        resultado = bot_sanciones.consultar_sanciones_sync("123456789")
        self.assertTrue(resultado["aplica"])
        self.assertEqual(list(resultado["listas"]), ["ONU"])
        self.assertEqual(resultado["listas_no_disponibles"], ["UE"])
        self.assertIn("UE", resultado["mensaje"])

    @patch.object(bot_sanciones.requests, "get")
    def test_ambas_listas_caidas_es_error(self, get):
        get.side_effect = requests.RequestException("internet caído")
        with self.assertRaises(bot_sanciones.BotSancionesError):
            bot_sanciones.consultar_sanciones_sync("123456789")


if __name__ == "__main__":
    unittest.main()
