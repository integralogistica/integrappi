import unittest
from unittest.mock import Mock, patch

from Funciones import bot_ofac


XML = b'''<?xml version="1.0"?>
<sdnList xmlns="https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/XML">
  <publshInformation><Publish_Date>08/28/2026</Publish_Date><Record_Count>19321</Record_Count></publshInformation>
  <sdnEntry>
    <uid>56062</uid><firstName>Gustavo Francisco</firstName><lastName>PETRO URREGO</lastName><sdnType>Individual</sdnType>
    <programList><program>ILLICIT-DRUGS-EO14059</program></programList>
    <idList>
      <id><uid>123</uid><idType>Cedula No.</idType><idNumber>208079</idNumber><idCountry>Colombia</idCountry></id>
      <id><uid>124</uid><idType>Gender</idType><idNumber>Male</idNumber></id>
    </idList>
  </sdnEntry>
  <sdnEntry>
    <uid>90001</uid><lastName>EMPRESA DE PRUEBA S.A.S.</lastName><sdnType>Entity</sdnType>
    <programList><program>TEST-PROGRAM</program></programList>
    <idList><id><idType>Tax ID No.</idType><idNumber>900.123.456-7</idNumber><idCountry>Colombia</idCountry></id></idList>
  </sdnEntry>
</sdnList>'''


class TestBotOfac(unittest.TestCase):
    def setUp(self):
        bot_ofac._INDICE = {}
        bot_ofac._INDICE_NIT = {}
        bot_ofac._METADATA = {}
        bot_ofac._CARGADO_EN = 0.0

    def _respuesta(self):
        respuesta = Mock(content=XML)
        respuesta.raise_for_status.return_value = None
        return respuesta

    @patch.object(bot_ofac.requests, "get")
    def test_cedula_208079_coincide_exactamente(self, get):
        get.return_value = self._respuesta()
        resultado = bot_ofac.consultar_ofac_sync("208.079")
        self.assertTrue(resultado["aplica"])
        self.assertFalse(resultado["no_registra"])
        self.assertEqual(resultado["coincidencias"][0]["uid"], "56062")
        self.assertEqual(resultado["coincidencias"][0]["nombre"], "Gustavo Francisco PETRO URREGO")
        self.assertEqual(resultado["fecha_publicacion"], "08/28/2026")

    @patch.object(bot_ofac.requests, "get")
    def test_documento_sin_coincidencia(self, get):
        get.return_value = self._respuesta()
        resultado = bot_ofac.consultar_ofac_sync("1033688842")
        self.assertFalse(resultado["aplica"])
        self.assertTrue(resultado["no_registra"])
        self.assertEqual(resultado["coincidencias"], [])

    @patch.object(bot_ofac.requests, "get")
    def test_dataset_se_reutiliza_en_memoria(self, get):
        get.return_value = self._respuesta()
        bot_ofac.consultar_ofac_sync("208079")
        bot_ofac.consultar_ofac_sync("1033688842")
        get.assert_called_once()

    @patch.object(bot_ofac.requests, "get")
    def test_nit_empresarial_es_fuente_separada(self, get):
        get.return_value = self._respuesta()
        empresa = bot_ofac.consultar_ofac_nit_sync("900123456-7")
        persona = bot_ofac.consultar_ofac_sync("9001234567")
        self.assertTrue(empresa["aplica"])
        self.assertEqual(empresa["coincidencias"][0]["tipo"], "Entity")
        self.assertFalse(persona["aplica"])


if __name__ == "__main__":
    unittest.main()
