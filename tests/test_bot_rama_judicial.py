import unittest
from Funciones.bot_rama_judicial import BotRamaJudicialError, _nombre, _pagina_url

class TestBotRamaJudicial(unittest.TestCase):
    def test_nombre_completo(self): self.assertEqual(_nombre("  Ana   María "), "ANA MARÍA")
    def test_nombre_invalido(self):
        with self.assertRaises(BotRamaJudicialError): _nombre("123")
    def test_paginacion_conserva_filtros(self):
        url = "https://x/api?nombre=ANA+PEREZ&tipoPersona=nat&SoloActivos=false&pagina=1"
        nueva = _pagina_url(url, 3)
        self.assertIn("SoloActivos=false", nueva)
        self.assertIn("pagina=3", nueva)
