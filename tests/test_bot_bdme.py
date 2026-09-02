import unittest
from Funciones.bot_bdme import BotBdmeSinResultado, _interpretar_resultado, _normalizar_documento

class TestBotBdme(unittest.TestCase):
    def test_normaliza_nit(self): self.assertEqual(_normalizar_documento("900.123.456-7"), "9001234567")
    def test_limpio(self):
        texto = "NO est� incluido en el BDME. NO ha incumplido acuerdos de pago."
        self.assertTrue(_interpretar_resultado(texto, [])["no_registra"])
    def test_reportado(self):
        texto = "SI est� incluido en el BDME. NO ha incumplido acuerdos de pago."
        self.assertFalse(_interpretar_resultado(texto, [["Entidad"]])["no_registra"])
    def test_ambiguo_falla(self):
        with self.assertRaises(BotBdmeSinResultado): _interpretar_resultado("Formulario", [])
