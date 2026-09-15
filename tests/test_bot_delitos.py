import unittest

from Funciones import bot_delitos_sexuales as bot


# Fragmentos calibrados con la consulta real del 2026-09-14 (1010213062).
RESULTADO_LIMPIO = (
    "Policía Nacional de Colombia informa: Que siendo las 19:15:28 horas del "
    "14/09/2026, el ciudadano identificado con cédula de ciudadanía No. 1010213062, "
    "NO REGISTRA INHABILIDAD La presente consulta se tendrá en consideración por "
    "la entidad o empresa GLAMPEROS S.A.S., con NIT 901923029-2"
)
RESULTADO_INHABILITADO = RESULTADO_LIMPIO.replace(
    "NO REGISTRA INHABILIDAD", "el mismo SI REGISTRA INHABILIDAD", 1
)


class TestBotDelitos(unittest.TestCase):
    def test_veredicto_no_registra_se_chequea_primero(self):
        # Las fórmulas se contienen: "REGISTRA INHABILIDAD" está dentro del
        # texto del caso limpio — el NO manda (mismo orden del fix PGN/CGR).
        self.assertTrue(bot._RE_NO_REGISTRA.search(RESULTADO_LIMPIO))
        self.assertFalse(bot._RE_NO_REGISTRA.search(RESULTADO_INHABILITADO))
        self.assertTrue(bot._RE_REGISTRA.search(RESULTADO_INHABILITADO))

    def test_fecha_consulta_del_portal(self):
        m = bot._RE_FECHA_CONSULTA.search(RESULTADO_LIMPIO)
        self.assertIsNotNone(m)
        self.assertEqual(m.group(1), "19:15:28")
        self.assertEqual(m.group(2), "14/09/2026")

    def test_nit_con_dv_se_calcula_si_falta(self):
        # DV real de GLAMPEROS (visto en el RUES): 901923029-2.
        self.assertEqual(bot._nit_con_dv("901923029"), "901923029-2")
        self.assertEqual(bot._nit_con_dv("901923029-2"), "901923029-2")
        with self.assertRaises(bot.BotDelitosError):
            bot._nit_con_dv("12")

    def test_fecha_normaliza_iso_y_valida(self):
        self.assertEqual(bot._fecha_ddmmaaaa("14/02/2012"), "14/02/2012")
        self.assertEqual(bot._fecha_ddmmaaaa("2012-02-14"), "14/02/2012")
        with self.assertRaises(bot.BotDelitosError):
            bot._fecha_ddmmaaaa("31/02/2012")  # fecha imposible
        with self.assertRaises(bot.BotDelitosError):
            bot._fecha_ddmmaaaa("2012/14/02")


if __name__ == "__main__":
    unittest.main()
