"""Tests del módulo de estudios de seguridad (auth, aislamiento, estados,
reintentos, caché, minimización). Sin red ni Mongo real: colecciones falsas.

Ejecutar:  python -m unittest tests.test_seguridad_estudios -v
"""
import asyncio
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch

from bson import ObjectId
from fastapi import HTTPException

from Funciones import orquestador_estudios as orch
from rutas import seguridad_estudios as se
from Funciones.auth_seguridad import (
    ROL_ADMIN_EMPRESA,
    ROL_ADMIN_INTEGRA,
    ROL_CONSULTADOR,
    _derivar_rol,
    crear_token_estudios,
)


EMPRESA_A = ObjectId()
EMPRESA_B = ObjectId()
USUARIO_1 = ObjectId()


def actor_consultador(empresa_id=EMPRESA_A, aislamiento=False):
    return {
        "usuario_id": str(USUARIO_1),
        "usuario": "JPEREZ",
        "usuario_nombre": "JUAN PEREZ",
        "usuario_correo": "jperez@integra.com",
        "perfil": "SEGURIDAD",
        "rol": ROL_CONSULTADOR,
        "empresa_id": str(empresa_id),
        "empresa_nombre": "EMPRESA A",
        "empresa_config": {"aislamiento_usuario": aislamiento},
    }


class TestRoles(unittest.TestCase):
    def test_admin_integra(self):
        rol, _ = _derivar_rol({"perfil": "ADMIN"})
        self.assertEqual(rol, ROL_ADMIN_INTEGRA)

    def test_admin_empresa(self):
        rol, _ = _derivar_rol({"perfil": "SEGURIDAD", "rol_seguridad": "ADMIN_EMPRESA", "empresa_id": EMPRESA_A})
        self.assertEqual(rol, ROL_ADMIN_EMPRESA)

    def test_consultador_default(self):
        rol, _ = _derivar_rol({"perfil": "SEGURIDAD", "empresa_id": EMPRESA_A})
        self.assertEqual(rol, ROL_CONSULTADOR)

    def test_perfil_ajeno_sin_rol(self):
        rol, _ = _derivar_rol({"perfil": "CONDUCTOR"})
        self.assertEqual(rol, "")


class TestFiltroEmpresa(unittest.TestCase):
    def test_consultador_ve_solo_su_empresa(self):
        filtro = se._filtro_empresa(actor_consultador())
        # El filtro acepta ObjectId Y string: los docs deben persistir ObjectId
        # (crear_documento_estudio lo garantiza) y el filtro tolera ambos.
        self.assertEqual(filtro["empresa_id"]["$in"], [EMPRESA_A, str(EMPRESA_A)])

    def test_aislamiento_usuario_lo_limita_a_sus_estudios(self):
        actor = actor_consultador(aislamiento=True)
        filtro = se._filtro_empresa(actor)
        self.assertEqual(filtro["usuario_id"]["$in"], [USUARIO_1, str(USUARIO_1)])

    def test_admin_integra_ve_todo(self):
        actor = actor_consultador()
        actor["rol"] = ROL_ADMIN_INTEGRA
        self.assertEqual(se._filtro_empresa(actor), {})

    def test_documento_estudio_persiste_objectid(self):
        """Regresión del bug 2026-08-29: empresa_id/usuario_id como string
        hacían invisible el estudio para su propio creador (filtro compara
        contra ObjectId). El doc DEBE nacer con ObjectId."""
        actor = actor_consultador()
        insertados = {}

        class ColFake:
            def insert_one(self, doc):
                doc["_id"] = ObjectId()
                insertados.update(doc)

        with patch.object(orch, "col_estudios", ColFake()):
            orch.crear_documento_estudio(
                consulta_id="ES-TEST", cedula="1033688842", actor=actor,
                empresa={"nombre": "X", "config": {}}, forzar=False, auditoria={},
            )
        self.assertIsInstance(insertados["empresa_id"], ObjectId)
        self.assertIsInstance(insertados["usuario_id"], ObjectId)


class TestEstadoGlobal(unittest.TestCase):
    def f(self, a, b):
        return orch.calcular_estado_global(
            {"manifiestos_rndc": {"estado": a}, "procuraduria": {"estado": b}}
        )

    def test_ambos_exito(self):
        self.assertEqual(self.f("EXITO", "EXITO"), "COMPLETADA")

    def test_una_advertencia(self):
        self.assertEqual(self.f("EXITO", "ADVERTENCIA"), "COMPLETADA_CON_ADVERTENCIAS")
        self.assertEqual(self.f("ADVERTENCIA", "ADVERTENCIA"), "COMPLETADA_CON_ADVERTENCIAS")

    def test_una_falla_es_parcial(self):
        self.assertEqual(self.f("EXITO", "NO_DISPONIBLE"), "PARCIAL")
        self.assertEqual(self.f("ADVERTENCIA", "ERROR"), "PARCIAL")

    def test_todas_fallan(self):
        self.assertEqual(self.f("ERROR", "NO_DISPONIBLE"), "ERROR")

    def test_nunca_completada_con_fuente_fallida(self):
        for fallo in ("NO_DISPONIBLE", "ERROR"):
            for otra in ("EXITO", "ADVERTENCIA", fallo):
                estado = self.f(otra, fallo)
                self.assertNotEqual(estado, "COMPLETADA")

    def test_deshabilitada_no_cuenta(self):
        estado = orch.calcular_estado_global(
            {"manifiestos_rndc": {"estado": "EXITO"}, "procuraduria": {"estado": "DESHABILITADA"}}
        )
        self.assertEqual(estado, "COMPLETADA")


class TestMinimizacion(unittest.TestCase):
    def test_no_persistir_campos_sensibles(self):
        seccion = orch._limpiar_seccion({"estado": "EXITO", "_pdf_bytes": b"x", "mensaje": "ok"})
        self.assertNotIn("_pdf_bytes", seccion)
        self.assertEqual(seccion["mensaje"], "ok")

    def test_mensaje_truncado_a_300(self):
        self.assertEqual(orch.MAX_MENSAJE, 300)
        # El truncado real se aplica en _ejecutar_fuente al construir la sección.
        largo = "x" * 500
        self.assertLessEqual(len(largo[: orch.MAX_MENSAJE]), 300)

    def test_enmascarar_cedula(self):
        self.assertEqual(orch.enmascarar_cedula("1033688842"), "10******42")
        self.assertEqual(orch.enmascarar_cedula("123"), "***")

    def test_codigo_verificacion_determinista(self):
        self.assertEqual(orch.codigo_verificacion("ES-1"), orch.codigo_verificacion("ES-1"))
        self.assertNotEqual(orch.codigo_verificacion("ES-1"), orch.codigo_verificacion("ES-2"))


class TestReintentos(unittest.TestCase):
    def _correr(self, corutina):
        return asyncio.run(corutina)

    def test_exito_primer_intento(self):
        async def invocar():
            return {"ok": True}

        resultado, intentos, _, error = self._correr(orch._llamar_con_reintento("rndc", "123", invocar))
        self.assertEqual(resultado, {"ok": True})
        self.assertEqual(intentos, 1)
        self.assertIsNone(error)

    def test_falla_una_vez_y_reintenta(self):
        estado = {"fallos": 0}

        async def invocar():
            if estado["fallos"] == 0:
                estado["fallos"] += 1
                raise RuntimeError("portal caído")
            return {"ok": True}

        with patch.object(orch, "BACKOFF_MS", 0):
            resultado, intentos, _, error = self._correr(orch._llamar_con_reintento("rndc", "123", invocar))
        self.assertEqual(resultado, {"ok": True})
        self.assertEqual(intentos, 2)
        self.assertIsNone(error)

    def test_falla_siempre_no_levanta(self):
        async def invocar():
            raise RuntimeError("siempre cae")

        with patch.object(orch, "BACKOFF_MS", 0):
            resultado, intentos, _, error = self._correr(orch._llamar_con_reintento("rndc", "123", invocar))
        self.assertIsNone(resultado)
        self.assertEqual(intentos, 2)
        self.assertIsInstance(error, RuntimeError)

    def test_clasificar_timeout(self):
        estado, error = orch._clasificar_error(asyncio.TimeoutError())
        self.assertEqual(estado, "NO_DISPONIBLE")
        self.assertEqual(error["tipo"], "TimeoutError")

    def test_clasificar_error_bot(self):
        from Funciones.bot_rndc2 import BotRNDC2Error

        estado, error = orch._clasificar_error(BotRNDC2Error("captcha ilegible"))
        self.assertEqual(estado, "ERROR")
        self.assertEqual(error["tipo"], "BotRNDC2Error")


class TestEjecutarFuente(unittest.TestCase):
    def _correr(self, corutina):
        return asyncio.run(corutina)

    def test_cache_hit_no_llama_al_bot(self):
        cache = {
            "_id": ObjectId(),
            "tipo": "manifiestos_rndc",
            "cedula": "1033688842",
            "desde": "2025/08/29",
            "hasta": "2026/08/29",
            "viajes": [{"Nro. de Radicado": "123456789"}],
            "columnas": ["Nro. de Radicado"],
            "total": 1,
        }
        with patch.object(orch, "_buscar_cache", return_value=cache):
            with patch.object(orch, "consultar_historial_viajes_sync") as bot:
                seccion = self._correr(orch._ejecutar_fuente("manifiestos_rndc", "1033688842", actor_consultador(), False))
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertEqual(seccion["origen"], "cache")
        self.assertEqual(seccion["intentos"], 0)
        bot.assert_not_called()

    def test_forzado_ignora_cache(self):
        with patch.object(orch, "_buscar_cache") as buscar:
            buscar.return_value = None  # force=True hace que _buscar_cache retorne None
            with patch.object(orch, "consultar_historial_viajes_sync") as bot:
                # Vacío CONFIRMADO por el portal (respuesta Ajax completa).
                bot.return_value = {"columnas": [], "viajes": [], "mensaje_portal": "Consulta realizada el 2026/08/29"}
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(orch._ejecutar_fuente("manifiestos_rndc", "1033688842", actor_consultador(), True))
            buscar.assert_called_once_with("manifiestos_rndc", "1033688842", True)
        self.assertEqual(seccion["origen"], "portal")
        self.assertEqual(seccion["estado"], "EXITO")  # vacío confirmado es válido
        bot.assert_called_once()

    def test_fallo_de_fuente_no_levanta_y_queda_registrado(self):
        from Funciones.bot_procuraduria import BotProcuraduriaError

        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_antecedentes_sync") as bot:
                bot.side_effect = BotProcuraduriaError("captcha ilegible")
                with patch.object(orch, "BACKOFF_MS", 0):
                    seccion = self._correr(orch._ejecutar_fuente("procuraduria", "1033688842", actor_consultador(), False))
        self.assertEqual(seccion["estado"], "ERROR")
        self.assertEqual(seccion["error"]["tipo"], "BotProcuraduriaError")
        self.assertEqual(seccion["intentos"], 2)

    def test_procuraduria_sin_veredicto_es_advertencia(self):
        resultado = {"no_registra": None, "mensaje": "Certificado generado; ver PDF", "texto_pdf": "", "pdf_bytes": b"PDF"}
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_antecedentes_sync", return_value=resultado):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(orch._ejecutar_fuente("procuraduria", "1033688842", actor_consultador(), False))
        self.assertEqual(seccion["estado"], "ADVERTENCIA")
        self.assertEqual(seccion["_pdf_bytes"], b"PDF")

    def test_viajes_invalidos_filtrados(self):
        resultado = {
            "columnas": ["Nro. de Radicado"],
            "viajes": [
                {"Nro. de Radicado": "123456789"},
                {"Nro. de Radicado": "ABC"},          # no numérico → fuera
                {"Nro. de Radicado": "123"},           # < 6 dígitos → fuera
            ],
            "mensaje_portal": "",
        }
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_historial_viajes_sync", return_value=resultado):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(orch._ejecutar_fuente("manifiestos_rndc", "1033688842", actor_consultador(), False))
        self.assertEqual(seccion["total"], 1)
        self.assertEqual(len(seccion["viajes"]), 1)


class TestFuentePolicia(unittest.TestCase):
    """Fuente "policia" (antecedentes judiciales): tri-estado de procuraduría,
    sin PDF (el portal no genera), nombre del consultado y anti-envenenamiento."""

    def _correr(self, corutina):
        return asyncio.run(corutina)

    def test_cache_hit_no_llama_al_bot(self):
        cache = {
            "_id": ObjectId(),
            "tipo": "policia",
            "cedula": "1033688842",
            "no_registra": True,
            "mensaje": "NO TIENE ASUNTOS PENDIENTES CON LAS AUTORIDADES JUDICIALES",
            "nombre_consultado": "AMAYA TOVAR JHOAM ORLANDO",
            "pdf_tamano": 0,
        }
        with patch.object(orch, "_buscar_cache", return_value=cache):
            with patch.object(orch, "consultar_antecedentes_policia_sync") as bot:
                seccion = self._correr(orch._ejecutar_fuente("policia", "1033688842", actor_consultador(), False))
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertEqual(seccion["origen"], "cache")
        self.assertEqual(seccion["no_registra"], True)
        self.assertEqual(seccion["nombre_consultado"], "AMAYA TOVAR JHOAM ORLANDO")
        bot.assert_not_called()

    def test_exito_cachea_con_leyenda_y_nombre(self):
        resultado = {
            "no_registra": True,
            "mensaje": "NO TIENE ASUNTOS PENDIENTES CON LAS AUTORIDADES JUDICIALES",
            "nombre_consultado": "AMAYA TOVAR JHOAM ORLANDO",
            "pdf_bytes": None,
        }
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_antecedentes_policia_sync", return_value=resultado):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(orch._ejecutar_fuente("policia", "1033688842", actor_consultador(), False))
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertEqual(seccion["origen"], "portal")
        self.assertEqual(seccion["no_registra"], True)
        doc_cache = col.insert_one.call_args[0][0]
        self.assertEqual(doc_cache["tipo"], "policia")
        self.assertEqual(doc_cache["nombre_consultado"], "AMAYA TOVAR JHOAM ORLANDO")

    def test_sin_veredicto_con_nombre_es_advertencia(self):
        # El portal respondió (trajo nombre) pero sin leyenda legible.
        resultado = {"no_registra": None, "mensaje": "", "nombre_consultado": "NOMBRE APELLIDO", "pdf_bytes": None}
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_antecedentes_policia_sync", return_value=resultado):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(orch._ejecutar_fuente("policia", "1033688842", actor_consultador(), False))
        self.assertEqual(seccion["estado"], "ADVERTENCIA")

    def test_resultado_vacio_es_no_disponible_sin_cachear(self):
        # Segunda barrera anti-envenenamiento: dict sin leyenda, sin nombre y
        # sin PDF → NO_DISPONIBLE y NO se escribe caché.
        resultado = {"no_registra": None, "mensaje": "", "nombre_consultado": "", "pdf_bytes": None}
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_antecedentes_policia_sync", return_value=resultado):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(orch._ejecutar_fuente("policia", "1033688842", actor_consultador(), False))
        self.assertEqual(seccion["estado"], "NO_DISPONIBLE")
        self.assertEqual(seccion["error"]["tipo"], "portal_inconsistente")
        col.insert_one.assert_not_called()

    def test_bot_sin_resultado_es_no_disponible(self):
        from Funciones.bot_policia import BotPoliciaSinResultado

        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_antecedentes_policia_sync") as bot:
                bot.side_effect = BotPoliciaSinResultado("sin veredicto")
                with patch.object(orch, "BACKOFF_MS", 0):
                    seccion = self._correr(orch._ejecutar_fuente("policia", "1033688842", actor_consultador(), False))
        self.assertEqual(seccion["estado"], "NO_DISPONIBLE")
        self.assertEqual(seccion["error"]["tipo"], "portal_inconsistente")

    def test_sin_captcha_key_es_no_disponible_y_no_error(self):
        # Falta de configuración NO debe ser ERROR: una causa de config no
        # puede disparar la cadena "todas fallidas → ERROR → reembolso".
        from Funciones.bot_policia import BotPoliciaSinCaptchaKey

        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_antecedentes_policia_sync") as bot:
                bot.side_effect = BotPoliciaSinCaptchaKey("falta key")
                with patch.object(orch, "BACKOFF_MS", 0):
                    seccion = self._correr(orch._ejecutar_fuente("policia", "1033688842", actor_consultador(), False))
        self.assertEqual(seccion["estado"], "NO_DISPONIBLE")
        self.assertEqual(seccion["error"]["tipo"], "configuracion_faltante")

    def test_captcha_fallido_es_error_de_tipo_captcha(self):
        from Funciones.bot_policia import BotPoliciaCaptchaFallido

        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_antecedentes_policia_sync") as bot:
                bot.side_effect = BotPoliciaCaptchaFallido("rechazado")
                with patch.object(orch, "BACKOFF_MS", 0):
                    seccion = self._correr(orch._ejecutar_fuente("policia", "1033688842", actor_consultador(), False))
        self.assertEqual(seccion["estado"], "ERROR")
        self.assertEqual(seccion["error"]["tipo"], "captcha")

    def test_deshabilitada_no_cuenta_para_estado_global(self):
        fuentes = {
            "manifiestos_rndc": {"estado": "EXITO"},
            "procuraduria": {"estado": "EXITO"},
            "policia": {"estado": "DESHABILITADA"},
        }
        self.assertEqual(orch.calcular_estado_global(fuentes), "COMPLETADA")

    def test_todas_exitosa_con_policia_es_completada(self):
        fuentes = {
            "manifiestos_rndc": {"estado": "EXITO"},
            "procuraduria": {"estado": "EXITO"},
            "policia": {"estado": "EXITO"},
        }
        self.assertEqual(orch.calcular_estado_global(fuentes), "COMPLETADA")

    def test_policia_sola_con_error_no_pasa_a_completada(self):
        fuentes = {
            "manifiestos_rndc": {"estado": "DESHABILITADA"},
            "procuraduria": {"estado": "DESHABILITADA"},
            "policia": {"estado": "ERROR"},
        }
        self.assertEqual(orch.calcular_estado_global(fuentes), "ERROR")



    def test_limite_por_empresa(self):
        se._RATE.clear()
        actor = actor_consultador()
        actor["empresa_config"] = {"consultas_por_minuto": 2}
        se._verificar_rate_limit(actor)  # 1
        se._verificar_rate_limit(actor)  # 2
        with self.assertRaises(HTTPException) as ctx:
            se._verificar_rate_limit(actor)  # 3 → 429
        self.assertEqual(ctx.exception.status_code, 429)

    def test_admin_integra_sin_limite(self):
        se._RATE.clear()
        actor = actor_consultador()
        actor["rol"] = ROL_ADMIN_INTEGRA
        for _ in range(30):
            se._verificar_rate_limit(actor)


class TestVerificacion(unittest.TestCase):
    def test_codigo_valido(self):
        doc = {
            "consulta_id": "ES-ABC",
            "codigo_verificacion": "XYZ1234567",
            "estado": "COMPLETADA",
            "creado_en": datetime(2026, 8, 29),
            "empresa_nombre": "EMPRESA A",
            "cedula": "1033688842",
        }
        with patch.object(se, "col_estudios") as col:
            col.find_one.return_value = doc
            with patch.object(se, "registrar_evento"):
                respuesta = se.verificar_estudio("ES-ABC", "XYZ1234567")
        self.assertTrue(respuesta["valido"])
        self.assertEqual(respuesta["cedula"], "10******42")

    def test_codigo_invalido_no_revela(self):
        with patch.object(se, "col_estudios") as col:
            col.find_one.return_value = None
            with patch.object(se, "registrar_evento"):
                respuesta = se.verificar_estudio("ES-ABC", "INVALIDO")
        self.assertEqual(respuesta, {"valido": False})


class TestObtenerEstudio(unittest.TestCase):
    def test_cross_tenant_404_y_evento(self):
        actor_b = actor_consultador(empresa_id=EMPRESA_B)
        doc = {"consulta_id": "ES-XYZ", "empresa_id": EMPRESA_A}
        with patch.object(se, "col_estudios") as col:
            # _filtro_empresa filtra por empresa B: no lo encuentra.
            col.find_one.return_value = None
            with patch.object(se, "registrar_evento") as evento:
                with self.assertRaises(HTTPException) as ctx:
                    se._obtener_estudio("ES-XYZ", actor_b)
        self.assertEqual(ctx.exception.status_code, 404)
        evento.assert_called_once()
        # El evento de acceso denegado cita la consulta intentada.
        self.assertEqual(evento.call_args.kwargs.get("consulta_id"), "ES-XYZ")


if __name__ == "__main__":
    unittest.main()


class TestEjecutarEstudioFuentesParcial(unittest.TestCase):
    """Regresión 2026-08-29: cuando SOLO algunas fuentes están habilitadas
    (usuario eligió una, o plan mono-fuente), las demás van DESHABILITADA.
    El gather debe aceptarlas (antes: TypeError unhashable dict) y el estado
    global ignora las deshabilitadas."""

    def test_fuente_no_elegida_queda_deshabilitada(self):
        import asyncio
        from unittest.mock import patch

        from Funciones import orquestador_estudios as orch

        empresa = {"_id": "emp", "nombre": "E", "config": {}}
        doc_inicial = {"_id": "x", "consulta_id": "ES-X"}
        persistido: dict = {}

        def _find_one(query=None, *a, **k):
            # 1ª lectura (busca _id inicial) / 2ª (doc persistido tras el update).
            return persistido.get("doc") or doc_inicial

        def _update_one(query, update):
            # Simular el $set del orquestador sobre el doc en memoria.
            persistido["doc"] = {**doc_inicial, **update.get("$set", {})}
            return None

        async def _fuente_ok(nombre, cedula, actor, forzar):
            return {"estado": "EXITO", "origen": "cache", "intentos": 1, "duraciones_s": [], "error": None}

        with patch.object(orch.col_estudios, "find_one", side_effect=_find_one), \
             patch.object(orch.col_estudios, "update_one", side_effect=_update_one), \
             patch.object(orch, "_ejecutar_fuente", side_effect=_fuente_ok):
            resultado = asyncio.run(orch.ejecutar_estudio(
                consulta_id="ES-X", cedula="1033688842", actor={"usuario": "U", "usuario_id": "x", "empresa_id": "e"},
                empresa=empresa, forzar=False, auditoria={},
                registrar_evento=lambda *a, **k: None,
                fuentes=["procuraduria"],  # SOLO procuraduría
            ))
        self.assertEqual(resultado["fuentes"]["procuraduria"]["estado"], "EXITO")
        self.assertEqual(resultado["fuentes"]["manifiestos_rndc"]["estado"], "DESHABILITADA")
        # El estado global se calcula SOLO sobre la fuente que corrió.
        self.assertEqual(resultado["estado"], "COMPLETADA")


class TestRNDCVacioSinConfirmacion(unittest.TestCase):
    """Regresión 2026-08-29: RNDC con 0 viajes y SIN 'Consulta realizada' es
    NO_DISPONIBLE y NO se cachea (un vacío sin confirmación era una respuesta
    Ajax incompleta que envenenaba la caché 24 h)."""

    def _correr(self, resultado_bot):
        import asyncio
        from unittest.mock import patch

        from Funciones import orquestador_estudios as orch

        async def invocar():
            return resultado_bot

        with patch.object(orch, "_buscar_cache", return_value=None), \
             patch.object(orch, "_llamar_con_reintento",
                          return_value=(resultado_bot, 1, [1.0], None)), \
             patch.object(orch.col_consultas, "insert_one") as insert_cache:
            seccion = asyncio.run(orch._ejecutar_fuente(
                "manifiestos_rndc", "1033688842",
                {"usuario": "U", "perfil": "SEGURIDAD", "empresa_id": "e", "usuario_id": "u"},
                forzar=False,
            ))
        return seccion, insert_cache

    def test_vacio_sin_confirmacion_no_cachea(self):
        seccion, insert_cache = self._correr({"viajes": [], "columnas": [], "mensaje_portal": ""})
        self.assertEqual(seccion["estado"], "NO_DISPONIBLE")
        self.assertEqual(seccion["total"], 0)
        insert_cache.assert_not_called()  # no envenena la caché

    def test_vacio_confirmado_si_es_exito_y_cachea(self):
        # El portal confirmó "sin resultados" y hay viajes reales de todos modos.
        seccion, insert_cache = self._correr({
            "viajes": [], "columnas": [],
            "mensaje_portal": "Consulta realizada el 2026/08/29 a las 10:00:00",
        })
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertEqual(seccion["total"], 0)
        insert_cache.assert_called_once()  # vacío CONFIRMADO sí se cachea

    def test_con_viajes_cachea_normal(self):
        viaje = {"Nro. de Radicado": "123408537", "Placa": "ABC123"}
        seccion, insert_cache = self._correr({
            "viajes": [viaje], "columnas": ["Nro. de Radicado"],
            "mensaje_portal": "Consulta realizada el 2026/08/29",
        })
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertEqual(seccion["total"], 1)
        insert_cache.assert_called_once()


class TestReintentoVacioSinConfirmar(unittest.TestCase):
    """Regresión 2026-08-29 (tarde): la respuesta RNDC incompleta (0 viajes sin
    'Consulta realizada') NO es excepción — antes se aceptaba al primer intento
    sin reintentar. Ahora consume intento y se reintenta; agotados, es
    NO_DISPONIBLE con tipo portal_inconsistente."""

    def test_vacio_sin_confirmar_reintenta_y_cede(self):
        import asyncio

        async def invocar():
            return {"viajes": [], "columnas": [], "mensaje_portal": ""}  # incompleta SIEMPRE

        with patch.object(orch, "BACKOFF_MS", 0):
            resultado, intentos, _, error = asyncio.run(
                orch._llamar_con_reintento("manifiestos_rndc", "123", invocar)
            )
        self.assertIsNone(resultado)
        self.assertEqual(intentos, 2)  # usó los 2 intentos
        self.assertIsInstance(error, orch.BotRNDC2Incompleto)
        estado, detalle = orch._clasificar_error(error)
        self.assertEqual(estado, "NO_DISPONIBLE")
        self.assertEqual(detalle["tipo"], "portal_inconsistente")

    def test_vacio_sin_confirmar_recupera_en_segundo_intento(self):
        import asyncio

        estado_interno = {"veces": 0}

        async def invocar():
            estado_interno["veces"] += 1
            if estado_interno["veces"] == 1:
                return {"viajes": [], "columnas": [], "mensaje_portal": ""}  # incompleta
            return {"viajes": [{"Nro. de Radicado": "123408537"}], "mensaje_portal": "Consulta realizada"}  # OK

        with patch.object(orch, "BACKOFF_MS", 0):
            resultado, intentos, _, error = asyncio.run(
                orch._llamar_con_reintento("manifiestos_rndc", "123", invocar)
            )
        self.assertIsNone(error)
        self.assertEqual(intentos, 2)
        self.assertEqual(len(resultado["viajes"]), 1)

    def test_resultado_normal_no_reintenta(self):
        import asyncio

        async def invocar():
            return {"viajes": [], "mensaje_portal": "Consulta realizada el 2026/08/29"}  # vacío CONFIRMADO

        resultado, intentos, _, error = asyncio.run(
            orch._llamar_con_reintento("manifiestos_rndc", "123", invocar)
        )
        self.assertIsNone(error)
        self.assertEqual(intentos, 1)  # vacío confirmado es válido a la primera
