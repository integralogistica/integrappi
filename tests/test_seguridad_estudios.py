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
from Funciones import auth_seguridad as auth
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


class TestApiKeyAuth(unittest.TestCase):
    """API keys de integración (2026-08-30): mismo header Bearer con prefijo
    sek_; actor CONSULTADOR de la empresa con canal="api". La clave plana solo
    existe al crearla (en BD vive su SHA-256)."""

    class ColFakeFind:
        def __init__(self, doc):
            self._doc = doc
            self.updates = []

        def find_one(self, filtro=None):
            return self._doc

        def update_one(self, filtro, cambios):
            self.updates.append((filtro, cambios))

    def _key(self, activo=True, empresa_id=EMPRESA_A):
        clave, doc = auth.generar_api_key("Integración SILO", empresa_id, "EZARATE")
        doc["_id"] = ObjectId()
        doc["activo"] = activo
        return clave, doc

    def _empresa_doc(self, activo=True):
        return {"_id": EMPRESA_A, "nombre": "EMPRESA A", "activo": activo, "config": {}}

    def test_generar_api_key_hash_y_prefijo(self):
        import hashlib

        clave, doc = auth.generar_api_key("TEST", EMPRESA_A, "EZARATE")
        self.assertTrue(clave.startswith("sek_"))
        self.assertEqual(doc["hash_sha256"], hashlib.sha256(clave.encode("utf-8")).hexdigest())
        self.assertTrue(doc["prefijo"].startswith("sek_"))
        self.assertTrue(doc["prefijo"].endswith("…"))
        # La clave plana jamás viaja en el doc persistible.
        self.assertNotIn(clave, str(doc))
        self.assertEqual(doc["scopes"], ["estudios:crear", "estudios:leer"])

    def test_actor_de_api_key_valida(self):
        clave, doc = self._key()
        with patch.object(auth, "col_api_keys", self.ColFakeFind(doc)):
            with patch.object(auth, "col_empresas", self.ColFakeFind(self._empresa_doc())):
                actor = auth._actor_de_api_key(clave)
        self.assertEqual(actor["rol"], ROL_CONSULTADOR)  # una API key NUNCA es admin
        self.assertEqual(actor["canal"], "api")
        self.assertEqual(actor["usuario"], "API:Integración SILO")
        self.assertIsNone(actor["usuario_id"])  # no hay humano detrás
        self.assertEqual(actor["empresa_id"], str(EMPRESA_A))
        self.assertEqual(actor["api_key_nombre"], "Integración SILO")

    def test_actor_de_api_key_revocada_401(self):
        clave, doc = self._key(activo=False)
        # El lookup filtra activo=True → la revocada no aparece (401 genérico).
        col = self.ColFakeFind(None)
        with patch.object(auth, "col_api_keys", col):
            with self.assertRaises(HTTPException) as ctx:
                auth._actor_de_api_key(clave)
        self.assertEqual(ctx.exception.status_code, 401)

    def test_actor_de_api_key_empresa_inactiva_401(self):
        clave, doc = self._key()
        with patch.object(auth, "col_api_keys", self.ColFakeFind(doc)):
            with patch.object(auth, "col_empresas", self.ColFakeFind(self._empresa_doc(activo=False))):
                with self.assertRaises(HTTPException) as ctx:
                    auth._actor_de_api_key(clave)
        self.assertEqual(ctx.exception.status_code, 401)

    def test_actor_actual_enruta_por_prefijo(self):
        """`actor_actual` con sek_… resuelve como API key (sin tocar el JWT);
        con un JWT normal va por _cargar_actor (canal portal)."""
        clave, doc = self._key()
        with patch.object(auth, "col_api_keys", self.ColFakeFind(doc)):
            with patch.object(auth, "col_empresas", self.ColFakeFind(self._empresa_doc())):
                actor = asyncio.run(auth.actor_actual(clave))
        self.assertEqual(actor["canal"], "api")

        token = crear_token_estudios(
            {"_id": USUARIO_1, "usuario": "JPEREZ", "perfil": "SEGURIDAD"}, str(EMPRESA_A), ROL_CONSULTADOR
        )
        actor_jwt = {"canal": "portal", "usuario_id": str(USUARIO_1)}
        with patch.object(auth, "_cargar_actor", return_value=actor_jwt) as cargar:
            resultado = asyncio.run(auth.actor_actual(token))
        cargar.assert_called_once_with(str(USUARIO_1))
        self.assertEqual(resultado["canal"], "portal")

    def test_doc_estudio_marca_canal_api(self):
        """El doc del estudio persiste canal="api" + api_key y SIN usuario_id."""
        actor = actor_consultador()
        actor.update({"canal": "api", "usuario_id": None, "usuario": "API:SILO",
                      "api_key_id": "5f1c" * 6, "api_key_nombre": "SILO"})
        insertados = {}

        class ColFake:
            def insert_one(self, doc):
                doc["_id"] = ObjectId()
                insertados.update(doc)

        with patch.object(orch, "col_estudios", ColFake()):
            orch.crear_documento_estudio(
                consulta_id="ES-API1", cedula="1033688842", actor=actor,
                empresa={"nombre": "X", "config": {}}, forzar=False, auditoria={},
            )
        self.assertEqual(insertados["canal"], "api")
        self.assertEqual(insertados["api_key"]["nombre"], "SILO")
        self.assertIsNone(insertados["usuario_id"])
        self.assertEqual(insertados["usuario"], "API:SILO")

    def test_doc_estudio_canal_portal_default(self):
        """Actor humano (sin canal) → doc con canal="portal" y api_key None."""
        insertados = {}

        class ColFake:
            def insert_one(self, doc):
                doc["_id"] = ObjectId()
                insertados.update(doc)

        with patch.object(orch, "col_estudios", ColFake()):
            orch.crear_documento_estudio(
                consulta_id="ES-PORTAL1", cedula="1033688842", actor=actor_consultador(),
                empresa={"nombre": "X", "config": {}}, forzar=False, auditoria={},
            )
        self.assertEqual(insertados["canal"], "portal")
        self.assertIsNone(insertados["api_key"])
        self.assertIsInstance(insertados["usuario_id"], ObjectId)

    def test_filtro_empresa_api_ignora_aislamiento(self):
        """Una API key es una integración de la EMPRESA: ve todos sus estudios
        aunque la empresa tenga aislamiento_usuario (que aplica a humanos)."""
        actor = actor_consultador(aislamiento=True)
        actor.update({"canal": "api"})
        filtro = se._filtro_empresa(actor)
        self.assertNotIn("usuario_id", filtro)
        # El humano con aislamiento sí lo tiene (regresión).
        self.assertIn("usuario_id", se._filtro_empresa(actor_consultador(aislamiento=True)))


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


class TestFuenteProcuraduriaAntiEnvenenamiento(unittest.TestCase):
    """Regresión del bug 2026-08-30 (cédula 1033688842, ES-8F34FEE82AAA): el
    postback de la PGN quedó en el formulario (sin veredicto y SIN PDF) y el
    bot lo retornaba silenciosamente → ADVERTENCIA 'no concluyente — ver PDF'
    apuntando a un anexo INEXISTENTE, y quedaba CACHÉ 24 h que repetía el
    resultado vacío aunque el portal respondiera bien después."""

    def _correr(self, corutina):
        return asyncio.run(corutina)

    def test_sin_veredicto_y_sin_pdf_no_disponible_sin_cachear(self):
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_antecedentes_sync") as bot:
                # El portal respondió la página pero sin certificado ni veredicto.
                bot.return_value = {"pdf_bytes": b"", "no_registra": None, "mensaje": "", "texto_pdf": "", "texto_resultado": "inicio"}
                with patch.object(orch, "col_consultas") as col:
                    seccion = self._correr(
                        orch._ejecutar_fuente("procuraduria", "1033688842", actor_consultador(), False)
                    )
        self.assertEqual(seccion["estado"], "NO_DISPONIBLE")
        self.assertEqual(seccion["error"]["tipo"], "portal_inconsistente")
        col.insert_one.assert_not_called()  # nada de esto va a caché

    def test_respuesta_sin_veredicto_no_se_cachea_aunque_haya_pdf(self):
        """Un archivo no sustituye el único dato requerido: el veredicto."""
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_antecedentes_sync") as bot:
                bot.return_value = {
                    "pdf_bytes": b"%PDF-foto-escaneada", "no_registra": None,
                    "mensaje": "Certificado generado; ver PDF", "texto_pdf": "",
                }
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("procuraduria", "1033688842", actor_consultador(), False)
                    )
        self.assertEqual(seccion["estado"], "NO_DISPONIBLE")
        self.assertNotIn("pdf_tamano", seccion)
        col.insert_one.assert_not_called()

    def test_cache_venenida_ignorada_al_leer(self):
        """Cachés ya escritas con el bug (no_registra None, sin PDF) se ignoran
        al leer: el hit no puede devolver un 'no concluyente' fantasma."""
        from rutas import seguridad as rseg

        viciada = {"_id": ObjectId(), "tipo": "procuraduria", "no_registra": None, "pdf_tamano": 0}
        with patch.object(rseg, "col_consultas") as col:
            col.find_one.return_value = viciada
            doc = rseg._buscar_cache("procuraduria", "1033688842", False)
        self.assertIsNone(doc)

    def test_excepcion_sin_resultado_es_no_disponible(self):
        """BotProcuraduriaSinResultado (postback que no llegó) → NO_DISPONIBLE
        portal_inconsistente (no ERROR: no dispara la cadena de reembolso)."""
        from Funciones.bot_procuraduria import BotProcuraduriaSinResultado

        estado, error = orch._clasificar_error(
            BotProcuraduriaSinResultado("postback sin respuesta")
        )
        self.assertEqual(estado, "NO_DISPONIBLE")
        self.assertEqual(error["tipo"], "portal_inconsistente")

    def test_timeout_de_navegacion_es_no_disponible(self):
        estado, error = orch._clasificar_error(
            orch.BotProcuraduriaSinResultado("Page.goto: net::ERR_CONNECTION_TIMED_OUT")
        )
        self.assertEqual(estado, "NO_DISPONIBLE")
        self.assertEqual(error["tipo"], "portal_inconsistente")

    def test_procuraduria_tiene_presupuesto_propio_150s(self):
        """2026-09-24: el presupuesto de procuraduría pasó de 300→90→150 s
        (decisión del usuario; con 90 reventaba en horas pico: postback PGN
        real de 101 s; SEGURIDAD_PROCURADURIA_TIMEOUT_S puede cambiarlo sin
        deploy); las demás siguen con el global (150 s). El mensaje de
        timeout debe nombrar el presupuesto de LA fuente, no el global."""
        self.assertEqual(orch._timeout_fuente("procuraduria"), 150.0)
        self.assertEqual(orch._timeout_fuente("runt"), orch.TIMEOUT_FUENTE_S)
        self.assertEqual(orch._timeout_fuente("simit"), 150.0)
        estado, error = orch._clasificar_error(asyncio.TimeoutError(), "procuraduria")
        self.assertIn("150", error["mensaje"])
        estado, error = orch._clasificar_error(asyncio.TimeoutError(), "runt")
        self.assertIn("150", error["mensaje"])


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
            buscar.assert_called_once_with("manifiestos_rndc", "1033688842", True, placa=None)
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

    def test_procuraduria_sin_veredicto_no_disponible(self):
        resultado = {"no_registra": None, "mensaje": "Certificado generado; ver PDF", "texto_pdf": "", "pdf_bytes": b"PDF"}
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_antecedentes_sync", return_value=resultado):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(orch._ejecutar_fuente("procuraduria", "1033688842", actor_consultador(), False))
        self.assertEqual(seccion["estado"], "NO_DISPONIBLE")
        self.assertNotIn("_pdf_bytes", seccion)

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


class TestFuenteRunt(unittest.TestCase):
    """Fuente "runt" (consulta de vehículo por placa + cédula del propietario):
    caché con clave (tipo, cédula, PLACA), semáforo SOAT (vencido = ADVERTENCIA)
    y anti-envenenamiento análogo al de policía."""

    RESULTADO_OK = {
        "placa": "MVX48E",
        "cedula": "1010213062",
        "no_registra": None,
        "mensaje": "",
        "datos_vehiculo": {
            "placa": "MVX48E", "marca": "HONDA", "linea": "CB 160F DLX", "modelo": "2018",
            "clase": "MOTOCICLETA", "numero_motor": "KC23E-7-3006584",
        },
        "soat": {
            "numero": "3453028900", "aseguradora": "AXA COLPATRIA SEGUROS SA",
            "fecha_inicio_vigencia": "2025-10-23", "fecha_fin_vigencia": "2099-10-22",
            "estado_portal": "VIGENTE", "vigente": True,
        },
        "polizas": [
            {
                "numero": "3453028900", "fecha_expedicion": "2025-10-04",
                "fecha_inicio_vigencia": "2025-10-23", "fecha_fin_vigencia": "2099-10-22",
                "aseguradora": "AXA COLPATRIA SEGUROS SA", "codigo_tarifa": "112", "estado": "VIGENTE",
            }
        ],
        "pdf_bytes": None,
    }

    def _correr(self, corutina):
        return asyncio.run(corutina)

    def test_cache_hit_no_llama_al_bot_y_recibe_placa(self):
        cache = {
            "_id": ObjectId(),
            "tipo": "runt",
            "cedula": "1010213062",
            "placa": "MVX48E",
            "no_registra": None,
            "mensaje": "",
            "datos_vehiculo": self.RESULTADO_OK["datos_vehiculo"],
            "soat": self.RESULTADO_OK["soat"],
            "polizas": self.RESULTADO_OK["polizas"],
        }
        with patch.object(orch, "_buscar_cache", return_value=cache) as buscar:
            with patch.object(orch, "consultar_vehiculo_runt_sync") as bot:
                seccion = self._correr(
                    orch._ejecutar_fuente("runt", "1010213062", actor_consultador(), False, placa="MVX48E")
                )
        buscar.assert_called_once_with("runt", "1010213062", False, placa="MVX48E")
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertEqual(seccion["origen"], "cache")
        self.assertEqual(seccion["datos_vehiculo"]["marca"], "HONDA")
        self.assertEqual(seccion["soat"]["vigente"], True)
        bot.assert_not_called()

    def test_exito_cachea_con_placa_y_datos(self):
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_vehiculo_runt_sync", return_value=self.RESULTADO_OK):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("runt", "1010213062", actor_consultador(), False, placa="MVX48E")
                    )
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertEqual(seccion["origen"], "portal")
        self.assertEqual(seccion["placa"], "MVX48E")
        doc_cache = col.insert_one.call_args[0][0]
        self.assertEqual(doc_cache["tipo"], "runt")
        self.assertEqual(doc_cache["placa"], "MVX48E")
        self.assertEqual(doc_cache["datos_vehiculo"]["marca"], "HONDA")
        self.assertEqual(doc_cache["soat"]["numero"], "3453028900")

    def test_placa_viaja_al_bot(self):
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_vehiculo_runt_sync") as bot:
                bot.return_value = self.RESULTADO_OK
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    self._correr(
                        orch._ejecutar_fuente("runt", "1010213062", actor_consultador(), False, placa="MVX48E")
                    )
        bot.assert_called_once_with("MVX48E", "1010213062")

    def test_soat_vencido_es_advertencia(self):
        resultado = {
            **self.RESULTADO_OK,
            "soat": {
                "numero": "3306307200", "aseguradora": "AXA",
                "fecha_inicio_vigencia": "2020-10-23", "fecha_fin_vigencia": "2021-10-22",
                "estado_portal": "NO VIGENTE", "vigente": False,
            },
            "polizas": [
                {
                    "numero": "3306307200", "fecha_expedicion": "2020-10-22",
                    "fecha_inicio_vigencia": "2020-10-23", "fecha_fin_vigencia": "2021-10-22",
                    "aseguradora": "AXA", "codigo_tarifa": "112", "estado": "NO VIGENTE",
                }
            ],
        }
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_vehiculo_runt_sync", return_value=resultado):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("runt", "1010213062", actor_consultador(), False, placa="MVX48E")
                    )
        self.assertEqual(seccion["estado"], "ADVERTENCIA")
        # Y contamina el estado global: con las demás EXITO → CON_ADVERTENCIAS.
        fuentes = {
            "manifiestos_rndc": {"estado": "EXITO"},
            "procuraduria": {"estado": "EXITO"},
            "policia": {"estado": "DESHABILITADA"},
            "runt": {"estado": "ADVERTENCIA"},
        }
        self.assertEqual(orch.calcular_estado_global(fuentes), "COMPLETADA_CON_ADVERTENCIAS")

    def test_soat_vencido_en_cache_degrada_en_el_hit(self):
        # Una caché de ayer con SOAT vigente ENTONCES puede estar vencida HOY:
        # el estado se recalcula en cada hit con la fecha de vencimiento.
        cache = {
            "_id": ObjectId(),
            "tipo": "runt",
            "cedula": "1010213062",
            "placa": "MVX48E",
            "no_registra": None,
            "mensaje": "",
            "datos_vehiculo": self.RESULTADO_OK["datos_vehiculo"],
            "soat": {
                "numero": "3306307200", "aseguradora": "AXA",
                "fecha_inicio_vigencia": "2020-10-23", "fecha_fin_vigencia": "2021-10-22",
                "estado_portal": "NO VIGENTE", "vigente": True,  # vencido hoy
            },
            "polizas": self.RESULTADO_OK["polizas"],
        }
        with patch.object(orch, "_buscar_cache", return_value=cache):
            seccion = self._correr(
                orch._ejecutar_fuente("runt", "1010213062", actor_consultador(), False, placa="MVX48E")
            )
        self.assertEqual(seccion["estado"], "ADVERTENCIA")

    def test_no_registra_se_cachea(self):
        # "Placa sin información" / "no propietario activo" son respuestas
        # DETERMINANTES del portal: se cachean (no son vacíos sospechosos).
        resultado = {
            "placa": "EYX243", "cedula": "15887928",
            "no_registra": False,
            "mensaje": "La cédula no corresponde a un propietario activo del vehículo",
            "datos_vehiculo": {}, "soat": None, "polizas": [], "pdf_bytes": None,
        }
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_vehiculo_runt_sync", return_value=resultado):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("runt", "15887928", actor_consultador(), False, placa="EYX243")
                    )
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertEqual(seccion["no_registra"], False)
        col.insert_one.assert_called_once()

    def test_resultado_vacio_es_no_disponible_sin_cachear(self):
        resultado = {
            "placa": "AAA123", "cedula": "1010213062",
            "no_registra": None, "mensaje": "",
            "datos_vehiculo": {}, "soat": None, "polizas": [], "pdf_bytes": None,
        }
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_vehiculo_runt_sync", return_value=resultado):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("runt", "1010213062", actor_consultador(), False, placa="AAA123")
                    )
        self.assertEqual(seccion["estado"], "NO_DISPONIBLE")
        self.assertEqual(seccion["error"]["tipo"], "portal_inconsistente")
        col.insert_one.assert_not_called()

    def test_bot_sin_resultado_es_no_disponible(self):
        from Funciones.bot_runt import BotRuntSinResultado

        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_vehiculo_runt_sync") as bot:
                bot.side_effect = BotRuntSinResultado("sin datos")
                with patch.object(orch, "BACKOFF_MS", 0):
                    seccion = self._correr(
                        orch._ejecutar_fuente("runt", "1010213062", actor_consultador(), False, placa="MVX48E")
                    )
        self.assertEqual(seccion["estado"], "NO_DISPONIBLE")
        self.assertEqual(seccion["error"]["tipo"], "portal_inconsistente")

    def test_sin_captcha_key_es_no_disponible_y_no_error(self):
        from Funciones.bot_runt import BotRuntSinCaptchaKey

        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_vehiculo_runt_sync") as bot:
                bot.side_effect = BotRuntSinCaptchaKey("falta key")
                with patch.object(orch, "BACKOFF_MS", 0):
                    seccion = self._correr(
                        orch._ejecutar_fuente("runt", "1010213062", actor_consultador(), False, placa="MVX48E")
                    )
        self.assertEqual(seccion["estado"], "NO_DISPONIBLE")
        self.assertEqual(seccion["error"]["tipo"], "configuracion_faltante")

    def test_captcha_fallido_es_error_de_tipo_captcha(self):
        from Funciones.bot_runt import BotRuntCaptchaFallido

        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_vehiculo_runt_sync") as bot:
                bot.side_effect = BotRuntCaptchaFallido("rechazado")
                with patch.object(orch, "BACKOFF_MS", 0):
                    seccion = self._correr(
                        orch._ejecutar_fuente("runt", "1010213062", actor_consultador(), False, placa="MVX48E")
                    )
        self.assertEqual(seccion["estado"], "ERROR")
        self.assertEqual(seccion["error"]["tipo"], "captcha")


class TestFuenteRuntPropietario(unittest.TestCase):
    """2026-08-30: el RUNT consulta con la cédula del PROPIETARIO ACTIVO de la
    placa, que puede ser DISTINTA de la persona evaluada (conductor). La caché,
    el bot y el doc de caché van con la cédula del propietario; el estudio
    persiste vehiculos[] con la relación propietario/evaluado."""

    RESULTADO_OK = TestFuenteRunt.RESULTADO_OK

    def _correr(self, corutina):
        return asyncio.run(corutina)

    def test_runt_usa_cedula_propietario_en_cache_y_bot(self):
        """Con cedula_propietario, la caché se busca y el bot se invoca con la
        cédula del DUEÑO, no con la del conductor evaluado."""
        with patch.object(orch, "_buscar_cache", return_value=None) as buscar:
            with patch.object(orch, "consultar_vehiculo_runt_sync") as bot:
                bot.return_value = self.RESULTADO_OK
                seccion = self._correr(
                    orch._ejecutar_fuente(
                        "runt", "1033688842", actor_consultador(), False,
                        placa="MVX48E", cedula_propietario="1010213062",
                    )
                )
        buscar.assert_called_once_with("runt", "1010213062", False, placa="MVX48E")
        bot.assert_called_once_with("MVX48E", "1010213062")
        self.assertEqual(seccion["estado"], "EXITO")

    def test_sin_cedula_propietario_usa_la_del_evaluado(self):
        """Sin cedula_propietario se mantiene el comportamiento previo: la
        consulta de runt va con la cédula de la persona evaluada."""
        with patch.object(orch, "_buscar_cache", return_value=None) as buscar:
            with patch.object(orch, "consultar_vehiculo_runt_sync") as bot:
                bot.return_value = self.RESULTADO_OK
                self._correr(
                    orch._ejecutar_fuente(
                        "runt", "1033688842", actor_consultador(), False, placa="MVX48E",
                    )
                )
        buscar.assert_called_once_with("runt", "1033688842", False, placa="MVX48E")
        bot.assert_called_once_with("MVX48E", "1033688842")

    def test_documento_persiste_vehiculos_propietario_distinto(self):
        actor = actor_consultador()
        insertados = {}

        class ColFake:
            def insert_one(self, doc):
                doc["_id"] = ObjectId()
                insertados.update(doc)

        with patch.object(orch, "col_estudios", ColFake()):
            orch.crear_documento_estudio(
                consulta_id="ES-PROP1", cedula="1033688842", actor=actor,
                empresa={"nombre": "X", "config": {}}, forzar=False, auditoria={},
                placa="MVX48E", cedula_propietario="1010213062",
            )
        self.assertEqual(insertados["vehiculos"], [{
            "placa": "MVX48E",
            "cedula_propietario": "1010213062",
            "propietario_es_evaluado": False,
        }])

    def test_documento_persiste_vehiculos_propietario_evaluado(self):
        """Con la misma cédula del evaluado (runt ya resuelto por el endpoint),
        la relación queda True. cedula_propietario=None + placa = solo simit
        (ese caso lo cubre TestFuenteSimitSoloPlaca)."""
        actor = actor_consultador()
        ced_prop = "1033688842"
        insertados = {}

        class ColFake:
            def insert_one(self, doc):
                doc["_id"] = ObjectId()
                insertados.update(doc)

        with patch.object(orch, "col_estudios", ColFake()):
            orch.crear_documento_estudio(
                consulta_id="ES-PROP2", cedula="1033688842", actor=actor,
                empresa={"nombre": "X", "config": {}}, forzar=False, auditoria={},
                placa="MVX48E", cedula_propietario=ced_prop,
            )
        self.assertEqual(insertados["vehiculos"], [{
            "placa": "MVX48E",
            "cedula_propietario": "1033688842",
            "propietario_es_evaluado": True,
        }])

    def test_documento_sin_runt_no_persiste_vehiculos(self):
        actor = actor_consultador()
        insertados = {}

        class ColFake:
            def insert_one(self, doc):
                doc["_id"] = ObjectId()
                insertados.update(doc)

        with patch.object(orch, "col_estudios", ColFake()):
            orch.crear_documento_estudio(
                consulta_id="ES-PROP3", cedula="1033688842", actor=actor,
                empresa={"nombre": "X", "config": {}}, forzar=False, auditoria={},
            )
        self.assertEqual(insertados["vehiculos"], [])
        self.assertIsNone(insertados["placa"])


class TestCacheRuntConPlaca(unittest.TestCase):
    """La caché de runt discrimina por placa: sin placa en la llamada NUNCA hay
    hit (evita cross-contaminación entre placas de la misma cédula)."""

    def test_runt_sin_placa_nunca_hace_hit(self):
        from rutas import seguridad as rseg

        with patch.object(rseg, "col_consultas") as col:
            col.find_one.return_value = {"_id": ObjectId(), "tipo": "runt"}
            doc = rseg._buscar_cache("runt", "1010213062", False, placa=None)
        self.assertIsNone(doc)
        col.find_one.assert_not_called()  # el guard corta antes de ir a Mongo

    def test_runt_con_placa_filtra_por_placa(self):
        from rutas import seguridad as rseg

        with patch.object(rseg, "col_consultas") as col:
            col.find_one.return_value = None
            rseg._buscar_cache("runt", "1010213062", False, placa="MVX48E")
        filtro = col.find_one.call_args[0][0]
        self.assertEqual(filtro["tipo"], "runt")
        self.assertEqual(filtro["cedula"], "1010213062")
        self.assertEqual(filtro["placa"], "MVX48E")

    def test_normalizar_placa(self):
        from fastapi import HTTPException

        from rutas import seguridad as rseg

        self.assertEqual(rseg._normalizar_placa("mvx 48e"), "MVX48E")
        self.assertEqual(rseg._normalizar_placa("AAA-123"), "AAA123")
        self.assertEqual(rseg._normalizar_placa("AB1234"), "AB1234")
        with self.assertRaises(HTTPException):
            rseg._normalizar_placa("123")
        with self.assertRaises(HTTPException):
            rseg._normalizar_placa("AAAAAA")


class TestFuenteSimit(unittest.TestCase):
    """Fuente "simit" (estado de cuenta de comparendos por PLACA, sin cédula):
    caché con clave (tipo, placa, cedula=None), semáforo por saldo EXIGIBLE
    (total_a_pagar > 0 = ADVERTENCIA) y anti-envenenamiento análogo."""

    RESULTADO_LIMPIO = {
        "placa": "MVX48E",
        "no_registra": None,
        "mensaje": "No tienes comparendos ni multas registradas en Simit",
        "total_comparendos": 0, "total_multas": 0, "total_acuerdos": 0,
        "total_deuda": 0.0, "total_a_pagar": 0.0,
        "comparendos": [],
        "pdf_bytes": None,
    }

    RESULTADO_DEUDA = {
        "placa": "ZZZ999",
        "no_registra": None,
        "mensaje": "",
        "total_comparendos": 88, "total_multas": 17, "total_acuerdos": 0,
        "total_deuda": 40257438.0, "total_a_pagar": 0.0,
        "comparendos": [{
            "numero": "130289A", "tipo": "Comparendo", "fecha_imposicion": "2000-04-11",
            "notificacion": "No aplica", "placa": "ZZZ999", "secretaria": "Villavicencio",
            "infraccion": "No respetar las señales de tránsito", "estado": "Pendiente",
            "estado_nota": "No tiene curso", "valor": 260130.0, "valor_a_pagar": 260130.0,
        }],
        "pdf_bytes": None,
    }

    def _correr(self, corutina):
        return asyncio.run(corutina)

    def test_exito_limpio_cachea_sin_cedula(self):
        with patch.object(orch, "_buscar_cache", return_value=None) as buscar:
            with patch.object(orch, "consultar_comparendos_simit_sync", return_value=self.RESULTADO_LIMPIO):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("simit", "1033688842", actor_consultador(), False, placa="MVX48E")
                    )
        # La caché va SIN cédula: la identidad del dato es la placa.
        buscar.assert_called_once_with("simit", None, False, placa="MVX48E")
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertEqual(seccion["origen"], "portal")
        self.assertEqual(seccion["total_comparendos"], 0)
        doc_cache = col.insert_one.call_args[0][0]
        self.assertEqual(doc_cache["tipo"], "simit")
        self.assertIsNone(doc_cache["cedula"])
        self.assertEqual(doc_cache["placa"], "MVX48E")

    def test_saldo_exigible_es_advertencia(self):
        resultado = {**self.RESULTADO_DEUDA, "total_a_pagar": 260130.0}
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_comparendos_simit_sync", return_value=resultado):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("simit", "1033688842", actor_consultador(), False, placa="ZZZ999")
                    )
        self.assertEqual(seccion["estado"], "ADVERTENCIA")
        # Y contamina el estado global: con las demás EXITO → CON_ADVERTENCIAS.
        fuentes = {
            "manifiestos_rndc": {"estado": "DESHABILITADA"},
            "procuraduria": {"estado": "EXITO"},
            "policia": {"estado": "DESHABILITADA"},
            "runt": {"estado": "DESHABILITADA"},
            "simit": {"estado": "ADVERTENCIA"},
        }
        self.assertEqual(orch.calcular_estado_global(fuentes), "COMPLETADA_CON_ADVERTENCIAS")

    def test_deuda_historica_sin_saldo_es_exito(self):
        # ZZZ999 real: 105 pendientes de 1999-2000 con $0 EXIGIBLE → EXITO
        # (el detalle queda en la sección/PDF, pero no es deuda vigente).
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_comparendos_simit_sync", return_value=self.RESULTADO_DEUDA):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("simit", "1033688842", actor_consultador(), False, placa="ZZZ999")
                    )
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertEqual(seccion["total_comparendos"], 88)
        self.assertEqual(len(seccion["comparendos"]), 1)

    def test_cache_hit_reconstruye_seccion(self):
        cache = {
            "_id": ObjectId(),
            "tipo": "simit", "cedula": None, "placa": "MVX48E",
            "no_registra": None,
            "mensaje": "No tienes comparendos ni multas registradas en Simit",
            "total_comparendos": 0, "total_multas": 0, "total_acuerdos": 0,
            "total_deuda": 0.0, "total_a_pagar": 0.0, "comparendos": [],
        }
        with patch.object(orch, "_buscar_cache", return_value=cache) as buscar:
            with patch.object(orch, "consultar_comparendos_simit_sync") as bot:
                seccion = self._correr(
                    orch._ejecutar_fuente("simit", "1033688842", actor_consultador(), False, placa="MVX48E")
                )
        buscar.assert_called_once_with("simit", None, False, placa="MVX48E")
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertEqual(seccion["origen"], "cache")
        self.assertEqual(seccion["placa"], "MVX48E")
        bot.assert_not_called()

    def test_resultado_vacio_es_no_disponible_sin_cachear(self):
        resultado = {
            "placa": "AAA123", "no_registra": None, "mensaje": "",
            "total_comparendos": None, "total_multas": None, "total_acuerdos": None,
            "total_deuda": None, "total_a_pagar": None, "comparendos": [],
            "pdf_bytes": None,
        }
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_comparendos_simit_sync", return_value=resultado):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("simit", "1033688842", actor_consultador(), False, placa="AAA123")
                    )
        self.assertEqual(seccion["estado"], "NO_DISPONIBLE")
        self.assertEqual(seccion["error"]["tipo"], "portal_inconsistente")
        col.insert_one.assert_not_called()

    def test_bot_sin_resultado_es_no_disponible(self):
        from Funciones.bot_simit import BotSimitSinResultado

        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_comparendos_simit_sync") as bot:
                bot.side_effect = BotSimitSinResultado("sin datos")
                with patch.object(orch, "BACKOFF_MS", 0):
                    seccion = self._correr(
                        orch._ejecutar_fuente("simit", "1033688842", actor_consultador(), False, placa="MVX48E")
                    )
        self.assertEqual(seccion["estado"], "NO_DISPONIBLE")
        self.assertEqual(seccion["error"]["tipo"], "portal_inconsistente")


class TestFuenteRues(unittest.TestCase):
    """Fuente "rues" (Registro Mercantil de Confecámaras por NIT sin DV):
    caché con clave (tipo, NIT) — viaja en el campo cédula como ofac_nit —,
    semáforo por estado de matrícula (distinto de ACTIVA → ADVERTENCIA,
    decisión de negocio 2026-09-03 análoga a SOAT vencido / saldo SIMIT) y
    anti-envenenamiento análogo. NIT sin registro = determinante (no_registra),
    jamás un "limpio" de la empresa."""

    RESULTADO_ACTIVA = {
        "nit": "901923029", "nit_con_dv": "901923029-2",
        "razon_social": "GLAMPEROS S.A.S.", "estado": "ACTIVA", "no_registra": False,
        "mensaje": "Matrícula ACTIVA — GLAMPEROS S.A.S. (cámara ABURRA SUR).",
        "camara": "ABURRA SUR", "codigo_camara": "55", "matricula": "281773",
        "fecha_matricula": "2025-03-03", "fecha_renovacion": "2026-03-09",
        "ultimo_ano_renovado": "2026", "fecha_cancelacion": None,
        "tipo_sociedad": "SOCIEDAD COMERCIAL",
        "organizacion_juridica": "SOCIEDADES POR ACCIONES SIMPLIFICADAS SAS",
        "categoria_matricula": "SOCIEDAD – PERSONA JURIDICA PRINCIPAL – ESAL",
        "ciiu": {"principal": {"codigo": "6312", "descripcion": "Portales web"}},
        "municipio": "ITAGUI", "departamento": "ANTIOQUIA",
        "representantes": [{"documento": "1010213062", "nombre": "ZARATE PEÑA EDWIN MISAEL"}],
        "fecha_actualizacion": "2026-03-09",
    }

    def _correr(self, corutina):
        return asyncio.run(corutina)

    def test_exito_activa_cachea_por_nit(self):
        with patch.object(orch, "_buscar_cache", return_value=None) as buscar:
            with patch.object(orch, "consultar_rues_sync", return_value=self.RESULTADO_ACTIVA):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("rues", "", actor_consultador(), False, nit="901923029")
                    )
        # La identidad de la caché es el NIT (viaja en el campo cédula, como ofac_nit).
        buscar.assert_called_once_with("rues", "901923029", False, placa=None)
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertEqual(seccion["origen"], "portal")
        self.assertEqual(seccion["estado_matricula"], "ACTIVA")
        self.assertEqual(seccion["razon_social"], "GLAMPEROS S.A.S.")
        doc_cache = col.insert_one.call_args[0][0]
        self.assertEqual(doc_cache["tipo"], "rues")
        self.assertEqual(doc_cache["cedula"], "901923029")
        self.assertEqual(doc_cache["nit"], "901923029")
        self.assertEqual(doc_cache["representantes"][0]["documento"], "1010213062")

    def test_matricula_distinta_de_activa_es_advertencia(self):
        # Caso real visto en la sonda: cancelada por Ley 1429 de 2010.
        resultado = {**self.RESULTADO_ACTIVA, "estado": "MATRICULACANCELADALEY1429"}
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_rues_sync", return_value=resultado):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("rues", "", actor_consultador(), False, nit="901923029")
                    )
        self.assertEqual(seccion["estado"], "ADVERTENCIA")
        # Y contamina el estado global: con las demás EXITO → CON_ADVERTENCIAS.
        fuentes = {
            "manifiestos_rndc": {"estado": "DESHABILITADA"},
            "procuraduria": {"estado": "EXITO"},
            "rues": {"estado": "ADVERTENCIA"},
        }
        self.assertEqual(orch.calcular_estado_global(fuentes), "COMPLETADA_CON_ADVERTENCIAS")

    def test_nit_sin_registro_es_exito_determinante(self):
        resultado = {"nit": "999999997", "no_registra": True, "estado": None,
                     "mensaje": "NIT sin registro en el Registro Mercantil del RUES."}
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_rues_sync", return_value=resultado):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("rues", "", actor_consultador(), False, nit="999999997")
                    )
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertTrue(seccion["no_registra"])
        col.insert_one.assert_called_once()

    def test_cache_hit_reconstruye_seccion(self):
        cache = {
            "_id": ObjectId(),
            "tipo": "rues", "cedula": "901923029", "nit": "901923029",
            "nit_con_dv": "901923029-2", "razon_social": "GLAMPEROS S.A.S.",
            "estado_matricula": "ACTIVA", "no_registra": False,
            "mensaje": "Matrícula ACTIVA", "camara": "ABURRA SUR", "matricula": "281773",
            "representantes": [], "ciiu": {},
        }
        with patch.object(orch, "_buscar_cache", return_value=cache) as buscar:
            with patch.object(orch, "consultar_rues_sync") as bot:
                seccion = self._correr(
                    orch._ejecutar_fuente("rues", "", actor_consultador(), False, nit="901923029")
                )
        buscar.assert_called_once_with("rues", "901923029", False, placa=None)
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertEqual(seccion["origen"], "cache")
        self.assertEqual(seccion["razon_social"], "GLAMPEROS S.A.S.")
        bot.assert_not_called()

    def test_resultado_indeterminado_es_no_disponible_sin_cachear(self):
        # Sin estado de matrícula y sin no_registra determinante: el API no
        # entregó un resultado usable (anti-envenenamiento, doble barrera).
        resultado = {"nit": "901923029", "no_registra": None, "estado": None, "mensaje": ""}
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_rues_sync", return_value=resultado):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("rues", "", actor_consultador(), False, nit="901923029")
                    )
        self.assertEqual(seccion["estado"], "NO_DISPONIBLE")
        self.assertEqual(seccion["error"]["tipo"], "portal_inconsistente")
        col.insert_one.assert_not_called()

    def test_bot_sin_resultado_es_no_disponible(self):
        from Funciones.bot_rues import BotRuesSinResultado

        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_rues_sync") as bot:
                bot.side_effect = BotRuesSinResultado("sin datos")
                with patch.object(orch, "BACKOFF_MS", 0):
                    seccion = self._correr(
                        orch._ejecutar_fuente("rues", "", actor_consultador(), False, nit="901923029")
                    )
        self.assertEqual(seccion["estado"], "NO_DISPONIBLE")
        self.assertEqual(seccion["error"]["tipo"], "portal_inconsistente")


class TestFuenteOnuUe(unittest.TestCase):
    """Fuente "onu_ue" (listas ONU+UE agregadas, molde ofac): coincidencia
    exacta por cédula → ADVERTENCIA + revisión humana; metadatos POR LISTA y
    degradación honesta cuando una lista no pudo descargarse."""

    RESULTADO = {
        "cedula": "123456789", "aplica": True, "no_registra": False,
        "total_coincidencias": 1,
        "coincidencias": [{
            "lista": "ONU", "uid": "6907993", "nombre": "ERIC BADEGE",
            "tipo": "Individual", "programas": ["DRC"], "referencia": "CDi.001",
            "tipo_documento": "national identification number",
            "numero_documento": "123.456.789", "pais_documento": "Democratic Republic of the Congo",
        }],
        "listas": {
            "ONU": {"fecha_publicacion": "2026-09-12", "total_registros_lista": 736, "sha256_dataset": "a" * 64},
            "UE": {"fecha_publicacion": "2026-08-05", "total_registros_lista": 4462, "sha256_dataset": "b" * 64},
        },
        "listas_no_disponibles": [],
        "metodo": "coincidencia_exacta_identificacion",
        "mensaje": "Coincidencia exacta de identificación en listas de sanciones (1 registro(s): ONU, UE).",
    }

    def _correr(self, corutina):
        return asyncio.run(corutina)

    def test_coincidencia_cachea_por_cedula_y_es_advertencia(self):
        with patch.object(orch, "_buscar_cache", return_value=None) as buscar:
            with patch.object(orch, "consultar_sanciones_sync", return_value=dict(self.RESULTADO)):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("onu_ue", "123456789", actor_consultador(), False)
                    )
        buscar.assert_called_once_with("onu_ue", "123456789", False, placa=None)
        self.assertEqual(seccion["estado"], "ADVERTENCIA")
        self.assertEqual(seccion["origen"], "portal")
        self.assertEqual(seccion["coincidencias"][0]["lista"], "ONU")
        self.assertEqual(seccion["listas"]["UE"]["total_registros_lista"], 4462)
        doc_cache = col.insert_one.call_args[0][0]
        self.assertEqual(doc_cache["tipo"], "onu_ue")
        self.assertEqual(doc_cache["cedula"], "123456789")
        self.assertEqual(doc_cache["total_coincidencias"], 1)

    def test_sin_coincidencia_es_exito(self):
        resultado = {**self.RESULTADO, "aplica": False, "no_registra": True,
                     "total_coincidencias": 0, "coincidencias": []}
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_sanciones_sync", return_value=resultado):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("onu_ue", "123456789", actor_consultador(), False)
                    )
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertTrue(seccion["no_registra"])

    def test_cache_hit_reconstruye_seccion(self):
        cache = {
            "_id": ObjectId(), "tipo": "onu_ue", "cedula": "123456789",
            "aplica": False, "no_registra": True, "total_coincidencias": 0,
            "coincidencias": [], "mensaje": "Sin coincidencias",
            "listas": {"ONU": {"fecha_publicacion": "2026-09-12", "total_registros_lista": 736}},
            "listas_no_disponibles": ["UE"], "metodo": "coincidencia_exacta_identificacion",
        }
        with patch.object(orch, "_buscar_cache", return_value=cache):
            with patch.object(orch, "consultar_sanciones_sync") as bot:
                seccion = self._correr(
                    orch._ejecutar_fuente("onu_ue", "123456789", actor_consultador(), False)
                )
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertEqual(seccion["origen"], "cache")
        self.assertEqual(seccion["listas_no_disponibles"], ["UE"])
        bot.assert_not_called()

    def test_sin_metadatos_de_lista_es_no_disponible_sin_cachear(self):
        # Anti-envenenamiento análogo al de OFAC: sin metadatos de NINGUNA
        # lista no hubo descarga válida que respalde el veredicto.
        resultado = {**self.RESULTADO, "listas": {}, "listas_no_disponibles": ["ONU", "UE"]}
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_sanciones_sync", return_value=resultado):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("onu_ue", "123456789", actor_consultador(), False)
                    )
        self.assertEqual(seccion["estado"], "NO_DISPONIBLE")
        self.assertEqual(seccion["error"]["tipo"], "dataset_incompleto")
        col.insert_one.assert_not_called()

    def test_bot_caido_es_no_disponible(self):
        from Funciones.bot_sanciones import BotSancionesError

        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_sanciones_sync") as bot:
                bot.side_effect = BotSancionesError("internet caído")
                with patch.object(orch, "BACKOFF_MS", 0):
                    seccion = self._correr(
                        orch._ejecutar_fuente("onu_ue", "123456789", actor_consultador(), False)
                    )
        self.assertEqual(seccion["estado"], "NO_DISPONIBLE")
        self.assertEqual(seccion["error"]["tipo"], "sanciones_no_disponible")


class TestRuntRtm(unittest.TestCase):
    """RTM del vehículo (2026-09-14): regex calibrado con el texto plano real
    del portal (MVX48E 2026-09-14) y semáforo RTM vencida → ADVERTENCIA
    (decisión análoga al SOAT vencido, recálculo en cada hit de caché)."""

    # Fragmento real del dump descargas_runt/resultado_ultimo.html (tablas
    # SOAT recortadas; la primera fila RTM es la vigente).
    TEXTO_RTM = (
        "Certificado de revisión técnico mecánica y de emisiones contaminantes (RTM) "
        "Tipo Revisión Fecha Expedición Fecha Vigencia CDA expide RTM Vigente "
        "Nro. certificado Información consistente Acciones "
        "REVISION TECNICO-MECANICO 04/10/2025 04/10/2026 "
        "CENTRO DE DIAGNOSTICO AUTOMOTOR LA AGUACATALA SI 184404264 SI download "
        "REVISION TECNICO-MECANICO 04/10/2024 04/10/2025 "
        "CENTRO DE DIAGNOSTICO AUTOMOTOR LA AGUACATALA NO 176330660 SI "
        "Registros por página 10 1 - 5 de 5 eco"
    )

    def test_regex_parsea_filas_calibradas(self):
        from Funciones import bot_runt

        filas = list(bot_runt._RE_RTM.finditer(self.TEXTO_RTM))
        self.assertEqual(len(filas), 2)
        primera = filas[0]
        self.assertEqual(primera.group(3), "04/10/2026")           # vigencia
        self.assertTrue(primera.group(5).upper() == "SI")          # vigente portal
        self.assertEqual(primera.group(6), "184404264")            # certificado
        self.assertIn("AGUACATALA", primera.group(4))              # CDA

    def test_rtm_vencida_es_advertencia(self):
        self.assertEqual(
            orch._estado_runt({"soat": {"vigente": True}, "rtm": {"vigente": False}}),
            "ADVERTENCIA",
        )
        # Sin RTM reportada no se inventa la advertencia.
        self.assertEqual(orch._estado_runt({"soat": {"vigente": True}, "rtm": None}), "EXITO")
        self.assertEqual(
            orch._estado_runt({"soat": {"vigente": True}, "rtm": {"vigente": True}}),
            "EXITO",
        )
        # El SOAT vencido sigue mandando.
        self.assertEqual(
            orch._estado_runt({"soat": {"vigente": False}, "rtm": {"vigente": False}}),
            "ADVERTENCIA",
        )

    def test_cache_hit_recalcula_vigencia_rtm(self):
        from datetime import date

        from Funciones.bot_runt import _soat_vigente

        # La caché guarda la revisión con fecha_vigencia ya vencida: el hit
        # debe degradar el semáforo aunque se cacheó como vigente.
        vencida_ayer = (date.today() - __import__("datetime").timedelta(days=1)).isoformat()
        cache = {
            "_id": ObjectId(), "tipo": "runt", "cedula": "1010213062", "placa": "MVX48E",
            "no_registra": None, "mensaje": "", "datos_vehiculo": {"marca": "HONDA"},
            "soat": {"numero": "3453028900", "aseguradora": "AXA",
                     "fecha_inicio_vigencia": "2025-10-23",
                     "fecha_fin_vigencia": "2099-10-22", "estado_portal": "VIGENTE"},
            "polizas": [], "rtm": {"numero_certificado": "184404264", "cda": "CDA",
                                   "fecha_expedicion": "2025-10-04",
                                   "fecha_vigencia": vencida_ayer,
                                   "vigente_portal": True, "vigente": True},
            "revisiones": [],
        }
        with patch.object(orch, "_buscar_cache", return_value=cache):
            with patch.object(orch, "consultar_vehiculo_runt_sync") as bot:
                seccion = asyncio.run(
                    orch._ejecutar_fuente("runt", "1010213062", actor_consultador(), False, placa="MVX48E")
                )
        self.assertEqual(seccion["origen"], "cache")
        self.assertFalse(seccion["rtm"]["vigente"])  # recalculada contra hoy
        self.assertEqual(seccion["estado"], "ADVERTENCIA")
        bot.assert_not_called()


class TestFuenteDelitos(unittest.TestCase):
    """Fuente "delitos_sexuales" (inhabilidades Ley 1918, DIJIN): caché por
    (tipo, cédula), semáforo registra → ADVERTENCIA, y anti-envenenamiento
    análogo contraloría (sin veredicto NO se cachea)."""

    RESULTADO = {
        "cedula": "1010213062", "no_registra": True,
        "mensaje": "No registra inhabilidad por delitos sexuales contra menores (Ley 1918 de 2018)",
        "fecha_consulta": "14/09/2026 19:15:28",
        "empresa_consultante": "GLAMPEROS S.A.S.",
        "fecha_expedicion": "14/02/2012",
    }

    def _correr(self, corutina):
        return asyncio.run(corutina)

    def test_exito_cachea_por_cedula(self):
        with patch.object(orch, "_buscar_cache", return_value=None) as buscar:
            with patch.object(orch, "consultar_inhabilidades_sync", return_value=dict(self.RESULTADO)):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente(
                            "delitos_sexuales", "1010213062", actor_consultador(), False,
                            fecha_expedicion="14/02/2012",
                            empresa_consultante={"nombre": "GLAMPEROS S.A.S.", "nit": "901923029-2"},
                        )
                    )
        buscar.assert_called_once_with("delitos_sexuales", "1010213062", False, placa=None)
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertEqual(seccion["origen"], "portal")
        self.assertTrue(seccion["no_registra"])
        self.assertEqual(seccion["empresa_consultante"], "GLAMPEROS S.A.S.")
        doc_cache = col.insert_one.call_args[0][0]
        self.assertEqual(doc_cache["tipo"], "delitos_sexuales")
        self.assertEqual(doc_cache["fecha_consulta"], "14/09/2026 19:15:28")

    def test_registra_inhabilidad_es_advertencia(self):
        resultado = {**self.RESULTADO, "no_registra": False,
                     "mensaje": "…REGISTRA INHABILIDAD…"}
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_inhabilidades_sync", return_value=resultado):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("delitos_sexuales", "1010213062", actor_consultador(), False)
                    )
        self.assertEqual(seccion["estado"], "ADVERTENCIA")

    def test_cache_hit_reconstruye_seccion(self):
        cache = {
            "_id": ObjectId(), "tipo": "delitos_sexuales", "cedula": "1010213062",
            "no_registra": True, "mensaje": "No registra inhabilidad",
            "fecha_consulta": "14/09/2026 19:15:28",
            "empresa_consultante": "GLAMPEROS S.A.S.",
        }
        with patch.object(orch, "_buscar_cache", return_value=cache):
            with patch.object(orch, "consultar_inhabilidades_sync") as bot_fn:
                seccion = self._correr(
                    orch._ejecutar_fuente("delitos_sexuales", "1010213062", actor_consultador(), False)
                )
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertEqual(seccion["origen"], "cache")
        bot_fn.assert_not_called()

    def test_sin_veredicto_es_no_disponible_sin_cachear(self):
        # Fecha de expedición equivocada → el portal responde sin fórmula:
        # NO se cachea (anti-envenenamiento).
        resultado = {**self.RESULTADO, "no_registra": None, "mensaje": "",
                     "texto_resultado": "página de error del portal"}
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_inhabilidades_sync", return_value=resultado):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("delitos_sexuales", "1010213062", actor_consultador(), False)
                    )
        self.assertEqual(seccion["estado"], "NO_DISPONIBLE")
        self.assertEqual(seccion["error"]["tipo"], "portal_inconsistente")
        col.insert_one.assert_not_called()

    def test_bot_sin_resultado_es_no_disponible(self):
        from Funciones.bot_delitos_sexuales import BotDelitosSinResultado

        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_inhabilidades_sync") as bot_fn:
                bot_fn.side_effect = BotDelitosSinResultado("sin veredicto")
                with patch.object(orch, "BACKOFF_MS", 0):
                    seccion = self._correr(
                        orch._ejecutar_fuente("delitos_sexuales", "1010213062", actor_consultador(), False)
                    )
        self.assertEqual(seccion["estado"], "NO_DISPONIBLE")
        self.assertEqual(seccion["error"]["tipo"], "portal_inconsistente")


class TestFuentesHabilitadasEfectivas(unittest.TestCase):
    """2026-09-01 (pedido del usuario): EL PLAN ES EL GATE — editar
    `fuentes_incluidas` de un plan (o agregar una fuente al catálogo) queda
    disponible para TODA empresa con ese plan INMEDIATAMENTE, sin migrar
    configs. `config.fuentes_habilitadas` deja de ser un whitelist: solo
    policia (opt-in) exige presencia explícita y `fuentes_excluidas` apaga
    por empresa."""

    def test_config_viejo_sin_sena_la_deja_correr(self):
        # El caso real que lo motivó: empresa con config persistido pre-sena
        # (GLAMPEROS) + plan BASICO editado con sena → antes DESHABILITADA
        # por la intersección con el whitelist; ahora corre sin script.
        empresa = {"config": {"fuentes_habilitadas": [
            "manifiestos_rndc", "procuraduria", "policia", "runt", "simit",
        ]}}
        efectivas = orch.fuentes_habilitadas_efectivas(empresa)
        self.assertIn("sena", efectivas)
        self.assertIn("policia", efectivas)  # estaba listada explícitamente

    def test_sin_config_todas_las_default(self):
        esperadas = ["manifiestos_rndc", "procuraduria", "contraloria", "delitos_sexuales", "runt", "simit", "sena", "sisconmp", "ofac", "ofac_nit", "onu_ue", "bdme", "bdme_nit", "rama_judicial", "rues", "situacion_militar"]
        self.assertEqual(orch.fuentes_habilitadas_efectivas({}), esperadas)
        self.assertEqual(orch.fuentes_habilitadas_efectivas(None), esperadas)
        self.assertEqual(
            orch.fuentes_habilitadas_efectivas({"config": {"fuentes_habilitadas": None}}),
            esperadas,
        )

    def test_policia_sigue_siendo_opt_in(self):
        self.assertNotIn("policia", orch.fuentes_habilitadas_efectivas({"config": {}}))
        self.assertIn(
            "policia",
            orch.fuentes_habilitadas_efectivas({"config": {"fuentes_habilitadas": ["policia"]}}),
        )

    def test_exclusion_por_empresa(self):
        empresa = {"config": {"fuentes_excluidas": ["simit"]}}
        efectivas = orch.fuentes_habilitadas_efectivas(empresa)
        self.assertNotIn("simit", efectivas)
        self.assertIn("sena", efectivas)

    def test_exclusion_pisa_al_whitelist(self):
        empresa = {"config": {"fuentes_habilitadas": ["procuraduria"], "fuentes_excluidas": ["procuraduria"]}}
        self.assertNotIn("procuraduria", orch.fuentes_habilitadas_efectivas(empresa))


class TestFuenteSena(unittest.TestCase):
    """Fuente "sena" (certificados de formación por CÉDULA): caché con clave
    (tipo, cédula) — el default del módulo —, SIEMPRE EXITO informativo
    (formación no es antecedente) y anti-envenenamiento análogo."""

    RESULTADO_CERTS = {
        "cedula": "1010213062",
        "no_registra": False,
        "mensaje": "",
        "total_certificados": 2,
        "certificados": [
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
        ],
        "pdf_bytes": None,
    }

    RESULTADO_VACIO = {
        "cedula": "1033688842",
        "no_registra": True,
        "mensaje": "La cédula no registra certificados disponibles en el SENA",
        "total_certificados": 0,
        "certificados": [],
        "pdf_bytes": None,
    }

    def _correr(self, corutina):
        return asyncio.run(corutina)

    def test_exito_con_certificados_cachea_por_cedula(self):
        with patch.object(orch, "_buscar_cache", return_value=None) as buscar:
            with patch.object(orch, "consultar_sena_sync", return_value=self.RESULTADO_CERTS) as bot:
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("sena", "1010213062", actor_consultador(), False)
                    )
        # Caché por cédula (default del módulo: sena no conoce placas).
        buscar.assert_called_once_with("sena", "1010213062", False, placa=None)
        bot.assert_called_once_with("1010213062")
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertEqual(seccion["origen"], "portal")
        self.assertEqual(seccion["total_certificados"], 2)
        self.assertEqual(len(seccion["certificados"]), 2)
        doc_cache = col.insert_one.call_args[0][0]
        self.assertEqual(doc_cache["tipo"], "sena")
        self.assertEqual(doc_cache["cedula"], "1010213062")
        self.assertEqual(doc_cache["no_registra"], False)

    def test_sin_certificados_tambien_es_exito(self):
        # Formación ≠ antecedente: registrar 0 certificados es determinante e
        # informativo (nunca ADVERTENCIA, nunca "limpio").
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_sena_sync", return_value=self.RESULTADO_VACIO):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("sena", "1033688842", actor_consultador(), False)
                    )
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertTrue(seccion["no_registra"])
        self.assertEqual(seccion["total_certificados"], 0)
        # El vacío legítimo SÍ se cachea (24 h) como los demás determinantes.
        self.assertEqual(col.insert_one.call_args[0][0]["no_registra"], True)

    def test_solo_sena_no_degrada_el_estado_global(self):
        fuentes = {
            "manifiestos_rndc": {"estado": "DESHABILITADA"},
            "procuraduria": {"estado": "DESHABILITADA"},
            "policia": {"estado": "DESHABILITADA"},
            "runt": {"estado": "DESHABILITADA"},
            "simit": {"estado": "DESHABILITADA"},
            "sena": {"estado": "EXITO"},
        }
        self.assertEqual(orch.calcular_estado_global(fuentes), "COMPLETADA")

    def test_cache_hit_reconstruye_seccion(self):
        cache = {
            "_id": ObjectId(),
            "tipo": "sena", "cedula": "1010213062",
            "no_registra": False,
            "mensaje": "",
            "total_certificados": 2,
            "certificados": self.RESULTADO_CERTS["certificados"],
        }
        with patch.object(orch, "_buscar_cache", return_value=cache) as buscar:
            with patch.object(orch, "consultar_sena_sync") as bot:
                seccion = self._correr(
                    orch._ejecutar_fuente("sena", "1010213062", actor_consultador(), False)
                )
        buscar.assert_called_once_with("sena", "1010213062", False, placa=None)
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertEqual(seccion["origen"], "cache")
        self.assertEqual(seccion["total_certificados"], 2)
        bot.assert_not_called()

    def test_resultado_vacio_sin_determinante_es_no_disponible(self):
        resultado = {
            "cedula": "1033688842", "no_registra": None, "mensaje": "",
            "total_certificados": None, "certificados": [], "pdf_bytes": None,
        }
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_sena_sync", return_value=resultado):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("sena", "1033688842", actor_consultador(), False)
                    )
        self.assertEqual(seccion["estado"], "NO_DISPONIBLE")
        self.assertEqual(seccion["error"]["tipo"], "portal_inconsistente")
        col.insert_one.assert_not_called()

    def test_bot_sin_resultado_es_no_disponible(self):
        from Funciones.bot_sena import BotSenaSinResultado

        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_sena_sync") as bot:
                bot.side_effect = BotSenaSinResultado("sin datos")
                with patch.object(orch, "BACKOFF_MS", 0):
                    seccion = self._correr(
                        orch._ejecutar_fuente("sena", "1033688842", actor_consultador(), False)
                    )
        self.assertEqual(seccion["estado"], "NO_DISPONIBLE")
        self.assertEqual(seccion["error"]["tipo"], "portal_inconsistente")


class TestFuenteSisconmp(unittest.TestCase):
    """Fuente "sisconmp" (capacitaciones de Mercancías Peligrosas por CÉDULA):
    caché (tipo, cédula) — el default del módulo —, semáforo de VIGENCIA
    (todas vencidas y ninguna vigente → ADVERTENCIA, análogo SOAT/RTM;
    recalculado en cada hit de caché) y anti-envenenamiento doble barrera (el
    DOM del portal miente: su handler error: muestra el mismo "no registra"
    del vacío legítimo — el bot lee la RESPUESTA AJAX, no el DOM)."""

    RESULTADO_CAPS = {
        "cedula": "79882073",
        "no_registra": False,
        "mensaje": "",
        "apellidos": "GOMEZ GOMEZ",
        "nombres": "MARIO",
        "total_capacitaciones": 2,
        "capacitaciones": [
            {
                "tipo_capacitacion": "CURSO BASICO",
                "nombre": "Curso Básico para el Transporte de Mercancías Peligrosas",
                "entidad_certificadora": "MEN",
                "institucion_educativa": "ACADEMIA X",
                "fecha_expedicion": "2020-01-10",
                "fecha_vencimiento": "2099-12-31",  # vigente (fecha lejana: el test nunca caduca)
                "fecha_registro": "2020-01-12",
                "clase": "", "descripcion_clase": "", "tipo_vehiculo": "",
                "vigente": True,
            },
            {
                "tipo_capacitacion": "TITULACION NCL",
                "nombre": "Titulación NCL TMR",
                "entidad_certificadora": "SENA",
                "institucion_educativa": "SENA",
                "fecha_expedicion": "2001-01-10",
                "fecha_vencimiento": "2001-01-10",  # vencida hace décadas
                "fecha_registro": "2001-01-12",
                "clase": "3", "descripcion_clase": "Líquidos inflamables",
                "tipo_vehiculo": "TRACTOCAMION",
                "vigente": False,
            },
        ],
        "pdf_bytes": None,
    }

    RESULTADO_VACIO = {
        "cedula": "1010213062",
        "no_registra": True,
        "mensaje": "No se encontrarón registros sobre el ciudadano.",
        "apellidos": "",
        "nombres": "",
        "total_capacitaciones": 0,
        "capacitaciones": [],
        "pdf_bytes": None,
    }

    def _correr(self, corutina):
        return asyncio.run(corutina)

    def test_exito_con_capacitacion_vigente_cachea_por_cedula(self):
        with patch.object(orch, "_buscar_cache", return_value=None) as buscar:
            with patch.object(orch, "consultar_capacitaciones_sisconmp_sync",
                              return_value=self.RESULTADO_CAPS) as bot:
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("sisconmp", "79882073", actor_consultador(), False)
                    )
        # Caché por cédula (default del módulo: sisconmp no conoce placas).
        buscar.assert_called_once_with("sisconmp", "79882073", False, placa=None)
        bot.assert_called_once_with("79882073")
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertEqual(seccion["origen"], "portal")
        self.assertEqual(seccion["total_capacitaciones"], 2)
        doc_cache = col.insert_one.call_args[0][0]
        self.assertEqual(doc_cache["tipo"], "sisconmp")
        self.assertEqual(doc_cache["cedula"], "79882073")
        self.assertEqual(doc_cache["nombres"], "MARIO")

    def test_todas_vencidas_es_advertencia(self):
        # Semáforo de vigencia (decisión 2026-09-25, análogo SOAT/RTM): registra
        # capacitaciones pero NINGUNA vigente → ADVERTENCIA.
        resultado = dict(self.RESULTADO_CAPS)
        resultado["capacitaciones"] = [
            {**self.RESULTADO_CAPS["capacitaciones"][1]}
        ]
        resultado["total_capacitaciones"] = 1
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_capacitaciones_sisconmp_sync", return_value=resultado):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("sisconmp", "79882073", actor_consultador(), False)
                    )
        self.assertEqual(seccion["estado"], "ADVERTENCIA")

    def test_sin_capacitaciones_es_exito_determinante(self):
        # El vacío es determinante del portal (registro sin capacitaciones MP):
        # EXITO informativo, no ADVERTENCIA — se cachea como los demás.
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_capacitaciones_sisconmp_sync",
                              return_value=self.RESULTADO_VACIO):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("sisconmp", "1010213062", actor_consultador(), False)
                    )
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertTrue(seccion["no_registra"])
        self.assertEqual(col.insert_one.call_args[0][0]["no_registra"], True)

    def test_cache_hit_recalcula_vigencia(self):
        # La caché guarda el veredicto del DÍA de la consulta: un hit posterior
        # RECALCULA `vigente` contra hoy (una capacitación "vigente" cacheada
        # con vencimiento 2001 está vencida HOY) — la caché no congela el
        # semáforo (mismo criterio que SOAT/RTM).
        cache = {
            "_id": ObjectId(),
            "tipo": "sisconmp", "cedula": "79882073",
            "no_registra": False,
            "mensaje": "",
            "apellidos": "GOMEZ GOMEZ", "nombres": "MARIO",
            "total_capacitaciones": 1,
            "capacitaciones": [{
                "tipo_capacitacion": "CURSO BASICO",
                "nombre": "Curso Básico TMR",
                "entidad_certificadora": "MEN",
                "institucion_educativa": "ACADEMIA X",
                "fecha_expedicion": "2001-01-10",
                "fecha_vencimiento": "2001-01-10",
                "clase": "", "descripcion_clase": "", "tipo_vehiculo": "",
                "vigente": True,  # stale: quedó así cuando se consultó
            }],
        }
        with patch.object(orch, "_buscar_cache", return_value=cache):
            with patch.object(orch, "consultar_capacitaciones_sisconmp_sync") as bot:
                seccion = self._correr(
                    orch._ejecutar_fuente("sisconmp", "79882073", actor_consultador(), False)
                )
        self.assertEqual(seccion["origen"], "cache")
        self.assertFalse(seccion["capacitaciones"][0]["vigente"])
        self.assertEqual(seccion["estado"], "ADVERTENCIA")
        bot.assert_not_called()

    def test_resultado_vacio_sin_determinante_es_no_disponible(self):
        # Segunda barrera anti-envenenamiento (la primera es el bot, que lee
        # la respuesta AJAX porque el DOM miente): dict vacío sin no_registra
        # ni mensaje NO se cachea.
        resultado = {
            "cedula": "79882073", "no_registra": None, "mensaje": "",
            "apellidos": "", "nombres": "",
            "total_capacitaciones": None, "capacitaciones": [], "pdf_bytes": None,
        }
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_capacitaciones_sisconmp_sync", return_value=resultado):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("sisconmp", "79882073", actor_consultador(), False)
                    )
        self.assertEqual(seccion["estado"], "NO_DISPONIBLE")
        self.assertEqual(seccion["error"]["tipo"], "portal_inconsistente")
        col.insert_one.assert_not_called()

    def test_bot_sin_resultado_es_no_disponible(self):
        from Funciones.bot_sisconmp import BotSisconmpSinResultado

        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_capacitaciones_sisconmp_sync") as bot:
                bot.side_effect = BotSisconmpSinResultado("respuesta sin JSON")
                with patch.object(orch, "BACKOFF_MS", 0):
                    seccion = self._correr(
                        orch._ejecutar_fuente("sisconmp", "79882073", actor_consultador(), False)
                    )
        self.assertEqual(seccion["estado"], "NO_DISPONIBLE")
        self.assertEqual(seccion["error"]["tipo"], "portal_inconsistente")


class TestFuenteSituacionMilitar(unittest.TestCase):
    """Fuente "situacion_militar" (libreta militar por CÉDULA): bot SIN
    navegador (GET al generador de certificados del Ejército, $0), caché
    (tipo, cédula), semáforo PENDIENTE/NO DEFINIDO/REMISO/APLAZADO →
    ADVERTENCIA y anti-envenenamiento análogo (certificado sin estado no se
    cachea)."""

    RESULTADO_RESERVISTA = {
        "cedula": "1010213062",
        "no_registra": False,
        "mensaje": "",
        "nombres": "EDWIN MISAEL",
        "apellidos": "ZARATE PEÑA",
        "nombre_completo": "EDWIN MISAEL ZARATE PEÑA",
        "tipo_documento": "Cédula de Ciudadanía",
        "estado_tarjeta_militar": "RESERVISTA - 2DA CLASE",
        "fecha_expedicion": "2026-09-24",
        "pdf_bytes": b"%PDF-falso",
        "pdf_ruta": None,
        "captura_jpg": None,
    }

    RESULTADO_VACIO = {
        "cedula": "99999999",
        "no_registra": True,
        "mensaje": "El ciudadano no registra situación militar con cédula de ciudadanía",
        "nombres": "", "apellidos": "", "nombre_completo": "",
        "estado_tarjeta_militar": "",
        "fecha_expedicion": None,
        "pdf_bytes": None, "pdf_ruta": None, "captura_jpg": None,
    }

    def _correr(self, corutina):
        return asyncio.run(corutina)

    def test_reservista_es_exito_y_cachea_por_cedula(self):
        with patch.object(orch, "_buscar_cache", return_value=None) as buscar:
            with patch.object(orch, "consultar_situacion_militar_sync",
                              return_value=self.RESULTADO_RESERVISTA) as bot:
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("situacion_militar", "1010213062", actor_consultador(), False)
                    )
        buscar.assert_called_once_with("situacion_militar", "1010213062", False, placa=None)
        bot.assert_called_once_with("1010213062")
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertEqual(seccion["origen"], "portal")
        self.assertEqual(seccion["estado_tarjeta_militar"], "RESERVISTA - 2DA CLASE")
        doc_cache = col.insert_one.call_args[0][0]
        self.assertEqual(doc_cache["tipo"], "situacion_militar")
        self.assertEqual(doc_cache["cedula"], "1010213062")
        # Minimización: el PDF del certificado NO se persiste en la caché.
        self.assertNotIn("pdf_bytes", doc_cache)

    def test_pendiente_es_advertencia(self):
        # Semáforo (decisión 2026-09-25): obligación militar SIN definir =
        # riesgo operativo para conducción → ADVERTENCIA (análogo SOAT/RTM).
        resultado = dict(self.RESULTADO_RESERVISTA,
                         estado_tarjeta_militar="PENDIENTE DE DEFINIR SITUACION MILITAR")
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_situacion_militar_sync", return_value=resultado):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("situacion_militar", "1010213062", actor_consultador(), False)
                    )
        self.assertEqual(seccion["estado"], "ADVERTENCIA")

    def test_sin_registro_es_exito_determinante(self):
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_situacion_militar_sync",
                              return_value=self.RESULTADO_VACIO):
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    seccion = self._correr(
                        orch._ejecutar_fuente("situacion_militar", "99999999", actor_consultador(), False)
                    )
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertTrue(seccion["no_registra"])
        self.assertEqual(col.insert_one.call_args[0][0]["no_registra"], True)

    def test_cache_hit_reconstruye_seccion(self):
        cache = {
            "_id": ObjectId(),
            "tipo": "situacion_militar", "cedula": "1010213062",
            "no_registra": False,
            "mensaje": "",
            "nombres": "EDWIN MISAEL", "apellidos": "ZARATE PEÑA",
            "nombre_completo": "EDWIN MISAEL ZARATE PEÑA",
            "estado_tarjeta_militar": "RESERVISTA - 2DA CLASE",
            "fecha_expedicion": "2026-09-24",
        }
        with patch.object(orch, "_buscar_cache", return_value=cache) as buscar:
            with patch.object(orch, "consultar_situacion_militar_sync") as bot:
                seccion = self._correr(
                    orch._ejecutar_fuente("situacion_militar", "1010213062", actor_consultador(), False)
                )
        buscar.assert_called_once_with("situacion_militar", "1010213062", False, placa=None)
        self.assertEqual(seccion["origen"], "cache")
        self.assertEqual(seccion["estado"], "EXITO")
        bot.assert_not_called()

    def test_bot_error_es_no_disponible(self):
        from Funciones.bot_situacion_militar import BotSituacionMilitarError

        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_situacion_militar_sync") as bot:
                bot.side_effect = BotSituacionMilitarError("API cayó")
                with patch.object(orch, "BACKOFF_MS", 0):
                    seccion = self._correr(
                        orch._ejecutar_fuente("situacion_militar", "1010213062", actor_consultador(), False)
                    )
        self.assertEqual(seccion["estado"], "NO_DISPONIBLE")
        self.assertEqual(seccion["error"]["tipo"], "situacion_militar_no_disponible")

    def test_fecha_expedicion_del_certificado(self):
        from Funciones.bot_situacion_militar import _fecha_expedicion

        texto = ("Se firma y se expide en BOGOTÁ, D.C. a los 24 días del mes de "
                 "SEPTIEMBRE de 2026, a las 21:22:55.")
        self.assertEqual(_fecha_expedicion(texto), "2026-09-24")
        self.assertIsNone(_fecha_expedicion("texto sin fecha"))


class TestFechasSisconmp(unittest.TestCase):
    """Parser de fechas del bot SISCONMP: ASP.NET serializa /Date(ms)/ (UTC)
    y el portal las muestra en hora LOCAL Colombia (UTC−5)."""

    def test_fecha_ms_a_iso_colombia(self):
        from datetime import datetime, timezone

        from Funciones.bot_sisconmp import _fecha_iso

        # Medianoche de Colombia del 2025-03-18 = 2025-03-18 05:00 UTC.
        ms = int(datetime(2025, 3, 18, 5, 0, tzinfo=timezone.utc).timestamp() * 1000)
        self.assertEqual(_fecha_iso(f"/Date({ms})/"), "2025-03-18")
        # 2025-03-18 04:59:59 UTC = 2025-03-17 23:59:59 Colombia → día anterior.
        ms2 = int(datetime(2025, 3, 18, 4, 59, 59, tzinfo=timezone.utc).timestamp() * 1000)
        self.assertEqual(_fecha_iso(f"/Date({ms2})/"), "2025-03-17")

    def test_fecha_iso_y_basura(self):
        from Funciones.bot_sisconmp import _fecha_iso

        self.assertEqual(_fecha_iso("2026-03-12T00:00:00"), "2026-03-12")
        self.assertIsNone(_fecha_iso(None))
        self.assertIsNone(_fecha_iso(""))
        self.assertIsNone(_fecha_iso("no es fecha"))

    def test_vigencia_contra_hoy(self):
        from Funciones.bot_sisconmp import _capacitacion_vigente

        self.assertTrue(_capacitacion_vigente("2099-12-31"))
        self.assertFalse(_capacitacion_vigente("2001-01-10"))
        self.assertIsNone(_capacitacion_vigente(None))
        self.assertIsNone(_capacitacion_vigente("basura"))


class TestFuenteSimitSoloPlaca(unittest.TestCase):
    """Estudio con SOLO simit: la placa se persiste (espejo) pero NO hay
    vehiculos[] ni cedula_propietario (simit no valida propiedad)."""

    def test_documento_solo_simit_sin_vehiculos(self):
        actor = actor_consultador()
        insertados = {}

        class ColFake:
            def insert_one(self, doc):
                doc["_id"] = ObjectId()
                insertados.update(doc)

        with patch.object(orch, "col_estudios", ColFake()):
            orch.crear_documento_estudio(
                consulta_id="ES-SIMIT1", cedula="1033688842", actor=actor,
                empresa={"nombre": "X", "config": {}}, forzar=False, auditoria={},
                placa="MVX48E", cedula_propietario=None,
            )
        self.assertEqual(insertados["placa"], "MVX48E")
        self.assertEqual(insertados["vehiculos"], [])


class TestCacheSimitConPlaca(unittest.TestCase):
    """La caché de simit es por (tipo, placa) SIN cédula: sin placa NUNCA hay
    hit; con placa el filtro no depende de la cédula evaluada."""

    def test_simit_sin_placa_nunca_hace_hit(self):
        from rutas import seguridad as rseg

        with patch.object(rseg, "col_consultas") as col:
            col.find_one.return_value = {"_id": ObjectId(), "tipo": "simit"}
            doc = rseg._buscar_cache("simit", None, False, placa=None)
        self.assertIsNone(doc)
        col.find_one.assert_not_called()

    def test_simit_filtra_por_placa_con_cedula_none(self):
        from rutas import seguridad as rseg

        with patch.object(rseg, "col_consultas") as col:
            col.find_one.return_value = None
            # La cédula del evaluado NO participa: mismo filtro para cualquier evaluado.
            rseg._buscar_cache("simit", "1033688842", False, placa="MVX48E")
        filtro = col.find_one.call_args[0][0]
        self.assertEqual(filtro["tipo"], "simit")
        self.assertIsNone(filtro["cedula"])
        self.assertEqual(filtro["placa"], "MVX48E")


class TestCaptchaProcuraduriaDocumento(unittest.TestCase):
    """2026-09-01: preguntas del captcha PGN derivadas DEL DOCUMENTO — el bot
    ya conoce la cédula del formulario; resolverlas determinista (antes iban a
    Gemini SIN la cédula y fallaban)."""

    CEDULA = "1033688842"

    def _correr(self, corutina):
        return asyncio.run(corutina)

    def test_dos_ultimos_digitos(self):
        from Funciones.bot_procuraduria import _resolver_captcha_documento

        self.assertEqual(
            _resolver_captcha_documento("¿Escriba los dos últimos dígitos del documento a consultar?", self.CEDULA),
            "42",
        )
        self.assertEqual(
            _resolver_captcha_documento("Escriba los ultimos dos digitos del documento", self.CEDULA),
            "42",
        )

    def test_variantes_del_documento(self):
        from Funciones.bot_procuraduria import _resolver_captcha_documento

        self.assertEqual(_resolver_captcha_documento("Escriba los tres últimos dígitos del documento", self.CEDULA), "842")
        self.assertEqual(_resolver_captcha_documento("¿Cuál es el primer dígito del documento?", self.CEDULA), "1")
        self.assertEqual(_resolver_captcha_documento("¿Cuántos dígitos tiene el documento?", self.CEDULA), "10")

    def test_preguntas_ajenas_al_documento_no_se_adivinan(self):
        from Funciones.bot_procuraduria import _resolver_captcha_documento

        # Sin pista de nombre: cae a Gemini/reintento.
        self.assertIsNone(
            _resolver_captcha_documento("¿Cuál es el primer nombre de la persona que está consultando?", self.CEDULA)
        )
        self.assertIsNone(_resolver_captcha_documento("¿Cuánto es 7 + 5?", self.CEDULA))
        self.assertIsNone(_resolver_captcha_documento("¿Capital de Francia?", self.CEDULA))
        self.assertIsNone(_resolver_captcha_documento("", self.CEDULA))

    def test_preguntas_de_nombre_con_pista_del_consultante(self):
        """2026-09-01 (estrategia): el portal pide nombres/apellidos cuando el
        plan incluye procuraduria — el captcha de nombre se responde con esa
        pista (SIN tildes, mayúsculas, ya normalizada por el endpoint)."""
        from Funciones.bot_procuraduria import _resolver_captcha_documento

        NOMBRES = "JHOAM ORLANDO"
        APELLIDOS = "AMAYA TOVAR"
        self.assertEqual(
            _resolver_captcha_documento(
                "¿Cuál es el primer nombre de la persona que está consultando?",
                self.CEDULA, NOMBRES, APELLIDOS,
            ),
            "JHOAM",
        )
        self.assertEqual(
            _resolver_captcha_documento("Escriba el primer apellido de la persona consultada", self.CEDULA, NOMBRES, APELLIDOS),
            "AMAYA",
        )
        self.assertEqual(
            _resolver_captcha_documento("¿Segundo nombre?", self.CEDULA, NOMBRES, APELLIDOS),
            "ORLANDO",
        )
        # Segundo apellido inexistente / sin pista: NO se adivina.
        self.assertIsNone(_resolver_captcha_documento("¿Tercer nombre?", self.CEDULA, NOMBRES, APELLIDOS))
        self.assertIsNone(
            _resolver_captcha_documento("¿Cuál es el primer nombre de la persona que está consultando?", self.CEDULA)
        )

    def test_orquestador_pasa_nombres_al_bot_procuraduria(self):
        """La pista viaja por toda la cadena hasta consultar_antecedentes_sync."""
        resultado = {
            "cedula": "1033688842", "no_registra": True,
            "mensaje": "NO REGISTRA SANCIONES NI INHABILIDADES VIGENTES",
            "texto_resultado": "ok", "pdf_bytes": b"%PDF-fake",
        }
        with patch.object(orch, "_buscar_cache", return_value=None):
            with patch.object(orch, "consultar_antecedentes_sync", return_value=resultado) as bot:
                with patch.object(orch, "col_consultas") as col:
                    col.insert_one.return_value = None
                    with patch.object(orch, "BACKOFF_MS", 0):
                        self._correr(
                            orch._ejecutar_fuente(
                                "procuraduria", "1033688842", actor_consultador(), False,
                                nombres="JHOAM ORLANDO", apellidos="AMAYA TOVAR",
                            )
                        )
        bot.assert_called_once_with(
            "1033688842", nombres="JHOAM ORLANDO", apellidos="AMAYA TOVAR",
        )

    def test_normalizar_nombre_sin_tildes(self):
        self.assertEqual(se._normalizar_nombre("Jhoam Orlandó Ámaya"), "JHOAM ORLANDO AMAYA")
        self.assertEqual(se._normalizar_nombre("josé muñoz"), "JOSE MUÑOZ")  # Ñ se conserva
        self.assertIsNone(se._normalizar_nombre("   "))
        self.assertIsNone(se._normalizar_nombre(None))
        self.assertIsNone(se._normalizar_nombre("123"))

    def test_normalizar_nit_sin_dv(self):
        self.assertEqual(se._normalizar_nit_sin_dv("900.123.456-7"), "900123456")
        self.assertEqual(se._normalizar_nit_sin_dv("900123456"), "900123456")


class TestMayoriaFuentesFallidas(unittest.TestCase):
    """Criterio del reembolso automático (decisión de negocio 2026-09-01): se
    devuelve la consulta SOLO si >51% de las fuentes CORRIDAS fallaron — con
    la mitad o menos caídas lo entregado es valioso y se cobra (antes solo
    reembolsaba el ERROR global = 100% caídas)."""

    def _f(self, estado):
        return {"estado": estado, "origen": "portal", "intentos": 1, "error": None}

    def test_mitad_fallida_no_reembolsa(self):
        # El caso del usuario: plan proc+simit, la PGN caída — simit entregó.
        fuentes = {"procuraduria": self._f("NO_DISPONIBLE"), "simit": self._f("EXITO")}
        self.assertFalse(orch.mayoria_fuentes_fallidas(fuentes))

    def test_una_de_tres_no_reembolsa(self):
        fuentes = {
            "manifiestos_rndc": self._f("EXITO"),
            "procuraduria": self._f("ERROR"),
            "simit": self._f("EXITO"),
        }
        self.assertFalse(orch.mayoria_fuentes_fallidas(fuentes))

    def test_dos_de_tres_reembolsa(self):
        # 66% caídas: ya no queda mayoritariamente nada valioso.
        fuentes = {
            "manifiestos_rndc": self._f("EXITO"),
            "procuraduria": self._f("NO_DISPONIBLE"),
            "simit": self._f("ERROR"),
        }
        self.assertTrue(orch.mayoria_fuentes_fallidas(fuentes))

    def test_todas_fallidas_reembolsa(self):
        # 100% (el ERROR global de siempre).
        fuentes = {"procuraduria": self._f("NO_DISPONIBLE"), "simit": self._f("ERROR")}
        self.assertTrue(orch.mayoria_fuentes_fallidas(fuentes))

    def test_una_de_una_reembolsa(self):
        self.assertTrue(orch.mayoria_fuentes_fallidas({"procuraduria": self._f("NO_DISPONIBLE")}))

    def test_advertencia_cuenta_como_entregada(self):
        fuentes = {"procuraduria": self._f("ADVERTENCIA"), "simit": self._f("ADVERTENCIA")}
        self.assertFalse(orch.mayoria_fuentes_fallidas(fuentes))

    def test_deshabilitadas_y_ausentes_no_cuentan(self):
        # El plan excluyó fuentes: no son fallos (ni salvación del conteo).
        fuentes = {
            "procuraduria": self._f("ERROR"),
            "simit": self._f("ERROR"),
            "runt": self._f("DESHABILITADA"),
            "policia": self._f("DESHABILITADA"),
        }
        self.assertTrue(orch.mayoria_fuentes_fallidas(fuentes))

    def test_nada_corrido_reembolsa(self):
        # Falla catastrófica (fuentes.error_global str o dict vacío): no se
        # entregó nada → reembolso (comportamiento previo del ERROR global).
        self.assertTrue(orch.mayoria_fuentes_fallidas({}))
        self.assertTrue(orch.mayoria_fuentes_fallidas({"error_global": "boom"}))



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
            "fuentes": {
                "procuraduria": {"estado": "EXITO"},
                "policia": {"estado": "DESHABILITADA"},
            },
            "pdf": {"sha256": "ab12" * 16},
        }
        with patch.object(se, "col_estudios") as col:
            col.find_one.return_value = doc
            with patch.object(se, "registrar_evento"):
                respuesta = se.verificar_estudio("ES-ABC", "XYZ1234567")
        self.assertTrue(respuesta["valido"])
        self.assertEqual(respuesta["cedula"], "10******42")
        self.assertEqual(respuesta["consulta_id"], "ES-ABC")
        self.assertEqual([f["codigo"] for f in respuesta["fuentes"]], ["procuraduria"])
        self.assertEqual(respuesta["huella_documento"], "ab12" * 16)

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

        async def _fuente_ok(nombre, cedula, actor, forzar, **kwargs):
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


class TestEvidenciasConsulta(unittest.TestCase):
    """Evidencias visuales (pantallazos del portal por fuente, patrón
    TusDatos, 2026-09-24): el bot retorna `captura_jpg` → sección `_captura`
    (volátil, jamás al doc) → caché 24 h (un hit muestra la MISMA evidencia) y
    GCS privado del estudio (referencia en `evidencias` del doc)."""

    ACTOR = {"usuario": "U", "perfil": "SEGURIDAD", "empresa_id": "e", "usuario_id": "u"}

    def _correr_policia(self, resultado):
        with patch.object(orch, "_buscar_cache", return_value=None), \
             patch.object(orch, "_llamar_con_reintento",
                          return_value=(resultado, 1, [1.0], None)), \
             patch.object(orch.col_consultas, "insert_one") as insert_cache:
            seccion = asyncio.run(
                orch._ejecutar_fuente("policia", "1033688842", self.ACTOR, forzar=False)
            )
        return seccion, insert_cache

    def test_captura_del_bot_va_a_la_seccion_y_a_la_cache(self):
        resultado = {
            "no_registra": True, "mensaje": "NO TIENE ASUNTOS PENDIENTES",
            "nombre_consultado": "FULANO DE TAL", "captura_jpg": b"JPEG_POLICIA",
        }
        seccion, insert_cache = self._correr_policia(resultado)
        self.assertEqual(seccion["estado"], "EXITO")
        self.assertEqual(seccion["_captura"], b"JPEG_POLICIA")
        doc_cache = insert_cache.call_args[0][0]
        self.assertEqual(doc_cache["captura_jpg"], b"JPEG_POLICIA")

    def test_sin_captura_la_cache_no_lleva_la_clave(self):
        resultado = {"no_registra": True, "mensaje": "x", "nombre_consultado": "F"}
        seccion, insert_cache = self._correr_policia(resultado)
        self.assertIsNone(seccion.get("_captura"))
        self.assertNotIn("captura_jpg", insert_cache.call_args[0][0])

    def test_hit_de_cache_restaura_la_captura(self):
        cache = {
            "_id": ObjectId(), "tipo": "policia", "cedula": "1033688842",
            "no_registra": True, "mensaje": "x", "nombre_consultado": "F",
            "captura_jpg": b"JPEG_CACHE",
        }
        with patch.object(orch, "_buscar_cache", return_value=cache):
            seccion = asyncio.run(
                orch._ejecutar_fuente("policia", "1033688842", self.ACTOR, forzar=False)
            )
        self.assertEqual(seccion["origen"], "cache")
        self.assertEqual(seccion["_captura"], b"JPEG_CACHE")

    def test_limpiar_seccion_descarta_la_captura(self):
        limpia = orch._limpiar_seccion({"estado": "EXITO", "mensaje": "x", "_captura": b"J"})
        self.assertNotIn("_captura", limpia)
        self.assertEqual(limpia["mensaje"], "x")

    def test_con_captura_solo_para_fuentes_de_navegador(self):
        self.assertEqual(orch._con_captura({}, "policia", b"J")["captura_jpg"], b"J")
        self.assertEqual(orch._con_captura({}, "policia", None), {})
        self.assertEqual(orch._con_captura({}, "ofac", b"J"), {})  # API/dataset: sin captura

    def test_ejecutar_estudio_sube_evidencias_y_persiste_referencias(self):
        from Funciones import storage_seguridad

        empresa = {"_id": "emp", "nombre": "E", "config": {}}
        doc_inicial = {"_id": "x", "consulta_id": "ES-EV"}
        persistido: dict = {}

        def _find_one(query=None, *a, **k):
            return persistido.get("doc") or doc_inicial

        def _update_one(query, update):
            persistido["doc"] = {**doc_inicial, **update.get("$set", {})}

        async def _fuente(nombre, cedula, actor, forzar, **kwargs):
            seccion = {"estado": "EXITO", "origen": "portal", "intentos": 1,
                       "duraciones_s": [2.0], "error": None}
            if nombre == "policia":
                seccion["_captura"] = b"JPEG_POLICIA"
            return seccion

        subidas = []

        def _subir(contenido, ruta, cedula, content_type="application/pdf"):
            subidas.append((contenido, ruta, content_type))
            return {"gcs_ruta": ruta, "sha256": "ab" * 32, "tamano": len(contenido)}

        with patch.object(orch.col_estudios, "find_one", side_effect=_find_one), \
             patch.object(orch.col_estudios, "update_one", side_effect=_update_one), \
             patch.object(orch, "_ejecutar_fuente", side_effect=_fuente), \
             patch.object(storage_seguridad, "ruta_blob",
                          side_effect=lambda e, a, c, s="", ext=".pdf":
                          f"{storage_seguridad.CARPETA_SEGURIDAD}/{e}/{a}/{c}{s}{ext}"), \
             patch.object(storage_seguridad, "subir_pdf", side_effect=_subir):
            resultado = asyncio.run(orch.ejecutar_estudio(
                consulta_id="ES-EV", cedula="1033688842",
                actor={"usuario": "U", "usuario_id": "x", "empresa_id": "emp"},
                empresa=empresa, forzar=False, auditoria={},
                registrar_evento=lambda *a, **k: None,
                fuentes=["policia"],
            ))
        self.assertEqual(len(subidas), 1)
        contenido, ruta, ctype = subidas[0]
        self.assertEqual(contenido, b"JPEG_POLICIA")
        self.assertEqual(ctype, "image/jpeg")
        self.assertIn("_captura_policia.jpg", ruta)
        self.assertEqual(persistido["doc"]["evidencias"]["policia"]["gcs_ruta"], ruta)
        # La captura JAMÁS llega al doc de la fuente (clave volátil `_`).
        self.assertNotIn("_captura", persistido["doc"]["fuentes"]["policia"])
        self.assertEqual(resultado["estado"], "COMPLETADA")

    def test_fallo_de_subida_de_evidencia_no_tumba_el_estudio(self):
        from Funciones import storage_seguridad

        empresa = {"_id": "emp", "nombre": "E", "config": {}}
        doc_inicial = {"_id": "x", "consulta_id": "ES-EV2"}
        persistido: dict = {}

        def _find_one(query=None, *a, **k):
            return persistido.get("doc") or doc_inicial

        def _update_one(query, update):
            persistido["doc"] = {**doc_inicial, **update.get("$set", {})}

        async def _fuente(nombre, cedula, actor, forzar, **kwargs):
            return {"estado": "EXITO", "origen": "portal", "intentos": 1,
                    "duraciones_s": [2.0], "error": None, "_captura": b"J"}

        with patch.object(orch.col_estudios, "find_one", side_effect=_find_one), \
             patch.object(orch.col_estudios, "update_one", side_effect=_update_one), \
             patch.object(orch, "_ejecutar_fuente", side_effect=_fuente), \
             patch.object(storage_seguridad, "subir_pdf", side_effect=RuntimeError("GCS abajo")):
            resultado = asyncio.run(orch.ejecutar_estudio(
                consulta_id="ES-EV2", cedula="1033688842",
                actor={"usuario": "U", "usuario_id": "x", "empresa_id": "emp"},
                empresa=empresa, forzar=False, auditoria={},
                registrar_evento=lambda *a, **k: None,
                fuentes=["policia"],
            ))
        self.assertEqual(resultado["estado"], "COMPLETADA")  # la fuente salió bien
        self.assertEqual(persistido["doc"]["evidencias"], {})  # sin evidencia, sin referencia
        self.assertIn("evidencia_error", persistido["doc"]["fuentes"]["policia"])


class TestCapturaEvidenciaHelper(unittest.TestCase):
    """capturar_viewport_jpeg: best-effort — un fallo del screenshot devuelve
    None y NUNCA tumba la consulta."""

    def test_screenshot_fallido_devuelve_none(self):
        from Funciones.captura_evidencia import capturar_viewport_jpeg

        class PaginaRota:
            async def screenshot(self, **kwargs):
                raise RuntimeError("navegador cerrado")

        self.assertIsNone(asyncio.run(capturar_viewport_jpeg(PaginaRota())))

    def test_screenshot_ok_devuelve_bytes_jpeg(self):
        from Funciones.captura_evidencia import capturar_viewport_jpeg

        class PaginaFake:
            async def screenshot(self, **kwargs):
                assert kwargs.get("type") == "jpeg"
                assert 1 <= kwargs.get("quality", 0) <= 100
                return b"IMAGEN_JPEG"

        self.assertEqual(asyncio.run(capturar_viewport_jpeg(PaginaFake())), b"IMAGEN_JPEG")


class TestRutaBlobExtension(unittest.TestCase):
    def test_extension_parametrizable_para_capturas(self):
        from Funciones import storage_seguridad

        ruta = storage_seguridad.ruta_blob("emp", 2026, "ES-1", "_captura_policia", ext=".jpg")
        self.assertTrue(ruta.endswith("ES-1_captura_policia.jpg"))
        # Compat: sin ext sigue siendo .pdf (todas las rutas existentes).
        por_defecto = storage_seguridad.ruta_blob("emp", 2026, "ES-1")
        self.assertTrue(por_defecto.endswith("ES-1.pdf"))


# === Memoria de personas consultadas (personas_seguridad, 2026-09-25) ==========

class ColPersonasFake:
    """Fake mínimo de personas_seguridad: update_one upsert + find_one."""

    def __init__(self):
        self.docs: dict[str, dict] = {}

    def update_one(self, filtro, update, upsert=False):
        cedula = filtro.get("cedula")
        doc = self.docs.get(cedula)
        creado = doc is None
        if creado:
            if not upsert:
                return
            doc = {"cedula": cedula}
            self.docs[cedula] = doc
        if "$setOnInsert" in update and not creado:
            pass  # solo aplica al insertar
        for operador, campos in update.items():
            if operador == "$set":
                doc.update(campos)
            elif operador == "$setOnInsert" and creado:
                doc.update(campos)
            elif operador == "$inc":
                for k, v in campos.items():
                    doc[k] = doc.get(k, 0) + v
            elif operador == "$addToSet":
                for k, v in campos.items():
                    doc.setdefault(k, [])
                    if v not in doc[k]:
                        doc[k].append(v)

    def find_one(self, filtro):
        doc = self.docs.get(filtro.get("cedula"))
        if doc is None:
            return None
        # El gate de visibilidad es el filtro por empresa.
        if filtro.get("empresas") is not None and filtro["empresas"] not in doc.get("empresas", []):
            return None
        return dict(doc)


class TestMemoriaPersonas(unittest.TestCase):
    def setUp(self):
        self.col = ColPersonasFake()
        self.patcher = patch.object(se.personas, "col_personas", self.col)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)

    def test_upsert_crea_e_incrementa(self):
        from Funciones import personas_seguridad as ps

        ps.registrar_consulta_persona("79882073", EMPRESA_A, nombres="DIDIER ALBEIRO", apellidos="PIÑEROS ARIZA")
        ps.registrar_consulta_persona("79882073", EMPRESA_A)
        doc = self.col.docs["79882073"]
        self.assertEqual(doc["total_consultas"], 2)
        self.assertEqual(doc["empresas"], [EMPRESA_A])
        # La segunda consulta no traía nombres: los de la primera SOBREVIVEN.
        self.assertEqual(doc["nombres"], "DIDIER ALBEIRO")
        self.assertIn("primera_consulta_en", doc)

    def test_nombre_consultado_verificado_gana(self):
        from Funciones import personas_seguridad as ps

        ps.registrar_consulta_persona("79882073", EMPRESA_A, nombre_consultado="PIÑEROS ARIZA DIDIER ALBEIRO")
        ps.registrar_consulta_persona("79882073", EMPRESA_A, nombre_consultado="PIÑEROS ARIZA DIDIER ALBEIRO")
        doc = self.col.docs["79882073"]
        self.assertEqual(doc["nombre_consultado"], "PIÑEROS ARIZA DIDIER ALBEIRO")

    def test_buscar_persona_aislada_por_empresa(self):
        from Funciones import personas_seguridad as ps

        ps.registrar_consulta_persona("79882073", EMPRESA_A, fecha_expedicion="14/02/2012")
        self.assertIsNotNone(ps.buscar_persona("79882073", EMPRESA_A))
        # OTRA empresa no la ha consultado: no ve la memoria (aislamiento).
        self.assertIsNone(ps.buscar_persona("79882073", EMPRESA_B))
        persona = ps.buscar_persona("79882073", EMPRESA_A)
        self.assertEqual(persona["fecha_expedicion"], "14/02/2012")
        self.assertEqual(persona["total_consultas"], 1)

    def test_registrar_jamas_lanza(self):
        from Funciones import personas_seguridad as ps

        class ColQueExplota(ColPersonasFake):
            def update_one(self, filtro, update, upsert=False):
                raise RuntimeError("mongo caído")

        with patch.object(ps, "col_personas", ColQueExplota()):
            ps.registrar_consulta_persona("79882073", EMPRESA_A)  # no raise
            self.assertIsNone(ps.buscar_persona("79882073", EMPRESA_A))


class TestCompletarDesdeMemoria(unittest.TestCase):
    def test_llena_vacios_con_memoria(self):
        with patch.object(se.personas, "buscar_persona", return_value={
            "nombres": "DIDIER ALBEIRO", "apellidos": "PIÑEROS ARIZA",
            "fecha_expedicion": "14/02/2012", "total_consultas": 3,
        }):
            nombres, apellidos, fecha = se._completar_desde_memoria(
                "79882073", str(EMPRESA_A), None, None, ""
            )
        self.assertEqual(nombres, "DIDIER ALBEIRO")
        self.assertEqual(apellidos, "PIÑEROS ARIZA")
        self.assertEqual(fecha, "14/02/2012")

    def test_lo_del_body_siempre_gana(self):
        with patch.object(se.personas, "buscar_persona", return_value={
            "nombres": "DIDIER ALBEIRO", "apellidos": "PIÑEROS ARIZA",
            "fecha_expedicion": "14/02/2012",
        }):
            nombres, apellidos, fecha = se._completar_desde_memoria(
                "79882073", str(EMPRESA_A), "DIDIER ALBERTO", "PÉREZ", "01/01/2001"
            )
        self.assertEqual(nombres, "DIDIER ALBERTO")
        self.assertEqual(apellidos, "PÉREZ")
        self.assertEqual(fecha, "01/01/2001")

    def test_sin_memoria_retorna_tal_cual(self):
        with patch.object(se.personas, "buscar_persona", return_value=None):
            nombres, apellidos, fecha = se._completar_desde_memoria(
                "79882073", str(EMPRESA_A), None, None, ""
            )
        self.assertIsNone(nombres)
        self.assertIsNone(apellidos)
        self.assertEqual(fecha, "")


class TestEndpointPersonas(unittest.TestCase):
    def test_retorna_datos_para_empresa_correcta(self):
        with patch.object(se.personas, "buscar_persona", return_value={
            "nombres": "DIDIER ALBEIRO", "apellidos": "PIÑEROS ARIZA",
            "nombre_consultado": "PIÑEROS ARIZA DIDIER ALBEIRO",
            "fecha_expedicion": "14/02/2012", "total_consultas": 2,
            "ultima_consulta_en": datetime(2026, 9, 25, 15, 0, 0),
        }) as mock_buscar:
            resp = se.buscar_persona_consultada(
                request=None, actor=actor_consultador(EMPRESA_A), cedula="79882073",
            )
        self.assertTrue(resp["encontrada"])
        self.assertEqual(resp["fecha_expedicion"], "14/02/2012")
        self.assertEqual(resp["total_consultas"], 2)
        self.assertTrue(resp["ultima_consulta_en"].endswith("Z"))
        # Aislamiento: el lookup se hace contra la empresa del ACTOR.
        mock_buscar.assert_called_once_with("79882073", str(EMPRESA_A))

    def test_no_encontrada_otra_empresa(self):
        with patch.object(se.personas, "buscar_persona", return_value=None):
            resp = se.buscar_persona_consultada(
                request=None, actor=actor_consultador(EMPRESA_B), cedula="79882073",
            )
        self.assertFalse(resp["encontrada"])

    def test_admin_integra_sin_empresa_exige_empresa_id(self):
        actor = actor_consultador(EMPRESA_A)
        actor["rol"] = ROL_ADMIN_INTEGRA
        actor["empresa_id"] = None
        with self.assertRaises(HTTPException) as ctx:
            # empresa_id explícito: llamado directo (el default es Query(None)).
            se.buscar_persona_consultada(
                request=None, actor=actor, cedula="79882073", empresa_id=None,
            )
        self.assertEqual(ctx.exception.status_code, 422)


class TestNombresVerificados(unittest.TestCase):
    """El nombre que alimenta la memoria debe ser el VERIFICADO por un portal
    oficial (situacion_militar/sisconmp separados), no el digitado a mano:
    un nombre mal escrito la primera vez no debe envenenar la memoria."""

    def _estudio(self, fuentes: dict) -> dict:
        return {"fuentes": fuentes}

    def test_situacion_militar_verificada_gana(self):
        estudio = self._estudio({
            "situacion_militar": {
                "estado": "EXITO", "nombres": "DIDIER ALBEIRO", "apellidos": "PIÑEROS ARIZA",
            },
        })
        self.assertEqual(
            se._nombres_verificados_del_estudio(estudio), ("DIDIER ALBEIRO", "PIÑEROS ARIZA")
        )

    def test_sisconmp_como_fallback(self):
        estudio = self._estudio({
            "situacion_militar": {"estado": "NO_DISPONIBLE", "nombres": "", "apellidos": ""},
            "sisconmp": {
                "estado": "EXITO", "nombres": "DIDIER ALBEIRO", "apellidos": "PIÑEROS ARIZA",
            },
        })
        self.assertEqual(
            se._nombres_verificados_del_estudio(estudio), ("DIDIER ALBEIRO", "PIÑEROS ARIZA")
        )

    def test_fuente_advertencia_tambien_cuenta(self):
        # ADVERTENCIA (p.ej. libreta REMISO) igual ENTREGÓ el nombre del acta.
        estudio = self._estudio({
            "situacion_militar": {
                "estado": "ADVERTENCIA", "nombres": "DIDIER ALBEIRO", "apellidos": "PIÑEROS ARIZA",
            },
        })
        self.assertEqual(
            se._nombres_verificados_del_estudio(estudio), ("DIDIER ALBEIRO", "PIÑEROS ARIZA")
        )

    def test_sin_fuente_verificada_retorna_none(self):
        estudio = self._estudio({
            "situacion_militar": {"estado": "NO_DISPONIBLE"},
            "sisconmp": {"estado": "ERROR"},
            "procuraduria": {"estado": "EXITO", "nombre_consultado": "PIÑEROS ARIZA DIDIER ALBEIRO"},
        })
        # La cascada PGN/Policía NO alimenta los campos separados (sin split
        # confiable): sigue siendo (None, None) y la memoria conserva lo
        # digitado.
        self.assertEqual(se._nombres_verificados_del_estudio(estudio), (None, None))
        self.assertEqual(se._nombres_verificados_del_estudio(None), (None, None))
        self.assertEqual(se._nombres_verificados_del_estudio({}), (None, None))


class TestCompletarFuentesPendientes(unittest.TestCase):
    """(2026-09-25) Completado de fuentes: re-ejecutar SOLO las que quedaron
    NO_DISPONIBLE/ERROR de un estudio cerrado, mergeando sobre lo que ya
    respondió, SIN consumo de cobro nuevo y contando los intentos."""

    def _doc(self) -> dict:
        return {
            "_id": "objid", "consulta_id": "ES-COMP01", "estado": "PARCIAL",
            "cedula": "1033688842", "nombres": "JHOAM", "apellidos": "AMAYA",
            "nit": None, "fecha_expedicion": "14/02/2012",
            "placa": "MVX48E",
            "vehiculos": [{"placa": "MVX48E", "cedula_propietario": "1010213062", "propietario_es_evaluado": False}],
            "nombre_consultado": "NOMBRE VIEJO",
            "evidencias": {"sena": {"gcs_ruta": "previa.jpg"}},
            "fuentes": {
                "manifiestos_rndc": {"estado": "EXITO", "origen": "cache"},
                "procuraduria": {"estado": "NO_DISPONIBLE", "origen": None,
                                 "error": {"tipo": "TimeoutError", "mensaje": "La fuente no respondió en 150 s"}},
                "sena": {"estado": "ERROR", "origen": None, "error": {"tipo": "Error", "mensaje": "x"}},
                "simit": {"estado": "DESHABILITADA", "origen": None},
            },
            "creado_en": datetime(2026, 9, 25, 12, 0, 0),
        }

    def _montar(self, doc, resultado_fuente):
        """Parchea col_estudios y _ejecutar_fuente; devuelve (persistido, llamadas)."""
        import asyncio
        from unittest.mock import patch

        from Funciones import orquestador_estudios as orch

        persistido: dict = {"doc": doc}
        llamadas: list[str] = []

        def _find_one(query=None, *a, **k):
            return persistido["doc"]

        def _update_one(query, update):
            doc.update(update.get("$set", {}))
            doc["completar_intentos"] = doc.get("completar_intentos", 0) + update.get("$inc", {}).get("completar_intentos", 0)
            return None

        async def _fuente(nombre, cedula, actor, forzar, **kwargs):
            llamadas.append(nombre)
            return dict(resultado_fuente(nombre))

        return persistido, llamadas, _find_one, _update_one, _fuente, orch

    def test_reintenta_solo_las_fallidas_y_merjea(self):
        import asyncio
        from unittest.mock import patch

        doc = self._doc()
        persistido, llamadas, _find_one, _update_one, _fuente, orch = self._montar(
            doc,
            lambda n: {"estado": "EXITO", "origen": "portal", "intentos": 1,
                       "duraciones_s": [5.0], "error": None,
                       "nombre_certificado": "AMAYA TOVAR JHOAM"} if n == "procuraduria" else
                      {"estado": "EXITO", "origen": "portal", "intentos": 1,
                       "duraciones_s": [5.0], "error": None},
        )
        with patch.object(orch.col_estudios, "find_one", side_effect=_find_one), \
             patch.object(orch.col_estudios, "update_one", side_effect=_update_one), \
             patch.object(orch, "_ejecutar_fuente", side_effect=_fuente):
            resultado, corridas = asyncio.run(orch.reintentar_fuentes_estudio(
                consulta_id="ES-COMP01",
                actor={"usuario": "U", "usuario_id": "x", "empresa_id": "emp"},
                empresa={"_id": "emp", "nombre": "E", "nit": "9001"},
                registrar_evento=lambda *a, **k: None,
            ))
        # Solo las dos fallidas se re-consultaron (no runt EXITO ni simit DESHABILITADA).
        self.assertEqual(sorted(llamadas), ["procuraduria", "sena"])
        self.assertEqual(sorted(corridas), ["procuraduria", "sena"])
        # Merge: la fuente que ya respondió queda INTACTA.
        self.assertEqual(resultado["fuentes"]["manifiestos_rndc"]["origen"], "cache")
        self.assertEqual(resultado["fuentes"]["procuraduria"]["estado"], "EXITO")
        # DESHABILITADA se conserva (fue decisión del plan, no un fallo).
        self.assertEqual(resultado["fuentes"]["simit"]["estado"], "DESHABILITADA")
        # Todas respondieron → estado global recalculado sin las deshabilitadas.
        self.assertEqual(resultado["estado"], "COMPLETADA")
        # La cascada del nombre NUEVO gana sobre la vieja.
        self.assertEqual(resultado["nombre_consultado"], "AMAYA TOVAR JHOAM")
        # Contador de intentos para el tope del endpoint.
        self.assertEqual(resultado["completar_intentos"], 1)

    def test_pasos_del_doc_original(self):
        """La re-consulta usa los parámetros persistidos: placa + cédula del
        PROPIETARIO (vehículos[0]), nombres, fecha de expedición."""
        import asyncio
        from unittest.mock import patch

        doc = self._doc()
        persistido, llamadas, _find_one, _update_one, _fuente, orch = self._montar(
            doc, lambda n: {"estado": "EXITO", "origen": "portal", "intentos": 1, "error": None},
        )
        kwargs_vistos: list[dict] = []

        async def _fuente_spy(nombre, cedula, actor, forzar, **kwargs):
            kwargs_vistos.append({"nombre": nombre, "cedula": cedula, **kwargs})
            return {"estado": "EXITO", "origen": "portal", "intentos": 1, "error": None}

        with patch.object(orch.col_estudios, "find_one", side_effect=_find_one), \
             patch.object(orch.col_estudios, "update_one", side_effect=_update_one), \
             patch.object(orch, "_ejecutar_fuente", side_effect=_fuente_spy):
            asyncio.run(orch.reintentar_fuentes_estudio(
                "ES-COMP01", {"usuario": "U", "empresa_id": "emp"},
                {"_id": "emp", "nombre": "E"}, lambda *a, **k: None,
            ))
        por_nombre = {k["nombre"]: k for k in kwargs_vistos}
        self.assertEqual(por_nombre["procuraduria"]["cedula"], "1033688842")
        self.assertEqual(por_nombre["procuraduria"]["nombres"], "JHOAM")
        self.assertEqual(por_nombre["procuraduria"]["fecha_expedicion"], "14/02/2012")
        self.assertEqual(por_nombre["procuraduria"]["apellidos"], "AMAYA")
        # La empresa consultante viaja (la exige el portal Ley 1918).
        self.assertEqual(por_nombre["procuraduria"]["empresa_consultante"]["nombre"], "E")

    def test_sin_pendientes_es_idempotente(self):
        import asyncio
        from unittest.mock import patch

        from Funciones import orquestador_estudios as orch

        doc = self._doc()
        doc["fuentes"] = {"manifiestos_rndc": {"estado": "EXITO"}}
        with patch.object(orch.col_estudios, "find_one", return_value=doc), \
             patch.object(orch.col_estudios, "update_one") as upd:
            resultado, corridas = asyncio.run(orch.reintentar_fuentes_estudio(
                "ES-COMP01", {"usuario": "U", "empresa_id": "emp"}, {}, lambda *a, **k: None,
            ))
        self.assertEqual(corridas, [])
        self.assertEqual(resultado["estado"], "PARCIAL")  # doc tal cual, sin tocar
        upd.assert_not_called()

    def test_fuentes_pendientes_estudio(self):
        from Funciones import orquestador_estudios as orch

        pendientes = orch.fuentes_pendientes_estudio(self._doc())
        self.assertEqual(sorted(pendientes), ["procuraduria", "sena"])
        # error_global (str) y claves ausentes no cuentan.
        self.assertEqual(
            orch.fuentes_pendientes_estudio({"fuentes": {"error_global": "boom"}}), [],
        )


class TestConsumosDelCompletado(unittest.TestCase):
    """(2026-09-25, decisión del usuario) Cobro del completado de fuentes:
    una consulta REEMBOLSADA (>51% fallidas) se vuelve a cobrar al completar
    (el estudio completo se terminó entregando); una consulta ya cobrada
    (≤51% caídas) completa gratis — esas fuentes ya se pagaron."""

    def _doc(self) -> dict:
        return {
            "consulta_id": "ES-COB01", "empresa_id": EMPRESA_A,
            "fuentes": {
                "manifiestos_rndc": {"estado": "EXITO"},
                "procuraduria": {"estado": "NO_DISPONIBLE"},
                "simit": {"estado": "DESHABILITADA"},  # decisión del plan: no se cobró
            },
        }

    def _fake_db(self, reembolso: bool, planes_consumo: list):
        """db fake con movimientos_cobro y planes; find_one para el lookup de
        REEMBOLSO y find para los CONSUMO con plan."""
        reembolsos = [{"_id": 1}] if reembolso else []

        class ColMov:
            def find_one(self, q, *a, **k):
                return reembolsos[0] if q.get("tipo") == "REEMBOLSO" and reembolsos else None

            def find(self, q, *a, **k):
                if q.get("tipo") == "CONSUMO":
                    return iter([{"plan_id": p} for p in planes_consumo])
                return iter([])

            def aggregate(self, pipeline):
                return iter([])

        class ColEmp:
            def find_one(self, q, *a, **k):
                return None

        return {
            "movimientos_cobro_seguridad": ColMov(),
            "planes_seguridad": ColEmp(),
        }

    def test_consulta_ya_cobrada_completa_gratis(self):
        from Funciones import cobro_seguridad as cobro
        from unittest.mock import patch

        doc = self._doc()
        with patch.object(se, "db", self._fake_db(reembolso=False, planes_consumo=[ObjectId()])) as db_fake, \
             patch.object(cobro, "reservar_consumos") as reservar:
            consumos = se._consumos_del_completado(
                doc, {"_id": EMPRESA_A, "nombre": "E", "planes": []},
                actor_consultador(EMPRESA_A), actor_consultador(EMPRESA_A),
            )
        self.assertEqual(consumos, [])
        reservar.assert_not_called()  # ≤51% caídas: ya se cobró, completa gratis

    def test_consulta_reembolsada_se_vuelve_a_cobrar(self):
        from Funciones import cobro_seguridad as cobro
        from unittest.mock import patch

        doc = self._doc()
        plan_original = ObjectId()
        llamada: dict = {}

        def reservar(empresa, actor, consulta_id, fuentes, plan_preferido_id=None, **kw):
            llamada["fuentes"] = list(fuentes)
            llamada["plan"] = plan_preferido_id
            return [{"monto_cop": 3000, "fuente": "procuraduria", "plan_nombre": "AVANZADO",
                     "precio_unitario_cop": 3000}]

        with patch.object(se, "db", self._fake_db(reembolso=True, planes_consumo=[plan_original])), \
             patch.object(cobro, "sincronizar_fuentes_planes", side_effect=lambda emp, *a, **k: emp), \
             patch.object(cobro, "reservar_consumos", side_effect=reservar), \
             patch.object(se, "registrar_evento"):
            consumos = se._consumos_del_completado(
                doc, {"_id": EMPRESA_A, "nombre": "E", "planes": []},
                actor_consultador(EMPRESA_A), actor_consultador(EMPRESA_A),
            )
        self.assertEqual(len(consumos), 1)
        # Se reserva para las fuentes que la original cobró (sin DESHABILITADA).
        self.assertEqual(sorted(llamada["fuentes"]), ["manifiestos_rndc", "procuraduria"])
        # Plan preferido = el de los CONSUMO originales (un solo plan).
        self.assertEqual(llamada["plan"], plan_original)

    def test_multi_plan_reserva_por_fifo(self):
        from Funciones import cobro_seguridad as cobro
        from unittest.mock import patch

        doc = self._doc()
        llamada: dict = {}

        def reservar(empresa, actor, consulta_id, fuentes, plan_preferido_id=None, **kw):
            llamada["plan"] = plan_preferido_id
            return []

        with patch.object(se, "db", self._fake_db(reembolso=True, planes_consumo=[ObjectId(), ObjectId()])), \
             patch.object(cobro, "sincronizar_fuentes_planes", side_effect=lambda emp, *a, **k: emp), \
             patch.object(cobro, "reservar_consumos", side_effect=reservar), \
             patch.object(se, "registrar_evento"):
            se._consumos_del_completado(
                doc, {"_id": EMPRESA_A, "nombre": "E", "planes": []},
                actor_consultador(EMPRESA_A), actor_consultador(EMPRESA_A),
            )
        self.assertIsNone(llamada["plan"])  # multi-plan: FIFO como en la creación


class TestDetectarNombres(unittest.TestCase):
    """Cascada de nombres (2026-09-25, decisión del usuario): cuando el plan
    necesita nombres (captcha PGN / rama_judicial) y ni el body ni la memoria
    los tienen, se detectan solos: situacion_militar (~1-2 s) → sisconmp
    (~11 s) → (None, None) y el endpoint decide (422 como última barrera)."""

    ACTOR = {"usuario": "U", "perfil": "SEGURIDAD", "empresa_id": "e", "usuario_id": "u"}

    def _correr(self, respuestas: dict):
        import asyncio
        from unittest.mock import patch

        from Funciones import orquestador_estudios as orch

        async def _fuente(nombre, cedula, actor, forzar, **kw):
            if nombre in respuestas:
                return respuestas[nombre]
            raise AssertionError(f"fuente inesperada en la cascada: {nombre}")

        with patch.object(orch, "_ejecutar_fuente", side_effect=_fuente):
            return asyncio.run(orch.detectar_nombres("1010213062", self.ACTOR))

    def test_situacion_militar_responde_y_no_consulta_sisconmp(self):
        nombres, apellidos = self._correr({
            "situacion_militar": {"estado": "EXITO", "nombres": "EDWIN MISAEL", "apellidos": "ZARATE PEÑA"},
            "sisconmp": AssertionError and {},
        })
        self.assertEqual((nombres, apellidos), ("EDWIN MISAEL", "ZARATE PEÑA"))

    def test_sin_librete_cae_a_sisconmp(self):
        # Mujer/extranjero sin registro de libreta: situacion_militar no
        # entrega nombres → el fallback sisconmp resuelve.
        nombres, apellidos = self._correr({
            "situacion_militar": {"estado": "EXITO", "no_registra": True, "nombres": "", "apellidos": ""},
            "sisconmp": {"estado": "EXITO", "nombres": "MARIA", "apellidos": "PEREZ GOMEZ"},
        })
        self.assertEqual((nombres, apellidos), ("MARIA", "PEREZ GOMEZ"))

    def test_todo_falla_retorna_none(self):
        nombres, apellidos = self._correr({
            "situacion_militar": {"estado": "NO_DISPONIBLE", "nombres": "", "apellidos": ""},
            "sisconmp": {"estado": "NO_DISPONIBLE", "nombres": "", "apellidos": ""},
        })
        self.assertEqual((nombres, apellidos), (None, None))


class TestCambioYRecuperacionClave(unittest.TestCase):
    """(2026-09-25) Autogestión de clave del portal: cambio autenticado
    (menú del avatar) y recuperación por código de 6 dígitos por correo.
    Los usuarios viven en `baseusuarios` — los endpoints de aut2 apuntan a
    `usuarios` (Torre de Control) y NO les sirven a los clientes del portal."""

    def _usuario(self, clave="secreta123"):
        return {"_id": ObjectId(), "email": "mgomez@glamperos.com", "usuario": "mgomez",
                "clave": clave, "activo": True}

    def test_cambiar_clave_verifica_actual_y_hashea_nueva(self):
        from Funciones.claves import crear_hash

        usuario = self._usuario(clave=crear_hash("vieja123"))
        actualizado: dict = {}

        def _find_one(q, *a, **k):
            if q.get("_id") == usuario["_id"]:
                return usuario
            return None

        def _update_one(q, u):
            actualizado.update(u.get("$set", {}))
            return None

        with patch.object(se.col_usuarios, "find_one", side_effect=_find_one), \
             patch.object(se.col_usuarios, "update_one", side_effect=_update_one), \
             patch.object(se, "registrar_evento"):
            resp = se.cambiar_clave_estudios(
                datos=se.CambiarClaveIn(clave_actual="vieja123", clave_nueva="nueva456"),
                request=None,
                actor={"usuario_id": str(usuario["_id"]), "usuario": "mgomez", "empresa_id": "e"},
            )
        self.assertEqual(resp["mensaje"], "Clave actualizada correctamente")
        from Funciones.claves import verificar_clave
        self.assertTrue(verificar_clave("nueva456", actualizado["clave"]))

    def test_cambiar_clave_rechaza_actual_incorrecta(self):
        usuario = self._usuario(clave="vieja123")  # sin hash = dual-mode plano
        with patch.object(se.col_usuarios, "find_one", return_value=usuario), \
             patch.object(se, "registrar_evento"):
            with self.assertRaises(HTTPException) as ctx:
                se.cambiar_clave_estudios(
                    datos=se.CambiarClaveIn(clave_actual="equivocada", clave_nueva="nueva456"),
                    request=None,
                    actor={"usuario_id": str(usuario["_id"]), "usuario": "mgomez", "empresa_id": "e"},
                )
        self.assertEqual(ctx.exception.status_code, 400)

    def test_cambiar_clave_rechaza_api_key(self):
        with self.assertRaises(HTTPException) as ctx:
            se.cambiar_clave_estudios(
                datos=se.CambiarClaveIn(clave_actual="x", clave_nueva="nueva456"),
                request=None,
                actor={"usuario_id": None, "usuario": "API: SILO", "empresa_id": "e"},
            )
        self.assertEqual(ctx.exception.status_code, 403)

    def test_recuperar_solicitar_neutro_y_guarda_codigo(self):
        from fastapi import BackgroundTasks
        from Funciones.claves import verificar_clave

        usuario = self._usuario()
        guardado: dict = {}

        def _update_one(q, u):
            guardado.update(u.get("$set", {}))
            return None

        with patch.object(se.col_usuarios, "find_one", return_value=usuario), \
             patch.object(se.col_usuarios, "update_one", side_effect=_update_one), \
             patch.object(se, "registrar_evento"), \
             patch.object(se, "_enviar_correo_codigo") as correo:
            bt = BackgroundTasks()
            bt.add_task(lambda: None)  # la real se encola en el router
            resp = se.recuperar_solicitar(
                datos=se.RecuperarSolicitarIn(correo="MGOMEZ@GLAMPEROS.COM"),
                request=None, background_tasks=bt,
            )
        # Respuesta neutra (mismo mensaje exista o no el correo).
        self.assertIn("Si el correo está registrado", resp["mensaje"])
        # Código guardado HASHEADO (nunca en plano) + expiración.
        self.assertIn("reset_codigo_hash", guardado)
        self.assertFalse(guardado["reset_codigo_hash"].isdigit())

    def test_recuperar_confirmar_cambia_clave(self):
        from Funciones.claves import crear_hash, verificar_clave

        codigo_hash = crear_hash("123456")
        desde = datetime(2026, 9, 25, 12, 0, 0)
        expira = datetime(2026, 9, 25, 12, 15, 0)
        usuario = {**self._usuario(), "reset_codigo_hash": codigo_hash,
                   "reset_codigo_exp": expira, "reset_codigo_intentos": 0}
        guardado: dict = {}

        def _update_one(q, u):
            guardado.update(u.get("$set", {}))
            return None

        with patch.object(se, "_utcnow", return_value=desde), \
             patch.object(se.col_usuarios, "find_one", return_value=usuario), \
             patch.object(se.col_usuarios, "update_one", side_effect=_update_one), \
             patch.object(se, "registrar_evento"):
            resp = se.recuperar_confirmar(
                datos=se.RecuperarConfirmarIn(
                    correo="mgomez@glamperos.com", codigo="123456", clave_nueva="nueva789"),
                request=None,
            )
        self.assertIn("restablecida", resp["mensaje"])
        self.assertTrue(verificar_clave("nueva789", guardado["clave"]))
        # El código queda inservible.
        self.assertIsNone(guardado["reset_codigo_hash"])

    def test_recuperar_confirmar_codigo_vencido(self):
        from Funciones.claves import crear_hash

        usuario = {**self._usuario(), "reset_codigo_hash": crear_hash("123456"),
                   "reset_codigo_exp": datetime(2026, 9, 25, 12, 15, 0)}
        with patch.object(se, "_utcnow", return_value=datetime(2026, 9, 25, 13, 0, 0)), \
             patch.object(se.col_usuarios, "find_one", return_value=usuario), \
             patch.object(se.col_usuarios, "update_one") as upd, \
             patch.object(se, "registrar_evento"):
            with self.assertRaises(HTTPException) as ctx:
                se.recuperar_confirmar(
                    datos=se.RecuperarConfirmarIn(
                        correo="mgomez@glamperos.com", codigo="123456", clave_nueva="nueva789"),
                    request=None,
                )
        self.assertEqual(ctx.exception.status_code, 400)
