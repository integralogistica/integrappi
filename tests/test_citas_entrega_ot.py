"""Tests del módulo «Citas de Entrega» (2026-11-09): la cita MANUAL de
/CitasEntrega (`citas_entrega`) GANA sobre `citas_agendadas` en el veredicto
OT de _calcular_ot_filas; borrada la manual, vuelve a agendadas. Además
valida el CRUD del router (upsert, franjas, Excel todo-o-nada, duplicados) y
la invalidación de los DOS caches (GUIAS + TABLAS)."""
import io
import sys
import types
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

if "bd.bd_cliente" not in sys.modules:
    _stub = types.ModuleType("bd.bd_cliente")

    class _BDFake(dict):
        def __getitem__(self, clave):
            return self.setdefault(clave, MagicMock())

    _stub.bd_cliente = _BDFake()
    sys.modules["bd.bd_cliente"] = _stub

from fastapi import FastAPI
from fastapi.testclient import TestClient
from openpyxl import Workbook

from rutas import citas_entrega, indicadores_cliente
from tests.test_alta_seguridad import FakeColeccion, _match_documento


def cliente_de_prueba(router) -> TestClient:
    app = FastAPI()
    app.include_router(router)
    return TestClient(app, raise_server_exceptions=False)


class ColCitasFake(FakeColeccion):
    """FakeColeccion + lo que usa citas_entrega: upsert con $setOnInsert,
    $push (historial), delete_one, bulk_write(UpdateOne) e insert_many.
    ⚠️ NO emula `$each` dentro de `$set` (Mongo lo guardaría como dict
    literal y el historial dejaría de ser lista — bug corregido 2026-11-09)."""

    def update_one(self, filtro, cambio, upsert=False):
        doc = self.find_one(filtro)
        if doc is None:
            if not upsert:
                return MagicMock(matched_count=0)
            doc = {}
            self.documents.append(doc)
        if "_id" not in doc and "$setOnInsert" in cambio:
            doc.update(cambio["$setOnInsert"])
        for k, v in (cambio.get("$set") or {}).items():
            doc[k] = v
        for k, v in (cambio.get("$push") or {}).items():
            items = v["$each"] if isinstance(v, dict) and "$each" in v else [v]
            doc.setdefault(k, [])
            doc[k].extend(items if isinstance(items, list) else [items])
            if isinstance(v, dict) and isinstance(v.get("$slice"), int) and v["$slice"] < 0:
                del doc[k][:v["$slice"]]
        return MagicMock(matched_count=1)

    def delete_one(self, filtro):
        for i, d in enumerate(self.documents):
            if _match_documento(d, filtro):
                del self.documents[i]
                return MagicMock(deleted_count=1)
        return MagicMock(deleted_count=0)

    def bulk_write(self, ops, ordered=True):
        for op in ops:
            self.update_one(op._filter, op._doc, upsert=op._upsert)
        return MagicMock()

    def insert_many(self, docs, ordered=True):
        for d in docs:
            self.insert_one(d)
        return MagicMock()

    def create_index(self, *a, **k):
        pass


_INFO = {"usuario": "TADMIN", "nombre": "Admin De Prueba", "perfil": "ADMIN"}


def _fila_ot(**extra) -> dict:
    """Fila CARGA de Bogotá entregada el 2026-11-10, emitida el 2026-11-01
    (domingo). Con tiempos 'BOGOTA DC': 1 día → promesa evaluable, así el
    veredicto llega a la comparación de la CITA."""
    fila = {
        "guia": "999123",
        "nombre_cliente": "FRESENIUS KABI",
        "destinatario": "CLINICA X",
        "ciudad_destino": "BOGOTA DC - BOGOTA DC",
        "ciudad_origen": "",
        "estado": "ENTREGADA",
        "fecha_emision": "2026-11-01",
        "fecha_entrega": "2026-11-10",
    }
    fila.update(extra)
    return fila


# ── Precedencia de la cita manual en el veredicto OT ─────────────────────────


class PrecedenciaCitaOTTests(unittest.TestCase):
    """Parchea los mapas de tablas (nada de Mongo) y llama _calcular_ot_filas."""

    def _calcular(self, agendadas: dict, manuales: dict) -> dict:
        fila = _fila_ot()
        with patch.object(indicadores_cliente, "_mapa_citas_agendadas", lambda: agendadas), \
             patch.object(indicadores_cliente, "_mapa_citas_entrega", lambda: manuales), \
             patch.object(indicadores_cliente, "_mapa_novedades_siscore", lambda: {}), \
             patch.object(indicadores_cliente, "_mapa_tiempos_carga",
                          lambda: {"par": {}, "mun": {"BOGOTA DC": 1}}), \
             patch.object(indicadores_cliente, "_mapa_tiempos_st", lambda: {"par": {}, "mun": {}}), \
             patch.object(indicadores_cliente, "_ciudad_y_dpto",
                          lambda f, usar_origen: ("BOGOTA DC", "CUNDINAMARCA")):
            indicadores_cliente._calcular_ot_filas([fila])
        self.assertIsNotNone(fila["fecha_promesa"])  # el veredicto sí llegó a la CITA
        return fila

    def test_cita_manual_gana_sobre_agendadas(self):
        """Agendada 2026-11-01 (vencida) + manual 2026-11-15 → cumple con la manual."""
        fila = self._calcular(agendadas={"999123": "2026-11-01"}, manuales={"999123": "2026-11-15"})
        self.assertEqual(fila["ot"], 1)
        self.assertEqual(fila["origen_promesa"], "CITA")
        self.assertEqual(fila["cita_entrega"], "2026-11-15")
        self.assertEqual(fila["fecha_cita_usada"], "2026-11-15")  # la cita del veredicto

    def test_sin_manual_cae_a_citas_agendadas(self):
        """Sin manual, el veredicto es el de siempre (agendada vencida → no cumple)."""
        fila = self._calcular(agendadas={"999123": "2026-11-01"}, manuales={})
        self.assertEqual(fila["ot"], 0)
        self.assertEqual(fila["origen_promesa"], "CITA")
        self.assertIsNone(fila["cita_entrega"])
        self.assertEqual(fila["fecha_cita_usada"], "2026-11-01")  # la agendada

    def test_manual_con_fecha_basura_cae_a_agendadas(self):
        """Fecha manual no evaluable (None-safe) → degrada a citas_agendadas."""
        fila = self._calcular(agendadas={"999123": "2026-11-01"}, manuales={"999123": "ELIMINADA"})
        self.assertEqual(fila["ot"], 0)
        self.assertIsNone(fila["cita_entrega"])


# ── Router: registro manual, validaciones y caches ───────────────────────────


class GuardarCitaTests(unittest.TestCase):

    def test_guardar_crea_actualiza_y_limpia_caches(self):
        fake, fake_aud = ColCitasFake(), ColCitasFake()
        cliente = cliente_de_prueba(citas_entrega.router)
        ic = indicadores_cliente
        ic._CACHE_TABLAS["citas_entrega"] = (1.0, {"vieja": "2026-01-01"})
        ic._CACHE_GUIAS[("x",)] = {"payload": 1}
        try:
            with patch.object(citas_entrega, "_resolver_usuario", lambda u: dict(_INFO)), \
                 patch.object(citas_entrega, "col_citas", fake), \
                 patch.object(citas_entrega, "col_auditoria", fake_aud):
                r1 = cliente.post("/citas-entrega/guardar", params={"usuario": "TADMIN"},
                                  json={"guia": "999123", "fecha": "2026-11-15",
                                        "hora_inicio": "08:00", "hora_fin": "12:00"})
                r2 = cliente.post("/citas-entrega/guardar", params={"usuario": "TADMIN"},
                                  json={"guia": "999123", "fecha": "2026-11-16",
                                        "hora_inicio": "08:00", "hora_fin": "12:00"})
        finally:
            ic._CACHE_TABLAS.pop("citas_entrega", None)
            ic._CACHE_GUIAS.clear()
        self.assertEqual(r1.status_code, 200)
        self.assertEqual(r1.json()["accion"], "creada")
        self.assertEqual(r2.json()["accion"], "actualizada")
        self.assertEqual(len(fake.documents), 1)  # UNA por guía
        doc = fake.documents[0]
        self.assertEqual(doc["fecha"], "2026-11-16")
        self.assertEqual(doc["creado_por"]["usuario"], "TADMIN")  # $setOnInsert
        # El historial debe ser LISTA (regresión 2026-11-09: $each dentro de
        # $set guardaba un dict literal y el formulario no mostraba nada).
        self.assertIsInstance(doc["historial"], list)
        # Cada entrada es el CAMBIO completo: previo → nuevo, quién y cuándo.
        self.assertEqual(doc["historial"][0]["previo"]["fecha"], "2026-11-15")
        self.assertEqual(doc["historial"][0]["nuevo"]["fecha"], "2026-11-16")
        # Invalidación de los DOS caches tras escribir.
        self.assertNotIn(("x",), ic._CACHE_GUIAS)

    def test_franja_invertida_y_hora_invalida_rechazan(self):
        fake = ColCitasFake()
        cliente = cliente_de_prueba(citas_entrega.router)
        with patch.object(citas_entrega, "_resolver_usuario", lambda u: dict(_INFO)), \
             patch.object(citas_entrega, "col_citas", fake), \
             patch.object(citas_entrega, "col_auditoria", ColCitasFake()):
            r_inv = cliente.post("/citas-entrega/guardar", params={"usuario": "TADMIN"},
                                 json={"guia": "999123", "fecha": "2026-11-15",
                                       "hora_inicio": "14:00", "hora_fin": "08:00"})
            r_hora = cliente.post("/citas-entrega/guardar", params={"usuario": "TADMIN"},
                                  json={"guia": "999123", "fecha": "2026-11-15",
                                        "hora_inicio": "25:00", "hora_fin": "08:00"})
            r_fecha = cliente.post("/citas-entrega/guardar", params={"usuario": "TADMIN"},
                                   json={"guia": "999123", "fecha": "15/11/2026",
                                         "hora_inicio": "08:00", "hora_fin": "12:00"})
            r_basura = cliente.post("/citas-entrega/guardar", params={"usuario": "TADMIN"},
                                    json={"guia": "999123", "fecha": "pronto",
                                          "hora_inicio": "08:00", "hora_fin": "12:00"})
        self.assertEqual(r_inv.status_code, 422)
        self.assertEqual(r_hora.status_code, 422)
        self.assertEqual(r_basura.status_code, 422)
        self.assertEqual(r_fecha.status_code, 200)  # D/M/YYYY también se normaliza
        self.assertEqual(fake.documents[0]["fecha"], "2026-11-15")
        self.assertEqual(len(fake.documents), 1)

    def test_eliminar_mueve_a_citas_eliminadas_con_trazabilidad(self):
        """Al eliminar, el doc COMPLETO pasa a `citas_eliminadas` con quién y
        cuándo lo borró (auditoría) y sale de `citas_entrega` (el OT vuelve a
        citas_agendadas)."""
        from bson import ObjectId
        oid = str(ObjectId())
        fake = ColCitasFake([{"_id": oid, "guia": "999123", "fecha": "2026-11-15",
                              "hora_inicio": "08:00", "hora_fin": "12:00",
                              "historial": [{"previo": {"fecha": "2026-11-10"},
                                             "nuevo": {"fecha": "2026-11-15"}}]}])
        fake_elim = ColCitasFake()
        cliente = cliente_de_prueba(citas_entrega.router)
        with patch.object(citas_entrega, "_resolver_usuario", lambda u: dict(_INFO)), \
             patch.object(citas_entrega, "col_citas", fake), \
             patch.object(citas_entrega, "col_eliminadas", fake_elim), \
             patch.object(citas_entrega, "col_auditoria", ColCitasFake()):
            r = cliente.delete("/citas-entrega/eliminar", params={"usuario": "TADMIN", "id": oid})
            r_listo = cliente.get("/citas-entrega/eliminadas", params={"usuario": "TADMIN"})
        self.assertEqual(r.status_code, 200)
        self.assertEqual(fake.documents, [])          # salió de citas_entrega
        self.assertEqual(len(fake_elim.documents), 1)  # quedó en citas_eliminadas
        elim = fake_elim.documents[0]
        self.assertEqual(elim["guia"], "999123")
        self.assertEqual(elim["fecha"], "2026-11-15")
        self.assertEqual(len(elim["historial"]), 1)     # conservó su historial
        self.assertEqual(elim["eliminada_por"]["usuario"], "TADMIN")
        self.assertIn("eliminada_el", elim)
        # El listado de eliminadas (auditoría) la devuelve.
        self.assertEqual(r_listo.status_code, 200)
        self.assertEqual(r_listo.json()["total"], 1)
        self.assertEqual(r_listo.json()["eliminadas"][0]["guia"], "999123")

    def test_eliminadas_solo_admin_y_control(self):
        cliente = cliente_de_prueba(citas_entrega.router)
        info_operativo = {**_INFO, "perfil": "OPERATIVO"}
        with patch.object(citas_entrega, "_resolver_usuario", lambda u: info_operativo):
            r = cliente.get("/citas-entrega/eliminadas", params={"usuario": "TOPER"})
        self.assertEqual(r.status_code, 403)

    def test_guardar_autorepara_historial_corrupto(self):
        """Doc editado con el bug del `$each` en `$set` (historial = dict
        literal): al volver a editar, el guardado lo convierte en lista con
        las entradas recuperadas + la nueva (antes: 500 de Mongo, "must be
        an array")."""
        corrupto = {"$each": [{"fecha": "2026-11-01", "hora_inicio": "08:00",
                               "hora_fin": "12:00"}], "$slice": -20}
        fake = ColCitasFake([{"_id": "id-9", "guia": "999123", "fecha": "2026-11-10",
                              "hora_inicio": "14:00", "hora_fin": "20:00",
                              "historial": corrupto}])
        cliente = cliente_de_prueba(citas_entrega.router)
        with patch.object(citas_entrega, "_resolver_usuario", lambda u: dict(_INFO)), \
             patch.object(citas_entrega, "col_citas", fake), \
             patch.object(citas_entrega, "col_auditoria", ColCitasFake()):
            r = cliente.post("/citas-entrega/guardar", params={"usuario": "TADMIN"},
                             json={"guia": "999123", "fecha": "2026-11-12",
                                   "hora_inicio": "08:00", "hora_fin": "12:00"})
        self.assertEqual(r.status_code, 200)
        doc = fake.documents[0]
        self.assertIsInstance(doc["historial"], list)
        self.assertEqual(len(doc["historial"]), 2)  # recuperada + nueva
        self.assertEqual(doc["historial"][0]["fecha"], "2026-11-01")            # recuperada del dict (plana)
        self.assertEqual(doc["historial"][-1]["previo"]["fecha"], "2026-11-10")  # el previo real del cambio
        self.assertEqual(doc["historial"][-1]["nuevo"]["fecha"], "2026-11-12")   # el cambio hecho
        self.assertEqual(doc["fecha"], "2026-11-12")                            # la nueva vigente

    def test_perfil_sin_permiso_rechazado(self):
        cliente = cliente_de_prueba(citas_entrega.router)
        info_analista = {**_INFO, "perfil": "ANALISTA"}
        with patch.object(citas_entrega, "_resolver_usuario", lambda u: info_analista):
            r = cliente.get("/citas-entrega/", params={"usuario": "TANALISTA"})
        self.assertEqual(r.status_code, 403)


# ── Excel: plantilla de carga todo-o-nada ────────────────────────────────────


def _excel(filas: list) -> bytes:
    return _excel_encabezado(["GUIA", "FECHA", "HORA INICIO", "HORA FIN"], filas)


def _excel_encabezado(cabeceras: list, filas: list) -> bytes:
    wb = Workbook()
    ws = wb.active
    ws.append(cabeceras)
    for f in filas:
        ws.append(f)
    salida = io.BytesIO()
    wb.save(salida)
    return salida.getvalue()


class ImportarExcelTests(unittest.TestCase):

    def _subir(self, contenido: bytes):
        fake, fake_aud = ColCitasFake(), ColCitasFake()
        cliente = cliente_de_prueba(citas_entrega.router)
        with patch.object(citas_entrega, "_resolver_usuario", lambda u: dict(_INFO)), \
             patch.object(citas_entrega, "col_citas", fake), \
             patch.object(citas_entrega, "col_auditoria", fake_aud):
            r = cliente.post(
                "/citas-entrega/importar-excel", params={"usuario": "TADMIN"},
                files={"archivo": ("cargas.xlsx", contenido,
                                   "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            )
        return r, fake

    def test_fecha_dmy_y_celda_datetime_se_normalizan(self):
        """'9/11/2026' y celda datetime → ambas ISO '2026-11-09'; '8:00' → '08:00'."""
        r, fake = self._subir(_excel([
            ["AAA-001", "9/11/2026", "8:00", "12:00"],
            ["BBB-002", datetime(2026, 11, 9, 0, 0), "13:00", "17:00"],
        ]))
        self.assertEqual(r.status_code, 200)
        self.assertEqual(len(fake.documents), 2)
        por_guia = {d["guia"]: d for d in fake.documents}
        self.assertEqual(por_guia["AAA-001"]["fecha"], "2026-11-09")
        self.assertEqual(por_guia["AAA-001"]["hora_inicio"], "08:00")
        self.assertEqual(por_guia["BBB-002"]["fecha"], "2026-11-09")

    def test_plantilla_oficial_cabecera_ddmmaaaa(self):
        """La plantilla pide FECHA (DD/MM/AAAA): esa cabecera y fechas planas
        dd/mm/aaaa deben subir sin fricción."""
        r, fake = self._subir(_excel_encabezado(
            ["GUIA", "FECHA (DD/MM/AAAA)", "HORA INICIO", "HORA FIN"],
            [["AAA-001", "10/11/2026", "08:00", "12:00"]]))
        self.assertEqual(r.status_code, 200)
        self.assertEqual(fake.documents[0]["fecha"], "2026-11-10")

    def test_franja_invertida_rechaza_toda_la_carga(self):
        r, fake = self._subir(_excel([
            ["AAA-001", "2026-11-09", "08:00", "12:00"],
            ["BBB-002", "2026-11-09", "15:00", "08:00"],  # inválida
        ]))
        self.assertEqual(r.status_code, 422)
        detail = r.json()["detail"]
        self.assertIn("errores", detail)
        self.assertEqual(len(detail["errores"]), 1)
        self.assertEqual(detail["errores"][0]["guia"], "BBB-002")
        self.assertEqual(fake.documents, [])  # TODO-O-NADA: nada escrito

    def test_reagenda_en_el_mismo_archivo_la_ultima_gana(self):
        """La guía puede venir DOS veces (tenía 8-nov, no se cumplió, nueva
        fecha 9-nov): la ÚLTIMA queda vigente y la anterior en historial."""
        r, fake = self._subir(_excel([
            ["AAA-001", "2026-11-08", "08:00", "12:00"],
            ["AAA-001", "2026-11-09", "08:00", "12:00"],
        ]))
        self.assertEqual(r.status_code, 200)
        acciones = {p["guia"]: p["accion"] for p in r.json()["procesadas"]}
        self.assertEqual(len(fake.documents), 1)
        doc = fake.documents[0]
        self.assertEqual(doc["fecha"], "2026-11-09")  # la última es la vigente
        self.assertEqual(len(doc.get("historial", [])), 1)
        # La entrada registra el cambio COMPLETO: 8-nov → 9-nov.
        self.assertEqual(doc["historial"][0]["previo"]["fecha"], "2026-11-08")
        self.assertEqual(doc["historial"][0]["nuevo"]["fecha"], "2026-11-09")
        self.assertEqual(acciones["AAA-001"], "actualizada")  # 1ª crea, 2ª re-agenda

    def test_guia_duplicada_identica_no_escribe(self):
        """La misma guía dos veces IGUAL → sin_cambio (ni doc extra ni ruido
        en el historial)."""
        r, fake = self._subir(_excel([
            ["AAA-001", "2026-11-09", "08:00", "12:00"],
            ["AAA-001", "2026-11-09", "08:00", "12:00"],
        ]))
        self.assertEqual(r.status_code, 200)
        acciones = {p["guia"]: p["accion"] for p in r.json()["procesadas"]}
        self.assertEqual(len(fake.documents), 1)
        self.assertEqual(acciones["AAA-001"], "sin_cambio")
        self.assertFalse(fake.documents[0].get("historial"))  # sin ruido de re-agenda

    def test_reagenda_sobre_fecha_existente_en_bd(self):
        """Carga 1: 8-nov. Carga 2 (otro archivo): 9-nov → vigente 9-nov y la
        de 8-nov queda en historial (así re-agenda el operativo)."""
        r1, fake = self._subir(_excel([["AAA-001", "2026-11-08", "08:00", "12:00"]]))
        self.assertEqual(r1.status_code, 200)
        contenido2 = _excel([["AAA-001", "2026-11-09", "08:00", "12:00"]])
        cliente = cliente_de_prueba(citas_entrega.router)
        with patch.object(citas_entrega, "_resolver_usuario", lambda u: dict(_INFO)), \
             patch.object(citas_entrega, "col_citas", fake), \
             patch.object(citas_entrega, "col_auditoria", ColCitasFake()):
            r2 = cliente.post(
                "/citas-entrega/importar-excel", params={"usuario": "TADMIN"},
                files={"archivo": ("cargas2.xlsx", contenido2,
                                   "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")},
            )
        self.assertEqual(r2.status_code, 200)
        doc = fake.documents[0]
        self.assertEqual(doc["fecha"], "2026-11-09")
        self.assertEqual(doc["historial"][0]["previo"]["fecha"], "2026-11-08")
        self.assertEqual(doc["historial"][0]["nuevo"]["fecha"], "2026-11-09")


if __name__ == "__main__":
    unittest.main()
