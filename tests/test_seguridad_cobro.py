"""Tests del motor de cobro postpago (Funciones/cobro_seguridad.py).

Sin red ni Mongo real: colecciones falsas que respetan las condiciones de los
updates (incluida la atomicidad de find_one_and_update con $gt: 0) y los
índices únicos parciales (DuplicateKeyError en REEMBOLSO repetido).

Ejecutar:  python -m unittest tests.test_seguridad_cobro -v
"""
import asyncio
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from bson import ObjectId
from fastapi import HTTPException
from pymongo.errors import DuplicateKeyError

from Funciones import cobro_seguridad as cobro

EMPRESA_ID = ObjectId()
PLAN_ID = ObjectId()
ACTOR = {
    "usuario": "MGOMEZ", "usuario_nombre": "MARIA GOMEZ", "usuario_id": "x",
    "rol": "CONSULTADOR", "empresa_id": str(EMPRESA_ID),
}
ACTOR_ADMIN = {**ACTOR, "usuario": "EZARATE", "rol": "ADMIN_INTEGRA"}


def empresa_doc(cupo_autorizado=3, cupo_disponible=None, plan_id=PLAN_ID, vigencia=None):
    plan = {
        "plan_id": plan_id,
        "plan_nombre": "ESTÁNDAR",
        "cupo_autorizado": cupo_autorizado,
        "cupo_disponible": cupo_autorizado if cupo_disponible is None else cupo_disponible,
        "cupo_consumido": cupo_autorizado - (cupo_autorizado if cupo_disponible is None else cupo_disponible),
        "asignado_en": datetime(2026, 1, 1),
    }
    return {"_id": EMPRESA_ID, "nombre": "EMPRESA A", "plan": plan}


def empresa_planes_doc(entradas):
    """Empresa con el array multi-plan (una entrada por fuente)."""
    return {"_id": EMPRESA_ID, "nombre": "EMPRESA A", "planes": entradas}


def entrada_plan(fuente, plan=None, cupo_autorizado=3, cupo_disponible=None, cupo_consumido=0, asignado_en=None):
    """Entrada del array `planes` para una fuente."""
    return {
        "plan_id": plan["_id"] if plan else PLAN_ID,
        "plan_nombre": (plan or {}).get("nombre", "ESTÁNDAR"),
        "fuente": fuente,
        "cupo_autorizado": cupo_autorizado,
        "cupo_disponible": cupo_autorizado if cupo_disponible is None else cupo_disponible,
        "cupo_consumido": cupo_consumido,
        "asignado_en": asignado_en or datetime(2026, 1, 1),
    }


def plan_doc(precio=3500, activo=True, vigencia_dias=None, nombre="ESTÁNDAR", plan_id=None):
    return {
        "_id": plan_id or PLAN_ID, "nombre": nombre, "precio_por_estudio": precio,
        "fuentes_incluidas": ["manifiestos_rndc", "procuraduria"],
        "vigencia_dias": vigencia_dias, "activo": activo,
    }


class ColFake:
    """Colección Mongo mínima con updates condicionados y unique parcial."""

    def __init__(self, documentos=None, unique_parcial=None, unique_compuesto=None):
        self.documents = list(documentos or [])
        self.unique_parcial = unique_parcial or {}  # campo -> tipos afectados
        # Índice único parcial compuesto: ([campos], tipos) — p.ej.
        # (["consulta_id", "consumo_id"], {"REEMBOLSO"}).
        self.unique_compuesto = unique_compuesto

    # -- lectura ------------------------------------------------------------
    def find(self, query=None, *a, **k):
        return FakeCursor([d for d in self.documents if self._match(d, query or {})])

    def find_one(self, query=None, *a, **k):
        for d in self.documents:
            if self._match(d, query or {}):
                return d
        return None

    def find_one_and_update(self, query, update, return_document=False):
        for d in self.documents:
            if self._match(d, query):
                self._aplicar(d, update)
                return d
        return None  # condición no matcheó (atomicidad del cupo)

    # -- escritura ----------------------------------------------------------
    def insert_one(self, doc):
        self._validar_unicos(doc)
        doc.setdefault("_id", ObjectId())
        self.documents.append(doc)

        class R:
            inserted_id = doc["_id"]
        return R()

    def update_one(self, query, update):
        n = 0
        for d in self.documents:
            if self._match(d, query):
                self._aplicar(d, update)
                n += 1

        class R:
            modified_count = n
        return R()

    def update_many(self, query, update):
        return self.update_one(query, update)

    def delete_one(self, query):
        antes = len(self.documents)
        self.documents = [d for d in self.documents if not self._match(d, query)]

        class R:
            deleted_count = antes - len(self.documents)
        return R()

    # -- agregación usada (find simple) ---------------------------------------
    def aggregate(self, pipeline):
        return FakeCursor(list(self.documents))

    # -- helpers -------------------------------------------------------------
    def _match(self, doc, query):
        for k, v in query.items():
            valor = self._obtener(doc, k)
            if isinstance(v, dict):
                for op, esperado in v.items():
                    if op == "$gt" and not (valor is not None and valor > esperado):
                        return False
                    if op == "$ne" and valor == esperado:
                        return False
                    if op == "$in" and valor not in esperado:
                        return False
                    if op == "$lte" and not (valor is not None and valor <= esperado):
                        return False
                    if op == "$gte" and not (valor is not None and valor >= esperado):
                        return False
            elif valor != v:
                return False
        return True

    @staticmethod
    def _obtener(doc, camino):
        """Soporta rutas con punto de N niveles: 'plan.x' y 'planes.0.x'."""
        actual = doc
        for parte in camino.split("."):
            if isinstance(actual, list):
                try:
                    actual = actual[int(parte)]
                except (ValueError, IndexError):
                    return None
            elif isinstance(actual, dict):
                actual = actual.get(parte)
            else:
                return None
        return actual

    def _aplicar(self, doc, update):
        def _bajar(contenedor, partes):
            """Baja la ruta hasta el padre directo del último segmento.
            Soporta dicts y listas (índices numéricos)."""
            for parte in partes[:-1]:
                if isinstance(contenedor, list):
                    contenedor = contenedor[int(parte)]
                else:
                    contenedor = contenedor.setdefault(parte, {})
            return contenedor

        if "$set" in update:
            for k, v in update["$set"].items():
                partes = k.split(".")
                if len(partes) == 1:
                    doc[k] = v
                else:
                    padre = _bajar(doc, partes)
                    hijo = partes[-1]
                    if isinstance(padre, list):
                        padre[int(hijo)] = v
                    else:
                        padre[hijo] = v
        if "$inc" in update:
            for k, v in update["$inc"].items():
                partes = k.split(".")
                if len(partes) == 1:
                    doc[k] = doc.get(k, 0) + v
                else:
                    padre = _bajar(doc, partes)
                    hijo = partes[-1]
                    padre[hijo] = padre.get(hijo, 0) + v

    def _validar_unicos(self, doc):
        for campo, tipos in self.unique_parcial.items():
            if doc.get("tipo") in tipos:
                for d in self.documents:
                    if d.get(campo) == doc.get(campo) and d.get("tipo") in tipos:
                        raise DuplicateKeyError(f"duplicado {campo} para tipo {doc.get('tipo')}")
        if self.unique_compuesto:
            campos, tipos = self.unique_compuesto
            if doc.get("tipo") in tipos:
                for d in self.documents:
                    if d.get("tipo") in tipos and all(d.get(c) == doc.get(c) for c in campos):
                        raise DuplicateKeyError(f"duplicado {campos} para tipo {doc.get('tipo')}")


class FakeCursor:
    def __init__(self, items):
        self.items = items

    def sort(self, *a, **k):
        return self

    def limit(self, n):
        return self

    def skip(self, n):
        return self

    def __iter__(self):
        return iter(self.items)


def _montar(empresa=None, plan=None, movimientos=None):
    col_emp = ColFake([empresa or empresa_doc()])
    col_pla = ColFake([plan or plan_doc()])
    col_mov = ColFake(
        movimientos or [],
        unique_compuesto=(["consulta_id", "consumo_id"], {"REEMBOLSO"}),
    )
    return col_emp, col_pla, col_mov


class TestPeriodoColombia(unittest.TestCase):
    def test_cruce_de_medianoche_utc(self):
        # 2026-09-01 02:00 UTC = 2026-08-31 21:00 en Colombia → período 2026-08
        self.assertEqual(cobro.periodo_colombia(datetime(2026, 9, 1, 2, 0)), "2026-08")

    def test_medianoche_colombia(self):
        self.assertEqual(cobro.periodo_colombia(datetime(2026, 9, 1, 5, 0)), "2026-09")


class TestReservarConsumo(unittest.TestCase):
    def test_consumo_atomico_decrementa_cupo(self):
        col_emp, col_pla, col_mov = _montar()
        mov = cobro.reservar_consumo(
            empresa_doc(), ACTOR, "ES-1", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla
        )
        self.assertEqual(mov["tipo"], "CONSUMO")
        self.assertEqual(mov["monto_cop"], 3500)
        self.assertEqual(mov["precio_unitario_cop"], 3500)  # snapshot
        self.assertEqual(col_emp.documents[0]["plan"]["cupo_disponible"], 2)
        self.assertEqual(col_emp.documents[0]["plan"]["cupo_consumido"], 1)

    def test_dos_consumos_con_cupo_1_solo_uno_pasa(self):
        empresa = empresa_doc(cupo_autorizado=1)
        col_emp, col_pla, col_mov = _montar(empresa)
        cobro.reservar_consumo(empresa, ACTOR, "ES-1", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        # La segunda reserva ve cupo 0 en BD → find_one_and_update no matchea.
        with self.assertRaises(HTTPException) as ctx:
            cobro.reservar_consumo(
                empresa_doc(cupo_autorizado=1, cupo_disponible=0), ACTOR, "ES-2",
                col_mov=col_mov, col_emp=col_emp, col_pla=col_pla,
            )
        self.assertEqual(ctx.exception.status_code, 402)
        self.assertIn("agotado", ctx.exception.detail)

    def test_sin_plan_402_y_admin_exento(self):
        empresa_sin = {"_id": EMPRESA_ID, "nombre": "EMPRESA A", "plan": None}
        col_emp, col_pla, col_mov = _montar(empresa_sin)
        with self.assertRaises(HTTPException) as ctx:
            cobro.reservar_consumo(empresa_sin, ACTOR, "ES-1", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        self.assertEqual(ctx.exception.status_code, 402)
        self.assertIn("no tiene un plan", ctx.exception.detail)

        mov = cobro.reservar_consumo(empresa_sin, ACTOR_ADMIN, "ES-1", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        self.assertTrue(mov["exento"])
        self.assertEqual(mov["monto_cop"], 0)

    def test_plan_vencido_402(self):
        empresa = empresa_doc()
        # Asignado 2026-01-01 + 30 días de vigencia = vencido hace rato.
        plan = plan_doc(vigencia_dias=30)
        col_emp, col_pla, col_mov = _montar(empresa, plan)
        with self.assertRaises(HTTPException) as ctx:
            cobro.reservar_consumo(empresa, ACTOR, "ES-1", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        self.assertEqual(ctx.exception.status_code, 402)
        self.assertIn("venció", ctx.exception.detail)

    def test_cambio_de_precio_no_toca_consumos_viejos(self):
        col_emp, col_pla, col_mov = _montar()
        cobro.reservar_consumo(empresa_doc(), ACTOR, "ES-1", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        col_pla.documents[0]["precio_por_estudio"] = 4000
        cobro.reservar_consumo(empresa_doc(cupo_disponible=2), ACTOR, "ES-2", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        precios = sorted(m["precio_unitario_cop"] for m in col_mov.find({"tipo": "CONSUMO"}))
        self.assertEqual(precios, [3500, 4000])


class TestPlanIlimitado(unittest.TestCase):
    """Plan sin tope (cupo_autorizado None): pospago puro — nunca bloquea."""

    def _empresa(self):
        return {
            "_id": EMPRESA_ID,
            "nombre": "EMPRESA A",
            "plan": {
                "plan_id": PLAN_ID, "plan_nombre": "ESTÁNDAR",
                "cupo_autorizado": None, "cupo_disponible": None, "cupo_consumido": 0,
                "asignado_en": datetime(2026, 1, 1),
            },
        }

    def test_consumo_sin_tope_nunca_bloquea(self):
        empresa = self._empresa()
        col_emp, col_pla, col_mov = _montar(empresa)
        for i in range(5):  # más allá de cualquier cupo: todas pasan
            mov = cobro.reservar_consumo(empresa, ACTOR, f"ES-{i}", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
            self.assertEqual(mov["monto_cop"], 3500)
        plan = col_emp.documents[0]["plan"]
        self.assertEqual(plan["cupo_consumido"], 5)
        self.assertIsNone(plan["cupo_disponible"])  # nunca se toca

    def test_reembolso_ilimitado_solo_revierte_contador(self):
        empresa = self._empresa()
        col_emp, col_pla, col_mov = _montar(empresa)
        consumo = cobro.reservar_consumo(empresa, ACTOR, "ES-9", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        mov = cobro.reembolsar_consumo(empresa, ACTOR_ADMIN, consumo, "ERROR", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        self.assertEqual(mov["monto_cop"], -3500)
        plan = col_emp.documents[0]["plan"]
        self.assertEqual(plan["cupo_consumido"], 0)      # contador revertido
        self.assertIsNone(plan["cupo_disponible"])        # sin cupo que devolver


class TestReembolso(unittest.TestCase):
    def _consumo(self, col_mov, empresa, consulta_id="ES-9"):
        return cobro.reservar_consumo(empresa, ACTOR, consulta_id, col_mov=col_mov, col_emp=ColFake([empresa]), col_pla=ColFake([plan_doc()]))

    def test_reembolso_devuelve_cupo_y_cop(self):
        empresa = empresa_doc()
        col_emp, col_pla, col_mov = _montar(empresa)
        consumo = cobro.reservar_consumo(empresa, ACTOR, "ES-9", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        mov = cobro.reembolsar_consumo(empresa, ACTOR_ADMIN, consumo, "estudio en ERROR", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        self.assertEqual(mov["tipo"], "REEMBOLSO")
        self.assertEqual(mov["monto_cop"], -3500)
        self.assertEqual(mov["unidades"], -1)
        self.assertEqual(col_emp.documents[0]["plan"]["cupo_disponible"], 3)  # devuelto
        reembolsados = [m for m in col_mov.documents if m["tipo"] == "REEMBOLSO"]
        self.assertEqual(len(reembolsados), 1)

    def test_reembolso_doble_bloqueado(self):
        empresa = empresa_doc()
        col_emp, col_pla, col_mov = _montar(empresa)
        consumo = cobro.reservar_consumo(empresa, ACTOR, "ES-9", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        primero = cobro.reembolsar_consumo(empresa, ACTOR_ADMIN, consumo, "ERROR", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        self.assertIsNotNone(primero)
        # Segundo intento: el flag reembolsado ya está → None sin movimiento.
        segundo = cobro.reembolsar_consumo(empresa, ACTOR_ADMIN, consumo, "ERROR", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        self.assertIsNone(segundo)
        self.assertEqual(len([m for m in col_mov.documents if m["tipo"] == "REEMBOLSO"]), 1)
        self.assertEqual(col_emp.documents[0]["plan"]["cupo_disponible"], 3)

    def test_reembolso_con_cambio_de_plan_solo_monetario(self):
        empresa = empresa_doc()
        col_emp, col_pla, col_mov = _montar(empresa)
        consumo = cobro.reservar_consumo(empresa, ACTOR, "ES-9", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        otro_plan = ObjectId()
        col_emp.documents[0]["plan"]["plan_id"] = otro_plan  # la empresa cambió de plan
        mov = cobro.reembolsar_consumo(empresa, ACTOR_ADMIN, consumo, "ERROR", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        self.assertEqual(mov["monto_cop"], -3500)      # el COP sí se devuelve
        self.assertEqual(col_emp.documents[0]["plan"]["cupo_disponible"], 2)  # el cupo NO (otro plan)

    def test_consumo_exento_no_genera_reembolso(self):
        empresa_sin = {"_id": EMPRESA_ID, "nombre": "EMPRESA A", "plan": None}
        col_emp, col_pla, col_mov = _montar(empresa_sin)
        consumo = cobro.reservar_consumo(empresa_sin, ACTOR_ADMIN, "ES-9", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        mov = cobro.reembolsar_consumo(empresa_sin, ACTOR_ADMIN, consumo, "ERROR", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        self.assertIsNone(mov)
        self.assertEqual(len(col_mov.documents), 1)  # solo el consumo exento


class TestPagosYAjustes(unittest.TestCase):
    def test_pago_reduc_deuda_y_marca_pagada(self):
        empresa = empresa_doc()
        col_emp, col_pla, col_mov = _montar(empresa)
        for i in range(2):
            cobro.reservar_consumo(empresa, ACTOR, f"ES-{i}", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        for m in col_mov.documents:
            m["periodo"] = "2026-07"
        col_per = ColFake([{
            "_id": ObjectId(), "empresa_id": EMPRESA_ID, "periodo": "2026-07",
            "estado": "PENDIENTE_COBRO",
            "totales": {"total_cop": 7000},
        }])
        mov = cobro.registrar_pago(empresa, ACTOR_ADMIN, 7000, "2026-09-05", "TRANSFERENCIA", periodo="2026-07", col_mov=col_mov, col_per=col_per)
        self.assertEqual(mov["monto_cop"], -7000)
        self.assertEqual(mov["periodo_pagado"], "2026-07")
        self.assertEqual(col_per.documents[0]["estado"], "PAGADA")

    def test_pago_en_periodo_cerrado_permitido(self):
        """Pagar la cuenta de un mes pasado cerrado es el flujo natural del
        negocio: el pago entra (y salda el cierre si cubre el total)."""
        empresa = empresa_doc()
        col_emp, col_pla, col_mov = _montar(empresa)
        col_per = ColFake([{
            "_id": ObjectId(), "empresa_id": EMPRESA_ID, "periodo": "2026-07",
            "estado": "PENDIENTE_COBRO", "totales": {"total_cop": 1000},
        }])
        mov = cobro.registrar_pago(empresa, ACTOR_ADMIN, 1000, "2026-09-05", "EFECTIVO", periodo="2026-07", col_mov=col_mov, col_per=col_per)
        self.assertEqual(mov["monto_cop"], -1000)
        self.assertEqual(col_per.documents[0]["estado"], "PAGADA")

    def test_ajuste_en_periodo_cerrado_409(self):
        empresa = empresa_doc()
        col_emp, col_pla, col_mov = _montar(empresa)
        col_per = ColFake([{"_id": ObjectId(), "empresa_id": EMPRESA_ID, "periodo": "2026-07", "estado": "PENDIENTE_COBRO"}])
        with self.assertRaises(HTTPException) as ctx:
            cobro.registrar_ajuste(empresa, ACTOR_ADMIN, -500, "descuento", periodo="2026-07", col_mov=col_mov, col_per=col_per)
        self.assertEqual(ctx.exception.status_code, 409)

    def test_ajuste_requiere_motivo_en_router(self):
        # El motor acepta el motivo vacío; el router lo exige. Aquí: flujo normal.
        empresa = empresa_doc()
        col_emp, col_pla, col_mov = _montar(empresa)
        mov = cobro.registrar_ajuste(empresa, ACTOR_ADMIN, -500, "descuento comercial", col_mov=col_mov, col_per=ColFake())
        self.assertEqual(mov["monto_cop"], -500)
        self.assertEqual(mov["motivo"], "descuento comercial")


class TestCierrePeriodo(unittest.TestCase):
    def _movimientos_basicos(self, col_mov, empresa):
        cobro.reservar_consumo(empresa, ACTOR, "ES-A", col_mov=col_mov, col_emp=ColFake([empresa]), col_pla=ColFake([plan_doc(precio=3500)]))
        cobro.reservar_consumo(empresa, ACTOR, "ES-B", col_mov=col_mov, col_emp=ColFake([empresa]), col_pla=ColFake([plan_doc(precio=4000)]))

    def test_cierre_con_precio_snapshot(self):
        empresa = empresa_doc()
        col_emp, col_pla, col_mov = _montar(empresa)
        cobro.reservar_consumo(empresa, ACTOR, "ES-A", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        col_pla.documents[0]["precio_por_estudio"] = 4000  # cambio de precio
        cobro.reservar_consumo(empresa, ACTOR, "ES-B", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        # Forzar período de los movimientos a un mes pasado.
        for m in col_mov.documents:
            m["periodo"] = "2026-07"
        col_per = ColFake()
        cierre = cobro.cerrar_periodo(empresa, ACTOR_ADMIN, "2026-07", col_mov=col_mov, col_per=col_per)
        self.assertEqual(cierre["totales"]["subtotal_cop"], 7500)  # 3500 + 4000, no recalculado
        self.assertEqual(cierre["totales"]["unidades"], 2)
        self.assertTrue(all(m.get("cierre_id") == cierre["_id"] for m in col_mov.documents))

    def test_cerrar_periodo_actual_rechazado(self):
        empresa = empresa_doc()
        col_emp, col_pla, col_mov = _montar(empresa)
        actual = cobro.periodo_colombia()
        with self.assertRaises(HTTPException) as ctx:
            cobro.cerrar_periodo(empresa, ACTOR_ADMIN, actual, col_mov=col_mov, col_per=ColFake())
        self.assertEqual(ctx.exception.status_code, 422)

    def test_cerrar_dos_veces_409(self):
        empresa = empresa_doc()
        col_emp, col_pla, col_mov = _montar(empresa)
        cobro.reservar_consumo(empresa, ACTOR, "ES-A", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        for m in col_mov.documents:
            m["periodo"] = "2026-07"
        col_per = ColFake()
        cobro.cerrar_periodo(empresa, ACTOR_ADMIN, "2026-07", col_mov=col_mov, col_per=col_per)
        with self.assertRaises(HTTPException) as ctx:
            cobro.cerrar_periodo(empresa, ACTOR_ADMIN, "2026-07", col_mov=col_mov, col_per=col_per)
        self.assertEqual(ctx.exception.status_code, 409)

    def test_reabrir_descongela_movimientos(self):
        empresa = empresa_doc()
        col_emp, col_pla, col_mov = _montar(empresa)
        cobro.reservar_consumo(empresa, ACTOR, "ES-A", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        for m in col_mov.documents:
            m["periodo"] = "2026-07"
        col_per = ColFake()
        cierre = cobro.cerrar_periodo(empresa, ACTOR_ADMIN, "2026-07", col_mov=col_mov, col_per=col_per)
        cobro.reabrir_periodo(cierre, ACTOR_ADMIN, "falta un pago", col_mov=col_mov, col_per=col_per)
        self.assertTrue(all(m.get("cierre_id") is None for m in col_mov.documents))
        self.assertEqual(len(col_per.documents), 0)


class TestTotales(unittest.TestCase):
    def test_totales_con_reembolso_y_pago(self):
        empresa = empresa_doc()
        col_emp, col_pla, col_mov = _montar(empresa)
        c1 = cobro.reservar_consumo(empresa, ACTOR, "ES-1", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        cobro.reservar_consumo(empresa, ACTOR, "ES-2", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        cobro.reembolsar_consumo(empresa, ACTOR_ADMIN, c1, "ERROR", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla)
        for m in col_mov.documents:
            m["periodo"] = "2026-07"  # los movimientos nacen en el período actual
        pago = cobro._nuevo_movimiento(empresa, ACTOR_ADMIN, "PAGO", unidades=0, monto_cop=-1000, periodo="2026-07")
        col_mov.insert_one(pago)
        t = cobro.totales_periodo(EMPRESA_ID, "2026-07", col_mov=col_mov)
        self.assertEqual(t["consumos"], 2)
        self.assertEqual(t["unidades"], 1)
        self.assertEqual(t["subtotal_cop"], 7000)
        self.assertEqual(t["reembolsos_cop"], -3500)
        self.assertEqual(t["pagos_cop"], -1000)
        self.assertEqual(t["total_cop"], 2500)


class TestSerializacionRespuestas(unittest.TestCase):
    """Regresión del bug 2026-08-29: insert_one añade _id (ObjectId) al dict
    retornado y FastAPI explotaba con 500 al serializar la respuesta."""

    def test_crear_plan_retorna_json_serializable(self):
        from fastapi.encoders import jsonable_encoder

        from rutas import seguridad_cobro as sc

        class ColPlanesFake:
            def __init__(self):
                self.docs = []

            def find_one(self, q=None, *a, **k):
                return None

            def insert_one(self, doc):
                doc["_id"] = ObjectId()  # pymongo hace esto siempre
                self.docs.append(doc)

                class R:
                    inserted_id = doc["_id"]
                return R()

            def create_index(self, *a, **k):
                pass

        original = sc.col_planes
        sc.col_planes = ColPlanesFake()
        try:
            with patch.object(sc, "registrar_evento"):
                respuesta = sc.crear_plan(
                    sc.PlanCrear(nombre="PLAN TEST", precio_por_estudio=1000,
                                 fuentes_incluidas=["procuraduria"]),
                    request=None, actor={"usuario": "EZARATE", "usuario_nombre": "E"},
                )
            # Si esto explota, es el bug de serialización.
            jsonable_encoder(respuesta)
            self.assertNotIn("_id", respuesta)
            self.assertTrue(respuesta["id"])
        finally:
            sc.col_planes = original


class TestCrearEstudioIntegraConsumo(unittest.TestCase):
    """Integración del endpoint crear_estudio con el cobro: el flujo completo
    (reserva → ejecución → PDF → reembolso si ERROR) debe correr sin NameError
    ni ObjectId sin serializar (regresión del typo consumpo_precio 2026-08-29)."""

    def _correr(self, corutina):
        return asyncio.run(corutina)

    def test_estudio_completado_consumiendo_cupo(self):
        from types import SimpleNamespace

        from rutas import seguridad_estudios as se

        empresa = {
            "_id": EMPRESA_ID, "nombre": "EMPRESA A", "activo": True,
            "config": {"fuentes_habilitadas": ["manifiestos_rndc", "procuraduria"]},
            "plan": {"plan_id": PLAN_ID, "plan_nombre": "ESTÁNDAR", "cupo_autorizado": 5,
                     "cupo_disponible": 5, "cupo_consumido": 0,
                     "asignado_en": datetime(2026, 1, 1)},
        }
        actor = {**ACTOR, "rol": "CONSULTADOR"}
        estudio_doc = {
            "consulta_id": "ES-TEST1", "estado": "COMPLETADA",
            "fuentes": {"manifiestos_rndc": {"estado": "EXITO"}},
            "empresa_id": EMPRESA_ID,
        }
        consumo = {
            "tipo": "CONSUMO", "monto_cop": 3500, "plan_nombre": "ESTÁNDAR",
            "precio_unitario_cop": 3500, "consulta_id": "ES-TEST1",
            "_id": ObjectId(), "exento": False, "unidades": 1, "plan_id": PLAN_ID,
            "periodo": cobro.periodo_colombia(),
        }

        class ColEmpresasFake:
            def find_one(self, q=None, *a, **k):
                return empresa if (q or {}).get("_id") == EMPRESA_ID else None

        with patch.object(se, "col_empresas", ColEmpresasFake()), \
             patch.object(se, "_verificar_rate_limit"), \
             patch.object(se, "crear_documento_estudio"), \
             patch.object(se, "ejecutar_estudio", new=lambda **k: _asyncio_ok(estudio_doc)), \
             patch.object(se, "registrar_evento"), \
             patch.object(se, "_respuesta_estudio", side_effect=lambda d: d), \
             patch("Funciones.storage_seguridad.subir_pdf", return_value={"gcs_ruta": "x", "sha256": "y", "tamano": 1}), \
             patch("Funciones.pdf_estudio_seguridad.generar_pdf_estudio", return_value=b"%PDF-test"), \
             patch("Funciones.cobro_seguridad.fuentes_con_plan", side_effect=lambda emp, fs, col=None: fs), \
             patch("Funciones.cobro_seguridad.reservar_consumos", return_value=[consumo]) as reservar, \
             patch("Funciones.cobro_seguridad.reembolsar_consumos_consulta") as reembolsar:
            respuesta = self._correr(se.crear_estudio(
                datos=SimpleNamespace(cedula="1033688842", forzar=False, empresa_id=None),
                request=SimpleNamespace(client=SimpleNamespace(host="127.0.0.1"), headers={}),
                actor=actor,
            ))
        reservar.assert_called_once()
        reembolsar.assert_not_called()  # COMPLETADA no reembolsa
        self.assertEqual(respuesta["estado"], "COMPLETADA")
        self.assertTrue(respuesta["pdf"]["gcs_ruta"])

    def test_estudio_error_reembolsa(self):
        from types import SimpleNamespace

        from rutas import seguridad_estudios as se

        empresa = {
            "_id": EMPRESA_ID, "nombre": "EMPRESA A", "activo": True,
            "config": {"fuentes_habilitadas": ["manifiestos_rndc", "procuraduria"]},
            "plan": {"plan_id": PLAN_ID, "cupo_autorizado": 5, "cupo_disponible": 5,
                     "cupo_consumido": 0, "asignado_en": datetime(2026, 1, 1)},
        }
        actor = {**ACTOR, "rol": "CONSULTADOR"}
        estudio_doc = {
            "consulta_id": "ES-TEST2", "estado": "ERROR",
            "fuentes": {}, "empresa_id": EMPRESA_ID,
        }
        consumo = {
            "tipo": "CONSUMO", "monto_cop": 3500, "plan_nombre": "ESTÁNDAR",
            "precio_unitario_cop": 3500, "consulta_id": "ES-TEST2",
            "_id": ObjectId(), "exento": False, "unidades": 1, "plan_id": PLAN_ID,
            "periodo": cobro.periodo_colombia(),
        }

        class ColEmpresasFake:
            def find_one(self, q=None, *a, **k):
                return empresa if (q or {}).get("_id") == EMPRESA_ID else None

        with patch.object(se, "col_empresas", ColEmpresasFake()), \
             patch.object(se, "_verificar_rate_limit"), \
             patch.object(se, "crear_documento_estudio"), \
             patch.object(se, "ejecutar_estudio", new=lambda **k: _asyncio_ok(estudio_doc)), \
             patch.object(se, "registrar_evento"), \
             patch.object(se, "_respuesta_estudio", side_effect=lambda d: d), \
             patch("Funciones.storage_seguridad.subir_pdf", return_value={"gcs_ruta": "x", "sha256": "y", "tamano": 1}), \
             patch("Funciones.pdf_estudio_seguridad.generar_pdf_estudio", return_value=b"%PDF-test"), \
             patch("Funciones.cobro_seguridad.fuentes_con_plan", side_effect=lambda emp, fs, col=None: fs), \
             patch("Funciones.cobro_seguridad.reservar_consumos", return_value=[consumo]), \
             patch("Funciones.cobro_seguridad.reembolsar_consumos_consulta") as reembolsar:
            self._correr(se.crear_estudio(
                datos=SimpleNamespace(cedula="1033688842", forzar=False, empresa_id=None),
                request=SimpleNamespace(client=SimpleNamespace(host="127.0.0.1"), headers={}),
                actor=actor,
            ))
        reembolsar.assert_called_once()  # ERROR global → reembolso


def _asyncio_ok(valor):
    async def _corutina(**_kwargs):
        return valor
    return _corutina()


class TestPdfCuentaCobro(unittest.TestCase):
    def _fixture(self, n_consumos=3):
        movimientos = []
        for i in range(n_consumos):
            movimientos.append({
                "tipo": "CONSUMO", "creado_en": datetime(2026, 8, 1) + timedelta(hours=i),
                "consulta_id": f"ES-AA{i}", "cedula": "1033688842",
                "estado_estudio": "COMPLETADA", "plan_nombre": "ESTÁNDAR",
                "precio_unitario_cop": 3500, "monto_cop": 3500, "unidades": 1,
                "exento": False,
            })
        movimientos.append({
            "tipo": "PAGO", "creado_en": datetime(2026, 9, 5), "fecha_pago": datetime(2026, 9, 5),
            "metodo": "TRANSFERENCIA", "referencia": "009812", "monto_cop": -5000, "unidades": 0,
        })
        cierre = {
            "periodo": "2026-07", "cerrado_en": datetime(2026, 9, 1), "cerrado_por": "EZARATE",
            "estado": "PENDIENTE_COBRO",
            "totales": {"unidades": n_consumos, "subtotal_cop": 3500 * n_consumos,
                        "reembolsos_cop": 0, "ajustes_cop": 0, "pagos_cop": -5000,
                        "total_cop": 3500 * n_consumos - 5000},
        }
        empresa = {"_id": EMPRESA_ID, "nombre": "GLAMPEROS SAS", "nit": "901923029"}
        return empresa, cierre, movimientos

    def test_bytes_pdf_validos(self):
        from Funciones.pdf_cuenta_cobro import generar_pdf_cuenta

        contenido = generar_pdf_cuenta(*self._fixture())
        self.assertTrue(contenido.startswith(b"%PDF"))
        self.assertGreater(len(contenido), 1500)

    def test_multipagina_y_contenido(self):
        import io

        import pdfplumber

        from Funciones.pdf_cuenta_cobro import generar_pdf_cuenta

        empresa, cierre, movimientos = self._fixture(n_consumos=45)
        contenido = generar_pdf_cuenta(empresa, cierre, movimientos)
        gris = (0.501961, 0.501961, 0.501961)
        with pdfplumber.open(io.BytesIO(contenido)) as pdf:
            self.assertGreater(len(pdf.pages), 1)
            partes = []
            for p in pdf.pages:
                # Excluir la marca de agua (misma técnica de test_pdf_estudio_seguridad).
                chars = [c for c in p.chars if not (c.get("non_stroking_color") == gris and c.get("size", 0) > 9)]
                chars.sort(key=lambda c: (round(c["top"], 1), c["x0"]))
                partes.append("".join(c["text"] for c in chars))
            texto = "".join(partes).replace(" ", "")
        for esperado in ("CUENTADECOBRO", "GLAMPEROSSAS", "ES-AA0", "10******42", "TOTALAPAGAR", "$152.500", "Página1de"):
            self.assertIn(esperado, texto)

    def test_reproducible(self):
        from Funciones.pdf_cuenta_cobro import generar_pdf_cuenta

        a = generar_pdf_cuenta(*self._fixture())
        b = generar_pdf_cuenta(*self._fixture())
        self.assertEqual(a, b)


class TestReservarConsumosMultiFuente(unittest.TestCase):
    """Multi-plan: un CONSUMO por fuente, con cupos y precios independientes."""

    RNDC, PROC = "manifiestos_rndc", "procuraduria"

    def _montaje(self, entradas, planes=None):
        empresa = empresa_planes_doc(entradas)
        col_emp = ColFake([empresa])
        col_pla = ColFake(planes or [plan_doc()])
        col_mov = ColFake([], unique_compuesto=(["consulta_id", "consumo_id"], {"REEMBOLSO"}))
        return empresa, col_emp, col_pla, col_mov

    def test_dos_fuentes_dos_consumos_con_precios_distintos(self):
        plan_rndc = plan_doc(nombre="SOLO RNDC")
        plan_proc = plan_doc(precio=1500, nombre="SOLO PROC", plan_id=ObjectId())
        empresa, col_emp, col_pla, col_mov = self._montaje(
            [entrada_plan(self.RNDC, plan_rndc, cupo_autorizado=10),
             entrada_plan(self.PROC, plan_proc, cupo_autorizado=5)],
            planes=[plan_rndc, plan_proc],
        )
        movs = cobro.reservar_consumos(
            empresa, ACTOR, "ES-M1", [self.RNDC, self.PROC],
            col_mov=col_mov, col_emp=col_emp, col_pla=col_pla,
        )
        self.assertEqual(len(movs), 2)
        por_fuente = {m["fuente"]: m for m in movs}
        self.assertEqual(por_fuente[self.RNDC]["monto_cop"], 3500)
        self.assertEqual(por_fuente[self.PROC]["monto_cop"], 1500)
        # Cupos decrementados por separado.
        planes_doc = col_emp.documents[0]["planes"]
        self.assertEqual(planes_doc[0]["cupo_disponible"], 9)
        self.assertEqual(planes_doc[1]["cupo_disponible"], 4)
        self.assertEqual(planes_doc[0]["cupo_consumido"], 1)
        self.assertEqual(planes_doc[1]["cupo_consumido"], 1)

    def test_multi_plan_misma_fuente_fifo(self):
        """Dos planes acumulados en la MISMA fuente: se consume primero el
        plan asignado más antiguo (FIFO) y cada consumo cobra SU precio
        congelado. Al agotarse el primero sigue con el segundo."""
        plan_av = plan_doc(precio=3000, nombre="AVANZADO", plan_id=ObjectId())
        plan_pro = plan_doc(precio=2000, nombre="PRO", plan_id=ObjectId())
        empresa, col_emp, col_pla, col_mov = self._montaje(
            [entrada_plan(self.RNDC, plan_av, cupo_autorizado=2, asignado_en=datetime(2026, 1, 1)),
             entrada_plan(self.RNDC, plan_pro, cupo_autorizado=5, asignado_en=datetime(2026, 2, 1))],
            planes=[plan_av, plan_pro],
        )
        movs = []
        for i in range(3):
            movs += cobro.reservar_consumos(
                empresa, ACTOR, f"ES-FIFO-{i}", [self.RNDC],
                col_mov=col_mov, col_emp=col_emp, col_pla=col_pla,
            )
        # 2 primeros del plan antiguo (3000), el 3ro del nuevo (2000).
        self.assertEqual([m["monto_cop"] for m in movs], [3000, 3000, 2000])
        self.assertEqual([m["plan_nombre"] for m in movs], ["AVANZADO", "AVANZADO", "PRO"])
        planes_doc = col_emp.documents[0]["planes"]
        self.assertEqual(planes_doc[0]["cupo_disponible"], 0)
        self.assertEqual(planes_doc[1]["cupo_disponible"], 4)

    def test_multi_plan_misma_fuente_todos_agotados_402(self):
        plan_a = plan_doc(nombre="A", plan_id=ObjectId())
        plan_b = plan_doc(nombre="B", plan_id=ObjectId())
        empresa, col_emp, col_pla, col_mov = self._montaje(
            [entrada_plan(self.RNDC, plan_a, cupo_autorizado=0, cupo_disponible=0),
             entrada_plan(self.RNDC, plan_b, cupo_autorizado=0, cupo_disponible=0)],
            planes=[plan_a, plan_b],
        )
        with self.assertRaises(HTTPException) as ctx:
            cobro.reservar_consumos(
                empresa, ACTOR, "ES-FIFO-402", [self.RNDC],
                col_mov=col_mov, col_emp=col_emp, col_pla=col_pla,
            )
        self.assertEqual(ctx.exception.status_code, 402)
        # El 402 nombra los planes agotados.
        self.assertIn("A", ctx.exception.detail)
        self.assertIn("B", ctx.exception.detail)

    def test_fuera_sin_plan_se_omite(self):
        empresa, col_emp, col_pla, col_mov = self._montaje(
            [entrada_plan(self.RNDC, cupo_autorizado=10)]
        )
        movs = cobro.reservar_consumos(
            empresa, ACTOR, "ES-M2", [self.RNDC, self.PROC],  # pide 2, hay plan para 1
            col_mov=col_mov, col_emp=col_emp, col_pla=col_pla,
        )
        self.assertEqual(len(movs), 1)
        self.assertEqual(movs[0]["fuente"], self.RNDC)

    def test_cupo_agotado_en_segunda_compensa_la_primera(self):
        empresa, col_emp, col_pla, col_mov = self._montaje(
            [entrada_plan(self.RNDC, cupo_autorizado=10),
             entrada_plan(self.PROC, cupo_autorizado=0, cupo_disponible=0, cupo_consumido=0)]
        )
        with self.assertRaises(HTTPException) as ctx:
            cobro.reservar_consumos(
                empresa, ACTOR, "ES-M3", [self.RNDC, self.PROC],
                col_mov=col_mov, col_emp=col_emp, col_pla=col_pla,
            )
        self.assertEqual(ctx.exception.status_code, 402)
        self.assertIn(self.PROC, ctx.exception.detail)
        # La reserva de RNDC fue compensada: cupo intacto, sin movimientos.
        planes_doc = col_emp.documents[0]["planes"]
        self.assertEqual(planes_doc[0]["cupo_disponible"], 10)
        self.assertEqual(planes_doc[0]["cupo_consumido"], 0)
        self.assertEqual(len(col_mov.documents), 0)

    def test_ninguna_fuente_con_plan_402(self):
        empresa, col_emp, col_pla, col_mov = self._montaje([])
        with self.assertRaises(HTTPException) as ctx:
            cobro.reservar_consumos(
                empresa, ACTOR, "ES-M4", [self.RNDC],
                col_mov=col_mov, col_emp=col_emp, col_pla=col_pla,
            )
        self.assertEqual(ctx.exception.status_code, 402)

    def test_admin_sin_planes_exento_unico(self):
        empresa, col_emp, col_pla, col_mov = self._montaje([])
        movs = cobro.reservar_consumos(
            empresa, ACTOR_ADMIN, "ES-M5", [self.RNDC, self.PROC],
            col_mov=col_mov, col_emp=col_emp, col_pla=col_pla,
        )
        self.assertEqual(len(movs), 1)
        self.assertTrue(movs[0]["exento"])
        self.assertEqual(movs[0]["monto_cop"], 0)

    def test_ilimitado_por_fuente(self):
        empresa, col_emp, col_pla, col_mov = self._montaje(
            [entrada_plan(self.RNDC, cupo_autorizado=None, cupo_disponible=None),
             entrada_plan(self.PROC, cupo_autorizado=2)]
        )
        # 2 consultas con ambas fuentes agotan el cupo de PROC (2).
        for i in range(2):
            cobro.reservar_consumos(
                empresa, ACTOR, f"ES-M6-{i}", [self.RNDC, self.PROC],
                col_mov=col_mov, col_emp=col_emp, col_pla=col_pla,
            )
        # La 3.ª corre SOLO RNDC (PROC sin cupo): no debe fallar.
        cobro.reservar_consumos(
            empresa, ACTOR, "ES-M6-2", [self.RNDC],
            col_mov=col_mov, col_emp=col_emp, col_pla=col_pla,
        )
        planes_doc = col_emp.documents[0]["planes"]
        self.assertEqual(planes_doc[0]["cupo_consumido"], 3)      # RNDC ilimitado
        self.assertIsNone(planes_doc[0]["cupo_disponible"])
        self.assertEqual(planes_doc[1]["cupo_consumido"], 2)      # PROC con tope
        self.assertEqual(planes_doc[1]["cupo_disponible"], 0)

    def test_error_global_reembolsa_todos(self):
        plan_rndc = plan_doc(nombre="SOLO RNDC")
        plan_proc = plan_doc(precio=1500, nombre="SOLO PROC", plan_id=ObjectId())
        empresa, col_emp, col_pla, col_mov = self._montaje(
            [entrada_plan(self.RNDC, plan_rndc, cupo_autorizado=10),
             entrada_plan(self.PROC, plan_proc, cupo_autorizado=5)],
            planes=[plan_rndc, plan_proc],
        )
        cobro.reservar_consumos(
            empresa, ACTOR, "ES-M7", [self.RNDC, self.PROC],
            col_mov=col_mov, col_emp=col_emp, col_pla=col_pla,
        )
        reembolsos = cobro.reembolsar_consumos_consulta(
            "ES-M7", empresa, ACTOR_ADMIN, "ERROR global", automatico=True,
            col_mov=col_mov, col_emp=col_emp, col_pla=col_pla,
        )
        self.assertEqual(len(reembolsos), 2)
        self.assertEqual(sum(r["monto_cop"] for r in reembolsos), -(3500 + 1500))
        planes_doc = col_emp.documents[0]["planes"]
        self.assertEqual(planes_doc[0]["cupo_disponible"], 10)  # devueltos
        self.assertEqual(planes_doc[1]["cupo_disponible"], 5)

    def test_reembolso_doble_por_consumo_bloqueado(self):
        empresa, col_emp, col_pla, col_mov = self._montaje(
            [entrada_plan(self.RNDC, cupo_autorizado=10)]
        )
        cobro.reservar_consumos(
            empresa, ACTOR, "ES-M8", [self.RNDC],
            col_mov=col_mov, col_emp=col_emp, col_pla=col_pla,
        )
        primera = cobro.reembolsar_consumos_consulta(
            "ES-M8", empresa, ACTOR_ADMIN, "ERROR", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla,
        )
        self.assertEqual(len(primera), 1)
        segunda = cobro.reembolsar_consumos_consulta(
            "ES-M8", empresa, ACTOR_ADMIN, "ERROR", col_mov=col_mov, col_emp=col_emp, col_pla=col_pla,
        )
        self.assertEqual(segunda, [])  # idempotente: flag reembolsado
        self.assertEqual(len([m for m in col_mov.documents if m["tipo"] == "REEMBOLSO"]), 1)
        # El cupo se devuelve UNA vez.
        self.assertEqual(col_emp.documents[0]["planes"][0]["cupo_disponible"], 10)

    def test_fallback_subdoc_viejo(self):
        """Empresa con subdoc plan viejo (pre-backfill): delega en la reserva
        de subdoc único — 1 movimiento, 1 unidad, precio del plan."""
        empresa = empresa_doc(cupo_autorizado=4)  # subdoc viejo
        col_emp = ColFake([empresa])
        col_pla = ColFake([plan_doc()])
        col_mov = ColFake([], unique_compuesto=(["consulta_id", "consumo_id"], {"REEMBOLSO"}))
        movs = cobro.reservar_consumos(
            empresa, ACTOR, "ES-M9", [self.RNDC, self.PROC],
            col_mov=col_mov, col_emp=col_emp, col_pla=col_pla,
        )
        self.assertEqual(len(movs), 1)       # interfaz vieja: 1 por consulta
        self.assertEqual(movs[0]["monto_cop"], 3500)
        # El cupo del subdoc se descontó 1.
        self.assertEqual(col_emp.documents[0]["plan"]["cupo_disponible"], 3)


if __name__ == "__main__":
    unittest.main()
