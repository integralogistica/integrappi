from __future__ import annotations

import math
import threading
import time
import uuid
from datetime import datetime, timezone
from io import BytesIO

from openpyxl import Workbook
from pymongo import ASCENDING, MongoClient, ReturnDocument

from .errors import (RNDCBusinessError, RNDCCredentialsError, RNDCNoDataError,
                     RNDCResponseParseError, RNDCSoapFaultError, RNDCTransportError)
from .excel_service import (ENTRADAS_RESULTADO, SALIDAS, _ajustar_hoja,
                            _formatear_columnas_numericas, _numero_excel,
                            leer_consultas_excel, resumir_rutas)
from .execution import RNDC_EXECUTION_LOCK
from .models import ExploracionRutaRequest
from .rndc_client import RNDCClient

MAX_FILAS_JOB = 2000
TAMANO_LOTE = 30
TRANSITORIOS = (RNDCResponseParseError, RNDCSoapFaultError, RNDCTransportError)


def _ahora():
    return datetime.now(timezone.utc)


class SicetacBatchJobs:
    def __init__(self, settings, shared_client=None):
        self.settings = settings
        self.client = shared_client or MongoClient(settings.mongodb_uri)
        db = self.client[settings.mongodb_database]
        self.jobs = db[f"{settings.mongodb_collection}_excel_jobs"]
        self.rows = db[f"{settings.mongodb_collection}_excel_job_rows"]
        self.jobs.create_index([("estado", ASCENDING), ("creado_en", ASCENDING)])
        self.rows.create_index([("job_id", ASCENDING), ("fila_entrada", ASCENDING)], unique=True)
        self._event = threading.Event()
        self._thread = None
        self._thread_lock = threading.Lock()

    def recuperar(self):
        # En el arranque no puede quedar un worker anterior del mismo proceso.
        self.jobs.update_many({"estado": "ejecutando"}, {"$set": {"estado": "pendiente", "actualizado_en": _ahora()}})
        self.iniciar_worker()

    def crear(self, content: bytes, nombre_archivo: str, creado_por=None):
        consultas, errores = leer_consultas_excel(content, max_filas=MAX_FILAS_JOB)
        job_id = uuid.uuid4().hex
        total = len(consultas) + len(errores)
        now = _ahora()
        documents = []
        for fila, payload in consultas:
            documents.append({
                "job_id": job_id, "fila_entrada": fila, "estado": "pendiente",
                "payload": payload.model_dump(mode="json"), "intentos": 0,
                "creado_en": now, "actualizado_en": now,
            })
        for fila, raw, mensaje in errores:
            documents.append({
                "job_id": job_id, "fila_entrada": fila, "estado": "ERROR_VALIDACION",
                "payload": raw, "resultados": [], "mensaje": mensaje, "intentos": 0,
                "creado_en": now, "actualizado_en": now,
            })
        self.jobs.insert_one({
            "job_id": job_id, "estado": "pendiente", "archivo": nombre_archivo,
            "creado_por": creado_por, "filas_totales": total,
            "filas_procesadas": len(errores), "filas_exitosas": 0,
            "filas_sin_resultado": 0, "filas_con_error": len(errores),
            "resultados_generados": 0, "tamano_lote": TAMANO_LOTE,
            "lotes_totales": math.ceil(total / TAMANO_LOTE), "creado_en": now,
            "actualizado_en": now,
        })
        if documents:
            self.rows.insert_many(documents, ordered=False)
        self.iniciar_worker()
        return self.obtener(job_id)

    def iniciar_worker(self):
        with self._thread_lock:
            if self._thread and self._thread.is_alive():
                self._event.set()
                return
            self._thread = threading.Thread(target=self._worker_loop, name="sicetac-excel-worker", daemon=True)
            self._thread.start()

    def _worker_loop(self):
        while True:
            job = self.jobs.find_one_and_update(
                {"estado": "pendiente"},
                {"$set": {"estado": "ejecutando", "iniciado_en": _ahora(), "actualizado_en": _ahora()}},
                sort=[("creado_en", ASCENDING)], return_document=ReturnDocument.AFTER,
            )
            if not job:
                self._event.clear()
                self._event.wait(5)
                if not self.jobs.count_documents({"estado": "pendiente"}, limit=1):
                    return
                continue
            with RNDC_EXECUTION_LOCK:
                self._procesar_job(job)

    def _procesar_job(self, job):
        job_id = job["job_id"]
        client = RNDCClient(self.settings.soap_url, self.settings.username, self.settings.password)
        try:
            for row in self.rows.find({"job_id": job_id, "estado": "pendiente"}).sort("fila_entrada", ASCENDING):
                self._procesar_fila(client, job_id, row)
            self.jobs.update_one({"job_id": job_id}, {"$set": {
                "estado": "completada", "finalizado_en": _ahora(), "actualizado_en": _ahora()
            }})
        except RNDCCredentialsError as exc:
            self.jobs.update_one({"job_id": job_id}, {"$set": {
                "estado": "fallida", "error": "RNDC rechazó las credenciales",
                "finalizado_en": _ahora(), "actualizado_en": _ahora()
            }})
        except Exception as exc:
            self.jobs.update_one({"job_id": job_id}, {"$set": {
                "estado": "pendiente", "ultimo_error": f"{type(exc).__name__}: {exc}",
                "actualizado_en": _ahora()
            }})
        finally:
            client.close()

    def _procesar_fila(self, client, job_id, row):
        payload = ExploracionRutaRequest.model_validate(row["payload"])
        documentos = None
        ultimo_error = None
        for attempt in range(1, 4):
            try:
                documentos = client.explorar(payload.periodo, payload.configuracion, payload.origen,
                                             payload.destino, payload.condicion_carga)
                ultimo_error = None
                break
            except RNDCNoDataError as exc:
                documentos, ultimo_error = [], None
                break
            except RNDCBusinessError as exc:
                if "RNDC13" in str(exc).upper():
                    documentos, ultimo_error = [], None
                    break
                ultimo_error = exc
            except TRANSITORIOS as exc:
                ultimo_error = exc
            if attempt < 3:
                time.sleep(1)

        if ultimo_error:
            estado, mensaje, rutas = "ERROR", f"{type(ultimo_error).__name__}: {ultimo_error}", []
        else:
            rutas, _ = resumir_rutas(
                documentos or [], payload.limit, payload.unidad_transporte_nombre,
                payload.tipo_carga_nombre, payload.horas_totales_cargue,
                payload.horas_totales_descargue,
            )
            estado = "OK" if rutas else "SIN_RESULTADO"
            mensaje = "" if rutas else "RNDC no devolvió coincidencias para los filtros"
        self.rows.update_one({"_id": row["_id"]}, {"$set": {
            "estado": estado, "mensaje": mensaje, "resultados": rutas,
            "intentos": 3 if ultimo_error else 1, "actualizado_en": _ahora(),
        }})
        increments = {"filas_procesadas": 1, "resultados_generados": len(rutas)}
        increments["filas_exitosas" if estado == "OK" else "filas_sin_resultado" if estado == "SIN_RESULTADO" else "filas_con_error"] = 1
        self.jobs.update_one({"job_id": job_id}, {
            "$inc": increments, "$set": {"actualizado_en": _ahora()}
        })

    def obtener(self, job_id):
        job = self.jobs.find_one({"job_id": job_id}, {"_id": 0})
        if not job:
            return None
        job["ejecucion_id"] = job["job_id"]
        total, done = job.get("filas_totales", 0), job.get("filas_procesadas", 0)
        job["progreso_porcentaje"] = round(done * 100 / total, 2) if total else 0
        job["lote_actual"] = min(math.ceil(done / TAMANO_LOTE), job.get("lotes_totales", 0)) if done else 0
        for key, value in list(job.items()):
            if isinstance(value, datetime):
                job[key] = value.isoformat()
        return job

    def listar(self, limit=10):
        """Devuelve los jobs mas recientes con el mismo resumen de progreso de ``obtener``."""
        jobs = []
        cursor = self.jobs.find({}, {"_id": 0}).sort("creado_en", -1).limit(limit)
        for job in cursor:
            job["ejecucion_id"] = job["job_id"]
            total, done = job.get("filas_totales", 0), job.get("filas_procesadas", 0)
            job["progreso_porcentaje"] = round(done * 100 / total, 2) if total else 0
            job["lote_actual"] = min(
                math.ceil(done / TAMANO_LOTE), job.get("lotes_totales", 0)
            ) if done else 0
            for key, value in list(job.items()):
                if isinstance(value, datetime):
                    job[key] = value.isoformat()
            jobs.append(job)
        return jobs

    def generar_excel(self, job_id):
        job = self.jobs.find_one({"job_id": job_id})
        if not job:
            raise KeyError(job_id)
        if job["estado"] != "completada":
            raise RuntimeError("La ejecución todavía no está completada")
        wb = Workbook(); ws = wb.active; ws.title = "resultados"
        ws.append(ENTRADAS_RESULTADO + SALIDAS)
        for row in self.rows.find({"job_id": job_id}).sort("fila_entrada", ASCENDING):
            payload = row.get("payload", {})
            base = [payload.get(x) for x in ENTRADAS_RESULTADO]
            routes = row.get("resultados") or []
            if not routes:
                ws.append(base + [row["fila_entrada"], row["estado"], row.get("mensaje", "")] + [None] * (len(SALIDAS) - 3))
                continue
            for ruta in routes:
                ws.append(base + [
                    row["fila_entrada"], "OK", "", ruta.get("periodo"), ruta.get("origen_codigo"),
                    ruta.get("origen_nombre"), ruta.get("destino_codigo"), ruta.get("destino_nombre"),
                    ruta.get("configuracion"), ruta.get("condicion_carga"), ruta.get("tipo_carga_codigo"),
                    ruta.get("tipo_carga_nombre"), ruta.get("unidad_transporte_codigo"),
                    ruta.get("unidad_transporte_nombre"), ruta.get("ruta_id"), ruta.get("via"),
                    _numero_excel(ruta.get("kilometros")), _numero_excel(ruta.get("horas_recorrido")),
                    _numero_excel(ruta.get("valor_moviliza")), _numero_excel(ruta.get("valor_hora")),
                    _numero_excel(ruta.get("horas_logisticas_total")), _numero_excel(ruta.get("costo_total_calculado")),
                ])
        _formatear_columnas_numericas(ws); _ajustar_hoja(ws)
        output = BytesIO(); wb.save(output); output.seek(0)
        return output
