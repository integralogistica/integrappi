# Funciones/trazabilidad_guia.py
"""
Trazabilidad de guía — enriquecimiento COMPARTIDO (2026-11-13).

Dos consumidores de la misma lógica:
- ``GET /indicadores-cliente/{id}/consulta-guia`` (pestaña 🧭 Trazabilidad del
  portal de clientes).
- Bot de WhatsApp (``rutas/whatsapp_integra.py``, flujo 🧾 CLIENTE) — el bot
  muestra CUALQUIER guía (sin filtro de cliente).

Recibe el payload crudo del SOAP de Siscore (``consultar_guia_ws``) y le agrega:

- ``placa`` / ``ciudad_destino_tms``: fila de la guía en ``informe_guias_tms``.
- ``ot``: veredicto On Time corriendo la MISMA ``_calcular_ot_filas`` del
  informe (no puede divergir del dashboard).
- ``misma_placa``: otras guías del mismo vehículo emitidas el mismo día.
- ``vehiculo``: última posición GPS (API Monitor de Mayasoft), solo si la guía
  NO está entregada/anulada/devolución total.
- ``justificada``: novedad justificada de ``novedades_justificadas_ot``.
- ``cita_entrega``: cita manual de ``citas_entrega`` (con re-agendas).
- ``evidencia_url``: PNG público de la entrega digitalizada de Siscore
  (espejo del armado del frontend ``DashboardCliente/index.tsx``).

Además PERSISTE en ``guias_anuladas`` toda guía que el WS en vivo reporte
ANULADA (⚠️ el informe del TMS nunca la trae) y limpia los caches del OT —
detección orgánica: funciona igual la consulta venga del portal o del bot.

Todo el estado del router (colecciones, caches, ``_calcular_ot_filas``) se
resuelve PEREZOSAMENTE vía ``rutas.indicadores_cliente`` (evita import
circular y mantiene vivos los patches de los tests sobre ese módulo).
"""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# URL pública del soporte gráfico de entregas DIGITALIZADAS de Siscore:
# .../app/<token>/<token>/imagenes/<año>/<mes>/<día de EMISIÓN>/<guía>.png
# (espejo del frontend — el token es el mismo público que viaja en los links
# de tracking; si Siscore lo rota hay que actualizarlo ahí y acá).
_TOKEN_PUBLICO_SISCORE = "xMr3hVYKKkVzL0gGjnCFySIRO6RLPLb1qUb1bwop94Zq8Of"
URL_EVIDENCIA_SISCORE = (
    f"https://integra.appsiscore.com/app/{_TOKEN_PUBLICO_SISCORE}"
    f"/{_TOKEN_PUBLICO_SISCORE}/imagenes"
)


def guia_ya_entregada(payload: dict) -> bool:
    """¿La guía ya se entregó según el WS de Siscore? Mismo criterio del
    frontend (``index.tsx``: Estado con 'ENTREGA') más 'DIGITALIZADA'
    (entrega con soporte gráfico). No se usa ``ot.entregada`` porque depende
    de que la guía exista en el TMS con fecha de emisión."""
    estado = str((payload.get("data") or {}).get("Estado") or "").upper()
    return "ENTREGA" in estado or "DIGITALIZADA" in estado


def url_evidencia_guia(payload: dict) -> Optional[str]:
    """PNG público de la evidencia de entrega cuando el estado trae
    DIGITALIZADA (la entrega ya surtida con soporte gráfico). La ruta usa el
    día de EMISIÓN de la guía (TMS; fallback FecEnv del WS) y SOLO si es una
    fecha ISO evaluable — si no, no hay URL (caen al tracking normal)."""
    estado = str((payload.get("data") or {}).get("Estado") or "").upper()
    if "DIGITALIZADA" not in estado:
        return None
    ot = payload.get("ot") or {}
    fec_env = (payload.get("data") or {}).get("FecEnv")
    fecha = str(ot.get("fecha_emision") or fec_env or "").strip()[:10]
    if len(fecha) != 10 or fecha[4] != "-" or fecha[7] != "-":
        return None
    return f"{URL_EVIDENCIA_SISCORE}/{fecha.replace('-', '/')}/{payload.get('guia')}.png"


async def enriquecer_consulta_guia(
    guia: str,
    payload: dict,
    nombres_cliente: Optional[List[str]] = None,
    detectada_por: Optional[str] = None,
) -> dict:
    """Enriquece (en el MISMO dict, que también devuelve) el payload del SOAP
    de Siscore con todo lo que sabe Integra: TMS, OT, cita, novedad,
    ubicación del vehículo y evidencia. No lanza: cada pieza falla en
    silencio (la consulta SOAP ya tardó 40-90s, no se pierde por un extra).

    ``nombres_cliente``: filtro para «otras guías del vehículo» (el portal
    externo no debe descubrir guías de otros clientes); ``None`` (bot) las
    lista TODAS. ``detectada_por``: quién consulta — queda en la auditoría
    de ``guias_anuladas``.
    """
    # Acceso perezoso al módulo del router: colecciones Mongo, caches del OT
    # y _calcular_ot_filas viven ahí (y los tests parchean sobre ese módulo).
    from rutas import indicadores_cliente as ic

    g = (guia or "").strip()

    # Placa asociada a la guía según el TMS (informe_guias_tms); ciudad
    # destino del TMS como refuerzo de la que trae el WS.
    tms = ic.guia_tms_placa(g) or {}
    if tms.get("placa"):
        payload["placa"] = tms["placa"]
    if tms.get("ciudad_destino"):
        payload["ciudad_destino_tms"] = tms["ciudad_destino"]

    # ⚠️ El informe del TMS NUNCA trae 'ANULADA' (deja el último estado,
    # p. ej. 'Transito Nacional' — caso 801232571): si el WS EN VIVO dice que
    # la guía fue anulada, se persiste en `guias_anuladas` para que el OT la
    # trate como «Con novedad» (auto-cumple) en TODO el dashboard. Solo
    # escribe la PRIMERA vez; limpia los caches para efecto inmediato
    # (el bot comparte proceso → su detección también refresca el portal).
    estado_ws = str((payload.get("data") or {}).get("Estado") or "").upper()
    if "ANULADA" in estado_ws:
        try:
            if not ic.col_guias_anuladas.find_one({"guia": g.upper()}):
                ic.col_guias_anuladas.insert_one({
                    "guia": g.upper(),
                    "estado_siscore": estado_ws,
                    "detectada_el": datetime.now().isoformat(timespec="seconds"),
                    "detectada_por": detectada_por,
                })
                ic._CACHE_TABLAS.pop("guias_anuladas", None)
                ic._CACHE_GUIAS.clear()
                ic._CACHE_PENDIENTES.clear()
                logger.info(
                    "[trazabilidad-guia] Guía %s ANULADA según el WS — pasa a «Con novedad» en el OT", g
                )
        except Exception:
            logger.warning(f"[trazabilidad-guia] No se pudo persistir la anulación de {g}", exc_info=True)

    # ¿Cumplió la guía el OT? MISMA función del informe (_calcular_ot_filas)
    # sobre la fila del TMS — el veredicto no puede divergir del que ven en
    # el gráfico/tabla. Viaja aparte de `justificada` (Mongo); el consumidor
    # prioriza la justificación si rescata una no cumplida.
    if tms.get("fecha_emision"):
        fila_ot = {
            "guia": g,
            "estado": tms.get("estado"),
            "nombre_cliente": tms.get("nombre_cliente"),
            "destinatario": tms.get("destinatario"),
            "ciudad_origen": tms.get("ciudad_origen"),
            "departamento_origen": tms.get("departamento_origen"),
            "departamento_destino": tms.get("departamento_destino"),
            "fecha_carga": tms.get("fecha_carga"),
            "novedad_tms": tms.get("novedad_tms"),
            "fecha_emision": tms.get("fecha_emision"),
            "fecha_entrega": tms.get("fecha_entrega"),
            "fecha_cita": tms.get("fecha_cita"),
            "ciudad_destino": tms.get("ciudad_destino"),
        }
        fila_ot["estado"], _ = ic._normalizar_estado(fila_ot["estado"])
        ic._calcular_ot_filas([fila_ot])
        payload["ot"] = {
            "ot": fila_ot.get("ot"),
            "cumple_novedad": fila_ot.get("cumple_novedad"),
            "tipo_servicio": fila_ot.get("tipo_servicio"),
            "ruta": fila_ot.get("ruta"),
            "fecha_emision": fila_ot.get("fecha_emision"),
            "fecha_entrega": fila_ot.get("fecha_entrega"),
            "fecha_promesa": fila_ot.get("fecha_promesa"),
            "origen_promesa": fila_ot.get("origen_promesa"),
            # La CITA con la que se juzgó (manual de /CitasEntrega o agendada)
            # y si fue manual — la Trazabilidad del portal la muestra.
            "fecha_cita_usada": fila_ot.get("fecha_cita_usada"),
            "cita_manual": bool(fila_ot.get("cita_entrega")),
            "dias_habiles": fila_ot.get("dias_habiles"),
            "entregada": fila_ot.get("estado") == ic.ESTADO_ENTREGADO,
            # Guía EN CURSO (sin fecha de entrega): segmento «Pendientes»/
            # «Pendientes vencidas» — fuera del % OT; `ot` viaja None.
            "pendiente": bool(fila_ot.get("ot_pendiente")),
            "pendiente_vencida": bool(fila_ot.get("ot_pendiente_vencida")),
            # Guía ANULADA (estado del informe O detectada por el WS en vivo):
            # auto-cumple como «Con novedad» — chip propio, no «Con novedad».
            "anulada": bool(fila_ot.get("anulada")),
            # DEVOLUCIÓN TOTAL (novedad del informe TMS): mismo trato.
            "devolucion_total": bool(fila_ot.get("devolucion_total")),
        }

    # Otras guías del mismo vehículo emitidas el mismo día (la remesa que
    # viajó junta) — solo conocible desde el TMS. Con ``nombres_cliente``
    # (portal externo) SOLO las del cliente del dashboard: el vehículo
    # comparte viaje entre clientes y el portal no debe descubrir guías
    # ajenas; sin él (bot) se listan todas.
    if tms.get("placa") and tms.get("fecha_emision"):
        companeras = ic.guias_placa_dia(tms["placa"], tms["fecha_emision"], g, nombres_cliente)
        for cp in companeras:
            cp["estado"], _ = ic._normalizar_estado(cp.get("estado"))
        payload["misma_placa"] = companeras

    # ¿Dónde está el VEHÍCULO? — solo para guías NO entregadas: última
    # posición GPS de la placa según Monitor (Mayasoft, api monitor.pdf).
    # Un fallo de Monitor JAMÁS rompe la consulta (que ya tardó 40-90s en el
    # SOAP): queda en log y el bloque no aparece. Las ANULADAS tampoco (el
    # servicio se canceló) ni DEVOLUCIÓN TOTAL (la carga volvió al remitente).
    if (
        tms.get("placa")
        and not guia_ya_entregada(payload)
        and not payload.get("ot", {}).get("anulada")
        and not payload.get("ot", {}).get("devolucion_total")
    ):
        try:
            from Funciones.monitor_ws import consultar_vehiculo_monitor
            # Guías del viaje para validar el manifiesto: la consultada + las
            # compañeras del mismo vehículo/día. Monitor cruza contra el
            # clientreferencenumber de las remesas → coincide_guia en el
            # payload: sin coincidencia, el manifiesto vigente de la placa es
            # OTRO viaje y la posición no debe mostrarse como de estas guías.
            guias_viaje = {g.upper()}
            for cp in payload.get("misma_placa") or []:
                if cp.get("guia"):
                    guias_viaje.add(str(cp["guia"]).strip().upper())
            payload["vehiculo"] = await consultar_vehiculo_monitor(tms["placa"], guias=guias_viaje)
        except Exception:
            logger.warning(
                "[trazabilidad-guia] Error consultando ubicación en Monitor de la placa %s (guía %s)",
                tms.get("placa"), g, exc_info=True,
            )

    # ¿La guía tiene novedad JUSTIFICADA registrada? (novedades_justificadas_ot
    # — las que cargan en /NovedadesOT).
    nov = ic.col_novedades_ot.find_one(
        {"guia": g},
        {"causal": 1, "justificacion": 1, "creado_por": 1, "actualizado_por": 1, "_id": 0},
    )
    if nov:
        por = nov.get("actualizado_por") or nov.get("creado_por") or {}
        fecha_por = por.get("fecha")
        payload["justificada"] = {
            "causal": nov.get("causal"),
            "justificacion": nov.get("justificacion"),
            "nombre": por.get("nombre") or por.get("usuario"),
            "fecha": fecha_por.isoformat()[:10] if hasattr(fecha_por, "isoformat") else fecha_por,
        }

    # ¿Tiene CITA DE ENTREGA registrada a mano (/CitasEntrega)? La fecha
    # vigente es la que se usó para el veredicto OT de arriba; las re-agendas
    # anteriores viajan en `anteriores[]` (con quién las asignó).
    cita_doc = ic.col_citas_entrega.find_one(
        {"guia": g},
        {"fecha": 1, "hora_inicio": 1, "hora_fin": 1, "creado_por": 1,
         "actualizado_por": 1, "updated_at": 1, "historial": 1, "_id": 0},
    )
    if cita_doc:
        por = cita_doc.get("actualizado_por") or cita_doc.get("creado_por") or {}
        anteriores: List[Dict[str, Any]] = []
        # Últimas 10, de la MÁS RECIENTE a la más antigua (el array es
        # append-order: se toma la cola y se invierte). Cada entrada es el
        # CAMBIO completo {previo, nuevo, por, fecha_cambio}; las entradas
        # viejas (planas: solo el valor anterior) se normalizan a previo.
        for h in (cita_doc.get("historial") or [])[-10:][::-1]:
            if not isinstance(h, dict):
                continue
            quien = h.get("por") or {}
            f_cambio = quien.get("fecha") or h.get("fecha_cambio")
            previo = h.get("previo") or {k: h.get(k) for k in ("fecha", "hora_inicio", "hora_fin")}
            anteriores.append({
                "previo": previo,
                "nuevo": h.get("nuevo"),
                "nombre": quien.get("nombre") or quien.get("usuario"),
                "cambiada": f_cambio.isoformat()[:10] if hasattr(f_cambio, "isoformat") else f_cambio,
            })
        payload["cita_entrega"] = {
            "fecha": cita_doc.get("fecha"),
            "hora_inicio": cita_doc.get("hora_inicio"),
            "hora_fin": cita_doc.get("hora_fin"),
            "nombre": por.get("nombre") or por.get("usuario"),
            "fecha_asignada": cita_doc.get("updated_at").isoformat()[:10] if hasattr(cita_doc.get("updated_at"), "isoformat") else cita_doc.get("updated_at"),
            "anteriores": anteriores,
        }

    # Evidencia de entrega digitalizada (PNG público de Siscore) — reemplaza
    # al link de tracking genérico cuando la entrega ya está digitalizada.
    payload["evidencia_url"] = url_evidencia_guia(payload)

    return payload
