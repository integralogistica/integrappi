"""
Script de migración: unifica las tres implementaciones del cruce en _motor_cruce
y agrega visibilidad de zona_gris + llave_vacia.
"""
import re, sys

FILE = r'C:\Users\ASUS\OneDrive - Integra Logistica\Desarrollos\integra\integrappi\rutas\pacientes_medical_care.py'

with open(FILE, 'r', encoding='utf-8') as f:
    src = f.read()
lines = src.splitlines(keepends=True)

# ── Nuevas funciones ──────────────────────────────────────────────────────────

MOTOR_CRUCE = r'''
def _motor_cruce(pacientes: list, registros_v3: list, cronograma_dict: dict):
    """
    Motor central del cruce pacientes <-> V3.
    Generador: emite dicts de progreso y un dict final con stage='complete' y key 'result'.
    Criterio 1: nombre paciente vs cliente_destino >= 95%  (match_tipo 'nombre')
    Criterio 2: llave paciente vs llave V3 >= 73%          (match_tipo 'llave')
    Criterio 3: celular paciente vs telefono V3 exacto     (match_tipo 'celular')
    Tras la etapa de pacientes, clasifica cada V3 restante como:
      sin_paciente  : mejor similitud < 0.75
      zona_gris     : mejor similitud >= 0.75 pero ningún paciente lo eligió
      llave_vacia   : V3 sin llave (no pudo participar en el cruce)
    """
    from rapidfuzz.fuzz import ratio as fuzz_ratio
    from rapidfuzz import process as _fuzz_process

    total_pacientes = len(pacientes)
    total_v3 = len(registros_v3)
    yield {'stage': 'loading', 'progress': 8,
           'message': f'{total_pacientes} pacientes y {total_v3} pedidos V3 cargados'}

    # ── Construir índices V3 ─────────────────────────────────────────────────
    llaves_v3 = [doc['llave'] for doc in registros_v3 if doc.get('llave')]

    docs_v3_por_llave: dict = {}
    contador_pedidos_por_llave: dict = {}
    for doc in registros_v3:
        llave = doc.get('llave') or ''
        if not llave:
            continue
        contador_pedidos_por_llave[llave] = contador_pedidos_por_llave.get(llave, 0) + 1
        if llave not in docs_v3_por_llave:
            docs_v3_por_llave[llave] = doc
        else:
            exist = docs_v3_por_llave[llave]
            fe_exist = exist.get('fecha_entrega', '')
            fe_curr  = doc.get('fecha_entrega', '')
            if fe_curr and not fe_exist:
                docs_v3_por_llave[llave] = doc
            elif fe_curr and fe_exist:
                if doc.get('estado_pedido') == 'ENTREGADO' and exist.get('estado_pedido') != 'ENTREGADO':
                    docs_v3_por_llave[llave] = doc

    nombres_v3 = [
        (doc.get('cliente_destino', ''), doc['llave'])
        for doc in registros_v3 if doc.get('llave') and doc.get('cliente_destino')
    ]
    nombres_v3_strs = [n for n, _ in nombres_v3]
    dict_telefonos_v3 = {
        _normalizar_cel(doc.get('telefono_original', '')): doc['llave']
        for doc in registros_v3
        if doc.get('llave') and len(_normalizar_cel(doc.get('telefono_original', ''))) >= 7
    }

    # ── Etapa 2: pacientes -> V3 ─────────────────────────────────────────────
    resultado_pacientes: list = []
    llaves_v3_con_paciente: set = set()
    paso_reporte = max(1, total_pacientes // 20)

    for idx, p in enumerate(pacientes):
        llave_paciente = p.get('llave', '') or ''
        if not llave_paciente:
            continue

        cedi_raw = p.get('cedi', '') or ''
        cedi = _CEDI_MAPA.get(cedi_raw.upper(), cedi_raw.upper())
        paciente_norm = p.get('paciente', '') or ''
        en_v3 = False
        llave_v3_match = ''
        match_tipo = None
        similitud = 0.0

        # Criterio 1: nombre >= 95%
        if paciente_norm and nombres_v3_strs:
            res_n = _fuzz_process.extractOne(
                paciente_norm, nombres_v3_strs, scorer=fuzz_ratio, score_cutoff=95
            )
            if res_n:
                en_v3 = True
                llave_v3_match = nombres_v3[res_n[2]][1]
                match_tipo = 'nombre'
                similitud = round(res_n[1], 1)

        # Criterio 2: llave >= 73%
        if not en_v3 and llave_paciente and llaves_v3:
            res_l = _fuzz_process.extractOne(llave_paciente, llaves_v3, scorer=fuzz_ratio)
            if res_l:
                similitud = round(res_l[1], 1)
                llave_v3_match = res_l[0]
                if res_l[1] >= 73:
                    en_v3 = True
                    match_tipo = 'llave'

        # Criterio 3: celular exacto
        if not en_v3 and dict_telefonos_v3:
            tel1 = _normalizar_cel(p.get('telefono1', '') or '')
            tel2 = _normalizar_cel(p.get('telefono2', '') or '')
            celular_p = next(
                (t for t in (tel1, tel2) if len(t) >= 7 and t in dict_telefonos_v3), ''
            )
            if celular_p:
                en_v3 = True
                llave_v3_match = dict_telefonos_v3[celular_p]
                match_tipo = 'celular'

        if en_v3 and llave_v3_match:
            llaves_v3_con_paciente.add(llave_v3_match)

        doc_v3 = docs_v3_por_llave.get(llave_v3_match, {}) if (en_v3 and llave_v3_match) else {}
        paciente_result = {
            'paciente':           p.get('paciente_original', ''),
            'cedula':             p.get('cedula_original', ''),
            'direccion_original': p.get('direccion_original', ''),
            'ruta':               p.get('ruta', '') or 'SIN RUTA',
            'cedi':               cedi,
            'llave':              llave_paciente,
            'similitud':          similitud,
            'match_tipo':         match_tipo,
            'llave_v3':           llave_v3_match,
            'en_v3':              en_v3,
            'estado':             p.get('estado', 'ACTIVO'),
            'estado_pedido':      doc_v3.get('estado_pedido', ''),
            'fecha_pedido':       _fmt_fecha(doc_v3.get('fecha_pedido')),
            'fecha_preferente':   _fmt_fecha(doc_v3.get('fecha_preferente')),
            'fecha_entrega':      _fmt_fecha(doc_v3.get('fecha_entrega')),
            'planilla':           doc_v3.get('planilla', ''),
            'municipio_destino':  doc_v3.get('municipio_destino', ''),
            'divipola':           doc_v3.get('divipola', ''),
            'ruta_v3':            doc_v3.get('ruta', ''),
            'cliente_destino_v3': doc_v3.get('cliente_destino_original', ''),
            'celular_paciente':   ' / '.join(filter(None, [
                                      p.get('telefono1', '') or '',
                                      p.get('telefono2', '') or '',
                                  ])),
            'telefono_v3':        doc_v3.get('telefono_original', ''),
            'f_pref_teorica':     cronograma_dict.get(p.get('cedula', ''), ''),
            'cant_pedidos_v3':    contador_pedidos_por_llave.get(llave_v3_match, 0)
                                  if (en_v3 and llave_v3_match) else 0,
        }
        try:
            paciente_result['estado_cruce'] = _determinar_estado_cruce(
                en_v3=en_v3,
                estado_pedido=paciente_result['estado_pedido'],
                f_pref_teorica=paciente_result['f_pref_teorica'],
                f_pedido=paciente_result['fecha_pedido'],
            )
        except Exception:
            paciente_result['estado_cruce'] = '—'
        resultado_pacientes.append(paciente_result)

        if (idx + 1) % paso_reporte == 0 or (idx + 1) == total_pacientes:
            pct = round(10 + ((idx + 1) / total_pacientes) * 50)
            yield {
                'stage': 'comparing_patients', 'progress': pct,
                'processed': idx + 1, 'total': total_pacientes,
                'message': f'Paciente {idx + 1} de {total_pacientes}',
            }

    # ── Agrupar ocupación por ruta ───────────────────────────────────────────
    rutas_ocupacion: dict = {}
    for p in resultado_pacientes:
        ruta = p['ruta']
        if ruta not in rutas_ocupacion:
            rutas_ocupacion[ruta] = {
                'pacientes': [], 'total': 0, 'en_v3': 0,
                'entregados': 0, 'cedi': p['cedi'], 'planillas': set(),
            }
        rutas_ocupacion[ruta]['pacientes'].append(p)
        rutas_ocupacion[ruta]['total'] += 1
        if p['en_v3']:
            rutas_ocupacion[ruta]['en_v3'] += 1
        if p['en_v3'] and p.get('estado_pedido') == 'ENTREGADO':
            rutas_ocupacion[ruta]['entregados'] += 1
        if p.get('planilla'):
            rutas_ocupacion[ruta]['planillas'].add(p['planilla'])

    ocupacion_resultado = []
    for ruta, datos in sorted(rutas_ocupacion.items()):
        total = datos['total']
        en_v3 = datos['en_v3']
        entregados = datos['entregados']
        ocupacion_resultado.append({
            'ruta':                ruta,
            'cedi':                datos['cedi'],
            'total_pacientes':     total,
            'pacientes_en_v3':     en_v3,
            'ocupacion_pct':       round(en_v3 / total * 100, 1) if total else 0.0,
            'pacientes_entregados': entregados,
            'pct_entregados':      round(entregados / total * 100, 1) if total else 0.0,
            'vehiculos':           len(datos['planillas']),
            'pacientes':           sorted(datos['pacientes'],
                                          key=lambda x: (not x['en_v3'], -x['similitud'])),
        })

    # ── Etapa 3: V3 sin paciente / zona gris / llave vacía ──────────────────
    llaves_pacientes = [p['llave'] for p in resultado_pacientes if p.get('llave')]
    sin_paciente: list = []
    zona_gris: list = []
    llave_vacia: list = []
    paso_reporte_v3 = max(1, total_v3 // 20)

    yield {'stage': 'comparing_v3', 'progress': 62,
           'message': f'Verificando {total_v3} pedidos V3...'}

    for idx, reg in enumerate(registros_v3):
        llave_v3 = reg.get('llave', '') or ''
        bodega   = reg.get('bodega_origen', '') or ''
        cedi_v3  = _CEDI_MAPA.get(bodega.upper(), bodega.upper())

        reg_base = {
            'codigo_pedido':    reg.get('codigo_pedido', ''),
            'cliente_destino':  reg.get('cliente_destino_original', ''),
            'direccion_destino': reg.get('direccion_destino_original', ''),
            'ruta':             reg.get('ruta', '') or 'SIN RUTA',
            'cedi':             cedi_v3,
            'estado_pedido':    reg.get('estado_pedido', ''),
            'telefono':         reg.get('telefono_original', ''),
            'fecha_preferente': _fmt_fecha(reg.get('fecha_preferente')),
            'llave':            llave_v3,
        }

        if not llave_v3:
            llave_vacia.append(reg_base)
        elif llave_v3 in llaves_v3_con_paciente:
            pass  # reclamado por un paciente
        else:
            res_sp = (
                _fuzz_process.extractOne(llave_v3, llaves_pacientes, scorer=fuzz_ratio)
                if llaves_pacientes else None
            )
            mejor_sim = res_sp[1] / 100.0 if res_sp else 0.0
            mejor_llave_p = res_sp[0] if res_sp else ''
            reg_con_sim = {
                **reg_base,
                'similitud': round(mejor_sim * 100, 1),
                'llave_paciente_cercana': mejor_llave_p,
            }
            if mejor_sim < 0.75:
                sin_paciente.append(reg_con_sim)
            else:
                zona_gris.append(reg_con_sim)

        if (idx + 1) % paso_reporte_v3 == 0 or (idx + 1) == total_v3:
            pct = round(62 + ((idx + 1) / total_v3) * 28)
            yield {
                'stage': 'comparing_v3', 'progress': pct,
                'processed': idx + 1, 'total': total_v3,
                'message': f'V3 {idx + 1} de {total_v3}',
            }

    def _agrupar_por_ruta(lista: list) -> list:
        rutas: dict = {}
        for reg in lista:
            ruta = reg['ruta']
            if ruta not in rutas:
                rutas[ruta] = {'registros': [], 'cedi': reg['cedi']}
            rutas[ruta]['registros'].append(reg)
        return [
            {
                'ruta': ruta, 'cedi': datos['cedi'], 'total': len(datos['registros']),
                'registros': sorted(datos['registros'],
                                    key=lambda x: x.get('similitud', 0), reverse=True),
            }
            for ruta, datos in sorted(rutas.items())
        ]

    yield {'stage': 'saving', 'progress': 95, 'message': 'Preparando resultados...'}

    yield {
        'stage': 'complete',
        'result': {
            'ocupacion_resultado':  ocupacion_resultado,
            'v3_sin_paciente':      _agrupar_por_ruta(sin_paciente),
            'v3_zona_gris':         _agrupar_por_ruta(zona_gris),
            'v3_llave_vacia':       llave_vacia,
            'total_sin_paciente':   len(sin_paciente),
            'total_zona_gris':      len(zona_gris),
            'total_llave_vacia':    len(llave_vacia),
        },
    }

'''

EJECUTAR_CRUCE_AUTO = r'''
def ejecutar_cruce_automatico(usuario: str = 'sync_automatico') -> dict:
    """
    Ejecuta el cruce completo pacientes <-> V3 y guarda en cache_cruce_mc.
    Llamado automáticamente tras cada sync_v3 exitoso. No usa SSE.
    Usa _motor_cruce como motor central (misma lógica que el endpoint SSE).
    """
    import logging, threading
    logger = logging.getLogger(__name__)

    try:
        cronograma_dict = _get_cronograma_mes_actual()
        pacientes = list(coleccion.find(
            {},
            {'llave': 1, 'paciente': 1, 'paciente_original': 1, 'direccion_original': 1,
             'ruta': 1, 'estado': 1, 'cedula': 1, 'cedula_original': 1, 'cedi': 1,
             'telefono1': 1, 'telefono2': 1}
        ))
        coleccion_v3 = bd['v3']
        registros_v3 = list(coleccion_v3.find(
            {'llave': {'$exists': True}},
            {'llave': 1, 'cliente_destino': 1, 'cliente_destino_original': 1,
             'direccion_destino_original': 1, 'ruta': 1, 'estado_pedido': 1,
             'codigo_pedido': 1, 'bodega_origen': 1, 'telefono_original': 1,
             'fecha_pedido': 1, 'fecha_preferente': 1,
             'fecha_entrega': 1, 'planilla': 1,
             'divipola': 1, 'municipio_destino': 1}
        ))

        result = None
        for event in _motor_cruce(pacientes, registros_v3, cronograma_dict):
            if event.get('stage') == 'complete':
                result = event['result']

        if not result:
            raise RuntimeError('_motor_cruce no retornó resultado')

        fecha_calculo = time.strftime('%Y-%m-%d %H:%M:%S')
        coleccion_cache.update_one(
            {'tipo': 'cruce_completo'},
            {'$set': {
                'tipo':               'cruce_completo',
                'ocupacion_rutas':    result['ocupacion_resultado'],
                'v3_sin_paciente':    result['v3_sin_paciente'],
                'v3_zona_gris':       result['v3_zona_gris'],
                'v3_llave_vacia':     result['v3_llave_vacia'],
                'total_sin_paciente': result['total_sin_paciente'],
                'total_zona_gris':    result['total_zona_gris'],
                'total_llave_vacia':  result['total_llave_vacia'],
                'calculado_por':      usuario,
                'fecha_calculo':      fecha_calculo,
            }},
            upsert=True
        )
        logger.info(
            f"[cruce_automatico] OK — {len(pacientes)} pacientes, "
            f"{result['total_sin_paciente']} sin paciente, "
            f"{result['total_zona_gris']} zona gris, "
            f"{result['total_llave_vacia']} llave vacía"
        )
        threading.Thread(
            target=enviar_excel_cruce_por_correo,
            args=(usuario, fecha_calculo),
            daemon=True
        ).start()
        return {
            'ok':                 True,
            'total_pacientes':    len(pacientes),
            'total_sin_paciente': result['total_sin_paciente'],
            'total_zona_gris':    result['total_zona_gris'],
            'total_llave_vacia':  result['total_llave_vacia'],
            'fecha_calculo':      fecha_calculo,
        }

    except Exception as e:
        logger.error(f"[cruce_automatico] Error: {e}")
        return {'ok': False, 'error': str(e)}

'''

RECALCULAR_ENDPOINT = r'''
@router.post("/recalcular-cruce")
async def recalcular_cruce(usuario: str, enviar_correo: bool = True):
    """
    Ejecuta el cruce pacientes <-> V3 con progreso en tiempo real via SSE.
    Usa _motor_cruce como motor central (misma lógica que ejecutar_cruce_automatico).
    """
    import logging, threading
    logger = logging.getLogger(__name__)
    logger.info(f"[/recalcular-cruce] usuario={usuario}, enviar_correo={enviar_correo}")

    def generar_eventos():
        try:
            yield f"data: {json.dumps({'stage': 'loading', 'progress': 0, 'message': 'Cargando pacientes y pedidos V3...'})}\n\n"

            cronograma_dict = _get_cronograma_mes_actual()
            pacientes = list(coleccion.find(
                {},
                {'llave': 1, 'paciente': 1, 'paciente_original': 1, 'direccion_original': 1,
                 'ruta': 1, 'estado': 1, 'cedula': 1, 'cedula_original': 1, 'cedi': 1,
                 'telefono1': 1, 'telefono2': 1}
            ))
            coleccion_v3 = bd['v3']
            registros_v3 = list(coleccion_v3.find(
                {'llave': {'$exists': True}},
                {'llave': 1, 'cliente_destino': 1, 'cliente_destino_original': 1,
                 'direccion_destino_original': 1, 'ruta': 1, 'estado_pedido': 1,
                 'codigo_pedido': 1, 'bodega_origen': 1, 'telefono_original': 1,
                 'fecha_pedido': 1, 'fecha_preferente': 1,
                 'fecha_entrega': 1, 'planilla': 1,
                 'divipola': 1, 'municipio_destino': 1}
            ))

            for event in _motor_cruce(pacientes, registros_v3, cronograma_dict):
                if event.get('stage') == 'complete':
                    result = event['result']
                    fecha_calculo = time.strftime('%Y-%m-%d %H:%M:%S')
                    coleccion_cache.update_one(
                        {'tipo': 'cruce_completo'},
                        {'$set': {
                            'tipo':               'cruce_completo',
                            'ocupacion_rutas':    result['ocupacion_resultado'],
                            'v3_sin_paciente':    result['v3_sin_paciente'],
                            'v3_zona_gris':       result['v3_zona_gris'],
                            'v3_llave_vacia':     result['v3_llave_vacia'],
                            'total_sin_paciente': result['total_sin_paciente'],
                            'total_zona_gris':    result['total_zona_gris'],
                            'total_llave_vacia':  result['total_llave_vacia'],
                            'calculado_por':      usuario,
                            'fecha_calculo':      fecha_calculo,
                        }},
                        upsert=True
                    )
                    if enviar_correo:
                        threading.Thread(
                            target=enviar_excel_cruce_por_correo,
                            args=(usuario, fecha_calculo),
                            daemon=True
                        ).start()
                    logger.info(
                        f"[recalcular-cruce] Completado. fecha={fecha_calculo}, "
                        f"pacientes={len(pacientes)}, "
                        f"sin_paciente={result['total_sin_paciente']}, "
                        f"zona_gris={result['total_zona_gris']}, "
                        f"llave_vacia={result['total_llave_vacia']}"
                    )
                    yield f"data: {json.dumps({'stage': 'complete', 'progress': 100, 'message': 'Cruce completado', 'rutas': result['ocupacion_resultado'], 'v3_sin_paciente': result['v3_sin_paciente'], 'v3_zona_gris': result['v3_zona_gris'], 'v3_llave_vacia': result['v3_llave_vacia'], 'total_sin_paciente': result['total_sin_paciente'], 'total_zona_gris': result['total_zona_gris'], 'total_llave_vacia': result['total_llave_vacia'], 'fecha_calculo': fecha_calculo, 'calculado_por': usuario})}\n\n"
                else:
                    yield f"data: {json.dumps(event)}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

    return StreamingResponse(generar_eventos(), media_type="text/event-stream")

'''

OCUPACION_ENDPOINT = r'''
@router.get("/ocupacion-rutas")
async def ocupacion_rutas():
    """
    Retorna el último resultado de ocupación por rutas guardado en cache.
    Para recalcular usar POST /recalcular-cruce.
    """
    cache = coleccion_cache.find_one({'tipo': 'cruce_completo'}, {'_id': 0})
    if not cache:
        return {
            'rutas': [], 'fecha_calculo': None, 'calculado_por': None,
            'total_sin_paciente': 0, 'total_zona_gris': 0, 'total_llave_vacia': 0,
        }
    return {
        'rutas':              cache.get('ocupacion_rutas', []),
        'fecha_calculo':      cache.get('fecha_calculo'),
        'calculado_por':      cache.get('calculado_por'),
        'total_sin_paciente': cache.get('total_sin_paciente', 0),
        'total_zona_gris':    cache.get('total_zona_gris', 0),
        'total_llave_vacia':  cache.get('total_llave_vacia', 0),
    }

'''

V3SIN_ENDPOINT = r'''
@router.get("/v3-sin-paciente")
async def v3_sin_paciente():
    """
    Retorna el último resultado de V3 sin paciente guardado en cache.
    Incluye zona_gris (similitud >= 75% pero sin reclamar) y llave_vacia.
    Para recalcular usar POST /recalcular-cruce.
    """
    cache = coleccion_cache.find_one({'tipo': 'cruce_completo'}, {'_id': 0})
    if not cache:
        return {
            'total_sin_paciente': 0, 'rutas': [],
            'v3_zona_gris': [], 'total_zona_gris': 0,
            'v3_llave_vacia': [], 'total_llave_vacia': 0,
            'fecha_calculo': None, 'calculado_por': None,
        }
    return {
        'total_sin_paciente': cache.get('total_sin_paciente', 0),
        'rutas':              cache.get('v3_sin_paciente', []),
        'v3_zona_gris':       cache.get('v3_zona_gris', []),
        'total_zona_gris':    cache.get('total_zona_gris', 0),
        'v3_llave_vacia':     cache.get('v3_llave_vacia', []),
        'total_llave_vacia':  cache.get('total_llave_vacia', 0),
        'fecha_calculo':      cache.get('fecha_calculo'),
        'calculado_por':      cache.get('calculado_por'),
    }

'''

# ── Aplicar cambios ───────────────────────────────────────────────────────────

# 1. Reemplazar _calcular_cruce (muerto) + ejecutar_cruce_automatico (duplicado)
#    con _motor_cruce + ejecutar_cruce_automatico unificado

# Marcador de inicio: def _calcular_cruce():
# Marcador de fin: linea justo antes de @router.post("/recalcular-cruce")

start_marker = '\ndef _calcular_cruce():'
end_marker_after = '\n@router.post("/recalcular-cruce")'

idx_start = src.find(start_marker)
idx_end   = src.find(end_marker_after)

if idx_start == -1 or idx_end == -1:
    print(f"ERROR: marcadores no encontrados. idx_start={idx_start}, idx_end={idx_end}")
    sys.exit(1)

# Reemplazar todo entre (incluyendo) _calcular_cruce hasta (excluyendo) @router.post("/recalcular-cruce")
src = src[:idx_start] + MOTOR_CRUCE + EJECUTAR_CRUCE_AUTO + src[idx_end:]
print("✓ _calcular_cruce eliminado, _motor_cruce + ejecutar_cruce_automatico insertados")

# 2. Reemplazar endpoint /recalcular-cruce completo
start_recalcular = '\n@router.post("/recalcular-cruce")'
end_recalcular   = '\n@router.get("/ocupacion-rutas")'

idx_start = src.find(start_recalcular)
idx_end   = src.find(end_recalcular)

if idx_start == -1 or idx_end == -1:
    print(f"ERROR: marcadores recalcular no encontrados. idx_start={idx_start}, idx_end={idx_end}")
    sys.exit(1)

src = src[:idx_start] + RECALCULAR_ENDPOINT + src[idx_end:]
print("✓ /recalcular-cruce endpoint reemplazado")

# 3. Reemplazar /ocupacion-rutas endpoint
start_ocup = '\n@router.get("/ocupacion-rutas")'
end_ocup   = '\n@router.get("/v3-sin-paciente")'

idx_start = src.find(start_ocup)
idx_end   = src.find(end_ocup)

if idx_start == -1 or idx_end == -1:
    print(f"ERROR: marcadores ocupacion-rutas no encontrados. idx_start={idx_start}, idx_end={idx_end}")
    sys.exit(1)

src = src[:idx_start] + OCUPACION_ENDPOINT + src[idx_end:]
print("✓ /ocupacion-rutas endpoint actualizado")

# 4. Reemplazar /v3-sin-paciente endpoint
start_v3sin = '\n@router.get("/v3-sin-paciente")'
end_v3sin   = '\ndef _generar_excel_bytes'

idx_start = src.find(start_v3sin)
idx_end   = src.find(end_v3sin)

if idx_start == -1 or idx_end == -1:
    print(f"ERROR: marcadores v3-sin-paciente no encontrados. idx_start={idx_start}, idx_end={idx_end}")
    sys.exit(1)

src = src[:idx_start] + V3SIN_ENDPOINT + src[idx_end:]
print("✓ /v3-sin-paciente endpoint actualizado")

# ── Escribir resultado ────────────────────────────────────────────────────────
with open(FILE, 'w', encoding='utf-8') as f:
    f.write(src)

print(f"\n✅ Migración completada. Líneas resultantes: {src.count(chr(10))}")
