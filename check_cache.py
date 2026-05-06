# -*- coding: utf-8 -*-
import sys
sys.stdout.reconfigure(encoding='utf-8')

from bd.bd_cliente import bd_cliente
import json

bd = bd_cliente['integra']
cache = bd['cache_cruce_mc'].find_one({'tipo': 'cruce_completo'}, {'_id': 0})

print('Has cache:', 'ocupacion_rutas' in cache if cache else False)

if cache and 'ocupacion_rutas' in cache:
    rutas = cache['ocupacion_rutas']
    if rutas and len(rutas) > 0:
        p = rutas[0].get('pacientes', [])
        if p and len(p) > 0:
            print('Has pacientes:', True)
            print('Has estado_cruce:', 'estado_cruce' in p[0])
            print('estado_cruce value:', p[0].get('estado_cruce', 'NO_EXISTE'))
            print('en_v3:', p[0].get('en_v3', 'NO_EXISTE'))
            pac = p[0].get('paciente', 'NO_EXISTE')
            if pac and pac != 'NO_EXISTE':
                print('paciente:', pac[:50])
            else:
                print('paciente: NO_EXISTE')
        else:
            print('No pacientes')
    else:
        print('No rutas')
else:
    print('No cache')
