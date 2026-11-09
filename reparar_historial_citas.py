"""Reparación PUNTUAL (2026-11-09): el historial de `citas_entrega` (y
`novedades_justificadas_ot`) quedó guardado como dict literal
`{'$each': [...], '$slice': -20}` en vez de ARRAY, porque el código ponía el
`$each` dentro de `$set` (solo es válido en `$push`). Ya corregido en los
routers; este script convierte los dicts corruptos en las listas que debían
ser. Idempotente: si no hay docs corruptos, no hace nada.

Uso:  cd integrappi && python reparar_historial_citas.py
"""
from bd.bd_cliente import bd_cliente

db = bd_cliente["integra"]
total = 0
for nombre in ("citas_entrega", "novedades_justificadas_ot"):
    col = db[nombre]
    rotos = list(col.find({"historial": {"$type": "object"}}))
    for d in rotos:
        h = d.get("historial") or {}
        entradas = h.get("$each") if isinstance(h, dict) else None
        if entradas is None:
            print(f"[{nombre}] guia {d.get('guia')}: historial dict SIN $each — se omite (revisar a mano)")
            continue
        col.update_one({"_id": d["_id"]}, {"$set": {"historial": entradas}})
        print(f"[{nombre}] guia {d.get('guia')}: reparado ({len(entradas)} entrada(s))")
        total += 1
    print(f"[{nombre}] {len(rotos)} doc(s) con historial en forma de dict")
print(f"Listo: {total} documento(s) reparado(s).")
