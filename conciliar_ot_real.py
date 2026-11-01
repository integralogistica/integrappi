# (2026-10-31) Conciliación: ¿nuestro OT nuevo llega al mismo veredicto que el
# Excel real? Cruza BASE (valores cacheados) por guía con _calcular_ot_filas.
import warnings
from collections import Counter

warnings.filterwarnings("ignore")
import openpyxl
from bd.bd_postgres import guias_minimas_cliente
from rutas.indicadores_cliente import (
    _normalizar_estado, _calcular_ot_filas, _adjuntar_novedades,
)

RUTA = r"C:\Users\ASUS\OneDrive - Integra Logistica\Desarrollos\integra\integrappi\calculo real ot.xlsx"

wb = openpyxl.load_workbook(RUTA, data_only=True)
ws = wb["BASE"]
excel = {}
for fila in ws.iter_rows(min_row=2, values_only=True):
    if not fila[0]:
        continue
    excel[str(fila[0]).strip()] = {
        "emision": str(fila[29] or "")[:10],
        "entrega": str(fila[34] or "")[:10],
        "tipo": fila[53],
        "dias": fila[60],
        "promesa": str(fila[61] or "")[:10],
        "cumpl": fila[62],
        "cita": fila[65],
        "novedades": fila[67],
    }


def veredicto_excel(v):
    if v is True: return "CUMPLE"
    if v is False: return "NO_CUMPLE"
    s = str(v or "").strip().upper()
    if s.startswith("CUMPLE CON NOVEDAD"): return "CON_NOVEDAD"
    if s == "NO APLICA": return "NO_APLICA"
    return f"OTRO:{s[:20]}" if s else "VACIO"


dist_excel = Counter(veredicto_excel(v["cumpl"]) for v in excel.values())
print(f"Excel BASE: {len(excel)} guías — veredictos: {dict(dist_excel)}")

nombres = ["FRESENIUS KABI", "FK SERVICIO TECNICO", "FRESENIUSKABISERVICIOTECNICO"]
filas = guias_minimas_cliente(nombres, [], [])
for f in filas:
    f["estado"], _ = _normalizar_estado(f.get("estado"))
_calcular_ot_filas(filas)
_adjuntar_novedades(filas)
nuestro = {}
for f in filas:
    if f.get("novedad") or f.get("cumple_novedad"):
        v = "CON_NOVEDAD"
    elif f.get("ot") == 1:
        v = "CUMPLE"
    elif f.get("ot") == 0:
        v = "NO_CUMPLE"
    else:
        v = "NO_APLICA"
    nuestro[str(f.get("guia")).strip()] = {"v": v, "f": f}

comunes = set(excel) & set(nuestro)
print(f"Coinciden por guía: {len(comunes)} · solo en Excel: {len(set(excel) - set(nuestro))}")


def reporte(subset, titulo):
    mat = Counter(); mism = []
    for g in subset:
        ve, vn = veredicto_excel(excel[g]["cumpl"]), nuestro[g]["v"]
        mat[(ve, vn)] += 1
        if ve != vn:
            mism.append(g)
    ok = sum(n for (ve, vn), n in mat.items() if ve == vn)
    print(f"\n== {titulo}: {ok}/{len(subset)} = {ok/len(subset)*100:.1f}% ==")
    for (ve, vn), n in sorted(mat.items(), key=lambda kv: -kv[1]):
        print(f"  {ve:14} → {vn:14} {n:6}{'' if ve == vn else '  ← DIFIEREN'}")
    return mism


entregadas = {g for g in comunes if excel[g]["entrega"] not in ("", "None", "0000-00-00")}
mism = reporte(entregadas, "ENTREGADAS (comparable estable)")
reporte(comunes - entregadas, "EN CURSO (HOY distinto al del Excel)")

print("\nMuestras de diferencias (guia | excel→nuestro | promesa E/N | dias E/N | tipo E→N | motivo):")
for g in mism[:25]:
    e, n = excel[g], nuestro[g]
    print(f"  {g} | {veredicto_excel(e['cumpl'])}→{n['v']} | {e['promesa']}/{n['f'].get('fecha_promesa')} | "
          f"{e['dias']}/{n['f'].get('dias_promesa')} | {str(e['tipo'])[:16]}→{n['f'].get('tipo_servicio')} | {str(e['novedades'])[:30]!r}")
