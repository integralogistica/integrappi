# Drill-down de las 290 guías en PG pero NO en el Excel (dentro del rango del Excel)
import warnings
from collections import Counter

warnings.filterwarnings("ignore")
import openpyxl
from bd.bd_postgres import guias_minimas_cliente

RUTA = r"C:\Users\ASUS\OneDrive - Integra Logistica\Desarrollos\integra\integrappi\calculo real ot.xlsx"
wb = openpyxl.load_workbook(RUTA, data_only=True)
ws = wb["BASE"]
excel = set()
for fila in ws.iter_rows(min_row=2, values_only=True):
    if fila[0]:
        excel.add(str(fila[0]).strip())

nombres = ["FRESENIUS KABI", "FK SERVICIO TECNICO", "FRESENIUSKABISERVICIOTECNICO"]
filas = guias_minimas_cliente(nombres, [], [])
print(f"PG filas crudas: {len(filas)} (únicas: {len({str(f['guia']).strip() for f in filas})})")

solo = [f for f in filas
        if str(f["guia"]).strip() not in excel
        and "2026-01-02" <= str(f.get("fecha_emision") or "")[:10] <= "2026-10-29"]

print(f"\nEn PG pero NO en Excel, dentro del rango: {len(solo)}")
print("\nPor estado:")
for k, n in Counter(str(f.get("estado")) for f in solo).most_common():
    print(f"  {k}: {n}")
print("\nPor nombre_cliente:")
for k, n in Counter(str(f.get("nombre_cliente")) for f in solo).most_common():
    print(f"  {k}: {n}")
print("\nPor año-mes de emisión:")
for k, n in sorted(Counter(str(f.get("fecha_emision"))[:7] for f in solo).items()):
    print(f"  {k}: {n}")
print("\nDetalle completo (guia | emis | estado | nombre | ciudad_destino | destUsuario):")
for f in sorted(solo, key=lambda x: str(x.get("fecha_emision"))):
    print(f"  {f['guia']} | {str(f.get('fecha_emision'))[:10]} | {str(f.get('estado'))[:18]:18} | "
          f"{str(f.get('nombre_cliente'))[:12]:12} | {str(f.get('ciudad_destino'))[:20]:20} | "
          f"{str(f.get('destinatario'))[:25]}")
