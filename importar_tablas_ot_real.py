# (2026-10-31) Importación de TODAS las tablas del Excel real del OT
# (`calculo real ot.xlsx`) a Mongo base `integra`. Idempotente (delete+insert):
# re-ejecutar cuando el área actualice el Excel.
#
# ⚠️ (2026-11-12) DEPRECATED — NO EJECUTAR. El Excel fue ELIMINADO del repo y
# **Mongo es la fuente de verdad** de las 5 tablas: los cambios van DIRECTO en
# las colecciones (ej. PUERTO CONCORDIA/META/3 días agregado a tiempos_entrega,
# caso guía 801233225). Este script hace DELETE+INSERT: si consiguiera un Excel
# viejo y se ejecutara, BORRARÍA toda edición manual. Queda solo como referencia
# de cómo se cargaron originalmente las tablas (respaldo histórico en git:
# `calculo real ot.xlsx` en el historial del repo).
#
#   tiempos_entrega      ← hoja TIEMPOS DE ENTREGA (CARGA, 189 municipios)
#   tiempos_entrega_st   ← hoja TIEMPOS DE ENTREGA ST (envío directo/recolección)
#   correcciones_nombres ← hoja CORRECCIONES (ciudades/dptos mal escritos)
#   novedades_siscore    ← hoja NOVEDADES (reporte de Siscore: guía → MOTIVO)
#   citas_agendadas      ← hoja CITASACT (guía → fecha de cita; VLOOKUP primera)
#
# Uso: python importar_tablas_ot_real.py
import re
import warnings
import unicodedata
from datetime import datetime

warnings.filterwarnings("ignore")
import openpyxl
from bd.bd_cliente import bd_cliente

RUTA = r"C:\Users\ASUS\OneDrive - Integra Logistica\Desarrollos\integra\integrappi\calculo real ot.xlsx"
ISO = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")


def norm(t):
    s = unicodedata.normalize("NFD", str(t or "").upper().replace("Ñ", "N"))
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", s).strip()


def num(v):
    if v is None or (isinstance(v, str) and v.strip() in ("#N/A", "", "#REF!")):
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


wb = openpyxl.load_workbook(RUTA, data_only=True)
db = bd_cliente["integra"]
ts = datetime.now().isoformat()

# ── tiempos_entrega (CARGA) ──
te = []
for fila in wb["TIEMPOS DE ENTREGA"].iter_rows(min_row=2, values_only=True):
    municipio, dpto, tipo, dias_cm, dias_2026 = fila[1], fila[2], fila[3], fila[4], fila[5]
    if not municipio or not dpto:
        continue
    te.append({"municipio": norm(municipio), "departamento": norm(dpto),
               "tipo_servicio": norm(tipo) or "NO ES ST",
               "dias_cm": num(dias_cm), "dias_2026": num(dias_2026)})
db["tiempos_entrega"].delete_many({})
db["tiempos_entrega"].insert_many(te)
print(f"tiempos_entrega: {len(te)} ({sum(1 for d in te if d['dias_2026'] is not None)} con días 2026)")

# ── tiempos_entrega_st ──
st = []
for fila in wb["TIEMPOS DE ENTREGA ST"].iter_rows(min_row=2, values_only=True):
    origen, destino, dpto, zona, tipo, d_envio, d_reco = fila[1], fila[2], fila[3], fila[4], fila[5], fila[6], fila[7]
    if not dpto or not tipo:
        continue
    tipo_n = norm(tipo)
    municipio = destino if tipo_n == "ENVIO DIRECTO" else origen
    if not municipio:
        continue
    st.append({"municipio": norm(municipio), "departamento": norm(dpto), "tipo": tipo_n,
               "zona": norm(zona), "dias_envio": num(d_envio), "dias_recoleccion": num(d_reco)})
db["tiempos_entrega_st"].delete_many({})
db["tiempos_entrega_st"].insert_many(st)
print(f"tiempos_entrega_st: {len(st)} "
      f"({sum(1 for d in st if d['tipo'] == 'ENVIO DIRECTO')} envío / "
      f"{sum(1 for d in st if d['tipo'] == 'RECOLECCION')} recolección)")

# ── correcciones_nombres ──
cor = []
for fila in wb["CORRECCIONES"].iter_rows(min_row=2, values_only=True):
    if fila[0] and fila[1]:
        cor.append({"error": norm(fila[0]), "correccion": norm(fila[1])})
db["correcciones_nombres"].delete_many({})
db["correcciones_nombres"].insert_many(cor)
print(f"correcciones_nombres: {len(cor)}")

# ── novedades_siscore (guía → MOTIVO, fórmula BP) ──
nov = []
for fila in wb["NOVEDADES"].iter_rows(min_row=2, values_only=True):
    guia, novedad, motivo = fila[0], fila[10], fila[12]
    if not guia or not motivo:
        continue
    nov.append({"guia": str(guia).strip(), "motivo": norm(motivo), "novedad": norm(novedad),
                "emision": str(fila[9] or "")[:10], "importado": ts})
db["novedades_siscore"].delete_many({})
db["novedades_siscore"].insert_many(nov)
print(f"novedades_siscore: {len(nov)}")

# ── citas_agendadas (CITASACT, fórmula BN — primera aparición por guía) ──
docs, sin_fecha = [], 0
for fila in wb["CITASACT"].iter_rows(min_row=2, values_only=True):
    guia, fecha = fila[0], fila[4]
    if not guia:
        continue
    if hasattr(fecha, "strftime"):
        iso = fecha.strftime("%Y-%m-%d")
    elif fecha and ISO.match(str(fecha).strip()):
        iso = str(fecha).strip()[:10]
    else:
        sin_fecha += 1  # 'ELIMINADA' y similares
        continue
    docs.append({"guia": str(guia).strip(), "fecha_cita": iso,
                 "pedir_cita": str(fila[3] or "").strip(), "importado": ts})
vistos, unicos = set(), []
for d in docs:
    if d["guia"] not in vistos:
        vistos.add(d["guia"])
        unicos.append(d)
c = db["citas_agendadas"]
c.delete_many({})
c.insert_many(unicos)
c.create_index("guia", unique=True)
print(f"citas_agendadas: {len(unicos)} ({sin_fecha} sin fecha útil, {len(docs) - len(unicos)} duplicados)")
print("\nImportación completa:", ts)
