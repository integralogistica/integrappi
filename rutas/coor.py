import pandas as pd
from geopy.geocoders import Nominatim
from time import sleep
import os

# =========================
# CONFIG
# =========================
ARCHIVO_ENTRADA = r"C:\Users\ASUS\Downloads\coordenadas.xlsx"
ARCHIVO_SALIDA = r"C:\Users\ASUS\Downloads\coordenadas_completas.xlsx"
GUARDAR_CADA_N = 10          # guarda progreso cada N filas
SLEEP_SEGUNDOS = 1           # recomendado por Nominatim

# =========================
# CARGA EXCEL
# =========================
if not os.path.exists(ARCHIVO_ENTRADA):
    raise FileNotFoundError(f"No se encontró el archivo: {ARCHIVO_ENTRADA}")

df = pd.read_excel(ARCHIVO_ENTRADA)

# Normalizar nombres de columnas
df.columns = [str(c).strip() for c in df.columns]
print("Columnas detectadas:", df.columns.tolist())

# Buscar columnas requeridas (case-insensitive)
def buscar_columna(nombre_objetivo: str) -> str:
    objetivo = nombre_objetivo.strip().lower()
    for c in df.columns:
        if str(c).strip().lower() == objetivo:
            return c
    return ""

col_municipio = buscar_columna("Municipio Destino")
col_departamento = buscar_columna("Departamento Destino")

if not col_municipio or not col_departamento:
    raise ValueError(
        "No se encontraron las columnas requeridas.\n"
        "Se esperaba: 'Municipio Destino' y 'Departamento Destino'\n"
        f"Se encontró: {df.columns.tolist()}"
    )

# =========================
# GEO (OpenStreetMap / Nominatim)
# =========================
geolocator = Nominatim(user_agent="municipios_colombia_integra")

# Diagnóstico: probar 1 consulta para ver si hay conexión
print("Probando una consulta...")
prueba = geolocator.geocode("Jericó, Antioquia, Colombia", timeout=15)
print("Respuesta prueba:", prueba)

def obtener_coordenadas(municipio, departamento):
    try:
        if pd.isna(municipio) or pd.isna(departamento):
            return None

        query = f"{str(municipio).strip()}, {str(departamento).strip()}, Colombia"
        location = geolocator.geocode(query, timeout=15)

        if location:
            return float(location.latitude), float(location.longitude)

        return None
    except Exception as e:
        print("ERROR en geocode:", e)
        return None

# =========================
# PROCESO
# =========================
total = len(df)

# Si ya existe columna "coordenadas" con valores, se reutiliza para continuar
col_coordenadas_existente = buscar_columna("coordenadas")
if col_coordenadas_existente and df[col_coordenadas_existente].notna().any():
    print("Se detectaron coordenadas existentes. Se intentará continuar sin recalcular las ya llenas.")
else:
    col_coordenadas_existente = "coordenadas"
    if col_coordenadas_existente not in df.columns:
        df[col_coordenadas_existente] = None

for i in range(total):
    municipio = df.loc[i, col_municipio]
    departamento = df.loc[i, col_departamento]

    # Saltar si ya hay coordenadas
    if pd.notna(df.loc[i, col_coordenadas_existente]):
        print(f"[{i+1}/{total}] Ya tiene coordenadas -> {df.loc[i, col_coordenadas_existente]}")
        continue

    print(f"[{i+1}/{total}] Buscando: {municipio}, {departamento} ...")

    coord = obtener_coordenadas(municipio, departamento)

    if coord:
        lat, lon = coord
        df.loc[i, col_coordenadas_existente] = f"{lat},{lon}"
        print(f"    -> OK: {lat},{lon}")
    else:
        df.loc[i, col_coordenadas_existente] = None
        print("    -> No encontrado")

    # Guardado parcial
    if (i + 1) % GUARDAR_CADA_N == 0:
        df.to_excel(ARCHIVO_SALIDA, index=False)
        print("Guardado parcial:", ARCHIVO_SALIDA)

    sleep(SLEEP_SEGUNDOS)

# Guardado final
df.to_excel(ARCHIVO_SALIDA, index=False)
print("Archivo generado:", ARCHIVO_SALIDA)
