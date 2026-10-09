# -*- coding: utf-8 -*-
"""
Ejercicio Néstor: cruce por similitud textual entre Planeacion.xlsx y V3.xlsx.

- Para cada fila de Planeacion busca la fila más similar de V3 y escribe el
  porcentaje (0-100, 2 decimales) en la columna existente "estan en v3".
- Para cada fila de V3 busca la más similar de Planeacion y lo escribe en
  "esta en planeacion".
- Similitud = rapidfuzz.ratio sobre texto normalizado; 50% nombre + 50%
  dirección; si uno de los dos campos está vacío en cualquiera de los lados,
  el promedio se hace solo con el campo disponible; si ambos están vacíos = 0.
- Conserva filas, columnas y formatos: solo se escriben las celdas de la
  columna de resultado, con openpyxl sobre los libros originales guardados
  como copias (_comparado). Los originales no se tocan.
"""
import sys
from pathlib import Path

import numpy as np
from openpyxl import load_workbook
from rapidfuzz.fuzz import ratio as fuzz_ratio
from rapidfuzz.process import cdist

CARPETA = Path(r"C:\Users\ASUS\OneDrive - Integra Logistica\Desarrollos\integra\integrappi\EJERCICIO_NESTOR")
sys.path.insert(0, str(CARPETA.parent))  # para importar Funciones.*

from Funciones.normalizacion_medical_care import (  # noqa: E402
    fx_normalizar_paciente,
    fx_normalizar_direccion,
)

ARCH_PLA = CARPETA / "Planeacion.xlsx"
ARCH_V3 = CARPETA / "V3.xlsx"
SAL_PLA = CARPETA / "planeacion_comparado.xlsx"
SAL_V3 = CARPETA / "v3_comparado.xlsx"


def _norm(texto):
    if texto is None:
        return ""
    return (texto or "").strip()


def leer_datos(ruta, col_nombre, col_direccion, col_resultado):
    """Lee con openpyxl (conserva formato) y devuelve filas normalizadas."""
    wb = load_workbook(ruta)
    ws = wb.active
    header = {str(c.value).strip(): c.column for c in ws[1] if c.value is not None}
    i_nombre = header[col_nombre]
    i_dir = header[col_direccion]
    i_res = header[col_resultado]

    nombres, direcciones = [], []
    for row in ws.iter_rows(min_row=2, values_only=False):
        # detectar filas totalmente vacías (no son registros)
        if all(c.value in (None, "") for c in row):
            nombres.append("")
            direcciones.append("")
            continue
        nombre_raw = row[i_nombre - 1].value if len(row) >= i_nombre else None
        dir_raw = row[i_dir - 1].value if len(row) >= i_dir else None
        nombres.append(fx_normalizar_paciente(_norm(nombre_raw)) or "")
        direcciones.append(fx_normalizar_direccion(_norm(dir_raw)) or "")
    return wb, ws, i_res, nombres, direcciones


def matriz_similitud(a, b):
    """Matriz len(a) x len(b) de ratio 0-100 (float32)."""
    return cdist(a, b, scorer=fuzz_ratio, dtype=np.float32, workers=-1)


def combinar(sim_n, sim_d, nom_a, nom_b, dir_a, dir_b):
    """
    Promedio 50/50 de nombre y dirección; los campos vacíos (en cualquier
    lado) no cuentan como coincidencia ni influyen: el promedio se reparte
    entre los campos disponibles de la pareja. Ambos vacíos => 0.
    """
    usable_n = (nom_a[:, None] & nom_b[None, :])
    usable_d = (dir_a[:, None] & dir_b[None, :])
    w_n = usable_n.astype(np.float32) * 0.5
    w_d = usable_d.astype(np.float32) * 0.5
    total_w = w_n + w_d
    score = w_n * sim_n + w_d * sim_d
    out = np.where(total_w > 0, score / np.where(total_w == 0, 1, total_w), 0.0)
    return out.astype(np.float32)


def resumen(valores):
    v100 = int((valores >= 99.995).sum())
    v90 = int(((valores >= 90) & (valores < 99.995)).sum())
    vmenos = int((valores < 90).sum())
    return v100, v90, vmenos


def main():
    print("Cargando Planeacion.xlsx ...")
    wb_p, ws_p, i_res_p, nom_p, dir_p = leer_datos(
        ARCH_PLA, "NOMBRE PACIENTE", "DIRECCIÓN", "estan en v3")
    print(f"  {len(nom_p)} filas")

    print("Cargando V3.xlsx ...")
    wb_v, ws_v, i_res_v, nom_v, dir_v = leer_datos(
        ARCH_V3, "Cliente Destino", "Direccion Destino", "esta en planeacion")
    print(f"  {len(nom_v)} filas")

    nom_p_ok = np.array([bool(x) for x in nom_p])
    dir_p_ok = np.array([bool(x) for x in dir_p])
    nom_v_ok = np.array([bool(x) for x in nom_v])
    dir_v_ok = np.array([bool(x) for x in dir_v])

    print("Calculando similitud de nombres (matriz completa) ...")
    sim_nombres = matriz_similitud(nom_p, nom_v)
    print("Calculando similitud de direcciones (matriz completa) ...")
    sim_dirs = matriz_similitud(dir_p, dir_v)

    print("Combinando 50/50 con reglas de campos vacíos ...")
    combined = combinar(sim_nombres, sim_dirs, nom_p_ok, nom_v_ok, dir_p_ok, dir_v_ok)

    maximos_p = combined.max(axis=1)  # mejor V3 para cada planeacion
    maximos_v = combined.max(axis=0)  # mejor planeacion para cada v3
    mejor_v_por_p = combined.argmax(axis=1)   # índice del V3 con mayor score
    mejor_p_por_v = combined.argmax(axis=0)   # índice de la planeacion con mayor score

    # ── Texto original de la contraparte (para evaluación visual) ──
    # V3: columnas de nombre y dirección destino (encabezados con strip)
    hdr_v = {str(c.value).strip(): c.column for c in ws_v[1] if c.value is not None}
    i_nom_v = hdr_v["Cliente Destino"]
    i_dir_v = hdr_v["Direccion Destino"]
    nombres_v_orig, dirs_v_orig = [], []
    for row in ws_v.iter_rows(min_row=2, values_only=True):
        nombres_v_orig.append(_norm(row[i_nom_v - 1]) if len(row) >= i_nom_v else "")
        dirs_v_orig.append(_norm(row[i_dir_v - 1]) if len(row) >= i_dir_v else "")

    hdr_p = {str(c.value).strip(): c.column for c in ws_p[1] if c.value is not None}
    i_nom_p = hdr_p["NOMBRE PACIENTE"]
    i_dir_p = hdr_p["DIRECCIÓN"]
    nombres_p_orig, dirs_p_orig = [], []
    for row in ws_p.iter_rows(min_row=2, values_only=True):
        nombres_p_orig.append(_norm(row[i_nom_p - 1]) if len(row) >= i_nom_p else "")
        dirs_p_orig.append(_norm(row[i_dir_p - 1]) if len(row) >= i_dir_p else "")

    def _texto_match(pct, texto):
        return texto if pct > 0 else ""  # sin campos comparables => sin match

    # ── Escribir resultados (columna de % + columnas de coincidencia) ──
    print("Escribiendo planeacion_comparado.xlsx ...")
    col_nom = ws_p.max_column + 1
    col_dir = ws_p.max_column + 2
    ws_p.cell(row=1, column=col_nom, value="coincidió con (V3)")
    ws_p.cell(row=1, column=col_dir, value="dirección coincidencia (V3)")
    for fila, (pct, idx_v) in enumerate(zip(maximos_p, mejor_v_por_p), start=2):
        ws_p.cell(row=fila, column=i_res_p, value=round(float(pct), 2))
        ws_p.cell(row=fila, column=col_nom, value=_texto_match(pct, nombres_v_orig[idx_v]))
        ws_p.cell(row=fila, column=col_dir, value=_texto_match(pct, dirs_v_orig[idx_v]))
    wb_p.save(SAL_PLA)

    print("Escribiendo v3_comparado.xlsx ...")
    col_nom_v = ws_v.max_column + 1
    col_dir_v = ws_v.max_column + 2
    ws_v.cell(row=1, column=col_nom_v, value="coincidió con (planeacion)")
    ws_v.cell(row=1, column=col_dir_v, value="dirección coincidencia (planeacion)")
    for fila, (pct, idx_p) in enumerate(zip(maximos_v, mejor_p_por_v), start=2):
        ws_v.cell(row=fila, column=i_res_v, value=round(float(pct), 2))
        ws_v.cell(row=fila, column=col_nom_v, value=_texto_match(pct, nombres_p_orig[idx_p]))
        ws_v.cell(row=fila, column=col_dir_v, value=_texto_match(pct, dirs_p_orig[idx_p]))
    wb_v.save(SAL_V3)

    # ── Reporte ──
    for nombre, vals, total in (
        ("Planeacion (estan en v3)", maximos_p, len(nom_p)),
        ("V3 (esta en planeacion)", maximos_v, len(nom_v)),
    ):
        v100, v90, vmenos = resumen(vals)
        print(f"\n=== {nombre} — {total} registros ===")
        print(f"  100%              : {v100}")
        print(f"  >=90% y <100%     : {v90}")
        print(f"  <90%              : {vmenos}")
        print(f"  (filas sin nombre ni dirección: quedaron en 0)")

    print("\nListo:")
    print(f"  {SAL_PLA}")
    print(f"  {SAL_V3}")


if __name__ == "__main__":
    main()
