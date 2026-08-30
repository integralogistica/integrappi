"""Genera el PDF del estudio de MARIA con el código ACTUAL y extrae las
coordenadas de las palabras de la tabla, para verificar si las columnas se
respetan o se invaden (diagnóstico directo, sin pasar por GCS)."""
import io
import sys

sys.path.insert(0, r"C:\Users\ASUS\OneDrive - Integra Logistica\Desarrollos\integra\integrappi")
from dotenv import load_dotenv

load_dotenv(r"C:\Users\ASUS\OneDrive - Integra Logistica\Desarrollos\integra\integrappi\.env")
from bd.bd_cliente import bd_cliente
from Funciones.pdf_estudio_seguridad import generar_pdf_estudio
import pdfplumber

doc = bd_cliente["integra"]["estudios_seguridad"].find_one({"consulta_id": "ES-BD7751400B24"})
doc.pop("_id", None)
contenido = generar_pdf_estudio(doc)

with pdfplumber.open(io.BytesIO(contenido)) as pdf:
    print("páginas:", len(pdf.pages))
    # Buscar la página de la tabla (donde está el radicado 123408537)
    for i, p in enumerate(pdf.pages):
        palabras = p.extract_words()
        texto = " ".join(w["text"] for w in palabras)
        if "123408537" not in texto:
            continue
        print(f"--- tabla en página {i+1} ---")
        # Agrupar palabras por línea (top) y mostrar rangos x0-x1 de cada palabra
        lineas = {}
        for w in palabras:
            clave = round(w["top"] / 6)
            lineas.setdefault(clave, []).append(w)
        # La fila del radicado: línea que contiene 123408537
        for clave, ws in sorted(lineas.items()):
            if any(w["text"] == "123408537" for w in ws):
                ws.sort(key=lambda w: w["x0"])
                for w in ws:
                    print(f"  x0={w['x0']:6.1f} x1={w['x1']:6.1f} | {w['text'][:40]}")
                break
        break
open(r"C:\Users\ASUS\AppData\Local\Temp\pdf_debug_tabla.pdf", "wb").write(contenido)
print("PDF guardado en C:\\Users\\ASUS\\AppData\\Local\\Temp\\pdf_debug_tabla.pdf")
