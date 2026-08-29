"""Extrae del portal oficial de test RNDC (WebForms) las variables del proceso 12.

Simula el postback de ASP.NET: GET de la página, relleno PROCESOID y pulsa
btProceso; la respuesta contiene la tabla de variables aceptadas por el
proceso. Sin credenciales (solo metadatos del formulario).
"""
import re
import sys

import httpx
from html import unescape

URL = "https://rndc.mintransporte.gov.co/wstest/default.aspx"


def extraer_campos(html: str) -> dict:
    campos = {}
    for m in re.finditer(r"<input[^>]*>", html, re.IGNORECASE):
        tag = m.group(0)
        name = re.search(r'name="([^"]+)"', tag)
        value = re.search(r'value="([^"]*)"', tag)
        ttype = re.search(r'type="([^"]+)"', tag, re.IGNORECASE)
        if name:
            campos[name.group(1)] = {
                "valor": unescape(value.group(1)) if value else "",
                "tipo": (ttype.group(1).lower() if ttype else "text"),
            }
    return campos


def main():
    proceso = sys.argv[1] if len(sys.argv) > 1 else "12"
    cliente = httpx.Client(timeout=httpx.Timeout(60, connect=15), follow_redirects=True)
    try:
        r = cliente.get(URL)
        r.raise_for_status()
        campos = extraer_campos(r.text)
        datos = {k: v["valor"] for k, v in campos.items() if v["tipo"] == "hidden"}
        datos.update({
            "PROCESOID": proceso,
            "USUARIO": "",
            "PASSWORD": "",
            "FechaInicial": "",
            "FechaFinal": "",
            "btProceso": "Proceso",  # "click" del botón
        })
        r2 = cliente.post(URL, data=datos, headers={"Referer": URL, "Content-Type": "application/x-www-form-urlencoded"})
        r2.raise_for_status()
        # La tabla de variables: filas con checkbox + nombre de variable
        texto = re.sub(r"<[^>]+>", "|", r2.text)
        texto = unescape(texto)
        variables = sorted(set(
            v for v in re.findall(r"[A-Z][A-Z0-9_]{3,30}", texto)
            if v not in {"VIEWSTATE", "VIEWSTATEGENERATOR", "VIEWSTATEENCRYPTED", "EVENTVALIDATION", "POSTBACK",
                         "ASPX", "HTTP", "XML", "PDF", "HTML", "JAVASCRIPT", "CSS", "UTF", "SERVER", "FRAMEWORK"}
        ))
        print(f"=== Variables candidatas del proceso {proceso} (portal test) ===")
        for v in variables:
            print(" -", v)
        # mostrar también el XML ejemplo si la página lo trae
        m = re.search(r"(&lt;root[\s\S]*?&lt;/root&gt;|<root[\s\S]*?</root>)", r2.text)
        if m:
            print("\n=== XML generado por el portal ===")
            print(unescape(re.sub(r"<[^>]+>", "", m.group(0)))[:3000])
    finally:
        cliente.close()


if __name__ == "__main__":
    main()
