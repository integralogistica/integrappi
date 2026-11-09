"""Sonda de la API Monitor (Mayasoft) — posición GPS del vehículo de una placa.

Prueba las credenciales del web service (MONITOR_USER/MONITOR_PASSWORD del
.env — las entrega Mayasoft, NO valen las del portal web) y el flujo completo
de Funciones/monitor_ws.py: login + token cacheado + consulta por placa +
selección del manifiesto vigente. Si se pasa una GUÍA en vez de una placa,
resuelve la placa en informe_guias_tms (como lo hace la Trazabilidad del
portal) — útil para replicar exactamente lo que verá el cliente.

Uso (desde integrappi/):
    python scripts/probar_monitor.py TRN399            # por placa
    python scripts/probar_monitor.py 999123            # por guía (resuelve placa en el TMS)
    MONITOR_AUTH_SCHEME=Bearer python scripts/probar_monitor.py TRN399
                                                          # si Mayasoft pide Bearer
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

# ⚠️ monitor_ws lee las env vars AL IMPORTAR: el load_dotenv de arriba debe
# ejecutarse primero (por eso el import va después, contrario a la costumbre).
from Funciones import monitor_ws  # noqa: E402


PLACA_PATTERN = __import__("re").compile(r"^[A-Z0-9]{4,6}$")


def es_placa(valor: str) -> bool:
    v = valor.strip().upper()
    return bool(PLACA_PATTERN.match(v)) and any(c.isalpha() for c in v)


def resolver_placa(valor: str) -> str | None:
    """Si el argumento es una guía (no placa), saca la placa del TMS — mismo
    paso que hace get_consulta_guia en producción."""
    if es_placa(valor):
        return valor.strip().upper()
    from bd.bd_postgres import guia_tms_placa
    tms = guia_tms_placa(valor.strip())
    if not tms:
        print(f"✗ La guía {valor} no existe en informe_guias_tms (¿número correcto?).")
        return None
    placa = (tms.get("placa") or "").strip()
    print(f"Guía {valor}: estado TMS «{tms.get('estado') or '—'}» · placa «{placa or 'SIN PLACA'}»")
    return placa or None


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("valor", help="Placa (ej. TRN399) o número de guía del TMS")
    args = parser.parse_args()

    print(f"Base URL : {monitor_ws.MONITOR_BASE_URL}")
    print(f"Usuario  : {monitor_ws.MONITOR_USER or '(VACÍO — falta MONITOR_USER)'}")
    print(f"Esquema  : {monitor_ws.MONITOR_AUTH_SCHEME or 'token plano'}")
    if not monitor_ws.MONITOR_USER or not monitor_ws.MONITOR_PASSWORD:
        print("\n✗ Faltan MONITOR_USER / MONITOR_PASSWORD en el .env — pídele las credenciales del WS a Mayasoft.")
        return 2

    placa = resolver_placa(args.valor)
    if not placa:
        return 2
    print(f"\n→ Consultando Monitor por la placa {placa}…\n")

    r = await monitor_ws.consultar_vehiculo_monitor(placa)
    if not r.get("ok"):
        if r.get("sin_credenciales"):
            print("✗ Sin credenciales configuradas.")
        else:
            print(f"✗ Falló la consulta: {r.get('error')}")
            print("  (si es login 400: las credenciales no son las del WS;")
            print("   si es 401/403 en la consulta: prueba MONITOR_AUTH_SCHEME=Bearer)")
        return 1
    if not r.get("existe"):
        print(f"✓ Login OK, pero la placa {placa} no tiene despachos en Monitor (nadie la está monitoreando ahora).")
        return 0

    v = r["vehiculo"]
    print("✓ POSICIÓN ENCONTRADA\n" + "─" * 52)
    print(f"  Placa        : {v['placa']}")
    print(f"  Manifiesto   : {v['manifiesto']} · {v['estado_viaje']}")
    print(f"  Conductor    : {v['conductor'] or '—'}")
    print(f"  Ubicación    : {v['ubicacion'] or '—'}")
    print(f"  Coordenadas  : {v['lat']}, {v['lng']}")
    print(f"  Velocidad    : {v['velocidad']} km/h · GPS {v['estado_gps'] or '—'}")
    print(f"  Reporte GPS  : {v['fecha_reporte_gps'] or '—'}")
    print(f"  ETA          : {v['eta_fecha'] or '—'}" + (f" · {v['eta_distancia_km']} km" if v.get("eta_distancia_km") is not None else ""))
    if v["lat"] is not None and v["lng"] is not None:
        print(f"  Mapa         : https://www.google.com/maps?q={v['lat']},{v['lng']}")
    print("─" * 52)
    print("\nSi esto salió bien: levanta el backend (uvicorn main:app) y consulta la")
    print("guía en el portal (pestaña 🧭 Trazabilidad) — el panel 🚛 debe aparecer")
    print("mientras la guía NO esté entregada.")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
