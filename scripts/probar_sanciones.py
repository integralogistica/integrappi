"""Prueba standalone del bot de sanciones ONU/UE (descarga viva).

Uso (dev): python scripts/probar_sanciones.py [documento]
Sin argumento corre con un documento de prueba (la cédula oficial 208079 del
caso OFAC no aplica aquí: cada lista indexa sus propios identificadores).
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from Funciones.bot_sanciones import consultar_sanciones_sync  # noqa: E402


def main() -> None:
    documento = sys.argv[1] if len(sys.argv) > 1 else "1033688842"
    resultado = consultar_sanciones_sync(documento)
    resultado["coincidencias"] = resultado["coincidencias"][:10]
    print(json.dumps(resultado, ensure_ascii=False, indent=2)[:4000])


if __name__ == "__main__":
    main()
