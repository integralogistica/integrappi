"""Backup JSON de baseusuarios y conductores (reemplazo de mongodump si no está instalado).

Uso: python scripts/backup_usuarios_json.py
Escribe scripts/backups/baseusuarios_conductores_YYYYMMDD_HHMMSS.json
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from bson import ObjectId

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bd.bd_cliente import bd_cliente as cliente_mongo  # noqa: E402


def _serializar(valor):
    if isinstance(valor, ObjectId):
        return str(valor)
    if isinstance(valor, datetime):
        return valor.astimezone(timezone.utc).isoformat()
    if isinstance(valor, dict):
        return {k: _serializar(v) for k, v in valor.items()}
    if isinstance(valor, list):
        return [_serializar(v) for v in valor]
    return valor


def main():
    db = cliente_mongo["integra"]
    respaldo = {
        "creado_en": datetime.now(timezone.utc).isoformat(),
        "baseusuarios": [_serializar(d) for d in db.baseusuarios.find()],
        "conductores": [_serializar(d) for d in db.conductores.find()],
    }
    destino = Path(__file__).parent / "backups"
    destino.mkdir(exist_ok=True)
    archivo = destino / f"baseusuarios_conductores_{datetime.now():%Y%m%d_%H%M%S}.json"
    archivo.write_text(json.dumps(respaldo, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"Backup OK: {archivo}")
    print(f"  baseusuarios: {len(respaldo['baseusuarios'])} docs")
    print(f"  conductores:  {len(respaldo['conductores'])} docs")


if __name__ == "__main__":
    main()
