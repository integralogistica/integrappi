# ============================================================
# Controlador de Gastos (PERSONAL) — SIN auth, ruta pública
# ============================================================
# Control de gastos personal con 3 fuentes de dinero:
#   - efectivo_usd : dólares físicos
#   - virtual_usd  : dólares virtuales (tarjeta)
#   - efectivo_cop : pesos colombianos (se muestran también
#                    en USD con la TRM del día)
#
# El saldo de cada fuente se CALCULA a partir de los movimientos
# (ingresos - gastos), nunca se guarda: así un movimiento borrado
# o editado recalcula solo. La primera vez (colección vacía) se
# siembran los saldos iniciales como ingresos tipo "inicial".
#
# TRM del día: se consulta en trm-colombia (proxy público de la
# Superfinanciera) con fallback a open.er-api.com; se cachea en
# Mongo por día (clave 'trm') y si ambas fuentes fallan se usa la
# última TRM guardada.
#
# Endpoints (todos públicos, de uso doméstico):
#   GET    /estado            → TRM + saldos por fuente + totales + movimientos
#   POST   /movimiento        → registra un gasto o ingreso
#   DELETE /movimiento/{id}   → borra un movimiento (recalcula saldos)
# ============================================================
from datetime import datetime
from typing import Optional

import pytz
import requests
from bson import ObjectId
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from bd.bd_cliente import bd_cliente

ruta_controlador_gastos = APIRouter(prefix="/controlador-gastos", tags=["Controlador Gastos"])

bd = bd_cliente['integra']
coleccion_movimientos = bd['controlador_gastos_movimientos']
coleccion_config = bd['controlador_gastos_config']

_tz_bogota = pytz.timezone("America/Bogota")

# Fuentes válidas. La moneda define en qué unidad está el monto de
# cada movimiento (un gasto de la fuente COP va en pesos, no en USD).
FUENTES = {
    "efectivo_usd": {"etiqueta": "Dólares físicos", "moneda": "USD"},
    "virtual_usd": {"etiqueta": "Dólares virtuales (tarjeta)", "moneda": "USD"},
    "efectivo_cop": {"etiqueta": "Pesos (COP)", "moneda": "COP"},
}

TRM_POR_DEFECTO = 4000.0     # solo si nunca se ha podido consultar
TRM_TIMEOUT_S = 8


# --- Helpers -------------------------------------------------

def _ahora_bogota() -> datetime:
    return datetime.now(_tz_bogota)


def _hoy_str() -> str:
    return _ahora_bogota().strftime("%Y-%m-%d")


def _serializar(doc: dict) -> dict:
    out = {}
    for k, v in doc.items():
        if isinstance(v, ObjectId):
            out[k] = str(v)
        elif isinstance(v, datetime):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out


def _sembrar_iniciales():
    """Primera vez: saldos iniciales acordados (1000 USD efectivo,
    1000 USD virtuales, 8.000.000 COP) como movimientos tipo 'inicial'.
    Se ejecuta una única vez (colección vacía)."""
    if coleccion_movimientos.count_documents({}) > 0:
        return
    ahora = _ahora_bogota()
    iniciales = [
        {"fuente": "efectivo_usd", "monto": 1000.0},
        {"fuente": "virtual_usd", "monto": 1000.0},
        {"fuente": "efectivo_cop", "monto": 8000000.0},
    ]
    for ini in iniciales:
        coleccion_movimientos.insert_one({
            "tipo": "ingreso",
            "subtipo": "inicial",
            "fuente": ini["fuente"],
            "moneda": FUENTES[ini["fuente"]]["moneda"],
            "monto": ini["monto"],
            "descripcion": "Saldo inicial",
            "fecha": ahora,
        })


def _obtener_trm() -> dict:
    """TRM del día (COP por USD). Cache por día en Mongo; si las fuentes
    externas fallan se devuelve la última conocida."""
    hoy = _hoy_str()
    cache = coleccion_config.find_one({"clave": "trm"})
    if cache and cache.get("fecha") == hoy and cache.get("valor"):
        return {"valor": cache["valor"], "fecha": cache.get("fecha"), "origen": cache.get("origen", "cache")}

    valor = None
    origen = None
    try:
        r = requests.get("https://trm-colombia.vercel.app/api", timeout=TRM_TIMEOUT_S)
        if r.ok:
            v = float(r.json()["data"]["valor"])
            valor, origen = v, "trm-colombia"
    except Exception:
        pass
    if valor is None:
        try:
            r = requests.get("https://open.er-api.com/v6/latest/USD", timeout=TRM_TIMEOUT_S)
            if r.ok:
                v = float(r.json()["rates"]["COP"])
                valor, origen = v, "er-api"
        except Exception:
            pass

    if valor is None:
        # Sin internet/fuentes caídas: última TRM guardada (o la de emergencia).
        if cache and cache.get("valor"):
            return {"valor": cache["valor"], "fecha": cache.get("fecha"), "origen": "ultima-conocida"}
        return {"valor": TRM_POR_DEFECTO, "fecha": hoy, "origen": "por-defecto"}

    coleccion_config.update_one(
        {"clave": "trm"},
        {"$set": {"clave": "trm", "valor": valor, "fecha": hoy, "origen": origen}},
        upsert=True,
    )
    return {"valor": valor, "fecha": hoy, "origen": origen}


def _calcular_estado() -> dict:
    """Saldos por fuente y totales, a partir de los movimientos."""
    saldos = {f: 0.0 for f in FUENTES}
    pipeline = [{"$group": {
        "_id": "$fuente",
        "saldo": {"$sum": {"$cond": [
            {"$eq": ["$tipo", "ingreso"]}, "$monto", {"$multiply": ["$monto", -1]}
        ]}}
    }}]
    for row in coleccion_movimientos.aggregate(pipeline):
        if row["_id"] in saldos:
            saldos[row["_id"]] = float(row["saldo"])

    trm = _obtener_trm()
    t = trm["valor"]

    fuentes = []
    total_usd = 0.0
    for fid, meta in FUENTES.items():
        saldo = saldos[fid]
        saldo_usd = saldo if meta["moneda"] == "USD" else round(saldo / t, 2)
        total_usd += saldo_usd
        fuentes.append({
            "id": fid,
            "etiqueta": meta["etiqueta"],
            "moneda": meta["moneda"],
            "saldo": round(saldo, 2),
            "saldo_usd": round(saldo_usd, 2),
        })

    return {
        "trm": trm,
        "fuentes": fuentes,
        "total_usd": round(total_usd, 2),
        "total_cop": round(total_usd * t, 0),
        "actualizado": _ahora_bogota().isoformat(),
    }


# --- Modelos -------------------------------------------------

class MovimientoIn(BaseModel):
    tipo: str = Field(..., description="gasto | ingreso")
    fuente: str
    monto: float = Field(..., gt=0)
    descripcion: Optional[str] = None
    categoria: Optional[str] = None


# --- Endpoints -----------------------------------------------

@ruta_controlador_gastos.get("/estado")
async def estado(limite: int = 100):
    """TRM del día, saldo por fuente, totales y últimos movimientos."""
    _sembrar_iniciales()
    movs = list(
        coleccion_movimientos.find({}).sort("fecha", -1).limit(max(1, min(limite, 500)))
    )
    out = _calcular_estado()
    out["movimientos"] = [_serializar(m) for m in movs]
    out["total_movimientos"] = coleccion_movimientos.count_documents({})
    return out


@ruta_controlador_gastos.post("/movimiento")
async def crear_movimiento(mov: MovimientoIn):
    """Registra un gasto o un ingreso en una fuente."""
    if mov.tipo not in ("gasto", "ingreso"):
        raise HTTPException(400, "tipo debe ser 'gasto' o 'ingreso'")
    if mov.fuente not in FUENTES:
        raise HTTPException(400, f"fuente debe ser una de: {', '.join(FUENTES)}")

    if mov.tipo == "gasto":
        # No dejar la fuente en negativo: es una billetera compartida,
        # un gasto mayor al saldo es casi seguro un error de dedo.
        est = _calcular_estado()
        fuente = next(f for f in est["fuentes"] if f["id"] == mov.fuente)
        if mov.monto > fuente["saldo"]:
            raise HTTPException(
                409,
                f"El gasto ({mov.monto:,.0f} {fuente['moneda']}) supera el saldo de "
                f"{fuente['etiqueta']} ({fuente['saldo']:,.0f} {fuente['moneda']}). "
                "Si es real, registra primero un ingreso.",
            )

    doc = {
        "tipo": mov.tipo,
        "fuente": mov.fuente,
        "moneda": FUENTES[mov.fuente]["moneda"],
        "monto": round(mov.monto, 2),
        "descripcion": (mov.descripcion or "").strip() or None,
        "categoria": (mov.categoria or "").strip() or None,
        "fecha": _ahora_bogota(),
    }
    res = coleccion_movimientos.insert_one(doc)
    doc["_id"] = res.inserted_id
    return {"movimiento": _serializar(doc), "estado": _calcular_estado()}


@ruta_controlador_gastos.delete("/movimiento/{id_movimiento}")
async def borrar_movimiento(id_movimiento: str):
    """Borra un movimiento (p. ej. registrado por error); los saldos se recalculan."""
    if not ObjectId.is_valid(id_movimiento):
        raise HTTPException(400, "id inválido")
    res = coleccion_movimientos.delete_one({"_id": ObjectId(id_movimiento)})
    if res.deleted_count == 0:
        raise HTTPException(404, "Movimiento no encontrado")
    return {"ok": True, "estado": _calcular_estado()}
