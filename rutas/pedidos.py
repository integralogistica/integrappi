# archivo: rutas/ruta_pedidos.py

from fastapi import APIRouter, HTTPException, status, UploadFile, File, Body
from fastapi.responses import StreamingResponse
from pymongo import MongoClient
from bson import ObjectId
from pydantic import BaseModel
from typing import List, Optional, Dict
from io import BytesIO
import os
import pandas as pd
from typing import Literal
from datetime import datetime

# ------------------------------
# 🔗 Conexión MongoDB
# ------------------------------
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
client = MongoClient(MONGO_URI)
db = client["integra"]
coleccion_pedidos  = db["pedidos"]
coleccion_clientes = db["clientes"]
coleccion_fletes   = db["tarifas"]
coleccion_usuarios = db["baseusuarios"]

# ------------------------------
# 🚦 Configuración Router
# ------------------------------
ruta_pedidos = APIRouter(
    prefix="/pedidos",
    tags=["Pedidos"],
    responses={status.HTTP_404_NOT_FOUND: {"message": "No encontrado"}},
)

# ------------------------------
# 📌 Esquema Pydantic
# ------------------------------
class Pedido(BaseModel):
    fecha: str
    cliente_nombre: str
    origen: str
    destino: str
    num_cajas: int
    num_kilos: float
    tipo_vehiculo: str
    valor_declarado: float
    planilla_siscore: Optional[str] = None
    valor_flete: float
    observaciones: Optional[str] = None
    placa: str
    creado_por: str
    tipo_viaje: Literal["CARGA MASIVA", "PAQUETEO"]

# ------------------------------
# 📌 Modelo de salida
# ------------------------------
def modelo_pedido(p: dict) -> dict:
    p["id"] = str(p.pop("_id"))
    return p

# ------------------------------
# 📦 Cargar pedidos masivamente desde Excel
# ------------------------------
@ruta_pedidos.post("/cargar-masivo", response_model=dict)
async def cargar_pedidos_masivo(
    creado_por: str = Body(..., embed=True),
    archivo: UploadFile = File(...)
):
    user = coleccion_usuarios.find_one({"usuario": creado_por.upper().strip()})
    if not user:
        raise HTTPException(404, "Usuario no encontrado")
    regional = user["regional"]

    df = pd.read_excel(archivo.file)
    df = df.apply(lambda col: col.map(lambda x: str(x).strip() if pd.notnull(x) else ""))

    req = [
        "FECHA","CLIENTE_NOMBRE","ORIGEN","DESTINO",
        "NUM_CAJAS","NUM_KILOS","TIPO_VEHICULO","VALOR_DECLARADO",
        "PLANILLA_SISCORE","VALOR_FLETE","OBSERVACIONES","PLACA","TIPO_VIAJE"
    ]
    missing = [c for c in req if c not in [col.upper() for col in df.columns]]
    if missing:
        raise HTTPException(400, f"Columnas faltantes: {missing}")

    errores: List[str] = []
    registros: List[Dict] = []
    acumulados_por_placa: Dict[str, float] = {}

    # 1er pase: validar y acumular fletes por placa
    for idx, row in df.iterrows():
        fila = idx + 2
        placa = row["PLACA"].upper()
        try:
            val_flete = float(row["VALOR_FLETE"])
        except:
            errores.append(f"Fila {fila}: VALOR_FLETE no numérico")
            continue

        acumulados_por_placa.setdefault(placa, 0.0)
        acumulados_por_placa[placa] += val_flete
        
        tipo_viaje = row["TIPO_VIAJE"].strip().upper()
        if tipo_viaje not in {"CARGA MASIVA", "PAQUETEO"}:
            errores.append(f"Fila {fila}: TIPO_VIAJE debe ser 'CARGA MASIVA' o 'PAQUETEO'")
            continue

        nombre_cli = row["CLIENTE_NOMBRE"].upper()
        if not coleccion_clientes.find_one({"nombre": nombre_cli}):
            errores.append(f"Fila {fila}: Cliente '{row['CLIENTE_NOMBRE']}' no existe")

        o, d = row["ORIGEN"].upper(), row["DESTINO"].upper()
        veh = row["TIPO_VEHICULO"].upper()
        f = coleccion_fletes.find_one({"origen": o, "destino": d})
        if not f or veh not in f["tarifas"]:
            errores.append(f"Fila {fila}: Tarifa para {o}→{d} y vehículo {veh} no definida")
            continue

        try:
            num_cajas = int(row["NUM_CAJAS"])
            num_kilos  = float(row["NUM_KILOS"])
        except:
            errores.append(f"Fila {fila}: NUM_CAJAS o NUM_KILOS no numérico")
            continue

        if any(e.startswith(f"Fila {fila}:") for e in errores):
            continue

        registros.append({
            "fecha": row["FECHA"],
            "cliente_nombre": nombre_cli,
            "origen": o,
            "destino": d,
            "num_cajas": num_cajas,
            "num_kilos": num_kilos,
            "tipo_vehiculo": veh,
            "tipo_viaje": tipo_viaje,
            "valor_declarado": float(row["VALOR_DECLARADO"]),
            "planilla_siscore": row["PLANILLA_SISCORE"],
            "valor_flete": val_flete,
            "observaciones": row["OBSERVACIONES"],
            "placa": placa,
            "creado_por": user["usuario"],
            "regional": regional,            
        })

    if errores:
        raise HTTPException(400, detail={"mensaje": "Errores en archivo masivo", "errores": errores})

    # 2º pase: asignar estado según acumulado
    for r in registros:
        placa = r["placa"]
        o, d, veh = r["origen"], r["destino"], r["tipo_vehiculo"]
        valor_bd = coleccion_fletes.find_one({"origen": o, "destino": d})["tarifas"][veh]
        total = acumulados_por_placa[placa]

        if total <= valor_bd + 50000:
            estado = "AUTORIZADO"
            fecha_aut = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            r["autorizado_por"]    = "NA"
            r["fecha_autorizacion"] = fecha_aut
        else:
            estado = "REQUIERE AUTORIZACION"
            r["autorizado_por"]    = "NA"
            r["fecha_autorizacion"] = "NA"

        r["estado"]             = estado
        r["valor_flete_sistema"] = valor_bd

    # Insertar en bloque
    if registros:
        result = coleccion_pedidos.insert_many(registros)
        insertados = list(coleccion_pedidos.find({"_id": {"$in": result.inserted_ids}}))
        detalles = [modelo_pedido(p) for p in insertados[:5]]
    else:
        detalles = []

    return {"mensaje": f"{len(registros)} pedidos cargados", "detalles": detalles}

# ------------------------------
# 🗂 Listar pedidos filtrables
# ------------------------------
@ruta_pedidos.get("/", response_model=List[dict])
async def listar_pedidos(
    estado: Optional[str] = None,
    regional: Optional[str] = None
):
    filtro = {}
    if estado:
        filtro["estado"] = estado.upper().strip()
    if regional:
        filtro["regional"] = regional.upper().strip()
    docs = coleccion_pedidos.find(filtro)
    return [modelo_pedido(d) for d in docs]

# ------------------------------
# 🔄 Actualizar estado de TODOS los pedidos de una placa
# ------------------------------
@ruta_pedidos.put("/{pedido_id}/estado", response_model=dict)
async def actualizar_estado(
    pedido_id: str,
    estado: str = Body(..., embed=True),
    usuario: str = Body(..., embed=True)
):
    nuevo = estado.upper().strip()
    if nuevo not in {"AUTORIZADO", "REQUIERE AUTORIZACION", "PROCESADO"}:
        raise HTTPException(400, "Estado inválido")

    try:
        oid = ObjectId(pedido_id)
    except:
        raise HTTPException(400, "ID de pedido inválido")

    pedido = coleccion_pedidos.find_one({"_id": oid})
    if not pedido:
        raise HTTPException(404, "Pedido no encontrado")

    anterior, placa = pedido["estado"], pedido["placa"]
    user = coleccion_usuarios.find_one({"usuario": usuario.upper().strip()})
    if not user:
        raise HTTPException(404, "Usuario que actualiza no encontrado")
    perfil = user["perfil"].upper()

    # REQUIERE_AUTORIZACION → AUTORIZADO
    if anterior == "REQUIERE AUTORIZACION" and nuevo == "AUTORIZADO":
        if perfil not in {"ADMIN", "GERENTE"}:
            raise HTTPException(403, "Solo ADMIN/Gerente pueden autorizar.")

    # AUTORIZADO → PROCESADO
    if nuevo == "PROCESADO":
        if perfil not in {"ADMIN", "GERENTE", "ANALISTA"}:
            raise HTTPException(403, "Solo ANALISTA/Admin/Gerente pueden procesar.")
        if anterior != "AUTORIZADO":
            raise HTTPException(400, f"Sólo pedidos en 'AUTORIZADO' pueden procesarse, estado actual: {anterior}")

    # Preparar datos de actualización
    update_data = {"estado": nuevo}
    if anterior == "REQUIERE AUTORIZACION" and nuevo == "AUTORIZADO":
        update_data.update({
            "autorizado_por": user["usuario"],
            "fecha_autorizacion": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    res = coleccion_pedidos.update_many(
        {"placa": placa, "estado": anterior},
        {"$set": update_data}
    )
    if res.matched_count == 0:
        raise HTTPException(404, f"No se encontraron pedidos de placa {placa} en estado {anterior}")

    return {
        "mensaje": f"{res.modified_count} pedidos de placa '{placa}' actualizados de {anterior} a {nuevo} por {user['usuario']}"
    }

# ------------------------------
# 🔄 Procesar pedidos masivamente por IDs
# ------------------------------
@ruta_pedidos.put("/procesar-masivo", response_model=dict)
async def procesar_masivo(
    pedido_ids: List[str] = Body(..., embed=True),
    usuario: str     = Body(..., embed=True)
):
    user = coleccion_usuarios.find_one({"usuario": usuario.upper().strip()})
    if not user:
        raise HTTPException(404, "Usuario no encontrado")
    perfil = user["perfil"].upper()
    if perfil not in {"ADMIN", "GERENTE", "ANALISTA"}:
        raise HTTPException(403, "No tienes permiso para procesar pedidos masivamente")

    # Convertir a ObjectId y filtrar solo AUTORIZADO
        # 1) Convertir a ObjectId y validar
    oids = []
    for pid in pedido_ids:
        try:
            oids.append(ObjectId(pid))
        except:
            raise HTTPException(400, f"ID inválido: {pid}")

    # 2) Leer todas las placas de esos pedidos
    docs = list(coleccion_pedidos.find({"_id": {"$in": oids}, "estado": "AUTORIZADO"}, {"placa": 1}))
    if not docs:
        raise HTTPException(404, "No se encontraron pedidos AUTORIZADO con esos IDs")
    placas = {d["placa"] for d in docs}

    # 3) Procesar **todos** los pedidos que compartan esas placas
    res = coleccion_pedidos.update_many(
        {"placa": {"$in": list(placas)}, "estado": "AUTORIZADO"},
        {"$set": {"estado": "PROCESADO"}}
    )

    if res.matched_count == 0:
        raise HTTPException(404, "No se encontraron pedidos AUTORIZADO para procesar")
    return {"mensaje": f"{res.modified_count} pedidos procesados por {user['usuario']}"}

# ------------------------------
# ❌ Eliminar pedido por ID (y todos de la misma placa)
# ------------------------------
@ruta_pedidos.delete("/{pedido_id}", response_model=dict)
async def eliminar_pedido(pedido_id: str):
    try:
        oid = ObjectId(pedido_id)
    except:
        raise HTTPException(400, "ID de pedido inválido")

    pedido = coleccion_pedidos.find_one({"_id": oid})
    if not pedido:
        raise HTTPException(404, "Pedido no encontrado")

    placa = pedido["placa"]
    if pedido["estado"] not in {"AUTORIZADO", "REQUIERE AUTORIZACION"}:
        raise HTTPException(400, "Solo se pueden eliminar pedidos con esos estados")

    res = coleccion_pedidos.delete_many({
        "placa": placa,
        "estado": {"$in": ["AUTORIZADO", "REQUIERE AUTORIZACION"]}
    })
    return {"mensaje": f"Se eliminaron {res.deleted_count} pedidos de placa '{placa}'"}


# ------------------------------
# ✅ Exportar pedidos AUTORIZADOS a Excel (con datos de facturación)
# ------------------------------
@ruta_pedidos.get("/exportar-autorizados")
async def exportar_autorizados():
    # 1. Obtener sólo los pedidos AUTORIZADOS
    docs = list(coleccion_pedidos.find({"estado": "AUTORIZADO"}))
    if not docs:
        raise HTTPException(404, "No hay pedidos AUTORIZADOS para exportar")

    rows = []
    for d in docs:
        # cargar datos de cliente
        cliente = coleccion_clientes.find_one({"nombre": d["cliente_nombre"]})
        flete = coleccion_fletes.find_one({
            "origen": d["origen"],
            "destino": d["destino"]
        })
        if not flete:
            raise HTTPException(500, f"No se encontró tarifa para {d['origen']}→{d['destino']}")
        # helper para extraer campo o string vacío
        getc = lambda k: cliente.get(k, "") if cliente else ""

        # construir cada fila con los mapeos fijos, de campo y cálculos
        rows.append({
            "Tipo de viaje":               flete["tipo"],                       # tipo
            "Linea de negocio":            "MASIVO",
            "Estado":                      "PENDIENTE",
            "Fecha pedido":                d["fecha"],
            "Fecha vigencia":              d["fecha"],
            "Observación":                 d.get("planilla_siscore",""),
            "Cliente":                     d["cliente_nombre"],
            "Facturar a":                  d["cliente_nombre"],
            "Ubicación fact":              getc("ubicacion"),
            "Contacto":                    getc("contacto"),
            "Cargo":                       getc("cargo"),
            "Teléfono":                    getc("telefono"),
            "Fax":                         getc("fax"),
            "E-mail":                      getc("email"),
            "Origen":                      d["origen"],
            "Destino":                     d["destino"],
            "Pedido cliente":              d.get("planilla_siscore",""),
            "Dirección de fact":           getc("direccion"),
            "Teléfono de fact":            getc("telefono"),
            "Ciudad de fact":              getc("ubicacion"),
            "Agencia despacho":            "BOGOTA",
            "Agencia de fact":             "BOGOTA",
            "Forma de pago":               getc("forma_pago"),
            "Guía":                        d.get("planilla_siscore",""),
            "centro costo":                flete.get("equivalencia_centro_costo", "") + " " + d["tipo_viaje"] + " OPERACIONES CARGA " +  getc("equivalencia_centro_costo"),
            "Ubicación Cargue":            "CALLE 1",
            "Direccion cargue":            "CALLE 1",
            "Ubicación Descargue":         "CALLE 1",
            "Direccion Descargue":         "CALLE 1",
            "Producto":                    "MEDICAMENTOS (CON EXCLUSION DE LOS PRODUCTOS DE LAS PARTIDAS 3002; 30",
            "Naturaleza":                  "NORMAL",
            "Tipo de vehiculo":            "CAMIONETA",
            "unidad":                      "vehiculos",
            "Cantidad":                    1,
            "Tipo embalaje":               "paquetes",
            "Toneladas":                   d["num_kilos"] / 1000,
            "Tipo pago":                   "cupo",
            "Flete unidad":                d["valor_flete"],
            "Tolerancia":                  0,
            "Vlr hora STBY":               0,
            "Vlr. Declar. Mercancia":      d["valor_declarado"],
            "Aprobar Poliza":              1,
            "Flete por":                   "cupo",
            "Valor unitario":              d["valor_flete"] / 0.7,
            "Aprobar cupo credito":        1,
            "Aprobar rentabilidad":        1,
            "Otras caracteristicas":       "furgon",
            "REMESAS":                     1,
            "REMISION DEL CLIENTE":        1,
            "GUIA DE TRANSPORTE":          1,
            "MANIFIESTO":                  1
        })

    # 2. Crear DataFrame y escribir a Excel en memoria
    df = pd.DataFrame(rows)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Autorizados')
    output.seek(0)

    # 3. Devolver como descarga
    filename = f"pedidos_autorizados_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )