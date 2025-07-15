# archivo: rutas/ruta_pedidos.py

from fastapi import APIRouter, HTTPException, status, UploadFile, File, Body, Form, Query
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
coleccion_pedidos_completados = db["pedidos_completados"]
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
class FiltrosPedidos(BaseModel):
    estados: Optional[List[str]] = None
    regionales: Optional[List[str]] = None

class FiltrosConUsuario(BaseModel):
    usuario: str
    filtros: Optional[FiltrosPedidos] = None


class Pedido(BaseModel):
    fecha_creacion: str
    cliente_nombre: str
    origen: str
    destino: str
    num_cajas: int
    num_kilos: float
    tipo_vehiculo: str
    valor_declarado: float
    planilla_siscore: Optional[str] = None
    valor_flete: float
    ubicacion_cargue: Optional[str] = None
    direccion_cargue: Optional[str] = None
    ubicacion_descargue: Optional[str] = None
    direccion_descargue: Optional[str] = None
    observaciones: Optional[str] = None
    vehiculo: str
    consecutivo_pedido: int
    consecutivo_integrapp: str
    desvio: float
    cargue_descargue: float
    punto_adicional: float
    creado_por: str
    tipo_viaje: Literal["CARGA MASIVA", "PAQUETEO"]
    observaciones_aprobador: Optional[str] = None

# ------------------------------
# 📌 Modelo de salida
# ------------------------------
def modelo_pedido(p: dict) -> dict:
    p["id"] = str(p.pop("_id"))
    return p

# ------------------------------
# 📦 Cargar pedidos masivamente desde Excel
@ruta_pedidos.post("/cargar-masivo", response_model=dict)
async def cargar_pedidos_masivo(
    creado_por: str = Form(...),
    archivo: UploadFile = File(...)
):
    user = coleccion_usuarios.find_one({"usuario": creado_por.upper().strip()})
    if not user:
        raise HTTPException(404, "Usuario no encontrado")
    regional = user["regional"].upper()

    # ── Prefijos por regional para los mensajes de error ──
    prefix_map = {
        "GIRARDOTA": "Ave Maria!, ",
        "CALI": "¡mirá ve!, ",
        "BUCARAMANGA": "¡Oiga mano!, ",
        "FUNZA": "¡Oiga chino!, ",
        "BARRANQUILLA": "¡No joda!, "
    }
    prefix = prefix_map.get(regional, "")

    df = pd.read_excel(archivo.file)
    # ── Limpiar encabezados y convertir a mayúsculas sin espacios ──
    df.columns = [col.strip().upper() for col in df.columns]
    df = df.apply(lambda col: col.map(lambda x: str(x).strip() if pd.notnull(x) else ""))

    req = [
        "CLIENTE_NOMBRE","ORIGEN","DESTINO",
        "NUM_CAJAS","NUM_KILOS","TIPO_VEHICULO","VEHICULO","VALOR_DECLARADO",
        "PLANILLA_SISCORE","VALOR_FLETE","UBICACION_CARGUE","DIRECCION_CARGUE",
        "UBICACION_DESCARGUE","DIRECCION_DESCARGUE","OBSERVACIONES",
        "TIPO_VIAJE","CONSECUTIVO_PEDIDO",
        "DESVIO","CARGUE_DESCARGUE","PUNTO_ADICIONAL"
    ]
    missing = [c for c in req if c not in df.columns]
    if missing:
        raise HTTPException(400, f"{prefix}Columnas faltantes: {missing}")

    errores: List[str] = []
    registros: List[Dict] = []

    # ── Fechas para consecutivos ──
    fecha_actual = datetime.now().strftime("%Y-%m-%d %H:%M")
    fecha_sin_guiones = datetime.now().strftime("%Y%m%d")

    # Para cálculo de fletes máximos por vehiculo+original
    maximo_flete_por_vehiculo_consecutivo: Dict[str, Dict[int, float]] = {}
    # Para totales de cajas y kilos por vehículo
    total_cajas_por_vehiculo: Dict[str, int] = {}
    total_kilos_por_vehiculo: Dict[str, float] = {}

    # Validación de duplicados de consecutivo en el mismo archivo
    seen_consecutivos: Dict[int, str] = {}

    # Consistencia tipo_vehiculo y destino por vehiculo
    tipos_por_vehiculo: Dict[str, str] = {}
    destinos_por_vehiculo: Dict[str, str] = {}

    for idx, row in df.iterrows():
        fila = idx + 2
        vehiculo = row["VEHICULO"].upper()
        tipo_vehiculo = row["TIPO_VEHICULO"].upper()

        # 1) Parsear y validar consecutivo original
        try:
            original_int = int(row["CONSECUTIVO_PEDIDO"])
        except:
            errores.append(f"{prefix}Fila {fila}: CONSECUTIVO_PEDIDO '{row['CONSECUTIVO_PEDIDO']}' no es numérico")
            continue

        # 2) No permitir mismo consecutivo en dos vehículos distintos
        if original_int in seen_consecutivos and seen_consecutivos[original_int] != vehiculo:
            errores.append(
                f"{prefix}Fila {fila}: El CONSECUTIVO_PEDIDO {original_int} está duplicado en '{vehiculo}' "
                f"y en '{seen_consecutivos[original_int]}'. No permitido."
            )
            continue
        seen_consecutivos[original_int] = vehiculo

        # 3) Consistencia tipo_vehiculo por vehículo
        if vehiculo in tipos_por_vehiculo:
            if tipos_por_vehiculo[vehiculo] != tipo_vehiculo:
                errores.append(
                    f"{prefix}Fila {fila}: TIPO_VEHICULO '{tipo_vehiculo}' no coincide con "
                    f"'{tipos_por_vehiculo[vehiculo]}' registrado para '{vehiculo}'"
                )
                continue
        else:
            tipos_por_vehiculo[vehiculo] = tipo_vehiculo

        # 4) Consistencia destino por vehículo
        destino = row["DESTINO"].upper()
        if vehiculo in destinos_por_vehiculo:
            if destinos_por_vehiculo[vehiculo] != destino:
                errores.append(
                    f"{prefix}Fila {fila}: DESTINO '{destino}' no coincide con "
                    f"'{destinos_por_vehiculo[vehiculo]}' registrado para '{vehiculo}'"
                )
                continue
        else:
            destinos_por_vehiculo[vehiculo] = destino

        # 5) Validar valor_flete
        try:
            val_flete = float(row["VALOR_FLETE"])
        except:
            errores.append(f"{prefix}Fila {fila}: VALOR_FLETE '{row['VALOR_FLETE']}' no es numérico")
            continue

        # 6) Validar tipo de viaje
        tipo_viaje = row["TIPO_VIAJE"].upper()
        if tipo_viaje not in {"CARGA MASIVA", "PAQUETEO"}:
            errores.append(f"{prefix}Fila {fila}: TIPO_VIAJE debe ser 'CARGA MASIVA' o 'PAQUETEO'")
            continue

        # 7) Validar cliente
        nombre_cli = row["CLIENTE_NOMBRE"].upper()
        if not coleccion_clientes.find_one({"nombre": nombre_cli}):
            errores.append(f"{prefix}Fila {fila}: Cliente '{nombre_cli}' no existe")
            continue

        # 8) Validar tarifa
        o, d = row["ORIGEN"].upper(), row["DESTINO"].upper()
        f = coleccion_fletes.find_one({"origen": o, "destino": d})
        if not f or tipo_vehiculo not in f["tarifas"]:
            errores.append(f"{prefix}Fila {fila}: Tarifa para {o}→{d} y tipo '{tipo_vehiculo}' no definida")
            continue

        # 9) Validar cajas y kilos
        try:
            num_cajas = int(row["NUM_CAJAS"])
            num_kilos = float(row["NUM_KILOS"])
        except:
            errores.append(f"{prefix}Fila {fila}: NUM_CAJAS o NUM_KILOS no son numéricos")
            continue

        # Acumular totales de cajas y kilos por vehículo
        total_cajas_por_vehiculo[vehiculo] = total_cajas_por_vehiculo.get(vehiculo, 0) + num_cajas
        total_kilos_por_vehiculo[vehiculo] = total_kilos_por_vehiculo.get(vehiculo, 0.0) + num_kilos

        # 10) Leer observaciones
        observaciones = row.get("OBSERVACIONES", "")

        # 11) Parsear y validar nuevos campos numéricos
        # DESVIO
        desv_raw = row["DESVIO"]
        if desv_raw == "":
            desvio = 0
        else:
            try:
                desvio = float(desv_raw)
            except:
                errores.append(f"{prefix}Fila {fila}: DESVIO '{desv_raw}' no es numérico")
                continue
            if desvio > 15:
                errores.append(f"{prefix}Fila {fila}: DESVIO '{desvio}' no puede ser mayor a 15")
                continue

        # CARGUE_DESCARGUE
        cd_raw = row["CARGUE_DESCARGUE"]
        if cd_raw == "":
            cargue_descargue = 0
        else:
            try:
                cargue_descargue = float(cd_raw)
            except:
                errores.append(f"{prefix}Fila {fila}: CARGUE_DESCARGUE '{cd_raw}' no es numérico")
                continue
            if cargue_descargue > 15:
                errores.append(f"{prefix}Fila {fila}: CARGUE_DESCARGUE '{cargue_descargue}' no puede ser mayor a 15")
                continue

        # PUNTO_ADICIONAL
        pa_raw = row["PUNTO_ADICIONAL"]
        if pa_raw == "":
            punto_adicional = 0
        else:
            try:
                punto_adicional = float(pa_raw)
            except:
                errores.append(f"{prefix}Fila {fila}: PUNTO_ADICIONAL '{pa_raw}' no es numérico")
                continue
            if punto_adicional > 15:
                errores.append(f"{prefix}Fila {fila}: PUNTO_ADICIONAL '{punto_adicional}' no puede ser mayor a 15")
                continue

        # 12) Construir consecutivos usando el original
        consecutivo_pedido = original_int
        consecutivo_integrapp = f"{regional}-{fecha_sin_guiones}-{original_int}"
        consecutivo_vehiculo  = f"{regional}-{fecha_sin_guiones}-{vehiculo}"

        # 13) Validar duplicado en BD
        existe = coleccion_pedidos.find_one({
            "consecutivo_integrapp": consecutivo_integrapp,
            "estado": {"$in": ["AUTORIZADO", "REQUIERE AUTORIZACION"]}
        })
        if existe:
            docs = coleccion_pedidos.find({
                "consecutivo_integrapp": {"$regex": f"^{regional}-{fecha_sin_guiones}-"},
                "estado": {"$in": ["AUTORIZADO", "REQUIERE AUTORIZACION"]}
            })
            usados = [doc["consecutivo_pedido"] for doc in docs]
            siguiente = max(usados) + 1 if usados else 1
            errores.append(
                f"{prefix}Fila {fila}: El CONSECUTIVO_PEDIDO {original_int} de la regional '{regional}' "
                f"con fecha {fecha_sin_guiones} ya fue utilizado. Debes usar del {siguiente} en adelante."
            )
            continue

        # 14) Acumular máximo valor_flete por (vehiculo, original)
        maximos = maximo_flete_por_vehiculo_consecutivo.setdefault(vehiculo, {})
        maximos[original_int] = max(val_flete, maximos.get(original_int, 0.0))

        # 15) Agregar registro al batch
        registros.append({
            "fecha_creacion": fecha_actual,
            "cliente_nombre": nombre_cli,
            "origen": o,
            "destino": d,
            "num_cajas": num_cajas,
            "num_kilos": num_kilos,
            "tipo_vehiculo": tipo_vehiculo,
            "vehiculo": vehiculo,
            "tipo_viaje": tipo_viaje,
            "valor_declarado": float(row["VALOR_DECLARADO"]),
            "planilla_siscore": row["PLANILLA_SISCORE"],
            "valor_flete": val_flete,
            "ubicacion_cargue": row["UBICACION_CARGUE"],
            "direccion_cargue": row["DIRECCION_CARGUE"],
            "ubicacion_descargue": row["UBICACION_DESCARGUE"],
            "direccion_descargue": row["DIRECCION_DESCARGUE"],
            "observaciones": observaciones,
            "desvio": desvio,
            "cargue_descargue": cargue_descargue,
            "punto_adicional": punto_adicional,
            "creado_por": user["usuario"],
            "regional": regional,
            "consecutivo_pedido": consecutivo_pedido,
            "consecutivo_integrapp": consecutivo_integrapp,
            "consecutivo_vehiculo":  consecutivo_vehiculo,
        })

    if errores:
        raise HTTPException(400, detail={"mensaje": "Errores en archivo masivo", "errores": errores})

    # 16) Calcular total_flete_vehiculo y asignar estado + nuevos campos
    total_flete_por_vehiculo = {
        veh: sum(vals.values())
        for veh, vals in maximo_flete_por_vehiculo_consecutivo.items()
    }

    for r in registros:
        veh = r["vehiculo"]
        o, d, tv = r["origen"], r["destino"], r["tipo_vehiculo"]
        valor_bd = coleccion_fletes.find_one({"origen": o, "destino": d})["tarifas"][tv]
        total = total_flete_por_vehiculo.get(veh, 0.0)

        if total <= valor_bd + 50000:
            r["estado"] = "AUTORIZADO"
            r["autorizado_por"] = "NA"
            r["fecha_autorizacion"] = datetime.now().strftime("%Y-%m-%d %H:%M")  # ajustada
        else:
            r["estado"] = "REQUIERE AUTORIZACION"
            r["autorizado_por"] = "NA"
            r["fecha_autorizacion"] = "NA"

        r["valor_flete_sistema"] = valor_bd
        r["total_flete_vehiculo"] = total
        r["total_cajas_vehiculo"] = total_cajas_por_vehiculo.get(veh, 0)
        r["total_kilos_vehiculo"] = total_kilos_por_vehiculo.get(veh, 0.0)
        r["diferencia_flete"] = total - valor_bd

    # 17) Insertar en bloque y devolver detalles
    if registros:
        result = coleccion_pedidos.insert_many(registros)
        insertados = list(coleccion_pedidos.find({"_id": {"$in": result.inserted_ids}}))
        detalles = [modelo_pedido(p) for p in insertados[:5]]
    else:
        detalles = []

    consecutivos_unicos = len(set(r["consecutivo_vehiculo"] for r in registros))
    mensaje = (
        f"{consecutivos_unicos} vehículo cargado"
        if consecutivos_unicos == 1
        else f"{consecutivos_unicos} vehículos cargados"
    )
    return {"mensaje": mensaje, "detalles": detalles}


# ------------------------------
# 🗂 Listar pedidos por consecutivo_vehiculo con multiestado
# ------------------------------
@ruta_pedidos.post("/", response_model=List[dict])
async def listar_pedidos_vehiculos(datos: FiltrosConUsuario):
    usuario = datos.usuario
    filtros = datos.filtros or FiltrosPedidos()

    user = coleccion_usuarios.find_one({"usuario": usuario.upper().strip()})
    if not user:
        raise HTTPException(404, "Usuario no encontrado")

    perfil = user["perfil"].upper()
    regional_usuario = user["regional"].upper()

    filtro: Dict[str, any] = {}
    if filtros.estados:
        filtro["estado"] = {"$in": [e.upper().strip() for e in filtros.estados]}

    if perfil in {"ADMIN", "GERENTE", "ANALISTA"}:
        if filtros.regionales:
            filtro["regional"] = {"$in": [r.upper().strip() for r in filtros.regionales]}
    else:
        filtro["regional"] = regional_usuario

    pipeline = [
        {"$match": filtro},
        {"$group": {
            "_id": "$consecutivo_vehiculo",
            "tipo_vehiculo":      {"$first": "$tipo_vehiculo"},
            "pedidos":            {"$push": "$$ROOT"},
            "estados_unicos":     {"$addToSet": "$estado"},
            "total_cajas":        {"$first": "$total_cajas_vehiculo"},
            "total_kilos":        {"$first": "$total_kilos_vehiculo"},
            "total_flete":        {"$first": "$total_flete_vehiculo"},
            "valor_flete_sistema":{"$first": "$valor_flete_sistema"},
            "diferencia_flete":   {"$first": "$diferencia_flete"}
        }},
        {"$sort": {"_id": 1}}
    ]

    grupos = list(coleccion_pedidos.aggregate(pipeline))

    respuesta: List[dict] = []
    for g in grupos:
        pedidos = [modelo_pedido(p) for p in g["pedidos"]]
        estados = g.get("estados_unicos", [])
        multiestado = len(estados) > 1

        respuesta.append({
            "consecutivo_vehiculo":   g["_id"],
            "tipo_vehiculo": g.get("tipo_vehiculo", ""),
            "multiestado":            multiestado,
            "estados":                estados,
            "total_cajas_vehiculo":   g.get("total_cajas", 0),
            "total_kilos_vehiculo":   g.get("total_kilos", 0.0),
            "total_flete_vehiculo":   g.get("total_flete", 0.0),
            "valor_flete_sistema":    g.get("valor_flete_sistema", 0.0),
            "diferencia_flete":       g.get("diferencia_flete", 0.0),
            "pedidos":                pedidos
        })

    return respuesta


# ---------------------------------------------------
# 🔄 Autorizar pedidos por consecutivo_vehiculo
# ---------------------------------------------------
@ruta_pedidos.put("/autorizar-por-consecutivo-vehiculo", response_model=dict,  summary="Autorizar pedidos por vehiculo")
async def autorizar_por_consecutivo_vehiculo(
    consecutivos: List[str] = Body(..., embed=True, description="Lista de consecutivo_vehiculo a autorizar"),
    usuario: str = Body(..., embed=True, description="Usuario que realiza la autorización"),
    observaciones_aprobador: Optional[str] = Body(
        None,
        embed=True,
        description="Observaciones del aprobador (opcional)"
    )
):
    # 1) Validar usuario
    user = coleccion_usuarios.find_one({"usuario": usuario.upper().strip()})
    if not user:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Usuario no encontrado")

    perfil = user["perfil"].upper()
    if perfil not in {"ADMIN", "GERENTE"}:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Solo ADMIN o GERENTE pueden autorizar pedidos")

    # 2) Validar input
    if not consecutivos:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Debes indicar al menos un consecutivo_vehiculo")

    # 3) Construir filtro y datos a actualizar
    filtro = {
        "consecutivo_vehiculo": {"$in": consecutivos},
        "estado": "REQUIERE AUTORIZACION"
    }
    datos_a_setear = {
        "estado": "AUTORIZADO",
        "autorizado_por": user["usuario"],
        "fecha_autorizacion": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    # Solo añadir observaciones_aprobador si viene en la petición
    if observaciones_aprobador is not None:
        datos_a_setear["observaciones_aprobador"] = observaciones_aprobador

    # 4) Ejecutar la actualización
    res = coleccion_pedidos.update_many(filtro, {"$set": datos_a_setear})

    if res.matched_count == 0:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail="No se encontraron pedidos en estado REQUIERE AUTORIZACION para los consecutivo_vehiculo dados"
        )

    return {
        "mensaje": f"{len(set(consecutivos))} vehiculo autorizado correctamente por {user['usuario']}"
    }


# ------------------------------
# ❌ Eliminar pedidos por consecutivo_vehiculo
# ------------------------------
@ruta_pedidos.delete("/eliminar-por-consecutivo-vehiculo", response_model=dict,  summary="Eliminar pedidos por vehiculo")
async def eliminar_pedidos_por_consecutivo_vehiculo(
    consecutivo_vehiculo: str = Query(..., description="Consecutivo vehicular (ej. FUNZA-20250711-FUN123)"),
    usuario: str = Query(..., description="Usuario que solicita la eliminación")
):
    user = coleccion_usuarios.find_one({"usuario": usuario.upper().strip()})
    if not user:
        raise HTTPException(404, "Usuario no encontrado")

    perfil = user["perfil"].upper()
    if perfil == "GERENTE":
        raise HTTPException(403, "Los usuarios con perfil GERENTE no pueden eliminar pedidos.")

    # Buscar al menos un pedido que coincida
    pedido = coleccion_pedidos.find_one({
        "consecutivo_vehiculo": consecutivo_vehiculo
    })

    if not pedido:
        raise HTTPException(404, f"No se encontró ningún pedido con consecutivo_vehiculo '{consecutivo_vehiculo}'")

    if pedido["estado"] not in {"AUTORIZADO", "REQUIERE AUTORIZACION", "COMPLETADO"}:
        raise HTTPException(400, "Solo se pueden eliminar pedidos en estado AUTORIZADO o REQUIERE AUTORIZACION")

    # Eliminar todos los pedidos con ese consecutivo y estado válido
    res = coleccion_pedidos.delete_many({
        "consecutivo_vehiculo": consecutivo_vehiculo,
        "estado": {"$in": ["AUTORIZADO", "REQUIERE AUTORIZACION", "COMPLETADO"]}
    })

    return {"mensaje": f"Se elimino el vehiculo '{consecutivo_vehiculo}'"}



# ------------------------------
# ✅ Exportar pedidos AUTORIZADOS a Excel (con datos de facturación)
# ------------------------------
@ruta_pedidos.get("/exportar-autorizados", summary="Exportar pedidos AUTORIZADOS a Excel")
async def exportar_autorizados():
    # 1. Obtener sólo los pedidos AUTORIZADOS
    docs = list(coleccion_pedidos.find({"estado": "AUTORIZADO"}))
    if not docs:
        raise HTTPException(404, "No hay pedidos AUTORIZADOS para exportar")

    rows = []

    def mapear_tipo_vehiculo(vehiculo: str) -> str:
       
        if vehiculo == "CARRY":
            return "CARRY"
        elif vehiculo in {"NHR_VARIOS_DESTINOS", "NHR_1_DESTINO"}:
            return "CAMIONETA"
        elif vehiculo in {"TURBO_VARIOS_DESTINOS", "TURBO_1_DESTINO"}:
            return "TURBO"
        elif vehiculo in {"NIES_VARIOS_DESTINOS", "NIES_1_DESTINO", "SENCILLO_VARIOS_DESTINOS", "SENCILLO_1_DESTINO"}:
            return "SENCILLO"
        elif vehiculo in {"PATINETA_VARIOS_DESTINOS", "PATINETA_1_DESTINO"}:
            return "TRACTOCAMION"
        return vehiculo  # Por si llega otro valor inesperado

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
        # Esto es  para elegir el tipo de producto para estos dos clientes
        producto = "VARIOS"
        if d["cliente_nombre"] in {"FRESENIUS MEDICAL CARE SAS", "FRESENIUS KABI COLOMBIA SAS"}:
            producto = "MEDICAMENTOS (CON EXCLUSION DE LOS PRODUCTOS DE LAS PARTIDAS 3002; 30"

        # construir cada fila con los mapeos fijos, de campo y cálculos
        rows.append({
            "Tipo de viaje":               flete["tipo"],                       # tipo
            "Linea de negocio":            "MASIVO",
            "Estado":                      "PENDIENTE",
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
            "Ubicación Cargue":            d["ubicacion_cargue"],
            "Direccion cargue":            d["direccion_cargue"],
            "Ubicación Descargue":         d["ubicacion_descargue"],
            "Direccion Descargue":         d["direccion_descargue"],
            "Producto":                    producto,
            "Naturaleza":                  "NORMAL",
            "Tipo de vehiculo":            mapear_tipo_vehiculo(d["tipo_vehiculo"]),
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
            "Valor unitario":              int(((d["valor_flete"] / 0.7 + 49) // 50) * 50),
            "Aprobar cupo credito":        1,
            "Aprobar rentabilidad":        1,
            "Otras caracteristicas":       "furgon",
            "REMESAS":                     1,
            "REMISION DEL CLIENTE":        1,
            "GUIA DE TRANSPORTE":          1,
            "MANIFIESTO":                  1,            
            "consecutivo integrapp":       d["consecutivo_integrapp"]
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

# ------------------------------
# 📥 Cargar masivo numero_pedido desde Excel (por consecutivo_integrapp)
#   y mover vehículos completamente terminados
# ------------------------------
@ruta_pedidos.post("/cargar-numeros-pedido", response_model=dict, summary="Cargar los pedidos desde vulcano masivo")
async def cargar_numeros_pedido(
    usuario: str = Form(...),
    archivo: UploadFile = File(...)
):
    user = coleccion_usuarios.find_one({"usuario": usuario.upper().strip()})
    if not user:
        raise HTTPException(404, "Usuario no encontrado")

    perfil = user["perfil"].upper()
    if perfil not in {"ADMIN", "ANALISTA"}:
        raise HTTPException(403, "No tienes permiso para actualizar numero_pedido masivamente")

    # Leer y limpiar Excel
    df = pd.read_excel(archivo.file)

    # 1. Eliminar filas completamente vacías
    df = df.dropna(how='all')

    # 2. Eliminar filas donde consecutivo_integrapp esté vacío o solo espacios
    if "consecutivo_integrapp" in df.columns:
        df = df[df["consecutivo_integrapp"].notna() & (df["consecutivo_integrapp"].str.strip() != "")]

    # 3. Limpiar espacios y NaNs
    df = df.apply(lambda col: col.map(lambda x: str(x).strip() if pd.notnull(x) else ""))

    # Validar columnas
    required_cols = {"consecutivo_integrapp", "numero_pedido"}
    if not required_cols.issubset(df.columns):
        raise HTTPException(400, f"El archivo debe contener las columnas: {required_cols}")

    df = df.drop_duplicates(subset=["consecutivo_integrapp"])

    errores = []
    registros_validos = []
    vehiculos_a_verificar = set()

    for idx, row in df.iterrows():
        fila = idx + 2
        ci = row["consecutivo_integrapp"]
        nped = row["numero_pedido"]

        if not ci:
            errores.append(f"Fila {fila}: consecutivo_integrapp no puede estar vacío")
            continue
        if not nped:
            errores.append(f"Fila {fila}: numero_pedido no puede estar vacío")
            continue

        docs = list(coleccion_pedidos.find({
            "consecutivo_integrapp": ci,
            "estado": "AUTORIZADO"
        }))
        if not docs:
            errores.append(f"Fila {fila}: '{ci}' no existe o no está en estado AUTORIZADO")
            continue

        veh = docs[0]["consecutivo_vehiculo"]
        vehiculos_a_verificar.add(veh)
        registros_validos.append((ci, nped))

    # ❌ Si hay errores, no actualizamos nada
    if errores:
        raise HTTPException(400, detail={
            "mensaje": "No se realizó ninguna actualización. Hay errores en el archivo.",
            "errores": errores
        })

    # ✅ Si no hay errores, ahora sí actualizamos
    actualizados = 0
    for ci, nped in registros_validos:
        res = coleccion_pedidos.update_many(
            {"consecutivo_integrapp": ci, "estado": "AUTORIZADO"},
            {"$set": {
                "numero_pedido": nped,
                "pedido_actualizado_vulcano_por": user["usuario"],
                "fecha_pedido_actualizado_vulcano": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "estado": "COMPLETADO"
            }}
        )
        if res.modified_count:
            actualizados += res.modified_count

    # Verificar vehículos completos
    movidos = []
    for veh in vehiculos_a_verificar:
        total_docs = coleccion_pedidos.count_documents({"consecutivo_vehiculo": veh})
        completados = coleccion_pedidos.count_documents({
            "consecutivo_vehiculo": veh,
            "estado": "COMPLETADO"
        })
        if total_docs > 0 and total_docs == completados:
            docs_para_mover = list(coleccion_pedidos.find({"consecutivo_vehiculo": veh}))
            for d in docs_para_mover:
                d.pop("_id")
            coleccion_pedidos_completados.insert_many(docs_para_mover)
            coleccion_pedidos.delete_many({"consecutivo_vehiculo": veh})
            movidos.append(veh)

    return {
        "mensaje": f"{actualizados} documentos actualizados; "
                   f"{len(movidos)} vehículos movidos a completados",
        "vehiculos_completados": movidos
    }



# Exportar a excel COMPLETADOS por rango fechas
@ruta_pedidos.get(
    "/exportar-completados",
    summary="Exportar a excel COMPLETADOS por rango fechas"
)
async def exportar_completados(
    usuario: str = Query(..., description="Usuario que exporta"),
    fecha_inicial: str = Query(..., description="YYYY-MM-DD"),
    fecha_final:   str = Query(..., description="YYYY-MM-DD"),
    regionales:    List[str] = Query(None, description="Opcional para ADMIN/Gerente/Analista")
):
    # 1) validar usuario
    user = coleccion_usuarios.find_one({"usuario": usuario.upper().strip()})
    if not user:
        raise HTTPException(404, "Usuario no encontrado")
    perfil, reg_user = user["perfil"].upper(), user["regional"].upper()

    # 2) validar fechas
    try:
        datetime.strptime(fecha_inicial, "%Y-%m-%d")
        datetime.strptime(fecha_final,   "%Y-%m-%d")
    except:
        raise HTTPException(400, "Formato de fecha inválido. Use YYYY-MM-DD.")

    # 3) armar filtro sólo por fecha_creacion y, si aplica, por regional
    filtro = {
        "fecha_creacion": {
            "$gte": f"{fecha_inicial} 00:00:00",
            "$lte": f"{fecha_final} 23:59:59"
        }
    }
    if perfil in {"ADMIN", "GERENTE", "ANALISTA"} and regionales:
        filtro["regional"] = {"$in": [r.upper().strip() for r in regionales]}
    elif perfil not in {"ADMIN", "GERENTE", "ANALISTA"}:
        filtro["regional"] = reg_user

    # 4) traer todos los campos
    docs = list(coleccion_pedidos_completados.find(filtro))
    if not docs:
        raise HTTPException(404, "No se encontraron pedidos en ese rango.")

    # 5) convertir ObjectId a string
    for d in docs:
        d["id"] = str(d.pop("_id"))

    # 6) DataFrame con todas las columnas presentes en los documentos
    df = pd.DataFrame(docs)

    # 7) escribir Excel en memoria
    out = BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as w:
        df.to_excel(w, index=False, sheet_name="Completados")
    out.seek(0)

    # 8) devolver descarga
    fn = f"pedidos_completados_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
    return StreamingResponse(
        out,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={fn}"}
    )


# ------------------------------
# 🗂 Listar sólo vehículos COMPLETADOS (multiestado = false)
# ------------------------------
@ruta_pedidos.post(
    "/listar-vehiculo-completados",
    response_model=List[dict],
    summary="Lista los vehiculos 100% COMPLETADOS"
)
async def listar_vehiculos_completados(
    datos: FiltrosConUsuario,
    fecha_inicial: str = Query(..., description="Fecha inicial YYYY-MM-DD"),
    fecha_final:   str = Query(..., description="Fecha final YYYY-MM-DD"),
):
    usuario = datos.usuario
    filtros = datos.filtros or FiltrosPedidos()

    # 1) Validar usuario
    user = coleccion_usuarios.find_one({"usuario": usuario.upper().strip()})
    if not user:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Usuario no encontrado")
    perfil = user["perfil"].upper()
    regional_usuario = user["regional"].upper()

    # 2) Validar formato de fechas
    try:
        datetime.strptime(fecha_inicial, "%Y-%m-%d")
        datetime.strptime(fecha_final,   "%Y-%m-%d")
    except:
        raise HTTPException(400, "Formato de fecha inválido. Use YYYY-MM-DD.")

    # 3) Construir filtro base (fecha + regional)
    match_base: Dict[str, any] = {
        "fecha_pedido_actualizado_vulcano": {
            "$gte": f"{fecha_inicial} 00:00:00",
            "$lte": f"{fecha_final} 23:59:59"
        }
    }
    if perfil in {"ADMIN", "GERENTE", "ANALISTA"}:
        if filtros.regionales:
            match_base["regional"] = {"$in": [r.upper().strip() for r in filtros.regionales]}
    else:
        match_base["regional"] = regional_usuario

    # 4) Pipeline de agregación sobre la colección de completados
    pipeline = [
        {"$match": match_base},
        {"$group": {
            "_id": "$consecutivo_vehiculo",
            "pedidos":       {"$push": "$$ROOT"},
            "estados_unicos": {"$addToSet": "$estado"}
        }},
        # Sólo vehículos cuyo único estado sea COMPLETADO
        {"$match": {
            "estados_unicos": {"$size": 1, "$all": ["COMPLETADO"]}
        }},
        {"$sort": {"_id": 1}}
    ]

    grupos = list(coleccion_pedidos_completados.aggregate(pipeline))
    if not grupos:
        raise HTTPException(404, "No se encontraron vehículos 100% COMPLETADOS en ese rango.")

    # 5) Formatear la salida
    respuesta = []
    for grp in grupos:
        pedidos_modelados = [modelo_pedido(p) for p in grp["pedidos"]]
        respuesta.append({
            "consecutivo_vehiculo": grp["_id"],
            "pedidos":       pedidos_modelados
        })

    return respuesta

