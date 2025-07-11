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
    ubicacion_cargue: Optional[str] = None
    direccion_cargue: Optional[str] = None
    ubicacion_descargue: Optional[str] = None
    direccion_descargue: Optional[str] = None
    observaciones: Optional[str] = None
    placa: str
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
# ------------------------------
@ruta_pedidos.post("/cargar-masivo", response_model=dict)
async def cargar_pedidos_masivo(
    creado_por: str = Form(...),
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
        "PLANILLA_SISCORE","VALOR_FLETE","UBICACION_CARGUE","DIRECCION_CARGUE",
        "UBICACION_DESCARGUE","DIRECCION_DESCARGUE","OBSERVACIONES","PLACA","TIPO_VIAJE"
    ]
    missing = [c for c in req if c not in [col.upper() for col in df.columns]]
    if missing:
        raise HTTPException(400, f"Columnas faltantes: {missing}")

    errores: List[str] = []
    registros: List[Dict] = []
    acumulados_por_placa: Dict[str, float] = {}

    # 1er pase: validar y acumular fletes por placa
    tipos_por_placa: Dict[str, str] = {}
    destinos_por_placa: Dict[str, str] = {}
    for idx, row in df.iterrows():
        fila = idx + 2
        placa = row["PLACA"].upper()

        vehiculo = row["TIPO_VEHICULO"].strip().upper()
        if placa in tipos_por_placa:
            if tipos_por_placa[placa] != vehiculo:
                errores.append(f"Fila {fila}: tipo de vehículo '{vehiculo}' no coincide con tipo '{tipos_por_placa[placa]}' ya usado para placa '{placa}'")
        else:
            tipos_por_placa[placa] = vehiculo

        destino = row["DESTINO"].upper()
        if placa in destinos_por_placa:
            if destinos_por_placa[placa] != destino:
                errores.append(f"Fila {fila}: destino '{destino}' no coincide con destino '{destinos_por_placa[placa]}' ya usado para placa '{placa}'")
        else:
            destinos_por_placa[placa] = destino


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
        
        ubicacion_cargue     = row.get("UBICACION_CARGUE", "")
        direccion_cargue     = row.get("DIRECCION_CARGUE", "")
        ubicacion_descargue  = row.get("UBICACION_DESCARGUE", "")
        direccion_descargue  = row.get("DIRECCION_DESCARGUE", "")

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
            "ubicacion_cargue": ubicacion_cargue,
            "direccion_cargue": direccion_cargue,
            "ubicacion_descargue": ubicacion_descargue,
            "direccion_descargue": direccion_descargue,            
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

    return {"mensaje": f"{len(registros)} lineas cargadas", "detalles": detalles}

# ------------------------------
# 🗂 Listar pedidos filtrables
# ------------------------------
@ruta_pedidos.post("/", response_model=List[dict])
async def listar_pedidos(datos: FiltrosConUsuario):
    usuario = datos.usuario
    filtros = datos.filtros or FiltrosPedidos()

    user = coleccion_usuarios.find_one({"usuario": usuario.upper().strip()})
    if not user:
        raise HTTPException(404, "Usuario no encontrado")

    perfil = user["perfil"].upper()
    regional_usuario = user["regional"].upper()

    filtro = {}

    if filtros.estados:
        filtro["estado"] = {"$in": [e.upper().strip() for e in filtros.estados]}

    if perfil in {"ADMIN", "GERENTE", "ANALISTA"}:
        if filtros.regionales:
            filtro["regional"] = {"$in": [r.upper().strip() for r in filtros.regionales]}
    else:
        filtro["regional"] = regional_usuario

    docs = coleccion_pedidos.find(filtro)
    return [modelo_pedido(d) for d in docs]


# ------------------------------
# 🔄 Actualizar estado de TODOS los pedidos de una placa
# ------------------------------
@ruta_pedidos.put("/{pedido_id}/estado", response_model=dict)
async def actualizar_estado(
    pedido_id: str,
    estado: str = Body(..., embed=True),
    usuario: str = Body(..., embed=True),
    observaciones_aprobador: Optional[str] = Body(None, embed=True) 
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
    if observaciones_aprobador:
        update_data["observaciones_aprobador"] = observaciones_aprobador

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
async def eliminar_pedido(
    pedido_id: str,
    usuario: str = Query(..., description="Usuario que solicita la eliminación")
):
    try:
        oid = ObjectId(pedido_id)
    except:
        raise HTTPException(400, "ID de pedido inválido")

    user = coleccion_usuarios.find_one({"usuario": usuario.upper().strip()})
    if not user:
        raise HTTPException(404, "Usuario no encontrado")

    perfil = user["perfil"].upper()
    if perfil == "GERENTE":
        raise HTTPException(403, "Los usuarios con perfil GERENTE no pueden eliminar pedidos.")

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
            "id_linea":                   str(d["_id"]),
            "numero_pedido":               d.get("numero_pedido", "")
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
# 📥 Cargar masivo numero_pedido desde Excel (modificado)
# ------------------------------
@ruta_pedidos.post("/cargar-numeros-pedido", response_model=dict)
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

    # Leer el archivo Excel
    df = pd.read_excel(archivo.file)
    df = df.apply(lambda col: col.map(lambda x: str(x).strip() if pd.notnull(x) else ""))

    # Validar columnas requeridas
    required_cols = {"id_linea", "numero_pedido"}
    if not required_cols.issubset(set(df.columns)):
        raise HTTPException(400, f"El archivo debe contener las columnas: {required_cols}")

    errores = []
    updates = []  # Almacenar updates válidos

    # 1. Validar todas las filas antes de actualizar
    for idx, row in df.iterrows():
        fila = idx + 2  # Excel es 1-indexed y +1 por encabezado
        id_linea = row["id_linea"]
        numero_pedido = row["numero_pedido"]

        # Validar numero_pedido no vacío
        if not numero_pedido:
            errores.append(f"Fila {fila}: numero_pedido no puede estar vacío")
            continue

        try:
            oid = ObjectId(id_linea)
        except:
            errores.append(f"Fila {fila}: id_linea inválido '{id_linea}'")
            continue

        pedido = coleccion_pedidos.find_one({"_id": oid})
        if not pedido:
            errores.append(f"Fila {fila}: No se encontró pedido con id_linea '{id_linea}'")
            continue

        # Validar estado AUTORIZADO
        if pedido.get("estado") != "AUTORIZADO":
            errores.append(f"Fila {fila}: El pedido no está en estado AUTORIZADO (estado actual: {pedido.get('estado')})")
            continue

        # Si todo ok, agrega a lista de updates
        updates.append({
            "_id": oid,
            "numero_pedido": numero_pedido
        })

    # 2. Si hay errores, abortar sin actualizar nada
    if errores:
        raise HTTPException(400, detail={"mensaje": "Errores en el archivo. No se actualizó ningún registro.", "errores": errores})

    # 3. Realizar los updates ahora que no hay errores
    actualizados = 0
    for u in updates:
        res = coleccion_pedidos.update_one(
            {"_id": u["_id"]},
            {"$set": {
                "numero_pedido": u["numero_pedido"],
                "pedido_actualizado_por": user["usuario"],
                "fecha_actualizacion": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "estado": "COMPLETADO"
            }}
        )
        if res.modified_count > 0:
            actualizados += 1

    return {"mensaje": f"{actualizados} registros actualizados correctamente"}



# ------------------------------
# 📤 Exportar pedidos COMPLETADOS filtrados por fechas y regional con permisos de perfil
# ------------------------------
@ruta_pedidos.get("/exportar-completados")
async def exportar_completados(
    usuario: str = Query(..., description="Usuario que exporta"),
    fecha_inicial: str = Query(..., description="Fecha inicial en formato YYYY-MM-DD"),
    fecha_final: str = Query(..., description="Fecha final en formato YYYY-MM-DD"),
    regionales: List[str] = Query(None, description="Lista de regionales (opcional para OPERADOR)")
):
    # Validar usuario
    user = coleccion_usuarios.find_one({"usuario": usuario.upper().strip()})
    if not user:
        raise HTTPException(404, "Usuario no encontrado")
    perfil = user["perfil"].upper()
    regional_usuario = user["regional"].upper()

    # Validar formato de fechas
    try:
        fecha_ini_dt = datetime.strptime(fecha_inicial, "%Y-%m-%d")
        fecha_fin_dt = datetime.strptime(fecha_final, "%Y-%m-%d")
    except:
        raise HTTPException(400, "Formato de fecha inválido. Use YYYY-MM-DD.")

    # Construir filtro
    filtro = {
        "estado": "COMPLETADO",
        "fecha_actualizacion": {
            "$gte": f"{fecha_inicial} 00:00:00",
            "$lte": f"{fecha_final} 23:59:59"
        }
    }

    # Lógica de regional según perfil
    if perfil in {"ADMIN", "GERENTE", "ANALISTA"}:
        if regionales:
            filtro["regional"] = {"$in": [r.upper().strip() for r in regionales]}
    else:  # OPERADOR u otro perfil limitado
        filtro["regional"] = regional_usuario

    # Consultar en base de datos
    docs = list(coleccion_pedidos.find(filtro))
    if not docs:
        raise HTTPException(404, "No se encontraron pedidos COMPLETADOS para los filtros dados.")

    # Campos a exportar
    campos = [
        "fecha", "cliente_nombre", "origen", "destino", "num_cajas", "num_kilos",
        "tipo_vehiculo", "tipo_viaje", "valor_declarado", "planilla_siscore",
        "valor_flete", "valor_flete_sistema", "ubicacion_cargue", "ubicacion_descargue",
        "observaciones", "placa", "creado_por", "regional", "autorizado_por",
        "fecha_autorizacion", "observaciones_aprobador", "fecha_actualizacion",
        "numero_pedido", "pedido_actualizado_por"
    ]

    # Preparar filas
    rows = []
    for d in docs:
        fila = {}
        for campo in campos:
            fila[campo] = d.get(campo, "")
        rows.append(fila)

    # Crear DataFrame y exportar
    df = pd.DataFrame(rows)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Completados')
    output.seek(0)

    # Devolver como descarga
    filename = f"pedidos_completados_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@ruta_pedidos.get("/listar-completados")
async def listar_completados(
    usuario: str = Query(...),
    fecha_inicial: str = Query(...),
    fecha_final: str = Query(...),
    regionales: List[str] = Query(None)
):
    # Validar usuario
    user = coleccion_usuarios.find_one({"usuario": usuario.upper().strip()})
    if not user:
        raise HTTPException(404, "Usuario no encontrado")
    perfil = user["perfil"].upper()
    regional_usuario = user["regional"].upper()

    # Validar formato de fechas
    try:
        fecha_ini_dt = datetime.strptime(fecha_inicial, "%Y-%m-%d")
        fecha_fin_dt = datetime.strptime(fecha_final, "%Y-%m-%d")
    except:
        raise HTTPException(400, "Formato de fecha inválido. Use YYYY-MM-DD.")

    # Construir filtro
    filtro = {
        "estado": "COMPLETADO",
        "fecha_actualizacion": {
            "$gte": f"{fecha_inicial} 00:00:00",
            "$lte": f"{fecha_final} 23:59:59"
        }
    }

    # Lógica de regional según perfil
    if perfil in {"ADMIN", "GERENTE", "ANALISTA"}:
        if regionales:
            filtro["regional"] = {"$in": [r.upper().strip() for r in regionales]}
    else:  # OPERADOR u otro perfil limitado
        filtro["regional"] = regional_usuario

    # Consultar en base de datos
    docs = list(coleccion_pedidos.find(filtro))
    if not docs:
        raise HTTPException(404, "No se encontraron pedidos COMPLETADOS.")

    # Formatear resultados
    resultados = []
    for d in docs:
        resultados.append(modelo_pedido(d))

    return resultados
