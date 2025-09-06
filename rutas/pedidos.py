# archivo: rutas/ruta_pedidos.py

from fastapi import APIRouter, HTTPException, status, UploadFile, File, Body, Form, Query
from fastapi.responses import StreamingResponse
from pymongo import MongoClient
# from bson import ObjectId
from pydantic import BaseModel
from typing import List, Optional, Dict
from io import BytesIO
import os
import pandas as pd
from typing import Literal
from datetime import datetime
import time
from collections import defaultdict 

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
# 📌 Modelo de salida
# ------------------------------
def modelo_pedido(p: dict) -> dict:
    p["id"] = str(p.pop("_id"))
    return p


# Esquemas
typedef = Literal["CARGA MASIVA", "PAQUETEO"]

class FiltrosPedidos(BaseModel):
    estados: Optional[List[str]] = None
    regionales: Optional[List[str]] = None
class FiltrosConUsuario(BaseModel):
    usuario: str
    filtros: Optional[FiltrosPedidos]

class Pedido(BaseModel):
    fecha_creacion: str
    nit_cliente: str
    nombre_cliente: Optional[str] = None
    origen: str
    destino: str
    destino_real: str
    num_cajas: int
    num_kilos: float
    num_kilos_sicetac: Optional[float] = None
    tipo_vehiculo: str
    tipo_vehiculo_sicetac: Optional[str] = None
    valor_declarado: float
    planilla_siscore: Optional[str]
    valor_flete: float
    ubicacion_cargue: Optional[str]
    direccion_cargue: Optional[str]
    ubicacion_descargue: Optional[str]
    direccion_descargue: Optional[str]
    observaciones: Optional[str]
    Observaciones_ajustes: Optional[str]
    vehiculo: str
    consecutivo_pedido: int
    consecutivo_integrapp: str
    desvio: float
    cargue_descargue: float
    punto_adicional: float
    creado_por: str
    tipo_viaje: typedef
    observaciones_aprobador: Optional[str]
    total_puntos_vehiculo: int
    punto_adicional_teorico: float
    cargue_descargue_teorico: float
    total_puntos: int
    total_desvio_vehiculo: float

class AjusteVehiculo(BaseModel):
    consecutivo_vehiculo: str
    tipo_vehiculo_sicetac: Optional[str] = None
    total_kilos_vehiculo_sicetac: Optional[float] = None
    total_desvio_vehiculo: Optional[float] = None
    total_punto_adicional: Optional[float] = None
    Observaciones_ajustes: Optional[str] = None
    total_cargue_descargue: Optional[float] = None
    total_flete_solicitado: Optional[float] = None


class AjustesVehiculosPayload(BaseModel):
    usuario: str
    ajustes: List[AjusteVehiculo]

# ====== MODELO PARA FUSIONAR VEHÍCULOS ======
class FusionVehiculosPayload(BaseModel):
    usuario: str
    consecutivos: List[str]               # 2 o más consecutivos a fusionar
    nuevo_destino: str                    # nuevo destino a aplicar a todos los docs
    tipo_vehiculo_sicetac: str            # tipo (RUNT) a validar y aplicar
    total_flete_solicitado: float         # override vehicular
    total_cargue_descargue: float         # override vehicular
    total_punto_adicional: float          # override vehicular
    total_desvio_vehiculo: float          # override vehicular
    observacion_fusion: Optional[str] = None  # opcional



# Formatea la salida (pone 'id' en lugar de '_id')
def formatear_salida(doc: dict) -> dict:
    doc["id"] = str(doc.pop("_id"))
    return doc

# ------------------------------
# 🔧 Helpers de autorización (porcentaje sobre teórico)
# ------------------------------
def estado_por_autorizacion(costo_real: float, costo_teorico: float):
    """
    Devuelve (estado, porcentaje_sobre_teorico)
    Estados posibles:
      - 'PREAUTORIZADO'
      - 'REQUIERE AUTORIZACION COORDINADOR'
      - 'REQUIERE AUTORIZACION GERENTE'
    """
    if costo_teorico <= 0:
        return ("REQUIERE AUTORIZACION GERENTE", 0.0)

    diff = costo_real - costo_teorico
    porc = round((diff / costo_teorico) * 100.0, 2)

    if diff <= 0:
        return ("PREAUTORIZADO", max(porc, 0.0))

    if porc <= 7.0:
        return ("REQUIERE AUTORIZACION COORDINADOR", porc)

    return ("REQUIERE AUTORIZACION GERENTE", porc)


# Jerarquía para autorizar en función del estado textual
def perfil_puede_autorizar(perfil: str, estado: str) -> bool:
    p = (perfil or "").upper()
    e = (estado or "").upper()
    if p == "ADMIN":
        return True
    if "GERENTE" in e:
        return p == "GERENTE"
    if "COORDINADOR" in e:
        return p in {"COORDINADOR", "GERENTE"}
    return False  # por seguridad


# ------------------------
# carga masivo excel
# ------------------------
@ruta_pedidos.post(
    "/cargar-masivo",
    response_model=dict,
    summary="Cargar masivo para autorizar"
)
async def cargar_masivo(creado_por: str = Form(...), archivo: UploadFile = File(...)):
    start_time = time.time()
    # 1) Usuario y prefijo
    usuario_db = db["baseusuarios"].find_one({"usuario": creado_por.upper().strip()})
    if not usuario_db:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Usuario no encontrado")
    region = usuario_db["regional"].upper()
    prefijo = {
        "GIRARDOTA": "Ave Maria!, ",
        "CALI": "¡mirá ve!, ",
        "BUCARAMANGA": "¡Oiga mano!, ",
        "FUNZA": "¡Oiga chino!, ",
        "CELTA": "¡Oiga chino!, ",
        "BARRANQUILLA": "¡No joda!, "
    }.get(region, "")

    # 2) Leer Excel y normalizar
    df_pedidos = pd.read_excel(archivo.file)
    df_pedidos.columns = [c.strip().upper() for c in df_pedidos.columns]
    df_pedidos = df_pedidos.fillna("").astype(str).applymap(str.strip)

    # 3) Columnas obligatorias
    columnas_req = [
        "NIT_CLIENTE","ORIGEN","DESTINO","NUM_CAJAS","NUM_KILOS","NUM_KILOS_SICETAC",
        "TIPO_VEHICULO",  "TIPO_VEHICULO_SICETAC","VEHICULO","VALOR_DECLARADO","PLANILLA_SISCORE",
        "VALOR_FLETE","UBICACION_CARGUE","DIRECCION_CARGUE",
        "UBICACION_DESCARGUE","DIRECCION_DESCARGUE","OBSERVACIONES",
        "TIPO_VIAJE","CONSECUTIVO_PEDIDO","DESVIO","CARGUE_DESCARGUE",
        "PUNTO_ADICIONAL","TOTAL_PUNTOS","SEGURO","FLETE_REAL","DESTINO_REAL"
    ]
    faltantes = set(columnas_req) - set(df_pedidos.columns)
    if faltantes:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"{prefijo}Columnas faltantes: {list(faltantes)}")

    errores, registros = [], []
    ahora = datetime.now()
    fecha_creacion = ahora.strftime("%Y-%m-%d %H:%M")
    fecha_corta = ahora.strftime("%Y%m%d")

    # Acumuladores por vehículo
    reales_por_veh, desviaciones_por_veh = {}, {}
    cajas_por_veh, kilos_por_veh, puntos_por_veh = {}, {}, {}
    vistos_cons, tipo_por_veh, destino_por_veh = {}, {}, {}
    kilos_sic_por_veh = {} 

    # Helper para números
    def to_num(campo: str, valor: str) -> float:
        try:
            return float(valor) if valor else 0.0
        except:
            raise ValueError(f"{campo} '{valor}' no es numérico")

    # 4) Procesar cada fila
    tarifas_col = db["tarifas"]
    otros_col = db["otros_costos"]
    clientes_col = db["clientes"]
    pedidos_col = db["pedidos"]

    for idx, fila in df_pedidos.iterrows():
        num_fila = idx + 2
        vehiculo = fila["VEHICULO"].upper()

        # ===== tipo_vehiculo (principal) =====
        tipo_veh = fila["TIPO_VEHICULO"].upper()

        # ===== tipo_vehiculo_sicetac (desde Excel si viene; si no, igual al principal) =====
        tipo_veh_sic = (fila.get("TIPO_VEHICULO_SICETAC", "") or "").upper() or tipo_veh

        # consecutivo
        try:
            cons = int(fila["CONSECUTIVO_PEDIDO"])
        except:
            errores.append(f"{prefijo}Fila {num_fila}: CONSECUTIVO_PEDIDO '{fila['CONSECUTIVO_PEDIDO']}' no es numérico")
            continue

        if cons in vistos_cons and vistos_cons[cons] != vehiculo:
            errores.append(f"{prefijo}Fila {num_fila}: CONSECUTIVO_PEDIDO duplicado en {vehiculo}")
            continue
        vistos_cons[cons] = vehiculo

        # consistencia tipo y destino (usa el 'principal' que ya validabas)
        if vehiculo in tipo_por_veh and tipo_por_veh[vehiculo] != tipo_veh:
            errores.append(f"{prefijo}Fila {num_fila}: TIPO_VEHICULO inconsistente para {vehiculo}")
            continue
        tipo_por_veh[vehiculo] = tipo_veh

        destino = fila["DESTINO"].upper()
        if vehiculo in destino_por_veh and destino_por_veh[vehiculo] != destino:
            errores.append(f"{prefijo}Fila {num_fila}: DESTINO inconsistente para {vehiculo}")
            continue
        destino_por_veh[vehiculo] = destino

        # valor_flete sin tope
        try:
            valor_flete = float(fila["VALOR_FLETE"])
        except:
            errores.append(f"{prefijo}Fila {num_fila}: VALOR_FLETE '{fila['VALOR_FLETE']}' no es numérico")
            continue

        # tipo viaje
        tipo_viaje = fila["TIPO_VIAJE"].upper()
        if tipo_viaje not in {"CARGA MASIVA","PAQUETEO"}:
            errores.append(f"{prefijo}Fila {num_fila}: TIPO_VIAJE inválido")
            continue

        # cliente_nit existe
        cliente_nit = fila["NIT_CLIENTE"]
        if not clientes_col.find_one({"nit": cliente_nit}):
            errores.append(f"{prefijo}Fila {num_fila}: Cliente '{cliente_nit}' no existe")
            continue

        # tarifa definida (usa el principal para validar)
        tf = tarifas_col.find_one({"origen": fila["ORIGEN"].upper(), "destino": destino})
        if not tf or tipo_veh not in tf["tarifas"]:
            errores.append(f"{prefijo}Fila {num_fila}: Tarifa no definida para {fila['ORIGEN']}→{destino}, tipo '{tipo_veh}'")
            continue

        # números adicionales
        try:
            desvio = to_num("DESVIO", fila["DESVIO"])
            cargue = to_num("CARGUE_DESCARGUE", fila["CARGUE_DESCARGUE"])
            punto_extra = to_num("PUNTO_ADICIONAL", fila["PUNTO_ADICIONAL"])
            puntos = int(fila["TOTAL_PUNTOS"])
            cajas = int(fila["NUM_CAJAS"])
            kilos = float(fila["NUM_KILOS"])
            
        except Exception as e:
            errores.append(f"{prefijo}Fila {num_fila}: {e}")
            continue

        # ===== num_kilos_sicetac (desde Excel si viene; si no, igual al principal) =====
        try:
            # Toma la columna si existe y tiene algo; si no, usa 'kilos'
            if "NUM_KILOS_SICETAC" in df_pedidos.columns and str(fila.get("NUM_KILOS_SICETAC", "")).strip() != "":
                kilos_sic = float(fila["NUM_KILOS_SICETAC"])
            else:
                kilos_sic = kilos
        except:
            errores.append(f"{prefijo}Fila {num_fila}: NUM_KILOS_SICETAC '{fila.get('NUM_KILOS_SICETAC')}' no es numérico")
            continue

        # acumular (nota: aquí acumulamos por el principal como ya venía)
        reales_por_veh[vehiculo] = reales_por_veh.get(vehiculo, 0) + valor_flete + desvio + cargue + punto_extra
        desviaciones_por_veh[vehiculo] = desviaciones_por_veh.get(vehiculo, 0) + desvio
        cajas_por_veh[vehiculo] = cajas_por_veh.get(vehiculo, 0) + cajas
        kilos_por_veh[vehiculo] = kilos_por_veh.get(vehiculo, 0) + kilos
        kilos_sic_por_veh[vehiculo] = kilos_sic_por_veh.get(vehiculo, 0.0) + kilos_sic
        puntos_por_veh[vehiculo] = puntos_por_veh.get(vehiculo, 0) + puntos

        # evitar consecutivo_integrapp repetido
        cons_int = f"{region}-{fecha_corta}-{cons}"
        if pedidos_col.find_one({
            "consecutivo_integrapp": cons_int,
            "estado": {"$in": [
                "PREAUTORIZADO",
                "REQUIERE AUTORIZACION COORDINADOR",
                "REQUIERE AUTORIZACION GERENTE",
                "AUTORIZADO"  # opcional, evita reusar incluso si ya está autorizado
            ]}
        }):
            errores.append(f"{prefijo}Fila {num_fila}: Consecutivo_integrapp ya usado: {cons_int}")
            continue

        # registrar en base de datos (incluye los nuevos campos si venían; si no, van con el valor espejo)
        registros.append({
            "fecha_creacion": fecha_creacion,
            "nit_cliente": cliente_nit,
            "origen": fila["ORIGEN"].upper(),
            "destino": destino,
            "num_cajas": cajas,
            "num_kilos": kilos,
            "num_kilos_sicetac": kilos_sic,
            "tipo_viaje": tipo_viaje,
            "tipo_vehiculo": tipo_veh,
            "tipo_vehiculo_sicetac": tipo_veh_sic,
            "vehiculo": vehiculo,
            "valor_flete": valor_flete,
            "valor_declarado": float(fila["VALOR_DECLARADO"] or 0),
            "planilla_siscore": fila["PLANILLA_SISCORE"],
            "ubicacion_cargue": fila["UBICACION_CARGUE"],
            "direccion_cargue": fila["DIRECCION_CARGUE"],
            "ubicacion_descargue": fila["UBICACION_DESCARGUE"],
            "direccion_descargue": fila["DIRECCION_DESCARGUE"],
            "observaciones": fila["OBSERVACIONES"],
            "seguro": float(fila["SEGURO"] or 0),
            "desvio": desvio,
            "cargue_descargue": cargue,
            "punto_adicional": punto_extra,
            "total_puntos": puntos,
            "flete_real": float(fila["FLETE_REAL"] or 0),
            "destino_real": fila["DESTINO_REAL"].upper(),
            "creado_por": usuario_db["usuario"],
            "regional": region,
            "consecutivo_pedido": cons,
            "consecutivo_integrapp": cons_int,
            "consecutivo_vehiculo": f"{region}-{fecha_corta}-{vehiculo}"
        })


    if errores:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail={"mensaje": "Errores en archivo masivo", "errores": errores})

    # 5) Calcular teóricos y estado
    for r in registros:
        veh = r["vehiculo"]
        real = reales_por_veh.get(veh, 0)
        desvio_total = desviaciones_por_veh.get(veh, 0)
        origen, destino = r["origen"], r["destino"]
        tbase = tarifas_col.find_one({"origen": origen, "destino": destino})["tarifas"][r["tipo_vehiculo"]]
        otros = otros_col.find_one({"tipo_vehiculo": r["tipo_vehiculo"]})
        max_p = int(otros["max_puntos"])
        exceso = max(0, puntos_por_veh.get(veh, 0) - max_p)
        pad_teo = exceso * float(otros["valor_punto_adicional"])
        cargue_teo = float(otros["cargue_descargue"])
        costo_teorico = tbase + pad_teo + cargue_teo
        costo_real    = real  # total_flete_vehiculo
        estado_calc, porc = estado_por_autorizacion(costo_real, costo_teorico)

        r.update({
            "valor_flete_sistema": tbase,
            "total_flete_vehiculo": costo_real,
            "total_desvio_vehiculo": desvio_total,
            "total_cajas_vehiculo": cajas_por_veh.get(veh, 0),
            "total_kilos_vehiculo": kilos_por_veh.get(veh, 0),
            "total_kilos_vehiculo_sicetac": kilos_sic_por_veh.get(veh, 0.0), 
            "total_puntos_vehiculo": puntos_por_veh.get(veh, 0),
            "punto_adicional_teorico": pad_teo,
            "cargue_descargue_teorico": cargue_teo,
            "costo_teorico_vehiculo": costo_teorico,
            "estado": estado_calc,
            "porcentaje_sobre_teorico": porc,
            "autorizado_por": "SISTEMA" if estado_calc == "PREAUTORIZADO" else "NA",
            "fecha_autorizacion": fecha_creacion if estado_calc == "PREAUTORIZADO" else "NA",
            "diferencia_flete": costo_real - costo_teorico
        })

    # 6) Insertar y responder
    resultado = pedidos_col.insert_many(registros) if registros else None
    insertados = list(pedidos_col.find({"_id": {"$in": resultado.inserted_ids}})) if resultado else []
    detalles = [formatear_salida(doc) for doc in insertados[:5]]
    vehiculos_cargados = len({r["consecutivo_vehiculo"] for r in registros})
    elapsed = time.time() - start_time
    elapsed = round(elapsed, 3)   # en segundos, con 3 decimales
    return {"mensaje": f"{vehiculos_cargados} vehículo{'s' if vehiculos_cargados>1 else ''} cargado{'s' if vehiculos_cargados>1 else ''}", "tiempo_segundos": elapsed,"detalles": detalles}



# -----------------------------------------------------
# 🗂 Solicitar ajustes por consecutivo_vehiculo 
# -----------------------------------------------------
@ruta_pedidos.put(
    "/ajustar-totales-vehiculo",
    response_model=dict,
    summary="Ajustar totales por vehiculo y recalcular estado"
)
async def ajustar_totales_vehiculo(payload: AjustesVehiculosPayload):
    usuario = payload.usuario.upper().strip()
    solicitante = usuario

    # 1) Validar usuario y perfil
    user = coleccion_usuarios.find_one({"usuario": usuario})
    if not user:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Usuario no encontrado")

    perfil = (user.get("perfil") or "").upper()
    if perfil not in {"ADMIN", "DESPACHADOR","ANALISTA","OPERADOR"}:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "No tienes permisos para ajustar vehículos")

    regional_usuario = (user.get("regional") or "").upper()

    if not payload.ajustes:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Debes enviar al menos un ajuste")

    resultados, errores = [], []
    ahora_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for adj in payload.ajustes:
        cv = (adj.consecutivo_vehiculo or "").strip()
        if not cv:
            errores.append("Se envió un ajuste sin consecutivo_vehiculo")
            continue

        # 2) Traer documentos del vehículo
        docs = list(coleccion_pedidos.find({"consecutivo_vehiculo": cv}))
        if not docs:
            errores.append(f"{cv}: no se encontró ningún documento")
            continue

        # 3) Restringir por regional para DESPACHADOR/OPERADOR
        regional_doc = (docs[0].get("regional") or "").upper()
        if perfil in {"DESPACHADOR", "OPERADOR"} and regional_doc != regional_usuario:
            errores.append(f"{cv}: sin permiso, el vehículo pertenece a la regional {regional_doc}")
            continue

        # 4) Bloquear COMPLETADO en esta colección (por seguridad)
        if any(d.get("estado") == "COMPLETADO" for d in docs):
            errores.append(f"{cv}: no se puede ajustar, contiene documentos COMPLETADO")
            continue

        # 5) Valores actuales / derivados (sumas reales por documentos)
        doc0 = docs[0]
        suma_flete   = sum(float(d.get("valor_flete", 0) or 0) for d in docs)
        suma_cargue  = sum(float(d.get("cargue_descargue", 0) or 0) for d in docs)

        desvio_actual       = float(doc0.get("total_desvio_vehiculo", 0) or sum(float(d.get("desvio", 0) or 0) for d in docs))
        punto_extra_actual  = float(doc0.get("total_punto_adicional", 0) or sum(float(d.get("punto_adicional", 0) or 0) for d in docs))
        kilos_sic_actual    = float(doc0.get("total_kilos_vehiculo_sicetac", 0) or sum(float(d.get("num_kilos_sicetac", 0) or 0) for d in docs))
        tipo_sic_actual     = (doc0.get("tipo_vehiculo_sicetac") or doc0.get("tipo_vehiculo") or "").upper()

        # 6) Overrides recibidos (si vienen en el payload, se usan)
        total_desvio_vehiculo        = float(adj.total_desvio_vehiculo) if adj.total_desvio_vehiculo is not None else desvio_actual
        total_punto_adicional        = float(adj.total_punto_adicional) if adj.total_punto_adicional is not None else punto_extra_actual
        total_kilos_vehiculo_sicetac = float(adj.total_kilos_vehiculo_sicetac) if adj.total_kilos_vehiculo_sicetac is not None else kilos_sic_actual
        tipo_vehiculo_sicetac        = (adj.tipo_vehiculo_sicetac or tipo_sic_actual).upper()

        # 👉 Overrides vehiculares
        total_cargue_descargue = float(adj.total_cargue_descargue) if getattr(adj, "total_cargue_descargue", None) is not None else float(suma_cargue)
        total_flete_solicitado = float(adj.total_flete_solicitado) if getattr(adj, "total_flete_solicitado", None) is not None else float(suma_flete)

        # 7) Recalcular real/teórico y estado
        valor_flete_sistema      = float(doc0.get("valor_flete_sistema", 0) or 0)
        punto_adicional_teorico  = float(doc0.get("punto_adicional_teorico", 0) or 0)
        cargue_descargue_teorico = float(doc0.get("cargue_descargue_teorico", 0) or 0)

        costo_teorico = valor_flete_sistema + punto_adicional_teorico + cargue_descargue_teorico
        # 👇 ahora usa el flete solicitado (override si lo enviaron)
        costo_real    = total_flete_solicitado + total_cargue_descargue + total_desvio_vehiculo + total_punto_adicional

        estado_calc, porc = estado_por_autorizacion(costo_real, costo_teorico)
        set_aut_por   = usuario if estado_calc == "PREAUTORIZADO" else "NA"
        set_fecha_aut = ahora_str if estado_calc == "PREAUTORIZADO" else "NA"

        # 8) Campos a actualizar en TODOS los docs del vehículo
        update_fields = {
            "tipo_vehiculo_sicetac":         tipo_vehiculo_sicetac,
            "total_kilos_vehiculo_sicetac":  total_kilos_vehiculo_sicetac,
            "total_desvio_vehiculo":         total_desvio_vehiculo,
            "total_punto_adicional":         total_punto_adicional,
            "total_cargue_descargue":        total_cargue_descargue,
            "total_flete_solicitado":        total_flete_solicitado,  # 👈 NUEVO
            "usr_solicita_ajuste":           solicitante,

            # Totales y diferenciales
            "total_flete_vehiculo":          costo_real,
            "costo_teorico_vehiculo":        costo_teorico,
            "diferencia_flete":              costo_real - costo_teorico,

            # Estado y % sobre teórico
            "estado":                        estado_calc,
            "porcentaje_sobre_teorico":      porc,

            "autorizado_por":                set_aut_por,
            "fecha_autorizacion":            set_fecha_aut,
        }

        # Observaciones_ajustes solo si viene (para no sobreescribir vacíos)
        if adj.Observaciones_ajustes is not None:
            update_fields["Observaciones_ajustes"] = adj.Observaciones_ajustes

        res = coleccion_pedidos.update_many(
            {"consecutivo_vehiculo": cv},
            {"$set": update_fields}
        )

        resultados.append({
            "consecutivo_vehiculo":            cv,
            "regional":                        regional_doc,
            "docs_actualizados":               res.modified_count,
            "usr_solicita_ajuste":             solicitante,
            "tipo_vehiculo_sicetac":           tipo_vehiculo_sicetac,
            "total_kilos_vehiculo_sicetac":    total_kilos_vehiculo_sicetac,
            "total_desvio_vehiculo":           total_desvio_vehiculo,
            "total_punto_adicional":           total_punto_adicional,
            "total_cargue_descargue":          total_cargue_descargue,
            "total_flete_solicitado":          total_flete_solicitado,  # 👈 en respuesta
            "costo_real_vehiculo":             costo_real,
            "costo_teorico_vehiculo":          costo_teorico,
            "diferencia_flete":                costo_real - costo_teorico,
            "nuevo_estado":                    estado_calc,
            "porcentaje_sobre_teorico":        porc,
        })

    mensaje = f"{len(resultados)} vehículo(s) ajustado(s)"
    if errores:
        return {"mensaje": mensaje, "resultados": resultados, "errores": errores}
    return {"mensaje": mensaje, "resultados": resultados}

# -----------------------------------------------------
# 🗂 Listar pedidos por consecutivo_vehiculo con multiestado
# -----------------------------------------------------
@ruta_pedidos.post("/", response_model=List[dict], summary="Listar pedidos agrupados por consecutivo_vehiculo con multiestado")
async def listar_pedidos_vehiculos(datos: FiltrosConUsuario):
    usuario = datos.usuario.upper().strip()
    filtros = datos.filtros or FiltrosPedidos()

    usuario_db = coleccion_usuarios.find_one({"usuario": usuario})
    if not usuario_db:
        raise HTTPException(404, "Usuario no encontrado")

    perfil, regional = usuario_db["perfil"].upper(), usuario_db["regional"].upper()
    filtro = {}
    if filtros.estados:
        filtro["estado"] = {"$in": [e.upper().strip() for e in filtros.estados]}
    filtro["regional"] = (
        {"$in": [r.upper().strip() for r in filtros.regionales]}
        if perfil in {"ADMIN", "COORDINADOR", "GERENTE", "ANALISTA"} and filtros.regionales
        else regional
    )

    pipeline = [
        {"$match": filtro},

        # 1) Traer cliente por NIT
        {"$lookup": {
            "from": "clientes",
            "localField": "nit_cliente",
            "foreignField": "nit",
            "as": "cliente"
        }},
        {"$unwind": {
            "path": "$cliente",
            "preserveNullAndEmptyArrays": True
        }},

        # 2) Propagar el nombre al documento del pedido (no al grupo)
        {"$set": {"nombre_cliente": {"$ifNull": ["$cliente.nombre", "edwin"]}}},

        # (opcional) limpia el objeto cliente para no inflar respuesta
        {"$project": {"cliente": 0}},

        # 3) Agrupa por vehículo
        {"$group": {
            "_id": "$consecutivo_vehiculo",
            "tipo_vehiculo": {"$first": "$tipo_vehiculo"},
            "tipo_vehiculo_sicetac": {"$first": "$tipo_vehiculo_sicetac"},
            "destino": {"$first": "$destino"},
            "Observaciones_ajustes": {"$first": "$Observaciones_ajustes"},
            "pedidos": {"$push": "$$ROOT"},
            "estados": {"$addToSet": "$estado"},

            # 👇 Suma por documentos y override vehicular para FLETE SOLICITADO
            "flete_solicitado_sum_docs": {"$sum": "$valor_flete"},
            "flete_solicitado_override": {"$first": "$total_flete_solicitado"},

            # Puntos y cargue: override vehicular + sum-docs para fallback
            "punto_adicional_total_veh": {"$first": "$total_punto_adicional"},
            "punto_adicional_sum_docs": {"$sum": "$punto_adicional"},
            "cargue_descargue_total": {"$first": "$total_cargue_descargue"},

            # Totales varios
            "totales": {"$first": {
                "cajas": "$total_cajas_vehiculo",
                "kilos": "$total_kilos_vehiculo",
                "kilos_sicetac": "$total_kilos_vehiculo_sicetac",
                "flete": "$total_flete_vehiculo",
                "desvio": "$total_desvio_vehiculo",
                "puntos": "$total_puntos_vehiculo",
                "flete_sistema": "$valor_flete_sistema",
                "punto_teorico": "$punto_adicional_teorico",
                "cargue_teorico": "$cargue_descargue_teorico",
                "costo_real": "$total_flete_vehiculo",
                "diferencia": "$diferencia_flete",
            }},
        }},

        # 4) Coalesce de campos calculados (preferir override si existe)
        {"$set": {
            "punto_adicional_total": {
                "$ifNull": ["$punto_adicional_total_veh", "$punto_adicional_sum_docs"]
            },
            "flete_solicitado": {
                "$ifNull": ["$flete_solicitado_override", "$flete_solicitado_sum_docs"]
            }
        }},

        {"$sort": {"_id": 1}}
    ]

    grupos = list(coleccion_pedidos.aggregate(pipeline))

    return [{
        "consecutivo_vehiculo": g["_id"],
        "tipo_vehiculo": g["tipo_vehiculo"],
        "tipo_vehiculo_sicetac": g.get("tipo_vehiculo_sicetac"),
        "destino": g["destino"],
        "Observaciones_ajustes": g.get("Observaciones_ajustes"),
        "multiestado": len(g["estados"]) > 1,
        "estados": g["estados"],

        "total_cajas_vehiculo": g["totales"].get("cajas", 0),
        "total_kilos_vehiculo": g["totales"].get("kilos", 0.0),
        "total_kilos_vehiculo_sicetac": g["totales"].get("kilos_sicetac", 0.0),

        # Totales reales / costos
        "total_flete_vehiculo": g["totales"].get("flete", 0.0),
        "total_desvio_vehiculo": g["totales"].get("desvio", 0.0),
        "total_puntos_vehiculo": g["totales"].get("puntos", 0),
        "valor_flete_sistema": g["totales"].get("flete_sistema", 0.0),
        "total_punto_adicional_teorico": g["totales"].get("punto_teorico", 0.0),
        "total_cargue_descargue_teorico": g["totales"].get("cargue_teorico", 0.0),
        "costo_teorico_vehiculo": sum([
            g["totales"].get("flete_sistema", 0.0),
            g["totales"].get("punto_teorico", 0.0),
            g["totales"].get("cargue_teorico", 0.0)
        ]),
        "costo_real_vehiculo": g["totales"].get("costo_real", 0.0),
        "diferencia_flete": g["totales"].get("diferencia", 0.0),

        # Adicionales y solicitados (usando override si existe)
        "total_punto_adicional": g.get("punto_adicional_total", 0.0),
        "total_cargue_descargue": g.get("cargue_descargue_total", 0.0),
        "total_flete_solicitado": g.get("flete_solicitado", 0.0),

        # Detalle de pedidos
        "pedidos": [modelo_pedido(p) for p in g["pedidos"]],
    } for g in grupos]


# ---------------------------------------------------
# 🔄 Autorizar pedidos por consecutivo_vehiculo
# ---------------------------------------------------
@ruta_pedidos.put("/autorizar-por-consecutivo-vehiculo", response_model=dict, summary="Autorizar pedidos por vehiculo (según estado requerido)")
async def autorizar_por_consecutivo_vehiculo(
    consecutivos: List[str] = Body(..., embed=True, description="Lista de consecutivo_vehiculo a autorizar"),
    usuario: str = Body(..., embed=True, description="Usuario que realiza la autorización"),
    observaciones_aprobador: Optional[str] = Body(None, embed=True, description="Observaciones del aprobador (opcional)")
):
    # 1) Validar usuario
    user = coleccion_usuarios.find_one({"usuario": usuario.upper().strip()})
    if not user:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Usuario no encontrado")

    perfil = (user.get("perfil") or "").upper()
    if perfil not in {"ADMIN", "COORDINADOR", "GERENTE"}:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Solo ADMIN, COORDINADOR o GERENTE pueden autorizar pedidos")

    # 2) Sanitizar input
    consecutivos = [c.strip() for c in (consecutivos or []) if c and c.strip()]
    if not consecutivos:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Debes indicar al menos un consecutivo_vehiculo")

    ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    autorizados_ok, rechazados = [], []

    # 3) Procesar cada vehículo
    for cv in sorted(set(consecutivos)):
        docs = list(coleccion_pedidos.find({
            "consecutivo_vehiculo": cv,
            "estado": {"$in": ["REQUIERE AUTORIZACION COORDINADOR", "REQUIERE AUTORIZACION GERENTE"]}
        }))

        if not docs:
            rechazados.append({"consecutivo_vehiculo": cv, "motivo": "No está en estado de REQUIERE AUTORIZACION"})
            continue

        # Determinar el requerimiento máximo entre documentos (si hubiese mezcla)
        estados = { (d.get("estado") or "").upper() for d in docs }
        # Si alguno exige GERENTE, tomamos GERENTE
        estado_requerido = "REQUIERE AUTORIZACION GERENTE" if any("GERENTE" in e for e in estados) else "REQUIERE AUTORIZACION COORDINADOR"

        # Validar perfil con el estado requerido
        if not perfil_puede_autorizar(perfil, estado_requerido):
            rechazados.append({"consecutivo_vehiculo": cv, "motivo": f"Requiere perfil acorde a '{estado_requerido}'. Perfil actual: {perfil}"})
            continue

        # Autorizar todos los documentos del vehículo que estén en cualquiera de los dos estados de 'requiere'
        set_fields = {
            "estado": "AUTORIZADO",
            "autorizado_por": user["usuario"],
            "fecha_autorizacion": ahora
        }
        if observaciones_aprobador is not None:
            set_fields["observaciones_aprobador"] = observaciones_aprobador

        res = coleccion_pedidos.update_many(
            {
                "consecutivo_vehiculo": cv,
                "estado": {"$in": ["REQUIERE AUTORIZACION COORDINADOR", "REQUIERE AUTORIZACION GERENTE"]}
            },
            {"$set": set_fields}
        )

        if res.matched_count == 0:
            rechazados.append({"consecutivo_vehiculo": cv, "motivo": "No hubo documentos para autorizar"})
        else:
            autorizados_ok.append({"consecutivo_vehiculo": cv, "docs_autorizados": res.modified_count, "estado_requerido": estado_requerido})

    if not autorizados_ok:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={"mensaje": "No se autorizó ningún vehículo", "rechazados": rechazados}
        )

    return {
        "mensaje": f"{len(autorizados_ok)} vehículo(s) autorizados por {user['usuario']}",
        "autorizados": autorizados_ok,
        "rechazados": rechazados
    }

# -----------------------------------------------------
# ✅ Confirmar PREAUTORIZADOS → AUTORIZADO por consecutivo_vehiculo
#    Perfiles permitidos: ADMIN, DESPACHADOR, OPERADOR
# -----------------------------------------------------
@ruta_pedidos.put(
    "/confirmar-preautorizados",
    response_model=dict,
    summary="Cambiar de PREAUTORIZADO a AUTORIZADO por vehiculo"
)
async def confirmar_preautorizados_por_consecutivo_vehiculo(
    consecutivos: List[str] = Body(..., embed=True, description="Lista de consecutivo_vehiculo a confirmar"),
    usuario: str = Body(..., embed=True, description="Usuario que realiza la confirmación"),
    observaciones_aprobador: Optional[str] = Body(
        None,
        embed=True,
        description="Observaciones del aprobador (opcional)"
    )
):
    # 1) Validar usuario y perfil
    user = coleccion_usuarios.find_one({"usuario": usuario.upper().strip()})
    if not user:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Usuario no encontrado")

    perfil = (user.get("perfil") or "").upper()
    if perfil not in {"ADMIN", "DESPACHADOR", "ANALISTA", "OPERADOR"}:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            detail="Solo ADMIN, DESPACHADOR u OPERADOR pueden confirmar PREAUTORIZADOS"
        )

    regional_usuario = (user.get("regional") or "").upper()

    # 2) Validar input
    consecutivos = [c.strip() for c in (consecutivos or []) if c and c.strip()]
    if not consecutivos:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Debes indicar al menos un consecutivo_vehiculo")

    ahora_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    vehiculos_autorizados = []
    vehiculos_sin_permiso = []
    vehiculos_no_encontrados = []
    vehiculos_sin_preaut = []
    detalles = []

    # 3) Procesar uno a uno para poder validar regional por vehículo
    for cv in sorted(set(consecutivos)):
        # a) Traer cualquier doc del vehículo (para ver regional) y contar PREAUTORIZADOS
        docs_veh = list(coleccion_pedidos.find({"consecutivo_vehiculo": cv}))
        if not docs_veh:
            vehiculos_no_encontrados.append(cv)
            continue

        # b) Validación de regional para DESPACHADOR / OPERADOR
        regional_doc = (docs_veh[0].get("regional") or "").upper()
        if perfil in {"DESPACHADOR", "OPERADOR"} and regional_doc != regional_usuario:
            vehiculos_sin_permiso.append({"consecutivo_vehiculo": cv, "regional": regional_doc})
            continue

        # c) Verificar que haya al menos un PREAUTORIZADO
        preaut_count = coleccion_pedidos.count_documents({
            "consecutivo_vehiculo": cv,
            "estado": "PREAUTORIZADO"
        })
        if preaut_count == 0:
            vehiculos_sin_preaut.append(cv)
            continue

        # d) Autorizar todos los PREAUTORIZADOS del vehículo
        filtro = {
            "consecutivo_vehiculo": cv,
            "estado": "PREAUTORIZADO"
        }
        # (para DESP/OPER ya validamos regional antes; no es necesario filtrar aquí)
        set_fields = {
            "estado": "AUTORIZADO",
            "autorizado_por": user["usuario"],
            "fecha_autorizacion": ahora_str
        }
        if observaciones_aprobador is not None:
            set_fields["observaciones_aprobador"] = observaciones_aprobador

        res = coleccion_pedidos.update_many(filtro, {"$set": set_fields})
        vehiculos_autorizados.append(cv)
        detalles.append({
            "consecutivo_vehiculo": cv,
            "docs_autorizados": res.modified_count,
            "regional": regional_doc
        })

    # 4) Si no se logró autorizar ninguno, devolver 404 con explicación
    if not vehiculos_autorizados:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND,
            detail={
                "mensaje": "No se autorizó ningún vehículo",
                "vehiculos_no_encontrados": vehiculos_no_encontrados,
                "vehiculos_sin_permiso": vehiculos_sin_permiso,
                "vehiculos_sin_preautorizados": vehiculos_sin_preaut
            }
        )

    return {
        "mensaje": f"{len(vehiculos_autorizados)} vehículo(s) confirmados a AUTORIZADO por {user['usuario']}",
        "vehiculos_autorizados": vehiculos_autorizados,
        "detalles": detalles,
        "vehiculos_no_encontrados": vehiculos_no_encontrados,
        "vehiculos_sin_permiso": vehiculos_sin_permiso,
        "vehiculos_sin_preautorizados": vehiculos_sin_preaut
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
    if perfil in {"COORDINADOR", "GERENTE"}:
        raise HTTPException(403, "Los usuarios con perfil GERENTE O COORDINADOR no pueden eliminar pedidos.")

    # Buscar al menos un pedido que coincida
    pedido = coleccion_pedidos.find_one({
        "consecutivo_vehiculo": consecutivo_vehiculo
    })

    if not pedido:
        raise HTTPException(404, f"No se encontró ningún pedido con consecutivo_vehiculo '{consecutivo_vehiculo}'")

    estados_eliminables = {
        "AUTORIZADO",
        "REQUIERE AUTORIZACION COORDINADOR",
        "REQUIERE AUTORIZACION GERENTE",
        "COMPLETADO"
    }
    if pedido["estado"] not in estados_eliminables:
        raise HTTPException(400, "Solo se pueden eliminar pedidos en estado AUTORIZADO o REQUIERE AUTORIZACION (Coord./Gerente) o COMPLETADO")


    # Eliminar todos los pedidos con ese consecutivo y estado válido
    res = coleccion_pedidos.delete_many({
        "consecutivo_vehiculo": consecutivo_vehiculo,
        "estado": {"$in": ["AUTORIZADO", "REQUIERE AUTORIZACION COORDINADOR",  "REQUIERE AUTORIZACION GERENTE","COMPLETADO"]}
    })

    return {"mensaje": f"Se elimino el vehiculo '{consecutivo_vehiculo}'"}


# ------------------------------
# ✅ Exportar pedidos AUTORIZADOS a Excel (con datos de facturación)
# ------------------------------
from collections import defaultdict  # asegúrate de tener este import arriba

@ruta_pedidos.get("/exportar-autorizados", summary="Exportar pedidos AUTORIZADOS a Excel")
async def exportar_autorizados():
    # 1) Traer AUTORIZADOS
    docs = list(coleccion_pedidos.find({"estado": "AUTORIZADO"}))
    if not docs:
        raise HTTPException(404, "No hay pedidos AUTORIZADOS para exportar")

    # --- Agrupar por consecutivo_integrapp (para concatenar docs) ---
    docs_por_ci = defaultdict(list)
    for d in docs:
        docs_por_ci[d["consecutivo_integrapp"]].append(d)

    # --- Agrupar por consecutivo_vehiculo (totales vehiculares) ---
    docs_por_veh = defaultdict(list)
    for d in docs:
        docs_por_veh[d["consecutivo_vehiculo"]].append(d)

    # Concatenar planillas por CI (únicas y en orden)
    def concat_docs(lista_docs):
        vistos = set()
        resultado = []
        for x in lista_docs:
            v = (x.get("planilla_siscore") or "").strip().upper()
            if v and v not in vistos:
                vistos.add(v)
                resultado.append(v)
        return ", ".join(resultado)

    docs_concat_por_ci = {ci: concat_docs(lst) for ci, lst in docs_por_ci.items()}

    # --- Totales vehiculares con fallback ---
    # Kilos SICETAC del vehículo
    kilos_sic_por_veh = {}
    for veh, lst in docs_por_veh.items():
        doc0 = lst[0]
        total_kilos_sic = doc0.get("total_kilos_vehiculo_sicetac")
        if total_kilos_sic is None:
            total_kilos_sic = sum(float(x.get("num_kilos_sicetac", 0) or 0) for x in lst)
        kilos_sic_por_veh[veh] = float(total_kilos_sic or 0)

    # Punto adicional del vehículo
    punto_adic_por_veh = {}
    for veh, lst in docs_por_veh.items():
        doc0 = lst[0]
        total_pa = doc0.get("total_punto_adicional")
        if total_pa is None:
            total_pa = sum(float(x.get("punto_adicional", 0) or 0) for x in lst)
        punto_adic_por_veh[veh] = float(total_pa or 0)

    # Desvío del vehículo
    desvio_por_veh = {}
    for veh, lst in docs_por_veh.items():
        doc0 = lst[0]
        total_desv = doc0.get("total_desvio_vehiculo")
        if total_desv is None:
            total_desv = sum(float(x.get("desvio", 0) or 0) for x in lst)
        desvio_por_veh[veh] = float(total_desv or 0)

    rows = []
    vistos_ci = set()     # primera fila por consecutivo_integrapp
    vistos_veh = set()    # primera fila por consecutivo_vehiculo

    def mapear_tipo_vehiculo(vehiculo: str) -> str:
        if vehiculo == "CARRY":
            return "CARRY"
        elif vehiculo in {"NHR"}:
            return "CAMIONETA"
        elif vehiculo in {"TURBO"}:
            return "TURBO"
        elif vehiculo in {"NIES", "SENCILLO"}:
            return "SENCILLO"
        elif vehiculo in {"PATINETA"}:
            return "TRACTOCAMION"
        return vehiculo

    for d in docs:
        ci = d["consecutivo_integrapp"]
        veh = d["consecutivo_vehiculo"]

        # Primera fila del CI
        es_primera_ci = ci not in vistos_ci
        if es_primera_ci:
            seguro = d.get("seguro", 0)
            cargue_desc = d.get("cargue_descargue_teorico", 0)  # teórico por CI
            pedido_cliente_concat = docs_concat_por_ci.get(ci, "")
            vistos_ci.add(ci)
        else:
            seguro = 0
            cargue_desc = 0
            pedido_cliente_concat = ""

        # Primera fila del vehículo
        es_primera_veh = veh not in vistos_veh
        if es_primera_veh:
            kilos_veh = float(kilos_sic_por_veh.get(veh, 0.0) or 0.0) # está en kg
            toneladas_val = round(kilos_veh / 1000.0, 3)
            punto_adicional_val = punto_adic_por_veh.get(veh, 0.0)
            extra_desvio_para_flete_unidad = desvio_por_veh.get(veh, 0.0)
            vistos_veh.add(veh)
        else:
            toneladas_val = 0
            punto_adicional_val = 0
            extra_desvio_para_flete_unidad = 0

        # Datos de cliente y tarifa
        cliente_nit = coleccion_clientes.find_one({"nit": d["nit_cliente"]})
        flete = coleccion_fletes.find_one({"origen": d["origen"], "destino": d["destino"]})
        if not flete:
            raise HTTPException(500, f"No se encontró tarifa para {d['origen']}→{d['destino']}")

        getc = lambda k: cliente_nit.get(k, "") if cliente_nit else ""
        producto = "VARIOS"
        if d["nit_cliente"] in {"901689684", "900402080"}:
            producto = "MEDICAMENTOS (CON EXCLUSION DE LOS PRODUCTOS DE LAS PARTIDAS 3002;  30"

        observacion = f"DN {pedido_cliente_concat}" if d["nit_cliente"] == "900402080" else (d.get("observaciones") or "").upper()

        valor_flete_doc = float(d.get("valor_flete", 0) or 0)

        rows.append({
            "Consecutivo":              ci,
            "Tipo de viaje":            flete["tipo"],
            "Linea de negocio":         "MASIVO",
            "Estado":                   "PENDIENTE",
            "Observación":              observacion,
            "Cliente":                  d["nit_cliente"],
            "Origen":                   d["origen"].upper(),
            "Destino":                  d["destino"].upper(),

            # Concatenado en la primera fila del CI
            "Pedido cliente":           pedido_cliente_concat,

            "Guía":                     (d.get("planilla_siscore") or "").upper(),
            "CENTRO COSTO":             f"{flete.get('equivalencia_centro_costo', '')} {d.get('tipo_viaje','')} OPERACIONES CARGA {getc('equivalencia_centro_costo')}",
            "Ubicación Cargue":         (d.get("ubicacion_cargue") or "").upper(),
            "Direccion cargue":         (d.get("direccion_cargue") or "").upper(),
            "Ubicación Descargue":      (d.get("ubicacion_descargue") or "").upper(),
            "Direccion Descargue":      (d.get("direccion_descargue") or "").upper(),
            "Producto":                 producto,
            "Naturaleza":               "NORMAL",
            "Tipo de vehiculo":         mapear_tipo_vehiculo((d.get("tipo_vehiculo_sicetac") or d.get("tipo_vehiculo") or "")),

            "unidad":                   "VEHICULOS",
            "Cantidad":                 1,
            "Tipo embalaje":            "PAQUETES",

            # 👉 Kilos (SICETAC) del vehículo – 1ª fila del vehículo
            "Toneladas":                toneladas_val,
            "Tipo pago":                "CUPO",

            # 👉 SOLICITADO: Flete unidad = valor_flete_doc + total_desvio_vehiculo (solo 1ª fila del vehículo)
            "Flete unidad":             valor_flete_doc + extra_desvio_para_flete_unidad + punto_adicional_val,

            "Tolerancia":               0,
            "Vlr hora STBY":            0,
            "Vlr Declar Mercancia":     d.get("valor_declarado", 0),
            "Aprobar Poliza":           1,
            "Flete por":                "CUPO",

            # 👉 Valor unitario: vuelve a tu fórmula de redondeo por documento
            "Valor unitario":           int(((((valor_flete_doc  + extra_desvio_para_flete_unidad + punto_adicional_val) / 0.7) + 49) // 50) * 50),

            "Aprobar cupo credito":     1,
            "Aprobar rentabilidad":     1,
            "Otras caracteristicas":    "FURGON",
            "REMESAS":                  1,
            "REMISION DEL CLIENTE":     1,
            "GUIA DE TRANSPORTE":       1,
            "MANIFIESTO":               1,

            # 👉 Total vehicular ajustado (o fallback) – 1ª fila del vehículo
            "PUNTO ADICIONAL":          punto_adicional_val,

            "SEGURO":                   seguro,
            # Teórico por CI
            "CARGUE-DESCARGUE PER JURIDICA": cargue_desc,
        })

    # 2) DataFrame → Excel en memoria
    df = pd.DataFrame(rows)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='plantilla')
    output.seek(0)

    # 3) Respuesta de descarga
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

    # Leer Excel como texto para no perder formato
    df = pd.read_excel(archivo.file, dtype=str)
    df = df.dropna(how="all")

    # Normalizar encabezados
    df.columns = df.columns.str.strip().str.lower()

    # Mapear encabezados del archivo a los que espera la API
    rename_map = {
        "pedido": "numero_pedido",
        "n° pedido": "numero_pedido",
        "n. pedido": "numero_pedido",
        "consecutivo": "consecutivo_integrapp",
        "consecutivo_integrapp": "consecutivo_integrapp",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Remover fila de totales como “N. registros: 16,0”
    mask_totales = df.apply(
        lambda r: r.astype(str).str.contains("registros", case=False, na=False).any(),
        axis=1
    )
    df = df[~mask_totales]

    # Limpiar espacios
    for c in df.columns:
        df[c] = df[c].fillna("").astype(str).str.strip()

    # Validar columnas requeridas
    required_cols = {"consecutivo_integrapp", "numero_pedido"}
    if not required_cols.issubset(df.columns):
        raise HTTPException(400, f"El archivo debe contener las columnas (o equivalentes): {required_cols}")

    # Mantener solo filas completas
    df = df[(df["consecutivo_integrapp"] != "") & (df["numero_pedido"] != "")]

    # Normalizar numero_pedido: quitar .0 al final si viene desde Excel
    df["numero_pedido"] = df["numero_pedido"].str.replace(r"\.0$", "", regex=True)

    # Quitar duplicados por consecutivo
    df = df.drop_duplicates(subset=["consecutivo_integrapp"])

    errores = []
    registros_validos = []
    vehiculos_a_verificar = set()

    for idx, row in df.iterrows():
        fila = idx + 2  # índice Excel-like
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
    # Captura tanto ?regionales=R1 como ?regionales=R1&regionales=R2
    regionales: Optional[List[str]] = Query(None, description="Opcional: lista de regionales")
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
    filtro: Dict[str, any] = {
        "fecha_creacion": {
            "$gte": f"{fecha_inicial} 00:00:00",
            "$lte": f"{fecha_final} 23:59:59"
        }
    }

    # Si es ADMIN/COORDINADOR/GERENTE/Analista y envió regionales, úsalas; de lo contrario, su regional por cookie
    if perfil in {"ADMIN", "COORDINADOR", "GERENTE", "ANALISTA"}:
        if regionales:
            filtro["regional"] = {"$in": [r.upper().strip() for r in regionales]}
    else:
        filtro["regional"] = reg_user

    # 4) traer documentos
    docs = list(coleccion_pedidos_completados.find(filtro))
    if not docs:
        raise HTTPException(404, "No se encontraron pedidos en ese rango.")

    # 5) convertir ObjectId a string
    for d in docs:
        d["id"] = str(d.pop("_id"))

    # 6) DataFrame y Excel
    df = pd.DataFrame(docs)
    out = BytesIO()
    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False, sheet_name="Completados")
    out.seek(0)

    # 7) devolver descarga
    fn = f"pedidos_completados_{datetime.now():%Y%m%d_%H%M%S}.xlsx"
    return StreamingResponse(
        out,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={fn}"}
    )


# ------------------------------
# 🗂 Listar sólo vehículos COMPLETADOS
# ------------------------------
@ruta_pedidos.post(
    "/listar-vehiculo-completados",
    response_model=List[dict],
    summary="Listar sólo vehículos 100% COMPLETADOS"
)
async def listar_vehiculos_completados(
    datos: FiltrosConUsuario,
    fecha_inicial: str = Query(..., description="Fecha inicial YYYY-MM-DD"),
    fecha_final:   str = Query(..., description="Fecha final YYYY-MM-DD"),
):
    usuario = datos.usuario.upper().strip()
    filtros = datos.filtros or FiltrosPedidos()

    # 1) Validar usuario y permisos
    user = coleccion_usuarios.find_one({"usuario": usuario})
    if not user:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Usuario no encontrado")
    perfil = user["perfil"].upper()
    regional_usuario = user["regional"].upper()

    # 2) Validar formato de fechas
    try:
        datetime.strptime(fecha_inicial, "%Y-%m-%d")
        datetime.strptime(fecha_final,   "%Y-%m-%d")
    except:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Formato de fecha inválido. Use YYYY-MM-DD.")

    # 3) Construir filtro base (fecha + estado opcional)
    filtro: Dict[str, any] = {
        "fecha_creacion": {
            "$gte": f"{fecha_inicial} 00:00:00",
            "$lte": f"{fecha_final} 23:59:59"
        }
    }
    if filtros.estados:
        filtro["estado"] = {"$in": [e.upper().strip() for e in filtros.estados]}

    # 4) Filtrar por regional según perfil
    if perfil in {"ADMIN", "COORDINADOR", "GERENTE", "ANALISTA"} and filtros.regionales:
        filtro["regional"] = {"$in": [r.upper().strip() for r in filtros.regionales]}
    else:
        filtro["regional"] = regional_usuario

    # 5) Pipeline de agregación
    pipeline = [
        {"$match": filtro},

        # 1) Traer cliente por NIT
        {"$lookup": {
            "from": "clientes",
            "localField": "nit_cliente",
            "foreignField": "nit",
            "as": "cliente"
        }},
        {"$unwind": {
            "path": "$cliente",
            "preserveNullAndEmptyArrays": True
        }},

        # 2) Propagar el nombre al documento del pedido (no al grupo)
        {"$set": {"nombre_cliente": {"$ifNull": ["$cliente.nombre", None]}}},

        # (opcional) limpia el objeto cliente para no inflar respuesta
        {"$project": {"cliente": 0}},

        # 3) Agrupa por vehículo, pero SIN nombre_cliente aquí
        {"$group": {
            "_id": "$consecutivo_vehiculo",
            "tipo_vehiculo": {"$first": "$tipo_vehiculo"},
            "tipo_vehiculo_sicetac": {"$first": "$tipo_vehiculo_sicetac"},
            "destino": {"$first": "$destino"},
            "Observaciones_ajustes": {"$first": "$Observaciones_ajustes"},            
            "pedidos": {"$push": "$$ROOT"},  
            "estados": {"$addToSet": "$estado"},
            "flete_solicitado":{"$sum": "$valor_flete"},
            "punto_adicional_total_veh": {"$first": "$total_punto_adicional"},
            "punto_adicional_sum_docs": {"$sum": "$punto_adicional"},            
            "cargue_descargue_total": {"$first": "$total_cargue_descargue"},
            "totales": {"$first": {
                "cajas": "$total_cajas_vehiculo",
                "kilos": "$total_kilos_vehiculo",
                "kilos_sicetac": "$total_kilos_vehiculo_sicetac", 
                "flete": "$total_flete_vehiculo",
                "desvio": "$total_desvio_vehiculo",
                "puntos": "$total_puntos_vehiculo",
                "flete_sistema": "$valor_flete_sistema",
                "punto_teorico": "$punto_adicional_teorico",
                "cargue_teorico": "$cargue_descargue_teorico",
                "costo_real": "$total_flete_vehiculo",
                "diferencia": "$diferencia_flete",
            }},
        }},
        {"$set": {
            "punto_adicional_total": {
                "$ifNull": ["$punto_adicional_total_veh", "$punto_adicional_sum_docs"]
            }
        }},
        {"$sort": {"_id": 1}}
    ]


    grupos = list(coleccion_pedidos_completados.aggregate(pipeline))
    if not grupos:
        return [] 

    # 6) Formar la respuesta con los mismos campos que el multiestado
    respuesta = []
    for g in grupos:
        totales = g["totales"]
        costo_teorico = (
            totales.get("flete_sistema", 0.0)
            + totales.get("punto_teorico", 0.0)
            + totales.get("cargue_teorico", 0.0)
            + totales.get("desvio", 0.0)
        )
        costo_real = totales.get("costo_real", 0.0)
        diferencia = totales.get("diferencia", costo_real - costo_teorico)

        respuesta.append({
            "consecutivo_vehiculo":            g["_id"],
            "tipo_vehiculo":                   g["tipo_vehiculo"],
            "tipo_vehiculo_sicetac":           g.get("tipo_vehiculo_sicetac"),
            "destino":                         g["destino"],
            "multiestado":                     False,
            "estados":                         g["estados"],

            # Totales reales
            "total_cajas_vehiculo":            totales.get("cajas", 0),
            "total_kilos_vehiculo":            totales.get("kilos", 0.0),
            "total_kilos_vehiculo_sicetac":    totales.get("kilos_sicetac", 0.0),
            "total_flete_vehiculo":            costo_real,
            "total_desvio_vehiculo":           totales.get("desvio", 0.0),
            "total_puntos_vehiculo":           totales.get("puntos", 0),

            # Totales teóricos
            "valor_flete_sistema":             totales.get("flete_sistema", 0.0),
            "total_punto_adicional_teorico":   totales.get("punto_teorico", 0.0),
            "total_cargue_descargue_teorico":  totales.get("cargue_teorico", 0.0),
            "costo_teorico_vehiculo":          costo_teorico,

            # Diferenciales y adicionales
            "costo_real_vehiculo":             costo_real,
            "diferencia_flete":                diferencia,
            "total_punto_adicional":           g.get("punto_adicional_total", 0.0),
            "total_cargue_descargue":          g.get("cargue_descargue_total", 0.0),
            "total_flete_solicitado":          g.get("flete_solicitado", 0.0),

            # Detalle de pedidos
            "pedidos":                         [modelo_pedido(p) for p in g["pedidos"]],
        })

    return respuesta



# ------------------------------
# 🗂 Fusionar vehículos (con logs y errores claros)
# ------------------------------
@ruta_pedidos.post(
    "/fusionar-vehiculos",
    response_model=dict,
    summary="Fusionar 2+ consecutivo_vehiculo en uno solo, recalculando totales y estado"
)
async def fusionar_vehiculos(payload: FusionVehiculosPayload):
    try:
        usuario = (payload.usuario or "").upper().strip()
        user = coleccion_usuarios.find_one({"usuario": usuario})
        if not user:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "Usuario no encontrado")

        perfil = (user.get("perfil") or "").upper()
        if perfil not in {"ADMIN", "DESPACHADOR", "OPERADOR"}:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "No tienes permisos para fusionar vehículos")

        # 1) Sanitizar consecutivos (mínimo 2)
        consecutivos = [c.strip() for c in (payload.consecutivos or []) if c and c.strip()]
        if len(consecutivos) < 2:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Debes enviar al menos 2 consecutivo_vehiculo")

        # 🔐 Sólo estados permitidos
        estados_permitidos = {
            "PREAUTORIZADO",
            "REQUIERE AUTORIZACION COORDINADOR",
            "REQUIERE AUTORIZACION GERENTE",
        }

        for cv in consecutivos:
            total_cv = coleccion_pedidos.count_documents({"consecutivo_vehiculo": cv})
            if total_cv == 0:
                raise HTTPException(status.HTTP_404_NOT_FOUND, f"{cv}: no se encontró ningún documento")

            fuera_permitidos = coleccion_pedidos.count_documents({
                "consecutivo_vehiculo": cv,
                "estado": {"$nin": list(estados_permitidos)}
            })
            if fuera_permitidos > 0:
                raise HTTPException(
                    status.HTTP_400_BAD_REQUEST,
                    f"{cv}: solo se pueden fusionar vehículos en PREAUTORIZADO o REQUIERE AUTORIZACION (Coord./Gerente)"
                )

        # 2) Traer docs de todos los consecutivos
        docs = list(coleccion_pedidos.find({"consecutivo_vehiculo": {"$in": consecutivos}}))
        if not docs:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "No se encontraron documentos para esos consecutivos")

        # Defensa extra
        if any((d.get("estado") or "").upper() == "COMPLETADO" for d in docs):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "No se puede fusionar: hay documentos en estado COMPLETADO")

        # 3) Regional homogénea y permiso por regional
        regionales = {(d.get("regional") or "").upper() for d in docs}
        if len(regionales) != 1:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Todos los consecutivos deben pertenecer a la misma regional")
        regional_doc = next(iter(regionales))
        if perfil in {"DESPACHADOR", "OPERADOR"} and (user.get("regional") or "").upper() != regional_doc:
            raise HTTPException(status.HTTP_403_FORBIDDEN, f"Sin permiso: tu regional es distinta ({regional_doc})")

        # 4) Mismo ORIGEN (para tarifario)
        origenes = {(d.get("origen") or "").upper() for d in docs}
        if len(origenes) != 1:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Todos los consecutivos deben tener el mismo ORIGEN para poder fusionar")
        origen = next(iter(origenes))

        # 5) Consecutivo resultante = primero
        target_cv = consecutivos[0]

        # 6) Agregados por documentos
        total_cajas = sum(int(d.get("num_cajas", 0) or 0) for d in docs)
        total_kilos = sum(float(d.get("num_kilos", 0) or 0) for d in docs)
        total_kilos_sic = sum(float(d.get("num_kilos_sicetac", d.get("num_kilos", 0)) or 0) for d in docs)
        total_puntos_docs = sum(int(d.get("total_puntos", 0) or 0) for d in docs)

        # 7) Tarifas / otros costos según tipo y destino nuevos
        tipo_sic = (payload.tipo_vehiculo_sicetac or "").upper().strip()
        nuevo_destino = (payload.nuevo_destino or "").upper().strip()
        if not tipo_sic or not nuevo_destino:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "Debes enviar tipo_vehiculo_sicetac y nuevo_destino")

        tf = db["tarifas"].find_one({"origen": origen, "destino": nuevo_destino})
        if not tf or "tarifas" not in tf or tipo_sic not in tf["tarifas"]:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                f"No hay tarifa para {origen}→{nuevo_destino} con tipo '{tipo_sic}'"
            )
        tbase = float(tf["tarifas"][tipo_sic])

        otros = db["otros_costos"].find_one({"tipo_vehiculo": tipo_sic})
        if not otros:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                f"No hay configuración de 'otros_costos' para el tipo '{tipo_sic}'"
            )
        max_p = int(otros.get("max_puntos", 0) or 0)
        val_pto = float(otros.get("valor_punto_adicional", 0) or 0)
        cargue_teorico = float(otros.get("cargue_descargue", 0) or 0)

        exceso = max(0, total_puntos_docs - max_p)
        pto_teorico = exceso * val_pto
        costo_teorico = tbase + pto_teorico + cargue_teorico

        # 8) Overrides solicitados
        total_flete_solicitado = float(payload.total_flete_solicitado or 0)
        total_cargue_descargue = float(payload.total_cargue_descargue or 0)
        total_punto_adicional  = float(payload.total_punto_adicional or 0)
        total_desvio_vehiculo  = float(payload.total_desvio_vehiculo or 0)

        costo_real = total_flete_solicitado + total_cargue_descargue + total_desvio_vehiculo + total_punto_adicional

        estado_calc, porc = estado_por_autorizacion(costo_real, costo_teorico)
        ahora_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 9) Update masivo
        set_fields = {
            "consecutivo_vehiculo":             target_cv,
            "destino":                          nuevo_destino,
            "tipo_vehiculo_sicetac":            tipo_sic,

            # Totales vehiculares
            "total_cajas_vehiculo":             total_cajas,
            "total_kilos_vehiculo":             total_kilos,
            "total_kilos_vehiculo_sicetac":     total_kilos_sic,
            "total_puntos_vehiculo":            total_puntos_docs,

            # Teóricos
            "valor_flete_sistema":              tbase,
            "punto_adicional_teorico":          pto_teorico,
            "cargue_descargue_teorico":         cargue_teorico,
            "costo_teorico_vehiculo":           costo_teorico,

            # Solicitados (overrides)
            "total_flete_solicitado":           total_flete_solicitado,
            "total_cargue_descargue":           total_cargue_descargue,
            "total_punto_adicional":            total_punto_adicional,
            "total_desvio_vehiculo":            total_desvio_vehiculo,

            # Reales y diferencia
            "total_flete_vehiculo":             costo_real,
            "diferencia_flete":                 costo_real - costo_teorico,

            # Estado
            "estado":                           estado_calc,
            "porcentaje_sobre_teorico":         porc,
            "autorizado_por":                   "SISTEMA" if estado_calc == "PREAUTORIZADO" else "NA",
            "fecha_autorizacion":               ahora_str if estado_calc == "PREAUTORIZADO" else "NA",

            # Trazabilidad fusión
            "usuario_fusion":                   usuario,
            "observacion_fusion":               (payload.observacion_fusion or ""),
            "fecha_fusion":                     ahora_str,
        }

        res = coleccion_pedidos.update_many(
            {"consecutivo_vehiculo": {"$in": consecutivos}},
            {"$set": set_fields}
        )

        # 📌 Log útil en backend (aparece en consola del server)
        print("[fusionar_vehiculos] OK",
              {"consecutivos": consecutivos, "target": target_cv, "docs_actualizados": res.modified_count})

        return {
            "mensaje": f"Fusionados {len(consecutivos)} consecutivos en '{target_cv}'",
            "consecutivo_resultante": target_cv,
            "docs_actualizados": res.modified_count,
            "totales": {
                "total_cajas_vehiculo":         total_cajas,
                "total_kilos_vehiculo":         total_kilos,
                "total_kilos_vehiculo_sicetac": total_kilos_sic,
                "total_puntos_vehiculo":        total_puntos_docs,
                "valor_flete_sistema":          tbase,
                "punto_adicional_teorico":      pto_teorico,
                "cargue_descargue_teorico":     cargue_teorico,
                "costo_teorico_vehiculo":       costo_teorico,
                "total_flete_solicitado":       total_flete_solicitado,
                "total_cargue_descargue":       total_cargue_descargue,
                "total_punto_adicional":        total_punto_adicional,
                "total_desvio_vehiculo":        total_desvio_vehiculo,
                "total_flete_vehiculo":         costo_real,
                "diferencia_flete":             costo_real - costo_teorico,
            },
            "estado": {
                "nuevo_estado":             estado_calc,
                "porcentaje_sobre_teorico": porc
            },
            "consecutivos_fusionados": consecutivos
        }

    except HTTPException:
        # Deja pasar errores con detail claro para que el front los vea
        raise
    except Exception as e:
        # Log del stacktrace y error claro al front (evita {} vacíos)
        import traceback
        print("[fusionar_vehiculos][ERROR]", traceback.format_exc())
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno en fusionar_vehiculos: {e}"
        )