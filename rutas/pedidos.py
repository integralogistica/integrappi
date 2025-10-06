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
import time
from collections import defaultdict 
from zoneinfo import ZoneInfo

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
    descargue_kabi: float
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
    nuevo_destino: Optional[str] = None
    destino_desde_real: Optional[str] = None



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

# ====== MODELOS PARA DIVIDIR EN HASTA 3 CARROS ======
class OverridesVehiculo(BaseModel):
    # (opcionales) si no se envían, se usan sumas por documento
    total_flete_solicitado: Optional[float] = None
    total_cargue_descargue: Optional[float] = None
    total_punto_adicional: Optional[float] = None
    total_desvio_vehiculo: Optional[float] = None

class SplitConfig(BaseModel):
    # uno de los dos:
    doc_id: Optional[str] = None
    consecutivo_integrapp: Optional[str] = None
    kilos: float
    cajas: Optional[int] = None

class GrupoDivision(BaseModel):
    # Puedes indicar el subset por destinatarios o por consecutivos_integrapp:
    destinatarios: Optional[List[str]] = None
    consecutivos_integrapp: Optional[List[str]] = None
    overrides: Optional[OverridesVehiculo] = None
    split: Optional[SplitConfig] = None  

class DividirHastaTresPayload(BaseModel):
    usuario: str
    consecutivo_origen: str
    destino_unico: str                    # obligatorio: todos quedan con este destino
    observacion_division: Optional[str] = None
    # Grupos A/B/C. A = conserva consecutivo; B y C son opcionales
    grupo_A: Optional[GrupoDivision] = None
    grupo_B: Optional[GrupoDivision] = None
    grupo_C: Optional[GrupoDivision] = None
    # Campo que identifica al "Destinatario" en tus docs. Por defecto 'destinatario'
    campo_destinatario: Optional[str] = "destinatario"


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
      - 'REQUIERE AUTORIZACION CONTROL'
    """
    if costo_teorico <= 0:
        return ("REQUIERE AUTORIZACION CONTROL", 0.0)

    diff = costo_real - costo_teorico
    porc = round((diff / costo_teorico) * 100.0, 2)

    if diff <= 0:
        return ("PREAUTORIZADO", max(porc, 0.0))

    if porc <= 7.0:
        return ("REQUIERE AUTORIZACION COORDINADOR", porc)

    return ("REQUIERE AUTORIZACION CONTROL", porc)


# Jerarquía para autorizar en función del estado textual
def perfil_puede_autorizar(perfil: str, estado: str) -> bool:
    p = (perfil or "").upper()
    e = (estado or "").upper()
    if p == "ADMIN":
        return True
    if "CONTROL" in e:
        return p == "CONTROL"
    if "COORDINADOR" in e:
        return p in {"COORDINADOR", "CONTROL"}
    return False  # por seguridad



# ======== Visibilidad/permiso por regional ========
REGIONALES_PAREADAS = {"CELTA", "FUNZA"}

def regionales_visibles_para(user: dict):
    """
    Devuelve:
      - None  -> sin restricción (ADMIN/COORDINADOR/CONTROL/ANALISTA)
      - [..]  -> lista de regionales visibles (DESPACHADOR/OPERADOR: CELTA+FUNZA si aplica; de lo contrario, su propia regional)
    """
    perfil = (user.get("perfil") or "").upper()
    reg    = (user.get("regional") or "").upper()

    if perfil in {"ADMIN", "COORDINADOR", "CONTROL", "ANALISTA"}:
        return None  # sin restricción por regional, se respeta lo que manden por filtros

    if perfil in {"DESPACHADOR", "OPERADOR"}:
        if reg in REGIONALES_PAREADAS:
            return list(REGIONALES_PAREADAS)  # puede ver CELTA y FUNZA
        return [reg]

    # perfiles no contemplados: restringir a su regional por seguridad
    return [reg]


def usuario_puede_operar_en_regional(user: dict, regional_doc: str) -> bool:
    """
    True si el usuario está autorizado para operar (ajustar/autorizar/etc.) en la regional del documento.
    DESPACHADOR/OPERADOR de CELTA o FUNZA puede operar en ambas.
    """
    perfil = (user.get("perfil") or "").upper()
    reg_doc = (regional_doc or "").upper()
    reg_user = (user.get("regional") or "").upper()

    if perfil == "ADMIN":
        return True

    if perfil in {"COORDINADOR", "CONTROL", "ANALISTA"}:
        # estos roles suelen operar multi-región (ajusta si quieres limitar)
        return True

    if perfil in {"DESPACHADOR", "OPERADOR"}:
        if reg_user in REGIONALES_PAREADAS and reg_doc in REGIONALES_PAREADAS:
            return True
        return reg_doc == reg_user

    return False


# ------------------------
# carga masivo excel
# ------------------------
@ruta_pedidos.post(
    "/cargar-masivo",
    response_model=dict,
    summary="Cargar masivo para autorizar"
)
async def cargar_masivo(creado_por: str = Form(...), archivo: UploadFile = File(...)):
    import unicodedata
    start_time = time.time()

    # 1) Usuario y prefijo
    usuario_db = db["baseusuarios"].find_one({"usuario": creado_por.upper().strip()})
    if not usuario_db:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Usuario no encontrado")
    region = (usuario_db["regional"] or "").upper().strip()
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
        "TIPO_VEHICULO","TIPO_VEHICULO_SICETAC","VEHICULO","VALOR_DECLARADO","PLANILLA_SISCORE",
        "VALOR_FLETE","UBICACION_CARGUE","DIRECCION_CARGUE",
        "UBICACION_DESCARGUE","DIRECCION_DESCARGUE","OBSERVACIONES",
        "TIPO_VIAJE","CONSECUTIVO_PEDIDO","DESVIO","CARGUE_DESCARGUE","DESCARGUE_KABI",
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
    destinos_reales_por_veh = {}  # set de DESTINO_REAL únicos por vehículo

    # Helper para números
    def to_num(campo: str, valor: str) -> float:
        try:
            return float(valor) if valor else 0.0
        except Exception:
            raise ValueError(f"{campo} '{valor}' no es numérico")

    # 4) Procesar cada fila
    tarifas_col = db["tarifas"]
    otros_col = db["otros_costos"]
    clientes_col = db["clientes"]
    pedidos_col = db["pedidos"]

    for idx, fila in df_pedidos.iterrows():
        num_fila = idx + 2
        vehiculo = fila["VEHICULO"].upper()

        # tipo_vehiculo (principal) y sicetac
        tipo_veh = fila["TIPO_VEHICULO"].upper()
        tipo_veh_sic = (fila.get("TIPO_VEHICULO_SICETAC", "") or "").upper() or tipo_veh

        # consecutivo
        try:
            cons = int(fila["CONSECUTIVO_PEDIDO"])
        except Exception:
            errores.append(f"{prefijo}Fila {num_fila}: CONSECUTIVO_PEDIDO '{fila['CONSECUTIVO_PEDIDO']}' no es numérico")
            continue

        if cons in vistos_cons and vistos_cons[cons] != vehiculo:
            errores.append(f"{prefijo}Fila {num_fila}: CONSECUTIVO_PEDIDO duplicado en {vehiculo}")
            continue
        vistos_cons[cons] = vehiculo

        # consistencia tipo y destino
        if vehiculo in tipo_por_veh and tipo_por_veh[vehiculo] != tipo_veh:
            errores.append(f"{prefijo}Fila {num_fila}: TIPO_VEHICULO inconsistente para {vehiculo}")
            continue
        tipo_por_veh[vehiculo] = tipo_veh

        destino = fila["DESTINO"].upper()
        if vehiculo in destino_por_veh and destino_por_veh[vehiculo] != destino:
            errores.append(f"{prefijo}Fila {num_fila}: DESTINO inconsistente para {vehiculo}")
            continue
        destino_por_veh[vehiculo] = destino

        # valor_flete
        try:
            valor_flete = float(fila["VALOR_FLETE"])
        except Exception:
            errores.append(f"{prefijo}Fila {num_fila}: VALOR_FLETE '{fila['VALOR_FLETE']}' no es numérico")
            continue

        # tipo viaje
        tipo_viaje = fila["TIPO_VIAJE"].upper()
        if tipo_viaje not in {"CARGA MASIVA", "PAQUETEO"}:
            errores.append(f"{prefijo}Fila {num_fila}: TIPO_VIAJE inválido")
            continue

        # cliente existe
        cliente_nit = fila["NIT_CLIENTE"]
        if not clientes_col.find_one({"nit": cliente_nit}):
            errores.append(f"{prefijo}Fila {num_fila}: Cliente '{cliente_nit}' no existe")
            continue

        # tarifa definida
        tf = tarifas_col.find_one({"origen": fila["ORIGEN"].upper(), "destino": destino})
        if not tf or tipo_veh not in tf["tarifas"]:
            errores.append(f"{prefijo}Fila {num_fila}: Tarifa no definida para {fila['ORIGEN']}→{destino}, tipo '{tipo_veh}'")
            continue

        # números adicionales
        try:
            desvio = to_num("DESVIO", fila["DESVIO"])
            cargue = to_num("CARGUE_DESCARGUE", fila["CARGUE_DESCARGUE"])
            descargue_kabi=to_num("DESCARGUE_KABI", fila["DESCARGUE_KABI"])
            punto_extra = to_num("PUNTO_ADICIONAL", fila["PUNTO_ADICIONAL"])
            puntos = int(fila["TOTAL_PUNTOS"])
            cajas = int(fila["NUM_CAJAS"])
            kilos = float(fila["NUM_KILOS"])
        except Exception as e:
            errores.append(f"{prefijo}Fila {num_fila}: {e}")
            continue

        # num_kilos_sicetac (si no viene, usa kilos)
        try:
            if "NUM_KILOS_SICETAC" in df_pedidos.columns and str(fila.get("NUM_KILOS_SICETAC", "")).strip() != "":
                kilos_sic = float(fila["NUM_KILOS_SICETAC"])
            else:
                kilos_sic = kilos
        except Exception:
            errores.append(f"{prefijo}Fila {num_fila}: NUM_KILOS_SICETAC '{fila.get('NUM_KILOS_SICETAC')}' no es numérico")
            continue

        # DESTINO_REAL por vehículo (para puntos por destinos únicos)
        destino_real_up = (fila["DESTINO_REAL"] or "").upper().strip()
        if vehiculo not in destinos_reales_por_veh:
            destinos_reales_por_veh[vehiculo] = set()
        if destino_real_up:
            destinos_reales_por_veh[vehiculo].add(destino_real_up)

        # acumuladores por vehículo
        reales_por_veh[vehiculo] = reales_por_veh.get(vehiculo, 0.0) + valor_flete + desvio + cargue + punto_extra
        desviaciones_por_veh[vehiculo] = desviaciones_por_veh.get(vehiculo, 0.0) + desvio
        cajas_por_veh[vehiculo] = cajas_por_veh.get(vehiculo, 0) + cajas
        kilos_por_veh[vehiculo] = kilos_por_veh.get(vehiculo, 0.0) + kilos
        kilos_sic_por_veh[vehiculo] = kilos_sic_por_veh.get(vehiculo, 0.0) + kilos_sic
        puntos_por_veh[vehiculo] = puntos_por_veh.get(vehiculo, 0) + puntos

        # evitar consecutivo_integrapp repetido
        cons_int = f"{region}-{fecha_corta}-{cons}"
        if pedidos_col.find_one({
            "consecutivo_integrapp": cons_int,
            "estado": {"$in": [
                "PREAUTORIZADO",
                "REQUIERE AUTORIZACION COORDINADOR",
                "REQUIERE AUTORIZACION CONTROL",
                "AUTORIZADO"
            ]}
        }):
            errores.append(f"{prefijo}Fila {num_fila}: Consecutivo_integrapp ya usado: {cons_int}")
            continue

        # registrar (fila -> documento)
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
            "descargue_kabi": descargue_kabi,
            "punto_adicional": punto_extra,
            "total_puntos": puntos,
            "flete_real": float(fila["FLETE_REAL"] or 0),
            "destino_real": destino_real_up,
            "creado_por": usuario_db["usuario"],
            "regional": region,
            "consecutivo_pedido": cons,
            "consecutivo_integrapp": cons_int,
            "consecutivo_vehiculo": f"{region}-{fecha_corta}-{vehiculo}"
        })

    if errores:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail={"mensaje": "Errores en archivo masivo", "errores": errores}
        )

    # 5) Calcular teóricos y estado (punto adicional independiente del cargue)
    def _is_truthy(v) -> bool:
        s = str(v or "").strip()
        s = unicodedata.normalize("NFKD", s)
        s = "".join(c for c in s if not unicodedata.combining(c)).upper()
        return s in {"SI", "S", "1", "TRUE", "VERDADERO", "YES", "Y"}

    for r in registros:
        veh = r["vehiculo"]
        real = float(reales_por_veh.get(veh, 0.0))
        desvio_total = float(desviaciones_por_veh.get(veh, 0.0))
        origen, destino = r["origen"], r["destino"]

        tf_doc = tarifas_col.find_one({"origen": origen, "destino": destino})
        if not tf_doc:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"No hay tarifa para {origen}→{destino}")

        # Base por tipo de vehículo
        tbase = float(tf_doc["tarifas"][r["tipo_vehiculo"]])

        # Otros costos (por tipo de vehículo)
        otros = otros_col.find_one({"tipo_vehiculo": r["tipo_vehiculo"]}) or {}
        val_pto = float(otros.get("valor_punto_adicional", 0) or 0)
        cargue_cfg = float(otros.get("cargue_descargue", 0) or 0)

        # Flag para cargue/descargue
        paga_cd = _is_truthy(tf_doc.get("pago_cargue_desc"))

        # Puntos: max entre destinos reales únicos y lo sumado del Excel
        destinos_unicos = len(destinos_reales_por_veh.get(veh, set()))
        puntos_excel = int(puntos_por_veh.get(veh, 0) or 0)
        total_puntos_calc = max(destinos_unicos, puntos_excel)

        # Punto adicional teórico (independiente del flag de cargue)
        adicionales = max(0, total_puntos_calc - 1)
        pad_teo = adicionales * val_pto

        # Cargue/descargue teórico (solo si la tarifa lo paga)
        cargue_teo = cargue_cfg if paga_cd else 0.0

        # Costos y estado
        costo_teorico = tbase + pad_teo + cargue_teo
        costo_real = real
        estado_calc, porc = estado_por_autorizacion(costo_real, costo_teorico)

        r.update({
            "valor_flete_sistema": tbase,
            "total_flete_vehiculo": costo_real,
            "total_desvio_vehiculo": desvio_total,
            "total_cajas_vehiculo": int(cajas_por_veh.get(veh, 0)),
            "total_kilos_vehiculo": float(kilos_por_veh.get(veh, 0.0)),
            "total_kilos_vehiculo_sicetac": float(kilos_sic_por_veh.get(veh, 0.0)),
            "total_puntos_vehiculo": total_puntos_calc,
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
    elapsed = round(time.time() - start_time, 3)

    return {
        "mensaje": f"{vehiculos_cargados} vehículo{'s' if vehiculos_cargados > 1 else ''} cargado{'s' if vehiculos_cargados > 1 else ''}",
        "tiempo_segundos": elapsed,
        "detalles": detalles
    }


# -----------------------------------------------------
# 🗂 Solicitar ajustes por consecutivo_vehiculo 
# -----------------------------------------------------
@ruta_pedidos.put(
    "/ajustar-totales-vehiculo",
    response_model=dict,
    summary="Ajustar totales por vehiculo y recalcular estado (permite agregar línea extra para destinos especiales)"
)
async def ajustar_totales_vehiculo(payload: AjustesVehiculosPayload):
    usuario = payload.usuario.upper().strip()
    solicitante = usuario

    # 0) Mapeo de ciudades especiales -> ubicacion_descargue
    SPECIAL_DESTS = {
        "GIRARDOTA":     "FKC_INTEGRA_GIRARDOTA",
        "BARRANQUILLA":  "FKC_INTEGRA_BARRANQUILLA",
        "YUMBO":         "FKC_INTEGRA_YUMBO",
        "BUCARAMANGA":   "FKC_INTEGRA_BUCARAMANGA",
    }

    # 1) Validar usuario y perfil
    user = coleccion_usuarios.find_one({"usuario": usuario})
    if not user:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Usuario no encontrado")

    perfil = (user.get("perfil") or "").upper()
    if perfil not in {"ADMIN", "DESPACHADOR", "ANALISTA", "OPERADOR"}:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "No tienes permisos para ajustar vehículos")

    if not payload.ajustes:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "Debes enviar al menos un ajuste")

    resultados, errores = [], []
    ahora_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    YES = {"SI", "S", "1", "TRUE", "VERDADERO", "YES", "Y"}

    def _norm(s) -> str:
        import unicodedata, re
        s = "" if s is None else str(s)
        s = unicodedata.normalize("NFKD", s.strip())
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
        return re.sub(r"\s+", " ", s).upper()

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
        if not usuario_puede_operar_en_regional(user, regional_doc):
            errores.append(f"{cv}: sin permiso, el vehículo pertenece a la regional {regional_doc}")
            continue

        # 4) Bloquear COMPLETADO
        if any((d.get("estado") or "").upper() == "COMPLETADO" for d in docs):
            errores.append(f"{cv}: no se puede ajustar, contiene documentos COMPLETADO")
            continue

        # 5) Sumas reales por documentos (antes de cualquier inserción)
        doc0 = docs[0]
        suma_flete = sum(float(d.get("valor_flete", 0) or 0) for d in docs)
        suma_cargue = sum(float(d.get("cargue_descargue", 0) or 0) for d in docs)
        suma_descargue_kabi = sum(float(d.get("descargue_kabi", 0) or 0) for d in docs)

        desvio_actual = float(doc0.get("total_desvio_vehiculo", 0) or sum(float(d.get("desvio", 0) or 0) for d in docs))
        punto_extra_actual = float(doc0.get("total_punto_adicional", 0) or sum(float(d.get("punto_adicional", 0) or 0) for d in docs))
        kilos_sic_actual = float(doc0.get("total_kilos_vehiculo_sicetac", 0) or sum(float(d.get("num_kilos_sicetac", 0) or 0) for d in docs))
        tipo_sic_actual = (doc0.get("tipo_vehiculo_sicetac") or doc0.get("tipo_vehiculo") or "").upper()

        # 6) Overrides recibidos
        total_desvio_vehiculo = float(adj.total_desvio_vehiculo) if adj.total_desvio_vehiculo is not None else desvio_actual
        total_punto_adicional = float(adj.total_punto_adicional) if adj.total_punto_adicional is not None else punto_extra_actual
        total_kilos_vehiculo_sicetac = float(adj.total_kilos_vehiculo_sicetac) if adj.total_kilos_vehiculo_sicetac is not None else kilos_sic_actual
        tipo_vehiculo_sicetac = (adj.tipo_vehiculo_sicetac or tipo_sic_actual).upper()

        total_cargue_descargue = float(adj.total_cargue_descargue) if getattr(adj, "total_cargue_descargue", None) is not None else float(suma_cargue)
        total_flete_solicitado = float(adj.total_flete_solicitado) if getattr(adj, "total_flete_solicitado", None) is not None else float(suma_flete)

        # === Mayor entre cargue y descargue_kabi por vehículo ===
        total_cargue_per_juridica = max(float(total_cargue_descargue), float(suma_descargue_kabi))

        # ====== Edición de DESTINO / o Adición de línea especial ======
        destino_actual = (doc0.get("destino") or "").upper().strip()
        origenes = {(d.get("origen") or "").upper().strip() for d in docs}
        if len(origenes) == 0:
            errores.append(f"{cv}: no tiene ORIGEN en los documentos")
            continue
        origen = next(iter(origenes))

        destino_seleccionado = None
        add_line_for_special = False
        special_city = None

        if (adj.nuevo_destino or adj.destino_desde_real):
            # Si viene 'nuevo_destino', puede ser ciudad especial o destino normal
            if adj.nuevo_destino:
                candidato = _norm(adj.nuevo_destino)
                if candidato in SPECIAL_DESTS:
                    # Agregar línea (si falta) Y además mover el vehículo a esa ciudad
                    add_line_for_special = True
                    special_city = candidato
                    destino_seleccionado = candidato  # 👈 también cambia el destino del vehículo
                else:
                    # Cambio de destino normal (como antes)
                    destino_seleccionado = candidato
            else:
                # Selección desde un destino_real existente (comportamiento previo)
                destinos_reales_set = {_norm(d.get("destino_real")) for d in docs if (d.get("destino_real") or "").strip()}
                candidato = _norm(adj.destino_desde_real)
                if candidato not in destinos_reales_set:
                    errores.append(f"{cv}: 'destino_desde_real' no coincide con los destino_real del vehículo")
                    continue
                destino_seleccionado = candidato

        # 6.1) Si debe adicionarse una línea para ciudad especial y aún no existe, insertar doc "virtual"
        if add_line_for_special:
            destinos_reales_set = {_norm(d.get("destino_real")) for d in docs if (d.get("destino_real") or "").strip()}
            if special_city not in destinos_reales_set:
                from copy import deepcopy
                nuevo = deepcopy(doc0)
                nuevo.pop("_id", None)
                nuevo["fecha_creacion"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # Marca el destino real y la ubicación de la bodega especial
                nuevo["destino_real"] = special_city
                nuevo["ubicacion_descargue"] = SPECIAL_DESTS[special_city]

                # Cantidades/valores en 0 para no alterar agregados
                nuevo["num_cajas"] = 0
                nuevo["num_kilos"] = 0.0
                nuevo["num_kilos_sicetac"] = 0.0
                nuevo["valor_flete"] = 0.0
                nuevo["desvio"] = 0.0
                nuevo["cargue_descargue"] = 0.0
                nuevo["descargue_kabi"] = 0.0
                nuevo["punto_adicional"] = 0.0

                # Trazabilidad
                nuevo["observaciones"] = f"{(doc0.get('observaciones') or '').upper()} | SE ENVIA A BODEGA"

                # ❗❗ Mantener mismos CI/CP para que el nuevo punto herede el mismo código
                nuevo["consecutivo_integrapp"] = doc0.get("consecutivo_integrapp", "")
                nuevo["consecutivo_pedido"] = doc0.get("consecutivo_pedido", "")

                coleccion_pedidos.insert_one(nuevo)

                # Refrescar docs y sumas
                docs = list(coleccion_pedidos.find({"consecutivo_vehiculo": cv}))
                suma_flete = sum(float(d.get("valor_flete", 0) or 0) for d in docs)
                suma_cargue = sum(float(d.get("cargue_descargue", 0) or 0) for d in docs)
                suma_descargue_kabi = sum(float(d.get("descargue_kabi", 0) or 0) for d in docs)

        # 7) Recalcular real/teórico y estado
        if destino_seleccionado:
            tf = db["tarifas"].find_one({"origen": origen, "destino": destino_seleccionado})
            if not tf or "tarifas" not in tf or tipo_vehiculo_sicetac not in tf["tarifas"]:
                errores.append(
                    f"{cv}: No hay tarifa para {origen}→{destino_seleccionado} con tipo '{tipo_vehiculo_sicetac}'"
                )
                continue
            tbase = float(tf["tarifas"][tipo_vehiculo_sicetac])

            otros = db["otros_costos"].find_one({"tipo_vehiculo": tipo_vehiculo_sicetac}) or {}
            val_pto_cfg = float(otros.get("valor_punto_adicional", 0) or 0)
            cargue_cfg = float(otros.get("cargue_descargue", 0) or 0)

            paga_cd = str(tf.get("pago_cargue_desc", "")).strip().upper() in YES

            destinos_unicos = len({_norm(d.get("destino_real")) for d in docs if _norm(d.get("destino_real")) != ""})
            puntos_excel = sum(int(d.get("total_puntos", 0) or 0) for d in docs)
            total_puntos_calc = max(destinos_unicos, puntos_excel)

            adicionales = max(0, total_puntos_calc - 1)
            punto_adicional_teorico = adicionales * val_pto_cfg
            cargue_descargue_teorico = cargue_cfg if paga_cd else 0.0
            valor_flete_sistema = tbase
            destino_a_reportar = destino_seleccionado
        else:
            # Mantener destino y actualizar teóricos (p.ej. si solo se añadió línea especial)
            tf_doc = db["tarifas"].find_one({"origen": origen, "destino": destino_actual})
            if not tf_doc or "tarifas" not in tf_doc:
                valor_flete_sistema = float(doc0.get("valor_flete_sistema", 0) or 0)
                punto_adicional_teorico = float(doc0.get("punto_adicional_teorico", 0) or 0)
                cargue_descargue_teorico = float(doc0.get("cargue_descargue_teorico", 0) or 0)
            else:
                tbase = float(tf_doc["tarifas"].get(tipo_vehiculo_sicetac, doc0.get("valor_flete_sistema", 0.0)) or 0.0)
                otros = db["otros_costos"].find_one({"tipo_vehiculo": tipo_vehiculo_sicetac}) or {}
                val_pto_cfg = float(otros.get("valor_punto_adicional", 0) or 0)
                cargue_cfg = float(otros.get("cargue_descargue", 0) or 0)
                paga_cd = str(tf_doc.get("pago_cargue_desc", "")).strip().upper() in YES

                destinos_unicos = len({_norm(d.get("destino_real")) for d in docs if _norm(d.get("destino_real")) != ""})
                puntos_excel = sum(int(d.get("total_puntos", 0) or 0) for d in docs)
                total_puntos_calc = max(destinos_unicos, puntos_excel)

                adicionales = max(0, total_puntos_calc - 1)
                punto_adicional_teorico = adicionales * val_pto_cfg
                cargue_descargue_teorico = cargue_cfg if paga_cd else 0.0
                valor_flete_sistema = tbase

            destino_a_reportar = destino_actual

        costo_teorico = float(valor_flete_sistema) + float(punto_adicional_teorico) + float(cargue_descargue_teorico)
        # Real: mayor entre cargue y descargue_kabi + desvío + puntos adicionales + flete solicitado
        costo_real = total_flete_solicitado + total_desvio_vehiculo + total_punto_adicional + total_cargue_per_juridica

        estado_calc, porc = estado_por_autorizacion(costo_real, costo_teorico)
        set_aut_por = usuario if estado_calc == "PREAUTORIZADO" else "NA"
        set_fecha_aut = ahora_str if estado_calc == "PREAUTORIZADO" else "NA"

        # 8) Campos a actualizar en TODOS los docs del vehículo
        update_fields = {
            "tipo_vehiculo_sicetac":        tipo_vehiculo_sicetac,
            "total_kilos_vehiculo_sicetac": total_kilos_vehiculo_sicetac,
            "total_desvio_vehiculo":        total_desvio_vehiculo,
            "total_punto_adicional":        total_punto_adicional,
            "total_cargue_descargue":       total_cargue_descargue,
            "total_descargue_kabi":         float(suma_descargue_kabi),
            "total_cargue_per_juridica":    float(total_cargue_per_juridica),
            "total_flete_solicitado":       total_flete_solicitado,
            "usr_solicita_ajuste":          solicitante,

            # Totales y diferenciales
            "total_flete_vehiculo":         costo_real,
            "costo_teorico_vehiculo":       costo_teorico,
            "diferencia_flete":             costo_real - costo_teorico,

            # Estado y % sobre teórico
            "estado":                       estado_calc,
            "porcentaje_sobre_teorico":     porc,
            "autorizado_por":               set_aut_por,
            "fecha_autorizacion":           set_fecha_aut,
        }

        # Actualización de destino/teóricos
        update_fields.update({
            "destino":                   destino_a_reportar,
            "valor_flete_sistema":       valor_flete_sistema,
            "punto_adicional_teorico":   punto_adicional_teorico,
            "cargue_descargue_teorico":  cargue_descargue_teorico,
        })

        if destino_seleccionado:
            update_fields["usuario_ajusta_destino"] = usuario
            update_fields["fecha_ajuste_destino"] = ahora_str

        if adj.Observaciones_ajustes is not None:
            update_fields["Observaciones_ajustes"] = adj.Observaciones_ajustes

        res = coleccion_pedidos.update_many(
            {"consecutivo_vehiculo": cv},
            {"$set": update_fields}
        )

        resultados.append({
            "consecutivo_vehiculo":         cv,
            "regional":                     regional_doc,
            "docs_actualizados":            res.modified_count,
            "usr_solicita_ajuste":          solicitante,
            "tipo_vehiculo_sicetac":        tipo_vehiculo_sicetac,
            "total_kilos_vehiculo_sicetac": total_kilos_vehiculo_sicetac,
            "total_desvio_vehiculo":        total_desvio_vehiculo,
            "total_punto_adicional":        total_punto_adicional,
            "total_cargue_descargue":       total_cargue_descargue,
            "total_descargue_kabi":         float(suma_descargue_kabi),
            "total_cargue_per_juridica":    float(total_cargue_per_juridica),
            "total_flete_solicitado":       total_flete_solicitado,
            "costo_real_vehiculo":          costo_real,
            "costo_teorico_vehiculo":       costo_teorico,
            "diferencia_flete":             costo_real - costo_teorico,
            "nuevo_estado":                 estado_calc,
            "porcentaje_sobre_teorico":     porc,
            "destino_actualizado_a":        destino_a_reportar,
            "linea_especial_agregada":      bool(add_line_for_special),
            "ciudad_especial":              special_city if add_line_for_special else None
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

    perfil = (usuario_db.get("perfil") or "").upper()
    # 🔑 Usa el helper para calcular qué regionales puede ver el usuario.
    #   - ADMIN/COORDINADOR/CONTROL/ANALISTA -> None (sin restricción)
    #   - DESPACHADOR/OPERADOR de CELTA o FUNZA -> ["CELTA","FUNZA"]
    #   - Otros -> [su propia regional]
    visibles = regionales_visibles_para(usuario_db)

    # --------- Construcción del filtro base ----------
    filtro = {}
    if filtros.estados:
        filtro["estado"] = {"$in": [e.upper().strip() for e in filtros.estados]}

    # Regional:
    # - Perfiles amplios: si envían filtros.regionales, se respetan; si no, sin restricción.
    # - Otros (incluye DESPACHADOR/OPERADOR): se restringe a 'visibles' calculado por el helper.
    if perfil in {"ADMIN", "COORDINADOR", "CONTROL", "ANALISTA"}:
        if filtros.regionales:
            filtro["regional"] = {"$in": [r.upper().strip() for r in filtros.regionales]}
        # else: sin filtro de regional (puede ver todas)
    else:
        # visibles siempre es lista para estos perfiles
        filtro["regional"] = {"$in": visibles}

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

            "flete_solicitado_sum_docs": {"$sum": "$valor_flete"},
            "flete_solicitado_override": {"$first": "$total_flete_solicitado"},

            "punto_adicional_total_veh": {"$first": "$total_punto_adicional"},
            "punto_adicional_sum_docs": {"$sum": "$punto_adicional"},

            # 👇 NUEVO: override + suma por documentos
            "cargue_descargue_total_veh": {"$first": "$total_cargue_descargue"},
            "cargue_descargue_sum_docs": {"$sum": "$cargue_descargue"},
            # (opcional) si quieres tenerlo a mano:
            "cargue_per_juridica": {"$first": "$total_cargue_per_juridica"},

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
            },
            # 👇 NUEVO: override vehicular si existe; si no, suma de documentos
            "cargue_descargue_total": {
                "$ifNull": ["$cargue_descargue_total_veh", "$cargue_descargue_sum_docs"]
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
    if perfil not in {"ADMIN", "COORDINADOR", "CONTROL"}:
        raise HTTPException(status.HTTP_403_FORBIDDEN, detail="Solo ADMIN, COORDINADOR o CONTROL pueden autorizar pedidos")

    # 2) Sanitizar input
    consecutivos = [c.strip() for c in (consecutivos or []) if c and c.strip()]
    if not consecutivos:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Debes indicar al menos un consecutivo_vehiculo")

    ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    autorizados_ok, rechazados = [], []

    # 3) Procesar cada vehículo
    for cv in sorted(set(consecutivos)):
        # a) Verificar que exista el consecutivo (en cualquier estado)
        docs_any = list(coleccion_pedidos.find(
            {"consecutivo_vehiculo": cv},
            projection={"regional": 1, "estado": 1}
        ))
        if not docs_any:
            rechazados.append({"consecutivo_vehiculo": cv, "motivo": "Consecutivo no encontrado"})
            continue

        # b) Validar permiso por regional (COORDINADOR/CONTROL/ADMIN aceptados; otros perfíles serían rechazados arriba)
        regional_doc = (docs_any[0].get("regional") or "").upper()
        if not usuario_puede_operar_en_regional(user, regional_doc):
            rechazados.append({"consecutivo_vehiculo": cv, "motivo": f"Sin permiso en regional {regional_doc}"})
            continue

        # c) Traer sólo los docs que están en estados que requieren autorización
        docs = [d for d in docs_any if (d.get("estado") or "").upper() in {
            "REQUIERE AUTORIZACION COORDINADOR", "REQUIERE AUTORIZACION CONTROL"
        }]

        if not docs:
            rechazados.append({"consecutivo_vehiculo": cv, "motivo": "No está en estado de REQUIERE AUTORIZACION"})
            continue

        # d) Determinar el requerimiento máximo entre documentos (si hubiese mezcla)
        estados = {(d.get("estado") or "").upper() for d in docs}
        estado_requerido = (
            "REQUIERE AUTORIZACION CONTROL"
            if any("CONTROL" in e for e in estados)
            else "REQUIERE AUTORIZACION COORDINADOR"
        )

        # e) Validar perfil con el estado requerido
        if not perfil_puede_autorizar(perfil, estado_requerido):
            rechazados.append({
                "consecutivo_vehiculo": cv,
                "motivo": f"Requiere perfil acorde a '{estado_requerido}'. Perfil actual: {perfil}"
            })
            continue

        # f) Autorizar todos los documentos del vehículo en cualquiera de los dos estados de 'requiere'
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
                "estado": {"$in": ["REQUIERE AUTORIZACION COORDINADOR", "REQUIERE AUTORIZACION CONTROL"]}
            },
            {"$set": set_fields}
        )

        if res.matched_count == 0:
            rechazados.append({"consecutivo_vehiculo": cv, "motivo": "No hubo documentos para autorizar"})
        else:
            autorizados_ok.append({
                "consecutivo_vehiculo": cv,
                "docs_autorizados": res.modified_count,
                "estado_requerido": estado_requerido
            })

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
        if not usuario_puede_operar_en_regional(user, regional_doc):
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
    if perfil in {"COORDINADOR", "CONTROL"}:
        raise HTTPException(403, "Los usuarios con perfil CONTROL O COORDINADOR no pueden eliminar pedidos.")

    # Buscar al menos un pedido que coincida
    pedido = coleccion_pedidos.find_one({
        "consecutivo_vehiculo": consecutivo_vehiculo
    })

    if not pedido:
        raise HTTPException(404, f"No se encontró ningún pedido con consecutivo_vehiculo '{consecutivo_vehiculo}'")

    estados_eliminables = {
        "PREAUTORIZADO",
        "AUTORIZADO",
        "REQUIERE AUTORIZACION COORDINADOR",
        "REQUIERE AUTORIZACION CONTROL",
        "COMPLETADO"
    }
    if pedido["estado"] not in estados_eliminables:
        raise HTTPException(400, "Solo se pueden eliminar pedidos en estado AUTORIZADO o REQUIERE AUTORIZACION (Coord./CONTROL) o COMPLETADO")


    # Eliminar todos los pedidos con ese consecutivo y estado válido
    res = coleccion_pedidos.delete_many({
        "consecutivo_vehiculo": consecutivo_vehiculo,
        "estado": {"$in": ["PREAUTORIZADO","AUTORIZADO", "REQUIERE AUTORIZACION COORDINADOR",  "REQUIERE AUTORIZACION CONTROL","COMPLETADO"]}
    })

    return {"mensaje": f"Se elimino el vehiculo '{consecutivo_vehiculo}'"}


# ------------------------------
# ✅ Exportar pedidos AUTORIZADOS a Excel (ordenado por consecutivo_vehiculo)
# ------------------------------
# ------------------------------
# ✅ Exportar pedidos AUTORIZADOS a Excel (ordenado por consecutivo_vehiculo)
# ------------------------------
@ruta_pedidos.get("/exportar-autorizados", summary="Exportar pedidos AUTORIZADOS a Excel")
async def exportar_autorizados():
    # 1) Traer AUTORIZADOS ordenados asc por consecutivo_vehiculo (y CI para estabilidad)
    cursor = coleccion_pedidos.find({"estado": "AUTORIZADO"}).sort([
        ("consecutivo_vehiculo", 1),
        ("consecutivo_integrapp", 1)
    ])
    docs = list(cursor)
    if not docs:
        raise HTTPException(404, "No hay pedidos AUTORIZADOS para exportar")

    # Caches para evitar consultas repetidas
    cliente_cache = {}
    tarifa_cache = {}

    def get_cliente(nit: str) -> dict:
        if nit not in cliente_cache:
            cliente_cache[nit] = coleccion_clientes.find_one({"nit": nit}) or {}
        return cliente_cache[nit]

    def get_tarifa(origen: str, destino: str) -> dict:
        key = (origen, destino)
        if key not in tarifa_cache:
            tarifa_cache[key] = coleccion_fletes.find_one({"origen": origen, "destino": destino}) or {}
        return tarifa_cache[key]

    # --- Agrupar por consecutivo_integrapp (para concatenar guías/planillas) ---
    docs_por_ci = defaultdict(list)
    for d in docs:
        docs_por_ci[d["consecutivo_integrapp"]].append(d)

    # --- Agrupar por consecutivo_vehiculo (totales vehiculares) ---
    docs_por_veh = defaultdict(list)
    for d in docs:
        docs_por_veh[d["consecutivo_vehiculo"]].append(d)

    # === Totales/flags por vehículo (respetando overrides si existen) ===
    flete_solicitado_por_veh = {}
    cargue_por_veh = {}
    descargue_kabi_por_veh = {}
    desvio_por_veh = {}
    punto_adic_por_veh = {}        # valor monetario (suma u override)
    kilos_sic_por_veh = {}
    seguro_por_veh = {}            # ← 6.000 si hay NIT 900402080 en el vehículo; si no, suma de 'seguro'
    adicionales_cnt_por_veh = {}   # ← NUEVO: cantidad de puntos adicionales (= total_puntos_vehiculo - 1, mínimo 0)

    NIT_FRESENIUS = "900402080"

    for veh, lst in docs_por_veh.items():
        doc0 = lst[0]

        # Flete solicitado vehicular: override si existe; si no suma de docs
        flete_override = doc0.get("total_flete_solicitado")
        if flete_override is None:
            flete_override = sum(float(x.get("valor_flete", 0) or 0) for x in lst)
        flete_solicitado_por_veh[veh] = float(flete_override or 0)

        # Cargue/descargue vehicular: override si existe; si no suma de docs
        carg_override = doc0.get("total_cargue_descargue")
        if carg_override is None:
            carg_override = sum(float(x.get("cargue_descargue", 0) or 0) for x in lst)
        cargue_por_veh[veh] = float(carg_override or 0)

        # Descargue KABI por vehículo (suma de docs)
        descargue_kabi_por_veh[veh] = sum(float(x.get("descargue_kabi", 0) or 0) for x in lst)

        # Desvío vehicular: override si existe; si no suma de docs
        desv_override = doc0.get("total_desvio_vehiculo")
        if desv_override is None:
            desv_override = sum(float(x.get("desvio", 0) or 0) for x in lst)
        desvio_por_veh[veh] = float(desv_override or 0)

        # Punto adicional vehicular (VALOR): override si existe; si no suma de docs
        pto_override = doc0.get("total_punto_adicional")
        if pto_override is None:
            pto_override = sum(float(x.get("punto_adicional", 0) or 0) for x in lst)
        punto_adic_por_veh[veh] = float(pto_override or 0)

        # Kilos SICETAC del vehículo (override si existe; si no suma de docs)
        total_kilos_sic = doc0.get("total_kilos_vehiculo_sicetac")
        if total_kilos_sic is None:
            total_kilos_sic = sum(float(x.get("num_kilos_sicetac", 0) or 0) for x in lst)
        kilos_sic_por_veh[veh] = float(total_kilos_sic or 0)

        # Cantidad de puntos adicionales = total_puntos_vehiculo - 1 (mínimo 0)
        total_puntos_veh = int(doc0.get("total_puntos_vehiculo", 0) or 0)
        adicionales_cnt_por_veh[veh] = max(total_puntos_veh - 1, 0)

        # ¿Este vehículo es de Fresenius Kabi? -> por NIT exacto 900402080
        es_fresenius = any((x.get("nit_cliente") or "").strip() == NIT_FRESENIUS for x in lst)

        # Seguro por vehículo:
        # - Si es Fresenius -> 6.000 obligatorio
        # - Si no, suma de 'seguro' de los docs
        if es_fresenius:
            seguro_por_veh[veh] = 6000.0
        else:
            seguro_por_veh[veh] = sum(float(x.get("seguro", 0) or 0) for x in lst)

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

        # --- Primera fila de CI: solo para concatenar "Pedido cliente" ---
        es_primera_ci = ci not in vistos_ci
        if es_primera_ci:
            pedido_cliente_concat = docs_concat_por_ci.get(ci, "")
            vistos_ci.add(ci)
        else:
            pedido_cliente_concat = ""

        # --- Primera fila del vehículo: escribir Toneladas, Seguro y valores vehiculares ---
        es_primera_veh = veh not in vistos_veh
        if es_primera_veh:
            kilos_veh = float(kilos_sic_por_veh.get(veh, 0.0) or 0.0)
            toneladas_val = round(kilos_veh / 1000.0, 3)

            # ====== PUNTO ADICIONAL ======
            # Monetario ya consolidado (override o suma)
            punto_adic_monetario = float(punto_adic_por_veh.get(veh, 0.0) or 0.0)
            # Cantidad de puntos adicionales
            puntos_adic_cnt = int(adicionales_cnt_por_veh.get(veh, 0) or 0)
            # Piso mínimo: 70.000 por cada punto adicional
            piso_min_por_puntos = 70000.0 * puntos_adic_cnt
            # Tomar el mayor entre lo monetario existente y el piso mínimo
            punto_adicional_val = max(punto_adic_monetario, piso_min_por_puntos)

            desvio_val            = float(desvio_por_veh.get(veh, 0.0) or 0.0)
            cargue_solicitado_val = float(cargue_por_veh.get(veh, 0.0) or 0.0)
            descargue_kabi_val    = float(descargue_kabi_por_veh.get(veh, 0.0) or 0.0)

            # Mayor entre descargue_kabi y cargue_solicitado
            mayor_cargue_per_juridica = max(descargue_kabi_val, cargue_solicitado_val)

            flete_base_veh = float(flete_solicitado_por_veh.get(veh, 0.0) or 0.0)
            # Fórmula de flete unidad (manteniendo cargue_solicitado_val aquí)
            flete_unidad_val = flete_base_veh + desvio_val + punto_adic_monetario + cargue_solicitado_val

            # Seguro según regla de Fresenius (6.000) o suma normal
            seguro_val = float(seguro_por_veh.get(veh, 0.0) or 0.0)

            vistos_veh.add(veh)
        else:
            toneladas_val = 0
            punto_adicional_val = 0
            desvio_val = 0
            cargue_solicitado_val = 0
            descargue_kabi_val = 0
            mayor_cargue_per_juridica = 0
            flete_unidad_val = 0
            seguro_val = 0

        # "Valor unitario" calculado sobre el total vehicular
        valor_base_para_unitario = float(flete_solicitado_por_veh.get(veh, 0.0) or 0.0)
        valor_unitario = int(((((valor_base_para_unitario) / 0.7) + 49) // 50) * 50)

        # Datos auxiliares (cacheados)
        cliente_doc = get_cliente(d["nit_cliente"])
        flete_doc = get_tarifa(d["origen"], d["destino"])
        if not flete_doc:
            raise HTTPException(500, f"No se encontró tarifa para {d['origen']}→{d['destino']}")

        observacion = (
            f"DN {docs_concat_por_ci.get(ci,'')}"
            if d["nit_cliente"] == "900402080"
            else (d.get("observaciones") or "").upper()
        )

        rows.append({
            "Consecutivo":              ci,
            "Tipo de viaje":            flete_doc.get("tipo", ""),
            "Linea de negocio":         "MASIVO",
            "Estado":                   "PENDIENTE",
            "Observación":              observacion,
            "Cliente":                  d["nit_cliente"],
            "Origen":                   d["origen"].upper(),
            "Destino":                  d["destino"].upper(),

            # Concatenado en la primera fila del CI
            "Pedido cliente":           pedido_cliente_concat,

            "Guía":                     (d.get("planilla_siscore") or "").upper(),
            "CENTRO COSTO":             f"{flete_doc.get('equivalencia_centro_costo', '')} {d.get('tipo_viaje','')} "
                                        f"OPERACIONES CARGA {cliente_doc.get('equivalencia_centro_costo','') if cliente_doc else ''}",
            "Ubicación Cargue":         (d.get("ubicacion_cargue") or "").upper(),
            "Direccion cargue":         (d.get("direccion_cargue") or "").upper(),
            "Ubicación Descargue":      (d.get("ubicacion_descargue") or "").upper(),
            "Direccion Descargue":      (d.get("direccion_descargue") or "").upper(),
            "Producto":                 "VARIOS" if d["nit_cliente"] not in {"901689684", "900402080"} else
                                        "MEDICAMENTOS (CON EXCLUSION DE LOS PRODUCTOS DE LAS PARTIDAS 3002;  30",
            "Naturaleza":               "NORMAL",
            "Tipo de vehiculo":         mapear_tipo_vehiculo((d.get("tipo_vehiculo_sicetac") or d.get("tipo_vehiculo") or "")),

            "unidad":                   "VEHICULOS",
            "Cantidad":                 1,
            "Tipo embalaje":            "PAQUETES",

            # === Vehicular, solo en 1ª fila del vehículo ===
            "Toneladas":                toneladas_val,
            "Flete unidad":             flete_unidad_val,
            "PUNTO ADICIONAL":          punto_adicional_val,
            "CARGUE-DESCARGUE PER JURIDICA": mayor_cargue_per_juridica,
            "SEGURO":                   seguro_val,  # ← 6.000 si hay NIT 900402080 en el vehículo
            "Tipo pago":                "CUPO",
            "Tolerancia":               0,
            "Vlr hora STBY":            0,
            "Vlr Declar Mercancia":     d.get("valor_declarado", 0),
            "Aprobar Poliza":           1,
            "Flete por":                "CUPO",

            # Unitario calculado sobre el total vehicular
            "Valor unitario":           valor_unitario,

            "Aprobar cupo credito":     1,
            "Aprobar rentabilidad":     1,
            "Otras caracteristicas":    "FURGON",
            "REMESAS":                  1,
            "REMISION DEL CLIENTE":     1,
            "GUIA DE TRANSPORTE":       1,
            "MANIFIESTO":               1,
        })

    # 2) DataFrame → Excel en memoria
    df = pd.DataFrame(rows)
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='plantilla')
    output.seek(0)

    # 3) Respuesta de descarga
    # Hora local de Bogotá (24h), sin caracteres problemáticos
    ahora_co = datetime.now(ZoneInfo("America/Bogota"))
    filename = f"pedidos_autorizados_{ahora_co:%Y%m%d_%H%M%S}.xlsx"

    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
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

    # Si es ADMIN/COORDINADOR/CONTROL/Analista y envió regionales, úsalas; de lo contrario, su regional por cookie
    if perfil in {"ADMIN", "COORDINADOR", "CONTROL", "ANALISTA"}:
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
    perfil = (user.get("perfil") or "").upper()

    # 🔑 Regionales visibles según perfil (usa el helper global)
    #   - ADMIN/COORDINADOR/CONTROL/ANALISTA -> None (sin restricción)
    #   - DESPACHADOR/OPERADOR de CELTA o FUNZA -> ["CELTA","FUNZA"]
    #   - Otros -> [su propia regional]
    visibles = regionales_visibles_para(user)

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

    # 4) Filtrar por regional según perfil/visibilidad
    if perfil in {"ADMIN", "COORDINADOR", "CONTROL", "ANALISTA"}:
        # Perfiles amplios: si envían 'regionales' en filtros, se respetan; si no, sin restricción
        if filtros.regionales:
            filtro["regional"] = {"$in": [r.upper().strip() for r in filtros.regionales]}
    else:
        # Otros perfiles (incluye DESPACHADOR/OPERADOR): limitar a 'visibles'
        if visibles is not None:
            filtro["regional"] = {"$in": visibles}

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
            "flete_solicitado": {"$sum": "$valor_flete"},
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
# 🗂 Fusionar vehículos 
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
            "REQUIERE AUTORIZACION CONTROL",
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
                    f"{cv}: solo se pueden fusionar vehículos en PREAUTORIZADO o REQUIERE AUTORIZACION (Coord./CONTROL)"
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
        if not usuario_puede_operar_en_regional(user, regional_doc):
            raise HTTPException(status.HTTP_403_FORBIDDEN, f"Sin permiso en regional {regional_doc}")


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
        val_pto_cfg = float(otros.get("valor_punto_adicional", 0) or 0)
        cargue_cfg  = float(otros.get("cargue_descargue", 0) or 0)

        # ✅ Puntos = cantidad de DESTINO_REAL únicos (normalizados)
        import unicodedata, re
        YES = {"SI", "S", "1", "TRUE", "VERDADERO", "YES", "Y"}

        def _norm_city(s: str) -> str:
            s = unicodedata.normalize("NFKD", (s or "").strip())
            s = "".join(ch for ch in s if not unicodedata.combining(ch))
            s = re.sub(r"\s+", " ", s).upper()
            return s

        paga_cd = str(tf.get("pago_cargue_desc", "")).strip().upper() in YES
        destinos_unicos = len({
            _norm_city(d.get("destino_real"))
            for d in docs
            if _norm_city(d.get("destino_real")) != ""
        })

        # El total de puntos del vehículo = número de destinos únicos (mínimo 1)
        total_puntos_calc = max(1, destinos_unicos)
        adicionales       = max(0, total_puntos_calc - 1)
        pto_teorico       = adicionales * val_pto_cfg
        cargue_teorico    = cargue_cfg if paga_cd else 0.0
        costo_teorico     = tbase + pto_teorico + cargue_teorico

        # 8) Overrides solicitados
        total_flete_solicitado = float(payload.total_flete_solicitado or 0)
        total_cargue_descargue = float(payload.total_cargue_descargue or 0)
        total_punto_adicional  = float(payload.total_punto_adicional or 0)
        total_desvio_vehiculo  = float(payload.total_desvio_vehiculo or 0)

        costo_real = total_flete_solicitado + total_cargue_descargue + total_desvio_vehiculo + total_punto_adicional

        estado_calc, porc = estado_por_autorizacion(costo_real, costo_teorico)
        ahora_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 9) Update masivo (incluye CI del primer carro)
        set_fields = {
            "consecutivo_vehiculo":             target_cv,
            "destino":                          nuevo_destino,
            "tipo_vehiculo_sicetac":            tipo_sic,

            # Totales vehiculares
            "total_cajas_vehiculo":             total_cajas,
            "total_kilos_vehiculo":             total_kilos,
            "total_kilos_vehiculo_sicetac":     total_kilos_sic,
            "total_puntos_vehiculo":            total_puntos_calc,

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

        # --- Unificar consecutivo_integrapp al del primer carro ---
        from collections import Counter
        docs_primer_carro = [d for d in docs if (d.get("consecutivo_vehiculo") or "").strip() == target_cv]
        ci_candidatos = [
            (d.get("consecutivo_integrapp") or "").strip()
            for d in docs_primer_carro
            if (d.get("consecutivo_integrapp") or "").strip()
        ]
        if ci_candidatos:
            ci_a_conservar = Counter(ci_candidatos).most_common(1)[0][0]
            set_fields["consecutivo_integrapp"] = ci_a_conservar  # ← todos quedarán con este CI

        res = coleccion_pedidos.update_many(
            {"consecutivo_vehiculo": {"$in": consecutivos}},
            {"$set": set_fields}
        )

        print("[fusionar_vehiculos] OK",
              {"consecutivos": consecutivos, "target": target_cv, "docs_actualizados": res.modified_count})

        return {
            "mensaje": f"Fusionados {len(consecutivos)} consecutivos en '{target_cv}'",
            "consecutivo_resultante": target_cv,
            "consecutivo_integrapp_conservado": set_fields.get("consecutivo_integrapp"),
            "docs_actualizados": res.modified_count,
            "totales": {
                "total_cajas_vehiculo":         total_cajas,
                "total_kilos_vehiculo":         total_kilos,
                "total_kilos_vehiculo_sicetac": total_kilos_sic,
                "total_puntos_vehiculo":        total_puntos_calc,
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
        raise
    except Exception as e:
        import traceback
        print("[fusionar_vehiculos][ERROR]", traceback.format_exc())
        raise HTTPException(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error interno en fusionar_vehiculos: {e}"
        )

# ------------------------------
# 🗂 Dividir vehículos (sobrescribe C.I./C.P. y permite split por kilos RUNT)
# ------------------------------
@ruta_pedidos.post(
    "/dividir-vehiculo",
    response_model=dict,
    summary=("Divide un consecutivo_vehiculo en hasta 3 (A conserva; B y C se crean con sufijos). "
             "Puedes seleccionar por destinatario, consecutivo_integrapp o ubicacion_descargue, "
             "y también partir un único documento por KILOS (RUNT) hacia B y/o C.")
)
async def dividir_vehiculo(payload: DividirHastaTresPayload):
    import re, unicodedata
    from collections import defaultdict
    from copy import deepcopy
    from bson import ObjectId

    usuario = (payload.usuario or "").upper().strip()
    user = coleccion_usuarios.find_one({"usuario": usuario})
    if not user:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "Usuario no encontrado")

    perfil = (user.get("perfil") or "").upper()
    if perfil not in {"ADMIN", "DESPACHADOR", "OPERADOR"}:
        raise HTTPException(status.HTTP_403_FORBIDDEN, "No tienes permisos para dividir vehículos")

    cv_origen = (payload.consecutivo_origen or "").strip()
    if not cv_origen:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "consecutivo_origen requerido")

    destino_unico = (payload.destino_unico or "").upper().strip()
    if not destino_unico:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "destino_unico requerido")

    # 1) Traer docs del vehículo origen
    docs_origen = list(coleccion_pedidos.find({"consecutivo_vehiculo": cv_origen}))
    if not docs_origen:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"No hay documentos para {cv_origen}")

    # 2) Estados permitidos
    estados_permitidos = {
        "PREAUTORIZADO",
        "REQUIERE AUTORIZACION COORDINADOR",
        "REQUIERE AUTORIZACION CONTROL",
    }
    if any((d.get("estado") or "").upper() not in estados_permitidos for d in docs_origen):
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            "Solo se pueden dividir vehículos en PREAUTORIZADO o REQUIERE AUTORIZACION (Coord./CONTROL)"
        )

    # 3) Regional homogénea y permisos por regional
    regionales = {(d.get("regional") or "").upper() for d in docs_origen}
    if len(regionales) != 1:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "El vehículo origen debe tener regional homogénea")
    regional_doc = next(iter(regionales))
    if not usuario_puede_operar_en_regional(user, regional_doc):
        raise HTTPException(status.HTTP_403_FORBIDDEN, f"Sin permiso: vehículo de regional {regional_doc}")

    # 4) Origen homogéneo (para tarifario)
    origenes = {(d.get("origen") or "").upper() for d in docs_origen}
    if len(origenes) != 1:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "El vehículo origen debe tener ORIGEN homogéneo")
    origen_tarifa = next(iter(origenes))

    # --- Normalizador
    def _norm(s: str) -> str:
        s = unicodedata.normalize("NFKD", s or "")
        s = "".join(ch for ch in s if not unicodedata.combining(ch))
        s = re.sub(r"\s+", " ", s).strip().upper()
        return s

    # 5) Armado de grupos por destinatario/campo dinámico o por consecutivo_integrapp
    campo_dest = (payload.campo_destinatario or "destinatario").strip()

    docs_por_ci = defaultdict(list)
    for d in docs_origen:
        docs_por_ci[d.get("consecutivo_integrapp")] = docs_por_ci.get(d.get("consecutivo_integrapp"), []) + [d]

    def _leer_campo_dest(d: dict) -> str:
        raw = d.get(campo_dest, None)
        if raw is None and isinstance(campo_dest, str):
            raw = d.get(campo_dest.lower(), d.get(campo_dest.upper(), ""))
        return _norm(str(raw))

    def filtrar_docs(grupo) -> list:
        if not grupo:
            return []
        resultado = []

        # 5.1 Por destinatarios (campo dinámico, ej: ubicacion_descargue)
        if getattr(grupo, "destinatarios", None):
            valores = {_norm(v) for v in grupo.destinatarios if v and str(v).strip()}
            for d in docs_origen:
                if _leer_campo_dest(d) in valores:
                    resultado.append(d)

        # 5.2 Por consecutivos_integrapp (agrega TODOS los docs de cada CI)
        if getattr(grupo, "consecutivos_integrapp", None):
            for ci in grupo.consecutivos_integrapp:
                ci = (ci or "").strip()
                if ci and ci in docs_por_ci:
                    resultado.extend(docs_por_ci[ci])

        # quitar duplicados por _id
        seen = set()
        uniq = []
        for d in resultado:
            k = d.get("_id")
            if k not in seen:
                uniq.append(d)
                seen.add(k)
        return uniq

    docs_B = filtrar_docs(payload.grupo_B)
    docs_C = filtrar_docs(payload.grupo_C)

    # Si hay split, no queremos además mover ese mismo CI completo por filtro.
    if payload.grupo_B and payload.grupo_B.split:
        ci_b = (payload.grupo_B.split.consecutivo_integrapp or "").strip()
        docs_B = [d for d in docs_B if (d.get("consecutivo_integrapp") or "") != ci_b]
    if payload.grupo_C and payload.grupo_C.split:
        ci_c = (payload.grupo_C.split.consecutivo_integrapp or "").strip()
        docs_C = [d for d in docs_C if (d.get("consecutivo_integrapp") or "") != ci_c]

    # Sufijos y nuevos consecutivos vehiculares (SIN guion)
    quiere_B = bool(docs_B) or bool(getattr(getattr(payload, "grupo_B", None), "split", None))
    quiere_C = bool(docs_C) or bool(getattr(getattr(payload, "grupo_C", None), "split", None))

    cv_B = f"{cv_origen}B" if quiere_B else None
    cv_C = f"{cv_origen}C" if quiere_C else None

    ahora_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # ---------- Helpers de cálculo ----------
    YES = {"SI", "S", "1", "TRUE", "VERDADERO", "YES", "Y"}

    def _destinos_reales_unicos(docs: list) -> int:
        vals = {_norm(d.get("destino_real") or "") for d in docs}
        vals.discard("")
        return len(vals)

    # ⇩⇩⇩ AQUÍ el mapeo idéntico a la macro
    def tipo_por_kilos(kilos_sic: float) -> str:
        k = float(kilos_sic or 0)
        if k <= 2300:
            return "NHR"
        if k <= 4500:
            return "TURBO"
        if k <= 6100:
            return "NIES"
        if k <= 9000:
            return "SENCILLO"
        if k <= 17000:
            return "PATINETA"
        return "TRACTOMULA"

    def _calc(docs: list, overrides):
        # Totales del grupo
        total_cajas     = sum(int(d.get("num_cajas", 0) or 0) for d in docs)
        total_kilos     = sum(float(d.get("num_kilos", 0) or 0) for d in docs)
        total_kilos_sic = sum(float(d.get("num_kilos_sicetac", d.get("num_kilos", 0)) or 0) for d in docs)

        # Tipo por kilos (SICETAC) según macro
        tipo_sic = tipo_por_kilos(total_kilos_sic)

        # Tarifas/otros costos para el tipo calculado
        tf = db["tarifas"].find_one({"origen": origen_tarifa, "destino": destino_unico})
        if not tf or "tarifas" not in tf or tipo_sic not in tf["tarifas"]:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"No hay tarifa para {origen_tarifa}→{destino_unico} con tipo '{tipo_sic}'")
        tbase = float(tf["tarifas"][tipo_sic])

        otros = db["otros_costos"].find_one({"tipo_vehiculo": tipo_sic})
        if not otros:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"No hay configuración de 'otros_costos' para '{tipo_sic}'")
        val_pto_cfg = float(otros.get("valor_punto_adicional", 0) or 0)
        cargue_cfg  = float(otros.get("cargue_descargue", 0) or 0)

        paga_cd = str(tf.get("pago_cargue_desc", "")).strip().upper() in YES
        destinos_unicos = _destinos_reales_unicos(docs)
        puntos_excel = sum(int(d.get("total_puntos", 0) or 0) for d in docs)
        puntos_calc = max(destinos_unicos, puntos_excel)
        adicionales = max(0, puntos_calc - 1)
        pto_teorico = adicionales * val_pto_cfg
        cargue_teorico = cargue_cfg if paga_cd else 0.0

        sum_flete = sum(float(d.get("valor_flete", 0) or 0) for d in docs)
        sum_cargue = sum(float(d.get("cargue_descargue", 0) or 0) for d in docs)
        sum_desvio = sum(float(d.get("desvio", 0) or 0) for d in docs)
        sum_punto  = sum(float(d.get("punto_adicional", 0) or 0) for d in docs)

        tflete = overrides.total_flete_solicitado if (overrides and overrides.total_flete_solicitado is not None) else sum_flete
        tcarg  = overrides.total_cargue_descargue if (overrides and overrides.total_cargue_descargue is not None) else sum_cargue
        tdesv  = overrides.total_desvio_vehiculo  if (overrides and overrides.total_desvio_vehiculo  is not None) else sum_desvio
        tpad   = overrides.total_punto_adicional  if (overrides and overrides.total_punto_adicional  is not None) else sum_punto

        costo_teorico = tbase + pto_teorico + cargue_teorico
        costo_real = float(tflete) + float(tcarg) + float(tdesv) + float(tpad)
        estado_calc, porc = estado_por_autorizacion(costo_real, costo_teorico)

        return {
            "tipo_sic": tipo_sic,
            "tbase": tbase,
            "pto_teo": pto_teorico,
            "carg_teo": cargue_teorico,
            "costo_teo": costo_teorico,
            "cajas": total_cajas,
            "kilos": total_kilos,
            "kilos_sic": total_kilos_sic,
            "puntos": puntos_calc,
            "tflete": float(tflete),
            "tcarg": float(tcarg),
            "tdesv": float(tdesv),
            "tpad": float(tpad),
            "creal": float(costo_real),
            "estado": estado_calc,
            "porc": porc
        }

    def _apply(cv: str, calc: dict):
        coleccion_pedidos.update_many(
            {"consecutivo_vehiculo": cv},
            {"$set": {
                "destino":                         destino_unico,
                "tipo_vehiculo_sicetac":           calc["tipo_sic"],
                "total_cajas_vehiculo":            calc["cajas"],
                "total_kilos_vehiculo":            calc["kilos"],
                "total_kilos_vehiculo_sicetac":    calc["kilos_sic"],
                "total_puntos_vehiculo":           calc["puntos"],
                "total_puntos":                    calc["puntos"],
                "valor_flete_sistema":             calc["tbase"],
                "punto_adicional_teorico":         calc["pto_teo"],
                "cargue_descargue_teorico":        calc["carg_teo"],
                "costo_teorico_vehiculo":          calc["costo_teo"],
                "total_flete_solicitado":          calc["tflete"],
                "total_cargue_descargue":          calc["tcarg"],
                "total_desvio_vehiculo":           calc["tdesv"],
                "total_punto_adicional":           calc["tpad"],
                "total_flete_vehiculo":            calc["creal"],
                "diferencia_flete":                calc["creal"] - calc["costo_teo"],
                "estado":                          calc["estado"],
                "porcentaje_sobre_teorico":        calc["porc"],
                "autorizado_por":                  "SISTEMA" if calc["estado"] == "PREAUTORIZADO" else "NA",
                "fecha_autorizacion":              ahora_str if calc["estado"] == "PREAUTORIZADO" else "NA",
                "usuario_division":                usuario,
                "observacion_division":            (payload.observacion_division or ""),
                "fecha_division":                  ahora_str,
            }}
        )

    # 6) Movimientos por filtro (si los hay) – actualiza CV y sufijos de CI/CP
    def _sobrescribir_campos_doc(doc: dict, sufijo: str) -> dict:
        ci_orig = str(doc.get("consecutivo_integrapp") or "")
        cp_orig = str(doc.get("consecutivo_pedido") or "")
        return {
            "consecutivo_integrapp": f"{ci_orig}{sufijo}",
            "consecutivo_pedido":    f"{cp_orig}{sufijo}",
        }

    if docs_B:
        coleccion_pedidos.update_many(
            {"consecutivo_vehiculo": cv_origen, "_id": {"$in": [d["_id"] for d in docs_B]}},
            {"$set": {
                "consecutivo_vehiculo": cv_B,
                "destino": destino_unico,
                "usuario_division": usuario,
                "observacion_division": (payload.observacion_division or ""),
                "fecha_division": ahora_str,
            }}
        )
        for d in docs_B:
            coleccion_pedidos.update_one({"_id": d["_id"]}, {"$set": _sobrescribir_campos_doc(d, "B")})

    if docs_C:
        coleccion_pedidos.update_many(
            {"consecutivo_vehiculo": cv_origen, "_id": {"$in": [d["_id"] for d in docs_C]}},
            {"$set": {
                "consecutivo_vehiculo": cv_C,
                "destino": destino_unico,
                "usuario_division": usuario,
                "observacion_division": (payload.observacion_division or ""),
                "fecha_division": ahora_str,
            }}
        )
        for d in docs_C:
            coleccion_pedidos.update_one({"_id": d["_id"]}, {"$set": _sobrescribir_campos_doc(d, "C")})

    # 7) SPLIT por KILOS (RUNT) hacia B/C – requiere doc_id si el CI no es único en A
    def _split_por_kilos(
        ci_objetivo: str,
        kilos_a_mover: float,
        sufijo: str,
        cv_destino: str,
        cajas_explicit: Optional[int] = None,
        doc_id: Optional[str] = None
    ):
        ci_objetivo = (ci_objetivo or "").strip()
        if not ci_objetivo:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"split.{sufijo}: consecutivo_integrapp requerido")
        if not cv_destino:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"split.{sufijo}: no hay consecutivo destino")

        # Buscar el doc en A. Si hay muchos CI iguales, exige doc_id
        query_base = {"consecutivo_vehiculo": cv_origen, "consecutivo_integrapp": ci_objetivo}
        candidatos = list(coleccion_pedidos.find(query_base, projection={"_id": 1}))
        if len(candidatos) == 0:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"split.{sufijo}: no se encontró el C.I. '{ci_objetivo}' en A")
        if len(candidatos) > 1:
            if not doc_id:
                raise HTTPException(
                    status.HTTP_400_BAD_REQUEST,
                    f"split.{sufijo}: el consecutivo_integrapp '{ci_objetivo}' no es único en A (hay {len(candidatos)}). Envía doc_id."
                )
            try:
                oid = ObjectId(str(doc_id))
            except Exception:
                raise HTTPException(status.HTTP_400_BAD_REQUEST, f"split.{sufijo}: doc_id inválido")
            query_base["_id"] = oid

        doc_src = coleccion_pedidos.find_one(query_base)
        if not doc_src:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"split.{sufijo}: no se encontró el documento indicado en A")

        # Base del corte = kilos RUNT (SICETAC). Si no existe, cae a kilos físicos
        kilos_runt_total = float(doc_src.get("num_kilos_sicetac") or doc_src.get("num_kilos") or 0.0)
        kilos_fis_total  = float(doc_src.get("num_kilos") or doc_src.get("num_kilos_sicetac") or 0.0)
        cajas_total      = int(doc_src.get("num_cajas", 0) or 0)
        flete_total      = float(doc_src.get("valor_flete", 0) or 0.0)

        if kilos_a_mover is None:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"split.{sufijo}: kilos requerido")
        kilos_a_mover = float(kilos_a_mover)
        if kilos_a_mover <= 0:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"split.{sufijo}: kilos debe ser > 0")

        EPS = 1e-6
        if kilos_a_mover >= kilos_runt_total - EPS:
            raise HTTPException(
                status.HTTP_400_BAD_REQUEST,
                f"split.{sufijo}: kilos ({kilos_a_mover}) exceden o igualan el total del doc RUNT ({kilos_runt_total})"
            )

        # Proporción del corte usando la base RUNT
        p = kilos_a_mover / kilos_runt_total

        # Destino (lo que va a B/C)
        kilos_runt_dest = round(kilos_a_mover, 2)
        kilos_fis_dest  = round(kilos_fis_total * p, 2)
        flete_dest      = round(flete_total * p, 2)

        # Cajas
        if cajas_explicit is not None:
            cajas_dest = max(0, int(cajas_explicit))
        else:
            cajas_dest = int(round(cajas_total * p)) if cajas_total else 0
            if cajas_total and cajas_dest == 0:
                cajas_dest = 1

        # Remanente en A
        kilos_runt_rem = round(kilos_runt_total - kilos_runt_dest, 2)
        kilos_fis_rem  = round(kilos_fis_total - kilos_fis_dest, 2)
        cajas_rem      = max(0, cajas_total - cajas_dest)
        flete_rem      = round(flete_total - flete_dest, 2)
        if kilos_runt_rem <= EPS:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"split.{sufijo}: el remanente quedaría en 0 kg RUNT")

        # Clonar hacia el destino
        doc_new = deepcopy(doc_src)
        doc_new.pop("_id", None)
        doc_new["consecutivo_vehiculo"] = cv_destino
        doc_new["consecutivo_integrapp"] = f"{doc_src['consecutivo_integrapp']}{sufijo}"
        doc_new["consecutivo_pedido"]    = f"{str(doc_src.get('consecutivo_pedido',''))}{sufijo}"
        doc_new["num_cajas"]             = cajas_dest
        doc_new["num_kilos_sicetac"]     = kilos_runt_dest
        doc_new["num_kilos"]             = kilos_fis_dest
        doc_new["valor_flete"]           = flete_dest
        doc_new["destino"]               = destino_unico
        doc_new["usuario_division"]      = usuario
        doc_new["observacion_division"]  = (payload.observacion_division or "")
        doc_new["fecha_division"]        = ahora_str

        # Actualizar el doc origen (A)
        coleccion_pedidos.update_one(
            {"_id": doc_src["_id"]},
            {"$set": {
                "num_cajas":         cajas_rem,
                "num_kilos_sicetac": kilos_runt_rem,
                "num_kilos":         kilos_fis_rem,
                "valor_flete":       flete_rem
            }}
        )
        coleccion_pedidos.insert_one(doc_new)

    # Ejecutar splits si vienen (doc_id es opcional pero requerido si el CI no es único)
    if getattr(getattr(payload, "grupo_B", None), "split", None):
        _split_por_kilos(
            payload.grupo_B.split.consecutivo_integrapp,
            float(payload.grupo_B.split.kilos),
            "B",
            cv_B,
            getattr(payload.grupo_B.split, "cajas", None),
            getattr(payload.grupo_B.split, "doc_id", None)
        )

    if getattr(getattr(payload, "grupo_C", None), "split", None):
        _split_por_kilos(
            payload.grupo_C.split.consecutivo_integrapp,
            float(payload.grupo_C.split.kilos),
            "C",
            cv_C,
            getattr(payload.grupo_C.split, "cajas", None),
            getattr(payload.grupo_C.split, "doc_id", None)
        )

    # 8) Refrescar grupos A/B/C tras movimientos/splits
    docs_A  = list(coleccion_pedidos.find({"consecutivo_vehiculo": cv_origen}))
    docs_B2 = list(coleccion_pedidos.find({"consecutivo_vehiculo": cv_B})) if cv_B else []
    docs_C2 = list(coleccion_pedidos.find({"consecutivo_vehiculo": cv_C})) if cv_C else []

    if not docs_A:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "El grupo A no puede quedar vacío (A conserva el consecutivo original)")
    if cv_C and not cv_B:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "No puedes crear C sin B")
    if not docs_B2 and not docs_C2:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, "No hay nada para dividir (ni filtros ni split)")

    # 9) Recalcular por carro (tipo_vehiculo_sicetac calculado por kilos del grupo)
    if docs_A:
        calc_A = _calc(docs_A, payload.grupo_A.overrides if payload.grupo_A else None)
        _apply(cv_origen, calc_A)

    if docs_B2:
        calc_B = _calc(docs_B2, payload.grupo_B.overrides if payload.grupo_B else None)
        _apply(cv_B, calc_B)

    if docs_C2:
        calc_C = _calc(docs_C2, payload.grupo_C.overrides if payload.grupo_C else None)
        _apply(cv_C, calc_C)

    resumen = {
        "A": {"vehiculo": cv_origen, "docs": len(docs_A)},
        "B": {"vehiculo": cv_B, "docs": len(docs_B2)} if docs_B2 else None,
        "C": {"vehiculo": cv_C, "docs": len(docs_C2)} if docs_C2 else None,
        "destino_unico": destino_unico
    }
    return {"mensaje": "División realizada", "resumen": resumen}
