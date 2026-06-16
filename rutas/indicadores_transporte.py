# archivo: rutas/indicadores_transporte.py

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
import psycopg2
import os

router = APIRouter(
    prefix="/indicadores-transporte",
    tags=["Indicadores Transporte"],
)

def get_pg_connection():
    return psycopg2.connect(
        host=os.environ.get("PG_HOST"),
        port=int(os.environ.get("PG_PORT", "5432")),
        database=os.environ.get("PG_DATABASE"),
        user=os.environ.get("PG_USUARIO"),
        password=os.environ.get("PG_CLAVE"),
        sslmode="require",
    )

def format_date(val):
    if val and hasattr(val, 'strftime'):
        return val.strftime('%Y-%m-%d')
    elif val:
        return str(val).split('T')[0]
    return ''

@router.get("/guias")
def get_guias_indicadores(
    fecha_inicio: Optional[str] = Query(None),
    fecha_fin: Optional[str] = Query(None),
    estado: Optional[str] = Query(None),
    cliente: Optional[List[str]] = Query(None),
    anio: Optional[List[int]] = Query(None),
    mes: Optional[List[int]] = Query(None),
):
    try:
        conn = get_pg_connection()
        cursor = conn.cursor()

        # Obtener todos los años disponibles para el selector (excluye fechas nulas)
        cursor.execute("SELECT DISTINCT EXTRACT(YEAR FROM fecha_emision)::int AS anio FROM informe_guias_tms WHERE fecha_emision IS NOT NULL ORDER BY anio DESC")
        anios_disponibles = [r[0] for r in cursor.fetchall() if r[0] is not None]

        # Construir cláusula WHERE común para todos los filtros
        where = " WHERE fecha_emision IS NOT NULL"
        params: list = []

        if fecha_inicio:
            where += " AND fecha_emision >= %s"
            params.append(fecha_inicio)
        if fecha_fin:
            where += " AND fecha_emision <= %s"
            params.append(fecha_fin)
        if anio:
            # Convertir años a rangos de fecha (sargable, permite usar índice de fecha)
            rangos = []
            for a in anio:
                rangos.append("(fecha_emision >= %s AND fecha_emision < %s)")
                params.append(f"{int(a)}-01-01")
                params.append(f"{int(a)+1}-01-01")
            where += " AND (" + " OR ".join(rangos) + ")"
        if mes:
            where += " AND EXTRACT(MONTH FROM fecha_emision) IN (" + ','.join(['%s'] * len(mes)) + ")"
            params.extend(mes)
        if estado:
            where += " AND estado = %s"
            params.append(estado)
        if cliente:
            where += " AND (" + " OR ".join(["nombre_cliente ILIKE %s" for _ in cliente]) + ")"
            params.extend([f"%{c}%" for c in cliente])

        # 1) KPIs (agregación total)
        cursor.execute(f"""
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE estado = 'ENTREGADO') AS entregados,
                COUNT(*) FILTER (WHERE estado = 'PENDIENTE') AS pendientes,
                COUNT(*) FILTER (WHERE estado = 'CON NOVEDAD') AS con_novedad,
                COALESCE(SUM(CASE WHEN piezas ~ '^[0-9]+(\\.[0-9]+)?$' THEN piezas::numeric ELSE 0 END), 0) AS total_piezas,
                COALESCE(SUM(CASE WHEN kilos ~ '^[0-9]+(\\.[0-9]+)?$' THEN kilos::numeric ELSE 0 END), 0) AS total_kilos
            FROM informe_guias_tms {where}
        """, params)
        kpi_row = cursor.fetchone()
        total_guias = kpi_row[0] or 0
        entregados = kpi_row[1] or 0
        pendientes = kpi_row[2] or 0
        con_novedad = kpi_row[3] or 0
        total_piezas = int(kpi_row[4] or 0)
        total_kilos = float(kpi_row[5] or 0)

        # 2) Conteo por estado
        cursor.execute(f"""
            SELECT estado, COUNT(*) AS cantidad
            FROM informe_guias_tms {where}
            GROUP BY estado
        """, params)
        conteo_por_estado = {r[0]: r[1] for r in cursor.fetchall() if r[0]}

        # 3) Datos para gráfico de pedidos (conteo por día y estado)
        cursor.execute(f"""
            SELECT DATE(fecha_emision) AS fecha, estado, COUNT(*) AS cantidad
            FROM informe_guias_tms {where}
            GROUP BY DATE(fecha_emision), estado
            ORDER BY fecha
        """, params)
        guias_por_dia = {}
        for fecha, est, cant in cursor.fetchall():
            clave = format_date(fecha)
            guias_por_dia.setdefault(clave, {})[est] = cant
        datos_grafico = [{"fecha": f, **d} for f, d in sorted(guias_por_dia.items())]

        # 4) Datos para gráfico de cajas (suma piezas por día y estado)
        cursor.execute(f"""
            SELECT DATE(fecha_emision) AS fecha, estado, COALESCE(SUM(CASE WHEN piezas ~ '^[0-9]+(\\.[0-9]+)?$' THEN piezas::numeric ELSE 0 END), 0) AS piezas
            FROM informe_guias_tms {where}
            GROUP BY DATE(fecha_emision), estado
            ORDER BY fecha
        """, params)
        piezas_por_dia = {}
        for fecha, est, pie in cursor.fetchall():
            clave = format_date(fecha)
            piezas_por_dia.setdefault(clave, {})[est] = int(pie or 0)
        datos_cajas = [{"fecha": f, **d} for f, d in sorted(piezas_por_dia.items())]

        # 5) Datos por cliente (conteo por cliente y estado)
        cursor.execute(f"""
            SELECT COALESCE(NULLIF(nombre_cliente, ''), 'Sin cliente') AS cliente, estado, COUNT(*) AS cantidad
            FROM informe_guias_tms {where}
            GROUP BY COALESCE(NULLIF(nombre_cliente, ''), 'Sin cliente'), estado
        """, params)
        cliente_estados = {}
        for cli, est, cant in cursor.fetchall():
            cliente_estados.setdefault(cli, {})[est] = cant
        datos_por_cliente = sorted(
            [{"cliente": c, "total": sum(v.values()), **v} for c, v in cliente_estados.items()],
            key=lambda x: x['total'], reverse=True,
        )

        # 6) Listas de estados y clientes
        cursor.execute(f"SELECT DISTINCT estado FROM informe_guias_tms {where} AND estado IS NOT NULL", params)
        estados_lista = sorted([r[0] for r in cursor.fetchall() if r[0]])
        cursor.execute(f"SELECT DISTINCT nombre_cliente FROM informe_guias_tms {where} AND nombre_cliente IS NOT NULL AND nombre_cliente <> ''", params)
        clientes_lista = sorted([r[0] for r in cursor.fetchall() if r[0]])

        cursor.close()
        conn.close()

        return {
            "success": True,
            "data": {
                "kpis": {
                    "totalGuias": total_guias,
                    "porcentajeEntregados": round((entregados / total_guias * 100), 1) if total_guias > 0 else 0,
                    "porcentajePendientes": round((pendientes / total_guias * 100), 1) if total_guias > 0 else 0,
                    "porcentajeConNovedad": round((con_novedad / total_guias * 100), 1) if total_guias > 0 else 0,
                    "conteo": {
                        "ENTREGADO": entregados,
                        "PENDIENTE": pendientes,
                        "CON NOVEDAD": con_novedad,
                        "otros": total_guias - entregados - pendientes - con_novedad,
                    },
                    "conteoPorEstado": conteo_por_estado,
                    "peso": {
                        "totalPiezas": total_piezas,
                        "totalToneladas": round(total_kilos / 1000, 2),
                    },
                },
                "datosGrafico": datos_grafico,
                "datosCajas": datos_cajas,
                "datosPorCliente": datos_por_cliente,
                "estados": estados_lista,
                "clientes": clientes_lista,
                "anios": anios_disponibles,
            },
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/guias/detalle")
def get_detalle_dia(
    fecha: str = Query(None),
    fecha_inicio: Optional[str] = Query(None),
    fecha_fin: Optional[str] = Query(None),
    estado: Optional[List[str]] = Query(None),
    cliente: Optional[List[str]] = Query(None),
):
    try:
        conn = get_pg_connection()
        cursor = conn.cursor()

        query = """
            SELECT
                guia, nombre_cliente, ciudad_destino, estado, novedad,
                DATE(fecha_emision) as fecha_emision,
                fecha_entrega, fecha_preferente, servicio, piezas, kilos, destinatario
            FROM informe_guias_tms
            WHERE fecha_emision IS NOT NULL
        """
        params: list = []

        # Si es un rango de fechas (para mes agrupado)
        if fecha_inicio and fecha_fin:
            query += " AND DATE(fecha_emision) >= %s AND DATE(fecha_emision) <= %s"
            params.extend([fecha_inicio, fecha_fin])
        elif fecha:
            # Si es una fecha individual
            query += " AND DATE(fecha_emision) = %s"
            params.append(fecha)

        if cliente:
            conditions = ["nombre_cliente ILIKE %s" for _ in cliente]
            params.extend([f"%{c}%" for c in cliente])
            query += f" AND ({' OR '.join(conditions)})"

        if estado:
            placeholders = ','.join(['%s'] * len(estado))
            query += f" AND estado IN ({placeholders})"
            params.extend(estado)

        query += " ORDER BY estado, nombre_cliente"

        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

        for row in rows:
            for key in ['fecha_emision', 'fecha_entrega', 'fecha_preferente']:
                row[key] = format_date(row.get(key))

        cursor.close()
        conn.close()

        return {"success": True, "data": rows, "total": len(rows)}
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
