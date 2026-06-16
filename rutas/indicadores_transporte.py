# archivo: rutas/indicadores_transporte.py

from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
import psycopg2
import os

router = APIRouter(
    prefix="/indicadores-transporte",
    tags=["Indicadores Transporte"],
)

# Clientes que se excluyen de los indicadores de transporte
CLIENTES_EXCLUIDOS = [
    "GESTION DE RECAUDO Y RENTAS",
    "FAMISANAR",
]

def build_exclusion_clause(column: str = "nombre_cliente") -> tuple:
    """Genera una cláusula SQL para excluir los clientes de CLIENTES_EXCLUIDOS.
    Compara en mayúsculas para no depender de cómo venga el texto.
    Retorna (sql_clause, params)."""
    if not CLIENTES_EXCLUIDOS:
        return ("", [])
    conditions = [f"UPPER({column}) NOT LIKE %s" for _ in CLIENTES_EXCLUIDOS]
    params = [f"%{c}%" for c in CLIENTES_EXCLUIDOS]
    return (" AND " + " AND ".join(conditions), params)

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
    fecha_inicio: str = Query(...),
    fecha_fin: str = Query(...),
    estado: Optional[str] = Query(None),
    cliente: Optional[List[str]] = Query(None),
):
    try:
        conn = get_pg_connection()
        cursor = conn.cursor()

        query = """
            SELECT DISTINCT
                guia, nombre_cliente, ciudad_destino, estado, novedad,
                DATE(fecha_emision) as fecha_emision,
                fecha_entrega, fecha_preferente, servicio, piezas, kilos, destinatario
            FROM informe_guias_tms
            WHERE 1=1
        """
        params: list = []

        # Excluir clientes no deseados
        excl_sql, excl_params = build_exclusion_clause()
        query += excl_sql
        params.extend(excl_params)

        if fecha_inicio:
            query += " AND fecha_emision >= %s"
            params.append(fecha_inicio)

        if fecha_fin:
            query += " AND fecha_emision <= %s"
            params.append(fecha_fin)

        if estado:
            query += " AND estado = %s"
            params.append(estado)

        if cliente:
            conditions = ["nombre_cliente ILIKE %s" for _ in cliente]
            params.extend([f"%{c}%" for c in cliente])
            query += f" AND ({' OR '.join(conditions)})"

        query += " ORDER BY fecha_emision DESC"

        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

        for row in rows:
            for key in ['fecha_emision', 'fecha_entrega', 'fecha_preferente']:
                row[key] = format_date(row.get(key))

        total_guias = len(rows)
        entregados = sum(1 for r in rows if r['estado'] == 'ENTREGADO')
        pendientes = sum(1 for r in rows if r['estado'] == 'PENDIENTE')
        con_novedad = sum(1 for r in rows if r['estado'] == 'CON NOVEDAD')

        total_piezas = sum(int(r.get('piezas') or 0) for r in rows)
        total_kilos = sum(float(r.get('kilos') or 0) for r in rows)

        conteo_por_estado = {}
        for r in rows:
            e = r['estado']
            conteo_por_estado[e] = conteo_por_estado.get(e, 0) + 1

        guias_por_dia = {}
        piezas_por_dia = {}
        for r in rows:
            fecha = r.get('fecha_emision')
            if fecha:
                if fecha not in guias_por_dia:
                    guias_por_dia[fecha] = {}
                    piezas_por_dia[fecha] = {}
                e = r['estado']
                guias_por_dia[fecha][e] = guias_por_dia[fecha].get(e, 0) + 1
                piezas_por_dia[fecha][e] = piezas_por_dia[fecha].get(e, 0) + int(r.get('piezas') or 0)

        datos_grafico = [
            {"fecha": f, **estados_data}
            for f, estados_data in sorted(guias_por_dia.items())
        ]

        datos_cajas = [
            {"fecha": f, **estados_data}
            for f, estados_data in sorted(piezas_por_dia.items())
        ]

        cliente_estados = {}
        for r in rows:
            c = r.get('nombre_cliente') or 'Sin cliente'
            if c not in cliente_estados:
                cliente_estados[c] = {}
            e = r['estado']
            cliente_estados[c][e] = cliente_estados[c].get(e, 0) + 1

        datos_por_cliente = sorted(
            [{"cliente": c, "total": sum(v.values()), **v} for c, v in cliente_estados.items()],
            key=lambda x: x['total'],
            reverse=True,
        )

        estados_lista = sorted(set(r['estado'] for r in rows))
        clientes_lista = sorted(set(r.get('nombre_cliente', '') for r in rows if r.get('nombre_cliente')))

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
            },
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/guias/detalle")
def get_detalle_dia(
    fecha: str = Query(...),
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
            WHERE DATE(fecha_emision) = %s
        """
        params: list = [fecha]

        # Excluir clientes no deseados
        excl_sql, excl_params = build_exclusion_clause()
        query += excl_sql
        params.extend(excl_params)

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
