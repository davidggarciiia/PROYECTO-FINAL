"""db/financiero.py — Operaciones BD para el módulo financiero."""
from __future__ import annotations
import json, logging
from typing import Optional
from db.conexion import get_db

logger = logging.getLogger(__name__)


async def get_benchmarks_sector(sector: str) -> dict:
    async with get_db() as conn:
        row = await conn.fetchrow("SELECT * FROM benchmarks_sector WHERE sector_codigo=$1", sector)
    if row:
        return dict(row)
    logger.warning("Sector '%s' sin benchmarks, usando genéricos", sector)
    return {
        "sector_codigo": sector, "ticket_medio_min": 15.0, "ticket_medio_max": 60.0,
        "margen_bruto_tipico": 0.60, "coste_personal_pct": 0.28, "coste_suministros_pct": 0.04,
        "alquiler_sobre_ventas_max": 0.15, "reforma_m2_min": 300.0, "reforma_m2_max": 700.0,
        "conversion_rate_min": 0.005, "conversion_rate_max": 0.020, "horas_apertura_dia": 9.0,
        "dias_apertura_mes_tipico": 26, "empleados_por_m2": 25.0,
        "salario_base_mensual_convenio": 1620.0, "coste_suministros_por_m2": 5.0,
        "coste_gestoria_mensual": 185.0, "seguro_rc_mensual": 100.0,
        "equipamiento_base_min": 5000.0, "equipamiento_base_max": 20000.0,
        "coste_licencias_apertura": 1500.0, "otros_iniciales_fijos": 2000.0,
        "is_appointment_based": False, "clientes_dia_por_puesto_min": 2.0,
        "clientes_dia_por_puesto_max": 5.0,
    }


async def get_parametros_precalculados(zona_id: str, sector: str) -> Optional[dict]:
    try:
        async with get_db() as conn:
            row = await conn.fetchrow("""
                SELECT pfz.* FROM v_parametros_financieros_actuales pfz
                JOIN sectores s ON s.id=pfz.sector_id
                WHERE pfz.zona_id=$1 AND s.codigo=$2
            """, zona_id, sector)
        return dict(row) if row else None
    except Exception as e:
        logger.warning("get_parametros_precalculados fail: %s", e)
        return None


async def guardar_analisis_financiero(session_id: str, zona_id: str,
                                      params: dict, resultado: dict) -> None:
    try:
        async with get_db() as conn:
            await conn.execute("""
                INSERT INTO analisis_financieros
                    (session_id,zona_id,params,inversion_total,
                     ingresos_anuales_conservador,ingresos_anuales_optimista,
                     ebitda_conservador,ebitda_optimista,
                     roi_3a_conservador,roi_3a_optimista,
                     payback_meses_conservador,payback_meses_optimista,proyeccion_json)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
                ON CONFLICT (session_id,zona_id) DO UPDATE SET
                    params=EXCLUDED.params,
                    inversion_total=EXCLUDED.inversion_total,
                    ingresos_anuales_conservador=EXCLUDED.ingresos_anuales_conservador,
                    ingresos_anuales_optimista=EXCLUDED.ingresos_anuales_optimista,
                    ebitda_conservador=EXCLUDED.ebitda_conservador,
                    ebitda_optimista=EXCLUDED.ebitda_optimista,
                    roi_3a_conservador=EXCLUDED.roi_3a_conservador,
                    roi_3a_optimista=EXCLUDED.roi_3a_optimista,
                    payback_meses_conservador=EXCLUDED.payback_meses_conservador,
                    payback_meses_optimista=EXCLUDED.payback_meses_optimista,
                    proyeccion_json=EXCLUDED.proyeccion_json
            """,
            session_id, zona_id, json.dumps(params),
            resultado.get("inversion_total",0),
            resultado.get("ingresos_anuales_conservador",0),
            resultado.get("ingresos_anuales_optimista",0),
            resultado.get("ebitda_anual_conservador",0),
            resultado.get("ebitda_anual_optimista",0),
            resultado.get("roi_3a_conservador",0),
            resultado.get("roi_3a_optimista",0),
            resultado.get("payback_meses_conservador",999),
            resultado.get("payback_meses_optimista",999),
            json.dumps(resultado.get("proyeccion",[])))
    except Exception as e:
        logger.warning("guardar_analisis_financiero fail: %s", e)


async def get_analisis_financiero(session_id: str, zona_id: str) -> Optional[dict]:
    try:
        async with get_db() as conn:
            row = await conn.fetchrow("""
                SELECT * FROM analisis_financieros
                WHERE session_id=$1 AND zona_id=$2
                ORDER BY created_at DESC LIMIT 1
            """, session_id, zona_id)
        return dict(row) if row else None
    except Exception as e:
        logger.warning("get_analisis_financiero fail: %s", e)
        return None
