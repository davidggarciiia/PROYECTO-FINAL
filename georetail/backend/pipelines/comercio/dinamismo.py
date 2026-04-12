"""
pipelines/comercio/dinamismo.py — Snapshot mensual de dinamismo comercial por zona.

Agrega datos de varias fuentes en la tabla dinamismo_zonal:
  - llicencies_activitat  → apertura/cierre de licencias
  - negocios_historico    → supervivencia histórica
  - variables_zona        → tendencia demográfica (si hay snapshots múltiples)
  - v_mercado_zona        → variación de precios de alquiler

Frecuencia: mensual, día 1 a las 02:00 (scheduler.py).
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
from datetime import date, datetime, timezone
from typing import Any

from db.conexion import get_db

logger = logging.getLogger(__name__)

_MUESTRA_MINIMA = 5   # negocios_historico_count mínimo para calcular supervivencia


# ---------------------------------------------------------------------------
# Score y tendencia
# ---------------------------------------------------------------------------

def _calcular_score(
    ratio_1a: float | None,
    supervivencia: float | None,
    renta_var: float | None,
    hhi: float | None,
) -> float:
    """Score de dinamismo 0-10 a partir de los indicadores de la zona."""
    score = 5.0

    if ratio_1a is not None:
        if ratio_1a > 1.5:
            score += 2.0
        elif ratio_1a >= 1.0:
            score += 1.0
        elif ratio_1a < 0.5:
            score -= 2.0

    if supervivencia is not None:
        if supervivencia > 0.65:
            score += 1.0
        elif supervivencia < 0.40:
            score -= 1.0

    if renta_var is not None:
        if renta_var > 0.05:
            score += 1.0
        elif renta_var < -0.05:
            score -= 1.0

    if hhi is not None and hhi > 0.50:
        score -= 1.0

    return round(max(0.0, min(10.0, score)), 2)


def _clasificar_tendencia(
    score: float,
    ratio_1a: float | None,
    hhi: float | None,
    count: int,
) -> str:
    if count < _MUESTRA_MINIMA:
        return "sin_datos"
    if score >= 7.0 and (ratio_1a or 0.0) > 1.2:
        return "emergente"
    if score >= 5.0 and (hhi or 0.0) > 0.50:
        return "saturado"
    if score >= 5.0:
        return "estable"
    return "declive"


# ---------------------------------------------------------------------------
# Helpers de cálculo por zona
# ---------------------------------------------------------------------------

async def _licencias_zona(conn, zona_id: int) -> dict[str, Any]:
    """Cuenta licencias abiertas/cerradas en la zona."""
    row = await conn.fetchrow(
        """
        SELECT
            COUNT(*) FILTER (WHERE activa AND fecha_alta >= NOW() - INTERVAL '1 year')  AS abiertas_1a,
            COUNT(*) FILTER (WHERE NOT activa AND fecha_baja >= NOW() - INTERVAL '1 year') AS cerradas_1a,
            COUNT(*) FILTER (WHERE activa AND fecha_alta >= NOW() - INTERVAL '3 years') AS abiertas_3a,
            COUNT(*) FILTER (WHERE NOT activa AND fecha_baja >= NOW() - INTERVAL '3 years') AS cerradas_3a
        FROM llicencies_activitat
        WHERE zona_id = $1
        """,
        zona_id,
    )
    if not row:
        return {}
    ab1 = row["abiertas_1a"] or 0
    ce1 = row["cerradas_1a"] or 0
    ab3 = row["abiertas_3a"] or 0
    ce3 = row["cerradas_3a"] or 0
    return {
        "licencias_abiertas_1a": ab1,
        "licencias_cerradas_1a": ce1,
        "licencias_abiertas_3a": ab3,
        "licencias_cerradas_3a": ce3,
        "ratio_apertura_cierre_1a": round(ab1 / ce1, 3) if ce1 else None,
        "ratio_apertura_cierre_3a": round(ab3 / ce3, 3) if ce3 else None,
    }


async def _supervivencia_zona(conn, zona_id: int) -> dict[str, Any]:
    """Tasa de supervivencia histórica de negocios en la zona."""
    row = await conn.fetchrow(
        """
        SELECT
            COUNT(*)            AS total,
            AVG(supervivio::int) AS tasa
        FROM negocios_historico
        WHERE zona_id = $1
        """,
        zona_id,
    )
    if not row or not row["total"]:
        return {"negocios_historico_count": 0, "tasa_supervivencia_3a": None}
    return {
        "negocios_historico_count": row["total"],
        "tasa_supervivencia_3a": round(float(row["tasa"]), 3) if row["tasa"] is not None else None,
    }


async def _hhi_zona(conn, zona_id: int) -> float | None:
    """Herfindahl-Hirschman Index sectorial para la zona."""
    rows = await conn.fetch(
        """
        SELECT sector, COUNT(*) AS n
        FROM negocios_historico
        WHERE zona_id = $1 AND sector IS NOT NULL
        GROUP BY sector
        """,
        zona_id,
    )
    if not rows:
        return None
    total = sum(r["n"] for r in rows)
    if total == 0:
        return None
    hhi = sum((r["n"] / total) ** 2 for r in rows)
    return round(hhi, 4)


async def _demografia_tendencia(conn, zona_id: int) -> dict[str, Any]:
    """Variación % de renta y población: snapshot más reciente vs más antiguo en ventana 3 años."""
    rows = await conn.fetch(
        """
        SELECT renta_bruta_hogar, poblacion, updated_at::date AS fecha
        FROM variables_zona
        WHERE zona_id = $1
          AND renta_bruta_hogar IS NOT NULL
          AND updated_at >= NOW() - INTERVAL '3 years'
        ORDER BY updated_at DESC
        """,
        zona_id,
    )
    if len(rows) < 2:
        return {"renta_variacion_3a": None, "poblacion_variacion_3a": None}

    # Más reciente vs más antiguo dentro de la ventana de 3 años
    reciente, antiguo = rows[0], rows[-1]

    def _pct_cambio(nuevo, viejo) -> float | None:
        if viejo and viejo != 0 and nuevo is not None:
            return round((float(nuevo) - float(viejo)) / abs(float(viejo)), 4)
        return None

    return {
        "renta_variacion_3a":     _pct_cambio(reciente["renta_bruta_hogar"], antiguo["renta_bruta_hogar"]),
        "poblacion_variacion_3a": _pct_cambio(reciente["poblacion"], antiguo["poblacion"]),
    }


async def _vacantes_zona(conn, zona_id: int) -> float | None:
    row = await conn.fetchrow(
        "SELECT pct_locales_vacios FROM variables_zona WHERE zona_id = $1 ORDER BY updated_at DESC LIMIT 1",
        zona_id,
    )
    if not row or row["pct_locales_vacios"] is None:
        return None
    return float(row["pct_locales_vacios"])


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

async def _procesar_zona(conn, zona_id: int, periodo: date) -> bool:
    """Calcula y hace upsert de una zona. Devuelve True si ok."""
    try:
        lic   = await _licencias_zona(conn, zona_id)
        surv  = await _supervivencia_zona(conn, zona_id)
        demo  = await _demografia_tendencia(conn, zona_id)
        hhi   = await _hhi_zona(conn, zona_id)
        vacan = await _vacantes_zona(conn, zona_id)

        ratio_1a    = lic.get("ratio_apertura_cierre_1a")
        superviv    = surv.get("tasa_supervivencia_3a")
        renta_var   = demo.get("renta_variacion_3a")
        count       = surv.get("negocios_historico_count", 0)

        score     = _calcular_score(ratio_1a, superviv, renta_var, hhi)
        tendencia = _clasificar_tendencia(score, ratio_1a, hhi, count)

        await conn.execute(
            """
            INSERT INTO dinamismo_zonal (
                zona_id, periodo,
                licencias_abiertas_1a, licencias_cerradas_1a,
                licencias_abiertas_3a, licencias_cerradas_3a,
                ratio_apertura_cierre_1a, ratio_apertura_cierre_3a,
                negocios_historico_count, tasa_supervivencia_3a,
                renta_variacion_3a, poblacion_variacion_3a,
                hhi_sectorial, vacantes_ratio,
                score_dinamismo, tendencia, updated_at
            ) VALUES (
                $1, $2,
                $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12, $13, $14,
                $15, $16, now()
            )
            ON CONFLICT (zona_id, periodo) DO UPDATE SET
                licencias_abiertas_1a    = EXCLUDED.licencias_abiertas_1a,
                licencias_cerradas_1a    = EXCLUDED.licencias_cerradas_1a,
                licencias_abiertas_3a    = EXCLUDED.licencias_abiertas_3a,
                licencias_cerradas_3a    = EXCLUDED.licencias_cerradas_3a,
                ratio_apertura_cierre_1a = EXCLUDED.ratio_apertura_cierre_1a,
                ratio_apertura_cierre_3a = EXCLUDED.ratio_apertura_cierre_3a,
                negocios_historico_count = EXCLUDED.negocios_historico_count,
                tasa_supervivencia_3a    = EXCLUDED.tasa_supervivencia_3a,
                renta_variacion_3a       = EXCLUDED.renta_variacion_3a,
                poblacion_variacion_3a   = EXCLUDED.poblacion_variacion_3a,
                hhi_sectorial            = EXCLUDED.hhi_sectorial,
                vacantes_ratio           = EXCLUDED.vacantes_ratio,
                score_dinamismo          = EXCLUDED.score_dinamismo,
                tendencia                = EXCLUDED.tendencia,
                updated_at               = now()
            """,
            zona_id, periodo,
            lic.get("licencias_abiertas_1a", 0),
            lic.get("licencias_cerradas_1a", 0),
            lic.get("licencias_abiertas_3a", 0),
            lic.get("licencias_cerradas_3a", 0),
            ratio_1a,
            lic.get("ratio_apertura_cierre_3a"),
            count,
            superviv,
            renta_var,
            demo.get("poblacion_variacion_3a"),
            hhi,
            vacan,
            score,
            tendencia,
        )
        return True

    except Exception as exc:  # noqa: BLE001
        logger.error("dinamismo: error zona %d — %s", zona_id, exc)
        return False


async def run() -> dict:
    """Entry point del pipeline. Llamado por scheduler y api/admin."""
    inicio = datetime.now(timezone.utc)
    exec_id: int | None = None

    async with get_db() as conn:
        # Registrar inicio
        exec_id = await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones (pipeline, estado) VALUES ($1, 'running') RETURNING id",
            "dinamismo_comercial",
        )

        periodo = date.today().replace(day=1)   # snapshot del mes en curso
        zonas   = await conn.fetch("SELECT id FROM zonas ORDER BY id")
        total   = len(zonas)
        ok = 0

        for i, row in enumerate(zonas, 1):
            if await _procesar_zona(conn, row["id"], periodo):
                ok += 1
            if i % 50 == 0:
                logger.info("dinamismo: %d/%d zonas procesadas (%d ok)", i, total, ok)

        duracion = (datetime.now(timezone.utc) - inicio).total_seconds()
        resultado = {"zonas_ok": ok, "zonas_total": total, "periodo": str(periodo), "segundos": duracion}

        await conn.execute(
            """
            UPDATE pipeline_ejecuciones
            SET estado = 'completed', fin = now(), resultado = $1
            WHERE id = $2
            """,
            json.dumps(resultado),
            exec_id,
        )

    logger.info("dinamismo_comercial completado: %s", resultado)
    return resultado


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())
