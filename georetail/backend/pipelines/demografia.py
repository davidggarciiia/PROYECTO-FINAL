"""
pipelines/demografia.py — Pipeline mensual de datos demográficos.

Fuentes:
  - Renda disponible llars BCN (CSV descargable BCN Open Data): renta media por hogar
    Dataset: "Disposable income of households per capita(€) in the city of Barcelona"
    Columnas: Any, Codi_Districte, Nom_Districte, Codi_Barri, Nom_Barri,
              Seccio_Censal, Import_Euros
    Estrategia de matching: promedio por Codi_Districte (evita el mapeo barrio
    oficial ↔ barrio simplificado de la BD, que no es 1:1).

  - Padró Municipal BCN (CKAN datastore): población, edad, extranjeros
"""
from __future__ import annotations
import csv
import io
import logging
import os
from datetime import date

import httpx
from db.conexion import get_db

logger = logging.getLogger(__name__)

# ── Open Data Barcelona ───────────────────────────────────────────────────────
_CKAN = "https://opendata-ajuntament.barcelona.cat/data/api/action"
_CKAN_HEADERS: dict = (
    {"Authorization": os.environ.get("OPEN_DATA_BCN_API_KEY", "")}
    if os.environ.get("OPEN_DATA_BCN_API_KEY")
    else {}
)

# URL del CSV 2022 de renda disponible por persona (año más reciente disponible)
_RENDA_CSV_URL = (
    "https://opendata-ajuntament.barcelona.cat/data/dataset/"
    "78db0c75-fa56-4604-9510-8b92834a7fd2/resource/"
    "3df0c5b9-de69-4c94-b924-57540e52932f/download/"
    "2022_renda_disponible_llars_per_persona.csv"
)


async def ejecutar() -> dict:
    eid = await _init()
    ok = 0
    try:
        ok += await _poblar_renta()
        ok += await _poblar_padro()
        await _fin(eid, ok, "ok")
        return {"registros": ok}
    except Exception as e:
        logger.error("Pipeline demografia error: %s", e)
        await _fin(eid, ok, "error", str(e))
        raise


async def _poblar_renta() -> int:
    """
    Descarga el CSV de renda disponible por persona y actualiza
    variables_zona.renta_media_hogar usando el promedio por distrito.

    Usa promedios por Codi_Districte porque nuestros 28 barrios simplificados
    no mapean 1:1 con los 73 barrios oficiales de BCN.
    """
    ok = 0
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(30.0), headers=_CKAN_HEADERS, follow_redirects=True
        ) as c:
            r = await c.get(_RENDA_CSV_URL)
            r.raise_for_status()

        # Calcular renta media por distrito (Codi_Districte)
        dist_rentas: dict[str, list[float]] = {}
        reader = csv.DictReader(io.StringIO(r.text))
        for row in reader:
            try:
                dist = str(row.get("Codi_Districte", "") or "").strip().zfill(2)
                renta = float(row.get("Import_Euros", 0) or 0)
                if dist and renta > 0:
                    dist_rentas.setdefault(dist, []).append(renta)
            except (ValueError, TypeError):
                continue

        renta_por_distrito: dict[str, float] = {
            d: sum(vals) / len(vals)
            for d, vals in dist_rentas.items()
            if vals
        }
        logger.info("Renta media por distrito: %s", {
            d: round(v, 0) for d, v in renta_por_distrito.items()
        })

        fecha = date(2022, 6, 1)

        async with get_db() as conn:
            # Para cada zona, obtener el distrito → renta
            rows = await conn.fetch("""
                SELECT z.id AS zona_id, d.codigo AS dist_codigo
                FROM zonas z
                JOIN barrios b ON b.id = z.barrio_id
                JOIN distritos d ON d.id = b.distrito_id
            """)

            for row in rows:
                dist = row["dist_codigo"]
                renta = renta_por_distrito.get(dist)
                if not renta:
                    continue
                # Anchor en variables_zona
                await conn.execute("""
                    INSERT INTO variables_zona (zona_id, fecha, fuente)
                    VALUES ($1, $2, 'renda_bcn_2022')
                    ON CONFLICT (zona_id, fecha) DO UPDATE
                    SET fuente = EXCLUDED.fuente, updated_at = NOW()
                """, row["zona_id"], fecha)
                # Datos demográficos en tabla satélite vz_demografia
                await conn.execute("""
                    INSERT INTO vz_demografia (zona_id, fecha, renta_media_hogar, fuente)
                    VALUES ($1, $2, $3, 'renda_bcn_2022')
                    ON CONFLICT (zona_id, fecha) DO UPDATE
                    SET renta_media_hogar = EXCLUDED.renta_media_hogar,
                        fuente = EXCLUDED.fuente,
                        updated_at = NOW()
                """, row["zona_id"], fecha, round(renta, 2))
                ok += 1

    except Exception as e:
        logger.warning("_poblar_renta error: %s", e)
    return ok


async def _poblar_padro() -> int:
    """Carga datos del padrón: edad media, % extranjeros, densidad."""
    ok = 0
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0), headers=_CKAN_HEADERS) as c:
            sql = """SELECT * FROM "pad_mdbas" LIMIT 2000"""
            r = await c.get(f"{_CKAN}/datastore_search_sql", params={"sql": sql})
            r.raise_for_status()
            rows = r.json().get("result",{}).get("records",[])

        async with get_db() as conn:
            for row in rows:
                barri_code = str(row.get("Codi_Barri","")).zfill(6)
                year = int(row.get("Any",2023))
                fecha = f"{year}-01-01"

                poblacion    = int(row.get("Total") or 0)
                pct_estran   = float(str(row.get("Pct_Estrangers") or "0").replace(",", ".") or 0) / 100
                edad_mediana = float(str(row.get("Edat_Mediana") or "42").replace(",", ".") or 42)

                zona_ids = await conn.fetch("""
                    SELECT z.id FROM zonas z
                    JOIN barrios b ON b.id=z.barrio_id
                    WHERE b.codigo=$1
                """, barri_code)

                for z in zona_ids:
                    # Anchor en variables_zona
                    await conn.execute("""
                        INSERT INTO variables_zona (zona_id, fecha, fuente)
                        VALUES ($1, $2, 'padro_bcn')
                        ON CONFLICT (zona_id, fecha) DO UPDATE
                        SET fuente = EXCLUDED.fuente, updated_at = NOW()
                    """, z["id"], fecha)
                    # Datos demográficos en tabla satélite vz_demografia
                    await conn.execute("""
                        INSERT INTO vz_demografia (zona_id, fecha, poblacion, pct_extranjeros, edad_media, fuente)
                        VALUES ($1, $2, $3, $4, $5, 'padro_bcn')
                        ON CONFLICT (zona_id, fecha) DO UPDATE
                        SET poblacion        = EXCLUDED.poblacion,
                            pct_extranjeros  = EXCLUDED.pct_extranjeros,
                            edad_media       = EXCLUDED.edad_media,
                            fuente           = EXCLUDED.fuente,
                            updated_at       = NOW()
                    """, z["id"], fecha, poblacion, pct_estran, edad_mediana)
                    ok += 1

    except Exception as e:
        logger.warning("_poblar_padro error: %s", e)
    return ok


async def _init():
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones(pipeline,estado) VALUES('demografia','running') RETURNING id")

async def _fin(eid, reg, estado, msg=None):
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones SET fecha_fin=NOW(),registros=$1,estado=$2,mensaje_error=$3 WHERE id=$4",
            reg, estado, msg, eid)
