"""
pipelines/aforaments.py — Ingesta diaria de flujo peatonal desde Open Data BCN.

Dataset: `aforaments-detall` (CKAN Open Data BCN)
URL: https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search_sql
Tabla destino: variables_zona (columnas flujo_peatonal_*)

ESTRATEGIA DE ASIGNACIÓN (v2):
  En vez de ST_Within (solo asigna si el sensor cae exactamente dentro de la zona),
  usamos ST_DWithin con radio 200m + ponderación por distancia inversa al cuadrado.

  Esto resuelve que en un mismo barrio haya varias zonas pero solo una tenga
  un aforador encima — ahora todas las zonas dentro de 200m reciben una
  estimación proporcional a su distancia al sensor.

  Fórmula de ponderación:
    peso_i = 1 / distancia_i²
    flujo_zona_i = flujo_sensor × (peso_i / Σ pesos)

  Si hay varios sensores dentro del radio de una zona, sus flujos se suman.

ZONAS COMERCIALES:
  Las zonas con actividad comercial alta reciben un multiplicador basado
  en el ratio de locales comerciales vs total de locales (Cens Locals BCN).
  Multiplicador: 1.0 (sin dato) → hasta 1.35 (zona muy comercial).
"""
from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from typing import Optional

import httpx

from db.conexion import get_db

logger = logging.getLogger(__name__)

# ── Open Data Barcelona ───────────────────────────────────────────────────────
# API key opcional — sin key funciona con límite ~1000 req/día por IP.
# Configurar OPEN_DATA_BCN_API_KEY en el .env para límite ~10.000 req/día.
# Registro gratuito: https://opendata-ajuntament.barcelona.cat → Mi cuenta → API Key
_CKAN_BASE  = "https://opendata-ajuntament.barcelona.cat/data/api/action"
_CKAN_HEADERS: dict = (
    {"Authorization": os.environ.get("OPEN_DATA_BCN_API_KEY", "")}
    if os.environ.get("OPEN_DATA_BCN_API_KEY")
    else {}
)
_RADIO_M   = 200   # Radio de influencia de cada sensor en metros
_MAX_ZONAS = 8     # Máximo de zonas que puede influir un sensor


async def ejecutar() -> dict:
    eid = await _init("aforaments")
    try:
        desde = await _ultima_fecha("aforaments")
        logger.info("Aforaments: procesando desde %s", desde)

        total = 0
        offset = 0
        limit  = 500

        async with httpx.AsyncClient(timeout=30.0, headers=_CKAN_HEADERS) as client:
            while True:
                sql = (
                    f'SELECT * FROM "aforaments-detall" '
                    f"WHERE \"Data\" >= '{desde}' "
                    f"LIMIT {limit} OFFSET {offset}"
                )
                resp = await client.get(
                    f"{_CKAN_BASE}/datastore_search_sql",
                    params={"sql": sql},
                )
                resp.raise_for_status()
                rows = resp.json().get("result", {}).get("records", [])
                if not rows:
                    break

                await _procesar_lote(rows)
                total  += len(rows)
                offset += limit
                if len(rows) < limit:
                    break

        await _recalcular_totales(desde)
        await imputar_zonas_sin_cobertura(desde)

        await _fin(eid, total, "ok")
        logger.info("Aforaments OK — %d registros", total)
        return {"registros": total}

    except Exception as exc:
        logger.error("Pipeline aforaments ERROR: %s", exc, exc_info=True)
        await _fin(eid, 0, "error", str(exc))
        raise


async def _procesar_lote(rows: list[dict]) -> None:
    async with get_db() as conn:
        for row in rows:
            try:
                lat   = float((row.get("Latitud",  "0") or "0").replace(",", "."))
                lng   = float((row.get("Longitud", "0") or "0").replace(",", "."))
                hora  = int(row.get("Hora", -1) or -1)
                valor = float(row.get("Valor", "0") or 0)
                fecha = str(row.get("Data", ""))[:10]

                if not fecha or not lat or not lng or valor <= 0:
                    continue

                franja = _hora_a_franja(hora)
                if franja is None:
                    continue

                col = f"flujo_peatonal_{franja}"

                # Zonas dentro del radio, ordenadas por distancia al centroide
                zonas = await conn.fetch(
                    """
                    SELECT
                        z.id AS zona_id,
                        ST_Distance(
                            z.geometria::geography,
                            ST_SetSRID(ST_MakePoint($1,$2),4326)::geography
                        ) AS distancia_m,
                        COALESCE(vz.ratio_locales_comerciales, 0.0) AS ratio_comercial
                    FROM zonas z
                    LEFT JOIN LATERAL (
                        SELECT ratio_locales_comerciales
                        FROM variables_zona
                        WHERE zona_id = z.id
                        ORDER BY fecha DESC LIMIT 1
                    ) vz ON TRUE
                    WHERE ST_DWithin(
                        z.geometria::geography,
                        ST_SetSRID(ST_MakePoint($1,$2),4326)::geography,
                        $3
                    )
                    ORDER BY distancia_m ASC
                    LIMIT $4
                    """,
                    lng, lat, _RADIO_M, _MAX_ZONAS,
                )

                if not zonas:
                    continue

                # Ponderación por distancia inversa al cuadrado
                pesos     = [1.0 / max(1.0, float(z["distancia_m"])) ** 2 for z in zonas]
                suma_pesos = sum(pesos) or 1.0

                for zona, peso in zip(zonas, pesos):
                    fraccion  = peso / suma_pesos
                    flujo_zona = valor * fraccion

                    # Multiplicador comercial (calles comerciales tienen más peatones)
                    ratio_com    = float(zona["ratio_comercial"] or 0.0)
                    multiplicador = 1.0 + min(0.35, ratio_com * 0.70)
                    flujo_zona   *= multiplicador

                    await conn.execute(
                        f"""
                        INSERT INTO variables_zona (zona_id, fecha, {col}, fuente)
                        VALUES ($1, $2, $3, 'aforadors_bcn_v2')
                        ON CONFLICT (zona_id, fecha) DO UPDATE
                        SET {col} = COALESCE(variables_zona.{col}, 0) + $3,
                            fuente = 'aforadors_bcn_v2'
                        """,
                        zona["zona_id"], fecha, round(flujo_zona, 1),
                    )

            except Exception as exc:
                logger.debug("Error aforament row: %s", exc)


async def _recalcular_totales(desde: str) -> None:
    """Recalcula flujo_peatonal_total = suma de las tres franjas."""
    async with get_db() as conn:
        await conn.execute(
            """
            UPDATE variables_zona
            SET flujo_peatonal_total = (
                COALESCE(flujo_peatonal_manana, 0) +
                COALESCE(flujo_peatonal_tarde,  0) +
                COALESCE(flujo_peatonal_noche,  0)
            )
            WHERE fecha >= $1 AND fuente = 'aforadors_bcn_v2'
            """,
            desde,
        )
    logger.info("Totales recalculados desde %s", desde)


async def imputar_zonas_sin_cobertura(fecha: str) -> int:
    """
    Para zonas sin aforador a menos de 200m, imputa con la media
    de las zonas del mismo barrio que sí tienen dato real.
    Fallback: media del distrito → medias globales históricas.
    """
    async with get_db() as conn:
        ids_con_dato = {
            r["zona_id"] for r in await conn.fetch(
                "SELECT zona_id FROM variables_zona "
                "WHERE fecha=$1 AND fuente='aforadors_bcn_v2' AND flujo_peatonal_total>0",
                fecha,
            )
        }

        todas = await conn.fetch(
            "SELECT z.id, b.id AS barrio_id, d.id AS distrito_id "
            "FROM zonas z "
            "JOIN barrios b ON z.barrio_id = b.id "
            "JOIN distritos d ON b.distrito_id = d.id"
        )
        sin_dato = [z for z in todas if z["id"] not in ids_con_dato]
        if not sin_dato:
            return 0

        mb = {r["barrio_id"]: r for r in await conn.fetch(
            """
            SELECT b.id AS barrio_id,
                   AVG(vz.flujo_peatonal_manana) AS manana,
                   AVG(vz.flujo_peatonal_tarde)  AS tarde,
                   AVG(vz.flujo_peatonal_noche)  AS noche,
                   AVG(vz.flujo_peatonal_total)  AS total
            FROM variables_zona vz
            JOIN zonas z  ON z.id = vz.zona_id
            JOIN barrios b ON z.barrio_id = b.id
            WHERE vz.fecha=$1 AND vz.fuente='aforadors_bcn_v2'
            GROUP BY b.id
            """, fecha,
        )}
        md = {r["distrito_id"]: r for r in await conn.fetch(
            """
            SELECT d.id AS distrito_id,
                   AVG(vz.flujo_peatonal_manana) AS manana,
                   AVG(vz.flujo_peatonal_tarde)  AS tarde,
                   AVG(vz.flujo_peatonal_noche)  AS noche,
                   AVG(vz.flujo_peatonal_total)  AS total
            FROM variables_zona vz
            JOIN zonas z  ON z.id = vz.zona_id
            JOIN barrios b ON z.barrio_id = b.id
            JOIN distritos d ON b.distrito_id = d.id
            WHERE vz.fecha=$1 AND vz.fuente='aforadors_bcn_v2'
            GROUP BY d.id
            """, fecha,
        )}

        fallback = {"manana": 297.5, "tarde": 357.0, "noche": 195.5, "total": 850.0}
        n = 0
        for zona in sin_dato:
            m = mb.get(zona["barrio_id"]) or md.get(zona["distrito_id"]) or fallback
            await conn.execute(
                """
                INSERT INTO variables_zona
                    (zona_id, fecha, flujo_peatonal_manana, flujo_peatonal_tarde,
                     flujo_peatonal_noche, flujo_peatonal_total, fuente)
                VALUES ($1,$2,$3,$4,$5,$6,'imputado_barrio')
                ON CONFLICT (zona_id, fecha) DO NOTHING
                """,
                zona["id"], fecha,
                round(float(m["manana"] or fallback["manana"]), 1),
                round(float(m["tarde"]  or fallback["tarde"]),  1),
                round(float(m["noche"]  or fallback["noche"]),  1),
                round(float(m["total"]  or fallback["total"]),  1),
            )
            n += 1

    logger.info("Imputadas %d zonas sin cobertura para %s", n, fecha)
    return n


def _hora_a_franja(hora: int) -> Optional[str]:
    if 8  <= hora < 14: return "manana"
    if 14 <= hora < 20: return "tarde"
    if 20 <= hora < 23: return "noche"
    return None


async def _ultima_fecha(pipeline: str) -> str:
    async with get_db() as conn:
        row = await conn.fetchrow(
            "SELECT fecha_inicio FROM pipeline_ejecuciones "
            "WHERE pipeline=$1 AND estado='ok' "
            "ORDER BY fecha_inicio DESC LIMIT 1",
            pipeline,
        )
    if row:
        return str(row["fecha_inicio"].date() - timedelta(days=3))
    return str(date.today() - timedelta(days=30))


async def _init(pipeline: str) -> int:
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones (pipeline, estado) "
            "VALUES ($1,'running') RETURNING id",
            pipeline,
        )


async def _fin(eid: int, registros: int, estado: str, mensaje: Optional[str] = None) -> None:
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones "
            "SET fecha_fin=NOW(), registros=$1, estado=$2, mensaje_error=$3 "
            "WHERE id=$4",
            registros, estado, mensaje, eid,
        )
