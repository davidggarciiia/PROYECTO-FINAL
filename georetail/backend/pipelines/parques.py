"""pipelines/parques.py — Parques del AMB: descarga, inserción y cálculo espacial.

Fuente: opendata.amb.cat/serveis_parcs/search/csv
Tabla destino: parques_amb (creada en migración 018_demografia_v5.sql)
Escribe en: vz_entorno.m2_zonas_verdes_cercanas

Frecuencia: mensual día 2, 06:00 (APScheduler)

Flujo:
  1. Descarga CSV desde AMB Open Data
  2. Parsea filas → (nom, municipi, superficie_ha, lat, lon)
  3. Upsert en parques_amb (clave: nom + municipi)
  4. Para cada zona en zonas, cuenta m² de parques en radio 500m
     usando ST_DWithin sobre la geometría de la zona y el centroide del parque
  5. Actualiza vz_entorno.m2_zonas_verdes_cercanas
"""
from __future__ import annotations

import csv
import io
import logging
from datetime import date, datetime, timezone
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

_AMB_PARCS_URL = "http://opendata.amb.cat/serveis_parcs/search/csv"
_RADIO_M = 500          # radio de búsqueda en metros
_TIMEOUT = 30.0         # segundos timeout HTTP


# ── Columnas esperadas del CSV (pueden venir con mayúsculas/tildes distintas) ──
# El CSV del AMB suele tener: NOM, MUNICIPI, SUPERFICIE_HA, LATITUD, LONGITUD
# (o variantes: nom, municipi, hectarees, lat, lng …)
_COL_ALIASES: dict[str, list[str]] = {
    "nom":           ["nom", "name", "nombre", "NOM", "NAME"],
    "municipi":      ["municipi", "municipio", "MUNICIPI", "MUNICIPIO"],
    "superficie_ha": ["superficie_ha", "hectarees", "hectareas", "superficie",
                      "SUPERFICIE_HA", "HECTAREES", "AREA_HA"],
    "lat":           ["latitud", "lat", "LATITUD", "LAT", "y", "Y"],
    "lon":           ["longitud", "lon", "lng", "LONGITUD", "LON", "LNG", "x", "X"],
}


def _parse_float(value, default: float = 0.0) -> float:
    try:
        return float(str(value).replace(",", ".").strip())
    except (TypeError, ValueError):
        return default


def _resolve_columns(header: list[str]) -> dict[str, Optional[str]]:
    """Mapea nombres canónicos → nombre real en el CSV (o None si no existe)."""
    header_lower = {h.lower().strip(): h for h in header}
    mapping: dict[str, Optional[str]] = {}
    for canonical, aliases in _COL_ALIASES.items():
        found = None
        for alias in aliases:
            if alias.lower() in header_lower:
                found = header_lower[alias.lower()]
                break
        mapping[canonical] = found
    return mapping


async def _descargar_csv() -> list[dict]:
    """Descarga el CSV de parques AMB y devuelve lista de dicts normalizados."""
    async with httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=True) as client:
        resp = await client.get(_AMB_PARCS_URL)
        resp.raise_for_status()

    # Intentar decodificar UTF-8; fallback a latin-1 (frecuente en CSVs catalanes)
    try:
        text = resp.content.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = resp.content.decode("latin-1")

    reader = csv.DictReader(io.StringIO(text))
    header = reader.fieldnames or []
    col = _resolve_columns(list(header))

    parques: list[dict] = []
    for row in reader:
        nom       = (row.get(col["nom"]) or "").strip() if col["nom"] else ""
        municipi  = (row.get(col["municipi"]) or "").strip() if col["municipi"] else ""
        sup_ha    = _parse_float(row.get(col["superficie_ha"]) if col["superficie_ha"] else None, 0.0)
        lat       = _parse_float(row.get(col["lat"]) if col["lat"] else None, 0.0)
        lon       = _parse_float(row.get(col["lon"]) if col["lon"] else None, 0.0)

        if not nom or lat == 0.0 or lon == 0.0:
            continue  # fila inválida — sin nombre o sin coordenadas

        parques.append({
            "nom": nom[:300],
            "municipi": municipi[:100],
            "superficie_ha": sup_ha if sup_ha > 0 else None,
            "lat": lat,
            "lon": lon,
        })

    logger.info("CSV AMB parcs: %d parques con coordenadas válidas", len(parques))
    return parques


async def _upsert_parques(conn, parques: list[dict]) -> int:
    """Inserta / actualiza parques en la tabla parques_amb. Devuelve nº insertados."""
    now = datetime.now(timezone.utc)
    inserted = 0
    for p in parques:
        wkt_point = f"SRID=4326;POINT({p['lon']} {p['lat']})"
        await conn.execute(
            """
            INSERT INTO parques_amb (nom, municipi, superficie_ha, geometria, fuente, updated_at)
            VALUES ($1, $2, $3, ST_GeomFromEWKT($4), 'amb_opendata', $5)
            ON CONFLICT (nom, municipi) DO UPDATE
               SET superficie_ha = EXCLUDED.superficie_ha,
                   geometria     = EXCLUDED.geometria,
                   updated_at    = EXCLUDED.updated_at
            """,
            p["nom"], p["municipi"], p["superficie_ha"], wkt_point, now,
        )
        inserted += 1
    return inserted


async def _calcular_m2_zonas_verdes(conn) -> int:
    """
    Para cada zona calcula la suma de m² de parques AMB en radio 500m
    (usando el centroide de la zona) y actualiza vz_entorno.m2_zonas_verdes_cercanas.

    superficie_ha → m²: 1 ha = 10 000 m².
    Si un parque no tiene superficie_ha usa 0 (contribuye como existencia, no como área).
    Devuelve el número de zonas actualizadas.
    """
    rows = await conn.fetch(
        """
        SELECT z.id AS zona_id,
               COALESCE(SUM(COALESCE(p.superficie_ha, 0) * 10000), 0)::float AS m2_verdes
        FROM zonas z
        JOIN parques_amb p
          ON ST_DWithin(
                ST_Centroid(z.geometria)::geography,
                p.geometria::geography,
                $1
             )
        GROUP BY z.id
        """,
        _RADIO_M,
    )

    today = date.today()
    updated = 0
    for row in rows:
        await conn.execute(
            """
            INSERT INTO vz_entorno (zona_id, fecha, m2_zonas_verdes_cercanas)
            VALUES ($1, $2, $3)
            ON CONFLICT (zona_id, fecha) DO UPDATE
               SET m2_zonas_verdes_cercanas = EXCLUDED.m2_zonas_verdes_cercanas
            """,
            row["zona_id"], today, row["m2_verdes"],
        )
        updated += 1

    # Zonas sin parques cercanos → escribir 0 explícito para no dejar NULL
    await conn.execute(
        """
        INSERT INTO vz_entorno (zona_id, fecha, m2_zonas_verdes_cercanas)
        SELECT z.id, $1, 0
        FROM zonas z
        WHERE z.id NOT IN (
            SELECT DISTINCT zona_id FROM vz_entorno WHERE fecha = $1
              AND m2_zonas_verdes_cercanas IS NOT NULL
        )
        ON CONFLICT (zona_id, fecha) DO NOTHING
        """,
        today,
    )

    return updated


async def _init_pipeline() -> int:
    """Registra inicio de ejecución en pipeline_ejecuciones. Devuelve el id."""
    from db.conexion import get_db  # noqa: PLC0415
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones(pipeline, estado) "
            "VALUES('parques', 'running') RETURNING id"
        )


async def _fin_pipeline(eid: int, registros: int, estado: str, msg: str = None) -> None:
    """Actualiza el registro de ejecución con el resultado final."""
    from db.conexion import get_db  # noqa: PLC0415
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones "
            "SET fecha_fin = NOW(), registros = $1, estado = $2, mensaje_error = $3 "
            "WHERE id = $4",
            registros, estado, msg, eid,
        )


async def actualizar_parques_amb() -> dict:
    """Punto de entrada principal. Descarga, inserta y calcula zonas verdes.

    Returns:
        dict con claves: parques_descargados, parques_upserted, zonas_actualizadas
    """
    from db.conexion import get_db  # noqa: PLC0415

    logger.info("Iniciando pipeline parques AMB")

    # D1 FIX: registrar ejecución en pipeline_ejecuciones (igual que demografia.py)
    eid = await _init_pipeline()
    result = {"parques_descargados": 0, "parques_upserted": 0, "zonas_actualizadas": 0}

    try:
        parques = await _descargar_csv()
        if not parques:
            logger.warning("parques AMB: CSV vacío o sin filas válidas — abortando")
            await _fin_pipeline(eid, 0, "ok")
            return result

        async with get_db() as conn:
            upserted = await _upsert_parques(conn, parques)
            zonas_upd = await _calcular_m2_zonas_verdes(conn)

        result = {
            "parques_descargados": len(parques),
            "parques_upserted": upserted,
            "zonas_actualizadas": zonas_upd,
        }
        await _fin_pipeline(eid, zonas_upd, "ok")
        logger.info("Pipeline parques AMB completado: %s", result)
        return result

    except Exception as e:
        logger.error("Pipeline parques AMB error: %s", e)
        await _fin_pipeline(eid, 0, "error", str(e))
        raise


# Alias para scheduler.py
ejecutar = actualizar_parques_amb
