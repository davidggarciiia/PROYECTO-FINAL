"""
pipelines/entorno_comercial.py — Pipeline mensual de datos de entorno comercial (v1).

Fuentes:
  - Mercats Municipals (CKAN Open Data BCN):
    mercados_municipales_1km (conteo en radio 1km por zona)
  - Mapa Estratègic de Soroll (CKAN Open Data BCN):
    nivel_ruido_db (media ponderada Lden por zona)
  - Equipaments i Serveis (CKAN Open Data BCN):
    score_equipamientos (diversidad + densidad de equipamientos por zona)

Columnas escritas en vz_entorno:
  mercados_municipales_1km, nivel_ruido_db, score_equipamientos

Ejecuta mensual día 4, 06:00 (scheduler.py).
"""
from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
from collections import defaultdict
from datetime import date
from typing import Optional

import httpx

from db.conexion import get_db

logger = logging.getLogger(__name__)

# ── Constantes ────────────────────────────────────────────────────────────────
_HTTP_RETRIES = 3
_HTTP_RETRY_DELAY = 1.0

_CKAN = "https://opendata-ajuntament.barcelona.cat/data/api/action"
_CKAN_HEADERS: dict = (
    {"Authorization": os.environ.get("OPEN_DATA_BCN_API_KEY", "")}
    if os.environ.get("OPEN_DATA_BCN_API_KEY")
    else {}
)

# Dataset IDs para CKAN package_show
_MERCATS_DATASET_ID = "mercats-municipals"
_SOROLL_DATASET_ID = "tramer-mapa-estrategic-soroll"
_EQUIPAMENTS_DATASET_ID = "equipament-serveis-socials"


async def ejecutar() -> dict:
    """Punto de entrada del pipeline. Llamado por scheduler o manualmente."""
    eid = await _init()
    ok = 0
    try:
        ok += await _poblar_mercats_municipals()
        ok += await _poblar_ruido()
        ok += await _poblar_equipamientos()
        await _fin(eid, ok, "ok")
        return {"registros": ok}
    except Exception as e:
        logger.error("Pipeline entorno_comercial error: %s", e)
        await _fin(eid, ok, "error", str(e))
        raise


# ── 1. Mercats Municipals (CKAN) ────────────────────────────────────────────

async def _poblar_mercats_municipals() -> int:
    """
    Descarga el dataset mercats-municipals de Open Data BCN,
    inserta en la tabla mercados_municipales y calcula mercados_municipales_1km
    por zona en vz_entorno.
    """
    ok = 0
    try:
        # Intentar primero JSON (datastore), fallback a CSV
        mercats_data = await _fetch_mercats_json()
        if not mercats_data:
            csv_url = await _find_latest_csv_url(_MERCATS_DATASET_ID)
            if csv_url:
                csv_text = await _download_resource(csv_url)
                if csv_text:
                    mercats_data = _parse_mercats_csv(csv_text)

        if not mercats_data:
            logger.warning("No se encontraron datos de mercats municipals")
            return 0

        async with get_db() as conn:
            # Insertar mercados
            for m in mercats_data:
                await conn.execute("""
                    INSERT INTO mercados_municipales (id, nombre, direccion, distrito,
                                                      barrio, geometria, fuente)
                    VALUES ($1, $2, $3, $4, $5,
                            ST_SetSRID(ST_MakePoint($6, $7), 4326),
                            'opendata_bcn')
                    ON CONFLICT (id) DO UPDATE SET
                        nombre    = EXCLUDED.nombre,
                        direccion = EXCLUDED.direccion,
                        distrito  = EXCLUDED.distrito,
                        barrio    = EXCLUDED.barrio,
                        geometria = EXCLUDED.geometria,
                        updated_at = NOW()
                """, m["id"], m["nombre"], m["direccion"],
                     m["distrito"], m["barrio"], m["lng"], m["lat"])

            logger.info("Mercats municipals insertados: %d", len(mercats_data))

            # Calcular mercados_municipales_1km por zona
            hoy = date.today()
            zonas = await conn.fetch("""
                SELECT z.id AS zona_id,
                       (SELECT COUNT(*)::int
                        FROM mercados_municipales mm
                        WHERE ST_DWithin(mm.geometria::geography,
                                         ST_Centroid(z.geometria)::geography, 1000)
                       ) AS mercados_1km
                FROM zonas z
            """)

            for z in zonas:
                await conn.execute("""
                    INSERT INTO variables_zona (zona_id, fecha, fuente)
                    VALUES ($1, $2, 'entorno_mercats')
                    ON CONFLICT (zona_id, fecha) DO UPDATE
                    SET fuente = EXCLUDED.fuente, updated_at = NOW()
                """, z["zona_id"], hoy)

                await conn.execute("""
                    INSERT INTO vz_entorno (zona_id, fecha, mercados_municipales_1km, fuente)
                    VALUES ($1, $2, $3, 'entorno_mercats')
                    ON CONFLICT (zona_id, fecha) DO UPDATE SET
                        mercados_municipales_1km = EXCLUDED.mercados_municipales_1km,
                        fuente                   = EXCLUDED.fuente,
                        updated_at               = NOW()
                """, z["zona_id"], hoy, z["mercados_1km"])
                ok += 1

        logger.info("Mercats municipals: %d zonas actualizadas", ok)

    except Exception as e:
        logger.warning("_poblar_mercats_municipals error: %s", e)
    return ok


async def _fetch_mercats_json() -> list[dict]:
    """Intenta obtener mercats via datastore_search (JSON directo)."""
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(30.0), headers=_CKAN_HEADERS,
            follow_redirects=True
        ) as client:
            # Primero obtener resource_id
            r = await client.get(
                f"{_CKAN}/package_show", params={"id": _MERCATS_DATASET_ID}
            )
            r.raise_for_status()
            resources = r.json().get("result", {}).get("resources", [])

            # Buscar recurso JSON o datastore
            for res in resources:
                fmt = (res.get("format") or "").upper()
                if fmt in ("JSON", "GEOJSON") or res.get("datastore_active"):
                    ds_r = await client.get(
                        f"{_CKAN}/datastore_search",
                        params={"resource_id": res["id"], "limit": 200}
                    )
                    ds_r.raise_for_status()
                    records = ds_r.json().get("result", {}).get("records", [])
                    return _parse_mercats_records(records)
    except Exception as e:
        logger.debug("_fetch_mercats_json fallback: %s", e)
    return []


def _parse_mercats_records(records: list[dict]) -> list[dict]:
    """Parsea registros de datastore_search de mercats."""
    result = []
    for rec in records:
        lat = _try_float(
            rec.get("geo_epgs_4326_lat") or rec.get("Latitud") or
            rec.get("latitud") or rec.get("LATITUD")
        )
        lng = _try_float(
            rec.get("geo_epgs_4326_lon") or rec.get("Longitud") or
            rec.get("longitud") or rec.get("LONGITUD")
        )
        if lat is None or lng is None:
            continue

        nombre = (
            rec.get("Nom") or rec.get("nom") or
            rec.get("Equipament") or rec.get("equipament") or
            rec.get("Nom_Mercat") or rec.get("nom_mercat") or ""
        ).strip()
        if not nombre:
            continue

        result.append({
            "id": f"mercat_{hash(nombre) % 100000:05d}",
            "nombre": nombre,
            "direccion": (
                rec.get("Adreca") or rec.get("adreca") or
                rec.get("Direccio") or rec.get("direccio") or ""
            ).strip(),
            "distrito": (
                rec.get("Nom_Districte") or rec.get("Districte") or
                rec.get("districte") or ""
            ).strip(),
            "barrio": (
                rec.get("Nom_Barri") or rec.get("Barri") or
                rec.get("barri") or ""
            ).strip(),
            "lat": lat,
            "lng": lng,
        })
    return result


def _parse_mercats_csv(csv_text: str) -> list[dict]:
    """Parsea CSV de mercats-municipals."""
    reader = csv.DictReader(io.StringIO(csv_text))
    return _parse_mercats_records(list(reader))


# ── 2. Mapa Estratègic de Soroll (CKAN) ─────────────────────────────────────

async def _poblar_ruido() -> int:
    """
    Descarga el mapa de soroll de Open Data BCN y calcula
    nivel_ruido_db medio por zona.

    Usa el indicador Lden (nivel día-tarde-noche) que es el estándar EU
    para mapas de ruido estratégicos.
    """
    ok = 0
    try:
        csv_url = await _find_latest_csv_url(_SOROLL_DATASET_ID)
        if not csv_url:
            logger.warning("No se encontró CSV de mapa de soroll")
            return 0

        csv_text = await _download_resource(csv_url)
        if not csv_text:
            return 0

        # Parsear CSV de ruido: agregar por barrio/distrito
        ruido_by_barri = _parse_soroll_csv(csv_text)
        if not ruido_by_barri:
            logger.warning("CSV de soroll vacío o sin datos parseables")
            return 0

        hoy = date.today()
        async with get_db() as conn:
            # Mapear barrio → zonas
            zonas_barri = await conn.fetch("""
                SELECT z.id AS zona_id, b.nombre AS barrio_nombre
                FROM zonas z
                JOIN barrios b ON b.id = z.barrio_id
            """)

            barri_to_zonas: dict[str, list[str]] = defaultdict(list)
            for r in zonas_barri:
                barri_to_zonas[_normalize_name(r["barrio_nombre"])].append(r["zona_id"])

            for barri_key, ruido_db in ruido_by_barri.items():
                zona_ids = barri_to_zonas.get(barri_key, [])
                if not zona_ids:
                    continue

                for zona_id in zona_ids:
                    await conn.execute("""
                        INSERT INTO variables_zona (zona_id, fecha, fuente)
                        VALUES ($1, $2, 'entorno_soroll')
                        ON CONFLICT (zona_id, fecha) DO UPDATE
                        SET fuente = EXCLUDED.fuente, updated_at = NOW()
                    """, zona_id, hoy)

                    await conn.execute("""
                        INSERT INTO vz_entorno (zona_id, fecha, nivel_ruido_db, fuente)
                        VALUES ($1, $2, $3, 'entorno_soroll')
                        ON CONFLICT (zona_id, fecha) DO UPDATE SET
                            nivel_ruido_db = EXCLUDED.nivel_ruido_db,
                            fuente         = EXCLUDED.fuente,
                            updated_at     = NOW()
                    """, zona_id, hoy, round(ruido_db, 1))
                    ok += 1

        logger.info("Mapa soroll: %d zonas actualizadas", ok)

    except Exception as e:
        logger.warning("_poblar_ruido error: %s", e)
    return ok


def _parse_soroll_csv(csv_text: str) -> dict[str, float]:
    """
    Parsea CSV del mapa estratègic de soroll y agrega por barrio.

    Busca el indicador Lden (nivel día-tarde-noche en dB).
    Devuelve dict[barri_key] → media Lden en dB.
    """
    totals: dict[str, list[float]] = defaultdict(list)

    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        # Buscar barrio
        barri_raw = (
            row.get("Nom_Barri") or row.get("Barri") or
            row.get("nom_barri") or row.get("barri") or ""
        )
        barri_key = _normalize_name(barri_raw)
        if not barri_key:
            continue

        # Buscar valor Lden (indicador estándar EU)
        lden = _try_float(
            row.get("Lden") or row.get("lden") or
            row.get("LDEN") or row.get("Valor_Lden") or
            row.get("DB_Lden") or row.get("db_lden")
        )
        if lden is None:
            # Fallback: Ld (nivel día) o cualquier indicador de dB
            lden = _try_float(
                row.get("Ld") or row.get("ld") or
                row.get("Valor") or row.get("valor") or
                row.get("DB") or row.get("db")
            )
        if lden is not None and 30 <= lden <= 100:
            totals[barri_key].append(lden)

    # Media por barrio
    return {
        barri: round(sum(vals) / len(vals), 1)
        for barri, vals in totals.items()
        if vals
    }


# ── 3. Equipamientos y servicios (CKAN) ─────────────────────────────────────

async def _poblar_equipamientos() -> int:
    """
    Descarga equipamientos y servicios de Open Data BCN
    y calcula score_equipamientos por zona basado en densidad y diversidad.
    """
    ok = 0
    try:
        csv_url = await _find_latest_csv_url(_EQUIPAMENTS_DATASET_ID)
        if not csv_url:
            logger.warning("No se encontró CSV de equipaments")
            return 0

        csv_text = await _download_resource(csv_url)
        if not csv_text:
            return 0

        equip_by_barri = _parse_equipaments_csv(csv_text)
        if not equip_by_barri:
            logger.warning("CSV de equipaments vacío o sin datos parseables")
            return 0

        hoy = date.today()
        async with get_db() as conn:
            zonas_barri = await conn.fetch("""
                SELECT z.id AS zona_id, b.nombre AS barrio_nombre
                FROM zonas z
                JOIN barrios b ON b.id = z.barrio_id
            """)

            barri_to_zonas: dict[str, list[str]] = defaultdict(list)
            for r in zonas_barri:
                barri_to_zonas[_normalize_name(r["barrio_nombre"])].append(r["zona_id"])

            for barri_key, score in equip_by_barri.items():
                zona_ids = barri_to_zonas.get(barri_key, [])
                if not zona_ids:
                    continue

                for zona_id in zona_ids:
                    await conn.execute("""
                        INSERT INTO variables_zona (zona_id, fecha, fuente)
                        VALUES ($1, $2, 'entorno_equipaments')
                        ON CONFLICT (zona_id, fecha) DO UPDATE
                        SET fuente = EXCLUDED.fuente, updated_at = NOW()
                    """, zona_id, hoy)

                    await conn.execute("""
                        INSERT INTO vz_entorno (zona_id, fecha, score_equipamientos, fuente)
                        VALUES ($1, $2, $3, 'entorno_equipaments')
                        ON CONFLICT (zona_id, fecha) DO UPDATE SET
                            score_equipamientos = EXCLUDED.score_equipamientos,
                            fuente              = EXCLUDED.fuente,
                            updated_at          = NOW()
                    """, zona_id, hoy, round(score, 1))
                    ok += 1

        logger.info("Equipaments: %d zonas actualizadas", ok)

    except Exception as e:
        logger.warning("_poblar_equipamientos error: %s", e)
    return ok


def _parse_equipaments_csv(csv_text: str) -> dict[str, float]:
    """
    Parsea CSV de equipamientos y calcula score por barrio.

    Score = min(100, (num_equipamientos / max_equipamientos_barrio) * 70 +
                     (num_tipos_distintos / max_tipos) * 30)

    Combina densidad (70%) + diversidad de tipos (30%).
    """
    import math

    counts: dict[str, set[str]] = defaultdict(set)     # barri → tipos únicos
    totals: dict[str, int] = defaultdict(int)           # barri → conteo total

    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        barri_raw = (
            row.get("Nom_Barri") or row.get("Barri") or
            row.get("nom_barri") or row.get("barri") or ""
        )
        barri_key = _normalize_name(barri_raw)
        if not barri_key:
            continue

        tipo = (
            row.get("Tipus") or row.get("tipus") or
            row.get("Tipus_Equipament") or row.get("Sector") or
            row.get("sector") or "generico"
        ).strip().lower()

        totals[barri_key] += 1
        counts[barri_key].add(tipo)

    if not totals:
        return {}

    max_count = max(totals.values())
    max_tipos = max(len(s) for s in counts.values())

    scores = {}
    for barri in totals:
        density_score = (totals[barri] / max(max_count, 1)) * 70.0
        diversity_score = (len(counts[barri]) / max(max_tipos, 1)) * 30.0
        scores[barri] = min(100.0, density_score + diversity_score)

    return scores


# ── Helpers comunes ──────────────────────────────────────────────────────────

def _normalize_name(nombre: str) -> str:
    """Normaliza nombre de barrio/distrito para matching."""
    import unicodedata
    if not nombre:
        return ""
    s = nombre.strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s


def _try_float(val) -> Optional[float]:
    """Intenta convertir a float, devuelve None si falla."""
    if val is None:
        return None
    try:
        return float(str(val).replace(",", ".").strip())
    except (ValueError, TypeError):
        return None


async def _find_latest_csv_url(dataset_id: str) -> Optional[str]:
    """Busca la URL del CSV más reciente en un dataset CKAN de Open Data BCN."""
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(30.0), headers=_CKAN_HEADERS,
            follow_redirects=True
        ) as client:
            r = await client.get(
                f"{_CKAN}/package_show", params={"id": dataset_id}
            )
            r.raise_for_status()
            pkg = r.json()

        resources = pkg.get("result", {}).get("resources", [])
        csv_resources = [
            res for res in resources
            if (res.get("format", "").upper() == "CSV" or
                res.get("url", "").lower().endswith(".csv"))
        ]
        if not csv_resources:
            return None

        csv_resources.sort(
            key=lambda r: r.get("name", "") or r.get("url", ""),
            reverse=True,
        )
        return csv_resources[0].get("url")

    except Exception as e:
        logger.warning("_find_latest_csv_url(%s) error: %s", dataset_id, e)
        return None


async def _download_resource(url: str) -> Optional[str]:
    """Descarga un recurso desde una URL con retry."""
    last_err = None
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(60.0), headers=_CKAN_HEADERS,
        follow_redirects=True
    ) as client:
        for attempt in range(_HTTP_RETRIES):
            try:
                r = await client.get(url)
                r.raise_for_status()
                return r.text
            except Exception as e:
                last_err = e
                if attempt < _HTTP_RETRIES - 1:
                    await asyncio.sleep(_HTTP_RETRY_DELAY)
    logger.warning("_download_resource(%s) falló tras %d intentos: %s",
                    url, _HTTP_RETRIES, last_err)
    return None


# ── Pipeline tracking ─────────────────────────────────────────────────────────

async def _init():
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones(pipeline,estado) "
            "VALUES('entorno_comercial','running') RETURNING id"
        )


async def _fin(eid, reg, estado, msg=None):
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones "
            "SET fecha_fin=NOW(),registros=$1,estado=$2,mensaje_error=$3 WHERE id=$4",
            reg, estado, msg, eid,
        )
