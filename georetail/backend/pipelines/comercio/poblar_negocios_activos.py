"""
pipelines/comercio/poblar_negocios_activos.py — Población base de negocios_activos.

Fuentes (en orden de ejecución):
  1. Cens Comercial BCN CSVs (2014-2024) → negocios_activos (~60k negocios por año)
     Se procesan de más antiguo a más nuevo; el más reciente prevalece (ON CONFLICT UPDATE).
  2. OSM Overpass API             → negocios_activos (negocios actuales de OpenStreetMap)
  3. gosom scraper (Google Maps)  → negocios_activos (todas las zonas × 10 sectores)
     Fallback gracioso: si gosom no está disponible, se omite sin bloquear el pipeline.

Uso:
  - Mensual día 1, 00:30: garantiza base mínima antes de que competencia y
    google_maps calculen scores.
  - Manual vía POST /api/admin/pipelines/poblar_negocios (primera puesta en marcha).

Relación con otros pipelines:
  - competencia.py  usa gosom para enriquecer con datos frescos de Google Maps
  - google_maps.py  enriquece negocios_activos con ratings/popular_times
  - overpass.py     se delega directamente desde aquí (fuente 2)
"""
from __future__ import annotations

import asyncio
import csv
import json as _json
import logging
import os
from pathlib import Path
from typing import Optional

from db.conexion import get_db

logger = logging.getLogger(__name__)

# ── Configuración ──────────────────────────────────────────────────────────────

_CSV_DIR    = Path(os.environ.get("CSV_DATA_DIR", "/data/csv/_cleaned/cens_comercial"))
_BATCH_SIZE = 300
_PAUSA_GOSOM = float(os.environ.get("GOSOM_PAUSA_POBLACION", "0.5"))

# Categorías gosom por sector — primer elemento es la query principal
_SECTORES_GOSOM: dict[str, list[str]] = {
    "restauracion":  ["restaurante", "bar", "cafetería"],
    "moda":          ["tienda de ropa", "boutique", "zapatería"],
    "estetica":      ["peluquería", "salón de belleza", "barbería"],
    "tatuajes":      ["estudio de tatuajes", "piercing"],
    "shisha_lounge": ["shisha lounge", "hookah bar"],
    "alimentacion":  ["supermercado", "frutería", "panadería"],
    "salud":         ["farmacia", "clínica", "fisioterapia"],
    "servicios":     ["tienda de electrónica", "telefonía móvil", "lavandería"],
    "educacion":     ["librería", "academia", "papelería"],
    "deporte":       ["gimnasio", "tienda deportiva", "artes marciales"],
}

_VALID_SECTORES_GOSOM = set(_SECTORES_GOSOM)

# N_GRUPACT → sector_codigo. NULL = sector desconocido (permitido por esquema BD)
_SECTOR_MAP: dict[str, Optional[str]] = {
    "Restaurants, bars i hotels (Inclòs hostals, pensions i fondes)": "restauracion",
    "Restaurants, bars i hotels":                                      "restauracion",
    "Equipament personal":                                             "moda",
    "Quotidià alimentari":                                             None,
    "Quotidià no alimentari":                                          None,
    "Sanitat i assistència":                                           None,
    "Parament de la llar":                                             None,
    "Oci i cultura":                                                   None,
    "Equipaments culturals i recreatius":                              None,
    "Ensenyament":                                                     None,
    "Manteniment, neteja i producció":                                 None,
    "Reparacions (Electrodomèstics i automòbils)":                    None,
    "Altres":                                                          None,
}


# ── Punto de entrada ───────────────────────────────────────────────────────────

async def ejecutar() -> dict:
    """
    Orquesta la población de negocios_activos desde todas las fuentes base.
    Seguro de re-ejecutar — ON CONFLICT DO UPDATE en todos los casos.
    """
    eid = await _init()
    total = 0
    resumen: dict = {}

    try:
        n_cens = await _cargar_cens_comercial()
        resumen["cens_comercial"] = n_cens
        total += n_cens

        n_osm = await _cargar_overpass()
        resumen["overpass_osm"] = n_osm
        total += n_osm

        n_gosom = await _cargar_gosom_todos_sectores()
        resumen["gosom"] = n_gosom
        total += n_gosom

        await _fin(eid, total, "ok")
        logger.info(
            "poblar_negocios_activos OK — cens=%d osm=%d gosom=%d total=%d",
            n_cens, n_osm, n_gosom, total,
        )
        return resumen

    except Exception as exc:
        logger.error("Pipeline poblar_negocios_activos ERROR: %s", exc, exc_info=True)
        await _fin(eid, total, "error", str(exc))
        raise


# ── Fuente 1: Cens Comercial BCN (todos los años) ─────────────────────────────

async def _cargar_cens_comercial() -> int:
    """
    Carga todos los CSVs del Cens Comercial BCN disponibles, de más antiguo a
    más nuevo, de modo que el año más reciente prevalece via ON CONFLICT UPDATE.
    Cada CSV puede tener estructura distinta — las filas sin lat/lng se omiten.
    """
    csvs = sorted(_CSV_DIR.glob("*_cens_comercial.csv"))
    if not csvs:
        logger.warning("Sin CSVs de Cens Comercial en %s — omitiendo", _CSV_DIR)
        return 0

    sectores_validos = await _get_sectores_validos()
    total = 0

    async with get_db() as conn:
        for csv_path in csvs:
            any_year = csv_path.stem.split("_")[0]
            filas = _leer_csv_activos(csv_path, sectores_validos)
            if not filas:
                logger.info("Cens %s: sin filas válidas", any_year)
                continue
            n = 0
            for i in range(0, len(filas), _BATCH_SIZE):
                n += await _upsert_batch(conn, filas[i : i + _BATCH_SIZE])
            logger.info("Cens %s: %d negocios", any_year, n)
            total += n

        # Asignar zona_id a todos los registros sin zona (una sola query espacial)
        await conn.execute(
            """
            UPDATE negocios_activos na
            SET zona_id = z.id
            FROM zonas z
            WHERE ST_Within(na.geometria, z.geometria)
              AND na.fuente = 'cens_comercial_bcn'
              AND na.zona_id IS NULL
            """
        )

    logger.info("Cens Comercial BCN total: %d negocios insertados/actualizados", total)
    return total


def _leer_csv_activos(csv_path: Path, sectores_validos: set[str]) -> list[tuple]:
    """Lee un CSV del Cens Comercial y devuelve tuplas (id, nombre, sector, lat, lng).

    Maneja las distintas convenciones de columnas por año:
      2016: ID_BCN, N_LOCAL, N_GRUPACT, LATITUD, LONGITUD, N_PRINCIP
      2019+: ID_Bcn_2016, Nom_Local, Nom_Grup_Activitat, Latitud, Longitud (sin N_PRINCIP)
      2014: solo UTM ED50 (X/Y), sin lat/lng → se salta automáticamente
    Los años 2019+ usan ID_Bcn_2016 como clave, actualizando los registros del 2016.
    """
    filas: list[tuple] = []
    try:
        with open(csv_path, newline="", encoding="utf-8-sig", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Filtro N_PRINCIP solo si existe (2016)
                n_princip = (row.get("N_PRINCIP") or "").strip()
                if n_princip and n_princip != "Actiu":
                    continue

                # Coordenadas: LATITUD/LONGITUD (2016) o Latitud/Longitud (2019+)
                try:
                    lat = float(row.get("LATITUD") or row.get("Latitud") or 0)
                    lng = float(row.get("LONGITUD") or row.get("Longitud") or 0)
                except (ValueError, TypeError):
                    continue
                if not lat or not lng:
                    continue

                # ID: ID_BCN (2016) o ID_Bcn_2016 (2019+, mantiene misma clave)
                bcn_id = (
                    row.get("ID_BCN") or row.get("ID_Bcn_2016") or ""
                ).strip()
                if not bcn_id:
                    continue
                negocio_id = f"ccb_{bcn_id}"

                # Nombre: N_LOCAL (2016) o Nom_Local (2019+)
                nombre = (
                    row.get("N_LOCAL") or row.get("Nom_Local") or ""
                ).strip()[:300] or f"Negocio {negocio_id}"

                # Sector: N_GRUPACT (2016) o Nom_Grup_Activitat (2019+)
                grupo = (
                    row.get("N_GRUPACT") or row.get("Nom_Grup_Activitat") or ""
                ).strip()
                sector = _SECTOR_MAP.get(grupo)
                if sector and sector not in sectores_validos:
                    sector = None

                filas.append((negocio_id, nombre, sector, lat, lng))
    except Exception as exc:
        logger.error("Error leyendo %s: %s", csv_path.name, exc)

    return filas


async def _get_sectores_validos() -> set[str]:
    """Consulta los códigos de sector existentes en la BD."""
    try:
        async with get_db() as conn:
            rows = await conn.fetch("SELECT codigo FROM sectores")
        return {r["codigo"] for r in rows}
    except Exception:
        return set()


async def _upsert_batch(conn, batch: list[tuple]) -> int:
    """Upsert en negocios_activos para un batch de filas (sin zona_id — se asigna después)."""
    n = 0
    for negocio_id, nombre, sector, lat, lng in batch:
        try:
            await conn.execute(
                """
                INSERT INTO negocios_activos
                    (id, nombre, sector_codigo, lat, lng, geometria,
                     es_activo, fuente, updated_at)
                VALUES ($1,$2,$3,$4,$5,
                        ST_SetSRID(ST_MakePoint($5,$4),4326),
                        TRUE,'cens_comercial_bcn',NOW())
                ON CONFLICT (id) DO UPDATE SET
                    nombre        = COALESCE(EXCLUDED.nombre, negocios_activos.nombre),
                    sector_codigo = COALESCE(negocios_activos.sector_codigo, EXCLUDED.sector_codigo),
                    updated_at    = NOW()
                """,
                negocio_id, nombre, sector, lat, lng,
            )
            n += 1
        except Exception as exc:
            logger.debug("upsert ccb '%s': %s", negocio_id, exc)
    return n


# ── Fuente 2: OSM Overpass ─────────────────────────────────────────────────────

async def _cargar_overpass() -> int:
    """Llama al pipeline overpass.py (ya escribe en negocios_activos) y devuelve el conteo."""
    try:
        from pipelines.entorno.overpass import ejecutar as ejecutar_overpass
        resultado = await ejecutar_overpass()
        if isinstance(resultado, dict):
            return int(resultado.get("negocios", resultado.get("registros", 0)))
        return 0
    except Exception as exc:
        logger.error("Error ejecutando overpass: %s", exc)
        return 0


# ── Fuente 3: gosom (Google Maps scraper) ─────────────────────────────────────

async def _cargar_gosom_todos_sectores() -> int:
    """
    Fuente 3: gosom — scraping de negocios activos por zona × sector.

    Itera todas las zonas × todos los sectores de _SECTORES_GOSOM, solicita
    búsquedas al scraper Go y hace upsert en negocios_activos.
    Si gosom no está disponible, devuelve 0 sin bloquear el pipeline.
    """
    from pipelines.inmobiliario.scraping.gosom_client import (
        GosomClient,
        GosomServiceUnavailable,
    )

    gosom = GosomClient()
    if not gosom.is_available():
        logger.warning("gosom no disponible — omitiendo fuente 3 (gosom)")
        return 0

    zonas = await _get_zonas_para_poblacion()
    if not zonas:
        logger.warning("Sin zonas en BD — omitiendo fuente 3 (gosom)")
        return 0

    total = 0
    logger.info(
        "gosom todos sectores: %d zonas × %d sectores",
        len(zonas), len(_SECTORES_GOSOM),
    )

    async with gosom:
        for zona in zonas:
            for sector, categorias in _SECTORES_GOSOM.items():
                try:
                    entries = await gosom.search_zona(
                        zona_nombre=zona["zona_nombre"],
                        categoria=categorias[0],
                        lat=zona["lat"],
                        lng=zona["lng"],
                    )
                    if entries:
                        n = await _upsert_gosom_negocios(entries, zona["zona_id"], sector)
                        total += n
                except GosomServiceUnavailable:
                    logger.warning("gosom dejó de responder — deteniendo fuente 3")
                    return total
                except Exception as exc:
                    logger.debug(
                        "gosom zona=%s sector=%s: %s",
                        zona["zona_id"], sector, exc,
                    )
                await asyncio.sleep(_PAUSA_GOSOM)

    logger.info("gosom todos sectores: %d negocios insertados/actualizados", total)
    return total


async def _get_zonas_para_poblacion() -> list[dict]:
    """Devuelve todas las zonas con centroide para poblar negocios_activos."""
    async with get_db() as conn:
        rows = await conn.fetch("""
            SELECT
                z.id     AS zona_id,
                z.nombre AS zona_nombre,
                ST_Y(ST_Centroid(z.geometria)) AS lat,
                ST_X(ST_Centroid(z.geometria)) AS lng
            FROM zonas z
            ORDER BY z.id
        """)
    return [dict(r) for r in rows]


async def _upsert_gosom_negocios(entries: list, zona_id: str, sector: str) -> int:
    """Upsert de resultados gosom en negocios_activos."""
    from scoring.taxonomia import subsector_valido

    n = 0
    async with get_db() as conn:
        for entry in entries:
            if not entry.title or entry.latitude is None or entry.longitude is None:
                continue

            _, subsector_inf = entry.clasificar()
            if subsector_inf and not subsector_valido(sector, subsector_inf):
                subsector_inf = None

            place_id = entry.place_id or f"gosom_{zona_id}_{abs(hash(entry.title))}"
            pt_json  = _json.dumps(entry.popular_times) if entry.popular_times else None

            try:
                await conn.execute(
                    """
                    INSERT INTO negocios_activos
                        (id, nombre, sector_codigo, subsector_codigo,
                         lat, lng, geometria, zona_id,
                         rating, total_resenas, review_count,
                         google_place_id, popular_times,
                         es_activo, fuente, updated_at)
                    VALUES ($1,$2,$3,$4, $5,$6,
                            ST_SetSRID(ST_MakePoint($6,$5),4326),
                            $7, $8,$9,$9, $10,$11::jsonb,
                            TRUE,'gosom',NOW())
                    ON CONFLICT (id) DO UPDATE SET
                        subsector_codigo = COALESCE(EXCLUDED.subsector_codigo, negocios_activos.subsector_codigo),
                        rating           = COALESCE(EXCLUDED.rating,           negocios_activos.rating),
                        total_resenas    = COALESCE(EXCLUDED.total_resenas,    negocios_activos.total_resenas),
                        review_count     = COALESCE(EXCLUDED.review_count,     negocios_activos.review_count),
                        google_place_id  = COALESCE(EXCLUDED.google_place_id,  negocios_activos.google_place_id),
                        popular_times    = COALESCE(EXCLUDED.popular_times,    negocios_activos.popular_times),
                        updated_at       = NOW()
                    """,
                    place_id, entry.title,
                    sector if sector in _VALID_SECTORES_GOSOM else None,
                    subsector_inf,
                    float(entry.latitude), float(entry.longitude),
                    zona_id,
                    entry.review_rating,
                    entry.review_count,
                    entry.place_id or None,
                    pt_json,
                )
                n += 1
            except Exception as exc:
                logger.debug("upsert gosom '%s': %s", entry.title, exc)
    return n


# ── Control de ejecución ───────────────────────────────────────────────────────

async def _init() -> int:
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones(pipeline,estado) "
            "VALUES('poblar_negocios','running') RETURNING id"
        )


async def _fin(eid: int, registros: int, estado: str, msg: Optional[str] = None) -> None:
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones "
            "SET fecha_fin=NOW(), registros=$1, estado=$2, mensaje_error=$3 WHERE id=$4",
            registros, estado, msg, eid,
        )
