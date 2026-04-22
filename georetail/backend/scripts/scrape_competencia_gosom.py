"""
scripts/scrape_competencia_gosom.py — Puente entre el contenedor gosom (HTTP
modo -web) y el pipeline competencia.

Motivo: `pipelines.comercio.competencia_scrape.ejecutar()` invoca a
`gosom_runner.ejecutar()` que asume un binario Go en subprocess. Aquí
tenemos el servicio Dockerizado en `gosom-scraper:8080`. Este script
sustituye el paso de scraping por llamadas HTTP al `GosomClient` pero
reutiliza `_upsert_negocio`, `_recalcular_competencia` y
`_precalentar_cache` del pipeline para no duplicar la lógica de ingesta.

Uso (dentro del contenedor backend):

    python -m scripts.scrape_competencia_gosom --max-zonas 10
    python -m scripts.scrape_competencia_gosom           # todas las zonas
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import re
import sys
from typing import Optional

from db.conexion import close_db_pool, get_db, init_db_pool
from pipelines.comercio.competencia_scrape import (
    _cargar_zonas,
    _precalentar_cache,
    _recalcular_competencia,
    _registrar_fin,
    _registrar_inicio,
    _upsert_negocio,
)
from pipelines.inmobiliario.scraping.gosom_client import (
    GosomClient,
    GosomError,
    GosomEntry,
)

logger = logging.getLogger("scrape_gosom")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# Un término por sector es suficiente para gosom — la página de resultados
# mezcla cafeterías, bares y restaurantes bajo la búsqueda de "restaurante".
_TERMINO_POR_SECTOR: dict[str, str] = {
    "restauracion":  "restaurante",
    "moda":          "tienda de ropa",
    "estetica":      "peluquería",
    "tatuajes":      "estudio de tatuajes",
    "shisha_lounge": "shisha",
}

_SECTORES_DEFAULT = list(_TERMINO_POR_SECTOR.keys())


def _price_range_to_level(pr: str) -> Optional[int]:
    """Convierte strings tipo "€€" o "$$" que devuelve gosom a 1-4."""
    if not pr:
        return None
    pr = pr.strip()
    count = pr.count("€") or pr.count("$")
    if count == 0:
        return None
    return max(1, min(4, count))


def _gosom_to_upsert_dict(e: GosomEntry, sector_fallback: str) -> Optional[dict]:
    """Transforma un `GosomEntry` a un dict listo para `_upsert_negocio`.

    Retorna ``None`` cuando el entry está en estado cerrado irrecuperable o
    le faltan coordenadas — no queremos contaminar `negocios_activos`.
    """
    if not e.title or not e.latitude or not e.longitude:
        return None

    # Clasificación fina (sector, subsector) desde la taxonomía canónica.
    sector, subsector = e.clasificar()
    sector_final = sector or sector_fallback

    # `id` estable: usamos el place_id de Google si está disponible; si no,
    # un hash del (nombre, lat, lng) para que el upsert no duplique.
    if e.place_id:
        neg_id = f"gs_{e.place_id}"
    else:
        key = re.sub(r"\s+", "_", (e.title + f"_{e.latitude:.5f}_{e.longitude:.5f}").lower())
        neg_id = f"gs_{key}"[:40]

    # `status` gosom — si viene algo tipo "Permanently closed" lo marcamos.
    status_lc = (e.status or "").lower()
    cerrado = any(kw in status_lc for kw in ("permanently closed", "temporarily closed", "closed"))

    return {
        "id":               neg_id,
        "nombre":           e.title[:200],
        "sector_codigo":    sector_final,
        "subsector_codigo": subsector,
        "lat":              e.latitude,
        "lng":              e.longitude,
        "rating":           e.review_rating or None,
        "num_resenas":      e.review_count or None,
        "precio_nivel":     _price_range_to_level(e.price_range),
        "horario":          e.open_hours if e.open_hours else None,
        "_cerrado":         cerrado,
        "fuente":           "google_scrape",
    }


async def _scrape_zona_sector(
    client: GosomClient,
    zona: dict,
    sector: str,
) -> list[GosomEntry]:
    """Lanza una query gosom para (zona, sector). Devuelve [] si falla."""
    termino = _TERMINO_POR_SECTOR.get(sector, sector)
    nombre = zona.get("nombre_barrio") or zona.get("zona_nombre") or "Barcelona"
    try:
        entries = await client.search_zona(
            zona_nombre=nombre,
            categoria=termino,
            lat=zona["lat"],
            lng=zona["lng"],
            zoom=17,
            depth=1,
        )
        logger.info(
            "zona=%s sector=%s barrio=%s → %d resultados",
            zona["zona_id"], sector, nombre, len(entries),
        )
        return entries
    except GosomError as e:
        logger.warning("zona=%s sector=%s: GosomError %s", zona["zona_id"], sector, e)
        return []
    except Exception as e:  # pragma: no cover — red/timeout inesperados
        logger.warning("zona=%s sector=%s: error %s", zona["zona_id"], sector, e)
        return []


async def _cargar_zonas_con_latlng(max_zonas: Optional[int]) -> list[dict]:
    """Igual que `_cargar_zonas` del pipeline pero añade lat/lng del centroide
    para que gosom tenga dónde geo-anclar la búsqueda."""
    base = await _cargar_zonas(max_zonas=max_zonas)
    if not base:
        return []

    ids = [z["zona_id"] for z in base]
    async with get_db() as conn:
        rows = await conn.fetch(
            """
            SELECT id AS zona_id,
                   ST_Y(ST_Centroid(geometria))::float AS lat,
                   ST_X(ST_Centroid(geometria))::float AS lng
            FROM zonas
            WHERE id = ANY($1::varchar[])
            """,
            ids,
        )
    coords = {r["zona_id"]: (r["lat"], r["lng"]) for r in rows}

    zonas: list[dict] = []
    for z in base:
        lat_lng = coords.get(z["zona_id"])
        if not lat_lng:
            continue
        lat, lng = lat_lng
        zonas.append({**z, "lat": lat, "lng": lng})
    return zonas


async def ejecutar(
    sectores: Optional[list[str]] = None,
    max_zonas: Optional[int] = None,
) -> dict:
    sectores = sectores or _SECTORES_DEFAULT

    eid = await _registrar_inicio()
    stats = {
        "zonas": 0, "sectores": len(sectores),
        "entries_crudos": 0, "upsertadas": 0,
        "cobertura_alta": 0, "cobertura_ninguna": 0,
    }

    try:
        zonas = await _cargar_zonas_con_latlng(max_zonas=max_zonas)
        stats["zonas"] = len(zonas)
        if not zonas:
            logger.warning("No hay zonas con datos recientes — nada que scrapear")
            await _registrar_fin(eid, 0, "ok")
            return stats

        logger.info(
            "Scrape gosom: %d zonas × %d sectores = %d queries",
            len(zonas), len(sectores), len(zonas) * len(sectores),
        )

        async with GosomClient() as client:
            async with get_db() as conn:
                for i, zona in enumerate(zonas, start=1):
                    for sector in sectores:
                        entries = await _scrape_zona_sector(client, zona, sector)
                        stats["entries_crudos"] += len(entries)

                        for e in entries:
                            n = _gosom_to_upsert_dict(e, sector_fallback=sector)
                            if n is None:
                                continue
                            try:
                                await _upsert_negocio(conn, n)
                                stats["upsertadas"] += 1
                            except Exception as ex:
                                logger.warning("upsert falló id=%s: %s", n.get("id"), ex)
                    logger.info("Progreso: %d/%d zonas procesadas", i, len(zonas))

        # Recalcular agregados + cache Redis.
        # NOTA: _recalcular_competencia tiene un bug SQL preexistente
        # ("zona_id ambiguous"). El panel de competencia funciona sin los
        # agregados porque `db/zonas.get_competencia_zona` calcula al vuelo
        # desde `negocios_activos` cuando no hay fila pre-calculada.
        try:
            cobertura = await _recalcular_competencia(sectores)
            stats["cobertura_alta"]    = cobertura.get("alta", 0)
            stats["cobertura_ninguna"] = cobertura.get("ninguna", 0)
            await _precalentar_cache(sectores)
        except Exception as exc:
            logger.warning(
                "Recálculo/cache saltado por error en pipeline: %s "
                "(el panel sigue funcionando — se calcula al vuelo)", exc,
            )

        await _registrar_fin(eid, stats["upsertadas"], "ok")
        logger.info("DONE stats=%s", stats)
        return stats

    except Exception as exc:
        logger.exception("Fallo crítico: %s", exc)
        await _registrar_fin(eid, stats["upsertadas"], "error", str(exc))
        raise


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--max-zonas", type=int, default=None,
                   help="Limita a N zonas para smoke-test. Por defecto todas.")
    p.add_argument("--sectores", nargs="+", default=None,
                   help="Sectores a scrapear (por defecto los 5 estándar).")
    return p.parse_args()


async def _main_async(args: argparse.Namespace) -> int:
    await init_db_pool()
    try:
        await ejecutar(sectores=args.sectores, max_zonas=args.max_zonas)
    finally:
        await close_db_pool()
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(_main_async(_parse_args())))
