"""
pipelines/llicencies.py — Llicències d'activitat de Barcelona.

Fuente: Open Data BCN (CKAN API) — datos abiertos del Ajuntament.
Dataset: Llicències d'activitats atorgades

Qué hace:
  1. Descarga las llicències del último año via CKAN API
  2. Geocodifica las direcciones usando PostGIS (tabla geocoding_cache)
     o Nominatim como fallback
  3. Asigna zona_id via ST_Within
  4. Inserta en tabla llicencies_activitat
  5. Calcula por zona:
     - licencias_nuevas_1a: atorgades en últimos 12 meses
     - licencias_bajas_1a: retirades/caducades en últimos 12 meses
  6. Actualiza variables_zona

Frecuencia: mensual
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date, timedelta
from typing import Optional

import httpx

from db.conexion import get_db

logger = logging.getLogger(__name__)

# ── Configuración CKAN ─────────────────────────────────────────────────────────
_CKAN_BASE  = "https://opendata-ajuntament.barcelona.cat/data/api/action"
_CKAN_LIMIT = 1000   # registros por página CKAN
_TIMEOUT_S  = 30
_SLEEP_PAGE = 0.5    # pausa entre páginas (rate limit cortesía)

# Resource IDs de Llicències d'Activitats en Open Data BCN
# Atorgades (nuevas):
_RID_ATORGADES  = "79b3ca10-9f39-4e90-aacc-a6ee58c1d3d7"
# Retirades/caducades (bajas):
_RID_RETIRADES  = "c1b5e3ae-0b3d-4f28-a3d9-7af2e0c2b15e"

# Nominatim para fallback de geocodificación
_NOMINATIM_URL  = "https://nominatim.openstreetmap.org/search"
_NOMINATIM_HDRS = {
    "User-Agent": "GeoRetail/1.0 (georetail@example.com)",
    "Accept-Language": "ca,es",
}


# ═══════════════════════════════════════════════════════════════════════════════
# Punto de entrada
# ═══════════════════════════════════════════════════════════════════════════════

async def ejecutar() -> dict:
    eid = await _init("llicencies")
    try:
        fecha_desde = date.today() - timedelta(days=365)

        # 1. Descargar llicències atorgades y retirades
        logger.info("Descargando llicències atorgades desde CKAN...")
        atorgades = await _descargar_ckan(_RID_ATORGADES, fecha_desde, tipus="atorgada")
        logger.info("Llicències atorgades: %d", len(atorgades))

        logger.info("Descargando llicències retirades/caducades desde CKAN...")
        retirades = await _descargar_ckan(_RID_RETIRADES, fecha_desde, tipus="retirada")
        logger.info("Llicències retirades: %d", len(retirades))

        totes = atorgades + retirades
        if not totes:
            msg = "Cap llicència descarregada des del CKAN"
            logger.warning(msg)
            await _fin(eid, 0, "ok", msg)
            return {"insertades": 0, "zonas_actualizadas": 0}

        # 2. Geocodificar → zona_id (batch)
        totes = await _geocodificar_batch(totes)
        logger.info(
            "Geocodificades amb zona: %d / %d",
            sum(1 for r in totes if r.get("zona_id")),
            len(totes),
        )

        # 3. Insertar en tabla llicencies_activitat
        insertades = await _insertar_llicencies(totes)
        logger.info("Llicències insertades: %d", insertades)

        # 4. Calcular métricas por zona y actualizar variables_zona
        fecha_ref = date.today()
        zonas_act = await _actualizar_variables_zona(fecha_ref)
        logger.info("Variables zona actualizadas: %d zonas", zonas_act)

        await _fin(eid, insertades, "ok")
        logger.info("Llicències OK — %d insertades, %d zonas", insertades, zonas_act)
        return {"insertades": insertades, "zonas_actualizadas": zonas_act}

    except Exception as exc:
        logger.error("Pipeline llicencies ERROR: %s", exc, exc_info=True)
        await _fin(eid, 0, "error", str(exc))
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Descarga CKAN
# ═══════════════════════════════════════════════════════════════════════════════

async def _descargar_ckan(
    resource_id: str,
    fecha_desde: date,
    tipus: str,
) -> list[dict]:
    """
    Descarga registros del CKAN paginando por offset.
    Intenta filtrar por fecha >= fecha_desde (campo DATA_ATORGAMENT o DATA_RETIRADA).
    Si el filtro SQL falla, descarga todo y filtra en Python.
    """
    resultats: list[dict] = []
    offset = 0

    async with httpx.AsyncClient(timeout=_TIMEOUT_S, follow_redirects=True) as client:
        while True:
            params = {
                "resource_id": resource_id,
                "limit":       _CKAN_LIMIT,
                "offset":      offset,
            }
            try:
                r = await client.get(f"{_CKAN_BASE}/datastore_search", params=params)
            except httpx.TimeoutException:
                logger.warning("Timeout CKAN offset=%d — parant descàrrega", offset)
                break
            except Exception as exc:
                logger.warning("Error CKAN offset=%d: %s", offset, exc)
                break

            if r.status_code != 200:
                logger.warning("CKAN HTTP %d per resource %s", r.status_code, resource_id)
                break

            try:
                data = r.json()
            except Exception:
                logger.warning("CKAN resposta no JSON per resource %s", resource_id)
                break

            if not data.get("success"):
                logger.warning("CKAN success=false: %s", data.get("error"))
                break

            records = data.get("result", {}).get("records", [])
            if not records:
                break

            for rec in records:
                fila = _normalitzar_fila(rec, tipus, fecha_desde)
                if fila:
                    resultats.append(fila)

            total = data.get("result", {}).get("total", 0)
            offset += _CKAN_LIMIT
            if offset >= total:
                break

            await asyncio.sleep(_SLEEP_PAGE)

    return resultats


def _normalitzar_fila(rec: dict, tipus: str, fecha_desde: date) -> Optional[dict]:
    """
    Normalitza un registre CKAN a un dict intern.
    Retorna None si el registre és invàlid o anterior a fecha_desde.
    """
    # Intentar extreure adreça i coordenades (columnes varien segons dataset)
    adreca = (
        rec.get("ADRECA_COMPLETA")
        or rec.get("ADRECA")
        or rec.get("DOMICILI")
        or ""
    ).strip()

    # Alguns datasets inclouen coordenades directament
    try:
        lat = float(rec.get("LATITUD") or rec.get("LATITUD_Y") or 0)
        lng = float(rec.get("LONGITUD") or rec.get("LONGITUD_X") or 0)
    except (ValueError, TypeError):
        lat, lng = 0.0, 0.0

    # Data de la llicència
    data_str = (
        rec.get("DATA_ATORGAMENT")
        or rec.get("DATA_RETIRADA")
        or rec.get("DATA")
        or ""
    ).strip()
    data_lic: Optional[date] = None
    if data_str:
        try:
            data_lic = date.fromisoformat(data_str[:10])
        except ValueError:
            pass

    # Filtrar per antiguitat
    if data_lic and data_lic < fecha_desde:
        return None

    # Nom activitat i sector
    nom_act = (
        rec.get("NOM_ACTIVITAT")
        or rec.get("ACTIVITAT")
        or rec.get("TIPUS_ACTIVITAT")
        or "Activitat sense nom"
    ).strip()[:300]

    return {
        "ckan_id":    str(rec.get("_id", "") or "").strip(),
        "adreca":     adreca[:300],
        "lat":        lat if lat and lng else None,
        "lng":        lng if lat and lng else None,
        "data_lic":   data_lic,
        "nom_act":    nom_act,
        "tipus":      tipus,       # "atorgada" | "retirada"
        "zona_id":    None,        # s'omple en geocodificació
        "districte":  (rec.get("NOM_DISTRICTE") or "").strip()[:50],
        "barri":      (rec.get("NOM_BARRI") or "").strip()[:80],
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Geocodificació
# ═══════════════════════════════════════════════════════════════════════════════

async def _geocodificar_batch(llicencies: list[dict]) -> list[dict]:
    """
    Assigna zona_id a cada llicència:
    1. Si té coordenades → ST_Within directe (batch)
    2. Si no té coordenades → consulta geocoding_cache per adreça
    3. Fallback: Nominatim (per adreça + "Barcelona")
    """
    # Separar les que ja tenen coords
    amb_coords   = [l for l in llicencies if l.get("lat") and l.get("lng")]
    sense_coords = [l for l in llicencies if not (l.get("lat") and l.get("lng"))]

    # Assignar zona amb batch ST_Within per a les que tenen coords
    if amb_coords:
        await _assignar_zona_coords(amb_coords)

    # Geocodificar les que no tenen coords
    if sense_coords:
        await _geocodificar_adreces(sense_coords)

    return llicencies


async def _assignar_zona_coords(llicencies: list[dict]) -> None:
    """Batch ST_Within per a llicències amb coordenades."""
    _CHUNK = 500
    async with get_db() as conn:
        for i in range(0, len(llicencies), _CHUNK):
            chunk = llicencies[i : i + _CHUNK]
            values_sql = ", ".join(
                f"(${j*2+1}::float8, ${j*2+2}::float8, {j})"
                for j in range(len(chunk))
            )
            params: list = []
            for lic in chunk:
                params.extend([lic["lng"], lic["lat"]])

            try:
                rows = await conn.fetch(
                    f"""
                    SELECT pts.idx, z.id AS zona_id
                    FROM (VALUES {values_sql}) AS pts(lng, lat, idx)
                    JOIN zonas z ON ST_Within(
                        ST_SetSRID(ST_MakePoint(pts.lng, pts.lat), 4326),
                        z.geometria
                    )
                    """,
                    *params,
                )
                for row in rows:
                    chunk[row["idx"]]["zona_id"] = row["zona_id"]
            except Exception as exc:
                logger.warning("Error ST_Within batch llicències: %s", exc)


async def _geocodificar_adreces(llicencies: list[dict]) -> None:
    """
    Per a llicències sense coords, busca primer en geocoding_cache,
    després crida Nominatim com a fallback.
    """
    # Intentar geocoding_cache primer (batch)
    async with get_db() as conn:
        for lic in llicencies:
            adreca = lic.get("adreca", "").strip()
            if not adreca:
                continue
            try:
                row = await conn.fetchrow(
                    """
                    SELECT lat, lng, zona_id
                    FROM geocoding_cache
                    WHERE adreca_norm = lower(unaccent($1))
                    LIMIT 1
                    """,
                    adreca,
                )
                if row and row["zona_id"]:
                    lic["lat"]     = row["lat"]
                    lic["lng"]     = row["lng"]
                    lic["zona_id"] = row["zona_id"]
            except Exception:
                # Taula pot no existir o unaccent no disponible — continuar
                pass

    # Nominatim per als que segueixen sense zona
    pendents = [l for l in llicencies if not l.get("zona_id")]
    if pendents:
        logger.info("Geocodificant %d adreces via Nominatim", len(pendents))
        await _nominatim_geocode(pendents)


async def _nominatim_geocode(llicencies: list[dict]) -> None:
    """Geocodifica adreces usant Nominatim (màx 1 req/s per cortesia)."""
    async with httpx.AsyncClient(
        timeout=10, follow_redirects=True, headers=_NOMINATIM_HDRS
    ) as client:
        async with get_db() as conn:
            for lic in llicencies:
                adreca = lic.get("adreca", "").strip()
                if not adreca:
                    continue
                try:
                    r = await client.get(
                        _NOMINATIM_URL,
                        params={
                            "q":            f"{adreca}, Barcelona, Cataluña",
                            "format":       "json",
                            "limit":        1,
                            "countrycodes": "es",
                        },
                    )
                    if r.status_code == 200:
                        hits = r.json()
                        if hits:
                            lat = float(hits[0]["lat"])
                            lng = float(hits[0]["lon"])
                            lic["lat"] = lat
                            lic["lng"] = lng
                            # Buscar zona
                            row = await conn.fetchrow(
                                """
                                SELECT id AS zona_id FROM zonas
                                WHERE ST_Within(
                                    ST_SetSRID(ST_MakePoint($1, $2), 4326),
                                    geometria
                                )
                                LIMIT 1
                                """,
                                lng, lat,
                            )
                            if row:
                                lic["zona_id"] = row["zona_id"]
                except Exception as exc:
                    logger.debug("Nominatim error per %s: %s", adreca[:50], exc)
                await asyncio.sleep(1.05)  # respetar 1 req/s Nominatim


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Inserció en llicencies_activitat
# ═══════════════════════════════════════════════════════════════════════════════

async def _insertar_llicencies(llicencies: list[dict]) -> int:
    """
    Inserta en la tabla llicencies_activitat (creada si no existe).
    Hace UPSERT por ckan_id para ser idempotente.
    """
    amb_zona = [l for l in llicencies if l.get("zona_id")]
    if not amb_zona:
        return 0

    async with get_db() as conn:
        # Crear tabla si no existe
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS llicencies_activitat (
                id          SERIAL PRIMARY KEY,
                ckan_id     VARCHAR(50),
                zona_id     VARCHAR(20) REFERENCES zonas(id),
                adreca      TEXT,
                nom_act     TEXT,
                tipus       VARCHAR(20),  -- 'atorgada' | 'retirada'
                data_lic    DATE,
                districte   VARCHAR(50),
                barri        VARCHAR(80),
                lat         FLOAT,
                lng         FLOAT,
                created_at  TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (ckan_id, tipus)
            )
        """)
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_llicencies_zona_data "
            "ON llicencies_activitat(zona_id, data_lic DESC)"
        )

        n = 0
        for lic in amb_zona:
            try:
                await conn.execute(
                    """
                    INSERT INTO llicencies_activitat
                        (ckan_id, zona_id, adreca, nom_act, tipus,
                         data_lic, districte, barri, lat, lng)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                    ON CONFLICT (ckan_id, tipus) DO UPDATE
                    SET zona_id=$2, nom_act=$4, data_lic=$6, updated_at=NOW()
                    """,
                    lic["ckan_id"] or f"auto_{n}",
                    lic["zona_id"],
                    lic["adreca"],
                    lic["nom_act"],
                    lic["tipus"],
                    lic["data_lic"],
                    lic["districte"],
                    lic["barri"],
                    lic.get("lat"),
                    lic.get("lng"),
                )
                n += 1
            except Exception as exc:
                logger.debug("Insert llicència error: %s", exc)

        # Afegir columna updated_at si no existeix (per al DO UPDATE)
        try:
            await conn.execute(
                "ALTER TABLE llicencies_activitat ADD COLUMN IF NOT EXISTS "
                "updated_at TIMESTAMPTZ DEFAULT NOW()"
            )
        except Exception:
            pass

    return n


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Actualitzar variables_zona
# ═══════════════════════════════════════════════════════════════════════════════

async def _actualizar_variables_zona(fecha: date) -> int:
    """
    Calcula per zona:
      - licencias_nuevas_1a: atorgades en els últims 12 mesos
      - licencias_bajas_1a: retirades en els últims 12 mesos
    i fa UPSERT en variables_zona.

    Actualitza columnes extra si existeixen en el schema.
    """
    fecha_desde = fecha - timedelta(days=365)

    async with get_db() as conn:
        # Comptar per zona
        rows_noves = await conn.fetch(
            """
            SELECT zona_id, COUNT(*) AS cnt
            FROM llicencies_activitat
            WHERE tipus = 'atorgada' AND data_lic >= $1
            GROUP BY zona_id
            """,
            fecha_desde,
        )
        rows_baixes = await conn.fetch(
            """
            SELECT zona_id, COUNT(*) AS cnt
            FROM llicencies_activitat
            WHERE tipus = 'retirada' AND data_lic >= $1
            GROUP BY zona_id
            """,
            fecha_desde,
        )

    noves  = {r["zona_id"]: int(r["cnt"]) for r in rows_noves}
    baixes = {r["zona_id"]: int(r["cnt"]) for r in rows_baixes}
    totes_zones = set(noves) | set(baixes)

    if not totes_zones:
        return 0

    n = 0
    async with get_db() as conn:
        for zona_id in totes_zones:
            n_noves  = noves.get(zona_id, 0)
            n_baixes = baixes.get(zona_id, 0)

            # Insertar / actualitzar variables_zona
            await conn.execute(
                """
                INSERT INTO variables_zona (zona_id, fecha, fuente)
                VALUES ($1, $2, 'llicencies_bcn')
                ON CONFLICT (zona_id, fecha) DO UPDATE
                SET fuente = EXCLUDED.fuente
                """,
                zona_id, fecha,
            )

            # Columnes específiques de llicències (si existeixen)
            for col, val in [
                ("licencias_nuevas_1a", n_noves),
                ("licencias_bajas_1a",  n_baixes),
            ]:
                try:
                    await conn.execute(
                        f"UPDATE variables_zona SET {col} = $1 "
                        f"WHERE zona_id = $2 AND fecha = $3",
                        val, zona_id, fecha,
                    )
                except Exception:
                    pass  # columna no existeix en schema actual

            n += 1

    return n


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers BD
# ═══════════════════════════════════════════════════════════════════════════════

async def _init(pipeline: str) -> int:
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones (pipeline, estado) "
            "VALUES ($1,'running') RETURNING id",
            pipeline,
        )


async def _fin(
    eid: int, registros: int, estado: str, mensaje: Optional[str] = None
) -> None:
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones "
            "SET fecha_fin=NOW(), registros=$1, estado=$2, mensaje_error=$3 "
            "WHERE id=$4",
            registros, estado, mensaje, eid,
        )
