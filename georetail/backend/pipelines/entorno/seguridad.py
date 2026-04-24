"""
pipelines/seguridad.py — Pipeline mensual de datos de seguridad (v1).

Fuentes:
  - Accidents/Incidents Guardia Urbana BCN (CKAN Open Data BCN):
    incidencias_por_1000hab (total), hurtos_por_1000hab, robatoris_por_1000hab,
    danys_por_1000hab, incidencias_noche_pct
  - Comissaries de policia (CKAN Open Data BCN):
    comisarias_1km, dist_comisaria_m
  - IERMB seguretat al barri (API IERMB id_ind=1892):
    seguridad_barri_score (0-10, nivel ciudad)

Columnas CSV confirmadas (accidents-gu-bcn):
  Número d'expedient, Codi districte, Nom districte, NK barri, Nom barri,
  Codi carrer, Nom carrer, Num postal caption, Descripció dia setmana,
  Dia de setmana, Descripció tipus dia, NK Any, Mes de any, Nom mes,
  Dia de mes, Hora de dia, Descripció torn, Descripció causa vianant,
  Número de morts, Número de lesionats lleus, Número de lesionats greus,
  Número de víctimes, Número de vehicles implicats,
  Coordenada UTM (Y), Coordenada UTM (X)

Ejecuta mensual día 1, 07:30 (scheduler.py).
"""
from __future__ import annotations

import asyncio
import csv
import io
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
_ACCIDENTS_DATASET_ID = "accidents-gu-bcn"
_ACCIDENTS_TIPUS_DATASET_ID = "accidents-tipus-gu-bcn"
_COMISSARIES_DATASET_ID = "comissaries-policia"

# IERMB (misma config que en demografia.py — se mantiene aquí para independencia)
_IERMB_TOKEN = os.environ.get("IERMB_TOKEN", "AGEF894MGIE0220GOLLEOF")
_IERMB_BASE = "https://iermbdb.uab.cat/datasets2/index.php"
_IERMB_SEGURETAT_ID = "1892"

# Clasificación de incidencias por tipo.
# Las claves son substrings que se buscan en el campo 'Descripció causa vianant'
# o 'Descripció torn' del CSV de la Guardia Urbana.
_TIPO_HURTO = {"furt", "hurl", "sostracció", "carterisme", "descuit"}
_TIPO_ROBO = {"robatori", "atracament", "violència", "intimidació", "força"}
_TIPO_DANY = {"dany", "vandalisme", "destrosses", "incendi"}


async def ejecutar() -> dict:
    """Punto de entrada del pipeline. Llamado por scheduler o manualmente."""
    eid = await _init()
    ok = 0
    try:
        ok += await _poblar_incidencias_gu()
        ok += await _poblar_comisarias()
        ok += await _poblar_seguridad_iermb()
        await _fin(eid, ok, "ok")
        return {"registros": ok}
    except Exception as e:
        logger.error("Pipeline seguridad error: %s", e)
        await _fin(eid, ok, "error", str(e))
        raise


# ── 1. Incidencias Guardia Urbana (CKAN) ──────────────────────────────────────

async def _poblar_incidencias_gu() -> int:
    """
    Descarga el CSV más reciente de accidents-gu-bcn vía CKAN,
    clasifica incidencias por tipo y hora, agrega por barrio y
    escribe en vz_entorno.
    """
    ok = 0
    try:
        csv_url = await _find_latest_csv_url(_ACCIDENTS_DATASET_ID)
        if not csv_url:
            logger.warning("No se encontró CSV reciente en %s", _ACCIDENTS_DATASET_ID)
            return 0

        # Descargar CSV
        csv_text = await _download_csv(csv_url)
        if not csv_text:
            return 0

        # Parsear y agregar por barrio
        stats_by_barri = _parse_incidents_csv(csv_text)
        if not stats_by_barri:
            logger.warning("CSV de incidencias vacío o sin datos parseables")
            return 0

        # Obtener población por barrio para normalizar
        poblacion_barri = await _get_poblacion_por_barrio()

        # Escribir en BD
        hoy = date.today()
        async with get_db() as conn:
            # Obtener zonas agrupadas por barrio
            zonas_barri = await conn.fetch("""
                SELECT z.id AS zona_id, b.nombre AS barrio_nombre
                FROM zonas z
                JOIN barrios b ON b.id = z.barrio_id
            """)

            # Mapear barrio → lista de zona_ids
            barri_to_zonas: dict[str, list[str]] = defaultdict(list)
            for r in zonas_barri:
                barri_to_zonas[_normalize_barri(r["barrio_nombre"])].append(r["zona_id"])

            for barri_key, stats in stats_by_barri.items():
                zona_ids = barri_to_zonas.get(barri_key, [])
                if not zona_ids:
                    continue

                # Normalizar por población (por 1000 hab)
                pob = poblacion_barri.get(barri_key, 5000)  # fallback 5000
                total = stats["total"]
                hurtos = stats["hurtos"]
                robatoris = stats["robatoris"]
                danys = stats["danys"]
                noche = stats["noche"]

                factor = 1000.0 / max(pob, 1)
                inc_1000 = round(total * factor, 2)
                hurtos_1000 = round(hurtos * factor, 2)
                rob_1000 = round(robatoris * factor, 2)
                danys_1000 = round(danys * factor, 2)
                noche_pct = round(noche / max(total, 1), 3)

                for zona_id in zona_ids:
                    # Asegurar fila coordinadora en variables_zona
                    await conn.execute("""
                        INSERT INTO variables_zona (zona_id, fecha, fuente)
                        VALUES ($1, $2, 'seguridad_gu')
                        ON CONFLICT (zona_id, fecha) DO UPDATE
                        SET fuente = EXCLUDED.fuente, updated_at = NOW()
                    """, zona_id, hoy)

                    # Escribir en vz_entorno
                    await conn.execute("""
                        INSERT INTO vz_entorno (
                            zona_id, fecha, incidencias_por_1000hab,
                            hurtos_por_1000hab, robatoris_por_1000hab,
                            danys_por_1000hab, incidencias_noche_pct, fuente
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'seguridad_gu')
                        ON CONFLICT (zona_id, fecha) DO UPDATE SET
                            incidencias_por_1000hab = EXCLUDED.incidencias_por_1000hab,
                            hurtos_por_1000hab      = EXCLUDED.hurtos_por_1000hab,
                            robatoris_por_1000hab   = EXCLUDED.robatoris_por_1000hab,
                            danys_por_1000hab       = EXCLUDED.danys_por_1000hab,
                            incidencias_noche_pct   = EXCLUDED.incidencias_noche_pct,
                            fuente                  = EXCLUDED.fuente,
                            updated_at              = NOW()
                    """, zona_id, hoy, inc_1000, hurtos_1000, rob_1000,
                         danys_1000, noche_pct)
                    ok += 1

        logger.info("Incidencias GU: %d zonas actualizadas desde %s",
                     ok, _ACCIDENTS_DATASET_ID)

    except Exception as e:
        logger.warning("_poblar_incidencias_gu error: %s", e)
    return ok


def _parse_incidents_csv(csv_text: str) -> dict[str, dict]:
    """
    Parsea el CSV de accidents-gu-bcn y agrega por barrio.

    Devuelve dict[barri_key] → {total, hurtos, robatoris, danys, noche}
    """
    stats: dict[str, dict] = defaultdict(
        lambda: {"total": 0, "hurtos": 0, "robatoris": 0, "danys": 0, "noche": 0}
    )

    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        barri_raw = (
            row.get("Nom barri") or row.get("Nom_barri") or
            row.get("nom_barri") or ""
        )
        barri_key = _normalize_barri(barri_raw)
        if not barri_key:
            continue

        stats[barri_key]["total"] += 1

        # Clasificar tipo de incidencia
        causa = (
            row.get("Descripció causa vianant") or
            row.get("Descripcio_causa_vianant") or ""
        ).lower()

        if any(t in causa for t in _TIPO_HURTO):
            stats[barri_key]["hurtos"] += 1
        elif any(t in causa for t in _TIPO_ROBO):
            stats[barri_key]["robatoris"] += 1
        elif any(t in causa for t in _TIPO_DANY):
            stats[barri_key]["danys"] += 1

        # Nocturnidad: 20:00-06:00
        hora_raw = row.get("Hora de dia") or row.get("Hora_de_dia") or ""
        try:
            hora = int(str(hora_raw).strip().split(":")[0].split(".")[0])
            if hora >= 20 or hora < 6:
                stats[barri_key]["noche"] += 1
        except (ValueError, IndexError):
            pass

    return dict(stats)


def _normalize_barri(nombre: str) -> str:
    """Normaliza nombre de barrio para matching."""
    import unicodedata
    if not nombre:
        return ""
    s = nombre.strip().lower()
    # Eliminar acentos
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s


# ── 2. Comisarías de policía (CKAN) ──────────────────────────────────────────

async def _poblar_comisarias() -> int:
    """
    Descarga el dataset comissaries-policia de Open Data BCN,
    inserta en la tabla comisarias y calcula comisarias_1km + dist_comisaria_m
    por zona en vz_entorno.
    """
    ok = 0
    try:
        csv_url = await _find_latest_csv_url(_COMISSARIES_DATASET_ID)
        if not csv_url:
            logger.warning("No se encontró CSV de comissaries")
            return 0

        csv_text = await _download_csv(csv_url)
        if not csv_text:
            return 0

        # Parsear CSV de comisarías
        reader = csv.DictReader(io.StringIO(csv_text))
        comisarias_data = []
        for row in reader:
            # Intentar obtener coordenadas (lat/lng o UTM)
            lat = _try_float(row.get("Latitud") or row.get("latitud") or
                             row.get("geo_epgs_4326_lat") or row.get("LATITUD"))
            lng = _try_float(row.get("Longitud") or row.get("longitud") or
                             row.get("geo_epgs_4326_lon") or row.get("LONGITUD"))

            if lat is None or lng is None:
                # Intentar con coordenadas X/Y (UTM EPSG:25831)
                x = _try_float(row.get("X_UTM_ETRS89") or row.get("x_etrs89"))
                y = _try_float(row.get("Y_UTM_ETRS89") or row.get("y_etrs89"))
                if x and y:
                    # Se convertirá en PostGIS con ST_Transform
                    lat, lng = y, x  # placeholder, real conversion in SQL
                else:
                    continue

            nombre = (row.get("Nom") or row.get("nom") or
                      row.get("Equipament") or row.get("equipament") or "")
            if not nombre:
                continue

            # Detectar tipo
            nombre_lower = nombre.lower()
            if "mossos" in nombre_lower:
                tipo = "mossos_esquadra"
            elif "guàrdia" in nombre_lower or "guardia" in nombre_lower:
                tipo = "guardia_urbana"
            elif "policia nacional" in nombre_lower or "cnp" in nombre_lower:
                tipo = "policia_nacional"
            else:
                tipo = "otros"

            comisarias_data.append({
                "id": f"com_{hash(nombre) % 100000:05d}",
                "nombre": nombre.strip(),
                "tipo": tipo,
                "direccion": (row.get("Adreca") or row.get("adreca") or
                              row.get("Direccion") or "").strip(),
                "distrito": (row.get("Nom_Districte") or row.get("Districte") or "").strip(),
                "barrio": (row.get("Nom_Barri") or row.get("Barri") or "").strip(),
                "lat": lat,
                "lng": lng,
            })

        if not comisarias_data:
            logger.warning("CSV de comissaries vacío")
            return 0

        async with get_db() as conn:
            # Insertar comisarías
            for c in comisarias_data:
                await conn.execute("""
                    INSERT INTO comisarias (id, nombre, tipo, direccion, distrito, barrio,
                                           geometria, fuente)
                    VALUES ($1, $2, $3, $4, $5, $6,
                            ST_SetSRID(ST_MakePoint($7, $8), 4326),
                            'opendata_bcn')
                    ON CONFLICT (id) DO UPDATE SET
                        nombre    = EXCLUDED.nombre,
                        tipo      = EXCLUDED.tipo,
                        direccion = EXCLUDED.direccion,
                        distrito  = EXCLUDED.distrito,
                        barrio    = EXCLUDED.barrio,
                        geometria = EXCLUDED.geometria,
                        updated_at = NOW()
                """, c["id"], c["nombre"], c["tipo"], c["direccion"],
                     c["distrito"], c["barrio"], c["lng"], c["lat"])

            logger.info("Comisarias insertadas: %d", len(comisarias_data))

            # Calcular comisarias_1km y dist_comisaria_m por zona
            hoy = date.today()
            zonas = await conn.fetch("""
                SELECT z.id AS zona_id,
                       (SELECT COUNT(*)::int
                        FROM comisarias c
                        WHERE ST_DWithin(c.geometria::geography,
                                         ST_Centroid(z.geometria)::geography, 1000)
                       ) AS comisarias_1km,
                       (SELECT MIN(ST_Distance(c.geometria::geography,
                                               ST_Centroid(z.geometria)::geography))::int
                        FROM comisarias c
                       ) AS dist_comisaria_m
                FROM zonas z
            """)

            for z in zonas:
                await conn.execute("""
                    INSERT INTO variables_zona (zona_id, fecha, fuente)
                    VALUES ($1, $2, 'seguridad_comisarias')
                    ON CONFLICT (zona_id, fecha) DO UPDATE
                    SET fuente = EXCLUDED.fuente, updated_at = NOW()
                """, z["zona_id"], hoy)

                await conn.execute("""
                    INSERT INTO vz_entorno (zona_id, fecha, comisarias_1km,
                                            dist_comisaria_m, fuente)
                    VALUES ($1, $2, $3, $4, 'seguridad_comisarias')
                    ON CONFLICT (zona_id, fecha) DO UPDATE SET
                        comisarias_1km   = EXCLUDED.comisarias_1km,
                        dist_comisaria_m = EXCLUDED.dist_comisaria_m,
                        fuente           = EXCLUDED.fuente,
                        updated_at       = NOW()
                """, z["zona_id"], hoy, z["comisarias_1km"],
                     z["dist_comisaria_m"])
                ok += 1

        logger.info("Proximidad policial: %d zonas actualizadas", ok)

    except Exception as e:
        logger.warning("_poblar_comisarias error: %s", e)
    return ok


# ── 3. IERMB seguretat al barri ──────────────────────────────────────────────

async def _poblar_seguridad_iermb() -> int:
    """
    Descarga el indicador IERMB 'Nivell de seguretat al barri' (id_ind=1892)
    y escribe seguridad_barri_score en vz_entorno.

    Nota: dato a nivel ciudad AMB (Barcelona como municipio).
    Se aplica como valor común a todas las zonas de Barcelona.
    """
    ok = 0
    try:
        data = None
        last_err: Optional[Exception] = None
        async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as c:
            for attempt in range(_HTTP_RETRIES):
                try:
                    r = await c.get(
                        _IERMB_BASE,
                        params={
                            "token": _IERMB_TOKEN,
                            "id_ind": _IERMB_SEGURETAT_ID,
                            "type": "json",
                        },
                    )
                    r.raise_for_status()
                    data = r.json()
                    break
                except Exception as e:
                    last_err = e
                    if attempt < _HTTP_RETRIES - 1:
                        await asyncio.sleep(_HTTP_RETRY_DELAY)
        if data is None:
            raise last_err  # type: ignore[misc]

        score_bcn: Optional[float] = _extract_iermb_bcn_value(data)
        if score_bcn is None:
            logger.warning("IERMB seguretat: no se encontró valor para Barcelona")
            return 0

        if score_bcn > 10:
            score_bcn = min(score_bcn / 10.0, 10.0)

        iermb_year = _extract_iermb_bcn_year(data)
        fecha = date(iermb_year, 1, 1)

        async with get_db() as conn:
            zona_ids = await conn.fetch("""
                SELECT z.id FROM zonas z
                JOIN barrios b ON b.id = z.barrio_id
                JOIN distritos d ON d.id = b.distrito_id
            """)
            for z in zona_ids:
                await conn.execute("""
                    INSERT INTO variables_zona (zona_id, fecha, fuente)
                    VALUES ($1, $2, 'iermb_seguretat')
                    ON CONFLICT (zona_id, fecha) DO UPDATE
                    SET fuente = EXCLUDED.fuente, updated_at = NOW()
                """, z["id"], fecha)
                await conn.execute("""
                    INSERT INTO vz_entorno (zona_id, fecha, seguridad_barri_score, fuente)
                    VALUES ($1, $2, $3, 'iermb_seguretat')
                    ON CONFLICT (zona_id, fecha) DO UPDATE
                    SET seguridad_barri_score = EXCLUDED.seguridad_barri_score,
                        fuente               = EXCLUDED.fuente,
                        updated_at           = NOW()
                """, z["id"], fecha, round(score_bcn, 2))
                ok += 1

        logger.info("IERMB seguretat: score=%.2f → %d zonas actualizadas", score_bcn, ok)

    except Exception as e:
        logger.warning("_poblar_seguridad_iermb error: %s", e)
    return ok


# ── Helpers IERMB ─────────────────────────────────────────────────────────────

def _extract_iermb_bcn_year(data) -> int:
    """Extrae el año más reciente del JSON de IERMB para Barcelona."""
    try:
        records = data if isinstance(data, list) else (
            data.get("data") or data.get("results") or data.get("rows") or []
        ) if isinstance(data, dict) else []

        bcn_codes = {"08019", "8019", "barcelona"}
        best_year = 0
        for rec in records:
            if not isinstance(rec, dict):
                continue
            muni = str(
                rec.get("codi_muni") or rec.get("municipi") or
                rec.get("Municipi") or rec.get("municipio") or ""
            ).lower().strip()
            if not any(code in muni for code in bcn_codes):
                continue
            any_val = (rec.get("any") or rec.get("Any") or
                       rec.get("year") or rec.get("Year") or 0)
            try:
                year = int(any_val)
                if year > best_year:
                    best_year = year
            except (ValueError, TypeError):
                pass
        return best_year if best_year > 0 else 2023
    except Exception:
        return 2023


def _extract_iermb_bcn_value(data) -> Optional[float]:
    """Extrae el valor de Barcelona del JSON de IERMB (año más reciente)."""
    try:
        records = data if isinstance(data, list) else (
            data.get("data") or data.get("results") or data.get("rows") or []
        ) if isinstance(data, dict) else []

        bcn_codes = {"08019", "8019", "barcelona"}
        by_year: dict[int, float] = {}
        for rec in records:
            if not isinstance(rec, dict):
                continue
            muni = str(
                rec.get("codi_muni") or rec.get("municipi") or
                rec.get("Municipi") or rec.get("municipio") or ""
            ).lower().strip()
            if not any(code in muni for code in bcn_codes):
                continue
            any_val = (rec.get("any") or rec.get("Any") or
                       rec.get("year") or rec.get("Year") or 0)
            try:
                year = int(any_val)
            except (ValueError, TypeError):
                year = 0
            for key in ("valor", "value", "Valor", "Value", "ind_valor"):
                v = rec.get(key)
                if v is not None:
                    try:
                        by_year[year] = float(v)
                        break
                    except (ValueError, TypeError):
                        pass
        if by_year:
            return by_year[max(by_year)]
    except Exception:
        pass
    return None


# ── Helpers CKAN ──────────────────────────────────────────────────────────────

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
        # Buscar CSVs, preferir el más reciente (por nombre o posición)
        csv_resources = [
            res for res in resources
            if (res.get("format", "").upper() == "CSV" or
                res.get("url", "").lower().endswith(".csv"))
        ]
        if not csv_resources:
            return None

        # Ordenar por nombre descendente (2025 > 2024 > 2023...)
        csv_resources.sort(
            key=lambda r: r.get("name", "") or r.get("url", ""),
            reverse=True,
        )
        return csv_resources[0].get("url")

    except Exception as e:
        logger.warning("_find_latest_csv_url(%s) error: %s", dataset_id, e)
        return None


async def _download_csv(url: str) -> Optional[str]:
    """Descarga un CSV desde una URL con retry."""
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
    logger.warning("_download_csv(%s) falló tras %d intentos: %s",
                    url, _HTTP_RETRIES, last_err)
    return None


async def _get_poblacion_por_barrio() -> dict[str, int]:
    """Obtiene población por barrio desde vz_demografia."""
    result: dict[str, int] = {}
    try:
        async with get_db() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (b.nombre)
                       b.nombre, d.poblacion
                FROM vz_demografia d
                JOIN zonas z ON z.id = d.zona_id
                JOIN barrios b ON b.id = z.barrio_id
                WHERE d.poblacion IS NOT NULL
                ORDER BY b.nombre, d.fecha DESC
            """)
            for r in rows:
                key = _normalize_barri(r["nombre"])
                result[key] = int(r["poblacion"] or 5000)
    except Exception as e:
        logger.warning("_get_poblacion_por_barrio error: %s", e)
    return result


def _try_float(val) -> Optional[float]:
    """Intenta convertir a float, devuelve None si falla."""
    if val is None:
        return None
    try:
        return float(str(val).replace(",", ".").strip())
    except (ValueError, TypeError):
        return None


# ── Pipeline tracking ─────────────────────────────────────────────────────────

async def _init():
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones(pipeline,estado) "
            "VALUES('seguridad','running') RETURNING id"
        )


async def _fin(eid, reg, estado, msg=None):
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones "
            "SET fecha_fin=NOW(),registros=$1,estado=$2,mensaje_error=$3 WHERE id=$4",
            reg, estado, msg, eid,
        )
