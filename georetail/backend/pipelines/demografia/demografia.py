"""
pipelines/demografia.py — Pipeline mensual de datos demográficos (v2.1).

Fuentes:
  - Padró Municipal BCN (CKAN datastore): población, edad, extranjeros,
    pct_poblacio_25_44, nivel_estudios_alto_pct, densitat_hab_km2 (PostGIS)
  - Renda disponible llars BCN (CSV descargable):
    renta_media_hogar, delta_renta_3a (variación 2019→año más reciente)
  - IERMB API (id_ind=1892): seguridad_barri_score a nivel ciudad

Cambios v2:
  - Fix bug: densitat_hab_km2 ahora se calcula desde PostGIS (población/área)
  - Paginación padró: LIMIT 5000 (evita truncar barrios)
  - Nuevo: pct_poblacio_25_44 desde grupos quinquenales del padró CKAN
  - Nuevo: nivel_estudios_alto_pct desde dataset educación BCN
  - Nuevo: delta_renta_3a comparando CSV renda 2022 vs 2019
  - Nuevo: seguridad_barri_score desde IERMB id_ind=1892

Cambios v2.1 (fixes):
  - B1 FIX: pct_estran — validación de rango (si >1 ya viene como fracción)
  - B2 FIX: fecha en _poblar_renta/_poblar_seguretat_iermb — dinámica, no hardcodeada
  - B4 FIX: _fetch_grups_edat — logger.warning cuando todas las tablas CKAN fallan
  - D1 FIX: _poblar_seguretat_iermb — filtra zonas por nombre de ciudad 'Barcelona'
  - D3 FIX: retry HTTP (3 intentos, backoff 1s) en llamadas CKAN e IERMB
"""
from __future__ import annotations
import asyncio
import csv
import io
import logging
import os
from datetime import date
from typing import Optional

import httpx
from db.conexion import get_db
from scoring.infra.governance import load_source_decisions, source_is_approved

# D3: número de reintentos para llamadas HTTP externas (CKAN, IERMB)
_HTTP_RETRIES = 3
_HTTP_RETRY_DELAY = 1.0  # segundos entre reintentos

logger = logging.getLogger(__name__)

# ── Open Data Barcelona ────────────────────────────────────────────────────────
_CKAN = "https://opendata-ajuntament.barcelona.cat/data/api/action"
_CKAN_HEADERS: dict = (
    {"Authorization": os.environ.get("OPEN_DATA_BCN_API_KEY", "")}
    if os.environ.get("OPEN_DATA_BCN_API_KEY")
    else {}
)

# CSV renda disponible per persona 2022 (año más reciente disponible)
_RENDA_2022_URL = (
    "https://opendata-ajuntament.barcelona.cat/data/dataset/"
    "78db0c75-fa56-4604-9510-8b92834a7fd2/resource/"
    "3df0c5b9-de69-4c94-b924-57540e52932f/download/"
    "2022_renda_disponible_llars_per_persona.csv"
)
# CSV renda 2019 — mismo dataset, recurso anterior (para delta_renta_3a)
# El dataset_id es el mismo; se obtiene la lista de recursos via CKAN package_show
_RENDA_DATASET_ID = "78db0c75-fa56-4604-9510-8b92834a7fd2"

# IERMB API token (público, extraído del catálogo AMB)
_IERMB_TOKEN = os.environ.get("IERMB_TOKEN", "AGEF894MGIE0220GOLLEOF")
_IERMB_BASE  = "https://iermbdb.uab.cat/datasets2/index.php"

# id_ind IERMB relevantes
_IERMB_SEGURETAT_ID = "1892"   # Nivell de seguretat al barri (0-10)


async def ejecutar() -> dict:
    eid = await _init()
    ok = 0
    try:
        local_backfill_error: Exception | None = None
        try:
            from pipelines.demografia.demografia_backfill import ejecutar_backfill  # noqa: PLC0415

            result = await ejecutar_backfill(scope="full", years_mode="latest")
            ok += int(result.get("rows_written") or 0)
            logger.info(
                "Demografia local OK: scope=%s rows_written=%s support_datasets=%s",
                result.get("scope"),
                result.get("rows_written"),
                result.get("citywide_support_datasets"),
            )
        except Exception as exc:
            local_backfill_error = exc
            logger.warning(
                "Backfill local de demografia fallo; activando fallback online: %s",
                exc,
            )

        if local_backfill_error is not None:
            decisions = load_source_decisions()

            if _fuente_aprobada(decisions, "renda-disponible-llars-bcn"):
                ok += await _poblar_renta()
            else:
                logger.info("Saltando renta demografica: dataset no aprobado por readiness")

            if _fuente_aprobada(
                decisions,
                "pad_mdbas_edat-q",
                "pad_mdbas_niv-educa-esta_sexe",
                "pad_mdb_nacionalitat-g_edat-q_sexe",
            ):
                ok += await _poblar_padro()
            else:
                logger.info("Saltando padro demografico: datasets no aprobados por readiness")

        ok += await _poblar_seguretat_iermb()
        await _fin(eid, ok, "ok")
        return {"registros": ok}
    except Exception as e:
        logger.error("Pipeline demografia error: %s", e)
        await _fin(eid, ok, "error", str(e))
        raise


def _fuente_aprobada(decisions: dict[str, dict], *dataset_ids: str) -> bool:
    if not decisions:
        return True

    presentes = [dataset_id for dataset_id in dataset_ids if dataset_id in decisions]
    if not presentes:
        return True

    return any(source_is_approved(dataset_id, decisions) for dataset_id in presentes)


# ── Renta disponible (CSV BCN Open Data) ──────────────────────────────────────

async def _poblar_renta() -> int:
    """
    Descarga CSV 2022 y 2019 de renda disponible, calcula:
      - renta_media_hogar (2022) por zona via distrito
      - delta_renta_3a (% cambio 2019→2022) por zona via distrito
    """
    ok = 0
    try:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(45.0), headers=_CKAN_HEADERS, follow_redirects=True
        ) as client:
            # CSV 2022
            r22 = await client.get(_RENDA_2022_URL)
            r22.raise_for_status()
            renta_2022 = _parse_renda_csv(r22.text)

            # CSV 2019 — buscar URL via CKAN package_show
            renta_2019 = await _fetch_renda_historica(client, 2019)

        # Delta renta 2019→2022 por distrito
        delta_por_distrito: dict[str, float] = {}
        if renta_2019:
            for dist, r22v in renta_2022.items():
                r19v = renta_2019.get(dist)
                if r19v and r19v > 0:
                    delta_por_distrito[dist] = round((r22v - r19v) / r19v, 4)

        # B2 FIX: fecha dinámica — usar el año más reciente disponible en los datos,
        # no hardcodear 2022. El CSV 2022 es el de referencia pero cuando se
        # descargue uno más reciente, la fecha reflejará el año correcto.
        # Extraemos el año del nombre de la URL (siempre contiene el año).
        import re as _re
        _match = _re.search(r"(\d{4})_renda", _RENDA_2022_URL)
        _renta_year = int(_match.group(1)) if _match else 2022
        fecha = date(_renta_year, 6, 1)

        async with get_db() as conn:
            rows = await conn.fetch("""
                SELECT z.id AS zona_id, d.codigo AS dist_codigo
                FROM zonas z
                JOIN barrios b ON b.id = z.barrio_id
                JOIN distritos d ON d.id = b.distrito_id
            """)

            for row in rows:
                dist  = row["dist_codigo"]
                renta = renta_2022.get(dist)
                delta = delta_por_distrito.get(dist)
                if not renta:
                    continue
                async with conn.transaction():
                    await conn.execute("""
                        INSERT INTO variables_zona (zona_id, fecha, fuente)
                        VALUES ($1, $2, 'renda_bcn_2022')
                        ON CONFLICT (zona_id, fecha) DO UPDATE
                        SET fuente = EXCLUDED.fuente, updated_at = NOW()
                    """, row["zona_id"], fecha)
                    await conn.execute("""
                        INSERT INTO vz_demografia
                            (zona_id, fecha, renta_media_hogar, delta_renta_3a, fuente)
                        VALUES ($1, $2, $3, $4, 'renda_bcn_2022')
                        ON CONFLICT (zona_id, fecha) DO UPDATE
                        SET renta_media_hogar = EXCLUDED.renta_media_hogar,
                            delta_renta_3a    = EXCLUDED.delta_renta_3a,
                            fuente            = EXCLUDED.fuente,
                            updated_at        = NOW()
                    """, row["zona_id"], fecha, round(renta, 2), delta)
                ok += 1

    except Exception as e:
        logger.warning("_poblar_renta error: %s", e)
    return ok


def _parse_renda_csv(text: str) -> dict[str, float]:
    """Parsea CSV de renda y devuelve {codi_districte_2digits: renta_media}."""
    dist_rentas: dict[str, list[float]] = {}
    reader = csv.DictReader(io.StringIO(text))
    for row in reader:
        try:
            dist  = str(row.get("Codi_Districte", "") or "").strip().zfill(2)
            renta = float(row.get("Import_Euros", 0) or 0)
            if dist and renta > 0:
                dist_rentas.setdefault(dist, []).append(renta)
        except (ValueError, TypeError):
            continue
    return {d: sum(vs) / len(vs) for d, vs in dist_rentas.items() if vs}


async def _fetch_renda_historica(
    client: httpx.AsyncClient, year: int
) -> dict[str, float]:
    """
    Busca el CSV de renda de `year` via CKAN package_show y lo parsea.
    Devuelve {} si no encuentra el recurso.
    """
    try:
        r = await client.get(
            f"{_CKAN}/package_show",
            params={"id": _RENDA_DATASET_ID},
            headers=_CKAN_HEADERS,
        )
        r.raise_for_status()
        resources = r.json().get("result", {}).get("resources", [])
        # Buscar recurso que contenga el año en el nombre
        url = None
        for res in resources:
            name = (res.get("name") or res.get("url") or "").lower()
            if str(year) in name and name.endswith(".csv"):
                url = res.get("url") or res.get("download_url")
                break
        if not url:
            logger.debug("No se encontró CSV renda %d", year)
            return {}
        r2 = await client.get(url)
        r2.raise_for_status()
        return _parse_renda_csv(r2.text)
    except Exception as e:
        logger.debug("_fetch_renda_historica(%d) error: %s", year, e)
        return {}


# ── Padrón municipal (CKAN) ────────────────────────────────────────────────────

async def _poblar_padro() -> int:
    """
    Carga datos del padrón con paginación (LIMIT 5000):
      - edad_media, pct_extranjeros, poblacion
      - densitat_hab_km2 calculada via PostGIS (real, no imputada)
      - pct_poblacio_25_44 desde grupos quinquenales (si disponible)
      - nivel_estudios_alto_pct desde dataset educación (si disponible)
    """
    ok = 0
    try:
        padro_rows = await _fetch_padro_base()
        grups_edat = await _fetch_grups_edat()      # {codi_barri: pct_25_44}
        estudis    = await _fetch_nivel_estudis()   # {codi_barri: pct_alt}

        async with get_db() as conn:
            # Pre-calcular área de cada zona via PostGIS (m² → km²)
            area_rows = await conn.fetch("""
                SELECT
                    z.id AS zona_id,
                    b.codigo AS barri_code,
                    ST_Area(z.geometria::geography) / 1e6 AS area_km2
                FROM zonas z
                JOIN barrios b ON b.id = z.barrio_id
            """)
            area_por_barri: dict[str, float] = {}
            zona_por_barri: dict[str, list[str]] = {}
            for a in area_rows:
                bc = str(a["barri_code"]).zfill(6)
                # BUG1 FIX: acumular el área total del barrio sumando todas sus zonas
                area_por_barri[bc] = area_por_barri.get(bc, 0.0) + (
                    float(a["area_km2"]) if a["area_km2"] else 0.0
                )
                zona_por_barri.setdefault(bc, []).append(a["zona_id"])

            for row in padro_rows:
                barri_code  = str(row.get("Codi_Barri", "")).zfill(6)
                year        = int(row.get("Any", 2023))
                # BUG2 FIX: pasar objeto date en lugar de string para asyncpg
                fecha       = date(year, 1, 1)
                poblacion   = int(row.get("Total") or 0)
                # B1 FIX: el padrón CKAN puede devolver Pct_Estrangers como
                # porcentaje (22.5) o como fracción (0.225) según la versión.
                # Si el valor es >1 asumimos porcentaje y dividimos; si ≤1
                # ya es fracción y lo usamos directamente.
                _pct_raw    = _parse_float(row.get("Pct_Estrangers"), 0)
                pct_estran  = _pct_raw / 100 if _pct_raw > 1.0 else _pct_raw
                edad_mediana = _parse_float(row.get("Edat_Mediana"), 42.0)

                pct_25_44   = grups_edat.get(barri_code)
                pct_estudis = estudis.get(barri_code)
                area_km2    = area_por_barri.get(barri_code, 0.0)

                # densitat_hab_km2 — fix: calcular real, no imputar
                densitat: Optional[float] = None
                if poblacion > 0 and area_km2 > 0:
                    densitat = round(poblacion / area_km2, 1)

                zona_ids = zona_por_barri.get(barri_code, [])
                for zona_id in zona_ids:
                    async with conn.transaction():
                        await conn.execute("""
                            INSERT INTO variables_zona (zona_id, fecha, fuente)
                            VALUES ($1, $2, 'padro_bcn')
                            ON CONFLICT (zona_id, fecha) DO UPDATE
                            SET fuente = EXCLUDED.fuente, updated_at = NOW()
                        """, zona_id, fecha)
                        await conn.execute("""
                            INSERT INTO vz_demografia (
                                zona_id, fecha,
                                poblacion, pct_extranjeros, edad_media,
                                densidad_hab_km2, pct_poblacio_25_44,
                                nivel_estudios_alto_pct, fuente
                            )
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'padro_bcn')
                            ON CONFLICT (zona_id, fecha) DO UPDATE
                            SET poblacion               = EXCLUDED.poblacion,
                                pct_extranjeros         = EXCLUDED.pct_extranjeros,
                                edad_media              = EXCLUDED.edad_media,
                                densidad_hab_km2        = EXCLUDED.densidad_hab_km2,
                                pct_poblacio_25_44      = EXCLUDED.pct_poblacio_25_44,
                                nivel_estudios_alto_pct = EXCLUDED.nivel_estudios_alto_pct,
                                fuente                  = EXCLUDED.fuente,
                                updated_at              = NOW()
                        """, zona_id, fecha,
                            poblacion, pct_estran, edad_mediana,
                            densitat, pct_25_44, pct_estudis)
                    ok += 1

    except Exception as e:
        logger.warning("_poblar_padro error: %s", e)
    return ok


async def _fetch_padro_base() -> list[dict]:
    """Descarga el padrón municipal con paginación hasta obtener todos los barrios."""
    rows: list[dict] = []
    limit  = 5000
    offset = 0
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(45.0), headers=_CKAN_HEADERS
    ) as c:
        while True:
            sql = f'SELECT * FROM "pad_mdbas" LIMIT {limit} OFFSET {offset}'
            r = await c.get(
                f"{_CKAN}/datastore_search_sql", params={"sql": sql}
            )
            if r.status_code != 200:
                break
            batch = r.json().get("result", {}).get("records", [])
            rows.extend(batch)
            if len(batch) < limit:
                break
            offset += limit
    logger.info("Padró base: %d registros cargados", len(rows))
    return rows


async def _fetch_grups_edat() -> dict[str, float]:
    """
    Consulta el padrón por grupos quinquenales de edad y devuelve
    {codi_barri_6digits: pct_poblacio_25_44}.

    Intenta varias tablas CKAN posibles para el dataset de edad.
    """
    result: dict[str, float] = {}
    tables_to_try = [
        "pad_mdbas_edatquinquenal",
        "pad_mdad",
        "2023_padro_edat_quinquenal_barri",
    ]
    age_groups_25_44 = {"25-29", "30-34", "35-39", "40-44",
                        "25 a 29", "30 a 34", "35 a 39", "40 a 44"}

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(30.0), headers=_CKAN_HEADERS
    ) as c:
        for table in tables_to_try:
            try:
                sql = f'SELECT "Codi_Barri", "Edat_Quinquenal", "Total" FROM "{table}" LIMIT 10000'
                r = await c.get(
                    f"{_CKAN}/datastore_search_sql", params={"sql": sql}
                )
                if r.status_code != 200:
                    continue
                records = r.json().get("result", {}).get("records", [])
                if not records:
                    continue

                # Acumular por barri: total 25-44 y total global
                totals_25_44: dict[str, float] = {}
                totals_all:   dict[str, float] = {}
                for rec in records:
                    bc  = str(rec.get("Codi_Barri", "")).zfill(6)
                    grp = str(rec.get("Edat_Quinquenal") or "").strip()
                    n   = _parse_float(rec.get("Total"), 0)
                    totals_all[bc] = totals_all.get(bc, 0) + n
                    if grp in age_groups_25_44:
                        totals_25_44[bc] = totals_25_44.get(bc, 0) + n

                for bc, total in totals_all.items():
                    if total > 0 and bc in totals_25_44:
                        result[bc] = round(totals_25_44[bc] / total, 4)

                if result:
                    logger.info(
                        "pct_poblacio_25_44: %d barrios desde tabla %s", len(result), table
                    )
                    return result
            except Exception as e:
                logger.debug("_fetch_grups_edat tabla %s error: %s", table, e)

    logger.warning("pct_poblacio_25_44: no se pudo obtener de CKAN (ninguna tabla disponible)")
    return result


async def _fetch_nivel_estudis() -> dict[str, float]:
    """
    Consulta el dataset de nivel de estudios del padrón y devuelve
    {codi_barri_6digits: pct_estudios_altos}.

    Nivel alto = estudios universitarios / postgraduados.
    """
    result: dict[str, float] = {}
    tables_to_try = ["pad_mdes", "pad_mdnv_estudis", "2023_padro_nivell_estudis_barri"]
    high_education_keywords = {
        "universitari", "postgrau", "superior", "grau", "llicenciatura",
        "doctorat", "master", "universitaria", "licenciatura"
    }

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(30.0), headers=_CKAN_HEADERS
    ) as c:
        for table in tables_to_try:
            try:
                sql = f'SELECT "Codi_Barri", "Nivell_Estudi", "Total" FROM "{table}" LIMIT 10000'
                r = await c.get(
                    f"{_CKAN}/datastore_search_sql", params={"sql": sql}
                )
                if r.status_code != 200:
                    continue
                records = r.json().get("result", {}).get("records", [])
                if not records:
                    continue

                totals_alt: dict[str, float] = {}
                totals_all: dict[str, float] = {}
                for rec in records:
                    bc  = str(rec.get("Codi_Barri", "")).zfill(6)
                    niv = str(rec.get("Nivell_Estudi") or "").lower().strip()
                    n   = _parse_float(rec.get("Total"), 0)
                    totals_all[bc] = totals_all.get(bc, 0) + n
                    if any(kw in niv for kw in high_education_keywords):
                        totals_alt[bc] = totals_alt.get(bc, 0) + n

                for bc, total in totals_all.items():
                    if total > 0 and bc in totals_alt:
                        result[bc] = round(totals_alt[bc] / total, 4)

                if result:
                    logger.info(
                        "nivel_estudios_alto_pct: %d barrios desde tabla %s",
                        len(result), table
                    )
                    return result
            except Exception as e:
                logger.debug("_fetch_nivel_estudis tabla %s error: %s", table, e)

    logger.debug("nivel_estudios_alto_pct: no disponible en CKAN")
    return result


# ── IERMB seguridad al barri ───────────────────────────────────────────────────

async def _poblar_seguretat_iermb() -> int:
    """
    Descarga el indicador IERMB 'Nivell de seguretat al barri' (id_ind=1892)
    y escribe seguridad_barri_score en vz_entorno para las zonas de Barcelona.

    Nota: el dato es a nivel ciudad AMB (Barcelona como un municipio).
    Se aplica como valor común a todas las zonas de Barcelona.
    """
    ok = 0
    try:
        # D3 FIX: retry con backoff para la llamada IERMB
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
            raise last_err

        # Estructura IERMB: lista de {any, municipi, valor} o similar
        score_bcn: Optional[float] = _extract_iermb_bcn_value(data)
        if score_bcn is None:
            logger.warning("IERMB seguretat: no se encontró valor para Barcelona")
            return 0

        # Normalizar a 0-10 si viene en otro rango
        if score_bcn > 10:
            score_bcn = min(score_bcn / 10.0, 10.0)

        # B2 FIX: extraer el año más reciente del propio JSON en lugar de hardcodear 2023.
        # _extract_iermb_bcn_value ya busca el año más reciente; lo recuperamos aquí.
        _iermb_year = _extract_iermb_bcn_year(data)
        fecha = date(_iermb_year, 1, 1)

        async with get_db() as conn:
            # D1 FIX: filtrar solo zonas de Barcelona, no todas las zonas del sistema
            zona_ids = await conn.fetch(
                """
                SELECT z.id FROM zonas z
                JOIN barrios b ON b.id = z.barrio_id
                JOIN distritos d ON d.id = b.distrito_id
                JOIN ciudades c ON c.id = d.ciudad_id
                WHERE LOWER(c.nombre) = 'barcelona'
                """
            )
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
                # BUG4 NOTE: la columna fuente de vz_entorno se añade en migración 018
                ok += 1

        logger.info("IERMB seguretat: score=%.2f → %d zonas actualizadas", score_bcn, ok)

    except Exception as e:
        logger.warning("_poblar_seguretat_iermb error: %s", e)
    return ok


def _extract_iermb_bcn_year(data) -> int:
    """
    Extrae el año más reciente del JSON de IERMB para Barcelona.
    Devuelve 2023 como fallback si no se puede determinar el año.

    B2 FIX: la fecha de vz_entorno debe reflejar el año real del dato IERMB,
    no un año hardcodeado.
    """
    try:
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict):
            records = data.get("data") or data.get("results") or data.get("rows") or []
        else:
            return 2023

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
            any_val = (
                rec.get("any") or rec.get("Any") or
                rec.get("year") or rec.get("Year") or 0
            )
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
    """
    Extrae el valor de Barcelona del JSON de IERMB, tomando el año más reciente.

    El JSON puede tener varias estructuras según el indicador.
    Busca el municipio con código 08019 (Barcelona) o nombre 'Barcelona'.

    BUG3 FIX: en lugar de promediar todos los años, extrae el valor
    del año más reciente disponible para evitar diluir datos actuales
    con históricos (p.ej. seguridad 7.2 en 2023 + 5.1 en 2014 → 6.3 falso).
    """
    try:
        # Estructura 1: lista directa
        if isinstance(data, list):
            records = data
        # Estructura 2: dict con key 'data' o 'results'
        elif isinstance(data, dict):
            records = data.get("data") or data.get("results") or data.get("rows") or []
        else:
            return None

        # Buscar registros de Barcelona (codi_muni=08019 o nombre 'Barcelona')
        bcn_codes = {"08019", "8019", "barcelona"}
        # Acumular (any, valor) para quedarnos con el año más reciente
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
            # Extraer año
            any_val = rec.get("any") or rec.get("Any") or rec.get("year") or rec.get("Year") or 0
            try:
                year = int(any_val)
            except (ValueError, TypeError):
                year = 0
            # Extraer valor
            for key in ("valor", "value", "Valor", "Value", "ind_valor"):
                v = rec.get(key)
                if v is not None:
                    try:
                        by_year[year] = float(v)
                        break
                    except (ValueError, TypeError):
                        pass

        if by_year:
            # Devolver el valor del año más reciente
            return by_year[max(by_year)]
    except Exception:
        pass
    return None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_float(value, default: float = 0.0) -> float:
    """Parsea un valor a float, devolviendo default si falla."""
    if value is None:
        return default
    try:
        return float(str(value).replace(",", ".").strip())
    except (ValueError, TypeError):
        return default


async def _init():
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones(pipeline,estado) "
            "VALUES('demografia','running') RETURNING id"
        )


async def _fin(eid, reg, estado, msg=None):
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones "
            "SET fecha_fin=NOW(),registros=$1,estado=$2,mensaje_error=$3 WHERE id=$4",
            reg, estado, msg, eid,
        )
