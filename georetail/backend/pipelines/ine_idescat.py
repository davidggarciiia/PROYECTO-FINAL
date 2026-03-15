"""
pipelines/ine_idescat.py — Datos de INE, Idescat y Open Data BCN complementarios.

Pobla las columnas de variables_zona que no cubren otros pipelines:

  Fuente          Variable                    Descripción
  ──────────────  ──────────────────────────  ─────────────────────────────────────
  Calculada (DB)  densidad_hab_km2            poblacion / (area_m2 / 1e6)
  INE Padrón      nivel_estudios_alto_pct     % población con estudios universitarios
  INE DIRCE       tasa_rotacion_anual         altas/bajas empresas por CNAE, prov. 08
  Idescat emex    (complemento a los anteriores cuando INE no responde)
  BCN Open Data   ratio_locales_comerciales   Cens Locals Planta Baixa
  BCN Open Data   pct_locales_vacios          Cens Locals Planta Baixa
  BCN Open Data   score_equipamientos         Equipaments BCN (salud, educación...)
  BCN Open Data   m2_zonas_verdes_cercanas    Zones Verdes catalogades
  BCN Open Data   incidencias_por_1000hab     Incidents Guàrdia Urbana
  BCN Open Data   nivel_ruido_db              Mapa acústic Barcelona
  Calculada (DB)  score_turismo               hoteles + atracciones cerca (negocios_activos)

Frecuencia: mensual (día 1, 09:00 — después del pipeline de demografía)

APIs externas:
  INE:     https://servicios.ine.es/wstempus/js/ES/
  Idescat: https://api.idescat.cat/emex/v1/dades.json
  BCN:     https://opendata-ajuntament.barcelona.cat/data/api/action/datastore_search_sql
"""
from __future__ import annotations

import logging
import os
from typing import Any, Optional

import httpx

from db.conexion import get_db

logger = logging.getLogger(__name__)

# ── Configuración ──────────────────────────────────────────────────────────────
_CKAN      = "https://opendata-ajuntament.barcelona.cat/data/api/action"
_INE_BASE  = "https://servicios.ine.es/wstempus/js/ES"
_IDESCAT   = "https://api.idescat.cat/emex/v1/dades.json"
_TIMEOUT   = 30.0

_CKAN_HEADERS: dict = (
    {"Authorization": os.environ.get("OPEN_DATA_BCN_API_KEY", "")}
    if os.environ.get("OPEN_DATA_BCN_API_KEY")
    else {}
)

# Código de municipio Barcelona para Idescat (INE: 08019  →  Idescat: 080193)
_BCN_MUN   = "080193"
# Código de provincia Barcelona para INE DIRCE
_BCN_PROV  = "08"

# Mapping de nuestros sectores a códigos CNAE-2009 para INE DIRCE
# Clave = sector_codigo, valor = lista de CNAE 2 dígitos equivalentes
_SECTOR_CNAE = {
    "restauracion":  ["56"],           # Servicios de comidas y bebidas
    "tatuajes":      ["96"],           # Otros servicios personales
    "moda":          ["47"],           # Comercio al por menor
    "estetica":      ["96"],           # Otros servicios personales
    "shisha_lounge": ["56", "93"],     # Hostelería + actividades recreativas
}


# =============================================================================
# ENTRY POINT
# =============================================================================

async def ejecutar() -> dict:
    eid = await _init()
    total = 0
    try:
        async with httpx.AsyncClient(
            timeout=_TIMEOUT,
            headers=_CKAN_HEADERS,
            follow_redirects=True,
        ) as client:

            # 1. Calculadas desde BD — sin llamada API
            n = await _calcular_densidad()
            logger.info("  densidad_hab_km2: %d zonas", n); total += n

            # 2. INE — Padrón Continuo (nivel de estudios)
            n = await _poblar_ine_educacion(client)
            logger.info("  nivel_estudios (INE): %d zonas", n); total += n

            # 3. INE — DIRCE (tasas de rotación empresarial por sector)
            n = await _poblar_ine_dirce(client)
            logger.info("  tasa_rotacion (INE DIRCE): %d zonas", n); total += n

            # 4. Idescat — complemento si INE no respondió
            n = await _poblar_idescat(client)
            logger.info("  idescat emex: %d zonas", n); total += n

            # 5. BCN Open Data — Cens Locals (ratio comercial y vacíos)
            n = await _poblar_cens_locals(client)
            logger.info("  cens_locals: %d zonas", n); total += n

            # 6. BCN Open Data — Equipaments
            n = await _poblar_equipaments(client)
            logger.info("  score_equipamientos: %d zonas", n); total += n

            # 7. BCN Open Data — Zones verdes
            n = await _poblar_zones_verdes(client)
            logger.info("  m2_zonas_verdes: %d zonas", n); total += n

            # 8. BCN Open Data — Incidents GUB
            n = await _poblar_incidents_gub(client)
            logger.info("  incidencias_gub: %d zonas", n); total += n

            # 9. BCN Open Data — Mapa acústic
            n = await _poblar_soroll(client)
            logger.info("  nivel_ruido_db: %d zonas", n); total += n

            # 10. Calculada desde BD — score_turismo
            n = await _calcular_score_turismo()
            logger.info("  score_turismo: %d zonas", n); total += n

        await _fin(eid, total, "ok")
        return {"registros": total}

    except Exception as exc:
        logger.error("Pipeline ine_idescat error: %s", exc, exc_info=True)
        await _fin(eid, total, "error", str(exc))
        raise


# =============================================================================
# 1. DENSIDAD HAB/KM² — calculada desde datos ya en BD
# =============================================================================

async def _calcular_densidad() -> int:
    """
    densidad_hab_km2 = poblacion / (area_m2 / 1_000_000)
    Usa la población ya cargada por pipelines/demografia.py y el area_m2 de zonas.
    Solo actualiza zonas que tienen población pero no tienen densidad.
    """
    ok = 0
    try:
        async with get_db() as conn:
            rows = await conn.fetch("""
                SELECT z.id AS zona_id,
                       z.area_m2,
                       vz.poblacion,
                       vz.fecha
                FROM zonas z
                JOIN variables_zona vz ON vz.zona_id = z.id
                WHERE vz.poblacion IS NOT NULL
                  AND vz.poblacion > 0
                  AND z.area_m2     IS NOT NULL
                  AND z.area_m2     > 0
                  AND vz.densidad_hab_km2 IS NULL
            """)
            for r in rows:
                densidad = r["poblacion"] / (r["area_m2"] / 1_000_000)
                await conn.execute("""
                    UPDATE variables_zona
                    SET densidad_hab_km2 = $1
                    WHERE zona_id = $2 AND fecha = $3
                """, round(densidad, 1), r["zona_id"], r["fecha"])
                ok += 1
    except Exception as exc:
        logger.warning("_calcular_densidad error: %s", exc)
    return ok


# =============================================================================
# 2. INE — Nivel de estudios (Padrón Continuo)
# =============================================================================

async def _poblar_ine_educacion(client: httpx.AsyncClient) -> int:
    """
    nivel_estudios_alto_pct: % de población con estudios universitarios.
    Fuente: INE — Estadística del Padrón Continuo, tabla 9697
    (Población de 16+ años por nivel de formación y municipio >50k hab.)

    El dato es a nivel municipio (Barcelona ciudad = 08019).
    Se aplica como valor de referencia a todas las zonas sin dato propio.
    """
    pct_univ = None

    # ── Intento 1: INE API tabla 9697 ─────────────────────────────────────────
    try:
        url = f"{_INE_BASE}/DATOS_TABLA/9697"
        r = await client.get(url, params={"nult": "2"}, timeout=20.0)
        r.raise_for_status()
        data = r.json()

        # Estructura INE: lista de series con MetaData y Data
        total_pob = 0.0
        pob_univ  = 0.0
        for serie in (data if isinstance(data, list) else []):
            meta = serie.get("MetaData", [])
            nombre = " ".join(m.get("Valor", "") for m in meta).lower()
            municipio_ok = any(
                m.get("Codigo", "") == "08019"
                for m in meta
            )
            if not municipio_ok:
                continue
            valores = serie.get("Data", [])
            if not valores:
                continue
            v = float(valores[0].get("Valor") or 0)
            if "universidad" in nombre or "superior" in nombre or "doctorado" in nombre:
                pob_univ += v
            if v > 0:
                total_pob += v

        if total_pob > 0:
            pct_univ = round(pob_univ / total_pob, 4)
            logger.info("    INE educación BCN: %.1f%% universitarios (API)", pct_univ * 100)

    except Exception as exc:
        logger.debug("INE tabla 9697 no disponible: %s", exc)

    # ── Intento 2: Idescat nivel de estudios ──────────────────────────────────
    if pct_univ is None:
        try:
            # f281: % con estudios superiores (Idescat emex)
            r = await client.get(
                _IDESCAT,
                params={"i": "f281", "res": f"mun:{_BCN_MUN}", "lang": "es", "tipus": "b"},
                timeout=20.0,
            )
            r.raise_for_status()
            val = _idescat_valor(r.json(), "f281")
            if val is not None:
                pct_univ = round(float(val) / 100, 4)
                logger.info("    Idescat educación BCN: %.1f%% (f281)", pct_univ * 100)
        except Exception as exc:
            logger.debug("Idescat f281 no disponible: %s", exc)

    # ── Fallback: media conocida de Barcelona (Cens 2021) ─────────────────────
    if pct_univ is None:
        # Barcelona tiene aprox. 41-43% de población con educación universitaria
        pct_univ = 0.42
        logger.info("    nivel_estudios_alto_pct: usando fallback hardcoded (42%%)")

    # Aplicar a todas las zonas sin dato propio
    ok = 0
    try:
        async with get_db() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (zona_id) zona_id, fecha
                FROM variables_zona
                WHERE nivel_estudios_alto_pct IS NULL
                ORDER BY zona_id, fecha DESC
            """)
            for r in rows:
                await conn.execute("""
                    UPDATE variables_zona
                    SET nivel_estudios_alto_pct = $1
                    WHERE zona_id = $2 AND fecha = $3
                """, pct_univ, r["zona_id"], r["fecha"])
                ok += 1
    except Exception as exc:
        logger.warning("Actualización nivel_estudios error: %s", exc)
    return ok


# =============================================================================
# 3. INE DIRCE — Tasa de rotación empresarial por sector
# =============================================================================

async def _poblar_ine_dirce(client: httpx.AsyncClient) -> int:
    """
    tasa_rotacion_anual: proporción de empresas que cierran cada año.
    Fuente: INE — DIRCE (Directorio Central de Empresas), provincia 08.
    Cálculo: bajas_anuales / (altas + activas) por CNAE mapeado a nuestros sectores.
    Se aplica como línea de base a todas las zonas (dato de provincia, no de barrio).
    """
    # Tasas por sector (se actualizarán con datos de la API si están disponibles)
    tasas: dict[str, float] = {
        "restauracion":  0.21,   # Hostelería — alta rotación
        "tatuajes":      0.15,   # Servicios personales — media
        "moda":          0.23,   # Comercio al por menor — alta rotación
        "estetica":      0.14,   # Servicios personales — media-baja
        "shisha_lounge": 0.25,   # Mix hostelería/ocio — muy alta
    }

    # ── Intento: INE API DIRCE ─────────────────────────────────────────────────
    try:
        # Paso 1: obtener tablas disponibles en la operación DIRCE
        r = await client.get(
            f"{_INE_BASE}/OPERACION_TABLAS/DIRCE",
            params={"lang": "ES"},
            timeout=20.0,
        )
        r.raise_for_status()
        tablas = r.json()

        # Buscar tabla con altas y bajas por CNAE y provincia
        tabla_id = None
        for t in (tablas if isinstance(tablas, list) else []):
            nombre = (t.get("Nombre") or "").lower()
            if "alta" in nombre and "cnae" in nombre and "provincia" in nombre:
                tabla_id = t.get("Id")
                break
            if "baja" in nombre and "actividad" in nombre:
                tabla_id = t.get("Id")
                break

        if tabla_id:
            # Paso 2: descargar tabla y calcular tasas por CNAE
            r2 = await client.get(
                f"{_INE_BASE}/DATOS_TABLA/{tabla_id}",
                params={"nult": "1"},
                timeout=30.0,
            )
            r2.raise_for_status()
            series = r2.json()

            altas:  dict[str, float] = {}
            bajas:  dict[str, float] = {}
            activas: dict[str, float] = {}

            for serie in (series if isinstance(series, list) else []):
                meta  = serie.get("MetaData", [])
                datos = serie.get("Data", [])
                if not datos:
                    continue
                valor = float(datos[0].get("Valor") or 0)
                nombre = " ".join(m.get("Valor", "") for m in meta).lower()
                cnae2  = _extraer_cnae2(meta)
                prov   = _extraer_provincia(meta)
                if prov != _BCN_PROV or not cnae2:
                    continue
                if "alta" in nombre:
                    altas[cnae2]  = altas.get(cnae2,  0) + valor
                elif "baja" in nombre:
                    bajas[cnae2]  = bajas.get(cnae2,  0) + valor
                else:
                    activas[cnae2] = activas.get(cnae2, 0) + valor

            # Calcular tasa por sector
            for sector, cnaes in _SECTOR_CNAE.items():
                total_bajas   = sum(bajas.get(c,  0) for c in cnaes)
                total_activas = sum(activas.get(c, 0) for c in cnaes)
                total_altas   = sum(altas.get(c,  0) for c in cnaes)
                base = total_activas + total_altas
                if base > 0 and total_bajas > 0:
                    tasas[sector] = round(total_bajas / base, 4)

            logger.info("    INE DIRCE: tasas obtenidas para %d sectores", len(tasas))

    except Exception as exc:
        logger.info("    INE DIRCE no disponible (%s) — usando fallback", exc)

    # Aplicar tasa de rotación a todas las zonas (valor de línea de base de provincia)
    ok = 0
    try:
        # Calcular tasa promedio de todos los sectores para zonas sin sector específico
        tasa_media = round(sum(tasas.values()) / len(tasas), 4)

        async with get_db() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (zona_id) zona_id, fecha
                FROM variables_zona
                WHERE tasa_rotacion_anual IS NULL
                ORDER BY zona_id, fecha DESC
            """)
            for r in rows:
                await conn.execute("""
                    UPDATE variables_zona
                    SET tasa_rotacion_anual = $1
                    WHERE zona_id = $2 AND fecha = $3
                """, tasa_media, r["zona_id"], r["fecha"])
                ok += 1
    except Exception as exc:
        logger.warning("Actualización tasa_rotacion error: %s", exc)
    return ok


# =============================================================================
# 4. IDESCAT emex — Indicadores complementarios de Barcelona
# =============================================================================

async def _poblar_idescat(client: httpx.AsyncClient) -> int:
    """
    Obtiene indicadores a nivel municipio desde Idescat emex API.
    Complementa los datos del Padrón BCN para zonas sin dato propio.
    Actualiza: renta_media_hogar, densidad_hab_km2 (si siguen vacíos)
    """
    ok = 0
    try:
        # Indicadores emex:
        #   f172 → densidad de población (hab/km²)
        #   f192 → edad media
        #   f231 → tasa de paro (%)
        #   f261 → renta bruta media por habitante (€)
        r = await client.get(
            _IDESCAT,
            params={
                "i":    "f172,f192,f231,f261",
                "res":  f"mun:{_BCN_MUN}",
                "lang": "es",
                "tipus": "b",
            },
            timeout=20.0,
        )
        r.raise_for_status()
        data = r.json()

        densidad = _idescat_valor(data, "f172")
        renta    = _idescat_valor(data, "f261")

        logger.info("    Idescat emex — densidad: %s, renta_bruta: %s", densidad, renta)

        async with get_db() as conn:
            rows = await conn.fetch("""
                SELECT DISTINCT ON (zona_id) zona_id, fecha
                FROM variables_zona
                ORDER BY zona_id, fecha DESC
            """)
            for row in rows:
                updates: list[str] = []
                vals:    list[Any] = []
                i = 1

                if densidad is not None:
                    # Solo si no hay densidad calculada ya
                    existing = await conn.fetchval(
                        "SELECT densidad_hab_km2 FROM variables_zona "
                        "WHERE zona_id=$1 AND fecha=$2",
                        row["zona_id"], row["fecha"],
                    )
                    if existing is None:
                        updates.append(f"densidad_hab_km2=${i}")
                        vals.append(round(float(densidad), 1)); i += 1

                if renta is not None:
                    existing = await conn.fetchval(
                        "SELECT renta_media_hogar FROM variables_zona "
                        "WHERE zona_id=$1 AND fecha=$2",
                        row["zona_id"], row["fecha"],
                    )
                    if existing is None:
                        # Idescat da renta per cápita; multiplicamos ×2.5 para estimar por hogar
                        renta_hogar = round(float(renta) * 2.5, 0)
                        updates.append(f"renta_media_hogar=${i}")
                        vals.append(renta_hogar); i += 1

                if updates:
                    vals += [row["zona_id"], row["fecha"]]
                    await conn.execute(
                        f"UPDATE variables_zona SET {', '.join(updates)} "
                        f"WHERE zona_id=${i} AND fecha=${i+1}",
                        *vals,
                    )
                    ok += 1

    except Exception as exc:
        logger.warning("Idescat emex error: %s", exc)
    return ok


# =============================================================================
# 5. BCN Open Data — Cens Locals Planta Baixa
# =============================================================================

async def _poblar_cens_locals(client: httpx.AsyncClient) -> int:
    """
    ratio_locales_comerciales: locales con actividad comercial / total locales.
    pct_locales_vacios: locales sin actividad / total locales.
    Fuente: Cens Activitats Econòmiques en Planta Baixa (Open Data BCN).
    """
    ok = 0
    # Intentar varios nombres de recurso CKAN
    tables = [
        "cens-activitats-economiques-en-planta-baixa",
        "cens-locals-planta-baixa",
        "cens-activitats-economiques",
    ]
    rows = []
    for tbl in tables:
        try:
            sql = f"""SELECT "Codi_Barri","Seccio_Activitat","Actiu","Grup_Activitat"
                      FROM "{tbl}" LIMIT 100000"""
            r = await client.get(f"{_CKAN}/datastore_search_sql", params={"sql": sql}, timeout=_TIMEOUT)
            if r.status_code == 200:
                rows = r.json().get("result", {}).get("records", [])
                if rows:
                    logger.info("    Cens Locals: %d registros desde '%s'", len(rows), tbl)
                    break
        except Exception:
            continue

    if not rows:
        logger.info("    Cens Locals: dataset no accesible — saltando")
        return 0

    # Agregar por barrio
    barri_stats: dict[str, dict] = {}
    for row in rows:
        barri = str(row.get("Codi_Barri") or "").zfill(6)
        if not barri or barri == "000000":
            continue
        s = barri_stats.setdefault(barri, {"total": 0, "comercial": 0, "buit": 0})
        s["total"] += 1
        activitat = str(row.get("Seccio_Activitat") or row.get("Grup_Activitat") or "").lower()
        actiu = row.get("Actiu")
        if actiu is False or "buit" in activitat or "vacant" in activitat or "sense" in activitat:
            s["buit"] += 1
        elif activitat and activitat not in ("residencial", "habitatge", ""):
            s["comercial"] += 1

    try:
        async with get_db() as conn:
            for barri_code, stats in barri_stats.items():
                if stats["total"] == 0:
                    continue
                ratio_com = round(stats["comercial"] / stats["total"], 4)
                pct_buit  = round(stats["buit"]      / stats["total"], 4)

                zona_ids = await conn.fetch("""
                    SELECT DISTINCT ON (z.id) z.id AS zona_id,
                           vz.fecha
                    FROM zonas z
                    JOIN barrios b ON b.id = z.barrio_id
                    JOIN variables_zona vz ON vz.zona_id = z.id
                    WHERE b.codigo = $1
                    ORDER BY z.id, vz.fecha DESC
                """, barri_code)

                for z in zona_ids:
                    await conn.execute("""
                        UPDATE variables_zona
                        SET ratio_locales_comerciales = $1,
                            pct_locales_vacios        = $2
                        WHERE zona_id = $3 AND fecha = $4
                    """, ratio_com, pct_buit, z["zona_id"], z["fecha"])
                    ok += 1
    except Exception as exc:
        logger.warning("Actualización cens_locals error: %s", exc)
    return ok


# =============================================================================
# 6. BCN Open Data — Equipaments
# =============================================================================

async def _poblar_equipaments(client: httpx.AsyncClient) -> int:
    """
    score_equipamientos (0-100): escuelas, CAPs, bibliotecas, centros deportivos...
    Fuente: Equipaments i serveis catalogats (Open Data BCN).
    """
    ok = 0
    tables = [
        "equipaments-bcn",
        "equipaments-i-serveis-catalogats",
        "equipaments-municipals",
    ]
    rows = []
    for tbl in tables:
        try:
            sql = f"""SELECT "Barri","Tipus_Equipament","Latitud","Longitud"
                      FROM "{tbl}" LIMIT 50000"""
            r = await client.get(f"{_CKAN}/datastore_search_sql", params={"sql": sql}, timeout=_TIMEOUT)
            if r.status_code == 200:
                rows = r.json().get("result", {}).get("records", [])
                if rows:
                    logger.info("    Equipaments: %d registros desde '%s'", len(rows), tbl)
                    break
        except Exception:
            continue

    if not rows:
        logger.info("    Equipaments: dataset no accesible — saltando")
        return 0

    # Pesos por tipo de equipamiento
    _PESOS = {
        "ensenyament": 1.5, "escola": 1.5, "institut": 1.5, "educació": 1.5,
        "salut": 2.0, "cap": 2.0, "hospital": 2.0, "ambulatori": 2.0,
        "biblioteques": 1.5, "biblioteca": 1.5, "cultural": 1.3, "centre cívic": 1.3,
        "esportiu": 1.0, "piscina": 1.0, "pavelló": 1.0,
        "social": 1.2, "casal": 1.2, "residència": 1.0,
    }

    # Agregar puntuación por barrio
    barri_scores: dict[str, float] = {}
    for row in rows:
        barri = str(row.get("Barri") or "").strip()
        tipus = str(row.get("Tipus_Equipament") or "").lower()
        if not barri:
            continue
        pes = 1.0
        for k, v in _PESOS.items():
            if k in tipus:
                pes = v
                break
        barri_scores[barri] = barri_scores.get(barri, 0) + pes

    if not barri_scores:
        return 0

    # Normalizar a 0-100 (percentil)
    max_score = max(barri_scores.values())
    if max_score == 0:
        return 0

    try:
        async with get_db() as conn:
            for barri_nom, raw_score in barri_scores.items():
                score = min(100.0, round(raw_score / max_score * 100, 1))

                zona_ids = await conn.fetch("""
                    SELECT DISTINCT ON (z.id) z.id AS zona_id,
                           vz.fecha
                    FROM zonas z
                    JOIN barrios b ON b.id = z.barrio_id
                    JOIN variables_zona vz ON vz.zona_id = z.id
                    WHERE LOWER(b.nombre) = LOWER($1)
                    ORDER BY z.id, vz.fecha DESC
                """, barri_nom)

                for z in zona_ids:
                    await conn.execute("""
                        UPDATE variables_zona
                        SET score_equipamientos = $1
                        WHERE zona_id = $2 AND fecha = $3
                    """, score, z["zona_id"], z["fecha"])
                    ok += 1
    except Exception as exc:
        logger.warning("Actualización equipaments error: %s", exc)
    return ok


# =============================================================================
# 7. BCN Open Data — Zones verdes
# =============================================================================

async def _poblar_zones_verdes(client: httpx.AsyncClient) -> int:
    """
    m2_zonas_verdes_cercanas: m² de parques y jardines catalogados por barrio.
    Fuente: Zones Verdes catalogades (Open Data BCN).
    """
    ok = 0
    tables = [
        "zones-verdes-catalogades-bcn",
        "parcs-i-jardins-bcn",
        "zones-verdes",
        "zones-verdes-i-biodiversitat",
    ]
    rows = []
    for tbl in tables:
        try:
            sql = f"""SELECT "Codi_Barri","Nom_Barri","Area_Ha","Tipologia"
                      FROM "{tbl}" LIMIT 10000"""
            r = await client.get(f"{_CKAN}/datastore_search_sql", params={"sql": sql}, timeout=_TIMEOUT)
            if r.status_code == 200:
                rows = r.json().get("result", {}).get("records", [])
                if rows:
                    logger.info("    Zones verdes: %d registros desde '%s'", len(rows), tbl)
                    break
        except Exception:
            continue

    if not rows:
        logger.info("    Zones verdes: dataset no accesible — saltando")
        return 0

    # Sumar área por barrio
    barri_m2: dict[str, float] = {}
    for row in rows:
        area_ha = float(row.get("Area_Ha") or row.get("area_ha") or 0)
        barri   = str(row.get("Codi_Barri") or "").zfill(6)
        if barri and barri != "000000":
            barri_m2[barri] = barri_m2.get(barri, 0) + area_ha * 10_000  # Ha → m²

    try:
        async with get_db() as conn:
            for barri_code, m2 in barri_m2.items():
                zona_ids = await conn.fetch("""
                    SELECT DISTINCT ON (z.id) z.id AS zona_id,
                           vz.fecha
                    FROM zonas z
                    JOIN barrios b ON b.id = z.barrio_id
                    JOIN variables_zona vz ON vz.zona_id = z.id
                    WHERE b.codigo = $1
                    ORDER BY z.id, vz.fecha DESC
                """, barri_code)
                for z in zona_ids:
                    await conn.execute("""
                        UPDATE variables_zona
                        SET m2_zonas_verdes_cercanas = $1
                        WHERE zona_id = $2 AND fecha = $3
                    """, round(m2, 0), z["zona_id"], z["fecha"])
                    ok += 1
    except Exception as exc:
        logger.warning("Actualización zones_verdes error: %s", exc)
    return ok


# =============================================================================
# 8. BCN Open Data — Incidents Guàrdia Urbana
# =============================================================================

async def _poblar_incidents_gub(client: httpx.AsyncClient) -> int:
    """
    incidencias_por_1000hab: incidencias de la Guàrdia Urbana por 1000 habitantes.
    Fuente: Estadística d'incidents de la GUB (Open Data BCN).
    """
    ok = 0
    # Intentar con el año más reciente disponible (hacia atrás)
    tables = [
        "guia-i-incidents-gub-bcn-2024",
        "guia-i-incidents-gub-bcn-2023",
        "incidents-gub-bcn-2023",
        "incidents-gub-bcn-2022",
        "estadistica-incidents-gub",
    ]
    rows = []
    for tbl in tables:
        try:
            sql = f"""SELECT "Barri","Codi_Barri","Nom_Barri","Mes_Any","Any"
                      FROM "{tbl}" LIMIT 100000"""
            r = await client.get(f"{_CKAN}/datastore_search_sql", params={"sql": sql}, timeout=_TIMEOUT)
            if r.status_code == 200:
                rows = r.json().get("result", {}).get("records", [])
                if rows:
                    logger.info("    Incidents GUB: %d registros desde '%s'", len(rows), tbl)
                    break
        except Exception:
            continue

    if not rows:
        logger.info("    Incidents GUB: dataset no accesible — saltando")
        return 0

    # Contar incidents por barrio (código o nombre)
    barri_count: dict[str, int] = {}
    barri_name_to_code: dict[str, str] = {}

    for row in rows:
        barri_code = str(row.get("Codi_Barri") or "").zfill(6)
        barri_nom  = str(row.get("Barri") or row.get("Nom_Barri") or "")
        if barri_code and barri_code != "000000":
            barri_count[barri_code] = barri_count.get(barri_code, 0) + 1
            if barri_nom:
                barri_name_to_code[barri_nom.lower()] = barri_code
        elif barri_nom:
            k = barri_nom.lower()
            barri_count[k] = barri_count.get(k, 0) + 1

    try:
        async with get_db() as conn:
            for key, count in barri_count.items():
                # Buscar por código o por nombre
                if key.isdigit() or (len(key) == 6 and key.startswith("0")):
                    zona_ids = await conn.fetch("""
                        SELECT DISTINCT ON (z.id) z.id AS zona_id,
                               vz.fecha, vz.poblacion
                        FROM zonas z
                        JOIN barrios b ON b.id = z.barrio_id
                        JOIN variables_zona vz ON vz.zona_id = z.id
                        WHERE b.codigo = $1
                        ORDER BY z.id, vz.fecha DESC
                    """, key)
                else:
                    zona_ids = await conn.fetch("""
                        SELECT DISTINCT ON (z.id) z.id AS zona_id,
                               vz.fecha, vz.poblacion
                        FROM zonas z
                        JOIN barrios b ON b.id = z.barrio_id
                        JOIN variables_zona vz ON vz.zona_id = z.id
                        WHERE LOWER(b.nombre) = LOWER($1)
                        ORDER BY z.id, vz.fecha DESC
                    """, key)

                for z in zona_ids:
                    pob = z["poblacion"] or 1000
                    inc_per_1000 = round(count / (pob / 1000), 2)
                    await conn.execute("""
                        UPDATE variables_zona
                        SET incidencias_por_1000hab = $1
                        WHERE zona_id = $2 AND fecha = $3
                    """, inc_per_1000, z["zona_id"], z["fecha"])
                    ok += 1
    except Exception as exc:
        logger.warning("Actualización incidents_gub error: %s", exc)
    return ok


# =============================================================================
# 9. BCN Open Data — Mapa acústic
# =============================================================================

async def _poblar_soroll(client: httpx.AsyncClient) -> int:
    """
    nivel_ruido_db: nivel de ruido en decibelios (Leq24h).
    Fuente: Mapa de Soroll de Barcelona (Open Data BCN).
    Si el dataset no está disponible como datastore, estima según actividad comercial.
    """
    ok = 0
    tables = [
        "mapa-de-soroll-bcn",
        "mapa-de-soroll-de-barcelona",
        "mapa-acoustic-barcelona",
        "mapa-acustic-bcn",
        "soroll-trafic-barcelona",
    ]
    rows = []
    for tbl in tables:
        try:
            sql = f"""SELECT "Codi_Barri","Nom_Barri","Leq24","Ld","Ln"
                      FROM "{tbl}" LIMIT 10000"""
            r = await client.get(f"{_CKAN}/datastore_search_sql", params={"sql": sql}, timeout=_TIMEOUT)
            if r.status_code == 200:
                rows = r.json().get("result", {}).get("records", [])
                if rows:
                    logger.info("    Mapa acústic: %d registros desde '%s'", len(rows), tbl)
                    break
        except Exception:
            continue

    if rows:
        # Calcular media Leq24 por barrio
        barri_db: dict[str, list] = {}
        for row in rows:
            barri = str(row.get("Codi_Barri") or "").zfill(6)
            db    = float(row.get("Leq24") or row.get("Ld") or 0)
            if barri and barri != "000000" and db > 0:
                barri_db.setdefault(barri, []).append(db)

        try:
            async with get_db() as conn:
                for barri_code, dbs in barri_db.items():
                    media_db = round(sum(dbs) / len(dbs), 1)
                    zona_ids = await conn.fetch("""
                        SELECT DISTINCT ON (z.id) z.id AS zona_id, vz.fecha
                        FROM zonas z
                        JOIN barrios b ON b.id = z.barrio_id
                        JOIN variables_zona vz ON vz.zona_id = z.id
                        WHERE b.codigo = $1
                        ORDER BY z.id, vz.fecha DESC
                    """, barri_code)
                    for z in zona_ids:
                        await conn.execute("""
                            UPDATE variables_zona SET nivel_ruido_db = $1
                            WHERE zona_id = $2 AND fecha = $3
                        """, media_db, z["zona_id"], z["fecha"])
                        ok += 1
        except Exception as exc:
            logger.warning("Actualización soroll error: %s", exc)

    else:
        # El mapa acústic no está en el datastore — estimar desde ratio comercial
        # Fórmula: ruido_base (58dB) + aportación comercial (hasta +20dB)
        logger.info("    Mapa acústic: usando estimación desde ratio_locales_comerciales")
        try:
            async with get_db() as conn:
                rows_vz = await conn.fetch("""
                    SELECT DISTINCT ON (zona_id) zona_id, fecha,
                           ratio_locales_comerciales, flujo_peatonal_total
                    FROM variables_zona
                    WHERE nivel_ruido_db IS NULL
                    ORDER BY zona_id, fecha DESC
                """)
                for r in rows_vz:
                    ratio = r["ratio_locales_comerciales"] or 0.3
                    flujo = r["flujo_peatonal_total"]      or 500
                    # Estimación: zona residencial ~55-60dB, zona comercial ~65-75dB
                    db_est = round(55 + ratio * 15 + min(flujo / 500, 1.0) * 5, 1)
                    await conn.execute("""
                        UPDATE variables_zona SET nivel_ruido_db = $1
                        WHERE zona_id = $2 AND fecha = $3
                    """, db_est, r["zona_id"], r["fecha"])
                    ok += 1
        except Exception as exc:
            logger.warning("Estimación soroll error: %s", exc)

    return ok


# =============================================================================
# 10. SCORE TURISMO — calculado desde negocios_activos en BD
# =============================================================================

async def _calcular_score_turismo() -> int:
    """
    score_turismo (0-100): presencia de hoteles, restaurantes turísticos,
    atracciones y tiendas de souvenirs en el área de la zona.
    Se calcula desde la tabla negocios_activos (ya populada por el pipeline de reseñas).
    Si negocios_activos está vacía, usa ratio_locales_comerciales como proxy.
    """
    ok = 0
    try:
        async with get_db() as conn:
            # Comprobar si negocios_activos tiene datos
            n_neg = await conn.fetchval("SELECT COUNT(*) FROM negocios_activos LIMIT 1")

            if n_neg:
                # Score basado en densidad de negocios turísticos cercanos
                rows = await conn.fetch("""
                    SELECT DISTINCT ON (vz.zona_id) vz.zona_id,
                           vz.fecha,
                           COUNT(na.id) FILTER (
                               WHERE ST_DWithin(
                                   ST_SetSRID(ST_MakePoint(na.lng, na.lat), 4326)::geography,
                                   z.geometria::geography,
                                   500
                               )
                           ) AS n_negocios
                    FROM variables_zona vz
                    JOIN zonas z ON z.id = vz.zona_id
                    LEFT JOIN negocios_activos na ON na.activo = TRUE
                    WHERE vz.score_turismo IS NULL
                    GROUP BY vz.zona_id, vz.fecha, z.geometria
                    ORDER BY vz.zona_id, vz.fecha DESC
                """)
                max_n = max((r["n_negocios"] or 0 for r in rows), default=1) or 1
                for r in rows:
                    score = min(100.0, round((r["n_negocios"] or 0) / max_n * 80, 1))
                    await conn.execute("""
                        UPDATE variables_zona SET score_turismo = $1
                        WHERE zona_id = $2 AND fecha = $3
                    """, score, r["zona_id"], r["fecha"])
                    ok += 1
            else:
                # Fallback: usar ratio_locales_comerciales como proxy de actividad
                rows = await conn.fetch("""
                    SELECT DISTINCT ON (zona_id) zona_id, fecha,
                           ratio_locales_comerciales
                    FROM variables_zona
                    WHERE score_turismo IS NULL
                    ORDER BY zona_id, fecha DESC
                """)
                for r in rows:
                    ratio = r["ratio_locales_comerciales"] or 0.3
                    # Zonas muy comerciales = mayor afluencia turística potencial
                    score = round(min(100.0, ratio * 100 * 0.7), 1)
                    await conn.execute("""
                        UPDATE variables_zona SET score_turismo = $1
                        WHERE zona_id = $2 AND fecha = $3
                    """, score, r["zona_id"], r["fecha"])
                    ok += 1

    except Exception as exc:
        logger.warning("_calcular_score_turismo error: %s", exc)
    return ok


# =============================================================================
# HELPERS
# =============================================================================

def _idescat_valor(data: dict, indicador: str) -> Optional[float]:
    """Extrae el valor de un indicador de la respuesta emex de Idescat."""
    try:
        ind = data.get("emex", {}).get("i", {}).get(indicador, {})
        d   = ind.get("d", {})
        # El primer (y único) elemento de d
        primer = next(iter(d.values()), {})
        val = primer.get("v")
        if val is not None:
            return float(str(val).replace(",", "."))
    except Exception:
        pass
    return None


def _extraer_cnae2(meta: list[dict]) -> Optional[str]:
    """Extrae el código CNAE-2009 de 2 dígitos de los metadatos de una serie INE."""
    for m in meta:
        codigo = str(m.get("Codigo") or "")
        nombre = str(m.get("Nombre") or m.get("Valor") or "").lower()
        if "cnae" in nombre and codigo.isdigit() and 10 <= int(codigo) <= 99:
            return codigo[:2]
        if codigo.isdigit() and 10 <= int(codigo) <= 99:
            return codigo[:2]
    return None


def _extraer_provincia(meta: list[dict]) -> Optional[str]:
    """Extrae el código de provincia (2 dígitos) de los metadatos de una serie INE."""
    for m in meta:
        codigo = str(m.get("Codigo") or "")
        if len(codigo) == 2 and codigo.isdigit():
            return codigo
        if len(codigo) >= 2 and codigo[:2].isdigit():
            return codigo[:2]
    return None


# =============================================================================
# CONTROL DE PIPELINE
# =============================================================================

async def _init() -> int:
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones(pipeline, estado) "
            "VALUES('ine_idescat', 'running') RETURNING id"
        )


async def _fin(eid: int, registros: int, estado: str, mensaje: str = None) -> None:
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones "
            "SET fecha_fin=NOW(), registros=$1, estado=$2, mensaje_error=$3 "
            "WHERE id=$4",
            registros, estado, mensaje, eid,
        )
