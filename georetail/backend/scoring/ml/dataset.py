"""
scoring/dataset.py — Construcción del dataset de entrenamiento XGBoost.

Fuente de verdad:
  - `negocios_historico` → negocios reales de Barcelona con fecha de apertura
    y cierre (si cerraron). Poblado por `pipelines/registre_mercantil.py`.
  - `variables_zona` → features del entorno en la fecha de apertura del negocio.
  - `competencia_por_local` → competencia en radio 300m en la fecha de apertura.
  - `precios_alquiler_zona` → precio m² en la fecha de apertura.

Variable objetivo (label):
  - 1 → el negocio sobrevivió ≥ 3 años (o sigue activo)
  - 0 → cerró antes de 3 años

El dataset resultante sigue el orden de `FEATURE_NAMES` (ver scoring/features.py)
y una columna `label` binaria.

Uso:
    from scoring.ml.dataset import construir_dataset
    X, y, meta = await construir_dataset(sector="restauracion")
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

import numpy as np
import pandas as pd

from db.conexion import get_db
from scoring.infra.governance import (
    DEMOGRAFIA_SELECTABLE_EXISTING_FEATURES,
)
from scoring.features import FEATURE_NAMES, _MEDIAS

logger = logging.getLogger(__name__)

# Un negocio que supera 3 años se considera "superviviente"
_UMBRAL_SUPERVIVENCIA_DIAS = 365 * 3

# Mínimo de registros para entrenar — por debajo de esto no tiene sentido
_MIN_REGISTROS = 200
_DEMOGRAFIA_AUDIT_FEATURES = DEMOGRAFIA_SELECTABLE_EXISTING_FEATURES


async def construir_dataset(
    sector: Optional[str] = None,
    fecha_corte: Optional[date] = None,
) -> tuple[np.ndarray, np.ndarray, pd.DataFrame]:
    """
    Construye el dataset de entrenamiento completo.

    Args:
        sector: Filtrar por sector (ej: 'restauracion'). None = todos los sectores.
        fecha_corte: Ignorar negocios abiertos después de esta fecha.
                     Por defecto: hoy - 3 años (para poder calcular la supervivencia).

    Returns:
        X     → np.ndarray (n_samples, len(FEATURE_NAMES)) con las features normalizadas
        y     → np.ndarray (n_samples,)    con los labels (0/1)
        meta  → pd.DataFrame con columnas auxiliares (negocio_id, zona_id, sector,
                fecha_apertura, fecha_cierre, label) para análisis posterior.

    Raises:
        ValueError: Si hay menos de _MIN_REGISTROS ejemplos tras filtrar.
    """
    X, y, meta, _ = await _construir_dataset_interno(
        sector=sector,
        fecha_corte=fecha_corte,
        incluir_auditoria=False,
    )
    return X, y, meta

    if fecha_corte is None:
        fecha_corte = date.today() - timedelta(days=_UMBRAL_SUPERVIVENCIA_DIAS)

    logger.info(
        "Construyendo dataset — sector=%s | fecha_corte=%s",
        sector or "todos", fecha_corte,
    )

    # ── 1. Cargar negocios históricos ─────────────────────────────────────────
    negocios = await _cargar_negocios(sector=sector, fecha_corte=fecha_corte)

    if len(negocios) < _MIN_REGISTROS:
        raise ValueError(
            f"Dataset insuficiente: {len(negocios)} registros "
            f"(mínimo requerido: {_MIN_REGISTROS}). "
            "Ejecuta primero `pipelines/registre_mercantil.py`."
        )

    logger.info("Negocios cargados: %d (positivos=%d, negativos=%d)",
        len(negocios),
        sum(1 for n in negocios if n["label"] == 1),
        sum(1 for n in negocios if n["label"] == 0),
    )

    # ── 2. Construir features para cada negocio ───────────────────────────────
    # Las features se extraen de la snapshot histórica más cercana a la fecha
    # de apertura del negocio — no los datos actuales. Esto es crítico para
    # evitar data leakage: el modelo nunca ve información futura.
    zona_ids      = [n["zona_id"]      for n in negocios]
    sectores      = [n["sector_codigo"] for n in negocios]
    fechas_ap     = [n["fecha_apertura"] for n in negocios]

    features_rows = await _construir_features_historicas(
        zona_ids=zona_ids,
        sectores=sectores,
        fechas=fechas_ap,
    )

    # ── 3. Ensamblar matrices ─────────────────────────────────────────────────
    X_list: list[list[float]] = []
    y_list: list[int]         = []
    meta_list: list[dict]     = []

    skipped = 0
    for negocio, features in zip(negocios, features_rows):
        if features is None:
            # Sin datos históricos para esta zona en esa fecha → descartar
            skipped += 1
            continue

        X_list.append(features)
        y_list.append(negocio["label"])
        meta_list.append({
            "negocio_id":    negocio["id"],
            "zona_id":       negocio["zona_id"],
            "sector":        negocio["sector_codigo"],
            "fecha_apertura": negocio["fecha_apertura"],
            "fecha_cierre":  negocio.get("fecha_cierre"),
            "label":         negocio["label"],
        })

    if skipped > 0:
        logger.warning(
            "%d negocios descartados por falta de datos históricos en su zona",
            skipped,
        )

    X   = np.array(X_list, dtype=np.float32)
    y   = np.array(y_list,  dtype=np.int32)
    meta = pd.DataFrame(meta_list)

    logger.info(
        "Dataset final: %d samples | %d features | ratio positivos=%.1f%%",
        len(X), X.shape[1], y.mean() * 100,
    )

    return X, y, meta


# ─── Queries internas ─────────────────────────────────────────────────────────

async def construir_dataset_auditoria(
    sector: Optional[str] = None,
    fecha_corte: Optional[date] = None,
) -> tuple[np.ndarray, np.ndarray, pd.DataFrame, pd.DataFrame]:
    return await _construir_dataset_interno(
        sector=sector,
        fecha_corte=fecha_corte,
        incluir_auditoria=True,
    )


async def _construir_dataset_interno(
    sector: Optional[str],
    fecha_corte: Optional[date],
    *,
    incluir_auditoria: bool,
) -> tuple[np.ndarray, np.ndarray, pd.DataFrame, pd.DataFrame]:
    if fecha_corte is None:
        fecha_corte = date.today() - timedelta(days=_UMBRAL_SUPERVIVENCIA_DIAS)

    logger.info(
        "Construyendo dataset â€” sector=%s | fecha_corte=%s",
        sector or "todos", fecha_corte,
    )

    negocios = await _cargar_negocios(sector=sector, fecha_corte=fecha_corte)

    if len(negocios) < _MIN_REGISTROS:
        raise ValueError(
            f"Dataset insuficiente: {len(negocios)} registros "
            f"(mÃ­nimo requerido: {_MIN_REGISTROS}). "
            "Ejecuta primero `pipelines/registre_mercantil.py`."
        )

    logger.info(
        "Negocios cargados: %d (positivos=%d, negativos=%d)",
        len(negocios),
        sum(1 for n in negocios if n["label"] == 1),
        sum(1 for n in negocios if n["label"] == 0),
    )

    zona_ids = [n["zona_id"] for n in negocios]
    sectores = [n["sector_codigo"] for n in negocios]
    fechas_ap = [n["fecha_apertura"] for n in negocios]

    features_rows = await _construir_features_historicas_detalladas(
        zona_ids=zona_ids,
        sectores=sectores,
        fechas=fechas_ap,
    )

    X_list: list[list[float]] = []
    y_list: list[int] = []
    meta_list: list[dict] = []
    audit_list: list[dict] = []

    skipped = 0
    for negocio, feature_bundle in zip(negocios, features_rows):
        if feature_bundle is None:
            skipped += 1
            continue

        coverage = feature_bundle["demografia_cobertura"]
        missing_count = sum(1 for present in coverage.values() if not present)

        X_list.append(feature_bundle["vector"])
        y_list.append(negocio["label"])
        meta_list.append(
            {
                "negocio_id": negocio["id"],
                "zona_id": negocio["zona_id"],
                "sector": negocio["sector_codigo"],
                "fecha_apertura": negocio["fecha_apertura"],
                "fecha_cierre": negocio.get("fecha_cierre"),
                "label": negocio["label"],
            }
        )

        if incluir_auditoria:
            audit_list.append(
                {
                    "negocio_id": negocio["id"],
                    "zona_id": negocio["zona_id"],
                    "sector": negocio["sector_codigo"],
                    "label": negocio["label"],
                    "demografia_missing_count": missing_count,
                    "demografia_missing_ratio": round(
                        missing_count / max(len(coverage), 1),
                        4,
                    ),
                    **{
                        f"{feature}_present": bool(present)
                        for feature, present in coverage.items()
                    },
                }
            )

    if skipped > 0:
        logger.warning(
            "%d negocios descartados por falta de datos histÃ³ricos en su zona",
            skipped,
        )

    X = np.array(X_list, dtype=np.float32)
    y = np.array(y_list, dtype=np.int32)
    meta = pd.DataFrame(meta_list)
    audit = pd.DataFrame(audit_list)

    logger.info(
        "Dataset final: %d samples | %d features | ratio positivos=%.1f%%",
        len(X), X.shape[1], y.mean() * 100,
    )

    return X, y, meta, audit


async def _cargar_negocios(
    sector: Optional[str],
    fecha_corte: date,
) -> list[dict]:
    """
    Carga negocios de `negocios_historico` y calcula el label de supervivencia.

    La tabla `negocios_historico` tiene:
      - id, zona_id, sector_codigo
      - fecha_apertura (date)
      - fecha_cierre   (date | NULL si sigue activo)
      - fuente         ('registre_mercantil' | 'google_places' | 'manual')
    """
    query = """
        SELECT
            id,
            zona_id,
            sector_codigo,
            fecha_apertura,
            fecha_cierre,
            CASE
                WHEN fecha_cierre IS NULL
                    THEN 1  -- Sigue activo → superviviente
                WHEN fecha_cierre - fecha_apertura >= $1
                    THEN 1  -- Aguantó ≥ 3 años → superviviente
                ELSE 0      -- Cerró antes de 3 años → fracaso
            END AS label
        FROM negocios_historico
        WHERE fecha_apertura <= $2
          AND ($3::text IS NULL OR sector_codigo = $3)
          -- Excluir negocios sin suficiente historia observable:
          -- (la condición fecha_apertura <= $2 de la línea anterior ya excluye
          --  los que abrieron después de fecha_corte, por lo que no es necesario
          --  un AND NOT adicional)
        ORDER BY fecha_apertura
    """

    async with get_db() as conn:
        rows = await conn.fetch(
            query,
            _UMBRAL_SUPERVIVENCIA_DIAS,
            fecha_corte,
            sector,
        )

    return [dict(r) for r in rows]


async def _construir_features_historicas(
    zona_ids: list[str],
    sectores: list[str],
    fechas:   list[date],
) -> list[Optional[list[float]]]:
    """
    Para cada (zona_id, sector, fecha_apertura) construye el vector de features
    usando la snapshot de datos más cercana ANTERIOR a esa fecha.

    Esto evita data leakage — el modelo ve cómo era el entorno cuando el negocio
    abrió, no cómo es hoy.

    Devuelve una lista del mismo tamaño que los inputs, con None si no hay datos.
    """
    detalles = await _construir_features_historicas_detalladas(
        zona_ids=zona_ids,
        sectores=sectores,
        fechas=fechas,
    )
    return [item["vector"] if item is not None else None for item in detalles]

    async with get_db() as conn:
        trans_query = """
            SELECT
                z.id AS zona_id,
                COUNT(DISTINCT pl.linea_id)::int AS num_lineas,
                COUNT(DISTINCT pt.id)::int        AS num_paradas
            FROM zonas z
            JOIN paradas_transporte pt
                ON ST_DWithin(pt.geometria::geography, z.geometria::geography, 500)
            JOIN paradas_lineas pl ON pl.parada_id = pt.id
            WHERE z.id = ANY($1)
            GROUP BY z.id
        """

        # ── Per-negocio snapshots (evita data leakage) ───────────────────────
        # Usamos unnest+ordinality para hacer una sola query que devuelve
        # la snapshot correcta para cada negocio según su fecha_apertura.
        # Esto es crucial: un negocio de 2015 debe ver las variables de 2015,
        # no las actuales de 2024. Sin esto el modelo tiene data leakage.

        # Queries por-negocio usando unnest
        vz_per_negocio_query = """
            WITH input AS (
                SELECT (ordinality - 1)::int AS idx, zone_id, fecha_ref
                FROM unnest($1::text[], $2::date[]) WITH ORDINALITY AS t(zone_id, fecha_ref, ordinality)
            )
            SELECT DISTINCT ON (i.idx)
                i.idx,
                vz.flujo_peatonal_manana, vz.flujo_peatonal_tarde, vz.flujo_peatonal_noche,
                vz.flujo_peatonal_total,
                vz.renta_media_hogar, vz.edad_media, vz.pct_extranjeros, vz.densidad_hab_km2,
                vz.pct_locales_vacios, vz.tasa_rotacion_anual,
                vz.score_turismo, vz.incidencias_por_1000hab,
                vz.nivel_ruido_db, vz.score_equipamientos, vz.m2_zonas_verdes_cercanas,
                vz.ratio_locales_comerciales,
                -- v3: turismo y dinamismo comercial
                vz.airbnb_density_500m,
                vz.airbnb_occupancy_est,
                vz.licencias_nuevas_1a,
                vz.eventos_culturales_500m,
                vz.booking_hoteles_500m,
                ST_Distance(
                    ST_Centroid(z.geometria)::geography,
                    ST_GeomFromText(
                        'LINESTRING(2.1850 41.3740,2.1940 41.3792,2.2030 41.3840,'
                        '2.2130 41.3900,2.2250 41.3970,2.2380 41.4020)', 4326
                    )::geography
                )::int AS dist_playa_m
            FROM input i
            JOIN variables_zona vz ON vz.zona_id = i.zone_id AND vz.fecha <= i.fecha_ref
            JOIN zonas z ON z.id = vz.zona_id
            ORDER BY i.idx, vz.fecha DESC
        """

        comp_per_negocio_query = """
            WITH input AS (
                SELECT (ordinality - 1)::int AS idx, zone_id, sec, fecha_ref
                FROM unnest($1::text[], $2::text[], $3::date[]) WITH ORDINALITY
                    AS t(zone_id, sec, fecha_ref, ordinality)
            )
            SELECT DISTINCT ON (i.idx)
                i.idx,
                cp.num_competidores, cp.rating_medio, cp.score_saturacion
            FROM input i
            JOIN competencia_por_local cp
                ON cp.zona_id = i.zone_id
                AND cp.sector_codigo = i.sec
                AND cp.radio_m = 300
                AND cp.fecha_calculo <= i.fecha_ref
            ORDER BY i.idx, cp.fecha_calculo DESC
        """

        precio_per_negocio_query = """
            WITH input AS (
                SELECT (ordinality - 1)::int AS idx, zone_id, fecha_ref
                FROM unnest($1::text[], $2::date[]) WITH ORDINALITY AS t(zone_id, fecha_ref, ordinality)
            )
            SELECT DISTINCT ON (i.idx)
                i.idx,
                paz.precio_m2
            FROM input i
            JOIN precios_alquiler_zona paz
                ON paz.zona_id = i.zone_id
                AND paz.fecha <= i.fecha_ref
            ORDER BY i.idx, paz.fecha DESC
        """

        # v3: media de reseñas Google de negocios activos en 300m por zona
        # (no depende de fecha histórica — es un proxy del dinamismo actual de la zona)
        google_reviews_query = """
            SELECT
                z.id AS zona_id,
                AVG(na.review_count)::float AS avg_reviews
            FROM zonas z
            JOIN negocios_activos na
                ON na.activo = TRUE
               AND ST_DWithin(na.geometria::geography, z.geometria::geography, 300)
               AND na.review_count > 0
            WHERE z.id = ANY($1)
            GROUP BY z.id
        """

        # Transporte no depende de fecha (la red es relativamente estable)
        zona_ids_unicos = list(set(zona_ids))
        trans_rows = await conn.fetch(trans_query, zona_ids_unicos)
        trans_dict: dict[str, dict] = {r["zona_id"]: dict(r) for r in trans_rows}

        # Per-negocio snapshots (indexed by position, same order as zona_ids/fechas)
        vz_rows = await conn.fetch(vz_per_negocio_query, zona_ids, fechas)
        vz_by_idx: dict[int, dict] = {r["idx"]: dict(r) for r in vz_rows}

        comp_rows = await conn.fetch(comp_per_negocio_query, zona_ids, sectores, fechas)
        comp_by_idx: dict[int, dict] = {r["idx"]: dict(r) for r in comp_rows}

        precio_rows = await conn.fetch(precio_per_negocio_query, zona_ids, fechas)
        precio_by_idx: dict[int, Optional[float]] = {
            r["idx"]: float(r["precio_m2"]) for r in precio_rows
        }

        # v3: google reviews por zona (batch, no por índice)
        google_rows = await conn.fetch(google_reviews_query, zona_ids_unicos)
        google_reviews_dict: dict[str, Optional[float]] = {
            r["zona_id"]: r["avg_reviews"] for r in google_rows
        }

    # ── Construir vector de features por negocio ──────────────────────────────
    resultados: list[Optional[list[float]]] = []

    for idx, zona_id in enumerate(zona_ids):
        vz    = vz_by_idx.get(idx, {})
        comp  = comp_by_idx.get(idx, {})
        precio = precio_by_idx.get(idx)
        trans = trans_dict.get(zona_id, {})

        # Si no hay datos de variables_zona para esta zona → descartamos
        if not vz:
            resultados.append(None)
            continue

        total = vz.get("flujo_peatonal_total") or 0

        raw = {
            "flujo_peatonal_total":     total,
            "flujo_manana_pct":         (vz.get("flujo_peatonal_manana") or 0) / total if total else None,
            "flujo_tarde_pct":          (vz.get("flujo_peatonal_tarde")  or 0) / total if total else None,
            "flujo_noche_pct":          (vz.get("flujo_peatonal_noche")  or 0) / total if total else None,
            "renta_media_hogar":        vz.get("renta_media_hogar"),
            "edad_media":               vz.get("edad_media"),
            "pct_extranjeros":          vz.get("pct_extranjeros"),
            "densidad_hab_km2":         vz.get("densidad_hab_km2"),
            "num_competidores_300m":    comp.get("num_competidores"),
            "rating_medio_competidores": comp.get("rating_medio"),
            "score_saturacion":         comp.get("score_saturacion"),
            "precio_m2_alquiler":       precio,
            "pct_locales_vacios":       vz.get("pct_locales_vacios"),
            "tasa_rotacion_anual":      vz.get("tasa_rotacion_anual"),
            "score_turismo":            vz.get("score_turismo"),
            "incidencias_por_1000hab":  vz.get("incidencias_por_1000hab"),
            "nivel_ruido_db":           vz.get("nivel_ruido_db"),
            "score_equipamientos":      vz.get("score_equipamientos"),
            "num_lineas_transporte":    trans.get("num_lineas"),
            "num_paradas_500m":         trans.get("num_paradas"),
            "m2_zonas_verdes_cercanas": vz.get("m2_zonas_verdes_cercanas"),
            # v2: granularidad geográfica de nivel zona
            "dist_playa_m":              vz.get("dist_playa_m"),
            "ratio_locales_comerciales": vz.get("ratio_locales_comerciales"),
            # v3: turismo y dinamismo comercial
            "airbnb_density_500m":       vz.get("airbnb_density_500m"),
            "airbnb_occupancy_est":      vz.get("airbnb_occupancy_est"),
            "google_review_count_medio": google_reviews_dict.get(zona_id),
            "licencias_nuevas_1a":       vz.get("licencias_nuevas_1a"),
            "eventos_culturales_500m":   vz.get("eventos_culturales_500m"),
            "booking_hoteles_500m":      vz.get("booking_hoteles_500m"),
        }

        # Imputar con medias del dataset de entrenamiento donde haya None
        vec = [
            float(raw[f]) if raw.get(f) is not None else _MEDIAS[f]
            for f in FEATURE_NAMES
        ]
        resultados.append(vec)

    return resultados


async def _construir_features_historicas_detalladas(
    zona_ids: list[str],
    sectores: list[str],
    fechas: list[date],
) -> list[Optional[dict]]:
    async with get_db() as conn:
        trans_query = """
            SELECT
                z.id AS zona_id,
                COUNT(DISTINCT pl.linea_id)::int AS num_lineas,
                COUNT(DISTINCT pt.id)::int        AS num_paradas
            FROM zonas z
            JOIN paradas_transporte pt
                ON ST_DWithin(pt.geometria::geography, z.geometria::geography, 500)
            JOIN paradas_lineas pl ON pl.parada_id = pt.id
            WHERE z.id = ANY($1)
            GROUP BY z.id
        """

        vz_per_negocio_query = """
            WITH input AS (
                SELECT (ordinality - 1)::int AS idx, zone_id, fecha_ref
                FROM unnest($1::text[], $2::date[]) WITH ORDINALITY AS t(zone_id, fecha_ref, ordinality)
            )
            SELECT DISTINCT ON (i.idx)
                i.idx,
                vz.flujo_peatonal_manana, vz.flujo_peatonal_tarde, vz.flujo_peatonal_noche,
                vz.flujo_peatonal_total,
                vz.renta_media_hogar, vz.edad_media, vz.pct_extranjeros, vz.densidad_hab_km2,
                vz.pct_locales_vacios, vz.tasa_rotacion_anual,
                vz.score_turismo, vz.incidencias_por_1000hab,
                vz.nivel_ruido_db, vz.score_equipamientos, vz.m2_zonas_verdes_cercanas,
                vz.ratio_locales_comerciales,
                vz.airbnb_density_500m,
                vz.airbnb_occupancy_est,
                vz.licencias_nuevas_1a,
                vz.eventos_culturales_500m,
                vz.booking_hoteles_500m,
                vz.pct_poblacio_25_44,
                vz.delta_renta_3a,
                vz.nivel_estudios_alto_pct,
                vz.gini,
                vz.p80_p20,
                vz.tamano_hogar,
                vz.hogares_con_menores,
                vz.personas_solas,
                vz.renta_media_uc,
                vz.renta_mediana_uc,
                vz.seasonality_summer_lift,
                vz.seasonality_christmas_lift,
                vz.seasonality_rebajas_lift,
                vz.seasonality_volatility,
                vz.seasonality_peak_concentration,
                vz.weekend_lift,
                vz.sunday_lift,
                vz.weekday_midday_share,
                vz.weekend_evening_share,
                vz.late_night_share,
                vz.holiday_proxy_score,
                vz.temporal_confianza,
                ST_Distance(
                    ST_Centroid(z.geometria)::geography,
                    ST_GeomFromText(
                        'LINESTRING(2.1850 41.3740,2.1940 41.3792,2.2030 41.3840,'
                        '2.2130 41.3900,2.2250 41.3970,2.2380 41.4020)', 4326
                    )::geography
                )::int AS dist_playa_m
            FROM input i
            JOIN v_variables_zona vz ON vz.zona_id = i.zone_id AND vz.fecha <= i.fecha_ref
            JOIN zonas z ON z.id = vz.zona_id
            ORDER BY i.idx, vz.fecha DESC
        """

        comp_per_negocio_query = """
            WITH input AS (
                SELECT (ordinality - 1)::int AS idx, zone_id, sec, fecha_ref
                FROM unnest($1::text[], $2::text[], $3::date[]) WITH ORDINALITY
                    AS t(zone_id, sec, fecha_ref, ordinality)
            )
            SELECT DISTINCT ON (i.idx)
                i.idx,
                cp.num_competidores, cp.rating_medio, cp.score_saturacion
            FROM input i
            JOIN competencia_por_local cp
                ON cp.zona_id = i.zone_id
                AND cp.sector_codigo = i.sec
                AND cp.radio_m = 300
                AND cp.fecha_calculo <= i.fecha_ref
            ORDER BY i.idx, cp.fecha_calculo DESC
        """

        precio_per_negocio_query = """
            WITH input AS (
                SELECT (ordinality - 1)::int AS idx, zone_id, fecha_ref
                FROM unnest($1::text[], $2::date[]) WITH ORDINALITY AS t(zone_id, fecha_ref, ordinality)
            )
            SELECT DISTINCT ON (i.idx)
                i.idx,
                paz.precio_m2
            FROM input i
            JOIN precios_alquiler_zona paz
                ON paz.zona_id = i.zone_id
                AND paz.fecha <= i.fecha_ref
            ORDER BY i.idx, paz.fecha DESC
        """

        google_reviews_query = """
            SELECT
                z.id AS zona_id,
                AVG(na.review_count)::float AS avg_reviews
            FROM zonas z
            JOIN negocios_activos na
                ON na.activo = TRUE
               AND ST_DWithin(na.geometria::geography, z.geometria::geography, 300)
               AND na.review_count > 0
            WHERE z.id = ANY($1)
            GROUP BY z.id
        """

        zona_ids_unicos = list(set(zona_ids))
        trans_rows = await conn.fetch(trans_query, zona_ids_unicos)
        trans_dict: dict[str, dict] = {r["zona_id"]: dict(r) for r in trans_rows}

        vz_rows = await conn.fetch(vz_per_negocio_query, zona_ids, fechas)
        vz_by_idx: dict[int, dict] = {r["idx"]: dict(r) for r in vz_rows}

        comp_rows = await conn.fetch(comp_per_negocio_query, zona_ids, sectores, fechas)
        comp_by_idx: dict[int, dict] = {r["idx"]: dict(r) for r in comp_rows}

        precio_rows = await conn.fetch(precio_per_negocio_query, zona_ids, fechas)
        precio_by_idx: dict[int, Optional[float]] = {
            r["idx"]: float(r["precio_m2"]) for r in precio_rows
        }

        google_rows = await conn.fetch(google_reviews_query, zona_ids_unicos)
        google_reviews_dict: dict[str, Optional[float]] = {
            r["zona_id"]: r["avg_reviews"] for r in google_rows
        }

    resultados: list[Optional[dict]] = []

    for idx, zona_id in enumerate(zona_ids):
        vz = vz_by_idx.get(idx, {})
        comp = comp_by_idx.get(idx, {})
        precio = precio_by_idx.get(idx)
        trans = trans_dict.get(zona_id, {})

        if not vz:
            resultados.append(None)
            continue

        total = vz.get("flujo_peatonal_total") or 0
        raw = {
            "flujo_peatonal_total": total,
            "flujo_manana_pct": (vz.get("flujo_peatonal_manana") or 0) / total if total else None,
            "flujo_tarde_pct": (vz.get("flujo_peatonal_tarde") or 0) / total if total else None,
            "flujo_noche_pct": (vz.get("flujo_peatonal_noche") or 0) / total if total else None,
            "renta_media_hogar": vz.get("renta_media_hogar"),
            "edad_media": vz.get("edad_media"),
            "pct_extranjeros": vz.get("pct_extranjeros"),
            "densidad_hab_km2": vz.get("densidad_hab_km2"),
            "num_competidores_300m": comp.get("num_competidores"),
            "rating_medio_competidores": comp.get("rating_medio"),
            "score_saturacion": comp.get("score_saturacion"),
            "precio_m2_alquiler": precio,
            "pct_locales_vacios": vz.get("pct_locales_vacios"),
            "tasa_rotacion_anual": vz.get("tasa_rotacion_anual"),
            "score_turismo": vz.get("score_turismo"),
            "incidencias_por_1000hab": vz.get("incidencias_por_1000hab"),
            "nivel_ruido_db": vz.get("nivel_ruido_db"),
            "score_equipamientos": vz.get("score_equipamientos"),
            "num_lineas_transporte": trans.get("num_lineas"),
            "num_paradas_500m": trans.get("num_paradas"),
            "m2_zonas_verdes_cercanas": vz.get("m2_zonas_verdes_cercanas"),
            "dist_playa_m": vz.get("dist_playa_m"),
            "ratio_locales_comerciales": vz.get("ratio_locales_comerciales"),
            "airbnb_density_500m": vz.get("airbnb_density_500m"),
            "airbnb_occupancy_est": vz.get("airbnb_occupancy_est"),
            "google_review_count_medio": google_reviews_dict.get(zona_id),
            "licencias_nuevas_1a": vz.get("licencias_nuevas_1a"),
            "eventos_culturales_500m": vz.get("eventos_culturales_500m"),
            "booking_hoteles_500m": vz.get("booking_hoteles_500m"),
            "pct_poblacio_25_44": vz.get("pct_poblacio_25_44"),
            "delta_renta_3a": vz.get("delta_renta_3a"),
            "nivel_estudios_alto_pct": vz.get("nivel_estudios_alto_pct"),
            "gini": vz.get("gini"),
            "p80_p20": vz.get("p80_p20"),
            "tamano_hogar": vz.get("tamano_hogar"),
            "hogares_con_menores": vz.get("hogares_con_menores"),
            "personas_solas": vz.get("personas_solas"),
            "renta_media_uc": vz.get("renta_media_uc"),
            "renta_mediana_uc": vz.get("renta_mediana_uc"),
            "seasonality_summer_lift": vz.get("seasonality_summer_lift"),
            "seasonality_christmas_lift": vz.get("seasonality_christmas_lift"),
            "seasonality_rebajas_lift": vz.get("seasonality_rebajas_lift"),
            "seasonality_volatility": vz.get("seasonality_volatility"),
            "seasonality_peak_concentration": vz.get("seasonality_peak_concentration"),
            "weekend_lift": vz.get("weekend_lift"),
            "sunday_lift": vz.get("sunday_lift"),
            "weekday_midday_share": vz.get("weekday_midday_share"),
            "weekend_evening_share": vz.get("weekend_evening_share"),
            "late_night_share": vz.get("late_night_share"),
            "holiday_proxy_score": vz.get("holiday_proxy_score"),
            "temporal_confianza": vz.get("temporal_confianza"),
        }
        raw["indice_potencial_consumo"] = (
            None
            if any(raw.get(feature) is None for feature in ("densidad_hab_km2", "pct_poblacio_25_44", "renta_media_hogar"))
            else float(
                (
                    min(1.0, float(raw["densidad_hab_km2"]) / 30000.0) ** 0.40
                    * min(1.0, float(raw["pct_poblacio_25_44"]) / 0.42) ** 0.35
                    * min(1.0, max(0.0, (float(raw["renta_media_hogar"]) - 17000.0) / (60000.0 - 17000.0))) ** 0.25
                )
                * 100.0
            )
        )

        vec = [
            float(raw.get(feature)) if raw.get(feature) is not None else _MEDIAS[feature]
            for feature in FEATURE_NAMES
        ]
        resultados.append(
            {
                "vector": vec,
                "demografia_cobertura": {
                    feature: raw.get(feature) is not None
                    for feature in _DEMOGRAFIA_AUDIT_FEATURES
                },
            }
        )

    return resultados


async def resumen_dataset(sector: Optional[str] = None) -> dict:
    """
    Estadísticas rápidas del dataset sin construirlo completo.
    Útil para verificar antes de entrenar.
    """
    async with get_db() as conn:
        total = await conn.fetchval(
            "SELECT COUNT(*) FROM negocios_historico"
            + (" WHERE sector_codigo=$1" if sector else ""),
            *([sector] if sector else []),
        )
        supervivientes = await conn.fetchval(
            """
            SELECT COUNT(*) FROM negocios_historico
            WHERE (fecha_cierre IS NULL OR fecha_cierre - fecha_apertura >= $1)
            """ + (" AND sector_codigo=$2" if sector else ""),
            _UMBRAL_SUPERVIVENCIA_DIAS,
            *([sector] if sector else []),
        )
        sectores_disponibles = await conn.fetch(
            "SELECT sector_codigo, COUNT(*) as n FROM negocios_historico GROUP BY sector_codigo ORDER BY n DESC"
        )

    return {
        "total_negocios": total,
        "supervivientes": supervivientes,
        "fracasos": total - supervivientes,
        "ratio_supervivencia": round(supervivientes / total, 3) if total else 0,
        "sectores": {r["sector_codigo"]: r["n"] for r in sectores_disponibles},
    }
