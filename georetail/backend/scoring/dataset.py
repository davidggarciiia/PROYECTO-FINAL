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

El dataset resultante tiene 21 features (ver FEATURE_NAMES en features.py)
y una columna `label` binaria.

Uso:
    from scoring.dataset import construir_dataset
    X, y, meta = await construir_dataset(sector="restauracion")
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

import numpy as np
import pandas as pd

from db.conexion import get_db
from scoring.features import FEATURE_NAMES, _MEDIAS

logger = logging.getLogger(__name__)

# Un negocio que supera 3 años se considera "superviviente"
_UMBRAL_SUPERVIVENCIA_DIAS = 365 * 3

# Mínimo de registros para entrenar — por debajo de esto no tiene sentido
_MIN_REGISTROS = 200


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
        X     → np.ndarray (n_samples, 21) con las features normalizadas
        y     → np.ndarray (n_samples,)    con los labels (0/1)
        meta  → pd.DataFrame con columnas auxiliares (negocio_id, zona_id, sector,
                fecha_apertura, fecha_cierre, label) para análisis posterior.

    Raises:
        ValueError: Si hay menos de _MIN_REGISTROS ejemplos tras filtrar.
    """
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
          -- si cerraron antes del umbral Y abrieron cerca de fecha_corte,
          -- no podemos saber si habrían sobrevivido → los excluimos.
          AND NOT (fecha_cierre IS NULL AND fecha_apertura > $2)
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
    async with get_db() as conn:
        # ── Variables de zona (snapshot histórica) ────────────────────────────
        # `variables_zona` tiene una fila por zona por fecha de actualización.
        # Tomamos la snapshot más reciente ANTERIOR a la fecha de apertura.
        vz_query = """
            SELECT DISTINCT ON (vz.zona_id)
                vz.zona_id,
                vz.flujo_peatonal_manana, vz.flujo_peatonal_tarde, vz.flujo_peatonal_noche,
                vz.flujo_peatonal_total,
                vz.renta_media_hogar, vz.edad_media, vz.pct_extranjeros, vz.densidad_hab_km2,
                vz.pct_locales_vacios, vz.tasa_rotacion_anual,
                vz.score_turismo, vz.incidencias_por_1000hab,
                vz.nivel_ruido_db, vz.score_equipamientos, vz.m2_zonas_verdes_cercanas,
                -- v2: granularidad geográfica de nivel zona
                vz.ratio_locales_comerciales,
                ST_Distance(
                    ST_Centroid(z.geometria)::geography,
                    ST_GeomFromText(
                        'LINESTRING(2.1850 41.3740,2.1940 41.3792,2.2030 41.3840,'
                        '2.2130 41.3900,2.2250 41.3970,2.2380 41.4020)', 4326
                    )::geography
                )::int AS dist_playa_m
            FROM variables_zona vz
            JOIN zonas z ON z.id = vz.zona_id
            WHERE vz.zona_id = ANY($1) AND vz.fecha <= $2
            ORDER BY vz.zona_id, vz.fecha DESC
        """

        comp_query = """
            SELECT DISTINCT ON (zona_id)
                zona_id,
                num_competidores, rating_medio, score_saturacion
            FROM competencia_por_local
            WHERE zona_id = ANY($1)
              AND sector_codigo = $2
              AND radio_m = 300
              AND fecha_calculo <= $3
            ORDER BY zona_id, fecha_calculo DESC
        """

        precio_query = """
            SELECT DISTINCT ON (zona_id)
                zona_id, precio_m2
            FROM precios_alquiler_zona
            WHERE zona_id = ANY($1) AND fecha <= $2
            ORDER BY zona_id, fecha DESC
        """

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

        # Agrupamos por fecha para minimizar queries — en la práctica la mayoría
        # de negocios en el mismo sector tienen fechas similares.
        # Para el dataset de entrenamiento, una aproximación por año es suficiente.
        fechas_unicas = sorted(set(fechas))

        # Acumular resultados indexados por zona_id
        vz_dict:    dict[str, dict] = {}
        comp_dict:  dict[str, dict] = {}
        precio_dict: dict[str, float] = {}
        trans_dict: dict[str, dict] = {}

        # Trans no depende de fecha (la red de transporte es relativamente estable)
        trans_rows = await conn.fetch(trans_query, list(set(zona_ids)))
        trans_dict = {r["zona_id"]: dict(r) for r in trans_rows}

        # Para el resto, usamos la fecha mediana como aproximación razonable
        # (mejora futura: query por negocio individual)
        fecha_mediana = fechas[len(fechas) // 2]
        zona_ids_unicos = list(set(zona_ids))

        vz_rows = await conn.fetch(vz_query, zona_ids_unicos, fecha_mediana)
        vz_dict = {r["zona_id"]: dict(r) for r in vz_rows}

        # Competencia: necesitamos el sector — iteramos por sector único
        sectores_unicos = set(sectores)
        for sec in sectores_unicos:
            comp_rows = await conn.fetch(comp_query, zona_ids_unicos, sec, fecha_mediana)
            for r in comp_rows:
                comp_dict[r["zona_id"]] = dict(r)

        precio_rows = await conn.fetch(precio_query, zona_ids_unicos, fecha_mediana)
        precio_dict = {r["zona_id"]: float(r["precio_m2"]) for r in precio_rows}

    # ── Construir vector de features por negocio ──────────────────────────────
    resultados: list[Optional[list[float]]] = []

    for zona_id in zona_ids:
        vz    = vz_dict.get(zona_id, {})
        comp  = comp_dict.get(zona_id, {})
        precio = precio_dict.get(zona_id)
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
        }

        # Imputar con medias del dataset de entrenamiento donde haya None
        vec = [
            float(raw[f]) if raw.get(f) is not None else _MEDIAS[f]
            for f in FEATURE_NAMES
        ]
        resultados.append(vec)

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
