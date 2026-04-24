-- ============================================================================
-- Migracion 029 - Split writers de score_turismo + dimension turismo
--
-- Objetivo:
--   1. Anadir columnas score_turismo_airbnb y score_turismo_hut a vz_turismo
--      para separar la salida de cada pipeline (antes ambos pisaban score_turismo
--      con semanticas distintas).
--   2. Mantener score_turismo como columna deprecated (la rellena ahora el
--      modulo dimensiones/turismo.py en runtime, ya no los pipelines).
--   3. Recrear v_variables_zona exponiendo las nuevas columnas y
--      venues_musicales_500m (estaba en la tabla pero no en la vista).
-- ============================================================================

-- ── 1. Anadir nuevas columnas al satelite ──────────────────────────────────
ALTER TABLE vz_turismo
    ADD COLUMN IF NOT EXISTS score_turismo_airbnb FLOAT,
    ADD COLUMN IF NOT EXISTS score_turismo_hut    FLOAT;

COMMENT ON COLUMN vz_turismo.score_turismo_airbnb IS
    'Score 0-100 de presencia turistica via Airbnb. Escrito por pipelines/turismo/airbnb.py.';
COMMENT ON COLUMN vz_turismo.score_turismo_hut IS
    'Score 0-100 de presencia turistica via licencias HUT. Escrito por pipelines/turismo/hut.py.';
COMMENT ON COLUMN vz_turismo.score_turismo IS
    'DEPRECATED: usar score_turismo_airbnb y score_turismo_hut por separado. '
    'El score combinado lo calcula scoring/dimensiones/turismo.py en runtime.';

-- ── 2. Backfill desde score_turismo existente ──────────────────────────────
UPDATE vz_turismo
SET
    score_turismo_airbnb = score_turismo,
    score_turismo_hut    = score_turismo
WHERE
    score_turismo IS NOT NULL
    AND score_turismo_airbnb IS NULL
    AND score_turismo_hut    IS NULL;

-- ── 3. Recrear v_variables_zona exponiendo nuevos campos ───────────────────
DROP VIEW IF EXISTS v_variables_zona CASCADE;

CREATE VIEW v_variables_zona AS
SELECT
    vz.zona_id,
    vz.fecha,
    vz.fuente,
    vz.created_at,
    vz.updated_at,
    -- flujo peatonal
    f.flujo_peatonal_total,
    f.flujo_peatonal_manana,
    f.flujo_peatonal_tarde,
    f.flujo_peatonal_noche,
    f.flujo_popular_times_score,
    f.vcity_flujo_peatonal,
    f.weekend_lift,
    f.sunday_lift,
    f.weekday_midday_share,
    f.weekend_evening_share,
    f.late_night_share,
    f.holiday_proxy_score,
    f.temporal_confianza,
    f.seasonality_summer_lift,
    f.seasonality_christmas_lift,
    f.seasonality_rebajas_lift,
    f.seasonality_volatility,
    f.seasonality_peak_concentration,
    -- turismo (split writers + venues musicales expuesto)
    t.score_turismo,
    t.score_turismo_airbnb,
    t.score_turismo_hut,
    t.airbnb_density_500m,
    t.airbnb_occupancy_est,
    t.booking_hoteles_500m,
    t.booking_rating_medio,
    t.eventos_culturales_500m,
    t.venues_musicales_500m,
    -- demografia
    d.poblacion,
    d.edad_media,
    d.renta_media_hogar,
    d.renta_media_uc,
    d.renta_mediana_uc,
    d.pct_extranjeros,
    d.densidad_hab_km2,
    d.nivel_estudios_alto_pct,
    d.pct_poblacio_25_44,
    d.delta_renta_3a,
    d.gini,
    d.p80_p20,
    d.tamano_hogar,
    d.hogares_con_menores,
    d.personas_solas,
    -- comercial
    c.num_negocios_activos,
    c.ratio_locales_comerciales,
    c.pct_locales_vacios,
    c.tasa_rotacion_anual,
    c.licencias_nuevas_1a,
    c.google_review_count_medio,
    -- entorno y seguridad
    e.incidencias_por_1000hab,
    e.nivel_ruido_db,
    e.m2_zonas_verdes_cercanas,
    e.score_equipamientos,
    e.seguridad_barri_score,
    e.hurtos_por_1000hab,
    e.robatoris_por_1000hab,
    e.danys_por_1000hab,
    e.incidencias_noche_pct,
    e.comisarias_1km,
    e.dist_comisaria_m,
    e.mercados_municipales_1km
FROM variables_zona vz
LEFT JOIN vz_flujo      f USING (zona_id, fecha)
LEFT JOIN vz_turismo    t USING (zona_id, fecha)
LEFT JOIN vz_demografia d USING (zona_id, fecha)
LEFT JOIN vz_comercial  c USING (zona_id, fecha)
LEFT JOIN vz_entorno    e USING (zona_id, fecha);

COMMENT ON VIEW v_variables_zona IS
    'Vista de compatibilidad con variables_zona. '
    'Version 029: split writers de score_turismo + expone venues_musicales_500m + booking_rating_medio.';
