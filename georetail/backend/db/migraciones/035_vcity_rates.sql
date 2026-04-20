-- 035_vcity_rates.sql
-- Expone 3 rates de VCity (tourist / shopping / resident) en v_variables_zona
-- para que los scorers de turismo, demografía y flujo puedan consumirlos.
--
-- Estos campos ya existen en vz_flujo (poblados por pipelines/peatonal/vcity.py)
-- pero no estaban en el SELECT de la vista.
--
-- Idempotente: CREATE OR REPLACE VIEW preservando el orden exacto de columnas
-- previo (Postgres exige no renombrar/reordenar columnas existentes) y
-- agregando los 3 nuevos campos al final.

CREATE OR REPLACE VIEW v_variables_zona AS
SELECT
    vz.zona_id,
    vz.fecha,
    vz.fuente,
    vz.created_at,
    vz.updated_at,
    -- Flujo peatonal (orden original preservado)
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
    -- Turismo
    t.score_turismo,
    t.score_turismo_airbnb,
    t.score_turismo_hut,
    t.airbnb_density_500m,
    t.airbnb_occupancy_est,
    t.booking_hoteles_500m,
    t.booking_rating_medio,
    t.eventos_culturales_500m,
    t.venues_musicales_500m,
    t.dist_landmark_top3_m,
    -- Demografía
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
    -- Comercial
    c.num_negocios_activos,
    c.ratio_locales_comerciales,
    c.pct_locales_vacios,
    c.tasa_rotacion_anual,
    c.licencias_nuevas_1a,
    c.google_review_count_medio,
    -- Entorno
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
    e.mercados_municipales_1km,
    -- >>> NUEVO mig 035: rates VCity (0..1) al final para preservar orden <<<
    f.vcity_tourist_rate,
    f.vcity_shopping_rate,
    f.vcity_resident_rate
FROM variables_zona vz
LEFT JOIN vz_flujo       f USING (zona_id, fecha)
LEFT JOIN vz_turismo     t USING (zona_id, fecha)
LEFT JOIN vz_demografia  d USING (zona_id, fecha)
LEFT JOIN vz_comercial   c USING (zona_id, fecha)
LEFT JOIN vz_entorno     e USING (zona_id, fecha);
