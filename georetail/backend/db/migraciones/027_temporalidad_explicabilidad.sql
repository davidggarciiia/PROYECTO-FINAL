-- ============================================================================
-- Migracion 027 - Temporalidad BCN + columnas de explicabilidad temporal
--
-- Objetivo:
--   1. Crear un calendario local de Barcelona reutilizable por pipelines.
--   2. Ampliar vz_flujo con señales temporales semanales y estacionales.
--   3. Recrear v_variables_zona exponiendo la nueva capa temporal.
-- ============================================================================

CREATE TABLE IF NOT EXISTS dim_calendario_bcn (
    fecha DATE PRIMARY KEY,
    anio INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    dia INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL DEFAULT FALSE,
    is_saturday BOOLEAN NOT NULL DEFAULT FALSE,
    is_sunday BOOLEAN NOT NULL DEFAULT FALSE,
    is_public_holiday_es BOOLEAN NOT NULL DEFAULT FALSE,
    is_public_holiday_ct BOOLEAN NOT NULL DEFAULT FALSE,
    is_public_holiday_bcn BOOLEAN NOT NULL DEFAULT FALSE,
    is_school_holiday BOOLEAN NOT NULL DEFAULT FALSE,
    is_bridge_day BOOLEAN NOT NULL DEFAULT FALSE,
    is_summer BOOLEAN NOT NULL DEFAULT FALSE,
    is_christmas BOOLEAN NOT NULL DEFAULT FALSE,
    is_rebajas_winter BOOLEAN NOT NULL DEFAULT FALSE,
    is_rebajas_summer BOOLEAN NOT NULL DEFAULT FALSE,
    season VARCHAR(16) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_dim_calendario_bcn_anio_mes
    ON dim_calendario_bcn(anio, mes);

ALTER TABLE vz_flujo
    ADD COLUMN IF NOT EXISTS weekend_lift FLOAT,
    ADD COLUMN IF NOT EXISTS sunday_lift FLOAT,
    ADD COLUMN IF NOT EXISTS weekday_midday_share FLOAT,
    ADD COLUMN IF NOT EXISTS weekend_evening_share FLOAT,
    ADD COLUMN IF NOT EXISTS late_night_share FLOAT,
    ADD COLUMN IF NOT EXISTS holiday_proxy_score FLOAT,
    ADD COLUMN IF NOT EXISTS temporal_confianza FLOAT,
    ADD COLUMN IF NOT EXISTS seasonality_summer_lift FLOAT,
    ADD COLUMN IF NOT EXISTS seasonality_christmas_lift FLOAT,
    ADD COLUMN IF NOT EXISTS seasonality_rebajas_lift FLOAT,
    ADD COLUMN IF NOT EXISTS seasonality_volatility FLOAT,
    ADD COLUMN IF NOT EXISTS seasonality_peak_concentration FLOAT;

COMMENT ON COLUMN vz_flujo.weekend_lift IS
    'Lift del fin de semana frente a laborable. >1 indica mas intensidad en sabado/domingo.';
COMMENT ON COLUMN vz_flujo.sunday_lift IS
    'Lift del domingo frente a laborable.';
COMMENT ON COLUMN vz_flujo.weekday_midday_share IS
    'Fraccion del volumen semanal concentrado en laborables 11:00-15:00.';
COMMENT ON COLUMN vz_flujo.weekend_evening_share IS
    'Fraccion del volumen semanal concentrado en sabado-domingo 18:00-23:00.';
COMMENT ON COLUMN vz_flujo.late_night_share IS
    'Fraccion del volumen semanal concentrado en viernes-sabado noche.';
COMMENT ON COLUMN vz_flujo.holiday_proxy_score IS
    'Proxy 0-100 de respuesta de la zona a fines de semana/festivos.';
COMMENT ON COLUMN vz_flujo.temporal_confianza IS
    'Confianza 0-1 de la capa temporal segun cobertura de fuentes.';
COMMENT ON COLUMN vz_flujo.seasonality_summer_lift IS
    'Lift estacional verano vs media estructural de la zona.';
COMMENT ON COLUMN vz_flujo.seasonality_christmas_lift IS
    'Lift estacional Navidad vs media estructural de la zona.';
COMMENT ON COLUMN vz_flujo.seasonality_rebajas_lift IS
    'Lift estacional rebajas de invierno/verano vs media estructural de la zona.';
COMMENT ON COLUMN vz_flujo.seasonality_volatility IS
    'Volatilidad estacional de la serie mensual de peatones.';
COMMENT ON COLUMN vz_flujo.seasonality_peak_concentration IS
    'Concentracion del pico mensual frente a la media anual de la serie.';

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
    -- turismo
    t.score_turismo,
    t.airbnb_density_500m,
    t.airbnb_occupancy_est,
    t.booking_hoteles_500m,
    t.eventos_culturales_500m,
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
    'Vista de compatibilidad total con variables_zona. '
    'Version 027: añade senales temporales semanales y estacionales para flujo.';
