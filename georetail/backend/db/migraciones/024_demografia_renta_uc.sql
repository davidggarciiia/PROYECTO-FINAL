-- ============================================================================
-- Migracion 024 - Demografia: renta por unidad de consumo
--
-- Cambios:
--   1. vz_demografia: añade renta_media_uc y renta_mediana_uc
--   2. Recrea v_variables_zona con las nuevas columnas demograficas
-- ============================================================================

ALTER TABLE vz_demografia
    ADD COLUMN IF NOT EXISTS renta_media_uc FLOAT,
    ADD COLUMN IF NOT EXISTS renta_mediana_uc FLOAT;

COMMENT ON COLUMN vz_demografia.renta_media_uc IS
    'Renta media por unidad de consumo agregada a barrio y repartida por zona.';
COMMENT ON COLUMN vz_demografia.renta_mediana_uc IS
    'Renta mediana por unidad de consumo agregada a barrio y repartida por zona.';

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
    c.ratio_locales_comerciales,
    c.pct_locales_vacios,
    c.tasa_rotacion_anual,
    c.licencias_nuevas_1a,
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
    'Version 024: añade renta_media_uc y renta_mediana_uc.';
