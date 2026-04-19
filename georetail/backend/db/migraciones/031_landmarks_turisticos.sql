-- ============================================================================
-- Migracion 031 - Landmarks turisticos (OSM + Wikidata filter)
--
-- Motivacion:
--   eventos_culturales_500m (vz_turismo) mezcla Sagrada Familia con bibliotecas
--   de barrio. Demasiado ruido para usarse como senal turistica en el scorer.
--   Esta tabla solo persiste POIs turisticos "reales" (tourism=attraction CON
--   tag wikidata=*, filtro de calidad), con posibilidad de ponderar por
--   relevancia real (Wikipedia pageviews) en el futuro.
--
-- Uso:
--   - Pipeline pipelines/turismo/landmarks.py puebla la tabla via Overpass.
--   - El mismo pipeline calcula dist_landmark_top3_m por zona y lo guarda en
--     vz_turismo.
--   - scoring/dimensiones/turismo.py consume dist_landmark_top3_m para aplicar
--     un ajuste aditivo de proximidad a landmark.
-- ============================================================================

CREATE TABLE IF NOT EXISTS landmarks_turisticos (
    id              SERIAL PRIMARY KEY,
    osm_id          BIGINT UNIQUE,                      -- OSM identifier (node/way id)
    nombre          VARCHAR(200) NOT NULL,
    wikidata_id     VARCHAR(20),                        -- e.g. Q9188
    peso_turistico  FLOAT NOT NULL DEFAULT 1.0,         -- 1.0 por defecto; enriquecible con pageviews
    geom            GEOMETRY(Point, 4326) NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_landmarks_geom
    ON landmarks_turisticos USING GIST(geom);

CREATE INDEX IF NOT EXISTS idx_landmarks_wikidata
    ON landmarks_turisticos(wikidata_id);

COMMENT ON TABLE landmarks_turisticos IS
    'POIs turisticos reales de Barcelona (OSM tourism=attraction con tag wikidata=*). '
    'Filtro de calidad: solo se incluyen atracciones con identificador Wikidata, lo que '
    'descarta la mayoria de ruido (bibliotecas de barrio, miradores menores, etc.).';

COMMENT ON COLUMN landmarks_turisticos.peso_turistico IS
    'Peso 0..N de relevancia turistica. Default 1.0. Puede enriquecerse con Wikipedia '
    'pageviews mensuales del articulo asociado al wikidata_id.';

-- ── Anadir dist_landmark_top3_m a vz_turismo ──────────────────────────────
ALTER TABLE vz_turismo
    ADD COLUMN IF NOT EXISTS dist_landmark_top3_m FLOAT;

COMMENT ON COLUMN vz_turismo.dist_landmark_top3_m IS
    'Distancia media ponderada (metros) desde el centroide de la zona a los 3 landmarks '
    'turisticos mas cercanos, ponderada por peso_turistico. Calculada por '
    'pipelines/turismo/landmarks.py.';

-- ── Recrear v_variables_zona exponiendo dist_landmark_top3_m ──────────────
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
    -- turismo (split writers + landmarks 031)
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
    'Version 031: anade dist_landmark_top3_m (landmarks turisticos OSM+Wikidata).';
