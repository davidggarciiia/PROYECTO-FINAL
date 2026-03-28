-- =============================================================================
-- 017_split_variables_zona.sql
-- Descripción: Split de variables_zona (36 columnas, 5 dominios no relacionados)
--              en 5 tablas satélite + vista de compatibilidad v_variables_zona.
--
-- Resultado:
--   - variables_zona  → tabla coordinadora delgada (zona_id, fecha, fuente, timestamps)
--   - vz_flujo        → flujo peatonal (vianants, vcity, google_maps, aforaments)
--   - vz_turismo      → datos de turismo (booking, airbnb, venues_ocio)
--   - vz_demografia   → datos demográficos (demografia)
--   - vz_comercial    → actividad comercial (precios, llicencies, google_maps)
--   - vz_entorno      → entorno y seguridad (aforaments, fuentes externas)
--   - v_variables_zona → vista de compatibilidad — mismas columnas que variables_zona
--
-- NOTA: variables_zona NO se elimina. Sigue siendo el anchor (zona_id, fecha).
--       Las columnas de datos se mantienen en variables_zona para compatibilidad
--       hacia atrás hasta que los pipelines lean sólo de la vista v_variables_zona.
--
-- Dependencias: 001_schema_inicial.sql, 006_nuevos_datos.sql, 007_vianants.sql,
--               008_vcity.sql, 013_audit_columns.sql ejecutadas.
--
-- Ejecutar:
--   psql $DATABASE_URL -f db/migraciones/017_split_variables_zona.sql
-- =============================================================================

BEGIN;

-- =============================================================================
-- PASO 1: Crear tablas satélite
-- =============================================================================

-- ── vz_flujo: datos de flujo peatonal ─────────────────────────────────────────
-- Escrito por: vianants.py, vcity.py, google_maps.py, aforaments.py
CREATE TABLE IF NOT EXISTS vz_flujo (
    zona_id                   VARCHAR(20) NOT NULL REFERENCES zonas(id),
    fecha                     DATE        NOT NULL,
    flujo_peatonal_manana     FLOAT,
    flujo_peatonal_tarde      FLOAT,
    flujo_peatonal_noche      FLOAT,
    flujo_peatonal_total      FLOAT,
    flujo_popular_times_score FLOAT,
    vcity_flujo_peatonal      FLOAT,
    vcity_tourist_rate        FLOAT,
    vcity_shopping_rate       FLOAT,
    vcity_resident_rate       FLOAT,
    fuente                    VARCHAR(50),
    created_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (zona_id, fecha)
);

COMMENT ON TABLE vz_flujo IS
    'Flujo peatonal por zona/fecha. '
    'Escrito por: pipelines/vianants.py, vcity.py, google_maps.py, aforaments.py. '
    'Parte del split de variables_zona (migración 017).';

CREATE INDEX IF NOT EXISTS idx_vz_flujo_zona_fecha
    ON vz_flujo(zona_id, fecha DESC);

-- ── vz_turismo: datos de turismo ───────────────────────────────────────────────
-- Escrito por: booking.py, airbnb.py (y venues_ocio indirectamente)
CREATE TABLE IF NOT EXISTS vz_turismo (
    zona_id                        VARCHAR(20) NOT NULL REFERENCES zonas(id),
    fecha                          DATE        NOT NULL,
    score_turismo                  FLOAT,
    airbnb_density_500m            INTEGER,
    airbnb_occupancy_est           FLOAT,
    booking_hoteles_500m           INTEGER,
    booking_rating_medio           FLOAT,
    cruceros_visitantes_anuales    INTEGER,
    puntos_interes_culturales_500m INTEGER,
    mercados_municipales_500m      INTEGER,
    eventos_culturales_500m        INTEGER,
    venues_musicales_500m          INTEGER,
    fuente                         VARCHAR(50),
    created_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (zona_id, fecha)
);

COMMENT ON TABLE vz_turismo IS
    'Datos de turismo por zona/fecha. '
    'Escrito por: pipelines/booking.py, airbnb.py. '
    'Parte del split de variables_zona (migración 017).';

CREATE INDEX IF NOT EXISTS idx_vz_turismo_zona_fecha
    ON vz_turismo(zona_id, fecha DESC);

-- ── vz_demografia: datos demográficos ─────────────────────────────────────────
-- Escrito por: demografia.py
CREATE TABLE IF NOT EXISTS vz_demografia (
    zona_id                  VARCHAR(20) NOT NULL REFERENCES zonas(id),
    fecha                    DATE        NOT NULL,
    poblacion                INT,
    edad_media               FLOAT,
    renta_media_hogar        FLOAT,
    pct_extranjeros          FLOAT,
    densidad_hab_km2         FLOAT,
    nivel_estudios_alto_pct  FLOAT,
    fuente                   VARCHAR(50),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (zona_id, fecha)
);

COMMENT ON TABLE vz_demografia IS
    'Datos demográficos por zona/fecha. '
    'Escrito por: pipelines/demografia.py. '
    'Parte del split de variables_zona (migración 017).';

CREATE INDEX IF NOT EXISTS idx_vz_demografia_zona_fecha
    ON vz_demografia(zona_id, fecha DESC);

-- ── vz_comercial: actividad comercial ─────────────────────────────────────────
-- Escrito por: precios.py, llicencies.py, google_maps.py
CREATE TABLE IF NOT EXISTS vz_comercial (
    zona_id                   VARCHAR(20) NOT NULL REFERENCES zonas(id),
    fecha                     DATE        NOT NULL,
    num_negocios_activos      INT,
    pct_locales_vacios        FLOAT,
    tasa_rotacion_anual       FLOAT,
    ratio_locales_comerciales FLOAT,
    licencias_nuevas_1a       INTEGER,
    licencias_bajas_1a        INTEGER,
    google_review_count_medio FLOAT,
    fuente                    VARCHAR(50),
    created_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (zona_id, fecha)
);

COMMENT ON TABLE vz_comercial IS
    'Actividad comercial por zona/fecha. '
    'Escrito por: pipelines/llicencies.py, google_maps.py (y precios.py para ratio_locales). '
    'Parte del split de variables_zona (migración 017).';

CREATE INDEX IF NOT EXISTS idx_vz_comercial_zona_fecha
    ON vz_comercial(zona_id, fecha DESC);

-- ── vz_entorno: entorno físico y seguridad ─────────────────────────────────────
-- Escrito por: aforaments.py y fuentes externas (mapa acústico, equipamientos)
CREATE TABLE IF NOT EXISTS vz_entorno (
    zona_id                  VARCHAR(20) NOT NULL REFERENCES zonas(id),
    fecha                    DATE        NOT NULL,
    incidencias_por_1000hab  FLOAT,
    nivel_ruido_db           FLOAT,
    m2_zonas_verdes_cercanas FLOAT,
    score_equipamientos      FLOAT,
    fuente                   VARCHAR(50),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (zona_id, fecha)
);

COMMENT ON TABLE vz_entorno IS
    'Entorno físico y seguridad por zona/fecha. '
    'Escrito por: pipelines/aforaments.py y fuentes externas. '
    'Parte del split de variables_zona (migración 017).';

CREATE INDEX IF NOT EXISTS idx_vz_entorno_zona_fecha
    ON vz_entorno(zona_id, fecha DESC);


-- =============================================================================
-- PASO 2: Migrar datos existentes de variables_zona a las tablas satélite
-- =============================================================================

-- ── Migrar flujo ───────────────────────────────────────────────────────────────
INSERT INTO vz_flujo (
    zona_id, fecha,
    flujo_peatonal_manana, flujo_peatonal_tarde,
    flujo_peatonal_noche, flujo_peatonal_total,
    flujo_popular_times_score,
    vcity_flujo_peatonal, vcity_tourist_rate,
    vcity_shopping_rate, vcity_resident_rate,
    fuente
)
SELECT
    zona_id, fecha,
    flujo_peatonal_manana, flujo_peatonal_tarde,
    flujo_peatonal_noche, flujo_peatonal_total,
    flujo_popular_times_score,
    vcity_flujo_peatonal, vcity_tourist_rate,
    vcity_shopping_rate, vcity_resident_rate,
    fuente
FROM variables_zona
WHERE (
    flujo_peatonal_total      IS NOT NULL OR
    flujo_popular_times_score IS NOT NULL OR
    vcity_flujo_peatonal      IS NOT NULL
)
ON CONFLICT DO NOTHING;

-- ── Migrar turismo ─────────────────────────────────────────────────────────────
INSERT INTO vz_turismo (
    zona_id, fecha,
    score_turismo,
    airbnb_density_500m, airbnb_occupancy_est,
    booking_hoteles_500m, booking_rating_medio,
    cruceros_visitantes_anuales,
    puntos_interes_culturales_500m,
    mercados_municipales_500m,
    eventos_culturales_500m,
    venues_musicales_500m,
    fuente
)
SELECT
    zona_id, fecha,
    score_turismo,
    airbnb_density_500m, airbnb_occupancy_est,
    booking_hoteles_500m, booking_rating_medio,
    cruceros_visitantes_anuales,
    puntos_interes_culturales_500m,
    mercados_municipales_500m,
    eventos_culturales_500m,
    venues_musicales_500m,
    fuente
FROM variables_zona
WHERE (
    score_turismo          IS NOT NULL OR
    airbnb_density_500m    IS NOT NULL OR
    booking_hoteles_500m   IS NOT NULL OR
    eventos_culturales_500m IS NOT NULL
)
ON CONFLICT DO NOTHING;

-- ── Migrar demografía ──────────────────────────────────────────────────────────
INSERT INTO vz_demografia (
    zona_id, fecha,
    poblacion, edad_media, renta_media_hogar,
    pct_extranjeros, densidad_hab_km2,
    nivel_estudios_alto_pct,
    fuente
)
SELECT
    zona_id, fecha,
    poblacion, edad_media, renta_media_hogar,
    pct_extranjeros, densidad_hab_km2,
    nivel_estudios_alto_pct,
    fuente
FROM variables_zona
WHERE (
    poblacion         IS NOT NULL OR
    renta_media_hogar IS NOT NULL OR
    edad_media        IS NOT NULL
)
ON CONFLICT DO NOTHING;

-- ── Migrar comercial ───────────────────────────────────────────────────────────
INSERT INTO vz_comercial (
    zona_id, fecha,
    num_negocios_activos, pct_locales_vacios,
    tasa_rotacion_anual, ratio_locales_comerciales,
    licencias_nuevas_1a, licencias_bajas_1a,
    google_review_count_medio,
    fuente
)
SELECT
    zona_id, fecha,
    num_negocios_activos, pct_locales_vacios,
    tasa_rotacion_anual, ratio_locales_comerciales,
    licencias_nuevas_1a, licencias_bajas_1a,
    -- google_review_count_medio no existe en variables_zona — NULL en migración inicial
    NULL::FLOAT AS google_review_count_medio,
    fuente
FROM variables_zona
WHERE (
    num_negocios_activos    IS NOT NULL OR
    pct_locales_vacios      IS NOT NULL OR
    ratio_locales_comerciales IS NOT NULL OR
    licencias_nuevas_1a     IS NOT NULL
)
ON CONFLICT DO NOTHING;

-- ── Migrar entorno ─────────────────────────────────────────────────────────────
INSERT INTO vz_entorno (
    zona_id, fecha,
    incidencias_por_1000hab, nivel_ruido_db,
    m2_zonas_verdes_cercanas, score_equipamientos,
    fuente
)
SELECT
    zona_id, fecha,
    incidencias_por_1000hab, nivel_ruido_db,
    m2_zonas_verdes_cercanas, score_equipamientos,
    fuente
FROM variables_zona
WHERE (
    incidencias_por_1000hab  IS NOT NULL OR
    nivel_ruido_db           IS NOT NULL OR
    m2_zonas_verdes_cercanas IS NOT NULL OR
    score_equipamientos      IS NOT NULL
)
ON CONFLICT DO NOTHING;


-- =============================================================================
-- PASO 3: Crear vista de compatibilidad v_variables_zona
--
-- La vista expone exactamente las mismas columnas que variables_zona
-- para que todo el código de lectura existente (features.py, scorer.py,
-- zonas.py) no necesite cambios.
-- =============================================================================

CREATE OR REPLACE VIEW v_variables_zona AS
SELECT
    vz.id,
    vz.zona_id,
    vz.fecha,
    vz.fuente,
    vz.created_at,
    vz.updated_at,
    -- ── flujo peatonal ──────────────────────────────────────────────────────
    f.flujo_peatonal_manana,
    f.flujo_peatonal_tarde,
    f.flujo_peatonal_noche,
    f.flujo_peatonal_total,
    f.flujo_popular_times_score,
    f.vcity_flujo_peatonal,
    f.vcity_tourist_rate,
    f.vcity_shopping_rate,
    f.vcity_resident_rate,
    -- ── turismo ─────────────────────────────────────────────────────────────
    t.score_turismo,
    t.airbnb_density_500m,
    t.airbnb_occupancy_est,
    t.booking_hoteles_500m,
    t.booking_rating_medio,
    t.cruceros_visitantes_anuales,
    t.puntos_interes_culturales_500m,
    t.mercados_municipales_500m,
    t.eventos_culturales_500m,
    t.venues_musicales_500m,
    -- ── demografía ──────────────────────────────────────────────────────────
    d.poblacion,
    d.edad_media,
    d.renta_media_hogar,
    d.pct_extranjeros,
    d.densidad_hab_km2,
    d.nivel_estudios_alto_pct,
    -- ── comercial ───────────────────────────────────────────────────────────
    c.num_negocios_activos,
    c.pct_locales_vacios,
    c.tasa_rotacion_anual,
    c.ratio_locales_comerciales,
    c.licencias_nuevas_1a,
    c.licencias_bajas_1a,
    c.google_review_count_medio,
    -- ── entorno ─────────────────────────────────────────────────────────────
    e.incidencias_por_1000hab,
    e.nivel_ruido_db,
    e.m2_zonas_verdes_cercanas,
    e.score_equipamientos
FROM variables_zona vz
LEFT JOIN vz_flujo     f USING (zona_id, fecha)
LEFT JOIN vz_turismo   t USING (zona_id, fecha)
LEFT JOIN vz_demografia d USING (zona_id, fecha)
LEFT JOIN vz_comercial c USING (zona_id, fecha)
LEFT JOIN vz_entorno   e USING (zona_id, fecha);

COMMENT ON VIEW v_variables_zona IS
    'Vista de compatibilidad total con variables_zona. '
    'Combina las 5 tablas satélite (vz_flujo, vz_turismo, vz_demografia, '
    'vz_comercial, vz_entorno) con la tabla coordinadora variables_zona. '
    'Los pipelines escriben en las tablas satélite; el código de lectura '
    'puede usar esta vista sin modificaciones (mismas columnas). '
    'Creada por migración 017_split_variables_zona.sql.';


-- =============================================================================
-- PASO 4: Triggers updated_at para las tablas satélite
-- (reutiliza fn_set_updated_at() creada en 013_audit_columns.sql)
-- =============================================================================

DO $$
DECLARE
    t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY['vz_flujo','vz_turismo','vz_demografia','vz_comercial','vz_entorno']
    LOOP
        EXECUTE format('
            DROP TRIGGER IF EXISTS trg_updated_at ON %I;
            CREATE TRIGGER trg_updated_at
            BEFORE UPDATE ON %I
            FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();
        ', t, t);
    END LOOP;
END $$;


COMMIT;
