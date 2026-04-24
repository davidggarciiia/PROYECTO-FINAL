-- ============================================================================
-- Migración 020 — Seguridad v2: dimensión multivariable
--
-- Cambios:
--   1. vz_entorno: 6 nuevas columnas de seguridad granular
--   2. comisarias: nueva tabla con ubicaciones policiales (PostGIS)
--   3. Recrea v_variables_zona con nuevas columnas de seguridad
-- ============================================================================

-- ── 1. Nuevas columnas en vz_entorno ──────────────────────────────────────────
ALTER TABLE vz_entorno
    ADD COLUMN IF NOT EXISTS hurtos_por_1000hab     FLOAT,
    ADD COLUMN IF NOT EXISTS robatoris_por_1000hab   FLOAT,
    ADD COLUMN IF NOT EXISTS danys_por_1000hab       FLOAT,
    ADD COLUMN IF NOT EXISTS incidencias_noche_pct   FLOAT,
    ADD COLUMN IF NOT EXISTS comisarias_1km          INT,
    ADD COLUMN IF NOT EXISTS dist_comisaria_m        FLOAT;

COMMENT ON COLUMN vz_entorno.hurtos_por_1000hab IS
    'Hurts (carterisme, furts) per 1000 habitants/any. '
    'Font: Accidents/Incidents GU BCN Open Data CKAN. '
    'Representa ~50%% dels fets delictius a Barcelona.';
COMMENT ON COLUMN vz_entorno.robatoris_por_1000hab IS
    'Robatoris amb força o violència per 1000 habitants/any. '
    'Font: Accidents/Incidents GU BCN Open Data CKAN.';
COMMENT ON COLUMN vz_entorno.danys_por_1000hab IS
    'Danys a la propietat (vandalisme) per 1000 habitants/any. '
    'Font: Accidents/Incidents GU BCN Open Data CKAN.';
COMMENT ON COLUMN vz_entorno.incidencias_noche_pct IS
    'Fracció d''incidències entre 20:00-06:00 (0-1). '
    'Font: camp Hora de dia del CSV GU BCN. '
    'Clau per negocis nocturns (restaurants, bars).';
COMMENT ON COLUMN vz_entorno.comisarias_1km IS
    'Nombre de comissaries (Mossos + GU) en radi 1km des del centroide de la zona. '
    'Calculat amb ST_DWithin sobre taula comisarias.';
COMMENT ON COLUMN vz_entorno.dist_comisaria_m IS
    'Distància en metres a la comissaria més propera. '
    'Calculat amb ST_Distance sobre taula comisarias.';


-- ── 2. Tabla comisarias ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS comisarias (
    id          VARCHAR(50) PRIMARY KEY,
    nombre      TEXT NOT NULL,
    tipo        VARCHAR(30),          -- 'mossos_esquadra', 'guardia_urbana', 'policia_nacional'
    direccion   TEXT,
    distrito    VARCHAR(50),
    barrio      VARCHAR(80),
    geometria   GEOMETRY(POINT, 4326),
    fuente      VARCHAR(50) NOT NULL DEFAULT 'opendata_bcn',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_comisarias_geometria
    ON comisarias USING GIST(geometria);

COMMENT ON TABLE comisarias IS
    'Comissaries de policia a Barcelona (Mossos d''Esquadra + Guàrdia Urbana). '
    'Font: Open Data BCN dataset comissaries-policia. '
    'Usat per calcular comisarias_1km i dist_comisaria_m en vz_entorno.';

-- Trigger updated_at
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'trg_comisarias_updated_at'
          AND tgrelid = 'comisarias'::regclass
    ) THEN
        CREATE TRIGGER trg_comisarias_updated_at
            BEFORE UPDATE ON comisarias
            FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();
    END IF;
EXCEPTION WHEN undefined_function THEN
    NULL;  -- fn_set_updated_at definida en migración 013
END;
$$;


-- ── 3. Recrear v_variables_zona con columnas de seguridad ────────────────────
DROP VIEW IF EXISTS v_variables_zona CASCADE;

CREATE VIEW v_variables_zona AS
SELECT
    vz.zona_id,
    vz.fecha,
    vz.fuente,
    vz.created_at,
    vz.updated_at,
    -- ── flujo peatonal (vz_flujo) ─────────────────────────────────────────────
    f.flujo_peatonal_total,
    f.flujo_peatonal_manana,
    f.flujo_peatonal_tarde,
    f.flujo_peatonal_noche,
    f.flujo_popular_times_score,
    f.vcity_flujo_peatonal,
    -- ── turismo (vz_turismo) ──────────────────────────────────────────────────
    t.score_turismo,
    t.airbnb_density_500m,
    t.airbnb_occupancy_est,
    t.booking_hoteles_500m,
    -- ── demografía (vz_demografia) ────────────────────────────────────────────
    d.poblacion,
    d.edad_media,
    d.renta_media_hogar,
    d.pct_extranjeros,
    d.densidad_hab_km2,
    d.nivel_estudios_alto_pct,
    d.pct_poblacio_25_44,
    d.delta_renta_3a,
    -- ── comercial (vz_comercial) ──────────────────────────────────────────────
    c.ratio_locales_comerciales,
    c.pct_locales_vacios,
    c.tasa_rotacion_anual,
    c.licencias_nuevas_1a,
    -- ── entorno y seguridad (vz_entorno) ──────────────────────────────────────
    e.incidencias_por_1000hab,
    e.nivel_ruido_db,
    e.m2_zonas_verdes_cercanas,
    e.score_equipamientos,
    e.seguridad_barri_score,
    -- v2 seguridad: desglose por tipo + nocturnidad + policía
    e.hurtos_por_1000hab,
    e.robatoris_por_1000hab,
    e.danys_por_1000hab,
    e.incidencias_noche_pct,
    e.comisarias_1km,
    e.dist_comisaria_m
FROM variables_zona vz
LEFT JOIN vz_flujo      f USING (zona_id, fecha)
LEFT JOIN vz_turismo    t USING (zona_id, fecha)
LEFT JOIN vz_demografia d USING (zona_id, fecha)
LEFT JOIN vz_comercial  c USING (zona_id, fecha)
LEFT JOIN vz_entorno    e USING (zona_id, fecha);

COMMENT ON VIEW v_variables_zona IS
    'Vista de compatibilidad total con variables_zona. '
    'Combina las 5 tablas satélite con la tabla coordinadora. '
    'Versión 020: añade hurtos_por_1000hab, robatoris_por_1000hab, '
    'danys_por_1000hab, incidencias_noche_pct, comisarias_1km, dist_comisaria_m.';


-- ── 4. Actualizar pesos de seguridad por sector ─────────────────────────────
-- Con la dimensión de seguridad ahora multivariable (5 sub-scores), subir su peso
-- y compensar restando de la dimensión con menos impacto relativo para cada sector.
-- Los pesos siempre suman 1.00.
UPDATE sectores SET peso_seguridad = 0.08, peso_flujo = 0.25 WHERE codigo = 'restauracion';
UPDATE sectores SET peso_seguridad = 0.10, peso_flujo = 0.20 WHERE codigo = 'tatuajes';
UPDATE sectores SET peso_seguridad = 0.07, peso_flujo = 0.27 WHERE codigo = 'moda';
UPDATE sectores SET peso_seguridad = 0.08, peso_flujo = 0.18 WHERE codigo = 'estetica';
UPDATE sectores SET peso_seguridad = 0.12, peso_flujo = 0.16 WHERE codigo = 'shisha_lounge';
