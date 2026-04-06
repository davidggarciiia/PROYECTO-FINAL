-- ============================================================================
-- Migración 021 — Entorno Comercial v2: dimensión multivariable
--
-- Cambios:
--   1. vz_entorno: 1 nueva columna (mercados_municipales_1km)
--   2. mercados_municipales: nueva tabla con ubicaciones (PostGIS)
--   3. Recrea v_variables_zona con nueva columna
--   4. Actualiza pesos de entorno por sector
-- ============================================================================

-- ── 1. Nueva columna en vz_entorno ──────────────────────────────────────────
ALTER TABLE vz_entorno
    ADD COLUMN IF NOT EXISTS mercados_municipales_1km INT;

COMMENT ON COLUMN vz_entorno.mercados_municipales_1km IS
    'Nombre de mercats municipals en radi 1km des del centroide de la zona. '
    'Font: Open Data BCN dataset mercats-municipals. '
    'Calculat amb ST_DWithin sobre taula mercados_municipales.';


-- ── 2. Tabla mercados_municipales ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mercados_municipales (
    id          VARCHAR(50) PRIMARY KEY,
    nombre      TEXT NOT NULL,
    direccion   TEXT,
    distrito    VARCHAR(50),
    barrio      VARCHAR(80),
    geometria   GEOMETRY(POINT, 4326),
    fuente      VARCHAR(50) NOT NULL DEFAULT 'opendata_bcn',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_mercados_municipales_geometria
    ON mercados_municipales USING GIST(geometria);

COMMENT ON TABLE mercados_municipales IS
    'Mercats municipals de Barcelona (La Boqueria, Sant Antoni, etc.). '
    'Font: Open Data BCN dataset mercats-municipals. '
    'Usat per calcular mercados_municipales_1km en vz_entorno.';

-- Trigger updated_at
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'trg_mercados_municipales_updated_at'
          AND tgrelid = 'mercados_municipales'::regclass
    ) THEN
        CREATE TRIGGER trg_mercados_municipales_updated_at
            BEFORE UPDATE ON mercados_municipales
            FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();
    END IF;
EXCEPTION WHEN undefined_function THEN
    NULL;  -- fn_set_updated_at definida en migración 013
END;
$$;


-- ── 3. Recrear v_variables_zona con columna de mercados ────────────────────
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
    t.eventos_culturales_500m,
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
    e.dist_comisaria_m,
    -- v2 entorno comercial: mercados municipales
    e.mercados_municipales_1km
FROM variables_zona vz
LEFT JOIN vz_flujo      f USING (zona_id, fecha)
LEFT JOIN vz_turismo    t USING (zona_id, fecha)
LEFT JOIN vz_demografia d USING (zona_id, fecha)
LEFT JOIN vz_comercial  c USING (zona_id, fecha)
LEFT JOIN vz_entorno    e USING (zona_id, fecha);

COMMENT ON VIEW v_variables_zona IS
    'Vista de compatibilidad total con variables_zona. '
    'Combina las 5 tablas satélite con la tabla coordinadora. '
    'Versión 021: añade mercados_municipales_1km para entorno comercial v2.';


-- ── 4. Actualizar pesos de entorno por sector ──────────────────────────────
-- Con la dimensión de entorno ahora multivariable (6 sub-scores), subir su peso
-- y compensar restando de turismo y transporte para mantener suma = 1.00.
UPDATE sectores SET peso_entorno = 0.10, peso_turismo = 0.03, peso_transporte = 0.09, peso_flujo = 0.23 WHERE codigo = 'restauracion';
UPDATE sectores SET peso_entorno = 0.08, peso_turismo = 0.03, peso_transporte = 0.09, peso_flujo = 0.18 WHERE codigo = 'tatuajes';
UPDATE sectores SET peso_entorno = 0.10, peso_turismo = 0.03, peso_transporte = 0.09, peso_flujo = 0.25 WHERE codigo = 'moda';
UPDATE sectores SET peso_entorno = 0.10, peso_turismo = 0.03, peso_transporte = 0.09, peso_flujo = 0.16 WHERE codigo = 'estetica';
UPDATE sectores SET peso_entorno = 0.08, peso_turismo = 0.03, peso_transporte = 0.09, peso_flujo = 0.14 WHERE codigo = 'shisha_lounge';
