-- ============================================================================
-- Migración 018 — Demografía v5: nuevas columnas y tabla de parques AMB
--
-- Cambios:
--   1. vz_demografia: añade pct_poblacio_25_44 + delta_renta_3a
--   2. vz_entorno: añade seguridad_barri_score (IERMB 0-10)
--   3. parques_amb: nueva tabla espacial con parques AMB geocodificados
--   4. Recrea v_variables_zona con nuevas columnas
-- ============================================================================

-- ── 1. Nuevas columnas en vz_demografia ───────────────────────────────────────
ALTER TABLE vz_demografia
    ADD COLUMN IF NOT EXISTS pct_poblacio_25_44  FLOAT,
    ADD COLUMN IF NOT EXISTS delta_renta_3a      FLOAT;

COMMENT ON COLUMN vz_demografia.pct_poblacio_25_44 IS
    '% de población 25-44 años (consumidores activos). '
    'Calculado desde padrón CKAN por grupos quinquenales.';
COMMENT ON COLUMN vz_demografia.delta_renta_3a IS
    '% variación renta disponible 2019→2022 por distrito. '
    'Proxy de gentrificación / dinamismo económico de la zona.';

-- ── 2. Nuevas columnas en vz_entorno ──────────────────────────────────────────
ALTER TABLE vz_entorno
    ADD COLUMN IF NOT EXISTS seguridad_barri_score FLOAT,
    -- BUG4 FIX: fuente necesaria para INSERT de _poblar_seguretat_iermb
    ADD COLUMN IF NOT EXISTS fuente VARCHAR(50);

COMMENT ON COLUMN vz_entorno.seguridad_barri_score IS
    'Valoración media de seguridad en el barrio (0=inseguro, 10=muy seguro). '
    'Fuente: IERMB id_ind=1892, AMB. Nivel ciudad.';
COMMENT ON COLUMN vz_entorno.fuente IS
    'Pipeline que escribió la última actualización de esta fila.';

-- ── 3. Tabla parques_amb ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS parques_amb (
    id          SERIAL PRIMARY KEY,
    nom         VARCHAR(300)  NOT NULL,
    municipi    VARCHAR(100)  NOT NULL DEFAULT '',
    superficie_ha FLOAT,           -- área del parque en hectáreas
    geometria   GEOMETRY(Point, 4326) NOT NULL,
    fuente      VARCHAR(50) NOT NULL DEFAULT 'amb_opendata',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_parques_amb_nom_municipi UNIQUE (nom, municipi)
);

CREATE INDEX IF NOT EXISTS idx_parques_amb_geometria
    ON parques_amb USING GIST(geometria);
CREATE INDEX IF NOT EXISTS idx_parques_amb_municipi
    ON parques_amb(municipi);

COMMENT ON TABLE parques_amb IS
    'Parques del AMB con localización. '
    'Fuente: opendata.amb.cat/serveis_parcs/search/csv. '
    'Escrito por: pipelines/parques.py. '
    'Usado para calcular m2_zonas_verdes_cercanas en vz_entorno.';

-- ── 4. Trigger updated_at para parques_amb ────────────────────────────────────
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'trg_updated_at'
          AND tgrelid = 'parques_amb'::regclass
    ) THEN
        CREATE TRIGGER trg_updated_at
            BEFORE UPDATE ON parques_amb
            FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();
    END IF;
EXCEPTION WHEN undefined_function THEN
    NULL;  -- fn_set_updated_at aún no existe; el trigger se creará con 013
END;
$$;

-- ── 5. Recrear v_variables_zona con nuevas columnas ──────────────────────────
-- La vista debe incluir pct_poblacio_25_44, delta_renta_3a y seguridad_barri_score
-- BUG5 FIX: CASCADE para eliminar también vistas/funciones dependientes sin error
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
    -- ── entorno (vz_entorno) ──────────────────────────────────────────────────
    e.incidencias_por_1000hab,
    e.nivel_ruido_db,
    e.m2_zonas_verdes_cercanas,
    e.score_equipamientos,
    e.seguridad_barri_score
FROM variables_zona vz
LEFT JOIN vz_flujo      f USING (zona_id, fecha)
LEFT JOIN vz_turismo    t USING (zona_id, fecha)
LEFT JOIN vz_demografia d USING (zona_id, fecha)
LEFT JOIN vz_comercial  c USING (zona_id, fecha)
LEFT JOIN vz_entorno    e USING (zona_id, fecha);

COMMENT ON VIEW v_variables_zona IS
    'Vista de compatibilidad total con variables_zona. '
    'Combina las 5 tablas satélite (vz_flujo, vz_turismo, vz_demografia, '
    'vz_comercial, vz_entorno) con la tabla coordinadora variables_zona. '
    'Versión 018: añade pct_poblacio_25_44, delta_renta_3a, seguridad_barri_score.';
