-- 016_inmuebles_zona_fk.sql
-- Add zona_id to inmuebles_portales for spatial linkage.
-- Requires a backfill job (pipelines/mercado_inmobiliario.py) to populate via ST_Within.

ALTER TABLE inmuebles_portales
    ADD COLUMN IF NOT EXISTS zona_id VARCHAR(20)
    REFERENCES zonas(id) ON DELETE SET NULL;

-- Add activo flag so listings can be marked as inactive without deletion
ALTER TABLE inmuebles_portales
    ADD COLUMN IF NOT EXISTS activo BOOLEAN NOT NULL DEFAULT TRUE;

CREATE INDEX IF NOT EXISTS idx_inmuebles_portales_zona
    ON inmuebles_portales(zona_id)
    WHERE zona_id IS NOT NULL;

COMMENT ON COLUMN inmuebles_portales.zona_id IS
    'Populated by pipelines/mercado_inmobiliario.py via ST_Within spatial join. NULL = not yet assigned.';

-- View: market stats by zona (replaces or supplements v_mercado_zona which uses free-text barrio)
-- NOTE: uses column names from 004_inmuebles_portales.sql:
--   fuente       (not fuente_portal)
--   precio       (not precio_mes — monthly rent for locals)
CREATE OR REPLACE VIEW v_mercado_zona_spatial AS
SELECT
    zona_id,
    fuente                                         AS fuente_portal,
    COUNT(*)                                       AS total_listings,
    ROUND(AVG(precio)::numeric, 0)                 AS precio_mes_medio,
    ROUND(AVG(precio_m2)::numeric, 1)              AS precio_m2_medio,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY precio_m2) AS precio_m2_mediana,
    MIN(precio_m2)                                 AS precio_m2_min,
    MAX(precio_m2)                                 AS precio_m2_max,
    ROUND(AVG(superficie_util)::numeric, 1)        AS superficie_media,
    COUNT(*) FILTER (WHERE activo = TRUE)          AS listings_activos,
    MAX(fecha_scraping)                            AS ultima_actualizacion
FROM inmuebles_portales
WHERE zona_id IS NOT NULL
GROUP BY zona_id, fuente;
