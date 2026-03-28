-- 008_vcity.sql: columna vcity_flujo_peatonal para datos de mobilitat BCN
--
-- Fuente: dataset "mobilitat-persones" del CKAN de BCN Open Data.
-- Si no disponible: proxy desde flujo_peatonal_total (vianants_bcn).
--
-- Pipeline: pipelines/vcity.py (mensual día 12, 04:00)
-- Dependencias: 001_schema_inicial.sql + 007_vianants.sql ejecutadas.
--
-- Ejecutar:
--   psql $DATABASE_URL -f db/migraciones/008_vcity.sql

-- ── Columna principal ─────────────────────────────────────────────────────────

ALTER TABLE variables_zona
    ADD COLUMN IF NOT EXISTS vcity_flujo_peatonal FLOAT DEFAULT NULL;

COMMENT ON COLUMN variables_zona.vcity_flujo_peatonal IS
    'Promedio peatones/día por zona (fuente: BCN Open Data mobilitat o proxy vianants_bcn)';

-- Índice parcial para consultas de scoring
CREATE INDEX IF NOT EXISTS idx_variables_zona_vcity_flujo
    ON variables_zona(vcity_flujo_peatonal)
    WHERE vcity_flujo_peatonal IS NOT NULL;

-- ── Vista auxiliar: mejor estimación de flujo peatonal disponible ─────────────

CREATE OR REPLACE VIEW v_flujo_peatonal_fuentes AS
SELECT
    vz.zona_id,
    z.nombre                    AS zona_nombre,
    vz.fecha,
    vz.flujo_peatonal_total     AS flujo_vianants_bcn,
    vz.vcity_flujo_peatonal     AS flujo_mobilitat_bcn,
    COALESCE(
        vz.vcity_flujo_peatonal,
        vz.flujo_peatonal_total
    )                           AS flujo_mejor_estimacion
FROM variables_zona vz
JOIN zonas z ON z.id = vz.zona_id
WHERE (
    vz.flujo_peatonal_total IS NOT NULL
    OR vz.vcity_flujo_peatonal IS NOT NULL
);

COMMENT ON VIEW v_flujo_peatonal_fuentes IS
    'Flujo peatonal por zona: vianants_bcn vs mobilitat_bcn. '
    'flujo_mejor_estimacion usa mobilitat cuando disponible, vianants como fallback.';
