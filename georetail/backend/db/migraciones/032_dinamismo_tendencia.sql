-- ============================================================================
-- Migracion 032 - Anade columna tendencia faltante a dinamismo_zonal
--
-- Bug: la migracion 026 incluye tendencia VARCHAR(20) en su CREATE TABLE IF
-- NOT EXISTS, pero como la tabla ya existia antes de 026 (sin esa columna),
-- el IF NOT EXISTS la saltaba y la columna nunca se anadia. Resultado: el
-- endpoint /api/local devolvia 500 por UndefinedColumnError al hacer
-- LEFT JOIN LATERAL (SELECT dz2.tendencia FROM dinamismo_zonal ...) desde
-- db/zonas.py:get_zona_completa.
-- ============================================================================

ALTER TABLE dinamismo_zonal
    ADD COLUMN IF NOT EXISTS tendencia VARCHAR(20);

-- Recrear la vista para que incluya la nueva columna
DROP VIEW IF EXISTS v_dinamismo_zona CASCADE;
CREATE VIEW v_dinamismo_zona AS
SELECT DISTINCT ON (zona_id) *
FROM dinamismo_zonal
ORDER BY zona_id, periodo DESC;

COMMENT ON COLUMN dinamismo_zonal.tendencia IS
    'Clasificacion textual (emergente/estable/saturado/declive/sin_datos) '
    'derivada del score_dinamismo y otros indicadores. '
    'Escrita por pipelines/comercio/dinamismo.py.';
