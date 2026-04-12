-- =============================================================================
-- 022_modelos_demografia_feature_names.sql
-- Descripción: Añade el subconjunto demográfico activo usado por cada versión
--              del modelo para mantener compatibilidad entre train/evaluate/scorer.
-- =============================================================================

ALTER TABLE modelos_versiones
    ADD COLUMN IF NOT EXISTS demografia_feature_names JSONB;

COMMENT ON COLUMN modelos_versiones.demografia_feature_names IS
    'Subset demográfico activo usado por la versión del modelo. '
    'Si es NULL, train/evaluate/scorer hacen fallback al bloque demográfico '
    'por defecto definido en scoring/demografia_governance.py.';
