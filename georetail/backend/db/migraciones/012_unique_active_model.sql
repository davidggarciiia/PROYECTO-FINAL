-- 012_unique_active_model.sql
-- Enforce that only one XGBoost model can be active per sector at a time.
-- Prevents scorer.py from loading an unpredictable model on concurrent train runs.

CREATE UNIQUE INDEX IF NOT EXISTS uq_modelos_activo_sector
    ON modelos_versiones(sector)
    WHERE activo = TRUE;

COMMENT ON INDEX uq_modelos_activo_sector IS
    'Only one model can be activo=TRUE per sector. scorer.py relies on this for deterministic model loading.';
