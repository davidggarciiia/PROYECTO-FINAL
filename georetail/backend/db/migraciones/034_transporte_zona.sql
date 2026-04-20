-- =============================================================================
-- 034_transporte_zona.sql
-- Descripción: Añade columnas agregadas de transporte a variables_zona para
--              que el scorer pueda consumir num_lineas_transporte y
--              score_transporte_calculado sin hacer un join espacial en runtime
--              en cada llamada batch.
--
-- Escrito por: pipelines/transporte/agregado_zona.py (tras pipelines/transporte.ejecutar)
-- Leído por : scoring/scorer.py (_score_manual, _get_datos_zona_completos batch),
--             scoring/ml/dataset.py
--
-- Patrón    : ALTER TABLE idempotente (IF NOT EXISTS). No toca filas existentes.
-- =============================================================================

BEGIN;

ALTER TABLE variables_zona
    ADD COLUMN IF NOT EXISTS num_lineas_transporte       INTEGER,
    ADD COLUMN IF NOT EXISTS num_paradas_transporte      INTEGER,
    ADD COLUMN IF NOT EXISTS score_transporte_calculado  FLOAT;

-- Índice parcial para filtrar zonas con transporte en queries de scoring
CREATE INDEX IF NOT EXISTS idx_variables_zona_num_lineas
    ON variables_zona (num_lineas_transporte)
    WHERE num_lineas_transporte IS NOT NULL;

COMMIT;
