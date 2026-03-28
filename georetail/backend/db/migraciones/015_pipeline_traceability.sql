-- 015_pipeline_traceability.sql
-- Add pipeline_run_id FK to key tables so data can be traced back to the pipeline run that created it.

ALTER TABLE variables_zona
    ADD COLUMN IF NOT EXISTS pipeline_run_id INT
    REFERENCES pipeline_ejecuciones(id) ON DELETE SET NULL;

ALTER TABLE precios_alquiler_zona
    ADD COLUMN IF NOT EXISTS pipeline_run_id INT
    REFERENCES pipeline_ejecuciones(id) ON DELETE SET NULL;

ALTER TABLE scores_zona
    ADD COLUMN IF NOT EXISTS pipeline_run_id INT
    REFERENCES pipeline_ejecuciones(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_variables_zona_run_id
    ON variables_zona(pipeline_run_id)
    WHERE pipeline_run_id IS NOT NULL;
