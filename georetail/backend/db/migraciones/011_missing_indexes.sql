-- 011_missing_indexes.sql
-- Add missing indexes for frequent query patterns in zonas.py and scorer.py

-- precios_alquiler_zona: latest price per zone (queried on every scoring call)
CREATE INDEX IF NOT EXISTS idx_precios_zona_fecha_desc
    ON precios_alquiler_zona(zona_id, fecha DESC);

-- scores_zona: latest score per zone+sector
CREATE INDEX IF NOT EXISTS idx_scores_zona_sector_fecha
    ON scores_zona(zona_id, sector_id, fecha_calculo DESC);

-- alertas_zona: active alerts per zone
CREATE INDEX IF NOT EXISTS idx_alertas_zona_activa
    ON alertas_zona(zona_id)
    WHERE activa = TRUE;

-- locales: available ground floor premises per zone
CREATE INDEX IF NOT EXISTS idx_locales_zona_pb_disponible
    ON locales(zona_id, disponible, planta)
    WHERE disponible = TRUE AND planta = 'PB';

-- negocios_activos: partial GIST for spatial queries on active businesses only
DROP INDEX IF EXISTS idx_negocios_geometria;
CREATE INDEX IF NOT EXISTS idx_negocios_activos_geometria
    ON negocios_activos USING GIST(geometria)
    WHERE activo = TRUE;

-- negocios_activos: zona_id + sector lookup
CREATE INDEX IF NOT EXISTS idx_negocios_activos_zona_sector
    ON negocios_activos(zona_id, sector_codigo)
    WHERE activo = TRUE;

-- variables_zona: recent data per zone (scorer reads latest row)
CREATE INDEX IF NOT EXISTS idx_variables_zona_zona_fecha
    ON variables_zona(zona_id, fecha DESC);

-- pipeline_ejecuciones: latest run per pipeline name
CREATE INDEX IF NOT EXISTS idx_pipeline_pipeline_fecha
    ON pipeline_ejecuciones(pipeline, fecha_inicio DESC);
