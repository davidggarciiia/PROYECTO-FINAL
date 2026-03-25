-- ============================================================
-- 005_habitaclia_locales.sql
-- Adapta la tabla locales para recibir datos de Habitaclia.
--
-- Cambios:
--   1. Amplía locales.id a VARCHAR(50) — IDs de Habitaclia tienen ~35 chars
--   2. Añade locales.url para enlazar al anuncio original
--   3. Añade locales.titulo para mostrar el nombre del anuncio
--   4. Añade locales.descripcion para texto libre
--   5. Índice adicional por fuente para filtrar rápido
--
-- Ejecutar (una sola vez):
--   docker exec georetail_postgres psql -U postgres -d georetail \
--     -f /docker-entrypoint-initdb.d/005_habitaclia_locales.sql
-- ============================================================

BEGIN;

-- 1. Ampliar id a VARCHAR(50)
ALTER TABLE locales ALTER COLUMN id TYPE VARCHAR(50);

-- 2. Añadir columnas nuevas si no existen
ALTER TABLE locales
  ADD COLUMN IF NOT EXISTS url        TEXT,
  ADD COLUMN IF NOT EXISTS titulo     TEXT,
  ADD COLUMN IF NOT EXISTS descripcion TEXT;

-- 3. Índice por fuente para filtrar rápido
CREATE INDEX IF NOT EXISTS idx_locales_fuente ON locales(fuente);

-- 4. Índice por zona + disponible (usado en filtrar_zonas_candidatas)
CREATE INDEX IF NOT EXISTS idx_locales_zona_disponible
  ON locales(zona_id, disponible)
  WHERE disponible = TRUE;

-- 5. Función helper: elimina locales del seed si ya hay datos reales del mismo barrio
--    Llamada automáticamente al final de la sincronización de Habitaclia.
CREATE OR REPLACE FUNCTION limpiar_seed_si_hay_real()
RETURNS INT LANGUAGE plpgsql AS $$
DECLARE
  eliminados INT;
BEGIN
  DELETE FROM locales seed
  WHERE seed.fuente IN ('seed', 'manual', 'demo')
    AND EXISTS (
      SELECT 1 FROM locales real
      WHERE real.zona_id = seed.zona_id
        AND real.fuente  = 'habitaclia'
        AND real.disponible = TRUE
    );
  GET DIAGNOSTICS eliminados = ROW_COUNT;
  RETURN eliminados;
END;
$$;

COMMIT;
