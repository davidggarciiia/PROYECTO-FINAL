-- ============================================================
-- Archivo  : 01_extensiones.sql
-- Proyecto : GeoRetail
-- Propósito: Habilitar extensiones PostgreSQL necesarias para el
--            esquema geoespacial (PostGIS, pgvector, búsqueda
--            fuzzy) y registrar la función de auditoría
--            fn_set_updated_at() reutilizable por todos los
--            triggers updated_at del proyecto.
-- Orden    : Debe ejecutarse primero (prefijo 01_).
--            Docker monta /docker-entrypoint-initdb.d en orden
--            alfabético; este archivo no tiene dependencias.
-- ============================================================

-- ── Extensiones ─────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS postgis;        -- geometrías + índices GIST
CREATE EXTENSION IF NOT EXISTS vector;         -- embeddings pgvector
CREATE EXTENSION IF NOT EXISTS pg_trgm;        -- búsqueda fuzzy / ILIKE acelerado

-- ── Función de auditoría reutilizable ───────────────────────

CREATE OR REPLACE FUNCTION fn_set_updated_at()
    RETURNS TRIGGER
    LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

COMMENT ON FUNCTION fn_set_updated_at() IS
    'Trigger function reutilizable: actualiza updated_at al momento '
    'exacto de cada UPDATE. Asociar con CREATE TRIGGER ... BEFORE UPDATE.';
