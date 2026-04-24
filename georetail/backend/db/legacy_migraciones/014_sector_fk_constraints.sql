-- 014_sector_fk_constraints.sql
-- Add FK constraints from sector_codigo columns to sectores.codigo.
-- Prevents invalid sector codes from being inserted anywhere in the schema.
--
-- SAFETY: Uses DEFERRABLE INITIALLY DEFERRED so bulk inserts in the same
-- transaction can insert sector rows before referencing them.
-- Includes a cleanup step for any orphaned sector_codigo values.

BEGIN;

-- Step 1: Clean up any existing rows with invalid sector codes
-- (safe to run even if tables are empty)
DELETE FROM negocios_activos
WHERE sector_codigo IS NOT NULL
  AND sector_codigo NOT IN (SELECT codigo FROM sectores);

DELETE FROM competencia_por_local
WHERE sector_codigo IS NOT NULL
  AND sector_codigo NOT IN (SELECT codigo FROM sectores);

DELETE FROM negocios_historico
WHERE sector_codigo IS NOT NULL
  AND sector_codigo NOT IN (SELECT codigo FROM sectores);

DELETE FROM restricciones_geograficas_sector
WHERE sector_codigo IS NOT NULL
  AND sector_codigo NOT IN (SELECT codigo FROM sectores);

DELETE FROM requisitos_legales_sector
WHERE sector_codigo IS NOT NULL
  AND sector_codigo NOT IN (SELECT codigo FROM sectores);

-- Step 2: Add FK constraints
-- (DO $$ pattern used for idempotency: PostgreSQL < 16 does not support
--  ADD CONSTRAINT IF NOT EXISTS; this project targets PostgreSQL 15+)

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'fk_negocios_activos_sector'
    ) THEN
        ALTER TABLE negocios_activos
            ADD CONSTRAINT fk_negocios_activos_sector
            FOREIGN KEY (sector_codigo) REFERENCES sectores(codigo)
            ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'fk_competencia_sector'
    ) THEN
        ALTER TABLE competencia_por_local
            ADD CONSTRAINT fk_competencia_sector
            FOREIGN KEY (sector_codigo) REFERENCES sectores(codigo)
            ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'fk_historico_sector'
    ) THEN
        ALTER TABLE negocios_historico
            ADD CONSTRAINT fk_historico_sector
            FOREIGN KEY (sector_codigo) REFERENCES sectores(codigo)
            ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'fk_restricciones_sector'
    ) THEN
        ALTER TABLE restricciones_geograficas_sector
            ADD CONSTRAINT fk_restricciones_sector
            FOREIGN KEY (sector_codigo) REFERENCES sectores(codigo)
            ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints
        WHERE constraint_name = 'fk_requisitos_sector'
    ) THEN
        ALTER TABLE requisitos_legales_sector
            ADD CONSTRAINT fk_requisitos_sector
            FOREIGN KEY (sector_codigo) REFERENCES sectores(codigo)
            ON UPDATE CASCADE DEFERRABLE INITIALLY DEFERRED;
    END IF;
END $$;

COMMIT;
