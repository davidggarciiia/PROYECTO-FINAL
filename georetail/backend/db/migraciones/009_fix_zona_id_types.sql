-- 009_fix_zona_id_types.sql
-- Fix: venues_ocio, alojamientos_turisticos, llicencies_activitat, vianants_trams
-- declared zona_id as UUID but zonas.id is VARCHAR(20).
-- All FK relationships were silently broken.

BEGIN;

-- venues_ocio
ALTER TABLE venues_ocio DROP CONSTRAINT IF EXISTS venues_ocio_zona_id_fkey;
ALTER TABLE venues_ocio ALTER COLUMN zona_id TYPE VARCHAR(20) USING zona_id::text;
ALTER TABLE venues_ocio ADD CONSTRAINT fk_venues_ocio_zona
    FOREIGN KEY (zona_id) REFERENCES zonas(id) ON DELETE SET NULL;

-- alojamientos_turisticos
ALTER TABLE alojamientos_turisticos DROP CONSTRAINT IF EXISTS alojamientos_turisticos_zona_id_fkey;
ALTER TABLE alojamientos_turisticos ALTER COLUMN zona_id TYPE VARCHAR(20) USING zona_id::text;
ALTER TABLE alojamientos_turisticos ADD CONSTRAINT fk_alojamientos_zona
    FOREIGN KEY (zona_id) REFERENCES zonas(id) ON DELETE SET NULL;

-- llicencies_activitat
ALTER TABLE llicencies_activitat DROP CONSTRAINT IF EXISTS llicencies_activitat_zona_id_fkey;
ALTER TABLE llicencies_activitat ALTER COLUMN zona_id TYPE VARCHAR(20) USING zona_id::text;
ALTER TABLE llicencies_activitat ADD CONSTRAINT fk_llicencies_zona
    FOREIGN KEY (zona_id) REFERENCES zonas(id) ON DELETE SET NULL;

-- vianants_trams
ALTER TABLE vianants_trams DROP CONSTRAINT IF EXISTS vianants_trams_zona_id_fkey;
ALTER TABLE vianants_trams ALTER COLUMN zona_id TYPE VARCHAR(20) USING zona_id::text;
ALTER TABLE vianants_trams ADD CONSTRAINT fk_vianants_trams_zona
    FOREIGN KEY (zona_id) REFERENCES zonas(id) ON DELETE SET NULL;

COMMIT;
