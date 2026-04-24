-- 010_rename_airbnb_column.sql
-- Rename airbnb_listings_500m → airbnb_density_500m to match features.py model feature name.
-- This eliminates the silent rename in scoring code that confuses SHAP value analysis.

BEGIN;

ALTER TABLE variables_zona
    RENAME COLUMN airbnb_listings_500m TO airbnb_density_500m;

-- Update the view if it references the old name
DROP VIEW IF EXISTS v_flujo_peatonal_fuentes;
-- (view will be recreated by migration 017 or manually)

COMMIT;
