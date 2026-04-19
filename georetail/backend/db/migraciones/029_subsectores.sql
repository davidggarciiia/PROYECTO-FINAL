-- =============================================================================
-- Migración 029 — Subsectores finos en negocios_activos + competencia
-- =============================================================================
--
-- Contexto:
--   La v13 del modelo ya trae 58 features, 5 dimensiones y un scorer de
--   competencia avanzado (`pipelines/comercio/competencia.py` + dimensión
--   `scoring/dimensiones/competencia.py`). Todo trabaja a nivel de sector macro
--   (restauracion/moda/estetica/tatuajes/shisha_lounge/...).
--
--   El salto de señal real llega cuando se distingue subsector: un pizzero
--   "ve" 3 pizzerías a 150 m en lugar de 18 restaurantes a 150 m — un orden
--   de magnitud distinto. La clasificación fina vive en
--   `scoring/taxonomia.py` (25 subsectores repartidos en los 5 macro).
--
-- Alcance de esta migración:
--   - `negocios_activos.subsector_codigo`: columna nueva, rellena por gosom_client
--     y por el pipeline que persiste reseñas cuando hay `category` mapeable.
--   - `competencia_detalle_zona.subsector_codigo`: columna nueva, rellena por
--     el pipeline de competencia cuando el perfil del negocio declara subsector.
--   - NO tocamos `competencia_por_local`: su agregado sigue siendo macro y la
--     capa de subsector se calcula on-the-fly desde `negocios_activos`.
-- =============================================================================

-- 1. Negocios individuales — subsector clasificado al insertar.
ALTER TABLE negocios_activos
    ADD COLUMN IF NOT EXISTS subsector_codigo VARCHAR(30);

CREATE INDEX IF NOT EXISTS idx_negocios_sector_sub
    ON negocios_activos(sector_codigo, subsector_codigo)
    WHERE activo = TRUE AND subsector_codigo IS NOT NULL;

COMMENT ON COLUMN negocios_activos.subsector_codigo IS
    'Subsector fino clasificado desde category/categories (gosom). '
    'NULL = sector conocido pero subsector no mapeable por la taxonomía. '
    'Fuente canónica: scoring/taxonomia.py (25 subsectores).';

-- 2. Agregado zonal — permite opcionalmente desglosar por subsector cuando el
--    usuario declara uno específico. Si `subsector_codigo IS NULL` la fila es
--    el agregado macro del sector (compatible con filas v1 ya existentes).
ALTER TABLE competencia_detalle_zona
    ADD COLUMN IF NOT EXISTS subsector_codigo VARCHAR(30);

CREATE INDEX IF NOT EXISTS idx_competencia_detalle_sub
    ON competencia_detalle_zona(sector_codigo, subsector_codigo)
    WHERE subsector_codigo IS NOT NULL;

COMMENT ON COLUMN competencia_detalle_zona.subsector_codigo IS
    'Dimensión del agregado: NULL = todo el sector macro, valor = sólo ese subsector.';
