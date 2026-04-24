-- ============================================================================
-- Migracion 030 - Tabla ground truth: intensitat_turismo_oficial (Open Data BCN)
--
-- Fuente:
--   opendata-ajuntament.barcelona.cat/data/dataset/intensitat-activitat-turistica
--   3 capas raster vectorizado a poligonos (~100m resolucion):
--     - allotjament (2019): concentracion de alojamiento turistico
--     - atractius   (2017): concentracion de atractivos turisticos
--     - oci         (2016): concentracion de ocio
--   Cada poligono tiene un valor DN (intensidad 0-100).
--
-- Uso:
--   Validacion del modulo dimensiones/turismo.py via Spearman correlation
--   contra esta capa oficial (script scoring/dimensiones/evaluar_turismo.py).
-- ============================================================================

CREATE TABLE IF NOT EXISTS intensitat_turismo_oficial (
    id          SERIAL PRIMARY KEY,
    capa        VARCHAR(20) NOT NULL,   -- 'allotjament' | 'atractius' | 'oci'
    anio        INTEGER NOT NULL,       -- 2019 | 2017 | 2016
    intensidad  INTEGER NOT NULL,       -- DN (~7-100)
    geom        GEOMETRY(Polygon, 4326) NOT NULL  -- reproyectado de EPSG:25831
);

CREATE INDEX IF NOT EXISTS idx_intensitat_oficial_geom
    ON intensitat_turismo_oficial USING GIST(geom);

CREATE INDEX IF NOT EXISTS idx_intensitat_oficial_capa
    ON intensitat_turismo_oficial(capa);

COMMENT ON TABLE intensitat_turismo_oficial IS
    'Ground truth oficial de Open Data BCN sobre intensidad turistica. '
    '3 capas (allotjament 2019, atractius 2017, oci 2016) con valor DN 0-100. '
    'Usada para validar el modulo dimensiones/turismo.py.';
