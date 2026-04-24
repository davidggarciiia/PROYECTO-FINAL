-- ============================================================================
-- Migración 028 — Añadir score_complementarios a competencia_detalle_zona
--
-- Problema: la tabla competencia_detalle_zona almacena ratio_complementarios
-- (valor 0-1 bruto) pero no el score 0-100 que produce _score_complementarios().
-- Esto obliga a get_competencia_zona() a aproximar score_complementarios como
-- ratio * 66.67, que no captura el bonus de proximidad (<200m) del scoring.
--
-- Solución: añadir columna score_complementarios y que el pipeline la escriba.
-- ============================================================================

ALTER TABLE competencia_detalle_zona
    ADD COLUMN IF NOT EXISTS score_complementarios FLOAT DEFAULT NULL;

COMMENT ON COLUMN competencia_detalle_zona.score_complementarios IS
    'Score de sinergia con sectores complementarios (0-100). '
    'Calculado por scoring.dimensiones.competencia._score_complementarios(). '
    'Incluye bonus por proximidad (<200m = peso doble). '
    'Escrito por: pipelines/comercio/competencia.py. '
    'Columna añadida en migración 028 — puede ser NULL en filas antiguas.';
