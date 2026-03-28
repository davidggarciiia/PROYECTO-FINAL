-- ─────────────────────────────────────────────────────────────────────────────
-- 008_vcity.sql  — Flujo peatonal VCity BSC (columna nueva en variables_zona)
--
-- VCity (vcity.dataviz.bsc.es) es una aplicación del Barcelona Supercomputing
-- Center (BSC DataViz Group) que muestra el promedio diario anual de peatones
-- por tramo de calle en Barcelona, calculado a partir de datos de telefonía
-- móvil y GPS.
--
-- Esta migración añade la columna vcity_flujo_peatonal a variables_zona para
-- almacenar el resultado del pipeline pipelines/vcity.py.
--
-- Diferencias respecto a otras fuentes de flujo peatonal:
--   aforaments.py         → IMD de VEHÍCULOS (proxy incorrecto para comercio)
--   vianants.py           → Comptadors de persones (Open Data BCN, mensual)
--   vcity.py (este pip.)  → Promedio diario anual BSC (móvil+GPS, annual)
--
-- La columna se mantiene NULL hasta que el pipeline vcity.py ejecute con éxito.
-- Si VCity no está disponible, el pipeline copia flujo_peatonal_total como
-- fallback (ver pipelines/vcity.py → _fallback_desde_vianants).
--
-- Ejecutar:
--   psql $DATABASE_URL -f db/migraciones/008_vcity.sql
--
-- Dependencias: 001_schema_inicial.sql + 007_vianants.sql ejecutadas.
-- ─────────────────────────────────────────────────────────────────────────────

-- ── Nueva columna: vcity_flujo_peatonal ──────────────────────────────────────
-- Promedio diario anual de peatones por zona según VCity BSC.
-- Unidad: peatones/día (intensitat media diaria anual, fuente BSC móvil+GPS).
-- NULL: pipeline no ha ejecutado aún o VCity no devolvió datos para esta zona.

ALTER TABLE variables_zona
    ADD COLUMN IF NOT EXISTS vcity_flujo_peatonal FLOAT DEFAULT NULL;

COMMENT ON COLUMN variables_zona.vcity_flujo_peatonal IS
    'Promedio diario anual de peatones por zona según VCity BSC '
    '(vcity.dataviz.bsc.es — Barcelona Supercomputing Center DataViz Group). '
    'Calculado a partir de datos de telefonía móvil + GPS. '
    'Unidad: peatones/día (IMD peatonal anual). '
    'NULL si pipeline vcity.py no ha ejecutado o VCity no cubre la zona. '
    'Fallback: copia de flujo_peatonal_total (vianants_bcn) cuando VCity no disponible. '
    'Pipeline: pipelines/vcity.py (mensual día 12, 04:00). '
    'Fuente: https://vcity.dataviz.bsc.es';

-- Índice parcial para consultas de scoring que filtran por esta columna
CREATE INDEX IF NOT EXISTS idx_variables_zona_vcity_flujo
    ON variables_zona(vcity_flujo_peatonal)
    WHERE vcity_flujo_peatonal IS NOT NULL;

-- ── Vista auxiliar: comparación de fuentes de flujo peatonal ─────────────────
-- Facilita auditoría y calibración del modelo de scoring.
-- Muestra las tres fuentes de flujo peatonal disponibles por zona.

CREATE OR REPLACE VIEW v_flujo_peatonal_fuentes AS
SELECT
    vz.zona_id,
    z.nombre                                AS zona_nombre,
    vz.fecha,
    vz.flujo_peatonal_total                 AS flujo_aforaments_vianants,
    vz.vcity_flujo_peatonal                 AS flujo_vcity_bsc,
    vz.fuente                               AS fuente_principal,
    -- Diferencia relativa entre fuentes (NULL si alguna es NULL o 0)
    CASE
        WHEN vz.flujo_peatonal_total > 0
         AND vz.vcity_flujo_peatonal IS NOT NULL
        THEN ROUND(
            (vz.vcity_flujo_peatonal - vz.flujo_peatonal_total)
            / vz.flujo_peatonal_total * 100.0, 1
        )
        ELSE NULL
    END                                     AS diferencia_pct,
    -- Mejor estimación disponible (VCity > vianants > aforaments)
    COALESCE(
        vz.vcity_flujo_peatonal,
        vz.flujo_peatonal_total
    )                                       AS flujo_mejor_estimacion
FROM variables_zona vz
JOIN zonas z ON z.id = vz.zona_id
WHERE (
    vz.flujo_peatonal_total IS NOT NULL
    OR vz.vcity_flujo_peatonal IS NOT NULL
);

COMMENT ON VIEW v_flujo_peatonal_fuentes IS
    'Comparación de fuentes de flujo peatonal por zona: '
    'aforaments/vianants vs VCity BSC. '
    'Usar flujo_mejor_estimacion para scoring (VCity prevalece cuando disponible).';
