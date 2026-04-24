-- ============================================================================
-- Migración 019 — Competencia v2: análisis avanzado buena vs mala competencia
--
-- Cambios:
--   1. competencia_detalle_zona: tabla de análisis avanzado (radio 500m)
--      - cluster_score         — efecto aglomeración sectorial (campana)
--      - amenaza_incumbentes   — gravity model rating × distancia
--      - oportunidad_mercado   — gap captuable (vulnerables + HHI + precio)
--      - ratio_complementarios — negocios sinérgicos en radio
--      - pct_vulnerables       — % competidores desplazables (rating < 3.5)
--      - hhi_index             — Herfindahl-Hirschman (0=atomizado, 1=monopolio)
--      - score_competencia_v2  — score final ponderado (reemplaza 100-saturacion)
--   2. Índices espaciales y de búsqueda
--   3. Columnas de soporte en competencia_por_local (compatibilidad)
-- ============================================================================


-- ── 1. Tabla principal ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS competencia_detalle_zona (
    id               SERIAL PRIMARY KEY,
    zona_id          VARCHAR(20)  NOT NULL REFERENCES zonas(id) ON DELETE CASCADE,
    sector_codigo    VARCHAR(30)  NOT NULL,
    radio_m          INT          NOT NULL DEFAULT 500,
    fecha            DATE         NOT NULL DEFAULT CURRENT_DATE,

    -- ── Competencia directa (mismo sector + sustitutos) ──────────────────────
    num_directos              INT     DEFAULT 0,   -- nº competidores directos en radio
    rating_medio_directos     FLOAT,               -- media ponderada por dist inversa
    precio_nivel_medio        FLOAT,               -- 1-4, segmento de mercado dominante
    num_resenas_medio         FLOAT,               -- reseñas medias (proxy popularidad)

    -- ── Métricas de vulnerabilidad ────────────────────────────────────────────
    pct_vulnerables           FLOAT   DEFAULT 0.0, -- % directos con rating < 3.5
    hhi_index                 FLOAT   DEFAULT 0.0, -- 0=atomizado, 1=monopolio
    -- HHI = Σ(share_i²) donde share_i = rating_i / Σrating

    -- ── Efecto aglomeración ───────────────────────────────────────────────────
    cluster_score             FLOAT   DEFAULT 50.0, -- 0-100 campana óptima por sector
    num_sectores_complementarios INT  DEFAULT 0,    -- nº sectores distintos sinérgicos
    ratio_complementarios     FLOAT   DEFAULT 0.0,  -- complementarios / máx esperado

    -- ── Síntesis ──────────────────────────────────────────────────────────────
    amenaza_incumbentes       FLOAT   DEFAULT 50.0, -- 0-100: qué peligrosos son los que hay
    oportunidad_mercado       FLOAT   DEFAULT 50.0, -- 0-100: gap captuable
    score_competencia_v2      FLOAT   DEFAULT 50.0, -- score final 0-100

    -- ── Control ───────────────────────────────────────────────────────────────
    fuente                    VARCHAR(30) NOT NULL DEFAULT 'google_places',
    created_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_competencia_detalle UNIQUE (zona_id, sector_codigo, radio_m, fecha)
);

COMMENT ON TABLE competencia_detalle_zona IS
    'Análisis avanzado de competencia por zona y sector. '
    'Distingue buena competencia (aglomeración, complementarios) de mala '
    '(incumbentes fuertes, saturación). Radio 500m. '
    'Escrito por: pipelines/competencia.py. '
    'Usado por: scoring/competencia.py → scorer.py.';

COMMENT ON COLUMN competencia_detalle_zona.cluster_score IS
    'Efecto aglomeración sectorial (0-100). Campana con óptimo por sector: '
    'restauracion=8, moda=5, estetica=4, tatuajes=3, shisha_lounge=2. '
    'Más competidores hasta el óptimo = mejor (destino consolidado). '
    'Más allá del óptimo = saturación = peor.';

COMMENT ON COLUMN competencia_detalle_zona.amenaza_incumbentes IS
    'Gravity model: Σ(fortaleza_i × 1/dist_m²) normalizado 0-100. '
    'Fortaleza_i = (rating/5)² × log(num_resenas+1). '
    '100 = zona dominada por competidores excelentes. '
    '0 = ningún competidor fuerte.';

COMMENT ON COLUMN competencia_detalle_zona.oportunidad_mercado IS
    'Gap de mercado captuable (0-100). '
    'Compuesto: pct_vulnerables×50 + (1-hhi)×30 + gap_precio×20. '
    'Alto = muchos competidores débiles, mercado atomizado, nicho de precio libre.';

COMMENT ON COLUMN competencia_detalle_zona.hhi_index IS
    'Índice Herfindahl-Hirschman basado en ratings ponderados. '
    'Mide concentración del mercado: 0=atomizado (muchos igualados), '
    '1=monopolio (uno domina). Bajo = oportunidad de entrada.';

COMMENT ON COLUMN competencia_detalle_zona.score_competencia_v2 IS
    'Score final de competencia v2 (0-100). '
    'Fórmula: 0.30×cluster + 0.30×(100-amenaza) + 0.25×oportunidad + 0.15×complementarios. '
    'Reemplaza la fórmula simplista "100 - score_saturacion" de v1.';


-- ── 2. Índices ────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_comp_detalle_zona_sector
    ON competencia_detalle_zona (zona_id, sector_codigo);

CREATE INDEX IF NOT EXISTS idx_comp_detalle_fecha
    ON competencia_detalle_zona (fecha DESC);

CREATE INDEX IF NOT EXISTS idx_comp_detalle_score
    ON competencia_detalle_zona (score_competencia_v2 DESC);


-- ── 3. Trigger updated_at ─────────────────────────────────────────────────────

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgname = 'trg_updated_at'
          AND tgrelid = 'competencia_detalle_zona'::regclass
    ) THEN
        CREATE TRIGGER trg_updated_at
            BEFORE UPDATE ON competencia_detalle_zona
            FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();
    END IF;
EXCEPTION WHEN undefined_function THEN
    NULL;  -- fn_set_updated_at se crea en migración 013
END;
$$;


-- ── 4. Añadir columnas de soporte en competencia_por_local (compatibilidad) ───
-- Permite que el scorer v1 siga funcionando mientras se migra a v2

ALTER TABLE competencia_por_local
    ADD COLUMN IF NOT EXISTS num_vulnerables   INT,
    ADD COLUMN IF NOT EXISTS num_complementarios INT,
    ADD COLUMN IF NOT EXISTS fuente            VARCHAR(30) DEFAULT 'google_places';

COMMENT ON COLUMN competencia_por_local.num_vulnerables IS
    'Nº de competidores directos con rating < 3.5 (proxy oportunidad de desplazar).';
COMMENT ON COLUMN competencia_por_local.num_complementarios IS
    'Nº de negocios en sectores complementarios en radio 500m.';


-- ── 5. Vista de compatibilidad ────────────────────────────────────────────────
-- Une la vista v_variables_zona con los datos de competencia v2

DROP VIEW IF EXISTS v_competencia_zona CASCADE;

CREATE VIEW v_competencia_zona AS
SELECT
    cdz.zona_id,
    cdz.sector_codigo,
    cdz.fecha,
    -- v1 (compatibilidad)
    cpl.num_competidores,
    cpl.rating_medio,
    cpl.score_saturacion,
    -- v2 (nuevos)
    cdz.num_directos,
    cdz.rating_medio_directos,
    cdz.precio_nivel_medio,
    cdz.pct_vulnerables,
    cdz.hhi_index,
    cdz.cluster_score,
    cdz.num_sectores_complementarios,
    cdz.ratio_complementarios,
    cdz.amenaza_incumbentes,
    cdz.oportunidad_mercado,
    cdz.score_competencia_v2
FROM competencia_detalle_zona cdz
LEFT JOIN competencia_por_local cpl
    ON  cpl.zona_id      = cdz.zona_id
    AND cpl.sector_codigo = cdz.sector_codigo
    AND cpl.radio_m       = cdz.radio_m
    AND cpl.fecha_calculo = cdz.fecha;

COMMENT ON VIEW v_competencia_zona IS
    'Vista unificada de competencia v1 + v2. '
    'Usar cdz.score_competencia_v2 para scoring avanzado. '
    'cpl.score_saturacion disponible para compatibilidad retroactiva.';
