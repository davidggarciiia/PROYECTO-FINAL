-- 026_dinamismo_zonal.sql
-- Snapshot mensual del dinamismo comercial por zona.
-- Combina: evolución de licencias, supervivencia histórica de negocios,
-- tendencia demográfica y mercado inmobiliario.

CREATE TABLE IF NOT EXISTS dinamismo_zonal (
    zona_id  VARCHAR(20) NOT NULL REFERENCES zonas(id) ON DELETE CASCADE,
    periodo  DATE    NOT NULL DEFAULT CURRENT_DATE,

    -- ── Apertura/cierre de licencias ─────────────────────────────────────────
    licencias_abiertas_1a    INTEGER DEFAULT 0,   -- nuevas licencias último año
    licencias_cerradas_1a    INTEGER DEFAULT 0,   -- bajas último año
    licencias_abiertas_3a    INTEGER DEFAULT 0,   -- nuevas licencias últimos 3 años
    licencias_cerradas_3a    INTEGER DEFAULT 0,   -- bajas últimos 3 años
    ratio_apertura_cierre_1a FLOAT,               -- abiertas/cerradas (1a); NULL si cerradas=0
    ratio_apertura_cierre_3a FLOAT,               -- abiertas/cerradas (3a)

    -- ── Supervivencia histórica (negocios_historico) ──────────────────────────
    negocios_historico_count INTEGER DEFAULT 0,   -- muestra disponible por zona
    tasa_supervivencia_3a    FLOAT,               -- % negocios que sobrevivieron ≥3 años

    -- ── Demografía evolutiva (variación % vs. 3 años atrás) ──────────────────
    renta_variacion_3a      FLOAT,                -- % cambio renta_bruta_hogar
    poblacion_variacion_3a  FLOAT,                -- % cambio población

    -- ── Mercado inmobiliario ───────────────────────────────────────────────────
    precio_alquiler_variacion_1a FLOAT,           -- % cambio precio_m2 alquiler comercial

    -- ── Diversidad sectorial ───────────────────────────────────────────────────
    hhi_sectorial  FLOAT,   -- Herfindahl-Hirschman Index (0=muy diverso, 1=monopolio)
    vacantes_ratio FLOAT,   -- ratio locales vacíos / total locales en zona

    -- ── Score y clasificación ─────────────────────────────────────────────────
    score_dinamismo FLOAT,
    tendencia VARCHAR(20) CHECK (tendencia IN (
        'emergente', 'estable', 'saturado', 'declive', 'sin_datos'
    )),

    updated_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (zona_id, periodo)
);

CREATE INDEX IF NOT EXISTS idx_dinamismo_zonal_zona    ON dinamismo_zonal(zona_id);
CREATE INDEX IF NOT EXISTS idx_dinamismo_zonal_periodo ON dinamismo_zonal(periodo DESC);
CREATE INDEX IF NOT EXISTS idx_dinamismo_zonal_score   ON dinamismo_zonal(score_dinamismo DESC NULLS LAST);

-- Vista: siempre el snapshot más reciente por zona
CREATE OR REPLACE VIEW v_dinamismo_zona AS
SELECT DISTINCT ON (zona_id) *
FROM dinamismo_zonal
ORDER BY zona_id, periodo DESC;

COMMENT ON TABLE dinamismo_zonal IS
    'Snapshot mensual del dinamismo comercial por zona — combina evolución de '
    'licencias, supervivencia histórica de negocios, tendencia demográfica '
    'y mercado inmobiliario. Ver scoring/dimensiones/dinamismo.py.';
