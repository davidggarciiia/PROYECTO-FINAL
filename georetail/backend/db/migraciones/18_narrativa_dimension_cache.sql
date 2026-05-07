-- ============================================================
-- Archivo  : 18_narrativa_dimension_cache.sql
-- Proyecto : GeoRetail
-- Propósito: Cache del análisis LLM por dimensión (lectura
--            interpretativa + 3 decisiones prácticas). Generado
--            on-demand al pulsar "Interpretar con IA" en cada
--            sección del dossier editorial.
-- Tabla    : narrativa_dimension_cache.
-- Orden    : Depende de 02_geografia.sql (zonas).
-- ============================================================

CREATE TABLE IF NOT EXISTS narrativa_dimension_cache (
    zona_id        VARCHAR(30)  NOT NULL,
    dim_key        VARCHAR(30)  NOT NULL,
    sector_codigo  VARCHAR(50)  NOT NULL,
    perfil_hash    VARCHAR(64)  NOT NULL,
    lectura        TEXT         NOT NULL,
    decisiones     JSONB        NOT NULL,
    generado_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_narrativa_dimension_cache
        PRIMARY KEY (zona_id, dim_key, sector_codigo, perfil_hash),
    CONSTRAINT fk_narrativa_dimension_cache_zona
        FOREIGN KEY (zona_id) REFERENCES zonas (id) ON DELETE CASCADE,
    CONSTRAINT ck_narrativa_dim_key
        CHECK (dim_key IN (
            'flujo_peatonal', 'demografia', 'transporte',
            'seguridad', 'turismo', 'dinamismo'
        ))
);

COMMENT ON TABLE narrativa_dimension_cache IS
    'Cache del análisis LLM por dimensión (lectura interpretativa + 3 '
    'decisiones prácticas). Se genera al pulsar "Interpretar con IA" en '
    'el dossier editorial. TTL implícito: 30 días (el endpoint comprueba '
    'generado_at >= NOW() - INTERVAL ''30 days''). Cache key incluye '
    'perfil_hash para reflejar las matices del perfil_refinado.';

CREATE INDEX IF NOT EXISTS idx_narrativa_dimension_generado
    ON narrativa_dimension_cache (generado_at DESC);
