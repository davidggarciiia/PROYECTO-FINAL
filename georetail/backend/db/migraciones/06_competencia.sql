-- ============================================================
-- Archivo  : 06_competencia.sql
-- Proyecto : GeoRetail
-- Propósito: Tablas de negocios activos y análisis de competencia.
--            Consolida legacy 001 (negocios_activos, competencia_por_local),
--            006 (review_count, booking_property_id, google_place_id),
--            007 (popular_times en negocios_activos),
--            011 (índices parciales y renombre idx_negocios_geometria),
--            014 (FK sector_codigo → sectores.codigo deferrable),
--            019 (competencia_detalle_zona + columnas en competencia_por_local
--                 + vista v_competencia_zona),
--            028 (score_complementarios en competencia_detalle_zona),
--            033 (subsector_codigo en negocios_activos y competencia_detalle_zona).
-- Orden    : Depende de 01_extensiones.sql (pg_trgm, fn_set_updated_at),
--            02_geografia.sql (zonas) y 05_sectores_scoring.sql (sectores).
-- ============================================================


-- ── Tabla: negocios_activos ──────────────────────────────────

CREATE TABLE negocios_activos (
    id                      VARCHAR(40)             NOT NULL,
    nombre                  VARCHAR(300)            NOT NULL,
    sector_codigo           VARCHAR(30),
    subsector_codigo        VARCHAR(30),
    lat                     DOUBLE PRECISION        NOT NULL,
    lng                     DOUBLE PRECISION        NOT NULL,
    geometria               GEOMETRY(POINT, 4326),
    zona_id                 VARCHAR(30),
    rating                  NUMERIC(3,2),
    total_resenas           INT                              DEFAULT 0,
    review_count            INT                              DEFAULT 0,
    precio_nivel            SMALLINT,
    horario                 JSONB,
    popular_times           JSONB,
    fecha_apertura          DATE,
    es_activo               BOOLEAN                 NOT NULL DEFAULT TRUE,
    booking_property_id     VARCHAR(100),
    google_place_id         VARCHAR(200),
    fuente                  VARCHAR(30),
    created_at              TIMESTAMPTZ             NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ             NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_negocios_activos
        PRIMARY KEY (id),
    CONSTRAINT fk_negocios_activos_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE SET NULL
        ON UPDATE CASCADE,
    CONSTRAINT fk_negocios_activos_sector
        FOREIGN KEY (sector_codigo)
        REFERENCES sectores (codigo)
        ON UPDATE CASCADE
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT ck_negocios_activos_rating
        CHECK (rating IS NULL OR rating BETWEEN 0 AND 5),
    CONSTRAINT ck_negocios_activos_total_resenas
        CHECK (total_resenas IS NULL OR total_resenas >= 0),
    CONSTRAINT ck_negocios_activos_review_count
        CHECK (review_count IS NULL OR review_count >= 0),
    CONSTRAINT ck_negocios_activos_precio_nivel
        CHECK (precio_nivel IS NULL OR precio_nivel BETWEEN 1 AND 4)
);

COMMENT ON TABLE negocios_activos IS
    'Todos los negocios activos en Barcelona. '
    'Fuente primaria: Google Places → Foursquare → Yelp → OSM → Cens Locals BCN. '
    'Consolida legacy 001 + 006 + 007 + 011 + 014 + 033. '
    'Renames: activo → es_activo, num_resenas → total_resenas. '
    'review_count (legacy 006) se mantiene por compatibilidad; usar total_resenas en código nuevo.';

COMMENT ON COLUMN negocios_activos.id IS
    'Identificador externo del negocio (Google Places ID, Foursquare ID, etc.).';
COMMENT ON COLUMN negocios_activos.nombre IS
    'Nombre comercial del negocio. Indexado con GIN trigram para búsqueda aproximada.';
COMMENT ON COLUMN negocios_activos.sector_codigo IS
    'FK a sectores(codigo). DEFERRABLE para bulk inserts. '
    'NULL si el sector no se ha podido clasificar.';
COMMENT ON COLUMN negocios_activos.subsector_codigo IS
    'Subsector fino clasificado por el pipeline gosom/resenas. '
    'NULL si el subsector no es mapeable por la taxonomía. '
    'Fuente canónica: scoring/taxonomia.py (25 subsectores). '
    'Añadido en legacy 033.';
COMMENT ON COLUMN negocios_activos.geometria IS
    'Punto PostGIS (SRID 4326). Derivable de lat/lng.';
COMMENT ON COLUMN negocios_activos.total_resenas IS
    'Número total de reseñas del negocio. Renombrado desde "num_resenas" (legacy 001).';
COMMENT ON COLUMN negocios_activos.review_count IS
    'Alias legacy de total_resenas añadido en migración 006. '
    'Mantener por compatibilidad; usar total_resenas en código nuevo.';
COMMENT ON COLUMN negocios_activos.popular_times IS
    'Histograma Popular Times de Google Maps: {dia: [24 valores 0-100 por hora]}. '
    'NULL si el negocio no tiene datos suficientes. Añadido en legacy 007.';
COMMENT ON COLUMN negocios_activos.horario IS
    'Horarios de apertura por día de la semana (JSON de Google Places).';
COMMENT ON COLUMN negocios_activos.es_activo IS
    'TRUE si el negocio está actualmente abierto. Renombrado desde "activo" (legacy 001).';
COMMENT ON COLUMN negocios_activos.booking_property_id IS
    'ID de propiedad en Booking.com, si aplica. Añadido en legacy 006.';
COMMENT ON COLUMN negocios_activos.google_place_id IS
    'Google Place ID canónico del negocio. Añadido en legacy 006.';

-- Índices: negocios_activos
-- Spatial parcial sobre negocios activos (legacy 011 — drop+recreate del idx_negocios_geometria)
CREATE INDEX IF NOT EXISTS idx_negocios_activos_geometria
    ON negocios_activos USING GIST (geometria)
    WHERE es_activo = TRUE;

CREATE INDEX IF NOT EXISTS idx_negocios_activos_zona
    ON negocios_activos (zona_id);

CREATE INDEX IF NOT EXISTS idx_negocios_activos_sector_codigo
    ON negocios_activos (sector_codigo);

-- Combo zona+sector solo para activos (legacy 011)
CREATE INDEX IF NOT EXISTS idx_negocios_activos_zona_sector
    ON negocios_activos (zona_id, sector_codigo)
    WHERE es_activo = TRUE;

-- Subsector combo (legacy 033)
CREATE INDEX IF NOT EXISTS idx_negocios_activos_sector_sub
    ON negocios_activos (sector_codigo, subsector_codigo)
    WHERE es_activo = TRUE AND subsector_codigo IS NOT NULL;

-- Búsqueda aproximada por nombre (pg_trgm, legacy 001)
CREATE INDEX IF NOT EXISTS idx_negocios_activos_nombre_trgm
    ON negocios_activos USING GIN (nombre gin_trgm_ops);

-- Popular times GIN (legacy 007)
CREATE INDEX IF NOT EXISTS idx_negocios_activos_popular_times
    ON negocios_activos USING GIN (popular_times)
    WHERE popular_times IS NOT NULL;

-- Horario GIN
CREATE INDEX IF NOT EXISTS idx_negocios_activos_horario_gin
    ON negocios_activos USING GIN (horario)
    WHERE horario IS NOT NULL;

DO $$
BEGIN
    EXECUTE
        'CREATE OR REPLACE TRIGGER tg_set_updated_at
         BEFORE UPDATE ON negocios_activos
         FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at()';
END;
$$;


-- ── Tabla: competencia_por_local ─────────────────────────────

CREATE TABLE competencia_por_local (
    id                  SERIAL                  NOT NULL,
    zona_id             VARCHAR(30)             NOT NULL,
    sector_codigo       VARCHAR(30),
    radio_m             INT                     NOT NULL,
    num_competidores    INT,
    rating_medio        NUMERIC(3,2),
    score_saturacion    NUMERIC(5,2),
    num_vulnerables     INT,
    num_complementarios INT,
    fuente              VARCHAR(30)                      DEFAULT 'google_places',
    fecha_calculo       DATE                    NOT NULL,

    CONSTRAINT pk_competencia_por_local
        PRIMARY KEY (id),
    CONSTRAINT fk_competencia_por_local_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE CASCADE,
    CONSTRAINT fk_competencia_por_local_sector
        FOREIGN KEY (sector_codigo)
        REFERENCES sectores (codigo)
        ON UPDATE CASCADE
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT uq_competencia_por_local
        UNIQUE (zona_id, sector_codigo, radio_m, fecha_calculo),
    CONSTRAINT ck_competencia_radio_m
        CHECK (radio_m > 0),
    CONSTRAINT ck_competencia_score_saturacion
        CHECK (score_saturacion IS NULL OR score_saturacion BETWEEN 0 AND 100)
);

COMMENT ON TABLE competencia_por_local IS
    'Resumen de competencia por zona, sector y radio de búsqueda. '
    'Consolida legacy 001 + 014 + 019. '
    'El scorer v1 utiliza score_saturacion; usar competencia_detalle_zona.score_competencia_v2 '
    'para scoring avanzado.';

COMMENT ON COLUMN competencia_por_local.radio_m IS
    'Radio de búsqueda en metros: típicamente 300, 500 o 1000. Debe ser > 0.';
COMMENT ON COLUMN competencia_por_local.score_saturacion IS
    'Score de saturación del sector en la zona (0–100). '
    '100 = zona muy saturada. Scorer v1. Para v2 usar competencia_detalle_zona.';
COMMENT ON COLUMN competencia_por_local.num_vulnerables IS
    'Número de competidores directos con rating < 3.5 (proxy de oportunidad). '
    'Añadido en legacy 019.';
COMMENT ON COLUMN competencia_por_local.num_complementarios IS
    'Número de negocios en sectores complementarios en radio 500m. '
    'Añadido en legacy 019.';
COMMENT ON COLUMN competencia_por_local.fuente IS
    'Fuente de los datos de competencia. Default: "google_places". '
    'Añadido en legacy 019.';

-- Índice en FK zona+sector para JOINs frecuentes
CREATE INDEX IF NOT EXISTS idx_competencia_por_local_zona_sector
    ON competencia_por_local (zona_id, sector_codigo);


-- ── Tabla: competencia_detalle_zona ─────────────────────────
-- Análisis avanzado de competencia (legacy 019 + 028 + 033).

CREATE TABLE competencia_detalle_zona (
    id                              SERIAL                  NOT NULL,
    zona_id                         VARCHAR(30)             NOT NULL,
    sector_codigo                   VARCHAR(30)             NOT NULL,
    subsector_codigo                VARCHAR(30),
    radio_m                         INT                     NOT NULL DEFAULT 500,
    fecha                           DATE                    NOT NULL DEFAULT CURRENT_DATE,

    -- Competencia directa
    num_directos                    INT                              DEFAULT 0,
    rating_medio_directos           DOUBLE PRECISION,
    precio_nivel_medio              DOUBLE PRECISION,
    num_resenas_medio               DOUBLE PRECISION,

    -- Métricas de vulnerabilidad
    pct_vulnerables                 DOUBLE PRECISION                 DEFAULT 0.0,
    hhi_index                       DOUBLE PRECISION                 DEFAULT 0.0,

    -- Efecto aglomeración
    cluster_score                   DOUBLE PRECISION                 DEFAULT 50.0,
    num_sectores_complementarios    INT                              DEFAULT 0,
    ratio_complementarios           DOUBLE PRECISION                 DEFAULT 0.0,

    -- Síntesis
    amenaza_incumbentes             DOUBLE PRECISION                 DEFAULT 50.0,
    oportunidad_mercado             DOUBLE PRECISION                 DEFAULT 50.0,
    score_competencia_v2            DOUBLE PRECISION                 DEFAULT 50.0,
    score_complementarios           DOUBLE PRECISION,

    -- Control
    fuente                          VARCHAR(30)             NOT NULL DEFAULT 'google_places',
    created_at                      TIMESTAMPTZ             NOT NULL DEFAULT NOW(),
    updated_at                      TIMESTAMPTZ             NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_competencia_detalle_zona
        PRIMARY KEY (id),
    CONSTRAINT fk_competencia_detalle_zona_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE CASCADE,
    CONSTRAINT uq_competencia_detalle
        UNIQUE (zona_id, sector_codigo, radio_m, fecha)
);

COMMENT ON TABLE competencia_detalle_zona IS
    'Análisis avanzado de competencia por zona y sector. '
    'Distingue buena competencia (aglomeración, complementarios) de mala '
    '(incumbentes fuertes, saturación). Radio 500m por defecto. '
    'Escrito por: pipelines/comercio/competencia.py. '
    'Usado por: scoring/dimensiones/competencia.py → scorer.py. '
    'Consolida legacy 019 + 028 + 033.';

COMMENT ON COLUMN competencia_detalle_zona.subsector_codigo IS
    'Hook para futura agregación por subsector. '
    'Actualmente NULL en la mayoría de filas; la señal de subsector se resuelve '
    'on-the-fly desde negocios_activos.subsector_codigo. '
    'Añadido en legacy 033.';
COMMENT ON COLUMN competencia_detalle_zona.cluster_score IS
    'Efecto aglomeración sectorial (0–100). Campana con óptimo por sector: '
    'restauracion=8, moda=5, estetica=4, tatuajes=3, shisha_lounge=2. '
    'Más competidores hasta el óptimo = mejor (destino consolidado).';
COMMENT ON COLUMN competencia_detalle_zona.amenaza_incumbentes IS
    'Gravity model: Σ(fortaleza_i × 1/dist_m²) normalizado 0–100. '
    'Fortaleza_i = (rating/5)² × log(num_resenas+1). '
    '100 = zona dominada por competidores excelentes.';
COMMENT ON COLUMN competencia_detalle_zona.oportunidad_mercado IS
    'Gap de mercado captuable (0–100). '
    'Compuesto: pct_vulnerables×50 + (1-hhi)×30 + gap_precio×20.';
COMMENT ON COLUMN competencia_detalle_zona.hhi_index IS
    'Índice Herfindahl-Hirschman basado en ratings ponderados. '
    '0 = atomizado (muchos igualados), 1 = monopolio (uno domina).';
COMMENT ON COLUMN competencia_detalle_zona.score_competencia_v2 IS
    'Score final de competencia v2 (0–100). '
    'Fórmula: 0.30×cluster + 0.30×(100-amenaza) + 0.25×oportunidad + 0.15×complementarios.';
COMMENT ON COLUMN competencia_detalle_zona.score_complementarios IS
    'Score de sinergia con sectores complementarios (0–100). '
    'Calculado por scoring.dimensiones.competencia._score_complementarios(). '
    'Incluye bonus por proximidad (<200 m = peso doble). '
    'Añadido en legacy 028. Puede ser NULL en filas antiguas.';

-- Índices: competencia_detalle_zona
CREATE INDEX IF NOT EXISTS idx_comp_detalle_zona_sector
    ON competencia_detalle_zona (zona_id, sector_codigo);

CREATE INDEX IF NOT EXISTS idx_comp_detalle_fecha
    ON competencia_detalle_zona (fecha DESC);

CREATE INDEX IF NOT EXISTS idx_comp_detalle_score
    ON competencia_detalle_zona (score_competencia_v2 DESC);

-- Subsector combo (legacy 033)
CREATE INDEX IF NOT EXISTS idx_competencia_detalle_sub
    ON competencia_detalle_zona (sector_codigo, subsector_codigo)
    WHERE subsector_codigo IS NOT NULL;

DO $$
BEGIN
    EXECUTE
        'CREATE OR REPLACE TRIGGER tg_set_updated_at
         BEFORE UPDATE ON competencia_detalle_zona
         FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at()';
END;
$$;


-- ── Vista: v_competencia_zona ────────────────────────────────
-- Une v1 (competencia_por_local) con v2 (competencia_detalle_zona).
-- Adaptada con los renames: activo → es_activo. (legacy 019)

CREATE OR REPLACE VIEW v_competencia_zona AS
SELECT
    cdz.zona_id,
    cdz.sector_codigo,
    cdz.fecha,
    -- v1 (compatibilidad retroactiva)
    cpl.num_competidores,
    cpl.rating_medio,
    cpl.score_saturacion,
    -- v2 (análisis avanzado)
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
    cdz.score_competencia_v2,
    cdz.score_complementarios
FROM competencia_detalle_zona  cdz
LEFT JOIN competencia_por_local cpl
    ON  cpl.zona_id       = cdz.zona_id
    AND cpl.sector_codigo = cdz.sector_codigo
    AND cpl.radio_m       = cdz.radio_m
    AND cpl.fecha_calculo = cdz.fecha;

COMMENT ON VIEW v_competencia_zona IS
    'Vista unificada de competencia v1 + v2. '
    'Usar cdz.score_competencia_v2 para scoring avanzado. '
    'cpl.score_saturacion disponible para compatibilidad retroactiva. '
    'Consolida legacy 019 + 028.';
