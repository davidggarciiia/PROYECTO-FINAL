-- ═══════════════════════════════════════════════════════════════════════════
-- Migración: 004_inmuebles_portales.sql
-- Descripción: Tabla unificada de anuncios inmobiliarios multi-portal
--              + vista de mercado + constraint en precios_alquiler_zona
--
-- Ejecutar:
--   psql $DATABASE_URL -f db/migraciones/004_inmuebles_portales.sql
--
-- Dependencias: tablas barrios, distritos, precios_alquiler_zona existentes.
-- ═══════════════════════════════════════════════════════════════════════════

BEGIN;

-- ── 1. Tabla principal ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS inmuebles_portales (
    id              BIGSERIAL    PRIMARY KEY,
    portal_id       TEXT         NOT NULL UNIQUE,  -- "{fuente}_{id_interno}" ej: "idealista_12345678"
    fuente          TEXT         NOT NULL,          -- "idealista"|"fotocasa"|"habitaclia"|"milanuncios"|"pisos"
    url             TEXT         NOT NULL,
    tipo_operacion  TEXT         NOT NULL,          -- "alquiler-locales"|"venta-locales"|"alquiler-viviendas"
    tipo_inmueble   TEXT         NOT NULL DEFAULT 'local',

    -- Precio
    precio              NUMERIC(12,2),
    precio_m2           NUMERIC(10,2),
    precio_anterior     NUMERIC(12,2),

    -- Superficie
    superficie_util      NUMERIC(10,2),
    superficie_construida NUMERIC(10,2),

    -- Localización
    titulo         TEXT,
    direccion      TEXT,
    barrio         TEXT,
    distrito       TEXT,
    codigo_postal  CHAR(5),
    lat            DOUBLE PRECISION,
    lon            DOUBLE PRECISION,

    -- Campos enriquecidos (principalmente Idealista)
    anyo_construccion    SMALLINT,
    estado_conservacion  TEXT,
    certificado_energia  CHAR(1),
    escaparate           BOOLEAN,
    esquina              BOOLEAN,
    sotano               BOOLEAN,
    altura_libre_m       NUMERIC(5,2),
    potencia_kw          NUMERIC(8,2),
    dias_publicado       SMALLINT,
    es_particular        BOOLEAN  DEFAULT FALSE,
    agente_nombre        TEXT,
    num_fotos            SMALLINT DEFAULT 0,
    etiquetas            TEXT[]   DEFAULT '{}',

    -- Control
    fecha_scraping  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE inmuebles_portales IS
    'Anuncios inmobiliarios scrapeados de múltiples portales. '
    'Portales: idealista, fotocasa, habitaclia, milanuncios, pisos. '
    'Pipeline: pipelines/mercado_inmobiliario.py. '
    'Fuente de precio_m2_alquiler para scoring/features.py.';

COMMENT ON COLUMN inmuebles_portales.portal_id IS
    'ID único global: "{fuente}_{id_del_portal}". Ej: "idealista_12345678", "fotocasa_87654321".';

-- ── 2. Índices ────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_portales_fuente_tipo
    ON inmuebles_portales (fuente, tipo_operacion, tipo_inmueble);

CREATE INDEX IF NOT EXISTS idx_portales_barrio
    ON inmuebles_portales (barrio)
    WHERE barrio IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_portales_distrito
    ON inmuebles_portales (distrito)
    WHERE distrito IS NOT NULL;

-- Índice parcial para el cálculo de medianas (solo locales alquiler recientes)
CREATE INDEX IF NOT EXISTS idx_portales_precio_alquiler
    ON inmuebles_portales (barrio, precio_m2)
    WHERE tipo_operacion = 'alquiler-locales'
      AND precio_m2 IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_portales_fecha_scraping
    ON inmuebles_portales (fecha_scraping DESC);


-- ── 3. Añadir columnas a precios_alquiler_zona (si no existen) ────────────────

ALTER TABLE precios_alquiler_zona
    ADD COLUMN IF NOT EXISTS fuente     TEXT    DEFAULT 'manual',
    ADD COLUMN IF NOT EXISTS n_muestras INTEGER DEFAULT 0;

-- Constraint para el ON CONFLICT del pipeline (upsert diario por zona+tipo)
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uq_precios_zona_dia'
    ) THEN
        ALTER TABLE precios_alquiler_zona
            ADD CONSTRAINT uq_precios_zona_dia
            UNIQUE (zona_id, tipo, (DATE_TRUNC('day', fecha)));
    END IF;
END $$;


-- ── 4. Vista de mercado por zona ──────────────────────────────────────────────
--       Consumida por api/mercado.py

CREATE OR REPLACE VIEW v_mercado_zona AS
SELECT
    COALESCE(ip.barrio, ip.distrito)        AS zona,
    CASE
        WHEN ip.barrio IS NOT NULL THEN 'barrio'
        ELSE 'distrito'
    END                                     AS nivel,
    ip.tipo_operacion,
    ip.tipo_inmueble,
    COUNT(*)                                AS n_anuncios,
    COUNT(DISTINCT ip.fuente)               AS num_portales,

    -- Precios
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ip.precio_m2)::NUMERIC(10,2)
                                            AS precio_m2_mediana,
    ROUND(AVG(ip.precio_m2)::NUMERIC, 2)   AS precio_m2_media,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY ip.precio_m2)::NUMERIC(10,2)
                                            AS precio_m2_p25,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY ip.precio_m2)::NUMERIC(10,2)
                                            AS precio_m2_p75,
    MIN(ip.precio_m2)                       AS precio_m2_min,
    MAX(ip.precio_m2)                       AS precio_m2_max,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ip.precio)::NUMERIC(12,2)
                                            AS precio_mediano,

    -- Superficie
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY ip.superficie_util)::NUMERIC(10,2)
                                            AS superficie_mediana,
    ROUND(AVG(ip.superficie_util)::NUMERIC, 1)
                                            AS superficie_media,

    -- Mercado
    ROUND(AVG(ip.dias_publicado)::NUMERIC, 1)
                                            AS dias_mercado_medio,
    ROUND(100.0 * AVG(CASE WHEN ip.es_particular THEN 1.0 ELSE 0.0 END), 1)
                                            AS pct_particular,

    -- Características de locales (campos enriquecidos de Idealista)
    ROUND(100.0 * AVG(CASE WHEN ip.esquina    THEN 1.0 ELSE 0.0 END), 1)
                                            AS pct_esquina,
    ROUND(100.0 * AVG(CASE WHEN ip.escaparate THEN 1.0 ELSE 0.0 END), 1)
                                            AS pct_escaparate,

    MAX(ip.fecha_scraping)                  AS ultimo_scraping

FROM inmuebles_portales ip
WHERE ip.precio_m2 IS NOT NULL
  AND ip.precio_m2 BETWEEN 3 AND 300
  AND ip.fecha_scraping > NOW() - INTERVAL '90 days'
GROUP BY 1, 2, 3, 4
HAVING COUNT(*) >= 2;

COMMENT ON VIEW v_mercado_zona IS
    'Estadísticas de mercado por zona, agregadas de todos los portales. '
    'Consumida por api/mercado.py. Requiere >= 2 anuncios por zona.';


-- ── 5. Función de purga mensual ───────────────────────────────────────────────

CREATE OR REPLACE FUNCTION purgar_portales_antiguos(dias INTEGER DEFAULT 180)
RETURNS INTEGER
LANGUAGE plpgsql AS $$
DECLARE eliminados INTEGER;
BEGIN
    DELETE FROM inmuebles_portales
    WHERE fecha_scraping < NOW() - (dias || ' days')::INTERVAL
      AND tipo_inmueble = 'vivienda';
    GET DIAGNOSTICS eliminados = ROW_COUNT;
    RETURN eliminados;
END;
$$;

COMMENT ON FUNCTION purgar_portales_antiguos IS
    'Elimina anuncios de vivienda más antiguos de N días (default 180). '
    'Los locales y oficinas se conservan para histórico.';


COMMIT;
