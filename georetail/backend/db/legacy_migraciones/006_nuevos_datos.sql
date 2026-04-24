-- ═══════════════════════════════════════════════════════════════════════════
-- Migración: 006_nuevos_datos.sql
-- Descripción: Nuevas columnas de turismo/actividad comercial en variables_zona,
--              ampliar negocios_activos con datos de Google y Booking,
--              y tres tablas nuevas: venues_ocio, alojamientos_turisticos,
--              llicencies_activitat.
--
-- Ejecutar:
--   psql $DATABASE_URL -f db/migraciones/006_nuevos_datos.sql
--
-- Dependencias: 001_schema_inicial.sql ejecutada (tablas zonas, negocios_activos).
-- ═══════════════════════════════════════════════════════════════════════════

BEGIN;

-- ── 1. Nuevas columnas en variables_zona ─────────────────────────────────────
--       Datos turísticos y de actividad comercial granular

-- Turismo de plataformas colaborativas (Airbnb / Inside Airbnb)
ALTER TABLE variables_zona ADD COLUMN IF NOT EXISTS airbnb_listings_500m    INTEGER DEFAULT 0;
ALTER TABLE variables_zona ADD COLUMN IF NOT EXISTS airbnb_occupancy_est     FLOAT   DEFAULT 0.0;

-- Dinamismo comercial (Open Data BCN — llicències d'activitat)
ALTER TABLE variables_zona ADD COLUMN IF NOT EXISTS licencias_nuevas_1a      INTEGER DEFAULT 0;
ALTER TABLE variables_zona ADD COLUMN IF NOT EXISTS licencias_bajas_1a       INTEGER DEFAULT 0;

-- Ocio y cultura (OSM + Wikidata + Agenda BCN)
ALTER TABLE variables_zona ADD COLUMN IF NOT EXISTS eventos_culturales_500m  INTEGER DEFAULT 0;
ALTER TABLE variables_zona ADD COLUMN IF NOT EXISTS venues_musicales_500m    INTEGER DEFAULT 0;

-- Turismo alojado (Booking.com API)
ALTER TABLE variables_zona ADD COLUMN IF NOT EXISTS booking_hoteles_500m     INTEGER DEFAULT 0;
ALTER TABLE variables_zona ADD COLUMN IF NOT EXISTS booking_rating_medio      FLOAT;

-- Turismo crucerista (Port de Barcelona — datos anuales)
ALTER TABLE variables_zona ADD COLUMN IF NOT EXISTS cruceros_visitantes_anuales   INTEGER DEFAULT 0;

-- Puntos de interés (OSM + Agenda BCN)
ALTER TABLE variables_zona ADD COLUMN IF NOT EXISTS puntos_interes_culturales_500m INTEGER DEFAULT 0;
ALTER TABLE variables_zona ADD COLUMN IF NOT EXISTS mercados_municipales_500m      INTEGER DEFAULT 0;


-- ── 2. Ampliar negocios_activos ───────────────────────────────────────────────
--       Datos de Google Maps y Booking para enriquecer el análisis de competencia

ALTER TABLE negocios_activos ADD COLUMN IF NOT EXISTS review_count          INTEGER DEFAULT 0;
ALTER TABLE negocios_activos ADD COLUMN IF NOT EXISTS booking_property_id   VARCHAR(100);
ALTER TABLE negocios_activos ADD COLUMN IF NOT EXISTS google_place_id       VARCHAR(200);


-- ── 3. Tabla: venues_ocio ─────────────────────────────────────────────────────
--       Venues culturales y de ocio: festivales, salas de conciertos, teatros,
--       museos, mercados, playas, parques.
--       Pipeline: pipelines/venues_ocio.py (mensual día 7, 07:00)

CREATE TABLE IF NOT EXISTS venues_ocio (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    nombre            VARCHAR(300) NOT NULL,
    tipo              VARCHAR(50)  NOT NULL,
        -- 'festival' | 'sala_conciertos' | 'teatro' | 'museo' | 'mercado' | 'playa' | 'parque'
    subtipos          TEXT[],
    lat               FLOAT        NOT NULL,
    lng               FLOAT        NOT NULL,
    geometria         GEOMETRY(Point, 4326),
    aforo_max         INTEGER,
    eventos_anuales   INTEGER,
    visitantes_anuales INTEGER,
    rating            FLOAT,
    fuente            VARCHAR(50),
    url               VARCHAR(500),
    activo            BOOLEAN      DEFAULT TRUE,
    zona_id           UUID         REFERENCES zonas(id),
    created_at        TIMESTAMPTZ  DEFAULT NOW(),
    updated_at        TIMESTAMPTZ  DEFAULT NOW()
);

COMMENT ON TABLE venues_ocio IS
    'Venues culturales y de ocio en Barcelona: festivales, salas de conciertos, '
    'teatros, museos, mercados, playas, parques. '
    'Pipeline: pipelines/venues_ocio.py (mensual día 7, 07:00). '
    'Fuente: OSM + Agenda BCN + Wikidata.';

COMMENT ON COLUMN venues_ocio.tipo IS
    'Tipo de venue: festival | sala_conciertos | teatro | museo | mercado | playa | parque';

CREATE INDEX IF NOT EXISTS idx_venues_ocio_geometria ON venues_ocio USING GIST(geometria);
CREATE INDEX IF NOT EXISTS idx_venues_ocio_tipo      ON venues_ocio(tipo);
CREATE INDEX IF NOT EXISTS idx_venues_ocio_zona      ON venues_ocio(zona_id);


-- ── 4. Tabla: alojamientos_turisticos ────────────────────────────────────────
--       Hoteles y alojamientos turísticos recopilados vía Booking.com API y HUT
--       (Habitatges d'Ús Turístic — registre Generalitat).
--       Pipeline: pipelines/booking.py (semanal jueves 03:00)

CREATE TABLE IF NOT EXISTS alojamientos_turisticos (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    nombre              VARCHAR(300) NOT NULL,
    tipo                VARCHAR(50)  NOT NULL,
        -- 'hotel' | 'hostel' | 'apartamento' | 'hut'
    estrellas           SMALLINT,
    lat                 FLOAT        NOT NULL,
    lng                 FLOAT        NOT NULL,
    geometria           GEOMETRY(Point, 4326),
    num_habitaciones    INTEGER,
    precio_noche_medio  FLOAT,
    rating              FLOAT,
    num_reviews         INTEGER,
    booking_id          VARCHAR(100),
    zona_id             UUID         REFERENCES zonas(id),
    fuente              VARCHAR(50)  DEFAULT 'booking',
    activo              BOOLEAN      DEFAULT TRUE,
    created_at          TIMESTAMPTZ  DEFAULT NOW()
);

COMMENT ON TABLE alojamientos_turisticos IS
    'Hoteles y alojamientos turísticos de Barcelona. '
    'Fuente: Booking.com API + HUT (Habitatges d''Ús Turístic, Generalitat de Catalunya). '
    'Pipeline: pipelines/booking.py (semanal jueves 03:00). '
    'Usado como proxy del nivel de turismo alojado en cada zona.';

COMMENT ON COLUMN alojamientos_turisticos.tipo IS
    'Tipo de alojamiento: hotel | hostel | apartamento | hut';

CREATE INDEX IF NOT EXISTS idx_alojamientos_geometria ON alojamientos_turisticos USING GIST(geometria);
CREATE INDEX IF NOT EXISTS idx_alojamientos_zona      ON alojamientos_turisticos(zona_id);


-- ── 5. Tabla: llicencies_activitat ────────────────────────────────────────────
--       Llicències d'activitat de l'Ajuntament de Barcelona (altas y bajas
--       de negocios). Proxy de dinamismo comercial por zona.
--       Pipeline: pipelines/llicencies.py (mensual día 5, 06:00)

CREATE TABLE IF NOT EXISTS llicencies_activitat (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    expedient        VARCHAR(100),
    nom_activitat    VARCHAR(500),
    tipus_llicencia  VARCHAR(100),
    estat            VARCHAR(50),
        -- 'atorgada' | 'denegada' | 'retirada' | 'caducada'
    data_atorgament  DATE,
    data_caducitat   DATE,
    adreca           VARCHAR(500),
    lat              FLOAT,
    lng              FLOAT,
    geometria        GEOMETRY(Point, 4326),
    zona_id          UUID         REFERENCES zonas(id),
    barri            VARCHAR(100),
    districte        VARCHAR(100),
    seccio_censal    VARCHAR(20),
    any_activitat    INTEGER,
    created_at       TIMESTAMPTZ  DEFAULT NOW()
);

COMMENT ON TABLE llicencies_activitat IS
    'Llicències d''activitat de l''Ajuntament de Barcelona. '
    'Permite calcular dinàmica comercial por zona: altas (atorgada) y bajas (caducada/retirada). '
    'Pipeline: pipelines/llicencies.py (mensual día 5, 06:00). '
    'Fuente: Open Data BCN — dataset "Llicències d''activitats".';

COMMENT ON COLUMN llicencies_activitat.estat IS
    'Estat de la llicència: atorgada | denegada | retirada | caducada';

CREATE INDEX IF NOT EXISTS idx_llicencies_geometria ON llicencies_activitat USING GIST(geometria);
CREATE INDEX IF NOT EXISTS idx_llicencies_zona      ON llicencies_activitat(zona_id);
CREATE INDEX IF NOT EXISTS idx_llicencies_any       ON llicencies_activitat(any_activitat);
CREATE INDEX IF NOT EXISTS idx_llicencies_estat     ON llicencies_activitat(estat);


COMMIT;
