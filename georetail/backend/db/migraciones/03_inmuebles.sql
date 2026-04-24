-- ============================================================
-- Archivo  : 03_inmuebles.sql
-- Proyecto : GeoRetail
-- Propósito: Tablas de inmuebles y mercado inmobiliario.
--            Consolida las migraciones legacy 001 (bloque inmuebles),
--            004 (inmuebles_portales + v_mercado_zona + precios_alquiler_zona),
--            015 (columna pipeline_run_id en precios_alquiler_zona),
--            016 (columna zona_id en inmuebles_portales).
-- Orden    : Depende de 01_extensiones.sql (fn_set_updated_at)
--            y de 02_geografia.sql (zonas).
-- ============================================================

-- ── Tabla: locales ──────────────────────────────────────────

CREATE TABLE locales (
    id                      VARCHAR(40)             NOT NULL,
    zona_id                 VARCHAR(30),
    direccion               VARCHAR(500),
    lat                     DOUBLE PRECISION        NOT NULL,
    lng                     DOUBLE PRECISION        NOT NULL,
    geometria               GEOMETRY(POINT, 4326),
    m2                      DOUBLE PRECISION,
    planta                  VARCHAR(10),
    escaparate_ml           DOUBLE PRECISION,
    referencia_catastral    VARCHAR(30),
    alquiler_mensual        NUMERIC(10,2),
    esta_disponible         BOOLEAN                 NOT NULL DEFAULT TRUE,
    fuente                  VARCHAR(30),
    created_at              TIMESTAMPTZ             NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ             NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_locales
        PRIMARY KEY (id),
    CONSTRAINT fk_locales_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE SET NULL
        ON UPDATE CASCADE,
    CONSTRAINT ck_locales_m2
        CHECK (m2 IS NULL OR m2 > 0),
    CONSTRAINT ck_locales_alquiler
        CHECK (alquiler_mensual IS NULL OR alquiler_mensual >= 0)
);

COMMENT ON TABLE locales IS
    'Locales comerciales puntuales (uno por anuncio o registro manual). '
    'Consolida legacy 001 + 004 + 015 + 016. '
    'La columna esta_disponible reemplaza a la antigua "disponible".';

COMMENT ON COLUMN locales.id IS
    'Identificador único del local; VARCHAR(40) para acomodar IDs de portales externos.';
COMMENT ON COLUMN locales.zona_id IS
    'FK a zonas(id). NULL si el local no ha sido geocodificado a una zona.';
COMMENT ON COLUMN locales.lat IS
    'Latitud WGS-84 del local. Obligatoria.';
COMMENT ON COLUMN locales.lng IS
    'Longitud WGS-84 del local. Obligatoria.';
COMMENT ON COLUMN locales.geometria IS
    'Punto PostGIS en SRID 4326. Se puede derivar de lat/lng con ST_SetSRID(ST_MakePoint(lng, lat), 4326).';
COMMENT ON COLUMN locales.m2 IS
    'Superficie útil en metros cuadrados. Debe ser > 0 si se especifica.';
COMMENT ON COLUMN locales.planta IS
    'Planta del local: "PB" (planta baja), "1" (primera), "-1" (sótano), etc.';
COMMENT ON COLUMN locales.escaparate_ml IS
    'Longitud de escaparate en metros lineales. NULL si no disponible.';
COMMENT ON COLUMN locales.alquiler_mensual IS
    'Precio de alquiler mensual en euros. Debe ser >= 0 si se especifica.';
COMMENT ON COLUMN locales.esta_disponible IS
    'TRUE si el local está disponible para alquilar. Renombrado desde "disponible".';
COMMENT ON COLUMN locales.fuente IS
    'Origen del registro: "idealista", "fotocasa", "manual", etc.';

-- ── Índices: locales ────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_locales_zona_id
    ON locales (zona_id)
    WHERE zona_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_locales_geometria
    ON locales USING GIST (geometria)
    WHERE geometria IS NOT NULL;

-- ── Tabla: locales_historico_precios ────────────────────────

CREATE TABLE locales_historico_precios (
    id          SERIAL                      NOT NULL,
    local_id    VARCHAR(40)                 NOT NULL,
    fecha       DATE                        NOT NULL,
    precio      NUMERIC(10,2)               NOT NULL,
    fuente      VARCHAR(30),
    created_at  TIMESTAMPTZ                 NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_locales_historico
        PRIMARY KEY (id),
    CONSTRAINT fk_locales_historico_local
        FOREIGN KEY (local_id)
        REFERENCES locales (id)
        ON DELETE CASCADE,
    CONSTRAINT ck_locales_historico_precio
        CHECK (precio >= 0),
    CONSTRAINT uq_locales_historico_local_fecha_fuente
        UNIQUE (local_id, fecha, fuente)
);

COMMENT ON TABLE locales_historico_precios IS
    'Serie histórica de precios de alquiler por local. '
    'Un registro por local × fecha × fuente. '
    'Se elimina en cascada cuando se borra el local padre.';

COMMENT ON COLUMN locales_historico_precios.precio IS
    'Precio de alquiler mensual en euros en la fecha indicada. Debe ser >= 0.';
COMMENT ON COLUMN locales_historico_precios.fuente IS
    'Portal o fuente del precio: "idealista", "fotocasa", "manual", etc.';

-- Índice en FK (rendimiento JOIN)
CREATE INDEX IF NOT EXISTS idx_locales_historico_local_id
    ON locales_historico_precios (local_id);

-- ── Tabla: inmuebles_portales ────────────────────────────────

CREATE TABLE inmuebles_portales (
    id                      BIGSERIAL               NOT NULL,
    portal_id               VARCHAR(100)            NOT NULL,
    fuente                  VARCHAR(30)             NOT NULL,
    url                     VARCHAR(1000)           NOT NULL,
    tipo_operacion          VARCHAR(30)             NOT NULL,
    tipo_inmueble           VARCHAR(30)             NOT NULL DEFAULT 'local',
    precio                  NUMERIC(12,2),
    precio_m2               NUMERIC(10,2),
    precio_anterior         NUMERIC(12,2),
    superficie_util         NUMERIC(10,2),
    superficie_construida   NUMERIC(10,2),
    titulo                  VARCHAR(500),
    direccion               VARCHAR(500),
    barrio                  VARCHAR(100),
    distrito                VARCHAR(100),
    codigo_postal           CHAR(5),
    lat                     DOUBLE PRECISION,
    lon                     DOUBLE PRECISION,
    anyo_construccion       SMALLINT,
    estado_conservacion     VARCHAR(50),
    certificado_energia     CHAR(1),
    es_escaparate           BOOLEAN,
    es_esquina              BOOLEAN,
    es_sotano               BOOLEAN,
    altura_libre_m          NUMERIC(5,2),
    potencia_kw             NUMERIC(8,2),
    dias_publicado          SMALLINT,
    es_particular           BOOLEAN                 NOT NULL DEFAULT FALSE,
    agente_nombre           VARCHAR(200),
    num_fotos               SMALLINT                         DEFAULT 0,
    etiquetas               TEXT[]                           DEFAULT '{}',
    zona_id                 VARCHAR(30),
    es_activo               BOOLEAN                 NOT NULL DEFAULT TRUE,
    fecha_scraping          TIMESTAMPTZ             NOT NULL DEFAULT NOW(),
    created_at              TIMESTAMPTZ             NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_inmuebles_portales
        PRIMARY KEY (id),
    CONSTRAINT uq_inmuebles_portales_portal_id
        UNIQUE (portal_id),
    CONSTRAINT fk_inmuebles_portales_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE SET NULL
        ON UPDATE CASCADE,
    CONSTRAINT ck_inmuebles_portales_fuente
        CHECK (fuente IN ('idealista','fotocasa','habitaclia','milanuncios','pisos','booking'))
);

COMMENT ON TABLE inmuebles_portales IS
    'Anuncios de inmuebles scrapeados de portales inmobiliarios. '
    'Consolida legacy 004 + 016. '
    'Un registro por anuncio único (portal_id es la clave del portal de origen). '
    'Los booleanos de características llevan prefijo "es_" (es_escaparate, es_esquina, es_sotano).';

COMMENT ON COLUMN inmuebles_portales.portal_id IS
    'Identificador único del anuncio en el portal de origen. '
    'Formato: "<fuente>_<id_externo>", p. ej. "idealista_12345678".';
COMMENT ON COLUMN inmuebles_portales.fuente IS
    'Portal de origen. Valores válidos: idealista, fotocasa, habitaclia, milanuncios, pisos, booking.';
COMMENT ON COLUMN inmuebles_portales.precio IS
    'Precio en euros. Para alquiler, es el precio mensual; para venta, el total.';
COMMENT ON COLUMN inmuebles_portales.precio_m2 IS
    'Precio por metro cuadrado en euros/m². NULL si la superficie no está disponible.';
COMMENT ON COLUMN inmuebles_portales.zona_id IS
    'FK a zonas(id) asignada por geocodificación inversa. NULL hasta que se procese. '
    'Añadida en legacy 016.';
COMMENT ON COLUMN inmuebles_portales.es_activo IS
    'TRUE si el anuncio sigue activo en el portal. Renombrado desde "activo" (legacy 004).';
COMMENT ON COLUMN inmuebles_portales.certificado_energia IS
    'Letra de certificación energética: A, B, C, D, E, F o G.';
COMMENT ON COLUMN inmuebles_portales.altura_libre_m IS
    'Altura libre interior en metros. Relevante para locales comerciales.';
COMMENT ON COLUMN inmuebles_portales.potencia_kw IS
    'Potencia eléctrica contratada o disponible en kW.';
COMMENT ON COLUMN inmuebles_portales.etiquetas IS
    'Array de etiquetas descriptivas extraídas del anuncio.';

-- ── Índices: inmuebles_portales ──────────────────────────────

CREATE INDEX IF NOT EXISTS idx_portales_fuente_tipo
    ON inmuebles_portales (fuente, tipo_operacion, tipo_inmueble);

CREATE INDEX IF NOT EXISTS idx_portales_barrio
    ON inmuebles_portales (barrio)
    WHERE barrio IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_portales_distrito
    ON inmuebles_portales (distrito)
    WHERE distrito IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_portales_precio_alquiler
    ON inmuebles_portales (barrio, precio_m2)
    WHERE tipo_operacion = 'alquiler-locales'
      AND precio_m2 IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_portales_fecha_scraping
    ON inmuebles_portales (fecha_scraping DESC);

CREATE INDEX IF NOT EXISTS idx_portales_zona
    ON inmuebles_portales (zona_id)
    WHERE zona_id IS NOT NULL;

-- ── Tabla: precios_alquiler_zona ─────────────────────────────

CREATE TABLE precios_alquiler_zona (
    id              SERIAL                  NOT NULL,
    zona_id         VARCHAR(30)             NOT NULL,
    fecha           DATE                    NOT NULL,
    precio_m2       NUMERIC(10,2)           NOT NULL,
    precio_min      NUMERIC(10,2),
    precio_max      NUMERIC(10,2),
    num_muestras    INT                     NOT NULL DEFAULT 0,
    n_muestras      INT                              DEFAULT 0,
    fuente          VARCHAR(30)             NOT NULL DEFAULT 'manual',
    pipeline_run_id INT,
    created_at      TIMESTAMPTZ             NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_precios_alquiler_zona
        PRIMARY KEY (id),
    CONSTRAINT fk_precios_alquiler_zona_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE CASCADE,
    CONSTRAINT ck_precios_precio_m2
        CHECK (precio_m2 > 0),
    CONSTRAINT ck_precios_num_muestras
        CHECK (num_muestras >= 0),
    CONSTRAINT uq_precios_zona_fecha_fuente
        UNIQUE (zona_id, fecha, fuente)
);

COMMENT ON TABLE precios_alquiler_zona IS
    'Estadísticas de precio de alquiler comercial agregadas por zona y fecha. '
    'Consolida legacy 004 + 015. '
    'Cada fila representa la estadística de precios de una zona en un periodo dado.';

COMMENT ON COLUMN precios_alquiler_zona.precio_m2 IS
    'Precio mediano de alquiler en euros/m²/mes para la zona y fecha. Debe ser > 0.';
COMMENT ON COLUMN precios_alquiler_zona.precio_min IS
    'Precio mínimo observado en euros/m²/mes. NULL si no disponible.';
COMMENT ON COLUMN precios_alquiler_zona.precio_max IS
    'Precio máximo observado en euros/m²/mes. NULL si no disponible.';
COMMENT ON COLUMN precios_alquiler_zona.num_muestras IS
    'Número de anuncios usados para calcular el precio (columna principal).';
COMMENT ON COLUMN precios_alquiler_zona.n_muestras IS
    'Alias legacy de num_muestras. Mantenida por compatibilidad con callers legacy (migración 004). '
    'Preferir num_muestras en código nuevo.';
COMMENT ON COLUMN precios_alquiler_zona.pipeline_run_id IS
    'ID de la ejecución del pipeline que generó este registro (tabla pipeline_ejecuciones). '
    'Sin FK declarada para evitar dependencia cruzada; la integridad se valida en código.';
COMMENT ON COLUMN precios_alquiler_zona.fuente IS
    'Fuente de los datos: "idealista", "fotocasa", "open_data_bcn", "manual", etc.';

-- Índices: precios_alquiler_zona
CREATE INDEX IF NOT EXISTS idx_precios_zona_fecha
    ON precios_alquiler_zona (zona_id, fecha DESC);

-- ── Vista: v_mercado_zona ────────────────────────────────────

CREATE OR REPLACE VIEW v_mercado_zona AS
SELECT
    z.id                                    AS zona_id,
    z.nombre                                AS zona_nombre,
    COUNT(ip.id)
        FILTER (WHERE ip.es_activo)         AS num_anuncios_activos,
    AVG(ip.precio_m2)
        FILTER (WHERE ip.es_activo
                  AND ip.tipo_operacion = 'alquiler-locales')
                                            AS precio_m2_medio_alquiler,
    MIN(ip.precio_m2)
        FILTER (WHERE ip.es_activo
                  AND ip.tipo_operacion = 'alquiler-locales')
                                            AS precio_m2_min_alquiler,
    MAX(ip.precio_m2)
        FILTER (WHERE ip.es_activo
                  AND ip.tipo_operacion = 'alquiler-locales')
                                            AS precio_m2_max_alquiler,
    COUNT(ip.id)
        FILTER (WHERE ip.es_activo
                  AND ip.es_escaparate)     AS num_con_escaparate,
    COUNT(ip.id)
        FILTER (WHERE ip.es_activo
                  AND ip.es_esquina)        AS num_en_esquina,
    COUNT(ip.id)
        FILTER (WHERE ip.es_activo
                  AND ip.es_particular)     AS num_de_particular,
    MAX(ip.fecha_scraping)                  AS ultimo_scraping
FROM zonas z
LEFT JOIN inmuebles_portales ip
    ON ip.zona_id = z.id
GROUP BY z.id, z.nombre;

COMMENT ON VIEW v_mercado_zona IS
    'Resumen del mercado de alquiler comercial por zona. '
    'Usa los renames es_activo / es_escaparate / es_esquina / es_particular de legacy 016.';

-- ── Tabla: cache_geocoding ───────────────────────────────────

CREATE TABLE cache_geocoding (
    direccion_normalizada   VARCHAR(500)            NOT NULL,
    lat                     DOUBLE PRECISION        NOT NULL,
    lng                     DOUBLE PRECISION        NOT NULL,
    nivel_precision         VARCHAR(20),
    fuente                  VARCHAR(30),
    created_at              TIMESTAMPTZ             NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_cache_geocoding
        PRIMARY KEY (direccion_normalizada)
);

COMMENT ON TABLE cache_geocoding IS
    'Caché de resultados de geocodificación para evitar llamadas repetidas a APIs externas. '
    'La clave primaria es la dirección normalizada (minúsculas, sin tildes, sin dobles espacios).';

COMMENT ON COLUMN cache_geocoding.nivel_precision IS
    'Nivel de precisión devuelto por el geocodificador: "rooftop", "range_interpolated", '
    '"geometric_center", "approximate". Renombrado desde "precision" (keyword SQL).';
COMMENT ON COLUMN cache_geocoding.fuente IS
    'Geocodificador que resolvió la dirección: "google", "nominatim", "opencage".';
COMMENT ON COLUMN cache_geocoding.lat IS
    'Latitud WGS-84 de la dirección geocodificada.';
COMMENT ON COLUMN cache_geocoding.lng IS
    'Longitud WGS-84 de la dirección geocodificada.';

-- ── Triggers updated_at ──────────────────────────────────────
-- Solo tablas con columna updated_at: locales.
-- inmuebles_portales solo tiene created_at → no aplica trigger.

DO $$
BEGIN
    EXECUTE
        'CREATE OR REPLACE TRIGGER tg_set_updated_at
         BEFORE UPDATE ON locales
         FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at()';
END;
$$;
