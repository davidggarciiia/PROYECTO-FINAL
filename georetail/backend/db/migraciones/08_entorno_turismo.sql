-- ============================================================
-- Archivo  : 08_entorno_turismo.sql
-- Proyecto : GeoRetail
-- Propósito: Tablas de entorno urbano, turismo y dinamismo comercial.
--            Consolida legacy 006 (venues_ocio, alojamientos_turisticos,
--            llicencies_activitat), 007 (vianants_trams → trams_peatonales),
--            009 (fix FK VARCHAR en zonas), 018 (parques_amb),
--            020 (comisarias), 021 (mercados_municipales),
--            026 (dinamismo_zonal + v_dinamismo_zona),
--            030 (intensitat_turismo_oficial), 031 (landmarks_turisticos),
--            032 (fix tendencia en dinamismo_zonal).
-- Orden    : Depende de 01_extensiones.sql (postgis, fn_set_updated_at)
--            y de 02_geografia.sql (zonas).
-- ============================================================


-- ── Tabla: venues_ocio ───────────────────────────────────────

CREATE TABLE venues_ocio (
    id                  UUID                    NOT NULL DEFAULT gen_random_uuid(),
    nombre              VARCHAR(300)            NOT NULL,
    tipo                VARCHAR(30)             NOT NULL,
    subtipos            TEXT[],
    lat                 DOUBLE PRECISION        NOT NULL,
    lng                 DOUBLE PRECISION        NOT NULL,
    geometria           GEOMETRY(POINT, 4326),
    aforo_max           INT,
    eventos_anuales     INT,
    visitantes_anuales  INT,
    rating              NUMERIC(3,2),
    fuente              VARCHAR(50),
    url                 VARCHAR(500),
    es_activo           BOOLEAN                 NOT NULL DEFAULT TRUE,
    zona_id             VARCHAR(30),
    created_at          TIMESTAMPTZ             NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ             NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_venues_ocio
        PRIMARY KEY (id),
    CONSTRAINT fk_venues_ocio_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE SET NULL,
    CONSTRAINT ck_venues_ocio_tipo
        CHECK (tipo IN (
            'festival','sala_conciertos','teatro','museo',
            'mercado','playa','parque'
        )),
    CONSTRAINT ck_venues_ocio_rating
        CHECK (rating IS NULL OR rating BETWEEN 0 AND 5)
);

COMMENT ON TABLE venues_ocio IS
    'Venues culturales y de ocio en Barcelona. '
    'Cubre festivales, salas de conciertos, teatros, museos, mercados, playas, parques. '
    'Consolida legacy 006. '
    'Rename: activo → es_activo. FK zona_id corregida de UUID → VARCHAR(30) (legacy 009). '
    'Pipeline: pipelines/entorno/venues_ocio.py (mensual día 7, 07:00). '
    'Fuente: OSM + Agenda BCN + Wikidata.';

COMMENT ON COLUMN venues_ocio.tipo IS
    'Tipo de venue: festival, sala_conciertos, teatro, museo, mercado, playa, parque.';
COMMENT ON COLUMN venues_ocio.subtipos IS
    'Array de subtipos adicionales del venue (etiquetas OSM u otras fuentes).';
COMMENT ON COLUMN venues_ocio.aforo_max IS
    'Aforo máximo del venue. NULL si no disponible.';
COMMENT ON COLUMN venues_ocio.eventos_anuales IS
    'Número estimado de eventos por año. NULL si no disponible.';
COMMENT ON COLUMN venues_ocio.visitantes_anuales IS
    'Número estimado de visitantes al año. NULL si no disponible.';
COMMENT ON COLUMN venues_ocio.es_activo IS
    'TRUE si el venue está activo. Renombrado desde "activo" (legacy 006).';
COMMENT ON COLUMN venues_ocio.url IS
    'URL oficial del venue. Longitud máxima 500 caracteres.';

-- Índices: venues_ocio
CREATE INDEX IF NOT EXISTS idx_venues_ocio_geometria
    ON venues_ocio USING GIST (geometria)
    WHERE geometria IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_venues_ocio_tipo
    ON venues_ocio (tipo);

CREATE INDEX IF NOT EXISTS idx_venues_ocio_zona
    ON venues_ocio (zona_id)
    WHERE zona_id IS NOT NULL;

DO $$
BEGIN
    EXECUTE
        'CREATE OR REPLACE TRIGGER tg_set_updated_at
         BEFORE UPDATE ON venues_ocio
         FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at()';
END;
$$;


-- ── Tabla: alojamientos_turisticos ───────────────────────────

CREATE TABLE alojamientos_turisticos (
    id                  UUID                NOT NULL DEFAULT gen_random_uuid(),
    nombre              VARCHAR(300)        NOT NULL,
    tipo                VARCHAR(30),
    estrellas           SMALLINT,
    lat                 DOUBLE PRECISION    NOT NULL,
    lng                 DOUBLE PRECISION    NOT NULL,
    geometria           GEOMETRY(POINT, 4326),
    num_habitaciones    INT,
    precio_noche_medio  NUMERIC(8,2),
    rating              NUMERIC(3,2),
    num_reviews         INT,
    booking_id          VARCHAR(100),
    zona_id             VARCHAR(30),
    fuente              VARCHAR(30)                  DEFAULT 'booking',
    es_activo           BOOLEAN                      DEFAULT TRUE,
    created_at          TIMESTAMPTZ         NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_alojamientos_turisticos
        PRIMARY KEY (id),
    CONSTRAINT fk_alojamientos_turisticos_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE SET NULL,
    CONSTRAINT ck_alojamientos_tipo
        CHECK (tipo IS NULL OR tipo IN ('hotel','hostel','apartamento','hut')),
    CONSTRAINT ck_alojamientos_estrellas
        CHECK (estrellas IS NULL OR estrellas BETWEEN 0 AND 5),
    CONSTRAINT ck_alojamientos_rating
        CHECK (rating IS NULL OR rating BETWEEN 0 AND 5)
);

COMMENT ON TABLE alojamientos_turisticos IS
    'Hoteles y alojamientos turísticos de Barcelona. '
    'Consolida legacy 006. '
    'Rename: activo → es_activo. FK zona_id corregida de UUID → VARCHAR(30) (legacy 009). '
    'Pipeline: pipelines/turismo/booking.py (semanal jueves 03:00). '
    'Fuente: Booking.com API + HUT (Habitatges d''Ús Turístic, Generalitat de Catalunya).';

COMMENT ON COLUMN alojamientos_turisticos.tipo IS
    'Tipo de alojamiento: hotel, hostel, apartamento, hut (habitatge d''ús turístic).';
COMMENT ON COLUMN alojamientos_turisticos.estrellas IS
    'Categoría hotelera (0–5 estrellas). 0 = sin clasificación.';
COMMENT ON COLUMN alojamientos_turisticos.precio_noche_medio IS
    'Precio medio por noche en euros. NULL si no disponible.';
COMMENT ON COLUMN alojamientos_turisticos.es_activo IS
    'TRUE si el alojamiento sigue activo. Renombrado desde "activo" (legacy 006).';
COMMENT ON COLUMN alojamientos_turisticos.booking_id IS
    'ID de propiedad en Booking.com. NULL para alojamientos de otras fuentes.';

-- Índices: alojamientos_turisticos
CREATE INDEX IF NOT EXISTS idx_alojamientos_turisticos_geometria
    ON alojamientos_turisticos USING GIST (geometria)
    WHERE geometria IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_alojamientos_turisticos_zona
    ON alojamientos_turisticos (zona_id)
    WHERE zona_id IS NOT NULL;


-- ── Tabla: licencias_actividad ───────────────────────────────
-- Renombrada desde llicencies_activitat (legacy 006) para coherencia de idioma.
-- Los valores de columnas administrativas (expedient, nom_activitat, etc.)
-- conservan el catalán por ser identificadores de datos oficiales.

CREATE TABLE licencias_actividad (
    id              UUID                NOT NULL DEFAULT gen_random_uuid(),
    expedient       VARCHAR(100),
    nom_activitat   VARCHAR(500),
    tipus_llicencia VARCHAR(100),
    estat           VARCHAR(50),
    data_atorgament DATE,
    data_caducitat  DATE,
    adreca          VARCHAR(500),
    lat             DOUBLE PRECISION,
    lng             DOUBLE PRECISION,
    geometria       GEOMETRY(POINT, 4326),
    zona_id         VARCHAR(30),
    barri           VARCHAR(100),
    districte       VARCHAR(100),
    seccio_censal   VARCHAR(20),
    anyo_actividad  INT,
    created_at      TIMESTAMPTZ         NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_licencias_actividad
        PRIMARY KEY (id),
    CONSTRAINT fk_licencias_actividad_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE SET NULL,
    CONSTRAINT ck_licencias_actividad_estat
        CHECK (estat IS NULL OR estat IN ('atorgada','denegada','retirada','caducada'))
);

COMMENT ON TABLE licencias_actividad IS
    'Llicències d''activitat de l''Ajuntament de Barcelona. '
    'Permite calcular dinámica comercial por zona: altas (atorgada) y bajas (caducada/retirada). '
    'Renombrada desde "llicencies_activitat" (legacy 006). '
    'Los nombres de columnas de datos administrativos conservan el catalán '
    'por ser identifiers del dataset oficial (expedient, nom_activitat, etc.). '
    'FK zona_id corregida de UUID → VARCHAR(30) (legacy 009). '
    'Pipeline: pipelines/comercio/llicencies.py (mensual día 5, 06:00). '
    'Fuente: Open Data BCN — dataset "Llicències d''activitats".';

COMMENT ON COLUMN licencias_actividad.expedient IS
    'Número de expediente administrativo del Ajuntament (identificador oficial).';
COMMENT ON COLUMN licencias_actividad.nom_activitat IS
    'Nombre de la actividad económica en catalán (dato oficial).';
COMMENT ON COLUMN licencias_actividad.tipus_llicencia IS
    'Tipo de licencia (dato oficial del Ajuntament).';
COMMENT ON COLUMN licencias_actividad.estat IS
    'Estado de la licencia: atorgada, denegada, retirada, caducada.';
COMMENT ON COLUMN licencias_actividad.adreca IS
    'Dirección del establecimiento en catalán (dato oficial).';
COMMENT ON COLUMN licencias_actividad.seccio_censal IS
    'Sección censal del INE donde se ubica la actividad.';

-- Índices: licencias_actividad
CREATE INDEX IF NOT EXISTS idx_licencias_actividad_geometria
    ON licencias_actividad USING GIST (geometria)
    WHERE geometria IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_licencias_actividad_zona
    ON licencias_actividad (zona_id)
    WHERE zona_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_licencias_actividad_any
    ON licencias_actividad (anyo_actividad)
    WHERE anyo_actividad IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_licencias_actividad_estat
    ON licencias_actividad (estat)
    WHERE estat IS NOT NULL;


-- ── Tabla: trams_peatonales ──────────────────────────────────
-- Renombrada desde vianants_trams (legacy 007) para consistencia de idioma.

CREATE TABLE trams_peatonales (
    id          UUID                NOT NULL DEFAULT gen_random_uuid(),
    id_tram     VARCHAR(50),
    nom_tram    VARCHAR(300),
    anyo        INT,
    mes         INT,
    intensitat  DOUBLE PRECISION,
    lat         DOUBLE PRECISION,
    lng         DOUBLE PRECISION,
    geometria   GEOMETRY(POINT, 4326),
    zona_id     VARCHAR(30),
    fuente      VARCHAR(50)                  DEFAULT 'bcn_vianants',
    created_at  TIMESTAMPTZ         NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_trams_peatonales
        PRIMARY KEY (id),
    CONSTRAINT fk_trams_peatonales_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE SET NULL
);

COMMENT ON TABLE trams_peatonales IS
    'Tramos de intensidad de vianants (peatones) en Barcelona. '
    'Cada fila = un tramo × año × mes (mes NULL si valor anual). '
    'Renombrada desde "vianants_trams" (legacy 007). '
    'El campo intensitat representa personas/hora o IMD vianants. '
    'La columna nom_tram conserva el catalán por ser identificador de datos oficiales. '
    'FK zona_id corregida de UUID → VARCHAR(30) (legacy 009). '
    'Fuente: Comptadors de vianants BCN Open Data. '
    'Pipeline: pipelines/peatonal/vianants.py.';

COMMENT ON COLUMN trams_peatonales.id_tram IS
    'Identificador oficial del tramo en el dataset BCN Open Data.';
COMMENT ON COLUMN trams_peatonales.nom_tram IS
    'Nombre del tramo o calle en catalán (dato oficial).';
COMMENT ON COLUMN trams_peatonales.anyo IS
    'Año de la medición.';
COMMENT ON COLUMN trams_peatonales.mes IS
    'Mes de la medición (1–12). NULL si el valor es anual.';
COMMENT ON COLUMN trams_peatonales.intensitat IS
    'Intensidad de paso de peatones: personas/hora o IMD vianants según la fuente.';

-- Índices: trams_peatonales
CREATE INDEX IF NOT EXISTS idx_trams_peatonales_geometria
    ON trams_peatonales USING GIST (geometria)
    WHERE geometria IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_trams_peatonales_any_mes
    ON trams_peatonales (anyo, mes);

CREATE INDEX IF NOT EXISTS idx_trams_peatonales_zona
    ON trams_peatonales (zona_id)
    WHERE zona_id IS NOT NULL;


-- ── Tabla: parques_amb ────────────────────────────────────────
-- Parques del Área Metropolitana de Barcelona. Réplica exacta de legacy 018.

CREATE TABLE parques_amb (
    id              SERIAL                  NOT NULL,
    nom             VARCHAR(300)            NOT NULL,
    municipi        VARCHAR(100)            NOT NULL DEFAULT '',
    superficie_ha   DOUBLE PRECISION,
    geometria       GEOMETRY(POINT, 4326)   NOT NULL,
    fuente          VARCHAR(50)             NOT NULL DEFAULT 'amb_opendata',
    created_at      TIMESTAMPTZ             NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ             NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_parques_amb
        PRIMARY KEY (id),
    CONSTRAINT uq_parques_amb_nom_municipi
        UNIQUE (nom, municipi)
);

COMMENT ON TABLE parques_amb IS
    'Parques del AMB con localización puntual. '
    'Consolida legacy 018. '
    'Fuente: opendata.amb.cat/serveis_parcs/search/csv. '
    'Pipeline: pipelines/entorno/parques.py. '
    'Usado para calcular m2_zonas_verdes_cercanas en vz_entorno.';

COMMENT ON COLUMN parques_amb.nom IS
    'Nombre oficial del parque (dato del AMB).';
COMMENT ON COLUMN parques_amb.municipi IS
    'Municipio donde se ubica el parque. Vacío si no especificado.';
COMMENT ON COLUMN parques_amb.superficie_ha IS
    'Superficie del parque en hectáreas.';

-- Índices: parques_amb
CREATE INDEX IF NOT EXISTS idx_parques_amb_geometria
    ON parques_amb USING GIST (geometria);

CREATE INDEX IF NOT EXISTS idx_parques_amb_municipi
    ON parques_amb (municipi);

DO $$
BEGIN
    EXECUTE
        'CREATE OR REPLACE TRIGGER tg_set_updated_at
         BEFORE UPDATE ON parques_amb
         FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at()';
END;
$$;


-- ── Tabla: comisarias ─────────────────────────────────────────
-- Comisarías de policía en Barcelona. Réplica de legacy 020.

CREATE TABLE comisarias (
    id          VARCHAR(50)             NOT NULL,
    nombre      TEXT                    NOT NULL,
    tipo        VARCHAR(30),
    direccion   TEXT,
    distrito    VARCHAR(50),
    barrio      VARCHAR(80),
    geometria   GEOMETRY(POINT, 4326),
    fuente      VARCHAR(50)             NOT NULL DEFAULT 'opendata_bcn',
    created_at  TIMESTAMPTZ             NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ             NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_comisarias
        PRIMARY KEY (id)
);

COMMENT ON TABLE comisarias IS
    'Comisarías de policía en Barcelona (Mossos d''Esquadra + Guàrdia Urbana + Policía Nacional). '
    'Consolida legacy 020. '
    'Usado para calcular comisarias_1km y dist_comisaria_m en vz_entorno. '
    'Fuente: Open Data BCN dataset comissaries-policia.';

COMMENT ON COLUMN comisarias.tipo IS
    'Cuerpo policial: "mossos_esquadra", "guardia_urbana", "policia_nacional".';
COMMENT ON COLUMN comisarias.geometria IS
    'Punto PostGIS (SRID 4326). Indexado con GIST para ST_DWithin / ST_Distance.';

-- Índices: comisarias
CREATE INDEX IF NOT EXISTS idx_comisarias_geometria
    ON comisarias USING GIST (geometria)
    WHERE geometria IS NOT NULL;

DO $$
BEGIN
    EXECUTE
        'CREATE OR REPLACE TRIGGER tg_set_updated_at
         BEFORE UPDATE ON comisarias
         FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at()';
END;
$$;


-- ── Tabla: mercados_municipales ──────────────────────────────
-- Mercados municipales de Barcelona. Réplica exacta de legacy 021.

CREATE TABLE mercados_municipales (
    id          VARCHAR(50)             NOT NULL,
    nombre      TEXT                    NOT NULL,
    direccion   TEXT,
    distrito    VARCHAR(50),
    barrio      VARCHAR(80),
    geometria   GEOMETRY(POINT, 4326),
    fuente      VARCHAR(50)             NOT NULL DEFAULT 'opendata_bcn',
    created_at  TIMESTAMPTZ             NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ             NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_mercados_municipales
        PRIMARY KEY (id)
);

COMMENT ON TABLE mercados_municipales IS
    'Mercats municipals de Barcelona (La Boqueria, Sant Antoni, Santa Caterina, etc.). '
    'Consolida legacy 021. '
    'Usado para calcular mercados_municipales_1km en vz_entorno. '
    'Fuente: Open Data BCN dataset mercats-municipals.';

COMMENT ON COLUMN mercados_municipales.nombre IS
    'Nombre oficial del mercado municipal.';

-- Índices: mercados_municipales
CREATE INDEX IF NOT EXISTS idx_mercados_municipales_geometria
    ON mercados_municipales USING GIST (geometria)
    WHERE geometria IS NOT NULL;

DO $$
BEGIN
    EXECUTE
        'CREATE OR REPLACE TRIGGER tg_set_updated_at
         BEFORE UPDATE ON mercados_municipales
         FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at()';
END;
$$;


-- ── Tabla: landmarks_turisticos ───────────────────────────────
-- POIs turísticos de Barcelona filtrados por calidad Wikidata. (legacy 031)

CREATE TABLE landmarks_turisticos (
    id              SERIAL              NOT NULL,
    osm_id          BIGINT,
    nombre          VARCHAR(200)        NOT NULL,
    wikidata_id     VARCHAR(20),
    peso_turistico  DOUBLE PRECISION    NOT NULL DEFAULT 1.0,
    geom            GEOMETRY(POINT, 4326) NOT NULL,
    created_at      TIMESTAMPTZ                  DEFAULT NOW(),

    CONSTRAINT pk_landmarks_turisticos
        PRIMARY KEY (id),
    CONSTRAINT uq_landmarks_turisticos_osm_id
        UNIQUE (osm_id)
);

COMMENT ON TABLE landmarks_turisticos IS
    'POIs turísticos reales de Barcelona (OSM tourism=attraction con tag wikidata=*). '
    'El filtro de calidad por wikidata_id descarta ruido (bibliotecas de barrio, '
    'miradores menores, etc.). '
    'Consolida legacy 031. '
    'Pipeline: pipelines/turismo/landmarks.py (Overpass API). '
    'Usado por scoring/dimensiones/turismo.py para el ajuste de proximidad a landmark.';

COMMENT ON COLUMN landmarks_turisticos.osm_id IS
    'Identificador OSM del nodo o way (node id / way id).';
COMMENT ON COLUMN landmarks_turisticos.wikidata_id IS
    'ID de Wikidata del landmark, p. ej. "Q9188" (Sagrada Família). '
    'Permite enriquecer peso_turistico con Wikipedia pageviews.';
COMMENT ON COLUMN landmarks_turisticos.peso_turistico IS
    'Peso de relevancia turística (>= 0). Default 1.0. '
    'Puede enriquecerse con Wikipedia pageviews mensuales del wikidata_id asociado.';
COMMENT ON COLUMN landmarks_turisticos.geom IS
    'Punto PostGIS (SRID 4326). Columna nombrada "geom" por consistencia con legacy 031.';

-- Índices: landmarks_turisticos
CREATE INDEX IF NOT EXISTS idx_landmarks_turisticos_geom
    ON landmarks_turisticos USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_landmarks_turisticos_wikidata
    ON landmarks_turisticos (wikidata_id)
    WHERE wikidata_id IS NOT NULL;


-- ── Tabla: intensitat_turismo_oficial ────────────────────────
-- Ground truth oficial BCN sobre intensidad turística. (legacy 030)

CREATE TABLE intensitat_turismo_oficial (
    id          SERIAL                      NOT NULL,
    capa        VARCHAR(20)                 NOT NULL,
    anio        INTEGER                     NOT NULL,
    intensidad  INTEGER                     NOT NULL,
    geom        GEOMETRY(POLYGON, 4326)     NOT NULL,

    CONSTRAINT pk_intensitat_turismo_oficial
        PRIMARY KEY (id),
    CONSTRAINT ck_intensitat_capa
        CHECK (capa IN ('allotjament','atractius','oci'))
);

COMMENT ON TABLE intensitat_turismo_oficial IS
    'Ground truth oficial de Open Data BCN sobre intensidad turística. '
    '3 capas vectoriales raster (~100m resolución): '
    'allotjament (2019), atractius (2017), oci (2016). '
    'Cada polígono tiene un valor DN de intensidad (~7–100). '
    'Consolida legacy 030. '
    'Usado para validar scoring/dimensiones/turismo.py via correlación de Spearman. '
    'Fuente: opendata-ajuntament.barcelona.cat/data/dataset/intensitat-activitat-turistica.';

COMMENT ON COLUMN intensitat_turismo_oficial.capa IS
    'Capa de datos: allotjament (alojamiento turístico), '
    'atractius (atractivos turísticos), oci (ocio).';
COMMENT ON COLUMN intensitat_turismo_oficial.anio IS
    'Año de publicación de la capa oficial (2019, 2017 o 2016).';
COMMENT ON COLUMN intensitat_turismo_oficial.intensidad IS
    'Valor DN de intensidad turística (aproximadamente 7–100).';
COMMENT ON COLUMN intensitat_turismo_oficial.geom IS
    'Polígono reproyectado de EPSG:25831 a WGS-84 (SRID 4326).';

-- Índices: intensitat_turismo_oficial
CREATE INDEX IF NOT EXISTS idx_intensitat_oficial_geom
    ON intensitat_turismo_oficial USING GIST (geom);

CREATE INDEX IF NOT EXISTS idx_intensitat_oficial_capa
    ON intensitat_turismo_oficial (capa);


-- ── Tabla: dinamismo_zonal ────────────────────────────────────
-- Snapshot mensual de dinamismo comercial por zona. (legacy 026 + 032)

CREATE TABLE dinamismo_zonal (
    zona_id                     VARCHAR(30)     NOT NULL,
    periodo                     DATE            NOT NULL DEFAULT CURRENT_DATE,

    -- Apertura/cierre de licencias
    licencias_abiertas_1a       INTEGER                  DEFAULT 0,
    licencias_cerradas_1a       INTEGER                  DEFAULT 0,
    licencias_abiertas_3a       INTEGER                  DEFAULT 0,
    licencias_cerradas_3a       INTEGER                  DEFAULT 0,
    ratio_apertura_cierre_1a    DOUBLE PRECISION,
    ratio_apertura_cierre_3a    DOUBLE PRECISION,

    -- Supervivencia histórica (negocios_historico)
    negocios_historico_count    INTEGER                  DEFAULT 0,
    tasa_supervivencia_3a       DOUBLE PRECISION,

    -- Demografía evolutiva (variación % vs. 3 años atrás)
    renta_variacion_3a          DOUBLE PRECISION,
    poblacion_variacion_3a      DOUBLE PRECISION,

    -- Mercado inmobiliario
    precio_alquiler_variacion_1a DOUBLE PRECISION,

    -- Diversidad sectorial
    hhi_sectorial               DOUBLE PRECISION,
    vacantes_ratio              DOUBLE PRECISION,

    -- Score y clasificación
    score_dinamismo             DOUBLE PRECISION,
    tendencia                   VARCHAR(20),

    updated_at                  TIMESTAMPTZ              DEFAULT NOW(),

    CONSTRAINT pk_dinamismo_zonal
        PRIMARY KEY (zona_id, periodo),
    CONSTRAINT fk_dinamismo_zonal_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE CASCADE,
    CONSTRAINT ck_dinamismo_zonal_tendencia
        CHECK (tendencia IS NULL OR tendencia IN (
            'emergente','estable','saturado','declive','sin_datos'
        ))
);

COMMENT ON TABLE dinamismo_zonal IS
    'Snapshot mensual del dinamismo comercial por zona. '
    'Combina evolución de licencias, supervivencia histórica de negocios, '
    'tendencia demográfica y mercado inmobiliario. '
    'Consolida legacy 026 + 032 (fix columna tendencia que no se añadía con IF NOT EXISTS). '
    'Escrito por: pipelines/comercio/dinamismo.py (día 6 del mes, 03:00). '
    'Consumido por: scoring/dimensiones/dinamismo.py.';

COMMENT ON COLUMN dinamismo_zonal.periodo IS
    'Fecha del snapshot mensual (primer día del mes de referencia).';
COMMENT ON COLUMN dinamismo_zonal.ratio_apertura_cierre_1a IS
    'Ratio aperturas/cierres en el último año. NULL si licencias_cerradas_1a = 0.';
COMMENT ON COLUMN dinamismo_zonal.tasa_supervivencia_3a IS
    'Porcentaje de negocios históricos de la zona que sobrevivieron ≥ 3 años.';
COMMENT ON COLUMN dinamismo_zonal.hhi_sectorial IS
    'Índice Herfindahl-Hirschman sectorial: 0 = muy diverso, 1 = monopolio.';
COMMENT ON COLUMN dinamismo_zonal.vacantes_ratio IS
    'Ratio locales vacíos / total locales en la zona.';
COMMENT ON COLUMN dinamismo_zonal.score_dinamismo IS
    'Score compuesto de dinamismo (0–100). Calculado por pipelines/comercio/dinamismo.py.';
COMMENT ON COLUMN dinamismo_zonal.tendencia IS
    'Clasificación textual derivada del score_dinamismo y otros indicadores: '
    'emergente, estable, saturado, declive, sin_datos. '
    'Fix legacy 032: la columna ahora se crea siempre en la tabla consolidada.';

-- Índices: dinamismo_zonal
CREATE INDEX IF NOT EXISTS idx_dinamismo_zonal_zona
    ON dinamismo_zonal (zona_id);

CREATE INDEX IF NOT EXISTS idx_dinamismo_zonal_periodo
    ON dinamismo_zonal (periodo DESC);

CREATE INDEX IF NOT EXISTS idx_dinamismo_zonal_score
    ON dinamismo_zonal (score_dinamismo DESC NULLS LAST);


-- ── Vista: v_dinamismo_zona ───────────────────────────────────
-- Siempre el snapshot más reciente por zona. (legacy 026 + 032)

CREATE OR REPLACE VIEW v_dinamismo_zona AS
SELECT DISTINCT ON (zona_id)
    zona_id,
    periodo,
    licencias_abiertas_1a,
    licencias_cerradas_1a,
    licencias_abiertas_3a,
    licencias_cerradas_3a,
    ratio_apertura_cierre_1a,
    ratio_apertura_cierre_3a,
    negocios_historico_count,
    tasa_supervivencia_3a,
    renta_variacion_3a,
    poblacion_variacion_3a,
    precio_alquiler_variacion_1a,
    hhi_sectorial,
    vacantes_ratio,
    score_dinamismo,
    tendencia,
    updated_at
FROM dinamismo_zonal
ORDER BY zona_id, periodo DESC;

COMMENT ON VIEW v_dinamismo_zona IS
    'Vista que devuelve siempre el snapshot mensual más reciente de dinamismo_zonal '
    'para cada zona. Usada por db/zonas.py:get_zona_completa y api/local.py. '
    'Consolida legacy 026 + 032 (incluye columna tendencia).';
