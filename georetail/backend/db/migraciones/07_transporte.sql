-- ============================================================
-- Archivo  : 07_transporte.sql
-- Proyecto : GeoRetail
-- Propósito: Tablas de transporte público, Bicing y carriles bici.
--            Consolida legacy 001 (lineas_transporte, paradas_transporte,
--            paradas_lineas, frecuencias_transporte) y 025 (subtipo en
--            lineas_transporte, estaciones_bicing, carriles_bici).
-- Orden    : Depende de 01_extensiones.sql (postgis, fn_set_updated_at)
--            y de 02_geografia.sql (zonas).
-- ============================================================


-- ── Tabla: lineas_transporte ─────────────────────────────────

CREATE TABLE lineas_transporte (
    id          VARCHAR(20)         NOT NULL,
    codigo      VARCHAR(10)         NOT NULL,
    nombre      VARCHAR(200),
    tipo        VARCHAR(10)         NOT NULL,
    subtipo     VARCHAR(20),
    color_hex   VARCHAR(7),
    fuente      VARCHAR(20),
    created_at  TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ         NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_lineas_transporte
        PRIMARY KEY (id),
    CONSTRAINT ck_lineas_transporte_tipo
        CHECK (tipo IN ('metro','bus','tram','fgc','rodalies'))
);

COMMENT ON TABLE lineas_transporte IS
    'Líneas de transporte público de Barcelona (metro, bus, tram, FGC, Rodalies). '
    'Consolida legacy 001 + 025. '
    'La columna subtipo distingue variantes de línea (p. ej. bus_ortogonal). '
    'Fuente: TMB API (tmb.cat), GTFS ATM, GTFS Renfe.';

COMMENT ON COLUMN lineas_transporte.id IS
    'Identificador único de la línea. Formato libre, p. ej. "L1", "H12", "T1".';
COMMENT ON COLUMN lineas_transporte.codigo IS
    'Código oficial de la línea, p. ej. "L1", "H12", "R4".';
COMMENT ON COLUMN lineas_transporte.tipo IS
    'Modo de transporte: metro, bus, tram, fgc, rodalies.';
COMMENT ON COLUMN lineas_transporte.subtipo IS
    'Subtipo dentro del modo. Para bus: "bus_ortogonal" si el código empieza '
    'por H, V, D o X (red ortogonal TMB). En otros casos replica el tipo. '
    'Añadido en legacy 025.';
COMMENT ON COLUMN lineas_transporte.color_hex IS
    'Color corporativo de la línea en hexadecimal, p. ej. "#DC241F" (L1 metro).';
COMMENT ON COLUMN lineas_transporte.fuente IS
    'Fuente de los datos: "tmb", "gtfs_atm", "gtfs_renfe".';

DO $$
BEGIN
    EXECUTE
        'CREATE OR REPLACE TRIGGER tg_set_updated_at
         BEFORE UPDATE ON lineas_transporte
         FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at()';
END;
$$;


-- ── Tabla: paradas_transporte ────────────────────────────────

CREATE TABLE paradas_transporte (
    id                  VARCHAR(30)         NOT NULL,
    nombre              VARCHAR(300)        NOT NULL,
    lat                 DOUBLE PRECISION    NOT NULL,
    lng                 DOUBLE PRECISION    NOT NULL,
    geometria           GEOMETRY(POINT, 4326),
    zona_id             VARCHAR(30),
    es_accesible_pmr    BOOLEAN,
    fuente              VARCHAR(20),
    created_at          TIMESTAMPTZ         NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_paradas_transporte
        PRIMARY KEY (id),
    CONSTRAINT fk_paradas_transporte_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE SET NULL
        ON UPDATE CASCADE
);

COMMENT ON TABLE paradas_transporte IS
    'Paradas de transporte público de Barcelona (metro, bus, tram, FGC, Rodalies). '
    'Consolida legacy 001 + 025. '
    'Rename: accesible_pmr → es_accesible_pmr para coherencia con estilo del proyecto. '
    'Fuente: TMB API + GTFS ATM.';

COMMENT ON COLUMN paradas_transporte.id IS
    'Identificador único de la parada. Formato del proveedor de datos.';
COMMENT ON COLUMN paradas_transporte.zona_id IS
    'Zona comercial más cercana asignada por geocodificación inversa. '
    'NULL si la parada no está dentro de ninguna zona analizada.';
COMMENT ON COLUMN paradas_transporte.es_accesible_pmr IS
    'TRUE si la parada es accesible para personas con movilidad reducida (PMR). '
    'Renombrado desde "accesible_pmr" (legacy 001).';
COMMENT ON COLUMN paradas_transporte.geometria IS
    'Punto PostGIS (SRID 4326). Indexado con GIST para búsquedas espaciales.';

-- Índices: paradas_transporte
CREATE INDEX IF NOT EXISTS idx_paradas_transporte_geometria
    ON paradas_transporte USING GIST (geometria)
    WHERE geometria IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_paradas_transporte_zona
    ON paradas_transporte (zona_id)
    WHERE zona_id IS NOT NULL;


-- ── Tabla: paradas_lineas ────────────────────────────────────

CREATE TABLE paradas_lineas (
    parada_id   VARCHAR(30)     NOT NULL,
    linea_id    VARCHAR(20)     NOT NULL,
    orden       INT,

    CONSTRAINT pk_paradas_lineas
        PRIMARY KEY (parada_id, linea_id),
    CONSTRAINT fk_paradas_lineas_parada
        FOREIGN KEY (parada_id)
        REFERENCES paradas_transporte (id)
        ON DELETE CASCADE,
    CONSTRAINT fk_paradas_lineas_linea
        FOREIGN KEY (linea_id)
        REFERENCES lineas_transporte (id)
        ON DELETE CASCADE
);

COMMENT ON TABLE paradas_lineas IS
    'Relación N:M entre paradas y líneas de transporte. '
    'Una parada puede ser servida por múltiples líneas; '
    'una línea tiene múltiples paradas en orden secuencial. '
    'Consolida legacy 001.';

COMMENT ON COLUMN paradas_lineas.orden IS
    'Posición ordinal de la parada dentro de la línea (1 = primera parada).';

-- Índices en FKs para JOINs frecuentes
CREATE INDEX IF NOT EXISTS idx_paradas_lineas_parada_id
    ON paradas_lineas (parada_id);

CREATE INDEX IF NOT EXISTS idx_paradas_lineas_linea_id
    ON paradas_lineas (linea_id);


-- ── Tabla: frecuencias_transporte ────────────────────────────

CREATE TABLE frecuencias_transporte (
    id              SERIAL          NOT NULL,
    linea_id        VARCHAR(20)     NOT NULL,
    dia_tipo        VARCHAR(10),
    franja          VARCHAR(10),
    frecuencia_min  DOUBLE PRECISION,

    CONSTRAINT pk_frecuencias_transporte
        PRIMARY KEY (id),
    CONSTRAINT fk_frecuencias_transporte_linea
        FOREIGN KEY (linea_id)
        REFERENCES lineas_transporte (id)
        ON DELETE CASCADE,
    CONSTRAINT ck_frecuencias_dia_tipo
        CHECK (dia_tipo IS NULL OR dia_tipo IN ('laborable','sabado','festivo')),
    CONSTRAINT ck_frecuencias_franja
        CHECK (franja IS NULL OR franja IN ('manana','tarde','noche')),
    CONSTRAINT ck_frecuencias_frecuencia_min
        CHECK (frecuencia_min IS NULL OR frecuencia_min > 0)
);

COMMENT ON TABLE frecuencias_transporte IS
    'Frecuencia de paso de cada línea de transporte por franja horaria y tipo de día. '
    'Consolida legacy 001.';

COMMENT ON COLUMN frecuencias_transporte.dia_tipo IS
    'Tipo de día: laborable, sabado, festivo.';
COMMENT ON COLUMN frecuencias_transporte.franja IS
    'Franja horaria: manana (06–14h), tarde (14–20h), noche (20–01h).';
COMMENT ON COLUMN frecuencias_transporte.frecuencia_min IS
    'Minutos entre dos vehículos consecutivos en la línea. Debe ser > 0.';

-- Índice en FK linea_id
CREATE INDEX IF NOT EXISTS idx_frecuencias_transporte_linea_id
    ON frecuencias_transporte (linea_id);


-- ── Tabla: estaciones_bicing ─────────────────────────────────
-- Estaciones del sistema de bicicleta pública Bicing de Barcelona. (legacy 025)

CREATE TABLE estaciones_bicing (
    id              VARCHAR(20)         NOT NULL,
    nombre          VARCHAR(200)        NOT NULL,
    lat             DOUBLE PRECISION    NOT NULL,
    lng             DOUBLE PRECISION    NOT NULL,
    geometria       GEOMETRY(POINT, 4326),
    capacidad       INT,
    zona_id         VARCHAR(30),
    esta_activa     BOOLEAN                      DEFAULT TRUE,
    fuente          VARCHAR(20)                  DEFAULT 'bsmsa',
    fecha_carga     TIMESTAMPTZ                  DEFAULT NOW(),

    CONSTRAINT pk_estaciones_bicing
        PRIMARY KEY (id),
    CONSTRAINT fk_estaciones_bicing_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE SET NULL,
    CONSTRAINT ck_estaciones_bicing_capacidad
        CHECK (capacidad IS NULL OR capacidad >= 0)
);

COMMENT ON TABLE estaciones_bicing IS
    'Estaciones del sistema de bicicleta pública Bicing (BSMSA). '
    'Consolida legacy 025. '
    'Rename: activa → esta_activa para coherencia de prefijo booleano. '
    'Fuente: API GBFS de Bicing (bsmsa.cat/bicing).';

COMMENT ON COLUMN estaciones_bicing.id IS
    'Identificador oficial de la estación Bicing (BSMSA).';
COMMENT ON COLUMN estaciones_bicing.capacidad IS
    'Número total de anclajes de la estación. Debe ser >= 0.';
COMMENT ON COLUMN estaciones_bicing.esta_activa IS
    'TRUE si la estación está operativa. Renombrado desde "activa" (legacy 025).';
COMMENT ON COLUMN estaciones_bicing.fuente IS
    'Fuente de los datos. Default "bsmsa" (API GBFS Bicing).';
COMMENT ON COLUMN estaciones_bicing.fecha_carga IS
    'Timestamp de la última carga del pipeline de Bicing.';

-- Índices: estaciones_bicing
CREATE INDEX IF NOT EXISTS idx_bicing_geometria
    ON estaciones_bicing USING GIST (geometria)
    WHERE geometria IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_bicing_zona
    ON estaciones_bicing (zona_id)
    WHERE zona_id IS NOT NULL;


-- ── Tabla: carriles_bici ─────────────────────────────────────
-- Red de carriles bici de Barcelona. (legacy 025)

CREATE TABLE carriles_bici (
    id          SERIAL                  NOT NULL,
    tipo        VARCHAR(30),
    geometria   GEOMETRY(LINESTRING, 4326),
    longitud_m  DOUBLE PRECISION,
    fuente      VARCHAR(20)              DEFAULT 'open_data_bcn',
    fecha_carga TIMESTAMPTZ              DEFAULT NOW(),

    CONSTRAINT pk_carriles_bici
        PRIMARY KEY (id),
    CONSTRAINT ck_carriles_bici_longitud_m
        CHECK (longitud_m IS NULL OR longitud_m >= 0)
);

COMMENT ON TABLE carriles_bici IS
    'Red de carriles bici de Barcelona geocodificados como geometría LINESTRING. '
    'Consolida legacy 025. '
    'Fuente: Open Data BCN dataset carrils-bici.';

COMMENT ON COLUMN carriles_bici.tipo IS
    'Tipo de infraestructura ciclista: '
    '"carril_protegit", "carril_bus_bici", "vorera_bici", "ciclocarrer".';
COMMENT ON COLUMN carriles_bici.geometria IS
    'Geometría LINESTRING (SRID 4326) del tramo de carril bici.';
COMMENT ON COLUMN carriles_bici.longitud_m IS
    'Longitud del tramo en metros. Puede calcularse con ST_Length(geometria::geography).';

-- Índice GIST en geometría de carril
CREATE INDEX IF NOT EXISTS idx_carriles_bici_geometria
    ON carriles_bici USING GIST (geometria)
    WHERE geometria IS NOT NULL;
