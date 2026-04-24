-- ============================================================
-- Archivo  : 02_geografia.sql
-- Proyecto : GeoRetail
-- Propósito: Define la jerarquía geográfica del sistema:
--            distritos → barrios → zonas.
--            Consolida la migración legacy 001 (bloque geografía)
--            y la 013 (columnas de auditoría updated_at).
-- Fuente   : Cartografía oficial del Ayuntamiento de Madrid.
--            Las geometrías son opcionales en la carga inicial;
--            los seeds insertan registros sin geometria.
-- Orden    : Depende de 01_extensiones.sql (PostGIS + fn_set_updated_at).
-- ============================================================

-- ── Tabla: distritos ────────────────────────────────────────

CREATE TABLE distritos (
    id          SERIAL                          NOT NULL,
    codigo      VARCHAR(4)                      NOT NULL,
    nombre      VARCHAR(200)                    NOT NULL,
    geometria   GEOMETRY(MULTIPOLYGON, 4326),           -- NULL permitido: seeds no incluyen geometría
    created_at  TIMESTAMPTZ                     NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ                     NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_distritos          PRIMARY KEY (id),
    CONSTRAINT uq_distritos_codigo   UNIQUE      (codigo)
);

COMMENT ON TABLE distritos IS
    'Unidades administrativas de primer nivel (distrito municipal). '
    'Fuente: Cartografía oficial Ayuntamiento de Madrid. '
    'La columna geometria admite NULL para permitir seeds sin geometría.';

-- ── Tabla: barrios ───────────────────────────────────────────

CREATE TABLE barrios (
    id          SERIAL                          NOT NULL,
    codigo      VARCHAR(6)                      NOT NULL,
    nombre      VARCHAR(200)                    NOT NULL,
    distrito_id INT                             NOT NULL,
    geometria   GEOMETRY(MULTIPOLYGON, 4326),           -- NULL permitido: seeds no incluyen geometría
    created_at  TIMESTAMPTZ                     NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ                     NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_barrios            PRIMARY KEY (id),
    CONSTRAINT uq_barrios_codigo     UNIQUE      (codigo),
    CONSTRAINT fk_barrios_distrito   FOREIGN KEY (distrito_id)
        REFERENCES distritos (id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE
);

COMMENT ON TABLE barrios IS
    'Subdivisiones de distrito (barrio municipal). '
    'Fuente: Cartografía oficial Ayuntamiento de Madrid. '
    'La columna geometria admite NULL para permitir seeds sin geometría.';

-- ── Tabla: zonas ─────────────────────────────────────────────

CREATE TABLE zonas (
    id          VARCHAR(30)                     NOT NULL,   -- ampliado de 20 → 30
    nombre      VARCHAR(200)                    NOT NULL,
    barrio_id   INT                             NOT NULL,
    geometria   GEOMETRY(POLYGON, 4326),
    area_m2     DOUBLE PRECISION,
    created_at  TIMESTAMPTZ                     NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ                     NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_zonas              PRIMARY KEY (id),
    CONSTRAINT fk_zonas_barrio       FOREIGN KEY (barrio_id)
        REFERENCES barrios (id)
        ON DELETE RESTRICT
        ON UPDATE CASCADE
);

COMMENT ON TABLE zonas IS
    'Zonas comerciales de análisis dentro de cada barrio. '
    'Fuente: definición interna GeoRetail. '
    'id es clave natural de negocio (VARCHAR 30); '
    'area_m2 se puede calcular a partir de geometria con ST_Area.';

COMMENT ON COLUMN zonas.id IS
    'Clave natural de negocio; identificador único de zona (máx. 30 caracteres).';
COMMENT ON COLUMN zonas.geometria IS
    'Polígono simple (POLYGON) en WGS-84 que delimita la zona comercial.';
COMMENT ON COLUMN zonas.area_m2 IS
    'Superficie en metros cuadrados. Valor calculado o nulo si la geometría no está cargada.';

-- ── Índices espaciales (GIST) ────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_distritos_geometria  ON distritos USING GIST (geometria);
CREATE INDEX IF NOT EXISTS idx_barrios_geometria    ON barrios   USING GIST (geometria);
CREATE INDEX IF NOT EXISTS idx_zonas_geometria      ON zonas     USING GIST (geometria);

-- ── Índices en claves foráneas ───────────────────────────────

CREATE INDEX IF NOT EXISTS idx_barrios_distrito_id  ON barrios (distrito_id);
CREATE INDEX IF NOT EXISTS idx_zonas_barrio_id      ON zonas   (barrio_id);

-- ── Triggers updated_at en las 3 tablas ─────────────────────

DO $$
DECLARE
    t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY['distritos', 'barrios', 'zonas']
    LOOP
        EXECUTE format(
            'CREATE OR REPLACE TRIGGER tg_set_updated_at
             BEFORE UPDATE ON %I
             FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at()',
            t
        );
    END LOOP;
END;
$$;
