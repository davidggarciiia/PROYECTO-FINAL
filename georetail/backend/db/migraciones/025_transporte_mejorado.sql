-- 020_transporte_mejorado.sql
-- Mejoras en la dimensión de transporte:
--   1. Columna subtipo en lineas_transporte para distinguir bus ortogonal (H/V/D/X)
--   2. Tabla estaciones_bicing (Bicing Barcelona)
--   3. Tabla carriles_bici (red de carriles bici BCN)

-- ─── 1. subtipo en lineas_transporte ─────────────────────────────────────────

ALTER TABLE lineas_transporte
    ADD COLUMN IF NOT EXISTS subtipo VARCHAR(20);

-- Buses ortogonales: código empieza por H, V, D o X (red ortogonal TMB)
UPDATE lineas_transporte
SET subtipo = 'bus_ortogonal'
WHERE tipo = 'bus'
  AND codigo ~ '^[HVDXhvdx]';

-- Para el resto, subtipo = tipo
UPDATE lineas_transporte
SET subtipo = tipo
WHERE subtipo IS NULL;

-- ─── 2. estaciones_bicing ─────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS estaciones_bicing (
    id              VARCHAR(20) PRIMARY KEY,   -- ID estación BSMSA
    nombre          TEXT NOT NULL,
    lat             FLOAT NOT NULL,
    lng             FLOAT NOT NULL,
    geometria       GEOMETRY(POINT, 4326),
    capacidad       INT,                        -- número total de anclajes
    zona_id         VARCHAR(20) REFERENCES zonas(id),
    activa          BOOLEAN DEFAULT TRUE,
    fuente          VARCHAR(20) DEFAULT 'bsmsa',
    fecha_carga     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bicing_geometria
    ON estaciones_bicing USING GIST(geometria);

CREATE INDEX IF NOT EXISTS idx_bicing_zona
    ON estaciones_bicing(zona_id);

-- ─── 3. carriles_bici ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS carriles_bici (
    id              SERIAL PRIMARY KEY,
    tipo            VARCHAR(30),               -- 'carril_protegit','carril_bus_bici','vorera_bici','ciclocarrer'
    geometria       GEOMETRY(LINESTRING, 4326),
    longitud_m      FLOAT,
    fuente          VARCHAR(20) DEFAULT 'open_data_bcn',
    fecha_carga     TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_carriles_bici_geometria
    ON carriles_bici USING GIST(geometria);
