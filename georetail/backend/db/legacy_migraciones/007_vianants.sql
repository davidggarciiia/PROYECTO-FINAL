-- ─────────────────────────────────────────────────────────────────────────────
-- 007_vianants.sql  — Taula de trams de vianants (flux peatonal real)
--
-- Diferència respecte aforaments (sensors de trànsit rodador):
--   aforaments.py  → sensors IMD de vehicles + bicis → proxy INCORRECTE per comerç
--   vianants.py    → comptadors de PERSONES a peu    → correcte per localització comercial
--
-- El camp variables_zona.flujo_peatonal_total és sobreescrit per vianants.py
-- quan hi ha dades disponibles (fuente='vianants_bcn').
--
-- Addicionalment: columna popular_times a negocios_activos per emmagatzemar
-- el histograma de hores punta scraped des de Google Maps (Popular Times).
-- columna flujo_popular_times_score a variables_zona per la nova feature v4.
-- ─────────────────────────────────────────────────────────────────────────────

-- ── Taula principal de trams de vianants ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS vianants_trams (
    id          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    id_tram     VARCHAR(50),
    nom_tram    VARCHAR(300),
    any         INTEGER,
    mes         INTEGER,                          -- NULL si valor anual
    intensitat  FLOAT,                            -- personas/hora o IMD vianants
    lat         DOUBLE PRECISION,                 -- latitud del punt representatiu
    lng         DOUBLE PRECISION,                 -- longitud del punt representatiu
    geometria   GEOMETRY(Point, 4326),            -- punt central del tram
    zona_id     UUID         REFERENCES zonas(id) ON DELETE SET NULL,
    fuente      VARCHAR(50)  DEFAULT 'bcn_vianants',
    created_at  TIMESTAMPTZ  DEFAULT NOW()
);

-- Índexs
CREATE INDEX IF NOT EXISTS idx_vianants_geometria
    ON vianants_trams USING GIST(geometria);

CREATE INDEX IF NOT EXISTS idx_vianants_any_mes
    ON vianants_trams(any, mes);

CREATE INDEX IF NOT EXISTS idx_vianants_zona
    ON vianants_trams(zona_id);

-- ── Popular Times: nova columna a negocios_activos ─────────────────────────────
-- Histograma de hores punta scraped des de Google Maps.
-- Estructura: {"lunes": [0,0,10,20,40,70,80,60,40,20,10,0,...], ...}
-- 24 valors per dia (hores 0-23), escala 0-100.
-- NULL si el negoci no té dades de Popular Times a Google Maps.
ALTER TABLE negocios_activos
    ADD COLUMN IF NOT EXISTS popular_times JSONB DEFAULT NULL;

COMMENT ON COLUMN negocios_activos.popular_times IS
    'Histograma Popular Times de Google Maps. Format: {dia: [24 valors 0-100]}. '
    'Scraped per pipelines/google_maps.py via GoogleMapsScraper._extraer_popular_times(). '
    'NULL si el negoci no te dades suficients a Google Maps.';

CREATE INDEX IF NOT EXISTS idx_negocios_popular_times
    ON negocios_activos USING GIN(popular_times)
    WHERE popular_times IS NOT NULL;

-- ── flujo_popular_times_score: nova columna a variables_zona (feature v4) ────
-- Puntuació 0-100 del pic de concurrència de la zona calculada a partir dels
-- Popular Times dels negocios actius. Calculada per pipelines/google_maps.py.
-- Pendent d'incorporar a FEATURE_NAMES de scoring/features.py (v4).
ALTER TABLE variables_zona
    ADD COLUMN IF NOT EXISTS flujo_popular_times_score FLOAT DEFAULT NULL;

COMMENT ON COLUMN variables_zona.flujo_popular_times_score IS
    'Score 0-100 del pic de concurrència de la zona (Popular Times Google Maps). '
    'Media del pico màxim de popular_times dels negocios actius de la zona. '
    'Feature v4 — pendent d incorporar a XGBoost FEATURE_NAMES (scoring/features.py).';
