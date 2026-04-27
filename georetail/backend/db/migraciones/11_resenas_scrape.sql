-- Backfill de resenas scrapeadas desde gosom/google-maps-scraper.
-- Mantiene la trazabilidad por negocio y permite reanudar por estado.

ALTER TABLE resenas
    ADD COLUMN IF NOT EXISTS autor VARCHAR(200),
    ADD COLUMN IF NOT EXISTS url TEXT,
    ADD COLUMN IF NOT EXISTS metadata JSONB,
    ADD COLUMN IF NOT EXISTS scraped_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE resenas DROP CONSTRAINT IF EXISTS ck_resenas_fuente;
ALTER TABLE resenas
    ADD CONSTRAINT ck_resenas_fuente
    CHECK (fuente IS NULL OR fuente IN ('google', 'google_scrape', 'foursquare', 'yelp'));

CREATE INDEX IF NOT EXISTS idx_resenas_negocio_fecha
    ON resenas (negocio_id, fecha DESC)
    WHERE negocio_id IS NOT NULL;

ALTER TABLE negocios_activos
    ADD COLUMN IF NOT EXISTS resenas_scrape_estado VARCHAR(20) NOT NULL DEFAULT 'pendiente',
    ADD COLUMN IF NOT EXISTS resenas_scrape_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS resenas_scrape_error TEXT;

ALTER TABLE negocios_activos DROP CONSTRAINT IF EXISTS ck_negocios_resenas_scrape_estado;
ALTER TABLE negocios_activos
    ADD CONSTRAINT ck_negocios_resenas_scrape_estado
    CHECK (resenas_scrape_estado IN ('pendiente', 'ok', 'sin_resenas', 'sin_match', 'error'));

CREATE INDEX IF NOT EXISTS idx_negocios_resenas_scrape_estado
    ON negocios_activos (resenas_scrape_estado, resenas_scrape_at)
    WHERE es_activo = TRUE;
