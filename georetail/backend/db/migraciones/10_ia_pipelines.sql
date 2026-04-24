-- ============================================================
-- Archivo  : 10_ia_pipelines.sql
-- Proyecto : GeoRetail
-- Propósito: Análisis NLP de reseñas, alertas zonales,
--            perfiles semánticos de zona, trazabilidad de
--            pipelines de datos y log de llamadas a LLMs.
--            Consolida legacy 001 (bloques 6 y 11) +
--            legacy 011 (índice parcial alertas_zona activa).
-- Tablas   : resenas, alertas_zona, perfiles_zona_embedding,
--            pipeline_ejecuciones, pipeline_errores, llm_logs.
-- Orden    : Depende de 01_extensiones.sql (fn_set_updated_at,
--            extensión vector), 02_geografia.sql (zonas),
--            05_negocios.sql (negocios_activos).
-- ============================================================

-- ── resenas ───────────────────────────────────────────────────

CREATE TABLE resenas (
    id                      VARCHAR(50)     NOT NULL,   -- ID externo del portal de origen
    negocio_id              VARCHAR(40),
    zona_id                 VARCHAR(30),
    texto                   TEXT            NOT NULL,
    rating                  NUMERIC(3,2),
    fecha                   DATE,
    idioma                  VARCHAR(5),
    fuente                  VARCHAR(20),

    -- Resultados del análisis NLP (sentence-transformers 768 dims)
    embedding               VECTOR(768),
    categoria               VARCHAR(30),
    sentimiento             VARCHAR(10),
    confianza_categoria     NUMERIC(4,3),               -- renombrado desde score_categoria (legacy 001)
    procesada               BOOLEAN         NOT NULL DEFAULT FALSE,

    created_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_resenas
        PRIMARY KEY (id),
    CONSTRAINT fk_resenas_negocio
        FOREIGN KEY (negocio_id)
        REFERENCES negocios_activos (id)
        ON DELETE CASCADE,
    CONSTRAINT fk_resenas_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE SET NULL,
    CONSTRAINT ck_resenas_rating
        CHECK (rating IS NULL OR (rating >= 0 AND rating <= 5)),
    CONSTRAINT ck_resenas_fuente
        CHECK (fuente IS NULL OR fuente IN ('google', 'foursquare', 'yelp')),
    CONSTRAINT ck_resenas_sentimiento
        CHECK (sentimiento IS NULL OR sentimiento IN ('positivo', 'negativo', 'neutro')),
    CONSTRAINT ck_resenas_confianza_categoria
        CHECK (confianza_categoria IS NULL OR (confianza_categoria >= 0 AND confianza_categoria <= 1))
);

COMMENT ON TABLE resenas IS
    'Reseñas de Google, Foursquare y Yelp enriquecidas con análisis NLP. '
    'El pipeline nlp/clasificador.py rellena embedding, categoria, sentimiento y confianza_categoria. '
    'La columna confianza_categoria reemplaza a score_categoria (legacy 001). '
    'Consolida legacy 001 bloque 6.';

COMMENT ON COLUMN resenas.id IS
    'Identificador único del portal de origen. '
    'Formato: "<fuente>_<id_externo>", p. ej. "google_ChIJd8BlQ2BZwokRjMKtTjMezyr".';
COMMENT ON COLUMN resenas.negocio_id IS
    'FK a negocios_activos(id). ON DELETE CASCADE: si se borra el negocio, se borran sus reseñas.';
COMMENT ON COLUMN resenas.zona_id IS
    'FK a zonas(id). ON DELETE SET NULL: si la zona desaparece, la reseña se conserva sin zona.';
COMMENT ON COLUMN resenas.rating IS
    'Puntuación del autor (0.00 – 5.00). NUMERIC(3,2) permite un decimal.';
COMMENT ON COLUMN resenas.idioma IS
    'Código BCP-47 del idioma detectado: "es", "ca", "en", etc. NULL si no detectado.';
COMMENT ON COLUMN resenas.embedding IS
    'Vector VECTOR(768) generado por paraphrase-multilingual-mpnet-base-v2. '
    'NULL hasta que el pipeline de NLP procesa la reseña.';
COMMENT ON COLUMN resenas.categoria IS
    'Categoría semántica asignada: "servicio", "producto", "precio", "ubicacion", "ambiente", "otro". '
    'NULL hasta que nlp/clasificador.py procese la reseña.';
COMMENT ON COLUMN resenas.sentimiento IS
    'Sentimiento detectado: "positivo", "negativo" o "neutro".';
COMMENT ON COLUMN resenas.confianza_categoria IS
    'Confianza del clasificador en la categoría asignada (0.000 – 1.000). '
    'Renombrado desde "score_categoria" en legacy 001.';
COMMENT ON COLUMN resenas.procesada IS
    'FALSE hasta que nlp/clasificador.py ha rellenado embedding, categoria y sentimiento.';

CREATE INDEX idx_resenas_zona_id
    ON resenas (zona_id)
    WHERE zona_id IS NOT NULL;

CREATE INDEX idx_resenas_negocio_id
    ON resenas (negocio_id)
    WHERE negocio_id IS NOT NULL;

CREATE INDEX idx_resenas_pendientes
    ON resenas (procesada)
    WHERE procesada = FALSE;

-- Índice IVFFLAT para búsqueda por similitud semántica (cosine distance).
-- lists=100: recomendado para tablas con ~1 M de vectores (sqrt(n)).
-- Requiere que la tabla tenga datos antes de ser útil; se puede recrear tras bulk-load.
CREATE INDEX idx_resenas_embedding_ivfflat
    ON resenas USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

-- ── alertas_zona ──────────────────────────────────────────────

CREATE TABLE alertas_zona (
    id          SERIAL          NOT NULL,
    zona_id     VARCHAR(30)     NOT NULL,
    tipo        VARCHAR(10)     NOT NULL,
    texto       TEXT            NOT NULL,
    fuente      VARCHAR(20),
    confianza   NUMERIC(4,3),
    fecha       DATE,
    esta_activa BOOLEAN         NOT NULL DEFAULT TRUE,   -- renombrado desde "activa" (legacy 001)
    created_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_alertas_zona
        PRIMARY KEY (id),
    CONSTRAINT fk_alertas_zona_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE CASCADE,
    CONSTRAINT ck_alertas_tipo
        CHECK (tipo IN ('positiva', 'negativa', 'neutral')),
    CONSTRAINT ck_alertas_confianza
        CHECK (confianza IS NULL OR (confianza >= 0 AND confianza <= 1))
);

COMMENT ON TABLE alertas_zona IS
    'Alertas generadas por el análisis NLP de reseñas y noticias de cada zona. '
    'La columna esta_activa reemplaza a "activa" (legacy 001). '
    'El índice parcial idx_alertas_zona_activa consolida el índice de legacy 011.';

COMMENT ON COLUMN alertas_zona.tipo IS
    'Valoración de la alerta: "positiva", "negativa" o "neutral".';
COMMENT ON COLUMN alertas_zona.fuente IS
    'Origen de la alerta: "resenas", "noticias", "datos", etc.';
COMMENT ON COLUMN alertas_zona.confianza IS
    'Confianza del modelo en la alerta (0.000 – 1.000). NULL si es alerta manual.';
COMMENT ON COLUMN alertas_zona.esta_activa IS
    'TRUE si la alerta sigue vigente. Renombrado desde "activa" (legacy 001).';

-- Índice parcial: solo alertas activas (patrón de consulta habitual en api/)
CREATE INDEX idx_alertas_zona_activa
    ON alertas_zona (zona_id)
    WHERE esta_activa = TRUE;

-- ── perfiles_zona_embedding ───────────────────────────────────

CREATE TABLE perfiles_zona_embedding (
    zona_id     VARCHAR(30)     NOT NULL,
    embedding   VECTOR(768)     NOT NULL,
    updated_at  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_perfiles_zona_embedding
        PRIMARY KEY (zona_id),
    CONSTRAINT fk_perfiles_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE CASCADE
);

COMMENT ON TABLE perfiles_zona_embedding IS
    'Perfil semántico de cada zona: vector promedio de todos sus embeddings de reseñas. '
    'Generado por nlp/embeddings.py::actualizar_perfil_zona(). '
    'Permite búsquedas semánticas de zonas similares (cosine similarity sobre VECTOR(768)).';

COMMENT ON COLUMN perfiles_zona_embedding.embedding IS
    'Vector VECTOR(768) centroide de las reseñas de la zona. '
    'Calculado como media ponderada (por confianza_categoria) de los embeddings individuales.';
COMMENT ON COLUMN perfiles_zona_embedding.updated_at IS
    'Última vez que se recalculó el perfil. Actualizado automáticamente por trigger.';

-- Índice IVFFLAT para similitud entre perfiles de zona.
CREATE INDEX idx_perfiles_zona_embedding_ivfflat
    ON perfiles_zona_embedding USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

DO $$
BEGIN
    EXECUTE
        'CREATE OR REPLACE TRIGGER tg_perfiles_zona_updated_at
         BEFORE UPDATE ON perfiles_zona_embedding
         FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at()';
END;
$$;

-- ── pipeline_ejecuciones ──────────────────────────────────────

CREATE TABLE pipeline_ejecuciones (
    id              SERIAL          NOT NULL,
    pipeline        VARCHAR(50)     NOT NULL,
    fecha_inicio    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    fecha_fin       TIMESTAMPTZ,
    registros       INT,
    estado          VARCHAR(10)     NOT NULL DEFAULT 'running',
    mensaje_error   TEXT,

    CONSTRAINT pk_pipeline_ejecuciones
        PRIMARY KEY (id),
    CONSTRAINT ck_pipeline_ejecuciones_estado
        CHECK (estado IN ('running', 'ok', 'error')),
    CONSTRAINT ck_pipeline_ejecuciones_registros
        CHECK (registros IS NULL OR registros >= 0)
);

COMMENT ON TABLE pipeline_ejecuciones IS
    'Registro de cada ejecución de un pipeline de carga de datos. '
    'Cada pipeline abre un registro con estado "running" al inicio y lo actualiza '
    'a "ok" o "error" al finalizar. Usado por scheduler.py y api/admin.py.';

COMMENT ON COLUMN pipeline_ejecuciones.pipeline IS
    'Nombre del pipeline: "aforaments", "resenas", "precios", "scores", etc.';
COMMENT ON COLUMN pipeline_ejecuciones.fecha_fin IS
    'NULL mientras el pipeline está en ejecución.';
COMMENT ON COLUMN pipeline_ejecuciones.registros IS
    'Número de registros procesados (insertados o actualizados). '
    'NULL si el pipeline no reporta esta métrica. Debe ser >= 0.';
COMMENT ON COLUMN pipeline_ejecuciones.estado IS
    '"running": en ejecución. "ok": finalizado con éxito. "error": finalizado con error.';
COMMENT ON COLUMN pipeline_ejecuciones.mensaje_error IS
    'Mensaje de error en caso de estado = "error". NULL si el pipeline terminó bien.';

CREATE INDEX idx_pipeline_ejecuciones_pipeline_fecha
    ON pipeline_ejecuciones (pipeline, fecha_inicio DESC);

-- ── pipeline_errores ──────────────────────────────────────────

CREATE TABLE pipeline_errores (
    id          SERIAL          NOT NULL,
    pipeline    VARCHAR(50)     NOT NULL,
    fecha       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    error       TEXT,
    traceback   TEXT,

    CONSTRAINT pk_pipeline_errores
        PRIMARY KEY (id)
);

COMMENT ON TABLE pipeline_errores IS
    'Log detallado de errores de los pipelines de datos. '
    'Complementa pipeline_ejecuciones: un pipeline puede registrar múltiples errores '
    'antes de marcar su ejecución como "error". '
    'La columna traceback almacena el stack trace completo para depuración.';

COMMENT ON COLUMN pipeline_errores.pipeline IS
    'Nombre del pipeline que generó el error.';
COMMENT ON COLUMN pipeline_errores.error IS
    'Mensaje de error resumido (primera línea de la excepción).';
COMMENT ON COLUMN pipeline_errores.traceback IS
    'Stack trace completo en formato texto. Puede ser NULL para errores de red o timeout.';

CREATE INDEX idx_pipeline_errores_pipeline_fecha
    ON pipeline_errores (pipeline, fecha DESC);

-- ── llm_logs ─────────────────────────────────────────────────

CREATE TABLE llm_logs (
    id              SERIAL          NOT NULL,
    session_id      VARCHAR(36),                    -- sin FK: sesión puede haberse purgado
    proveedor       VARCHAR(20),
    modelo          VARCHAR(50),
    tokens_input    INT,
    tokens_output   INT,
    coste_usd       NUMERIC(10,6),
    latencia_ms     INT,
    endpoint_nombre VARCHAR(50),                    -- renombrado desde "endpoint" (legacy 001)
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_llm_logs
        PRIMARY KEY (id),
    CONSTRAINT ck_llm_logs_proveedor
        CHECK (proveedor IS NULL OR proveedor IN ('deepseek', 'openai', 'anthropic')),
    CONSTRAINT ck_llm_logs_tokens_input
        CHECK (tokens_input IS NULL OR tokens_input >= 0),
    CONSTRAINT ck_llm_logs_tokens_output
        CHECK (tokens_output IS NULL OR tokens_output >= 0),
    CONSTRAINT ck_llm_logs_coste_usd
        CHECK (coste_usd IS NULL OR coste_usd >= 0),
    CONSTRAINT ck_llm_logs_latencia_ms
        CHECK (latencia_ms IS NULL OR latencia_ms >= 0)
);

COMMENT ON TABLE llm_logs IS
    'Log de todas las llamadas a modelos de lenguaje (LLM) para control de coste y latencia. '
    'session_id no tiene FK declarada porque las sesiones pueden purgarse periódicamente '
    'sin que se deban perder los registros de coste. '
    'La columna endpoint_nombre reemplaza a "endpoint" (legacy 001).';

COMMENT ON COLUMN llm_logs.session_id IS
    'UUID de la sesión que originó la llamada. '
    'Sin FK: referencia soft para evitar pérdida de logs al purgar sesiones antiguas.';
COMMENT ON COLUMN llm_logs.proveedor IS
    'Proveedor del LLM: "deepseek", "openai" o "anthropic".';
COMMENT ON COLUMN llm_logs.modelo IS
    'Identificador del modelo: "deepseek-chat", "gpt-4o-mini", "claude-3-5-haiku-20241022", etc.';
COMMENT ON COLUMN llm_logs.tokens_input IS
    'Tokens enviados al modelo (prompt). Debe ser >= 0 si se especifica.';
COMMENT ON COLUMN llm_logs.tokens_output IS
    'Tokens recibidos como respuesta (completion). Debe ser >= 0 si se especifica.';
COMMENT ON COLUMN llm_logs.coste_usd IS
    'Coste en dólares de la llamada según tarifa del proveedor. NUMERIC(10,6) para micropagos.';
COMMENT ON COLUMN llm_logs.latencia_ms IS
    'Tiempo de respuesta del proveedor en milisegundos. Debe ser >= 0 si se especifica.';
COMMENT ON COLUMN llm_logs.endpoint_nombre IS
    'Endpoint interno que realizó la llamada: "validacion", "cuestionario", "analisis_zona", etc. '
    'Renombrado desde "endpoint" (legacy 001, conflicto con keyword SQL en algunos dialectos).';

-- Índice parcial: consultas de coste por sesión (solo cuando la sesión existe)
CREATE INDEX idx_llm_logs_session_id
    ON llm_logs (session_id)
    WHERE session_id IS NOT NULL;

-- Índice para listados cronológicos y dashboards de coste
CREATE INDEX idx_llm_logs_created_at
    ON llm_logs (created_at DESC);
