-- ============================================================
-- Archivo  : 05_sectores_scoring.sql
-- Proyecto : GeoRetail
-- Propósito: Tablas de sectores, scoring ML y datos de entrenamiento.
--            Consolida legacy 001 (sectores, scores_zona, modelos_versiones,
--            negocios_historico), 012 (unique index activo por sector),
--            013 (audit columns + fn_set_updated_at), 022 (demografia_feature_names
--            en modelos_versiones), 027 (dim_calendario_bcn),
--            033 (tabla lookup subsectores — la columna en negocios_activos
--            y competencia_detalle_zona va en 06_competencia.sql),
--            036 (normalización pesos suma=1.0 por sector).
-- Orden    : Depende de 01_extensiones.sql y 02_geografia.sql (zonas).
-- ============================================================


-- ── Tabla: sectores ─────────────────────────────────────────

CREATE TABLE sectores (
    id                  SERIAL                      NOT NULL,
    codigo              VARCHAR(30)                 NOT NULL,
    nombre              VARCHAR(200)                NOT NULL,
    descripcion         VARCHAR(500),
    peso_flujo          DOUBLE PRECISION            NOT NULL DEFAULT 0.25,
    peso_demo           DOUBLE PRECISION            NOT NULL DEFAULT 0.20,
    peso_competencia    DOUBLE PRECISION            NOT NULL DEFAULT 0.15,
    peso_precio         DOUBLE PRECISION            NOT NULL DEFAULT 0.15,
    peso_transporte     DOUBLE PRECISION            NOT NULL DEFAULT 0.10,
    peso_seguridad      DOUBLE PRECISION            NOT NULL DEFAULT 0.05,
    peso_turismo        DOUBLE PRECISION            NOT NULL DEFAULT 0.05,
    peso_entorno        DOUBLE PRECISION            NOT NULL DEFAULT 0.05,
    created_at          TIMESTAMPTZ                 NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ                 NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_sectores
        PRIMARY KEY (id),
    CONSTRAINT uq_sectores_codigo
        UNIQUE (codigo),
    CONSTRAINT ck_sectores_peso_flujo
        CHECK (peso_flujo       BETWEEN 0 AND 1),
    CONSTRAINT ck_sectores_peso_demo
        CHECK (peso_demo        BETWEEN 0 AND 1),
    CONSTRAINT ck_sectores_peso_competencia
        CHECK (peso_competencia BETWEEN 0 AND 1),
    CONSTRAINT ck_sectores_peso_precio
        CHECK (peso_precio      BETWEEN 0 AND 1),
    CONSTRAINT ck_sectores_peso_transporte
        CHECK (peso_transporte  BETWEEN 0 AND 1),
    CONSTRAINT ck_sectores_peso_seguridad
        CHECK (peso_seguridad   BETWEEN 0 AND 1),
    CONSTRAINT ck_sectores_peso_turismo
        CHECK (peso_turismo     BETWEEN 0 AND 1),
    CONSTRAINT ck_sectores_peso_entorno
        CHECK (peso_entorno     BETWEEN 0 AND 1)
);

COMMENT ON TABLE sectores IS
    'Sectores de negocio disponibles en GeoRetail (restauracion, moda, tatuajes, '
    'estetica, shisha_lounge). Cada sector define los pesos de scoring manual que '
    'se aplican cuando no hay modelo XGBoost entrenado disponible. '
    'Los pesos deben sumar 1.0; se verifica en el bloque de seeds. '
    'Consolida legacy 001 + 013 + 036.';

COMMENT ON COLUMN sectores.codigo IS
    'Código único del sector, en snake_case: restauracion, moda, tatuajes, '
    'estetica, shisha_lounge. Referenciado como FK por múltiples tablas.';
COMMENT ON COLUMN sectores.nombre IS
    'Nombre legible del sector. Máximo 200 caracteres.';
COMMENT ON COLUMN sectores.descripcion IS
    'Descripción breve del tipo de negocio que representa el sector.';
COMMENT ON COLUMN sectores.peso_flujo IS
    'Importancia del flujo peatonal para este sector (0–1). Suma de los 8 pesos = 1.0.';
COMMENT ON COLUMN sectores.peso_demo IS
    'Importancia del perfil demográfico de la zona (0–1).';
COMMENT ON COLUMN sectores.peso_competencia IS
    'Importancia del análisis de competencia (0–1).';
COMMENT ON COLUMN sectores.peso_precio IS
    'Importancia del precio del alquiler comercial (0–1).';
COMMENT ON COLUMN sectores.peso_transporte IS
    'Importancia de la accesibilidad en transporte público (0–1).';
COMMENT ON COLUMN sectores.peso_seguridad IS
    'Importancia del nivel de seguridad de la zona (0–1).';
COMMENT ON COLUMN sectores.peso_turismo IS
    'Importancia de la intensidad turística de la zona (0–1).';
COMMENT ON COLUMN sectores.peso_entorno IS
    'Importancia del entorno comercial (vacíos, mercados, dinamismo) (0–1).';

DO $$
BEGIN
    EXECUTE
        'CREATE OR REPLACE TRIGGER tg_set_updated_at
         BEFORE UPDATE ON sectores
         FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at()';
END;
$$;


-- ── Tabla: subsectores (lookup) ─────────────────────────────
-- Tabla de referencia para los 25 subsectores definidos en scoring/taxonomia.py.
-- La columna subsector_codigo en negocios_activos y competencia_detalle_zona
-- se crea en 06_competencia.sql (legacy 033).

CREATE TABLE subsectores (
    id              SERIAL                      NOT NULL,
    codigo          VARCHAR(30)                 NOT NULL,
    sector_codigo   VARCHAR(30)                 NOT NULL,
    nombre          VARCHAR(200)                NOT NULL,
    descripcion     VARCHAR(500),
    created_at      TIMESTAMPTZ                 NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ                 NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_subsectores
        PRIMARY KEY (id),
    CONSTRAINT uq_subsectores_codigo
        UNIQUE (codigo),
    CONSTRAINT fk_subsectores_sector
        FOREIGN KEY (sector_codigo)
        REFERENCES sectores (codigo)
        ON UPDATE CASCADE
);

COMMENT ON TABLE subsectores IS
    'Tabla de lookup para los subsectores finos del sistema de taxonomía '
    '(scoring/taxonomia.py). Permite mapear códigos de subsector a su sector '
    'padre y consultar descripciones. Los 25 subsectores se insertan por el '
    'pipeline de carga inicial, no por esta migración.';

COMMENT ON COLUMN subsectores.codigo IS
    'Código único del subsector, en snake_case. '
    'Fuente canónica: scoring/taxonomia.py.';
COMMENT ON COLUMN subsectores.sector_codigo IS
    'FK al sector padre (sectores.codigo). ON UPDATE CASCADE.';

DO $$
BEGIN
    EXECUTE
        'CREATE OR REPLACE TRIGGER tg_set_updated_at
         BEFORE UPDATE ON subsectores
         FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at()';
END;
$$;


-- ── Tabla: modelos_versiones ─────────────────────────────────

CREATE TABLE modelos_versiones (
    id                          SERIAL                  NOT NULL,
    version                     VARCHAR(50)             NOT NULL,
    sector                      VARCHAR(30),
    params                      JSONB,
    metricas                    JSONB,
    importancia_features        JSONB,
    demografia_feature_names    JSONB,
    ruta_disco                  VARCHAR(500),
    es_activo                   BOOLEAN                 NOT NULL DEFAULT FALSE,
    created_at                  TIMESTAMPTZ             NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_modelos_versiones
        PRIMARY KEY (id),
    CONSTRAINT uq_modelos_versiones_version
        UNIQUE (version)
);

COMMENT ON TABLE modelos_versiones IS
    'Registro de versiones del modelo XGBoost entrenado. '
    'Solo un modelo puede estar activo por sector a la vez '
    '(garantizado por el índice único parcial uq_modelos_activo_sector). '
    'Consolida legacy 001 + 012 + 022. '
    'La columna "activo" se renombra a "es_activo".';

COMMENT ON COLUMN modelos_versiones.version IS
    'Identificador único de la versión del modelo. '
    'Formato: "<sector>_<YYYYMMDD>_<HHMM>", p. ej. "restauracion_20260310_1430".';
COMMENT ON COLUMN modelos_versiones.sector IS
    'Sector al que pertenece el modelo. NULL = modelo global (todos los sectores).';
COMMENT ON COLUMN modelos_versiones.params IS
    'Hiperparámetros usados en el entrenamiento (resultado de Optuna o _DEFAULT_PARAMS).';
COMMENT ON COLUMN modelos_versiones.metricas IS
    'Métricas de evaluación: AUC, PR-AUC, Brier Score, etc.';
COMMENT ON COLUMN modelos_versiones.importancia_features IS
    'Feature importance XGBoost: {feature_name: importance_value}.';
COMMENT ON COLUMN modelos_versiones.demografia_feature_names IS
    'Subset demográfico activo usado en esta versión del modelo. '
    'NULL = fallback al bloque demográfico por defecto de scoring/infra/governance.py. '
    'Añadida en legacy 022.';
COMMENT ON COLUMN modelos_versiones.ruta_disco IS
    'Ruta absoluta al archivo .json del modelo en disco (MODELS_DIR env var).';
COMMENT ON COLUMN modelos_versiones.es_activo IS
    'TRUE si este modelo es el activo para su sector. '
    'Renombrado desde "activo". Solo uno activo por sector: ver uq_modelos_activo_sector.';

-- Índice único parcial: solo un modelo activo por sector (legacy 012)
CREATE UNIQUE INDEX IF NOT EXISTS uq_modelos_activo_sector
    ON modelos_versiones (sector)
    WHERE es_activo = TRUE;

COMMENT ON INDEX uq_modelos_activo_sector IS
    'Garantiza que solo un modelo tenga es_activo=TRUE por sector. '
    'scorer.py depende de esto para la carga determinista del modelo. '
    'Consolida legacy 012 (adaptado al rename activo→es_activo).';

-- Índice GIN en params para búsquedas de hiperparámetros
CREATE INDEX IF NOT EXISTS idx_modelos_versiones_params_gin
    ON modelos_versiones USING GIN (params)
    WHERE params IS NOT NULL;


-- ── Tabla: scores_zona ───────────────────────────────────────

CREATE TABLE scores_zona (
    id                          SERIAL                  NOT NULL,
    zona_id                     VARCHAR(30)             NOT NULL,
    sector_id                   INT                     NOT NULL,
    modelo_version              VARCHAR(50)             NOT NULL,
    fecha_calculo               TIMESTAMPTZ             NOT NULL DEFAULT NOW(),
    score_global                NUMERIC(5,2)            NOT NULL,
    score_flujo_peatonal        NUMERIC(5,2),
    score_demografia            NUMERIC(5,2),
    score_competencia           NUMERIC(5,2),
    score_precio_alquiler       NUMERIC(5,2),
    score_transporte            NUMERIC(5,2),
    score_seguridad             NUMERIC(5,2),
    score_turismo               NUMERIC(5,2),
    score_entorno_comercial     NUMERIC(5,2),
    probabilidad_supervivencia  NUMERIC(4,3),
    shap_values                 JSONB,
    pipeline_run_id             INT,

    CONSTRAINT pk_scores_zona
        PRIMARY KEY (id),
    CONSTRAINT fk_scores_zona_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE CASCADE,
    CONSTRAINT fk_scores_zona_sector
        FOREIGN KEY (sector_id)
        REFERENCES sectores (id)
        ON DELETE CASCADE,
    CONSTRAINT uq_scores_zona_sector_modelo
        UNIQUE (zona_id, sector_id, modelo_version),
    CONSTRAINT ck_scores_score_global
        CHECK (score_global BETWEEN 0 AND 100),
    CONSTRAINT ck_scores_flujo_peatonal
        CHECK (score_flujo_peatonal       IS NULL OR score_flujo_peatonal       BETWEEN 0 AND 100),
    CONSTRAINT ck_scores_demografia
        CHECK (score_demografia           IS NULL OR score_demografia           BETWEEN 0 AND 100),
    CONSTRAINT ck_scores_competencia
        CHECK (score_competencia          IS NULL OR score_competencia          BETWEEN 0 AND 100),
    CONSTRAINT ck_scores_precio_alquiler
        CHECK (score_precio_alquiler      IS NULL OR score_precio_alquiler      BETWEEN 0 AND 100),
    CONSTRAINT ck_scores_transporte
        CHECK (score_transporte           IS NULL OR score_transporte           BETWEEN 0 AND 100),
    CONSTRAINT ck_scores_seguridad
        CHECK (score_seguridad            IS NULL OR score_seguridad            BETWEEN 0 AND 100),
    CONSTRAINT ck_scores_turismo
        CHECK (score_turismo              IS NULL OR score_turismo              BETWEEN 0 AND 100),
    CONSTRAINT ck_scores_entorno_comercial
        CHECK (score_entorno_comercial    IS NULL OR score_entorno_comercial    BETWEEN 0 AND 100),
    CONSTRAINT ck_scores_prob_supervivencia
        CHECK (probabilidad_supervivencia IS NULL OR probabilidad_supervivencia BETWEEN 0 AND 1)
);

COMMENT ON TABLE scores_zona IS
    'Puntuaciones de viabilidad calculadas por zona y sector. '
    'Cada fila representa el score de una zona para un sector en una versión de modelo. '
    'La constraint única (zona_id, sector_id, modelo_version) permite un historial '
    'por versión. Consolida legacy 001 + 011 + 015.';

COMMENT ON COLUMN scores_zona.zona_id IS
    'FK a zonas(id). Se elimina en cascada si la zona se borra.';
COMMENT ON COLUMN scores_zona.sector_id IS
    'FK a sectores(id). Se elimina en cascada si el sector se borra.';
COMMENT ON COLUMN scores_zona.modelo_version IS
    'Identificador de la versión del modelo que generó el score: '
    '"xgboost_v1", "restauracion_20260310_1430", "manual_v2", etc.';
COMMENT ON COLUMN scores_zona.score_global IS
    'Puntuación global de viabilidad (0–100). '
    'Calculado como media ponderada de las dimensiones según pesos del sector.';
COMMENT ON COLUMN scores_zona.probabilidad_supervivencia IS
    'Probabilidad (0–1) de que un negocio sobreviva 3 años en esa zona. '
    'Salida directa del modelo XGBoost. NULL si se usa scoring manual.';
COMMENT ON COLUMN scores_zona.shap_values IS
    'Valores SHAP por feature: {feature_name: shap_value}. '
    'Permite explicar qué variables más contribuyeron al score.';
COMMENT ON COLUMN scores_zona.pipeline_run_id IS
    'ID de pipeline_ejecuciones que generó este registro. '
    'Sin FK declarada para evitar dependencia circular; se valida en aplicación.';

-- Índices: scores_zona
CREATE INDEX IF NOT EXISTS idx_scores_zona_zona_sector
    ON scores_zona (zona_id, sector_id);

CREATE INDEX IF NOT EXISTS idx_scores_zona_sector_fecha
    ON scores_zona (zona_id, sector_id, fecha_calculo DESC);

CREATE INDEX IF NOT EXISTS idx_scores_zona_shap_gin
    ON scores_zona USING GIN (shap_values)
    WHERE shap_values IS NOT NULL;


-- ── Tabla: negocios_historico ────────────────────────────────

CREATE TABLE negocios_historico (
    id                  SERIAL                  NOT NULL,
    nombre              VARCHAR(300),
    sector_codigo       VARCHAR(30),
    zona_id             VARCHAR(30),
    fecha_apertura      DATE,
    fecha_cierre        DATE,
    sobrevivio_3a       BOOLEAN,
    features_snapshot   JSONB,
    fuente              VARCHAR(30)             NOT NULL DEFAULT 'registre_mercantil',
    created_at          TIMESTAMPTZ             NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ             NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_negocios_historico
        PRIMARY KEY (id),
    CONSTRAINT fk_negocios_historico_sector
        FOREIGN KEY (sector_codigo)
        REFERENCES sectores (codigo)
        ON UPDATE CASCADE
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT fk_negocios_historico_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE SET NULL
);

COMMENT ON TABLE negocios_historico IS
    'Negocios históricos de Barcelona usados como datos de entrenamiento del modelo XGBoost. '
    'Fuente: Registre Mercantil (datos.gob.es) + Cens Locals BCN. '
    'La columna sobrevivio_3a es la label del modelo (TRUE = negoció ≥ 3 años). '
    'Consolida legacy 001 + 013 + 014. '
    'Rename: activo_3_anos → sobrevivio_3a.';

COMMENT ON COLUMN negocios_historico.sobrevivio_3a IS
    'Label del modelo XGBoost: TRUE si el negocio estuvo abierto ≥ 3 años. '
    'Renombrado desde "activo_3_anos" (legacy 001).';
COMMENT ON COLUMN negocios_historico.features_snapshot IS
    'Snapshot de variables de entorno en el momento de apertura. '
    'Evita data leakage al entrenar con datos futuros.';
COMMENT ON COLUMN negocios_historico.fuente IS
    'Origen de los datos. Default "registre_mercantil". '
    'Otros valores: "cens_locals_bcn", "manual".';

-- Índices: negocios_historico
CREATE INDEX IF NOT EXISTS idx_negocios_historico_sector
    ON negocios_historico (sector_codigo)
    WHERE sector_codigo IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_negocios_historico_zona
    ON negocios_historico (zona_id)
    WHERE zona_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_negocios_historico_features_gin
    ON negocios_historico USING GIN (features_snapshot)
    WHERE features_snapshot IS NOT NULL;

DO $$
BEGIN
    EXECUTE
        'CREATE OR REPLACE TRIGGER tg_set_updated_at
         BEFORE UPDATE ON negocios_historico
         FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at()';
END;
$$;


-- ── Tabla: dim_calendario_bcn ────────────────────────────────
-- Calendario local de Barcelona para pipelines de temporalidad.
-- Réplica exacta de legacy 027.

CREATE TABLE dim_calendario_bcn (
    fecha                   DATE                    NOT NULL,
    anio                    INTEGER                 NOT NULL,
    mes                     INTEGER                 NOT NULL,
    dia                     INTEGER                 NOT NULL,
    is_weekend              BOOLEAN                 NOT NULL DEFAULT FALSE,
    is_saturday             BOOLEAN                 NOT NULL DEFAULT FALSE,
    is_sunday               BOOLEAN                 NOT NULL DEFAULT FALSE,
    is_public_holiday_es    BOOLEAN                 NOT NULL DEFAULT FALSE,
    is_public_holiday_ct    BOOLEAN                 NOT NULL DEFAULT FALSE,
    is_public_holiday_bcn   BOOLEAN                 NOT NULL DEFAULT FALSE,
    is_school_holiday       BOOLEAN                 NOT NULL DEFAULT FALSE,
    is_bridge_day           BOOLEAN                 NOT NULL DEFAULT FALSE,
    is_summer               BOOLEAN                 NOT NULL DEFAULT FALSE,
    is_christmas            BOOLEAN                 NOT NULL DEFAULT FALSE,
    is_rebajas_winter       BOOLEAN                 NOT NULL DEFAULT FALSE,
    is_rebajas_summer       BOOLEAN                 NOT NULL DEFAULT FALSE,
    season                  VARCHAR(16)             NOT NULL,

    CONSTRAINT pk_dim_calendario_bcn
        PRIMARY KEY (fecha)
);

COMMENT ON TABLE dim_calendario_bcn IS
    'Calendario local de Barcelona reutilizable por los pipelines de temporalidad. '
    'Incluye festivos nacionales (es), catalanes (ct) y locales de Barcelona (bcn), '
    'días de vacaciones escolares, puentes, estación y periodos de rebajas. '
    'Consolida legacy 027.';

COMMENT ON COLUMN dim_calendario_bcn.season IS
    'Estación del año: "primavera", "verano", "otono", "invierno".';
COMMENT ON COLUMN dim_calendario_bcn.is_bridge_day IS
    'TRUE si es un día puente (día laborable entre festivo y fin de semana).';
COMMENT ON COLUMN dim_calendario_bcn.is_school_holiday IS
    'TRUE si es un día de vacaciones escolares en Cataluña.';

CREATE INDEX IF NOT EXISTS idx_dim_calendario_bcn_anio_mes
    ON dim_calendario_bcn (anio, mes);


-- ── Seeds: sectores ──────────────────────────────────────────
-- Pesos finales normalizados a suma=1.0 de legacy 036.
-- Se verifica con RAISE EXCEPTION si algún sector queda fuera del rango ±0.01.

INSERT INTO sectores (
    codigo,
    nombre,
    descripcion,
    peso_flujo,
    peso_demo,
    peso_competencia,
    peso_precio,
    peso_transporte,
    peso_seguridad,
    peso_turismo,
    peso_entorno
)
VALUES
    (
        'restauracion',
        'Restauración',
        'Bar, cafetería, restaurante',
        0.23, 0.18, 0.18, 0.11, 0.09, 0.08, 0.03, 0.10
    ),
    (
        'tatuajes',
        'Estudio de tatuajes',
        'Tatuajes, piercing, micropigmentación',
        0.19, 0.21, 0.15, 0.15, 0.09, 0.10, 0.03, 0.08
    ),
    (
        'moda',
        'Moda y retail',
        'Ropa, complementos, accesorios',
        0.23, 0.18, 0.15, 0.15, 0.09, 0.07, 0.03, 0.10
    ),
    (
        'estetica',
        'Estética y belleza',
        'Peluquería, spa, clínica de belleza',
        0.16, 0.20, 0.18, 0.16, 0.09, 0.08, 0.03, 0.10
    ),
    (
        'shisha_lounge',
        'Shisha Lounge',
        'Club privado de fumadores / shisha bar',
        0.16, 0.20, 0.15, 0.14, 0.11, 0.12, 0.03, 0.09
    )
ON CONFLICT (codigo) DO NOTHING;

-- Verificación de integridad de pesos: RAISE EXCEPTION si suma ∉ [0.99, 1.01]
DO $$
DECLARE
    r    RECORD;
    suma NUMERIC;
BEGIN
    FOR r IN
        SELECT
            codigo,
            COALESCE(peso_flujo,       0)
          + COALESCE(peso_demo,        0)
          + COALESCE(peso_competencia, 0)
          + COALESCE(peso_precio,      0)
          + COALESCE(peso_transporte,  0)
          + COALESCE(peso_seguridad,   0)
          + COALESCE(peso_turismo,     0)
          + COALESCE(peso_entorno,     0) AS s
        FROM sectores
    LOOP
        suma := r.s;
        IF ABS(suma - 1.0) > 0.01 THEN
            RAISE EXCEPTION
                'Sector % tiene pesos que no suman 1.0 (suma=%). '
                'Corrija los valores antes de continuar.',
                r.codigo, suma;
        END IF;
    END LOOP;
END
$$;
