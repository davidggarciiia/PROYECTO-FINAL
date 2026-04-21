-- =============================================================================
-- 001_schema_inicial.sql — Esquema completo de GeoRetail
--
-- Cómo ejecutar:
--   psql $DATABASE_URL < db/migraciones/001_schema_inicial.sql
--
-- Requisitos previos:
--   - PostgreSQL 15+
--   - Extensiones (plugins) PostGIS, pgvector y pg_trgm instaladas en el servidor
-- =============================================================================


-- -----------------------------------------------------------------------------
-- Extensiones (plugins que añaden funcionalidad extra a PostgreSQL)
-- -----------------------------------------------------------------------------

-- PostGIS → permite guardar coordenadas, polígonos y hacer cálculos de distancia
CREATE EXTENSION IF NOT EXISTS postgis;

-- pgvector → permite guardar vectores numéricos (los que genera la IA para comparar textos)
CREATE EXTENSION IF NOT EXISTS vector;

-- pg_trgm → permite búsqueda de texto aproximada (encuentra "cafeteria" aunque escribas "cafetería")
CREATE EXTENSION IF NOT EXISTS pg_trgm;


-- =============================================================================
-- BLOQUE 1 — Geografía de Barcelona
-- =============================================================================

-- Distritos de Barcelona (Eixample, Gràcia, Sants-Montjuïc...)
CREATE TABLE IF NOT EXISTS distritos (
    id          SERIAL PRIMARY KEY,
    codigo      VARCHAR(4)  UNIQUE NOT NULL,  -- código oficial: "01" … "10"
    nombre      TEXT        NOT NULL,
    geometria   GEOMETRY(MULTIPOLYGON, 4326)  -- polígono del distrito en coordenadas GPS
);

-- Barrios dentro de cada distrito (el Gótico, la Barceloneta, el Poblenou...)
CREATE TABLE IF NOT EXISTS barrios (
    id          SERIAL PRIMARY KEY,
    codigo      VARCHAR(6)  UNIQUE NOT NULL,  -- código oficial del Ajuntament
    nombre      TEXT        NOT NULL,
    distrito_id INT         REFERENCES distritos(id),
    geometria   GEOMETRY(MULTIPOLYGON, 4326)
);

-- Zonas comerciales — subzonas dentro de cada barrio
-- Son las unidades mínimas que GeoRetail analiza y muestra en el mapa
CREATE TABLE IF NOT EXISTS zonas (
    id          VARCHAR(20) PRIMARY KEY,      -- ejemplo: "bcn_eixample_01"
    nombre      TEXT        NOT NULL,
    barrio_id   INT         REFERENCES barrios(id),
    geometria   GEOMETRY(POLYGON, 4326),      -- polígono de la zona
    area_m2     FLOAT,
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Índices espaciales (aceleran las búsquedas por coordenadas y polígonos)
CREATE INDEX IF NOT EXISTS idx_zonas_geometria   ON zonas   USING GIST(geometria);
CREATE INDEX IF NOT EXISTS idx_barrios_geometria ON barrios USING GIST(geometria);


-- =============================================================================
-- BLOQUE 2 — Locales físicos disponibles para alquilar
-- =============================================================================

CREATE TABLE IF NOT EXISTS locales (
    id                   VARCHAR(30) PRIMARY KEY,
    zona_id              VARCHAR(20) REFERENCES zonas(id),
    direccion            TEXT,
    lat                  FLOAT NOT NULL,
    lng                  FLOAT NOT NULL,
    geometria            GEOMETRY(POINT, 4326),
    m2                   FLOAT,
    planta               VARCHAR(10),   -- "PB" (planta baja), "1", "-1" (sótano)
    escaparate_ml        FLOAT,         -- metros lineales de escaparate (la parte de cristal a la calle)
    referencia_catastral VARCHAR(30),   -- código que identifica el inmueble en el catastro
    alquiler_mensual     FLOAT,         -- €/mes
    disponible           BOOLEAN DEFAULT TRUE,
    fuente               VARCHAR(30),   -- de dónde viene el dato: 'idealista', 'cens_bcn', 'manual'
    created_at           TIMESTAMPTZ DEFAULT NOW(),
    updated_at           TIMESTAMPTZ DEFAULT NOW()
);

-- Histórico de precios de alquiler de cada local
CREATE TABLE IF NOT EXISTS locales_historico_precios (
    id          SERIAL PRIMARY KEY,
    local_id    VARCHAR(30) REFERENCES locales(id),
    fecha       DATE  NOT NULL,
    precio      FLOAT NOT NULL,
    fuente      VARCHAR(30)
);

CREATE INDEX IF NOT EXISTS idx_locales_geometria ON locales USING GIST(geometria);
CREATE INDEX IF NOT EXISTS idx_locales_zona      ON locales(zona_id);


-- =============================================================================
-- BLOQUE 3 — Variables del entorno por zona
-- (una fila por zona por fecha — se actualiza semanalmente)
-- =============================================================================

CREATE TABLE IF NOT EXISTS variables_zona (
    id                       SERIAL PRIMARY KEY,
    zona_id                  VARCHAR(20) REFERENCES zonas(id),
    fecha                    DATE NOT NULL,

    -- Flujo peatonal (personas que pasan por la zona)
    -- Fuente: aforadors (sensores de conteo) del Open Data BCN
    flujo_peatonal_manana    FLOAT,   -- personas/hora entre 8h y 14h
    flujo_peatonal_tarde     FLOAT,   -- personas/hora entre 14h y 20h
    flujo_peatonal_noche     FLOAT,   -- personas/hora entre 20h y 23h
    flujo_peatonal_total     FLOAT,   -- suma de las tres franjas

    -- Demografía (población, edad, renta...)
    -- Fuente: Padró Municipal + INE + Idescat
    poblacion                INT,
    edad_media               FLOAT,
    renta_media_hogar        FLOAT,   -- €/año por hogar
    pct_extranjeros          FLOAT,   -- porcentaje de población extranjera (0-1)
    densidad_hab_km2         FLOAT,   -- habitantes por km²
    nivel_estudios_alto_pct  FLOAT,   -- % de población con estudios universitarios

    -- Actividad comercial
    -- Fuente: Cens Locals BCN (Open Data BCN)
    num_negocios_activos     INT,
    pct_locales_vacios       FLOAT,   -- porcentaje de locales vacíos (0-1)
    tasa_rotacion_anual      FLOAT,   -- cuántos negocios cierran al año / total (0-1)
    ratio_locales_comerciales FLOAT,  -- locales comerciales / total locales (0-1)
                                      -- distingue calles comerciales de residenciales

    -- Turismo
    score_turismo            FLOAT,   -- puntuación 0-100 basada en hoteles, atracciones, Airbnb

    -- Seguridad
    -- Fuente: incidències de la Guàrdia Urbana (Open Data BCN)
    incidencias_por_1000hab  FLOAT,

    -- Entorno físico
    nivel_ruido_db           FLOAT,   -- decibelios medios (mapa acústico BCN)
    m2_zonas_verdes_cercanas FLOAT,   -- metros cuadrados de parques en radio 500m

    -- Equipamientos (escuelas, centros de salud, bibliotecas...)
    score_equipamientos      FLOAT,   -- puntuación 0-100

    fuente                   VARCHAR(50),
    UNIQUE (zona_id, fecha)
);

-- Precios de alquiler por zona (€/m² por mes)
-- Fuente: Idealista API + Open Data BCN
CREATE TABLE IF NOT EXISTS precios_alquiler_zona (
    id           SERIAL PRIMARY KEY,
    zona_id      VARCHAR(20) REFERENCES zonas(id),
    fecha        DATE        NOT NULL,
    precio_m2    FLOAT       NOT NULL,
    precio_min   FLOAT,
    precio_max   FLOAT,
    num_muestras INT,                       -- cuántos anuncios se usaron para calcular la media
    fuente       VARCHAR(30),               -- 'idealista', 'open_data_bcn', 'catastro'
    UNIQUE (zona_id, fecha, fuente)
);

CREATE INDEX IF NOT EXISTS idx_variables_zona_fecha ON variables_zona(zona_id, fecha DESC);


-- =============================================================================
-- BLOQUE 4 — Scoring (puntuación) con Machine Learning
-- =============================================================================

-- Sectores de negocio disponibles en GeoRetail
CREATE TABLE IF NOT EXISTS sectores (
    id               SERIAL PRIMARY KEY,
    codigo           VARCHAR(30) UNIQUE NOT NULL,  -- 'restauracion', 'moda', 'tatuajes'...
    nombre           TEXT NOT NULL,
    descripcion      TEXT,
    -- Pesos para la puntuación manual cuando no hay modelo XGBoost entrenado
    -- (cuánto importa cada factor para este tipo de negocio)
    peso_flujo       FLOAT DEFAULT 0.25,  -- importancia del flujo peatonal
    peso_demo        FLOAT DEFAULT 0.20,  -- importancia de la demografía
    peso_competencia FLOAT DEFAULT 0.15,  -- importancia de la competencia
    peso_precio      FLOAT DEFAULT 0.15,  -- importancia del precio del alquiler
    peso_transporte  FLOAT DEFAULT 0.10,
    peso_seguridad   FLOAT DEFAULT 0.05,
    peso_turismo     FLOAT DEFAULT 0.05,
    peso_entorno     FLOAT DEFAULT 0.05
);

-- Puntuaciones calculadas por zona y sector
CREATE TABLE IF NOT EXISTS scores_zona (
    id                         SERIAL PRIMARY KEY,
    zona_id                    VARCHAR(20) REFERENCES zonas(id),
    sector_id                  INT REFERENCES sectores(id),
    modelo_version             VARCHAR(50),   -- 'xgboost_v1', 'manual_v1'...
    fecha_calculo              TIMESTAMPTZ DEFAULT NOW(),

    score_global               FLOAT NOT NULL,  -- puntuación global 0-100

    -- Puntuaciones por cada dimensión (0-100)
    score_flujo_peatonal       FLOAT,
    score_demografia           FLOAT,
    score_competencia          FLOAT,
    score_precio_alquiler      FLOAT,
    score_transporte           FLOAT,
    score_seguridad            FLOAT,
    score_turismo              FLOAT,
    score_entorno_comercial    FLOAT,

    -- Resultados del modelo XGBoost (si está entrenado)
    probabilidad_supervivencia FLOAT,   -- probabilidad de seguir abierto a 3 años (0-1)
    shap_values                JSONB,   -- cuánto aporta cada variable al score final

    UNIQUE (zona_id, sector_id, modelo_version)
);

-- Negocios históricos de Barcelona — datos de entrenamiento del modelo XGBoost
-- Fuente: Registre Mercantil (datos.gob.es) + Cens Locals BCN
CREATE TABLE IF NOT EXISTS negocios_historico (
    id                SERIAL PRIMARY KEY,
    nombre            TEXT,
    sector_codigo     VARCHAR(30),
    zona_id           VARCHAR(20) REFERENCES zonas(id),
    fecha_apertura    DATE,
    fecha_cierre      DATE,          -- NULL si sigue abierto
    activo_3_anos     BOOLEAN,       -- TRUE si aguantó ≥ 3 años (la etiqueta del modelo)
    features_snapshot JSONB,         -- cómo era el entorno cuando abrió (para no usar datos futuros)
    fuente            VARCHAR(30)    DEFAULT 'registre_mercantil'
);

-- Versiones del modelo XGBoost entrenado
CREATE TABLE IF NOT EXISTS modelos_versiones (
    id              SERIAL PRIMARY KEY,
    version         VARCHAR(50) UNIQUE NOT NULL,  -- 'restauracion_20260310_1430'
    sector          VARCHAR(30),                  -- NULL = modelo global (todos los sectores)
    params          JSONB,                        -- hiperparámetros usados en el entrenamiento
    metricas        JSONB,                        -- AUC (puntuación de calidad), PR-AUC, etc.
    importancia_features JSONB,                   -- qué variables son más importantes
    ruta_disco      TEXT,                         -- dónde está guardado el modelo (.json)
    activo          BOOLEAN DEFAULT FALSE,        -- solo uno activo a la vez por sector
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scores_zona_sector ON scores_zona(zona_id, sector_id);


-- =============================================================================
-- BLOQUE 5 — Competencia (negocios activos en Barcelona)
-- =============================================================================

-- Todos los negocios activos en Barcelona
-- Fuente: Google Places → Foursquare → Yelp → OSM → Cens Locals BCN
CREATE TABLE IF NOT EXISTS negocios_activos (
    id            VARCHAR(40) PRIMARY KEY,  -- ID de Google Places o Foursquare
    nombre        TEXT        NOT NULL,
    sector_codigo VARCHAR(30),
    lat           FLOAT       NOT NULL,
    lng           FLOAT       NOT NULL,
    geometria     GEOMETRY(POINT, 4326),
    zona_id       VARCHAR(20) REFERENCES zonas(id),
    rating        FLOAT,                   -- puntuación media (0-5)
    num_resenas   INT,
    precio_nivel  INT,                     -- 1 (barato) a 4 (caro) según Google
    horario       JSONB,                   -- horarios de apertura por día
    fecha_apertura DATE,
    activo        BOOLEAN     DEFAULT TRUE,
    fuente        VARCHAR(20),
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- Resumen de competencia por zona, sector y radio de búsqueda
CREATE TABLE IF NOT EXISTS competencia_por_local (
    id               SERIAL PRIMARY KEY,
    zona_id          VARCHAR(20) REFERENCES zonas(id),
    sector_codigo    VARCHAR(30),
    radio_m          INT,         -- radio en metros: 300, 500 o 1000
    num_competidores INT,
    rating_medio     FLOAT,
    score_saturacion FLOAT,       -- 0-100 (100 = zona muy saturada de ese tipo de negocio)
    fecha_calculo    DATE,
    UNIQUE (zona_id, sector_codigo, radio_m, fecha_calculo)
);

CREATE INDEX IF NOT EXISTS idx_negocios_geometria     ON negocios_activos USING GIST(geometria);
CREATE INDEX IF NOT EXISTS idx_negocios_zona          ON negocios_activos(zona_id);
CREATE INDEX IF NOT EXISTS idx_negocios_sector        ON negocios_activos(sector_codigo);
CREATE INDEX IF NOT EXISTS idx_negocios_sector_zona   ON negocios_activos(sector_codigo, zona_id);
CREATE INDEX IF NOT EXISTS idx_negocios_nombre_trgm   ON negocios_activos USING GIN(nombre gin_trgm_ops);


-- =============================================================================
-- BLOQUE 6 — Reseñas y análisis de texto con IA
-- =============================================================================

-- Reseñas de Google, Foursquare y Yelp
CREATE TABLE IF NOT EXISTS resenas (
    id           VARCHAR(50) PRIMARY KEY,
    negocio_id   VARCHAR(40) REFERENCES negocios_activos(id),
    zona_id      VARCHAR(20) REFERENCES zonas(id),
    texto        TEXT        NOT NULL,
    rating       FLOAT,
    fecha        DATE,
    idioma       VARCHAR(5),
    fuente       VARCHAR(20),  -- 'google', 'foursquare', 'yelp'

    -- Resultados del análisis con IA
    embedding    VECTOR(768),  -- vector numérico que representa el significado del texto
    categoria    VARCHAR(30),  -- 'servicio', 'producto', 'precio', 'ubicacion', 'ambiente'
    sentimiento  VARCHAR(10),  -- 'positivo', 'negativo', 'neutro'
    score_categoria FLOAT,     -- confianza del clasificador (0-1)
    procesada    BOOLEAN       DEFAULT FALSE
);

-- Alertas generadas por el análisis de las reseñas de cada zona
CREATE TABLE IF NOT EXISTS alertas_zona (
    id        SERIAL PRIMARY KEY,
    zona_id   VARCHAR(20) REFERENCES zonas(id),
    tipo      VARCHAR(10)  NOT NULL,  -- 'positiva', 'negativa', 'neutral'
    texto     TEXT         NOT NULL,
    fuente    VARCHAR(20),            -- 'resenas', 'noticias', 'datos'
    confianza FLOAT,
    fecha     DATE,
    activa    BOOLEAN DEFAULT TRUE
);

-- Perfil semántico de cada zona (resumen vectorial de todas sus reseñas)
CREATE TABLE IF NOT EXISTS perfiles_zona_embedding (
    zona_id    VARCHAR(20) PRIMARY KEY REFERENCES zonas(id),
    embedding  VECTOR(768),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_resenas_zona      ON resenas(zona_id);
-- Índice para búsqueda por similitud semántica (encuentra reseñas parecidas)
CREATE INDEX IF NOT EXISTS idx_resenas_embedding ON resenas
    USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);


-- =============================================================================
-- BLOQUE 7 — Transporte público
-- =============================================================================

-- Líneas de metro, bus, tram, FGC y Rodalies
CREATE TABLE IF NOT EXISTS lineas_transporte (
    id        VARCHAR(20) PRIMARY KEY,
    codigo    VARCHAR(10) NOT NULL,
    nombre    TEXT,
    tipo      VARCHAR(10),   -- 'metro', 'bus', 'tram', 'fgc', 'rodalies'
    color_hex VARCHAR(7),    -- color de la línea (ejemplo: "#DC241F" para L1 de metro)
    fuente    VARCHAR(20)    -- 'tmb', 'gtfs_atm', 'gtfs_renfe'
);

-- Paradas de cada línea
CREATE TABLE IF NOT EXISTS paradas_transporte (
    id             VARCHAR(30) PRIMARY KEY,
    nombre         TEXT    NOT NULL,
    lat            FLOAT   NOT NULL,
    lng            FLOAT   NOT NULL,
    geometria      GEOMETRY(POINT, 4326),
    zona_id        VARCHAR(20) REFERENCES zonas(id),
    accesible_pmr  BOOLEAN,  -- accesible para personas con movilidad reducida
    fuente         VARCHAR(20)
);

-- Relación entre paradas y líneas (una parada puede tener varias líneas)
CREATE TABLE IF NOT EXISTS paradas_lineas (
    parada_id VARCHAR(30) REFERENCES paradas_transporte(id),
    linea_id  VARCHAR(20) REFERENCES lineas_transporte(id),
    orden     INT,         -- orden de la parada dentro de la línea
    PRIMARY KEY (parada_id, linea_id)
);

-- Frecuencia de paso de cada línea según franja horaria
CREATE TABLE IF NOT EXISTS frecuencias_transporte (
    id              SERIAL PRIMARY KEY,
    linea_id        VARCHAR(20) REFERENCES lineas_transporte(id),
    dia_tipo        VARCHAR(10),  -- 'laborable', 'sabado', 'festivo'
    franja          VARCHAR(10),  -- 'manana', 'tarde', 'noche'
    frecuencia_min  FLOAT         -- minutos entre dos vehículos consecutivos
);

CREATE INDEX IF NOT EXISTS idx_paradas_geometria ON paradas_transporte USING GIST(geometria);


-- =============================================================================
-- BLOQUE 8 — Sesiones de usuario
-- =============================================================================

-- Sesión de usuario (se crea cuando empieza una búsqueda)
CREATE TABLE IF NOT EXISTS sesiones (
    id         VARCHAR(36) PRIMARY KEY,   -- UUID (identificador único aleatorio)
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    ip_hash    VARCHAR(64),               -- IP hasheada (codificada) para analytics anónimos
    perfil     JSONB       DEFAULT '{}'   -- perfil del negocio que va construyendo el cuestionario
);

-- Registro de búsquedas realizadas (para analytics)
CREATE TABLE IF NOT EXISTS busquedas (
    id                   SERIAL PRIMARY KEY,
    session_id           VARCHAR(36) REFERENCES sesiones(id),
    descripcion_original TEXT,
    filtros              JSONB,
    perfil_negocio       JSONB,
    num_resultados       INT,
    created_at           TIMESTAMPTZ DEFAULT NOW()
);

-- Conversación del cuestionario adaptativo
CREATE TABLE IF NOT EXISTS mensajes_cuestionario (
    id         SERIAL PRIMARY KEY,
    session_id VARCHAR(36) REFERENCES sesiones(id),
    rol        VARCHAR(10) NOT NULL,   -- 'user' (el usuario) o 'assistant' (la IA)
    texto      TEXT        NOT NULL,
    orden      INT         NOT NULL,   -- número de mensaje en la conversación
    created_at TIMESTAMPTZ DEFAULT NOW()
);


-- =============================================================================
-- BLOQUE 9 — Módulo financiero
-- =============================================================================

-- Datos de referencia por sector para la calculadora financiera
-- Fuente: INE (Instituto Nacional de Estadística) por código CNAE
CREATE TABLE IF NOT EXISTS benchmarks_sector (
    id                             SERIAL PRIMARY KEY,
    sector_codigo                  VARCHAR(30) UNIQUE NOT NULL,
    ticket_medio_min               FLOAT,   -- precio mínimo típico por cliente (€)
    ticket_medio_max               FLOAT,   -- precio máximo típico por cliente (€)
    margen_bruto_tipico            FLOAT,   -- porcentaje de beneficio bruto sobre ventas (0-1)
    coste_personal_pct             FLOAT,   -- porcentaje del coste de personal sobre ventas
    coste_suministros_pct          FLOAT,
    alquiler_sobre_ventas_max      FLOAT DEFAULT 0.15,  -- regla del 15%: el alquiler no debería superar el 15% de las ventas
    reforma_m2_min                 FLOAT,   -- coste mínimo de reforma por m²
    reforma_m2_max                 FLOAT,
    -- Parámetros operativos (usados en financiero/estimador.py)
    conversion_rate_min            FLOAT DEFAULT 0.005,  -- tasa mínima de conversión peatón→cliente
    conversion_rate_max            FLOAT DEFAULT 0.020,  -- tasa máxima de conversión peatón→cliente
    horas_apertura_dia             FLOAT DEFAULT 9.0,
    dias_apertura_mes_tipico       INT   DEFAULT 26,
    empleados_por_m2               FLOAT DEFAULT 25.0,   -- m² por empleado
    salario_base_mensual_convenio  FLOAT DEFAULT 1620.0, -- € bruto/mes según convenio catalán 2024
    coste_suministros_por_m2       FLOAT DEFAULT 5.0,    -- €/m²/mes (luz, agua, gas)
    coste_gestoria_mensual         FLOAT DEFAULT 185.0,
    seguro_rc_mensual              FLOAT DEFAULT 100.0,
    equipamiento_base_min          FLOAT DEFAULT 5000.0,
    equipamiento_base_max          FLOAT DEFAULT 20000.0,
    coste_licencias_apertura       FLOAT DEFAULT 1500.0,
    otros_iniciales_fijos          FLOAT DEFAULT 2000.0,
    is_appointment_based           BOOLEAN DEFAULT FALSE, -- TRUE para estética/tatuajes (modelo cita previa)
    clientes_dia_por_puesto_min    FLOAT DEFAULT 2.0,    -- solo si is_appointment_based=TRUE
    clientes_dia_por_puesto_max    FLOAT DEFAULT 5.0,
    fuente                         VARCHAR(50),
    updated_at                     TIMESTAMPTZ DEFAULT NOW()
);

-- Parámetros financieros pre-calculados por zona (caché semanal)
CREATE TABLE IF NOT EXISTS parametros_financieros_zona (
    id                        SERIAL PRIMARY KEY,
    zona_id                   VARCHAR(20) REFERENCES zonas(id),
    sector_id                 INT REFERENCES sectores(id),  -- FK a sectores (no sector_codigo)
    fecha_calculo             DATE NOT NULL,

    -- Ingresos estimados
    ticket_medio              FLOAT,
    ticket_fuente             TEXT,
    ticket_confianza          VARCHAR(10),  -- 'alta', 'media', 'baja'
    ticket_rango_min          FLOAT,
    ticket_rango_max          FLOAT,

    clientes_dia_conservador  FLOAT,
    clientes_dia_optimista    FLOAT,
    clientes_fuente           TEXT,
    clientes_confianza        VARCHAR(10),
    dias_apertura_mes         FLOAT,

    -- Costes estimados
    alquiler_mensual          FLOAT,
    alquiler_fuente           TEXT,
    alquiler_confianza        VARCHAR(10),
    salarios_mensual          FLOAT,
    salarios_fuente           TEXT,
    num_empleados             INT,
    otros_fijos_mensual       FLOAT,
    coste_mercancia_pct       FLOAT,

    -- Inversión inicial estimada
    reforma_estimada          FLOAT,
    reforma_rango_min         FLOAT,
    reforma_rango_max         FLOAT,
    equipamiento_estimado     FLOAT,
    equipamiento_rango_min    FLOAT,
    equipamiento_rango_max    FLOAT,
    deposito_fianza           FLOAT,
    otros_iniciales           FLOAT,

    UNIQUE (zona_id, sector_id, fecha_calculo)
);

-- Vista (consulta guardada) que devuelve siempre los parámetros más recientes
-- db/financiero.py la usa con: JOIN sectores s ON s.id=pfz.sector_id
CREATE OR REPLACE VIEW v_parametros_financieros_actuales AS
SELECT DISTINCT ON (zona_id, sector_id) *
FROM parametros_financieros_zona
ORDER BY zona_id, sector_id, fecha_calculo DESC;

-- Análisis financieros guardados (para incluirlos en el PDF)
CREATE TABLE IF NOT EXISTS analisis_financieros (
    id                            SERIAL PRIMARY KEY,
    session_id                    VARCHAR(36) REFERENCES sesiones(id),
    zona_id                       VARCHAR(20) REFERENCES zonas(id),
    params                        JSONB NOT NULL,
    inversion_total               FLOAT,
    ingresos_anuales_conservador  FLOAT,
    ingresos_anuales_optimista    FLOAT,
    ebitda_conservador            FLOAT,  -- beneficio antes de impuestos, intereses y amortizaciones
    ebitda_optimista              FLOAT,
    roi_3a_conservador            FLOAT,  -- retorno de la inversión a 3 años (ROI)
    roi_3a_optimista              FLOAT,
    payback_meses_conservador     INT,    -- meses hasta recuperar la inversión
    payback_meses_optimista       INT,
    proyeccion_json               JSONB,  -- array de 36 meses con datos mes a mes
    created_at                    TIMESTAMPTZ DEFAULT NOW(),
    -- ON CONFLICT (session_id, zona_id) en db/financiero.py::guardar_analisis_financiero
    UNIQUE (session_id, zona_id)
);


-- =============================================================================
-- BLOQUE 10 — Requisitos legales por sector
-- (de momento se usan datos estáticos en api/legal.py — estas tablas son para el futuro)
-- =============================================================================

CREATE TABLE IF NOT EXISTS requisitos_legales_sector (
    id                   SERIAL PRIMARY KEY,
    sector_codigo        VARCHAR(30) UNIQUE NOT NULL,
    nombre               TEXT NOT NULL,
    viabilidad           VARCHAR(20),  -- 'viable', 'restringido', 'inviable'
    alerta               TEXT,
    modelo_legal         VARCHAR(30),  -- NULL o 'club_privado_fumadores'
    licencias            JSONB,        -- array de licencias necesarias
    requisitos_local     JSONB,        -- array de requisitos del local físico
    requisitos_operativos JSONB,
    updated_at           TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS restricciones_geograficas_sector (
    id                    SERIAL PRIMARY KEY,
    sector_codigo         VARCHAR(30),
    tipo                  VARCHAR(20),    -- 'densidad', 'distancia', 'prohibicion'
    descripcion           TEXT NOT NULL,
    radio_metros          INT,
    obligatorio_verificar BOOLEAN DEFAULT TRUE,
    -- Polígono de la zona donde aplica la restricción (si es geográficamente específica)
    geometria_restriccion GEOMETRY(MULTIPOLYGON, 4326)
);


-- =============================================================================
-- BLOQUE 11 — Control de pipelines (procesos de carga de datos) y caché
-- =============================================================================

-- Registro de cada ejecución de un pipeline
CREATE TABLE IF NOT EXISTS pipeline_ejecuciones (
    id            SERIAL PRIMARY KEY,
    pipeline      VARCHAR(50) NOT NULL,  -- 'aforaments', 'resenas', 'precios', 'scores'...
    fecha_inicio  TIMESTAMPTZ DEFAULT NOW(),
    fecha_fin     TIMESTAMPTZ,
    registros     INT,                   -- cuántos registros procesó
    estado        VARCHAR(10) DEFAULT 'running',  -- 'ok', 'error', 'running'
    mensaje_error TEXT
);

-- Log de errores detallados de los pipelines
CREATE TABLE IF NOT EXISTS pipeline_errores (
    id        SERIAL PRIMARY KEY,
    pipeline  VARCHAR(50),
    fecha     TIMESTAMPTZ DEFAULT NOW(),
    error     TEXT,
    traceback TEXT   -- traza completa del error para depurar
);

-- Caché de geocodificación (convertir direcciones a coordenadas GPS)
-- Evita repetir llamadas a Google Maps que cuestan dinero
CREATE TABLE IF NOT EXISTS cache_geocoding (
    direccion_normalizada TEXT PRIMARY KEY,
    lat                   FLOAT NOT NULL,
    lng                   FLOAT NOT NULL,
    precision             VARCHAR(20),   -- 'rooftop' (exacta), 'street' (calle), 'zone' (zona)
    fuente                VARCHAR(20),
    created_at            TIMESTAMPTZ DEFAULT NOW()
);

-- Log de todas las llamadas a modelos de IA (para controlar el coste)
CREATE TABLE IF NOT EXISTS llm_logs (
    id            SERIAL PRIMARY KEY,
    session_id    VARCHAR(36),
    proveedor     VARCHAR(20),   -- 'deepseek', 'openai'
    modelo        VARCHAR(50),
    tokens_input  INT,           -- tokens (palabras aproximadas) enviados al modelo
    tokens_output INT,           -- tokens recibidos como respuesta
    coste_usd     FLOAT,         -- coste en dólares de esa llamada
    latencia_ms   INT,           -- milisegundos que tardó en responder
    endpoint      VARCHAR(50),   -- qué endpoint llamó al LLM: 'validacion', 'cuestionario'...
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- Exportaciones PDF generadas
CREATE TABLE IF NOT EXISTS exportaciones (
    id          VARCHAR(36) PRIMARY KEY,    -- UUID del fichero
    session_id  VARCHAR(36) REFERENCES sesiones(id),
    zona_ids    TEXT[],                     -- array con los IDs de las zonas incluidas
    estado      VARCHAR(10) DEFAULT 'generando',  -- 'generando', 'ok', 'error'
    ruta_disco  TEXT,                       -- ruta donde está guardado el PDF en el servidor
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    expires_at  TIMESTAMPTZ,                -- cuándo expira el enlace de descarga (+1h)
    descargado  BOOLEAN DEFAULT FALSE
);


-- =============================================================================
-- BLOQUE 12 — Datos iniciales necesarios para arrancar
-- =============================================================================

-- Sectores disponibles con sus pesos de scoring por defecto
INSERT INTO sectores (codigo, nombre, descripcion, peso_flujo, peso_demo, peso_competencia, peso_precio, peso_transporte, peso_seguridad, peso_turismo, peso_entorno)
VALUES
    ('restauracion',  'Restauración',             'Bar, cafetería, restaurante',                    0.28, 0.18, 0.18, 0.14, 0.09, 0.05, 0.06, 0.02),
    ('tatuajes',      'Estudio de tatuajes',       'Tatuajes, piercing, micropigmentación',          0.22, 0.20, 0.15, 0.15, 0.10, 0.08, 0.07, 0.03),
    ('moda',          'Moda y retail',             'Ropa, complementos, accesorios',                 0.30, 0.18, 0.15, 0.17, 0.08, 0.04, 0.05, 0.03),
    ('estetica',      'Estética y belleza',        'Peluquería, spa, clínica de belleza',            0.20, 0.22, 0.18, 0.16, 0.10, 0.06, 0.05, 0.03),
    ('shisha_lounge', 'Shisha Lounge',             'Club privado de fumadores / shisha bar',         0.18, 0.20, 0.15, 0.14, 0.12, 0.10, 0.08, 0.03)
ON CONFLICT (codigo) DO NOTHING;

-- Benchmarks financieros por sector (basados en datos del INE por código CNAE)
-- Columnas: sector_codigo, ticket_medio_min, ticket_medio_max, margen_bruto_tipico,
--   coste_personal_pct, coste_suministros_pct, reforma_m2_min, reforma_m2_max,
--   conversion_rate_min, conversion_rate_max, horas_apertura_dia, dias_apertura_mes_tipico,
--   empleados_por_m2, salario_base_mensual_convenio, coste_suministros_por_m2,
--   coste_gestoria_mensual, seguro_rc_mensual, equipamiento_base_min, equipamiento_base_max,
--   coste_licencias_apertura, otros_iniciales_fijos, is_appointment_based,
--   clientes_dia_por_puesto_min, clientes_dia_por_puesto_max, fuente
INSERT INTO benchmarks_sector (
    sector_codigo, ticket_medio_min, ticket_medio_max, margen_bruto_tipico,
    coste_personal_pct, coste_suministros_pct, reforma_m2_min, reforma_m2_max,
    conversion_rate_min, conversion_rate_max, horas_apertura_dia, dias_apertura_mes_tipico,
    empleados_por_m2, salario_base_mensual_convenio, coste_suministros_por_m2,
    coste_gestoria_mensual, seguro_rc_mensual, equipamiento_base_min, equipamiento_base_max,
    coste_licencias_apertura, otros_iniciales_fijos, is_appointment_based,
    clientes_dia_por_puesto_min, clientes_dia_por_puesto_max, fuente
)
VALUES
--                                                                    conv_min conv_max  h_dia dias_mes  emp_m2  salario  sumi_m2  gest   seg_rc  equip_min equip_max  lic     ini     cita?    cli_min cli_max
    ('restauracion',  11,  65, 0.68, 0.32, 0.06,  650, 1400,  0.008,  0.025,   10,     26,     20.0,  1620.0,  7.0,   185.0, 110.0,  8000.0, 25000.0, 2000.0, 2500.0, FALSE,  2.0,  5.0, 'INE CNAE 56'),
    ('tatuajes',      80, 450, 0.82, 0.28, 0.03,  400,  900,  0.000,  0.000,    9,     26,     12.0,  1620.0,  4.0,   185.0,  90.0,  5000.0, 15000.0, 1800.0, 2000.0, TRUE,   4.0,  7.0, 'INE CNAE 9602'),
    ('moda',          28, 420, 0.55, 0.22, 0.04,  350,  800,  0.010,  0.030,   10,     26,     30.0,  1620.0,  5.0,   185.0,  90.0,  6000.0, 18000.0, 1500.0, 2000.0, FALSE,  2.0,  5.0, 'INE CNAE 4771'),
    ('estetica',      30, 160, 0.72, 0.35, 0.05,  400,  900,  0.000,  0.000,    9,     26,     10.0,  1620.0,  5.0,   185.0, 100.0,  5000.0, 15000.0, 1800.0, 2000.0, TRUE,   3.0,  6.0, 'INE CNAE 9602'),
    ('shisha_lounge', 25,  90, 0.75, 0.25, 0.08,  500, 1200,  0.005,  0.015,    8,     26,     25.0,  1620.0,  8.0,   200.0, 120.0, 10000.0, 30000.0, 3500.0, 3000.0, FALSE,  2.0,  5.0, 'estimacion_mercado')
ON CONFLICT (sector_codigo) DO NOTHING;
