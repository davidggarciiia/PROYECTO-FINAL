-- ============================================================
-- Archivo  : 09_usuario_financiero.sql
-- Proyecto : GeoRetail
-- Propósito: Sesiones de usuario, cuestionario adaptativo y
--            módulo financiero completo.
--            Consolida legacy 001 (bloques 8, 9 y 10) +
--            legacy 014 (FK sector_codigo → sectores.codigo).
-- Tablas   : sesiones, busquedas, mensajes_cuestionario,
--            benchmarks_sector, parametros_financieros_zona,
--            analisis_financieros, requisitos_legales_sector,
--            restricciones_geograficas_sector, exportaciones.
-- Vista    : v_parametros_financieros_actuales.
-- Orden    : Depende de 01_extensiones.sql (fn_set_updated_at),
--            02_geografia.sql (zonas),
--            04_scoring.sql (sectores).
-- ============================================================

-- ── sesiones ─────────────────────────────────────────────────

CREATE TABLE sesiones (
    id          VARCHAR(36)                     NOT NULL,   -- UUID v4 generado en Python
    created_at  TIMESTAMPTZ                     NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ                     NOT NULL DEFAULT NOW(),
    ip_hash     VARCHAR(64),                                -- SHA-256 de la IP (analytics anónimo)
    perfil      JSONB                           NOT NULL DEFAULT '{}'::JSONB,

    CONSTRAINT pk_sesiones PRIMARY KEY (id)
);

COMMENT ON TABLE sesiones IS
    'Sesión de usuario sin autenticación. '
    'El id es un UUID v4 generado en el backend y almacenado en cookie HttpOnly. '
    'La columna ip_hash guarda el SHA-256 de la IP para analytics agregados sin exponer IPs reales. '
    'perfil acumula el contexto del negocio construido durante el cuestionario adaptativo.';

COMMENT ON COLUMN sesiones.ip_hash IS
    'SHA-256 (hex) de la dirección IP del cliente. '
    'Nunca almacena la IP en claro; cumple RGPD art. 25 (privacidad por diseño).';
COMMENT ON COLUMN sesiones.perfil IS
    'Perfil JSONB del negocio que el cuestionario va completando. '
    'Estructura libre; los campos canónicos se documentan en schemas/models.py.';

CREATE INDEX idx_sesiones_perfil_gin
    ON sesiones USING GIN (perfil);

DO $$
BEGIN
    EXECUTE
        'CREATE OR REPLACE TRIGGER tg_sesiones_updated_at
         BEFORE UPDATE ON sesiones
         FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at()';
END;
$$;

-- ── busquedas ────────────────────────────────────────────────

CREATE TABLE busquedas (
    id                  SERIAL                  NOT NULL,
    session_id          VARCHAR(36)             NOT NULL,
    descripcion_original TEXT,
    filtros             JSONB,
    perfil_negocio      JSONB,
    num_resultados      INT,
    created_at          TIMESTAMPTZ             NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_busquedas
        PRIMARY KEY (id),
    CONSTRAINT fk_busquedas_sesion
        FOREIGN KEY (session_id)
        REFERENCES sesiones (id)
        ON DELETE CASCADE,
    CONSTRAINT ck_busquedas_num_resultados
        CHECK (num_resultados IS NULL OR num_resultados >= 0)
);

COMMENT ON TABLE busquedas IS
    'Registro de cada búsqueda de zonas realizada por el usuario. '
    'Usado para analytics de uso y para reproducir búsquedas anteriores.';

COMMENT ON COLUMN busquedas.descripcion_original IS
    'Texto libre introducido por el usuario en el campo de búsqueda.';
COMMENT ON COLUMN busquedas.filtros IS
    'Filtros activos al momento de la búsqueda (distrito, precio máximo, etc.).';
COMMENT ON COLUMN busquedas.num_resultados IS
    'Número de zonas devueltas. NULL si la búsqueda derivó en cuestionario. Debe ser >= 0.';

CREATE INDEX idx_busquedas_session_id
    ON busquedas (session_id);

-- ── mensajes_cuestionario ────────────────────────────────────

CREATE TABLE mensajes_cuestionario (
    id          SERIAL                          NOT NULL,
    session_id  VARCHAR(36)                     NOT NULL,
    rol         VARCHAR(10)                     NOT NULL,
    texto       TEXT                            NOT NULL,
    orden       INT                             NOT NULL,
    created_at  TIMESTAMPTZ                     NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_mensajes_cuestionario
        PRIMARY KEY (id),
    CONSTRAINT fk_mensajes_sesion
        FOREIGN KEY (session_id)
        REFERENCES sesiones (id)
        ON DELETE CASCADE,
    CONSTRAINT ck_mensajes_rol
        CHECK (rol IN ('user', 'assistant')),
    CONSTRAINT ck_mensajes_orden
        CHECK (orden >= 0),
    CONSTRAINT uq_mensajes_session_orden
        UNIQUE (session_id, orden)
);

COMMENT ON TABLE mensajes_cuestionario IS
    'Historial de la conversación del cuestionario adaptativo. '
    'Un registro por turno; orden indica la posición en la conversación (0-based).';

COMMENT ON COLUMN mensajes_cuestionario.rol IS
    'Emisor del mensaje: "user" (usuario) o "assistant" (LLM). '
    'Constraint ck_mensajes_rol garantiza solo esos dos valores.';
COMMENT ON COLUMN mensajes_cuestionario.orden IS
    'Número de turno (0-based). Junto con session_id forma la clave de unicidad.';

CREATE INDEX idx_mensajes_session_orden
    ON mensajes_cuestionario (session_id, orden);

-- ── benchmarks_sector ────────────────────────────────────────

CREATE TABLE benchmarks_sector (
    id                              SERIAL          NOT NULL,
    sector_codigo                   VARCHAR(30)     NOT NULL,

    -- Parámetros de ticket y margen
    ticket_medio_min                NUMERIC(10,2),
    ticket_medio_max                NUMERIC(10,2),
    margen_bruto_tipico             NUMERIC(5,4),
    coste_personal_pct              NUMERIC(5,4),
    coste_suministros_pct           NUMERIC(5,4),
    alquiler_sobre_ventas_max       NUMERIC(5,4)    NOT NULL DEFAULT 0.15,

    -- Reforma e inversión inicial
    reforma_m2_min                  NUMERIC(10,2),
    reforma_m2_max                  NUMERIC(10,2),

    -- Parámetros operativos (usados en financiero/estimador.py)
    conversion_rate_min             NUMERIC(7,5)    NOT NULL DEFAULT 0.005,
    conversion_rate_max             NUMERIC(7,5)    NOT NULL DEFAULT 0.020,
    horas_apertura_dia              NUMERIC(4,1)    NOT NULL DEFAULT 9.0,
    dias_apertura_mes_tipico        INT             NOT NULL DEFAULT 26,
    empleados_por_m2                NUMERIC(6,1)    NOT NULL DEFAULT 25.0,
    salario_base_mensual_convenio   NUMERIC(10,2)   NOT NULL DEFAULT 1620.00,
    coste_suministros_por_m2        NUMERIC(8,2)    NOT NULL DEFAULT 5.00,
    coste_gestoria_mensual          NUMERIC(8,2)    NOT NULL DEFAULT 185.00,
    seguro_rc_mensual               NUMERIC(8,2)    NOT NULL DEFAULT 100.00,
    equipamiento_base_min           NUMERIC(12,2)   NOT NULL DEFAULT 5000.00,
    equipamiento_base_max           NUMERIC(12,2)   NOT NULL DEFAULT 20000.00,
    coste_licencias_apertura        NUMERIC(10,2)   NOT NULL DEFAULT 1500.00,
    otros_iniciales_fijos           NUMERIC(10,2)   NOT NULL DEFAULT 2000.00,

    -- Modelo cita previa (estética, tatuajes)
    is_appointment_based            BOOLEAN         NOT NULL DEFAULT FALSE,
    clientes_dia_por_puesto_min     NUMERIC(5,1)    NOT NULL DEFAULT 2.0,
    clientes_dia_por_puesto_max     NUMERIC(5,1)    NOT NULL DEFAULT 5.0,

    fuente                          VARCHAR(50),
    updated_at                      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_benchmarks_sector
        PRIMARY KEY (id),
    CONSTRAINT uq_benchmarks_sector_codigo
        UNIQUE (sector_codigo),
    CONSTRAINT fk_benchmarks_sectores
        FOREIGN KEY (sector_codigo)
        REFERENCES sectores (codigo)
        ON UPDATE CASCADE
        DEFERRABLE INITIALLY DEFERRED
);

COMMENT ON TABLE benchmarks_sector IS
    'Parámetros financieros de referencia por sector de negocio. '
    'Fuente: INE por código CNAE y estimaciones de mercado para sectores sin CNAE directo. '
    'La FK a sectores es DEFERRABLE para permitir inserciones en bloque dentro de una misma transacción. '
    'Consolida legacy 001 bloque 9 + FK de legacy 014.';

COMMENT ON COLUMN benchmarks_sector.sector_codigo IS
    'Código único del sector. FK a sectores(codigo) con propagación ON UPDATE CASCADE.';
COMMENT ON COLUMN benchmarks_sector.alquiler_sobre_ventas_max IS
    'Regla del 15 %: el alquiler no debería superar este porcentaje de las ventas brutas.';
COMMENT ON COLUMN benchmarks_sector.is_appointment_based IS
    'TRUE para modelos de cita previa (tatuajes, estética). '
    'Cuando TRUE, los ingresos se calculan con clientes_dia_por_puesto en lugar de flujo peatonal.';
COMMENT ON COLUMN benchmarks_sector.fuente IS
    'Código CNAE o fuente alternativa: "INE CNAE 56", "estimacion_mercado", etc.';

DO $$
BEGIN
    EXECUTE
        'CREATE OR REPLACE TRIGGER tg_benchmarks_updated_at
         BEFORE UPDATE ON benchmarks_sector
         FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at()';
END;
$$;

-- ── Seed: benchmarks_sector ───────────────────────────────────
-- Valores basados en INE CNAE 2024 y estimaciones de mercado.
-- ON CONFLICT garantiza idempotencia en re-ejecuciones.

INSERT INTO benchmarks_sector (
    sector_codigo,
    ticket_medio_min, ticket_medio_max,
    margen_bruto_tipico, coste_personal_pct, coste_suministros_pct,
    reforma_m2_min, reforma_m2_max,
    conversion_rate_min, conversion_rate_max,
    horas_apertura_dia, dias_apertura_mes_tipico,
    empleados_por_m2, salario_base_mensual_convenio,
    coste_suministros_por_m2, coste_gestoria_mensual,
    seguro_rc_mensual,
    equipamiento_base_min, equipamiento_base_max,
    coste_licencias_apertura, otros_iniciales_fijos,
    is_appointment_based,
    clientes_dia_por_puesto_min, clientes_dia_por_puesto_max,
    fuente
)
VALUES
    ('restauracion',   11,   65, 0.68, 0.32, 0.06,  650, 1400, 0.008, 0.025, 10, 26, 20.0, 1620.00,  7.00, 185.00, 110.00,  8000.00, 25000.00, 2000.00, 2500.00, FALSE, 2.0, 5.0, 'INE CNAE 56'),
    ('tatuajes',       80,  450, 0.82, 0.28, 0.03,  400,  900, 0.000, 0.000,  9, 26, 12.0, 1620.00,  4.00, 185.00,  90.00,  5000.00, 15000.00, 1800.00, 2000.00, TRUE,  4.0, 7.0, 'INE CNAE 9602'),
    ('moda',           28,  420, 0.55, 0.22, 0.04,  350,  800, 0.010, 0.030, 10, 26, 30.0, 1620.00,  5.00, 185.00,  90.00,  6000.00, 18000.00, 1500.00, 2000.00, FALSE, 2.0, 5.0, 'INE CNAE 4771'),
    ('estetica',       30,  160, 0.72, 0.35, 0.05,  400,  900, 0.000, 0.000,  9, 26, 10.0, 1620.00,  5.00, 185.00, 100.00,  5000.00, 15000.00, 1800.00, 2000.00, TRUE,  3.0, 6.0, 'INE CNAE 9602'),
    ('shisha_lounge',  25,   90, 0.75, 0.25, 0.08,  500, 1200, 0.005, 0.015,  8, 26, 25.0, 1620.00,  8.00, 200.00, 120.00, 10000.00, 30000.00, 3500.00, 3000.00, FALSE, 2.0, 5.0, 'estimacion_mercado')
ON CONFLICT (sector_codigo) DO NOTHING;

-- ── parametros_financieros_zona ──────────────────────────────

CREATE TABLE parametros_financieros_zona (
    id                          SERIAL          NOT NULL,
    zona_id                     VARCHAR(30)     NOT NULL,
    sector_id                   INT             NOT NULL,
    fecha_calculo               DATE            NOT NULL,

    -- Ingresos estimados
    ticket_medio                NUMERIC(10,2),
    ticket_fuente               TEXT,
    ticket_confianza            VARCHAR(10),
    ticket_rango_min            NUMERIC(10,2),
    ticket_rango_max            NUMERIC(10,2),

    clientes_dia_conservador    NUMERIC(10,2),
    clientes_dia_optimista      NUMERIC(10,2),
    clientes_fuente             TEXT,
    clientes_confianza          VARCHAR(10),
    dias_apertura_mes           NUMERIC(5,1),

    -- Costes estimados
    alquiler_mensual            NUMERIC(10,2),
    alquiler_fuente             TEXT,
    alquiler_confianza          VARCHAR(10),
    salarios_mensual            NUMERIC(12,2),
    salarios_fuente             TEXT,
    num_empleados               INT,
    otros_fijos_mensual         NUMERIC(10,2),
    coste_mercancia_pct         NUMERIC(5,4),

    -- Inversión inicial estimada
    reforma_estimada            NUMERIC(12,2),
    reforma_rango_min           NUMERIC(12,2),
    reforma_rango_max           NUMERIC(12,2),
    equipamiento_estimado       NUMERIC(12,2),
    equipamiento_rango_min      NUMERIC(12,2),
    equipamiento_rango_max      NUMERIC(12,2),
    deposito_fianza             NUMERIC(10,2),
    otros_iniciales             NUMERIC(10,2),

    CONSTRAINT pk_parametros_financieros_zona
        PRIMARY KEY (id),
    CONSTRAINT fk_params_financieros_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE CASCADE,
    CONSTRAINT fk_params_financieros_sector
        FOREIGN KEY (sector_id)
        REFERENCES sectores (id)
        ON DELETE CASCADE,
    CONSTRAINT ck_params_ticket_confianza
        CHECK (ticket_confianza IS NULL OR ticket_confianza IN ('alta', 'media', 'baja')),
    CONSTRAINT ck_params_clientes_confianza
        CHECK (clientes_confianza IS NULL OR clientes_confianza IN ('alta', 'media', 'baja')),
    CONSTRAINT ck_params_alquiler_confianza
        CHECK (alquiler_confianza IS NULL OR alquiler_confianza IN ('alta', 'media', 'baja')),
    CONSTRAINT uq_params_financieros
        UNIQUE (zona_id, sector_id, fecha_calculo)
);

COMMENT ON TABLE parametros_financieros_zona IS
    'Caché semanal de parámetros financieros pre-calculados por zona × sector. '
    'Generada por pipelines/parametros_financieros.py. '
    'La vista v_parametros_financieros_actuales devuelve siempre el registro más reciente.';

COMMENT ON COLUMN parametros_financieros_zona.zona_id IS
    'FK a zonas(id). ON DELETE CASCADE: si se elimina la zona, se eliminan sus parámetros.';
COMMENT ON COLUMN parametros_financieros_zona.sector_id IS
    'FK a sectores(id). ON DELETE CASCADE.';
COMMENT ON COLUMN parametros_financieros_zona.fecha_calculo IS
    'Fecha en que se calcularon estos parámetros. Junto con zona_id y sector_id forma la UNIQUE.';
COMMENT ON COLUMN parametros_financieros_zona.ticket_confianza IS
    'Nivel de confianza en la estimación del ticket: "alta", "media" o "baja".';
COMMENT ON COLUMN parametros_financieros_zona.alquiler_confianza IS
    'Nivel de confianza en la estimación del alquiler: "alta", "media" o "baja".';

CREATE INDEX idx_params_financieros_zona_sector_fecha
    ON parametros_financieros_zona (zona_id, sector_id, fecha_calculo DESC);

-- ── Vista: v_parametros_financieros_actuales ─────────────────
-- Usa DISTINCT ON para retornar un único registro por (zona_id, sector_id):
-- el más reciente según fecha_calculo. Sin GROUP BY ni subquery → muy eficiente
-- con el índice idx_params_financieros_zona_sector_fecha.

CREATE OR REPLACE VIEW v_parametros_financieros_actuales AS
SELECT DISTINCT ON (zona_id, sector_id)
    *
FROM parametros_financieros_zona
ORDER BY
    zona_id,
    sector_id,
    fecha_calculo DESC;

COMMENT ON VIEW v_parametros_financieros_actuales IS
    'Devuelve el registro más reciente de parametros_financieros_zona para cada '
    'combinación (zona_id, sector_id). Usado por db/financiero.py.';

-- ── analisis_financieros ─────────────────────────────────────

CREATE TABLE analisis_financieros (
    id                              SERIAL          NOT NULL,
    session_id                      VARCHAR(36)     NOT NULL,
    zona_id                         VARCHAR(30)     NOT NULL,
    params                          JSONB           NOT NULL,
    inversion_total                 NUMERIC(12,2),
    ingresos_anuales_conservador    NUMERIC(14,2),
    ingresos_anuales_optimista      NUMERIC(14,2),
    ebitda_conservador              NUMERIC(14,2),
    ebitda_optimista                NUMERIC(14,2),
    roi_3a_conservador              NUMERIC(6,3),
    roi_3a_optimista                NUMERIC(6,3),
    payback_meses_conservador       SMALLINT,
    payback_meses_optimista         SMALLINT,
    proyeccion_json                 JSONB,
    created_at                      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_analisis_financieros
        PRIMARY KEY (id),
    CONSTRAINT fk_analisis_sesion
        FOREIGN KEY (session_id)
        REFERENCES sesiones (id)
        ON DELETE CASCADE,
    CONSTRAINT fk_analisis_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE CASCADE,
    CONSTRAINT uq_analisis_session_zona
        UNIQUE (session_id, zona_id)
);

COMMENT ON TABLE analisis_financieros IS
    'Resultado de la calculadora financiera para una sesión y zona concretas. '
    'Un único análisis por (session_id, zona_id); se sobreescribe con ON CONFLICT en código. '
    'proyeccion_json contiene el array de 36 meses mes a mes.';

COMMENT ON COLUMN analisis_financieros.params IS
    'Parámetros de entrada usados en el cálculo (ticket, m2, alquiler, overrides de slider).';
COMMENT ON COLUMN analisis_financieros.roi_3a_conservador IS
    'Retorno de la inversión a 3 años en el escenario conservador. '
    'NUMERIC(6,3): hasta 999.999 (es un ratio, no un porcentaje).';
COMMENT ON COLUMN analisis_financieros.proyeccion_json IS
    'Array JSONB de 36 elementos; cada elemento describe un mes de proyección '
    '(ingresos, costes, ebitda, cashflow acumulado).';

-- ── requisitos_legales_sector ─────────────────────────────────

CREATE TABLE requisitos_legales_sector (
    id                      SERIAL          NOT NULL,
    sector_codigo           VARCHAR(30)     NOT NULL,
    nombre                  VARCHAR(200)    NOT NULL,
    viabilidad              VARCHAR(20),
    alerta                  TEXT,
    modelo_legal            VARCHAR(30),
    licencias               JSONB,
    requisitos_local        JSONB,
    requisitos_operativos   JSONB,
    updated_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_requisitos_legales
        PRIMARY KEY (id),
    CONSTRAINT uq_requisitos_legales_sector
        UNIQUE (sector_codigo),
    CONSTRAINT fk_requisitos_sectores
        FOREIGN KEY (sector_codigo)
        REFERENCES sectores (codigo)
        ON UPDATE CASCADE
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT ck_requisitos_viabilidad
        CHECK (viabilidad IS NULL OR viabilidad IN ('viable', 'restringido', 'inviable'))
);

COMMENT ON TABLE requisitos_legales_sector IS
    'Requisitos legales y de viabilidad por sector de negocio en Barcelona. '
    'Actualmente los datos estáticos están en api/legal.py (_SECTORES). '
    'Cuando esta tabla esté poblada, api/legal.py leerá de aquí. '
    'FK deferrable para permitir inserciones en bloque con sectores en la misma transacción.';

COMMENT ON COLUMN requisitos_legales_sector.viabilidad IS
    'Resumen de viabilidad: "viable", "restringido" o "inviable". '
    'NULL si aún no evaluado.';
COMMENT ON COLUMN requisitos_legales_sector.modelo_legal IS
    'Régimen legal especial si aplica, p. ej. "club_privado_fumadores" para shisha_lounge.';
COMMENT ON COLUMN requisitos_legales_sector.licencias IS
    'Array JSONB de licencias requeridas (tipo, organismo, plazo orientativo).';
COMMENT ON COLUMN requisitos_legales_sector.requisitos_local IS
    'Array JSONB de requisitos físicos del local (superficie mínima, ventilación, etc.).';
COMMENT ON COLUMN requisitos_legales_sector.requisitos_operativos IS
    'Array JSONB de requisitos operativos (certificados, registros sanitarios, etc.).';

DO $$
BEGIN
    EXECUTE
        'CREATE OR REPLACE TRIGGER tg_requisitos_updated_at
         BEFORE UPDATE ON requisitos_legales_sector
         FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at()';
END;
$$;

-- ── restricciones_geograficas_sector ─────────────────────────

CREATE TABLE restricciones_geograficas_sector (
    id                      SERIAL          NOT NULL,
    sector_codigo           VARCHAR(30),
    tipo                    VARCHAR(20),
    descripcion             TEXT            NOT NULL,
    radio_metros            INT,
    obligatorio_verificar   BOOLEAN         NOT NULL DEFAULT TRUE,
    geometria_restriccion   GEOMETRY(MULTIPOLYGON, 4326),
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_restricciones_geograficas
        PRIMARY KEY (id),
    CONSTRAINT fk_restricciones_sectores
        FOREIGN KEY (sector_codigo)
        REFERENCES sectores (codigo)
        ON UPDATE CASCADE
        DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT ck_restricciones_tipo
        CHECK (tipo IS NULL OR tipo IN ('densidad', 'distancia', 'prohibicion')),
    CONSTRAINT ck_restricciones_radio
        CHECK (radio_metros IS NULL OR radio_metros >= 0)
);

COMMENT ON TABLE restricciones_geograficas_sector IS
    'Restricciones espaciales que aplican a ciertos sectores de negocio en Barcelona. '
    'Ejemplos: distancia mínima entre locales del mismo tipo, zonas de exclusión. '
    'FK deferrable para inserciones en bloque. '
    'geometria_restriccion es NULL si la restricción aplica a toda la ciudad.';

COMMENT ON COLUMN restricciones_geograficas_sector.tipo IS
    '"densidad": límite de establecimientos por km². '
    '"distancia": distancia mínima entre establecimientos del mismo sector. '
    '"prohibicion": zona geográfica donde el sector está prohibido.';
COMMENT ON COLUMN restricciones_geograficas_sector.radio_metros IS
    'Radio en metros de la restricción de distancia. Solo relevante si tipo = "distancia". '
    'Debe ser >= 0 si se especifica.';
COMMENT ON COLUMN restricciones_geograficas_sector.obligatorio_verificar IS
    'TRUE si el motor de análisis debe verificar esta restricción antes de emitir recomendación.';
COMMENT ON COLUMN restricciones_geograficas_sector.geometria_restriccion IS
    'Polígono (MULTIPOLYGON, SRID 4326) donde aplica la restricción. '
    'NULL implica que aplica a todo el ámbito de análisis.';

CREATE INDEX idx_restricciones_geometria
    ON restricciones_geograficas_sector USING GIST (geometria_restriccion)
    WHERE geometria_restriccion IS NOT NULL;

-- ── exportaciones ─────────────────────────────────────────────

CREATE TABLE exportaciones (
    id              VARCHAR(36)     NOT NULL,   -- UUID v4 del fichero PDF
    session_id      VARCHAR(36)     NOT NULL,
    zona_ids        TEXT[]          NOT NULL,
    estado          VARCHAR(15)     NOT NULL DEFAULT 'generando',
    ruta_disco      VARCHAR(500),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    expires_at      TIMESTAMPTZ,
    fue_descargado  BOOLEAN         NOT NULL DEFAULT FALSE,

    CONSTRAINT pk_exportaciones
        PRIMARY KEY (id),
    CONSTRAINT fk_exportaciones_sesion
        FOREIGN KEY (session_id)
        REFERENCES sesiones (id)
        ON DELETE CASCADE,
    CONSTRAINT ck_exportaciones_estado
        CHECK (estado IN ('generando', 'ok', 'error'))
);

COMMENT ON TABLE exportaciones IS
    'Registro de exportaciones PDF solicitadas por el usuario. '
    'El enlace de descarga expira 1 hora después de la generación (expires_at). '
    'La columna fue_descargado reemplaza a la antigua "descargado" (legacy 001).';

COMMENT ON COLUMN exportaciones.id IS
    'UUID v4 del fichero; usado como token en la URL de descarga.';
COMMENT ON COLUMN exportaciones.zona_ids IS
    'Array con los id de las zonas incluidas en el PDF.';
COMMENT ON COLUMN exportaciones.estado IS
    '"generando": el worker Celery aún procesa. '
    '"ok": PDF listo para descarga. '
    '"error": la generación falló.';
COMMENT ON COLUMN exportaciones.ruta_disco IS
    'Ruta absoluta del PDF en el servidor (bajo EXPORTS_DIR). '
    'NULL mientras el estado es "generando".';
COMMENT ON COLUMN exportaciones.expires_at IS
    'Momento en que el enlace de descarga deja de ser válido. '
    'Típicamente created_at + 1 hora.';
COMMENT ON COLUMN exportaciones.fue_descargado IS
    'TRUE en cuanto el usuario ha descargado el archivo al menos una vez. '
    'Renombrado desde "descargado" (legacy 001).';

CREATE INDEX idx_exportaciones_session_id
    ON exportaciones (session_id);

CREATE INDEX idx_exportaciones_estado_generando
    ON exportaciones (estado)
    WHERE estado = 'generando';
