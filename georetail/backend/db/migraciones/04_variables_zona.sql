-- ============================================================
-- Archivo  : 04_variables_zona.sql
-- Proyecto : GeoRetail
-- Propósito: Núcleo del refactor de variables por zona.
--            Sustituye la tabla fat original de legacy 001 por una
--            tabla coordinadora delgada + 5 tablas satélite 3NF +
--            vista unificada v_variables_zona.
--            Consolida las migraciones legacy:
--              001 (tabla fat — ELIMINADA),
--              017 (split inicial en satélites),
--              018 (pct_poblacio_25_44, delta_renta_3a, seguridad_barri_score),
--              019 (nada nuevo aquí),
--              020 (columnas de seguridad granular),
--              021 (mercados_municipales_1km),
--              022 (nada nuevo aquí),
--              023 (gini, p80_p20, tamano_hogar, hogares_con_menores, personas_solas),
--              024 (renta_media_uc, renta_mediana_uc),
--              027 (columnas de flujo temporal y estacional),
--              029 (score_turismo_airbnb, score_turismo_hut),
--              030 (nada nuevo aquí),
--              031 (dist_landmark_top3_m),
--              034 (num_lineas_transporte, num_paradas_transporte, score_transporte_calculado),
--              035 (vista v_variables_zona final — versión canónica).
-- Orden    : Depende de 01_extensiones.sql (fn_set_updated_at)
--            y de 02_geografia.sql (zonas).
-- ============================================================

-- ── Tabla coordinadora: variables_zona ──────────────────────

CREATE TABLE variables_zona (
    id                          SERIAL              NOT NULL,
    zona_id                     VARCHAR(30)         NOT NULL,
    fecha                       DATE                NOT NULL,
    fuente                      VARCHAR(50),
    num_lineas_transporte       INT,
    num_paradas_transporte      INT,
    score_transporte_calculado  DOUBLE PRECISION,
    pipeline_run_id             INT,
    created_at                  TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ         NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_variables_zona
        PRIMARY KEY (id),
    CONSTRAINT fk_variables_zona_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE CASCADE,
    CONSTRAINT uq_variables_zona_zona_fecha
        UNIQUE (zona_id, fecha)
);

COMMENT ON TABLE variables_zona IS
    'Tabla coordinadora delgada: un registro por zona × fecha. '
    'Los datos analíticos están en las 5 tablas satélite (vz_flujo, vz_turismo, '
    'vz_demografia, vz_comercial, vz_entorno). '
    'Contiene solo columnas de transporte porque agregado_zona.py las escribe '
    'directamente sin pasar por los satélites (legacy 034).';

COMMENT ON COLUMN variables_zona.zona_id IS
    'FK a zonas(id). VARCHAR(30) alineado con la definición de 02_geografia.sql.';
COMMENT ON COLUMN variables_zona.fecha IS
    'Fecha de referencia del snapshot de variables (normalmente primer día del mes).';
COMMENT ON COLUMN variables_zona.num_lineas_transporte IS
    'Número de líneas de transporte público con parada en la zona (TMB API). '
    'Legacy 034 — escrito por pipelines/transporte/transporte.py.';
COMMENT ON COLUMN variables_zona.num_paradas_transporte IS
    'Número de paradas de transporte público en la zona.';
COMMENT ON COLUMN variables_zona.score_transporte_calculado IS
    'Score de accesibilidad en transporte calculado por scoring/dimensiones/transporte.py. '
    'Rango 0–100.';
COMMENT ON COLUMN variables_zona.pipeline_run_id IS
    'ID de la ejecución del pipeline (tabla pipeline_ejecuciones). '
    'Sin FK declarada para evitar dependencia cruzada; la integridad se valida en código.';

-- Índices: variables_zona
CREATE INDEX IF NOT EXISTS idx_variables_zona_zona_fecha
    ON variables_zona (zona_id, fecha DESC);

CREATE INDEX IF NOT EXISTS idx_variables_zona_num_lineas
    ON variables_zona (num_lineas_transporte)
    WHERE num_lineas_transporte IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_variables_zona_run_id
    ON variables_zona (pipeline_run_id)
    WHERE pipeline_run_id IS NOT NULL;

-- ── Satélite: vz_flujo ───────────────────────────────────────

CREATE TABLE vz_flujo (
    zona_id                     VARCHAR(30)         NOT NULL,
    fecha                       DATE                NOT NULL,
    flujo_peatonal_manana       DOUBLE PRECISION,
    flujo_peatonal_tarde        DOUBLE PRECISION,
    flujo_peatonal_noche        DOUBLE PRECISION,
    flujo_peatonal_total        DOUBLE PRECISION,
    flujo_popular_times_score   DOUBLE PRECISION,
    vcity_flujo_peatonal        DOUBLE PRECISION,
    vcity_tourist_rate          DOUBLE PRECISION,
    vcity_shopping_rate         DOUBLE PRECISION,
    vcity_resident_rate         DOUBLE PRECISION,
    weekend_lift                DOUBLE PRECISION,
    sunday_lift                 DOUBLE PRECISION,
    weekday_midday_share        DOUBLE PRECISION,
    weekend_evening_share       DOUBLE PRECISION,
    late_night_share            DOUBLE PRECISION,
    holiday_proxy_score         DOUBLE PRECISION,
    temporal_confianza          DOUBLE PRECISION,
    seasonality_summer_lift     DOUBLE PRECISION,
    seasonality_christmas_lift  DOUBLE PRECISION,
    seasonality_rebajas_lift    DOUBLE PRECISION,
    seasonality_volatility      DOUBLE PRECISION,
    seasonality_peak_concentration DOUBLE PRECISION,
    fuente                      VARCHAR(50),
    created_at                  TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ         NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_vz_flujo
        PRIMARY KEY (zona_id, fecha),
    CONSTRAINT fk_vz_flujo_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE CASCADE
);

COMMENT ON TABLE vz_flujo IS
    'Satélite de flujo peatonal y temporal por zona × fecha. '
    'Consolida legacy 017 (base) + 027 (columnas temporales y estacionales). '
    'El pipeline peatonal/vcity.py y peatonal/vianants.py escriben aquí.';

COMMENT ON COLUMN vz_flujo.flujo_peatonal_manana IS
    'Flujo peatonal en franja de mañana (8-14h). Unidad: personas/hora estimadas.';
COMMENT ON COLUMN vz_flujo.flujo_peatonal_tarde IS
    'Flujo peatonal en franja de tarde (14-21h). Unidad: personas/hora estimadas.';
COMMENT ON COLUMN vz_flujo.flujo_peatonal_noche IS
    'Flujo peatonal en franja de noche (21-24h + 0-8h). Unidad: personas/hora estimadas.';
COMMENT ON COLUMN vz_flujo.flujo_peatonal_total IS
    'Flujo peatonal diario total. Unidad: personas/día estimadas.';
COMMENT ON COLUMN vz_flujo.vcity_tourist_rate IS
    'Fracción de turistas sobre el total de visitas según VCity BSC. Rango 0–1.';
COMMENT ON COLUMN vz_flujo.vcity_shopping_rate IS
    'Fracción de visitas con motivación de compras según VCity BSC. Rango 0–1.';
COMMENT ON COLUMN vz_flujo.vcity_resident_rate IS
    'Fracción de residentes sobre el total de visitas según VCity BSC. Rango 0–1.';
COMMENT ON COLUMN vz_flujo.weekend_lift IS
    'Ratio flujo fin de semana / flujo día laborable. >1 indica zona de ocio.';
COMMENT ON COLUMN vz_flujo.sunday_lift IS
    'Ratio flujo domingo / flujo lunes–viernes. Útil para negocios dominicales.';
COMMENT ON COLUMN vz_flujo.holiday_proxy_score IS
    'Score de actividad en festivos nacionales (0–1). Estimado desde Google Popular Times.';
COMMENT ON COLUMN vz_flujo.temporal_confianza IS
    'Confianza del modelo temporal (0–1): 1 = datos suficientes, 0 = imputado.';
COMMENT ON COLUMN vz_flujo.seasonality_summer_lift IS
    'Lift de flujo en verano (jul–ago) respecto a la media anual.';
COMMENT ON COLUMN vz_flujo.seasonality_christmas_lift IS
    'Lift de flujo en Navidad (dic) respecto a la media anual.';
COMMENT ON COLUMN vz_flujo.seasonality_rebajas_lift IS
    'Lift de flujo en rebajas (ene + jul primera semana) respecto a la media anual.';
COMMENT ON COLUMN vz_flujo.seasonality_volatility IS
    'Desviación estándar de los lifts mensuales. Alta = negocio muy estacional.';
COMMENT ON COLUMN vz_flujo.seasonality_peak_concentration IS
    'Fracción del flujo anual concentrada en los 3 meses pico. Rango 0–1.';

CREATE INDEX IF NOT EXISTS idx_vz_flujo_zona_fecha
    ON vz_flujo (zona_id, fecha DESC);

-- ── Satélite: vz_turismo ─────────────────────────────────────

CREATE TABLE vz_turismo (
    zona_id                     VARCHAR(30)         NOT NULL,
    fecha                       DATE                NOT NULL,
    score_turismo               DOUBLE PRECISION,
    score_turismo_airbnb        DOUBLE PRECISION,
    score_turismo_hut           DOUBLE PRECISION,
    airbnb_density_500m         INT,
    airbnb_occupancy_est        DOUBLE PRECISION,
    booking_hoteles_500m        INT,
    booking_rating_medio        NUMERIC(3,2),
    cruceros_visitantes_anuales INT,
    puntos_interes_culturales_500m INT,
    mercados_municipales_500m   INT,
    eventos_culturales_500m     INT,
    venues_musicales_500m       INT,
    dist_landmark_top3_m        DOUBLE PRECISION,
    fuente                      VARCHAR(50),
    created_at                  TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ         NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_vz_turismo
        PRIMARY KEY (zona_id, fecha),
    CONSTRAINT fk_vz_turismo_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE CASCADE,
    CONSTRAINT ck_vz_turismo_rating
        CHECK (booking_rating_medio IS NULL
               OR (booking_rating_medio >= 0 AND booking_rating_medio <= 10))
);

COMMENT ON TABLE vz_turismo IS
    'Satélite de turismo y puntos de interés por zona × fecha. '
    'Consolida legacy 017 (base) + 029 (score_turismo_airbnb/hut) + 031 (dist_landmark_top3_m).';

COMMENT ON COLUMN vz_turismo.score_turismo IS
    'DEPRECATED. Score turístico compuesto (0–100) de legacy 017. '
    'Mantenida por compatibilidad con callers legacy. Usar score_turismo_airbnb / score_turismo_hut.';
COMMENT ON COLUMN vz_turismo.score_turismo_airbnb IS
    'Score turístico derivado de densidad y ocupación Airbnb. Rango 0–100. Legacy 029.';
COMMENT ON COLUMN vz_turismo.score_turismo_hut IS
    'Score turístico derivado de HUT (Habitatges d''Ús Turístic) Generalitat. Rango 0–100. Legacy 029.';
COMMENT ON COLUMN vz_turismo.airbnb_density_500m IS
    'Número de listados Airbnb activos en radio 500m del centroide de la zona.';
COMMENT ON COLUMN vz_turismo.airbnb_occupancy_est IS
    'Ocupación estimada de los listados Airbnb (fracción 0–1). Fuente: InsideAirbnb.';
COMMENT ON COLUMN vz_turismo.booking_rating_medio IS
    'Rating medio de los hoteles Booking.com en radio 500m. Escala 0–10.';
COMMENT ON COLUMN vz_turismo.dist_landmark_top3_m IS
    'Distancia media a los 3 landmarks turísticos más cercanos en metros. Legacy 031.';

CREATE INDEX IF NOT EXISTS idx_vz_turismo_zona_fecha
    ON vz_turismo (zona_id, fecha DESC);

-- ── Satélite: vz_demografia ──────────────────────────────────

CREATE TABLE vz_demografia (
    zona_id                     VARCHAR(30)         NOT NULL,
    fecha                       DATE                NOT NULL,
    poblacion                   INT,
    edad_media                  DOUBLE PRECISION,
    renta_media_hogar           DOUBLE PRECISION,
    renta_media_uc              DOUBLE PRECISION,
    renta_mediana_uc            DOUBLE PRECISION,
    pct_extranjeros             DOUBLE PRECISION,
    densidad_hab_km2            DOUBLE PRECISION,
    nivel_estudios_alto_pct     DOUBLE PRECISION,
    pct_poblacio_25_44          DOUBLE PRECISION,
    delta_renta_3a              DOUBLE PRECISION,
    gini                        DOUBLE PRECISION,
    p80_p20                     DOUBLE PRECISION,
    tamano_hogar                DOUBLE PRECISION,
    hogares_con_menores         DOUBLE PRECISION,
    personas_solas              DOUBLE PRECISION,
    fuente                      VARCHAR(50),
    created_at                  TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ         NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_vz_demografia
        PRIMARY KEY (zona_id, fecha),
    CONSTRAINT fk_vz_demografia_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE CASCADE,
    CONSTRAINT ck_vz_demografia_pct_extranjeros
        CHECK (pct_extranjeros IS NULL
               OR (pct_extranjeros >= 0 AND pct_extranjeros <= 1)),
    CONSTRAINT ck_vz_demografia_nivel_estudios
        CHECK (nivel_estudios_alto_pct IS NULL
               OR (nivel_estudios_alto_pct >= 0 AND nivel_estudios_alto_pct <= 1)),
    CONSTRAINT ck_vz_demografia_pct_25_44
        CHECK (pct_poblacio_25_44 IS NULL
               OR (pct_poblacio_25_44 >= 0 AND pct_poblacio_25_44 <= 1))
);

COMMENT ON TABLE vz_demografia IS
    'Satélite demográfico por zona × fecha. '
    'Consolida legacy 017 (base) + 018 (pct_poblacio_25_44, delta_renta_3a) '
    '+ 023 (gini, p80_p20, tamano_hogar, hogares_con_menores, personas_solas) '
    '+ 024 (renta_media_uc, renta_mediana_uc). '
    'Fuente primaria: Padró Municipal + Renda Familiar per Barris (Ajuntament de Barcelona).';

COMMENT ON COLUMN vz_demografia.renta_media_hogar IS
    'Renta media bruta del hogar en euros/año. Fuente: Renda Familiar per Barris BCN.';
COMMENT ON COLUMN vz_demografia.renta_media_uc IS
    'Renta media por unidad de consumo en euros/año. Ajuste por tamaño de hogar. Legacy 024.';
COMMENT ON COLUMN vz_demografia.renta_mediana_uc IS
    'Renta mediana por unidad de consumo en euros/año. Legacy 024.';
COMMENT ON COLUMN vz_demografia.pct_extranjeros IS
    'Fracción de población extranjera (nacida fuera de España). Rango 0–1.';
COMMENT ON COLUMN vz_demografia.nivel_estudios_alto_pct IS
    'Fracción de población con estudios superiores (universitarios o FP superior). Rango 0–1.';
COMMENT ON COLUMN vz_demografia.pct_poblacio_25_44 IS
    'Fracción de población en rango de edad 25–44 años. Rango 0–1. Legacy 018.';
COMMENT ON COLUMN vz_demografia.delta_renta_3a IS
    'Variación porcentual de la renta media en los últimos 3 años. Legacy 018.';
COMMENT ON COLUMN vz_demografia.gini IS
    'Coeficiente de Gini de la distribución de rentas en la zona. Rango 0–1. Legacy 023.';
COMMENT ON COLUMN vz_demografia.p80_p20 IS
    'Ratio percentil 80 / percentil 20 de renta. Indicador de desigualdad. Legacy 023.';
COMMENT ON COLUMN vz_demografia.tamano_hogar IS
    'Tamaño medio del hogar (personas/hogar). Legacy 023.';
COMMENT ON COLUMN vz_demografia.hogares_con_menores IS
    'Fracción de hogares con al menos un menor de 18 años. Legacy 023.';
COMMENT ON COLUMN vz_demografia.personas_solas IS
    'Fracción de hogares unipersonales. Legacy 023.';

CREATE INDEX IF NOT EXISTS idx_vz_demografia_zona_fecha
    ON vz_demografia (zona_id, fecha DESC);

-- ── Satélite: vz_comercial ───────────────────────────────────

CREATE TABLE vz_comercial (
    zona_id                     VARCHAR(30)         NOT NULL,
    fecha                       DATE                NOT NULL,
    num_negocios_activos        INT,
    pct_locales_vacios          DOUBLE PRECISION,
    tasa_rotacion_anual         DOUBLE PRECISION,
    ratio_locales_comerciales   DOUBLE PRECISION,
    licencias_nuevas_1a         INT,
    licencias_bajas_1a          INT,
    google_review_count_medio   DOUBLE PRECISION,
    fuente                      VARCHAR(50),
    created_at                  TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ         NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_vz_comercial
        PRIMARY KEY (zona_id, fecha),
    CONSTRAINT fk_vz_comercial_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE CASCADE,
    CONSTRAINT ck_vz_comercial_pct_vacios
        CHECK (pct_locales_vacios IS NULL
               OR (pct_locales_vacios >= 0 AND pct_locales_vacios <= 1)),
    CONSTRAINT ck_vz_comercial_ratio_locales
        CHECK (ratio_locales_comerciales IS NULL
               OR (ratio_locales_comerciales >= 0 AND ratio_locales_comerciales <= 1))
);

COMMENT ON TABLE vz_comercial IS
    'Satélite de actividad comercial por zona × fecha. '
    'Consolida legacy 017. '
    'Escrito por pipelines/comercio/cens_comercial.py y pipelines/comercio/llicencies.py.';

COMMENT ON COLUMN vz_comercial.pct_locales_vacios IS
    'Fracción de locales comerciales vacíos (sin actividad). Rango 0–1.';
COMMENT ON COLUMN vz_comercial.tasa_rotacion_anual IS
    'Fracción de negocios que cambian de titular o cierran en un año.';
COMMENT ON COLUMN vz_comercial.ratio_locales_comerciales IS
    'Fracción de planta baja con uso comercial respecto al total de planta baja. Rango 0–1. '
    'Columna clave para el pipeline de flujo peatonal (ponderación ST_DWithin).';
COMMENT ON COLUMN vz_comercial.licencias_nuevas_1a IS
    'Nuevas licencias de actividad comercial en el último año. Fuente: CKAN BCN.';
COMMENT ON COLUMN vz_comercial.licencias_bajas_1a IS
    'Bajas de licencias de actividad comercial en el último año. Fuente: CKAN BCN.';
COMMENT ON COLUMN vz_comercial.google_review_count_medio IS
    'Número medio de reseñas de Google Maps de los negocios de la zona.';

CREATE INDEX IF NOT EXISTS idx_vz_comercial_zona_fecha
    ON vz_comercial (zona_id, fecha DESC);

-- ── Satélite: vz_entorno ─────────────────────────────────────

CREATE TABLE vz_entorno (
    zona_id                     VARCHAR(30)         NOT NULL,
    fecha                       DATE                NOT NULL,
    incidencias_por_1000hab     DOUBLE PRECISION,
    nivel_ruido_db              DOUBLE PRECISION,
    m2_zonas_verdes_cercanas    DOUBLE PRECISION,
    score_equipamientos         DOUBLE PRECISION,
    seguridad_barri_score       DOUBLE PRECISION,
    hurtos_por_1000hab          DOUBLE PRECISION,
    robatoris_por_1000hab       DOUBLE PRECISION,
    danys_por_1000hab           DOUBLE PRECISION,
    incidencias_noche_pct       DOUBLE PRECISION,
    comisarias_1km              INT,
    dist_comisaria_m            DOUBLE PRECISION,
    mercados_municipales_1km    INT,
    fuente                      VARCHAR(50),
    created_at                  TIMESTAMPTZ         NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ         NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_vz_entorno
        PRIMARY KEY (zona_id, fecha),
    CONSTRAINT fk_vz_entorno_zona
        FOREIGN KEY (zona_id)
        REFERENCES zonas (id)
        ON DELETE CASCADE,
    CONSTRAINT ck_vz_entorno_incidencias_noche_pct
        CHECK (incidencias_noche_pct IS NULL
               OR (incidencias_noche_pct >= 0 AND incidencias_noche_pct <= 1))
);

COMMENT ON TABLE vz_entorno IS
    'Satélite de entorno físico, seguridad y equipamientos por zona × fecha. '
    'Consolida legacy 017 (base) + 018 (seguridad_barri_score) '
    '+ 020 (hurtos/robatoris/danys/incidencias_noche/comisarias_1km/dist_comisaria_m) '
    '+ 021 (mercados_municipales_1km). '
    'Escrito por pipelines/entorno/seguridad.py y pipelines/comercio/entorno_comercial.py.';

COMMENT ON COLUMN vz_entorno.incidencias_por_1000hab IS
    'Total de incidencias Guardia Urbana por cada 1.000 habitantes. Fuente: CKAN BCN.';
COMMENT ON COLUMN vz_entorno.nivel_ruido_db IS
    'Nivel de ruido medio en dB(A) según mapa de ruido del Ajuntament.';
COMMENT ON COLUMN vz_entorno.m2_zonas_verdes_cercanas IS
    'Superficie total de parques y zonas verdes en radio 500m en m². Fuente: AMB OpenData.';
COMMENT ON COLUMN vz_entorno.score_equipamientos IS
    'Score de acceso a equipamientos públicos (0–100): escuelas, centros salud, bibliotecas, etc.';
COMMENT ON COLUMN vz_entorno.seguridad_barri_score IS
    'Score de seguridad percibida del barrio (0–100). Legacy 018. '
    'Calculado en scoring/dimensiones/seguridad.py.';
COMMENT ON COLUMN vz_entorno.hurtos_por_1000hab IS
    'Hurtos (< 400 €) registrados por Guardia Urbana por cada 1.000 habitantes. Legacy 020.';
COMMENT ON COLUMN vz_entorno.robatoris_por_1000hab IS
    'Robos con violencia o intimidación por cada 1.000 habitantes. Legacy 020.';
COMMENT ON COLUMN vz_entorno.danys_por_1000hab IS
    'Daños a la propiedad registrados por cada 1.000 habitantes. Legacy 020.';
COMMENT ON COLUMN vz_entorno.incidencias_noche_pct IS
    'Fracción de incidencias totales que ocurren entre las 22h y las 6h. Rango 0–1. Legacy 020.';
COMMENT ON COLUMN vz_entorno.comisarias_1km IS
    'Número de comisarías (Mossos d''Esquadra + GU) en radio 1km. Legacy 020.';
COMMENT ON COLUMN vz_entorno.dist_comisaria_m IS
    'Distancia en metros a la comisaría más cercana. Legacy 020.';
COMMENT ON COLUMN vz_entorno.mercados_municipales_1km IS
    'Número de mercados municipales (Mercats de Barcelona) en radio 1km. Legacy 021.';

CREATE INDEX IF NOT EXISTS idx_vz_entorno_zona_fecha
    ON vz_entorno (zona_id, fecha DESC);

-- ── Vista: v_variables_zona ──────────────────────────────────
-- Réplica exacta del SELECT de legacy 035.
-- El orden de columnas es CANÓNICO: scoring/features.py las lee por nombre.
-- No reordenar columnas sin actualizar features.py.

CREATE OR REPLACE VIEW v_variables_zona AS
SELECT
    vz.zona_id,
    vz.fecha,
    vz.fuente,
    vz.num_lineas_transporte,
    vz.num_paradas_transporte,
    vz.score_transporte_calculado,
    vz.pipeline_run_id,
    -- vz_flujo
    f.flujo_peatonal_manana,
    f.flujo_peatonal_tarde,
    f.flujo_peatonal_noche,
    f.flujo_peatonal_total,
    f.flujo_popular_times_score,
    f.vcity_flujo_peatonal,
    f.vcity_tourist_rate,
    f.vcity_shopping_rate,
    f.vcity_resident_rate,
    f.weekend_lift,
    f.sunday_lift,
    f.weekday_midday_share,
    f.weekend_evening_share,
    f.late_night_share,
    f.holiday_proxy_score,
    f.temporal_confianza,
    f.seasonality_summer_lift,
    f.seasonality_christmas_lift,
    f.seasonality_rebajas_lift,
    f.seasonality_volatility,
    f.seasonality_peak_concentration,
    -- vz_turismo
    t.score_turismo,
    t.score_turismo_airbnb,
    t.score_turismo_hut,
    t.airbnb_density_500m,
    t.airbnb_occupancy_est,
    t.booking_hoteles_500m,
    t.booking_rating_medio,
    t.cruceros_visitantes_anuales,
    t.puntos_interes_culturales_500m,
    t.mercados_municipales_500m,
    t.eventos_culturales_500m,
    t.venues_musicales_500m,
    t.dist_landmark_top3_m,
    -- vz_demografia
    d.poblacion,
    d.edad_media,
    d.renta_media_hogar,
    d.renta_media_uc,
    d.renta_mediana_uc,
    d.pct_extranjeros,
    d.densidad_hab_km2,
    d.nivel_estudios_alto_pct,
    d.pct_poblacio_25_44,
    d.delta_renta_3a,
    d.gini,
    d.p80_p20,
    d.tamano_hogar,
    d.hogares_con_menores,
    d.personas_solas,
    -- vz_comercial
    c.num_negocios_activos,
    c.pct_locales_vacios,
    c.tasa_rotacion_anual,
    c.ratio_locales_comerciales,
    c.licencias_nuevas_1a,
    c.licencias_bajas_1a,
    c.google_review_count_medio,
    -- vz_entorno
    e.incidencias_por_1000hab,
    e.nivel_ruido_db,
    e.m2_zonas_verdes_cercanas,
    e.score_equipamientos,
    e.seguridad_barri_score,
    e.hurtos_por_1000hab,
    e.robatoris_por_1000hab,
    e.danys_por_1000hab,
    e.incidencias_noche_pct,
    e.comisarias_1km,
    e.dist_comisaria_m,
    e.mercados_municipales_1km
FROM variables_zona vz
LEFT JOIN vz_flujo      f USING (zona_id, fecha)
LEFT JOIN vz_turismo    t USING (zona_id, fecha)
LEFT JOIN vz_demografia d USING (zona_id, fecha)
LEFT JOIN vz_comercial  c USING (zona_id, fecha)
LEFT JOIN vz_entorno    e USING (zona_id, fecha);

COMMENT ON VIEW v_variables_zona IS
    'Vista unificada de variables por zona × fecha. '
    'Replica el SELECT canónico de la migración legacy 035. '
    'El orden de columnas es fijo: scoring/features.py las consume por nombre. '
    'No reordenar columnas sin actualizar FEATURE_NAMES en scoring/features.py.';

-- ── Triggers updated_at ──────────────────────────────────────
-- Aplica a: variables_zona + 5 satélites (todos tienen updated_at).

DO $$
DECLARE
    t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'variables_zona',
        'vz_flujo',
        'vz_turismo',
        'vz_demografia',
        'vz_comercial',
        'vz_entorno'
    ]
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
