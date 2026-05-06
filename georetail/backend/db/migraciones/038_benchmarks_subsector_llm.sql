-- 038_benchmarks_subsector_llm.sql
-- Cache persistente para benchmarks generados por LLM.
-- Objetivo: hacer el fallback LLM determinista — primera llamada genera y guarda,
-- las siguientes leen de aquí sin llamar al LLM.
-- Los campos reflejan exactamente el schema de benchmarks_subsector para
-- que aplicar_subsector() pueda usarlos sin distinción de origen.

CREATE TABLE IF NOT EXISTS benchmarks_subsector_llm (
    subsector_codigo            TEXT PRIMARY KEY,
    sector_codigo               TEXT NOT NULL,
    ticket_medio_min            NUMERIC(10,2),
    ticket_medio_max            NUMERIC(10,2),
    margen_bruto_tipico         NUMERIC(5,3),
    is_appointment_based        BOOLEAN DEFAULT FALSE,
    empleados_por_m2            NUMERIC(6,2),
    horas_apertura_dia          NUMERIC(4,1),
    dias_apertura_mes_tipico    SMALLINT,
    conversion_rate_min         NUMERIC(6,4),
    conversion_rate_max         NUMERIC(6,4),
    created_at                  TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_benchmarks_subsector_llm_sector
    ON benchmarks_subsector_llm(sector_codigo);
