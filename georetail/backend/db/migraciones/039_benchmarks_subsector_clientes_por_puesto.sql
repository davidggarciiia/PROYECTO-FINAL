-- 039_benchmarks_subsector_clientes_por_puesto.sql
-- Añade columnas de demanda por puesto a benchmarks_subsector y benchmarks_subsector_llm.
-- _clientes_cita_base() ya lee estas columnas con fallback a 2/5 cuando no existen en BD.
-- Valores calibrados para negocios de cita en Barcelona (datos sector 2024).

ALTER TABLE benchmarks_subsector
    ADD COLUMN IF NOT EXISTS clientes_dia_por_puesto_min NUMERIC(6,2),
    ADD COLUMN IF NOT EXISTS clientes_dia_por_puesto_max NUMERIC(6,2);

ALTER TABLE benchmarks_subsector_llm
    ADD COLUMN IF NOT EXISTS clientes_dia_por_puesto_min NUMERIC(6,2),
    ADD COLUMN IF NOT EXISTS clientes_dia_por_puesto_max NUMERIC(6,2);

-- ─── Estética ────────────────────────────────────────────────────────────────
-- Barber shop: cortes 25-40 min → 6-9 clientes/silla/día (9h apertura, 70% ocupación)
UPDATE benchmarks_subsector
SET clientes_dia_por_puesto_min = 6.0, clientes_dia_por_puesto_max = 9.0
WHERE subsector_codigo = 'barber_shop';

-- Hair salon: servicios 45-90 min → 4-7 clientes/silla/día
UPDATE benchmarks_subsector
SET clientes_dia_por_puesto_min = 4.0, clientes_dia_por_puesto_max = 7.0
WHERE subsector_codigo = 'hair_salon';

-- Nail studio: servicios 45-75 min → 4-8 clientes/puesto/día
UPDATE benchmarks_subsector
SET clientes_dia_por_puesto_min = 4.0, clientes_dia_por_puesto_max = 8.0
WHERE subsector_codigo = 'nail_studio';

-- Day spa: tratamientos 60-120 min → 2-4 clientes/cabina/día
UPDATE benchmarks_subsector
SET clientes_dia_por_puesto_min = 2.0, clientes_dia_por_puesto_max = 4.0
WHERE subsector_codigo = 'day_spa';

-- Beauty clinic: procedimientos médico-estéticos 60-120 min → 2-4 por cabina
UPDATE benchmarks_subsector
SET clientes_dia_por_puesto_min = 2.0, clientes_dia_por_puesto_max = 4.0
WHERE subsector_codigo = 'beauty_clinic';

-- ─── Tatuajes ────────────────────────────────────────────────────────────────
-- Tattoo studio: sesiones 2-4h → 1-3 clientes/artista/día
UPDATE benchmarks_subsector
SET clientes_dia_por_puesto_min = 1.0, clientes_dia_por_puesto_max = 3.0
WHERE subsector_codigo = 'tattoo_studio';

-- Fine line studio: sesiones largas, detalle extremo → 1-2 clientes/artista/día
UPDATE benchmarks_subsector
SET clientes_dia_por_puesto_min = 1.0, clientes_dia_por_puesto_max = 2.0
WHERE subsector_codigo = 'fine_line_studio';

-- Street tattoo: estilos más rápidos → 2-4 clientes/artista/día
UPDATE benchmarks_subsector
SET clientes_dia_por_puesto_min = 2.0, clientes_dia_por_puesto_max = 4.0
WHERE subsector_codigo = 'street_tattoo';
