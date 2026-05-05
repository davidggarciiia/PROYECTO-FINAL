-- 037_benchmarks_subsector.sql
-- Capa de benchmarks granulares por subsector (base.sector.subsector de la taxonomía).
-- Fallback a benchmarks_sector cuando no existe fila para el subsector.

CREATE TABLE IF NOT EXISTS benchmarks_subsector (
    subsector_codigo          TEXT PRIMARY KEY,  -- slug de taxonomy: "tapas_bar", "fine_dining", …
    sector_codigo             TEXT NOT NULL,     -- sector padre: "restauracion", "moda", …

    -- Ticket
    ticket_medio_min          NUMERIC(10,2),
    ticket_medio_max          NUMERIC(10,2),

    -- Márgenes
    margen_bruto_tipico       NUMERIC(5,3),      -- 0-1

    -- Reforma e inversión inicial
    reforma_m2_min            NUMERIC(10,2),
    reforma_m2_max            NUMERIC(10,2),
    equipamiento_base_min     NUMERIC(12,2),
    equipamiento_base_max     NUMERIC(12,2),

    -- Operaciones
    horas_apertura_dia        NUMERIC(4,1),
    dias_apertura_mes_tipico  SMALLINT,

    -- Demanda
    conversion_rate_min       NUMERIC(6,4),
    conversion_rate_max       NUMERIC(6,4),

    -- Personal
    empleados_por_m2          NUMERIC(6,2),
    salario_base_mensual_convenio NUMERIC(10,2),

    -- Costes fijos adicionales
    coste_suministros_por_m2  NUMERIC(6,2),

    -- Umbral alquiler orientativo para este subsector
    alquiler_sobre_ventas_max NUMERIC(5,3),

    -- Modelo de negocio
    is_appointment_based      BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_benchmarks_subsector_sector ON benchmarks_subsector(sector_codigo);

-- ─── DATOS: restauración ────────────────────────────────────────────────────

INSERT INTO benchmarks_subsector VALUES
-- tapas_bar: alta rotación, ticket bajo-medio, dependiente del flujo
('tapas_bar', 'restauracion',
 12.0, 25.0,    -- ticket
 0.63,          -- margen bruto
 450.0, 750.0,  -- reforma/m²
 15000, 32000,  -- equipamiento
 11.0, 26,      -- horas, días
 0.008, 0.022,  -- conversion rate
 18.0, 1750.0,  -- empleados/m², salario convenio
 5.5,           -- suministros/m²
 0.14,          -- umbral alquiler
 FALSE),

-- specialty_coffee: alta inversión en equipo, ticket medio-alto, servicio selectivo
('specialty_coffee', 'restauracion',
 8.0, 18.0,
 0.68,
 400.0, 700.0,
 18000, 40000,
 9.0, 26,
 0.007, 0.022,
 20.0, 1750.0,
 5.0,
 0.13,
 FALSE),

-- neighborhood_cafe: bajo ticket, alta rotación, local de barrio
('neighborhood_cafe', 'restauracion',
 4.0, 11.0,
 0.65,
 280.0, 500.0,
 8000, 20000,
 10.0, 26,
 0.010, 0.030,
 22.0, 1700.0,
 5.0,
 0.15,
 FALSE),

-- express_cafe: mínima inversión, altísima rotación, máxima dependencia del paso
('express_cafe', 'restauracion',
 3.0, 8.0,
 0.70,
 200.0, 400.0,
 7000, 16000,
 12.0, 26,
 0.014, 0.038,
 25.0, 1650.0,
 4.5,
 0.15,
 FALSE),

-- brunch_house: ticket medio-alto, fin de semana fuerte, operación de comida
('brunch_house', 'restauracion',
 16.0, 32.0,
 0.66,
 400.0, 700.0,
 15000, 32000,
 8.0, 24,
 0.007, 0.018,
 18.0, 1750.0,
 5.5,
 0.13,
 FALSE),

-- fine_dining: altísima inversión, ticket premium, pocas mesas, cita previa
('fine_dining', 'restauracion',
 45.0, 95.0,
 0.68,
 800.0, 1400.0,
 35000, 90000,
 5.5, 22,
 0.002, 0.008,
 12.0, 2000.0,
 7.0,
 0.12,
 FALSE),

-- cocktail_bar: ticket alto, horario nocturno, alta inversión en barra
('cocktail_bar', 'restauracion',
 14.0, 28.0,
 0.72,
 500.0, 900.0,
 22000, 50000,
 6.0, 24,
 0.006, 0.018,
 15.0, 1750.0,
 6.0,
 0.13,
 FALSE),

-- street_food_counter: mínima inversión, ticket muy bajo, altísima rotación y flujo
('street_food_counter', 'restauracion',
 7.0, 16.0,
 0.65,
 280.0, 550.0,
 10000, 22000,
 11.0, 26,
 0.012, 0.032,
 25.0, 1650.0,
 4.5,
 0.15,
 FALSE),

-- vegan_bistro: ticket medio, margen algo inferior por ingredientes especiales
('vegan_bistro', 'restauracion',
 14.0, 28.0,
 0.63,
 400.0, 680.0,
 14000, 28000,
 9.0, 25,
 0.006, 0.016,
 18.0, 1750.0,
 5.5,
 0.13,
 FALSE),

-- coworking_cafe: ticket medio-alto, alta permanencia, pocas rotaciones
('coworking_cafe', 'restauracion',
 8.0, 18.0,
 0.67,
 380.0, 650.0,
 16000, 35000,
 10.0, 26,
 0.005, 0.014,
 20.0, 1750.0,
 5.0,
 0.13,
 FALSE)

ON CONFLICT (subsector_codigo) DO UPDATE SET
    ticket_medio_min          = EXCLUDED.ticket_medio_min,
    ticket_medio_max          = EXCLUDED.ticket_medio_max,
    margen_bruto_tipico       = EXCLUDED.margen_bruto_tipico,
    reforma_m2_min            = EXCLUDED.reforma_m2_min,
    reforma_m2_max            = EXCLUDED.reforma_m2_max,
    equipamiento_base_min     = EXCLUDED.equipamiento_base_min,
    equipamiento_base_max     = EXCLUDED.equipamiento_base_max,
    horas_apertura_dia        = EXCLUDED.horas_apertura_dia,
    dias_apertura_mes_tipico  = EXCLUDED.dias_apertura_mes_tipico,
    conversion_rate_min       = EXCLUDED.conversion_rate_min,
    conversion_rate_max       = EXCLUDED.conversion_rate_max,
    empleados_por_m2          = EXCLUDED.empleados_por_m2,
    salario_base_mensual_convenio = EXCLUDED.salario_base_mensual_convenio,
    coste_suministros_por_m2  = EXCLUDED.coste_suministros_por_m2,
    alquiler_sobre_ventas_max = EXCLUDED.alquiler_sobre_ventas_max,
    is_appointment_based      = EXCLUDED.is_appointment_based;

-- ─── DATOS: estética ────────────────────────────────────────────────────────

INSERT INTO benchmarks_subsector VALUES
('hair_salon', 'estetica',
 35.0, 85.0,
 0.68,
 280.0, 520.0,
 12000, 28000,
 9.0, 25,
 NULL, NULL,
 12.0, 1750.0,
 4.0,
 0.12,
 TRUE),

('barber_shop', 'estetica',
 18.0, 45.0,
 0.70,
 220.0, 450.0,
 8000, 20000,
 9.0, 25,
 NULL, NULL,
 14.0, 1700.0,
 3.5,
 0.12,
 TRUE),

('nail_studio', 'estetica',
 30.0, 70.0,
 0.68,
 200.0, 400.0,
 8000, 18000,
 9.0, 25,
 NULL, NULL,
 15.0, 1700.0,
 3.5,
 0.11,
 TRUE),

('day_spa', 'estetica',
 50.0, 130.0,
 0.65,
 400.0, 750.0,
 20000, 50000,
 9.0, 24,
 NULL, NULL,
 10.0, 1800.0,
 5.0,
 0.11,
 TRUE),

('beauty_clinic', 'estetica',
 60.0, 180.0,
 0.67,
 350.0, 650.0,
 18000, 45000,
 8.0, 22,
 NULL, NULL,
 10.0, 1900.0,
 4.5,
 0.10,
 TRUE)

ON CONFLICT (subsector_codigo) DO UPDATE SET
    ticket_medio_min = EXCLUDED.ticket_medio_min,
    ticket_medio_max = EXCLUDED.ticket_medio_max,
    margen_bruto_tipico = EXCLUDED.margen_bruto_tipico,
    reforma_m2_min = EXCLUDED.reforma_m2_min,
    reforma_m2_max = EXCLUDED.reforma_m2_max,
    equipamiento_base_min = EXCLUDED.equipamiento_base_min,
    equipamiento_base_max = EXCLUDED.equipamiento_base_max,
    horas_apertura_dia = EXCLUDED.horas_apertura_dia,
    dias_apertura_mes_tipico = EXCLUDED.dias_apertura_mes_tipico,
    empleados_por_m2 = EXCLUDED.empleados_por_m2,
    salario_base_mensual_convenio = EXCLUDED.salario_base_mensual_convenio,
    coste_suministros_por_m2 = EXCLUDED.coste_suministros_por_m2,
    alquiler_sobre_ventas_max = EXCLUDED.alquiler_sobre_ventas_max,
    is_appointment_based = EXCLUDED.is_appointment_based;

-- ─── DATOS: tatuajes ────────────────────────────────────────────────────────

INSERT INTO benchmarks_subsector VALUES
('tattoo_studio', 'tatuajes',
 80.0, 200.0,
 0.72,
 300.0, 600.0,
 15000, 35000,
 8.0, 22,
 NULL, NULL,
 20.0, 1800.0,
 3.5,
 0.10,
 TRUE),

('fine_line_studio', 'tatuajes',
 100.0, 280.0,
 0.74,
 350.0, 700.0,
 18000, 45000,
 8.0, 20,
 NULL, NULL,
 22.0, 1900.0,
 3.5,
 0.09,
 TRUE),

('street_tattoo', 'tatuajes',
 60.0, 140.0,
 0.68,
 250.0, 500.0,
 12000, 25000,
 9.0, 25,
 0.004, 0.012,
 18.0, 1700.0,
 4.0,
 0.12,
 FALSE)

ON CONFLICT (subsector_codigo) DO UPDATE SET
    ticket_medio_min = EXCLUDED.ticket_medio_min,
    ticket_medio_max = EXCLUDED.ticket_medio_max,
    margen_bruto_tipico = EXCLUDED.margen_bruto_tipico,
    reforma_m2_min = EXCLUDED.reforma_m2_min,
    reforma_m2_max = EXCLUDED.reforma_m2_max,
    equipamiento_base_min = EXCLUDED.equipamiento_base_min,
    equipamiento_base_max = EXCLUDED.equipamiento_base_max,
    horas_apertura_dia = EXCLUDED.horas_apertura_dia,
    dias_apertura_mes_tipico = EXCLUDED.dias_apertura_mes_tipico,
    empleados_por_m2 = EXCLUDED.empleados_por_m2,
    salario_base_mensual_convenio = EXCLUDED.salario_base_mensual_convenio,
    coste_suministros_por_m2 = EXCLUDED.coste_suministros_por_m2,
    alquiler_sobre_ventas_max = EXCLUDED.alquiler_sobre_ventas_max,
    is_appointment_based = EXCLUDED.is_appointment_based;
