-- ============================================================
-- SEED: Transporte público BCN — datos representativos
-- Ejecutar cuando los pipelines TMB/Bicing no están disponibles
-- ============================================================

-- Limpiar datos seed previos si existen
DELETE FROM paradas_lineas WHERE parada_id LIKE 'sp_%';
DELETE FROM paradas_transporte WHERE fuente = 'seed';
DELETE FROM estaciones_bicing WHERE fuente = 'seed';
DELETE FROM carriles_bici WHERE fuente = 'seed';

-- 1. Líneas de transporte representativas de BCN
INSERT INTO lineas_transporte (id, codigo, nombre, tipo, subtipo, color_hex, fuente) VALUES
  ('l1-metro',   'L1',  'Linia 1 (Roja)',          'metro',    'metro',         '#E3000B', 'seed'),
  ('l2-metro',   'L2',  'Linia 2 (Lila)',           'metro',    'metro',         '#9B2743', 'seed'),
  ('l3-metro',   'L3',  'Linia 3 (Verda)',          'metro',    'metro',         '#007A35', 'seed'),
  ('l4-metro',   'L4',  'Linia 4 (Groga)',          'metro',    'metro',         '#FFD700', 'seed'),
  ('l5-metro',   'L5',  'Linia 5 (Blava)',          'metro',    'metro',         '#0066CC', 'seed'),
  ('s1-fgc',     'S1',  'FGC Linia S1',             'fgc',      'fgc',           '#F4A301', 'seed'),
  ('t1-tram',    'T1',  'Trambaix T1',              'tram',     'tram',          '#83B040', 'seed'),
  ('t4-tram',    'T4',  'Trambesòs T4',             'tram',     'tram',          '#83B040', 'seed'),
  ('h12-bus',    'H12', 'Bus H12 Horitzontal',      'bus',      'bus_ortogonal', '#004A97', 'seed'),
  ('v15-bus',    'V15', 'Bus V15 Vertical',         'bus',      'bus_ortogonal', '#004A97', 'seed'),
  ('d20-bus',    'D20', 'Bus D20 Diagonal',         'bus',      'bus_ortogonal', '#004A97', 'seed'),
  ('x24-bus',    'X24', 'Bus X24 Express',          'bus',      'bus_ortogonal', '#004A97', 'seed'),
  ('b17-bus',    '17',  'Bus 17',                   'bus',      NULL,            '#E05206', 'seed'),
  ('b45-bus',    '45',  'Bus 45',                   'bus',      NULL,            '#E05206', 'seed'),
  ('b64-bus',    '64',  'Bus 64',                   'bus',      NULL,            '#E05206', 'seed'),
  ('r3-rodalies','R3',  'Rodalies R3',              'rodalies', 'rodalies',      '#FF6C00', 'seed')
ON CONFLICT (id) DO NOTHING;

-- 2. Paradas de metro/fgc/tram: una parada por zona, ID acortado
-- Format: sp_<6chars_zone>_<linea_short>  => max ~20 chars
INSERT INTO paradas_transporte (id, nombre, lat, lng, geometria, zona_id, es_accesible_pmr, fuente)
SELECT
    'sp_' || substring(z.id, 5, 6) || '_' || row_number() OVER (PARTITION BY z.id ORDER BY lt.id)::text || '_' || substring(lt.id, 1, 4),
    lt.codigo || ' - ' || left(z.nombre, 20),
    ST_Y(ST_Centroid(z.geometria)),
    ST_X(ST_Centroid(z.geometria)),
    ST_SetSRID(
        ST_MakePoint(
            ST_X(ST_Centroid(z.geometria)),
            ST_Y(ST_Centroid(z.geometria))
        ), 4326
    ),
    z.id,
    true,
    'seed'
FROM zonas z
CROSS JOIN lineas_transporte lt
WHERE lt.tipo IN ('metro', 'fgc', 'tram')
ON CONFLICT (id) DO NOTHING;

-- 3. Paradas de bus ortogonal: una por zona, ligeramente desplazadas
INSERT INTO paradas_transporte (id, nombre, lat, lng, geometria, zona_id, es_accesible_pmr, fuente)
SELECT
    'sbo_' || substring(z.id, 5, 6) || '_' || row_number() OVER (PARTITION BY z.id ORDER BY lt.id)::text || '_' || substring(lt.id, 1, 4),
    lt.codigo || ' bus - ' || left(z.nombre, 16),
    ST_Y(ST_Centroid(z.geometria)) + 0.0002,
    ST_X(ST_Centroid(z.geometria)) + 0.0002,
    ST_SetSRID(
        ST_MakePoint(
            ST_X(ST_Centroid(z.geometria)) + 0.0002,
            ST_Y(ST_Centroid(z.geometria)) + 0.0002
        ), 4326
    ),
    z.id,
    true,
    'seed'
FROM zonas z
CROSS JOIN lineas_transporte lt
WHERE lt.subtipo = 'bus_ortogonal'
ON CONFLICT (id) DO NOTHING;

-- 4. Vincular paradas metro/fgc/tram con sus líneas
-- Las paradas tienen en su id la forma sp_<zone>_<n>_<linea_prefix>
-- Usamos linea_id extraído del row via JOIN directo
INSERT INTO paradas_lineas (parada_id, linea_id)
SELECT DISTINCT p.id, lt.id
FROM paradas_transporte p
JOIN lineas_transporte lt
  ON p.id LIKE 'sp_%' AND p.id LIKE '%_' || substring(lt.id, 1, 4)
  AND lt.tipo IN ('metro', 'fgc', 'tram')
WHERE p.fuente = 'seed'
ON CONFLICT DO NOTHING;

-- Vincular paradas bus ortogonal
INSERT INTO paradas_lineas (parada_id, linea_id)
SELECT DISTINCT p.id, lt.id
FROM paradas_transporte p
JOIN lineas_transporte lt
  ON p.id LIKE 'sbo_%' AND p.id LIKE '%_' || substring(lt.id, 1, 4)
  AND lt.subtipo = 'bus_ortogonal'
WHERE p.fuente = 'seed'
ON CONFLICT DO NOTHING;

-- 5. Frecuencias de transporte (laborable, todas las franjas)
INSERT INTO frecuencias_transporte (linea_id, dia_tipo, franja, frecuencia_min)
SELECT
    lt.id,
    dias.dia_tipo,
    franjas.franja,
    CASE lt.tipo
        WHEN 'metro'    THEN CASE WHEN franjas.franja = 'manana' THEN 4.0 WHEN franjas.franja = 'tarde' THEN 5.0 ELSE 8.0 END
        WHEN 'fgc'      THEN CASE WHEN franjas.franja = 'manana' THEN 7.0 WHEN franjas.franja = 'tarde' THEN 8.0 ELSE 12.0 END
        WHEN 'tram'     THEN CASE WHEN franjas.franja = 'manana' THEN 8.0 WHEN franjas.franja = 'tarde' THEN 10.0 ELSE 15.0 END
        WHEN 'bus'      THEN CASE WHEN franjas.franja = 'manana' THEN 10.0 WHEN franjas.franja = 'tarde' THEN 12.0 ELSE 20.0 END
        WHEN 'rodalies' THEN CASE WHEN franjas.franja = 'manana' THEN 20.0 WHEN franjas.franja = 'tarde' THEN 20.0 ELSE 30.0 END
        ELSE 15.0
    END
FROM lineas_transporte lt
CROSS JOIN (VALUES ('laborable'), ('sabado'), ('festivo')) AS dias(dia_tipo)
CROSS JOIN (VALUES ('manana'), ('tarde'), ('noche')) AS franjas(franja)
WHERE lt.fuente = 'seed';

-- 6. Estaciones Bicing: 3 por zona, ID máx 20 chars
-- Format: bc_<6chars_zone>_<n>  => 3+1+6+1+1 = 12 chars
INSERT INTO estaciones_bicing (id, nombre, lat, lng, geometria, capacidad, zona_id, esta_activa, fuente)
SELECT
    'bc_' || substring(z.id, 5, 6) || '_' || n.num,
    'Bicing ' || left(z.nombre, 12) || ' ' || n.num,
    ST_Y(ST_Centroid(z.geometria)) + ((n.num - 2)::double precision * 0.0005),
    ST_X(ST_Centroid(z.geometria)) + ((n.num - 2)::double precision * 0.0004),
    ST_SetSRID(
        ST_MakePoint(
            ST_X(ST_Centroid(z.geometria)) + ((n.num - 2)::double precision * 0.0004),
            ST_Y(ST_Centroid(z.geometria)) + ((n.num - 2)::double precision * 0.0005)
        ), 4326
    ),
    20 + (n.num * 5),
    z.id,
    true,
    'seed'
FROM zonas z
CROSS JOIN (VALUES (1), (2), (3)) AS n(num)
ON CONFLICT (id) DO NOTHING;

-- 7. Carriles bici: un carril ~300m por zona
INSERT INTO carriles_bici (tipo, geometria, longitud_m, fuente)
SELECT
    'carril_protegido',
    ST_SetSRID(
        ST_MakeLine(
            ST_MakePoint(
                ST_X(ST_Centroid(z.geometria)) - 0.0015,
                ST_Y(ST_Centroid(z.geometria))
            ),
            ST_MakePoint(
                ST_X(ST_Centroid(z.geometria)) + 0.0015,
                ST_Y(ST_Centroid(z.geometria))
            )
        ), 4326
    ),
    300.0,
    'seed'
FROM zonas z;

-- Verificación final
SELECT
    (SELECT COUNT(*) FROM lineas_transporte)        AS lineas,
    (SELECT COUNT(*) FROM paradas_transporte)        AS paradas,
    (SELECT COUNT(*) FROM paradas_lineas)            AS paradas_lineas,
    (SELECT COUNT(*) FROM frecuencias_transporte)    AS frecuencias,
    (SELECT COUNT(*) FROM estaciones_bicing)         AS bicing,
    (SELECT COUNT(*) FROM carriles_bici)             AS carriles_bici;
