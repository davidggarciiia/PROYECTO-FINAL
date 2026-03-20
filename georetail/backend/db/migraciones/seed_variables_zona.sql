-- seed_variables_zona.sql
-- Datos sintéticos realistas para variables_zona basados en características
-- reales de los distritos de Barcelona.
-- Generados una única vez para que el modelo XGBoost/pesos-manual tenga
-- diferenciación por zona mientras los pipelines externos no están activos.

INSERT INTO variables_zona (
    zona_id, fecha,
    flujo_peatonal_manana, flujo_peatonal_tarde, flujo_peatonal_noche, flujo_peatonal_total,
    poblacion, edad_media, renta_media_hogar, pct_extranjeros,
    densidad_hab_km2, nivel_estudios_alto_pct,
    num_negocios_activos, pct_locales_vacios, tasa_rotacion_anual,
    ratio_locales_comerciales,
    score_turismo, incidencias_por_1000hab, nivel_ruido_db,
    m2_zonas_verdes_cercanas, score_equipamientos,
    fuente
)
SELECT
    z.id AS zona_id,
    CURRENT_DATE AS fecha,

    -- Flujo peatonal (pax/h): varía por distrito y zona
    GREATEST(50, ROUND((
        CASE d.nombre
            WHEN 'Ciutat Vella'         THEN 1800
            WHEN 'Eixample'             THEN 1200
            WHEN 'Gràcia'               THEN  700
            WHEN 'Sant Martí'           THEN  600
            WHEN 'Sants-Montjuïc'       THEN  500
            WHEN 'Les Corts'            THEN  450
            WHEN 'Sarrià-Sant Gervasi'  THEN  380
            WHEN 'Horta-Guinardó'       THEN  320
            WHEN 'Sant Andreu'          THEN  400
            WHEN 'Nou Barris'           THEN  280
            ELSE 400
        END + (hashtext(z.id || 'flujo_m') % 300)
    )::numeric, 0)) AS flujo_peatonal_manana,

    GREATEST(80, ROUND((
        CASE d.nombre
            WHEN 'Ciutat Vella'         THEN 2200
            WHEN 'Eixample'             THEN 1500
            WHEN 'Gràcia'               THEN  950
            WHEN 'Sant Martí'           THEN  800
            WHEN 'Sants-Montjuïc'       THEN  650
            WHEN 'Les Corts'            THEN  580
            WHEN 'Sarrià-Sant Gervasi'  THEN  490
            WHEN 'Horta-Guinardó'       THEN  420
            WHEN 'Sant Andreu'          THEN  520
            WHEN 'Nou Barris'           THEN  360
            ELSE 550
        END + (hashtext(z.id || 'flujo_t') % 400)
    )::numeric, 0)) AS flujo_peatonal_tarde,

    GREATEST(30, ROUND((
        CASE d.nombre
            WHEN 'Ciutat Vella'         THEN 1400
            WHEN 'Eixample'             THEN  700
            WHEN 'Gràcia'               THEN  600
            WHEN 'Sant Martí'           THEN  300
            WHEN 'Sants-Montjuïc'       THEN  250
            WHEN 'Les Corts'            THEN  180
            WHEN 'Sarrià-Sant Gervasi'  THEN  150
            WHEN 'Horta-Guinardó'       THEN  130
            WHEN 'Sant Andreu'          THEN  200
            WHEN 'Nou Barris'           THEN  100
            ELSE 200
        END + (hashtext(z.id || 'flujo_n') % 250)
    )::numeric, 0)) AS flujo_peatonal_noche,

    -- total = suma aproximada de los tres periodos
    GREATEST(160, ROUND((
        CASE d.nombre
            WHEN 'Ciutat Vella'         THEN 5400
            WHEN 'Eixample'             THEN 3400
            WHEN 'Gràcia'               THEN 2250
            WHEN 'Sant Martí'           THEN 1700
            WHEN 'Sants-Montjuïc'       THEN 1400
            WHEN 'Les Corts'            THEN 1210
            WHEN 'Sarrià-Sant Gervasi'  THEN 1020
            WHEN 'Horta-Guinardó'       THEN  870
            WHEN 'Sant Andreu'          THEN 1120
            WHEN 'Nou Barris'           THEN  740
            ELSE 1150
        END + (hashtext(z.id || 'flujo_total') % 800)
    )::numeric, 0)) AS flujo_peatonal_total,

    -- Población
    GREATEST(1000, 2500 + (hashtext(z.id || 'pob') % 4000)) AS poblacion,

    -- Edad media
    ROUND((
        CASE d.nombre
            WHEN 'Sarrià-Sant Gervasi'  THEN 44.5
            WHEN 'Les Corts'            THEN 43.2
            WHEN 'Eixample'             THEN 42.8
            WHEN 'Gràcia'               THEN 41.5
            WHEN 'Sant Martí'           THEN 40.1
            WHEN 'Sants-Montjuïc'       THEN 39.8
            WHEN 'Horta-Guinardó'       THEN 42.3
            WHEN 'Sant Andreu'          THEN 40.9
            WHEN 'Nou Barris'           THEN 41.7
            WHEN 'Ciutat Vella'         THEN 37.5
            ELSE 41.0
        END + (hashtext(z.id || 'edad') % 5) - 2.0
    )::numeric, 1) AS edad_media,

    -- Renta media hogar (€/año)
    GREATEST(18000, ROUND((
        CASE d.nombre
            WHEN 'Sarrià-Sant Gervasi'  THEN 68000
            WHEN 'Les Corts'            THEN 52000
            WHEN 'Eixample'             THEN 46000
            WHEN 'Gràcia'               THEN 38000
            WHEN 'Sant Martí'           THEN 34000
            WHEN 'Sants-Montjuïc'       THEN 30000
            WHEN 'Horta-Guinardó'       THEN 32000
            WHEN 'Sant Andreu'          THEN 31000
            WHEN 'Nou Barris'           THEN 24000
            WHEN 'Ciutat Vella'         THEN 26000
            ELSE 34000
        END + (hashtext(z.id || 'renta') % 8000) - 4000
    )::numeric, 0)) AS renta_media_hogar,

    -- % extranjeros
    GREATEST(0.03, LEAST(0.55, ROUND((
        CASE d.nombre
            WHEN 'Ciutat Vella'         THEN 0.42
            WHEN 'Eixample'             THEN 0.22
            WHEN 'Gràcia'               THEN 0.18
            WHEN 'Sant Martí'           THEN 0.19
            WHEN 'Sants-Montjuïc'       THEN 0.24
            WHEN 'Les Corts'            THEN 0.15
            WHEN 'Sarrià-Sant Gervasi'  THEN 0.14
            WHEN 'Horta-Guinardó'       THEN 0.14
            WHEN 'Sant Andreu'          THEN 0.17
            WHEN 'Nou Barris'           THEN 0.22
            ELSE 0.18
        END + ((hashtext(z.id || 'ext') % 10) - 5) * 0.01
    )::numeric, 2))) AS pct_extranjeros,

    -- Densidad hab/km²
    GREATEST(5000, ROUND((
        CASE d.nombre
            WHEN 'Ciutat Vella'         THEN 22000
            WHEN 'Eixample'             THEN 35000
            WHEN 'Gràcia'               THEN 28000
            WHEN 'Sant Martí'           THEN 19000
            WHEN 'Sants-Montjuïc'       THEN 16000
            WHEN 'Les Corts'            THEN 12000
            WHEN 'Sarrià-Sant Gervasi'  THEN  7000
            WHEN 'Horta-Guinardó'       THEN 16000
            WHEN 'Sant Andreu'          THEN 18000
            WHEN 'Nou Barris'           THEN 20000
            ELSE 18000
        END + (hashtext(z.id || 'dens') % 3000)
    )::numeric, 0)) AS densidad_hab_km2,

    -- Nivel estudios alto (%)
    GREATEST(0.15, LEAST(0.75, ROUND((
        CASE d.nombre
            WHEN 'Sarrià-Sant Gervasi'  THEN 0.68
            WHEN 'Les Corts'            THEN 0.58
            WHEN 'Eixample'             THEN 0.52
            WHEN 'Gràcia'               THEN 0.50
            WHEN 'Sant Martí'           THEN 0.43
            WHEN 'Sants-Montjuïc'       THEN 0.35
            WHEN 'Horta-Guinardó'       THEN 0.37
            WHEN 'Sant Andreu'          THEN 0.36
            WHEN 'Nou Barris'           THEN 0.28
            WHEN 'Ciutat Vella'         THEN 0.38
            ELSE 0.40
        END + ((hashtext(z.id || 'edu') % 10) - 5) * 0.01
    )::numeric, 2))) AS nivel_estudios_alto_pct,

    -- Número negocios activos
    GREATEST(5, ROUND((
        CASE d.nombre
            WHEN 'Eixample'             THEN 180
            WHEN 'Ciutat Vella'         THEN 160
            WHEN 'Gràcia'               THEN 120
            WHEN 'Sant Martí'           THEN  90
            WHEN 'Sants-Montjuïc'       THEN  80
            WHEN 'Les Corts'            THEN  70
            WHEN 'Sarrià-Sant Gervasi'  THEN  65
            WHEN 'Horta-Guinardó'       THEN  50
            WHEN 'Sant Andreu'          THEN  65
            WHEN 'Nou Barris'           THEN  40
            ELSE  70
        END + (hashtext(z.id || 'neg') % 40)
    )::numeric, 0)) AS num_negocios_activos,

    -- % locales vacíos
    GREATEST(0.02, LEAST(0.35, ROUND((
        CASE d.nombre
            WHEN 'Nou Barris'           THEN 0.22
            WHEN 'Horta-Guinardó'       THEN 0.18
            WHEN 'Sant Andreu'          THEN 0.16
            WHEN 'Sants-Montjuïc'       THEN 0.15
            WHEN 'Sant Martí'           THEN 0.13
            WHEN 'Ciutat Vella'         THEN 0.12
            WHEN 'Gràcia'               THEN 0.10
            WHEN 'Les Corts'            THEN 0.09
            WHEN 'Eixample'             THEN 0.08
            WHEN 'Sarrià-Sant Gervasi'  THEN 0.07
            ELSE 0.13
        END + ((hashtext(z.id || 'vac') % 10) - 5) * 0.01
    )::numeric, 2))) AS pct_locales_vacios,

    -- Tasa rotación anual
    GREATEST(0.05, LEAST(0.45, ROUND((
        CASE d.nombre
            WHEN 'Nou Barris'           THEN 0.32
            WHEN 'Horta-Guinardó'       THEN 0.28
            WHEN 'Sants-Montjuïc'       THEN 0.25
            WHEN 'Sant Andreu'          THEN 0.24
            WHEN 'Sant Martí'           THEN 0.22
            WHEN 'Ciutat Vella'         THEN 0.25
            WHEN 'Gràcia'               THEN 0.18
            WHEN 'Les Corts'            THEN 0.16
            WHEN 'Eixample'             THEN 0.15
            WHEN 'Sarrià-Sant Gervasi'  THEN 0.12
            ELSE 0.20
        END + ((hashtext(z.id || 'rot') % 10) - 5) * 0.01
    )::numeric, 2))) AS tasa_rotacion_anual,

    -- Ratio locales comerciales
    GREATEST(0.10, LEAST(0.90, ROUND((
        CASE d.nombre
            WHEN 'Eixample'             THEN 0.72
            WHEN 'Ciutat Vella'         THEN 0.68
            WHEN 'Gràcia'               THEN 0.58
            WHEN 'Sant Martí'           THEN 0.50
            WHEN 'Sants-Montjuïc'       THEN 0.45
            WHEN 'Les Corts'            THEN 0.42
            WHEN 'Sarrià-Sant Gervasi'  THEN 0.38
            WHEN 'Horta-Guinardó'       THEN 0.35
            WHEN 'Sant Andreu'          THEN 0.42
            WHEN 'Nou Barris'           THEN 0.28
            ELSE 0.45
        END + ((hashtext(z.id || 'ratio') % 10) - 5) * 0.01
    )::numeric, 2))) AS ratio_locales_comerciales,

    -- Score turismo (0-100)
    GREATEST(5, LEAST(100, ROUND((
        CASE d.nombre
            WHEN 'Ciutat Vella'         THEN 90
            WHEN 'Eixample'             THEN 65
            WHEN 'Gràcia'               THEN 45
            WHEN 'Sant Martí'           THEN 35
            WHEN 'Sants-Montjuïc'       THEN 40
            WHEN 'Les Corts'            THEN 25
            WHEN 'Sarrià-Sant Gervasi'  THEN 20
            WHEN 'Horta-Guinardó'       THEN 15
            WHEN 'Sant Andreu'          THEN 20
            WHEN 'Nou Barris'           THEN 10
            ELSE 25
        END + (hashtext(z.id || 'tur') % 20) - 10
    )::numeric, 0))) AS score_turismo,

    -- Incidencias por 1000 hab
    GREATEST(1, ROUND((
        CASE d.nombre
            WHEN 'Ciutat Vella'         THEN 65
            WHEN 'Nou Barris'           THEN 42
            WHEN 'Sants-Montjuïc'       THEN 35
            WHEN 'Sant Andreu'          THEN 30
            WHEN 'Horta-Guinardó'       THEN 28
            WHEN 'Sant Martí'           THEN 26
            WHEN 'Gràcia'               THEN 22
            WHEN 'Eixample'             THEN 24
            WHEN 'Les Corts'            THEN 18
            WHEN 'Sarrià-Sant Gervasi'  THEN 14
            ELSE 28
        END + (hashtext(z.id || 'inc') % 15) - 7
    )::numeric, 1)) AS incidencias_por_1000hab,

    -- Nivel de ruido (dB)
    GREATEST(45, LEAST(82, ROUND((
        CASE d.nombre
            WHEN 'Ciutat Vella'         THEN 72
            WHEN 'Eixample'             THEN 68
            WHEN 'Gràcia'               THEN 62
            WHEN 'Sant Martí'           THEN 60
            WHEN 'Sants-Montjuïc'       THEN 60
            WHEN 'Les Corts'            THEN 58
            WHEN 'Sarrià-Sant Gervasi'  THEN 52
            WHEN 'Horta-Guinardó'       THEN 56
            WHEN 'Sant Andreu'          THEN 61
            WHEN 'Nou Barris'           THEN 62
            ELSE 61
        END + (hashtext(z.id || 'ruido') % 8) - 4
    )::numeric, 1))) AS nivel_ruido_db,

    -- m² zonas verdes cercanas
    GREATEST(0, ROUND((
        CASE d.nombre
            WHEN 'Sarrià-Sant Gervasi'  THEN 12000
            WHEN 'Les Corts'            THEN  8000
            WHEN 'Horta-Guinardó'       THEN  9000
            WHEN 'Gràcia'               THEN  3500
            WHEN 'Sants-Montjuïc'       THEN  6000
            WHEN 'Sant Martí'           THEN  4000
            WHEN 'Eixample'             THEN  2500
            WHEN 'Sant Andreu'          THEN  3000
            WHEN 'Nou Barris'           THEN  2800
            WHEN 'Ciutat Vella'         THEN  1800
            ELSE  3500
        END + (hashtext(z.id || 'verde') % 2000)
    )::numeric, 0)) AS m2_zonas_verdes_cercanas,

    -- Score equipamientos (0-100)
    GREATEST(20, LEAST(100, ROUND((
        CASE d.nombre
            WHEN 'Eixample'             THEN 78
            WHEN 'Gràcia'               THEN 72
            WHEN 'Ciutat Vella'         THEN 70
            WHEN 'Sarrià-Sant Gervasi'  THEN 65
            WHEN 'Sant Martí'           THEN 62
            WHEN 'Les Corts'            THEN 60
            WHEN 'Sants-Montjuïc'       THEN 58
            WHEN 'Sant Andreu'          THEN 55
            WHEN 'Horta-Guinardó'       THEN 52
            WHEN 'Nou Barris'           THEN 45
            ELSE 58
        END + (hashtext(z.id || 'equip') % 15) - 7
    )::numeric, 0))) AS score_equipamientos,

    'seed_barcelona_v1' AS fuente

FROM zonas z
JOIN barrios b ON b.id = z.barrio_id
JOIN distritos d ON d.id = b.distrito_id
ON CONFLICT (zona_id, fecha) DO NOTHING;
