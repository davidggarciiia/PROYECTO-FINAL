-- =============================================================================
--  Migración 036: Normalización de `sectores.peso_*` a suma=1.0
-- =============================================================================
--
--  Contexto
--  --------
--  Las migraciones 020 (`seguridad_v2`) y 021 (`entorno_v2`) ajustaron varios
--  `peso_*` por sector sin recomputar el resto → la suma dejó de ser 1.0 en los
--  5 sectores base. Consecuencias:
--    - `_recalcular_global()` en scoring/motor.py aplica los pesos sobre las
--      dimensiones sin renormalizar (solo las reglas de concepto/perfil renormalizan).
--    - Sectores con suma>1.0 inflaban el score_base; los de suma<1.0 lo
--      deflactaban. Bias sistemático por sector, invisible en tests.
--
--  Estado POSTERIOR a 020+021 (antes de esta migración):
--    restauracion  = 1.03 (sobra 0.03)
--    tatuajes      = 0.98 (falta 0.02)
--    moda          = 1.04 (sobra 0.04)
--    estetica      = 1.02 (sobra 0.02)
--    shisha_lounge = 0.95 (falta 0.05)
--
--  Estrategia
--  ----------
--  Re-fijar los 8 `peso_*` por sector con valores que suman exactamente 1.00
--  y preservan la intención de 020 (reforzar seguridad en alto riesgo) y de
--  021 (reforzar entorno y reducir turismo).
--
--  Las ideas cualitativas son las mismas; solo cambian los decimales para
--  cerrar el reparto. Los cambios son del orden de 0.01–0.03 por columna.
-- =============================================================================

BEGIN;

-- restauracion: era 1.03, bajamos precio (0.14→0.13) y entorno (0.10→0.10 igual, -0.02 aquí),
-- reduciendo precio -0.01 y entorno -0.02 cuadra. Preferimos bajar precio en -0.03.
UPDATE sectores SET
    peso_flujo       = 0.23,
    peso_demo        = 0.18,
    peso_competencia = 0.18,
    peso_precio      = 0.11,
    peso_transporte  = 0.09,
    peso_seguridad   = 0.08,
    peso_turismo     = 0.03,
    peso_entorno     = 0.10
WHERE codigo = 'restauracion';

-- tatuajes: era 0.98, subimos flujo +0.01 y demografía +0.01.
UPDATE sectores SET
    peso_flujo       = 0.19,
    peso_demo        = 0.21,
    peso_competencia = 0.15,
    peso_precio      = 0.15,
    peso_transporte  = 0.09,
    peso_seguridad   = 0.10,
    peso_turismo     = 0.03,
    peso_entorno     = 0.08
WHERE codigo = 'tatuajes';

-- moda: era 1.04, bajamos flujo -0.02 y precio -0.02.
UPDATE sectores SET
    peso_flujo       = 0.23,
    peso_demo        = 0.18,
    peso_competencia = 0.15,
    peso_precio      = 0.15,
    peso_transporte  = 0.09,
    peso_seguridad   = 0.07,
    peso_turismo     = 0.03,
    peso_entorno     = 0.10
WHERE codigo = 'moda';

-- estetica: era 1.02, bajamos demografía -0.02 (22→20).
UPDATE sectores SET
    peso_flujo       = 0.16,
    peso_demo        = 0.20,
    peso_competencia = 0.18,
    peso_precio      = 0.16,
    peso_transporte  = 0.09,
    peso_seguridad   = 0.08,
    peso_turismo     = 0.03,
    peso_entorno     = 0.10
WHERE codigo = 'estetica';

-- shisha_lounge: era 0.95, subimos flujo +0.02, transporte +0.02 y entorno +0.01.
UPDATE sectores SET
    peso_flujo       = 0.16,
    peso_demo        = 0.20,
    peso_competencia = 0.15,
    peso_precio      = 0.14,
    peso_transporte  = 0.11,
    peso_seguridad   = 0.12,
    peso_turismo     = 0.03,
    peso_entorno     = 0.09
WHERE codigo = 'shisha_lounge';

-- Verificación dura: si algún sector se queda fuera de [0.99, 1.01], aborta.
DO $$
DECLARE
    r RECORD;
    suma NUMERIC;
BEGIN
    FOR r IN SELECT codigo,
                    COALESCE(peso_flujo,0) + COALESCE(peso_demo,0) + COALESCE(peso_competencia,0)
                  + COALESCE(peso_precio,0) + COALESCE(peso_transporte,0) + COALESCE(peso_seguridad,0)
                  + COALESCE(peso_turismo,0) + COALESCE(peso_entorno,0) AS s
             FROM sectores
    LOOP
        suma := r.s;
        IF ABS(suma - 1.0) > 0.01 THEN
            RAISE EXCEPTION 'Sector % tiene pesos que no suman 1.0 (suma=%).', r.codigo, suma;
        END IF;
    END LOOP;
END
$$;

COMMIT;
