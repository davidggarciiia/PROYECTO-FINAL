# BUG-P1-1: Pesos por sector NO suman 1.0 tras migraciones 020 + 021

**Fecha:** 2026-04-22
**Severidad:** ALTA (bias silencioso en score_global de todos los sectores)
**Estado:** Resuelto
**Archivos afectados:**
- `georetail/backend/db/migraciones/020_seguridad_v2.sql:148-152`
- `georetail/backend/db/migraciones/021_entorno_v2.sql:128-132`
- `georetail/backend/scoring/motor.py` (añadido `verificar_pesos_sectores()`)
- `georetail/backend/main.py` (chequeo al arrancar)
- `georetail/backend/db/migraciones/036_fix_pesos_sectores.sql` (fix)

---

## Descripción

El seed inicial de `sectores` (migración 001) crea 5 filas con la invariante
`peso_flujo + peso_demo + peso_competencia + peso_precio + peso_transporte +
peso_seguridad + peso_turismo + peso_entorno = 1.00` para cada sector.

Las migraciones 020 (`seguridad_v2`) y 021 (`entorno_v2`) **actualizaron algunos
`peso_*` por sector sin recomputar el resto** → la suma dejó de ser 1.0:

| Sector        | Tras 001 | Tras 020 | Tras 021 | Delta vs 1.0 |
|---------------|:--------:|:--------:|:--------:|:------------:|
| restauracion  | 1.00     | 1.00     | **1.03** | +0.03        |
| tatuajes      | 1.00     | 1.00     | **0.98** | −0.02        |
| moda          | 1.00     | 1.00     | **1.04** | +0.04        |
| estetica      | 1.00     | 1.00     | **1.02** | +0.02        |
| shisha_lounge | 1.00     | 1.00     | **0.95** | −0.05        |

## Impacto

`scoring/motor.py:_recalcular_global()` aplica cada `peso_*` directamente sobre
la dimensión correspondiente sin renormalizar. Consecuencia:

- **Sectores con suma>1.0** (`restauracion`, `moda`, `estetica`): el
  `score_global` está **inflado** respecto del ranking intencionado.
- **Sectores con suma<1.0** (`tatuajes`, `shisha_lounge`): el `score_global`
  está **deflactado**; zonas con todas las dimensiones altas nunca pasan
  de ~95.

El sesgo es sistemático por sector, no lo detectan tests unitarios (las
pruebas usan fixtures de pesos fijos), y el frontend no lo indica porque
muestra el valor resultante sin referencia al "100% ideal".

Las reglas de modulación sí normalizan (`_normalizar_pesos` en motor.py),
pero solo para el path con `concepto_negocio` o `perfil_refinado`. El
path base (sin modulación, que es el del batch inicial del mapa) sufre
el bias.

## Causa raíz

Hábito de tocar solo los pesos "afectados" por una feature nueva sin
auditar que la suma total se mantiene. No había invariante declarado
en SQL ni un chequeo al arrancar.

## Fix aplicado

### 1. Chequeo de invariante al arrancar
Añadida `verificar_pesos_sectores()` en `scoring/motor.py`:
lee todas las filas de `sectores`, calcula la suma de pesos y loguea
ERROR si algún sector excede la tolerancia `±0.01`. Se llama desde el
`lifespan` de `main.py` tras `init_db_pool()`. No bloquea el arranque
para no romper despliegues en caliente.

### 2. Migración de fix
`036_fix_pesos_sectores.sql` re-fija los 8 `peso_*` por sector con
valores que suman exactamente 1.00 y preservan la intención cualitativa
de 020 (reforzar seguridad) y 021 (reforzar entorno). Los cambios son
del orden de 0.01–0.03 por columna.

Al final incluye un `DO $$ ... $$` que re-verifica en SQL y aborta si
algún sector se queda fuera de tolerancia (defensa en profundidad).

## Verificación

```sql
-- Ejecutar tras aplicar la migración. Todos los deltas deben ser 0.00.
SELECT codigo,
       ROUND(
         COALESCE(peso_flujo,0) + COALESCE(peso_demo,0) + COALESCE(peso_competencia,0)
       + COALESCE(peso_precio,0) + COALESCE(peso_transporte,0) + COALESCE(peso_seguridad,0)
       + COALESCE(peso_turismo,0) + COALESCE(peso_entorno,0)
       , 4) AS suma_pesos
FROM sectores
ORDER BY codigo;
```

## Notas

- Los `scores_zona` cacheados antes de la migración seguirán reflejando el
  bias hasta que el pipeline semanal (`pipelines/scores.py`) los recalcule
  con los pesos correctos. Acelerar con trigger manual si hace falta:
  `POST /api/admin/pipelines/scores` (admin key).
- Si alguien añade nuevas migraciones que modifican `peso_*`, debe
  ejecutar el bloque `DO $$ ... $$` del fix como auto-check antes del
  `COMMIT`, o el chequeo al arrancar lo detectará en el próximo despliegue.

---

**Tags:** #bug #alta #scoring #pesos #sectores #invariante #p1
