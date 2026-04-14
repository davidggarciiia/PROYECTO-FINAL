# Prioridad 1 — Auditoría de datos, modelo y competencia

## Estado

Esta es la **Prioridad 1** del siguiente ciclo de trabajo y el siguiente Codex debe arrancar por aquí.

- Rama objetivo de trabajo: `modelo-mejorado-2`
- Bloqueador antes de seguir refinando ranking, explicaciones UI o narrativa
- No asumir que una feature "entra" solo porque exista en `FEATURE_NAMES`

## Objetivo inmediato

Verificar de punta a punta qué datos:

- se escriben realmente en pipelines y tablas/vistas
- se leen online en scorer y detalle
- se reconstruyen históricamente para train/evaluate
- entran al modelo activo por gobernanza
- se quedan imputados, neutralizados o con semántica mezclada

Además, revisar competencia como bloque específico porque puede estar bien “funcionando” a nivel de tests básicos pero mal cableada entre v1, v2, radios y significado de score.

## Alcance

La auditoría es **end-to-end**:

- pipelines
- tablas y vistas
- builders de features online
- dataset histórico de train
- scorer
- detalle/API
- endpoint de competencia

## Hallazgos ya detectados y que hay que resolver primero

### 1. Desalineación train vs online

Hay features activas o visibles que no se reconstruyen igual en histórico que en inferencia.

Casos ya detectados:

- `flujo_peatonal_score` entra online, pero en histórico no se construye de verdad y puede acabar imputado
- `google_review_count_medio` tiene consulta de cálculo, pero hoy se neutraliza en los builders principales
- `delta_renta_3a` convive con `renta_variacion_3a` y hay que unificar su contrato

### 2. Competencia v1/v2 mezclada

Competencia necesita saneamiento explícito:

- `score_competencia_v2` significa score positivo de competencia
- `score_saturacion` es la señal legacy invertida
- hoy varios caminos renombran o reutilizan una como si fuera la otra

Además hay un posible desfase de radio:

- el pipeline v2 escribe compatibilidad en `competencia_por_local` con `radio_m=500`
- parte del scorer y del dataset siguen leyendo fallback v1 con `radio_m=300`

### 3. Gobernanza vs bug real

No todo lo que falta es un bug.

Hay features fuera por diseño y otras fuera por error. La auditoría tiene que separarlas bien:

- fuera por gobernanza
- fuera por falta de historificación
- fuera por bug de lectura/escritura
- fuera por semántica inconsistente

## Resultado esperado

La salida de esta prioridad debe dejar:

1. una matriz real por feature con fuente, escritura, lectura online, reconstrucción histórica y estado efectivo
2. una decisión clara para cada feature activa: mantener, recablear, historificar o sacar del modelo
3. una unificación de competencia con fuente canónica, radio canónico y semántica estable
4. validaciones automáticas para que una feature activa no pueda colarse al modelo si no entra realmente
5. si cambia la señal efectiva, dejar previsto retraining y recálculo de scores

## Criterio operativo para el siguiente Codex

El siguiente trabajo debe empezar por:

1. auditar `features.py`, `dataset.py`, `scorer.py`, `db/zonas.py` y el pipeline de competencia
2. comprobar una a una las features activas del modelo actual
3. corregir primero desalineaciones de datos antes de seguir con mejoras visuales o de ranking

Mientras esta prioridad no quede cerrada:

- no dar por bueno que el modelo “mira todo”
- no seguir construyendo narrativa encima de datos dudosos
- no usar competencia como base fiable sin cerrar su contrato v1/v2
