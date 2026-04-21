# BUG-P0-4: Double-encoding JSON en columnas JSONB

**Fecha:** 2026-04-22
**Severidad:** CRITICA
**Estado:** Resuelto
**Archivos afectados:**
- `georetail/backend/db/sesiones.py:33, 106, 121, 133`
- `georetail/backend/db/financiero.py:70, 80`
- `georetail/backend/scoring/scorer.py:607`
- `georetail/backend/scoring/ml/train.py:334-338`
- `georetail/backend/pipelines/comercio/competencia_scrape.py:195`

---

## Descripción

`db/conexion.py` registra un **type codec de asyncpg** para `jsonb` que ya llama
a `json.dumps` internamente (líneas 40-46):

```python
await conn.set_type_codec(
    "jsonb",
    encoder=lambda v: json.dumps(v, ensure_ascii=False),
    decoder=json.loads,
    schema="pg_catalog",
    format="text",
)
```

Sin embargo, múltiples queries a columnas JSONB pasaban ya un **string JSON**
(`json.dumps(...)`) como parámetro. El codec volvía a invocar `json.dumps` sobre
ese string → la columna acababa con un string JSON escapado (`"{\"a\":1}"`) en
lugar de un objeto JSON real (`{"a": 1}`).

## Impacto

Cuando se leía la columna con el decoder (`json.loads`) se obtenía un **string**
en lugar de un **dict/list**, con lo que:

- **`sesiones.py`** — `row["perfil"]` era string → `{**s["perfil"], **v}` fallaba
  silenciosamente o perdía datos. Sesiones con perfiles corruptos desde PG cuando
  el path Redis caía.
- **`busquedas`** — `filtros` y `perfil_negocio` inutilizables para analytics
  (cualquier query `WHERE perfil_negocio->>'sector' = ...` devolvía 0 filas).
- **`analisis_financieros.params` / `.proyeccion_json`** — el frontend recibía
  strings escapados cuando cacheaba resultados.
- **`scores_zona.shap_values`** — los componentes de explicabilidad no se podían
  indexar por clave en PG (los `->` y `->>` no operan sobre strings).
- **`modelos_versiones.params/metricas/importancia_features/demografia_feature_names`**
  — queries de analítica sobre modelos ML imposibles de escribir.
- **`negocios_activos.horario`** — filtros por horario de apertura rotos.

## Causa raíz

Patrón copy-paste de una época anterior al registro del codec JSONB; nadie
migró los call sites al pasar a codec central.

## Fix aplicado

Pasar **dicts/listas Python directamente** en cada INSERT/UPDATE a JSONB.
Comentarios añadidos en cada call site para prevenir reintroducción.

### Excepciones intencionadas (no tocar)

- **`pipelines/entorno/google_maps.py:226`** — usa cast explícito `$6::jsonb` en
  la query. Con cast explícito, asyncpg trata el parámetro como TEXT (no aplica
  el codec jsonb), por lo que **sí** hay que pasar `json.dumps(...)`. Caso
  válido.
- **Escrituras a Redis** (`r.setex(..., json.dumps(s), ...)`): Redis guarda
  strings, `json.dumps` correcto.
- **Archivos en disco** (`path.write_text(json.dumps(...))`) y **prompts LLM**:
  correcto.

## Verificación

### Búsqueda global
```bash
grep -rn "json\.dumps" georetail/backend --include="*.py"
```

Revisar que cada match restante corresponde a:
- Redis (setex/set/get)
- stdout (print)
- Files on disk
- LLM prompts
- Queries con cast `::jsonb`
- Tests (fixtures)

### Prueba de regresión (propuesta para Sprint 2)

```python
async def test_crear_sesion_perfil_es_dict_en_pg():
    session_id = str(uuid4())
    perfil = {"sector": "restauracion", "m2": 80}
    await crear_sesion(session_id, {"perfil": perfil}, "ip")
    async with get_db() as conn:
        row = await conn.fetchrow("SELECT perfil FROM sesiones WHERE id=$1", session_id)
    assert isinstance(row["perfil"], dict)        # ANTES: str
    assert row["perfil"] == perfil                # ANTES: '{"sector":"restauracion","m2":80}'
```

## Notas

- Los registros ya corruptos en producción (si los hay) se pueden sanear con
  una migración one-off:
  ```sql
  UPDATE sesiones SET perfil = (perfil::text)::jsonb
  WHERE jsonb_typeof(perfil) = 'string';
  ```
  (revisa primero con `jsonb_typeof` antes de ejecutar en prod).

---

**Tags:** #bug #critico #persistencia #jsonb #asyncpg #p0
