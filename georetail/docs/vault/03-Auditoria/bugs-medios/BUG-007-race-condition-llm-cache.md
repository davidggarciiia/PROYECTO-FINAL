# BUG-007: Race condition en caché LLM de perfil refinado

**Fecha:** 2026-04-21
**Severidad:** MEDIA
**Estado:** Resuelto (2026-04-21)
**Archivo:** `api/buscar.py:202-204`

---

## Descripcion

Dos requests HTTP concurrentes con la misma descripcion de negocio podian llegar
al bloque de refinamiento (lineas 202-204) con `cache_prev` vacio, saltarse la
guarda `if not perfil_refinado_dict:` y ambas invocar `refinar()` (LLM) en
paralelo. Esto generaba dos llamadas al LLM identicas, coste de API duplicado y
posibles escrituras de sesion conflictivas.

Patron original:

```python
cache_prev = (sesion.get("perfil") or {}).get("perfil_refinado_cache") or {}
perfil_refinado_dict = cache_prev.get(descripcion_hash) or {}
if not perfil_refinado_dict:
    perfil_refinado_model = await refinar(...)   # ambas requests llegan aqui
    perfil_refinado_dict = perfil_refinado_model.model_dump()
```

El cache `cache_prev` es un dict en memoria de sesion (no Redis): no proporciona
coordinacion entre workers/procesos distintos de uvicorn.

---

## Causa raiz

Ausencia de mecanismo de exclusion mutua distribuida antes de la llamada al LLM.
El cache de sesion solo protege al segundo request del *mismo* worker si la
sesion ya fue actualizada, pero no en escenarios concurrentes reales.

---

## Fix aplicado

Se introdujo un **distributed lock Redis** (`SET NX EX 30`) sobre la clave
`perfil_refinado:{descripcion_hash}` antes de llamar al LLM:

1. Ambos requests comprueban primero la clave Redis `perfil_refinado:{hash}`.
2. Solo el primero en obtener el lock (`SET NX EX 30`) llama al LLM.
3. El lock holder escribe el resultado en Redis (`SET EX 3600`) antes de liberar
   el lock.
4. El waiter espera 1 segundo y lee el resultado ya calculado desde Redis.
5. Si el lock holder fallo y no escribio nada, el waiter hace una llamada LLM de
   fallback para garantizar que la respuesta nunca queda vacia.

Archivos modificados:

- `backend/api/buscar.py` — importado `asyncio` y `db.redis_client.get_redis`;
  reemplazado el bloque de refinamiento (lineas 200-219) con logica de lock
  distribuido.

---

## Impacto

- **Antes:** hasta N llamadas LLM duplicadas por N requests concurrentes.
- **Despues:** exactamente 1 llamada LLM por descripcion unica; resultado
  compartido entre requests y entre sesiones via Redis (TTL 1 hora).

---

**Tags:** #bug #medio #resuelto #api #buscar #cache #LLM #concurrencia #redis
