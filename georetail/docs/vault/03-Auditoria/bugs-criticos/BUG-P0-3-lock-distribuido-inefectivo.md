# BUG-P0-3: Lock distribuido inefectivo en `/api/buscar`

**Fecha:** 2026-04-22
**Severidad:** CRITICA
**Estado:** Resuelto
**Archivo:** `georetail/backend/api/buscar.py:253-275`

---

## Descripción

El endpoint `/api/buscar` usaba un lock Redis (`SET NX EX 30`) para evitar que
dos requests concurrentes con la misma `descripcion` dispararan dos llamadas al
LLM refinador. Cuando una request **no** conseguía el lock, esperaba sólo
`asyncio.sleep(1)` antes de leer el cache y, si no estaba, caía al fallback que
**volvía a llamar al LLM**.

El LLM de refinamiento tarda entre **2 y 5 segundos** → el sleep de 1s era
siempre insuficiente → todos los waiters acababan invocando el LLM en paralelo.
El lock no protegía nada: con **N** requests simultáneas, había **N** llamadas
al LLM (×2 coste).

## Impacto

- **Coste LLM ×N** en ráfagas de tráfico con misma query (campañas, enlaces
  directos compartidos).
- **Latencia** de los waiters inflada (1s sleep + llamada LLM propia = 3-6s
  extra).
- **Inconsistencia**: cada waiter podía obtener un `perfil_refinado`
  ligeramente distinto (el LLM no es determinista al 100%).

## Reproducción

```bash
# 3 curls en paralelo a la misma descripción
for i in 1 2 3; do
  curl -s -X POST http://localhost:8000/api/buscar \
    -H "Content-Type: application/json" \
    -d '{"descripcion":"cafetería de especialidad en barrio de diseño"}' &
done
wait
# Revisar logs: aparecían 3 llamadas al refinador, no 1.
```

## Causa raíz

`asyncio.sleep(1)` < tiempo medio del LLM. El check post-sleep encontraba el
cache vacío y caía al fallback silenciosamente.

## Fix aplicado

Polling con back-off hasta el TTL del lock (30s):

```python
_POLL_INTERVAL_S = 0.25
_POLL_MAX_S = 30.0
waited = 0.0
while waited < _POLL_MAX_S:
    await asyncio.sleep(_POLL_INTERVAL_S)
    waited += _POLL_INTERVAL_S
    cached_raw = await redis.get(redis_cache_key)
    if cached_raw:
        # parsear y salir
        ...
    # Early exit: lock ya no existe y tampoco hay cache → el holder falló
    lock_still_held = await redis.get(redis_lock_key)
    if not lock_still_held and not cached_raw:
        break
```

- **Intervalo:** 0.25s (120 checks en 30s — carga Redis trivial, ~4 GET/s).
- **Early exit:** si el lock expiró/fue borrado y aún no hay cache, salimos
  inmediatamente para no quedarnos los 30s completos esperando a un holder
  muerto. El fallback LLM sólo se invoca si todos los intentos vacíos.
- **Fallback preservado:** si tras los 30s no hay cache (escenario extremo de
  LLM superlenta o Redis flapping), la request hace su propia llamada.

## Verificación

Diseño del test (a implementar en Sprint 2):

```python
async def test_lock_impide_duplicar_llamadas_llm():
    # Monkeypatch: refinar() tarda 3s y cuenta invocaciones
    contador = 0
    async def refinar_lento(**kw):
        nonlocal contador
        contador += 1
        await asyncio.sleep(3)
        return PerfilRefinado()
    # 3 requests en paralelo
    await asyncio.gather(*(buscar(...) for _ in range(3)))
    assert contador == 1  # ANTES: 3 · AHORA: 1
```

## Notas

- Alternativa descartada: **pub/sub de Redis** (publicar cuando el holder
  termina). Más elegante pero añade complejidad; polling a 0.25s es
  suficientemente eficiente para una query humana.
- El TTL del lock (30s) ya protege contra deadlocks si el holder se muere: el
  lock se libera automáticamente y el siguiente waiter entra a la rama
  `got_lock`.

---

**Tags:** #bug #critico #concurrencia #llm #redis #p0
