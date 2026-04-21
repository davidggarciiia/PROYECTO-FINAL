# BUG-004: Excepcion Redis no-WatchError silenciada sin fallback a PostgreSQL

**Fecha:** 2026-04-21
**Severidad:** CRITICA
**Estado:** Resuelto (2026-04-21)
**Archivo:** `db/sesiones.py:66-90`

---

## Descripcion del bug

En `actualizar_sesion()`, el bloque `except Exception` del bucle de reintentos Redis
(lineas 88-90 originales) capturaba cualquier excepcion que no fuera `WatchError`,
logaba un warning con nivel bajo y hacia `break` sin ejecutar ningun fallback.

```python
# CODIGO ORIGINAL (antes del fix):
        except WatchError:
            continue
        except Exception as e:
            logger.warning("Redis actualizar_sesion fail: %s", e)
            break   # <-- sale del bucle; s puede seguir siendo None

if s is None:
    return          # <-- retorna silenciosamente; la sesion NO se actualiza
```

El problema tiene dos variantes segun el momento en que falle Redis:

1. **Fallo antes de `s = json.loads(raw)`** (p.ej. timeout en `pipe.watch()`):
   `s` permanece `None`. La guarda `if s is None: return` en linea 92 salta
   directamente el bloque de escritura en PostgreSQL. El update se pierde por
   completo sin ningun registro de nivel ERROR.

2. **Fallo despues de parsear `s` pero antes de `pipe.execute()`** (p.ej. error
   de escritura Redis): `s` contiene los datos mergeados pero la escritura Redis
   no se completó. El flujo llegaba al bloque PG y sincronizaba el perfil correcto,
   pero solo si el error se producía después de la asignación de `s`. Esta
   condición de carrera hacía el comportamiento impredecible.

## Impacto concreto

Cuando `/api/buscar` escribe `perfil.sector` en la sesion via `actualizar_sesion()`
y Redis falla en ese momento, la sesion queda sin sector. La llamada posterior a
`GET /api/competencia/{zona_id}` lee la sesion, no encuentra `perfil.sector`, y
retorna **HTTP 400** ("sector no especificado"), rompiendo el flujo de usuario.

## Fix aplicado

Se reemplazo el `except Exception` simple por un bloque que, ante cualquier error
Redis no-WatchError, ejecuta inmediatamente un fallback directo a PostgreSQL:

```python
        except WatchError:
            continue
        except Exception as e:
            logger.warning("Redis actualizar_sesion fail (intento %d): %s", _attempt, e)
            # No silenciar el error — hacer fallback directo a PostgreSQL
            # con los updates recibidos, sin depender de `s`.
            try:
                async with get_db() as conn:
                    row = await conn.fetchrow(
                        "SELECT perfil FROM sesiones WHERE id=$1", session_id)
                    if row is not None:
                        perfil_actual = row["perfil"] or {}
                        perfil_update = updates.get("perfil", {})
                        if isinstance(perfil_update, dict) and isinstance(perfil_actual, dict):
                            perfil_merged = {**perfil_actual, **perfil_update}
                        else:
                            perfil_merged = perfil_update if perfil_update else perfil_actual
                        await conn.execute(
                            "UPDATE sesiones SET perfil=$1, updated_at=NOW() WHERE id=$2",
                            json.dumps(perfil_merged), session_id)
                        logger.info(
                            "actualizar_sesion: fallback PG exitoso para sesion %s", session_id)
            except Exception as pg_err:
                logger.error(
                    "actualizar_sesion: fallback PG tambien fallo para sesion %s: %s",
                    session_id, pg_err)
            return
```

### Detalles del fix

- El fallback lee primero el perfil actual de PG y hace merge con `updates["perfil"]`,
  replicando exactamente la logica del path Redis (lineas 77-79 del codigo original).
- Si el fallback PG tambien falla, se registra con nivel `ERROR` (no `WARNING`)
  para que sea visible en alertas de monitoring.
- Se usa `return` inmediato tras el fallback para no ejecutar el bloque PG duplicado
  al final de la funcion (el que sincroniza desde `s`).
- El caso `s is None` al final sigue siendo necesario para cubrir el caso en que
  los 3 reintentos fallen todos por WatchError (race condition alta carga).

## Archivos modificados

- `georetail/backend/db/sesiones.py` — funcion `actualizar_sesion`, lineas 86-100

---

**Tags:** #bug #critico #db #sesiones #redis #silentfailure #fallback #postgresql
