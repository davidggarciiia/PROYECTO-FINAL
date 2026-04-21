# BUG-006: Except silencioso en calcular_dinamismo (motor.py)

**Fecha:** 2026-04-21
**Severidad:** MEDIA
**Estado:** Resuelto (2026-04-21)
**Archivo:** `scoring/motor.py:357-366`

---

## Descripcion

El bloque `except Exception` que rodea la llamada a `calcular_dinamismo` en
`get_scores_zona` no registraba ningun mensaje de log. Cualquier error real
(fallo de BD, columna inexistente, importacion rota, etc.) quedaba silenciado:
el sistema aplicaba el fallback de escalado `* 10.0` sin dejar rastro en los
logs, haciendo imposible detectar y diagnosticar el problema.

## Fix aplicado

Se cambio la clausula de:

```python
except Exception:
    if raw.get("score_dinamismo") is not None:
        raw["score_dinamismo"] = round(float(raw["score_dinamismo"]) * 10.0, 1)
```

a:

```python
except Exception as exc:  # BUG-006 fix: log error instead of silently swallowing it
    logger.warning(
        "calcular_dinamismo failed for zona_id=%s, falling back to raw scale: %s",
        raw.get("zona_id"),
        exc,
    )
    if raw.get("score_dinamismo") is not None:
        raw["score_dinamismo"] = round(float(raw["score_dinamismo"]) * 10.0, 1)
```

El fallback de escalado se conserva (no se re-lanza la excepcion) para
mantener la resiliencia del endpoint, pero el error ahora queda visible en
los logs de nivel WARNING con el `zona_id` afectado y el mensaje de excepcion.

## Por que WARNING y no ERROR

- El endpoint puede seguir respondiendo con el valor de fallback.
- ERROR se reserva para situaciones en las que el sistema no puede continuar.
- WARNING es suficiente para alertar a quien revise los logs sin generar
  falsa alarma de criticidad.

---

**Tags:** #bug #medio #scoring #dinamismo #excepcion #logging
