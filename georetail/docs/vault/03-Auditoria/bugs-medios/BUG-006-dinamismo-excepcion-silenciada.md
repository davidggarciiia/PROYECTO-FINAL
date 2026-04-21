# BUG-006: Errores del módulo dinamismo silenciados en fallback

**Fecha:** 2026-04-21  
**Severidad:** MEDIA  
**Estado:** Resuelto (2026-04-21)  
**Archivo:** `scoring/motor.py:357-366`  

---

## Descripción

El `except Exception` atrapa cualquier error de `calcular_dinamismo`, incluyendo `KeyError` de retorno incorrecto del módulo. Los errores se silencian y se aplica un fallback que multiplica por 10.0, lo que puede ser incorrecto.

## Fix propuesto

Capturar solo `ImportError`/`ModuleNotFoundError` para el fallback de módulo faltante, y re-lanzar otros errores.

---

**Tags:** #bug #medio #scoring #dinamismo #excepcion
