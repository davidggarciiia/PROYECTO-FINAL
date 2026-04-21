# BUG-004: Actualizaciones de sesión se pierden silenciosamente en error Redis

**Fecha:** 2026-04-21  
**Severidad:** CRITICA  
**Estado:** Abierto  
**Archivo:** `db/sesiones.py:66-90`  

---

## Descripción

```python
# CÓDIGO ACTUAL:
s = None
for _attempt in range(3):
    try:
        # ... s se asigna aquí
    except WatchError:
        continue
if s is None:
    return  # ← retorna silenciosamente sin actualizar la sesión
```

Si los 3 intentos fallan por una excepción distinta a `WatchError` (p.ej. error de conexión, timeout), el bucle hace `break`, `s = None` y se retorna sin actualizar. El usuario no recibe ninguna notificación.

## Impacto

El usuario pierde cambios de sesión (perfil, zonas seleccionadas, configuración) sin saber que ha ocurrido. Especialmente crítico en flujos de cuestionario adaptativo donde el estado se guarda en sesión.

## Fix propuesto

Loguear el error con nivel ERROR y/o lanzar una excepción manejable que el endpoint pueda capturar y retornar 500.

---

**Tags:** #bug #critico #db #sesiones #redis #silentfailure
