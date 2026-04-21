# BUG-003: Score de supervivencia 0.0 ignorado por fallback con "or"

**Fecha:** 2026-04-21  
**Severidad:** CRITICA  
**Estado:** ✅ Resuelto (2026-04-21)  
**Archivo:** `api/buscar.py:98-99` y `api/buscar.py:105-106`  

---

## Descripción

```python
# CÓDIGO ACTUAL:
prob = z.get("probabilidad_supervivencia_3a") or z.get("probabilidad_supervivencia")
```

En Python, `0.0 or X` evalúa a `X` porque `0.0` es falsy. Si `probabilidad_supervivencia_3a = 0.0` (zona con muy baja supervivencia), se ignora y se usa el campo fallback, que puede tener un valor diferente.

## Impacto

Zonas con score de supervivencia muy bajo (~0-5%) se muestran con datos incorrectos. Afecta el ranking de viabilidad.

## Fix propuesto

```python
# CORRECTO:
_raw = z.get("probabilidad_supervivencia_3a")
prob = _raw if _raw is not None else z.get("probabilidad_supervivencia")
```

---

**Tags:** #bug #critico #api #buscar #scoring #probabilidad
