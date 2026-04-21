# BUG-005: zona_ideal_desde_perfil(None) puede fallar en concepto matcher

**Fecha:** 2026-04-21  
**Severidad:** MEDIA  
**Estado:** Abierto  
**Archivo:** `scoring/motor.py:509-512`  

---

## Descripción

Si `compilar_concepto_negocio()` retorna un dict sin clave `"zona_ideal"` y `perfil_negocio` es None/vacío, se llama `_zona_ideal_desde_perfil(None)`. El módulo `concepto.taxonomy` puede no manejar `None` correctamente.

## Fix propuesto

Validar antes de pasar:
```python
pn = compiled.get("perfil_negocio") or perfil_negocio
zona_ideal = compiled.get("zona_ideal") or (_zona_ideal_desde_perfil(pn) if pn else {})
```

---

**Tags:** #bug #medio #scoring #concepto #None
