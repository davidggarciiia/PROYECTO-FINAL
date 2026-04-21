# BUG-002: TypeError en normalizar_pesos si BD retorna NULL en peso_*

**Fecha:** 2026-04-21  
**Severidad:** CRITICA  
**Estado:** Abierto  
**Archivo:** `scoring/motor.py:112-115`  

---

## Descripción

```python
# CÓDIGO ACTUAL:
total = sum(float(pesos.get(k, 0.0) or 0.0) for k in campos)
```

Si `pesos.get(k)` retorna `None` (columna NULL en BD), entonces `float(None)` lanza `TypeError`.

## Impacto

El sistema falla silenciosamente cuando la BD tiene NULL en columnas de peso. Afecta sectores nuevos o zonas sin datos completos.

## Fix propuesto

```python
# CORRECTO:
total = sum(float(pesos.get(k) or 0.0) for k in campos)
```

---

**Tags:** #bug #critico #scoring #motor #TypeError
