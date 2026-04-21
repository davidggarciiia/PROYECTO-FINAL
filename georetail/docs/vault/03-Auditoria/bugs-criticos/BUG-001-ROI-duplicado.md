# BUG-001: ROI a 3 años inflado — inversión sumada dos veces

**Fecha:** 2026-04-21  
**Severidad:** CRITICA  
**Estado:** ✅ Resuelto (2026-04-21)  
**Archivo:** `financiero/calculadora.py:93-94`  

---

## Descripción

```python
# CÓDIGO ACTUAL (INCORRECTO):
ben_3a_c = proyeccion[-1]["acumulado_conservador"] + inversion
ben_3a_o = proyeccion[-1]["acumulado_optimista"] + inversion
```

El acumulado ya resta la inversión inicial (línea ~43-44: `acum_c = -inversion`). En el mes 36, `acumulado_conservador` ya contiene el flujo de caja neto **menos** la inversión. Sumar `inversion` de nuevo la cancela incorrectamente.

## Impacto

El ROI a 3 años está **inflado** en el valor de la inversión inicial. Ejemplo: si inversión = 50.000€ y el negocio realmente da -10.000€ de beneficio neto, se muestra +40.000€.

## Fix propuesto

```python
# CORRECTO:
ben_3a_c = proyeccion[-1]["acumulado_conservador"]
ben_3a_o = proyeccion[-1]["acumulado_optimista"]
```

---

**Tags:** #bug #critico #financiero #ROI #calculadora
