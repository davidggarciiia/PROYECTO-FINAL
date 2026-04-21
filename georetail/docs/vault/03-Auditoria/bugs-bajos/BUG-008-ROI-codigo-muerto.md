# BUG-008: Código muerto — ingresos año 2 calculados pero no retornados

**Fecha:** 2026-04-21  
**Severidad:** BAJA  
**Estado:** ✅ Resuelto (ya incluidos en commit 50a9c79 — los campos sí se retornan)  
**Archivo:** `financiero/calculadora.py:82-87`  

---

## Descripción

Se calculan `ing_a2_c` e `ing_a2_o` (ingresos año 2) pero no aparecen en el dict de respuesta final (líneas 111-125).

## Fix propuesto

O añadir al dict de respuesta o eliminar los cálculos.

---

**Tags:** #bug #bajo #financiero #codigomunerto
