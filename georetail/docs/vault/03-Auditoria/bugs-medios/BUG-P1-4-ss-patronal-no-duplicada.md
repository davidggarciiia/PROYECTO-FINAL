---
name: BUG-P1-4 SS patronal (auditoría 2026-04-21) — no reproducible en código actual
description: Investigación del reporte P1-4 sobre duplicación del multiplicador 1.31 de SS patronal; conclusión = falso positivo, test de regresión añadido.
type: project
---

# BUG-P1-4: ROI inflado por SS patronal × 1.31² — **NO REPRODUCIBLE**

**Fecha:** 2026-04-22
**Severidad original (auditoría):** MEDIA (cálculo financiero incorrecto)
**Estado:** Cerrado — **falso positivo** + test de regresión preventivo
**Archivos investigados:**
- `georetail/backend/financiero/estimador.py`
- `georetail/backend/financiero/calculadora.py`
- `georetail/backend/api/financiero.py`
- `georetail/backend/pipelines/parametros_financieros.py`
- `georetail/backend/tests/test_calculadora.py` (test de regresión añadido)

---

## Reclamo de la auditoría

Cita literal de `docs/AUDITORIA-2026-04-21.md` §P1-4 (aprox. líneas 312-326):

> El `estimador.py` calcula salarios como `salario_bruto × 1.31` (SS patronal incluida).
> Pero `calculadora.py` vuelve a multiplicar en un punto → **×1.31² = 1.72**.
>
> Resultado: ROI a 3 años sistemáticamente **deflactado ~13%** en sectores con peso
> salarial alto (restauración, shisha_lounge). El número del panel financiero es
> menos atractivo de lo que debería ser.
>
> **Fix sugerido:** eliminar la segunda multiplicación. Añadir test de regresión
> con snapshot para evitar reintroducir el bug.

## Investigación realizada

### 1. `estimador.py:_salarios()` — única aplicación de SS

```python
_SS_EMPRESA = 0.31  # Seguridad Social empresa ~31% sobre salario bruto
...
def _salarios(n, bench) -> PE:
    sal = bench.get("salario_base_mensual_convenio", 1650.0)
    total = round(n * sal * (1 + _SS_EMPRESA))  # ← línea 173, única aplicación
    return PE(total, f"{n} empleado(s) × {sal:.0f}€ × {1+_SS_EMPRESA:.0%} SS — Convenio Cat 2024",
              "media", round(total * 0.6), round(total * 1.5))
```

### 2. `calculadora.py:calcular_proyeccion()` — usa `salarios_mensual` AS-IS

```python
cf_mes = p["alquiler_mensual"] + p["salarios_mensual"] + p["otros_fijos_mensual"]
```

Línea 39. `salarios_mensual` entra al coste fijo mensual **sin multiplicador adicional**.
El bucle de 36 meses usa `cf_mes` tal cual sin re-escalar en ningún punto.

### 3. `api/financiero.py` — grep exhaustivo

```bash
grep -En "1\.31|_SS_EMPRESA|salario" georetail/backend/api/financiero.py
```

- Línea 32: comentario con `× 1.31 SS` — explicativo, no código.
- `_row_to_estimados` en `api/financiero.py` lee `salarios_mensual` de la BD y lo
  devuelve tal cual al frontend, sin escalar.
- No existe ninguna multiplicación por `1.31`, `_SS_EMPRESA` ni similar en el endpoint.

### 4. `pipelines/parametros_financieros.py`

Almacena `p.salarios_mensual.valor` en `estimados_financieros_zona_sector` tal cual.
No re-escala.

### 5. Grep global en `financiero/`

```bash
grep -rEn "1\.31|_SS_EMPRESA|SS_EMPRESA" georetail/backend
```

Solo 3 matches:
- `financiero/estimador.py:15` → definición `_SS_EMPRESA = 0.31`.
- `financiero/estimador.py:173` → aplicación única en `_salarios`.
- `financiero/estimador.py:174` → f-string del mensaje de fuente (`{1+_SS_EMPRESA:.0%}`).
- `tests/test_estimador.py` → tests que importan `_SS_EMPRESA` (no multiplican).

### 6. `coste_personal_pct` — campo no usado

El benchmark `coste_personal_pct` aparece en las tablas de `benchmarks_sector` pero
**no se lee en ninguna parte de `calculadora.py` ni `estimador.py`**. Inicialmente
pensé que podría ser el vector de duplicación (multiplicar ingresos × coste_personal_pct
después de tener salarios ya escalados), pero no existe tal código.

## Conclusión

**La duplicación descrita en la auditoría NO existe en el código actual.**

Hipótesis sobre el origen del reporte:
- **Opción A:** El bug existió en un estado anterior del código (antes del refactor
  actual de `_salarios()`) y fue corregido silenciosamente sin actualizar la auditoría.
  La auditoría es de 2026-04-21, y `estimador.py:_salarios()` en su forma actual
  aplica `(1 + _SS_EMPRESA)` exactamente una vez.
- **Opción B:** El autor de la auditoría confundió el campo `coste_personal_pct`
  (benchmark sectorial no usado) con una multiplicación activa. Este campo sigue
  en los dicts de benchmarks pero no entra en ningún cálculo.
- **Opción C:** Mala lectura del f-string de fuente en `_salarios`
  (`f"... × {1+_SS_EMPRESA:.0%} SS ..."`), que aparece textualmente como "× 1.31"
  en la fuente mostrada al usuario y puede parecer una segunda multiplicación si
  solo se grepea `1.31`.

## Acción aplicada — test de regresión

Aunque no hay bug, la auditoría pide explícitamente:
> "Añadir test de regresión con snapshot para evitar reintroducir el bug"

He añadido `TestSalariosSinDuplicar` en `tests/test_calculadora.py` que verifica:

1. **Snapshot explícito**: `calcular_proyeccion({"salarios_mensual": S, ...})`
   produce `costes_fijos = alquiler + S + otros_fijos` exactamente, donde
   `S` es el valor que recibe la calculadora. Sin multiplicador adicional.
2. **Ratio SS único**: dado el pipeline real (estimador → calculadora),
   si cambiamos `_SS_EMPRESA` de 0.31 a 0.50, el delta de `costes_fijos`
   resultante es exactamente `salario_base × num_empleados × 0.19`, no
   `× 0.19²` ni cualquier otra potencia. Demuestra que el factor se aplica
   una vez y solo una.
3. **Integración estimador→calculadora**: llamada directa de `_salarios(n, bench)`
   seguida de `calcular_proyeccion` con ese valor. `cf_mes − alquiler − otros_fijos`
   debe equivaler a `round(n × salario × (1 + _SS_EMPRESA))` exactamente.

Si alguien reintroduce una multiplicación por `1.31` en `calculadora.py` o en
`api/financiero.py`, estos tests fallarán.

## Verificación

```bash
cd georetail/backend
pytest tests/test_calculadora.py::TestSalariosSinDuplicar -v
# Esperado: 3 tests passed
```

## Notas

- Se actualiza la auditoría `docs/AUDITORIA-2026-04-21.md` §10 marcando P1-4 como
  `✅ resuelto (falso positivo + test de regresión)` con enlace a este bug-note.
- Si en el futuro se añade lógica de "coste de personal variable" (p.ej.
  horas extra, bonos por venta), revisar de nuevo para asegurar que no se
  acumula SS encima del salario ya escalado.

---

**Tags:** #bug #media #financiero #ss-patronal #falso-positivo #regresion #p1
