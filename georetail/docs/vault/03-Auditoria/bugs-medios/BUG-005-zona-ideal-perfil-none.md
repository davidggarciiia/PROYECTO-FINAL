# BUG-005: zona_ideal_desde_perfil llamada con perfil=None en motor.py
**Fecha:** 2026-04-21
**Severidad:** MEDIA
**Estado:** Resuelto (2026-04-21)
**Archivo:** `scoring/motor.py:515-517`

## Descripcion

La funcion `_zona_ideal_desde_perfil` podia ser invocada con `None` como argumento
cuando `compiled.get("perfil_negocio")` y la variable local `perfil_negocio` eran
ambas falsy (None, dict vacio o cadena vacia).

El patron original:

```python
zona_ideal = compiled.get("zona_ideal") or _zona_ideal_desde_perfil(
    compiled.get("perfil_negocio") or perfil_negocio
)
```

pasaba `None` directamente a `_zona_ideal_desde_perfil`, que delega en
`zona_ideal_desde_perfil` (taxonomy.py). Aunque esa funcion tiene su propio guard
(`if not perfil_negocio: return {}`), el acceso a atributos de objeto (`perfil.sector`)
en cualquier version o path alternativo de la funcion causaria `AttributeError`.

## Impacto

- Crash en tiempo de ejecucion del endpoint principal `/api/buscar` cuando el usuario
  no proporciona suficiente informacion para inferir un perfil de negocio.
- La respuesta devolveria HTTP 500 en lugar de degradarse con gracia.

## Fix aplicado

```python
# Antes
zona_ideal = compiled.get("zona_ideal") or _zona_ideal_desde_perfil(
    compiled.get("perfil_negocio") or perfil_negocio
)

# Despues
_pn = compiled.get("perfil_negocio") or perfil_negocio
zona_ideal = compiled.get("zona_ideal") or (_zona_ideal_desde_perfil(_pn) if _pn else {})
```

La llamada a `_zona_ideal_desde_perfil` se omite completamente si el perfil es falsy,
retornando `{}` como valor neutro. Los callers de `compilar_y_puntuar_concepto` ya
manejan `zona_ideal` como un dict posiblemente vacio.

## Callers verificados

- `motor.py` -> `compilar_y_puntuar_concepto` -> `zona_ideal` se incluye en el dict
  de retorno; el caller (`api/buscar.py`) lo usa como campo opcional del response.
- El valor `{}` es compatible con el schema de respuesta (campo nullable/optional).

## Referencias

- Archivo corregido: `scoring/motor.py` lineas 515-517
- Funcion delegada: `scoring/concepto/taxonomy.py:211` (`zona_ideal_desde_perfil`)
- Doc previo: `BUG-005-zona-ideal-None.md` (fix propuesto documentado antes de aplicar)
