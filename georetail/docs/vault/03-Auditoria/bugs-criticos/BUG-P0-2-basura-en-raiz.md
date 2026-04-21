# BUG-P0-2: Archivos basura en raíz del proyecto

**Fecha:** 2026-04-22
**Severidad:** CRITICA
**Estado:** Resuelto
**Archivo:** raíz del repo (`C:\Users\David\Desktop\PROYECTO-FINAL-1\`)

---

## Descripción

En la raíz del proyecto aparecían 6 archivos de 0 bytes con nombres raros,
creados por error por redirecciones de shell mal escapadas en sesiones
anteriores:

```
0
5%
Optional[float]
Optional[float]`
Promedio
str
```

## Impacto

- Ruido en `git status` (aparecían como untracked)
- Confusión cognitiva al listar el repo
- Riesgo de que alguien los commitease por despiste y contaminase el árbol

## Reproducción

Cualquier `ls` o `git status` en la raíz mostraba los 6 archivos.
Eran todos de 0 bytes (verificado con `ls -la`).

## Causa raíz

Redirecciones `>` accidentales en sesiones previas. Por ejemplo, un comando
del estilo `echo "algo" > Optional[float]` interpretaba `Optional[float]`
como nombre de archivo.

## Fix aplicado

```bash
rm "0" "5%" "Optional[float]" "Optional[float]\`" "Promedio" "str"
```

Los 6 archivos estaban vacíos, borrado 100% seguro.

## Verificación

```bash
ls | grep -E "^(0|5%|Optional|Promedio|str)$"   # → 0 líneas
git status                                       # → limpio de untracked basura
```

## Notas

Para evitar reincidencia: documentar en CLAUDE.md que los comandos Bash con
`>` deben llevar siempre el destino entre comillas si contiene caracteres
especiales. Ya mencionado en las reglas de Bash del harness.

---

**Tags:** #bug #critico #higiene #p0
