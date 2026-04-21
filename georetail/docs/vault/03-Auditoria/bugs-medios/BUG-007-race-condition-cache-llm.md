# BUG-007: Race condition en caché de perfil refinado LLM

**Fecha:** 2026-04-21  
**Severidad:** MEDIA  
**Estado:** Resuelto (2026-04-21)  
**Archivo:** `api/buscar.py:202-204`  

---

## Descripción

Dos requests concurrentes con la misma descripción pueden generar dos llamadas al LLM si la sesión aún no tiene el caché sincronizado en Redis. Coste innecesario de API.

## Fix propuesto

Usar un lock Redis (p.ej. `SET NX EX`) sobre la clave de hash de descripción antes de llamar al LLM.

---

**Tags:** #bug #medio #api #buscar #cache #LLM #concurrencia
