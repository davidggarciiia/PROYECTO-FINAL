# GeoRetail — Dashboard

> App que recomienda locales comerciales en Barcelona a emprendedores mediante scoring multidimensional + análisis financiero automático.

---

## Estado actual

| Componente | Estado | Rama activa |
|---|---|---|
| Backend (FastAPI) | ✅ Activo | `mejora-modelo-definitivo` |
| Frontend (Next.js) | ✅ Activo | `mejora-modelo-definitivo` |
| Scoring Engine | ✅ XGBoost + fallback manual | v15 features |
| Pipelines | ✅ APScheduler | 8 dimensiones |
| Auditoría de errores | 🔄 En progreso | 2026-04-21 |

---

## Navegación rápida

- [[01-Arquitectura/Overview]] — Arquitectura general
- [[02-Modulos/Scoring-Engine]] — Motor de puntuación
- [[02-Modulos/Agente-LLM]] — Módulo LLM
- [[02-Modulos/Pipelines]] — Pipelines de datos
- [[02-Modulos/Financiero]] — Módulo financiero
- [[03-Auditoria/README]] — Auditoría de errores activa
- [[04-API/Endpoints]] — Referencia de API
- [[05-DevOps/Docker-Compose]] — Setup y despliegue

---

## URLs

| Servicio | URL |
|---|---|
| Frontend | http://localhost:3000 |
| API docs (Swagger) | http://localhost:8000/docs |
| Health check | http://localhost:8000/api/health |

---

## Sectores soportados

`restauracion` · `moda` · `estetica` · `tatuajes` · `shisha_lounge` · `salud` · `deporte` · `educacion` · `alimentacion` · `servicios` · `otro`

---

## Dimensiones de scoring

| Dimensión | Peso |
|---|---|
| flujo_peatonal | 25% |
| demografia | 20% |
| competencia | 15% |
| transporte | 15% |
| entorno | 10% |
| dinamismo | 5% |
| seguridad | 5% |
| turismo | 5% |

> `precio` no es dimensión de scoring. `score_afinidad_concepto` (12%) se calcula en runtime.
