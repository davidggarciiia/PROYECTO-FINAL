# Arquitectura General

## Stack tecnológico

| Capa | Tecnología |
|---|---|
| Frontend | Next.js (TypeScript strict) |
| Backend | FastAPI + Python |
| Base de datos | PostgreSQL + PostGIS |
| Cache | Redis |
| Workers | Celery |
| ML | XGBoost + sentence-transformers |
| Scraping | Playwright + gosom (Go) |
| Orquestación | Docker Compose |

---

## Capas del backend

```
api/          → Routers FastAPI (thin, delegan a servicios)
scoring/      → Motor de puntuación (motor.py es la API pública)
agente/       → Análisis LLM, cuestionario adaptativo
financiero/   → Proyección 36 meses, estimación automática
nlp/          → sentence-transformers (768 dims)
pipelines/    → Ingesta de datos (APScheduler)
db/           → Acceso a PostgreSQL/Redis
workers/      → Tareas Celery
routers/      → LLM router, Places router, Geocoding router
```

---

## Reglas de diseño críticas

- `motor.py` es la API pública del scoring — NUNCA importar `scorer.py` directamente desde `api/`
- Todo acceso LLM va por `routers/llm_router.py` — fallback: Claude Sonnet 4.6 → GPT-4o → DeepSeek V3 → Kimi K2.5 → Gemini 2.0 Flash
- Todo env var va por `config.py` (pydantic-settings) — NUNCA `os.environ` directo
- Flujo peatonal: `ST_DWithin(200m)` no `ST_Within`
- Label XGBoost: 1 si negocio sobrevivió ≥3 años, 0 si cerró antes

---

## Feature versioning

| Versión | Descripción |
|---|---|
| v1–v13 | Features base |
| v14 | Seasonal (BCN temporal layer) |
| v15 | Weekly/live dinamismo |

`governance.py:slice_feature_matrix` — recorta la matriz de features al subconjunto exacto con el que fue entrenado el modelo activo.

---

## Ver también

- [[Backend]] — Detalle del backend
- [[Base-de-Datos]] — Esquema de tablas
