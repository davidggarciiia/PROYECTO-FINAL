# DevOps y Docker

## Comandos habituales

```bash
cd georetail/

# Levantar todo
docker-compose up -d

# Health check
curl http://localhost:8000/api/health

# Rebuild tras cambios en requirements.txt o package.json
docker-compose up -d --build

# Logs
docker-compose logs -f backend
docker-compose logs -f frontend
```

## Servicios

| Servicio | Puerto | Descripción |
|---|---|---|
| PostgreSQL+PostGIS | 5432 | Base de datos principal |
| Redis | 6379 | Cache y sesiones |
| FastAPI backend | 8000 | API REST |
| Celery worker | - | Jobs asíncronos |
| Next.js frontend | 3000 | UI |
| gosom scraper | 8080 | Google Maps scraper (Go) |

## Variables de entorno requeridas

**Mínimo para LLM:** `DEEPSEEK_API_KEY` u `OPENAI_API_KEY` (el router hace fallback DeepSeek → OpenAI)

**Mínimo para mapas:** `GOOGLE_MAPS_API_KEY` + `NEXT_PUBLIC_MAPBOX_TOKEN`

```bash
cp georetail/.env.example georetail/.env
# Editar .env con los valores reales
# NUNCA commitear .env
```

## Tests backend

```bash
cd georetail/backend
pytest                          # todos los tests
pytest tests/test_agente.py     # un archivo
pytest tests/test_estimador.py::test_estimar_parametros_basicos  # un test
```

## ML Training

```bash
cd georetail/backend
python -m scoring.ml.train --sector restauracion
python -m scoring.ml.evaluate --version restauracion_20260310_1430
python -m scoring.ml.hyperparams --sector todos --trials 100
python -m scoring.ml.train_synthetic   # sin DB live
python -m scoring.temporal_readiness  # gate antes de promover modelo
```
