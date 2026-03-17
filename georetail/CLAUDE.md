# CLAUDE.md — GeoRetail

Contexto completo del proyecto. Leer antes de tocar cualquier archivo.

---

## Qué es esto

App web que recomienda ubicaciones comerciales en Barcelona a emprendedores.
El usuario describe su negocio en lenguaje natural y recibe un mapa con zonas
rankeadas por score de viabilidad + análisis financiero automático.

**En una frase:** El análisis que haría una consultora de retail — en tiempo real, desde el navegador.

---

## Stack

| Capa | Tecnología |
|---|---|
| Frontend | Next.js + TypeScript + CSS Modules |
| Mapa | Leaflet + OpenStreetMap (sin token, gratuito) |
| Backend | FastAPI + Python 3.11 |
| Cola asíncrona | Celery + Redis |
| Base de datos | PostgreSQL 15 + PostGIS + pgvector |
| Agentes IA | LangChain + Claude Sonnet (principal) |
| ML / Scoring | XGBoost + scikit-learn + SHAP + Optuna |
| NLP reseñas | sentence-transformers (paraphrase-multilingual-mpnet-base-v2, 768 dims) |
| Scheduler | APScheduler |
| Contenedores | Docker + Docker Compose |
| Servidor | Hetzner VPS + Nginx |

**LLM Router con fallback:** Claude Sonnet 4.6 → GPT-4o → DeepSeek V3 → Kimi K2.5 → Gemini 2.0 Flash

---

## Estado actual del proyecto

### ✅ Archivos escritos y completos

```
backend/
├── config.py                          ← pydantic-settings, todas las env vars
├── main.py                            ← FastAPI app, lifespan, routers, CORS
├── schemas/
│   └── models.py                      ← todos los tipos Pydantic (contrato con front)
├── api/
│   ├── buscar.py                      ← POST /api/buscar
│   ├── cuestionario.py                ← POST /api/cuestionario
│   ├── local.py                       ← POST /api/local/preview + POST /api/local
│   ├── locales.py                     ← GET /api/locales
│   ├── legal.py                       ← GET /api/legal/{sector_codigo}
│   ├── financiero.py                  ← POST /api/financiero
│   ├── refinamiento.py                ← POST /api/refinamiento
│   ├── exportar.py                    ← POST /api/exportar + GET /api/exportar/download/{id}
│   └── health.py                      ← GET /api/health
├── db/
│   ├── conexion.py                    ← pool asyncpg min=5 max=20
│   ├── redis_client.py                ← redis.asyncio
│   ├── sesiones.py                    ← CRUD sesiones Redis + PostgreSQL
│   ├── zonas.py                       ← filtrar_zonas_candidatas, get_zona_preview, get_zona_completa
│   ├── financiero.py                  ← get_benchmarks_sector, guardar_analisis_financiero
│   └── exportaciones.py               ← CRUD exportaciones PDF
├── routers/
│   ├── llm_router.py                  ← fallback chain Anthropic→OpenAI→DeepSeek→Kimi→Gemini
│   ├── places_router.py               ← Google Places→Foursquare→Yelp→OSM
│   └── geocoding_router.py            ← Google→Nominatim→OpenCage + caché PG
├── scoring/
│   ├── features.py                    ← 21 features, construir_features, construir_features_batch
│   ├── scorer.py                      ← pesos manuales + XGBoost, guardar_scores
│   ├── dataset.py                     ← construcción dataset de entrenamiento desde PG
│   ├── train.py                       ← entrenamiento XGBoost, CV 5-fold, promover versión
│   ├── evaluate.py                    ← métricas, comparar versiones, SHAP global
│   └── hyperparams.py                 ← búsqueda hiperparámetros con Optuna
├── nlp/
│   ├── embeddings.py                  ← sentence-transformers, encode, actualizar_perfil_zona
│   └── clasificador.py                ← clasificar_batch, generar_alertas_zona
├── agente/
│   ├── validador.py                   ← validar_negocio (LLM)
│   ├── cuestionario.py                ← procesar_respuesta
│   ├── analizador.py                  ← analizar_zona
│   └── prompts/__init__.py            ← todos los prompts del sistema
├── financiero/
│   ├── estimador.py                   ← estimar_parametros automáticamente
│   └── calculadora.py                 ← calcular_proyeccion 36 meses, ROI, payback
├── workers/
│   ├── celery_app.py                  ← configuración Celery
│   └── tasks.py                       ← calcular_scores_batch, generar_pdf_task
├── exportar/
│   └── generador.py                   ← WeasyPrint + Jinja2, mapa estático Mapbox
└── pipelines/
    ├── scheduler.py                   ← APScheduler, 12 jobs
    ├── aforaments.py                  ← flujo peatonal CKAN (ST_DWithin + ponderación distancia)
    ├── resenas.py                     ← Google Places + Foursquare + NLP
    ├── precios.py                     ← Idealista + Open Data BCN
    ├── scores.py                      ← recálculo semanal XGBoost
    ├── demografia.py                  ← padró + renda BCN
    ├── registre_mercantil.py          ← training data XGBoost
    ├── parametros_financieros.py      ← pre-cálculo semanal financiero
    ├── transporte.py                  ← líneas/paradas TMB (semanal sábado 01:00)
    └── mercado_inmobiliario.py        ← scraping multi-portal locales/viviendas
```

### ✅ Archivos adicionales implementados

```
backend/
├── db/migraciones/
│   ├── 001_schema_inicial.sql         ← esquema completo de BD (32 tablas)
│   └── 004_inmuebles_portales.sql     ← tabla multi-portal + vista v_mercado_zona
├── requirements.txt
├── Dockerfile
├── pytest.ini
├── tests/                             ← suite completa (191 tests, 7 módulos)
│   ├── conftest.py                    ← stubs asyncpg/openai/anthropic/google para CI
│   ├── test_calculadora.py
│   ├── test_estimador.py
│   ├── test_scorer.py
│   ├── test_motor.py
│   ├── test_features.py
│   └── test_agente.py
└── .env.example                       ← en raíz del proyecto (georetail/.env.example)

frontend/                              ← Next.js completo con Leaflet + OpenStreetMap
├── src/app/page.tsx                   ← página principal
├── src/components/                    ← MapView, SearchBox, DetailPanel, FinancialPanel,
│                                         ZoneList, ScoreBars (con CSS Modules)
└── src/lib/                           ← api.ts, types.ts

docker-compose.yml                     ← postgres + redis + backend + worker + frontend
```

---

## Endpoints API

| Método | Ruta | Archivo | Descripción |
|--------|------|---------|-------------|
| POST | `/api/buscar` | api/buscar.py | Búsqueda principal, devuelve zonas o lanza cuestionario |
| POST | `/api/cuestionario` | api/cuestionario.py | Responder pregunta del cuestionario adaptativo |
| POST | `/api/local/preview` | api/local.py | Click en mapa — datos mínimos para tooltip (<200ms) |
| POST | `/api/local` | api/local.py | "Ver detalle" — análisis completo (~1-3s) |
| GET | `/api/locales` | api/locales.py | Vista lista con filtros y paginación |
| GET | `/api/legal/{sector}` | api/legal.py | Requisitos legales del sector |
| POST | `/api/financiero` | api/financiero.py | Calculadora financiera automática (debounce 300ms) |
| POST | `/api/refinamiento` | api/refinamiento.py | Filtrar resultados con lenguaje natural |
| POST | `/api/exportar` | api/exportar.py | Generar PDF |
| GET | `/api/exportar/download/{id}` | api/exportar.py | Descargar PDF (expira 1h) |
| GET | `/api/health` | api/health.py | Estado del sistema |

---

## Flujo UX

```
1. Usuario escribe descripción → POST /api/buscar
2. Si falta info → cuestionario (POST /api/cuestionario en bucle)
3. Mapa con zonas coloreadas (verde >75 / amarillo 50-75 / rojo <50)
4. Click en bolita → POST /api/local/preview  (tooltip rápido)
5. "Ver detalle" → POST /api/local            (panel completo ~1-3s)
6. "Análisis financiero" → POST /api/financiero (solo al pulsar, NO al abrir detalle)
7. Sliders ajustan parámetros → POST /api/financiero con overrides (debounce 300ms)
8. "Exportar" → POST /api/exportar → PDF
```

---

## Flujo peatonal — decisión de diseño importante

El pipeline `aforaments.py` usa **ST_DWithin (radio 200m)** en lugar de ST_Within.

**Por qué:** Los sensores de aforadors están en puntos concretos (cruces, paseos).
Con ST_Within solo se asignaban a la zona exacta donde cae el sensor.
En un barrio con 5 zonas y 1 sensor, las otras 4 quedaban con NULL.

**Solución actual:**
- Radio 200m desde cada sensor
- Ponderación por distancia inversa al cuadrado: `peso = 1 / distancia²`
- El flujo se distribuye entre todas las zonas del radio proporcionalmente
- Las zonas comerciales (alto `ratio_locales_comerciales`) reciben multiplicador ×1.0–1.35
- Las zonas sin ningún sensor a 200m se imputan con la media del barrio → distrito → global

**Columna clave:** `variables_zona.ratio_locales_comerciales`
Calculada por `pipelines/precios.py` desde el Cens Locals BCN.
Es lo que diferencia una calle comercial de una residencial en el mismo barrio.

---

## Modelo XGBoost — arquitectura

**Features (21):** Ver `scoring/features.py` → `FEATURE_NAMES`

**Label:** `1` si el negocio sobrevivió ≥ 3 años, `0` si cerró antes.

**Flujo de entrenamiento:**
```
scoring/dataset.py   → construye dataset desde negocios_historico + snapshots históricas
scoring/train.py     → CV 5-fold + scale_pos_weight + guardar en disco + promover si mejor AUC
scoring/evaluate.py  → ROC-AUC, PR-AUC, Brier, SHAP global, comparar versiones
scoring/hyperparams.py → Optuna (TPE + MedianPruner), guarda historial en SQLite
```

**CLI:**
```bash
python -m scoring.train --sector restauracion
python -m scoring.evaluate --version restauracion_20260310_1430
python -m scoring.hyperparams --sector todos --trials 100
```

**Hiperparámetros actuales (en train.py `_DEFAULT_PARAMS`):**
Resultado del último run de Optuna. Si el modelo se degrada, relanzar hyperparams.py.

---

## Módulo financiero — 100% automático

No hay inputs manuales. El usuario puede hacer override con sliders.

**Estimación automática de parámetros:**
- `ticket_medio` → mediana `precio_nivel` de competidores (Google Places)
- `clientes_dia` → flujo_peatonal × tasa_conversion / (num_competidores + 1)
- `alquiler_mensual` → Idealista o precio_m2 × m2 del local
- `salarios_mensual` → ceil(m2 / empleados_por_m2) × convenio × 1.31 SS
- `reforma_local` → m2 × benchmarks sectoriales
- `deposito_fianza` → alquiler × 2 (Art. 36 LAU)

**Proyección:** 36 meses con curva de rampa de arranque meses 1-12.
**Alerta:** Si alquiler/ventas > 15% → `alerta_alquiler = true`.

---

## Legislación por sector

| Sector | Régimen | Requisito especial |
|---|---|---|
| restauracion | Annexe III.2 | Informe urbanístico previo (IVU) obligatorio |
| tatuajes | Annexe III.2 | Diploma higiénico-sanitario + registro ASPCAT |
| shisha_lounge | Annexe III.2 | **Modelo club privado de fumadores** (Ley 28/2005) |
| moda | Comunicació prèvia | Ninguno especial (<120m²) |
| estetica | Comunicació prèvia / III.2 | Autorización sanitaria si hay aparatología |

Los datos están en `api/legal.py` → `_SECTORES` (dict estático).
Cuando exista la tabla `requisitos_legales_sector` en BD → leer de allí.

---

## Variables de entorno

```bash
# Base de datos
DATABASE_URL=postgresql://postgres:password@localhost:5432/georetail
REDIS_URL=redis://localhost:6379

# LLMs
ANTHROPIC_API_KEY=sk-ant-...
OPENAI_API_KEY=sk-...
DEEPSEEK_API_KEY=...
KIMI_API_KEY=...
GEMINI_API_KEY=...

# Google
GOOGLE_MAPS_API_KEY=...   # Places + Geocoding + Static Maps

# Reseñas
FOURSQUARE_API_KEY=...
YELP_API_KEY=...

# Transporte
TMB_APP_ID=...
TMB_APP_KEY=...

# Mapbox (frontend)
NEXT_PUBLIC_MAPBOX_TOKEN=pk.eyJ1...

# Rutas
MODELS_DIR=/data/models
OPTUNA_DIR=/data/optuna
EXPORTS_DIR=/data/exports
```

---

## Convenciones

- **Python:** black + isort. Type hints en todas las funciones públicas.
- **TypeScript:** strict mode. No `any`.
- Toda llamada a LLM pasa por `routers/llm_router.py` — nunca directamente.
- Todo acceso a datos externos pasa por el router de fallback correspondiente.
- Los endpoints siempre tienen `response_model`, `summary` y `tags`.
- Scoring: `scorer.py` intenta XGBoost primero, fallback a pesos manuales.
- Pipelines: siempre registran ejecución en `pipeline_ejecuciones` (inicio + fin + estado).

---

## Levantar en local

```bash
# 1. Infraestructura
docker-compose up -d postgres redis

# 2. Migrar BD
cd backend && psql $DATABASE_URL < db/migraciones/001_schema_inicial.sql
cd backend && psql $DATABASE_URL < db/migraciones/004_inmuebles_portales.sql

# 3. Backend
cd backend && uvicorn main:app --reload --port 8000

# 4. Celery worker
cd backend && celery -A workers.celery_app worker --loglevel=info

# 5. Frontend
cd frontend && npm run dev

# Docs API interactiva: http://localhost:8000/docs
```

---

## Docs adicionales

- `docs/base-de-datos.md` — esquema completo de todas las tablas
- `docs/apis-externas.md` — endpoints, auth, rate limits y alternativas gratuitas
- `docs/flujo-completo.md` — flujo detallado con diagramas de secuencia
- `docs/arquitectura.md` — decisiones técnicas y alternativas consideradas
- `docs/equipo.md` — división de responsabilidades
