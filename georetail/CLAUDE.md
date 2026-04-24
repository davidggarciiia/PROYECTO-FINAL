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
| Agentes IA | LangChain + DeepSeek V3 (principal) + GPT-4o-mini (fallback) |
| ML / Scoring | XGBoost + scikit-learn + SHAP + Optuna |
| NLP reseñas | sentence-transformers (paraphrase-multilingual-mpnet-base-v2, 768 dims) |
| Scheduler | APScheduler |
| Contenedores | Docker + Docker Compose |
| Servidor | Hetzner VPS + Nginx |

**LLM Router con fallback:** DeepSeek V3 → GPT-4o-mini

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
│   ├── health.py                      ← GET /api/health
│   ├── admin.py                       ← POST /api/admin/pipelines/{nombre} (trigger manual)
│   ├── competencia.py                 ← GET /api/competencia/{zona_id}
│   ├── mercado.py                     ← GET /api/mercado/{zona_id}
│   └── _utils.py                      ← helpers compartidos (score_to_color, etc.)
├── db/
│   ├── conexion.py                    ← pool asyncpg min=5 max=20
│   ├── redis_client.py                ← redis.asyncio
│   ├── sesiones.py                    ← CRUD sesiones Redis + PostgreSQL
│   ├── zonas.py                       ← filtrar_zonas_candidatas, get_zona_preview, get_zona_completa
│   ├── financiero.py                  ← get_benchmarks_sector, guardar_analisis_financiero
│   └── exportaciones.py               ← CRUD exportaciones PDF
├── routers/
│   ├── llm_router.py                  ← fallback chain DeepSeek→OpenAI
│   ├── places_router.py               ← Google Places→Foursquare→Yelp→OSM
│   └── geocoding_router.py            ← Google→Nominatim→OpenCage + caché PG
├── scoring/
│   ├── motor.py                       ← API pública: calcular_scores_batch, get_scores_zona
│   ├── scorer.py                      ← pesos manuales + XGBoost, guardar_scores
│   ├── features.py                    ← 58 features (v13), construir_features, construir_features_batch
│   ├── dimensiones/                   ← un scorer por dimensión (pure, sin I/O)
│   │   ├── flujo_peatonal.py          ← fusión ponderada 4 fuentes (popular_times, vcity, vianants, ratio)
│   │   ├── demografia.py              ← scoring demográfico multivariable (renta, edad, educación...)
│   │   ├── entorno.py                 ← entorno comercial (vacíos, rotación, licencias, ocio)
│   │   ├── seguridad.py               ← seguridad granular (hurtos, robos, daños, GU)
│   │   ├── transporte.py              ← acceso transporte multifactor (tránsito + bici + a pie)
│   │   ├── competencia.py             ← scoring avanzado competencia (aglomeración + saturación)
│   │   └── dinamismo.py               ← trayectoria comercial histórica (licencias+HHI+supervivencia)
│   ├── concepto/                      ← matching semántico tipo de negocio
│   │   ├── taxonomy.py                ← taxonomía conceptual unificada (~100 conceptos canónicos)
│   │   ├── matcher.py                 ← matching por embeddings a conceptos canónicos
│   │   ├── perfil.py                  ← perfil numérico del negocio desde taxonomía
│   │   └── idea_tags.py               ← capa compatibilidad idea_tags sobre taxonomía
│   ├── ml/                            ← entrenamiento y evaluación del modelo XGBoost
│   │   ├── dataset.py                 ← construcción dataset de entrenamiento desde PG
│   │   ├── train.py                   ← XGBoost CV 5-fold, promover versión
│   │   ├── train_synthetic.py         ← entrenamiento con datos sintéticos (sin BD)
│   │   ├── evaluate.py                ← métricas, SHAP global, comparar versiones
│   │   └── hyperparams.py             ← búsqueda hiperparámetros con Optuna
│   └── infra/                         ← gobernanza de datos y registro de modelos
│       ├── governance.py              ← decisiones de fuentes demográficas, slice_feature_matrix
│       ├── readiness.py               ← calidad datos demográficos (DuckDB + cleanlab)
│       ├── model_registry.py          ← obtener_modelo_activo, feature_names por versión
│       └── legal.py                   ← requisitos legales por zona+sector
├── nlp/
│   ├── embeddings.py                  ← sentence-transformers, encode, actualizar_perfil_zona
│   ├── clasificador.py                ← clasificar_batch, generar_alertas_zona
│   └── alertas.py                     ← generación y priorización de alertas NLP
├── agente/
│   ├── validador.py                   ← validar_negocio (LLM)
│   ├── analizador.py                  ← analizar_zona (LLM)
│   ├── analisis.py                    ← generar_analisis_zona (wrapper de analizador)
│   ├── refinamiento.py                ← procesar_refinamiento (LLM)
│   ├── traductor.py                   ← traducción de queries para APIs externas
│   ├── cuestionario.py                ← procesar_respuesta
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
    ├── scheduler.py                   ← APScheduler, orquesta todos los pipelines
    ├── scores.py                      ← recálculo semanal XGBoost (todas las zonas)
    ├── parametros_financieros.py      ← pre-cálculo semanal financiero (zonas × sectores)
    ├── peatonal/                      ← flujo de tráfico y peatonal
    │   ├── aforaments.py              ← tráfico rodado desde CSVs locales (ST_DWithin + ponderación)
    │   ├── vianants.py                ← aforament vianants BCN Open Data (sensores peatonales)
    │   └── vcity.py                   ← flujo peatonal VCity BSC (tileserver Martin)
    ├── demografia/                    ← datos demográficos
    │   ├── demografia.py              ← padró + renda BCN (mensual)
    │   ├── demografia_backfill.py     ← backfill histórico desde CSVs locales (_cleaned/)
    │   └── descarga_datos_publicos.py ← descarga auditable de fuentes públicas BCN/Catalunya
    ├── turismo/                       ← alojamiento turístico
    │   ├── airbnb.py                  ← InsideAirbnb listings (CSV.gz público)
    │   ├── booking.py                 ← Booking.com + HUT fallback (alojamientos)
    │   └── hut.py                     ← HUT Generalitat → score_turismo por zona
    ├── transporte/                    ← movilidad urbana
    │   ├── transporte.py              ← líneas/paradas TMB API (semanal sábado 01:00)
    │   └── bicing.py                  ← estaciones Bicing GBFS API
    ├── inmobiliario/                  ← mercado inmobiliario
    │   ├── mercado_inmobiliario.py    ← scraping multi-portal (Idealista, Fotocasa, Habitaclia...)
    │   ├── precios.py                 ← precios alquiler comercial (Open Data BCN + scrapers)
    │   └── scraping/                  ← scrapers por portal + modelos + URLs
    │       ├── base_scraper.py
    │       ├── idealista_scraper.py
    │       ├── fotocasa_scraper.py
    │       ├── habitaclia_scraper.py
    │       ├── milanuncios_scraper.py
    │       ├── pisos_scraper.py
    │       ├── booking_scraper.py
    │       ├── gosom_client.py        ← cliente REST para google-maps-scraper (Go)
    │       ├── models.py
    │       └── urls.py
    ├── comercio/                      ← actividad comercial y licencias
    │   ├── cens_comercial.py          ← Cens Comercial BCN → negocios_historico (labels XGBoost)
    │   ├── llicencies.py              ← llicències d'activitat BCN (CKAN)
    │   ├── competencia.py             ← análisis competencia mensual (Google Places + OSM)
    │   ├── registre_mercantil.py      ← empresas BCN datos.gob.es (training data XGBoost)
    │   ├── entorno_comercial.py       ← mercats municipals + datos comerciales BCN
    │   └── dinamismo.py               ← pipeline mensual (día 6 03:00) → dinamismo_zonal table
    └── entorno/                       ← entorno físico y urbano
        ├── overpass.py                ← negocios activos desde OSM Overpass API
        ├── parques.py                 ← parques AMB (opendata.amb.cat)
        ├── venues_ocio.py             ← equipaments culturals i d'oci BCN
        ├── seguridad.py               ← incidencias Guardia Urbana BCN (CKAN)
        ├── google_maps.py             ← enriquecimiento negocios vía gosom scraper
        └── resenas.py                 ← reseñas Google Places + Foursquare + NLP (diario)
```

### ✅ Archivos adicionales implementados

```
backend/
├── db/
│   ├── migraciones/                   ← 10 archivos consolidados + 2 seeds (orden alfabético)
│   │   ├── 01_extensiones.sql         ← postgis, vector, pg_trgm, fn_set_updated_at()
│   │   ├── 02_geografia.sql           ← distritos, barrios, zonas
│   │   ├── 03_inmuebles.sql           ← locales, locales_historico_precios, inmuebles_portales,
│   │   │                                 precios_alquiler_zona, cache_geocoding, v_mercado_zona
│   │   ├── 04_variables_zona.sql      ← variables_zona (coordinadora) + vz_flujo / vz_turismo /
│   │   │                                 vz_demografia / vz_comercial / vz_entorno + v_variables_zona
│   │   ├── 05_sectores_scoring.sql    ← sectores, subsectores, scores_zona, modelos_versiones,
│   │   │                                 negocios_historico, dim_calendario_bcn
│   │   ├── 06_competencia.sql         ← negocios_activos, competencia_por_local,
│   │   │                                 competencia_detalle_zona, v_competencia_zona
│   │   ├── 07_transporte.sql          ← lineas_transporte, paradas_transporte, paradas_lineas,
│   │   │                                 frecuencias_transporte, estaciones_bicing, carriles_bici
│   │   ├── 08_entorno_turismo.sql     ← venues_ocio, alojamientos_turisticos, licencias_actividad,
│   │   │                                 trams_peatonales, parques_amb, comisarias,
│   │   │                                 mercados_municipales, landmarks_turisticos,
│   │   │                                 intensitat_turismo_oficial, dinamismo_zonal, v_dinamismo_zona
│   │   ├── 09_usuario_financiero.sql  ← sesiones, busquedas, mensajes_cuestionario,
│   │   │                                 benchmarks_sector, parametros_financieros_zona,
│   │   │                                 analisis_financieros, requisitos_legales_sector,
│   │   │                                 restricciones_geograficas_sector, exportaciones,
│   │   │                                 v_parametros_financieros_actuales
│   │   ├── 10_ia_pipelines.sql        ← resenas (IVFFLAT), alertas_zona,
│   │   │                                 perfiles_zona_embedding (IVFFLAT),
│   │   │                                 pipeline_ejecuciones, pipeline_errores, llm_logs
│   │   ├── 98_seed_demo.sql           ← datos de demostración (usa nuevos nombres de columna)
│   │   └── 99_seed_variables_zona.sql ← seeds de variables escriben a vz_* (no a tabla fat)
│   └── legacy_migraciones/            ← 37 migraciones legacy 001-036 + 2 seeds históricos
│                                         (NO se ejecutan; solo referencia histórica)
├── models/
│   └── xgboost_synthetic_v3.json      ← modelo pre-entrenado con datos sintéticos
├── requirements.txt
├── Dockerfile
├── pytest.ini
├── tests/                             ← suite completa (26 módulos de test)
│   ├── conftest.py                    ← stubs asyncpg/openai/anthropic/google para CI
│   ├── scripts/
│   │   └── test_integracion_modelo.py ← diagnóstico interactivo (requiere BD live, no pytest)
│   └── test_*.py                      ← tests unitarios e integración por módulo
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
| GET | `/api/legal/{sector}` | api/legal.py | Requisitos legales del sector (estático) |
| POST | `/api/legal/roadmap` | api/legal.py | Roadmap jurídico LLM personalizado (zona+sector, cache 30d) |
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

**Features (58, v13):** Ver `scoring/features.py` → `FEATURE_NAMES`
Dimensiones scorer manual_v2: flujo 25%, demografía 20%, competencia 15%, transporte 15%,
entorno 10%, dinamismo 5%, seguridad 5%, turismo 5%. `precio` eliminado como dimensión.

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

# LLMs (basta con UNA de las dos; el router hace fallback DeepSeek → OpenAI)
DEEPSEEK_API_KEY=sk-...
OPENAI_API_KEY=sk-...

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

### Convenciones SQL

- **Nombres:** snake_case universal, todo en castellano (`licencias_actividad`, `trams_peatonales`).
- **Booleanos:** prefijo `es_` (estado permanente: `es_activo`, `es_escaparate`), `esta_` (estado transitorio: `esta_disponible`, `esta_activa`), `fue_` (acción pasada: `fue_descargado`), `sobrevivio_` (evento histórico: `sobrevivio_3a`).
- **Constraints nombrados:** prefijo `pk_` (primary key), `fk_` (foreign key), `uq_` (unique), `ck_` (check). Nunca dejar constraints sin nombre.
- **Tipos canónicos:**
  - Coordenadas: `DOUBLE PRECISION` (lat, lng, área en m²)
  - Dinero: `NUMERIC(12,2)` (precio, alquiler)
  - Rating 0-5: `NUMERIC(3,2)`
  - Embeddings: `VECTOR(768)` (paraphrase-multilingual-mpnet-base-v2)
  - Score 0-100: `NUMERIC(5,2)`
  - Probabilidad 0-1: `NUMERIC(4,3)`
- **Vistas de compatibilidad:** `v_variables_zona` agrega las 5 tablas satélite; no romper el orden de columnas sin actualizar `scoring/features.py`.

---

## Levantar en local

```bash
# 1. Infraestructura + BD completa (los archivos 01_...10_... y 98_/99_ se aplican
#    automáticamente vía docker-entrypoint-initdb.d en orden alfabético)
docker compose down -v && docker compose up --build

# 2. Backend (solo si se quiere correr fuera de Docker)
cd backend && uvicorn main:app --reload --port 8000

# 3. Celery worker
cd backend && celery -A workers.celery_app worker --loglevel=info

# 4. Frontend
cd frontend && npm run dev

# Docs API interactiva: http://localhost:8000/docs
```

> **Nota:** `docker compose down -v` destruye el volumen de datos y recrea el esquema desde cero
> aplicando los 10 archivos de migraciones + 2 seeds en orden. Las migraciones legacy (001-036)
> están en `backend/db/legacy_migraciones/` y **no se ejecutan**; sirven solo de referencia histórica.

---

## Docs adicionales

- `docs/base-de-datos.md` — esquema completo de todas las tablas
- `docs/apis-externas.md` — endpoints, auth, rate limits y alternativas gratuitas
- `docs/flujo-completo.md` — flujo detallado con diagramas de secuencia
- `docs/arquitectura.md` — decisiones técnicas y alternativas consideradas
- `docs/equipo.md` — división de responsabilidades
