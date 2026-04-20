# Flujo completo de GeoRetail — guía para programador

Mapa de qué hace cada archivo y cómo viaja una petición desde que el usuario
escribe su idea hasta que el mapa se pinta con zonas rankeadas y un PDF sale
por el otro lado. Todas las rutas están relativas a `georetail/`.

---

## 1. Stack en 10 segundos

| Capa | Tecnología | Puerto |
|------|-----------|--------|
| Frontend | Next.js 14 + TypeScript + Leaflet | 3000 |
| Backend API | FastAPI + Python 3.11 | 8000 |
| Cola asíncrona | Celery worker + Redis | 6379 |
| BD | PostgreSQL 15 + PostGIS + pgvector | 5432 |
| Scheduler | APScheduler (dentro del backend) | — |
| Orquestación | Docker Compose | — |

El entry point del backend es `backend/main.py` (levanta FastAPI, abre pools,
carga modelo XGBoost + embeddings, registra routers y arranca el scheduler).
El del frontend es `frontend/src/app/app/page.tsx`.

---

## 2. Viaje de una petición — extremo a extremo

```
 ┌──────────────────┐   1. "Cafetería de especialidad en Gràcia"
 │ Frontend (React) │ ────────────────────────────────┐
 └──────────────────┘                                 │
          │  POST /api/buscar                         ▼
          │                                   ┌─────────────────┐
          │                                   │ api/buscar.py   │  (router)
          │                                   └────┬────────────┘
          │                                        │
          │   2. Validar + interpretar idea        ▼
          │                                   ┌─────────────────┐
          │                                   │ agente/         │
          │                                   │ validador.py    │──► LLM Router
          │                                   │ refinador.py    │    (5 modelos)
          │                                   │ traductor.py    │
          │                                   └────┬────────────┘
          │                                        │ perfil_refinado
          │                                        ▼
          │   3. Candidatos geográficos       ┌─────────────────┐
          │                                   │ db/zonas.py     │──► PostgreSQL
          │                                   └────┬────────────┘
          │                                        │ List[zona_id]
          │                                        ▼
          │   4. Scoring por zona             ┌─────────────────┐
          │                                   │ scoring/motor   │
          │                                   │ scoring/scorer  │──► XGBoost
          │                                   │ scoring/features│    + pesos
          │                                   │ dimensiones/*   │      manuales
          │                                   │ concepto/*      │
          │                                   └────┬────────────┘
          │                                        │ zonas rankeadas
          │                                        ▼
          │   5. Respuesta                    { estado, zonas[], session_id }
          ▼
 ┌──────────────────┐   6. Pinta bolitas en mapa
 │ MapCanvas.tsx    │
 └──────────────────┘
          │
          │  Click → POST /api/local/preview  (tooltip rápido, <200ms)
          │  "Ver detalle" → POST /api/local  (panel completo ~1-3s)
          │  "Financiero" → POST /api/financiero
          │  "Exportar"   → POST /api/exportar → PDF (WeasyPrint)
          ▼
```

En paralelo, y sin tocar el request del usuario, corre un **scheduler
interno** (`backend/pipelines/scheduler.py`) que cada X horas/días llama a
APIs externas (TMB, Google Places, BCN Open Data, Airbnb, aforadors…) para
refrescar `variables_zona` y recalcular scores. De ahí salen los datos sobre
los que el scoring trabaja.

---

## 3. Backend — qué hace cada archivo

### 3.1 Raíz (`backend/`)

| Archivo | Responsabilidad |
|---------|-----------------|
| `main.py` | App FastAPI. `lifespan` abre/cierra pools de PG, Redis, LLMs; carga modelos en memoria; arranca el scheduler. Registra los 13 routers. |
| `config.py` | `pydantic-settings`. Centraliza todas las env vars (DATABASE_URL, API keys, rutas). |
| `requirements.txt` | Dependencias Python. |
| `Dockerfile` | Imagen Python 3.11 con deps del sistema (gdal, libgl…). |
| `pytest.ini` | Config de tests (asyncio mode, rootdir). |

### 3.2 Contrato de datos (`schemas/`)

| Archivo | Responsabilidad |
|---------|-----------------|
| `schemas/models.py` | **Todos** los Pydantic models que cruzan la red. `ZonaResumen`, `ZonaDetalle`, `PerfilRefinado`, `EstadoBusqueda`, request/response de cada endpoint. Si cambias un campo aquí, el frontend (ver `src/lib/types.ts`) tiene que moverse. |

### 3.3 Endpoints REST (`api/`)

Todos se montan con `prefix="/api"`. Un archivo = un router = una familia de
rutas.

| Archivo | Ruta(s) | Qué hace |
|---------|---------|----------|
| `api/buscar.py` | `POST /buscar` | Entrada principal. Crea sesión, llama al LLM para validar/refinar, filtra zonas candidatas y devuelve ranking. Puede derivar a `cuestionario` si falta info. |
| `api/cuestionario.py` | `POST /cuestionario` | Bucle adaptativo de preguntas cuando la idea es ambigua. Usa `agente/cuestionario.py` para decidir la siguiente pregunta. |
| `api/local.py` | `POST /local/preview`, `POST /local` | `preview`: datos mínimos para el tooltip del mapa (rápido). `local`: detalle completo de una zona + narrativa LLM. |
| `api/locales.py` | `GET /locales` | Listado paginado con filtros, para la vista de lista. |
| `api/legal.py` | `GET /legal/{sector}`, `POST /legal/roadmap` | Requisitos legales por sector (dict estático + roadmap LLM cacheado 30 días). |
| `api/financiero.py` | `POST /financiero` | Calculadora financiera 100% automática. Acepta overrides de sliders. |
| `api/refinamiento.py` | `POST /refinamiento` | "Filtra los resultados: quita las caras" — refina ranking con lenguaje natural. |
| `api/exportar.py` | `POST /exportar`, `GET /exportar/download/{id}` | Encola PDF en Celery y devuelve URL con expiración 1h. |
| `api/health.py` | `GET /health` | Ping: BD, Redis, modelos cargados. |
| `api/mercado.py` | `GET /mercado/{zona_id}` | Precios alquiler comercial (multi-portal + Open Data). |
| `api/admin.py` | `POST /admin/pipelines/{nombre}` | Trigger manual de pipelines (protegido con `X-Admin-Key`). |
| `api/competencia.py` | `GET /competencia/{zona_id}` | Mini-mapa FIFA-style con competidores directos + subsector. |
| `api/transporte.py` | `GET /transporte/...` | Endpoints de accesibilidad y paradas. |
| `api/_utils.py` | — | Helpers compartidos (`score_to_color`, formateo…). |

### 3.4 Acceso a datos (`db/`)

| Archivo | Responsabilidad |
|---------|-----------------|
| `db/conexion.py` | Pool asyncpg (min=5, max=20). `init_db_pool` / `close_db_pool`. |
| `db/redis_client.py` | Cliente `redis.asyncio`. |
| `db/sesiones.py` | CRUD de sesión (Redis como caché + tabla `sesiones` en PG). |
| `db/zonas.py` | Queries PostGIS: `filtrar_zonas_candidatas` (entrada de `/buscar`), `get_zona_preview`, `get_zona_completa`. |
| `db/financiero.py` | `get_benchmarks_sector`, `guardar_analisis_financiero`. |
| `db/exportaciones.py` | Inserta fila en `exportaciones`, devuelve ID para descargar. |
| `db/migraciones/*.sql` | Migraciones incrementales numeradas. La `001_schema_inicial.sql` crea las ~32 tablas; el resto añade columnas, vistas e índices. |

### 3.5 Routers de servicios externos (`routers/`)

Todo acceso a servicios externos pasa por aquí. **Nunca** hagas `openai.ChatCompletion.create(...)` directamente en la lógica de negocio.

| Archivo | Responsabilidad |
|---------|-----------------|
| `routers/llm_router.py` | Fallback chain Anthropic → OpenAI → DeepSeek → Kimi → Gemini. Cada cliente se cachea. Timeout + retry con backoff. |
| `routers/places_router.py` | Google Places → Foursquare → Yelp → OSM Overpass. Normaliza el output. |
| `routers/geocoding_router.py` | Google Geocoding → Nominatim → OpenCage + caché en PG. |

### 3.6 Scoring (`scoring/`) — el corazón del modelo

Un request llega con `zona_ids + sector_codigo + perfil_negocio`. De ahí sale un score por zona entre 0-100. Pipeline:

```
dimensiones/*  (8 scorers puros)              concepto/*  (matching semántico)
        │                                             │
        ▼                                             ▼
  features.py ── construir_features_batch ──► feature matrix (58 cols v13)
        │                                             │
        ▼                                             ▼
     scorer.py ── XGBoost.predict() + pesos manuales fallback
        │
        ▼
     motor.py (API pública) ──► explainability.py (SHAP por dimensión)
```

| Archivo | Responsabilidad |
|---------|-----------------|
| `scoring/motor.py` | **API pública** del módulo. `calcular_scores_batch` (usado por `/buscar`) y `get_scores_zona` (usado por `/local`). Aplica reglas declarativas de modulación según `PerfilRefinado` y suma `score_afinidad_concepto`. |
| `scoring/scorer.py` | `cargar_modelo`, `calcular_scores_batch`, `guardar_scores`. Primero intenta XGBoost; si falla, cae a pesos manuales por dimensión. |
| `scoring/features.py` | Define `FEATURE_NAMES` (58 features, v13). Construye la matriz numérica desde `variables_zona`. |
| `scoring/taxonomia.py` | Tabla plana sector → dimensiones relevantes. |
| `scoring/explainability.py` | Agrupa SHAP values por dimensión para explicar el score al usuario. |
| `scoring/temporal_readiness.py` | Chequea qué variables están frescas; penaliza si hay columnas stale. |
| `scoring/concepto_matcher.py` | Entry legacy al matcher (delega en `concepto/matcher.py`). |

#### Dimensiones (`scoring/dimensiones/`) — scorers puros sin I/O

| Archivo | Qué mide |
|---------|----------|
| `flujo_peatonal.py` | Fusión ponderada de 4 fuentes: popular_times, VCity BSC, aforament vianants BCN, ratio comercios. |
| `demografia.py` | Multivariable: renta, edad, educación, hogares, densidad. |
| `entorno.py` | Locales vacíos, rotación, licencias nuevas, ocio cercano. |
| `seguridad.py` | Hurtos, robos, daños, presencia Guardia Urbana (granular por tipo). |
| `transporte.py` | Acceso multifactor: tránsito + bici + a pie. |
| `competencia.py` | Aglomeración beneficiosa vs saturación real. Usa `subsector_codigo` fino. |
| `turismo.py` + `evaluar_turismo.py` | Presión turística y ajuste según sector. |
| `dinamismo.py` | Trayectoria histórica: licencias + HHI + supervivencia de negocios. |

#### Concepto (`scoring/concepto/`) — matching negocio ↔ zona

| Archivo | Responsabilidad |
|---------|-----------------|
| `taxonomy.py` | ~100 conceptos canónicos. Define `aplicar_pesos_a_sector`, `compilar_concepto_negocio`, `zona_ideal_desde_perfil`. |
| `matcher.py` | Embeddings (sentence-transformers) de la idea del usuario vs conceptos canónicos. |
| `perfil.py` | De la taxonomía saca un vector numérico que se mezcla con el scoring. |
| `idea_tags.py` | Capa de compatibilidad para el formato viejo `idea_tags`. |

#### ML (`scoring/ml/`) — entrenamiento offline

| Archivo | CLI | Qué hace |
|---------|-----|----------|
| `ml/dataset.py` | — | Construye dataset desde `negocios_historico` + snapshots de `variables_zona`. |
| `ml/train.py` | `python -m scoring.ml.train --sector restauracion` | XGBoost + CV 5-fold + `scale_pos_weight`. Promueve versión si mejora AUC. |
| `ml/train_synthetic.py` | — | Entrenamiento con datos sintéticos (modo sin BD). |
| `ml/evaluate.py` | `python -m scoring.ml.evaluate --version X` | ROC-AUC, PR-AUC, Brier, SHAP, compara versiones. |
| `ml/hyperparams.py` | `python -m scoring.ml.hyperparams --trials 100` | Optuna TPE + MedianPruner. Guarda historial en SQLite. |

#### Infra (`scoring/infra/`) — gobernanza y registro

| Archivo | Responsabilidad |
|---------|-----------------|
| `governance.py` | Decisiones de fuentes demográficas por fecha/barrio. `slice_feature_matrix`. |
| `readiness.py` | DuckDB + cleanlab para auditar calidad de datos demográficos. |
| `model_registry.py` | `obtener_modelo_activo(sector)`, `feature_names(version)`. Cada modelo en disco (`models/xgboost_synthetic_v3.json`). |
| `legal.py` | Requisitos legales por cruce zona+sector. |

### 3.7 Agente LLM (`agente/`)

| Archivo | Responsabilidad |
|---------|-----------------|
| `agente/validador.py` | `validar_negocio(texto)`: ¿es retail?, ¿tiene sentido?, ¿hay info mínima? |
| `agente/refinador.py` | `refinar(descripcion, respuestas)`: produce `PerfilRefinado`. `generar_pregunta_senal` para el loop de preservación. |
| `agente/analizador.py` | `analizar_zona(zona, perfil)`: genera narrativa razonada por zona. |
| `agente/analisis.py` | `generar_analisis_zona`: wrapper thin de `analizador`, usado por `/local`. |
| `agente/refinamiento.py` | `procesar_refinamiento(filtro_natural)`: aplica filtro LLM sobre ranking existente. |
| `agente/cuestionario.py` | `procesar_respuesta`: siguiente pregunta adaptativa. |
| `agente/traductor.py` | Traduce queries del usuario a formato que entienden APIs externas (Google Places, etc.). |
| `agente/prompts/__init__.py` | **Todos** los prompts. Versionado por fecha. |

### 3.8 NLP (`nlp/`)

| Archivo | Responsabilidad |
|---------|-----------------|
| `nlp/embeddings.py` | Carga `paraphrase-multilingual-mpnet-base-v2`. `encode(textos)`, `actualizar_perfil_zona(zona_id)`. |
| `nlp/clasificador.py` | `clasificar_batch` (texto→categoría), `generar_alertas_zona`. |
| `nlp/alertas.py` | Genera y prioriza alertas desde reseñas (ej: "seguridad percibida baja"). |

### 3.9 Financiero (`financiero/`)

| Archivo | Responsabilidad |
|---------|-----------------|
| `financiero/estimador.py` | `estimar_parametros`: ticket_medio, clientes/día, alquiler, salarios, reforma, depósito. Todo automático desde benchmarks. |
| `financiero/calculadora.py` | Proyección 36 meses con curva de rampa, ROI, payback, alerta `alquiler/ventas > 15%`. |

### 3.10 Workers asíncronos (`workers/`)

Tareas pesadas no bloquean el request. Se encolan en Redis, las procesa Celery.

| Archivo | Responsabilidad |
|---------|-----------------|
| `workers/celery_app.py` | Config de Celery (broker=Redis, backend=Redis). |
| `workers/tasks.py` | `calcular_scores_batch` (recálculo masivo), `generar_pdf_task` (exportar). |

### 3.11 Exportar (`exportar/`)

| Archivo | Responsabilidad |
|---------|-----------------|
| `exportar/generador.py` | WeasyPrint + plantillas Jinja2. Genera mapa estático con Mapbox Static API, lo incrusta en el PDF. |

### 3.12 Pipelines de datos (`pipelines/`) — lo que alimenta la BD

APScheduler dentro del backend los dispara según cron. Cada pipeline escribe
en `variables_zona` y/o tablas específicas, y registra su ejecución en
`pipeline_ejecuciones`.

| Subcarpeta / archivo | Fuente | Frecuencia |
|---------------------|--------|------------|
| `pipelines/scheduler.py` | Orquestador APScheduler | arranque |
| `pipelines/scores.py` | Recálculo global XGBoost | semanal |
| `pipelines/parametros_financieros.py` | Pre-cálculo zonas × sectores | semanal |
| `pipelines/parques.py` | Parques AMB | mensual |
| **peatonal/** | | |
| `peatonal/aforaments.py` | CSVs locales (ST_DWithin 200m + distancia inversa²) | diario |
| `peatonal/vianants.py` | Open Data BCN (sensores peatonales) | diario |
| `peatonal/vcity.py` | VCity BSC (tileserver Martin) | diario |
| **demografia/** | | |
| `demografia/demografia.py` | padró + renda BCN | mensual |
| `demografia/demografia_backfill.py` | CSVs `_cleaned/` | one-shot |
| `demografia/descarga_datos_publicos.py` | BCN/Generalitat | one-shot |
| **turismo/** | | |
| `turismo/airbnb.py` | InsideAirbnb listings.csv.gz | trimestral |
| `turismo/booking.py` | Booking.com + HUT fallback | mensual |
| `turismo/hut.py` | HUT Generalitat | mensual |
| **transporte/** | | |
| `transporte/transporte.py` | TMB API | semanal (sábado 01:00) |
| `transporte/bicing.py` | GBFS API | diario |
| **inmobiliario/** | | |
| `inmobiliario/mercado_inmobiliario.py` | scraping multi-portal | diario |
| `inmobiliario/precios.py` | Open Data BCN + scrapers | semanal |
| `inmobiliario/scraping/*.py` | idealista, fotocasa, habitaclia, milanuncios, pisos, booking | por portal |
| `inmobiliario/scraping/gosom_client.py` | Cliente REST al scraper Go | — |
| **comercio/** | | |
| `comercio/cens_comercial.py` | Cens Comercial BCN → `negocios_historico` (labels XGBoost) | mensual |
| `comercio/llicencies.py` | CKAN BCN | mensual |
| `comercio/competencia.py` | Google Places + OSM | mensual |
| `comercio/registre_mercantil.py` | datos.gob.es (training data) | mensual |
| `comercio/entorno_comercial.py` | mercats municipals BCN | mensual |
| `comercio/dinamismo.py` | pipeline agregado → `dinamismo_zonal` | mensual (día 6 03:00) |
| **entorno/** | | |
| `entorno/overpass.py` | OSM Overpass API | semanal |
| `entorno/parques.py` | opendata.amb.cat | mensual |
| `entorno/venues_ocio.py` | equipaments culturals BCN | mensual |
| `entorno/seguridad.py` | incidencias Guardia Urbana BCN (CKAN) | semanal |
| `entorno/google_maps.py` | gosom scraper (enriquecimiento) | semanal |
| `entorno/resenas.py` | Google Places + Foursquare + NLP | diario |

### 3.13 Tests (`tests/`)

`pytest`. `conftest.py` tiene stubs de asyncpg / openai / anthropic / google
para que el CI corra sin secretos. Tests por módulo:
`test_motor.py`, `test_demografia_v5.py`, `test_flujo_peatonal.py`,
`test_turismo_dimension.py`, `test_scorer_completo.py`, etc.
`tests/scripts/test_integracion_modelo.py` es diagnóstico interactivo (NO
corre en pytest — requiere BD en vivo).

---

## 4. Frontend — qué hace cada archivo

### 4.1 App (Next.js App Router — `frontend/src/app/`)

| Archivo | Responsabilidad |
|---------|-----------------|
| `app/layout.tsx` | Layout raíz, fuentes, metadata. |
| `app/page.tsx` | Landing pública. |
| `app/app/page.tsx` | **Página principal de la app**. Orquesta estado global (sesión, zonas, detalle), llama a `api.buscar` y coordina `MapCanvas`, `Dossier`, `ActiveDock`, `CommandBar`, `ZoneIndex`. |
| `app/admin/` | Panel admin protegido (pesos del modelo, disparo de pipelines). |
| `app/globals.css` | CSS global. |

### 4.2 Librería (`frontend/src/lib/`)

| Archivo | Responsabilidad |
|---------|-----------------|
| `lib/api.ts` | Cliente fetch tipado. Un método por endpoint: `api.buscar`, `api.local`, `api.financiero`, `api.exportar`… |
| `lib/types.ts` | Espejo TypeScript de `backend/schemas/models.py`. Si se desincroniza, se rompen los tipos en runtime. |

### 4.3 Componentes (`frontend/src/components/`)

Cada componente tiene su CSS Module al lado (`X.tsx` + `X.module.css`).

| Componente | Responsabilidad |
|------------|-----------------|
| `Onboarding.tsx` | Primera pantalla — recoge la descripción del negocio. |
| `SearchBox.tsx` | Input de búsqueda refinada dentro de la app. |
| `MapView.tsx` | Wrapper Leaflet clásico (vista lista). |
| `map/MapCanvas.tsx` | Mapa principal con bolitas coloreadas por score. |
| `map/ActiveDock.tsx` | Dock lateral con la zona activa y acciones rápidas. |
| `map/CommandBar.tsx` | Barra de comandos (refinar, filtros, cambiar basemap). |
| `map/Dossier.tsx` | Panel completo con análisis narrativo + scores + financiero. |
| `map/HudCoord.tsx` / `HudLegend.tsx` | HUD superpuesto (coords, leyenda colores). |
| `map/ZoneIndex.tsx` | Listado plegable de zonas. |
| `map/BasemapSwitcher.tsx` | Alternador dark/light/satélite. |
| `map/LoadingOverlay.tsx` | Overlay mientras se resuelve `/buscar`. |
| `ZoneList.tsx` | Vista lista clásica (alternativa al mapa). |
| `DetailPanel.tsx` | Panel de detalle de zona (usado antes de `Dossier`). |
| `ScoreBars.tsx` | Barras horizontales por dimensión (flujo, demo, seguridad…). |
| `FinancialPanel.tsx` | Calculadora financiera con sliders (debounce 300ms → `/api/financiero`). |
| `LegalPanel.tsx` | Requisitos legales del sector + roadmap LLM. |
| `CompetenciaPanel.tsx` | Mini-mapa FIFA-style con competidores directos por subsector. |
| `TransportePanel.tsx` | Accesibilidad (metro, bus, bici, a pie). |
| `DevPanel.tsx` | Panel de debug (feature flags, sesión, payload raw). |
| `landing/` | Componentes de la landing. |

### 4.4 Convenciones frontend

- `strict: true`, sin `any`.
- Todo fetch pasa por `lib/api.ts`.
- El contrato de tipos lo manda `lib/types.ts`; si Pydantic cambia, hay que propagar aquí.
- CSS Modules (nada de styled-components ni Tailwind).
- Hot-reload en Docker: los cambios en `.tsx` se ven en <1s.

---

## 5. Flujo UX paso a paso (lo que ve el usuario)

| Paso | Acción usuario | Archivos involucrados |
|------|----------------|----------------------|
| 1 | Escribe descripción | `Onboarding.tsx` → `api.buscar` |
| 2 | `POST /api/buscar` | `api/buscar.py` → `agente/validador.py` → `agente/refinador.py` → `db/zonas.py` → `scoring/motor.py` |
| 3 | (Si falta info) cuestionario | `api/cuestionario.py` en loop hasta que `refinar` dé `signal_score >= 70` |
| 4 | Mapa con bolitas verdes/amarillas/rojas | `MapCanvas.tsx` — umbrales >75 / 50-75 / <50 |
| 5 | Click en bolita | `POST /api/local/preview` (tooltip rápido <200ms) |
| 6 | "Ver detalle" | `POST /api/local` (~1-3s, con narrativa LLM vía `agente/analisis.py`) |
| 7 | "Análisis financiero" | `POST /api/financiero`. **Solo al pulsar**, no al abrir el detalle. Sliders → debounce 300ms → re-call. |
| 8 | "Refinar resultados" | `POST /api/refinamiento` con lenguaje natural. |
| 9 | "Exportar PDF" | `POST /api/exportar` encola Celery → `workers/tasks.py::generar_pdf_task` → `exportar/generador.py` → URL con `GET /api/exportar/download/{id}` expira en 1h. |

---

## 6. Modelo de scoring — dónde se calibra cada pieza

```
score_global = XGBoost(features) si versión activa     [scoring/scorer.py]
             | pesos_manuales · dimensiones + afinidad   (fallback + blending)
             └────────────────────────────────────────┘
                               │
            ┌──────────────────┼──────────────────┐
            ▼                  ▼                  ▼
     dimensiones/*     concepto/matcher   PerfilRefinado
     (8 scorers)       (embeddings)       (reglas declarativas
                                           en motor.py:REGLAS_MODULACION)
```

**Pesos manuales por defecto:** flujo 25% · demografía 20% · competencia 15%
· transporte 15% · entorno 10% · dinamismo 5% · seguridad 5% · turismo 5%.
`precio` se quitó como dimensión.

**Afinidad de concepto:** `_PESO_AFINIDAD = 0.12` en `motor.py`. Se calcula
en runtime (no se persiste) porque depende del negocio concreto.

**Modulación por perfil:** reglas declarativas en `motor.py:REGLAS_MODULACION`.
Ej: si `horarios_apertura contiene 'noche'` → `peso_seguridad ×= 1.30`,
`peso_turismo ×= 1.20`.

**Entrenamiento XGBoost (offline, manual):**
```bash
python -m scoring.ml.train --sector restauracion
python -m scoring.ml.evaluate --version restauracion_20260310_1430
python -m scoring.ml.hyperparams --sector todos --trials 100
```

---

## 7. Cómo levantar todo en local

```bash
# Desde la raíz del repo
cd georetail

# 1. Infraestructura (BD + Redis + backend + worker + frontend)
docker-compose up -d

# 2. Verificar
docker-compose ps
curl http://localhost:8000/api/health

# 3. URLs
#    Frontend:       http://localhost:3000
#    Swagger:        http://localhost:8000/docs
#    ReDoc:          http://localhost:8000/redoc
```

Hot-reload está activo en backend y frontend. Solo reconstruir imagen
(`docker-compose up -d --build`) si tocas `requirements.txt` o `package.json`.

---

## 8. Dónde tocar cuando quieras…

| Quiero… | Archivo(s) |
|---------|------------|
| Añadir un endpoint nuevo | crea `api/<nombre>.py`, añade `include_router` en `main.py`, añade tipo en `schemas/models.py` |
| Cambiar pesos del modelo | `scoring/scorer.py` (pesos manuales) o retrain con `scoring/ml/train.py` |
| Añadir una dimensión nueva | crea `scoring/dimensiones/<dim>.py`, añade feature(s) en `scoring/features.py:FEATURE_NAMES`, peso en `scorer.py` |
| Añadir una fuente de datos externa | crea `pipelines/<categoría>/<fuente>.py`, regístralo en `pipelines/scheduler.py`, escribe columnas en `variables_zona` (migración en `db/migraciones/`) |
| Cambiar un prompt LLM | `agente/prompts/__init__.py` (versionado por fecha) |
| Cambiar la UI del mapa | `frontend/src/components/map/*.tsx` |
| Exponer un nuevo campo al front | añádelo a `schemas/models.py` **y** a `frontend/src/lib/types.ts` |
| Cambiar pesos del modelo desde la UI | `frontend/src/app/admin/` + `api/admin.py` |
| Sumar un LLM al fallback | `routers/llm_router.py` |

---

## 9. Gotchas (cosas que muerden al novato)

- **El `.env` no está en git.** Está en `georetail/.env`, se crea desde `.env.example`. Sin `OPENAI_API_KEY` / `GOOGLE_MAPS_API_KEY` / `NEXT_PUBLIC_MAPBOX_TOKEN` no arranca.
- **Flujo peatonal usa `ST_DWithin` (radio 200m), no `ST_Within`.** Ver `pipelines/peatonal/aforaments.py`. Razón: los sensores son puntuales, `ST_Within` dejaba el 80% de las zonas con NULL.
- **`scorer.py` hace fallback a pesos manuales si XGBoost falla.** No siempre ves el error — mira los logs.
- **El PDF se genera en Celery, no en el request.** Si tu worker no está corriendo, `/api/exportar` devuelve un ID que nunca resuelve.
- **Embeddings se cargan con timeout 20s en `main.py::lifespan`.** Si fallan, la app arranca igual en modo degradado (sin matching semántico).
- **Los tipos del frontend NO se generan automáticamente desde Pydantic.** `schemas/models.py` y `frontend/src/lib/types.ts` hay que mantenerlos a mano.
- **No llames a LLMs directamente.** Siempre vía `routers/llm_router.py`. Si pones `openai.ChatCompletion(...)` en lógica de negocio, pierdes el fallback.

---

## 10. Referencias cruzadas

- `docs/prioridad-1-auditoria-modelo-y-competencia.md` — auditoría de datos y modelo en curso.
- `georetail/CLAUDE.md` — contexto vivo del proyecto (hay que leerlo antes de cambiar nada).
- `LEVANTAR_PROYECTO.txt` — guía paso a paso para levantar con Docker.
- `http://localhost:8000/docs` — Swagger UI interactivo (la fuente de verdad del contrato REST).
