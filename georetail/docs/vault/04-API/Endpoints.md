# API Endpoints

**Base URL:** `http://localhost:8000`  
**Docs:** `http://localhost:8000/docs`

---

## Endpoints principales

| Método | Path | Módulo | Descripción |
|---|---|---|---|
| GET | `/api/health` | health.py | Health check |
| POST | `/api/buscar` | buscar.py | Búsqueda principal de zonas |
| GET | `/api/local/{zona_id}` | local.py | Detalle de una zona |
| GET | `/api/locales` | locales.py | Listado de zonas |
| GET | `/api/competencia/{zona_id}` | competencia.py | Competencia de una zona |
| GET | `/api/transporte/{zona_id}` | transporte.py | Transporte de una zona |
| GET | `/api/mercado` | mercado.py | Datos de mercado |
| POST | `/api/financiero` | financiero.py | Análisis financiero |
| GET | `/api/legal/{sector}` | legal.py | Requisitos legales por sector |
| POST | `/api/cuestionario` | cuestionario.py | Cuestionario adaptativo |
| POST | `/api/refinamiento` | refinamiento.py | Refinamiento de análisis |
| GET | `/api/exportar` | exportar.py | Exportar resultados |
| GET/POST | `/api/admin` | admin.py | Admin endpoints |

---

## Convenciones

- Todos los endpoints tienen `response_model`, `summary` y `tags`
- Delegan inmediatamente a la capa de servicio
- No hay lógica de negocio en los routers

---

## Routers externos

| Router | Fallback chain |
|---|---|
| `llm_router.py` | Claude Sonnet 4.6 → GPT-4o → DeepSeek V3 → Kimi K2.5 → Gemini 2.0 Flash |
| `places_router.py` | Google Places API + gosom scraper (puerto 8080) |
| `geocoding_router.py` | Google Maps Geocoding API |
