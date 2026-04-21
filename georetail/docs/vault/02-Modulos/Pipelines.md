# Pipelines de datos

## Estructura

```
pipelines/
├─ comercio/
│   ├─ cens_comercial.py
│   ├─ competencia.py
│   ├─ competencia_scrape.py
│   ├─ dinamismo.py
│   ├─ entorno_comercial.py
│   ├─ llicencies.py
│   └─ registre_mercantil.py
├─ demografia/
│   ├─ demografia.py
│   ├─ demografia_backfill.py
│   └─ descarga_datos_publicos.py
├─ entorno/
│   ├─ google_maps.py
│   ├─ overpass.py
│   ├─ parques.py
│   ├─ resenas.py
│   ├─ seguridad.py
│   └─ venues_ocio.py
├─ inmobiliario/
│   ├─ mercado_inmobiliario.py
│   ├─ precios.py       ← calcula ratio_locales_comerciales
│   └─ scraping/        ← Playwright scrapers (idealista, fotocasa, habitaclia, etc.)
├─ parametros_financieros.py
└─ parques.py
```

## Orquestación

- APScheduler → `pipelines/scheduler.py`
- Cada pipeline loguea inicio/fin/estado en `pipeline_ejecuciones`

## Flujo peatonal

- `ST_DWithin(200m)` — sensores escasos, un sensor por intersección
- Peso: inverso del cuadrado de la distancia
- Sin sensor en 200m → imputación con media del barrio → distrito → media global
- Columna clave: `variables_zona.ratio_locales_comerciales`

## Scrapers Playwright

Después de `pip install -r requirements.txt`:
```bash
playwright install chromium
```
