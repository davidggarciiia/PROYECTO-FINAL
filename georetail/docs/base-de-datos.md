# Base de datos — GeoRetail

PostgreSQL 15 + PostGIS + pgvector. Esquema consolidado tras el refactor SQL (abril 2026).

---

## Estructura de archivos SQL

Los archivos viven en `backend/db/migraciones/` y se aplican automáticamente via
`docker-entrypoint-initdb.d` en orden alfabético al ejecutar `docker compose down -v && docker compose up --build`.

| Archivo | Contenido |
|---|---|
| `01_extensiones.sql` | Extensiones (postgis, vector, pg_trgm) + función `fn_set_updated_at()` |
| `02_geografia.sql` | `distritos`, `barrios`, `zonas` |
| `03_inmuebles.sql` | `locales`, `locales_historico_precios`, `inmuebles_portales`, `precios_alquiler_zona`, `cache_geocoding`, vista `v_mercado_zona` |
| `04_variables_zona.sql` | Tabla coordinadora `variables_zona` + 5 satélites `vz_flujo / vz_turismo / vz_demografia / vz_comercial / vz_entorno` + vista `v_variables_zona` |
| `05_sectores_scoring.sql` | `sectores`, `subsectores`, `scores_zona`, `modelos_versiones`, `negocios_historico`, `dim_calendario_bcn` |
| `06_competencia.sql` | `negocios_activos`, `competencia_por_local`, `competencia_detalle_zona`, vista `v_competencia_zona` |
| `07_transporte.sql` | `lineas_transporte`, `paradas_transporte`, `paradas_lineas`, `frecuencias_transporte`, `estaciones_bicing`, `carriles_bici` |
| `08_entorno_turismo.sql` | `venues_ocio`, `alojamientos_turisticos`, `licencias_actividad`, `trams_peatonales`, `parques_amb`, `comisarias`, `mercados_municipales`, `landmarks_turisticos`, `intensitat_turismo_oficial`, `dinamismo_zonal`, vista `v_dinamismo_zona` |
| `09_usuario_financiero.sql` | `sesiones`, `busquedas`, `mensajes_cuestionario`, `benchmarks_sector`, `parametros_financieros_zona`, `analisis_financieros`, `requisitos_legales_sector`, `restricciones_geograficas_sector`, `exportaciones`, vista `v_parametros_financieros_actuales` |
| `10_ia_pipelines.sql` | `resenas` (IVFFLAT), `alertas_zona`, `perfiles_zona_embedding` (IVFFLAT), `pipeline_ejecuciones`, `pipeline_errores`, `llm_logs` |
| `98_seed_demo.sql` | Datos de demostración (usa nombres de columna actualizados) |
| `99_seed_variables_zona.sql` | Seeds que escriben a las tablas `vz_*` (no a la tabla fat legacy) |

Las 37 migraciones legacy (001–036 + 2 seeds) están en `backend/db/legacy_migraciones/` y **no se ejecutan**; sirven solo de referencia histórica.

---

## Tipos canónicos

| Dato | Tipo PostgreSQL |
|---|---|
| Coordenadas (lat, lng, área) | `DOUBLE PRECISION` |
| Dinero (precio, alquiler) | `NUMERIC(12,2)` |
| Rating 0–5 | `NUMERIC(3,2)` |
| Score 0–100 | `NUMERIC(5,2)` |
| Probabilidad 0–1 | `NUMERIC(4,3)` |
| Embeddings NLP | `VECTOR(768)` |
| Geometría puntual | `GEOMETRY(POINT, 4326)` |
| Timestamps con TZ | `TIMESTAMPTZ` |

---

## Renames aplicados en el refactor

### Tablas renombradas

| Nombre legacy | Nombre actual |
|---|---|
| `llicencies_activitat` | `licencias_actividad` |
| `vianants_trams` | `trams_peatonales` |

### Columnas renombradas

| Tabla | Columna legacy | Columna actual |
|---|---|---|
| `locales` | `disponible` | `esta_disponible` |
| `exportaciones` | `descargado` | `fue_descargado` |
| `modelos_versiones` | `activo` | `es_activo` |
| `alertas_zona` | `activa` | `esta_activa` |
| `paradas_transporte` | `accesible_pmr` | `es_accesible_pmr` |
| `negocios_historico` | `activo_3_anos` | `sobrevivio_3a` |
| `negocios_activos` | `num_resenas` | `total_resenas` |
| `negocios_activos` | `activo` | `es_activo` |
| `resenas` | `score_categoria` | `confianza_categoria` |
| `llm_logs` | `endpoint` | `endpoint_nombre` |
| `cache_geocoding` | `precision` | `nivel_precision` |
| `estaciones_bicing` | `activa` | `esta_activa` |
| `venues_ocio` | `activo` | `es_activo` |
| `alojamientos_turisticos` | `activo` | `es_activo` |
| `inmuebles_portales` | `activo` | `es_activo` |
| `inmuebles_portales` | `escaparate` | `es_escaparate` |
| `inmuebles_portales` | `esquina` | `es_esquina` |
| `inmuebles_portales` | `sotano` | `es_sotano` |

**Convención de prefijos booleanos:**
- `es_` — estado permanente o clasif. estructural (`es_activo`, `es_escaparate`)
- `esta_` — estado transitorio (`esta_disponible`, `esta_activa`)
- `fue_` — acción pasada completada (`fue_descargado`)
- `sobrevivio_` — evento histórico binario (`sobrevivio_3a`)

---

## Arquitectura variables_zona

El antiguo modelo de tabla fat (`variables_zona` con 80+ columnas) se sustituyó por una tabla coordinadora delgada + 5 tablas satélite en tercera forma normal.

```
variables_zona (coordinadora)
  zona_id + fecha  ← clave compuesta única
  num_lineas_transporte, num_paradas_transporte, score_transporte_calculado
        │
        ├── vz_flujo        flujo peatonal (mañana/tarde/noche/total),
        │                   Popular Times, VCity, patrones temporales,
        │                   estacionalidad (verano/navidad/rebajas)
        │
        ├── vz_turismo      score_turismo, Airbnb density/occupancy,
        │                   Booking, cruceros, landmarks, POIs culturales
        │
        ├── vz_demografia   población, edad, rentas (media/mediana/uc),
        │                   extranjeros, educación, desigualdad (gini, p80/p20),
        │                   estructura hogar
        │
        ├── vz_comercial    negocios activos, vacíos, rotación,
        │                   ratio_locales_comerciales, licencias nuevas/bajas,
        │                   google_review_count_medio
        │
        └── vz_entorno      seguridad (hurtos/robos/daños), ruido, zonas verdes,
                            equipamientos, comisarías, mercados municipales

Vista v_variables_zona = LEFT JOIN de las 5 satélites sobre variables_zona.
El orden de columnas es canónico: scoring/features.py las lee por nombre.
```

**Regla importante:** Los 17 pipelines escriben **solo** a las tablas `vz_*` (sin dual-write a la tabla fat). La vista `v_variables_zona` mantiene compatibilidad de lectura para el código Python existente.

---

## Dominio 02 — Geografía

### distritos

| Columna | Tipo | Descripción |
|---|---|---|
| `id` | SERIAL PK | |
| `codigo` | VARCHAR(4) UNIQUE | Código oficial del distrito |
| `nombre` | VARCHAR(200) | |
| `geometria` | GEOMETRY(MULTIPOLYGON,4326) | NULL permitido (seeds sin geometría) |

### barrios

Subdivisión de distrito. FK `distrito_id → distritos.id`.

### zonas

Unidad mínima de análisis. `id` es clave natural VARCHAR(30). FK `barrio_id → barrios.id`.

---

## Dominio 03 — Inmuebles

### locales

Locales comerciales puntuales, uno por anuncio o registro manual.

| Columna relevante | Notas |
|---|---|
| `esta_disponible` | Renombrado desde `disponible` |
| `lat`, `lng` | `DOUBLE PRECISION`, obligatorios |
| `m2` | CHECK `> 0` si se especifica |
| `alquiler_mensual` | `NUMERIC(10,2)` |

### inmuebles_portales

Anuncios scrapeados de portales (Idealista, Fotocasa, Habitaclia, Milanuncios, Pisos, Booking).

| Columna relevante | Notas |
|---|---|
| `es_activo` | Renombrado desde `activo` |
| `es_escaparate` | Renombrado desde `escaparate` |
| `es_esquina` | Renombrado desde `esquina` |
| `es_sotano` | Renombrado desde `sotano` |
| `precio` | `NUMERIC(12,2)` |
| `precio_m2` | `NUMERIC(10,2)` |

### cache_geocoding

Caché de geocodificación. PK = `direccion_normalizada`.

- `nivel_precision`: renombrado desde `precision` (keyword SQL). Valores: `rooftop`, `range_interpolated`, `geometric_center`, `approximate`.

### Vista v_mercado_zona

Resumen de mercado por zona: `num_anuncios_activos`, `precio_m2_medio/min/max_alquiler`, `num_con_escaparate`, `num_en_esquina`, `ultimo_scraping`.

---

## Dominio 04 — Variables por zona

Ver sección "Arquitectura variables_zona" más arriba.

La vista `v_variables_zona` expone exactamente las mismas columnas que la antigua tabla fat. El orden de columnas no debe modificarse sin actualizar `scoring/features.py → FEATURE_NAMES`.

---

## Dominio 05 — Sectores y scoring

### sectores

5 sectores predefinidos: `restauracion`, `tatuajes`, `moda`, `estetica`, `shisha_lounge`. Pesos de scoring manual normalizados a suma=1.0 (verificado con `RAISE EXCEPTION` en el propio SQL).

### modelos_versiones

Registro de versiones XGBoost. `es_activo` (renombrado desde `activo`). Índice único parcial garantiza un solo modelo activo por sector.

### negocios_historico

Datos de entrenamiento del modelo. `sobrevivio_3a` (renombrado desde `activo_3_anos`) es la label del clasificador XGBoost (TRUE = negocio abierto ≥ 3 años).

### scores_zona

Score global + 8 dimensiones por zona × sector × versión de modelo. `probabilidad_supervivencia NUMERIC(4,3)` = salida directa XGBoost. `shap_values JSONB` = explicabilidad por feature.

### dim_calendario_bcn

Calendario local de Barcelona: festivos nacionales/catalanes/locales, vacaciones escolares, puentes, estación, rebajas.

---

## Dominio 06 — Competencia

### negocios_activos

Todos los negocios activos de Barcelona. Fuente: Google Places → Foursquare → Yelp → OSM → Cens Locals.

| Columna relevante | Notas |
|---|---|
| `es_activo` | Renombrado desde `activo` |
| `total_resenas` | Renombrado desde `num_resenas`; `review_count` se conserva por compat. legacy |
| `rating` | `NUMERIC(3,2)`, rango 0–5 |
| `popular_times` | JSONB, histograma horario de Google |
| `subsector_codigo` | FK a `subsectores.codigo` (25 subsectores) |

### competencia_detalle_zona

Análisis avanzado v2 con métricas de aglomeración (cluster_score), vulnerabilidad (hhi_index, pct_vulnerables) y síntesis (score_competencia_v2). Radio 500m por defecto.

### Vista v_competencia_zona

Une `competencia_por_local` (v1) con `competencia_detalle_zona` (v2). Usar `score_competencia_v2` en código nuevo.

---

## Dominio 07 — Transporte

### paradas_transporte

- `es_accesible_pmr`: renombrado desde `accesible_pmr`.

### estaciones_bicing

- `esta_activa`: renombrado desde `activa`.

Otras tablas del dominio: `lineas_transporte` (metro/bus/tram/fgc/rodalies + subtipo), `paradas_lineas` (N:M paradas↔líneas), `frecuencias_transporte` (minutos entre vehículos por franja), `carriles_bici` (LINESTRING).

---

## Dominio 08 — Entorno y turismo

### Tablas renombradas

- `licencias_actividad` — antes `llicencies_activitat` (CKAN BCN, licencias de actividad comercial).
- `trams_peatonales` — antes `vianants_trams` (tramos peatonales con aforadores de BCN).

### venues_ocio

- `es_activo`: renombrado desde `activo`.
- `rating`: `NUMERIC(3,2)`, rango 0–5.
- `tipo`: festival, sala_conciertos, teatro, museo, mercado, playa, parque.

### alojamientos_turisticos

- `es_activo`: renombrado desde `activo`.

Otras tablas: `parques_amb`, `comisarias`, `mercados_municipales`, `landmarks_turisticos`, `intensitat_turismo_oficial`, `dinamismo_zonal`.

### Vista v_dinamismo_zona

Dinámica comercial histórica: licencias nuevas/bajas, supervivencia, HHI, tendencia.

---

## Dominio 09 — Usuario y financiero

### sesiones

Cookie HttpOnly con UUID v4. `ip_hash` = SHA-256 de la IP (RGPD art. 25). `perfil JSONB` acumula el contexto del cuestionario.

### exportaciones

- `fue_descargado`: renombrado desde `descargado`.

### Vista v_parametros_financieros_actuales

Une `parametros_financieros_zona` con `benchmarks_sector` para devolver los parámetros financieros vigentes sin lógica en Python.

---

## Dominio 10 — IA y pipelines

### resenas

- `confianza_categoria NUMERIC(4,3)`: renombrado desde `score_categoria`. Rango 0–1.
- `embedding VECTOR(768)`: índice IVFFLAT para búsqueda semántica.
- Fuentes: google, foursquare, yelp.

### alertas_zona

- `esta_activa`: renombrado desde `activa`.

### perfiles_zona_embedding

Perfil semántico de zona. Índice IVFFLAT sobre `VECTOR(768)`.

### llm_logs

- `endpoint_nombre`: renombrado desde `endpoint` (keyword SQL reservado).

### pipeline_ejecuciones / pipeline_errores

Trazabilidad de todos los pipelines de datos. Todos los pipelines deben registrar inicio + fin + estado.

---

## Índices notables

| Índice | Tabla | Notas |
|---|---|---|
| GIST geometría | `zonas`, `barrios`, `distritos`, `locales`, `negocios_activos`, etc. | Búsquedas espaciales ST_DWithin, ST_Within |
| GIN JSONB | `scores_zona.shap_values`, `negocios_activos.popular_times`, `sesiones.perfil` | Búsqueda dentro de JSONB |
| GIN trigrama | `negocios_activos.nombre` | ILIKE acelerado con pg_trgm |
| IVFFLAT vector | `resenas.embedding`, `perfiles_zona_embedding.embedding` | Búsqueda semántica aproximada |
| Parcial es_activo | `negocios_activos`, `modelos_versiones` | Solo filas activas en índices espaciales y de unicidad |

---

## Notas operativas

- **Reconstruir esquema completo:** `docker compose down -v && docker compose up --build`. Los 10 archivos + 2 seeds se aplican en orden.
- **Migraciones legacy:** `backend/db/legacy_migraciones/` contiene los archivos originales 001–036. No se montan en Docker; son solo referencia histórica.
- **Variables de zona:** nunca escribir directamente a `variables_zona` salvo el bloque de transporte legacy. Usar siempre las tablas `vz_*`.
- **Constraints nombrados:** toda FK, unique y check lleva prefijo `fk_`, `uq_`, `ck_`, `pk_`. Facilita la identificación en logs de error.
