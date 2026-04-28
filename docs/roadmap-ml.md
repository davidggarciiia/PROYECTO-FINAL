# Roadmap ML — GeoRetail

Este documento describe el plan de evolución de la capa de Machine Learning del proyecto, organizado en 6 fases. La fase 1 (backtest del modelo XGBoost) está detallada al final como plan de implementación inmediato; las fases 2-6 son el roadmap a medio plazo.

## Contexto

Hoy el modelo XGBoost predice supervivencia comercial a 3 años desde 58 features raw, pero la evaluación que existe es agregada (global o por sector) — `georetail/backend/scoring/ml/evaluate.py:60` y `:120`. **No hay forma de auditarlo a nivel de negocio individual** ni de saber qué dimensiones (flujo, demografía, competencia…) empujaron a favor o en contra de cada predicción concreta.

La fase 1 construye un **pipeline de backtest** que recorre cada negocio de `negocios_historico`, predice su probabilidad de supervivencia con la snapshot de features que tenía en su `fecha_apertura` (sin data leakage), agrupa los SHAP values por dimensión, lo compara contra la label real (`sobrevivio_3a`) y persiste los resultados.

Esto da: (1) métricas reales por sector/zona (no sintéticas), (2) explicación dimensional de cada predicción, (3) detección de sectores donde el modelo falla, y (4) tabla auditable que el frontend puede consumir.

**Estado actual del código (verificado):**
- ✅ `construir_dataset()` en `scoring/ml/dataset.py:73` ya devuelve `X, y, meta` con `negocio_id, zona_id, sector, fecha_apertura, fecha_cierre, label`.
- ✅ `group_shap_by_dimension()` en `scoring/explainability.py:232` ya existe y es funcional. Usa el mapping `FEATURE_TO_DIMENSION` (línea 123) que cubre las 8 dimensiones.
- ✅ `obtener_modelo_activo()` y `obtener_feature_names_modelo()` en `scoring/infra/model_registry.py`.
- ✅ `slice_feature_matrix()` en `scoring/infra/governance.py` para reducir features al subset que el modelo conoce.
- ❌ **NO existe** función que itere negocio por negocio con SHAP individual.
- ❌ **NO existe** tabla para persistir backtest por negocio.
- ❌ **NO existe** endpoint API.
- ⚠️ Estado de `negocios_historico` desconocido — depende de si se han ejecutado `pipelines/comercio/cens_comercial.py` y/o `registre_mercantil.py`.

---

## Roadmap ML — 6 fases

Cada fase añade un paradigma ML distinto y mantiene **coherencia con las 8 dimensiones existentes** (ver sección "Coherencia con las 8 dimensiones" más abajo).

### Fase 1: Backtest + atribución dimensional (plan de implementación inmediato)
- **Paradigma**: validación de modelos supervisados + interpretabilidad SHAP.
- Backtest negocio a negocio del XGBoost actual con SHAP por dimensión persistido en BD.
- **Detalle de implementación**: ver sección "Plan de implementación de la Fase 1" al final.
- **Coherencia dimensional**: la salida ya está agrupada por las 8 dimensiones vía `group_shap_by_dimension`.

### Fase 2: Calibración de probabilidades
- **Paradigma**: calibración probabilística (Platt scaling / isotonic regression).
- Envolver XGBoost con `CalibratedClassifierCV(method="isotonic", cv=5)` en `scoring/ml/train.py`.
- Asegurar que un `prob = 0.78` ≡ 78% real → recalcular Brier antes/después.
- **Archivos modificados**: `scoring/ml/train.py` (~50 líneas), `scoring/scorer.py` (cargar el calibrador igual que el modelo).
- **Coherencia dimensional**: ninguna alteración. Solo afina la probabilidad final que ya alimenta `score_global = prob × 100`.
- Esfuerzo: ~2h.

### Fase 3: Clustering + Embeddings de zona (juntas, comparten infra)
- **Paradigma**: aprendizaje **no supervisado** + **representation learning** + reducción dimensional.
- **3a — Embeddings de zona**:
  - Autoencoder PyTorch (32 → 16 → 8 dim) o PCA inicial sobre las 58 features normalizadas.
  - Persistir el vector latente en `zonas.embedding VECTOR(16)` (pgvector ya está activo).
  - Permite nearest-neighbor con índice IVFFLAT: "zonas similares a esta" en <5 ms.
- **3b — Clustering**:
  - HDBSCAN sobre el espacio latente (o sobre las 58 raw como variante para comparar).
  - Persistir `cluster_id INT` + `cluster_label VARCHAR(60)` en `zonas`.
  - Etiquetas humanas asignadas inspeccionando los centroides ("eje comercial premium", "barrio dormitorio", "zona turística saturada"…).
- **3c — Visualización UMAP**: notebook + endpoint que devuelve coordenadas 2D para pintar el mapa de zonas BCN agrupadas.
- **Coherencia dimensional**:
  - El embedding **es una compresión de las 58 features = de las 8 dimensiones**. Dos zonas próximas en embedding → perfiles dimensionales similares.
  - El cluster_label se caracteriza describiendo las medias dimensionales de sus miembros ("este cluster: flujo alto, demografía media-alta, competencia alta, turismo alto").
  - **Las 8 dimensiones siguen siendo el lenguaje del usuario**; cluster y embedding son herramientas internas + capa de "zonas similares".
- **Archivos**:
  - Nuevo `pipelines/clustering_zonas.py` (mensual).
  - Nuevo `scoring/ml/embeddings_zona.py`.
  - Migración `12_embeddings_clusters.sql` (columnas en `zonas`).
  - Endpoints `GET /api/zonas/{id}/similares?k=10` y `GET /api/zonas/clusters`.
- Esfuerzo: medio-alto, ~400 líneas total entre los dos.

### Fase 4: Recomendador zona ↔ sector
- **Paradigma**: filtrado colaborativo / matrix factorization (sistemas de recomendación).
- Construir matriz `(zona × sector) → ratio_supervivencia` desde `negocios_historico`.
- Entrenar **LightFM** (`pip install lightfm`) o **`implicit` ALS** → vectores latentes de zona y sector.
- **Híbrido**: añadir features de zona (las 58) y de sector (taxonomía de `concepto/`) como side-information → mejora cold-start.
- Output:
  - `GET /api/sectores/{codigo}/zonas-recomendadas?k=20` → top zonas para un sector.
  - `GET /api/zonas/{id}/sectores-recomendados?k=10` → top sectores para una zona (útil cuando el usuario no sabe qué montar).
- **Coherencia dimensional**: el recomendador devuelve **ranking**; cada zona/sector recomendado se sigue mostrando con sus 8 dimensiones intactas.
- **Archivos**:
  - Nuevo `scoring/ml/recomendador.py`.
  - Migración `13_recomendador.sql` (tabla `recomendador_modelo` con vectores latentes).
  - Endpoints en `api/zonas.py` / `api/sectores.py`.
- Esfuerzo: medio, ~300 líneas.

### Fase 5: Topic modeling de reseñas (BERTopic)
- **Paradigma**: **NLP avanzado / topic modeling** sobre embeddings.
- Aplicar **BERTopic** (`pip install bertopic`) sobre los embeddings sentence-transformers ya almacenados en `resenas.embedding`.
- BERTopic combina UMAP + HDBSCAN + c-TF-IDF para extraer temas con palabras clave.
- Por zona: agregar reseñas → distribución de temas + sentimiento por tema.
- **Coherencia dimensional — INTEGRACIÓN COMPLETA** (decisión tomada):
  - Cada tema descubierto se mapea a **una o varias de las 8 dimensiones existentes** vía un dict editable `TOPIC_TO_DIMENSION`:
    - "ruido", "delincuencia", "limpieza" → `seguridad`
    - "precio caro/barato" → `precio_alquiler`
    - "transporte", "aparcamiento" → `transporte`
    - "ambiente", "decoración", "moderno", "auténtico" → `dinamismo`
    - "turistas", "guiris" → `turismo`
    - "calidad/precio percibido", "servicio" → feature derivada que entra a `competencia` o `dinamismo`
  - El sentimiento agregado por tema → genera una **feature nueva** `tema_X_sentimiento` que se añade a `FEATURE_NAMES` y `FEATURE_TO_DIMENSION` en `explainability.py`.
  - Si un tema no es mapeable, se descarta (no se rompe el esqueleto de las 8 dimensiones).
- **Archivos**:
  - Nuevo `pipelines/nlp/topic_modeling.py` (mensual).
  - Migración `14_topics.sql` (tabla `topics` + `tema_zona` con sentimiento).
  - Modificar `scoring/explainability.py` para añadir el mapping topic→dimensión.
  - Modificar `scoring/features.py` para añadir features `tema_X_sentimiento_*`.
- Esfuerzo: medio-alto, ~400 líneas.

### Fase 6: Anomaly detection (quick win opcional)
- **Paradigma**: **detección de anomalías** (outlier detection).
- `IsolationForest` o `LocalOutlierFactor` sobre las features de los negocios cerrados.
- Identificar cierres "inesperados" (features buenas pero el negocio cerró igual) → señal de factor oculto / cambio de barrio.
- Usos:
  - Limpieza del dataset de entrenamiento (descartar outliers ruidosos).
  - Flag visual en frontend "esta zona tiene cierres atípicos, investigar".
- **Coherencia dimensional**: marca anomalías sobre el espacio de features = dimensiones agregadas. No genera dimensiones nuevas.
- **Archivos**:
  - Función nueva en `scoring/ml/anomalies.py` (~80 líneas).
  - Reutiliza `construir_dataset()` y `FEATURE_NAMES`.
- Esfuerzo: bajo, ~3-4h.

---

## Coherencia con las 8 dimensiones

Las **8 dimensiones** (flujo, demografía, competencia, transporte, seguridad, turismo, dinamismo, precio_alquiler) son el lenguaje del usuario y la columna vertebral conceptual de la app. Toda fase ML del roadmap se diseña respetándolas:

| Fase | ¿Cómo se relaciona con las dimensiones? | ¿Crea dimensiones nuevas? |
|---|---|---|
| 1 — Backtest + SHAP | El SHAP de cada predicción se agrupa por dimensión vía `FEATURE_TO_DIMENSION` (`explainability.py:123`) | No |
| 2 — Calibración | Solo afina la probabilidad final del XGBoost. No toca features ni dimensiones | No |
| 3 — Embeddings + Clustering | El embedding **comprime** las 58 features (= dimensiones agregadas). Los clusters se **describen** mediante medias dimensionales | No |
| 4 — Recomendador | Es un **ranker** que ordena zonas/sectores. Cada elemento del ranking se sigue mostrando con sus 8 dimensiones | No |
| 5 — Topic modeling | Cada tema se **mapea** a una dimensión existente vía `TOPIC_TO_DIMENSION`. Genera features derivadas que entran a las dimensiones | No (enriquece existentes) |
| 6 — Anomaly detection | Marca puntos atípicos en el espacio de features = espacio dimensional. No reemplaza nada | No |

**Conclusión**: las 8 dimensiones son el **esqueleto interpretativo** que el usuario ve; los modelos ML añadidos son **capas de inteligencia** que enriquecen sin contradecir ese esqueleto. Esto da un proyecto con **coherencia conceptual sólida** y al mismo tiempo **variedad ML defendible**.

---

## Cobertura ML final del proyecto tras las 6 fases

| Paradigma | Componente | Fase |
|---|---|---|
| Supervisado (clasificación binaria) | XGBoost supervivencia | Existente |
| Calibración probabilística | CalibratedClassifierCV isotonic | 2 |
| **No supervisado — clustering** | **HDBSCAN sobre embeddings** | **3** |
| **Representation learning** | **Autoencoder/PCA → vector zona en pgvector** | **3** |
| Reducción dimensional + visualización | UMAP 2D para memoria | 3 |
| **Sistemas de recomendación** | **LightFM matrix factorization** | **4** |
| **NLP avanzado / topic modeling** | **BERTopic sobre reseñas** | **5** |
| **Detección de anomalías** | **IsolationForest** | **6** |
| Interpretabilidad | SHAP global + por-dimensión + por-negocio | 1 |
| Validación de modelos | Backtest + CV 5-fold + métricas reales | 1 |
| Optimización de hiperparámetros | Optuna (TPE + MedianPruner) | Existente |
| Embeddings semánticos | sentence-transformers multilingüe | Existente |

→ El proyecto cubre **8 paradigmas ML distintos** + interpretabilidad + validación. Con coherencia dimensional explicable.

---

## Lo que NO entra en NINGUNA fase del roadmap

- Frontend para visualizar `backtest_negocios` (otro PR; el endpoint de fase 1 queda listo para que el front lo consuma).
- Re-entrenar el modelo. La fase 1 solo evalúa el modelo activo; la promoción/reentreno sigue en `train.py`.
- Reescribir `scorer.py` para que use SHAP-por-dimensión en `_score_xgboost` (ya lo hace indirectamente vía `build_llm_grounding_payload`).
- GNN sobre grafo zonas-negocios (descartado: alto esfuerzo, baja garantía de mejora sobre XGBoost).
- Causal inference / uplift modeling (descartado: datos justos para identificación causal).
- Active learning del cuestionario (descartado para el roadmap actual; posible fase 7 si sobra tiempo).

---

## Plan de implementación de la Fase 1 (backtest)

### Archivos a crear / modificar

**Nuevos:**
1. `georetail/backend/db/migraciones/11_backtest.sql` — tabla `backtest_negocios` versionada por modelo.
2. `georetail/backend/scoring/ml/backtest.py` — núcleo del backtest (`correr_backtest` + helpers).
3. `georetail/backend/tests/test_backtest.py` — tests unitarios con datos sintéticos.

**Modificados:**
4. `georetail/backend/api/admin.py` — añadir 2 endpoints (`POST /api/admin/backtest/{version}` + `GET /api/admin/backtest/{version}`).
5. `georetail/backend/scoring/ml/evaluate.py` — añadir `evaluar_por_negocio()` que reutilice la lógica del backtest para CLI rápida.

### Esquema de la tabla nueva

```sql
CREATE TABLE backtest_negocios (
    id                 SERIAL PRIMARY KEY,
    modelo_version     VARCHAR(100) NOT NULL,
    negocio_id         INT NOT NULL,
    zona_id            VARCHAR(30),
    sector_codigo      VARCHAR(30),
    fecha_apertura     DATE NOT NULL,
    fecha_cierre       DATE,
    label_real         BOOLEAN NOT NULL,            -- sobrevivio_3a
    prob_predicha      NUMERIC(5,4) NOT NULL,       -- predict_proba clase 1
    pred_clase         BOOLEAN NOT NULL,            -- prob >= 0.5
    correcto           BOOLEAN NOT NULL,            -- pred_clase == label_real
    shap_por_dimension JSONB NOT NULL,              -- {dim: {contribucion, tendencia, top_features}}
    contribucion_total JSONB,                       -- {dim: contribucion} (rápido para queries)
    created_at         TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_backtest_negocio_version UNIQUE (modelo_version, negocio_id),
    CONSTRAINT fk_backtest_negocio FOREIGN KEY (negocio_id)
        REFERENCES negocios_historico(id) ON DELETE CASCADE,
    CONSTRAINT fk_backtest_modelo FOREIGN KEY (modelo_version)
        REFERENCES modelos_versiones(version) ON DELETE CASCADE
);
CREATE INDEX idx_backtest_modelo_sector ON backtest_negocios (modelo_version, sector_codigo);
CREATE INDEX idx_backtest_zona ON backtest_negocios (zona_id);
CREATE INDEX idx_backtest_correcto ON backtest_negocios (modelo_version, correcto);
```

### Lógica del backtest (`scoring/ml/backtest.py`)

```python
async def correr_backtest(
    version: Optional[str] = None,        # default = modelo activo
    sector: Optional[str] = None,         # filtrar por sector
    persistir: bool = True,
) -> dict:
    """
    1. Resolver modelo (versión o activo) -> ruta + feature_names del registry.
    2. Cargar dataset: X, y, meta = await construir_dataset(sector=sector).
    3. Slice: X_model = slice_feature_matrix(X, FEATURE_NAMES, feature_names).
    4. Cargar modelo: xgb.XGBClassifier().load_model(ruta).
    5. probs = modelo.predict_proba(X_model)[:, 1]
       preds = (probs >= 0.5).astype(int)
    6. SHAP batch: explainer.shap_values(X_model) (clase 1 si lista).
    7. Por cada fila i:
         shap_dict = {feature_names[j]: float(shap_vals[i, j])}
         dim_breakdown = group_shap_by_dimension(shap_dict)  # reutiliza explainability.py
         row = {
             modelo_version, negocio_id=meta.iloc[i].negocio_id,
             zona_id, sector, fecha_apertura, fecha_cierre,
             label_real=bool(y[i]), prob_predicha=float(probs[i]),
             pred_clase=bool(preds[i]), correcto=(preds[i]==y[i]),
             shap_por_dimension=dim_breakdown,
             contribucion_total={d: dim_breakdown[d]["contribucion"] for d in dim_breakdown},
         }
    8. Si persistir: INSERT batch en backtest_negocios (UPSERT por uq).
    9. Devolver resumen agregado: métricas globales + por sector + ranking dimensiones.
    """
```

**Funciones reutilizadas (sin duplicar código):**
- `scoring.ml.dataset.construir_dataset` — fuente de X/y/meta.
- `scoring.infra.model_registry.obtener_modelo_activo` / `obtener_feature_names_modelo`.
- `scoring.infra.governance.slice_feature_matrix` — subset features.
- `scoring.explainability.group_shap_by_dimension` — agregador SHAP→dimensión.
- `scoring.ml.evaluate.evaluar_modelo` — para el resumen agregado del retorno.

### Endpoints API (`api/admin.py`)

Detrás del header `X-Admin-Key` (mismo patrón que `_require_admin`):

- `POST /api/admin/backtest/{version}` — lanza el backtest y persiste. Body opcional `{ "sector": "restauracion" }`. Devuelve resumen.
- `GET  /api/admin/backtest/{version}` — lee `backtest_negocios` filtrando por sector/zona/`correcto`. Soporta paginación. Devuelve filas + agregados.

### CLI

Añadir flag `--backtest` a `scoring/ml/evaluate.py`:

```bash
python -m scoring.ml.evaluate --version restauracion_v3_20260310 --backtest [--sector restauracion]
```

### Pre-requisito: estado de `negocios_historico`

Antes de lanzar el backtest hay que verificar que la tabla está poblada:

```sql
SELECT sector_codigo, COUNT(*), SUM(sobrevivio_3a::int) AS supervivientes
FROM negocios_historico GROUP BY sector_codigo;
```

Si está vacía o tiene <200 negocios por sector (umbral mínimo en `dataset.py:81`):
- Ejecutar `POST /api/admin/pipelines/cens_comercial` (necesita CSVs en `/data/csv/cens_comercial/2016, 2022, 2024`).
- Y/o `POST /api/admin/pipelines/registre_mercantil` (API pública, sin credenciales).

### Verificación end-to-end

1. **Migración aplicada**: `docker compose down -v && docker compose up --build` reaplica los 10 SQL + el nuevo `11_backtest.sql`.
2. **Comprobar datos**:
   ```bash
   docker exec georetail-db psql -U postgres -d georetail -c \
     "SELECT sector_codigo, COUNT(*) FROM negocios_historico GROUP BY 1;"
   ```
3. **Lanzar backtest**:
   ```bash
   curl -X POST http://localhost:8000/api/admin/backtest/<version> \
     -H "X-Admin-Key: $ADMIN_API_KEY" \
     -H "Content-Type: application/json" \
     -d '{"sector": "restauracion"}'
   ```
4. **Consultar resultados**:
   ```bash
   curl "http://localhost:8000/api/admin/backtest/<version>?sector=restauracion&correcto=false&limit=20" \
     -H "X-Admin-Key: $ADMIN_API_KEY"
   ```
5. **Tests**:
   ```bash
   cd georetail/backend && pytest tests/test_backtest.py -v
   ```
   Cubre: SHAP por dimensión cuadra con `FEATURE_TO_DIMENSION`, métricas correctas con dataset sintético, persistencia idempotente (UPSERT no duplica).
6. **CLI rápida**:
   ```bash
   python -m scoring.ml.evaluate --version <v> --backtest --sector restauracion
   ```
   Imprime resumen con AUC global, top-3 sectores donde más falla y top dimensión más decisiva.

---

## Orden de ejecución sugerido

| Semana | Fase | Entregable |
|---|---|---|
| 1-2 | Fase 1 (backtest) | Tabla + endpoint + tests |
| 3 | Fase 2 (calibración) | Probabilidades calibradas en producción |
| 4-5 | Fase 3 (embeddings + clustering) | Vector + cluster por zona, UMAP visual |
| 6 | Fase 4 (recomendador) | Endpoints zona↔sector |
| 7 | Fase 5 (topic modeling) | Features derivadas integradas a dimensiones |
| 8 | Fase 6 (anomalies) + pulido | Flag de outliers + memoria final |
