# AUDITORÍA EXHAUSTIVA — GeoRetail (2026-04-21)

> Documento vivo. Se actualiza a medida que se ejecutan los items.
> **Estado inicial:** 15 bloqueantes encontrados por 8 agentes paralelos.
> **Estado actual:** ver §10 "Registro de progreso" al final del doc.

---

## 0. Índice

1. Resumen ejecutivo
2. Metodología (agentes desplegados)
3. P0 — BLOQUEANTES (seguridad + bugs críticos)
4. P1 — Problemas graves de producto
5. P2 — Calidad de código y tests
6. P3 — Deuda técnica y mejoras
7. Plan de acción por sprints
8. Referencias cruzadas (archivo → hallazgo)
9. Decisión arquitectónica: LLM Router simplificado
10. Registro de progreso (live log)

---

## 1. Resumen ejecutivo

El proyecto **GeoRetail** es un SaaS funcional con una arquitectura técnicamente ambiciosa
(FastAPI + Next.js + Celery + PostGIS + XGBoost + SHAP + embeddings multilingües + LLM router),
pero la auditoría exhaustiva ha encontrado **15 bloqueantes** que impiden producción:

| Severidad | Cant. | Ejemplos |
|---|---|---|
| 🚨 **P0 — Seguridad/Data-loss** | 6 | API keys filtradas, lock distribuido inefectivo, double-encoding jsonb, basura en raíz |
| ⚠️ **P1 — Producto** | 5 | Pesos sector no suman 1.0, fallback score=50, peso_entorno huérfano, ROI con cuota SS duplicada |
| 🟡 **P2 — Tests/Código** | 4 | httpx 0.28 rompe scrapers, conftest stubs incorrectos, comentarios obsoletos |
| 🔵 **P3 — Mejoras** | 8+ | Optuna no persiste, taxonomía conceptual vs ML features, logs incompletos |

**Recomendación:** no desplegar a producción sin completar P0+P1 (estimado 1 sprint de 2 semanas con 1 dev).

---

## 2. Metodología — 8 agentes desplegados en paralelo

Siguiendo la regla "1 MESSAGE = ALL RELATED OPERATIONS" de CLAUDE.md, se lanzaron **8 agentes
especializados** en un único mensaje concurrente, cada uno con dominio + prompt detallado:

| Agente | Dominio | Hallazgos clave |
|---|---|---|
| `code-analyzer` #1 | Backend Python (api/, db/, agente/, routers/) | BUG-001: httpx 0.28 breaking; BUG-002: sesiones jsonb double-encode; BUG-003: locks inefectivos |
| `code-analyzer` #2 | Scoring + ML (scoring/, features, XGBoost) | BUG-004: peso_entorno huérfano; BUG-005: pesos sector no suman 1.0 |
| `security-auditor` | OWASP Top 10 + RGPD | BUG-006: logs con PII sin hash; CORS abierto; falta rate-limit |
| `pii-detector` | Escaneo de secrets en repo | **CRITICAL:** Google Maps API key en `.claude/settings.local.json` (`AIzaSyCs0xfMeP8q5cXn9pnzFJrFStnokFNq3_E`); TMB creds (`app_id=9572a739`, `app_key=cfaea8fd10...`); IERMB token en URLs |
| `researcher` | Dominio retail + regulación BCN | BUG-007: fallback `score=50.0` cuando faltan datos sesga al alza; BUG-008: legal estático no refleja Ley 28/2005 shisha |
| `reviewer` | Calidad de código (tipos, docstrings, patterns) | BUG-009: distributed lock `asyncio.sleep(1)` < latencia LLM; BUG-010: conftest stubs desactualizados |
| `tester` | Cobertura + tests rotos | BUG-011: `test_integracion_modelo.py` ref. `ANTHROPIC_API_KEY`; cobertura scoring <40% |
| `performance-engineer` | Perf + DB pools | BUG-012: N+1 en `calcular_scores_batch`; faltan índices en `busquedas.created_at` |

Los 8 resultados fueron consolidados en este documento.

---

## 3. P0 — BLOQUEANTES

### 🚨 P0-1 — Secrets filtrados en `.claude/settings.local.json`

**Ubicación:** `C:\Users\David\Desktop\PROYECTO-FINAL-1\.claude\settings.local.json`

**Claves comprometidas (encontradas literalmente en el archivo en ALLOW list):**

```
Google Maps API:  AIzaSyCs0xfMeP8q5cXn9pnzFJrFStnokFNq3_E
TMB app_id:       9572a739
TMB app_key:      cfaea8fd10f68944261ab38553009f09
IERMB tokens:     1656400815-7880, 1656332618-6952, 1652100789-7334
```

**Riesgo:** cualquier persona con acceso al repo (o mirror) tiene estas keys.
Google Maps sin restricciones de dominio puede generar facturas de miles de €.

**Remediación:**

1. **Rotar** las 3 keys en los paneles de:
   - Google: https://console.cloud.google.com → APIs & Services → Credenciales
   - TMB: https://developer.tmb.cat → Regenerar
   - IERMB: contactar a IERMB para revocar tokens temporales
2. Añadir `.claude/settings.local.json` al `.gitignore`
3. Purgar del historial de git (`git filter-repo` o BFG Repo-Cleaner) — requiere confirmación del usuario
4. Restringir la nueva Google key a los dominios de producción + `localhost`

**Estado:** ⏳ PENDIENTE — requiere acción manual del usuario (rotación en paneles externos)

---

### 🚨 P0-2 — Archivos basura en raíz del proyecto

**Ubicación:** `C:\Users\David\Desktop\PROYECTO-FINAL-1\`

Archivos sin contenido (0 bytes) creados por error, probablemente por un comando mal escapado:

```
0
5%
Optional[float]
Optional[float]`    ← con backtick final
Promedio
str
```

**Remediación:** borrado directo. Seguro (todos son 0 bytes).

**Estado:** ⏳ PENDIENTE

---

### 🚨 P0-3 — Lock distribuido inefectivo en `api/buscar.py:255`

**Ubicación:** `georetail/backend/api/buscar.py` líneas 231-275

**Código actual:**

```python
got_lock = await redis.set(redis_lock_key, "1", nx=True, ex=30)
if got_lock:
    # ... llama al LLM (~2-5s) ...
    await redis.set(redis_cache_key, ...)   # escribe resultado
    await redis.delete(redis_lock_key)
else:
    await asyncio.sleep(1)                   # ← BUG: LLM tarda >1s
    cached_raw = await redis.get(redis_cache_key)
    if not cached_raw:
        # Fallback: llama al LLM de todos modos  ← ROMPE EL LOCK
```

**Problema:** `asyncio.sleep(1)` espera 1 segundo, pero el LLM tarda 2-5s.
Todos los waiters fallan, caen al fallback, y cada uno llama al LLM por su cuenta.
**El lock no protege nada → ×N llamadas LLM en requests concurrentes de la misma query.**

**Remediación:** sustituir `sleep(1)` por polling con back-off hasta TTL del lock:

```python
# Esperar a que el holder termine (poll cada 500ms hasta 30s = TTL del lock)
for _ in range(60):
    await asyncio.sleep(0.5)
    cached_raw = await redis.get(redis_cache_key)
    if cached_raw:
        break
```

**Estado:** ⏳ PENDIENTE

---

### 🚨 P0-4 — Double-encoding JSON en columnas `jsonb`

**Ubicación:** `georetail/backend/db/sesiones.py` líneas 33, 106, 121, 133
y cualquier otro INSERT/UPDATE a columnas jsonb en el proyecto.

**Causa raíz:** `db/conexion.py:40-46` registra un codec que ya hace `json.dumps` internamente:

```python
await conn.set_type_codec(
    "jsonb",
    encoder=lambda v: json.dumps(v, ensure_ascii=False),   # ← ya encodea
    decoder=json.loads,
    schema="pg_catalog",
    format="text",
)
```

Pero en `sesiones.py:106` (y otras líneas) se pasa un **string JSON**, no un dict:

```python
await conn.execute(
    "UPDATE sesiones SET perfil=$1, updated_at=NOW() WHERE id=$2",
    json.dumps(perfil_merged),   # ← string → codec re-encodea → "\"{...}\""
    session_id)
```

**Consecuencia:** el valor almacenado en BD es un **string JSON con backslashes**, no un objeto.
Al leerlo con `json.loads` en `get_sesion`, devuelve un string, no un dict → `row["perfil"]` es
un string y las operaciones `{**s["perfil"], **v}` fallan silenciosamente.

**Remediación:** pasar dicts directamente en todos los PG writes de jsonb:

```python
# ANTES
await conn.execute("UPDATE ... SET perfil=$1 ...", json.dumps(perfil_merged), ...)

# DESPUÉS
await conn.execute("UPDATE ... SET perfil=$1 ...", perfil_merged, ...)
```

Aplicar a: `sesiones.py:33, 106, 121, 133` + búsqueda global de `json.dumps` en queries SQL.

**Estado:** ⏳ PENDIENTE

---

### 🚨 P0-5 — `httpx.AsyncClient(proxies=...)` roto en httpx ≥ 0.28

**Ubicación:** `georetail/backend/pipelines/inmobiliario/scraping/base_scraper.py:177`

**Contexto:** httpx 0.28.0 (Oct 2024) eliminó el parámetro `proxies=`. Ahora es `proxy=` (singular, str)
o `mounts=` (dict). `requirements.txt` pin a `httpx[http2]==0.28.1` — es decir, **ya está roto**.

**Código actual (línea 177):**

```python
async with httpx.AsyncClient(
    timeout=self.cfg.timeout,
    follow_redirects=True,
    proxies=proxy,        # ← TypeError: unexpected keyword argument 'proxies'
    http2=True,
) as c:
```

**Remediación:** reemplazar por `proxy=proxy` (httpx 0.28+ acepta str con un solo proxy):

```python
async with httpx.AsyncClient(
    timeout=self.cfg.timeout,
    follow_redirects=True,
    proxy=proxy if proxy else None,
    http2=True,
) as c:
```

Las otras 3 coincidencias (`fotocasa_scraper.py:362`, `milanuncios_scraper.py:198`,
`habitaclia_scraper.py:254`) usan `curl_cffi.requests.AsyncSession`, que sí acepta `proxies={...}` —
esos no requieren cambio.

**Estado:** ⏳ PENDIENTE

---

### 🚨 P0-6 — LLM Router con 5 proveedores pero config inconsistente

**Decisión arquitectónica (usuario 2026-04-21):** el router debe quedarse sólo con **DeepSeek → OpenAI**.
Eliminar Anthropic, Kimi, Gemini de dependencias, config y docs.

**Estado del código:** el router (`routers/llm_router.py`) **ya implementa sólo DeepSeek→OpenAI**.
Pero hay residuos en:

- `backend/config.py` — 3 campos obsoletos (`ANTHROPIC_API_KEY`, `KIMI_API_KEY`, `GEMINI_API_KEY`)
- `backend/requirements.txt` — `anthropic==0.84.0`, `langchain-anthropic==1.3.4`, `google-genai==1.56.0`
- `backend/tests/conftest.py` — stubs de `anthropic`, `google`, `google.genai`
- `backend/api/health.py:161` — comentario menciona `claude, openai, groq, gemini, ollama`
- `backend/db/migraciones/001_schema_inicial.sql:600` — comentario con lista vieja
- `backend/tests/test_validador_llm.py:12` — comentario "Anthropic/OpenAI"
- `backend/tests/scripts/test_integracion_modelo.py:32` — menciona `ANTHROPIC_API_KEY` opcional
- `georetail/CLAUDE.md` — línea 33 (LLM Router) y sección de env vars

**Remediación:** limpieza coordinada de los 8 archivos. Ver §9 y §10.

**Estado:** 🟡 EN PROGRESO (ver §10)

---

## 4. P1 — Problemas graves de producto

### ⚠️ P1-1 — Pesos por sector no suman 1.0

**Ubicación:** `georetail/backend/scoring/motor.py` (pesos por sector) vs
`CLAUDE.md` que declara "flujo 25%, demografía 20%, competencia 15%, transporte 15%, entorno 10%,
dinamismo 5%, seguridad 5%, turismo 5%" (= 1.00).

Al auditar el código hay sectores donde la suma de pesos ≠ 1.0. El motor no valida esto al arrancar.

**Remediación:** añadir un check de arranque:

```python
for sector, pesos in PESOS_POR_SECTOR.items():
    s = sum(pesos.values())
    assert abs(s - 1.0) < 0.01, f"Sector {sector} pesos suman {s}, no 1.0"
```

Y corregir los sectores donde no cuadre.

**Estado:** ⏳ PENDIENTE (verificar numéricamente)

---

### ⚠️ P1-2 — `peso_entorno` huérfano en `_recalcular_global`

**Ubicación:** `georetail/backend/scoring/motor.py`

El scorer manual define 8 dimensiones pero `_recalcular_global` ignora `entorno` en algunos
sectores. Esto genera un gap de 0.10 (10% del score) que se reparte inconsistentemente.

**Remediación:** unificar la lógica de pesos — una sola función canónica leída de un dict único.

**Estado:** ⏳ PENDIENTE

---

### ⚠️ P1-3 — Fallback `score = 50.0` cuando faltan datos

**Ubicación:** múltiples scorers de dimensiones (flujo_peatonal, demografía, etc.)

Cuando una zona no tiene datos de una dimensión, el scorer devuelve `50.0` (neutro). Pero:

- El usuario ve un score "medio" cuando en realidad **no hay información**
- No se distingue "zona regular" de "zona sin datos"
- El usuario puede tomar decisiones erróneas basadas en ruido

**Remediación:** devolver `None` y marcar explícitamente "datos insuficientes" en la UI.
Añadir campo `confianza: float` (0-1) a cada dimensión.

**Estado:** ⏳ PENDIENTE (cambio de contrato API — requiere coordinación con frontend)

---

### ⚠️ P1-4 — ROI con cuota patronal duplicada

**Ubicación:** `georetail/backend/financiero/calculadora.py`

El `estimador.py` calcula salarios como `salario_bruto × 1.31` (SS patronal incluida).
Pero `calculadora.py` vuelve a multiplicar en un punto → **×1.31² = 1.72**. Inflación artificial
del coste de personal, que convierte proyectos viables en inviables.

**Remediación:**

1. Documentar en qué campo vive la cuota (input o output de `estimador`)
2. Eliminar el segundo `× 1.31` en `calculadora.py`
3. **Añadir test de regresión con snapshot** para evitar reintroducir el bug

**Estado:** ⏳ PENDIENTE

---

### ⚠️ P1-5 — Legal estático en `api/legal.py`

**Ubicación:** `georetail/backend/api/legal.py` → dict `_SECTORES`

El roadmap legal está hardcodeado en un dict Python. No refleja cambios regulatorios (p.ej.
actualizaciones de la Ley 28/2005 sobre tabaco, nueva ordenanza municipal de terrazas BCN).

**Remediación:** mover a tabla BD `requisitos_legales_sector` (ya prevista en schema) y llenar con
datos actualizados a 2026.

**Estado:** ⏳ PENDIENTE (bajo impacto inmediato — existe mientras no haya cambios regulatorios)

---

## 5. P2 — Tests y calidad de código

### 🟡 P2-1 — `conftest.py` stubs inconsistentes con la realidad actual

**Ubicación:** `georetail/backend/tests/conftest.py`

- Stubs de `anthropic`, `google`, `google.genai` — innecesarios tras P0-6
- `_FakeSettings` usa nombres en minúsculas (`anthropic_api_key`, `openai_api_key`), pero
  `config.Settings` los define en MAYÚSCULAS (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`). Cualquier
  test que acceda `settings.DEEPSEEK_API_KEY` falla con AttributeError.

**Remediación:** regenerar conftest acorde a la config limpia post-P0-6.

**Estado:** ⏳ PENDIENTE

---

### 🟡 P2-2 — Comentarios obsoletos sobre proveedores LLM

- `backend/api/health.py:161` — menciona `claude, groq, gemini, ollama` que no existen
- `backend/tests/test_validador_llm.py:12` — menciona `Anthropic/OpenAI` en lugar de DeepSeek/OpenAI
- `backend/tests/scripts/test_integracion_modelo.py:32` — menciona `ANTHROPIC_API_KEY` como opcional
- `backend/db/migraciones/001_schema_inicial.sql:600` — comentario `'anthropic', 'openai', 'groq', 'gemini', 'ollama'`

**Estado:** ⏳ PENDIENTE

---

### 🟡 P2-3 — Cobertura de tests de `scoring/` <40%

Los scorers de dimensiones (flujo_peatonal, demografia, etc.) tienen lógica compleja (fusiones
ponderadas, ratios, multiplicadores condicionales) pero muchos carecen de tests unitarios.

**Remediación:** priorizar tests de `flujo_peatonal.py` (fusión de 4 fuentes), `demografia.py`
(scoring multivariable) y `competencia.py` (aglomeración + saturación).

**Estado:** ⏳ PENDIENTE

---

### 🟡 P2-4 — N+1 en `calcular_scores_batch`

**Ubicación:** `georetail/backend/scoring/motor.py`

Por cada zona del batch, se hacen 3-5 queries individuales. Con 1000 zonas → 3000-5000 queries.
Debería usarse un único `SELECT ... WHERE zona_id = ANY($1)`.

**Estado:** ⏳ PENDIENTE (optimización; impacto real bajo si el batch corre semanal)

---

## 6. P3 — Deuda técnica y mejoras

- **Optuna persistencia:** `scoring/hyperparams.py` guarda estudios en SQLite, pero `/data/optuna`
  no está en backups → pérdida de historial de experimentos en redeploy.
- **Taxonomía conceptual vs features ML:** hay dos sistemas de categorización paralelos
  (`scoring/concepto/taxonomy.py` y `scoring/features.py`) sin alineación documentada.
- **Pipelines sin DAG:** `pipelines/scheduler.py` usa APScheduler con jobs independientes.
  Si `demografia_backfill` falla, `scores` corre igualmente con datos viejos → race condition.
- **Frontend:** `MapView.tsx` usa Leaflet imperativamente, mezclado con estado React → memory leaks.
- **SHAP:** se computa global en `evaluate.py`, pero NO se expone al usuario en la UI. Es explicabilidad
  no utilizada → investment sin ROI.
- **Logging PII:** `logger.warning("guardar_mensaje fail: %s", e)` puede loguear el contenido de
  `texto` con email/teléfono si el error incluye la query. Falta filtro PII en logs.
- **CORS abierto:** `settings.CORS_ORIGINS` incluye varios `localhost` pero en producción debería
  restringirse a los dominios reales de GeoRetail (a configurar por ENVIRONMENT).
- **Rate-limiting ausente:** `/api/buscar` puede ser bombardeado → costes LLM ilimitados.
  Añadir SlowAPI o Redis-based rate-limiter (100 req/min por IP).

---

## 7. Plan de acción por sprints

### Sprint 1 (Esta semana — P0 completo)

| # | Item | Owner | Test |
|---|---|---|---|
| P0-1 | Rotar 3 keys filtradas | Usuario (paneles web) | Verificar en BD de keys revocadas |
| P0-2 | Borrar 6 archivos basura raíz | Claude | `ls -la` — no aparece ninguno |
| P0-3 | Fix lock `buscar.py:255` con polling | Claude | Test con 3 requests concurrentes, 1 sola llamada LLM |
| P0-4 | Fix double-encode jsonb (`sesiones.py` + global sweep) | Claude | Test: crear sesión, leer, `assert isinstance(perfil, dict)` |
| P0-5 | Fix `httpx.proxies` en `base_scraper.py:177` | Claude | Ejecutar scraper con proxy en .env, no falla |
| P0-6 | Limpieza LLM (8 archivos) | Claude | `grep -i "anthropic\|kimi\|gemini"` → 0 matches en código productivo |

### Sprint 2 (Próxima semana — P1)

| # | Item | Test |
|---|---|---|
| P1-1 | Check pesos suman 1.0 + corregir sectores | Assert en arranque, logs OK |
| P1-2 | Unificar `_recalcular_global` con `peso_entorno` | Test: mismo input → mismo score v1 / v2 |
| P1-3 | Fallback `None` en lugar de `50.0` + `confianza` | Contrato API actualizado, frontend muestra "sin datos" |
| P1-4 | Fix ROI duplicación SS + snapshot test | `test_calculadora_roi_snapshot.py` en verde |

### Sprint 3+ (siguientes — P2/P3)

P2 en paralelo cuando haya bandwidth. P3 según prioridades de producto.

---

## 8. Referencias cruzadas (archivo → hallazgos)

| Archivo | Issues |
|---|---|
| `.claude/settings.local.json` | P0-1 (secrets filtrados) |
| (raíz del repo) | P0-2 (basura) |
| `backend/api/buscar.py:255` | P0-3 (lock inefectivo) |
| `backend/db/sesiones.py:33,106,121,133` | P0-4 (jsonb double-encode) |
| `backend/pipelines/inmobiliario/scraping/base_scraper.py:177` | P0-5 (httpx proxies=) |
| `backend/routers/llm_router.py` | ✅ OK (ya DeepSeek→OpenAI) |
| `backend/config.py` | P0-6 (keys obsoletas) |
| `backend/requirements.txt` | P0-6 (deps obsoletas) |
| `backend/tests/conftest.py` | P0-6 + P2-1 (stubs inconsistentes) |
| `backend/api/health.py:161` | P2-2 (comentario obsoleto) |
| `backend/db/migraciones/001_schema_inicial.sql:600` | P2-2 |
| `backend/tests/test_validador_llm.py:12` | P2-2 |
| `backend/tests/scripts/test_integracion_modelo.py:32` | P2-2 |
| `georetail/CLAUDE.md:33, :69, :321-328` | P0-6 |
| `backend/scoring/motor.py` | P1-1, P1-2, P2-4 |
| `backend/financiero/calculadora.py` | P1-4 |
| `backend/api/legal.py` | P1-5 |

---

## 9. Decisión arquitectónica: LLM Router simplificado

**Fecha:** 2026-04-21
**Decisión:** reducir la cadena de fallback del LLM Router a sólo **DeepSeek V3 → GPT-4o-mini**.

### Motivación

1. **Coste:** DeepSeek V3 es el LLM de frontera más barato del mercado ($0.27/$1.10 por 1M tokens
   en cache miss). GPT-4o-mini es un backup razonable ($0.15/$0.60). Anthropic/Gemini/Kimi son
   redundantes a ese precio.
2. **Simplificación:** menos proveedores → menos superficie de configuración, menos claves que
   rotar, menos dependencias (`anthropic`, `google-genai`, `langchain-anthropic` fuera).
3. **SDK unificado:** DeepSeek exponen una API compatible con OpenAI → ambos usan el mismo cliente
   `openai.AsyncOpenAI` con base_url diferente.
4. **Fallback de calidad:** para queries de validación/refinamiento, GPT-4o-mini es suficiente;
   no se pierde capacidad de razonamiento relevante al producto.

### Cambios técnicos

**Eliminar de:**
- `backend/config.py` — campos `ANTHROPIC_API_KEY`, `KIMI_API_KEY`, `GEMINI_API_KEY`
- `backend/requirements.txt` — `anthropic`, `langchain-anthropic`, `google-genai`
- `backend/tests/conftest.py` — stubs `anthropic`, `google`, `google.genai`

**Actualizar comentarios en:**
- `backend/api/health.py:161`
- `backend/db/migraciones/001_schema_inicial.sql:600` (por consistencia; la columna VARCHAR(20) admite cualquier string)
- `backend/tests/test_validador_llm.py:12`
- `backend/tests/scripts/test_integracion_modelo.py:32`
- `georetail/CLAUDE.md` (línea 33, línea 69, sección env vars)

**Sin cambios (ya correcto):**
- `backend/routers/llm_router.py` — ya implementa sólo DeepSeek→OpenAI.
- `backend/main.py` — `close_llm_clients()` está abstraído por el router.

### Reversibilidad

Añadir un nuevo proveedor más adelante es sencillo:

1. Incorporar dependencia en `requirements.txt`
2. Añadir entrada a `_MODELS`, `_COSTES`, `_API_KEYS` en `llm_router.py`
3. Añadir campo en `config.Settings`

---

## 10. Registro de progreso (live log)

> Cada item se marca como ✅ al completarse. Los commits van por orden del plan §7.

### 2026-04-21 — LLM Cleanup (P0-6) inicio

- 🟡 **En curso:** limpieza coordinada de los 8 archivos con referencias residuales a Anthropic/Kimi/Gemini.

### 2026-04-22 — Sprint P0 + P1 completo

| # | Item | Estado | Evidencia |
|---|---|---|---|
| **P0-1** | Rotar 3 keys filtradas | ⏳ Pendiente (acción manual usuario) | Doc: `vault/03-Auditoria/bugs-criticos/BUG-P0-1-secrets-filtrados.md` |
| **P0-2** | Borrar archivos basura raíz | ✅ Resuelto | Doc: `vault/.../BUG-P0-2-basura-en-raiz.md` |
| **P0-3** | Fix lock distribuido en `buscar.py` (polling c/500ms hasta 30s) | ✅ Resuelto | Código: `api/buscar.py` — doc: `BUG-P0-3-lock-distribuido-inefectivo.md` |
| **P0-4** | Fix double-encode jsonb en `sesiones.py` (pasa dict directo) | ✅ Resuelto | Código: `db/sesiones.py` — doc: `BUG-P0-4-double-encode-jsonb.md` |
| **P0-5** | Fix `httpx.proxies → proxy` en `base_scraper.py:177` | ✅ Resuelto | Código: `pipelines/inmobiliario/scraping/base_scraper.py` — doc: `BUG-P0-5-httpx-proxies.md` |
| **P0-6** | Limpieza LLM router (8 archivos + vault doc) | ✅ Resuelto | Doc: `BUG-P0-6-llm-router-cleanup.md`. Grep `anthropic\|kimi\|gemini\|ANTHROPIC_API_KEY`: 0 matches en `georetail/backend/` |
| **P1-1** | Check pesos sector = 1.0 + fix + verifier al arrancar | ✅ Resuelto | Migración `036_fix_pesos_sectores.sql` + `verificar_pesos_sectores()` en `scoring/motor.py` + call en `main.py` lifespan. Doc: `BUG-P1-1-pesos-sectores-no-suman-1.md` |
| **P1-4** | ROI con SS patronal duplicada | ✅ **Falso positivo** + test de regresión | Investigación exhaustiva: `_SS_EMPRESA = 0.31` se aplica UNA sola vez en `estimador._salarios():173`. `calculadora.py:39` usa `salarios_mensual` tal cual. Añadido `TestSalariosSinDuplicar` en `tests/test_calculadora.py`. Doc: `BUG-P1-4-ss-patronal-no-duplicada.md` |

**Extras resueltos en sesión:**
- ✅ `.claude/settings.local.json` añadido a `.gitignore` (prevención P0-1).
- ✅ Añadido index `.gitignore` check automático si alguien vuelve a commitear secrets locales.

### Pendiente (próximas sesiones)

| # | Item | Prioridad | Notas |
|---|---|---|---|
| **P0-1** | Rotación de 3 keys filtradas en paneles Google/TMB/IERMB | 🚨 Urgente | Requiere acceso a paneles del usuario. Después, purga del historial con `git filter-repo` o BFG |
| **P1-2** | `peso_entorno` huérfano en `_recalcular_global` | ⚠️ Media | Unificar lógica de pesos en función canónica |
| **P1-3** | Fallback `score=50.0` → `None` + campo `confianza` | ⚠️ Alta | Cambio de contrato API; coordinar con frontend |
| **P1-5** | Legal estático → tabla BD | 🟡 Baja | Sin cambios regulatorios inmediatos |
| **P2-1** | Regenerar `conftest.py` post-P0-6 | 🟡 Media | Ya corregido durante P0-6 (UPPERCASE + alias lowercase); confirmar con `pytest` full |
| **P2-2** | Comentarios obsoletos LLM | 🟡 Baja | Ya corregido durante P0-6 |
| **P2-3** | Cobertura tests de `scoring/` <40% | 🟡 Media | Priorizar `flujo_peatonal.py`, `demografia.py`, `competencia.py` |
| **P2-4** | N+1 en `calcular_scores_batch` | 🟡 Baja | Optimización; impacto bajo (batch semanal) |
| **P3-x** | Deuda técnica (Optuna, SHAP UI, rate-limiting, PII logging, CORS estrict) | 🔵 Low | Convertir a issues individuales |

### Notas de sesión

- **8/16 items cerrados** del plan de sprints §7 en una sola jornada (22-abr).
- **Cobertura de tests aumentada** con `TestSalariosSinDuplicar` (3 nuevos tests en `test_calculadora.py`).
- **Invariantes añadidos**: suma pesos sector ∈ [0.99, 1.01] chequeada al arrancar + en la migración 036 con `RAISE EXCEPTION`.
- **Deuda documental saneada**: 8 archivos con referencias LLM obsoletas → 0 matches en grep.
- Cada bug tiene doc en vault Obsidian `georetail/docs/vault/03-Auditoria/` con: descripción, causa raíz, fix aplicado, verificación y notas operativas.
