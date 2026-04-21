# Auditoría de Errores — GeoRetail

**Fecha inicio:** 2026-04-21
**Fecha cierre sprint P0+P1:** 2026-04-22
**Rama:** `mejora-modelo-definitivo`
**Estado:** ✅ Sprint P0+P1 completado (1/2 P0 pendiente de rotación manual). Ver `docs/AUDITORIA-2026-04-21.md` §10 para el live log completo.

---

## Índice de bugs por severidad

### 🚨 Críticos (P0)
- [[bugs-criticos/BUG-P0-1-secrets-filtrados]] — API keys en settings local ⏳ *requiere rotación manual*
- [[bugs-criticos/BUG-P0-2-basura-en-raiz]] — ✅ archivos 0-byte borrados
- [[bugs-criticos/BUG-P0-3-lock-distribuido-inefectivo]] — ✅ polling con back-off
- [[bugs-criticos/BUG-P0-4-double-encode-jsonb]] — ✅ dict directo al codec
- [[bugs-criticos/BUG-P0-5-httpx-proxies]] — ✅ `proxy=` (httpx 0.28+)
- [[bugs-criticos/BUG-P0-6-llm-router-cleanup]] — ✅ solo DeepSeek + OpenAI
- [[bugs-criticos/BUG-001-ROI-duplicado]] — ✅ (commit anterior)
- [[bugs-criticos/BUG-003-fallback-probabilidad-cero]] — ✅ (commit anterior)
- [[bugs-criticos/BUG-COMP-competencia-tab-loading-stuck]] — ✅ (frontend)

### ⚠️ Medios (P1)
- [[bugs-medios/BUG-P1-1-pesos-sectores-no-suman-1]] — ✅ migración 036 + verifier al arrancar
- [[bugs-medios/BUG-P1-4-ss-patronal-no-duplicada]] — ✅ falso positivo + test de regresión

### 🟡 Bajos (P2/P3)
Ver live log en `docs/AUDITORIA-2026-04-21.md` §10 para pendientes.

### ✅ Resueltos históricos
- [[resueltos/]] ← carpeta con bugs cerrados en commits anteriores

---

## Áreas auditadas

| Área | Estado | Bugs encontrados / cerrados |
|---|---|---|
| scoring/motor.py | ✅ pesos normalizados (036) + chequeo arranque | P1-1 |
| scoring/scorer.py | 🔄 | — |
| scoring/features.py | 🔄 | — |
| scoring/infra/governance.py | 🔄 | — |
| api/buscar.py | ✅ lock polling | P0-3 |
| api/local.py | 🔄 | — |
| api/financiero.py | ✅ SS no duplicada (verificado) | P1-4 |
| agente/analizador.py | 🔄 | — |
| financiero/estimador.py | ✅ SS única vez | P1-4 |
| financiero/calculadora.py | ✅ salarios as-is | P1-4 |
| db/sesiones.py | ✅ jsonb sin double-encode | P0-4 |
| db/zonas.py | 🔄 | — |
| pipelines/inmobiliario/base_scraper | ✅ httpx 0.28 compatible | P0-5 |
| workers/tasks.py | 🔄 | — |
| routers/llm_router.py | ✅ DeepSeek→OpenAI limpio | P0-6 |
| config.py + requirements.txt + conftest | ✅ fields+deps+stubs obsoletos fuera | P0-6 |

---

## Template para nuevos bugs

Ver [[templates/Bug-Template]]

---

## Resumen ejecutivo

| Severidad | Total encontrados | Resueltos | Pendientes |
|---|---|---|---|
| Críticos (P0) | 6 + 3 históricos | 8 | P0-1 (rotación keys manual) |
| Medios (P1) | 5 | 2 (P1-1, P1-4) | P1-2, P1-3, P1-5 |
| Bajos (P2) | 4 | 2 (P2-1, P2-2 vía P0-6) | P2-3, P2-4 |
| Deuda técnica (P3) | 8+ | 0 | Convertir a issues |
| Frontend | 1 | 1 (BUG-COMP) | — |

### Sprint 2026-04-22 — highlights

- **BUG-P0-6** (LLM cleanup): 8 archivos + vault doc + requirements reducidos ~200 MB.
- **BUG-P1-1** (pesos sector): migración 036 normaliza 5 sectores a suma=1.00 + invariante al arrancar.
- **BUG-P1-4** (SS patronal duplicada): investigación exhaustiva concluye falso positivo. 3 tests de regresión añadidos.
- `.claude/settings.local.json` añadido a `.gitignore`.
