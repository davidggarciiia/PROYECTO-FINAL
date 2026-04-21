# BUG-P0-6: Limpieza router LLM → solo DeepSeek + OpenAI

**Fecha:** 2026-04-22
**Severidad:** ALTA (deuda técnica, riesgo de runtime)
**Estado:** Resuelto
**Archivos afectados:**
- `georetail/backend/config.py` (fields y docstring)
- `georetail/backend/requirements.txt` (paquetes Python)
- `georetail/backend/tests/conftest.py` (stubs + `_FakeSettings`)
- `georetail/backend/api/health.py:161` (comentario)
- `georetail/backend/db/migraciones/001_schema_inicial.sql:600` (comentario SQL)
- `georetail/backend/tests/test_validador_llm.py:12` (comentario)
- `georetail/backend/tests/scripts/test_integracion_modelo.py:32,332,338-341,472` (skip condicional + prints)
- `georetail/CLAUDE.md` (3 secciones)
- `georetail/docs/vault/05-DevOps/Docker-Compose.md:35` (env mínimas)

---

## Descripción

El `routers/llm_router.py` ya hace tiempo que fue simplificado a **DeepSeek V3
→ GPT-4o-mini**. Sin embargo, quedaban referencias fósiles a los proveedores
eliminados (Anthropic/Kimi/Gemini) en:

- **Config pydantic-settings** — fields `ANTHROPIC_API_KEY`, `KIMI_API_KEY`,
  `GEMINI_API_KEY` declarados pero nunca leídos.
- **requirements.txt** — `anthropic==0.84.0`, `google-genai==1.56.0`,
  `langchain-anthropic==1.3.4` se seguían instalando en producción (~200 MB
  de dependencias inútiles y vulnerabilidades a rastrear).
- **Tests** — stubs de `anthropic`, `google`, `google.genai` que solo
  servían para evitar `ImportError` ya imposibles.
- **Comentarios en código + SQL + docs** — menciones a `claude`, `groq`,
  `ollama`, `gemini` como valores válidos en `llm_logs.proveedor`.

## Impacto

1. **Riesgo de runtime**: alguien podría copiar el nombre de un field
   obsoleto (`ANTHROPIC_API_KEY`) a un `.env` pensando que activa Claude,
   perdiendo tiempo de debug (no funciona — el router ni siquiera lo mira).
2. **Deuda de dependencias**: el `pip install` bajaba 3 paquetes pesados
   sin usar. `anthropic==0.84.0` pinnea versiones concretas de httpx/pydantic
   que pueden entrar en conflicto con otras deps en futuras actualizaciones.
3. **Documentación contradictoria**: `CLAUDE.md` listaba 5 proveedores y
   `llm_router.py` solo 2 → onboarding confuso para nuevos devs.
4. **Ruido en health checks y logs**: comentarios SQL y help strings
   mencionaban `'anthropic'`, `'groq'`, `'gemini'`, `'ollama'` como valores
   posibles de `llm_logs.proveedor`, pero el router nunca los escribe.

## Causa raíz

El router se simplificó en un PR previo pero no se hizo una limpieza
transversal. Típico caso de refactor incompleto.

## Fix aplicado

### 1. `config.py`
Eliminados 3 fields (`ANTHROPIC_API_KEY`, `KIMI_API_KEY`, `GEMINI_API_KEY`).
Docstring actualizada: router actual es DeepSeek → OpenAI.

### 2. `requirements.txt`
Eliminados `anthropic==0.84.0`, `google-genai==1.56.0`,
`langchain-anthropic==1.3.4`. Se mantiene `openai==2.26.0` (sirve tanto para
GPT-4o-mini como para DeepSeek vía `base_url`) y `langchain==1.2.12` (por si
se usa fuera del router).

### 3. `tests/conftest.py`
Eliminados stubs de `anthropic`, `google`, `google.genai`. `_FakeSettings`
actualizado con atributos en **MAYÚSCULAS** para coincidir con
`config.Settings` real (además de aliases en minúsculas para compat).

### 4. `api/health.py`, migraciones SQL, tests
Actualizados comentarios para reflejar los 2 proveedores reales
(`'deepseek'`, `'openai'`).

### 5. `georetail/CLAUDE.md` y vault docs
Tabla stack + sección env vars + diagrama de router actualizados.

## Verificación

```bash
# Ninguna referencia a providers muertos en código backend:
grep -rEni "anthropic|kimi|gemini|ANTHROPIC_API_KEY|KIMI_API_KEY|GEMINI_API_KEY" \
  georetail/backend
# (resultado: 0 matches)

# Router importa sólo los 2 proveedores vivos:
head -50 georetail/backend/routers/llm_router.py
```

## Notas de operación

- Si ya había un `.env` con las keys obsoletas, seguirán siendo ignoradas
  silenciosamente por `extra="ignore"` en `SettingsConfigDict`. No rompen
  el arranque, pero convendría limpiar los `.env` locales.
- Si se guardaron registros históricos en `llm_logs` con
  `proveedor='anthropic'` o similar, la columna sigue siendo `VARCHAR(20)` y
  los valores se conservan — solo el comentario del schema ha cambiado.

---

**Tags:** #bug #alta #llm #cleanup #deuda-tecnica #p0
