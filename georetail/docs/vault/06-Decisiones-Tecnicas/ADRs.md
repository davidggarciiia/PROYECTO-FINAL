# Decisiones Técnicas (ADRs)

## ADR-001: Flujo peatonal Vcity para la base + popular Times para los picos de gente

**Decisión:** Se usaran los datos del mapa de vcity como base y los datos de google popular Times para los picos de gente
**Motivo:** Son datos veridicos y eliminan el ruido

---

## ADR-002: motor.py como única API pública del scoring

**Decisión:** La capa `api/` solo importa de `scoring/motor.py`, nunca de `scorer.py` directamente.  
**Motivo:** Desacoplamiento. Permite cambiar la implementación sin tocar los routers.

---

## ADR-003: LLM Router con fallback chain

**Decisión:** Todas las llamadas LLM pasan por `routers/llm_router.py`  
**Fallback:** Claude Sonnet 4.6 → GPT-4o → DeepSeek V3 → Kimi K2.5 → Gemini 2.0 Flash  
**Motivo:** Resiliencia ante caídas de proveedores.

---

## ADR-004: Feature versioning incremental

**Decisión:** Features numeradas v1–v15. Cada modelo almacena los feature names con los que fue entrenado.  
**Implementación:** `governance.py:slice_feature_matrix` recorta la matriz en inferencia.  
**Motivo:** Compatibilidad backward con modelos entrenados en versiones anteriores de features.

---

## ADR-005: XGBoost label definition

**Decisión:** Label = 1 si negocio sobrevivió ≥3 años, 0 si cerró antes.  
**Fuente de datos:** `negocios_historico` + snapshots históricos de variables.

---

## ADR-006: score_afinidad_concepto en runtime

**Decisión:** `score_afinidad_concepto` (12%) se calcula en runtime, nunca se persiste.  
**Motivo:** Depende de la query del usuario, no de la zona.

---

## ADR-007: Sesión en Redis + PostgreSQL

**Decisión:** Estado de sesión en Redis (TTL rápido) Y PostgreSQL (durabilidad).  
**Motivo:** Fast path para reads frecuentes, durable storage para recovery.
