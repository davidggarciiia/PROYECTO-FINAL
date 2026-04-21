# Agente LLM

## Archivos

| Archivo | Rol |
|---|---|
| `agente/analizador.py` | Genera narrativa de zona |
| `agente/cuestionario.py` | Flujo de preguntas adaptativo |
| `agente/prompts/__init__.py` | System prompts (en inglés) |
| `agente/refinador.py` | Refinamiento de análisis |
| `agente/validador.py` | Validación de respuestas LLM |
| `agente/traductor.py` | Traducción EN→ES downstream |

## LLM Router

Fallback chain: **Claude Sonnet 4.6 → GPT-4o → DeepSeek V3 → Kimi K2.5 → Gemini 2.0 Flash**

- Todos los calls LLM van por `routers/llm_router.py`
- NUNCA llamar SDKs de provider directamente
- Prompts en inglés, output se traduce downstream si hace falta
