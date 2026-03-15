"""
routers/llm_router.py — Router LLM con cadena de fallback automática.

Orden: Claude Sonnet → GPT-4o → DeepSeek → Kimi → Gemini Flash
Lógica: si 429 → exhausted 1h, si 5xx → exhausted 15min, si timeout → exhausted 15min,
        si todos caídos → RuntimeError

════════════════════════════════════════════════════════════════════════════════
 DÓNDE PONER LAS API KEYS
════════════════════════════════════════════════════════════════════════════════
 Las claves NUNCA van escritas aquí. Se leen desde variables de entorno.
 Copia georetail/.env.example → georetail/.env y rellena cada valor:

   ANTHROPIC_API_KEY   → https://console.anthropic.com          (empieza por sk-ant-)
   OPENAI_API_KEY      → https://platform.openai.com/api-keys   (empieza por sk-)
   DEEPSEEK_API_KEY    → https://platform.deepseek.com          (empieza por sk-)
   KIMI_API_KEY        → https://platform.moonshot.ai           (empieza por sk-)
   GEMINI_API_KEY      → https://aistudio.google.com/apikey     (sin prefijo fijo)

 Con tener 1 o 2 activas ya funciona — el router salta automáticamente
 al siguiente proveedor si el actual falla o no tiene key configurada.
════════════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations
import asyncio
import logging
import time
from typing import Optional

import anthropic
from openai import AsyncOpenAI, RateLimitError as ORateLimit
from google import genai as google_genai
from google.genai import types as genai_types

from config import settings
from db.redis_client import get_redis
from db.conexion import get_db

logger = logging.getLogger(__name__)

# ── Tiempo máximo de espera por llamada LLM (segundos) ───────────────────────
# Si un proveedor no responde en este tiempo se trata como error de servidor
# y se pasa al siguiente proveedor de la cadena.
_TIMEOUT = 30

# ── Clientes singleton (se instancian una sola vez al primer uso) ─────────────
_ANT:    Optional[anthropic.AsyncAnthropic] = None   # cliente Anthropic
_OAI:    Optional[AsyncOpenAI]              = None   # cliente OpenAI
_DEEP:   Optional[AsyncOpenAI]              = None   # cliente DeepSeek (API compatible OpenAI)
_KIMI:   Optional[AsyncOpenAI]              = None   # cliente Kimi (API compatible OpenAI)
_GEMINI: Optional[google_genai.Client]      = None   # cliente Gemini (Google)

# ── Modelos activos ───────────────────────────────────────────────────────────
# Cambiar el nombre del modelo aquí para actualizar toda la app de golpe.
#
#   anthropic → claude-sonnet-4-6
#               Último Claude Sonnet. Mejor balance calidad/precio de Anthropic.
#               Contexto: 200k tokens.
#
#   openai    → gpt-4o
#               Modelo general de OpenAI. Referencia de calidad en el mercado.
#               Contexto: 128k tokens.
#
#   deepseek  → deepseek-chat   (alias de DeepSeek-V3.2)
#               Modelo chino de referencia, muy barato y sorprendentemente bueno.
#               Contexto: 128k tokens.
#               NOTA: existe también "deepseek-reasoner" (modo razonamiento lento)
#                     pero no lo usamos aquí porque añade latencia innecesaria.
#
#   kimi      → kimi-k2.5   (Moonshot AI)
#               Modelo multilingüe fuerte en chino/inglés/español.
#               Contexto: 128k tokens.
#               Alternativas disponibles: moonshot-v1-8k / moonshot-v1-32k / moonshot-v1-128k
#
#   gemini    → gemini-2.0-flash
#               El modelo Flash de Google: muy rápido y el más barato de la cadena.
#               Contexto: 1M tokens.
#               NOTA: gemini-1.5-flash está deprecated desde nov 2025.
_MODELS = {
    "anthropic": "claude-sonnet-4-6",
    "openai":    "gpt-4o",
    "deepseek":  "deepseek-chat",
    "kimi":      "kimi-k2.5",
    "gemini":    "gemini-2.0-flash",
}

# ── Costes por proveedor (USD por 1M tokens) ──────────────────────────────────
# Formato: (precio_input, precio_output)
# Se usan solo para el log de costes en BD — no afectan la lógica de fallback.
# Actualizar si los proveedores cambian precios.
_COSTES = {
    "anthropic": (3.0,   15.0),   # Anthropic Sonnet 4.6
    "openai":    (2.5,   10.0),   # GPT-4o
    "deepseek":  (0.27,  1.10),   # DeepSeek-V3.2 (cache miss)
    "kimi":      (0.15,  0.60),   # Kimi K2.5
    "gemini":    (0.075, 0.30),   # Gemini 2.0 Flash
}

# ── Keys de API indexadas por nombre de proveedor ────────────────────────────
# Permite comprobar si un proveedor tiene key configurada antes de intentarlo.
_API_KEYS = {
    "anthropic": lambda: settings.ANTHROPIC_API_KEY,
    "openai":    lambda: settings.OPENAI_API_KEY,
    "deepseek":  lambda: settings.DEEPSEEK_API_KEY,
    "kimi":      lambda: settings.KIMI_API_KEY,
    "gemini":    lambda: settings.GEMINI_API_KEY,
}

# Excepciones internas para distinguir tipo de error sin exponer detalles al caller
class _RL(Exception): pass   # Rate Limit  → exhausted 1 hora
class _SE(Exception): pass   # Server Error → exhausted 15 minutos


# ── Inicializadores de clientes ───────────────────────────────────────────────
# Cada función lee la API key desde settings (que la carga del .env).
# El cliente se crea una sola vez y se reutiliza en todas las llamadas.

def _ant() -> anthropic.AsyncAnthropic:
    # API key: ANTHROPIC_API_KEY en el .env
    # Registro: https://console.anthropic.com
    global _ANT
    if not _ANT:
        _ANT = anthropic.AsyncAnthropic(api_key=settings.ANTHROPIC_API_KEY)
    return _ANT

def _oai() -> AsyncOpenAI:
    # API key: OPENAI_API_KEY en el .env
    # Registro: https://platform.openai.com/api-keys
    global _OAI
    if not _OAI:
        _OAI = AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
    return _OAI

def _deep() -> AsyncOpenAI:
    # API key: DEEPSEEK_API_KEY en el .env
    # Registro: https://platform.deepseek.com → API Keys
    # Usa el mismo protocolo que OpenAI, solo cambia base_url y la key
    global _DEEP
    if not _DEEP:
        _DEEP = AsyncOpenAI(
            api_key=settings.DEEPSEEK_API_KEY,
            base_url="https://api.deepseek.com",          # ← endpoint oficial DeepSeek
        )
    return _DEEP

def _kimi() -> AsyncOpenAI:
    # API key: KIMI_API_KEY en el .env
    # Registro: https://platform.moonshot.ai → API Keys
    # También usa protocolo OpenAI-compatible, solo cambia base_url y la key
    global _KIMI
    if not _KIMI:
        _KIMI = AsyncOpenAI(
            api_key=settings.KIMI_API_KEY,
            base_url="https://api.moonshot.ai/v1",        # ← endpoint oficial Moonshot/Kimi
        )
    return _KIMI

def _gemini() -> google_genai.Client:
    # API key: GEMINI_API_KEY en el .env
    # Registro: https://aistudio.google.com/apikey
    global _GEMINI
    if not _GEMINI:
        _GEMINI = google_genai.Client(api_key=settings.GEMINI_API_KEY)
    return _GEMINI


def _has_key(proveedor: str) -> bool:
    """Devuelve True si el proveedor tiene API key configurada (no vacía)."""
    fn = _API_KEYS.get(proveedor)
    return bool(fn and fn().strip())


# ── Función principal — llamar desde cualquier endpoint del backend ───────────
async def completar(mensajes: list[dict], sistema: str, endpoint: str,
                    session_id: Optional[str] = None, max_tokens: int = 1500,
                    temperature: float = 0.3, requiere_json: bool = False) -> str:
    if requiere_json:
        # En inglés para ser coherente con la estrategia de prompts en inglés
        sistema += "\n\nRespond ONLY with valid JSON. No additional text or markdown."

    r = get_redis()
    # Cadena de fallback: se intenta en orden; si uno falla se pasa al siguiente
    for proveedor in ["anthropic", "openai", "deepseek", "kimi", "gemini"]:
        # Proveedor sin key configurada → saltar silenciosamente
        if not _has_key(proveedor):
            continue
        # Proveedor marcado como agotado temporalmente → saltar
        if await r.get(f"llm:exhausted:{proveedor}"):
            continue

        t0 = time.perf_counter()
        try:
            texto, ti, to = await asyncio.wait_for(
                _llamar(proveedor, mensajes, sistema, max_tokens, temperature),
                timeout=_TIMEOUT,
            )
            lat = int((time.perf_counter() - t0) * 1000)
            await r.set("llm:proveedor_activo", proveedor)
            await r.incr(f"llm:counter:{proveedor}:daily")
            await r.expire(f"llm:counter:{proveedor}:daily", 86400)
            await _log(session_id, proveedor, ti, to, lat, endpoint)
            return texto
        except _RL:
            # 429 Too Many Requests → bloquear proveedor 1 hora
            logger.warning("Rate limit %s → exhausted 1h", proveedor)
            await r.setex(f"llm:exhausted:{proveedor}", 3600, "1"); continue
        except _SE:
            # 5xx Server Error → bloquear proveedor 15 minutos
            logger.warning("Server error %s → exhausted 15min", proveedor)
            await r.setex(f"llm:exhausted:{proveedor}", 900, "1"); continue
        except asyncio.TimeoutError:
            # Proveedor no respondió en _TIMEOUT segundos → tratar como error de servidor
            logger.warning("Timeout %s (%ds) → exhausted 15min", proveedor, _TIMEOUT)
            await r.setex(f"llm:exhausted:{proveedor}", 900, "1"); continue
        except Exception as e:
            logger.error("Error inesperado %s: %s", proveedor, e); continue

    raise RuntimeError("Todos los proveedores LLM caídos o agotados.")


# ── Despachador por proveedor ─────────────────────────────────────────────────
async def _llamar(proveedor: str, mensajes: list[dict], sistema: str,
                  max_tokens: int, temperature: float) -> tuple[str, int, int]:
    # ── Anthropic (Claude) ────────────────────────────────────────────────────
    if proveedor == "anthropic":
        try:
            c = _ant()
            r = await c.messages.create(
                model=_MODELS["anthropic"],
                max_tokens=max_tokens,
                temperature=temperature,
                system=sistema,
                messages=mensajes,
            )
            if not r.content:
                raise _SE()
            texto = r.content[0].text
            if not texto:
                raise _SE()
            return texto, r.usage.input_tokens, r.usage.output_tokens
        except (_RL, _SE):
            raise
        except anthropic.RateLimitError:
            raise _RL()
        except anthropic.APIStatusError as e:
            if e.status_code >= 500:
                raise _SE()
            raise

    # ── OpenAI / DeepSeek / Kimi  (todos usan protocolo OpenAI-compatible) ────
    if proveedor in ("openai", "deepseek", "kimi"):
        try:
            if proveedor == "openai":     c = _oai()
            elif proveedor == "deepseek": c = _deep()
            else:                         c = _kimi()
            r = await c.chat.completions.create(
                model=_MODELS[proveedor],
                messages=[{"role": "system", "content": sistema}] + mensajes,
                max_tokens=max_tokens,
                temperature=temperature,
            )
            content = r.choices[0].message.content if r.choices else None
            if content is None:
                raise _SE()
            return content, r.usage.prompt_tokens, r.usage.completion_tokens
        except (_RL, _SE):
            raise
        except ORateLimit:
            raise _RL()
        except Exception as e:
            if getattr(e, "status_code", 0) >= 500:
                raise _SE()
            raise

    # ── Google Gemini ─────────────────────────────────────────────────────────
    # Usa el nuevo SDK google-genai (el antiguo google-generativeai está deprecated)
    # API key: GEMINI_API_KEY en el .env
    # Registro: https://aistudio.google.com/apikey
    if proveedor == "gemini":
        try:
            client = _gemini()
            contents = [
                {"role": "user" if m["role"] == "user" else "model",
                 "parts": [{"text": m["content"]}]}
                for m in mensajes
            ]
            res = await client.aio.models.generate_content(
                model=_MODELS["gemini"],
                contents=contents,
                config=genai_types.GenerateContentConfig(
                    system_instruction=sistema,
                    max_output_tokens=max_tokens,
                    temperature=temperature,
                ),
            )
            t = res.text
            if t is None:
                # Respuesta bloqueada por filtros de seguridad o vacía
                raise _SE()
            meta = res.usage_metadata
            ti = meta.prompt_token_count     if meta else len(sistema.split())
            to = meta.candidates_token_count if meta else len(t.split())
            return t, ti, to
        except (_RL, _SE):
            raise
        except Exception as e:
            msg = str(e).lower()
            if "429" in msg or "quota" in msg or "rate" in msg:
                raise _RL()
            if "500" in msg or "503" in msg:
                raise _SE()
            raise

    raise ValueError(f"Proveedor desconocido: {proveedor}")


# ── Log de uso en base de datos ───────────────────────────────────────────────
async def _log(session_id: Optional[str], proveedor: str, ti: int, to: int,
               lat: int, endpoint: str) -> None:
    try:
        cin, cout = _COSTES.get(proveedor, (0, 0))
        coste = ti / 1e6 * cin + to / 1e6 * cout
        async with get_db() as conn:
            await conn.execute(
                "INSERT INTO llm_logs(session_id,proveedor,modelo,tokens_input,tokens_output,"
                "coste_usd,latencia_ms,endpoint) VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
                session_id, proveedor, _MODELS.get(proveedor, "?"), ti, to,
                round(coste, 6), lat, endpoint)
    except Exception:
        pass
