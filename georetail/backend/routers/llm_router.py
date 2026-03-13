"""
routers/llm_router.py — Router LLM con cadena de fallback automática.

Orden: Claude Sonnet → GPT-4o → DeepSeek → Kimi → Gemini Flash
Lógica: si 429 → exhausted 1h, si 5xx → exhausted 15min, si todos caídos → RuntimeError
"""
from __future__ import annotations
import logging, os, time
from typing import Optional

import anthropic
from openai import AsyncOpenAI, RateLimitError as ORateLimit
from google import genai as google_genai
from google.genai import types as genai_types

from db.redis_client import get_redis
from db.conexion import get_db

logger = logging.getLogger(__name__)

_ANT:     Optional[anthropic.AsyncAnthropic] = None
_OAI:     Optional[AsyncOpenAI]              = None
_DEEP:    Optional[AsyncOpenAI]              = None
_KIMI:    Optional[AsyncOpenAI]              = None

_MODELS = {
    "anthropic": "claude-sonnet-4-6",
    "openai":    "gpt-4o",
    "deepseek":  "deepseek-chat",
    "kimi":      "kimi-k2.5",
    "gemini":    "gemini-2.0-flash",
}

_COSTES = {
    "anthropic": (3.0,   15.0),
    "openai":    (2.5,   10.0),
    "deepseek":  (0.27,  1.10),
    "kimi":      (0.15,  0.60),
    "gemini":    (0.075, 0.30),
}

class _RL(Exception): pass
class _SE(Exception): pass


def _ant():
    global _ANT
    if not _ANT: _ANT = anthropic.AsyncAnthropic(api_key=os.environ.get("ANTHROPIC_API_KEY",""))
    return _ANT

def _oai():
    global _OAI
    if not _OAI: _OAI = AsyncOpenAI(api_key=os.environ.get("OPENAI_API_KEY",""))
    return _OAI

def _deep():
    global _DEEP
    if not _DEEP: _DEEP = AsyncOpenAI(api_key=os.environ.get("DEEPSEEK_API_KEY",""),
                                       base_url="https://api.deepseek.com")
    return _DEEP

def _kimi():
    global _KIMI
    if not _KIMI: _KIMI = AsyncOpenAI(api_key=os.environ.get("KIMI_API_KEY",""),
                                       base_url="https://api.moonshot.ai/v1")
    return _KIMI


async def completar(mensajes: list[dict], sistema: str, endpoint: str,
                    session_id: Optional[str] = None, max_tokens: int = 1500,
                    temperature: float = 0.3, requiere_json: bool = False) -> str:
    if requiere_json:
        sistema += "\n\nResponde ÚNICAMENTE con JSON válido, sin texto adicional ni markdown."

    r = get_redis()
    for proveedor in ["anthropic", "openai", "deepseek", "kimi", "gemini"]:
        if await r.get(f"llm:exhausted:{proveedor}"):
            continue
        t0 = time.perf_counter()
        try:
            texto, ti, to = await _llamar(proveedor, mensajes, sistema, max_tokens, temperature)
            lat = int((time.perf_counter()-t0)*1000)
            await r.set("llm:proveedor_activo", proveedor)
            await r.incr(f"llm:counter:{proveedor}:daily")
            await r.expire(f"llm:counter:{proveedor}:daily", 86400)
            await _log(session_id, proveedor, ti, to, lat, endpoint)
            return texto
        except _RL:
            logger.warning("Rate limit %s → exhausted 1h", proveedor)
            await r.setex(f"llm:exhausted:{proveedor}", 3600, "1"); continue
        except _SE:
            logger.warning("Server error %s → exhausted 15min", proveedor)
            await r.setex(f"llm:exhausted:{proveedor}", 900, "1"); continue
        except Exception as e:
            logger.error("Error inesperado %s: %s", proveedor, e); continue

    raise RuntimeError("Todos los proveedores LLM caídos o agotados.")


async def _llamar(proveedor, mensajes, sistema, max_tokens, temperature):
    if proveedor == "anthropic":
        try:
            c = _ant()
            r = await c.messages.create(model=_MODELS["anthropic"], max_tokens=max_tokens,
                temperature=temperature, system=sistema, messages=mensajes)
            return r.content[0].text, r.usage.input_tokens, r.usage.output_tokens
        except anthropic.RateLimitError: raise _RL()
        except anthropic.APIStatusError as e:
            if e.status_code >= 500: raise _SE()
            raise

    if proveedor in ("openai", "deepseek", "kimi"):
        try:
            if proveedor == "openai":   c = _oai()
            elif proveedor == "deepseek": c = _deep()
            else:                        c = _kimi()
            r = await c.chat.completions.create(
                model=_MODELS[proveedor],
                messages=[{"role": "system", "content": sistema}] + mensajes,
                max_tokens=max_tokens, temperature=temperature)
            return r.choices[0].message.content, r.usage.prompt_tokens, r.usage.completion_tokens
        except ORateLimit: raise _RL()
        except Exception as e:
            if getattr(e, "status_code", 0) >= 500: raise _SE()
            raise

    if proveedor == "gemini":
        key = os.environ.get("GEMINI_API_KEY", "")
        if not key: raise _SE()
        try:
            client = google_genai.Client(api_key=key)
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
            meta = res.usage_metadata
            ti = meta.prompt_token_count if meta else len(sistema.split())
            to = meta.candidates_token_count if meta else len(t.split())
            return t, ti, to
        except Exception as e:
            msg = str(e).lower()
            if "429" in msg or "quota" in msg or "rate" in msg: raise _RL()
            if "500" in msg or "503" in msg: raise _SE()
            raise

    raise ValueError(f"Proveedor desconocido: {proveedor}")


async def _log(session_id, proveedor, ti, to, lat, endpoint):
    try:
        cin, cout = _COSTES.get(proveedor, (0, 0))
        coste = ti/1e6*cin + to/1e6*cout
        async with get_db() as conn:
            await conn.execute(
                "INSERT INTO llm_logs(session_id,proveedor,modelo,tokens_input,tokens_output,"
                "coste_usd,latencia_ms,endpoint) VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
                session_id, proveedor, _MODELS.get(proveedor, "?"), ti, to,
                round(coste, 6), lat, endpoint)
    except Exception: pass
