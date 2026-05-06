"""
routers/llm_router.py — Router LLM con cadena de fallback automática.

Orden: DeepSeek V3 → GPT-4o-mini
Lógica: si 429 → exhausted 1h, si 5xx → exhausted 15min, si timeout → exhausted 15min,
        si todos caídos → RuntimeError

════════════════════════════════════════════════════════════════════════════════
 DÓNDE PONER LAS API KEYS
════════════════════════════════════════════════════════════════════════════════
   DEEPSEEK_API_KEY → https://platform.deepseek.com   (empieza por sk-)
   OPENAI_API_KEY   → https://platform.openai.com/api-keys  (empieza por sk-)

 Con tener 1 key ya funciona — el router salta automáticamente al siguiente
 proveedor si el actual falla o no tiene key configurada.
════════════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations
import asyncio
import logging
import time
from typing import Optional

from openai import AsyncOpenAI, RateLimitError as ORateLimit

from config import settings
from db.redis_client import get_redis
from db.conexion import get_db

logger = logging.getLogger(__name__)

_TIMEOUT = 30

_DEEP: Optional[AsyncOpenAI] = None
_OAI:  Optional[AsyncOpenAI] = None

_MODELS = {
    "deepseek": "deepseek-chat",   # DeepSeek-V3.2, muy barato y capaz
    "openai":   "gpt-4o-mini",     # Fallback ligero de OpenAI
}

_COSTES = {
    "deepseek": (0.27, 1.10),   # USD por 1M tokens (cache miss)
    "openai":   (0.15, 0.60),
}

_API_KEYS = {
    "deepseek": lambda: settings.DEEPSEEK_API_KEY,
    "openai":   lambda: settings.OPENAI_API_KEY,
}

class _RL(Exception): pass   # Rate Limit  → exhausted 1 hora
class _SE(Exception): pass   # Server Error → exhausted 15 minutos


def _deep() -> AsyncOpenAI:
    global _DEEP
    if not _DEEP:
        _DEEP = AsyncOpenAI(
            api_key=settings.DEEPSEEK_API_KEY,
            base_url="https://api.deepseek.com",
        )
    return _DEEP

def _oai() -> AsyncOpenAI:
    global _OAI
    if not _OAI:
        _OAI = AsyncOpenAI(api_key=settings.OPENAI_API_KEY)
    return _OAI


async def close_llm_clients() -> None:
    global _DEEP, _OAI
    for nombre, cliente in [("deepseek", _DEEP), ("openai", _OAI)]:
        if cliente is not None:
            try:
                await cliente.close()
            except Exception as e:
                logger.debug("Error cerrando cliente %s: %s", nombre, e)
    _DEEP = _OAI = None


_PLACEHOLDERS = {"sk-ant-...", "sk-...", "...", "sk-proj-...", "AIza..."}

def _has_key(proveedor: str) -> bool:
    fn = _API_KEYS.get(proveedor)
    if not fn:
        return False
    key = fn().strip()
    return bool(key) and key not in _PLACEHOLDERS and not key.endswith("...")


async def completar(mensajes: list[dict], sistema: str, endpoint: str,
                    session_id: Optional[str] = None, max_tokens: int = 1500,
                    temperature: float = 0.3, requiere_json: bool = False,
                    timeout: int = _TIMEOUT) -> str:
    if requiere_json:
        sistema += "\n\nRespond ONLY with valid JSON. No additional text or markdown."

    r = get_redis()
    for proveedor in ["deepseek", "openai"]:
        if not _has_key(proveedor):
            continue
        if await r.get(f"llm:exhausted:{proveedor}"):
            continue

        t0 = time.perf_counter()
        try:
            texto, ti, to = await asyncio.wait_for(
                _llamar(proveedor, mensajes, sistema, max_tokens, temperature),
                timeout=timeout,
            )
            lat = int((time.perf_counter() - t0) * 1000)
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
        except asyncio.TimeoutError:
            # No marcar como exhausted por timeout — el proveedor puede ser lento
            # puntualmente (requests largos como el roadmap legal de 3000 tokens).
            logger.warning("Timeout %s (%ds) → trying next provider", proveedor, timeout)
            continue
        except Exception as e:
            logger.error("Error inesperado %s: %s", proveedor, e); continue

    raise RuntimeError("Todos los proveedores LLM caídos o agotados.")


async def _llamar(proveedor: str, mensajes: list[dict], sistema: str,
                  max_tokens: int, temperature: float) -> tuple[str, int, int]:
    try:
        c = _deep() if proveedor == "deepseek" else _oai()
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


async def _log(session_id: Optional[str], proveedor: str, ti: int, to: int,
               lat: int, endpoint: str) -> None:
    try:
        cin, cout = _COSTES.get(proveedor, (0, 0))
        coste = ti / 1e6 * cin + to / 1e6 * cout
        async with get_db() as conn:
            await conn.execute(
                "INSERT INTO llm_logs(session_id,proveedor,modelo,tokens_input,tokens_output,"
                "coste_usd,latencia_ms,endpoint_nombre) VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
                session_id, proveedor, _MODELS.get(proveedor, "?"), ti, to,
                round(coste, 6), lat, endpoint)
    except Exception:
        pass
