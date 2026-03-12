"""
routers/llm_router.py — Router LLM con cadena de fallback automática.

Orden: Claude Sonnet → GPT-4o → Groq Llama 3.3 → Gemini Flash → Ollama local
Lógica: si 429 → exhausted 1h, si 5xx → exhausted 15min, si todos caídos → RuntimeError
"""
from __future__ import annotations
import logging, os, time
from typing import Optional

import anthropic
from openai import AsyncOpenAI, RateLimitError as ORateLimit
import google.generativeai as genai
import httpx

from db.redis_client import get_redis
from db.conexion import get_db

logger = logging.getLogger(__name__)

_ANT: Optional[anthropic.AsyncAnthropic] = None
_OAI: Optional[AsyncOpenAI] = None
_GROQ: Optional[AsyncOpenAI] = None

_MODELS = {
    "anthropic": "claude-sonnet-4-20250514",
    "openai":    "gpt-4o",
    "groq":      "llama-3.3-70b-versatile",
    "gemini":    "gemini-1.5-flash",
    "ollama":    "llama3.3:8b",
}

_COSTES = {
    "anthropic": (3.0,   15.0),
    "openai":    (2.5,   10.0),
    "groq":      (0.59,  0.79),
    "gemini":    (0.075, 0.30),
    "ollama":    (0.0,   0.0),
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

def _groq():
    global _GROQ
    if not _GROQ: _GROQ = AsyncOpenAI(api_key=os.environ.get("GROQ_API_KEY",""),
                                       base_url="https://api.groq.com/openai/v1")
    return _GROQ


async def completar(mensajes: list[dict], sistema: str, endpoint: str,
                    session_id: Optional[str] = None, max_tokens: int = 1500,
                    temperature: float = 0.3, requiere_json: bool = False) -> str:
    if requiere_json:
        sistema += "\n\nResponde ÚNICAMENTE con JSON válido, sin texto adicional ni markdown."

    r = get_redis()
    for proveedor in ["anthropic","openai","groq","gemini","ollama"]:
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

    if proveedor in ("openai","groq"):
        try:
            c = _oai() if proveedor == "openai" else _groq()
            r = await c.chat.completions.create(
                model=_MODELS[proveedor],
                messages=[{"role":"system","content":sistema}]+mensajes,
                max_tokens=max_tokens, temperature=temperature)
            return r.choices[0].message.content, r.usage.prompt_tokens, r.usage.completion_tokens
        except ORateLimit: raise _RL()
        except Exception as e:
            if getattr(e,"status_code",0)>=500: raise _SE()
            raise

    if proveedor == "gemini":
        key = os.environ.get("GEMINI_API_KEY","")
        if not key: raise _SE()
        try:
            genai.configure(api_key=key)
            model = genai.GenerativeModel(_MODELS["gemini"], system_instruction=sistema)
            hist = [{"role":"user" if m["role"]=="user" else "model","parts":[m["content"]]}
                    for m in mensajes[:-1]]
            chat = model.start_chat(history=hist)
            res = await chat.send_message_async(
                mensajes[-1]["content"] if mensajes else "",
                generation_config=genai.GenerationConfig(max_output_tokens=max_tokens,
                                                          temperature=temperature))
            t = res.text
            return t, len(sistema.split()), len(t.split())
        except Exception as e:
            msg = str(e).lower()
            if "429" in msg or "quota" in msg: raise _RL()
            if "500" in msg or "503" in msg: raise _SE()
            raise

    if proveedor == "ollama":
        url = os.environ.get("OLLAMA_URL","http://localhost:11434")
        async with httpx.AsyncClient(timeout=30.0) as c:
            r = await c.post(f"{url}/api/chat", json={
                "model": _MODELS["ollama"],
                "messages": [{"role":"system","content":sistema}]+mensajes,
                "stream": False,
                "options": {"temperature": temperature, "num_predict": max_tokens}})
            r.raise_for_status()
            d = r.json()
            return d["message"]["content"], d.get("prompt_eval_count",0), d.get("eval_count",0)

    raise ValueError(f"Proveedor desconocido: {proveedor}")


async def _log(session_id, proveedor, ti, to, lat, endpoint):
    try:
        cin, cout = _COSTES.get(proveedor,(0,0))
        coste = ti/1e6*cin + to/1e6*cout
        async with get_db() as conn:
            await conn.execute(
                "INSERT INTO llm_logs(session_id,proveedor,modelo,tokens_input,tokens_output,"
                "coste_usd,latencia_ms,endpoint) VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
                session_id, proveedor, _MODELS.get(proveedor,"?"), ti, to,
                round(coste,6), lat, endpoint)
    except Exception: pass
