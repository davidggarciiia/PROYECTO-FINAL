"""
tests/conftest.py — Configuración global de pytest para GeoRetail.

Estrategia de aislamiento:
  Los módulos del proyecto tienen imports a nivel de módulo de dependencias
  pesadas (asyncpg, openai, pydantic-settings, etc.) que no están disponibles
  en el entorno de CI/CD sin base de datos.

  Para poder testear las FUNCIONES PURAS (sin I/O) inyectamos stubs en
  sys.modules ANTES de que los módulos productivos se importen.

  Esto es equivalente a un "shim" de dependencias — práctica estándar
  en proyectos que mezclan lógica de negocio con infraestructura.
"""
from __future__ import annotations
import sys
import os
from unittest.mock import MagicMock, AsyncMock
from contextlib import asynccontextmanager

# ─── Añadir backend al path ───────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


# ─── Stub de pydantic_settings ───────────────────────────────────────────────

class _FakeBaseSettings:
    model_config = {}
    def __init__(self, **_):
        pass

_pydantic_settings = MagicMock()
_pydantic_settings.BaseSettings = _FakeBaseSettings
_pydantic_settings.SettingsConfigDict = dict
sys.modules.setdefault("pydantic_settings", _pydantic_settings)


# ─── Stubs de drivers de BD ───────────────────────────────────────────────────

sys.modules.setdefault("asyncpg",       MagicMock())
sys.modules.setdefault("redis",         MagicMock())
sys.modules.setdefault("redis.asyncio", MagicMock())


# ─── Stubs de LLM providers ──────────────────────────────────────────────────
# Router actual: DeepSeek V3 → GPT-4o-mini. Ambos usan el SDK de openai,
# por eso solo necesitamos stubear "openai".

sys.modules.setdefault("openai",              MagicMock())


# ─── Stubs de config y DB ─────────────────────────────────────────────────────
# Stub de config.py para que get_settings() devuelva un objeto con atributos

class _FakeSettings:
    # Atributos en MAYÚSCULAS para coincidir con config.Settings real.
    DATABASE_URL      = "postgresql://test:test@localhost/test"
    REDIS_URL         = "redis://localhost:6379"
    DEEPSEEK_API_KEY  = "sk-deepseek-test"
    OPENAI_API_KEY    = "sk-test"
    MODELS_DIR        = "/tmp/models"
    ENVIRONMENT       = "test"
    # Alias en minúsculas por compatibilidad con tests antiguos
    database_url      = DATABASE_URL
    redis_url         = REDIS_URL
    openai_api_key    = OPENAI_API_KEY
    models_dir        = MODELS_DIR
    debug             = False

_fake_config = MagicMock()
_fake_config.get_settings = lambda: _FakeSettings()
_fake_config.settings     = _FakeSettings()
sys.modules.setdefault("config", _fake_config)


# ─── Stub de db.conexion ──────────────────────────────────────────────────────
# get_db es un context manager async; cuando motor.py llama a conn.fetchrow/fetch
# necesitamos que sean awaitable (AsyncMock), no un MagicMock síncrono.

def _make_async_conn():
    conn = MagicMock()
    conn.fetchrow = AsyncMock(return_value=None)
    conn.fetch    = AsyncMock(return_value=[])
    conn.execute  = AsyncMock(return_value=None)
    return conn

@asynccontextmanager
async def _fake_get_db():
    yield _make_async_conn()

_fake_conexion = MagicMock()
_fake_conexion.get_db = _fake_get_db

_fake_db = MagicMock()
_fake_db.conexion = _fake_conexion
sys.modules.setdefault("db",          _fake_db)
sys.modules.setdefault("db.conexion", _fake_conexion)


# ─── Stub de routers.llm_router ──────────────────────────────────────────────

_fake_llm_router = MagicMock()
_fake_llm_router.completar = AsyncMock(return_value='{"ok": true}')

_fake_routers = MagicMock()
_fake_routers.llm_router = _fake_llm_router
sys.modules.setdefault("routers",            _fake_routers)
sys.modules.setdefault("routers.llm_router", _fake_llm_router)


# ─── Stub de db.financiero ────────────────────────────────────────────────────

_fake_db_financiero = MagicMock()
_fake_db_financiero.get_benchmarks_sector = AsyncMock(return_value={})
sys.modules.setdefault("db.financiero", _fake_db_financiero)
