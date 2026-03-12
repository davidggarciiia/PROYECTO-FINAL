"""
config.py — Configuración centralizada con pydantic-settings.
Todas las variables de entorno se leen desde aquí. Nunca os.environ directamente.
"""
from __future__ import annotations
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # ── Base de datos ─────────────────────────────────────────────────────────
    DATABASE_URL: str = "postgresql://georetail:georetail@localhost:5432/georetail"
    REDIS_URL: str = "redis://localhost:6379"

    # ── LLMs ─────────────────────────────────────────────────────────────────
    ANTHROPIC_API_KEY: str = ""
    OPENAI_API_KEY: str = ""
    GROQ_API_KEY: str = ""
    GEMINI_API_KEY: str = ""
    DEEPSEEK_API_KEY: str = ""
    OLLAMA_URL: str = "http://localhost:11434"

    # ── Mapas / Places ────────────────────────────────────────────────────────
    GOOGLE_MAPS_API_KEY: str = ""
    FOURSQUARE_API_KEY: str = ""
    YELP_API_KEY: str = ""
    OPENCAGE_API_KEY: str = ""
    MAPBOX_PUBLIC_TOKEN: str = ""

    # ── Inmobiliario ──────────────────────────────────────────────────────────
    IDEALISTA_API_KEY: str = ""
    IDEALISTA_SECRET: str = ""

    # ── Transporte ────────────────────────────────────────────────────────────
    TMB_APP_ID: str = ""
    TMB_APP_KEY: str = ""

    # ── App ───────────────────────────────────────────────────────────────────
    ENVIRONMENT: str = "development"
    SENTRY_DSN: str = ""
    CORS_ORIGINS: list[str] = ["http://localhost:3000"]
    EXPORTS_DIR: str = "/data/exports"
    STREETVIEW_DIR: str = "/data/streetview"
    MODELS_DIR: str = "/data/models"

    # ── Celery ────────────────────────────────────────────────────────────────
    CELERY_BROKER_URL: str = "redis://localhost:6379/1"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/2"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
