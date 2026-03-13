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
    DEEPSEEK_API_KEY: str = ""
    KIMI_API_KEY: str = ""
    GEMINI_API_KEY: str = ""

    # ── Mapas / Places ────────────────────────────────────────────────────────
    GOOGLE_MAPS_API_KEY: str = ""
    FOURSQUARE_API_KEY: str = ""
    YELP_API_KEY: str = ""
    OPENCAGE_API_KEY: str = ""
    MAPBOX_PUBLIC_TOKEN: str = ""

    # ── Inmobiliario ──────────────────────────────────────────────────────────
    IDEALISTA_API_KEY: str = ""
    IDEALISTA_SECRET: str = ""

    # ── Scraping de portales inmobiliarios ────────────────────────────────────
    # Motor: "none" (curl_cffi+httpx directo) | "scrapingbee" | "zenrows"
    SCRAPING_SERVICE: str = "none"
    SCRAPINGBEE_API_KEY: str = ""
    ZENROWS_API_KEY: str = ""
    # Proxies residenciales ES separados por coma: http://user:pass@host:port,...
    SCRAPING_PROXIES: str = ""
    SCRAPING_DELAY_MIN: float = 1.5
    SCRAPING_DELAY_MAX: float = 4.5
    SCRAPING_TIMEOUT: int = 20
    SCRAPING_MAX_RETRIES: int = 3
    # Páginas por portal/zona (0 = sin límite)
    SCRAPING_MAX_PAGINAS: int = 0

    # ── Transporte ────────────────────────────────────────────────────────────
    TMB_APP_ID: str = ""
    TMB_APP_KEY: str = ""

    # ── App ───────────────────────────────────────────────────────────────────
    ENVIRONMENT: str = "development"
    SENTRY_DSN: str = ""
    CORS_ORIGINS: list[str] = ["http://localhost:3000"]
    EXPORTS_DIR: str = "/data/exports"
    OPTUNA_DIR: str = "/data/optuna"
    PORT: int = 8000
    STREETVIEW_DIR: str = "/data/streetview"
    MODELS_DIR: str = "/data/models"

    # ── Celery ────────────────────────────────────────────────────────────────
    CELERY_BROKER_URL: str = "redis://localhost:6379/1"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/2"


@lru_cache()
def get_settings() -> Settings:
    return Settings()

settings = get_settings()
