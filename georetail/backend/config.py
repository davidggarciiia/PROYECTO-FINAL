"""
config.py — Configuración centralizada con pydantic-settings.
Todas las variables de entorno se leen desde aquí. Nunca os.environ directamente.

════════════════════════════════════════════════════════════════════════════════
 CÓMO CONFIGURAR LAS API KEYS
════════════════════════════════════════════════════════════════════════════════
 1. Copia el archivo .env.example a .env  (está en la raíz de georetail/)
 2. Abre el .env y rellena los valores que necesites
 3. Nunca subas el .env a Git (ya está en .gitignore)

 Mínimo para que la app arranque con LLM:
   → Pon al menos DEEPSEEK_API_KEY u OPENAI_API_KEY.
     El router hace fallback automático DeepSeek → OpenAI, con una sola basta.

 Mínimo para mapas:
   → GOOGLE_MAPS_API_KEY  (cubre Places + Geocoding + Static Maps)
════════════════════════════════════════════════════════════════════════════════
"""
from __future__ import annotations
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # ── Base de datos ─────────────────────────────────────────────────────────
    DATABASE_URL: str = "postgresql://georetail:georetail@localhost:5432/georetail"
    REDIS_URL: str = "redis://localhost:6379"

    # ── LLMs (modelos de lenguaje) ────────────────────────────────────────────
    # Router actual: DeepSeek V3 → GPT-4o-mini (fallback).
    # Con tener UNA sola key ya funciona — el router salta al otro proveedor
    # si el actual no tiene key o devuelve error.
    #
    # Dónde conseguir cada key:
    #   DEEPSEEK_API_KEY  → https://platform.deepseek.com              (sk-...)
    #   OPENAI_API_KEY    → https://platform.openai.com/api-keys       (sk-...)
    OPENAI_API_KEY: str = ""
    DEEPSEEK_API_KEY: str = ""

    # ── Open Data Barcelona ───────────────────────────────────────────────────
    # API gratuita del Ajuntament de Barcelona (datos abiertos).
    # Sin key funciona pero con límite de ~1000 req/día por IP.
    # Con key (registro gratuito) sube a ~10.000 req/día.
    # Registro: https://opendata-ajuntament.barcelona.cat → Mi cuenta → API Key
    # La key se pasa como header: Authorization: <key>
    # Usada en: pipelines/aforaments.py, pipelines/demografia.py, pipelines/precios.py
    OPEN_DATA_BCN_API_KEY: str = ""

    # ── Mapas / Places ────────────────────────────────────────────────────────
    # GOOGLE_MAPS_API_KEY cubre las 3 APIs: Places, Geocoding y Static Maps
    # Activar en: https://console.cloud.google.com → APIs & Services → Credenciales
    GOOGLE_MAPS_API_KEY: str = ""
    FOURSQUARE_API_KEY: str = ""    # https://developer.foursquare.com
    YELP_API_KEY: str = ""          # https://www.yelp.com/developers
    OPENCAGE_API_KEY: str = ""      # https://opencagedata.com (geocodificación alternativa)
    MAPBOX_PUBLIC_TOKEN: str = ""   # https://account.mapbox.com → Tokens

    # ── Inmobiliario ──────────────────────────────────────────────────────────
    # Requiere solicitud manual de acceso a Idealista
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
    ADMIN_API_KEY: str = ""   # Obligatorio en producción para /api/admin/*
    ENVIRONMENT: str = "development"
    SENTRY_DSN: str = ""
    CORS_ORIGINS: list[str] = [
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "http://localhost:3001",
        "http://127.0.0.1:3001",
        "http://localhost:3333",
        "http://127.0.0.1:3333",
    ]
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
