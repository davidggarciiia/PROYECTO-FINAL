"""
pipelines/scraping/ — Sistema de scraping de portales inmobiliarios con anti-bot.

Portales soportados:
  - Wallapop    (api.wallapop.com)  — API JSON directa, SIN scraping HTML ✅
  - Habitaclia  (habitaclia.com)   — curl_cffi
  - Fotocasa    (fotocasa.es)      — Playwright + curl_cffi
  - Idealista   (idealista.com)    — curl_cffi (bloqueado, usar con cautela)
  - Milanuncios (milanuncios.com)  — curl_cffi + Playwright fallback
  - Pisos.com   (pisos.com)        — curl_cffi

Estrategia anti-bloqueo:
  PRIORIDAD 1: Wallapop API JSON — sin scraping, sin Cloudflare, sin captchas
  PRIORIDAD 2: Habitaclia HTML   — curl_cffi (TLS impersonation)
  PRIORIDAD 3: Fotocasa HTML     — curl_cffi + Playwright
  EVITAR: Idealista              — bloquea activamente (403/captcha)

Configurar en .env:
  SCRAPING_SERVICE=none|scrapingbee|zenrows   (default: none)
  SCRAPINGBEE_API_KEY=...
  ZENROWS_API_KEY=...
  SCRAPING_PROXIES=http://user:pass@proxy1:8080,http://user:pass@proxy2:8080
"""
from .base_scraper import BaseScraper, ScrapingConfig
from .idealista_scraper import IdealistaScraper
from .fotocasa_scraper import FotocasaScraper
from .habitaclia_scraper import HabitacliaScraper
from .milanuncios_scraper import MilanunciosScraper
from .pisos_scraper import PisosScraper
from .wallapop_scraper import WallapopScraper
from .models import InmueblePortal, desde_dict_scraper

__all__ = [
    "BaseScraper",
    "ScrapingConfig",
    "IdealistaScraper",
    "FotocasaScraper",
    "HabitacliaScraper",
    "MilanunciosScraper",
    "PisosScraper",
    "WallapopScraper",
    "InmueblePortal",
    "desde_dict_scraper",
]
