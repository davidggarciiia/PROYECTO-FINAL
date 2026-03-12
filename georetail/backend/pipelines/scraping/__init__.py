"""
pipelines/scraping/ — Sistema de scraping de portales inmobiliarios con anti-bot.

Portales soportados:
  - Fotocasa     (fotocasa.es)      — Playwright + curl_cffi
  - Habitaclia   (habitaclia.com)   — curl_cffi
  - Milanuncios  (milanuncios.com)  — curl_cffi + Playwright fallback
  - Pisos.com    (pisos.com)        — curl_cffi

Estrategia anti-bot (capas, de menor a mayor coste):
  1. curl_cffi  — impersona TLS fingerprint del navegador (derrota la mayoría de checks)
  2. Headers + User-Agent reales rotativos
  3. Delays aleatorios entre peticiones
  4. Playwright con stealth para páginas que exigen JS
  5. ScrapingBee / ZenRows como fallback para Cloudflare nivel alto

Configurar en .env:
  SCRAPING_SERVICE=none|scrapingbee|zenrows   (default: none)
  SCRAPINGBEE_API_KEY=...
  ZENROWS_API_KEY=...
  SCRAPING_PROXIES=http://user:pass@proxy1:8080,http://user:pass@proxy2:8080
"""
from .base_scraper import BaseScraper, ScrapingConfig
from .fotocasa_scraper import FotocasaScraper
from .habitaclia_scraper import HabitacliaScraper
from .milanuncios_scraper import MilanunciosScraper
from .pisos_scraper import PisosScraper

__all__ = [
    "BaseScraper",
    "ScrapingConfig",
    "FotocasaScraper",
    "HabitacliaScraper",
    "MilanunciosScraper",
    "PisosScraper",
]
