"""
scrapers/ — Módulo de scraping de portales inmobiliarios de locales comerciales.

Portales implementados:
  - Fotocasa    (Cloudflare → Playwright + stealth)
  - Habitaclia  (JS rendering → Playwright + stealth)
  - Milanuncios (moderado → httpx + fallback Playwright)
  - pisos.com   (básico → httpx + BeautifulSoup)

Técnicas anti-detección aplicadas:
  1. playwright-stealth   → parchea 23 señales de bot
  2. User-Agent rotativo  → pool de UAs reales de Chrome/Firefox
  3. Delays aleatorios    → distribución normal entre páginas
  4. Proxy residencial    → opcional via SCRAPER_PROXY_URL
  5. Headers completos    → Accept, Accept-Language, Sec-Fetch-*
  6. Viewport / locale aleatorio
  7. ScraperAPI           → último recurso si bloqueo persistente
"""
from scrapers.fotocasa import FotocasaScraper
from scrapers.habitaclia import HabitacliaScraper
from scrapers.milanuncios import MilanunciosScraper
from scrapers.pisos_com import PisosComScraper

__all__ = [
    "FotocasaScraper",
    "HabitacliaScraper",
    "MilanunciosScraper",
    "PisosComScraper",
]
