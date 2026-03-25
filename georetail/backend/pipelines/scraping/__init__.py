"""
pipelines/scraping/ — Sistema de scraping de portales inmobiliarios y Google Maps con anti-bot.

Portales soportados:
  - Idealista    (idealista.com)    — curl_cffi (reemplaza API oficial)
  - Fotocasa     (fotocasa.es)      — curl_cffi chrome124 + JSON-LD + BeautifulSoup
  - Habitaclia   (habitaclia.com)   — curl_cffi chrome124 + session warming
  - Milanuncios  (milanuncios.com)  — curl_cffi chrome124 + parsing robusto precio/m2
  - Pisos.com    (pisos.com)        — curl_cffi
  - Google Maps  (maps.google.com)  — Playwright stealth + Bezier mouse + session warming

Estrategia anti-bot (capas, de menor a mayor coste):
  1. curl_cffi  — impersona TLS fingerprint del navegador (derrota la mayoría de checks)
  2. Headers + User-Agent reales rotativos + cookies persistentes por sesión
  3. Session warming: homepage → búsqueda genérica → búsqueda real
  4. Delays aleatorios humanos entre peticiones (2-4s base, pausa larga 8% probabilidad)
  5. Playwright con stealth completo para páginas que exigen JS (Google Maps)
  6. Movimientos de ratón tipo Bezier (Google Maps)
  7. Backoff exponencial en rate limits (429, CAPTCHA)
  8. ScrapingBee / ZenRows como fallback para Cloudflare nivel alto

Configurar en .env:
  SCRAPING_SERVICE=none|scrapingbee|zenrows   (default: none)
  SCRAPINGBEE_API_KEY=...
  ZENROWS_API_KEY=...
  SCRAPING_PROXIES=http://user:pass@proxy1:8080,http://user:pass@proxy2:8080
  SCRAPING_SESSIONS_DIR=/tmp/georetail_sessions   (cookies persistentes)
"""
from .base_scraper import BaseScraper, ScrapingConfig
from .idealista_scraper import IdealistaScraper
from .fotocasa_scraper import FotocasaScraper, LocalScraped
from .habitaclia_scraper import HabitacliaScraper
from .milanuncios_scraper import MilanunciosScraper
from .pisos_scraper import PisosScraper
from .google_maps_scraper import GoogleMapsScraper, NegocioScraped

__all__ = [
    "BaseScraper",
    "ScrapingConfig",
    "IdealistaScraper",
    "FotocasaScraper",
    "LocalScraped",
    "HabitacliaScraper",
    "MilanunciosScraper",
    "PisosScraper",
    "GoogleMapsScraper",
    "NegocioScraped",
]
