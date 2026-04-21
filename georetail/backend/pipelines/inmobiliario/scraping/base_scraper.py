"""
pipelines/scraping/base_scraper.py — Cliente base con múltiples capas anti-bot.

Jerarquía de métodos (se prueba en orden hasta que uno funciona):
  1. curl_cffi  — TLS fingerprinting impersonation, cero dependencias de browser
  2. httpx      — Fallback ligero con headers realistas
  3. ScrapingBee / ZenRows — Si hay API key configurada
  4. Playwright — Browser real para páginas que necesitan JS (opcional)
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

# ── User-Agents reales (Chrome 120-131, Windows/Mac) ──────────────────────────
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.7; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
]

# ── Headers base que imitan un navegador real ──────────────────────────────────
def _browser_headers(ua: str, referer: str = "") -> dict:
    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "es-ES,es;q=0.9,ca;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none" if not referer else "same-origin",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }
    if referer:
        headers["Referer"] = referer
    return headers


@dataclass
class ScrapingConfig:
    """Configuración del sistema de scraping, leída de variables de entorno."""
    service: str = field(default_factory=lambda: os.environ.get("SCRAPING_SERVICE", "none").lower())
    scrapingbee_key: str = field(default_factory=lambda: os.environ.get("SCRAPINGBEE_API_KEY", ""))
    zenrows_key: str = field(default_factory=lambda: os.environ.get("ZENROWS_API_KEY", ""))
    proxies_raw: str = field(default_factory=lambda: os.environ.get("SCRAPING_PROXIES", ""))
    delay_min: float = 1.5   # segundos mínimos entre peticiones
    delay_max: float = 4.5   # segundos máximos entre peticiones
    timeout: int = 20        # timeout HTTP en segundos
    max_retries: int = 3

    @property
    def proxy_list(self) -> list[str]:
        if not self.proxies_raw:
            return []
        return [p.strip() for p in self.proxies_raw.split(",") if p.strip()]

    def random_proxy(self) -> Optional[str]:
        proxies = self.proxy_list
        return random.choice(proxies) if proxies else None


class BaseScraper:
    """
    Scraper base con múltiples capas anti-bot.

    Uso:
        async with BaseScraper() as s:
            html = await s.get("https://www.fotocasa.es/...")
    """

    def __init__(self, config: Optional[ScrapingConfig] = None):
        self.cfg = config or ScrapingConfig()
        self._session = None  # curl_cffi AsyncSession
        self._request_count = 0

    # ── Context manager ────────────────────────────────────────────────────────
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        if self._session:
            try:
                await self._session.close()
            except Exception:
                pass

    # ── Método principal ───────────────────────────────────────────────────────
    async def get(self, url: str, referer: str = "", **kwargs) -> str:
        """
        Descarga una página. Prueba métodos en orden hasta obtener contenido.
        Devuelve el HTML como string, o "" si todos los métodos fallan.
        """
        await self._delay()
        self._request_count += 1

        for attempt in range(self.cfg.max_retries):
            try:
                html = await self._get_with_best_method(url, referer, **kwargs)
                if html and len(html) > 500:  # respuesta mínima válida
                    return html
            except Exception as e:
                logger.warning("Intento %d/%d fallido para %s: %s",
                               attempt + 1, self.cfg.max_retries, url, e)
                if attempt < self.cfg.max_retries - 1:
                    await asyncio.sleep(2 ** attempt)  # backoff exponencial

        logger.warning("Todos los métodos fallaron para %s", url)
        return ""

    # ── Lógica de selección de método ─────────────────────────────────────────
    async def _get_with_best_method(self, url: str, referer: str, **kwargs) -> str:
        # 1. ScrapingBee / ZenRows si están configurados
        if self.cfg.service == "scrapingbee" and self.cfg.scrapingbee_key:
            return await self._get_scrapingbee(url)
        if self.cfg.service == "zenrows" and self.cfg.zenrows_key:
            return await self._get_zenrows(url)

        # 2. curl_cffi — impersona TLS fingerprint de Chrome
        html = await self._get_curl_cffi(url, referer)
        if html and len(html) > 500:
            return html

        # 3. httpx — fallback ligero
        return await self._get_httpx(url, referer)

    # ── curl_cffi (TLS impersonation) ──────────────────────────────────────────
    async def _get_curl_cffi(self, url: str, referer: str) -> str:
        try:
            from curl_cffi.requests import AsyncSession
            ua = random.choice(_USER_AGENTS)
            proxy = self.cfg.random_proxy()
            async with AsyncSession(
                impersonate="chrome131",  # impersona Chrome 131
                proxies={"https": proxy, "http": proxy} if proxy else None,
                timeout=self.cfg.timeout,
            ) as session:
                r = await session.get(
                    url,
                    headers=_browser_headers(ua, referer),
                    allow_redirects=True,
                )
                if r.status_code == 200:
                    return r.text
                logger.debug("curl_cffi status %d para %s", r.status_code, url)
                return ""
        except ImportError:
            logger.debug("curl_cffi no instalado — usando httpx")
            return ""
        except Exception as e:
            logger.debug("curl_cffi error en %s: %s", url, e)
            return ""

    # ── httpx — fallback ───────────────────────────────────────────────────────
    async def _get_httpx(self, url: str, referer: str) -> str:
        try:
            import httpx
            ua = random.choice(_USER_AGENTS)
            proxy = self.cfg.random_proxy()
            # httpx 0.28 eliminó `proxies=`; ahora es `proxy=` (str) o `mounts=` (dict).
            # random_proxy() devuelve str|None, directamente compatible.
            async with httpx.AsyncClient(
                timeout=self.cfg.timeout,
                follow_redirects=True,
                proxy=proxy if proxy else None,
                http2=True,
            ) as c:
                r = await c.get(url, headers=_browser_headers(ua, referer))
                if r.status_code == 200:
                    return r.text
                logger.debug("httpx status %d para %s", r.status_code, url)
                return ""
        except Exception as e:
            logger.debug("httpx error en %s: %s", url, e)
            return ""

    # ── ScrapingBee ────────────────────────────────────────────────────────────
    async def _get_scrapingbee(self, url: str) -> str:
        """
        ScrapingBee maneja Cloudflare, captchas y rotación de proxies.
        https://www.scrapingbee.com/
        Coste: ~$49/mes para 150k créditos. Cada petición JS = 5 créditos.
        """
        try:
            import httpx
            params = {
                "api_key": self.cfg.scrapingbee_key,
                "url": url,
                "render_js": "false",   # true si la página necesita JS
                "premium_proxy": "true",
                "country_code": "es",
                "block_ads": "true",
                "block_resources": "true",  # ahorra créditos
            }
            async with httpx.AsyncClient(timeout=30.0) as c:
                r = await c.get("https://app.scrapingbee.com/api/v1/", params=params)
                if r.status_code == 200:
                    return r.text
                logger.warning("ScrapingBee status %d para %s", r.status_code, url)
                return ""
        except Exception as e:
            logger.warning("ScrapingBee error: %s", e)
            return ""

    # ── ZenRows ────────────────────────────────────────────────────────────────
    async def _get_zenrows(self, url: str) -> str:
        """
        ZenRows — muy efectivo contra Cloudflare y Datadome.
        https://www.zenrows.com/
        Coste: ~$49/mes para 250k peticiones.
        """
        try:
            import httpx
            params = {
                "apikey": self.cfg.zenrows_key,
                "url": url,
                "antibot": "true",
                "premium_proxy": "true",
                "proxy_country": "es",
            }
            async with httpx.AsyncClient(timeout=30.0) as c:
                r = await c.get("https://api.zenrows.com/v1/", params=params)
                if r.status_code == 200:
                    return r.text
                logger.warning("ZenRows status %d para %s", r.status_code, url)
                return ""
        except Exception as e:
            logger.warning("ZenRows error: %s", e)
            return ""

    # ── Playwright (para páginas con JS obligatorio) ───────────────────────────
    async def get_with_playwright(self, url: str, wait_selector: str = "body") -> str:
        """
        Abre la URL en un Chromium real con medidas stealth.
        Usar solo cuando curl_cffi no funcione — es lento (3-8s/página).
        Requiere: pip install playwright && playwright install chromium
        """
        try:
            from playwright.async_api import async_playwright
            ua = random.choice(_USER_AGENTS)
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--disable-dev-shm-usage",
                        "--no-sandbox",
                        "--disable-gpu",
                    ],
                )
                ctx = await browser.new_context(
                    user_agent=ua,
                    locale="es-ES",
                    timezone_id="Europe/Madrid",
                    viewport={"width": 1366, "height": 768},
                    # Ocultar que es Playwright:
                    extra_http_headers={
                        "Accept-Language": "es-ES,es;q=0.9",
                        "DNT": "1",
                    },
                )
                # Eliminar señales de webdriver
                await ctx.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    Object.defineProperty(navigator, 'plugins', {get: () => {
                        const p = [{name:'Chrome PDF Plugin'},{name:'Chrome PDF Viewer'},{name:'Native Client'}];
                        p.refresh = () => {}; p.item = (i) => p[i]; p.namedItem = (n) => p.find(x=>x.name===n)||null;
                        return p;
                    }});
                    window.chrome = { runtime: {} };
                """)
                page = await ctx.new_page()
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_selector(wait_selector, timeout=10000)
                # Simular scroll humano
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 3)")
                await asyncio.sleep(random.uniform(0.5, 1.5))
                html = await page.content()
                await browser.close()
                return html
        except ImportError:
            logger.warning("Playwright no instalado. Instalar: pip install playwright && playwright install chromium")
            return ""
        except Exception as e:
            logger.warning("Playwright error en %s: %s", url, e)
            return ""

    # ── Delay aleatorio entre peticiones ──────────────────────────────────────
    async def _delay(self):
        """Espera aleatoria para no saturar el servidor y evitar detección."""
        if self._request_count > 0:
            delay = random.uniform(self.cfg.delay_min, self.cfg.delay_max)
            # Ocasionalmente hacer una pausa más larga (comportamiento humano)
            if random.random() < 0.1:
                delay *= random.uniform(2.0, 4.0)
            await asyncio.sleep(delay)
