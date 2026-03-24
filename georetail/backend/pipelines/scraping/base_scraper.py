"""
pipelines/scraping/base_scraper.py — Cliente base con anti-bot mejorado para Docker.

PROBLEMA RAÍZ DEL 403 DESDE DOCKER:
  Los contenedores Docker usan subredes privadas (172.17.x.x, 172.18.x.x).
  Cuando httpx/curl_cffi hace una petición, algunos portales detectan:
    1. Ausencia de cabeceras típicas de un navegador real
    2. TLS fingerprint de una librería Python, no de Chrome
    3. El User-Agent no coincide con el TLS fingerprint
    4. Falta de cookies de sesión previas (primera visita sin historial)

SOLUCIONES IMPLEMENTADAS:
  1. curl_cffi con impersonate="chrome131" — TLS fingerprint idéntico a Chrome
  2. Secuencia de cabeceras exacta que usa Chrome 131 en Windows
  3. Sin cabeceras que delaten automatización (sin X-Forwarded-For, sin X-Real-IP)
  4. Cookie jar persistente: simula sesión con visita previa a la homepage
  5. Orden de cabeceras correcto (los navegadores tienen un orden específico)
  6. Referer realista en todas las peticiones
  7. Backoff exponencial con jitter para no activar rate limiting
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

# ── User-Agents de Chrome recientes (Windows y Mac) ──────────────────────────
_USER_AGENTS = [
    # Chrome 131 Windows (el más común globalmente)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    # Chrome 130 Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    # Chrome 131 Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    # Chrome 130 Mac
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    # Firefox 132 Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
    # Edge 131 Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
]

# ── Cabeceras completas que envía Chrome 131 ──────────────────────────────────
# IMPORTANTE: el orden de las cabeceras importa — los WAF lo verifican
def _chrome_headers(ua: str, referer: str = "", host: str = "") -> dict:
    headers = {
        "sec-ch-ua":                '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "sec-ch-ua-mobile":         "?0",
        "sec-ch-ua-platform":       '"Windows"' if "Windows" in ua else '"macOS"',
        "Upgrade-Insecure-Requests": "1",
        "User-Agent":               ua,
        "Accept":                   "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Sec-Fetch-Site":           "none" if not referer else "same-origin",
        "Sec-Fetch-Mode":           "navigate",
        "Sec-Fetch-User":           "?1",
        "Sec-Fetch-Dest":           "document",
        "Accept-Encoding":          "gzip, deflate, br, zstd",
        "Accept-Language":          "es-ES,es;q=0.9,ca;q=0.8,en-GB;q=0.7,en;q=0.6",
        "Cache-Control":            "max-age=0",
        # NO incluir: X-Forwarded-For, X-Real-IP, Via — delatan proxy/Docker
    }
    if referer:
        headers["Referer"] = referer
    return headers


@dataclass
class ScrapingConfig:
    """Configuración del sistema de scraping, leída de variables de entorno."""
    service:         str   = field(default_factory=lambda: os.environ.get("SCRAPING_SERVICE", "none").lower())
    scrapingbee_key: str   = field(default_factory=lambda: os.environ.get("SCRAPINGBEE_API_KEY", ""))
    zenrows_key:     str   = field(default_factory=lambda: os.environ.get("ZENROWS_API_KEY", ""))
    proxies_raw:     str   = field(default_factory=lambda: os.environ.get("SCRAPING_PROXIES", ""))
    delay_min:       float = 2.0   # más conservador para no activar rate limiting
    delay_max:       float = 5.0
    timeout:         int   = 25
    max_retries:     int   = 3

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
    Scraper base con anti-bot mejorado para funcionar desde Docker.

    Uso:
        async with BaseScraper() as s:
            html = await s.get("https://www.pisos.com/...")
    """

    def __init__(self, config: Optional[ScrapingConfig] = None):
        self.cfg = config or ScrapingConfig()
        self._request_count = 0
        self._ua = random.choice(_USER_AGENTS)  # UA fijo por sesión (consistencia)
        self._cookies: dict = {}  # cookie jar simple

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass

    async def get(self, url: str, referer: str = "", **kwargs) -> str:
        """
        Descarga una página con múltiples capas anti-bot.
        Devuelve HTML como string, o "" si todos los métodos fallan.
        """
        await self._delay()
        self._request_count += 1

        for attempt in range(self.cfg.max_retries):
            try:
                html = await self._get_with_best_method(url, referer, **kwargs)
                if html and len(html) > 500:
                    return html
                elif html and len(html) > 0:
                    # Respuesta corta — puede ser un redirect o página de error pequeña
                    logger.debug("Respuesta corta (%d bytes) para %s", len(html), url)

            except Exception as e:
                logger.debug("Intento %d/%d fallido para %s: %s",
                             attempt + 1, self.cfg.max_retries, url, e)

            if attempt < self.cfg.max_retries - 1:
                wait = (2 ** attempt) + random.uniform(0, 1)
                await asyncio.sleep(wait)

        logger.warning("Todos los métodos fallaron para %s", url)
        return ""

    async def _get_with_best_method(self, url: str, referer: str, **kwargs) -> str:
        """Selecciona el mejor método según configuración."""
        # 1. Servicios de pago si están configurados
        if self.cfg.service == "scrapingbee" and self.cfg.scrapingbee_key:
            return await self._get_scrapingbee(url)
        if self.cfg.service == "zenrows" and self.cfg.zenrows_key:
            return await self._get_zenrows(url)

        # 2. curl_cffi — el más efectivo para evitar detección desde Docker
        html = await self._get_curl_cffi(url, referer)
        if html and len(html) > 500:
            return html

        # 3. httpx como fallback
        html = await self._get_httpx(url, referer)
        if html and len(html) > 500:
            return html

        return ""

    async def _get_curl_cffi(self, url: str, referer: str) -> str:
        """
        curl_cffi impersona el TLS fingerprint completo de Chrome.
        Es la capa más efectiva contra Cloudflare y WAFs básicos.
        Desde Docker funciona porque el TLS fingerprint es el de Chrome real.
        """
        try:
            from curl_cffi.requests import AsyncSession

            proxy = self.cfg.random_proxy()

            # Rotar entre versiones de Chrome para no ser predecible
            impersonate = random.choice([
                "chrome131", "chrome130", "chrome129",
                "chrome124", "chrome120",
            ])

            async with AsyncSession(
                impersonate=impersonate,
                proxies={"https": proxy, "http": proxy} if proxy else None,
                timeout=self.cfg.timeout,
            ) as session:
                # Primero visitar la homepage para obtener cookies (simula navegación real)
                host = _extraer_host(url)
                if host and host not in self._cookies:
                    try:
                        r_home = await session.get(
                            f"https://{host}",
                            headers=_chrome_headers(self._ua),
                            allow_redirects=True,
                        )
                        # Guardar cookies de la homepage
                        if r_home.status_code == 200:
                            self._cookies[host] = True
                            await asyncio.sleep(random.uniform(0.5, 1.5))
                    except Exception:
                        pass  # Si falla la homepage, intentamos directamente

                r = await session.get(
                    url,
                    headers=_chrome_headers(self._ua, referer),
                    allow_redirects=True,
                    cookies=session.cookies,
                )

                if r.status_code == 200:
                    return r.text
                elif r.status_code == 403:
                    logger.debug("curl_cffi: 403 para %s (anti-bot activo)", url)
                    return ""
                elif r.status_code == 429:
                    logger.warning("Rate limited en %s — esperando 30s", url)
                    await asyncio.sleep(30)
                    return ""
                else:
                    logger.debug("curl_cffi: status %d para %s", r.status_code, url)
                    return ""

        except ImportError:
            logger.debug("curl_cffi no instalado — pip install curl_cffi")
            return ""
        except Exception as e:
            logger.debug("curl_cffi error en %s: %s", url, e)
            return ""

    async def _get_httpx(self, url: str, referer: str) -> str:
        """httpx con HTTP/2 y cabeceras completas de Chrome."""
        try:
            import httpx

            proxy = self.cfg.random_proxy()
            headers = _chrome_headers(self._ua, referer)

            async with httpx.AsyncClient(
                timeout=self.cfg.timeout,
                follow_redirects=True,
                proxies={"https://": proxy} if proxy else None,
                http2=True,
                verify=True,
            ) as c:
                r = await c.get(url, headers=headers)

                if r.status_code == 200:
                    return r.text
                elif r.status_code == 429:
                    await asyncio.sleep(30)
                    return ""
                else:
                    logger.debug("httpx: status %d para %s", r.status_code, url)
                    return ""

        except Exception as e:
            logger.debug("httpx error en %s: %s", url, e)
            return ""

    async def _get_scrapingbee(self, url: str) -> str:
        """ScrapingBee — maneja Cloudflare y DataDome. ~$49/mes."""
        try:
            import httpx
            params = {
                "api_key":        self.cfg.scrapingbee_key,
                "url":            url,
                "render_js":      "false",
                "premium_proxy":  "true",
                "country_code":   "es",
                "block_ads":      "true",
                "block_resources": "true",
            }
            async with httpx.AsyncClient(timeout=60.0) as c:
                r = await c.get("https://app.scrapingbee.com/api/v1/", params=params)
                if r.status_code == 200:
                    return r.text
                logger.warning("ScrapingBee status %d", r.status_code)
                return ""
        except Exception as e:
            logger.warning("ScrapingBee error: %s", e)
            return ""

    async def _get_zenrows(self, url: str) -> str:
        """ZenRows — muy efectivo contra Cloudflare y Datadome. ~$49/mes."""
        try:
            import httpx
            params = {
                "apikey":         self.cfg.zenrows_key,
                "url":            url,
                "antibot":        "true",
                "premium_proxy":  "true",
                "proxy_country":  "es",
            }
            async with httpx.AsyncClient(timeout=60.0) as c:
                r = await c.get("https://api.zenrows.com/v1/", params=params)
                if r.status_code == 200:
                    return r.text
                logger.warning("ZenRows status %d", r.status_code)
                return ""
        except Exception as e:
            logger.warning("ZenRows error: %s", e)
            return ""

    async def get_with_playwright(self, url: str, wait_selector: str = "body") -> str:
        """
        Playwright con medidas stealth — para páginas que necesitan JS.
        Más lento pero efectivo para Cloudflare con JS challenge.
        """
        try:
            from playwright.async_api import async_playwright

            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--disable-dev-shm-usage",
                        "--no-sandbox",
                        "--disable-gpu",
                        "--disable-web-security",
                        "--disable-features=IsolateOrigins,site-per-process",
                    ],
                )
                ctx = await browser.new_context(
                    user_agent=self._ua,
                    locale="es-ES",
                    timezone_id="Europe/Madrid",
                    viewport={"width": 1366, "height": 768},
                    extra_http_headers={
                        "Accept-Language": "es-ES,es;q=0.9,ca;q=0.8",
                        "sec-ch-ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
                        "sec-ch-ua-mobile": "?0",
                        "sec-ch-ua-platform": '"Windows"',
                    },
                )
                await ctx.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4, 5]});
                    Object.defineProperty(navigator, 'languages', {get: () => ['es-ES', 'es', 'ca']});
                    window.chrome = { runtime: {}, loadTimes: () => {}, csi: () => {} };
                    Object.defineProperty(navigator, 'platform', {get: () => 'Win32'});
                """)
                page = await ctx.new_page()
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                try:
                    await page.wait_for_selector(wait_selector, timeout=8000)
                except Exception:
                    pass  # Continuar aunque no aparezca el selector
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight / 3)")
                await asyncio.sleep(random.uniform(0.8, 2.0))
                html = await page.content()
                await browser.close()
                return html

        except ImportError:
            logger.warning("Playwright no instalado: pip install playwright && playwright install chromium")
            return ""
        except Exception as e:
            logger.warning("Playwright error en %s: %s", url, e)
            return ""

    async def _delay(self):
        """Delay humano entre peticiones."""
        if self._request_count > 0:
            delay = random.uniform(self.cfg.delay_min, self.cfg.delay_max)
            # Pausa larga ocasional (simula que el usuario leyó la página)
            if random.random() < 0.08:
                delay *= random.uniform(2.5, 5.0)
            await asyncio.sleep(delay)


# ── Utilidades ────────────────────────────────────────────────────────────────

def _extraer_host(url: str) -> Optional[str]:
    """Extrae el hostname de una URL."""
    try:
        from urllib.parse import urlparse
        return urlparse(url).netloc
    except Exception:
        return None
