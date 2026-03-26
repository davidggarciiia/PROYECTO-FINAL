"""
pipelines/scraping/base_scraper.py — Cliente HTTP con capas anti-bot.

Orden de intentos:
  1. curl_cffi chrome131  — TLS fingerprint idéntico a Chrome (el más efectivo)
  2. httpx con HTTP/2     — fallback ligero
  3. ScrapingBee/ZenRows  — si hay API key configurada en .env
  4. Playwright           — browser real para páginas con JS challenge

NOTA SOBRE EL 403 DESDE DOCKER:
  El error que veíamos era causado por visitar la homepage antes de cada anuncio.
  Eso duplicaba las peticiones y activaba el rate limit de Cloudflare.
  Solución: NO hacer prefetch de homepage, simplemente intentar directo con curl_cffi.
  curl_cffi ya gestiona cookies automáticamente dentro de la sesión.
"""
from __future__ import annotations

import asyncio
import logging
import os
import random
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0",
]


def _browser_headers(ua: str, referer: str = "") -> dict:
    headers = {
        "User-Agent":               ua,
        "Accept":                   "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language":          "es-ES,es;q=0.9,ca;q=0.8,en;q=0.7",
        "Accept-Encoding":          "gzip, deflate, br",
        "sec-ch-ua":                '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "sec-ch-ua-mobile":         "?0",
        "sec-ch-ua-platform":       '"Windows"' if "Windows" in ua else '"macOS"',
        "Upgrade-Insecure-Requests":"1",
        "Sec-Fetch-Dest":           "document",
        "Sec-Fetch-Mode":           "navigate",
        "Sec-Fetch-Site":           "none" if not referer else "same-origin",
        "Sec-Fetch-User":           "?1",
        "Cache-Control":            "max-age=0",
        "Connection":               "keep-alive",
    }
    if referer:
        headers["Referer"] = referer
    return headers


@dataclass
class ScrapingConfig:
    service:         str   = field(default_factory=lambda: os.environ.get("SCRAPING_SERVICE", "none").lower())
    scrapingbee_key: str   = field(default_factory=lambda: os.environ.get("SCRAPINGBEE_API_KEY", ""))
    zenrows_key:     str   = field(default_factory=lambda: os.environ.get("ZENROWS_API_KEY", ""))
    proxies_raw:     str   = field(default_factory=lambda: os.environ.get("SCRAPING_PROXIES", ""))
    delay_min:       float = 1.5
    delay_max:       float = 4.5
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
    def __init__(self, config: Optional[ScrapingConfig] = None):
        self.cfg           = config or ScrapingConfig()
        self._request_count = 0
        self._ua           = random.choice(_USER_AGENTS)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        pass

    async def get(self, url: str, referer: str = "", **kwargs) -> str:
        await self._delay()
        self._request_count += 1

        for attempt in range(self.cfg.max_retries):
            try:
                html = await self._get_with_best_method(url, referer)
                if html and len(html) > 500:
                    return html
            except Exception as e:
                logger.debug("Intento %d/%d fallido para %s: %s",
                             attempt + 1, self.cfg.max_retries, url, e)
            if attempt < self.cfg.max_retries - 1:
                await asyncio.sleep(2 ** attempt + random.uniform(0, 1))

        logger.warning("Todos los métodos fallaron para %s", url)
        return ""

    async def _get_with_best_method(self, url: str, referer: str) -> str:
        if self.cfg.service == "scrapingbee" and self.cfg.scrapingbee_key:
            return await self._get_scrapingbee(url)
        if self.cfg.service == "zenrows" and self.cfg.zenrows_key:
            return await self._get_zenrows(url)

        html = await self._get_curl_cffi(url, referer)
        if html and len(html) > 500:
            return html

        html = await self._get_httpx(url, referer)
        if html and len(html) > 500:
            return html

        return ""

    async def _get_curl_cffi(self, url: str, referer: str) -> str:
        """
        curl_cffi impersona el TLS fingerprint completo de Chrome.
        Gestiona cookies automáticamente dentro de la sesión.
        NO hace prefetch de la homepage — eso causaba 403 por rate limit.
        """
        try:
            from curl_cffi.requests import AsyncSession

            proxy = self.cfg.random_proxy()
            impersonate = random.choice([
                "chrome131", "chrome130", "chrome129", "chrome124",
            ])

            async with AsyncSession(
                impersonate=impersonate,
                proxies={"https": proxy, "http": proxy} if proxy else None,
                timeout=self.cfg.timeout,
            ) as session:
                r = await session.get(
                    url,
                    headers=_browser_headers(self._ua, referer),
                    allow_redirects=True,
                )
                if r.status_code == 200:
                    return r.text
                if r.status_code == 429:
                    logger.warning("Rate limit (429) en %s — esperando 30s", url)
                    await asyncio.sleep(30)
                    return ""
                if r.status_code == 403:
                    logger.debug("curl_cffi: 403 para %s", url)
                    return ""
                logger.debug("curl_cffi: status %d para %s", r.status_code, url)
                return ""
        except ImportError:
            return ""
        except Exception as e:
            logger.debug("curl_cffi error en %s: %s", url, e)
            return ""

    async def _get_httpx(self, url: str, referer: str) -> str:
        try:
            import httpx
            proxy = self.cfg.random_proxy()
            async with httpx.AsyncClient(
                timeout=self.cfg.timeout,
                follow_redirects=True,
                proxies={"https://": proxy} if proxy else None,
                http2=True,
            ) as c:
                r = await c.get(url, headers=_browser_headers(self._ua, referer))
                if r.status_code == 200:
                    return r.text
                if r.status_code == 429:
                    await asyncio.sleep(30)
                return ""
        except Exception as e:
            logger.debug("httpx error en %s: %s", url, e)
            return ""

    async def _get_scrapingbee(self, url: str) -> str:
        try:
            import httpx
            params = {
                "api_key": self.cfg.scrapingbee_key, "url": url,
                "render_js": "false", "premium_proxy": "true",
                "country_code": "es",
            }
            async with httpx.AsyncClient(timeout=60.0) as c:
                r = await c.get("https://app.scrapingbee.com/api/v1/", params=params)
                return r.text if r.status_code == 200 else ""
        except Exception as e:
            logger.warning("ScrapingBee error: %s", e)
            return ""

    async def _get_zenrows(self, url: str) -> str:
        try:
            import httpx
            params = {
                "apikey": self.cfg.zenrows_key, "url": url,
                "antibot": "true", "premium_proxy": "true", "proxy_country": "es",
            }
            async with httpx.AsyncClient(timeout=60.0) as c:
                r = await c.get("https://api.zenrows.com/v1/", params=params)
                return r.text if r.status_code == 200 else ""
        except Exception as e:
            logger.warning("ZenRows error: %s", e)
            return ""

    async def get_with_playwright(self, url: str, wait_selector: str = "body") -> str:
        try:
            from playwright.async_api import async_playwright
            async with async_playwright() as p:
                browser = await p.chromium.launch(
                    headless=True,
                    args=["--disable-blink-features=AutomationControlled",
                          "--disable-dev-shm-usage", "--no-sandbox",
                          "--disable-gpu"],
                )
                ctx = await browser.new_context(
                    user_agent=self._ua, locale="es-ES",
                    timezone_id="Europe/Madrid",
                    viewport={"width": 1366, "height": 768},
                )
                await ctx.add_init_script(
                    "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
                    "window.chrome={runtime:{}};"
                )
                page = await ctx.new_page()
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                try:
                    await page.wait_for_selector(wait_selector, timeout=8000)
                except Exception:
                    pass
                await asyncio.sleep(random.uniform(0.5, 1.5))
                html = await page.content()
                await browser.close()
                return html
        except ImportError:
            logger.warning("Playwright no instalado")
            return ""
        except Exception as e:
            logger.warning("Playwright error en %s: %s", url, e)
            return ""

    async def _delay(self):
        if self._request_count > 0:
            delay = random.uniform(self.cfg.delay_min, self.cfg.delay_max)
            if random.random() < 0.06:
                delay *= random.uniform(2.0, 4.0)
            await asyncio.sleep(delay)
