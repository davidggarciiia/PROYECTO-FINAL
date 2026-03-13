"""
scrapers/base.py — Clase base con todas las técnicas anti-detección.

Cada portal hereda de BaseScraper y sólo implementa:
  - URL_PLANTILLA: f-string con {pagina}
  - _parsear_pagina(html: str) -> list[dict]

El bucle de paginación, delays, proxy, stealth y reintentos
quedan completamente encapsulados aquí.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import httpx

logger = logging.getLogger(__name__)

# ─── Pool de User-Agents reales (Chrome 124-128 / Firefox 127) ────────────────
_USER_AGENTS = [
    # Chrome 128 Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    # Chrome 127 macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.6533.119 Safari/537.36",
    # Chrome 126 Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    # Firefox 128 Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:128.0) Gecko/20100101 Firefox/128.0",
    # Firefox 127 macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.5; rv:127.0) Gecko/20100101 Firefox/127.0",
    # Edge 127
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36 Edg/127.0.0.0",
    # Chrome 128 Android
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.6613.84 Mobile Safari/537.36",
]

# ─── Viewports reales para Playwright ─────────────────────────────────────────
_VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1440, "height": 900},
    {"width": 1366, "height": 768},
    {"width": 2560, "height": 1440},
    {"width": 1280, "height": 800},
]


@dataclass
class LocalRaw:
    """Datos crudos de un local extraído de cualquier portal."""
    portal: str
    portal_id: str
    titulo: str = ""
    direccion: str = ""
    barrio: str = ""
    precio: float | None = None          # alquiler mensual €
    m2: float | None = None
    planta: str = "PB"
    lat: float | None = None
    lng: float | None = None
    url: str = ""
    caracteristicas: dict[str, Any] = field(default_factory=dict)

    @property
    def precio_m2(self) -> float | None:
        if self.precio and self.m2 and self.m2 > 0:
            return round(self.precio / self.m2, 2)
        return None


class BaseScraper(ABC):
    """
    Clase base para todos los scrapers de portales inmobiliarios.

    Estrategia de extracción por capas:
      1. httpx puro  (más rápido, suficiente si el portal no protege agresivamente)
      2. Playwright  (navegador real Chromium, JS completo, stealth activo)
      3. ScraperAPI  (servicio externo con rotación automática de IP + CAPTCHA solver)

    La subclase decide cuál usar sobreescribiendo `_modo_extraccion`.
    """

    #: URL con placeholder {pagina} (int empezando en 1)
    URL_PLANTILLA: str = ""
    #: Número máximo de páginas a raspar por ejecución
    MAX_PAGINAS: int = 5
    #: Pausa mínima (segundos) entre páginas — con jitter aleatorio
    DELAY_MIN: float = 2.5
    DELAY_MAX: float = 7.0
    #: Nombre del portal para logs y fuente en DB
    NOMBRE: str = "portal"
    #: Si True, usa Playwright aunque httpx tenga éxito
    FORZAR_PLAYWRIGHT: bool = False

    def __init__(self) -> None:
        self._proxy: str | None = os.environ.get("SCRAPER_PROXY_URL") or None
        self._scraperapi_key: str | None = os.environ.get("SCRAPERAPI_KEY") or None
        self._ua = random.choice(_USER_AGENTS)
        self._viewport = random.choice(_VIEWPORTS)

    # ──────────────────────────────────────────────────────────────────────────
    # Interfaz pública
    # ──────────────────────────────────────────────────────────────────────────

    async def scrape(self) -> list[LocalRaw]:
        """Raspa todas las páginas y devuelve la lista consolidada."""
        resultados: list[LocalRaw] = []
        for pagina in range(1, self.MAX_PAGINAS + 1):
            url = self.URL_PLANTILLA.format(pagina=pagina)
            logger.info("[%s] Página %d → %s", self.NOMBRE, pagina, url)
            try:
                html = await self._obtener_html(url)
                items = self._parsear_pagina(html)
                if not items:
                    logger.info("[%s] Página %d vacía — deteniendo", self.NOMBRE, pagina)
                    break
                resultados.extend(items)
                logger.info("[%s] Página %d: %d locales", self.NOMBRE, pagina, len(items))
            except Exception as exc:
                logger.warning("[%s] Error en página %d: %s", self.NOMBRE, pagina, exc)
                break
            await self._esperar()
        logger.info("[%s] Total: %d locales", self.NOMBRE, len(resultados))
        return resultados

    # ──────────────────────────────────────────────────────────────────────────
    # Implementación abstracta — cada portal define su parser
    # ──────────────────────────────────────────────────────────────────────────

    @abstractmethod
    def _parsear_pagina(self, html: str) -> list[LocalRaw]:
        """
        Recibe el HTML/JSON de una página y devuelve la lista de locales.
        Debe retornar lista vacía si no hay más resultados.
        """

    # ──────────────────────────────────────────────────────────────────────────
    # Motor de obtención de HTML — tres niveles de escalada
    # ──────────────────────────────────────────────────────────────────────────

    async def _obtener_html(self, url: str) -> str:
        """
        Intenta obtener el HTML con la estrategia más ligera disponible.
        Escala automáticamente si encuentra bloqueo.
        """
        # Nivel 1: httpx (si el portal no usa JS crítico)
        if not self.FORZAR_PLAYWRIGHT:
            html = await self._httpx_get(url)
            if html and self._parece_valido(html):
                return html

        # Nivel 2: Playwright headless + stealth
        html = await self._playwright_get(url)
        if html and self._parece_valido(html):
            return html

        # Nivel 3: ScraperAPI (CAPTCHA solver externo)
        if self._scraperapi_key:
            html = await self._scraperapi_get(url)
            if html and self._parece_valido(html):
                return html

        raise RuntimeError(f"No se pudo obtener HTML de: {url}")

    async def _httpx_get(self, url: str) -> str:
        """GET con httpx simulando navegador real."""
        headers = self._headers_navegador()
        proxy_map = {"all://": self._proxy} if self._proxy else None
        try:
            async with httpx.AsyncClient(
                headers=headers,
                proxies=proxy_map,
                timeout=20.0,
                follow_redirects=True,
            ) as c:
                r = await c.get(url)
                if r.status_code in (403, 429, 503):
                    logger.debug("[%s] httpx bloqueado (%d) → escalando", self.NOMBRE, r.status_code)
                    return ""
                r.raise_for_status()
                return r.text
        except Exception as exc:
            logger.debug("[%s] httpx error: %s", self.NOMBRE, exc)
            return ""

    async def _playwright_get(self, url: str) -> str:
        """
        GET con Playwright (Chromium headless) + playwright-stealth.

        Técnicas anti-bot activas:
          - playwright-stealth: parchea navigator.webdriver, plugins, Canvas fingerprint,
            WebGL, AudioContext, Chrome runtime, permisos, etc.
          - User-Agent real del pool rotativo
          - Viewport aleatorio con deviceScaleFactor 1 o 2
          - Locale es-ES, timezone Europe/Madrid
          - Proxy residencial si está configurado
          - Comportamiento humano: scroll suave antes de extraer
        """
        try:
            from playwright.async_api import async_playwright
            from playwright_stealth import stealth_async
        except ImportError:
            logger.warning("playwright o playwright-stealth no instalados — pip install playwright playwright-stealth")
            return ""

        try:
            async with async_playwright() as p:
                launch_opts: dict[str, Any] = {
                    "headless": True,
                    "args": [
                        "--no-sandbox",
                        "--disable-blink-features=AutomationControlled",
                        "--disable-infobars",
                        "--window-size=1920,1080",
                        "--disable-extensions",
                        "--disable-gpu",
                        "--disable-dev-shm-usage",
                        "--lang=es-ES",
                    ],
                }
                if self._proxy:
                    launch_opts["proxy"] = {"server": self._proxy}

                browser = await p.chromium.launch(**launch_opts)
                context = await browser.new_context(
                    viewport=self._viewport,
                    user_agent=self._ua,
                    locale="es-ES",
                    timezone_id="Europe/Madrid",
                    device_scale_factor=random.choice([1, 2]),
                    java_script_enabled=True,
                    bypass_csp=False,
                    # Evitar que el fingerprint de idioma delate el bot
                    extra_http_headers={
                        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
                    },
                )
                page = await context.new_page()
                # Aplicar stealth ANTES de navegar
                await stealth_async(page)

                # Bloquear recursos innecesarios (tracking, analytics, anuncios)
                await page.route(
                    "**/{analytics,gtm,hotjar,facebook,doubleclick,googlesyndication,adnxs}**",
                    lambda route: route.abort(),
                )

                await page.goto(url, wait_until="domcontentloaded", timeout=30_000)

                # Pausa corta post-carga (simula lectura humana)
                await asyncio.sleep(random.uniform(1.5, 3.0))

                # Scroll suave hacia abajo (activa lazy-load y parece humano)
                await page.evaluate("""
                    async () => {
                        await new Promise(resolve => {
                            let total = 0;
                            const step = () => {
                                window.scrollBy(0, Math.floor(Math.random() * 120) + 80);
                                total += 200;
                                if (total < document.body.scrollHeight * 0.7) {
                                    setTimeout(step, Math.floor(Math.random() * 200) + 100);
                                } else { resolve(); }
                            };
                            step();
                        });
                    }
                """)
                await asyncio.sleep(random.uniform(0.5, 1.5))

                html = await page.content()
                await browser.close()
                return html

        except Exception as exc:
            logger.warning("[%s] Playwright error: %s", self.NOMBRE, exc)
            return ""

    async def _scraperapi_get(self, url: str) -> str:
        """
        Último recurso: ScraperAPI (gestiona proxies rotativos + CAPTCHA solver).
        Requiere SCRAPERAPI_KEY en entorno.
        Precio: ~0.001 USD/req en plan básico.
        """
        api_url = (
            f"http://api.scraperapi.com?api_key={self._scraperapi_key}"
            f"&url={url}&country_code=es&render=true"
        )
        try:
            async with httpx.AsyncClient(timeout=60.0) as c:
                r = await c.get(api_url)
                r.raise_for_status()
                return r.text
        except Exception as exc:
            logger.warning("[%s] ScraperAPI error: %s", self.NOMBRE, exc)
            return ""

    # ──────────────────────────────────────────────────────────────────────────
    # Utilidades
    # ──────────────────────────────────────────────────────────────────────────

    def _headers_navegador(self) -> dict[str, str]:
        """Headers HTTP que imitan un navegador real."""
        return {
            "User-Agent": self._ua,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        }

    def _parece_valido(self, html: str) -> bool:
        """
        Detecta páginas de bloqueo típicas (Cloudflare, CAPTCHA, bot-detection).
        Retorna True si el HTML parece contener resultados reales.
        """
        if len(html) < 500:
            return False
        html_lower = html.lower()
        senales_bloqueo = [
            "just a moment",          # Cloudflare waiting room
            "cloudflare",             # Cloudflare challenge
            "captcha",                # CAPTCHA genérico
            "robot",                  # "Are you a robot?"
            "access denied",
            "403 forbidden",
            "enable javascript",      # Página de "activa JS"
            "checking your browser",  # Cloudflare checking
            "ddos-guard",
            "security check",
        ]
        return not any(s in html_lower for s in senales_bloqueo)

    async def _esperar(self) -> None:
        """Pausa aleatoria entre páginas con distribución normal truncada."""
        delay = random.gauss(
            (self.DELAY_MIN + self.DELAY_MAX) / 2,
            (self.DELAY_MAX - self.DELAY_MIN) / 4,
        )
        delay = max(self.DELAY_MIN, min(self.DELAY_MAX, delay))
        await asyncio.sleep(delay)

    @staticmethod
    def _extraer_next_data(html: str) -> dict | None:
        """
        Extrae el JSON de __NEXT_DATA__ presente en apps Next.js.
        La mayoría de portales inmobiliarios modernos (Fotocasa, Idealista, pisos.com)
        usan Next.js y embeben todos los datos en este script.
        """
        import re
        m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.S)
        if m:
            try:
                return json.loads(m.group(1))
            except json.JSONDecodeError:
                pass
        return None

    @staticmethod
    def _float_seguro(valor: Any, default: float | None = None) -> float | None:
        """Convierte cualquier valor a float de forma segura."""
        if valor is None:
            return default
        try:
            return float(str(valor).replace(".", "").replace(",", ".").replace("€", "").strip())
        except (ValueError, TypeError):
            return default
