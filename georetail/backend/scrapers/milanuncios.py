"""
scrapers/milanuncios.py — Scraper para Milanuncios (locales en alquiler, Barcelona).

Milanuncios es el mayor portal de clasificados de España.
  - Propietario: Adevinta (grupo Schibsted)
  - Protección: Moderada. PerimeterX/HUMAN Security bot protection.
  - Renderizado: React (SPA). Pero expone una API REST interna documentada
    que retorna JSON directamente → mucho más limpio que scraping HTML.

Estrategia:
  1. API JSON interna (endpoint /api/v1/ con headers correctos)
  2. Fallback: HTML con Playwright + stealth

URL API interna (no oficial, pero estable):
  GET https://www.milanuncios.com/api/v1/items?
    category=17300          (oficinas y locales)
    subfamilyId=subcat      (sin subcategoría = todos)
    province=8              (Barcelona provincia)
    locationId=12712        (Barcelona ciudad)
    isRent=true
    numPage={pagina}
    itemsPerPage=40

AVISO LEGAL: Milanuncios ToS (art. 5.4) prohíbe scraping automatizado
con fines comerciales. Usar solo para datos propios de pricing/benchmarking
y respetar robots.txt (crawl-delay 10s).
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any

import httpx

from scrapers.base import BaseScraper, LocalRaw

logger = logging.getLogger(__name__)

_API_BASE = "https://www.milanuncios.com/api/v1/items"
_WEB_BASE = "https://www.milanuncios.com"
_WEB_URL = (
    _WEB_BASE
    + "/oficinas-en-alquiler/?fromSearch=1&com=1&provincia=08&poblacion=Barcelona&pagina={pagina}"
)


class MilanunciosScraper(BaseScraper):
    """
    Scraper para Milanuncios.

    Intenta primero la API JSON interna (más fiable, sin JS needed).
    Si falla → Playwright + stealth sobre el HTML.

    Protección típica de Milanuncios:
      - PerimeterX (ahora HUMAN Security) → analiza comportamiento de mouse/teclado
      - Rate limiting: ~100 req/hora por IP
      - API key en header x-mla-api-key (valor rotativo, extraído del bundle JS)
      - User-Agent check

    Mitigación:
      - httpx con headers x-requested-with, referer, origin
      - Sin x-mla-api-key → la API pública funciona a nivel básico (listings de búsqueda)
      - Playwright si bloqueo HTTP detectado
    """

    NOMBRE = "milanuncios"
    MAX_PAGINAS = 5
    DELAY_MIN = 3.0
    DELAY_MAX = 8.0
    FORZAR_PLAYWRIGHT = False

    # URL de fallback HTML (Playwright)
    URL_PLANTILLA = _WEB_URL

    async def scrape(self) -> list[LocalRaw]:
        """Intenta API JSON primero; si falla va al HTML con Playwright."""
        resultados = await self._scrape_api()
        if resultados:
            return resultados
        logger.info("[milanuncios] API no disponible — usando Playwright HTML")
        return await self._scrape_html()

    # ── Capa 1: API JSON interna ───────────────────────────────────────────────

    async def _scrape_api(self) -> list[LocalRaw]:
        """Extrae datos via API JSON interna de Milanuncios."""
        resultados: list[LocalRaw] = []
        headers = self._headers_api()

        for pagina in range(1, self.MAX_PAGINAS + 1):
            params = {
                "category": "17300",        # Oficinas y locales
                "province": "8",             # Barcelona provincia (INE code)
                "locationId": "12712",       # Barcelona ciudad
                "isRent": "true",
                "numPage": str(pagina),
                "itemsPerPage": "40",
                "sortBy": "date",
                "sortOrder": "desc",
            }
            try:
                async with httpx.AsyncClient(headers=headers, timeout=20.0) as c:
                    r = await c.get(_API_BASE, params=params)

                    if r.status_code in (403, 429):
                        logger.info("[milanuncios] API bloqueada (%d) en pág %d", r.status_code, pagina)
                        break
                    if r.status_code != 200:
                        break

                    data = r.json()

            except (httpx.HTTPError, json.JSONDecodeError) as exc:
                logger.debug("[milanuncios] API error: %s", exc)
                break

            items = data.get("items") or data.get("data") or data.get("results") or []
            if not items:
                break

            for item in items:
                local = self._mapear_api_item(item)
                if local:
                    resultados.append(local)

            logger.info("[milanuncios] API pág %d: %d locales", pagina, len(items))
            await self._esperar()

        return resultados

    def _mapear_api_item(self, item: dict) -> LocalRaw | None:
        """Mapea un item de la API de Milanuncios a LocalRaw."""
        pid = str(item.get("id") or item.get("adId") or "")
        if not pid:
            return None

        precio = self._float_seguro(
            item.get("price") or _deep_get(item, "priceInfo", "price")
        )
        m2 = self._float_seguro(
            item.get("surface") or item.get("size") or _deep_get(item, "attributes", "surface")
        )

        # Dirección y localización
        location = item.get("location") or {}
        barrio = location.get("district") or location.get("zone") or ""
        direccion = location.get("address") or location.get("street") or ""
        cp = location.get("postalCode") or ""
        if cp and not direccion:
            direccion = f"Barcelona {cp}"

        lat = self._float_seguro(location.get("latitude") or _deep_get(item, "coordinates", "lat"))
        lng = self._float_seguro(location.get("longitude") or _deep_get(item, "coordinates", "lon"))

        # URL del anuncio
        slug = item.get("url") or item.get("slug") or ""
        url = f"{_WEB_BASE}/{slug}" if slug and not slug.startswith("http") else slug

        # Planta
        attrs = item.get("attributes") or {}
        planta_raw = attrs.get("floor") or attrs.get("planta") or ""
        planta = _normalizar_planta(str(planta_raw))

        return LocalRaw(
            portal=self.NOMBRE,
            portal_id=f"milanuncios_{pid}",
            titulo=item.get("title") or item.get("name") or "",
            direccion=str(direccion),
            barrio=str(barrio),
            precio=precio,
            m2=m2,
            planta=planta,
            lat=lat,
            lng=lng,
            url=url,
            caracteristicas={
                k: item.get(k)
                for k in ("type", "hasPhotos", "isUrgent", "isPro")
                if item.get(k) is not None
            },
        )

    def _headers_api(self) -> dict[str, str]:
        """Headers para la API interna de Milanuncios."""
        base = self._headers_navegador()
        base.update({
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.milanuncios.com/",
            "Origin": "https://www.milanuncios.com",
            "X-Requested-With": "XMLHttpRequest",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
        })
        return base

    # ── Capa 2: HTML con Playwright ────────────────────────────────────────────

    async def _scrape_html(self) -> list[LocalRaw]:
        """Fallback HTML scraping con Playwright."""
        resultados: list[LocalRaw] = []
        for pagina in range(1, self.MAX_PAGINAS + 1):
            url = _WEB_URL.format(pagina=pagina)
            logger.info("[milanuncios] Playwright pág %d → %s", pagina, url)
            try:
                html = await self._playwright_get(url)
                if not html or not self._parece_valido(html):
                    break
                items = self._parsear_pagina(html)
                if not items:
                    break
                resultados.extend(items)
            except Exception as exc:
                logger.warning("[milanuncios] HTML error pág %d: %s", pagina, exc)
                break
            await self._esperar()
        return resultados

    def _parsear_pagina(self, html: str) -> list[LocalRaw]:
        """Parser HTML para Milanuncios (fallback)."""
        items = self._parsear_next_data_ma(html) or self._parsear_html_ma(html)
        return items

    def _parsear_next_data_ma(self, html: str) -> list[LocalRaw]:
        """Intenta extraer de __NEXT_DATA__ (Milanuncios migró a Next.js)."""
        data = self._extraer_next_data(html)
        if not data:
            return []
        listings = (
            _deep_get(data, "props", "pageProps", "items")
            or _deep_get(data, "props", "pageProps", "data", "items")
            or []
        )
        return [r for r in (self._mapear_api_item(l) for l in listings) if r]

    def _parsear_html_ma(self, html: str) -> list[LocalRaw]:
        """Parsea HTML de Milanuncios con BeautifulSoup."""
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            return []

        soup = BeautifulSoup(html, "lxml")
        # Milanuncios usa <article class="ma-AdCard"> o <div data-testid="ad-card">
        cards = (
            soup.find_all("article", class_=re.compile(r"ma-AdCard|AdCard", re.I))
            or soup.find_all("div", attrs={"data-testid": re.compile(r"ad-card|listing", re.I)})
            or soup.find_all("li", class_=re.compile(r"ad-item|list-item", re.I))
        )

        resultados: list[LocalRaw] = []
        for card in cards:
            try:
                r = self._parsear_card_html(card)
                if r:
                    resultados.append(r)
            except Exception:
                continue
        return resultados

    def _parsear_card_html(self, card: Any) -> LocalRaw | None:
        """Extrae datos de una tarjeta HTML de Milanuncios."""
        from bs4 import Tag

        # ID
        pid = card.get("data-id") or card.get("id") or ""
        if not pid:
            enlace = card.find("a", href=re.compile(r"/(\d+)"))
            if enlace:
                m = re.search(r"-(\d+)\.htm", enlace.get("href", ""))
                pid = m.group(1) if m else ""
        if not pid:
            return None

        # Precio
        precio_el = card.find(class_=re.compile(r"price|precio|ma-AdPrice", re.I))
        precio_txt = precio_el.get_text(" ", strip=True) if precio_el else ""
        precio = self._float_seguro(re.sub(r"[^\d,.]", "", precio_txt) or None)

        # Superficie
        m2 = None
        texto_completo = card.get_text()
        m2_match = re.search(r"(\d+(?:[.,]\d+)?)\s*m²", texto_completo)
        if m2_match:
            m2 = self._float_seguro(m2_match.group(1))

        # Título
        titulo_el = card.find(["h2", "h3", "a"], class_=re.compile(r"title|titulo|ma-AdTitle", re.I))
        titulo = titulo_el.get_text(strip=True) if titulo_el else ""

        # URL
        enlace = card.find("a", href=re.compile(r"milanuncios|/oficinas"))
        href = enlace.get("href", "") if enlace else ""
        url = href if href.startswith("http") else f"{_WEB_BASE}{href}"

        return LocalRaw(
            portal=self.NOMBRE,
            portal_id=f"milanuncios_{pid}",
            titulo=titulo,
            precio=precio,
            m2=m2,
            url=url,
        )


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _deep_get(d: Any, *keys: Any) -> Any:
    for k in keys:
        if isinstance(d, dict):
            d = d.get(k)
        elif isinstance(d, list) and isinstance(k, int):
            try:
                d = d[k]
            except IndexError:
                return None
        else:
            return None
    return d


def _normalizar_planta(planta: str) -> str:
    planta = planta.strip().upper()
    if planta in ("0", "BAJA", "PLANTA BAJA", "PB", "BAJO", "BAIXA", "GROUND"):
        return "PB"
    if "SEMI" in planta or "SB" in planta:
        return "SS"
    if "SOTA" in planta or "SOTANO" in planta or "SÓTANO" in planta or "BASEMENT" in planta:
        return "S"
    m = re.search(r"\d+", planta)
    return m.group(0) if m else "PB"
