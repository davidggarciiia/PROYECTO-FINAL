"""
scrapers/habitaclia.py — Scraper para Habitaclia (locales en alquiler, Barcelona).

Habitaclia es el principal portal inmobiliario de Cataluña.
  - Propietario: Grupo Anuntis Segundamano (InfoJobs / Schibsted)
  - Protección: Moderada. Bot Manager básico, sin Cloudflare agresivo.
  - Renderizado: React/JS. Datos en window.__NUXT__ o en JSON-LD.

URL base:
  https://www.habitaclia.com/alquiler-local_comercial-en-barcelona.htm
  Paginación: -{pagina}.htm (ej. -2.htm, -3.htm…)

Estrategia:
  1. httpx con headers completos → suele funcionar (sin CF agresivo)
  2. Fallback Playwright si bloqueo detectado

Campos extraídos:
  id, precio, m2, titulo, direccion, barrio, lat, lng, url, planta, características
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any

from scrapers.base import BaseScraper, LocalRaw

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.habitaclia.com"
_URL_PAGINA1 = _BASE_URL + "/alquiler-local_comercial-en-barcelona.htm"
_URL_PAGINA_N = _BASE_URL + "/alquiler-local_comercial-en-barcelona-{pagina}.htm"


class HabitacliaScraper(BaseScraper):
    """
    Scraper para Habitaclia.

    Habitaclia usa paginación por sufijo numérico en la URL:
      Página 1: /alquiler-local_comercial-en-barcelona.htm
      Página 2: /alquiler-local_comercial-en-barcelona-2.htm
      ...

    Protección típica:
      - User-Agent check básico
      - Rate limiting suave (~60 req/hora por IP)
      - Bot score (Anuntis ThreatMetrix)

    Sin Cloudflare agresivo → httpx funciona bien con headers correctos.
    """

    NOMBRE = "habitaclia"
    MAX_PAGINAS = 5
    DELAY_MIN = 3.0
    DELAY_MAX = 7.0
    FORZAR_PLAYWRIGHT = False

    # Habitaclia tiene paginación especial: pagina 1 sin número, resto con número
    URL_PLANTILLA = _URL_PAGINA_N  # sobreescribimos _obtener_url_pagina

    async def scrape(self) -> list[LocalRaw]:
        """Override para manejar la paginación especial de Habitaclia."""
        resultados: list[LocalRaw] = []
        for pagina in range(1, self.MAX_PAGINAS + 1):
            if pagina == 1:
                url = _URL_PAGINA1
            else:
                url = _URL_PAGINA_N.format(pagina=pagina)

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

    def _parsear_pagina(self, html: str) -> list[LocalRaw]:
        """
        Habitaclia embebe datos en:
          1. window.__NUXT__ (Nuxt.js/Vue, versión más antigua)
          2. JSON-LD (<script type="application/ld+json">)
          3. HTML semántico con clases predecibles
        """
        items = (
            self._parsear_nuxt(html)
            or self._parsear_json_ld(html)
            or self._parsear_html(html)
        )
        return items

    # ── Método 1: window.__NUXT__ ─────────────────────────────────────────────

    def _parsear_nuxt(self, html: str) -> list[LocalRaw]:
        """Extrae del store de Nuxt.js si está presente."""
        # Habitaclia ha migrado partes a React, pero algunos endpoints aún usan Nuxt
        m = re.search(r"window\.__NUXT__\s*=\s*({.*?})\s*;?\s*</script>", html, re.S)
        if not m:
            return []
        try:
            nuxt = json.loads(m.group(1))
        except json.JSONDecodeError:
            return []

        # Ruta típica en el store de Nuxt: data[0].listings o state.search.list
        listings = (
            _deep_get(nuxt, "state", "search", "list")
            or _deep_get(nuxt, "data", 0, "listings")
            or []
        )
        return [r for r in (self._mapear_listing_nuxt(l) for l in listings) if r]

    def _mapear_listing_nuxt(self, l: dict) -> LocalRaw | None:
        if not l:
            return None
        pid = str(l.get("id") or l.get("ref") or "")
        if not pid:
            return None

        precio = (
            _deep_get(l, "price", "amount")
            or _deep_get(l, "priceRent")
            or l.get("price")
        )
        m2 = l.get("surface") or _deep_get(l, "features", "surface")
        lat = _deep_get(l, "geoposition", "lat") or l.get("lat")
        lng = _deep_get(l, "geoposition", "lon") or l.get("lng")
        barrio = _deep_get(l, "ubication", "zone") or _deep_get(l, "location", "district") or ""
        direccion = _deep_get(l, "ubication", "address") or l.get("address") or ""
        slug = l.get("url") or l.get("slug") or ""
        url = f"{_BASE_URL}{slug}" if slug.startswith("/") else slug

        return LocalRaw(
            portal=self.NOMBRE,
            portal_id=f"habitaclia_{pid}",
            titulo=l.get("title") or "",
            direccion=str(direccion),
            barrio=str(barrio),
            precio=self._float_seguro(precio),
            m2=self._float_seguro(m2),
            lat=self._float_seguro(lat),
            lng=self._float_seguro(lng),
            url=url,
        )

    # ── Método 2: JSON-LD ─────────────────────────────────────────────────────

    def _parsear_json_ld(self, html: str) -> list[LocalRaw]:
        """Extrae datos de bloques JSON-LD (schema.org ItemList / RealEstateListing)."""
        bloques = re.findall(
            r'<script type="application/ld\+json">(.*?)</script>', html, re.S
        )
        resultados: list[LocalRaw] = []
        for bloque in bloques:
            try:
                data = json.loads(bloque)
            except json.JSONDecodeError:
                continue

            # ItemList con apartados
            if data.get("@type") == "ItemList":
                for item in data.get("itemListElement", []):
                    r = self._mapear_json_ld_item(item.get("item", item))
                    if r:
                        resultados.append(r)
            # Listing individual embebido directamente
            elif data.get("@type") in ("RealEstateListing", "Apartment", "LocalBusiness"):
                r = self._mapear_json_ld_item(data)
                if r:
                    resultados.append(r)

        return resultados

    def _mapear_json_ld_item(self, item: dict) -> LocalRaw | None:
        if not item:
            return None
        url = item.get("url") or ""
        m = re.search(r"/(\d+)/?", url)
        pid = m.group(1) if m else ""
        if not pid:
            pid = str(item.get("identifier") or "")
        if not pid:
            return None

        precio = None
        offers = item.get("offers") or {}
        if offers:
            precio = self._float_seguro(offers.get("price"))

        area = item.get("floorSize") or {}
        m2 = self._float_seguro(area.get("value") if isinstance(area, dict) else area)

        geo = item.get("geo") or {}
        lat = self._float_seguro(geo.get("latitude"))
        lng = self._float_seguro(geo.get("longitude"))

        addr = item.get("address") or {}
        if isinstance(addr, dict):
            direccion = f"{addr.get('streetAddress', '')} {addr.get('addressLocality', '')}".strip()
            barrio = addr.get("addressRegion") or ""
        else:
            direccion = str(addr)
            barrio = ""

        return LocalRaw(
            portal=self.NOMBRE,
            portal_id=f"habitaclia_{pid}",
            titulo=item.get("name") or "",
            direccion=direccion,
            barrio=barrio,
            precio=precio,
            m2=m2,
            lat=lat,
            lng=lng,
            url=url,
        )

    # ── Método 3: HTML semántico ──────────────────────────────────────────────

    def _parsear_html(self, html: str) -> list[LocalRaw]:
        """Fallback BeautifulSoup para Habitaclia."""
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            return []

        soup = BeautifulSoup(html, "lxml")

        # Habitaclia usa <article class="list-item"> o <li class="js-list-item">
        items_html = (
            soup.find_all("article", class_=re.compile(r"list-item|property-item", re.I))
            or soup.find_all("li", class_=re.compile(r"js-list-item|item-list", re.I))
            or soup.find_all("div", attrs={"data-item-id": True})
        )

        resultados: list[LocalRaw] = []
        for item in items_html:
            try:
                r = self._parsear_item_html(item)
                if r:
                    resultados.append(r)
            except Exception:
                continue
        return resultados

    def _parsear_item_html(self, item: Any) -> LocalRaw | None:
        """Extrae un local de un elemento HTML de Habitaclia."""
        # ID
        pid = item.get("data-item-id") or item.get("data-id") or item.get("id") or ""
        if not pid:
            enlace = item.find("a", href=re.compile(r"/(\d+)"))
            if enlace:
                m = re.search(r"/(\d+)", enlace.get("href", ""))
                pid = m.group(1) if m else ""
        if not pid:
            return None

        # Precio
        precio_el = item.find(class_=re.compile(r"price|preu", re.I))
        precio_txt = precio_el.get_text(" ", strip=True) if precio_el else ""
        precio = self._float_seguro(re.sub(r"[^\d,.]", "", precio_txt) or None)

        # Superficie
        m2 = None
        m2_match = re.search(r"(\d+(?:[.,]\d+)?)\s*m²", item.get_text())
        if m2_match:
            m2 = self._float_seguro(m2_match.group(1))

        # Título
        titulo_el = item.find(["h2", "h3", "span"], class_=re.compile(r"title|nom|nombre", re.I))
        titulo = titulo_el.get_text(strip=True) if titulo_el else ""

        # Dirección
        addr_el = item.find(class_=re.compile(r"location|address|addr", re.I))
        direccion = addr_el.get_text(strip=True) if addr_el else ""

        # URL
        enlace_tag = item.find("a", href=re.compile(r"habitaclia\.com|/alquiler"))
        href = enlace_tag.get("href", "") if enlace_tag else ""
        url = href if href.startswith("http") else f"{_BASE_URL}{href}"

        return LocalRaw(
            portal=self.NOMBRE,
            portal_id=f"habitaclia_{pid}",
            titulo=titulo,
            direccion=direccion,
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
