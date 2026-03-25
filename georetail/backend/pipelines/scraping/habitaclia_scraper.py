"""
pipelines/scraping/habitaclia_scraper.py — Scraper de Habitaclia para locales en alquiler.

Habitaclia usa SSR con JSON incrustado en <script type="application/json" id="initData">.
Anti-bot: Cloudflare básico + rate limiting.

Mejoras respecto a la versión anterior:
  - curl_cffi en lugar de httpx (TLS fingerprinting real)
  - Session warming con homepage antes de cualquier búsqueda
  - Mejor extracción de lat/lng (múltiples paths en JSON embebido)
  - Más campos: referencia, año_construccion, planta
  - Cookies persistentes entre ejecuciones
  - Delay 2-4s entre requests, backoff exponencial en 429
"""
from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import time
from hashlib import md5
from pathlib import Path
from typing import Optional

from .base_scraper import ScrapingConfig, _USER_AGENTS, _browser_headers

logger = logging.getLogger(__name__)

# ── Constantes ────────────────────────────────────────────────────────────────
_BASE_URL   = "https://www.habitaclia.com"
# URL correcta — la anterior /buscar/local-comercial/{ciudad}/alquiler/ da 404
# Candidatos probados (en orden de preferencia):
#   1. /locales-en-alquiler-en-{ciudad}.htm  ← formato estándar Habitaclia
#   2. /buscar/locales/alquiler/{ciudad}/
#   3. /locales-comerciales-en-alquiler-{ciudad}.htm
_SEARCH_URL      = _BASE_URL + "/locales-en-alquiler-en-{ciudad}.htm"
_SEARCH_URL_ALT1 = _BASE_URL + "/buscar/locales/alquiler/{ciudad}/"
_SEARCH_URL_ALT2 = _BASE_URL + "/locales-comerciales-en-alquiler-{ciudad}.htm"
_API_URL         = _BASE_URL + "/api/v1/properties"

_DELAY_MIN = 2.0   # segundos entre requests
_DELAY_MAX = 4.5
_RATE_LIMIT_WAIT_MIN = 900   # 15 min
_RATE_LIMIT_WAIT_MAX = 1800  # 30 min

_SESSIONS_DIR = Path("/tmp/georetail_sessions")
_SESSIONS_DIR.mkdir(parents=True, exist_ok=True)

_CIUDADES = {
    "barcelona": "barcelona",
    "madrid":    "madrid",
    "valencia":  "valencia",
    "girona":    "girona",
    "tarragona": "tarragona",
}


# ── Gestión de cookies ────────────────────────────────────────────────────────

def _cookies_path(session_id: str) -> Path:
    return _SESSIONS_DIR / f"habitaclia_session_{session_id}.json"


def _cargar_cookies(session_id: str) -> dict:
    path = _cookies_path(session_id)
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return {}


def _guardar_cookies(session_id: str, cookies: dict) -> None:
    try:
        _cookies_path(session_id).write_text(json.dumps(cookies, ensure_ascii=False, indent=2))
    except Exception as e:
        logger.debug("Error guardando cookies Habitaclia: %s", e)


# ── Scraper ───────────────────────────────────────────────────────────────────

class HabitacliaScraper:
    """
    Extrae locales en alquiler de Habitaclia.

    Estrategia de fetch:
      1. curl_cffi con impersonate="chrome124" (TLS fingerprinting)
      2. Fallback a BaseScraper (curl_cffi chr131 → httpx)
      3. Fallback a Playwright si los anteriores fallan

    Estrategia de parsing:
      1. API JSON interna /api/v1/properties
      2. JSON incrustado en <script id="initData">
      3. window.__DATA__ / window.__INITIAL_STATE__
      4. JSON-LD schema.org
    """

    def __init__(
        self,
        config: Optional[ScrapingConfig] = None,
        session_id: str = "default",
    ):
        self.cfg = config or ScrapingConfig()
        self.session_id = session_id
        self._ua = random.choice(_USER_AGENTS)
        self._cookies = _cargar_cookies(session_id)
        self._rate_limit_until: float = 0.0
        self._request_count = 0

    async def scrape(self, ciudad: str = "barcelona", max_paginas: int = 5) -> list[dict]:
        """Devuelve lista de dicts con datos de locales en alquiler."""
        ciudad_slug = _CIUDADES.get(ciudad, ciudad)
        resultados: list[dict] = []

        # Session warming: visitar homepage para obtener cookies base
        await self._session_warming(ciudad_slug)

        for pagina in range(1, max_paginas + 1):
            # Respetar rate limit activo
            ahora = time.time()
            if ahora < self._rate_limit_until:
                espera = self._rate_limit_until - ahora
                logger.warning("Habitaclia rate limit — esperando %.0f min", espera / 60)
                await asyncio.sleep(espera)

            logger.info("Habitaclia scraping pág %d (ciudad=%s)", pagina, ciudad_slug)

            # Intentar API JSON primero
            items = await self._scrape_api(ciudad_slug, pagina)
            if items is None:
                # API no disponible o falló → scraping HTML
                items = await self._scrape_html(ciudad_slug, pagina)

            if not items:
                logger.info("Habitaclia: sin más resultados en pág %d", pagina)
                break

            resultados.extend(items)
            logger.info("Habitaclia: +%d locales (total %d)", len(items), len(resultados))

        _guardar_cookies(self.session_id, self._cookies)
        return resultados

    async def _session_warming(self, ciudad: str) -> None:
        """Visita la homepage de Habitaclia para obtener cookies iniciales."""
        try:
            logger.debug("Habitaclia session warming: visitando homepage")
            await self._get(_BASE_URL + "/", referer="")
            await asyncio.sleep(random.uniform(1.5, 3.5))
            # Visitar también la sección de búsqueda general (URL correcta)
            await self._get(_BASE_URL + "/locales-en-alquiler/", referer=_BASE_URL + "/")
            await asyncio.sleep(random.uniform(1.0, 2.5))
        except Exception as e:
            logger.debug("Habitaclia warming falló (no crítico): %s", e)

    async def _scrape_api(self, ciudad: str, pagina: int) -> Optional[list[dict]]:
        """Intenta obtener datos vía API JSON interna de Habitaclia."""
        try:
            url = (
                f"{_API_URL}?type=local&operation=rent"
                f"&city={ciudad}&page={pagina}&limit=30"
            )
            # Usar la URL correcta como referer (formato .htm)
            respuesta = await self._get(url, referer=_SEARCH_URL.format(ciudad=ciudad))
            if not respuesta:
                return None
            data = json.loads(respuesta)
            items_raw = (
                data.get("items")
                or data.get("properties")
                or data.get("results")
                or []
            )
            if not isinstance(items_raw, list):
                return None
            items = [_parse_habitaclia_item(i) for i in items_raw]
            return [i for i in items if i is not None]
        except (json.JSONDecodeError, Exception) as e:
            logger.debug("Habitaclia API falló: %s", e)
            return None

    async def _scrape_html(self, ciudad: str, pagina: int) -> list[dict]:
        """Scraping HTML de Habitaclia con extracción del JSON incrustado.

        Prueba múltiples patrones de URL en orden hasta obtener respuesta válida.
        """
        # Lista de patrones de URL para Habitaclia (el primero que devuelva HTML usable)
        url_candidates = [
            _SEARCH_URL.format(ciudad=ciudad),
            _SEARCH_URL_ALT1.format(ciudad=ciudad),
            _SEARCH_URL_ALT2.format(ciudad=ciudad),
        ]
        # Añadir paginación a la URL candidata
        if pagina > 1:
            url_candidates = [u + f"?page={pagina}" for u in url_candidates]

        html = ""
        url_usada = url_candidates[0]
        for url_candidate in url_candidates:
            referer = _SEARCH_URL.format(ciudad=ciudad) if pagina > 1 else _BASE_URL
            h = await self._get(url_candidate, referer=referer)
            if h and len(h) > 1000:
                html = h
                url_usada = url_candidate
                logger.debug("Habitaclia: URL activa es %s", url_candidate)
                break
            logger.debug("Habitaclia: URL %s sin respuesta o corta, probando siguiente", url_candidate)

        if not html:
            # Fallback a Playwright con la URL primaria
            logger.info("Habitaclia: intentando Playwright para pág %d", pagina)
            html = await self._get_playwright(url_candidates[0])
        if not html:
            return []
        return _parse_habitaclia_html(html)

    # ── HTTP ──────────────────────────────────────────────────────────────────

    async def _get(self, url: str, referer: str = "") -> str:
        """Descarga una URL. Usa curl_cffi primario, BaseScraper como fallback."""
        if self._request_count > 0:
            delay = random.uniform(_DELAY_MIN, _DELAY_MAX)
            if random.random() < 0.08:
                delay += random.uniform(3.0, 8.0)
            await asyncio.sleep(delay)
        self._request_count += 1

        # ── curl_cffi primario ─────────────────────────────────────────────────
        html = await self._curl_get(url, referer)
        if html and len(html) > 500:
            return html

        # ── Fallback: BaseScraper ──────────────────────────────────────────────
        try:
            from .base_scraper import BaseScraper
            async with BaseScraper(self.cfg) as base:
                html = await base.get(url, referer=referer)
            return html or ""
        except Exception as e:
            logger.debug("BaseScraper fallback error: %s", e)
            return ""

    async def _curl_get(self, url: str, referer: str) -> str:
        """curl_cffi GET con impersonate chrome124."""
        try:
            from curl_cffi.requests import AsyncSession
            proxy = self.cfg.random_proxy()
            headers = _browser_headers(self._ua, referer)
            async with AsyncSession(
                impersonate="chrome124",
                proxies={"https": proxy, "http": proxy} if proxy else None,
                timeout=self.cfg.timeout,
            ) as session:
                resp = await session.get(
                    url,
                    headers=headers,
                    cookies=self._cookies,
                    allow_redirects=True,
                )
                if resp.status_code == 200:
                    self._cookies.update({k: v for k, v in resp.cookies.items()})
                    return resp.text
                if resp.status_code == 429:
                    espera = random.uniform(_RATE_LIMIT_WAIT_MIN, _RATE_LIMIT_WAIT_MAX)
                    self._rate_limit_until = time.time() + espera
                    logger.warning("Habitaclia 429 — rate limit. Esperando %.0f min", espera / 60)
                    return ""
                logger.debug("Habitaclia curl_cffi status %d para %s", resp.status_code, url)
                return ""
        except ImportError:
            return ""
        except Exception as e:
            logger.debug("curl_cffi error Habitaclia: %s", e)
            return ""

    async def _get_playwright(self, url: str) -> str:
        """Playwright como último recurso."""
        try:
            from .base_scraper import BaseScraper
            async with BaseScraper(self.cfg) as base:
                return await base.get_with_playwright(url, wait_selector="[data-testid], article, .list-item")
        except Exception:
            return ""


# ── Parsing HTML ──────────────────────────────────────────────────────────────

def _parse_habitaclia_html(html: str) -> list[dict]:
    """
    Extrae listings del JSON incrustado en el HTML de Habitaclia.
    Intenta múltiples estrategias en orden de robustez.
    """
    results = []

    # ── 1. <script id="initData"> — payload principal ─────────────────────────
    match = re.search(
        r'<script[^>]+id="initData"[^>]*type="application/json"[^>]*>(.*?)</script>',
        html, re.DOTALL | re.IGNORECASE
    )
    if not match:
        # Sin el atributo type
        match = re.search(
            r'<script[^>]+id="initData"[^>]*>(.*?)</script>',
            html, re.DOTALL | re.IGNORECASE
        )
    if match:
        try:
            data = json.loads(match.group(1).strip())
            listings = (
                data.get("listings", [])
                or data.get("properties", [])
                or data.get("items", [])
                or data.get("results", [])
                or _deep_find_list(data, "listing")
                or _deep_find_list(data, "properties")
                or []
            )
            for item in listings:
                parsed = _parse_habitaclia_item(item)
                if parsed:
                    results.append(parsed)
            if results:
                return results
        except json.JSONDecodeError:
            pass

    # ── 2. window.__DATA__ o window.__INITIAL_STATE__ ────────────────────────
    for pattern in [
        r'window\.__DATA__\s*=\s*({.*?});\s*(?:</script>|window\.)',
        r'window\.__INITIAL_STATE__\s*=\s*({.*?});\s*(?:</script>|window\.)',
        r'window\.__NEXT_DATA__\s*=\s*({.*?});\s*(?:</script>|window\.)',
    ]:
        match = re.search(pattern, html, re.DOTALL)
        if match:
            try:
                data = json.loads(match.group(1))
                listings = (
                    _deep_find_list(data, "properties")
                    or _deep_find_list(data, "listings")
                    or _deep_find_list(data, "items")
                    or []
                )
                for item in listings:
                    parsed = _parse_habitaclia_item(item)
                    if parsed:
                        results.append(parsed)
                if results:
                    return results
            except json.JSONDecodeError:
                pass

    # ── 3. __NEXT_DATA__ en <script> ──────────────────────────────────────────
    match = re.search(
        r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        html, re.DOTALL | re.IGNORECASE
    )
    if match:
        try:
            data = json.loads(match.group(1).strip())
            props = data.get("props", {}).get("pageProps", {})
            listings = (
                props.get("results", [])
                or props.get("properties", [])
                or _deep_find_list(data, "properties")
                or []
            )
            for item in listings:
                parsed = _parse_habitaclia_item(item)
                if parsed:
                    results.append(parsed)
            if results:
                return results
        except json.JSONDecodeError:
            pass

    # ── 4. JSON-LD ─────────────────────────────────────────────────────────────
    bloques = re.findall(
        r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
        html, re.DOTALL | re.IGNORECASE
    )
    for bloque in bloques:
        try:
            data = json.loads(bloque.strip())
            if isinstance(data, list):
                for item in data:
                    parsed = _parse_jsonld_item(item)
                    if parsed:
                        results.append(parsed)
            elif isinstance(data, dict):
                tipo = data.get("@type", "")
                if tipo == "ItemList" and "itemListElement" in data:
                    for elem in data["itemListElement"]:
                        parsed = _parse_jsonld_item(elem.get("item", elem))
                        if parsed:
                            results.append(parsed)
                else:
                    parsed = _parse_jsonld_item(data)
                    if parsed:
                        results.append(parsed)
        except json.JSONDecodeError:
            pass

    return results


def _parse_habitaclia_item(item: dict) -> Optional[dict]:
    """Normaliza un listing de Habitaclia al formato interno."""
    try:
        # ── Precio ───────────────────────────────────────────────────────────
        price = (
            item.get("price")
            or item.get("priceMin")
            or item.get("monthlyCost")
            or item.get("rent")
            or item.get("amount")
            or _get_nested(item, "priceInfo", "amount")
        )
        price_float = _to_float(price)

        # ── Lat/Lng — varios paths posibles ──────────────────────────────────
        lat = (
            _to_float(item.get("lat"))
            or _to_float(item.get("latitude"))
            or _to_float(_get_nested(item, "geo", "lat"))
            or _to_float(_get_nested(item, "geo", "latitude"))
            or _to_float(_get_nested(item, "coordinates", "latitude"))
            or _to_float(_get_nested(item, "ubication", "latitude"))
            or _to_float(_get_nested(item, "location", "lat"))
            or _to_float(_get_nested(item, "location", "latitude"))
        )
        lng = (
            _to_float(item.get("lng"))
            or _to_float(item.get("longitude"))
            or _to_float(_get_nested(item, "geo", "lng"))
            or _to_float(_get_nested(item, "geo", "longitude"))
            or _to_float(_get_nested(item, "coordinates", "longitude"))
            or _to_float(_get_nested(item, "ubication", "longitude"))
            or _to_float(_get_nested(item, "location", "lng"))
            or _to_float(_get_nested(item, "location", "longitude"))
        )

        if not price_float and not lat:
            return None

        # ── Superficie ───────────────────────────────────────────────────────
        m2 = _to_float(
            item.get("area")
            or item.get("surface")
            or item.get("size")
            or item.get("buildingArea")
        )

        # ── Identificación ───────────────────────────────────────────────────
        prop_id = (
            item.get("id")
            or item.get("propertyId")
            or item.get("code")
            or item.get("referenceCode")
        )

        # ── Localización ─────────────────────────────────────────────────────
        address = (
            item.get("address")
            or item.get("fullAddress")
            or item.get("street")
            or _get_nested(item, "location", "address")
            or ""
        )
        district = (
            item.get("district")
            or _get_nested(item, "location", "district")
            or _get_nested(item, "ubication", "district")
            or ""
        )
        neighborhood = (
            item.get("neighborhood")
            or item.get("zone")
            or _get_nested(item, "location", "neighborhood")
            or ""
        )
        cp = (
            item.get("postalCode")
            or item.get("zipCode")
            or _get_nested(item, "location", "postalCode")
        )
        url_path = (
            item.get("url")
            or item.get("href")
            or item.get("slug")
            or item.get("detailUrl")
            or ""
        )

        # ── Campos enriquecidos ───────────────────────────────────────────────
        referencia = str(prop_id) if prop_id else None
        anyo = _to_int(
            item.get("constructionYear")
            or item.get("buildYear")
            or item.get("yearBuilt")
            or _get_nested(item, "characteristics", "constructionYear")
        )
        planta = (
            item.get("floor")
            or item.get("floorNumber")
            or _get_nested(item, "characteristics", "floor")
        )
        if planta is not None:
            planta = str(planta)

        return {
            "id": f"habitaclia_{prop_id}" if prop_id else f"habitaclia_{md5(str(item).encode()).hexdigest()[:8]}",
            "fuente": "habitaclia",
            "precio": price_float,
            "m2": m2,
            "precio_m2": round(price_float / m2, 2) if price_float and m2 and m2 > 0 else None,
            "lat": lat,
            "lng": lng,
            "direccion": address,
            "distrito": district,
            "barrio": neighborhood,
            "codigo_postal": cp,
            "url": f"{_BASE_URL}{url_path}" if url_path and not url_path.startswith("http") else url_path,
            # Campos enriquecidos
            "referencia": referencia,
            "anyo_construccion": anyo,
            "planta": planta,
        }
    except Exception as e:
        logger.debug("Error normalizando listing Habitaclia: %s", e)
        return None


def _parse_jsonld_item(item: dict) -> Optional[dict]:
    """Extrae datos de un bloque JSON-LD individual."""
    try:
        tipo = item.get("@type", "")
        if tipo not in ("RealEstateListing", "Product", "Accommodation", "LodgingBusiness", "Place"):
            return None

        price = (
            _to_float(_get_nested(item, "offers", "price"))
            or _to_float(item.get("price"))
        )
        geo = item.get("geo", {})
        addr = item.get("address", {}) if isinstance(item.get("address"), dict) else {}

        lat = _to_float(geo.get("latitude"))
        lng = _to_float(geo.get("longitude"))

        if not price and not lat:
            return None

        # m2 desde floorSize
        m2 = None
        floor_size = item.get("floorSize", {})
        if isinstance(floor_size, dict):
            m2 = _to_float(floor_size.get("value"))
        elif floor_size:
            m2 = _to_float(floor_size)

        identificador = item.get("identifier") or item.get("url", "")[-24:]
        anyo = _to_int(
            item.get("yearBuilt")
            or item.get("constructionDate")
        )

        return {
            "id": f"habitaclia_{identificador}",
            "fuente": "habitaclia",
            "precio": price,
            "m2": m2,
            "precio_m2": round(price / m2, 2) if price and m2 and m2 > 0 else None,
            "lat": lat,
            "lng": lng,
            "direccion": addr.get("streetAddress", ""),
            "distrito": addr.get("addressLocality", ""),
            "barrio": addr.get("addressRegion", ""),
            "codigo_postal": addr.get("postalCode"),
            "url": item.get("url", ""),
            "referencia": str(identificador) if identificador else None,
            "anyo_construccion": anyo,
            "planta": None,
        }
    except Exception as e:
        logger.debug("Error parseando JSON-LD Habitaclia: %s", e)
        return None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _to_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        if isinstance(val, str):
            val = val.replace(".", "").replace(",", ".").replace("€", "").strip()
        result = float(val)
        return result if result != 0.0 else None
    except (ValueError, TypeError):
        return None


def _to_int(val) -> Optional[int]:
    if val is None:
        return None
    try:
        return int(float(str(val).replace(",", ".")))
    except (ValueError, TypeError):
        return None


def _get_nested(obj, *keys):
    """Acceso seguro a estructura anidada mixta dict/list."""
    for key in keys:
        if obj is None:
            return None
        if isinstance(key, int) and isinstance(obj, list):
            obj = obj[key] if key < len(obj) else None
        elif isinstance(obj, dict):
            obj = obj.get(key)
        else:
            return None
    return obj


def _deep_find_list(obj, key: str, _depth: int = 0) -> Optional[list]:
    """Búsqueda recursiva de clave con lista (máx profundidad 6)."""
    if _depth > 6:
        return None
    if isinstance(obj, dict):
        if key in obj and isinstance(obj[key], list) and len(obj[key]) > 0:
            return obj[key]
        for v in obj.values():
            r = _deep_find_list(v, key, _depth + 1)
            if r:
                return r
    elif isinstance(obj, list):
        for item in obj:
            r = _deep_find_list(item, key, _depth + 1)
            if r:
                return r
    return None
