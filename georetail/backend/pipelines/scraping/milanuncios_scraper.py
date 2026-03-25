"""
pipelines/scraping/milanuncios_scraper.py — Scraper de Milanuncios para locales en alquiler.

Milanuncios es propiedad de Adevinta. Usa React con hidratación cliente.
Anti-bot: rate limiting moderado + User-Agent check.

Mejoras respecto a la versión anterior:
  - curl_cffi en lugar de httpx (TLS fingerprinting real, impersonate chrome124)
  - Session warming con homepage antes de la búsqueda
  - Mejor parsing de precio: maneja "1.200 €/mes", "1200€", "Consultar"
  - Mejor parsing de m2: extrae desde titulo, descripcion, atributos estructurados
  - Cookies persistentes entre ejecuciones
  - Delay 2-4s entre requests, backoff exponencial en 429/503
  - Parser DOM BeautifulSoup con selector article[data-testid="AD_CARD"] (v2)

URL patrón correcto: https://www.milanuncios.com/locales-comerciales-en-alquiler/{ciudad}.htm?pagina={pagina}
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
_BASE_URL   = "https://www.milanuncios.com"
# URL correcta: /locales-comerciales-en-alquiler/{ciudad}.htm?pagina={pagina}
# La URL con "-en-{ciudad}/" da 404 — corregida
_SEARCH_URL = _BASE_URL + "/locales-comerciales-en-alquiler/{ciudad}.htm?pagina={pagina}"

_DELAY_MIN = 2.0
_DELAY_MAX = 4.5
_RATE_LIMIT_WAIT_MIN = 900   # 15 min
_RATE_LIMIT_WAIT_MAX = 1800  # 30 min

_SESSIONS_DIR = Path("/tmp/georetail_sessions")
_SESSIONS_DIR.mkdir(parents=True, exist_ok=True)

_CIUDADES = {
    "barcelona": "barcelona",
    "madrid":    "madrid",
    "valencia":  "valencia",
    "sevilla":   "sevilla",
    "bilbao":    "bilbao",
}

# Warmup queries para simular navegación previa
_WARMUP_PATHS = [
    "/",
    "/locales-comerciales-en-alquiler/",   # sección correcta
]


# ── Gestión de cookies ────────────────────────────────────────────────────────

def _cookies_path(session_id: str) -> Path:
    return _SESSIONS_DIR / f"milanuncios_session_{session_id}.json"


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
        logger.debug("Error guardando cookies Milanuncios: %s", e)


# ── Scraper ───────────────────────────────────────────────────────────────────

class MilanunciosScraper:
    """
    Extrae locales en alquiler de Milanuncios.

    Usa curl_cffi con impersonate="chrome124" como cliente primario.
    Fallback a BaseScraper (curl_cffi → httpx → ScrapingBee/ZenRows).
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

        # Session warming
        await self._session_warming()

        for pagina in range(1, max_paginas + 1):
            # Respetar rate limit activo
            ahora = time.time()
            if ahora < self._rate_limit_until:
                espera = self._rate_limit_until - ahora
                logger.warning("Milanuncios rate limit — esperando %.0f min", espera / 60)
                await asyncio.sleep(espera)

            url = _SEARCH_URL.format(ciudad=ciudad_slug, pagina=pagina)
            referer = (
                _SEARCH_URL.format(ciudad=ciudad_slug, pagina=pagina - 1)
                if pagina > 1 else _BASE_URL + "/locales-comerciales-en-alquiler/"
            )
            logger.info("Milanuncios scraping pág %d: %s", pagina, url)

            html = await self._get(url, referer=referer)

            if not html:
                logger.info("Milanuncios: intentando Playwright para pág %d", pagina)
                html = await self._get_playwright(url)

            if not html:
                logger.warning("Milanuncios: sin respuesta pág %d — abortando", pagina)
                break

            items = _parse_milanuncios(html)
            if not items:
                logger.info("Milanuncios: sin más resultados en pág %d", pagina)
                break

            resultados.extend(items)
            logger.info("Milanuncios: +%d locales (total %d)", len(items), len(resultados))

        _guardar_cookies(self.session_id, self._cookies)
        return resultados

    # ── Internos ──────────────────────────────────────────────────────────────

    async def _session_warming(self) -> None:
        """Visita la homepage y sección general de Milanuncios."""
        for path in _WARMUP_PATHS:
            try:
                await self._get(_BASE_URL + path, referer="")
                await asyncio.sleep(random.uniform(1.2, 3.0))
            except Exception as e:
                logger.debug("Milanuncios warming path %s falló: %s", path, e)

    async def _get(self, url: str, referer: str = "") -> str:
        """HTTP GET con curl_cffi primario y BaseScraper como fallback."""
        if self._request_count > 0:
            delay = random.uniform(_DELAY_MIN, _DELAY_MAX)
            if random.random() < 0.08:
                delay += random.uniform(3.0, 8.0)
            await asyncio.sleep(delay)
        self._request_count += 1

        html = await self._curl_get(url, referer)
        if html and len(html) > 500:
            return html

        try:
            from .base_scraper import BaseScraper
            async with BaseScraper(self.cfg) as base:
                html = await base.get(url, referer=referer)
            return html or ""
        except Exception as e:
            logger.debug("BaseScraper fallback Milanuncios error: %s", e)
            return ""

    async def _curl_get(self, url: str, referer: str) -> str:
        """curl_cffi GET impersonando Chrome 124."""
        try:
            from curl_cffi.requests import AsyncSession
            proxy = self.cfg.random_proxy()
            headers = _browser_headers(self._ua, referer)
            # Cabeceras específicas de Milanuncios
            headers["Sec-CH-UA"] = '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"'
            headers["Sec-CH-UA-Mobile"] = "?0"
            headers["Sec-CH-UA-Platform"] = '"Windows"'

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
                    logger.warning("Milanuncios 429 — rate limit. Esperando %.0f min", espera / 60)
                    return ""
                if resp.status_code in (403, 503):
                    # Backoff corto y reintentar con BaseScraper
                    logger.warning("Milanuncios %d para %s", resp.status_code, url)
                    return ""
                logger.debug("Milanuncios curl_cffi status %d para %s", resp.status_code, url)
                return ""
        except ImportError:
            return ""
        except Exception as e:
            logger.debug("curl_cffi error Milanuncios: %s", e)
            return ""

    async def _get_playwright(self, url: str) -> str:
        try:
            from .base_scraper import BaseScraper
            async with BaseScraper(self.cfg) as base:
                return await base.get_with_playwright(url, wait_selector=".ma-AdCard, [class*='Card']")
        except Exception:
            return ""


# ── Parsing ───────────────────────────────────────────────────────────────────

def _parse_milanuncios(html: str) -> list[dict]:
    """Extrae listings del JSON incrustado en Milanuncios."""

    # ── 1. __NEXT_DATA__ JSON ─────────────────────────────────────────────────
    match = re.search(
        r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        html, re.DOTALL | re.IGNORECASE
    )
    if match:
        try:
            data = json.loads(match.group(1).strip())
            listings = _extract_ads(data)
            if listings:
                logger.debug("Milanuncios: parseado via __NEXT_DATA__ (%d items)", len(listings))
                return listings
        except json.JSONDecodeError:
            pass

    # ── 2. window.INITIAL_PROPS ───────────────────────────────────────────────
    match = re.search(
        r'window\.INITIAL_PROPS\s*=\s*({.*?});\s*</script>',
        html, re.DOTALL
    )
    if match:
        try:
            data = json.loads(match.group(1))
            listings = _extract_ads(data)
            if listings:
                logger.debug("Milanuncios: parseado via INITIAL_PROPS (%d items)", len(listings))
                return listings
        except json.JSONDecodeError:
            pass

    # ── 3. JSON genérico en cualquier <script> ────────────────────────────────
    script_blocks = re.findall(r'<script[^>]*>(.*?)</script>', html, re.DOTALL)
    for block in script_blocks:
        if '"adList"' in block or '"ads"' in block:
            try:
                # Buscar el JSON válido dentro del bloque
                json_match = re.search(r'(\{.*"adList".*\})', block, re.DOTALL)
                if json_match:
                    data = json.loads(json_match.group(1))
                    listings = _extract_ads(data)
                    if listings:
                        return listings
            except (json.JSONDecodeError, Exception):
                pass

    # ── 4. BeautifulSoup DOM — article[data-testid="AD_CARD"] ────────────────
    items = _parse_dom_beautifulsoup(html)
    if items:
        logger.debug("Milanuncios: parseado via BeautifulSoup DOM (%d items)", len(items))
        return items

    # ── 5. HTML fallback mejorado ─────────────────────────────────────────────
    return _parse_html_fallback_mejorado(html)


def _extract_ads(data: dict) -> list[dict]:
    """Extrae anuncios del payload JSON de Milanuncios."""
    results = []
    ads = (
        _deep_find(data, "adList")
        or _deep_find(data, "ads")
        or _deep_find(data, "items")
        or _deep_find(data, "listings")
        or []
    )
    for ad in ads:
        if not isinstance(ad, dict):
            continue
        parsed = _parse_ad(ad)
        if parsed:
            results.append(parsed)
    return results


def _parse_ad(ad: dict) -> Optional[dict]:
    """
    Normaliza un anuncio de Milanuncios al formato interno.

    Maneja las variaciones de estructura conocidas:
    - precio: int, float, "1.200 €/mes", "1200€", dict con amount
    - m2: int, "50 m²", None (se extrae del título/descripción)
    """
    try:
        # ── Precio — parsing robusto ─────────────────────────────────────────
        precio = _parse_precio(
            ad.get("price")
            or ad.get("priceLabel")
            or ad.get("amount")
            or _get_nested(ad, "priceInfo", "amount")
            or _get_nested(ad, "priceInfo", "price")
        )

        if precio is None and "Consultar" in str(ad.get("priceLabel", "")):
            precio = None  # explícito — precio a consultar

        # ── Coordenadas ──────────────────────────────────────────────────────
        lat = (
            _to_float(ad.get("latitude"))
            or _to_float(ad.get("lat"))
            or _to_float(_get_nested(ad, "geo", "latitude"))
            or _to_float(_get_nested(ad, "location", "latitude"))
            or _to_float(_get_nested(ad, "ubication", "latitude"))
        )
        lng = (
            _to_float(ad.get("longitude"))
            or _to_float(ad.get("lng"))
            or _to_float(_get_nested(ad, "geo", "longitude"))
            or _to_float(_get_nested(ad, "location", "longitude"))
            or _to_float(_get_nested(ad, "ubication", "longitude"))
        )

        # ── Superficie — parsing robusto ─────────────────────────────────────
        m2 = _parse_m2(ad)

        # ── Metadatos ────────────────────────────────────────────────────────
        ad_id = (
            ad.get("id")
            or ad.get("adId")
            or ad.get("itemId")
            or ad.get("listingId")
        )
        slug = (
            ad.get("url")
            or ad.get("href")
            or ad.get("slug")
            or ad.get("detailUrl")
            or ""
        )
        address = (
            _get_nested(ad, "location", "address")
            or ad.get("address")
            or _get_nested(ad, "ubication", "address")
            or ""
        )
        city = (
            _get_nested(ad, "location", "city")
            or ad.get("city")
            or _get_nested(ad, "location", "municipality")
            or ""
        )
        neighborhood = (
            ad.get("neighborhood")
            or _get_nested(ad, "location", "neighborhood")
            or _get_nested(ad, "location", "district")
            or ""
        )
        titulo = ad.get("title") or ad.get("subject") or ad.get("name") or ""

        if not precio:
            return None

        url_completa = f"{_BASE_URL}{slug}" if slug and not slug.startswith("http") else slug

        return {
            "id": f"milanuncios_{ad_id}" if ad_id else f"milanuncios_{md5(str(ad).encode()).hexdigest()[:8]}",
            "fuente": "milanuncios",
            "titulo": titulo,
            "precio": precio,
            "m2": m2,
            "precio_m2": round(precio / m2, 2) if precio and m2 and m2 > 0 else None,
            "lat": lat,
            "lng": lng,
            "direccion": address,
            "distrito": city,
            "barrio": neighborhood,
            "url": url_completa,
        }
    except Exception as e:
        logger.debug("Error normalizando anuncio Milanuncios: %s", e)
        return None


def _parse_precio(raw) -> Optional[float]:
    """
    Convierte cualquier representación de precio a float.

    Maneja:
      - int/float directos: 1200, 1200.0
      - strings: "1.200 €/mes", "1.200€", "1200", "1,200"
      - dicts: {"amount": 1200}, {"price": "1200"}
      - None, "", "Consultar", "A convenir" → None
    """
    if raw is None:
        return None

    if isinstance(raw, dict):
        raw = raw.get("amount") or raw.get("price") or raw.get("value")
        if raw is None:
            return None

    if isinstance(raw, (int, float)):
        return float(raw) if raw > 0 else None

    if isinstance(raw, str):
        texto = raw.strip()
        # Casos "sin precio"
        if not texto or any(k in texto.lower() for k in ("consultar", "convenir", "negociar", "regalo")):
            return None

        # Eliminar todo excepto dígitos, puntos y comas
        # Truco: en España "1.200" = 1200 (punto como separador de miles)
        # pero "1.200,50" = 1200.50
        # Estrategia: si hay coma decimal, el punto es miles; si no, ambiguo
        if "," in texto:
            # Coma decimal → punto es separador de miles
            limpio = re.sub(r'[^\d,]', '', texto).replace(".", "").replace(",", ".")
        else:
            # Solo punto → puede ser decimal o miles; si >3 dígitos post-punto, es miles
            match_decimales = re.search(r'\.(\d+)$', texto)
            if match_decimales and len(match_decimales.group(1)) > 2:
                # "1.200" → eliminar punto
                limpio = re.sub(r'[^\d]', '', texto)
            else:
                # "1.5" → es decimal
                limpio = re.sub(r'[^\d.]', '', texto)

        match = re.search(r'[\d.]+', limpio)
        if match:
            try:
                val = float(match.group(0))
                return val if val > 0 else None
            except ValueError:
                pass
    return None


def _parse_m2(ad: dict) -> Optional[float]:
    """
    Extrae la superficie en m2 de un anuncio.

    Busca en múltiples campos y como último recurso parsea el título/descripción.
    """
    # Campos directos
    m2_raw = (
        ad.get("surface")
        or ad.get("area")
        or ad.get("size")
        or ad.get("squareMeters")
        or _get_nested(ad, "characteristics", "surface")
        or _get_nested(ad, "characteristics", "constructedArea")
        or _get_nested(ad, "details", "surface")
    )
    if m2_raw is not None:
        val = _to_float(m2_raw)
        if val and val > 0:
            return val

    # Buscar "50 m²" o "50m2" en el título
    titulo = ad.get("title") or ad.get("subject") or ad.get("name") or ""
    if titulo:
        match = re.search(r'(\d+(?:[.,]\d+)?)\s*m[²2]', titulo, re.IGNORECASE)
        if match:
            val = _to_float(match.group(1).replace(",", "."))
            if val and val > 0:
                return val

    # Buscar en descripción (si está disponible)
    desc = ad.get("description") or ad.get("body") or ad.get("text") or ""
    if desc:
        match = re.search(r'(\d+(?:[.,]\d+)?)\s*m[²2]', desc, re.IGNORECASE)
        if match:
            val = _to_float(match.group(1).replace(",", "."))
            if val and val > 0:
                return val

    # Buscar en atributos tipo lista [{"key": "surface", "value": "50"}]
    for key in ("attributes", "features", "extras", "details"):
        attrs = ad.get(key)
        if isinstance(attrs, list):
            for attr in attrs:
                if isinstance(attr, dict):
                    k = str(attr.get("key", "") or attr.get("name", "")).lower()
                    if "surface" in k or "area" in k or "m2" in k or "superficie" in k:
                        val = _to_float(attr.get("value"))
                        if val and val > 0:
                            return val

    return None


def _parse_dom_beautifulsoup(html: str) -> list[dict]:
    """
    Parser DOM con BeautifulSoup para Milanuncios.

    Los anuncios están en <article data-testid="AD_CARD" class="ma-AdCardV2 ...">
    El __NEXT_DATA__ / INITIAL_PROPS NO contiene los listings (JS-hydrated),
    por lo que este parser DOM es el método primario para Milanuncios.

    Estructura típica de un article:
      - h2 o [class*="AdCard-title"] → título
      - [class*="AdPrice"] o span con "€" → precio
      - [class*="tag"] con "m²" → superficie
      - [class*="AdCard-location"] → barrio/ciudad
      - a[href] → URL del anuncio
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.debug("BeautifulSoup no disponible (pip install beautifulsoup4)")
        return []

    try:
        soup = BeautifulSoup(html, "html.parser")
    except Exception as e:
        logger.debug("Error instanciando BeautifulSoup Milanuncios: %s", e)
        return []

    # Selectores en orden de preferencia
    cards = (
        soup.select('article[data-testid="AD_CARD"]')
        or soup.select("article.ma-AdCardV2")
        or soup.select("article[class*='ma-AdCard']")
        or soup.select("article[class*='AdCard']")
    )

    if not cards:
        logger.debug("Milanuncios BeautifulSoup: no se encontraron article cards (HTML len=%d)", len(html))
        return []

    results = []
    for card in cards:
        try:
            # ── Título ────────────────────────────────────────────────────────
            titulo_el = (
                card.select_one("h2")
                or card.select_one("[class*='AdCard-title']")
                or card.select_one("[class*='AdTitle']")
                or card.select_one("[class*='title']")
            )
            titulo = titulo_el.get_text(strip=True) if titulo_el else None

            # ── URL + ID ──────────────────────────────────────────────────────
            link_el = card.select_one("a[href]")
            url_path = link_el["href"] if link_el and link_el.get("href") else ""
            url_completa = url_path if url_path.startswith("http") else f"{_BASE_URL}{url_path}"

            # Extraer ID desde la URL (p.ej. /anuncio/local-bcn-1234567.htm)
            id_match = re.search(r'[_-](\d{6,})', url_path)
            ad_id = id_match.group(1) if id_match else None
            if not ad_id:
                # Intentar desde data-id o data-adid del article
                ad_id = card.get("data-id") or card.get("data-adid") or card.get("id") or ""

            # ── Precio ────────────────────────────────────────────────────────
            precio = None
            # Buscar elemento con clase de precio
            precio_el = (
                card.select_one("[class*='AdPrice']")
                or card.select_one("[class*='price']")
                or card.select_one("[class*='Price']")
            )
            if precio_el:
                precio = _parse_precio(precio_el.get_text(strip=True))

            # Si no encontramos con clase, buscar cualquier texto con €/mes
            if precio is None:
                texto_card = card.get_text(" ", strip=True)
                precio_match = re.search(
                    r'([\d]{1,3}(?:[.\s]?\d{3})*(?:[,]\d{1,2})?)\s*€',
                    texto_card
                )
                if precio_match:
                    precio = _parse_precio(precio_match.group(0))

            if precio is None:
                continue  # sin precio, descartar

            # ── Superficie ───────────────────────────────────────────────────
            m2 = None
            # Buscar tags/badges con "m²"
            for el in card.select("[class*='tag'], [class*='Tag'], [class*='feature'], [class*='Feature'], [class*='detail']"):
                texto = el.get_text(strip=True)
                m2_match = re.search(r'(\d+(?:[.,]\d+)?)\s*m[²2]', texto, re.IGNORECASE)
                if m2_match:
                    m2 = _to_float(m2_match.group(1).replace(",", "."))
                    break
            # Fallback: buscar "m²" en todo el texto del card
            if m2 is None:
                texto_card = card.get_text(" ", strip=True)
                m2_match = re.search(r'(\d+(?:[.,]\d+)?)\s*m[²2]', texto_card, re.IGNORECASE)
                if m2_match:
                    m2 = _to_float(m2_match.group(1).replace(",", "."))

            # ── Barrio/Localización ──────────────────────────────────────────
            barrio = ""
            loc_el = (
                card.select_one("[class*='Location']")
                or card.select_one("[class*='location']")
                or card.select_one("[class*='AdCard-location']")
                or card.select_one("[class*='AdLocation']")
            )
            if loc_el:
                barrio = loc_el.get_text(strip=True)

            # Generar ID único
            if ad_id:
                item_id = f"milanuncios_{ad_id}"
            else:
                item_id = f"milanuncios_{md5((url_path or titulo or str(precio)).encode()).hexdigest()[:8]}"

            results.append({
                "id": item_id,
                "fuente": "milanuncios",
                "titulo": titulo,
                "precio": precio,
                "m2": m2,
                "precio_m2": round(precio / m2, 2) if precio and m2 and m2 > 0 else None,
                "lat": None,
                "lng": None,
                "direccion": "",
                "distrito": "",
                "barrio": barrio,
                "url": url_completa,
            })

        except Exception as e:
            logger.debug("Error procesando article Milanuncios: %s", e)
            continue

    return results


def _parse_html_fallback_mejorado(html: str) -> list[dict]:
    """
    Extracción mejorada mediante regex sobre el HTML como último recurso.
    Intenta extraer precio Y m2 cuando estén disponibles.
    """
    results = []

    # Buscar bloques de anuncio por data-id
    blocks = re.finditer(
        r'data-(?:id|adid|listing-id)="(\d+)"',
        html, re.IGNORECASE
    )

    seen_ids = set()
    for match in blocks:
        ad_id = match.group(1)
        if ad_id in seen_ids:
            continue
        seen_ids.add(ad_id)

        # Extraer el contexto HTML alrededor del anuncio (500 chars)
        start = max(0, match.start() - 100)
        end = min(len(html), match.end() + 800)
        context = html[start:end]

        # Precio
        precio = None
        precio_match = re.search(
            r'(?:"price"|class="[^"]*price[^"]*")[^>]*[>: ]*'
            r'(\d{1,3}(?:[.]\d{3})*(?:[,]\d{1,2})?|\d+)',
            context, re.IGNORECASE
        )
        if precio_match:
            precio = _parse_precio(precio_match.group(1))

        if not precio:
            continue

        # m2
        m2 = None
        m2_match = re.search(r'(\d+(?:[.,]\d+)?)\s*m[²2]', context, re.IGNORECASE)
        if m2_match:
            m2 = _to_float(m2_match.group(1).replace(",", "."))

        # URL
        url_match = re.search(r'href="(/[^"]+' + ad_id + r'[^"]*)"', context)
        url = f"{_BASE_URL}{url_match.group(1)}" if url_match else f"{_BASE_URL}/anuncio/{ad_id}"

        results.append({
            "id": f"milanuncios_{ad_id}",
            "fuente": "milanuncios",
            "titulo": None,
            "precio": precio,
            "m2": m2,
            "precio_m2": round(precio / m2, 2) if precio and m2 and m2 > 0 else None,
            "lat": None,
            "lng": None,
            "direccion": "",
            "distrito": "",
            "barrio": "",
            "url": url,
        })

        if len(results) >= 30:
            break

    # Si no encontramos nada con data-id, buscar con patrón de precio en HTML
    if not results:
        precio_blocks = re.findall(
            r'data-id="(\d+)"[^>]*>.*?<span[^>]*class="[^"]*price[^"]*"[^>]*>([\d.,]+)',
            html, re.DOTALL | re.IGNORECASE
        )
        for ad_id, precio_str in precio_blocks[:30]:
            try:
                precio = _parse_precio(precio_str)
                if not precio:
                    continue
                results.append({
                    "id": f"milanuncios_{ad_id}",
                    "fuente": "milanuncios",
                    "titulo": None,
                    "precio": precio,
                    "m2": None,
                    "precio_m2": None,
                    "lat": None,
                    "lng": None,
                    "direccion": "",
                    "distrito": "",
                    "barrio": "",
                    "url": f"{_BASE_URL}/anuncio/{ad_id}",
                })
            except (ValueError, AttributeError):
                pass

    return results


# ── Helpers ───────────────────────────────────────────────────────────────────

def _to_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        if isinstance(val, str):
            val = val.replace(".", "").replace(",", ".").replace("€", "").replace("m²", "").strip()
        result = float(val)
        return result if result != 0.0 else None
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


def _deep_find(obj, key: str, _depth: int = 0):
    """Búsqueda recursiva de clave con lista."""
    if _depth > 6:
        return None
    if isinstance(obj, dict):
        if key in obj and isinstance(obj[key], list):
            return obj[key]
        for v in obj.values():
            r = _deep_find(v, key, _depth + 1)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for item in obj:
            r = _deep_find(item, key, _depth + 1)
            if r is not None:
                return r
    return None
