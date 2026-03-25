"""
pipelines/scraping/fotocasa_scraper.py — Scraper de Fotocasa para locales comerciales.

Fotocasa usa React + Next.js con SSR. Los datos están incrustados en:
  - <script id="__NEXT_DATA__"> — payload principal (más estable)
  - JSON-LD schema.org — fallback
  - BeautifulSoup DOM — último recurso

Anti-bot: Cloudflare + DataDome.
curl_cffi con impersonate="chrome124" es el cliente HTTP primario.
Si DataDome bloquea → ScrapingBee/ZenRows.

URL patrón:
  https://www.fotocasa.es/es/alquiler/locales-comerciales/barcelona-capital/todas-las-zonas/l
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

from .base_scraper import BaseScraper, ScrapingConfig

logger = logging.getLogger(__name__)

# ── Constantes ────────────────────────────────────────────────────────────────
_BASE_URL  = "https://www.fotocasa.es"
_SEARCH_URL_ALQUILER = (
    _BASE_URL + "/es/alquiler/locales-comerciales/barcelona-capital/todas-las-zonas/l"
)
_SEARCH_URL_VENTA = (
    _BASE_URL + "/es/compra/locales-comerciales/barcelona-capital/todas-las-zonas/l"
)
_MAX_PAGINAS = 10    # máximo absoluto de páginas (200 resultados)
_ITEMS_POR_PAGINA = 20

# Delay entre requests: 2-4s (exigencia antibot del enunciado)
_DELAY_MIN = 2.0
_DELAY_MAX = 4.0

# Si status 429: esperar 15-30 minutos
_RATE_LIMIT_WAIT_MIN = 900   # 15 minutos en segundos
_RATE_LIMIT_WAIT_MAX = 1800  # 30 minutos en segundos

# Cookies de sesión persistentes
_SESSIONS_DIR = Path("/tmp/georetail_sessions")
_SESSIONS_DIR.mkdir(parents=True, exist_ok=True)

# ── Headers reales de Chrome ──────────────────────────────────────────────────
def _headers_chrome(ua: str, pagina: int = 1) -> dict:
    """
    Headers que imitan un Chrome real navegando Fotocasa.

    Fix Cloudflare:
    - pagina=0 (warmup): Referer = Google search (parece tráfico orgánico)
    - pagina=1 (primera búsqueda): Referer = fotocasa.es/ (viene de la home)
    - pagina>1 (paginación): Referer = página anterior de búsqueda
    - Añadir sec-ch-ua para coincidir con impersonate="chrome124"
    """
    if pagina == 0:
        # Primera visita (warmup) — viene de Google
        referer = "https://www.google.es/search?q=fotocasa+locales+alquiler+barcelona"
        sec_fetch_site = "cross-site"
    elif pagina == 1:
        # Primera página de búsqueda — viene de la homepage de Fotocasa
        referer = _BASE_URL + "/es/"
        sec_fetch_site = "same-origin"
    else:
        referer = _SEARCH_URL_ALQUILER
        sec_fetch_site = "same-origin"

    return {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "es-ES,es;q=0.9,ca;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Referer": referer,
        "DNT": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": sec_fetch_site,
        "Sec-Fetch-User": "?1",
        "Sec-CH-UA": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": '"Windows"',
        "Upgrade-Insecure-Requests": "1",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
    }


_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.208 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.142 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.183 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.208 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.142 Safari/537.36",
]


# ── Gestión de cookies persistentes ──────────────────────────────────────────

def _cookies_path(session_id: str) -> Path:
    return _SESSIONS_DIR / f"fotocasa_session_{session_id}.json"


def _cargar_cookies(session_id: str) -> dict:
    """Devuelve dict de cookies name→value para curl_cffi."""
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
        logger.debug("Error guardando cookies Fotocasa: %s", e)


# ── Modelo de datos ───────────────────────────────────────────────────────────

class LocalScraped:
    """Local comercial extraído de Fotocasa."""
    __slots__ = [
        "id", "fuente", "titulo", "tipo_operacion",
        "precio_mensual", "precio_venta", "m2",
        "direccion", "barrio", "distrito", "codigo_postal",
        "lat", "lng",
        "disponible", "url_anuncio", "referencia",
        "timestamp_scraping",
    ]

    def __init__(
        self,
        id: str,
        fuente: str = "fotocasa",
        titulo: Optional[str] = None,
        tipo_operacion: str = "alquiler",
        precio_mensual: Optional[float] = None,
        precio_venta: Optional[float] = None,
        m2: Optional[float] = None,
        direccion: Optional[str] = None,
        barrio: Optional[str] = None,
        distrito: Optional[str] = None,
        codigo_postal: Optional[str] = None,
        lat: Optional[float] = None,
        lng: Optional[float] = None,
        disponible: bool = True,
        url_anuncio: Optional[str] = None,
        referencia: Optional[str] = None,
    ):
        self.id = id
        self.fuente = fuente
        self.titulo = titulo
        self.tipo_operacion = tipo_operacion
        self.precio_mensual = precio_mensual
        self.precio_venta = precio_venta
        self.m2 = m2
        self.direccion = direccion
        self.barrio = barrio
        self.distrito = distrito
        self.codigo_postal = codigo_postal
        self.lat = lat
        self.lng = lng
        self.disponible = disponible
        self.url_anuncio = url_anuncio
        self.referencia = referencia
        self.timestamp_scraping = time.time()

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "fuente": self.fuente,
            "titulo": self.titulo,
            "tipo_operacion": self.tipo_operacion,
            "precio": self.precio_mensual if self.tipo_operacion == "alquiler" else self.precio_venta,
            "precio_mensual": self.precio_mensual,
            "precio_venta": self.precio_venta,
            "m2": self.m2,
            "precio_m2": round(self.precio_mensual / self.m2, 2)
                         if self.precio_mensual and self.m2 and self.m2 > 0 else None,
            "direccion": self.direccion,
            "barrio": self.barrio,
            "distrito": self.distrito,
            "codigo_postal": self.codigo_postal,
            "lat": self.lat,
            "lng": self.lng,
            "disponible": self.disponible,
            "url": self.url_anuncio,
            "referencia": self.referencia,
        }


# ── Scraper principal ─────────────────────────────────────────────────────────

class FotocasaScraper:
    """
    Extrae locales comerciales de Fotocasa con antibot robusto.

    Cliente HTTP primario: curl_cffi impersonate="chrome124"
    Fallback: BaseScraper (curl_cffi→httpx→ScrapingBee/ZenRows)

    Ejemplo:
        scraper = FotocasaScraper()
        locales = await scraper.buscar_locales(tipo_operacion="alquiler", pagina=1)

        # O scrape completo multi-página:
        todos = await scraper.scrape(ciudad="barcelona", max_paginas=5)
    """

    def __init__(
        self,
        config: Optional[ScrapingConfig] = None,
        session_id: str = "default",
    ):
        self.cfg = config or ScrapingConfig()
        self.cfg.delay_min = _DELAY_MIN
        self.cfg.delay_max = _DELAY_MAX
        self.session_id = session_id
        self._ua = random.choice(_USER_AGENTS)
        self._cookies = _cargar_cookies(session_id)
        self._rate_limit_until: float = 0.0
        self._request_count = 0

    async def buscar_locales(
        self,
        tipo_operacion: str = "alquiler",
        pagina: int = 1,
    ) -> list[LocalScraped]:
        """
        Obtiene locales de una página específica de Fotocasa.

        Args:
            tipo_operacion: "alquiler" | "venta"
            pagina:         Número de página (1-10)

        Returns:
            Lista de LocalScraped.
        """
        url = self._build_url(tipo_operacion, pagina)
        html = await self._get(url, pagina)
        if not html:
            return []
        return _parse_fotocasa_html(html, tipo_operacion)

    async def scrape(
        self,
        ciudad: str = "barcelona",
        max_paginas: int = 5,
        tipo_operacion: str = "alquiler",
    ) -> list[dict]:
        """
        Scrape completo multi-página. Devuelve list[dict] para compatibilidad
        con el resto de scrapers del pipeline.

        Args:
            ciudad:         "barcelona" (por ahora solo BCN en el pipeline)
            max_paginas:    Máximo de páginas a recorrer (máx absoluto = 10)
            tipo_operacion: "alquiler" | "venta"

        Returns:
            Lista de dicts con formato interno normalizado.
        """
        max_paginas = min(max_paginas, _MAX_PAGINAS)
        resultados: list[dict] = []

        # Session warming: visitar la homepage para obtener cookies base
        await self._session_warming()

        for pagina in range(1, max_paginas + 1):
            # Respetar penalización de rate limit
            ahora = time.time()
            if ahora < self._rate_limit_until:
                espera = self._rate_limit_until - ahora
                logger.warning("Fotocasa rate limit activo — esperando %.0f minutos", espera / 60)
                await asyncio.sleep(espera)

            locales = await self.buscar_locales(tipo_operacion, pagina)
            if not locales:
                logger.info("Fotocasa: sin más resultados en página %d — fin", pagina)
                break

            resultados.extend(loc.to_dict() for loc in locales)
            logger.info("Fotocasa: pág %d → +%d locales (total %d)", pagina, len(locales), len(resultados))

            # Si obtuvimos menos de la mitad del máximo esperado, probablemente es la última página
            if len(locales) < _ITEMS_POR_PAGINA // 2:
                logger.info("Fotocasa: página incompleta — probable última página")
                break

        _guardar_cookies(self.session_id, self._cookies)
        return resultados

    # ── Internos ──────────────────────────────────────────────────────────────

    def _build_url(self, tipo_operacion: str, pagina: int) -> str:
        base = _SEARCH_URL_ALQUILER if tipo_operacion == "alquiler" else _SEARCH_URL_VENTA
        if pagina == 1:
            return base
        return f"{base}?page={pagina}"

    async def _session_warming(self) -> None:
        """
        Visita la homepage de Fotocasa simulando tráfico desde Google.

        Fix Cloudflare: la primera request usa Referer de Google para parecer
        tráfico orgánico, luego la siguiente usa Referer de fotocasa.es.
        """
        try:
            logger.debug("Fotocasa session warming: visitando homepage via Google referer")
            # pagina=0 usa Referer de Google → parece tráfico orgánico
            await self._get(_BASE_URL + "/es/", pagina=0)
            await asyncio.sleep(random.uniform(2.0, 4.0))
            # Segunda visita con Referer = fotocasa.es (calienta cookies Cloudflare)
            await self._get(_BASE_URL + "/es/alquiler/locales-comerciales/todas-las-provincias/l", pagina=1)
            await asyncio.sleep(random.uniform(1.5, 3.0))
        except Exception as e:
            logger.debug("Fotocasa warming falló (no crítico): %s", e)

    async def _get(self, url: str, pagina: int) -> str:
        """Descarga una URL usando curl_cffi primario, con fallback a BaseScraper."""
        if self._request_count > 0:
            delay = random.uniform(_DELAY_MIN, _DELAY_MAX)
            if random.random() < 0.08:  # 8% pausa larga
                delay += random.uniform(3.0, 8.0)
            await asyncio.sleep(delay)
        self._request_count += 1

        headers = _headers_chrome(self._ua, pagina)

        # ── Primario: curl_cffi con impersonate chrome124 ──────────────────────
        html = await self._curl_cffi_get(url, headers)
        if html and len(html) > 500:
            return html

        # ── Fallback: BaseScraper (curl_cffi chr131 → httpx → ScrapBee/ZenRows) ─
        async with BaseScraper(self.cfg) as base:
            referer = _SEARCH_URL_ALQUILER if pagina > 1 else _BASE_URL
            html = await base.get(url, referer=referer)

        return html or ""

    async def _curl_cffi_get(self, url: str, headers: dict) -> str:
        """HTTP GET con curl_cffi impersonating Chrome 124."""
        try:
            from curl_cffi.requests import AsyncSession

            proxy = self.cfg.random_proxy()
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
                    # Actualizar cookies de sesión
                    self._cookies.update({k: v for k, v in resp.cookies.items()})
                    return resp.text
                if resp.status_code == 429:
                    espera = random.uniform(_RATE_LIMIT_WAIT_MIN, _RATE_LIMIT_WAIT_MAX)
                    self._rate_limit_until = time.time() + espera
                    logger.warning(
                        "Fotocasa 429 — rate limit. Esperando %.0f minutos", espera / 60
                    )
                    return ""
                if resp.status_code in (403, 503):
                    logger.warning("Fotocasa %d para %s", resp.status_code, url)
                    return ""
                logger.debug("Fotocasa curl_cffi status %d para %s", resp.status_code, url)
                return ""
        except ImportError:
            logger.debug("curl_cffi no disponible")
            return ""
        except Exception as e:
            logger.debug("curl_cffi error en %s: %s", url, e)
            return ""


# ── Parsing ───────────────────────────────────────────────────────────────────

def _parse_fotocasa_html(html: str, tipo_operacion: str = "alquiler") -> list[LocalScraped]:
    """
    Extrae locales del HTML de Fotocasa.
    Estrategia: JSON-LD primario → __NEXT_DATA__ → BeautifulSoup fallback.
    """
    # ── Método 1: JSON-LD (más estable que el DOM) ─────────────────────────────
    locales = _parse_jsonld(html, tipo_operacion)
    if locales:
        logger.debug("Fotocasa: parseado via JSON-LD (%d locales)", len(locales))
        return locales

    # ── Método 2: __NEXT_DATA__ JSON ──────────────────────────────────────────
    locales = _parse_next_data(html, tipo_operacion)
    if locales:
        logger.debug("Fotocasa: parseado via __NEXT_DATA__ (%d locales)", len(locales))
        return locales

    # ── Método 3: BeautifulSoup DOM ───────────────────────────────────────────
    locales = _parse_beautifulsoup(html, tipo_operacion)
    if locales:
        logger.debug("Fotocasa: parseado via BeautifulSoup (%d locales)", len(locales))
        return locales

    # ── Método 4: Regex último recurso ────────────────────────────────────────
    locales = _parse_regex_fallback(html, tipo_operacion)
    logger.debug("Fotocasa: parseado via regex fallback (%d locales)", len(locales))
    return locales


def _parse_jsonld(html: str, tipo_operacion: str) -> list[LocalScraped]:
    """Extrae locales desde los bloques JSON-LD de schema.org."""
    locales = []
    bloques = re.findall(
        r'<script[^>]+type="application/ld\+json"[^>]*>(.*?)</script>',
        html, re.DOTALL | re.IGNORECASE
    )
    for bloque in bloques:
        try:
            data = json.loads(bloque.strip())
            if isinstance(data, dict):
                # RealEstateListing o Product individuales
                local = _jsonld_item_to_local(data, tipo_operacion)
                if local:
                    locales.append(local)
            elif isinstance(data, list):
                for item in data:
                    local = _jsonld_item_to_local(item, tipo_operacion)
                    if local:
                        locales.append(local)
        except (json.JSONDecodeError, Exception):
            pass
    return locales


def _jsonld_item_to_local(data: dict, tipo_operacion: str) -> Optional[LocalScraped]:
    """Convierte un bloque JSON-LD de Fotocasa a LocalScraped."""
    tipo = data.get("@type", "")
    if tipo not in ("RealEstateListing", "Product", "Accommodation", "LodgingBusiness"):
        return None
    try:
        offers = data.get("offers", {})
        precio = None
        try:
            precio = float(offers.get("price") or data.get("price") or 0) or None
        except (ValueError, TypeError):
            pass

        geo = data.get("geo", {})
        addr = data.get("address", {}) if isinstance(data.get("address"), dict) else {}

        identificador = data.get("identifier") or data.get("url", "")[-24:]
        local_id = f"fotocasa_{identificador}"

        titulo = data.get("name") or data.get("headline")
        m2_raw = data.get("floorSize", {})
        m2 = None
        if isinstance(m2_raw, dict):
            m2 = _to_float(m2_raw.get("value"))
        elif m2_raw:
            m2 = _to_float(m2_raw)

        barrio = addr.get("addressLocality") or addr.get("addressRegion")
        cp = addr.get("postalCode")

        return LocalScraped(
            id=local_id,
            titulo=titulo,
            tipo_operacion=tipo_operacion,
            precio_mensual=precio if tipo_operacion == "alquiler" else None,
            precio_venta=precio if tipo_operacion == "venta" else None,
            m2=m2,
            direccion=addr.get("streetAddress"),
            barrio=barrio,
            codigo_postal=cp,
            lat=_to_float(geo.get("latitude")),
            lng=_to_float(geo.get("longitude")),
            url_anuncio=data.get("url"),
        )
    except Exception as e:
        logger.debug("Error parseando JSON-LD Fotocasa: %s", e)
        return None


def _parse_next_data(html: str, tipo_operacion: str) -> list[LocalScraped]:
    """Extrae listings del __NEXT_DATA__ de Next.js."""
    match = re.search(
        r'<script[^>]+id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        html, re.DOTALL | re.IGNORECASE
    )
    if not match:
        return []
    try:
        data = json.loads(match.group(1).strip())
    except json.JSONDecodeError:
        return []

    locales = []
    props = data.get("props", {}).get("pageProps", {})

    # Varios paths conocidos en la estructura de Fotocasa
    listings_raw = (
        props.get("initialProps", {}).get("results", [])
        or props.get("results", [])
        or props.get("listings", [])
        or props.get("properties", [])
        or _deep_find_list(data, "results")
        or _deep_find_list(data, "listings")
        or []
    )

    for item in listings_raw:
        if not isinstance(item, dict):
            continue
        local = _next_data_item_to_local(item, tipo_operacion)
        if local:
            locales.append(local)

    return locales


def _next_data_item_to_local(item: dict, tipo_operacion: str) -> Optional[LocalScraped]:
    """Normaliza un item de __NEXT_DATA__ de Fotocasa."""
    try:
        # Precio — múltiples estructuras posibles
        precio = (
            _to_float(item.get("price"))
            or _to_float(item.get("priceInfo", {}).get("amount"))
            or _to_float(_get_nested(item, "transactions", 0, "value", 0, "amount"))
        )

        lat = (
            _to_float(item.get("latitude"))
            or _to_float(item.get("coordinates", {}).get("latitude"))
            or _to_float(item.get("ubication", {}).get("latitude"))
        )
        lng = (
            _to_float(item.get("longitude"))
            or _to_float(item.get("coordinates", {}).get("longitude"))
            or _to_float(item.get("ubication", {}).get("longitude"))
        )
        m2 = _to_float(item.get("surface") or item.get("size") or item.get("area"))

        prop_id = (
            item.get("id")
            or item.get("propertyCode")
            or item.get("realEstateCode")
            or item.get("referenceId")
        )
        titulo = item.get("title") or item.get("name")
        address = (
            item.get("address")
            or item.get("location", {}).get("address")
            or item.get("street", "")
        )
        barrio = (
            item.get("neighborhood")
            or item.get("location", {}).get("neighborhood")
            or item.get("zone")
        )
        distrito = (
            item.get("district")
            or item.get("location", {}).get("district")
        )
        cp = item.get("postalCode") or item.get("zipCode")
        url_path = item.get("url") or item.get("detailUrl") or ""
        url_completa = url_path if url_path.startswith("http") else f"{_BASE_URL}{url_path}"
        referencia = str(prop_id) if prop_id else None

        if not precio and not lat:
            return None

        return LocalScraped(
            id=f"fotocasa_{prop_id}" if prop_id else f"fotocasa_{md5(str(item).encode()).hexdigest()[:8]}",
            titulo=titulo,
            tipo_operacion=tipo_operacion,
            precio_mensual=precio if tipo_operacion == "alquiler" else None,
            precio_venta=precio if tipo_operacion == "venta" else None,
            m2=m2,
            direccion=address,
            barrio=barrio,
            distrito=distrito,
            codigo_postal=cp,
            lat=lat,
            lng=lng,
            url_anuncio=url_completa,
            referencia=referencia,
        )
    except Exception as e:
        logger.debug("Error normalizando item __NEXT_DATA__ Fotocasa: %s", e)
        return None


def _parse_beautifulsoup(html: str, tipo_operacion: str) -> list[LocalScraped]:
    """
    Fallback: parsea el DOM HTML con BeautifulSoup.
    Más frágil que JSON pero útil cuando el JSON no está presente.
    """
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        logger.debug("BeautifulSoup no disponible (pip install beautifulsoup4)")
        return []

    try:
        soup = BeautifulSoup(html, "html.parser")
        locales = []

        # Buscar tarjetas de anuncio (selectores típicos de Fotocasa)
        cards = (
            soup.select("[data-testid='CardList'] li")
            or soup.select("article.re-CardPackMinimal")
            or soup.select("li.re-SearchResultsItem")
            or soup.select("[class*='CardList'] li")
        )

        for card in cards:
            try:
                # Título
                titulo_el = card.select_one("h2, h3, [class*='Title']")
                titulo = titulo_el.get_text(strip=True) if titulo_el else None

                # Precio
                precio = None
                precio_el = card.select_one("[class*='Price'], [data-testid='price']")
                if precio_el:
                    precio = _parse_precio_str(precio_el.get_text(strip=True))

                # m2
                m2 = None
                for el in card.select("[class*='Feature'], [class*='feature']"):
                    texto = el.get_text(strip=True)
                    m2_match = re.search(r'(\d+(?:[.,]\d+)?)\s*m[²2]', texto, re.IGNORECASE)
                    if m2_match:
                        m2 = _to_float(m2_match.group(1).replace(",", "."))
                        break

                # URL
                link = card.select_one("a[href]")
                url_path = link["href"] if link else ""
                url_completa = url_path if url_path.startswith("http") else f"{_BASE_URL}{url_path}"

                # Dirección
                addr_el = card.select_one("[class*='Address'], [class*='address'], [class*='Location']")
                direccion = addr_el.get_text(strip=True) if addr_el else None

                # ID desde la URL
                id_match = re.search(r'/(\d{6,})', url_path)
                prop_id = id_match.group(1) if id_match else md5(url_path.encode()).hexdigest()[:8]

                if not precio and not titulo:
                    continue

                locales.append(LocalScraped(
                    id=f"fotocasa_{prop_id}",
                    titulo=titulo,
                    tipo_operacion=tipo_operacion,
                    precio_mensual=precio if tipo_operacion == "alquiler" else None,
                    precio_venta=precio if tipo_operacion == "venta" else None,
                    m2=m2,
                    direccion=direccion,
                    url_anuncio=url_completa,
                ))
            except Exception as e:
                logger.debug("Error procesando card BeautifulSoup Fotocasa: %s", e)

        return locales
    except Exception as e:
        logger.debug("Error en BeautifulSoup Fotocasa: %s", e)
        return []


def _parse_regex_fallback(html: str, tipo_operacion: str) -> list[LocalScraped]:
    """Extracción mínima por regex cuando todo lo demás falla."""
    locales = []
    prices = re.findall(r'"price"\s*:\s*(\d+(?:\.\d+)?)', html)
    lats   = re.findall(r'"latitude"\s*:\s*([-\d.]+)', html)
    lngs   = re.findall(r'"longitude"\s*:\s*([-\d.]+)', html)
    ids    = re.findall(r'"(?:id|propertyCode|realEstateCode)"\s*:\s*"?(\d+)"?', html)

    for i, precio_str in enumerate(prices[:_ITEMS_POR_PAGINA]):
        try:
            precio = float(precio_str)
            lat = float(lats[i]) if i < len(lats) else None
            lng = float(lngs[i]) if i < len(lngs) else None
            prop_id = ids[i] if i < len(ids) else md5(f"{precio}{lat}{lng}".encode()).hexdigest()[:8]

            locales.append(LocalScraped(
                id=f"fotocasa_{prop_id}",
                tipo_operacion=tipo_operacion,
                precio_mensual=precio if tipo_operacion == "alquiler" else None,
                precio_venta=precio if tipo_operacion == "venta" else None,
                lat=lat,
                lng=lng,
            ))
        except (ValueError, TypeError):
            pass
    return locales


# ── Helpers de parsing ────────────────────────────────────────────────────────

def _to_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        if isinstance(val, str):
            val = val.replace(".", "").replace(",", ".").replace("€", "").strip()
        return float(val) or None
    except (ValueError, TypeError):
        return None


def _parse_precio_str(texto: str) -> Optional[float]:
    """Convierte '1.200 €/mes' o '1.200.000 €' a float."""
    if not texto:
        return None
    # Eliminar separadores de miles y símbolo moneda
    limpio = re.sub(r'[€\s/mes/month]', '', texto.replace(".", "").replace(",", "."))
    match = re.search(r'([\d.]+)', limpio)
    if match:
        try:
            return float(match.group(1)) or None
        except ValueError:
            pass
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
    """Búsqueda recursiva de clave con lista (máx 7 niveles)."""
    if _depth > 7:
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
