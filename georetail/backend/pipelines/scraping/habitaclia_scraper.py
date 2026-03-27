"""
pipelines/scraping/habitaclia_scraper.py — Scraper de Habitaclia (UN SOLO PASO).

POR QUÉ UN SOLO PASO:
  La estrategia de 2 pasos (listado + detalle) hace 1 + 15 = 16 peticiones por
  página. Cloudflare detecta el burst y bloquea la IP tras los primeros anuncios.

  Con un solo paso (solo páginas de listado) hacemos 1 petición por página.
  107 páginas = 107 peticiones totales, espaciadas 2-5s → invisible para Cloudflare.

QUÉ EXTRAEMOS DEL LISTADO:
  - id:       número en la URL  (.../i47595000000952.htm → "habitaclia_47595000000952")
  - url:      data-href del article (ya limpia, sin ?pag= ni parámetros de nav)
  - barrio:   slug de la URL (.../local_comercial-sant_gervasi_galvany-barcelona-i...)
  - precio:   tarjeta de precio dentro del article (.price, span con €)
  - m2:       li / span de características que contenga "m2" o "m²"
  - titulo:   h2 / h3 del article
  - escaparate: texto "escaparate" o "façana" en la tarjeta

QUÉ NO OBTENEMOS (aceptable):
  - Descripción larga (no necesaria para el scoring)
  - Uso permitido específico
  - Estos campos son opcionales en InmueblePortal
"""
from __future__ import annotations

import logging
import re
from typing import Optional
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse

from bs4 import BeautifulSoup

from .base_scraper import BaseScraper, ScrapingConfig

logger = logging.getLogger(__name__)

_BASE_URL   = "https://www.habitaclia.com"
_LISTING_P1 = _BASE_URL + "/alquiler-locales_comerciales-{ciudad}.htm"
_LISTING_PN = _BASE_URL + "/alquiler-locales_comerciales-{ciudad}-{n}.htm"

_CIUDADES = {
    "barcelona": "barcelona",
    "madrid":    "madrid",
    "valencia":  "valencia",
}


class HabitacliaScraper:
    """
    Extrae locales en alquiler de Habitaclia usando SOLO las páginas de listado.
    Sin páginas de detalle → 15× menos peticiones → sin bloqueos de Cloudflare.
    """

    def __init__(self, config: Optional[ScrapingConfig] = None):
        self.cfg = config or ScrapingConfig()

    async def scrape(self, ciudad: str = "barcelona", max_paginas: int = 107) -> list[dict]:
        ciudad_slug = _CIUDADES.get(ciudad, ciudad)
        resultados: list[dict] = []
        ids_vistos: set[str] = set()

        async with BaseScraper(self.cfg) as scraper:
            for num_pag in range(1, max_paginas + 1):
                url_listado = (
                    _LISTING_P1.format(ciudad=ciudad_slug)
                    if num_pag == 1
                    else _LISTING_PN.format(ciudad=ciudad_slug, n=num_pag)
                )
                referer = (
                    _BASE_URL
                    if num_pag == 1
                    else _LISTING_P1.format(ciudad=ciudad_slug)
                )

                html = await scraper.get(url_listado, referer=referer)

                if not html or len(html) < 1000:
                    logger.info("Habitaclia: sin HTML en pág %d — parando", num_pag)
                    break

                items = _parsear_listado(html, ids_vistos)

                if not items:
                    logger.info("Habitaclia: sin anuncios en pág %d — fin", num_pag)
                    break

                resultados.extend(items)
                logger.info(
                    "Habitaclia pág %d: +%d anuncios (total %d)",
                    num_pag, len(items), len(resultados),
                )

                max_pag = _extraer_max_paginas(html)
                if max_pag and num_pag >= max_pag:
                    logger.info("Habitaclia: última página alcanzada (%d)", max_pag)
                    break

        logger.info("HabitacliaScraper: %d locales extraídos en total", len(resultados))
        return resultados


# ── Parser: extrae todo del HTML del listado ──────────────────────────────────

def _parsear_listado(html: str, ids_vistos: set[str]) -> list[dict]:
    resultados = []
    try:
        soup = BeautifulSoup(html, "lxml")
        section = soup.find("section", class_="list-items") or soup
        articles = section.find_all("article", attrs={"data-href": True})
        if not articles:
            articles = soup.find_all("article", attrs={"data-href": True})

        for article in articles:
            url_raw = article.get("data-href", "")
            if not url_raw or not _es_url_valida(url_raw):
                continue

            # Limpiar parámetros de navegación de la URL
            url = _limpiar_url(url_raw)
            local_id = _id_desde_url(url)
            if not local_id or local_id in ids_vistos:
                continue

            item = _parsear_article(article, url)
            if item:
                ids_vistos.add(local_id)
                resultados.append(item)

    except Exception as e:
        logger.debug("Error parseando listado: %s", e)

    return resultados


def _parsear_article(article, url: str) -> Optional[dict]:
    """
    Extrae los datos de un article del listado.
    El barrio se obtiene del slug de la URL (muy fiable).
    El precio y m² se obtienen de las tarjetas del listado.
    """
    try:
        local_id = _id_desde_url(url)
        if not local_id:
            return None

        # ── Barrio (desde la URL — la fuente más fiable) ──────────────────────
        barrio = _barrio_desde_url(url)
        if not barrio:
            return None  # sin barrio no podemos asignar zona → descartar

        # ── Precio ────────────────────────────────────────────────────────────
        precio = None
        # Habitaclia puede usar varios contenedores de precio
        for selector in [
            {"class": re.compile(r"\bprice\b", re.I)},
            {"class": re.compile(r"price-detail", re.I)},
            {"itemprop": "price"},
        ]:
            el = article.find(attrs=selector)
            if el:
                precio = _limpiar_precio(el.get_text(" ", strip=True))
                if precio:
                    break

        # Fallback: buscar cualquier texto con € en el article
        if not precio:
            texto_completo = article.get_text(" ", strip=True)
            matches = re.findall(r"[\d.,]+\s*€(?:/mes)?", texto_completo)
            for m in matches:
                precio = _limpiar_precio(m)
                if precio:
                    break

        # ── m² ───────────────────────────────────────────────────────────────
        m2 = None
        for el in article.find_all(["li", "span", "div", "p"]):
            texto = el.get_text(strip=True)
            if ("m2" in texto.lower() or "m²" in texto) and "€" not in texto:
                nums = re.findall(r"\d+(?:[.,]\d+)?", texto)
                if nums:
                    try:
                        val = float(nums[0].replace(",", "."))
                        if 5 <= val <= 5000:
                            m2 = val
                            break
                    except ValueError:
                        pass

        # ── Escaparate ────────────────────────────────────────────────────────
        texto_card = article.get_text(" ", strip=True).lower()
        escaparate = True if ("escaparate" in texto_card or "façana" in texto_card) else None

        # ── Título ────────────────────────────────────────────────────────────
        titulo = ""
        for tag in ["h2", "h3", "h1"]:
            h = article.find(tag)
            if h:
                titulo = h.get_text(strip=True)
                break
        if not titulo:
            titulo = article.get("title") or article.get("aria-label") or ""

        distrito = _barrio_a_distrito(barrio)
        precio_m2 = round(precio / m2, 2) if precio and m2 and m2 > 0 else None

        return {
            "id":             local_id,
            "fuente":         "habitaclia",
            "titulo":         titulo,
            "precio":         precio,
            "m2":             m2,
            "precio_m2":      precio_m2,
            "lat":            None,
            "lng":            None,
            "direccion":      barrio,
            "barrio":         barrio,
            "distrito":       distrito,
            "url":            url,
            "escaparate":     escaparate,
            "planta_baja":    True,
            "tipo_operacion": "alquiler-locales",
            "tipo_inmueble":  "local",
        }

    except Exception as e:
        logger.debug("Error parseando article %s: %s", url, e)
        return None


# ── Extracción de barrio desde URL ────────────────────────────────────────────

def _barrio_desde_url(url: str) -> Optional[str]:
    """
    Extrae el barrio del slug de la URL de Habitaclia.

    Ejemplos de URLs reales:
      .../alquiler-local_comercial-sant_gervasi_galvany-barcelona-i123.htm
        → "sant gervasi galvany"
      .../alquiler-local_comercial-la_nova_esquerra_de_l_eixample-barcelona-i456.htm
        → "nova esquerra de l eixample"
      .../alquiler-local_comercial-local_luminoso_en_calle_tallers_el_raval-barcelona-i789.htm
        → "el raval"
    """
    # Extraer el slug entre "local_comercial-" y "-barcelona-i{numero}"
    m = re.search(r"/alquiler-local[_s]comercial-(.+?)-barcelona-i\d+", url)
    if not m:
        return None

    slug = m.group(1).replace("_", " ").strip()

    # Intentar detectar barrio conocido directamente en el slug
    barrio_detectado = _detectar_barrio_conocido(slug)
    if barrio_detectado:
        return barrio_detectado

    # Limpiar palabras genéricas del principio del slug
    _PREFIJOS_GENERICOS = [
        r"^local\s+en\s+calle\s+[\w\s]+?\s+",  # "local en calle tallers "
        r"^local\s+en\s+avinguda\s+[\w\s]+?\s+",
        r"^local\s+en\s+passeig\s+[\w\s]+?\s+",
        r"^local\s+comercial\s+en\s+",
        r"^local\s+en\s+",
        r"^local\s+",
        r"^en\s+calle\s+[\w\s]+?\d+\s+",
        r"^en\s+",
        r"^(magnifico|fantastico|espectacular|bonito|amplio|luminoso|exclusivo|"
        r"disponible|nuevo|especial|premier|excelente|inmejorable|unico|"
        r"gran|reformado|centrico|acogedor)\s+",
        r"^se\s+alquila\s+",
    ]
    slug_clean = slug
    for patron in _PREFIJOS_GENERICOS:
        nuevo = re.sub(patron, "", slug_clean, flags=re.I).strip()
        if nuevo != slug_clean:
            slug_clean = nuevo
            barrio_detectado = _detectar_barrio_conocido(slug_clean)
            if barrio_detectado:
                return barrio_detectado

    # Eliminar dirección concreta si está al principio (texto + número)
    slug_clean = re.sub(r"^[\w\s'·-]+?\d+\s+", "", slug_clean).strip()
    if slug_clean:
        barrio_detectado = _detectar_barrio_conocido(slug_clean)
        if barrio_detectado:
            return barrio_detectado

    # Tomar los últimos 3-4 palabras como barrio aproximado
    tokens = slug_clean.split()
    if tokens:
        return " ".join(tokens[-min(4, len(tokens)):])

    return None


_BARRIOS_CONOCIDOS_ORDENADOS = sorted([
    "sant gervasi galvany", "sant gervasi bonanova", "sant gervasi",
    "nova esquerra de l eixample", "antiga esquerra de l eixample",
    "nova esquerra eixample", "antiga esquerra eixample",
    "esquerra de l eixample", "dreta de l eixample",
    "sagrada familia", "fort pienc", "sant antoni",
    "la vila de gracia", "vila de gracia", "camp de gracia",
    "el raval", "barri gotic", "barrio gotico",
    "la barceloneta", "barceloneta",
    "sant pere santa caterina el born", "sant pere", "santa caterina", "el born",
    "el poblenou", "poblenou", "el clot", "clot",
    "vila olimpica", "el fort pienc",
    "sants", "hostafrancs", "poble sec",
    "les corts", "la maternitat",
    "sant andreu", "la sagrera", "el guinardo", "guinardo", "horta",
    "el carmel", "carmel",
    "nou barris", "prosperitat", "trinitat vella",
    "sarria", "sarrià", "pedralbes", "tres torres",
    "la salut", "el putget",
    "gracia", "gràcia",
    "eixample",
], key=len, reverse=True)  # más largos primero para evitar matches parciales incorrectos


def _detectar_barrio_conocido(texto: str) -> Optional[str]:
    """Detecta si el texto contiene un barrio conocido de Barcelona."""
    t = texto.lower().strip()
    for barrio in _BARRIOS_CONOCIDOS_ORDENADOS:
        if barrio in t:
            return barrio
    return None


# ── Helpers ────────────────────────────────────────────────────────────────────

def _limpiar_url(url: str) -> str:
    """Elimina parámetros de navegación (?pag=N, ?f=, ?geo=, etc.) de la URL."""
    try:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        for k in ["pag", "f", "geo", "from", "lo"]:
            params.pop(k, None)
        clean_query = urlencode({k: v[0] for k, v in params.items()})
        return urlunparse(parsed._replace(query=clean_query))
    except Exception:
        return url


def _extraer_max_paginas(html: str) -> Optional[int]:
    try:
        soup = BeautifulSoup(html, "lxml")
        nav = soup.find(id="js-nav")
        if not nav:
            nav = soup.find(class_=re.compile(r"pagination|paginacion", re.I))
        if not nav:
            return None
        li_next = nav.find("li", class_="next")
        if not li_next:
            return 1
        prev_li = li_next.find_previous_sibling("li")
        if prev_li:
            texto = prev_li.get_text(strip=True)
            if texto.isdigit():
                return int(texto)
    except Exception:
        pass
    return None


def _es_url_valida(url: str) -> bool:
    if not url or not url.startswith("http"):
        return False
    # Excluir anuncios de portales partner (/fa123, /fv456)
    if re.match(r"^https://.*?\.com/f[av]\d+", url):
        return False
    return "habitaclia.com" in url and "/alquiler-local" in url


def _id_desde_url(url: str) -> Optional[str]:
    """Extrae ID único desde la URL. Usa el número -i{N} del slug."""
    url_limpia = _limpiar_url(url)
    m = re.search(r"-i(\d+)(?:\.htm)?", url_limpia)
    if m:
        return f"habitaclia_{m.group(1)}"
    path = url_limpia.split("habitaclia.com")[-1].strip("/")
    return f"habitaclia_{hash(path) & 0x7FFFFFFF}" if path else None


def _limpiar_precio(texto: str) -> Optional[float]:
    if not texto:
        return None
    if "consultar" in texto.lower():
        return None
    # Quitar todo excepto dígitos, punto y coma
    limpio = re.sub(r"[€\s/mes]", "", texto.replace(".", "").replace(",", "."))
    m = re.search(r"\d+(?:\.\d+)?", limpio)
    if m:
        try:
            val = float(m.group())
            if 100 <= val <= 100_000:
                return val
        except ValueError:
            pass
    return None


# ── Mapa barrio → distrito (ampliado con variantes de URL) ────────────────────

_BARRIO_DISTRITO: dict[str, str] = {
    # Eixample
    "dreta de l eixample": "Eixample", "dreta de l´eixample": "Eixample",
    "esquerra de l eixample": "Eixample", "esquerra de l´eixample": "Eixample",
    "nova esquerra de l eixample": "Eixample", "nova esquerra de l´eixample": "Eixample",
    "antiga esquerra de l eixample": "Eixample", "antiga esquerra de l´eixample": "Eixample",
    "nova esquerra eixample": "Eixample", "antiga esquerra eixample": "Eixample",
    "fort pienc": "Eixample", "sagrada familia": "Eixample",
    "sagrada família": "Eixample", "eixample": "Eixample", "sant antoni": "Eixample",
    # Gràcia
    "vila de gracia": "Gràcia", "la vila de gracia": "Gràcia",
    "vila de gràcia": "Gràcia", "gràcia": "Gràcia", "gracia": "Gràcia",
    "camp de gracia": "Gràcia", "camp de gràcia": "Gràcia",
    "la salut": "Gràcia", "salut": "Gràcia",
    "el putget": "Gràcia", "putget": "Gràcia",
    # Sarrià-Sant Gervasi
    "sant gervasi galvany": "Sarrià-Sant Gervasi",
    "sant gervasi bonanova": "Sarrià-Sant Gervasi",
    "sant gervasi": "Sarrià-Sant Gervasi",
    "sarrià": "Sarrià-Sant Gervasi", "sarria": "Sarrià-Sant Gervasi",
    "tres torres": "Sarrià-Sant Gervasi", "pedralbes": "Sarrià-Sant Gervasi",
    "vallvidrera": "Sarrià-Sant Gervasi",
    # Ciutat Vella
    "el raval": "Ciutat Vella", "raval": "Ciutat Vella",
    "barri gotic": "Ciutat Vella", "barri gòtic": "Ciutat Vella",
    "barrio gotico": "Ciutat Vella", "gòtic": "Ciutat Vella", "gothic": "Ciutat Vella",
    "la barceloneta": "Ciutat Vella", "barceloneta": "Ciutat Vella",
    "sant pere": "Ciutat Vella", "santa caterina": "Ciutat Vella",
    "el born": "Ciutat Vella", "born": "Ciutat Vella",
    "sant pere santa caterina el born": "Ciutat Vella",
    "ciutat vella": "Ciutat Vella",
    # Sant Martí
    "el poblenou": "Sant Martí", "poblenou": "Sant Martí",
    "el clot": "Sant Martí", "clot": "Sant Martí",
    "vila olimpica": "Sant Martí", "vila olímpica": "Sant Martí",
    "provençals": "Sant Martí", "sant martí": "Sant Martí",
    # Sants-Montjuïc
    "sants": "Sants-Montjuïc", "hostafrancs": "Sants-Montjuïc",
    "la bordeta": "Sants-Montjuïc", "poble sec": "Sants-Montjuïc",
    "montjuïc": "Sants-Montjuïc", "marina": "Sants-Montjuïc",
    # Les Corts
    "les corts": "Les Corts", "la maternitat": "Les Corts", "maternitat": "Les Corts",
    # Horta-Guinardó
    "horta": "Horta-Guinardó", "el guinardo": "Horta-Guinardó",
    "guinardo": "Horta-Guinardó", "el guinardó": "Horta-Guinardó",
    "el carmel": "Horta-Guinardó", "carmel": "Horta-Guinardó",
    "baix guinardo": "Horta-Guinardó",
    # Nou Barris
    "nou barris": "Nou Barris", "prosperitat": "Nou Barris",
    "trinitat vella": "Nou Barris", "trinitat": "Nou Barris",
    "roquetes": "Nou Barris", "verdun": "Nou Barris", "porta": "Nou Barris",
    # Sant Andreu
    "sant andreu": "Sant Andreu", "la sagrera": "Sant Andreu",
    "sagrera": "Sant Andreu", "el bon pastor": "Sant Andreu",
    "bon pastor": "Sant Andreu", "navas": "Sant Andreu",
}


def _barrio_a_distrito(barrio: str) -> Optional[str]:
    if not barrio:
        return None
    b = barrio.lower().strip()
    if b in _BARRIO_DISTRITO:
        return _BARRIO_DISTRITO[b]
    for keyword, distrito in _BARRIO_DISTRITO.items():
        if keyword in b:
            return distrito
    return None
