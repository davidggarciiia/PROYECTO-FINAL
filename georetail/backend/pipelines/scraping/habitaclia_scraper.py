"""
pipelines/scraping/habitaclia_scraper.py — Scraper de Habitaclia para locales en alquiler.

TÉCNICA (probada con 4.747 registros reales en el repositorio de referencia):
  El repo github.com/habitaclia-scraping demostró que Habitaclia expone todos
  los datos en HTML estático sin JavaScript. La clave es el proceso en DOS PASOS:

  PASO 1 — Página de listado:
    URL:  https://www.habitaclia.com/alquiler-locales_comerciales-{ciudad}.htm
    URL:  https://www.habitaclia.com/alquiler-locales_comerciales-{ciudad}-{N}.htm
    Extrae: article[data-href] → lista de URLs de detalle

  PASO 2 — Cada página de detalle (una por anuncio):
    Extrae con BeautifulSoup usando selectores CSS estables:
      div.summary-left           → contenedor principal
      div.price span.font-2      → precio €/mes
      #js-ver-mapa-zona          → barrio/ubicación
      ul.feature-container li    → m², escaparate, planta, etc.
      section.detail             → características detalladas
      #js-detail-description     → descripción libre

  VOLUMEN ESTIMADO Barcelona:
    ~800-1.500 locales en alquiler activos en cualquier momento.
    ~30 anuncios/página → 30-50 páginas para cubrir todo.

  ANTI-BOT:
    Habitaclia tiene Cloudflare básico. BaseScraper con curl_cffi chrome131
    lo bypasea sin problemas. Delay entre peticiones: 0.8-2.0s.
"""
from __future__ import annotations

import asyncio
import logging
import re
from typing import Optional

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

_DELAY_DETALLE_MIN = 0.8
_DELAY_DETALLE_MAX = 2.0


class HabitacliaScraper:
    def __init__(self, config: Optional[ScrapingConfig] = None):
        self.cfg = config or ScrapingConfig()

    async def scrape(self, ciudad: str = "barcelona", max_paginas: int = 30) -> list[dict]:
        ciudad_slug = _CIUDADES.get(ciudad, ciudad)
        urls_detalle: list[str] = []
        ids_vistos: set[str] = set()

        async with BaseScraper(self.cfg) as scraper:
            await self._recopilar_urls(scraper, ciudad_slug, max_paginas, ids_vistos, urls_detalle)
            logger.info("Habitaclia: %d URLs de detalle recopiladas", len(urls_detalle))
            if not urls_detalle:
                logger.warning("Habitaclia: sin URLs de detalle — listado vacío o bloqueado")
                return []
            resultados = await self._descargar_detalles(scraper, urls_detalle)

        logger.info("Habitaclia: %d locales extraídos", len(resultados))
        return resultados

    async def _recopilar_urls(self, scraper, ciudad_slug, max_paginas, ids_vistos, urls_detalle):
        for num_pag in range(1, max_paginas + 1):
            url_listado = (
                _LISTING_P1.format(ciudad=ciudad_slug)
                if num_pag == 1
                else _LISTING_PN.format(ciudad=ciudad_slug, n=num_pag)
            )
            referer = _BASE_URL if num_pag == 1 else _LISTING_P1.format(ciudad=ciudad_slug)

            html = await scraper.get(url_listado, referer=referer)
            if not html or len(html) < 1000:
                logger.info("Habitaclia listado: sin respuesta en pág %d — parando", num_pag)
                break

            urls_nuevas = _extraer_urls_listado(html)
            if not urls_nuevas:
                logger.info("Habitaclia listado: sin artículos en pág %d — fin", num_pag)
                break

            nuevas = 0
            for url in urls_nuevas:
                local_id = _id_desde_url(url)
                if local_id and local_id not in ids_vistos:
                    ids_vistos.add(local_id)
                    urls_detalle.append(url)
                    nuevas += 1

            logger.info("Habitaclia pág %d: %d URLs (+%d nuevas)", num_pag, len(urls_nuevas), nuevas)
            if nuevas == 0 and num_pag > 1:
                break

            max_disp = _extraer_max_paginas(html)
            if max_disp and num_pag >= max_disp:
                break

    async def _descargar_detalles(self, scraper, urls):
        resultados = []
        for i, url in enumerate(urls):
            try:
                if i > 0:
                    await asyncio.sleep(
                        _DELAY_DETALLE_MIN +
                        ((_DELAY_DETALLE_MAX - _DELAY_DETALLE_MIN) * (hash(url) % 100) / 100)
                    )
                html = await scraper.get(url, referer=_BASE_URL)
                if not html or len(html) < 2000:
                    continue
                item = _parsear_detalle(html, url)
                if item:
                    resultados.append(item)
                    if len(resultados) % 50 == 0:
                        logger.info("Habitaclia: %d locales procesados...", len(resultados))
            except Exception as e:
                logger.debug("Habitaclia: error en detalle %s: %s", url, e)
        return resultados


def _extraer_urls_listado(html: str) -> list[str]:
    try:
        soup = BeautifulSoup(html, "lxml")
        section = soup.find("section", {"class": "list-items"})
        if section:
            articles = section.find_all("article", attrs={"data-href": True})
            urls = [a["data-href"] for a in articles if a.get("data-href")]
            if urls:
                return [u for u in urls if _es_url_valida(u)]
        articles = soup.find_all("article", attrs={"data-href": True})
        urls = [a["data-href"] for a in articles if a.get("data-href")]
        return [u for u in urls if _es_url_valida(u)]
    except Exception as e:
        logger.debug("Error extrayendo URLs de listado: %s", e)
        return []


def _extraer_max_paginas(html: str) -> Optional[int]:
    try:
        soup = BeautifulSoup(html, "lxml")
        nav = soup.find(id="js-nav")
        if not nav:
            return None
        li_next = nav.find("li", {"class": "next"})
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
    if re.match(r"^https:.*?\.com/f[av]\d+$", url):
        return False
    return "habitaclia.com" in url


def _id_desde_url(url: str) -> Optional[str]:
    m = re.search(r"/(\d+)(?:\.htm)?$", url)
    if m:
        return f"habitaclia_{m.group(1)}"
    path = url.split("habitaclia.com")[-1].strip("/")
    return f"habitaclia_{hash(path) & 0x7FFFFFFF}" if path else None


def _parsear_detalle(html: str, url: str) -> Optional[dict]:
    try:
        soup = BeautifulSoup(html, "lxml")

        summary = soup.find("div", {"class": "summary-left"})
        if not summary:
            summary = soup.find("div", {"class": re.compile(r"summary", re.I)})
        if not summary:
            return None

        # Precio
        precio = None
        price_div = summary.find("div", {"class": "price"})
        if price_div:
            price_span = price_div.find("span", {"class": "font-2"})
            if price_span and price_span.string:
                texto_precio = price_span.string.strip()
                if "consultar" in texto_precio.lower():
                    return None
                precio = _limpiar_precio(texto_precio)
        if not precio:
            return None

        # Nombre
        nombre = ""
        h1 = summary.find("h1")
        if h1:
            nombre = h1.get_text(strip=True)

        # Barrio / ubicación
        barrio = ""
        zona_el = summary.find(id="js-ver-mapa-zona")
        if zona_el:
            barrio = zona_el.get_text(strip=True)
        if not barrio:
            return None

        # Características (m², planta, escaparate)
        m2 = None
        planta_baja = None
        escaparate = None
        uso_permitido = None

        feature_container = summary.find("ul", {"class": "feature-container"})
        if feature_container:
            for li in feature_container.find_all("li"):
                texto = li.get_text(strip=True).lower()
                if "m2" in texto and "€/m2" not in texto:
                    nums = re.findall(r"\d+(?:[.,]\d+)?", texto)
                    if nums:
                        try:
                            val = float(nums[0].replace(",", "."))
                            if 5 <= val <= 5000:
                                m2 = val
                        except ValueError:
                            pass
                if "planta baja" in texto or texto.strip() == "pb":
                    planta_baja = True
                if "escaparate" in texto:
                    escaparate = True

        # Sección de detalle
        features_texto = []
        distribucion_texto = []
        detail_section = soup.find("section", {"class": "detail"})
        if detail_section:
            h3_carac = detail_section.find("h3", string=re.compile(r"Características", re.I))
            if h3_carac:
                ul = h3_carac.find_next("ul")
                if ul:
                    for li in ul.find_all("li", attrs={"class": None}):
                        texto = li.get_text(strip=True)
                        if texto:
                            features_texto.append(texto)
                            t = texto.lower()
                            if "escaparate" in t:
                                escaparate = True
                            if "planta baja" in t:
                                planta_baja = True
                            if any(k in t for k in ["hostelería","hosteleria","comercio","oficina","restaurante","bar","servicios"]):
                                uso_permitido = texto

            h3_dist = detail_section.find("h3", string=re.compile(r"Distribución", re.I))
            if h3_dist:
                ul = h3_dist.find_next("ul")
                if ul:
                    for li in ul.find_all("li"):
                        texto = li.get_text(strip=True)
                        if texto:
                            distribucion_texto.append(texto)
                            if not m2 and "m2" in texto.lower():
                                nums = re.findall(r"\d+(?:[.,]\d+)?", texto)
                                if nums:
                                    try:
                                        val = float(nums[0].replace(",", "."))
                                        if 5 <= val <= 5000:
                                            m2 = val
                                    except ValueError:
                                        pass

        # Descripción
        descripcion = ""
        if detail_section:
            desc_el = detail_section.find(id="js-detail-description")
            if desc_el:
                descripcion = desc_el.get_text(" ", strip=True)[:1000]

        local_id = _id_desde_url(url)
        if not local_id:
            return None

        precio_m2 = round(precio / m2, 2) if precio and m2 and m2 > 0 else None
        distrito   = _barrio_a_distrito(barrio)

        return {
            "id":              local_id,
            "fuente":          "habitaclia",
            "titulo":          nombre,
            "precio":          precio,
            "m2":              m2,
            "precio_m2":       precio_m2,
            "lat":             None,
            "lng":             None,
            "direccion":       barrio,
            "barrio":          barrio,
            "distrito":        distrito,
            "url":             url,
            "planta_baja":     planta_baja,
            "escaparate":      escaparate,
            "uso_permitido":   uso_permitido,
            "features_detail": "%;%".join(features_texto),
            "descripcion":     descripcion,
        }

    except Exception as e:
        logger.debug("Error parseando detalle Habitaclia %s: %s", url, e)
        return None


def _limpiar_precio(texto: str) -> Optional[float]:
    if not texto:
        return None
    limpio = re.sub(r"[€\s/mes]", "", texto.replace(".", "").replace(",", "."))
    match = re.search(r"\d+(?:\.\d+)?", limpio)
    if match:
        try:
            val = float(match.group())
            if 100 <= val <= 100_000:
                return val
        except ValueError:
            pass
    return None


_BARRIO_DISTRITO = {
    "dreta de l´eixample": "Eixample", "esquerra de l´eixample": "Eixample",
    "esquerra alta de l´eixample": "Eixample", "esquerra baixa de l´eixample": "Eixample",
    "nova esquerra de l´eixample": "Eixample", "antigua esquerra de l´eixample": "Eixample",
    "fort pienc": "Eixample", "sagrada família": "Eixample", "eixample": "Eixample",
    "vila de gràcia": "Gràcia", "gràcia": "Gràcia", "gracia": "Gràcia",
    "camp de l´arpa": "Gràcia", "camp de gràcia": "Gràcia", "salut": "Gràcia",
    "putget": "Gràcia", "farró": "Gràcia",
    "sant gervasi - galvany": "Sarrià-Sant Gervasi", "sant gervasi - bonanova": "Sarrià-Sant Gervasi",
    "sant gervasi": "Sarrià-Sant Gervasi", "sarrià": "Sarrià-Sant Gervasi",
    "sarria": "Sarrià-Sant Gervasi", "tres torres": "Sarrià-Sant Gervasi",
    "pedralbes": "Sarrià-Sant Gervasi", "vallvidrera": "Sarrià-Sant Gervasi",
    "raval": "Ciutat Vella", "gòtic": "Ciutat Vella", "gothic": "Ciutat Vella",
    "barceloneta": "Ciutat Vella", "st. pere": "Ciutat Vella", "sant pere": "Ciutat Vella",
    "born": "Ciutat Vella", "ribera": "Ciutat Vella", "santa caterina": "Ciutat Vella",
    "ciutat vella": "Ciutat Vella",
    "poblenou": "Sant Martí", "clot": "Sant Martí", "vila olímpica": "Sant Martí",
    "provençals": "Sant Martí", "sant martí": "Sant Martí",
    "sants": "Sants-Montjuïc", "hostafrancs": "Sants-Montjuïc", "bordeta": "Sants-Montjuïc",
    "marina": "Sants-Montjuïc", "montjuïc": "Sants-Montjuïc", "poble sec": "Sants-Montjuïc",
    "les corts": "Les Corts", "maternitat": "Les Corts",
    "horta": "Horta-Guinardó", "guinardó": "Horta-Guinardó", "carmel": "Horta-Guinardó",
    "baix guinardó": "Horta-Guinardó",
    "nou barris": "Nou Barris", "prosperitat": "Nou Barris", "trinitat": "Nou Barris",
    "roquetes": "Nou Barris", "verdun": "Nou Barris", "porta": "Nou Barris",
    "sant andreu": "Sant Andreu", "sagrera": "Sant Andreu", "bon pastor": "Sant Andreu",
    "navas": "Sant Andreu",
}


def _barrio_a_distrito(barrio: str) -> Optional[str]:
    if not barrio:
        return None
    barrio_lower = barrio.lower()
    if barrio_lower in _BARRIO_DISTRITO:
        return _BARRIO_DISTRITO[barrio_lower]
    for keyword, distrito in _BARRIO_DISTRITO.items():
        if keyword in barrio_lower:
            return distrito
    return None
