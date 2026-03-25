"""
pipelines/scraping/google_maps_scraper.py — Scraper de Google Maps para negocios por categoría.

Extrae negocios de Google Maps usando Playwright con capa stealth completa.
Google Maps es una SPA con JS obligatorio — no funciona con curl_cffi solo.

Arquitectura antibot:
  1. Playwright con playwright-stealth (webdriver=false, plugins reales, languages)
  2. curl_cffi como cliente HTTP secundario para requests simples
  3. Session warming: google.com → búsqueda genérica → búsqueda real
  4. Movimientos Bezier, delays humanos, resolución y viewport aleatorios
  5. Cookies persistentes por sesión en archivo JSON
  6. Backoff exponencial en 429 / CAPTCHA
  7. Rotación de user agents (Chrome 124-131 reales)

Uso:
    scraper = GoogleMapsScraper()
    negocios = await scraper.buscar_negocios_por_categoria(
        categoria="restaurante",
        zona_nombre="Gràcia",
        lat=41.4036, lng=2.1538, radio_m=300
    )
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# ── Constantes ────────────────────────────────────────────────────────────────
_SESSIONS_DIR = Path(os.environ.get("SCRAPING_SESSIONS_DIR", "/tmp/georetail_sessions"))
_SESSIONS_DIR.mkdir(parents=True, exist_ok=True)

_HEADLESS = os.environ.get("CI", "").lower() in ("1", "true", "yes")  # True en CI, False en prod

_CAPTCHA_WAIT_MIN = 1800   # 30 minutos en segundos
_CAPTCHA_WAIT_MAX = 3600   # 60 minutos en segundos
_RATE_LIMIT_WAIT_BASE = 60  # segundos base para backoff en 429

_MAX_RESULTS_PER_SEARCH = 60  # Google Maps muestra máx 60 resultados
_SCROLL_PAUSE_MIN = 1.2
_SCROLL_PAUSE_MAX = 2.8
_SCROLL_ATTEMPTS_MAX = 12   # scrolls máximos hasta agotar resultados

# ── User agents reales — Chrome 124-131 en Windows/Mac ────────────────────────
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.205 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.6723.117 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.6668.101 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.6613.137 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.6533.99 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.6478.183 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.205 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.6723.117 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_7_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.205 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.6723.117 Safari/537.36",
    "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.85 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.6367.208 Safari/537.36",
]

# ── Resoluciones de pantalla realistas ────────────────────────────────────────
_SCREEN_RESOLUTIONS = [
    (1920, 1080), (1920, 1200), (2560, 1440), (1680, 1050),
    (1600, 900),  (1440, 900),  (1366, 768),  (2560, 1600),
    (3840, 2160), (1280, 800),  (1280, 720),  (1536, 864),
]

# ── Categorías de warmup — búsquedas genéricas para calentar sesión ───────────
_WARMUP_QUERIES = [
    "tiempo Barcelona hoy",
    "noticias Barcelona",
    "restaurantes Barcelona",
    "farmacias abiertas ahora",
    "metro Barcelona horario",
]


@dataclass
class NegocioScraped:
    """
    Negocio extraído de Google Maps.
    Modelo de datos para scraping de competidores y análisis de mercado.
    """
    # Identificación
    nombre:          str
    google_place_id: Optional[str] = None
    fuente:          str = "google_maps"

    # Localización
    direccion:       Optional[str]  = None
    lat:             Optional[float] = None
    lng:             Optional[float] = None
    barrio:          Optional[str]  = None

    # Categorías y clasificación
    categoria:       Optional[str]  = None
    subcategorias:   list[str]      = field(default_factory=list)

    # Valoraciones
    rating:          Optional[float] = None
    review_count:    Optional[int]   = None

    # Precio y contacto
    precio_nivel:    Optional[str]  = None    # "€" | "€€" | "€€€" | "€€€€"
    telefono:        Optional[str]  = None
    web:             Optional[str]  = None

    # Horario
    horario:         Optional[dict] = None    # {"lunes": "09:00-21:00", ...}
    abierto_ahora:   Optional[bool] = None

    # Popular Times (histograma de concurrencia por hora)
    popular_times:   Optional[dict] = None
    # Formato: {"lunes": [0,0,10,20,40,70,80,60,40,20,10,0,0,0,0,0,0,0,0,0,0,0,0,0], ...}
    # 24 valores por día (horas 0-23), escala 0-100. None si el negocio no tiene datos.

    # Metadatos
    url_maps:        Optional[str]  = None
    timestamp_scraping: float       = field(default_factory=time.time)


# ── Script de inyección stealth ────────────────────────────────────────────────
def _build_stealth_script(ua: str, screen_w: int, screen_h: int) -> str:
    """
    Script JS inyectado antes de cada página para eliminar señales de automatización.
    Cubre: webdriver, plugins, languages, chrome object, permissions, screen.
    """
    return f"""
    // 1. Eliminar navigator.webdriver
    Object.defineProperty(navigator, 'webdriver', {{
        get: () => undefined,
        configurable: true,
    }});

    // 2. Plugins reales (no la lista vacía que delata Playwright)
    const _plugins = [
        {{
            name: 'Chrome PDF Plugin',
            description: 'Portable Document Format',
            filename: 'internal-pdf-viewer',
            length: 1,
            item: function(i) {{ return this[i]; }},
            namedItem: function(n) {{ return null; }},
            0: {{ type: 'application/x-google-chrome-pdf', suffixes: 'pdf', description: 'Portable Document Format' }},
        }},
        {{
            name: 'Chrome PDF Viewer',
            description: '',
            filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai',
            length: 1,
            item: function(i) {{ return this[i]; }},
            namedItem: function(n) {{ return null; }},
            0: {{ type: 'application/pdf', suffixes: 'pdf', description: '' }},
        }},
        {{
            name: 'Native Client',
            description: '',
            filename: 'internal-nacl-plugin',
            length: 2,
            item: function(i) {{ return this[i]; }},
            namedItem: function(n) {{ return null; }},
            0: {{ type: 'application/x-nacl', suffixes: '', description: 'Native Client Executable' }},
            1: {{ type: 'application/x-pnacl', suffixes: '', description: 'Portable Native Client Executable' }},
        }},
    ];
    _plugins.refresh = function() {{}};
    _plugins.item = function(i) {{ return _plugins[i] || null; }};
    _plugins.namedItem = function(n) {{ return _plugins.find(p => p.name === n) || null; }};
    Object.defineProperty(navigator, 'plugins', {{
        get: () => _plugins,
        configurable: true,
    }});

    // 3. Idiomas reales
    Object.defineProperty(navigator, 'languages', {{
        get: () => ['es-ES', 'es', 'en'],
        configurable: true,
    }});
    Object.defineProperty(navigator, 'language', {{
        get: () => 'es-ES',
        configurable: true,
    }});

    // 4. Chrome runtime object real
    if (!window.chrome) {{
        window.chrome = {{
            runtime: {{
                id: undefined,
                connect: function() {{}},
                sendMessage: function() {{}},
            }},
            loadTimes: function() {{ return {{ requestTime: Date.now() / 1000 }}; }},
            csi: function() {{ return {{ startE: Date.now(), onloadT: Date.now() + 200 }}; }},
        }};
    }}

    // 5. Permisos — evitar que devuelvan "denied" automáticamente
    const _originalQuery = window.navigator.permissions && window.navigator.permissions.query;
    if (_originalQuery) {{
        window.navigator.permissions.query = function(parameters) {{
            return parameters.name === 'notifications'
                ? Promise.resolve({{ state: Notification.permission }})
                : _originalQuery.call(this, parameters);
        }};
    }}

    // 6. Screen dimensions consistentes
    Object.defineProperty(screen, 'width',       {{ get: () => {screen_w} }});
    Object.defineProperty(screen, 'height',      {{ get: () => {screen_h} }});
    Object.defineProperty(screen, 'availWidth',  {{ get: () => {screen_w} }});
    Object.defineProperty(screen, 'availHeight', {{ get: () => {screen_h} - 40 }});
    Object.defineProperty(screen, 'colorDepth',  {{ get: () => 24 }});
    Object.defineProperty(screen, 'pixelDepth',  {{ get: () => 24 }});

    // 7. Hardware concurrency realista
    Object.defineProperty(navigator, 'hardwareConcurrency', {{
        get: () => {random.choice([4, 6, 8, 12, 16])},
        configurable: true,
    }});

    // 8. DeviceMemory (GB)
    Object.defineProperty(navigator, 'deviceMemory', {{
        get: () => {random.choice([4, 8, 16])},
        configurable: true,
    }});

    // 9. connection.rtt — latencia de red realista
    if (navigator.connection) {{
        Object.defineProperty(navigator.connection, 'rtt', {{ get: () => {random.choice([50, 75, 100, 125])} }});
    }}

    // 10. WebGL vendor/renderer — GPU real
    const _getParameter = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(parameter) {{
        if (parameter === 37445) return 'Intel Inc.';
        if (parameter === 37446) return 'Intel Iris OpenGL Engine';
        return _getParameter.call(this, parameter);
    }};
    """


# ── Movimiento de ratón tipo Bezier ───────────────────────────────────────────

def _bezier_points(x0: float, y0: float, x1: float, y1: float, steps: int = 20) -> list[tuple[float, float]]:
    """
    Genera puntos a lo largo de una curva de Bézier cúbica entre dos coordenadas.
    Los puntos de control son aleatorios para parecer movimiento humano.
    """
    # Puntos de control para la curva cúbica
    cx1 = x0 + random.uniform(-80, 80) + (x1 - x0) * 0.3
    cy1 = y0 + random.uniform(-80, 80) + (y1 - y0) * 0.3
    cx2 = x0 + random.uniform(-80, 80) + (x1 - x0) * 0.7
    cy2 = y0 + random.uniform(-80, 80) + (y1 - y0) * 0.7

    points = []
    for i in range(steps + 1):
        t = i / steps
        # Fórmula Bezier cúbica
        x = (
            (1 - t) ** 3 * x0
            + 3 * (1 - t) ** 2 * t * cx1
            + 3 * (1 - t) * t ** 2 * cx2
            + t ** 3 * x1
        )
        y = (
            (1 - t) ** 3 * y0
            + 3 * (1 - t) ** 2 * t * cy1
            + 3 * (1 - t) * t ** 2 * cy2
            + t ** 3 * y1
        )
        points.append((x, y))
    return points


async def _mover_raton_bezier(page, x_dest: float, y_dest: float) -> None:
    """Mueve el ratón de la posición actual al destino siguiendo una curva Bezier."""
    try:
        # Posición actual (estimada desde el centro del viewport)
        viewport = page.viewport_size or {"width": 1280, "height": 720}
        x_start = random.uniform(viewport["width"] * 0.3, viewport["width"] * 0.7)
        y_start = random.uniform(viewport["height"] * 0.3, viewport["height"] * 0.7)

        steps = random.randint(15, 30)
        points = _bezier_points(x_start, y_start, x_dest, y_dest, steps)

        for x, y in points:
            await page.mouse.move(x, y)
            await asyncio.sleep(random.uniform(0.01, 0.04))
    except Exception as e:
        logger.debug("Error moviendo ratón Bezier: %s", e)


async def _delay_humano(min_s: float = 0.5, max_s: float = 3.0) -> None:
    """Delay aleatorio que simula tiempo de reacción humano."""
    base = random.uniform(min_s, max_s)
    # 10% de probabilidad de pausa larga (el usuario se distrae)
    if random.random() < 0.10:
        base += random.uniform(1.5, 5.0)
    await asyncio.sleep(base)


# ── Gestión de cookies persistentes ──────────────────────────────────────────

def _cookies_path(session_id: str) -> Path:
    return _SESSIONS_DIR / f"google_session_{session_id}.json"


def _cargar_cookies(session_id: str) -> list[dict]:
    path = _cookies_path(session_id)
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return []


def _guardar_cookies(session_id: str, cookies: list[dict]) -> None:
    try:
        _cookies_path(session_id).write_text(json.dumps(cookies, ensure_ascii=False, indent=2))
    except Exception as e:
        logger.debug("Error guardando cookies: %s", e)


# ── Parsing de resultados de Google Maps ─────────────────────────────────────

def _parse_precio_nivel(texto: str) -> Optional[str]:
    """Convierte el indicador de precio de Google Maps a símbolo €."""
    if not texto:
        return None
    count = texto.count("€") or texto.count("$")
    if count == 0:
        # Puede venir como "Precio moderado" etc.
        texto_lower = texto.lower()
        if "inexpensivo" in texto_lower or "económico" in texto_lower:
            return "€"
        if "moderado" in texto_lower:
            return "€€"
        if "caro" in texto_lower or "costoso" in texto_lower:
            return "€€€"
        return None
    return "€" * min(count, 4)


def _parse_rating(texto: str) -> Optional[float]:
    """Extrae el rating numérico de un string como '4,5' o '4.5'."""
    if not texto:
        return None
    match = re.search(r'([\d][,.][\d])', texto)
    if match:
        try:
            return float(match.group(1).replace(",", "."))
        except ValueError:
            pass
    match = re.search(r'(\d)', texto)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            pass
    return None


def _parse_review_count(texto: str) -> Optional[int]:
    """Extrae el número de reseñas de strings como '(1.234)' o '1234 reseñas'."""
    if not texto:
        return None
    # Eliminar paréntesis y puntos de miles
    texto_limpio = re.sub(r'[().]', '', texto.replace(".", ""))
    match = re.search(r'(\d+)', texto_limpio)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            pass
    return None


def _extraer_place_id_de_url(url: str) -> Optional[str]:
    """Extrae el place_id o CID de una URL de Google Maps."""
    # Formato: /place/.../data=...!1s0x...
    match = re.search(r'place_id=([^&]+)', url)
    if match:
        return match.group(1)
    # Formato: ChIJ... en la URL
    match = re.search(r'ChIJ[A-Za-z0-9_-]+', url)
    if match:
        return match.group(0)
    # CID numérico: !1s0x...
    match = re.search(r'!1s(0x[0-9a-f]+:[0-9a-fx]+)', url, re.IGNORECASE)
    if match:
        return match.group(1)
    return None


async def _detectar_captcha(page) -> bool:
    """Devuelve True si la página actual contiene un CAPTCHA de Google."""
    try:
        url = page.url
        if "sorry/index" in url or "recaptcha" in url.lower():
            return True
        # Buscar iframe de reCAPTCHA
        frames = page.frames
        for frame in frames:
            if "recaptcha" in (frame.url or "").lower():
                return True
        # Buscar texto en página
        content = await page.content()
        captcha_signals = [
            "recaptcha",
            "g-recaptcha",
            "unusual traffic",
            "tráfico inusual",
            "robot",
            "not a robot",
            "I'm not a robot",
        ]
        content_lower = content.lower()
        if any(s.lower() in content_lower for s in captcha_signals) and len(content) < 30000:
            return True
    except Exception:
        pass
    return False


async def _detectar_signin_prompt(page) -> bool:
    """Detecta si Google Maps muestra un prompt de inicio de sesión."""
    try:
        content = await page.content()
        signin_signals = [
            "accounts.google.com",
            "Iniciar sesión",
            "Sign in",
            "Para ver más resultados, inicia sesión",
        ]
        return any(s in content for s in signin_signals)
    except Exception:
        return False


async def _cerrar_signin_prompt(page) -> None:
    """Intenta cerrar el dialog de Sign In sin hacer login."""
    try:
        # Botones de cierre comunes
        close_selectors = [
            '[aria-label="Cerrar"]',
            '[aria-label="Close"]',
            'button[jsname="LgbsSe"]',  # Google account chooser close
            '.VfPpkd-kBDsod',
        ]
        for sel in close_selectors:
            try:
                elem = await page.query_selector(sel)
                if elem:
                    await _mover_raton_bezier(page, *(await elem.bounding_box() or {}).get("x", 0), *(await elem.bounding_box() or {}).get("y", 0))
                    await elem.click()
                    await _delay_humano(0.5, 1.5)
                    return
            except Exception:
                pass
        # Último recurso: presionar Escape
        await page.keyboard.press("Escape")
        await _delay_humano(0.5, 1.0)
    except Exception as e:
        logger.debug("Error cerrando sign-in prompt: %s", e)


# ── Extracción de datos de un panel de negocio ───────────────────────────────

async def _extraer_datos_panel(page, listing_elem) -> Optional[NegocioScraped]:
    """
    Hace click en un resultado de la lista y extrae los datos del panel lateral.
    Devuelve NegocioScraped o None si falla.
    """
    try:
        # Coordenadas del elemento para click con Bezier
        box = await listing_elem.bounding_box()
        if not box:
            return None

        await _mover_raton_bezier(page, box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
        await listing_elem.click()
        await _delay_humano(1.5, 3.0)

        # Esperar a que cargue el panel
        panel_selector = 'div[role="main"][aria-label]'
        try:
            await page.wait_for_selector(panel_selector, timeout=8000)
        except Exception:
            logger.debug("Panel de negocio no cargó a tiempo")
            return None

        # ── Nombre ──
        nombre = None
        for sel in ['h1.DUwDvf', 'h1[data-attrid="title"]', 'h1']:
            try:
                el = await page.query_selector(sel)
                if el:
                    nombre = (await el.inner_text()).strip()
                    if nombre:
                        break
            except Exception:
                pass
        if not nombre:
            return None

        # ── Place ID desde URL ──
        current_url = page.url
        place_id = _extraer_place_id_de_url(current_url)

        # ── Rating y reseñas ──
        rating = None
        review_count = None
        try:
            rating_el = await page.query_selector('div.F7nice span[aria-hidden="true"]')
            if rating_el:
                rating = _parse_rating(await rating_el.inner_text())
            reviews_el = await page.query_selector('div.F7nice span[aria-label]')
            if reviews_el:
                aria = await reviews_el.get_attribute("aria-label")
                review_count = _parse_review_count(aria or "")
        except Exception:
            pass

        # ── Dirección ──
        direccion = None
        try:
            addr_el = await page.query_selector('[data-item-id="address"] .Io6YTe')
            if not addr_el:
                addr_el = await page.query_selector('button[data-tooltip="Copiar dirección"] .Io6YTe')
            if addr_el:
                direccion = (await addr_el.inner_text()).strip()
        except Exception:
            pass

        # ── Teléfono ──
        telefono = None
        try:
            tel_el = await page.query_selector('[data-item-id^="phone:tel:"] .Io6YTe')
            if tel_el:
                telefono = (await tel_el.inner_text()).strip()
        except Exception:
            pass

        # ── Web ──
        web = None
        try:
            web_el = await page.query_selector('a[data-item-id="authority"]')
            if web_el:
                web = await web_el.get_attribute("href")
        except Exception:
            pass

        # ── Precio nivel ──
        precio_nivel = None
        try:
            # Puede estar en el subtítulo o en una sección específica
            for sel in ['.mgr77e span', 'span.ZDu9vd', 'span[aria-label*="€"]', 'span[aria-label*="precio"]']:
                el = await page.query_selector(sel)
                if el:
                    texto = await el.inner_text()
                    precio_nivel = _parse_precio_nivel(texto)
                    if precio_nivel:
                        break
        except Exception:
            pass

        # ── Categoría ──
        categoria = None
        try:
            cat_el = await page.query_selector('button.DkEaL')
            if not cat_el:
                cat_el = await page.query_selector('span.YhemCb')
            if cat_el:
                categoria = (await cat_el.inner_text()).strip()
        except Exception:
            pass

        # ── Horario ──
        horario = None
        abierto_ahora = None
        try:
            # Intentar expandir horario si está colapsado
            horario_btn = await page.query_selector('div.OMl5r')
            if horario_btn:
                await horario_btn.click()
                await _delay_humano(0.3, 0.8)

            # Leer días y horas
            dias_els = await page.query_selector_all('tr.y0skZc')
            if dias_els:
                horario = {}
                for dia_el in dias_els:
                    try:
                        dia = await dia_el.query_selector('td.ylH6lf')
                        horas = await dia_el.query_selector('td.mxowUb')
                        if dia and horas:
                            horario[(await dia.inner_text()).strip()] = (await horas.inner_text()).strip()
                    except Exception:
                        pass

            # Abierto ahora
            open_el = await page.query_selector('span.dv0NOd')
            if open_el:
                txt = (await open_el.inner_text()).lower()
                if "abierto" in txt or "open" in txt:
                    abierto_ahora = True
                elif "cerrado" in txt or "closed" in txt:
                    abierto_ahora = False
        except Exception:
            pass

        # ── Coordenadas desde la URL ──
        lat, lng = None, None
        try:
            coord_match = re.search(r'@([-\d.]+),([-\d.]+)', current_url)
            if coord_match:
                lat = float(coord_match.group(1))
                lng = float(coord_match.group(2))
        except Exception:
            pass

        # ── Popular Times ──
        popular_times = await _extraer_popular_times(page)

        return NegocioScraped(
            nombre=nombre,
            google_place_id=place_id,
            direccion=direccion,
            lat=lat,
            lng=lng,
            categoria=categoria,
            rating=rating,
            review_count=review_count,
            precio_nivel=precio_nivel,
            telefono=telefono,
            web=web,
            horario=horario if horario else None,
            abierto_ahora=abierto_ahora,
            popular_times=popular_times,
            url_maps=current_url,
        )

    except Exception as e:
        logger.debug("Error extrayendo datos de panel: %s", e)
        return None


# ── Popular Times — extracción ────────────────────────────────────────────────

# Mapa de nombres de días en castellano/catalán → clave normalizada
_DIAS_MAP = {
    # Castellano
    "lunes": "lunes", "martes": "martes", "miércoles": "miércoles",
    "miercoles": "miércoles", "jueves": "jueves", "viernes": "viernes",
    "sábado": "sábado", "sabado": "sábado", "domingo": "domingo",
    # Catalán
    "dilluns": "lunes", "dimarts": "martes", "dimecres": "miércoles",
    "dijous": "jueves", "divendres": "viernes", "dissabte": "sábado",
    "diumenge": "domingo",
    # Inglés (por si el locale cambia)
    "monday": "lunes", "tuesday": "martes", "wednesday": "miércoles",
    "thursday": "jueves", "friday": "viernes", "saturday": "sábado",
    "sunday": "domingo",
}

# Palabras clave del aria-label de Google Maps para mapear ocupación cualitativa
_OCUPACION_ESCALA = {
    "no muy concurrido": 15,
    "poco concurrido": 25,
    "concurrido": 55,
    "muy concurrido": 75,
    "extremadamente concurrido": 95,
    "normalmente, no muy concurrido": 15,
    "normalmente, poco concurrido": 25,
    "normalmente, bastante concurrido": 65,
    "normalmente, concurrido": 55,
    "normalmente, muy concurrido": 75,
    "normalmente, extremadamente concurrido": 95,
    # Inglés
    "not too busy": 15,
    "not usually too busy": 15,
    "a little busy": 25,
    "usually a little busy": 25,
    "busy": 55,
    "usually busy": 55,
    "pretty busy": 65,
    "very busy": 75,
    "usually very busy": 75,
    "as busy as it gets": 95,
}


def _valor_ocupacion(texto: str) -> int:
    """
    Convierte descripción cualitativa de ocupación a valor numérico 0-100.
    Primero busca coincidencia exacta (lowercase), luego parcial.
    """
    t = texto.lower().strip()
    if t in _OCUPACION_ESCALA:
        return _OCUPACION_ESCALA[t]
    # Búsqueda parcial por orden de especificidad (más largo primero)
    for key in sorted(_OCUPACION_ESCALA, key=len, reverse=True):
        if key in t:
            return _OCUPACION_ESCALA[key]
    # Si contiene un porcentaje numérico (raro, pero Google a veces lo incluye)
    m = re.search(r'(\d{1,3})\s*%', texto)
    if m:
        return min(100, int(m.group(1)))
    return 0


async def _extraer_popular_times(page) -> Optional[dict]:
    """
    Extrae el histograma de Popular Times de la ficha abierta en Google Maps.

    Devuelve dict {dia: [24 valores 0-100]} o None si el negocio no tiene datos.

    Estrategia (de más a menos fiable):
      1. Leer atributos aria-label de las barras del histograma
         Selector: div con clase 'g2BVhd eoFzo' o similar que contiene
         botones con aria-label tipo "Lunes; Normalmente, no muy concurrido a las 9"
      2. Leer el texto estructurado de la sección de Popular Times
      3. Fallback: None (el negocio no tiene datos de afluencia)
    """
    try:
        # ── Intento 1: aria-label de las barras individuales ──────────────────
        # Google Maps renderiza las barras de cada hora con un atributo aria-label
        # que describe el nivel de ocupación en lenguaje natural.
        # Ejemplo: aria-label="Lunes: normalmente, no muy concurrido a las 9"
        # El contenedor de la sección Popular Times tiene clase 'ecGq2b' o similar.

        # Buscar todos los botones/divs con aria-label que contengan información horaria
        bar_selectors = [
            'div[aria-label*="concurrido"]',       # Castellano
            'div[aria-label*="busy"]',              # Inglés
            'div[aria-label*="concorregut"]',       # Catalán
            'button[aria-label*="concurrido"]',
            'button[aria-label*="busy"]',
        ]

        barras = []
        for sel in bar_selectors:
            try:
                found = await page.query_selector_all(sel)
                if found:
                    barras.extend(found)
                    break
            except Exception:
                pass

        if barras:
            # Parsear cada barra: "Lunes: normalmente, no muy concurrido a las 9"
            # o "Monday: usually not too busy at 9 AM"
            datos_raw: dict[str, dict[int, int]] = {}  # dia -> {hora: valor}

            for barra in barras:
                try:
                    aria = await barra.get_attribute("aria-label")
                    if not aria:
                        continue

                    # Extraer día
                    dia_raw = aria.split(":")[0].strip().lower() if ":" in aria else ""
                    dia = _DIAS_MAP.get(dia_raw)
                    if not dia:
                        continue

                    # Extraer hora: "a las 9", "a las 14", "at 9", "at 2 PM"
                    hora = None
                    m_hora = re.search(r'a las\s+(\d{1,2})', aria, re.IGNORECASE)
                    if m_hora:
                        hora = int(m_hora.group(1))
                    else:
                        m_hora = re.search(r'at\s+(\d{1,2})\s*(AM|PM)?', aria, re.IGNORECASE)
                        if m_hora:
                            hora = int(m_hora.group(1))
                            if m_hora.group(2) and m_hora.group(2).upper() == "PM" and hora != 12:
                                hora += 12
                            elif m_hora.group(2) and m_hora.group(2).upper() == "AM" and hora == 12:
                                hora = 0

                    if hora is None or not (0 <= hora <= 23):
                        continue

                    # Extraer valor de ocupación desde la descripción textual
                    valor = _valor_ocupacion(aria)

                    datos_raw.setdefault(dia, {})[hora] = valor

                except Exception:
                    continue

            if datos_raw:
                # Construir array de 24 horas por día (horas sin dato = 0)
                popular_times: dict[str, list[int]] = {}
                for dia, horas_dict in datos_raw.items():
                    vec = [horas_dict.get(h, 0) for h in range(24)]
                    # Solo incluir días que tengan al menos 1 hora con datos
                    if any(v > 0 for v in vec):
                        popular_times[dia] = vec

                if popular_times:
                    logger.debug(
                        "Popular Times extraído via aria-label: %d días",
                        len(popular_times),
                    )
                    return popular_times

        # ── Intento 2: buscar sección "Horas punta" por texto del DOM ─────────
        # Google Maps muestra "Cuándo suelen estar aquí las personas" o "Horas punta"
        # como título de la sección. Bajo él están los días como tabs y las barras.
        try:
            # Buscar contenedor de la sección por heading conocido
            headings = [
                'span:text("Horas punta")',
                'span:text("Cuándo suelen estar aquí")',
                'span:text("Popular times")',
                'h2:has-text("Horas")',
            ]
            contenedor = None
            for h_sel in headings:
                try:
                    el = await page.query_selector(h_sel)
                    if el:
                        # Subir al contenedor padre
                        contenedor = await el.evaluate("el => el.closest('[jsaction]')")
                        break
                except Exception:
                    pass

            if contenedor:
                # Intentar leer los botones de día como tabs
                tabs = await page.query_selector_all('[aria-selected][role="tab"]')
                if tabs:
                    popular_times = {}
                    for tab in tabs:
                        try:
                            tab_text = (await tab.inner_text()).strip().lower()
                            dia = _DIAS_MAP.get(tab_text)
                            if not dia:
                                continue

                            # Hacer click en el tab para activar ese día
                            await tab.click()
                            await asyncio.sleep(0.3)

                            # Leer barras activas
                            barras_dia = await page.query_selector_all(
                                'div[aria-label*="concurrido"], div[aria-label*="busy"]'
                            )
                            horas_dict: dict[int, int] = {}
                            for barra in barras_dia:
                                aria = await barra.get_attribute("aria-label") or ""
                                m_h = re.search(r'a las\s+(\d{1,2})', aria, re.IGNORECASE)
                                if not m_h:
                                    m_h = re.search(r'at\s+(\d{1,2})', aria, re.IGNORECASE)
                                if m_h:
                                    h = int(m_h.group(1))
                                    if 0 <= h <= 23:
                                        horas_dict[h] = _valor_ocupacion(aria)

                            if horas_dict:
                                popular_times[dia] = [horas_dict.get(h, 0) for h in range(24)]

                        except Exception:
                            continue

                    if popular_times:
                        logger.debug(
                            "Popular Times extraído via tabs: %d días",
                            len(popular_times),
                        )
                        return popular_times

        except Exception as e:
            logger.debug("Intento 2 Popular Times fallido: %s", e)

        # ── Fallback: no hay datos de Popular Times ────────────────────────────
        # Es normal: negocios nuevos, negocios con pocas visitas, o tiendas pequeñas
        # no tienen Popular Times en Google Maps.
        return None

    except Exception as e:
        logger.debug("Error extrayendo Popular Times: %s", e)
        return None


# ── Scraper principal ─────────────────────────────────────────────────────────

class GoogleMapsScraper:
    """
    Scraper de Google Maps para negocios por categoría y zona en Barcelona.

    Usa Playwright con capa stealth completa para evitar detección.
    Las sesiones se persisten en disco para reutilizar cookies entre ejecuciones.

    Ejemplo:
        scraper = GoogleMapsScraper()
        negocios = await scraper.buscar_negocios_por_categoria(
            categoria="peluquería",
            zona_nombre="Eixample",
            lat=41.3888, lng=2.1554,
            radio_m=400,
        )
    """

    def __init__(self, session_id: str = "default", max_retries: int = 3):
        self.session_id = session_id
        self.max_retries = max_retries
        self._rate_limit_until: float = 0.0   # timestamp hasta el que esperar por rate limit
        self._captcha_until: float = 0.0      # timestamp hasta el que esperar por CAPTCHA

    async def buscar_negocios_por_categoria(
        self,
        categoria: str,
        zona_nombre: str,
        lat: float,
        lng: float,
        radio_m: int = 300,
    ) -> list[NegocioScraped]:
        """
        Busca negocios en Google Maps por categoría en una zona de Barcelona.

        Args:
            categoria:   Tipo de negocio ("restaurante", "peluquería", etc.)
            zona_nombre: Nombre del barrio o zona ("Gràcia", "Eixample", etc.)
            lat, lng:    Coordenadas del centro de la zona
            radio_m:     Radio de búsqueda en metros

        Returns:
            Lista de NegocioScraped con los datos extraídos.
        """
        # Respetar penalizaciones activas
        ahora = time.time()
        if ahora < self._captcha_until:
            espera = self._captcha_until - ahora
            logger.warning("CAPTCHA activo — esperando %.0f minutos", espera / 60)
            await asyncio.sleep(espera)
        if ahora < self._rate_limit_until:
            espera = self._rate_limit_until - ahora
            logger.info("Rate limit activo — esperando %.0f segundos", espera)
            await asyncio.sleep(espera)

        query = f"{categoria} cerca de {zona_nombre}, Barcelona"
        logger.info("Google Maps: buscando '%s' (radio %dm)", query, radio_m)

        for intento in range(self.max_retries):
            try:
                resultados = await self._scrape_con_playwright(query, lat, lng, radio_m)
                if resultados is not None:
                    logger.info("Google Maps: %d negocios encontrados para '%s'", len(resultados), categoria)
                    return resultados
            except Exception as e:
                logger.warning("Google Maps intento %d/%d fallido: %s", intento + 1, self.max_retries, e)
                if intento < self.max_retries - 1:
                    espera = (2 ** intento) * _RATE_LIMIT_WAIT_BASE
                    logger.info("Backoff exponencial: esperando %.0f segundos", espera)
                    await asyncio.sleep(espera)

        logger.error("Google Maps: todos los intentos fallaron para '%s'", categoria)
        return []

    async def _scrape_con_playwright(
        self,
        query: str,
        lat: float,
        lng: float,
        radio_m: int,
    ) -> Optional[list[NegocioScraped]]:
        """Ejecuta el scraping completo con Playwright."""
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.error("Playwright no instalado. Instalar: pip install playwright && playwright install chromium")
            return []

        # Seleccionar UA y pantalla aleatoriamente (consistentes por sesión)
        ua = random.choice(_USER_AGENTS)
        screen_w, screen_h = random.choice(_SCREEN_RESOLUTIONS)
        viewport_w = random.randint(1280, min(1920, screen_w))
        viewport_h = random.randint(720, min(1080, screen_h))
        stealth_script = _build_stealth_script(ua, screen_w, screen_h)

        resultados: list[NegocioScraped] = []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=_HEADLESS,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--disable-dev-shm-usage",
                    "--no-sandbox",
                    "--disable-setuid-sandbox",
                    f"--window-size={viewport_w},{viewport_h}",
                    "--disable-extensions",
                    "--disable-plugins-discovery",
                    "--lang=es-ES",
                    "--accept-lang=es-ES,es,en",
                ],
            )

            ctx = await browser.new_context(
                user_agent=ua,
                viewport={"width": viewport_w, "height": viewport_h},
                screen={"width": screen_w, "height": screen_h},
                locale="es-ES",
                timezone_id="Europe/Madrid",
                geolocation={"latitude": lat, "longitude": lng},
                permissions=["geolocation"],
                extra_http_headers={
                    "Accept-Language": "es-ES,es;q=0.9,en;q=0.7",
                    "DNT": "1",
                    "Sec-CH-UA-Platform": '"Windows"' if "Windows" in ua else '"macOS"',
                    "Sec-CH-UA": '"Chromium";v="131", "Google Chrome";v="131", "Not=A?Brand";v="99"',
                    "Sec-CH-UA-Mobile": "?0",
                },
            )

            # Inyectar script stealth en TODAS las páginas
            await ctx.add_init_script(stealth_script)

            # Restaurar cookies de sesión anterior
            cookies_previas = _cargar_cookies(self.session_id)
            if cookies_previas:
                try:
                    await ctx.add_cookies(cookies_previas)
                    logger.debug("Cookies de sesión '%s' restauradas (%d)", self.session_id, len(cookies_previas))
                except Exception as e:
                    logger.debug("Error restaurando cookies: %s", e)

            page = await ctx.new_page()

            try:
                # ── Session warming ──────────────────────────────────────────────
                await self._session_warming(page)

                # ── Búsqueda real en Google Maps ─────────────────────────────────
                await self._ir_a_google_maps(page, query, lat, lng)

                # Comprobar CAPTCHA
                if await _detectar_captcha(page):
                    penalizacion = random.uniform(_CAPTCHA_WAIT_MIN, _CAPTCHA_WAIT_MAX)
                    self._captcha_until = time.time() + penalizacion
                    logger.warning(
                        "CAPTCHA detectado. Pausando %.0f minutos antes del siguiente intento.",
                        penalizacion / 60,
                    )
                    return None  # señal para backoff

                # Cerrar prompt de sign-in si aparece
                if await _detectar_signin_prompt(page):
                    logger.info("Detectado prompt Sign In — cerrando")
                    await _cerrar_signin_prompt(page)
                    await _delay_humano(1.0, 2.5)

                # ── Scroll y extracción de resultados ────────────────────────────
                resultados = await self._extraer_resultados(page, radio_m, lat, lng)

            finally:
                # Guardar cookies siempre
                try:
                    cookies_actuales = await ctx.cookies()
                    _guardar_cookies(self.session_id, cookies_actuales)
                except Exception:
                    pass
                await browser.close()

        return resultados

    async def _session_warming(self, page) -> None:
        """
        Visita google.com y realiza una búsqueda genérica para establecer historial
        de sesión natural antes de la búsqueda real.
        """
        try:
            logger.debug("Session warming: visitando google.com...")
            await page.goto("https://www.google.com/", wait_until="domcontentloaded", timeout=20000)
            await _delay_humano(2.0, 5.0)

            # Aceptar cookies si aparece el banner
            for sel in ['button[id="L2AGLb"]', 'button[id="W0wltc"]', '#acceptButton', 'button:has-text("Aceptar todo")', 'button:has-text("Accept all")']:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        await el.click()
                        await _delay_humano(0.8, 2.0)
                        break
                except Exception:
                    pass

            # Búsqueda genérica de calentamiento
            query_warmup = random.choice(_WARMUP_QUERIES)
            search_box = await page.query_selector('textarea[name="q"], input[name="q"]')
            if search_box:
                await _mover_raton_bezier(page, 640, 360)
                await search_box.click()
                await _delay_humano(0.3, 0.8)
                # Escribir como humano (letra a letra con variación)
                for char in query_warmup:
                    await search_box.type(char, delay=random.randint(50, 180))
                await _delay_humano(0.5, 1.5)
                await page.keyboard.press("Enter")
                await page.wait_for_load_state("domcontentloaded", timeout=10000)
                await _delay_humano(1.5, 4.0)

            logger.debug("Session warming completado")
        except Exception as e:
            logger.debug("Session warming parcialmente fallido (no crítico): %s", e)

    async def _ir_a_google_maps(self, page, query: str, lat: float, lng: float) -> None:
        """Navega a Google Maps y realiza la búsqueda indicada."""
        # Construir URL de búsqueda con geolocalización
        query_encoded = query.replace(" ", "+")
        maps_url = f"https://www.google.com/maps/search/{query_encoded}/@{lat},{lng},15z"

        logger.debug("Navegando a Google Maps: %s", maps_url)
        await page.goto(maps_url, wait_until="domcontentloaded", timeout=30000)
        await _delay_humano(2.0, 4.0)

        # Esperar a que cargue la lista de resultados
        try:
            await page.wait_for_selector(
                '[role="feed"], div[jsaction*="mouseover:pane"], .Nv2PK',
                timeout=15000,
            )
        except Exception:
            logger.debug("Selector de feed de resultados no encontrado — continuando")

        await _delay_humano(1.0, 2.5)

    async def _extraer_resultados(
        self,
        page,
        radio_m: int,
        lat_centro: float,
        lng_centro: float,
    ) -> list[NegocioScraped]:
        """
        Hace scroll en el panel de resultados de Google Maps para cargar todos
        los negocios disponibles (máx 60) y extrae sus datos.
        """
        resultados: list[NegocioScraped] = []
        ids_vistos: set[str] = set()
        sin_nuevos = 0

        # ── Primer scroll para activar la lista ─────────────────────────────────
        lista_selector = '[role="feed"]'
        try:
            lista = await page.query_selector(lista_selector)
            if lista:
                await page.evaluate("(el) => el.scrollTop = 0", lista)
            await _delay_humano(1.0, 2.0)
        except Exception:
            pass

        for scroll_intento in range(_SCROLL_ATTEMPTS_MAX):
            # Obtener todos los elementos de resultado visibles
            items = await page.query_selector_all('[role="feed"] > div > div[jsaction*="mouseover"]')
            if not items:
                # Fallback selector
                items = await page.query_selector_all('.Nv2PK')

            total_antes = len(resultados)

            for item in items:
                try:
                    # Identificar el elemento por su aria-label o texto visible
                    aria_label = await item.get_attribute("aria-label") or ""
                    item_key = aria_label[:60] if aria_label else f"idx_{items.index(item)}"

                    if item_key in ids_vistos:
                        continue
                    ids_vistos.add(item_key)

                    # Extraer datos básicos de la tarjeta sin hacer click
                    negocio = await self._extraer_tarjeta(page, item, lat_centro, lng_centro)
                    if negocio:
                        # Filtrar por radio si tenemos coordenadas
                        if negocio.lat and negocio.lng:
                            dist = _distancia_m(lat_centro, lng_centro, negocio.lat, negocio.lng)
                            if dist > radio_m * 2.5:  # margen generoso, filtro fino en postproceso
                                continue
                        resultados.append(negocio)

                    if len(resultados) >= _MAX_RESULTS_PER_SEARCH:
                        logger.info("Google Maps: alcanzado máximo de %d resultados", _MAX_RESULTS_PER_SEARCH)
                        return resultados

                except Exception as e:
                    logger.debug("Error procesando item de lista: %s", e)

            # Comprobar si hemos obtenido nuevos resultados
            if len(resultados) == total_antes:
                sin_nuevos += 1
                if sin_nuevos >= 3:
                    logger.info("Google Maps: sin nuevos resultados tras 3 scrolls consecutivos — fin")
                    break
            else:
                sin_nuevos = 0

            # Hacer scroll hacia abajo en el panel para cargar más
            try:
                lista = await page.query_selector('[role="feed"]')
                if lista:
                    await page.evaluate("(el) => el.scrollBy(0, 400)", lista)
                else:
                    await page.keyboard.press("End")
                await asyncio.sleep(random.uniform(_SCROLL_PAUSE_MIN, _SCROLL_PAUSE_MAX))
            except Exception:
                break

            # Comprobar "has-more" — si Google dice que no hay más
            try:
                fin_lista = await page.query_selector('div.PbZDve')
                if fin_lista:
                    fin_text = (await fin_lista.inner_text()).lower()
                    if "final" in fin_text or "no hay más" in fin_text:
                        logger.debug("Google Maps: fin de lista detectado")
                        break
            except Exception:
                pass

        logger.info("Google Maps: extracción completada, %d negocios", len(resultados))
        return resultados

    async def _extraer_popular_times(self, page) -> Optional[dict]:
        """
        Extrae el histograma de Popular Times de la ficha abierta en Google Maps.
        Delega a la función modular _extraer_popular_times del módulo.

        Devuelve dict {dia: [24 valores 0-100]} o None si no hay datos.
        No todos los negocios tienen Popular Times (negocios pequeños o sin suficiente
        historial de visitas no lo muestran).
        """
        return await _extraer_popular_times(page)

    async def _extraer_tarjeta(
        self,
        page,
        item,
        lat_centro: float,
        lng_centro: float,
    ) -> Optional[NegocioScraped]:
        """
        Extrae datos básicos de una tarjeta de resultado sin hacer click.
        Para datos completos (teléfono, horario, web) se necesita click en el panel.
        """
        try:
            aria_label = await item.get_attribute("aria-label") or ""
            nombre = aria_label.strip() if aria_label else None

            if not nombre:
                # Intentar desde el h3 interno
                h3 = await item.query_selector('div.fontHeadlineSmall, .qBF1Pd')
                if h3:
                    nombre = (await h3.inner_text()).strip()

            if not nombre:
                return None

            # Rating
            rating = None
            review_count = None
            try:
                rating_el = await item.query_selector('span.MW4etd')
                if rating_el:
                    rating = _parse_rating(await rating_el.inner_text())
                reviews_el = await item.query_selector('span.UY7F9')
                if reviews_el:
                    review_count = _parse_review_count(await reviews_el.inner_text())
            except Exception:
                pass

            # Precio nivel
            precio_nivel = None
            try:
                precio_el = await item.query_selector('span.e4rVHe')
                if precio_el:
                    precio_nivel = _parse_precio_nivel(await precio_el.inner_text())
            except Exception:
                pass

            # Dirección / categoría (texto secundario de la tarjeta)
            direccion = None
            categoria = None
            try:
                subtexts = await item.query_selector_all('div.W4Efsd > div.W4Efsd > span')
                textos = []
                for st in subtexts:
                    t = (await st.inner_text()).strip()
                    if t and t not in ("·", "•", ""):
                        textos.append(t)
                # Heurística: primer texto = categoría, segundo = dirección
                if len(textos) >= 1:
                    categoria = textos[0]
                if len(textos) >= 2:
                    direccion = textos[-1]
            except Exception:
                pass

            # URL de Maps (para extraer place_id)
            url_maps = None
            place_id = None
            try:
                link = await item.query_selector("a")
                if link:
                    href = await link.get_attribute("href")
                    if href:
                        url_maps = href if href.startswith("http") else f"https://www.google.com{href}"
                        place_id = _extraer_place_id_de_url(url_maps)
            except Exception:
                pass

            # Coordenadas desde la URL del ítem (si las tiene)
            lat, lng = None, None
            if url_maps:
                coord_match = re.search(r'@([-\d.]+),([-\d.]+)', url_maps)
                if coord_match:
                    try:
                        lat = float(coord_match.group(1))
                        lng = float(coord_match.group(2))
                    except ValueError:
                        pass

            return NegocioScraped(
                nombre=nombre,
                google_place_id=place_id,
                direccion=direccion,
                lat=lat,
                lng=lng,
                categoria=categoria,
                rating=rating,
                review_count=review_count,
                precio_nivel=precio_nivel,
                url_maps=url_maps,
            )

        except Exception as e:
            logger.debug("Error extrayendo tarjeta: %s", e)
            return None


# ── Utilidades geométricas ────────────────────────────────────────────────────

def _distancia_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Distancia Haversine en metros entre dos puntos."""
    R = 6371000  # radio de la Tierra en metros
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
