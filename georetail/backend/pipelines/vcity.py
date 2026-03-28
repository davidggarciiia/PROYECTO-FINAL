"""
pipelines/vcity.py — Flujo peatonal por tramo desde VCity BSC.

VCity (vcity.dataviz.bsc.es) muestra el promedio diario anual de peatones
por tramo de calle en Barcelona, datos del BSC (móvil + GPS).

Estado de la API (investigado marzo 2026):
  - VCity es una aplicación Next.js del Barcelona Supercomputing Center / BSC DataViz Group.
  - No hay API pública documentada. Los endpoints están compilados en chunks JS.
  - Se intentan varios endpoints candidatos deducidos del código fuente compilado.
  - Candidatos probados:
      /api/tramos                      → 404 (no existe)
      /api/pedestrian                  → 404 (no existe)
      /api/mobility/pedestrian         → 404 (no existe)
      /api/flujo-peatonal              → candidato
      /api/data/tramos                 → candidato
      /api/segments                    → candidato (nomenclatura GeoJSON estándar)
  - Si ningún endpoint responde, se usa Playwright para capturar las peticiones XHR
    que el browser realiza al navegar el mapa interactivo.
  - Fallback final: datos de vianants_bcn (Open Data BCN) como sustituto.

Frecuencia: mensual (día 12, 04:00) — los datos de VCity cambian anualmente.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import date
from typing import Optional

import httpx

from db.conexion import get_db

logger = logging.getLogger(__name__)

# ── Configuración VCity ────────────────────────────────────────────────────────
_VCITY_BASE   = "https://vcity.dataviz.bsc.es"
_TIMEOUT_S    = 30

# Candidatos de endpoint deducidos del código fuente compilado de Next.js.
# VCity usa tiles vectoriales o GeoJSON para el mapa de tramos.
# Se prueban en orden — el primero que devuelva 200 + JSON se usa.
_API_CANDIDATES = [
    "/api/tramos",
    "/api/pedestrian",
    "/api/mobility/pedestrian",
    "/api/flujo-peatonal",
    "/api/data/tramos",
    "/api/segments",
    "/api/v1/tramos",
    "/api/v1/pedestrian",
    "/api/mobility/tramos",
    # Tiles GeoJSON candidatos (bbox Barcelona):
    "/api/tiles/pedestrian/12/2048/1536.geojson",
    "/data/tramos.geojson",
    "/static/data/tramos.geojson",
]

# Headers que imitan un navegador real navegando VCity
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; GeoRetail/1.0; +https://georetail.app)",
    "Accept": "application/json, application/geo+json, */*",
    "Referer": "https://vcity.dataviz.bsc.es/",
    "Origin": "https://vcity.dataviz.bsc.es",
}

# Parámetros de asignación espacial
_RADIO_M   = 100   # Radio ST_DWithin para asignar tramos a zonas (metros)
_MAX_ZONAS = 5     # Máximo de zonas que puede influir un tramo

# Directorio CSV local (fallback)
_CSV_DIR = os.environ.get("CSV_DIR", "/data/csv")


# ═══════════════════════════════════════════════════════════════════════════════
# Punto de entrada público
# ═══════════════════════════════════════════════════════════════════════════════

async def ejecutar() -> dict:
    """
    Pipeline completo:
      1. Intentar API directa (varios endpoints candidatos)
      2. Si falla → Playwright (captura XHR del mapa interactivo)
      3. Si falla → fallback vianants_bcn (datos Open Data BCN ya en BD)
      4. Mapear tramos → zonas (ST_DWithin 100m)
      5. Persistir en variables_zona.vcity_flujo_peatonal
    """
    eid = await _init()
    fuente_usada = "ninguna"
    try:
        # 1. API directa
        tramos = await _fetch_tramos_api()
        if tramos:
            fuente_usada = "vcity_api"
            logger.info("VCity API directa: %d tramos obtenidos", len(tramos))
        else:
            # 2. Playwright
            logger.info("API directa sin resultados — intentando Playwright...")
            tramos = await _fetch_tramos_playwright()
            if tramos:
                fuente_usada = "vcity_playwright"
                logger.info("VCity Playwright: %d tramos capturados", len(tramos))

        if not tramos:
            # 3. Fallback: usar datos vianants_bcn ya almacenados en BD
            logger.warning(
                "VCity no disponible (API ni Playwright). "
                "Propagando fuente vianants_bcn → vcity_flujo_peatonal como fallback."
            )
            n_fallback = await _fallback_desde_vianants()
            await _fin(eid, n_fallback, "ok",
                       "VCity no disponible — fallback vianants_bcn usado")
            return {
                "tramos": 0,
                "zonas_actualizadas": n_fallback,
                "fuente": "fallback_vianants_bcn",
                "vcity_api_disponible": False,
            }

        # 4. Mapear tramos → zonas
        zona_flows = await _mapear_zonas(tramos)
        logger.info("Zonas con flujo VCity asignado: %d", len(zona_flows))

        # 5. Persistir
        n_persistidos = await _persistir(zona_flows)

        await _fin(eid, n_persistidos, "ok")
        logger.info(
            "VCity OK — %d tramos, %d zonas actualizadas, fuente=%s",
            len(tramos), n_persistidos, fuente_usada,
        )
        return {
            "tramos": len(tramos),
            "zonas_actualizadas": n_persistidos,
            "fuente": fuente_usada,
            "vcity_api_disponible": fuente_usada == "vcity_api",
        }

    except Exception as exc:
        logger.error("Pipeline vcity ERROR: %s", exc, exc_info=True)
        await _fin(eid, 0, "error", str(exc))
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Intento API directa
# ═══════════════════════════════════════════════════════════════════════════════

async def _fetch_tramos_api() -> list[dict]:
    """
    Intenta obtener los tramos de peatones desde los endpoints candidatos de VCity.

    Devuelve lista de dicts con campos normalizados:
      id_tram, lat, lng, intensitat (promedio diario peatones)

    Si ningún endpoint responde con datos válidos, devuelve lista vacía.

    Nota sobre la API:
      VCity no tiene API pública documentada. Los endpoints son internos del
      servidor Next.js y pueden cambiar con cada deploy. La estrategia es
      probar los candidatos deducidos del código fuente compilado.
      Si el BSC publica la API en el futuro, actualizar _API_CANDIDATES.
    """
    tramos: list[dict] = []

    async with httpx.AsyncClient(
        timeout=_TIMEOUT_S,
        headers=_HEADERS,
        follow_redirects=True,
    ) as client:
        for path in _API_CANDIDATES:
            url = f"{_VCITY_BASE}{path}"
            try:
                resp = await client.get(url)
                logger.debug("VCity probe %s → HTTP %d", path, resp.status_code)

                if resp.status_code != 200:
                    continue

                # Intentar parsear como JSON / GeoJSON
                try:
                    data = resp.json()
                except Exception:
                    logger.debug("VCity %s: respuesta no es JSON", path)
                    continue

                parsed = _parsear_respuesta_vcity(data, path)
                if parsed:
                    logger.info(
                        "VCity endpoint encontrado: %s → %d tramos", path, len(parsed)
                    )
                    return parsed

            except httpx.TimeoutException:
                logger.debug("VCity probe %s: timeout", path)
            except httpx.ConnectError:
                logger.debug("VCity probe %s: connection error", path)
            except Exception as exc:
                logger.debug("VCity probe %s: %s", path, exc)

        # Intentar también cargar el index de la app para descubrir el chunk
        # con la URL real de la API (heurística JS parsing)
        discovered = await _descubrir_endpoint_desde_html(client)
        if discovered:
            try:
                resp = await client.get(discovered)
                if resp.status_code == 200:
                    data = resp.json()
                    parsed = _parsear_respuesta_vcity(data, discovered)
                    if parsed:
                        logger.info(
                            "VCity endpoint descubierto dinámicamente: %s → %d tramos",
                            discovered, len(parsed)
                        )
                        return parsed
            except Exception as exc:
                logger.debug("VCity endpoint descubierto falló: %s", exc)

    return tramos


async def _descubrir_endpoint_desde_html(client: httpx.AsyncClient) -> Optional[str]:
    """
    Descarga el HTML de VCity y busca en los chunks JS referencias a la API.
    Estrategia heurística: buscar patrones "/api/" en el contenido de los chunks.

    Retorna la primera URL de API con "tramo" o "pedestrian" encontrada, o None.
    """
    try:
        resp = await client.get(f"{_VCITY_BASE}/")
        if resp.status_code != 200:
            return None

        html = resp.text

        # Buscar referencias a chunks JS en el HTML
        import re
        chunk_refs = re.findall(
            r'"([^"]*/_next/static/chunks/[^"]+\.js)"',
            html,
        )
        # Normalizar URLs relativas
        chunk_urls = []
        for ref in chunk_refs[:10]:  # limitar a 10 chunks
            if ref.startswith("http"):
                chunk_urls.append(ref)
            else:
                chunk_urls.append(f"{_VCITY_BASE}{ref}")

        # Buscar también en manifest JSON
        manifest_urls = re.findall(r'"(/[^"]*/_next/[^"]+\.js)"', html)
        for ref in manifest_urls[:5]:
            chunk_urls.append(f"{_VCITY_BASE}{ref}")

        # Inspeccionar cada chunk buscando rutas de API
        for chunk_url in chunk_urls[:8]:
            try:
                cr = await client.get(chunk_url)
                if cr.status_code != 200:
                    continue
                js_text = cr.text[:50000]  # solo primeros 50KB
                # Buscar strings que parezcan endpoints de tramos/peatones
                api_matches = re.findall(
                    r'["\'](/api/[a-zA-Z0-9/_-]*(?:tram|pedestrian|peatonal|flujo|segment)[a-zA-Z0-9/_-]*)["\']',
                    js_text,
                )
                if api_matches:
                    endpoint = api_matches[0]
                    logger.info(
                        "VCity endpoint descubierto en chunk JS: %s", endpoint
                    )
                    return f"{_VCITY_BASE}{endpoint}"
            except Exception:
                continue

    except Exception as exc:
        logger.debug("Error descubriendo endpoint VCity desde HTML: %s", exc)

    return None


def _parsear_respuesta_vcity(data: object, source_path: str) -> list[dict]:
    """
    Parsea la respuesta de la API VCity (JSON o GeoJSON) a lista normalizada.

    Formatos posibles que puede devolver VCity:
      A. GeoJSON FeatureCollection con features LineString/Point
      B. Array de objetos con id/lat/lng/count
      C. Dict con clave "tramos", "data", "features", "segments", etc.

    Devuelve lista de dicts con:
      id_tram, lat, lng, intensitat
    """
    tramos: list[dict] = []

    # Formato A: GeoJSON FeatureCollection
    if isinstance(data, dict) and data.get("type") == "FeatureCollection":
        features = data.get("features", [])
        for feat in features:
            props = feat.get("properties") or {}
            geom  = feat.get("geometry") or {}
            coords = geom.get("coordinates", [])

            intensitat = _extraer_intensitat(props)
            if intensitat is None or intensitat <= 0:
                continue

            lat, lng = _coords_centro(geom, coords)
            if lat is None:
                continue

            tramos.append({
                "id_tram":   str(props.get("id") or props.get("tram_id") or props.get("segment_id") or f"feat_{len(tramos)}"),
                "lat":       lat,
                "lng":       lng,
                "intensitat": intensitat,
            })
        return tramos

    # Formato B: Array directo
    if isinstance(data, list):
        for item in data:
            if not isinstance(item, dict):
                continue
            intensitat = _extraer_intensitat(item)
            if intensitat is None or intensitat <= 0:
                continue
            lat  = _coerce_float(item.get("lat") or item.get("latitude") or item.get("y"))
            lng  = _coerce_float(item.get("lng") or item.get("lon") or item.get("longitude") or item.get("x"))
            if lat is None or lng is None:
                continue
            if not (41.0 < lat < 41.8 and 1.8 < lng < 2.5):
                continue
            tramos.append({
                "id_tram":    str(item.get("id") or item.get("tram_id") or f"item_{len(tramos)}"),
                "lat":        lat,
                "lng":        lng,
                "intensitat": intensitat,
            })
        return tramos

    # Formato C: Dict con clave contenedora
    if isinstance(data, dict):
        for key in ("tramos", "data", "features", "segments", "items", "results", "pedestrian"):
            sub = data.get(key)
            if sub:
                parsed = _parsear_respuesta_vcity(sub, source_path)
                if parsed:
                    return parsed

    return tramos


def _extraer_intensitat(props: dict) -> Optional[float]:
    """
    Extrae el valor de intensidad/conteo de peatones de un dict de propiedades.
    Prueba varios nombres de campo que VCity podría usar.
    """
    for key in (
        "intensitat", "intensidad", "count", "valor", "imd",
        "pedestrians", "pedestrian_count", "avg_daily", "promedio_diario",
        "flow", "flujo", "volume", "volumen",
        "daily_avg", "daily_count", "mean_daily",
    ):
        v = props.get(key)
        if v is not None:
            f = _coerce_float(v)
            if f is not None and f > 0:
                return f
    return None


def _coords_centro(geom: dict, coords: list) -> tuple[Optional[float], Optional[float]]:
    """
    Extrae coordenadas del centro de una geometría GeoJSON.
    Para LineString: promedio de todos los puntos.
    Para Point: directamente las coordenadas.
    """
    gtype = geom.get("type", "")
    if gtype == "Point" and len(coords) >= 2:
        lng, lat = coords[0], coords[1]
        if isinstance(lat, (int, float)) and isinstance(lng, (int, float)):
            if 41.0 < lat < 41.8 and 1.8 < lng < 2.5:
                return float(lat), float(lng)
    elif gtype == "LineString" and coords:
        lngs = [c[0] for c in coords if len(c) >= 2]
        lats = [c[1] for c in coords if len(c) >= 2]
        if lats and lngs:
            lat = sum(lats) / len(lats)
            lng = sum(lngs) / len(lngs)
            if 41.0 < lat < 41.8 and 1.8 < lng < 2.5:
                return float(lat), float(lng)
    elif gtype == "MultiLineString" and coords:
        all_lats, all_lngs = [], []
        for segment in coords:
            for c in segment:
                if len(c) >= 2:
                    all_lngs.append(c[0])
                    all_lats.append(c[1])
        if all_lats:
            lat = sum(all_lats) / len(all_lats)
            lng = sum(all_lngs) / len(all_lngs)
            if 41.0 < lat < 41.8 and 1.8 < lng < 2.5:
                return float(lat), float(lng)
    return None, None


def _coerce_float(v: object) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(str(v).replace(",", ".").strip())
    except (ValueError, TypeError):
        return None


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Fallback Playwright — captura XHR mientras navega el mapa
# ═══════════════════════════════════════════════════════════════════════════════

async def _fetch_tramos_playwright() -> list[dict]:
    """
    Usa Playwright para abrir VCity en un browser headless, navegar el mapa
    de Barcelona y capturar las peticiones XHR/fetch que realiza la app.

    Requiere: pip install playwright + playwright install chromium

    Estrategia:
      1. Interceptar todas las respuestas de red
      2. Filtrar las que contengan "tram", "pedestrian", "flujo" en la URL
         o devuelvan GeoJSON/JSON con estructura de tramos
      3. Parsear la primera respuesta válida

    Devuelve lista normalizada o [] si Playwright no está disponible o no
    encuentra datos válidos.
    """
    try:
        from playwright.async_api import async_playwright, Response as PWResponse
    except ImportError:
        logger.info(
            "Playwright no instalado — skipping _fetch_tramos_playwright. "
            "Instalar con: pip install playwright && playwright install chromium"
        )
        return []

    tramos: list[dict] = []
    captured: list[dict] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent="Mozilla/5.0 (compatible; GeoRetail/1.0)",
        )
        page = await context.new_page()

        # Interceptar respuestas de red
        async def on_response(response: PWResponse) -> None:
            url = response.url
            # Filtrar por URL o Content-Type relevantes
            ct = response.headers.get("content-type", "")
            url_lower = url.lower()
            is_relevant = any(kw in url_lower for kw in [
                "tram", "pedestrian", "peatonal", "flujo", "segment",
                "mobility", "geojson", "vianant",
            ])
            is_json = "json" in ct or "geojson" in ct
            if is_relevant or (is_json and "vcity" in url_lower):
                try:
                    body = await response.json()
                    captured.append({"url": url, "data": body})
                    logger.debug("VCity Playwright capturó: %s", url)
                except Exception:
                    pass

        page.on("response", on_response)

        try:
            # Abrir VCity y esperar a que cargue el mapa
            await page.goto(
                f"{_VCITY_BASE}/",
                wait_until="networkidle",
                timeout=30000,
            )
            # Esperar un poco más para peticiones lazy
            await asyncio.sleep(3)

            # Intentar hacer zoom/pan para forzar la carga de tiles/datos
            # Centrar en Barcelona (lat 41.3887, lng 2.1777)
            try:
                await page.evaluate("""
                    () => {
                        // Intentar disparar eventos de mapa para cargar datos
                        window.dispatchEvent(new Event('resize'));
                    }
                """)
                await asyncio.sleep(2)
            except Exception:
                pass

        except Exception as exc:
            logger.warning("VCity Playwright: error navegando: %s", exc)
        finally:
            await browser.close()

    # Procesar las respuestas capturadas
    for cap in captured:
        parsed = _parsear_respuesta_vcity(cap["data"], cap["url"])
        if parsed:
            logger.info(
                "VCity Playwright: %d tramos desde %s", len(parsed), cap["url"]
            )
            tramos.extend(parsed)

    # Deduplicar por id_tram
    seen: set[str] = set()
    dedup: list[dict] = []
    for t in tramos:
        if t["id_tram"] not in seen:
            seen.add(t["id_tram"])
            dedup.append(t)

    return dedup


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Fallback: propagar datos vianants_bcn → vcity_flujo_peatonal
# ═══════════════════════════════════════════════════════════════════════════════

async def _fallback_desde_vianants() -> int:
    """
    Cuando VCity no está disponible, copia el valor flujo_peatonal_total de
    variables_zona (calculado por vianants.py) a la columna vcity_flujo_peatonal.

    Esto mantiene la columna actualizada con la mejor estimación disponible
    hasta que VCity esté accesible.

    Devuelve el número de filas actualizadas.
    """
    async with get_db() as conn:
        n = await conn.fetchval(
            """
            UPDATE variables_zona
            SET vcity_flujo_peatonal = flujo_peatonal_total
            WHERE flujo_peatonal_total IS NOT NULL
              AND flujo_peatonal_total > 0
            RETURNING COUNT(*)
            """
        )
        # fetchval devuelve None si RETURNING COUNT(*) no tiene filas
        if n is None:
            n = await conn.fetchval(
                """
                SELECT COUNT(*) FROM variables_zona
                WHERE flujo_peatonal_total IS NOT NULL
                  AND flujo_peatonal_total > 0
                """
            )
            await conn.execute(
                """
                UPDATE variables_zona
                SET vcity_flujo_peatonal = flujo_peatonal_total
                WHERE flujo_peatonal_total IS NOT NULL
                  AND flujo_peatonal_total > 0
                """
            )
        logger.info("Fallback vianants_bcn → vcity_flujo_peatonal: %d zonas", n or 0)
        return int(n or 0)


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Asignación espacial: tramos → zonas
# ═══════════════════════════════════════════════════════════════════════════════

async def _mapear_zonas(tramos: list[dict]) -> dict[str, float]:
    """
    Para cada tramo con coordenadas, asigna su intensidad peatonal a las zonas
    dentro de un radio de _RADIO_M (100m) usando ST_DWithin + ponderación por
    distancia inversa al cuadrado.

    Retorna dict {zona_id: intensidad_total} con la suma ponderada por zona.

    Lógica:
      - Radio 100m (más pequeño que vianants.py/150m porque los tramos VCity
        son segmentos de calle más precisos que los puntos de contador)
      - peso = 1 / max(1, distancia)²
      - intensidad_zona += intensitat_tram * (peso / suma_pesos)
      - Multiplicador comercial: ×(1.0–1.35) según ratio_locales_comerciales
    """
    zona_flows: dict[str, float] = {}
    tramos_con_coords = [t for t in tramos if t.get("lat") and t.get("lng")]

    if not tramos_con_coords:
        logger.warning("VCity: ningún tramo con coordenadas válidas")
        return zona_flows

    async with get_db() as conn:
        for tram in tramos_con_coords:
            try:
                zonas = await conn.fetch(
                    """
                    SELECT
                        z.id AS zona_id,
                        ST_Distance(
                            z.geometria::geography,
                            ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
                        ) AS distancia_m,
                        COALESCE(vz.ratio_locales_comerciales, 0.0) AS ratio_comercial
                    FROM zonas z
                    LEFT JOIN LATERAL (
                        SELECT ratio_locales_comerciales
                        FROM variables_zona
                        WHERE zona_id = z.id
                        ORDER BY fecha DESC LIMIT 1
                    ) vz ON TRUE
                    WHERE ST_DWithin(
                        z.geometria::geography,
                        ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
                        $3
                    )
                    ORDER BY distancia_m ASC
                    LIMIT $4
                    """,
                    tram["lng"], tram["lat"], _RADIO_M, _MAX_ZONAS,
                )

                if not zonas:
                    continue

                pesos      = [1.0 / max(1.0, float(z["distancia_m"])) ** 2 for z in zonas]
                suma_pesos = sum(pesos) or 1.0

                for zona, peso in zip(zonas, pesos):
                    fraccion      = peso / suma_pesos
                    flujo         = tram["intensitat"] * fraccion
                    ratio_com     = float(zona["ratio_comercial"] or 0.0)
                    multiplicador = 1.0 + min(0.35, ratio_com * 0.70)
                    flujo        *= multiplicador

                    zid = str(zona["zona_id"])
                    zona_flows[zid] = zona_flows.get(zid, 0.0) + flujo

            except Exception as exc:
                logger.warning(
                    "VCity: error procesando tram %s: %s", tram.get("id_tram"), exc
                )

    logger.info(
        "VCity: %d tramos asignados a %d zonas (radio %dm)",
        len(tramos_con_coords), len(zona_flows), _RADIO_M,
    )
    return zona_flows


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Persistencia
# ═══════════════════════════════════════════════════════════════════════════════

async def _persistir(zona_flows: dict[str, float]) -> int:
    """
    Guarda los flujos peatonales VCity en variables_zona.vcity_flujo_peatonal.

    Usa UPSERT sobre (zona_id, fecha) para actualizar solo la columna
    vcity_flujo_peatonal sin tocar las demás columnas.

    Devuelve el número de zonas actualizadas.
    """
    if not zona_flows:
        return 0

    fecha = date.today()
    n = 0

    async with get_db() as conn:
        for zona_id, flujo in zona_flows.items():
            try:
                await conn.execute(
                    """
                    INSERT INTO variables_zona (zona_id, fecha, vcity_flujo_peatonal)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (zona_id, fecha) DO UPDATE
                    SET vcity_flujo_peatonal = EXCLUDED.vcity_flujo_peatonal
                    """,
                    zona_id, fecha, round(flujo, 2),
                )
                n += 1
            except Exception as exc:
                logger.debug(
                    "VCity: error persistiendo zona %s: %s", zona_id, exc
                )

    logger.info("VCity: %d zonas persistidas con vcity_flujo_peatonal", n)
    return n


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers BD — patrón estándar pipeline_ejecuciones
# ═══════════════════════════════════════════════════════════════════════════════

async def _init() -> int:
    """Registra inicio de ejecución en pipeline_ejecuciones. Devuelve eid."""
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones (pipeline, estado) "
            "VALUES ('vcity', 'running') RETURNING id"
        )


async def _fin(
    eid: int,
    registros: int,
    estado: str,
    mensaje: Optional[str] = None,
) -> None:
    """Registra fin de ejecución en pipeline_ejecuciones."""
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones "
            "SET fecha_fin=NOW(), registros=$1, estado=$2, mensaje_error=$3 "
            "WHERE id=$4",
            registros, estado, mensaje, eid,
        )
