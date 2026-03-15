"""
pipelines/carga_inicial.py — Carga inicial completa de datos desde Open Data BCN.

Ejecutar UNA VEZ al instalar el sistema para poblar la BD con datos reales.

  cd backend
  python -m pipelines.carga_inicial

  # O con Docker (después de docker-compose up -d):
  docker-compose exec backend python -m pipelines.carga_inicial

Qué hace (en orden):
  1. Descarga 10 distritos + 73 barrios de Barcelona (GeoJSON Open Data BCN)
  2. Crea ~800 zonas de análisis subdividiendo los barrios (~300×300m)
  3. Flujo peatonal   → aforaments (sensores Ajuntament)
  4. Demografía       → padrón municipal + renta media (Open Data BCN)
  5. Precios alquiler → Open Data BCN + scrapers portales inmobiliarios
  6. Transporte       → TMB metro/bus (opcional, requiere TMB_APP_ID)

Tiempo estimado: 5-20 minutos según velocidad de la conexión y estado de las APIs.

Datasets Open Data BCN usados:
  - 20170706-districtes-barris  → polígonos GeoJSON de distritos y barrios
  - renda-disponible-llars-bcn  → renta media por hogar (pipeline demografía)
  - pad_mdbas                   → padrón municipal (pipeline demografía)
  - locllo-evolucio             → evolución precios alquiler (pipeline precios)
  - aforaments-detall           → flujo peatonal por sensor (pipeline aforaments)
"""
from __future__ import annotations

import asyncio
import json
import logging
import sys
from typing import Optional

import httpx
from shapely.geometry import shape
from shapely.validation import make_valid

from db.conexion import get_db, init_db_pool, close_db_pool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

_CKAN     = "https://opendata-ajuntament.barcelona.cat/data"
_TIMEOUT  = 60.0


# =============================================================================
# PASO 1 — Geografía: distritos, barrios y zonas
# =============================================================================

async def cargar_distritos(client: httpx.AsyncClient) -> int:
    """
    Carga los 10 distritos de Barcelona desde Open Data BCN.
    Intenta el CKAN API; si falla, usa polígonos hardcoded.
    """
    logger.info("  Descargando distritos desde Open Data BCN...")
    geojson = await _descargar_geojson_ckan(
        client,
        package_id="20170706-districtes-barris",
        keyword_geojson="districte",
    )
    if not geojson:
        logger.warning("  API no disponible — usando datos hardcoded para distritos")
        geojson = _distritos_hardcoded()

    features = geojson.get("features", [])
    n = 0
    async with get_db() as conn:
        for feat in features:
            props = feat.get("properties") or {}
            geom  = feat.get("geometry")
            if not geom:
                continue

            codigo = _prop(props, ["DISTRICTE", "CODI_DISTRICTE", "codi_districte", "CODIDIS"])
            nombre = _prop(props, ["NOM_DISTRICTE", "NOM", "nom_districte", "NOMDIS"])
            if not codigo or not nombre:
                continue

            codigo = str(int(float(codigo))).zfill(2)

            try:
                geom_shape = make_valid(shape(geom))
                wkt = geom_shape.wkt
            except Exception as exc:
                logger.warning("    Geometría inválida distrito '%s': %s", nombre, exc)
                continue

            await conn.execute("""
                INSERT INTO distritos (codigo, nombre, geometria)
                VALUES ($1, $2,
                    ST_Multi(
                        ST_GeomFromText($3, 4326)
                    )::GEOMETRY(MULTIPOLYGON, 4326)
                )
                ON CONFLICT (codigo) DO UPDATE
                SET nombre    = EXCLUDED.nombre,
                    geometria = EXCLUDED.geometria
            """, codigo, nombre, wkt)
            n += 1

    logger.info("  ✅ %d distritos cargados", n)
    return n


async def cargar_barrios(client: httpx.AsyncClient) -> int:
    """
    Carga los 73 barrios de Barcelona desde Open Data BCN.
    Intenta el CKAN API; si falla, usa polígonos hardcoded.
    """
    logger.info("  Descargando barrios desde Open Data BCN...")
    geojson = await _descargar_geojson_ckan(
        client,
        package_id="20170706-districtes-barris",
        keyword_geojson="barri",
    )
    if not geojson:
        logger.warning("  API no disponible — usando datos hardcoded para barrios")
        geojson = _barrios_hardcoded()

    features = geojson.get("features", [])
    n = 0
    async with get_db() as conn:
        dist_rows = await conn.fetch("SELECT id, codigo FROM distritos")
        dist_map  = {r["codigo"]: r["id"] for r in dist_rows}

        for feat in features:
            props = feat.get("properties") or {}
            geom  = feat.get("geometry")
            if not geom:
                continue

            c_barri = _prop(props, ["BARRI", "CODI_BARRI", "codi_barri", "CODIBARRI"])
            nombre  = _prop(props, ["NOM_BARRI",  "NOM",  "nom_barri",  "NOMBARRI"])
            c_dist  = _prop(props, ["DISTRICTE", "CODI_DISTRICTE", "codi_districte"])
            if not c_barri or not nombre:
                continue

            c_barri = str(int(float(c_barri))).zfill(6)
            c_dist  = str(int(float(c_dist))).zfill(2) if c_dist else None
            dist_id = dist_map.get(c_dist) if c_dist else None

            try:
                geom_shape = make_valid(shape(geom))
                wkt = geom_shape.wkt
            except Exception as exc:
                logger.warning("    Geometría inválida barrio '%s': %s", nombre, exc)
                continue

            await conn.execute("""
                INSERT INTO barrios (codigo, nombre, distrito_id, geometria)
                VALUES ($1, $2, $3,
                    ST_Multi(
                        ST_GeomFromText($4, 4326)
                    )::GEOMETRY(MULTIPOLYGON, 4326)
                )
                ON CONFLICT (codigo) DO UPDATE
                SET nombre      = EXCLUDED.nombre,
                    distrito_id = EXCLUDED.distrito_id,
                    geometria   = EXCLUDED.geometria
            """, c_barri, nombre, dist_id, wkt)
            n += 1

    logger.info("  ✅ %d barrios cargados", n)
    return n


async def crear_zonas() -> int:
    """
    Subdivide cada barrio en celdas ~300×300m con PostGIS ST_SquareGrid.
    ID de zona: bcn_{barrio_codigo_6d}_{seq_2d}  → máx 13 chars (cabe en VARCHAR(20))
    """
    logger.info("  Generando zonas (rejilla ~300m por barrio)...")

    async with get_db() as conn:
        n_barrios = await conn.fetchval("SELECT COUNT(*) FROM barrios")
        if n_barrios == 0:
            logger.error("  No hay barrios — no se pueden crear zonas")
            return 0

        # Borrar zonas anteriores para regenerar limpio
        await conn.execute("DELETE FROM zonas")

        # ST_SquareGrid(0.003°) ≈ 280-300m en latitud 41° (Barcelona)
        # ST_Intersection recorta la celda al polígono del barrio para
        # no crear zonas que sobresalgan fuera del límite administrativo.
        # La condición area_m2 > 15000 elimina los triángulos residuales
        # que aparecen en los bordes del barrio.
        inserted = await conn.fetchval("""
            WITH grid AS (
                SELECT
                    b.id        AS barrio_id,
                    b.codigo    AS barrio_codigo,
                    b.nombre    AS barrio_nombre,
                    ROW_NUMBER() OVER (
                        PARTITION BY b.id
                        ORDER BY
                            ST_X(ST_Centroid(g.geom)),
                            ST_Y(ST_Centroid(g.geom))
                    )           AS seq,
                    ST_Intersection(g.geom, b.geometria) AS geom_zona
                FROM barrios b
                CROSS JOIN LATERAL ST_SquareGrid(0.003, b.geometria) AS g(geom)
                WHERE ST_Intersects(g.geom, b.geometria)
                  AND ST_Area(
                        ST_Intersection(g.geom, b.geometria)::geography
                      ) > 15000
            ),
            ins AS (
                INSERT INTO zonas (id, nombre, barrio_id, geometria, area_m2)
                SELECT
                    'bcn_' || barrio_codigo || '_' || LPAD(seq::TEXT, 2, '0'),
                    barrio_nombre || ' — Zona ' || seq,
                    barrio_id,
                    geom_zona,
                    ST_Area(geom_zona::geography)
                FROM grid
                ON CONFLICT (id) DO NOTHING
                RETURNING id
            )
            SELECT COUNT(*) FROM ins
        """)

        await conn.execute("REINDEX INDEX idx_zonas_geometria")

    n = int(inserted or 0)
    logger.info("  ✅ %d zonas creadas", n)
    return n


# =============================================================================
# PASO 2 — Pipelines de datos (usan las zonas ya cargadas)
# =============================================================================

async def _run(nombre: str) -> dict:
    """Ejecuta un pipeline de datos y devuelve su resultado."""
    logger.info("  Ejecutando pipeline '%s'...", nombre)
    try:
        if nombre == "aforaments":
            from pipelines.aforaments import ejecutar
        elif nombre == "demografia":
            from pipelines.demografia import ejecutar
        elif nombre == "precios":
            from pipelines.precios import ejecutar
        elif nombre == "transporte":
            from pipelines.transporte import ejecutar
        else:
            raise ValueError(f"Pipeline desconocido: {nombre}")

        resultado = await ejecutar()
        logger.info("  ✅ %s → %s", nombre, resultado)
        return resultado
    except Exception as exc:
        logger.error("  ❌ %s falló: %s", nombre, exc)
        return {"error": str(exc)}


# =============================================================================
# MAIN
# =============================================================================

async def main() -> None:
    logger.info("=" * 60)
    logger.info("  GeoRetail — Carga inicial de datos")
    logger.info("  Fuente: Open Data Barcelona (CKAN)")
    logger.info("=" * 60)

    await init_db_pool()

    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=True) as client:

            # ── 1. Geografía ───────────────────────────────────────────────
            logger.info("\n[1/5] Geografía de Barcelona")
            n_dist    = await cargar_distritos(client)
            n_barrios = await cargar_barrios(client)

        if n_dist == 0 or n_barrios == 0:
            logger.error("No se cargaron distritos/barrios. Abortando.")
            sys.exit(1)

        n_zonas = await crear_zonas()
        logger.info("      %d distritos | %d barrios | %d zonas", n_dist, n_barrios, n_zonas)

        # ── 2. Flujo peatonal ──────────────────────────────────────────────
        logger.info("\n[2/5] Flujo peatonal (aforaments)")
        await _run("aforaments")

        # ── 3. Demografía ──────────────────────────────────────────────────
        logger.info("\n[3/5] Demografía (padrón + renta)")
        await _run("demografia")

        # ── 4. Precios de alquiler ─────────────────────────────────────────
        logger.info("\n[4/5] Precios de alquiler")
        await _run("precios")

        # ── 5. Transporte (opcional — requiere TMB_APP_ID) ─────────────────
        logger.info("\n[5/5] Transporte público (TMB)")
        await _run("transporte")

        # ── Resumen ────────────────────────────────────────────────────────
        async with get_db() as conn:
            n_z   = await conn.fetchval("SELECT COUNT(*) FROM zonas")
            n_vz  = await conn.fetchval("SELECT COUNT(*) FROM variables_zona")
            n_loc = await conn.fetchval("SELECT COUNT(*) FROM locales")
            n_pr  = await conn.fetchval("SELECT COUNT(*) FROM precios_alquiler_zona")

        logger.info("\n" + "=" * 60)
        logger.info("  ✅ Carga inicial completada")
        logger.info("     Zonas:           %d", n_z)
        logger.info("     Variables zona:  %d", n_vz)
        logger.info("     Locales:         %d", n_loc)
        logger.info("     Precios:         %d", n_pr)
        logger.info("=" * 60)
        logger.info("\nPróximo paso — arrancar el backend:")
        logger.info("  docker-compose up  (si no está arrancado ya)")
        logger.info("  → http://localhost:3000")

    finally:
        await close_db_pool()


# =============================================================================
# HELPERS — CKAN / GeoJSON
# =============================================================================

async def _descargar_geojson_ckan(
    client: httpx.AsyncClient,
    package_id: str,
    keyword_geojson: str,
) -> Optional[dict]:
    """
    Busca el recurso GeoJSON en un dataset CKAN y lo descarga.
    Busca primero por formato 'GEOJSON' y keyword en el nombre del recurso.
    """
    try:
        r = await client.get(
            f"{_CKAN}/api/action/package_show",
            params={"id": package_id},
            timeout=20.0,
        )
        r.raise_for_status()
        resources = r.json().get("result", {}).get("resources", [])

        # Prioridad: GeoJSON en WGS84 con keyword en el nombre
        url = None
        for priority_fmt in (["GEOJSON", "JSON"], [""]):
            for res in resources:
                fmt  = (res.get("format") or "").upper()
                name = (res.get("name")   or "").lower()
                if keyword_geojson.lower() in name:
                    if not priority_fmt[0] or fmt in priority_fmt:
                        url = res.get("url")
                        break
            if url:
                break

        if not url:
            return None

        resp = await client.get(url, timeout=_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()

        # Validar que es un GeoJSON con features
        if data.get("type") == "FeatureCollection" and data.get("features"):
            return data

    except Exception as exc:
        logger.warning("    CKAN error (%s / %s): %s", package_id, keyword_geojson, exc)

    return None


def _prop(props: dict, candidates: list[str]) -> Optional[str]:
    """Extrae el primer valor no nulo de una lista de posibles claves."""
    for k in candidates:
        v = props.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


# =============================================================================
# FALLBACK — Datos hardcoded si Open Data BCN no responde
# =============================================================================

def _distritos_hardcoded() -> dict:
    """
    GeoJSON con los 10 distritos de Barcelona.
    Polígonos aproximados basados en las coordenadas oficiales del Ajuntament.
    """
    distritos = [
        # (código, nombre, [min_lng, min_lat, max_lng, max_lat])
        ("01", "Ciutat Vella",        [2.163, 41.375, 2.192, 41.392]),
        ("02", "Eixample",            [2.137, 41.380, 2.195, 41.408]),
        ("03", "Sants-Montjuïc",      [2.109, 41.355, 2.170, 41.397]),
        ("04", "Les Corts",           [2.100, 41.376, 2.140, 41.403]),
        ("05", "Sarrià-Sant Gervasi", [2.100, 41.388, 2.155, 41.443]),
        ("06", "Gràcia",              [2.148, 41.396, 2.180, 41.426]),
        ("07", "Horta-Guinardó",      [2.145, 41.403, 2.200, 41.448]),
        ("08", "Nou Barris",          [2.154, 41.418, 2.200, 41.468]),
        ("09", "Sant Andreu",         [2.175, 41.413, 2.220, 41.455]),
        ("10", "Sant Martí",          [2.162, 41.383, 2.233, 41.445]),
    ]
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"DISTRICTE": int(cod), "NOM_DISTRICTE": nom},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [b[0], b[1]], [b[2], b[1]],
                        [b[2], b[3]], [b[0], b[3]],
                        [b[0], b[1]],
                    ]],
                },
            }
            for cod, nom, b in distritos
        ],
    }


def _barrios_hardcoded() -> dict:
    """
    GeoJSON con los 73 barrios oficiales de Barcelona.
    Polígonos de bounding box aproximados basados en el padrón municipal.
    Fuente de referencia: Ajuntament de Barcelona — Estadística.
    """
    # (barri_code, dist_code, nombre, [min_lng, min_lat, max_lng, max_lat])
    barrios = [
        # ── Districte 01 — Ciutat Vella ──────────────────────────────────
        ( 1, 1, "el Raval",                               [2.163, 41.376, 2.176, 41.387]),
        ( 2, 1, "el Barri Gòtic",                         [2.175, 41.378, 2.184, 41.387]),
        ( 3, 1, "la Barceloneta",                         [2.183, 41.374, 2.194, 41.383]),
        ( 4, 1, "Sant Pere, Santa Caterina i la Ribera",  [2.177, 41.383, 2.191, 41.392]),
        # ── Districte 02 — Eixample ──────────────────────────────────────
        ( 5, 2, "el Fort Pienc",                          [2.178, 41.393, 2.193, 41.403]),
        ( 6, 2, "la Sagrada Família",                     [2.168, 41.397, 2.183, 41.409]),
        ( 7, 2, "la Dreta de l'Eixample",                 [2.153, 41.389, 2.173, 41.402]),
        ( 8, 2, "l'Antiga Esquerra de l'Eixample",        [2.148, 41.381, 2.166, 41.393]),
        ( 9, 2, "la Nova Esquerra de l'Eixample",         [2.137, 41.377, 2.155, 41.393]),
        (10, 2, "Sant Antoni",                            [2.154, 41.379, 2.169, 41.390]),
        # ── Districte 03 — Sants-Montjuïc ───────────────────────────────
        (11, 3, "el Poble Sec - Parc de Montjuïc",        [2.146, 41.368, 2.170, 41.384]),
        (12, 3, "la Marina del Prat Vermell",              [2.132, 41.354, 2.156, 41.368]),
        (13, 3, "la Marina de Port",                       [2.133, 41.354, 2.157, 41.370]),
        (14, 3, "la Font de la Guatlla",                   [2.133, 41.369, 2.148, 41.381]),
        (15, 3, "Hostafrancs",                             [2.135, 41.373, 2.153, 41.383]),
        (16, 3, "la Bordeta",                              [2.124, 41.373, 2.143, 41.387]),
        (17, 3, "Sants - Badal",                           [2.121, 41.373, 2.143, 41.392]),
        (18, 3, "Sants",                                   [2.125, 41.377, 2.146, 41.393]),
        # ── Districte 04 — Les Corts ─────────────────────────────────────
        (19, 4, "les Corts",                               [2.117, 41.384, 2.139, 41.400]),
        (20, 4, "la Maternitat i Sant Ramon",              [2.109, 41.381, 2.128, 41.397]),
        (21, 4, "Pedralbes",                               [2.100, 41.389, 2.121, 41.412]),
        # ── Districte 05 — Sarrià-Sant Gervasi ──────────────────────────
        (22, 5, "Vallvidrera, el Tibidabo i les Planes",   [2.100, 41.409, 2.131, 41.443]),
        (23, 5, "Sarrià",                                  [2.111, 41.393, 2.136, 41.422]),
        (24, 5, "les Tres Torres",                         [2.123, 41.395, 2.142, 41.411]),
        (25, 5, "Sant Gervasi - la Bonanova",               [2.132, 41.395, 2.154, 41.413]),
        (26, 5, "Sant Gervasi - Galvany",                   [2.146, 41.394, 2.164, 41.408]),
        (27, 5, "el Putxet i el Farró",                     [2.146, 41.402, 2.167, 41.416]),
        # ── Districte 06 — Gràcia ────────────────────────────────────────
        (28, 6, "Vallcarca i els Penitents",                [2.147, 41.411, 2.163, 41.425]),
        (29, 6, "el Coll",                                  [2.151, 41.413, 2.165, 41.427]),
        (30, 6, "la Salut",                                 [2.157, 41.405, 2.175, 41.419]),
        (31, 6, "la Vila de Gràcia",                        [2.151, 41.397, 2.173, 41.412]),
        (32, 6, "el Camp d'en Grassot i Gràcia Nova",       [2.164, 41.398, 2.181, 41.412]),
        # ── Districte 07 — Horta-Guinardó ───────────────────────────────
        (33, 7, "el Baix Guinardó",                         [2.162, 41.405, 2.184, 41.419]),
        (34, 7, "Can Baró",                                 [2.162, 41.412, 2.179, 41.425]),
        (35, 7, "el Guinardó",                              [2.163, 41.417, 2.185, 41.432]),
        (36, 7, "la Font d'en Fargues",                     [2.154, 41.417, 2.173, 41.430]),
        (37, 7, "el Carmel",                                [2.150, 41.420, 2.171, 41.436]),
        (38, 7, "la Teixonera",                             [2.146, 41.424, 2.164, 41.437]),
        (39, 7, "Sant Genís dels Agudells",                 [2.145, 41.432, 2.165, 41.447]),
        (40, 7, "Montbau",                                  [2.150, 41.431, 2.168, 41.443]),
        (41, 7, "la Vall d'Hebron",                         [2.139, 41.424, 2.163, 41.446]),
        (42, 7, "la Clota",                                 [2.164, 41.424, 2.179, 41.436]),
        (43, 7, "Horta",                                    [2.159, 41.431, 2.183, 41.448]),
        # ── Districte 08 — Nou Barris ────────────────────────────────────
        (44, 8, "Vilapicina i la Torre Llobeta",            [2.161, 41.428, 2.182, 41.442]),
        (45, 8, "Porta",                                    [2.154, 41.426, 2.172, 41.438]),
        (46, 8, "el Turó de la Peira",                      [2.162, 41.433, 2.179, 41.446]),
        (47, 8, "Can Peguera",                              [2.167, 41.435, 2.183, 41.448]),
        (48, 8, "la Guineueta",                             [2.164, 41.437, 2.182, 41.450]),
        (49, 8, "Canyelles",                                [2.162, 41.445, 2.182, 41.458]),
        (50, 8, "les Roquetes",                             [2.155, 41.435, 2.178, 41.450]),
        (51, 8, "Verdun",                                   [2.154, 41.429, 2.172, 41.442]),
        (52, 8, "la Prosperitat",                           [2.154, 41.436, 2.173, 41.452]),
        (53, 8, "la Trinitat Nova",                         [2.174, 41.446, 2.197, 41.461]),
        (54, 8, "Torre Baró",                               [2.158, 41.448, 2.178, 41.463]),
        (55, 8, "Ciutat Meridiana",                         [2.162, 41.454, 2.181, 41.467]),
        (56, 8, "Vallbona",                                 [2.171, 41.455, 2.191, 41.468]),
        # ── Districte 09 — Sant Andreu ───────────────────────────────────
        (57, 9, "la Trinitat Vella",                        [2.184, 41.446, 2.207, 41.460]),
        (58, 9, "Baró de Viver",                            [2.195, 41.441, 2.215, 41.453]),
        (59, 9, "el Bon Pastor",                            [2.195, 41.431, 2.219, 41.447]),
        (60, 9, "Sant Andreu",                              [2.181, 41.431, 2.206, 41.450]),
        (61, 9, "la Sagrera",                               [2.181, 41.421, 2.203, 41.438]),
        (62, 9, "el Congrés i els Indians",                 [2.179, 41.415, 2.197, 41.429]),
        (63, 9, "Navas",                                    [2.177, 41.408, 2.197, 41.422]),
        # ── Districte 10 — Sant Martí ────────────────────────────────────
        (64, 10, "el Camp de l'Arpa del Clot",              [2.179, 41.402, 2.199, 41.416]),
        (65, 10, "el Clot",                                 [2.186, 41.406, 2.207, 41.423]),
        (66, 10, "el Parc i la Llacuna del Poblenou",       [2.191, 41.397, 2.211, 41.413]),
        (67, 10, "la Vila Olímpica del Poblenou",           [2.191, 41.384, 2.211, 41.399]),
        (68, 10, "el Poblenou",                             [2.195, 41.394, 2.219, 41.417]),
        (69, 10, "Diagonal Mar i el Front Marítim",         [2.198, 41.403, 2.226, 41.419]),
        (70, 10, "el Besòs i el Maresme",                   [2.208, 41.414, 2.233, 41.433]),
        (71, 10, "Provençals del Poblenou",                 [2.195, 41.411, 2.219, 41.427]),
        (72, 10, "Sant Martí de Provençals",                [2.186, 41.411, 2.208, 41.428]),
        (73, 10, "la Verneda i la Pau",                     [2.192, 41.423, 2.222, 41.444]),
    ]

    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "BARRI":     bc,
                    "DISTRICTE": dc,
                    "NOM_BARRI": nom,
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [[
                        [b[0], b[1]], [b[2], b[1]],
                        [b[2], b[3]], [b[0], b[3]],
                        [b[0], b[1]],
                    ]],
                },
            }
            for bc, dc, nom, b in barrios
        ],
    }


if __name__ == "__main__":
    asyncio.run(main())
