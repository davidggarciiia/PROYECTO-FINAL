"""
pipelines/venues_ocio.py — Venues culturales y de ocio de Barcelona.

Agrega datos de:
  - BCN Equipaments (CKAN): teatres, museus, biblioteques, etc.
  - OSM Overpass: amenity=theatre/music_venue/cinema, tourism=attraction
  - Hardcoded: venues principales de Barcelona (Primavera Sound, Grec, etc.)

Calcula por zona:
  - eventos_culturales_500m: venues culturales en 500m
  - venues_musicales_500m: salas de música/conciertos en 500m

Frecuencia: mensual
"""
from __future__ import annotations

import asyncio
import logging
from datetime import date
from typing import Optional

import httpx

from db.conexion import get_db

logger = logging.getLogger(__name__)

# ── Configuración CKAN ─────────────────────────────────────────────────────────
_CKAN_BASE    = "https://opendata-ajuntament.barcelona.cat/data/api/action"
_CKAN_LIMIT   = 1000
_TIMEOUT_S    = 30
_SLEEP_PAGE   = 0.5

# Resource IDs de Equipaments Municipals en Open Data BCN
_RID_EQUIPAMENTS = "f2d23d4a-1e80-4e93-8b18-b3c2dcd6e7a1"

# ── Overpass ───────────────────────────────────────────────────────────────────
_OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://z.overpass-api.de/api/interpreter",
]
_OVERPASS_TIMEOUT = 60
_BCN_BBOX = "41.32,2.07,41.47,2.23"   # min_lat,min_lng,max_lat,max_lng

# ── Radio ST_DWithin ───────────────────────────────────────────────────────────
_RADIO_M = 500

# ── Tipos de venue ─────────────────────────────────────────────────────────────
_TIPO_CULTURAL = "cultural"
_TIPO_MUSICAL  = "musical"

# ═══════════════════════════════════════════════════════════════════════════════
# Venues hardcoded (principales de Barcelona)
# ═══════════════════════════════════════════════════════════════════════════════
# (nombre, lat, lng, tipo)
_VENUES_HARDCODED: list[tuple[str, float, float, str]] = [
    # Grandes venues musicales / multitudinarios
    ("Palau Sant Jordi",                    41.3645,  2.1531, _TIPO_MUSICAL),
    ("Razzmatazz",                          41.4018,  2.1921, _TIPO_MUSICAL),
    ("Sala Apolo",                          41.3741,  2.1661, _TIPO_MUSICAL),
    ("Bikini",                              41.3884,  2.1312, _TIPO_MUSICAL),
    ("Primavera Sound (Parc Fòrum)",        41.4081,  2.2193, _TIPO_MUSICAL),
    ("Sónar (Fira BCN Gran Via)",           41.3572,  2.1278, _TIPO_MUSICAL),
    ("Grec Festival (Teatre Grec)",         41.3694,  2.1603, _TIPO_MUSICAL),
    ("Mercè Festival (Plaça de la Mercè)",  41.3818,  2.1788, _TIPO_MUSICAL),
    ("Palau de la Música Catalana",         41.3875,  2.1751, _TIPO_MUSICAL),
    ("Gran Teatre del Liceu",               41.3802,  2.1740, _TIPO_MUSICAL),
    ("BARTS (Barcelona Arts on Stage)",     41.3763,  2.1648, _TIPO_MUSICAL),
    ("Sala Beckett",                        41.4012,  2.1858, _TIPO_MUSICAL),
    # Museus i equipaments culturals
    ("MACBA (Museu Art Contemporani BCN)",  41.3834,  2.1660, _TIPO_CULTURAL),
    ("MNAC (Museu Nacional d'Art)",         41.3683,  2.1535, _TIPO_CULTURAL),
    ("Fundació Joan Miró",                  41.3684,  2.1594, _TIPO_CULTURAL),
    ("Museu Picasso",                       41.3847,  2.1812, _TIPO_CULTURAL),
    ("Fundació Antoni Tàpies",              41.3916,  2.1612, _TIPO_CULTURAL),
    ("CCCB (Centre de Cultura Contemporàn", 41.3833,  2.1661, _TIPO_CULTURAL),
    ("Museu d'Història de Catalunya",       41.3784,  2.1853, _TIPO_CULTURAL),
    ("Museu d'Història de Barcelona",       41.3838,  2.1770, _TIPO_CULTURAL),
    ("CosmoCaixa",                          41.4123,  2.1340, _TIPO_CULTURAL),
    # Estadis i grans recintes
    ("Camp Nou (FC Barcelona)",             41.3809,  2.1228, _TIPO_CULTURAL),
    ("Estadi Olímpic Lluís Companys",       41.3661,  2.1531, _TIPO_CULTURAL),
    ("Palau Blaugrana",                     41.3822,  2.1215, _TIPO_CULTURAL),
    # Teatres
    ("Teatre Nacional de Catalunya (TNC)",  41.4003,  2.1888, _TIPO_CULTURAL),
    ("Teatre Lliure",                       41.3698,  2.1603, _TIPO_CULTURAL),
    ("Teatre Romea",                        41.3801,  2.1720, _TIPO_CULTURAL),
    ("Mercat de les Flors",                 41.3700,  2.1600, _TIPO_CULTURAL),
    ("Teatre Victòria",                     41.3751,  2.1662, _TIPO_CULTURAL),
    # Cinemes rellevants
    ("Cinemes Verdi",                       41.4047,  2.1560, _TIPO_CULTURAL),
    ("Filmoteca de Catalunya",              41.3798,  2.1722, _TIPO_CULTURAL),
]


# ═══════════════════════════════════════════════════════════════════════════════
# Punto de entrada
# ═══════════════════════════════════════════════════════════════════════════════

async def ejecutar() -> dict:
    eid = await _init("venues_ocio")
    try:
        # 1. Recopilar venues de totes les fonts
        venues: list[dict] = []

        # 1a. BCN Equipaments (CKAN)
        equip = await _descargar_equipaments()
        logger.info("Equipaments CKAN: %d", len(equip))
        venues.extend(equip)

        # 1b. OSM Overpass
        osm = await _descargar_overpass()
        logger.info("Venues OSM: %d", len(osm))
        venues.extend(osm)

        # 1c. Hardcoded
        hard = _venues_hardcoded()
        logger.info("Venues hardcoded: %d", len(hard))
        venues.extend(hard)

        logger.info("Total venues (brut): %d", len(venues))

        if not venues:
            msg = "Cap venue obtingut de cap font"
            logger.warning(msg)
            await _fin(eid, 0, "ok", msg)
            return {"venues": 0, "zonas_actualizadas": 0}

        # 2. Desduplicar per coordenades (~50m de tolerància)
        venues = _deduplicar(venues)
        logger.info("Venues únics: %d", len(venues))

        # 3. Assignar zona_id
        venues = await _assignar_zones(venues)
        amb_zona = [v for v in venues if v.get("zona_id")]
        logger.info("Venues amb zona: %d / %d", len(amb_zona), len(venues))

        # 4. Insertar / actualitzar taula venues_ocio
        insertats = await _insertar_venues(amb_zona)
        logger.info("Venues inserits/actualitzats: %d", insertats)

        # 5. Calcular variables per zona i actualitzar variables_zona
        fecha_ref = date.today()
        zonas_act = await _actualitzar_variables_zona(fecha_ref)
        logger.info("Zones actualitzades: %d", zonas_act)

        await _fin(eid, insertats, "ok")
        logger.info("venues_ocio OK — %d venues, %d zones", insertats, zonas_act)
        return {"venues": insertats, "zonas_actualizadas": zonas_act}

    except Exception as exc:
        logger.error("Pipeline venues_ocio ERROR: %s", exc, exc_info=True)
        await _fin(eid, 0, "error", str(exc))
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# 1a. BCN Equipaments via CKAN
# ═══════════════════════════════════════════════════════════════════════════════

# Tipus d'equipament que ens interessen i el seu tipus intern
_EQUIP_TIPUS_MAP: dict[str, str] = {
    "teatre":        _TIPO_MUSICAL,
    "theatre":       _TIPO_MUSICAL,
    "música":        _TIPO_MUSICAL,
    "music":         _TIPO_MUSICAL,
    "concert":       _TIPO_MUSICAL,
    "cinema":        _TIPO_CULTURAL,
    "museu":         _TIPO_CULTURAL,
    "museum":        _TIPO_CULTURAL,
    "biblioteca":    _TIPO_CULTURAL,
    "library":       _TIPO_CULTURAL,
    "centre cívic":  _TIPO_CULTURAL,
    "cultural":      _TIPO_CULTURAL,
    "exposició":     _TIPO_CULTURAL,
    "sala":          _TIPO_MUSICAL,
    "auditori":      _TIPO_MUSICAL,
}


async def _descargar_equipaments() -> list[dict]:
    """Descarrega Equipaments Municipals des del CKAN."""
    resultats: list[dict] = []
    offset = 0

    async with httpx.AsyncClient(timeout=_TIMEOUT_S, follow_redirects=True) as client:
        while True:
            params = {
                "resource_id": _RID_EQUIPAMENTS,
                "limit":       _CKAN_LIMIT,
                "offset":      offset,
            }
            try:
                r = await client.get(f"{_CKAN_BASE}/datastore_search", params=params)
            except Exception as exc:
                logger.warning("Error CKAN equipaments offset=%d: %s", offset, exc)
                break

            if r.status_code != 200:
                logger.warning("CKAN equipaments HTTP %d", r.status_code)
                break

            try:
                data = r.json()
            except Exception:
                break

            if not data.get("success"):
                break

            records = data.get("result", {}).get("records", [])
            if not records:
                break

            for rec in records:
                venue = _normalitzar_equipament(rec)
                if venue:
                    resultats.append(venue)

            total = data.get("result", {}).get("total", 0)
            offset += _CKAN_LIMIT
            if offset >= total:
                break

            await asyncio.sleep(_SLEEP_PAGE)

    return resultats


def _normalitzar_equipament(rec: dict) -> Optional[dict]:
    """Normalitza un registre d'equipament CKAN."""
    # Determinar tipus
    tipus_raw = (
        rec.get("TIPUS_EQUIPAMENT")
        or rec.get("TIPUS")
        or rec.get("CATEGORIA")
        or ""
    ).lower()

    tipo = None
    for key, val in _EQUIP_TIPUS_MAP.items():
        if key in tipus_raw:
            tipo = val
            break
    if not tipo:
        return None

    # Coordenades
    try:
        lat = float(rec.get("LATITUD") or rec.get("LAT") or 0)
        lng = float(rec.get("LONGITUD") or rec.get("LNG") or rec.get("LON") or 0)
    except (ValueError, TypeError):
        lat, lng = 0.0, 0.0

    # Intentar extreure de camp GEO_EPGS_4326 ("POINT(lng lat)")
    if not lat or not lng:
        geo = str(rec.get("GEO_EPGS_4326") or "")
        if geo.startswith("POINT("):
            try:
                coords = geo[6:-1].split()
                lng, lat = float(coords[0]), float(coords[1])
            except Exception:
                pass

    if not lat or not lng:
        return None

    nom = (
        rec.get("NOM_EQUIPAMENT")
        or rec.get("NOM")
        or rec.get("ADRECA")
        or "Equipament sense nom"
    ).strip()[:300]

    return {
        "id":    f"equip_{rec.get('_id', '')}",
        "nom":   nom,
        "lat":   lat,
        "lng":   lng,
        "tipo":  tipo,
        "font":  "bcn_equipaments",
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 1b. OSM Overpass
# ═══════════════════════════════════════════════════════════════════════════════

_OVERPASS_QUERY = f"""[out:json][timeout:{_OVERPASS_TIMEOUT - 5}];
(
  node["amenity"~"theatre|cinema|arts_centre|community_centre|music_venue"]({_BCN_BBOX});
  node["tourism"~"attraction|museum|gallery|theme_park"]({_BCN_BBOX});
  node["leisure"~"stadium|sports_hall|sports_centre|arena"]({_BCN_BBOX});
  node["building"~"stadium|arena|concert_hall|theatre"]({_BCN_BBOX});
  way["amenity"~"theatre|cinema|arts_centre|music_venue"]({_BCN_BBOX});
  way["tourism"~"attraction|museum|gallery"]({_BCN_BBOX});
  way["leisure"~"stadium|sports_hall|sports_centre|arena"]({_BCN_BBOX});
);
out center;"""

_OSM_TIPO_MAP: dict[str, str] = {
    "theatre":           _TIPO_MUSICAL,
    "music_venue":       _TIPO_MUSICAL,
    "concert_hall":      _TIPO_MUSICAL,
    "stadium":           _TIPO_CULTURAL,
    "arena":             _TIPO_CULTURAL,
    "cinema":            _TIPO_CULTURAL,
    "arts_centre":       _TIPO_CULTURAL,
    "community_centre":  _TIPO_CULTURAL,
    "museum":            _TIPO_CULTURAL,
    "gallery":           _TIPO_CULTURAL,
    "attraction":        _TIPO_CULTURAL,
    "theme_park":        _TIPO_CULTURAL,
    "sports_hall":       _TIPO_CULTURAL,
    "sports_centre":     _TIPO_CULTURAL,
}


async def _descargar_overpass() -> list[dict]:
    """Consulta OSM Overpass per a venues culturals i musicals a Barcelona."""
    # Overpass rechaza POST sin User-Agent con HTTP 406 Not Acceptable.
    headers = {"User-Agent": "GeoRetail/1.0 (barcelona; venues ocio cultural)"}
    async with httpx.AsyncClient(timeout=_OVERPASS_TIMEOUT, follow_redirects=True, headers=headers) as client:
        for url in _OVERPASS_URLS:
            try:
                r = await client.post(url, data={"data": _OVERPASS_QUERY})
                if r.status_code == 429:
                    logger.warning("Overpass 429 a %s", url)
                    continue
                if r.status_code != 200:
                    continue
                elements = r.json().get("elements", [])
                venues = []
                for el in elements:
                    v = _normalitzar_osm(el)
                    if v:
                        venues.append(v)
                return venues
            except httpx.TimeoutException:
                logger.warning("Timeout Overpass a %s", url)
            except Exception as exc:
                logger.warning("Error Overpass %s: %s", url, exc)

    logger.warning("Totes les URL Overpass han fallat")
    return []


def _normalitzar_osm(el: dict) -> Optional[dict]:
    """Normalitza un element OSM a un venue intern."""
    tags = el.get("tags", {})

    # Determinar tipo
    tipo: Optional[str] = None
    for tag_key in ("amenity", "tourism", "leisure", "building"):
        val = tags.get(tag_key, "")
        if val in _OSM_TIPO_MAP:
            tipo = _OSM_TIPO_MAP[val]
            break
    if not tipo:
        return None

    # Coordenades
    lat = el.get("lat") or (el.get("center") or {}).get("lat")
    lng = el.get("lon") or (el.get("center") or {}).get("lon")
    if not lat or not lng:
        return None

    osm_id   = str(el.get("id", ""))
    osm_type = el.get("type", "node")[0]

    nom = (
        tags.get("name")
        or tags.get("name:ca")
        or tags.get("name:es")
        or f"Venue OSM {osm_id}"
    )[:300]

    return {
        "id":   f"osm_{osm_type}{osm_id}",
        "nom":  nom,
        "lat":  float(lat),
        "lng":  float(lng),
        "tipo": tipo,
        "font": "osm_overpass",
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 1c. Hardcoded
# ═══════════════════════════════════════════════════════════════════════════════

def _venues_hardcoded() -> list[dict]:
    return [
        {
            "id":   f"hard_{i}",
            "nom":  nom,
            "lat":  lat,
            "lng":  lng,
            "tipo": tipo,
            "font": "hardcoded",
        }
        for i, (nom, lat, lng, tipo) in enumerate(_VENUES_HARDCODED)
    ]


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Desduplicació
# ═══════════════════════════════════════════════════════════════════════════════

def _deduplicar(venues: list[dict]) -> list[dict]:
    """
    Elimina venues duplicats per proximitat (~50m).
    Prioritza hardcoded > bcn_equipaments > osm_overpass.

    Estratègia: ordena per font (prioritat) i marca com a duplicat
    qualsevol venue posterior a menys de 50m d'un ja vist.
    """
    from math import cos, radians, sqrt

    prioritat = {"hardcoded": 0, "bcn_equipaments": 1, "osm_overpass": 2}
    venues_ord = sorted(venues, key=lambda v: prioritat.get(v.get("font", ""), 9))

    vistos: list[tuple[float, float]] = []
    resultat: list[dict] = []
    _50M_DEG = 50 / 111_000  # ~50m en graus (aprox. uniforme per Barcelona)

    for v in venues_ord:
        lat, lng = v["lat"], v["lng"]
        duplicat = False
        for (vlat, vlng) in vistos:
            dlat = abs(lat - vlat)
            dlng = abs(lng - vlng) * cos(radians(lat))
            dist = sqrt(dlat ** 2 + dlng ** 2)
            if dist < _50M_DEG:
                duplicat = True
                break
        if not duplicat:
            vistos.append((lat, lng))
            resultat.append(v)

    return resultat


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Assignar zones
# ═══════════════════════════════════════════════════════════════════════════════

async def _assignar_zones(venues: list[dict]) -> list[dict]:
    """Assigna zona_id a cada venue via ST_Within (batch), fallback ST_DWithin 100m."""
    if not venues:
        return venues

    _CHUNK = 300
    async with get_db() as conn:
        # Pas 1: ST_Within
        for i in range(0, len(venues), _CHUNK):
            chunk = venues[i : i + _CHUNK]
            values_sql = ", ".join(
                f"(${j*2+1}::float8, ${j*2+2}::float8, {j})"
                for j in range(len(chunk))
            )
            params: list = []
            for v in chunk:
                params.extend([v["lng"], v["lat"]])
            try:
                rows = await conn.fetch(
                    f"""
                    SELECT pts.idx, z.id AS zona_id
                    FROM (VALUES {values_sql}) AS pts(lng, lat, idx)
                    JOIN zonas z ON ST_Within(
                        ST_SetSRID(ST_MakePoint(pts.lng, pts.lat), 4326),
                        z.geometria
                    )
                    """,
                    *params,
                )
                for row in rows:
                    chunk[row["idx"]]["zona_id"] = row["zona_id"]
            except Exception as exc:
                logger.warning("ST_Within batch venues error: %s", exc)

        # Pas 2: fallback ST_DWithin 100m
        pendents = [v for v in venues if not v.get("zona_id")]
        if pendents:
            for i in range(0, len(pendents), _CHUNK):
                chunk = pendents[i : i + _CHUNK]
                values_sql = ", ".join(
                    f"(${j*2+1}::float8, ${j*2+2}::float8, {j})"
                    for j in range(len(chunk))
                )
                params = []
                for v in chunk:
                    params.extend([v["lng"], v["lat"]])
                try:
                    rows = await conn.fetch(
                        f"""
                        SELECT DISTINCT ON (pts.idx) pts.idx, z.id AS zona_id
                        FROM (VALUES {values_sql}) AS pts(lng, lat, idx)
                        JOIN zonas z ON ST_DWithin(
                            z.geometria::geography,
                            ST_SetSRID(ST_MakePoint(pts.lng, pts.lat), 4326)::geography,
                            100
                        )
                        ORDER BY pts.idx,
                            ST_Distance(
                                z.geometria::geography,
                                ST_SetSRID(ST_MakePoint(pts.lng, pts.lat), 4326)::geography
                            )
                        """,
                        *params,
                    )
                    for row in rows:
                        chunk[row["idx"]]["zona_id"] = row["zona_id"]
                except Exception as exc:
                    logger.warning("ST_DWithin fallback venues error: %s", exc)

    return venues


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Inserció en venues_ocio
# ═══════════════════════════════════════════════════════════════════════════════

async def _insertar_venues(venues: list[dict]) -> int:
    """Insereix/actualitza en la taula venues_ocio (crea si no existeix)."""
    if not venues:
        return 0

    async with get_db() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS venues_ocio (
                id          VARCHAR(60)  PRIMARY KEY,
                nom         TEXT         NOT NULL,
                lat         FLOAT        NOT NULL,
                lng         FLOAT        NOT NULL,
                geometria   GEOMETRY(POINT, 4326),
                zona_id     VARCHAR(20)  REFERENCES zonas(id),
                tipo        VARCHAR(20)  NOT NULL,  -- 'cultural' | 'musical'
                font        VARCHAR(30),
                updated_at  TIMESTAMPTZ  DEFAULT NOW()
            )
        """)
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_venues_ocio_geometria "
            "ON venues_ocio USING GIST(geometria)"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_venues_ocio_zona "
            "ON venues_ocio(zona_id)"
        )

        n = 0
        for v in venues:
            try:
                await conn.execute(
                    """
                    INSERT INTO venues_ocio
                        (id, nom, lat, lng, geometria, zona_id, tipo, font, updated_at)
                    VALUES (
                        $1, $2, $3, $4,
                        ST_SetSRID(ST_MakePoint($4, $3), 4326),
                        $5, $6, $7, NOW()
                    )
                    ON CONFLICT (id) DO UPDATE
                    SET nom       = EXCLUDED.nom,
                        lat       = EXCLUDED.lat,
                        lng       = EXCLUDED.lng,
                        geometria = EXCLUDED.geometria,
                        zona_id   = EXCLUDED.zona_id,
                        tipo      = EXCLUDED.tipo,
                        font      = EXCLUDED.font,
                        updated_at = NOW()
                    """,
                    v["id"], v["nom"], v["lat"], v["lng"],
                    v.get("zona_id"), v["tipo"], v.get("font", "desconegut"),
                )
                n += 1
            except Exception as exc:
                logger.debug("Insert venue error %s: %s", v.get("id"), exc)

    return n


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Actualitzar variables_zona
# ═══════════════════════════════════════════════════════════════════════════════

async def _actualitzar_variables_zona(fecha: date) -> int:
    """
    Per cada zona calcula:
      - eventos_culturales_500m: venues culturals en radio 500m des del centroide
      - venues_musicales_500m:   venues musicals en radio 500m
      - mercados_municipales_500m: mercats municipals en radio 500m (via venues_ocio)

    # REFACTOR: abans UPDATE variables_zona (columnes que ja no existeixen a la
    # coordinadora), ara:
    #   1. INSERT INTO variables_zona (coordinadora) — per crear la fila ancora
    #   2. INSERT INTO vz_turismo — eventos_culturales_500m, venues_musicales_500m,
    #                               mercados_municipales_500m
    #   3. INSERT INTO vz_entorno — score_equipamientos (proxy de diversitat d'oci)
    """
    async with get_db() as conn:
        # Comptar per zona i tipus usant ST_DWithin des del centroide de la zona.
        # Afegim mercados_municipales (type 'mercado' si existeix, o bé via taula directa)
        rows = await conn.fetch(
            f"""
            SELECT
                z.id AS zona_id,
                COUNT(CASE WHEN v.tipo = 'cultural' THEN 1 END)  AS n_cultural,
                COUNT(CASE WHEN v.tipo = 'musical'  THEN 1 END)  AS n_musical
            FROM zonas z
            CROSS JOIN venues_ocio v
            WHERE ST_DWithin(
                ST_Centroid(z.geometria)::geography,
                ST_SetSRID(ST_MakePoint(v.lng, v.lat), 4326)::geography,
                {_RADIO_M}
            )
            GROUP BY z.id
            """
        )

        # Comptar mercados_municipales_500m des de la taula mercados_municipales
        mercats_rows = await conn.fetch(
            """
            SELECT
                z.id AS zona_id,
                COUNT(mm.id)::int AS n_mercats
            FROM zonas z
            LEFT JOIN mercados_municipales mm
                ON ST_DWithin(mm.geometria::geography,
                              ST_Centroid(z.geometria)::geography, 500)
            GROUP BY z.id
            HAVING COUNT(mm.id) > 0
            """
        )

    if not rows and not mercats_rows:
        return 0

    mercats_per_zona = {r["zona_id"]: int(r["n_mercats"]) for r in mercats_rows}

    n = 0
    async with get_db() as conn:
        # Unió de totes les zones afectades
        zona_ids_afectades = set(r["zona_id"] for r in rows) | set(mercats_per_zona.keys())
        venues_per_zona = {r["zona_id"]: r for r in rows}

        for zona_id in zona_ids_afectades:
            row = venues_per_zona.get(zona_id)
            n_cultural = int(row["n_cultural"] or 0) if row else 0
            n_musical  = int(row["n_musical"]  or 0) if row else 0
            n_mercats  = mercats_per_zona.get(zona_id, 0)

            # 1. Anchor en variables_zona (tabla coordinadora delgada)
            await conn.execute(
                """
                INSERT INTO variables_zona (zona_id, fecha, fuente)
                VALUES ($1, $2, 'venues_ocio')
                ON CONFLICT (zona_id, fecha) DO UPDATE
                SET fuente = EXCLUDED.fuente, updated_at = NOW()
                """,
                zona_id, fecha,
            )

            # 2. Dades de turisme i oci en taula satèl·lit vz_turismo
            # REFACTOR: abans UPDATE variables_zona SET eventos_culturales_500m / venues_musicales_500m
            await conn.execute(
                """
                INSERT INTO vz_turismo (
                    zona_id, fecha,
                    eventos_culturales_500m,
                    venues_musicales_500m,
                    mercados_municipales_500m,
                    fuente
                )
                VALUES ($1, $2, $3, $4, $5, 'venues_ocio')
                ON CONFLICT (zona_id, fecha) DO UPDATE
                SET eventos_culturales_500m  = EXCLUDED.eventos_culturales_500m,
                    venues_musicales_500m    = EXCLUDED.venues_musicales_500m,
                    mercados_municipales_500m = EXCLUDED.mercados_municipales_500m,
                    fuente                   = EXCLUDED.fuente,
                    updated_at               = NOW()
                """,
                zona_id, fecha, n_cultural, n_musical, n_mercats if n_mercats else None,
            )

            # 3. score_equipamientos com a proxy de diversitat d'oci en vz_entorno
            # REFACTOR: abans UPDATE variables_zona SET score_equipamientos (columna inexistent)
            total_venues = n_cultural + n_musical
            if total_venues > 0:
                score_equip = min(100.0, total_venues * 5.0)  # 20 venues → 100
                await conn.execute(
                    """
                    INSERT INTO vz_entorno (zona_id, fecha, score_equipamientos, fuente)
                    VALUES ($1, $2, $3, 'venues_ocio')
                    ON CONFLICT (zona_id, fecha) DO UPDATE
                    SET score_equipamientos = GREATEST(
                        COALESCE(vz_entorno.score_equipamientos, 0),
                        EXCLUDED.score_equipamientos
                    ),
                    fuente     = EXCLUDED.fuente,
                    updated_at = NOW()
                    """,
                    zona_id, fecha, score_equip,
                )

            n += 1

    return n


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers BD
# ═══════════════════════════════════════════════════════════════════════════════

async def _init(pipeline: str) -> int:
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones (pipeline, estado) "
            "VALUES ($1,'running') RETURNING id",
            pipeline,
        )


async def _fin(
    eid: int, registros: int, estado: str, mensaje: Optional[str] = None
) -> None:
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones "
            "SET fecha_fin=NOW(), registros=$1, estado=$2, mensaje_error=$3 "
            "WHERE id=$4",
            registros, estado, mensaje, eid,
        )
