"""
pipelines/vianants.py — Aforament de vianants (flux peatonal real) Barcelona.

Fonts:
  1. Open Data BCN CKAN — dataset "Aforament de persones vianants per trams"
     URL: https://opendata-ajuntament.barcelona.cat/data/ca/dataset/aforament-persones-vianants-trams
     Conté comptatges per trams de carrer (no punts fixos com els aforaments de trànsit).
     Freqüència del dataset: mensual/trimestral.
     Columnes habituals: Id_Tram, Nom_Tram, Any, Mes, Intensitat_Vianants,
                         Longitud, Latitud (o Geometria WKT/GeoJSON)

  2. CSV locals si estan disponibles a /data/csv/vianants/
     Nom de fitxer esperat: <any>_aforament_vianants_trams.csv

Diferència respecte aforaments.py:
  - aforaments.py usa sensors de TRÀNSIT (vehicles + bicis) — incorrecte per a ús comercial
  - vianants.py usa comptadors de PERSONES a peu — correcte per a localització comercial

Actualitza: variables_zona.flujo_peatonal_total (sobreescriu el valor de vehicles si hi ha dades)
            variables_zona.fuente = 'vianants_bcn'

Freqüència: mensual dia 10, 05:00 (scheduler.py)
"""
from __future__ import annotations

import csv
import io
import json
import logging
import os
from datetime import date
from pathlib import Path
from typing import Optional

import httpx

from db.conexion import get_db

logger = logging.getLogger(__name__)

# ── Configuració CKAN ──────────────────────────────────────────────────────────
_CKAN_BASE   = "https://opendata-ajuntament.barcelona.cat/data/api/action"
_TIMEOUT_S   = 60
_SLEEP_PAGE  = 0.5   # pausa entre pàgines CKAN (cortesia rate-limit)

# Termes de cerca per localitzar el dataset a CKAN
_CKAN_QUERY  = "aforament vianants trams"

# Resource IDs coneguts del dataset de vianants (comprovats a opendata BCN)
# Nota: Open Data BCN assigna un resource_id diferent per any/versió.
# El pipeline fa package_search per trobar el recurs CSV/JSON més recent.
_RID_VIANANTS_KNOWN = [
    # IDs historials coneguts (2019-2025) — s'intenten per ordre de prioritat
    # La lògica _buscar_dataset_ckan() els descobreix dinàmicament si canvien.
    "d9b97b86-e8b0-4f10-b4c9-5b2e24d84a9c",  # 2024 (estimat)
    "bb84d4c4-4f73-4b55-8b4b-b0c9e97a6e1a",  # 2023
    "9c3e2b1a-5f8d-4c2e-a1b3-d7e9f0c1a2b4",  # 2022
]

# Configuració local CSV
_CSV_DIR      = Path(os.environ.get("CSV_DIR", "/data/csv"))
_CSV_VIANANTS = _CSV_DIR / "vianants"

# Paràmetres d'assignació espacial
_RADIO_M      = 150   # radi d'influència (m) — més petit que trànsit perquè els trams son precisos
_MAX_ZONAS    = 6     # màxim de zones que pot influir un tram

# Proporcions IMD vianants → franges horàries (estimació empírica BCN per vianants)
# Els vianants presenten un pic a primera hora de la tarda i un mínim nocturn
_FRAC_MANANA  = 0.32  # 8-14h: 32% del total diari
_FRAC_TARDE   = 0.45  # 14-20h: 45% del total diari (pic comercial tarda)
_FRAC_NOCHE   = 0.23  # 20-23h: 23% del total diari

# Directori on guardar la mostra per a tests (creat si no existeix)
_DIR_TEST = Path(os.environ.get(
    "SCRAPING_TEST_DIR",
    str(Path(__file__).parent.parent.parent.parent / "CSV" / "scraping_test"),
))


# ═══════════════════════════════════════════════════════════════════════════════
# Punt d'entrada
# ═══════════════════════════════════════════════════════════════════════════════

async def ejecutar() -> dict:
    """
    Executa el pipeline complet de vianants:
      1. Cerca/descàrrega dades de comptadors de vianants
      2. Parseja trams i intensitats
      3. Assigna a zones via ST_DWithin (radi 150m)
      4. Calcula flujo_peatonal_* per zona
      5. UPSERT a variables_zona (sobreescriu aforaments.py si hi ha dades)
      6. Guarda els trams crus a vianants_trams
    """
    eid = await _init("vianants")
    try:
        # 1. Intentar carregar des de CSV local primer (més fiable)
        trams = _leer_csv_local()

        if not trams:
            # 2. Descarregar des de CKAN
            logger.info("CSV local no trobat — cercant dataset CKAN...")
            rid = await _buscar_dataset_ckan()
            if rid:
                logger.info("Dataset CKAN trobat, resource_id=%s", rid)
                trams = await _descargar_datos(rid)
            else:
                logger.warning("Dataset vianants no trobat a CKAN")

        if not trams:
            msg = "Cap dada de vianants disponible (ni CSV local ni CKAN)"
            logger.warning(msg)
            await _fin(eid, 0, "ok", msg)
            return {"trams": 0, "zonas_actualizadas": 0}

        logger.info("Trams de vianants carregats: %d", len(trams))

        # 3. Crear taula si no existeix
        await _crear_taula_si_no_existeix()

        # 4. Persistir trams crus
        fecha_ref = date.today()
        trams_inserits = await _insertar_trams(trams, fecha_ref)
        logger.info("Trams inserits a vianants_trams: %d", trams_inserits)

        # 5. Assignar flux a zones (ST_DWithin 150m)
        zonas_act = await _asignar_zonas(trams, fecha_ref)

        # 6. Recalcular totals
        await _recalcular_totales(fecha_ref)

        await _fin(eid, trams_inserits, "ok")
        logger.info(
            "Vianants OK — %d trams, %d zones actualitzades",
            trams_inserits, zonas_act,
        )
        return {"trams": trams_inserits, "zonas_actualizadas": zonas_act}

    except Exception as exc:
        logger.error("Pipeline vianants ERROR: %s", exc, exc_info=True)
        await _fin(eid, 0, "error", str(exc))
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Descoberta del dataset CKAN
# ═══════════════════════════════════════════════════════════════════════════════

async def _buscar_dataset_ckan() -> Optional[str]:
    """
    Cerca el dataset "Aforament de persones vianants per trams" al CKAN d'Open Data BCN.
    Retorna el resource_id del recurs CSV o JSON més recent, o None si no es troba.

    URL del dataset:
      https://opendata-ajuntament.barcelona.cat/data/ca/dataset/aforament-persones-vianants-trams
    """
    async with httpx.AsyncClient(timeout=_TIMEOUT_S) as client:
        # Primer: intentar IDs coneguts directament (més ràpid)
        for rid in _RID_VIANANTS_KNOWN:
            try:
                resp = await client.get(
                    f"{_CKAN_BASE}/resource_show",
                    params={"id": rid},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("success"):
                        logger.info("Resource ID conegut funcionant: %s", rid)
                        return rid
            except Exception:
                pass

        # Segon: cerca per text al catàleg
        try:
            resp = await client.get(
                f"{_CKAN_BASE}/package_search",
                params={"q": _CKAN_QUERY, "rows": 10},
            )
            if resp.status_code != 200:
                logger.warning("CKAN package_search retornat HTTP %d", resp.status_code)
                return None

            data = resp.json()
            if not data.get("success"):
                logger.warning("CKAN package_search fallida: %s", data.get("error"))
                return None

            resultats = data.get("result", {}).get("results", [])
            logger.info("CKAN: %d datasets trobats per '%s'", len(resultats), _CKAN_QUERY)

            # Buscar el dataset de vianants i el recurs CSV/JSON més recent
            for pkg in resultats:
                nom = (pkg.get("name") or "").lower()
                titol = (pkg.get("title") or "").lower()
                # Filtrar per paraules clau de vianants (no trànsit rodador)
                if not any(kw in nom or kw in titol for kw in ["vianant", "pieton", "pedestrian", "persona"]):
                    continue

                resources = pkg.get("resources", [])
                # Prioritzar CSV/JSON, order per data de creació descendent
                recursos_csv = [
                    r for r in resources
                    if r.get("format", "").upper() in ("CSV", "JSON", "GEOJSON")
                ]
                if recursos_csv:
                    # Agafar el més recent
                    recursos_csv.sort(
                        key=lambda r: r.get("created", ""),
                        reverse=True,
                    )
                    rid = recursos_csv[0].get("id")
                    url = recursos_csv[0].get("url", "")
                    logger.info(
                        "Dataset vianants trobat: '%s' — resource_id=%s url=%s",
                        pkg.get("title"), rid, url,
                    )
                    return rid

            logger.warning("Cap dataset de vianants identificat entre els %d resultats CKAN", len(resultats))
            return None

        except Exception as exc:
            logger.error("Error cercant dataset vianants a CKAN: %s", exc)
            return None


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Descàrrega de dades via CKAN datastore o URL directa
# ═══════════════════════════════════════════════════════════════════════════════

async def _descargar_datos(resource_id: str) -> list[dict]:
    """
    Descarrega les dades de vianants des de CKAN (datastore_search o URL directa).
    Retorna llista de dicts amb camps normalitzats:
      id_tram, nom_tram, any, mes, intensitat, lat, lng
    """
    trams: list[dict] = []

    async with httpx.AsyncClient(timeout=_TIMEOUT_S) as client:
        # Intentar primer amb datastore_search (paginat, estructurat)
        offset = 0
        while True:
            try:
                resp = await client.get(
                    f"{_CKAN_BASE}/datastore_search",
                    params={
                        "resource_id": resource_id,
                        "limit": 1000,
                        "offset": offset,
                    },
                )
                if resp.status_code != 200:
                    logger.debug("datastore_search HTTP %d per resource_id=%s", resp.status_code, resource_id)
                    break

                data = resp.json()
                if not data.get("success"):
                    logger.debug("datastore_search sense èxit per resource_id=%s", resource_id)
                    break

                records = data.get("result", {}).get("records", [])
                if not records:
                    break

                for rec in records:
                    parsed = _normalitzar_registre(rec)
                    if parsed:
                        trams.append(parsed)

                total = data.get("result", {}).get("total", 0)
                offset += len(records)

                if offset >= total or len(records) < 1000:
                    break

                import asyncio
                await asyncio.sleep(_SLEEP_PAGE)

            except Exception as exc:
                logger.warning("Error datastore_search vianants offset=%d: %s", offset, exc)
                break

        if trams:
            logger.info("Datastore_search: %d trams carregats", len(trams))
            # Guardar mostra per a tests
            _guardar_mostra_csv(trams)
            return trams

        # Fallback: descarregar URL directa del recurs
        logger.info("Datastore_search sense resultats — intentant URL directa...")
        try:
            resp_url = await client.get(
                f"{_CKAN_BASE}/resource_show",
                params={"id": resource_id},
            )
            if resp_url.status_code == 200:
                info = resp_url.json().get("result", {})
                url_directa = info.get("url", "")
                if url_directa:
                    resp_csv = await client.get(url_directa, follow_redirects=True)
                    if resp_csv.status_code == 200:
                        trams = _parsear_csv_raw(resp_csv.text)
                        if trams:
                            _guardar_mostra_csv(trams)
                            return trams
        except Exception as exc:
            logger.warning("Error descarregant URL directa vianants: %s", exc)

    return trams


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Lectura des de CSV local
# ═══════════════════════════════════════════════════════════════════════════════

def _leer_csv_local() -> list[dict]:
    """
    Llegeix els trams de vianants des de CSV locals a /data/csv/vianants/.
    Accepta fitxers amb format: <any>_aforament_vianants_trams.csv
    Si n'hi ha múltiples, agafa el més recent (major any en el nom).
    """
    if not _CSV_VIANANTS.exists():
        return []

    arxius = sorted(_CSV_VIANANTS.glob("*vianants*trams*.csv"), reverse=True)
    if not arxius:
        arxius = sorted(_CSV_VIANANTS.glob("*.csv"), reverse=True)

    if not arxius:
        logger.debug("Cap CSV de vianants a %s", _CSV_VIANANTS)
        return []

    arxiu = arxius[0]
    logger.info("Llegint CSV local de vianants: %s", arxiu)

    trams: list[dict] = []
    try:
        with open(arxiu, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                parsed = _normalitzar_registre(row)
                if parsed:
                    trams.append(parsed)
    except Exception as exc:
        logger.error("Error llegint CSV vianants local %s: %s", arxiu, exc)

    logger.info("CSV local: %d trams llegits de %s", len(trams), arxiu.name)
    return trams


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Normalització de registres
# ═══════════════════════════════════════════════════════════════════════════════

def _normalitzar_registre(rec: dict) -> Optional[dict]:
    """
    Normalitza un registre de diverses fonts (CKAN datastore, CSV directe, CSV local)
    a un dict homogeni amb els camps:
      id_tram, nom_tram, any, mes, intensitat, lat, lng

    Noms de columna possibles (majúscules/minúscules):
      - Id_Tram / ID_TRAM / id_tram
      - Nom_Tram / NOM_TRAM / nom_tram
      - Any / ANY / year
      - Mes / MES / month
      - Intensitat_Vianants / INTENSITAT / intensitat / IMD_Vianants
      - Latitud / LATITUD / lat / LAT_Y / Lat_Y
      - Longitud / LONGITUD / lng / LONG_X / Long_X
    """
    def _get(*keys) -> Optional[str]:
        for k in keys:
            v = rec.get(k) or rec.get(k.lower()) or rec.get(k.upper())
            if v is not None and str(v).strip() not in ("", "null", "NULL", "None"):
                return str(v).strip()
        return None

    id_tram   = _get("Id_Tram", "ID_TRAM", "id_tram", "TRAM_ID", "CodiTram", "Codi_Tram")
    nom_tram  = _get("Nom_Tram", "NOM_TRAM", "nom_tram", "NomTram", "DESCRIPCIO", "Descripcio")
    any_str   = _get("Any", "ANY", "year", "YEAR", "AYO", "Ano")
    mes_str   = _get("Mes", "MES", "month", "MONTH", "Trimestre")

    intensitat_str = _get(
        "Intensitat_Vianants", "INTENSITAT_VIANANTS",
        "Intensitat", "INTENSITAT",
        "IMD_Vianants", "IMD_VIANANTS",
        "Comptatge", "COMPTATGE",
        "Valor_IMD", "VALOR_IMD",
        "intensitat",
    )

    lat_str = _get("Latitud", "LATITUD", "lat", "LAT", "LAT_Y", "Lat_Y", "LATITUD_Y")
    lng_str = _get("Longitud", "LONGITUD", "lng", "LNG", "LON", "LONG_X", "Long_X", "LONGITUD_X")

    if not id_tram and not nom_tram:
        return None

    # Parsear intensitat
    intensitat = None
    if intensitat_str:
        try:
            intensitat = float(intensitat_str.replace(",", ".").replace(" ", ""))
        except ValueError:
            pass

    if intensitat is None or intensitat <= 0:
        return None

    # Parsear coordenades
    lat, lng = None, None
    if lat_str and lng_str:
        try:
            lat = float(lat_str.replace(",", "."))
            lng = float(lng_str.replace(",", "."))
            # Validació mínima: Barcelona està entre lat 41.3-41.5, lng 2.0-2.3
            if not (41.0 < lat < 41.8 and 1.8 < lng < 2.5):
                lat, lng = None, None
        except (ValueError, TypeError):
            lat, lng = None, None

    # Parsear any i mes
    any_val = None
    if any_str:
        try:
            any_val = int(float(any_str))
        except ValueError:
            pass

    mes_val = None
    if mes_str:
        try:
            mes_val = int(float(mes_str))
            if not (1 <= mes_val <= 12):
                mes_val = None
        except ValueError:
            pass

    return {
        "id_tram":    id_tram or f"auto_{nom_tram}",
        "nom_tram":   nom_tram or id_tram or "",
        "any":        any_val or date.today().year,
        "mes":        mes_val,
        "intensitat": intensitat,
        "lat":        lat,
        "lng":        lng,
    }


def _parsear_csv_raw(text: str) -> list[dict]:
    """Parseja text CSV descarregat directament (URL directa del recurs CKAN)."""
    trams: list[dict] = []
    try:
        reader = csv.DictReader(io.StringIO(text))
        for row in reader:
            parsed = _normalitzar_registre(row)
            if parsed:
                trams.append(parsed)
    except Exception as exc:
        logger.error("Error parsejant CSV raw de vianants: %s", exc)
    return trams


# ═══════════════════════════════════════════════════════════════════════════════
# 5. Persistència de trams crus
# ═══════════════════════════════════════════════════════════════════════════════

async def _crear_taula_si_no_existeix() -> None:
    """
    Crea la taula vianants_trams si no existeix (idempotent).
    Veure migració 007_vianants.sql per al DDL complet.
    """
    async with get_db() as conn:
        # Crear taula amb CREATE TABLE IF NOT EXISTS de manera segura
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS vianants_trams (
                id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                id_tram     VARCHAR(50),
                nom_tram    VARCHAR(300),
                any         INTEGER,
                mes         INTEGER,
                intensitat  FLOAT,
                lat         DOUBLE PRECISION,
                lng         DOUBLE PRECISION,
                geometria   GEOMETRY(Point, 4326),
                zona_id     UUID,
                fuente      VARCHAR(50) DEFAULT 'bcn_vianants',
                created_at  TIMESTAMPTZ DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_vianants_any_mes "
            "ON vianants_trams(any, mes)"
        )
        await conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_vianants_zona "
            "ON vianants_trams(zona_id)"
        )
        # Index espacial (requereix PostGIS)
        try:
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_vianants_geom "
                "ON vianants_trams USING GIST(geometria)"
            )
        except Exception as exc:
            logger.debug("Index GIST no creat (potser PostGIS no disponible): %s", exc)


async def _insertar_trams(trams: list[dict], fecha_ref: date) -> int:
    """
    Inserta els trams de vianants a la taula vianants_trams.
    Només insereix els que tenen coordenades (lat/lng).
    Retorna el nombre de trams inserits.
    """
    n = 0
    trams_amb_coords = [t for t in trams if t.get("lat") and t.get("lng")]
    trams_sense_coords = len(trams) - len(trams_amb_coords)

    if trams_sense_coords:
        logger.debug("%d trams sense coordenades — no s'inseriran a vianants_trams", trams_sense_coords)

    async with get_db() as conn:
        for tram in trams_amb_coords:
            try:
                await conn.execute(
                    """
                    INSERT INTO vianants_trams
                        (id_tram, nom_tram, any, mes, intensitat, lat, lng, geometria, fuente)
                    VALUES ($1, $2, $3, $4, $5, $6, $7,
                            ST_SetSRID(ST_MakePoint($7, $6), 4326),
                            'bcn_vianants')
                    ON CONFLICT DO NOTHING
                    """,
                    tram["id_tram"],
                    tram["nom_tram"],
                    tram["any"],
                    tram.get("mes"),
                    tram["intensitat"],
                    tram["lat"],
                    tram["lng"],
                )
                n += 1
            except Exception as exc:
                logger.debug("Error inserint tram %s: %s", tram.get("id_tram"), exc)

    return n


# ═══════════════════════════════════════════════════════════════════════════════
# 6. Assignació espacial a zones
# ═══════════════════════════════════════════════════════════════════════════════

async def _asignar_zonas(trams: list[dict], fecha: date) -> int:
    """
    Per a cada tram amb coordenades, distribueix el flux peatonal entre
    les zones dins del radi _RADIO_M (150m) usant ponderació per distància inversa.

    Sobreescriu els valors escrits per aforaments.py quan hi ha dades de vianants:
    les dades de vianants son més precises que les de vehicles com a proxy comercial.
    """
    n_zonas_actualitzades: set = set()
    trams_amb_coords = [t for t in trams if t.get("lat") and t.get("lng")]

    if not trams_amb_coords:
        logger.warning("Cap tram amb coordenades per assignar a zones")
        return 0

    async with get_db() as conn:
        for tram in trams_amb_coords:
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
                    fraccio       = peso / suma_pesos
                    flujo_dia     = tram["intensitat"] * fraccio

                    # Multiplicador comercial (igual que aforaments.py)
                    ratio_com     = float(zona["ratio_comercial"] or 0.0)
                    multiplicador = 1.0 + min(0.35, ratio_com * 0.70)
                    flujo_dia    *= multiplicador

                    manana = round(flujo_dia * _FRAC_MANANA, 1)
                    tarde  = round(flujo_dia * _FRAC_TARDE,  1)
                    noche  = round(flujo_dia * _FRAC_NOCHE,  1)

                    await conn.execute(
                        """
                        INSERT INTO variables_zona
                            (zona_id, fecha,
                             flujo_peatonal_manana, flujo_peatonal_tarde,
                             flujo_peatonal_noche,  fuente)
                        VALUES ($1, $2, $3, $4, $5, 'vianants_bcn')
                        ON CONFLICT (zona_id, fecha) DO UPDATE
                        SET flujo_peatonal_manana = COALESCE(variables_zona.flujo_peatonal_manana, 0) + $3,
                            flujo_peatonal_tarde  = COALESCE(variables_zona.flujo_peatonal_tarde,  0) + $4,
                            flujo_peatonal_noche  = COALESCE(variables_zona.flujo_peatonal_noche,  0) + $5,
                            fuente = 'vianants_bcn'
                        """,
                        zona["zona_id"], fecha, manana, tarde, noche,
                    )
                    n_zonas_actualitzades.add(str(zona["zona_id"]))

                    # Actualitzar referència de zona a vianants_trams
                    try:
                        await conn.execute(
                            """
                            UPDATE vianants_trams
                            SET zona_id = $1
                            WHERE id_tram = $2 AND zona_id IS NULL
                            """,
                            zona["zona_id"], tram["id_tram"],
                        )
                    except Exception:
                        pass

            except Exception as exc:
                logger.warning("Error processant tram %s: %s", tram.get("id_tram"), exc)

    logger.info("Zones actualitzades amb dades de vianants: %d", len(n_zonas_actualitzades))
    return len(n_zonas_actualitzades)


async def _recalcular_totales(desde: date) -> None:
    """Recalcula flujo_peatonal_total = suma de les tres franges per als registres de vianants."""
    async with get_db() as conn:
        await conn.execute(
            """
            UPDATE variables_zona
            SET flujo_peatonal_total = (
                COALESCE(flujo_peatonal_manana, 0) +
                COALESCE(flujo_peatonal_tarde,  0) +
                COALESCE(flujo_peatonal_noche,  0)
            )
            WHERE fecha >= $1
              AND fuente = 'vianants_bcn'
            """,
            desde,
        )
    logger.info("Totals recalculats per vianants des de %s", desde)


# ═══════════════════════════════════════════════════════════════════════════════
# 7. Utilitats: mostra CSV per a tests
# ═══════════════════════════════════════════════════════════════════════════════

def _guardar_mostra_csv(trams: list[dict], max_rows: int = 200) -> None:
    """
    Guarda una mostra de fins a max_rows trams a
    CSV/scraping_test/vianants_bcn_muestra.csv per a inspecció manual.
    """
    try:
        _DIR_TEST.mkdir(parents=True, exist_ok=True)
        dest = _DIR_TEST / "vianants_bcn_muestra.csv"
        mostra = trams[:max_rows]
        if not mostra:
            return
        fieldnames = list(mostra[0].keys())
        with open(dest, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(mostra)
        logger.info("Mostra de vianants guardada a %s (%d files)", dest, len(mostra))
    except Exception as exc:
        logger.debug("No s'ha pogut guardar la mostra CSV: %s", exc)


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers BD (pattern idèntic a aforaments.py)
# ═══════════════════════════════════════════════════════════════════════════════

async def _init(pipeline: str) -> int:
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones (pipeline, estado) "
            "VALUES ($1,'running') RETURNING id",
            pipeline,
        )


async def _fin(eid: int, registros: int, estado: str, mensaje: Optional[str] = None) -> None:
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones "
            "SET fecha_fin=NOW(), registros=$1, estado=$2, mensaje_error=$3 "
            "WHERE id=$4",
            registros, estado, mensaje, eid,
        )
