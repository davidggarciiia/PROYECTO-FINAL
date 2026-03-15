"""
test_pipelines.py — Diagnóstico de conectividad de todos los pipelines.

Ejecutar sin Docker:
    cd backend && python test_pipelines.py

Comprueba:
  1. Open Data BCN (CKAN API) — distritos, barrios, aforaments, demografía, precios
  2. INE API — tabla 9697 (educación) y DIRCE (rotación)
  3. Idescat API — indicadores mun:080193
  4. Google Maps Places API
  5. Foursquare Places API
  6. TMB API — líneas de bus
  7. ScrapingBee — test de conectividad
"""

import os
import sys
import json
import time
import requests
from datetime import datetime

# Cargar .env manualmente (sin pydantic-settings)
def load_env(path=".env"):
    env = {}
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    env[k.strip()] = v.strip()
    except FileNotFoundError:
        print(f"⚠️  {path} no encontrado, usando variables de entorno del sistema")
    return env

ENV = load_env(os.path.join(os.path.dirname(__file__), "../.env"))

def get(key, default=""):
    return ENV.get(key) or os.environ.get(key, default)

# ─────────────────────────────────────────────────────────────────────────────
RESULTS = []

def check(name, ok, detail=""):
    icon = "✅" if ok else "❌"
    RESULTS.append((name, ok, detail))
    print(f"  {icon} {name}: {detail}")

def section(title):
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")

# ─── 1. Open Data BCN ─────────────────────────────────────────────────────────
section("1. Open Data BCN (CKAN API)")

BASE = "https://opendata-ajuntament.barcelona.cat/data/api/action"

try:
    r = requests.get(f"{BASE}/package_show", params={"id": "districtes-de-barcelona"}, timeout=10)
    if r.status_code == 200 and r.json().get("success"):
        resources = r.json()["result"]["resources"]
        check("Distritos BCN", True, f"{len(resources)} recursos disponibles")
    else:
        check("Distritos BCN", False, f"HTTP {r.status_code}")
except Exception as e:
    check("Distritos BCN", False, str(e))

try:
    r = requests.get(f"{BASE}/package_show", params={"id": "barris-de-barcelona"}, timeout=10)
    ok = r.status_code == 200 and r.json().get("success")
    check("Barrios BCN", ok, f"HTTP {r.status_code}")
except Exception as e:
    check("Barrios BCN", False, str(e))

try:
    r = requests.get(f"{BASE}/package_show", params={"id": "aforaments-eixos-comercials-bcn"}, timeout=10)
    ok = r.status_code == 200 and r.json().get("success")
    check("Aforaments (flujo peatonal)", ok, f"HTTP {r.status_code}")
except Exception as e:
    check("Aforaments", False, str(e))

try:
    r = requests.get(f"{BASE}/package_show", params={"id": "pad_mdbas"}, timeout=10)
    ok = r.status_code == 200 and r.json().get("success")
    check("Padrón (demografía)", ok, f"HTTP {r.status_code}")
except Exception as e:
    check("Padrón (demografía)", False, str(e))

try:
    r = requests.get(f"{BASE}/package_show", params={"id": "renda-disponible-llars-bcn"}, timeout=10)
    ok = r.status_code == 200 and r.json().get("success")
    check("Renta disponible hogares", ok, f"HTTP {r.status_code}")
except Exception as e:
    check("Renta disponible hogares", False, str(e))

try:
    r = requests.get(f"{BASE}/package_show", params={"id": "cens-locals-planta-baixa-barcelona"}, timeout=10)
    ok = r.status_code == 200 and r.json().get("success")
    check("Cens Locals Planta Baixa", ok, f"HTTP {r.status_code}")
except Exception as e:
    check("Cens Locals Planta Baixa", False, str(e))

try:
    r = requests.get(f"{BASE}/package_show", params={"id": "zones-verdes-catalogades"}, timeout=10)
    ok = r.status_code == 200 and r.json().get("success")
    check("Zones Verdes", ok, f"HTTP {r.status_code}")
except Exception as e:
    check("Zones Verdes", False, str(e))

try:
    r = requests.get(f"{BASE}/package_show", params={"id": "incidents-gub-barcelona"}, timeout=10)
    ok = r.status_code == 200 and r.json().get("success")
    check("Incidents GUB", ok, f"HTTP {r.status_code}")
except Exception as e:
    check("Incidents GUB", False, str(e))

# ─── 2. INE API ───────────────────────────────────────────────────────────────
section("2. INE API (servicios.ine.es)")

try:
    r = requests.get("https://servicios.ine.es/wstempus/js/ES/OPERACIONES_DISPONIBLES", timeout=15)
    ok = r.status_code == 200
    count = len(r.json()) if ok else 0
    check("INE — operaciones disponibles", ok, f"{count} operaciones" if ok else f"HTTP {r.status_code}")
except Exception as e:
    check("INE — operaciones disponibles", False, str(e))

try:
    # Tabla 9697: Padrón Continuo educación — municipio Barcelona (08019)
    r = requests.get(
        "https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/9697",
        params={"nult": "1"},
        timeout=15
    )
    ok = r.status_code == 200 and len(r.json()) > 0
    count = len(r.json()) if ok else 0
    check("INE — tabla 9697 (educación padrón)", ok, f"{count} series" if ok else f"HTTP {r.status_code}")
except Exception as e:
    check("INE — tabla 9697 (educación padrón)", False, str(e))

try:
    # DIRCE altas empresas provincia Barcelona (08)
    r = requests.get(
        "https://servicios.ine.es/wstempus/js/ES/DATOS_METADATAOPERACION/DIRCE",
        timeout=15
    )
    ok = r.status_code == 200
    check("INE — DIRCE metadata", ok, f"HTTP {r.status_code}")
except Exception as e:
    check("INE — DIRCE metadata", False, str(e))

# ─── 3. Idescat API ───────────────────────────────────────────────────────────
section("3. Idescat API (api.idescat.cat)")

try:
    r = requests.get(
        "https://api.idescat.cat/emex/v1/dades.json",
        params={"id": "f172,f192", "lang": "es", "i": "mun:080193"},
        timeout=15
    )
    ok = r.status_code == 200
    data = r.json() if ok else {}
    has_data = bool(data.get("fitxes", {}).get("fitxa"))
    check("Idescat — densidad + edad media (Barcelona)", ok and has_data,
          "datos recibidos" if has_data else f"HTTP {r.status_code}, sin datos")
except Exception as e:
    check("Idescat — indicadores", False, str(e))

try:
    r = requests.get(
        "https://api.idescat.cat/emex/v1/dades.json",
        params={"id": "f231,f261", "lang": "es", "i": "mun:080193"},
        timeout=15
    )
    ok = r.status_code == 200
    check("Idescat — paro + renta (Barcelona)", ok, f"HTTP {r.status_code}")
except Exception as e:
    check("Idescat — paro + renta", False, str(e))

# ─── 4. Google Maps API ───────────────────────────────────────────────────────
section("4. Google Maps / Places API")

GOOGLE_KEY = get("GOOGLE_MAPS_API_KEY")
if not GOOGLE_KEY:
    check("Google Maps", False, "GOOGLE_MAPS_API_KEY no configurada")
else:
    try:
        r = requests.get(
            "https://maps.googleapis.com/maps/api/place/nearbysearch/json",
            params={
                "location": "41.3874,2.1686",
                "radius": 200,
                "type": "restaurant",
                "key": GOOGLE_KEY
            },
            timeout=15
        )
        data = r.json()
        ok = r.status_code == 200 and data.get("status") in ("OK", "ZERO_RESULTS")
        status = data.get("status", r.status_code)
        count = len(data.get("results", []))
        check("Google Places nearby", ok, f"status={status}, {count} resultados")
        if data.get("error_message"):
            print(f"     ⚠️  {data['error_message']}")
    except Exception as e:
        check("Google Places nearby", False, str(e))

# ─── 5. Foursquare API ────────────────────────────────────────────────────────
section("5. Foursquare Places API")

FSQ_KEY = get("FOURSQUARE_API_KEY")
if not FSQ_KEY:
    check("Foursquare", False, "FOURSQUARE_API_KEY no configurada")
else:
    try:
        r = requests.get(
            "https://api.foursquare.com/v3/places/search",
            headers={"Authorization": FSQ_KEY, "Accept": "application/json"},
            params={"ll": "41.3874,2.1686", "radius": 200, "categories": "13065", "limit": 5},
            timeout=15
        )
        ok = r.status_code == 200
        count = len(r.json().get("results", [])) if ok else 0
        check("Foursquare nearby search", ok, f"HTTP {r.status_code}, {count} resultados")
    except Exception as e:
        check("Foursquare nearby search", False, str(e))

# ─── 6. TMB API ───────────────────────────────────────────────────────────────
section("6. TMB API (transporte Barcelona)")

TMB_ID  = get("TMB_APP_ID")
TMB_KEY = get("TMB_APP_KEY")

if not TMB_KEY:
    check("TMB", False, "TMB_APP_KEY no configurada")
elif not TMB_ID:
    check("TMB", False, "TMB_APP_ID no configurada (solo se tiene el KEY)")
else:
    try:
        r = requests.get(
            "https://api.tmb.cat/v1/transit/linies/bus",
            params={"app_id": TMB_ID, "app_key": TMB_KEY},
            timeout=15
        )
        ok = r.status_code == 200
        count = len(r.json().get("features", [])) if ok else 0
        check("TMB — líneas de bus", ok, f"HTTP {r.status_code}, {count} líneas")
    except Exception as e:
        check("TMB — líneas de bus", False, str(e))

# ─── 7. ScrapingBee ───────────────────────────────────────────────────────────
section("7. ScrapingBee (scraping anti-bot)")

SB_KEY = get("SCRAPINGBEE_API_KEY")
if not SB_KEY:
    check("ScrapingBee", False, "SCRAPINGBEE_API_KEY no configurada")
else:
    try:
        r = requests.get(
            "https://app.scrapingbee.com/api/v1/",
            params={
                "api_key": SB_KEY,
                "url": "https://httpbin.org/get",
                "render_js": "false"
            },
            timeout=30
        )
        ok = r.status_code == 200
        check("ScrapingBee — test scrape", ok, f"HTTP {r.status_code}")
    except Exception as e:
        check("ScrapingBee — test scrape", False, str(e))

# ─── Resumen ──────────────────────────────────────────────────────────────────
section("RESUMEN")

ok_count  = sum(1 for _, ok, _ in RESULTS if ok)
fail_count = sum(1 for _, ok, _ in RESULTS if not ok)

print(f"\n  Total: {len(RESULTS)} comprobaciones")
print(f"  ✅ OK:    {ok_count}")
print(f"  ❌ Fallo: {fail_count}")

if fail_count:
    print("\n  Fallos:")
    for name, ok, detail in RESULTS:
        if not ok:
            print(f"    • {name}: {detail}")

print()
sys.exit(0 if fail_count == 0 else 1)
