"""
pipelines/vcity.py — Flujo peatonal diario por zona desde Open Data BCN.

Fuente primaria: dataset "mobilitat-persones" del CKAN de BCN Open Data.
Si no disponible: usa flujo_peatonal_total (vianants_bcn) como proxy.

Actualiza: variables_zona.vcity_flujo_peatonal

Frecuencia: mensual (día 12, 04:00)
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional

import httpx

from db.conexion import get_db

logger = logging.getLogger(__name__)

# ── BCN Open Data CKAN ─────────────────────────────────────────────────────────
_CKAN_BASE   = "https://opendata-ajuntament.barcelona.cat/data/api/action"
_TIMEOUT_S   = 30

# Headers estándar para peticiones al portal Open Data BCN
_HEADERS = {
    "User-Agent": "GeoRetail/1.0 (georetail.app — contacto: info@georetail.app)",
    "Accept": "application/json",
}


# ═══════════════════════════════════════════════════════════════════════════════
# Punto de entrada público
# ═══════════════════════════════════════════════════════════════════════════════

async def ejecutar() -> dict:
    """
    Pipeline de flujo peatonal:
      1. Buscar dataset "mobilitat persones" en CKAN BCN Open Data
      2. Si hay datos → agregar por zona y persistir
      3. Si no hay datos → fallback: copiar flujo_peatonal_total (vianants_bcn)
    """
    eid = await _init()
    try:
        registros = await _fetch_mobilitat_bcn()

        if registros:
            zona_flows = _agregar_por_zona(registros)
            logger.info("Mobilitat BCN: %d registros → %d zonas", len(registros), len(zona_flows))
            n = await _persistir(zona_flows)
            await _fin(eid, n, "ok")
            return {
                "zonas_actualizadas": n,
                "fuente": "mobilitat_bcn_opendata",
                "registros_fuente": len(registros),
            }

        # Fallback: vianants_bcn ya cargado por pipelines/vianants.py
        logger.warning(
            "Mobilitat BCN no disponible en CKAN. "
            "Usando flujo_peatonal_total (vianants_bcn) como proxy."
        )
        n = await _fallback_desde_vianants()
        await _fin(eid, n, "ok", "mobilitat_bcn no disponible — proxy vianants usado")
        return {
            "zonas_actualizadas": n,
            "fuente": "vianants_proxy",
            "registros_fuente": 0,
        }

    except Exception as exc:
        logger.error("Pipeline vcity ERROR: %s", exc, exc_info=True)
        await _fin(eid, 0, "error", str(exc))
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Fetch Open Data BCN — mobilitat persones
# ═══════════════════════════════════════════════════════════════════════════════

async def _fetch_mobilitat_bcn() -> list[dict]:
    """
    Busca el dataset "mobilitat persones" en el CKAN de BCN Open Data y
    descarga los registros del primer recurso CSV/JSON encontrado.

    Devuelve lista de dicts con al menos: zona_id (o nom_barri), valor_mobilitat.
    Devuelve lista vacía si el dataset no existe o no tiene datos utilizables.
    """
    async with httpx.AsyncClient(timeout=_TIMEOUT_S, headers=_HEADERS) as client:
        # Paso 1: buscar el paquete por nombre
        try:
            search_url = (
                f"{_CKAN_BASE}/package_search"
                "?q=mobilitat+persones&rows=5"
            )
            resp = await client.get(search_url)
            resp.raise_for_status()
            result = resp.json()
        except Exception as exc:
            logger.warning("CKAN package_search error: %s", exc)
            return []

        packages = (result.get("result") or {}).get("results", [])
        if not packages:
            logger.info("CKAN: no se encontró dataset 'mobilitat persones'")
            return []

        # Paso 2: buscar el primer recurso con formato JSON o CSV
        resource_id: Optional[str] = None
        for pkg in packages:
            for resource in pkg.get("resources", []):
                fmt = (resource.get("format") or "").upper()
                if fmt in ("JSON", "CSV", "GEOJSON"):
                    resource_id = resource.get("id")
                    logger.info(
                        "CKAN resource encontrado: %s (pkg=%s, format=%s)",
                        resource_id, pkg.get("name"), fmt,
                    )
                    break
            if resource_id:
                break

        if not resource_id:
            logger.info("CKAN: ningún recurso JSON/CSV en los paquetes encontrados")
            return []

        # Paso 3: descargar registros con datastore_search
        try:
            ds_url = (
                f"{_CKAN_BASE}/datastore_search"
                f"?resource_id={resource_id}&limit=5000"
            )
            resp = await client.get(ds_url)
            resp.raise_for_status()
            ds_result = resp.json()
        except Exception as exc:
            logger.warning("CKAN datastore_search error (resource=%s): %s", resource_id, exc)
            return []

        records = (ds_result.get("result") or {}).get("records", [])
        logger.info("CKAN datastore_search: %d registros descargados", len(records))
        return records


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Agregación por zona
# ═══════════════════════════════════════════════════════════════════════════════

def _agregar_por_zona(records: list[dict]) -> dict[str, float]:
    """
    Agrega registros de mobilitat por zona/barri.

    El CKAN BCN usa campos variables según el dataset; se prueba un conjunto
    de nombres de columna habituales. Devuelve {zona_id_o_nombre: valor_medio}.
    """
    from collections import defaultdict

    acum: dict[str, list[float]] = defaultdict(list)

    for rec in records:
        # Intentar encontrar el identificador de zona
        zona_key = (
            rec.get("Codi_Barri")
            or rec.get("codi_barri")
            or rec.get("NOM_BARRI")
            or rec.get("nom_barri")
            or rec.get("zona_id")
            or rec.get("ZONA")
        )
        if not zona_key:
            continue

        # Intentar encontrar el valor de mobilitat/intensitat
        valor = None
        for campo in ("Valor", "valor", "VALOR", "intensitat", "count", "total_persones"):
            v = rec.get(campo)
            if v is not None:
                try:
                    valor = float(str(v).replace(",", ".").strip())
                    break
                except (ValueError, TypeError):
                    continue

        if valor is not None and valor >= 0:
            acum[str(zona_key)].append(valor)

    # Promediar por zona
    return {k: sum(vals) / len(vals) for k, vals in acum.items() if vals}


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Fallback: vianants_bcn → vcity_flujo_peatonal
# ═══════════════════════════════════════════════════════════════════════════════

async def _fallback_desde_vianants() -> int:
    """
    Copia flujo_peatonal_total → vcity_flujo_peatonal con fuente='vianants_proxy'
    para las zonas que ya tienen datos de vianants.py.

    Devuelve el número de filas actualizadas.
    """
    async with get_db() as conn:
        await conn.execute(
            """
            UPDATE variables_zona
            SET vcity_flujo_peatonal = flujo_peatonal_total
            WHERE flujo_peatonal_total IS NOT NULL
              AND flujo_peatonal_total > 0
            """
        )
        n = await conn.fetchval(
            """
            SELECT COUNT(*) FROM variables_zona
            WHERE vcity_flujo_peatonal IS NOT NULL
            """
        )
    n = int(n or 0)
    logger.info("Proxy vianants → vcity_flujo_peatonal: %d zonas", n)
    return n


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Persistencia
# ═══════════════════════════════════════════════════════════════════════════════

async def _persistir(zona_flows: dict[str, float]) -> int:
    """
    Guarda los flujos en variables_zona.vcity_flujo_peatonal usando UPSERT
    sobre (zona_id, fecha). Devuelve el número de zonas actualizadas.

    Nota: zona_flows puede usar código_barri o nombre de barrio como clave.
    Si la clave no hace match con zona_id numérico, se omite el registro.
    """
    if not zona_flows:
        return 0

    fecha = date.today()
    n = 0

    async with get_db() as conn:
        for zona_key, flujo in zona_flows.items():
            try:
                # Intentar interpretar la clave como zona_id numérico directamente
                zona_id: Optional[int] = None
                try:
                    zona_id = int(zona_key)
                except ValueError:
                    # Buscar por nombre de barrio en la tabla zonas
                    row = await conn.fetchrow(
                        "SELECT id FROM zonas WHERE LOWER(nombre) = LOWER($1) LIMIT 1",
                        zona_key,
                    )
                    if row:
                        zona_id = row["id"]

                if zona_id is None:
                    continue

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
                logger.debug("vcity: error persistiendo zona %s: %s", zona_key, exc)

    logger.info("vcity: %d zonas persistidas con vcity_flujo_peatonal", n)
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
