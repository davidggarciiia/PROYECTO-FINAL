"""
scoring/transporte_score.py — Scoring de transporte enriquecido para GeoRetail BCN.

Sistema de puntuación multifactor que evalúa el acceso a transporte de una zona
combinando tres dimensiones:

  1. Score de tránsito (peso 80%, o 65% si negocio orientado a movilidad activa)
     Pondera cada línea de transporte cercana según:
       - Calidad intrínseca del tipo (metro > FGC > tram > bus ortogonal > bus > Rodalies)
       - Decaimiento exponencial con la distancia a la parada más cercana
       - Factor de frecuencia real (o fallback por tipo)
       - Radio de búsqueda diferenciado: la gente camina más para mejor transporte
     Bonus intermodal por diversidad de tipos de alta calidad (hasta +20 pts).
     Penalización si la única opción disponible es transporte de baja fiabilidad.

  2. Score de movilidad activa (peso 20%, o 35% para negocios ecológicos/ciclistas)
     Combina estaciones Bicing en radio 400 m y presencia de carril bici en 200 m.

  3. Bonus PMR (opcional, hasta +8 pts)
     Activado por tags clientela_movilidad_reducida / salud_tercera_edad.
     Proporcional al % de paradas accesibles en radio 500 m.

Calidades de referencia BCN 2025:
  metro 1.0 · FGC 0.85 · tram 0.75 · bus ortogonal 0.70 · bus 0.35 · Rodalies 0.15

El máximo teórico en Barcelona (~8.0 unidades ponderadas) se normaliza a 100
mediante el factor x12.5.
"""
from __future__ import annotations

import logging
import math
from typing import Optional

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Constantes exportadas (usadas también por tests y el pipeline)
# ---------------------------------------------------------------------------

_CALIDAD_TIPO: dict[str, float] = {
    "metro":         1.00,
    "fgc":           0.85,
    "tram":          0.75,
    "bus_ortogonal": 0.70,
    "bus":           0.35,
    "rodalies":      0.15,
}

# Alias público exportado para tests
CALIDAD_TIPO = _CALIDAD_TIPO

_RADIO_TIPO: dict[str, int] = {
    "metro":         600,
    "fgc":           600,
    "tram":          500,
    "bus_ortogonal": 450,
    "bus":           300,
    "rodalies":      400,
}

_FREQ_FALLBACK: dict[str, float] = {
    "metro":         4.0,
    "fgc":           7.0,
    "tram":          8.0,
    "bus_ortogonal": 7.0,
    "bus":           15.0,
    "rodalies":      20.0,
}

# Prefijos de la red ortogonal de Barcelona (H12, V15, D20, X3…)
_PREFIJOS_ORTOGONAL = frozenset("HVDX")

# Factor de normalización: máximo teórico BCN ~8.0 → × 12.5 = 100
_FACTOR_NORM_TRANSIT = 12.5

# Bonus intermodal según número de tipos de alta calidad presentes
_BONUS_INTERMODAL: dict[int, float] = {0: 0.0, 1: 0.0, 2: 8.0, 3: 15.0, 4: 20.0}

# Umbral para considerar un tipo "calidad alta"
_UMBRAL_CALIDAD_ALTA = 0.70

# Radio máximo de captura SQL (filtrado fino por tipo se hace en Python)
_RADIO_SQL_MAX = 700


# ---------------------------------------------------------------------------
# Query SQL
# ---------------------------------------------------------------------------

_SQL_TRANSPORTE = """
SELECT
    lt.tipo,
    lt.codigo,
    lt.id AS linea_id,
    MIN(ST_Distance(
        pt.geometria::geography,
        ST_Centroid(z.geometria)::geography
    ))::float AS dist_min_m,
    COALESCE(AVG(ft.frecuencia_min), NULL)::float AS freq_media_min
FROM paradas_transporte pt
JOIN paradas_lineas pl ON pl.parada_id = pt.id
JOIN lineas_transporte lt ON lt.id = pl.linea_id
JOIN zonas z ON z.id = $1
LEFT JOIN frecuencias_transporte ft
    ON ft.linea_id = lt.id
    AND ft.dia_tipo = 'laborable'
    AND ft.franja = $2
WHERE ST_DWithin(
    pt.geometria::geography,
    ST_Centroid(z.geometria)::geography,
    {radio}
)
GROUP BY lt.tipo, lt.codigo, lt.id
"""

_SQL_BICING = """
SELECT COUNT(*)::int AS num_estaciones
FROM estaciones_bicing eb
JOIN zonas z ON z.id = $1
WHERE ST_DWithin(
    eb.geometria::geography,
    ST_Centroid(z.geometria)::geography,
    400
)
"""

_SQL_CARRIL_BICI = """
SELECT EXISTS(
    SELECT 1 FROM carriles_bici cb
    JOIN zonas z ON z.id = $1
    WHERE ST_DWithin(
        cb.geometria::geography,
        ST_Centroid(z.geometria)::geography,
        200
    )
)::boolean AS tiene_carril
"""

_SQL_PMR = """
SELECT
    COUNT(CASE WHEN pt.accesible_pmr = TRUE THEN 1 END)::int AS accesibles,
    COUNT(*)::int AS total
FROM paradas_transporte pt
JOIN paradas_lineas pl ON pl.parada_id = pt.id
JOIN lineas_transporte lt ON lt.id = pl.linea_id
JOIN zonas z ON z.id = $1
WHERE ST_DWithin(pt.geometria::geography, ST_Centroid(z.geometria)::geography, 500)
"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_subtipo(tipo: str, codigo: str) -> str:
    """
    Devuelve el subtipo efectivo de una línea.

    Un bus cuyo código empieza por H, V, D o X pertenece a la red ortogonal
    de Barcelona y recibe una calidad superior (0.70 vs 0.35).

    Args:
        tipo:   Tipo de línea tal como aparece en lineas_transporte.tipo.
        codigo: Código de la línea (ej. "H12", "V15", "D20", "X3", "57").

    Returns:
        Subtipo efectivo: "bus_ortogonal" o el tipo original.
    """
    if tipo == "bus" and codigo and codigo[0].upper() in _PREFIJOS_ORTOGONAL:
        return "bus_ortogonal"
    return tipo


def _franja_de_tags(idea_tags: list[str]) -> str:
    """Determina la franja horaria a consultar según los tags del negocio."""
    if idea_tags and "horario_nocturno" in idea_tags:
        return "noche"
    return "manana"


def _get_db():
    """Importación lazy de get_db para evitar ciclos en startup."""
    from db.conexion import get_db
    return get_db


# ---------------------------------------------------------------------------
# Cálculo del score de tránsito
# ---------------------------------------------------------------------------

def _calcular_score_transit(filas: list) -> tuple[float, dict]:
    """
    Procesa las filas de la query principal y devuelve (score_transit, detalles).

    Args:
        filas: Lista de registros asyncpg con campos
               tipo, codigo, linea_id, dist_min_m, freq_media_min.

    Returns:
        Tupla con el score de tránsito (0-100) y el diccionario de detalles.
    """
    score_ponderado = 0.0
    tipos_calidad_alta: set[str] = set()
    tipos_baja_calidad: set[str] = set()
    tipos_unicos: set[str] = set()
    top_lineas: list[dict] = []

    for fila in filas:
        tipo = fila["tipo"] or "bus"
        codigo = fila["codigo"] or ""
        dist_min_m: float = fila["dist_min_m"] or 0.0
        freq_media_min: Optional[float] = fila["freq_media_min"]

        subtipo = _get_subtipo(tipo, codigo)
        calidad = _CALIDAD_TIPO.get(subtipo, 0.35)
        radio_max = _RADIO_TIPO.get(subtipo, 400)

        # Filtrar líneas cuya parada más cercana supera el radio de su tipo
        if dist_min_m > radio_max:
            continue

        tipos_unicos.add(subtipo)

        # Decaimiento exponencial por distancia
        factor_dist = math.exp(-dist_min_m / 250.0)

        # Factor de frecuencia: 8 min = sin penalización; 30 min = 0.27
        freq = freq_media_min if freq_media_min else _FREQ_FALLBACK.get(subtipo, 15.0)
        factor_freq = min(1.0, 8.0 / freq)

        aporte = calidad * factor_dist * factor_freq
        score_ponderado += aporte
        top_lineas.append(
            {
                "codigo": codigo,
                "tipo": tipo,
                "subtipo": subtipo,
                "distancia_m": round(float(dist_min_m), 1),
                "frecuencia_media_min": round(float(freq), 1) if freq is not None else None,
                "aporte": round(float(aporte), 4),
            }
        )

        if calidad >= _UMBRAL_CALIDAD_ALTA:
            tipos_calidad_alta.add(subtipo)
        if calidad < 0.4:
            tipos_baja_calidad.add(subtipo)

    # Normalizar a 0-100
    score_transit_base = min(100.0, score_ponderado * _FACTOR_NORM_TRANSIT)

    # Bonus intermodal (solo tipos con calidad >= 0.70)
    bonus_intermodal = _BONUS_INTERMODAL.get(min(len(tipos_calidad_alta), 4), 20.0)

    # Penalización si la única opción disponible es de baja calidad
    penalizacion = 0.0
    if tipos_unicos and tipos_unicos == tipos_baja_calidad and len(tipos_unicos) == 1:
        penalizacion = -15.0

    score_transit = min(100.0, max(0.0, score_transit_base + bonus_intermodal + penalizacion))

    detalles = {
        "num_lineas_calidad": round(score_ponderado, 4),
        "tipos_calidad_alta": sorted(tipos_calidad_alta),
        "bonus_intermodal": bonus_intermodal,
        "penalizacion_dependencia": penalizacion,
        "top_lineas": sorted(top_lineas, key=lambda row: row["aporte"], reverse=True)[:5],
    }
    return round(score_transit, 1), detalles


# ---------------------------------------------------------------------------
# Función principal
# ---------------------------------------------------------------------------

async def calcular_score_transporte(
    zona_id: str,
    idea_tags: Optional[list[str]] = None,
    perfil_negocio: Optional[dict] = None,
) -> dict:
    """
    Calcula el score de transporte multifactor para una zona de Barcelona.

    Combina transporte público (tránsito) y movilidad activa (Bicing + carril
    bici) en un score final 0-100. Los pesos se ajustan según los tags del
    negocio: negocios ecológicos o para ciclistas elevan el peso de movilidad
    activa del 20% al 35%.

    Un bonus PMR opcional (hasta +8 pts) se añade si el negocio atiende a
    personas con movilidad reducida o tercera edad.

    Args:
        zona_id:   Identificador de la zona en la tabla `zonas`.
        idea_tags: Tags semánticos del negocio extraídos por el LLM (pueden
                   ser None). Influyen en franja horaria, pesos y bonus PMR.

    Returns:
        Diccionario con:
          - score_transporte (float, 0-100): score final combinado.
          - score_transit (float, 0-100): sub-score transporte público.
          - score_movilidad_activa (float, 0-100): sub-score movilidad activa.
          - detalles (dict): métricas intermedias para debug y SHAP.

    Raises:
        No lanza excepciones externas; captura errores de DB y retorna score 0.
    """
    tags = idea_tags or []
    franja = _franja_de_tags(tags)
    get_db = _get_db()

    # ── 1. Score de tránsito ─────────────────────────────────────────────────
    score_transit = 0.0
    detalles_transit: dict = {
        "num_lineas_calidad": 0.0,
        "tipos_calidad_alta": [],
        "bonus_intermodal": 0.0,
        "penalizacion_dependencia": 0.0,
    }

    try:
        async with get_db() as conn:
            filas = await conn.fetch(
                _SQL_TRANSPORTE.format(radio=_RADIO_SQL_MAX),
                zona_id,
                franja,
            )
        if filas:
            score_transit, detalles_transit = _calcular_score_transit(filas)
        else:
            logger.debug("transporte_score: sin paradas en radio %dm para zona %s", _RADIO_SQL_MAX, zona_id)
    except Exception as exc:
        logger.error("transporte_score: error query tránsito zona=%s: %s", zona_id, exc)

    # ── 2. Score de movilidad activa ─────────────────────────────────────────
    num_bicing = 0
    tiene_carril = False

    try:
        async with get_db() as conn:
            fila_bicing = await conn.fetchrow(_SQL_BICING, zona_id)
            num_bicing = fila_bicing["num_estaciones"] if fila_bicing else 0
    except Exception as exc:
        # Captura específica de tabla inexistente y errores genéricos
        _es_tabla_faltante = "UndefinedTable" in type(exc).__name__ or "42P01" in str(exc)
        if _es_tabla_faltante:
            logger.debug("transporte_score: tabla estaciones_bicing no existe")
        else:
            logger.warning("transporte_score: error query bicing zona=%s: %s", zona_id, exc)
        num_bicing = 0

    try:
        async with get_db() as conn:
            fila_carril = await conn.fetchrow(_SQL_CARRIL_BICI, zona_id)
            tiene_carril = bool(fila_carril["tiene_carril"]) if fila_carril else False
    except Exception as exc:
        _es_tabla_faltante = "UndefinedTable" in type(exc).__name__ or "42P01" in str(exc)
        if _es_tabla_faltante:
            logger.debug("transporte_score: tabla carriles_bici no existe")
        else:
            logger.warning("transporte_score: error query carril bici zona=%s: %s", zona_id, exc)
        tiene_carril = False

    score_bicing = min(100.0, (num_bicing or 0) * 15.0)  # 7+ estaciones → 100
    score_carril = 30.0 if tiene_carril else 0.0
    score_movilidad = min(100.0, score_bicing * 0.7 + score_carril * 0.3)
    score_movilidad = round(score_movilidad, 1)

    # ── 3. Pesos combinados ──────────────────────────────────────────────────
    peso_transit = 0.80
    peso_movilidad = 0.20

    if tags and any(t in tags for t in ("clientela_ciclista", "ecologico_sostenible")):
        peso_transit = 0.65
        peso_movilidad = 0.35

    score_transporte = round(
        score_transit * peso_transit + score_movilidad * peso_movilidad, 1
    )

    # ── 4. Bonus PMR ─────────────────────────────────────────────────────────
    if tags and any(t in tags for t in ("clientela_movilidad_reducida", "salud_tercera_edad")):
        try:
            async with get_db() as conn:
                fila_pmr = await conn.fetchrow(_SQL_PMR, zona_id)
            if fila_pmr:
                accesibles: int = fila_pmr["accesibles"] or 0
                total: int = fila_pmr["total"] or 0
                pct_pmr = accesibles / max(total, 1)
                bonus_pmr = round(pct_pmr * 8.0, 1)
                score_transporte = min(100.0, score_transporte + bonus_pmr)
                score_transporte = round(score_transporte, 1)
        except Exception as exc:
            logger.warning("transporte_score: error query PMR zona=%s: %s", zona_id, exc)

    return {
        "score_transporte": score_transporte,
        "score_transit": score_transit,
        "score_movilidad_activa": score_movilidad,
        "detalles": {
            **detalles_transit,
            "num_bicing_400m": num_bicing,
            "tiene_carril_bici": tiene_carril,
        },
    }
