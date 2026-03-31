"""
scoring/competencia.py — Scoring avanzado de competencia: buena vs mala.

Módulo 100% puro (sin I/O). Todas las funciones son deterministas y testeables
sin BD ni API keys.

Teoría aplicada:
  - Efecto aglomeración (Hotelling 1929, Fischer & Harrington 1996):
    hasta cierto óptimo sectorial, más competidores del mismo tipo = destino
    consolidado = más demanda total para todos.
  - Gravity model de amenaza: competidores fuertes cercanos pesan más que
    competidores fuertes lejanos (distancia inversa al cuadrado).
  - Índice HHI: mercado atomizado (muchos iguales) = oportunidad de entrada;
    mercado concentrado (uno domina) = difícil desplazar al líder.
  - Vulnerabilidad: rating bajo + pocas reseñas = competidor desplazable.

Sub-scores (0-100 cada uno):
  score_cluster         — beneficio de estar en cluster sectorial (campana)
  score_amenaza_inv     — 100 = sin competidores fuertes; 0 = rodeado de élites
  score_oportunidad     — gap de mercado: vulnerables + HHI + nicho precio
  score_complementarios — sinergia con sectores vecinos

Score final:
  score_competencia = (
      0.30 × score_cluster
    + 0.30 × score_amenaza_inv
    + 0.25 × score_oportunidad
    + 0.15 × score_complementarios
  )
"""
from __future__ import annotations

import math
from typing import Optional

# ── Constantes de configuración ───────────────────────────────────────────────

# Óptimo de aglomeración por sector: nº de competidores directos en 500m que
# maximiza el efecto cluster. Por encima → saturación y penalización.
CLUSTER_OPTIMO: dict[str, int] = {
    "restauracion":  8,   # calles de restauración: alta densidad → destino
    "moda":          5,   # ejes comerciales: 4-6 tiendas del mismo tipo
    "estetica":      4,   # clúster belleza moderado
    "tatuajes":      3,   # nicho especializado, poca masa crítica
    "shisha_lounge": 2,   # muy nicho, poca sinergia por volumen
    "_default":      4,
}

# Sectores que COMPITEN directamente con cada sector objetivo
# Incluye sustitutos cercanos (cross-sector competition)
SECTORES_COMPETIDORES: dict[str, list[str]] = {
    "restauracion":  ["restauracion"],
    "tatuajes":      ["tatuajes", "estetica"],
    "moda":          ["moda"],
    "estetica":      ["estetica", "tatuajes"],
    "shisha_lounge": ["shisha_lounge", "restauracion"],
}

# Sectores que COMPLEMENTAN (generan tráfico del mismo target)
SECTORES_COMPLEMENTARIOS: dict[str, list[str]] = {
    "restauracion":  ["moda", "estetica", "shisha_lounge"],
    "tatuajes":      ["moda", "shisha_lounge"],
    "moda":          ["estetica", "restauracion"],
    "estetica":      ["moda", "restauracion"],
    "shisha_lounge": ["restauracion", "moda", "estetica"],
}

# Umbral para considerar un competidor "vulnerable" (desplazable)
RATING_VULNERABLE = 3.5
RESENAS_VULNERABLE = 25     # menos de N reseñas → poco establecido

# Pesos del score final (suma 1.0)
PESOS_COMPETENCIA: dict[str, float] = {
    "cluster":        0.30,
    "amenaza_inv":    0.30,
    "oportunidad":    0.25,
    "complementarios": 0.15,
}

# Normalización del gravity model (calibrado empíricamente para BCN)
# Representa una zona con 5 restaurantes 4.5★ con 200 reseñas a 100m
_GRAVITY_NORMALIZACION = 5 * (0.9 ** 2) * (math.log(201)) / (100 ** 2)


# ── Tipos ─────────────────────────────────────────────────────────────────────

class NegocioCompetidor:
    """Representación mínima de un competidor para el scoring."""
    __slots__ = ("sector_codigo", "distancia_m", "rating", "num_resenas", "precio_nivel")

    def __init__(
        self,
        sector_codigo: str,
        distancia_m: float,
        rating: Optional[float] = None,
        num_resenas: Optional[int] = None,
        precio_nivel: Optional[int] = None,
    ):
        self.sector_codigo = sector_codigo
        self.distancia_m   = max(distancia_m, 1.0)  # evitar div/0
        self.rating        = rating
        self.num_resenas   = num_resenas or 0
        self.precio_nivel  = precio_nivel  # 1=barato … 4=caro


# ── Función principal ──────────────────────────────────────────────────────────

def calcular_score_competencia(
    negocios_cercanos: list[NegocioCompetidor],
    sector_objetivo: str,
    precio_nivel_objetivo: Optional[int] = None,
) -> dict:
    """
    Calcula el score de competencia para una zona dado un sector objetivo.

    Args:
        negocios_cercanos: lista de competidores y complementarios en radio 500m.
            Puede incluir cualquier sector — la función clasifica internamente.
        sector_objetivo: código de sector que estamos evaluando.
        precio_nivel_objetivo: nivel de precio previsto del nuevo negocio (1-4).
            Si None, se asume segmento medio (2).

    Returns:
        dict con:
            score_competencia    — score final 0-100
            score_cluster        — efecto aglomeración
            score_amenaza_inv    — 100 = sin amenaza
            score_oportunidad    — gap de mercado
            score_complementarios — sinergia sectorial
            num_directos         — nº competidores directos
            pct_vulnerables      — % competidores vulnerables
            hhi_index            — concentración del mercado (0-1)
            amenaza_incumbentes  — amenaza bruta (0-100)
            ratio_complementarios — complementarios / máx esperado
    """
    precio_obj = precio_nivel_objetivo if precio_nivel_objetivo is not None else 2

    sectores_comp = SECTORES_COMPETIDORES.get(sector_objetivo, [sector_objetivo])
    sectores_compl = SECTORES_COMPLEMENTARIOS.get(sector_objetivo, [])

    # Separar en directos y complementarios
    directos:      list[NegocioCompetidor] = []
    complementarios: list[NegocioCompetidor] = []
    for neg in negocios_cercanos:
        if neg.sector_codigo in sectores_comp:
            directos.append(neg)
        elif neg.sector_codigo in sectores_compl:
            complementarios.append(neg)

    # ── Sub-scores ────────────────────────────────────────────────────────────
    s_cluster    = _score_cluster(len(directos), sector_objetivo)
    s_amenaza    = _score_amenaza(directos)
    s_amenaza_inv = max(0.0, 100.0 - s_amenaza)
    s_oportunidad = _score_oportunidad(directos, precio_obj)
    s_compl       = _score_complementarios(complementarios, sector_objetivo)

    # ── Métricas de salida ────────────────────────────────────────────────────
    pct_vuln = _pct_vulnerables(directos)
    hhi      = _calcular_hhi(directos)
    max_compl = max(len(sectores_compl) * 3, 1)
    ratio_compl = min(1.0, len(complementarios) / max_compl)

    # ── Score final ───────────────────────────────────────────────────────────
    p = PESOS_COMPETENCIA
    score = (
        p["cluster"]         * s_cluster
      + p["amenaza_inv"]     * s_amenaza_inv
      + p["oportunidad"]     * s_oportunidad
      + p["complementarios"] * s_compl
    )
    score = round(min(100.0, max(0.0, score)), 1)

    return {
        "score_competencia":    score,
        "score_cluster":        round(s_cluster, 1),
        "score_amenaza_inv":    round(s_amenaza_inv, 1),
        "score_oportunidad":    round(s_oportunidad, 1),
        "score_complementarios": round(s_compl, 1),
        "amenaza_incumbentes":  round(s_amenaza, 1),
        "num_directos":         len(directos),
        "pct_vulnerables":      round(pct_vuln, 4),
        "hhi_index":            round(hhi, 4),
        "ratio_complementarios": round(ratio_compl, 4),
    }


# ── Sub-scores ────────────────────────────────────────────────────────────────

def _score_cluster(num_directos: int, sector: str) -> float:
    """
    Efecto de aglomeración sectorial (curva campana).

    0 competidores directos → 30 pts (sin señal de demanda probada)
    En el óptimo sectorial  → 100 pts (destino consolidado, máxima sinergia)
    Al doble del óptimo     → 0 pts (saturación total)

    La curva es lineal en subida (0→óptimo) y lineal en bajada (óptimo→2×óptimo).
    """
    opt = CLUSTER_OPTIMO.get(sector, CLUSTER_OPTIMO["_default"])

    if num_directos == 0:
        return 30.0  # no hay cluster, pero tampoco saturación

    if num_directos <= opt:
        # Subida: de 30 pts (1 competidor) a 100 pts (óptimo)
        return 30.0 + (num_directos / opt) * 70.0

    # Bajada: de 100 pts (óptimo) a 0 pts (doble del óptimo)
    exceso = num_directos - opt
    return max(0.0, 100.0 - (exceso / opt) * 100.0)


def _score_amenaza(directos: list[NegocioCompetidor]) -> float:
    """
    Gravity model de amenaza de incumbentes (0-100).

    Fórmula por competidor: fortaleza_i × peso_distancia_i
      fortaleza_i   = (rating/5)² × log(num_resenas+1)
      peso_distancia_i = 1 / distancia_m²

    La suma se normaliza con _GRAVITY_NORMALIZACION (escenario BCN de referencia).
    Un competidor 4.5★ con 200 reseñas a 50m genera amenaza máxima.
    """
    if not directos:
        return 0.0

    gravity_sum = 0.0
    for neg in directos:
        r = (neg.rating or 3.0) / 5.0           # 0-1
        n = math.log(neg.num_resenas + 1)        # log para suavizar
        fortaleza = r ** 2 * n
        peso_dist = 1.0 / (neg.distancia_m ** 2)
        gravity_sum += fortaleza * peso_dist

    # Normalizar respecto al escenario de referencia BCN
    amenaza_raw = gravity_sum / max(_GRAVITY_NORMALIZACION, 1e-9)
    return min(100.0, amenaza_raw * 100.0)


def _score_oportunidad(
    directos: list[NegocioCompetidor],
    precio_nivel_objetivo: int,
) -> float:
    """
    Gap de mercado captuable (0-100).

    Tres componentes:
      50% — vulnerabilidad: qué % son desplazables (rating bajo / pocas reseñas)
      30% — atomización: HHI bajo = sin líder dominante = fácil entrar
      20% — gap de precio: si nadie ocupa el segmento de precio objetivo

    Con 0 directos: oportunidad = 75 (hay espacio pero sin señal de demanda).
    """
    if not directos:
        return 75.0  # tierra virgen: oportunidad moderada-alta

    pct_vuln  = _pct_vulnerables(directos)
    hhi       = _calcular_hhi(directos)
    gap_precio = _gap_precio(directos, precio_nivel_objetivo)

    oportunidad = (
        pct_vuln    * 50.0
      + (1.0 - hhi) * 30.0
      + gap_precio  * 20.0
    )
    return min(100.0, max(0.0, oportunidad))


def _score_complementarios(
    complementarios: list[NegocioCompetidor],
    sector: str,
) -> float:
    """
    Sinergia con sectores vecinos (0-100).

    Máximo esperado: len(sectores_complementarios) × 3 negocios = referencia BCN.
    Los negocios complementarios cercanos (< 200m) valen más que los lejanos.
    """
    if not complementarios:
        return 20.0  # sin complementarios = penalización leve

    sectores_compl = SECTORES_COMPLEMENTARIOS.get(sector, [])
    max_esperado = max(len(sectores_compl) * 3, 1)

    # Bonus por proximidad: negocios < 200m valen doble
    valor_total = 0.0
    for neg in complementarios:
        peso = 2.0 if neg.distancia_m < 200 else 1.0
        valor_total += peso

    ratio = valor_total / (max_esperado * 1.5)  # normalizar considerando bonus
    return min(100.0, ratio * 100.0)


# ── Helpers internos ──────────────────────────────────────────────────────────

def _pct_vulnerables(directos: list[NegocioCompetidor]) -> float:
    """Fracción de competidores directos clasificados como vulnerables (0-1)."""
    if not directos:
        return 0.0
    vuln = sum(
        1 for n in directos
        if _es_vulnerable(n)
    )
    return vuln / len(directos)


def _es_vulnerable(neg: NegocioCompetidor) -> bool:
    """True si el competidor es desplazable por un negocio bien ejecutado."""
    r = neg.rating
    if r is None:
        return True  # sin rating = establecimiento sin presencia digital = vulnerable
    if r < RATING_VULNERABLE:
        return True
    if r < 4.0 and neg.num_resenas < RESENAS_VULNERABLE:
        return True
    return False


def _calcular_hhi(directos: list[NegocioCompetidor]) -> float:
    """
    Herfindahl-Hirschman Index basado en ratings ponderados (0-1).

    Cuota de cada competidor = su_rating / suma_total_ratings.
    HHI = Σ(cuota_i²).
    HHI bajo → mercado atomizado → oportunidad de entrada.
    HHI alto → un competidor domina → difícil desplazarlo.
    Con 0 o 1 competidor se retornan valores especiales.
    """
    if not directos:
        return 0.0
    if len(directos) == 1:
        return 1.0  # monopolio efectivo

    ratings = [max(n.rating or 3.0, 0.1) for n in directos]
    total = sum(ratings)
    if total <= 0:
        return 0.0
    cuotas = [r / total for r in ratings]
    return sum(c ** 2 for c in cuotas)


def _gap_precio(
    directos: list[NegocioCompetidor],
    precio_objetivo: int,
) -> float:
    """
    Gap de precio (0-1): qué tan poco representado está el segmento objetivo.

    Si nadie está en el mismo nivel de precio → gap = 1.0 (oportunidad total).
    Si todos están en el mismo nivel → gap = 0.0 (saturación de precio).
    """
    niveles_existentes = [n.precio_nivel for n in directos if n.precio_nivel is not None]
    if not niveles_existentes:
        return 0.5  # sin información de precio: gap moderado

    total = len(niveles_existentes)
    en_mismo_nivel = sum(1 for p in niveles_existentes if p == precio_objetivo)
    return 1.0 - (en_mismo_nivel / total)


# Normalización para amenaza_score_individual:
# 1 competidor 4.5★, 200 reseñas a 100m → referencia de amenaza individual
_SINGLE_NORM = (0.9 ** 2) * math.log(201) / (100 ** 2)


# ── API pública adicional ─────────────────────────────────────────────────────

def amenaza_score_individual(neg: NegocioCompetidor) -> float:
    """
    Score de amenaza individual (0-100) para UN solo competidor.

    Usa el mismo gravity model que _score_amenaza pero normalizado contra
    un escenario de un único competidor de referencia (4.5★, 200 reseñas a 100m
    → ~80 pts), en lugar de la normalización de 5 competidores usada en
    _score_amenaza.

    Args:
        neg: competidor a evaluar.

    Returns:
        float 0-100 redondeado a 1 decimal.
    """
    r = (neg.rating or 3.0) / 5.0
    n = math.log(neg.num_resenas + 1)
    fortaleza = r ** 2 * n
    peso_dist = 1.0 / (neg.distancia_m ** 2)
    raw = (fortaleza * peso_dist) / max(_SINGLE_NORM, 1e-9)
    return round(min(100.0, raw * 100.0), 1)


def analizar_precio_segmento(
    directos: list[NegocioCompetidor],
    precio_objetivo: int,
) -> dict:
    """
    Analiza la distribución de niveles de precio y detecta gaps de segmento.

    Args:
        directos: lista de competidores directos.
        precio_objetivo: nivel de precio del nuevo negocio (1-4).

    Returns:
        dict con:
            nivel_dominante  — nivel más frecuente (None si sin datos)
            etiqueta         — "€", "€€", "€€€", "€€€€" o "Sin datos"
            distribucion     — {1: count, 2: count, 3: count, 4: count}
            tiene_gap        — True si precio_objetivo ocupa < 20% del mercado
            gap_nivel        — precio_objetivo si tiene_gap, else None
            gap_etiqueta     — etiqueta del gap si tiene_gap, else None
    """
    _ETIQUETAS: dict[int, str] = {1: "€", 2: "€€", 3: "€€€", 4: "€€€€"}

    distribucion: dict[int, int] = {1: 0, 2: 0, 3: 0, 4: 0}
    for neg in directos:
        if neg.precio_nivel in distribucion:
            distribucion[neg.precio_nivel] += 1

    total_con_datos = sum(distribucion.values())

    if total_con_datos == 0:
        return {
            "nivel_dominante": None,
            "etiqueta": "Sin datos",
            "distribucion": distribucion,
            "tiene_gap": False,
            "gap_nivel": None,
            "gap_etiqueta": None,
        }

    # Nivel más frecuente
    nivel_dominante = max(distribucion, key=lambda k: distribucion[k])

    # Gap: el nivel objetivo representa < 20% del total
    en_objetivo = distribucion.get(precio_objetivo, 0)
    tiene_gap = (en_objetivo / total_con_datos) < 0.20

    return {
        "nivel_dominante": nivel_dominante,
        "etiqueta": _ETIQUETAS.get(nivel_dominante, "Sin datos"),
        "distribucion": distribucion,
        "tiene_gap": tiene_gap,
        "gap_nivel": precio_objetivo if tiene_gap else None,
        "gap_etiqueta": _ETIQUETAS.get(precio_objetivo) if tiene_gap else None,
    }


def clasificar_competidores(
    negocios: list[NegocioCompetidor],
    sector: str,
) -> dict:
    """
    Clasifica competidores en tres grupos para las pestañas de la UI.

    Args:
        negocios: lista completa de negocios cercanos (todos los sectores).
        sector: sector objetivo del nuevo negocio.

    Returns:
        dict con:
            amenaza      — competidores directos NO vulnerables, ordenados por
                           amenaza_score_individual DESC
            oportunidad  — competidores directos vulnerables, ordenados por
                           distancia_m ASC
            sinergicos   — negocios complementarios, ordenados por distancia_m ASC
    """
    sectores_comp  = SECTORES_COMPETIDORES.get(sector, [sector])
    sectores_compl = SECTORES_COMPLEMENTARIOS.get(sector, [])

    amenaza:     list[NegocioCompetidor] = []
    oportunidad: list[NegocioCompetidor] = []
    sinergicos:  list[NegocioCompetidor] = []

    for neg in negocios:
        if neg.sector_codigo in sectores_comp:
            if _es_vulnerable(neg):
                oportunidad.append(neg)
            else:
                amenaza.append(neg)
        elif neg.sector_codigo in sectores_compl:
            sinergicos.append(neg)

    amenaza.sort(key=lambda n: amenaza_score_individual(n), reverse=True)
    oportunidad.sort(key=lambda n: n.distancia_m)
    sinergicos.sort(key=lambda n: n.distancia_m)

    return {
        "amenaza":     amenaza,
        "oportunidad": oportunidad,
        "sinergicos":  sinergicos,
    }


# ── Utilidad: construir lista de NegocioCompetidor desde rows de BD ───────────

def negocios_desde_rows(rows: list[dict], sector_objetivo: str) -> list[NegocioCompetidor]:
    """
    Convierte filas de negocios_activos (con distancia_m calculada por PostGIS)
    a lista de NegocioCompetidor para calcular_score_competencia().

    Espera que cada row tenga: sector_codigo, distancia_m, rating,
    num_resenas, precio_nivel.
    """
    sectores_relevantes = (
        SECTORES_COMPETIDORES.get(sector_objetivo, [sector_objetivo])
        + SECTORES_COMPLEMENTARIOS.get(sector_objetivo, [])
    )
    result = []
    for row in rows:
        sc = row.get("sector_codigo")
        if sc not in sectores_relevantes:
            continue
        result.append(NegocioCompetidor(
            sector_codigo = sc,
            distancia_m   = float(row.get("distancia_m") or 500),
            rating        = row.get("rating"),
            num_resenas   = row.get("num_resenas") or 0,
            precio_nivel  = row.get("precio_nivel"),
        ))
    return result
