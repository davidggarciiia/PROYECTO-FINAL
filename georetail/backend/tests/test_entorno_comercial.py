"""Tests para la dimensión de entorno comercial v8 (fórmula compuesta)."""
import pytest
import sys
import os

# Añadir backend al path para imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from scoring.scorer import _calcular_score_entorno


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _zona_eixample() -> dict:
    """Zona comercial consolidada: Eixample (bajo vacío, buen entorno)."""
    return {
        "pct_locales_vacios": 0.05,
        "tasa_rotacion_anual": 0.08,
        "licencias_nuevas_1a": 8.0,
        "ratio_locales_comerciales": 0.35,
        "nivel_ruido_db": 58.0,
        "score_equipamientos": 75.0,
        "m2_zonas_verdes_cercanas": 3500.0,
        "mercados_municipales_1km": 3,
        "eventos_culturales_500m": 5,
    }


def _zona_degradada() -> dict:
    """Zona degradada: alto vacío, mucho ruido, sin mercados."""
    return {
        "pct_locales_vacios": 0.30,
        "tasa_rotacion_anual": 0.35,
        "licencias_nuevas_1a": 1.0,
        "ratio_locales_comerciales": 0.08,
        "nivel_ruido_db": 75.0,
        "score_equipamientos": 20.0,
        "m2_zonas_verdes_cercanas": 200.0,
        "mercados_municipales_1km": 0,
        "eventos_culturales_500m": 0,
    }


def _zona_optima() -> dict:
    """Zona óptima teórica: todos los indicadores al máximo."""
    return {
        "pct_locales_vacios": 0.0,
        "tasa_rotacion_anual": 0.0,
        "licencias_nuevas_1a": 12.0,
        "ratio_locales_comerciales": 0.40,
        "nivel_ruido_db": 45.0,
        "score_equipamientos": 100.0,
        "m2_zonas_verdes_cercanas": 8000.0,
        "mercados_municipales_1km": 4,
        "eventos_culturales_500m": 7,
    }


# ── Tests de rango ────────────────────────────────────────────────────────────

def test_score_entorno_rango_0_100_eixample():
    score = _calcular_score_entorno(_zona_eixample())
    assert 0.0 <= score <= 100.0


def test_score_entorno_rango_0_100_degradada():
    score = _calcular_score_entorno(_zona_degradada())
    assert 0.0 <= score <= 100.0


def test_score_entorno_rango_0_100_optima():
    score = _calcular_score_entorno(_zona_optima())
    assert 0.0 <= score <= 100.0


# ── Tests de diferenciación ───────────────────────────────────────────────────

def test_eixample_mayor_que_degradada():
    score_eix = _calcular_score_entorno(_zona_eixample())
    score_deg = _calcular_score_entorno(_zona_degradada())
    assert score_eix > score_deg + 10, (
        f"Eixample ({score_eix:.1f}) debe superar degradada ({score_deg:.1f}) por >10 pts"
    )


def test_zona_consolidada_score_alto():
    """Zona tipo Eixample debe dar score >60."""
    score = _calcular_score_entorno(_zona_eixample())
    assert score >= 60.0, f"Eixample esperado >=60, obtenido {score:.1f}"


def test_zona_degradada_score_bajo():
    """Zona degradada debe dar score <40."""
    score = _calcular_score_entorno(_zona_degradada())
    assert score <= 40.0, f"Degradada esperado <=40, obtenido {score:.1f}"


def test_zona_optima_score_muy_alto():
    """Zona óptima teórica debe dar score >85."""
    score = _calcular_score_entorno(_zona_optima())
    assert score >= 85.0, f"Óptima esperado >=85, obtenido {score:.1f}"


# ── Tests de fallback (datos ausentes) ────────────────────────────────────────

def test_fallback_todos_none():
    """Con todos los datos ausentes, debe usar fallbacks y dar ~50 (medio)."""
    score = _calcular_score_entorno({})
    assert 30.0 <= score <= 70.0, f"Fallback esperado 30-70, obtenido {score:.1f}"


def test_fallback_parcial():
    """Con datos parciales, no debe crashear."""
    datos = {
        "pct_locales_vacios": 0.10,
        "nivel_ruido_db": 60.0,
        # resto None → fallbacks
    }
    score = _calcular_score_entorno(datos)
    assert 0.0 <= score <= 100.0


# ── Tests de sub-scores individuales ──────────────────────────────────────────

def test_ruido_alto_penaliza():
    """Más ruido (75 dB) debe dar peor score que poco ruido (50 dB)."""
    base = _zona_eixample()

    alto_ruido = {**base, "nivel_ruido_db": 78.0}
    bajo_ruido = {**base, "nivel_ruido_db": 48.0}

    score_alto = _calcular_score_entorno(alto_ruido)
    score_bajo = _calcular_score_entorno(bajo_ruido)
    assert score_bajo > score_alto, (
        f"Bajo ruido ({score_bajo:.1f}) debe superar alto ({score_alto:.1f})"
    )


def test_mas_mercados_mejora():
    """Más mercados municipales cerca debe mejorar el score."""
    base = _zona_eixample()

    sin_mercados = {**base, "mercados_municipales_1km": 0}
    con_mercados = {**base, "mercados_municipales_1km": 4}

    score_sin = _calcular_score_entorno(sin_mercados)
    score_con = _calcular_score_entorno(con_mercados)
    assert score_con > score_sin


def test_mas_vacios_penaliza():
    """Más locales vacíos debe penalizar el score."""
    base = _zona_eixample()

    pocos_vacios = {**base, "pct_locales_vacios": 0.03}
    muchos_vacios = {**base, "pct_locales_vacios": 0.30}

    score_pocos = _calcular_score_entorno(pocos_vacios)
    score_muchos = _calcular_score_entorno(muchos_vacios)
    assert score_pocos > score_muchos


def test_mas_zonas_verdes_mejora():
    """Más zonas verdes debe mejorar el score."""
    base = _zona_eixample()

    sin_verde = {**base, "m2_zonas_verdes_cercanas": 100.0}
    con_verde = {**base, "m2_zonas_verdes_cercanas": 6000.0}

    score_sin = _calcular_score_entorno(sin_verde)
    score_con = _calcular_score_entorno(con_verde)
    assert score_con > score_sin
