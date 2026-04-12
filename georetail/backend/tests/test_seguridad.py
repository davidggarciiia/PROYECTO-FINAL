"""
tests/test_seguridad.py — Tests para la dimensión de seguridad multivariable (v7).

Cubre:
  - _calcular_score_seguridad: fórmula compuesta con 5 sub-scores
  - Rango [0, 100] para todos los sub-scores
  - Ciutat Vella (alta criminalidad) < Sarrià (baja criminalidad)
  - Fallback cuando faltan datos (usa valores por defecto)
  - Comportamiento con valores extremos
  - Pipeline seguridad: funciones puras (_parse_incidents_csv, _normalize_barri)
"""
import pytest

from scoring.scorer import _calcular_score_seguridad


# ─── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def datos_ciutat_vella():
    """Ciutat Vella: zona turística con alta criminalidad."""
    return {
        "incidencias_por_1000hab": 95.0,
        "hurtos_por_1000hab": 55.0,
        "robatoris_por_1000hab": 18.0,
        "danys_por_1000hab": 12.0,
        "incidencias_noche_pct": 0.48,
        "comisarias_1km": 4,
        "dist_comisaria_m": 200,
        "seguridad_barri_score": 4.5,
    }


@pytest.fixture
def datos_sarria():
    """Sarrià-Sant Gervasi: zona residencial de baja criminalidad."""
    return {
        "incidencias_por_1000hab": 12.0,
        "hurtos_por_1000hab": 5.0,
        "robatoris_por_1000hab": 2.0,
        "danys_por_1000hab": 1.5,
        "incidencias_noche_pct": 0.18,
        "comisarias_1km": 2,
        "dist_comisaria_m": 600,
        "seguridad_barri_score": 7.8,
    }


@pytest.fixture
def datos_vacios():
    """Zona sin datos de seguridad (debe usar fallbacks)."""
    return {}


# ─── Tests de rango ─────────────────────────────────────────────────────────────

def test_score_rango_0_100(datos_ciutat_vella, datos_sarria, datos_vacios):
    """El score compuesto siempre está entre 0 y 100."""
    for datos in [datos_ciutat_vella, datos_sarria, datos_vacios]:
        score = _calcular_score_seguridad(datos)
        assert 0.0 <= score <= 100.0, f"Score fuera de rango: {score}"


def test_ciutat_vella_menor_que_sarria(datos_ciutat_vella, datos_sarria):
    """Ciutat Vella (alta criminalidad) debe puntuar menos que Sarrià."""
    s_cv = _calcular_score_seguridad(datos_ciutat_vella)
    s_sa = _calcular_score_seguridad(datos_sarria)
    assert s_cv < s_sa, f"Ciutat Vella ({s_cv}) debería puntuar menos que Sarrià ({s_sa})"


def test_ciutat_vella_score_bajo(datos_ciutat_vella):
    """Ciutat Vella debe tener un score relativamente bajo (< 50)."""
    score = _calcular_score_seguridad(datos_ciutat_vella)
    assert score < 50.0, f"Ciutat Vella score ({score}) debería ser < 50"


def test_sarria_score_alto(datos_sarria):
    """Sarrià debe tener un score relativamente alto (> 70)."""
    score = _calcular_score_seguridad(datos_sarria)
    assert score > 70.0, f"Sarrià score ({score}) debería ser > 70"


# ─── Tests de fallback ──────────────────────────────────────────────────────────

def test_datos_vacios_usa_fallback(datos_vacios):
    """Con datos vacíos, debe usar fallbacks y devolver un score razonable."""
    score = _calcular_score_seguridad(datos_vacios)
    # Fallback debería dar un score medio (~50-70)
    assert 30.0 <= score <= 80.0, f"Score con fallback ({score}) fuera de rango razonable"


def test_datos_parciales():
    """Con solo algunos campos, no debe fallar."""
    datos = {"incidencias_por_1000hab": 40.0, "comisarias_1km": 3}
    score = _calcular_score_seguridad(datos)
    assert 0.0 <= score <= 100.0


# ─── Tests de valores extremos ──────────────────────────────────────────────────

def test_extremo_zona_peligrosa():
    """Zona extremadamente peligrosa: todo al máximo negativo."""
    datos = {
        "incidencias_por_1000hab": 120.0,
        "hurtos_por_1000hab": 70.0,
        "robatoris_por_1000hab": 30.0,
        "danys_por_1000hab": 20.0,
        "incidencias_noche_pct": 0.55,
        "comisarias_1km": 0,
        "dist_comisaria_m": 3000,
        "seguridad_barri_score": 2.0,
    }
    score = _calcular_score_seguridad(datos)
    assert score < 25.0, f"Zona extrema peligrosa ({score}) debería ser < 25"


def test_extremo_zona_segura():
    """Zona extremadamente segura: todo al mínimo positivo."""
    datos = {
        "incidencias_por_1000hab": 5.0,
        "hurtos_por_1000hab": 2.0,
        "robatoris_por_1000hab": 0.5,
        "danys_por_1000hab": 0.3,
        "incidencias_noche_pct": 0.15,
        "comisarias_1km": 5,
        "dist_comisaria_m": 100,
        "seguridad_barri_score": 9.5,
    }
    score = _calcular_score_seguridad(datos)
    assert score > 85.0, f"Zona extrema segura ({score}) debería ser > 85"


# ─── Tests de IERMB fallback ────────────────────────────────────────────────────

def test_iermb_none_usa_fallback():
    """Si seguridad_barri_score es None, se usa fallback de 55."""
    datos_con = {"seguridad_barri_score": 8.0, "incidencias_por_1000hab": 20.0}
    datos_sin = {"seguridad_barri_score": None, "incidencias_por_1000hab": 20.0}
    score_con = _calcular_score_seguridad(datos_con)
    score_sin = _calcular_score_seguridad(datos_sin)
    # Con IERMB=8.0 (80 pts) debería ser mayor que con fallback=55
    assert score_con > score_sin


# ─── Tests pipeline funciones puras ──────────────────────────────────────────────

def test_normalize_barri():
    """_normalize_barri normaliza acentos y casing."""
    pytest.importorskip("httpx")
    from pipelines.entorno.seguridad import _normalize_barri
    assert _normalize_barri("El Raval") == "el raval"
    assert _normalize_barri("Sarrià-Sant Gervasi") == "sarria-sant gervasi"
    assert _normalize_barri("L'Eixample") == "l'eixample"
    assert _normalize_barri("") == ""


def test_parse_incidents_csv():
    """_parse_incidents_csv clasifica correctamente por tipo y nocturnidad."""
    pytest.importorskip("httpx")
    from pipelines.entorno.seguridad import _parse_incidents_csv

    csv_text = """Nom barri,Hora de dia,Descripció causa vianant
El Raval,22,Furt de carterisme
El Raval,14,Robatori amb violència
El Raval,3,Danys vandalisme
Sarrià,10,Desconegut
Sarrià,23,Furt descuit"""

    stats = _parse_incidents_csv(csv_text)
    assert "el raval" in stats
    assert stats["el raval"]["total"] == 3
    assert stats["el raval"]["hurtos"] == 1
    assert stats["el raval"]["robatoris"] == 1
    assert stats["el raval"]["danys"] == 1
    assert stats["el raval"]["noche"] == 2  # 22h and 3h are nighttime

    assert "sarria" in stats
    assert stats["sarria"]["total"] == 2
    assert stats["sarria"]["noche"] == 1  # 23h is nighttime
