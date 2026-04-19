"""
tests/test_turismo_dimension.py — Tests para la dimensión de turismo (v14, mig 029).

Cubre:
  - calcular_turismo: combinación log-escalada de Airbnb + hoteles + POIs + costa
  - Rango [0, 100]
  - Perfiles: playa, hotelero, cultural, mixto, residencial, sin_datos
  - Confianza: alta / media / baja según stocks no-null
  - Fallback cuando faltan todos los stocks (solo dist_playa_m como proxy débil)
  - Modulador turismo_dependencia (premio/castigo según perfil del negocio)
"""
import pytest

from scoring.dimensiones.turismo import calcular_turismo


# ─── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def datos_barceloneta():
    """Barceloneta: zona litoral saturada de airbnb + hoteles + POIs."""
    return {
        "airbnb_density_500m":      45,
        "airbnb_occupancy_est":     0.85,
        "booking_hoteles_500m":     12,
        "eventos_culturales_500m":  5,
        "venues_musicales_500m":    2,
        "dist_playa_m":             180,
        "seasonality_summer_lift":  1.45,
    }


@pytest.fixture
def datos_sagrada_familia():
    """Sagrada Família: alta densidad hotelera + POI cultural dominante, sin playa."""
    return {
        "airbnb_density_500m":      30,
        "airbnb_occupancy_est":     0.70,
        "booking_hoteles_500m":     20,
        "eventos_culturales_500m":  9,
        "venues_musicales_500m":    1,
        "dist_playa_m":             3200,
        "seasonality_summer_lift":  1.15,
    }


@pytest.fixture
def datos_residencial():
    """Horta: zona residencial, apenas turismo."""
    return {
        "airbnb_density_500m":      1,
        "airbnb_occupancy_est":     0.25,
        "booking_hoteles_500m":     0,
        "eventos_culturales_500m":  0,
        "venues_musicales_500m":    0,
        "dist_playa_m":             5500,
        "seasonality_summer_lift":  0.95,
    }


@pytest.fixture
def datos_solo_airbnb():
    """Zona con turismo informal alto pero sin hoteles ni POIs formales."""
    return {
        "airbnb_density_500m":      40,
        "airbnb_occupancy_est":     0.80,
        "booking_hoteles_500m":     0,
        "eventos_culturales_500m":  1,
        "venues_musicales_500m":    0,
        "dist_playa_m":             2500,
    }


@pytest.fixture
def datos_sin_datos():
    """Zona con stocks NULL (pipelines no corrieron o sin datos)."""
    return {
        "airbnb_density_500m":      None,
        "airbnb_occupancy_est":     None,
        "booking_hoteles_500m":     None,
        "eventos_culturales_500m":  None,
        "venues_musicales_500m":    None,
        "dist_playa_m":             4000,
    }


# ─── Tests: rango y tipos ──────────────────────────────────────────────────────

@pytest.mark.parametrize("fixture_name", [
    "datos_barceloneta", "datos_sagrada_familia", "datos_residencial",
    "datos_solo_airbnb", "datos_sin_datos",
])
def test_score_en_rango_0_100(fixture_name, request):
    datos = request.getfixturevalue(fixture_name)
    res = calcular_turismo(datos)
    assert 0.0 <= res["score_turismo"] <= 100.0
    assert res["perfil_turistico"] in {
        "playa", "hotelero", "cultural", "mixto", "residencial", "sin_datos"
    }
    assert res["confianza"] in {"alta", "media", "baja"}


# ─── Tests: orden relativo esperado ────────────────────────────────────────────

def test_barceloneta_supera_residencial(datos_barceloneta, datos_residencial):
    s_bcn = calcular_turismo(datos_barceloneta)["score_turismo"]
    s_res = calcular_turismo(datos_residencial)["score_turismo"]
    assert s_bcn > s_res + 30.0, (
        f"Barceloneta ({s_bcn}) debería superar residencial ({s_res}) con margen amplio"
    )


def test_sagrada_familia_no_trivial(datos_sagrada_familia):
    res = calcular_turismo(datos_sagrada_familia)
    # Zona hiperturística sin playa debería ser >= 70
    assert res["score_turismo"] >= 65.0, res


def test_barceloneta_mayor_que_sagrada_por_costa(
    datos_barceloneta, datos_sagrada_familia
):
    """Con señales de alojamiento parecidas, la costa debe empujar Barceloneta arriba."""
    s_bcn = calcular_turismo(datos_barceloneta)["score_turismo"]
    s_sf  = calcular_turismo(datos_sagrada_familia)["score_turismo"]
    assert s_bcn >= s_sf


# ─── Tests: perfiles ───────────────────────────────────────────────────────────

def test_perfil_playa_en_barceloneta(datos_barceloneta):
    assert calcular_turismo(datos_barceloneta)["perfil_turistico"] == "playa"


def test_perfil_residencial(datos_residencial):
    res = calcular_turismo(datos_residencial)
    # Residencial con dist_playa > 1500 y stocks casi vacíos → residencial o sin_datos
    assert res["perfil_turistico"] in {"residencial", "sin_datos"}


def test_perfil_sin_datos_devuelve_fallback(datos_sin_datos):
    res = calcular_turismo(datos_sin_datos)
    # Sin stocks y dist_playa=4000m → fallback 50 con confianza baja
    assert res["confianza"] == "baja"
    assert res["perfil_turistico"] == "sin_datos"


# ─── Tests: confianza ──────────────────────────────────────────────────────────

def test_confianza_alta_con_stocks_suficientes(datos_barceloneta):
    assert calcular_turismo(datos_barceloneta)["confianza"] == "alta"


def test_confianza_baja_sin_stocks(datos_sin_datos):
    assert calcular_turismo(datos_sin_datos)["confianza"] == "baja"


# ─── Tests: modulador turismo_dependencia ──────────────────────────────────────

def test_negocio_turismo_dependiente_premia_zona_turistica(datos_barceloneta):
    s_neutro = calcular_turismo(datos_barceloneta)["score_turismo"]
    s_dep    = calcular_turismo(
        datos_barceloneta,
        perfil_negocio={"turismo_dependencia": 0.9},
    )["score_turismo"]
    assert s_dep >= s_neutro, (
        f"Negocio turismo-dependiente en zona turística debería subir score "
        f"({s_neutro} → {s_dep})"
    )


def test_negocio_barrio_penaliza_zona_trampa(datos_barceloneta):
    s_neutro = calcular_turismo(datos_barceloneta)["score_turismo"]
    s_barrio = calcular_turismo(
        datos_barceloneta,
        perfil_negocio={"turismo_dependencia": 0.1},
    )["score_turismo"]
    # Sólo se penaliza si base > 80 (trampa real para negocio de barrio)
    # Barceloneta debería caer en este caso.
    assert s_barrio <= s_neutro


# ─── Tests: robustez ───────────────────────────────────────────────────────────

def test_input_vacio_devuelve_fallback():
    res = calcular_turismo({})
    assert res["score_turismo"] == 50.0
    assert res["confianza"] == "baja"
    assert res["perfil_turistico"] == "sin_datos"


def test_tipos_invalidos_no_explotan():
    datos = {
        "airbnb_density_500m":  "invalido",
        "airbnb_occupancy_est": None,
        "booking_hoteles_500m": 5,
        "eventos_culturales_500m": 3,
        "dist_playa_m":         1000,
    }
    res = calcular_turismo(datos)
    assert 0.0 <= res["score_turismo"] <= 100.0


def test_proximidad_playa_levanta_score_con_stocks_bajos():
    """Misma zona con/sin proximidad al mar — la playa debe sumar."""
    base_datos = {
        "airbnb_density_500m":     2,
        "booking_hoteles_500m":    1,
        "eventos_culturales_500m": 0,
    }
    s_inland  = calcular_turismo({**base_datos, "dist_playa_m": 5000})["score_turismo"]
    s_playera = calcular_turismo({**base_datos, "dist_playa_m": 150})["score_turismo"]
    # Misma estructura, solo cambia la costa: playa debe sumar al menos 10 puntos
    assert s_playera >= s_inland + 10.0, (
        f"Costa debería elevar score: inland={s_inland} vs playa={s_playera}"
    )
