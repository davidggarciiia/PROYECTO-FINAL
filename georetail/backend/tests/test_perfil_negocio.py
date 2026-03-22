"""
tests/test_perfil_negocio.py — Tests para scoring/perfil_negocio.py

Cubre:
  - PerfilNegocio: construcción, to_dict/from_dict, to_array
  - Clipping de valores fuera de [0,1]
  - perfil_desde_tags: que los tags producen valores coherentes
  - DEFAULTS_POR_SECTOR: que los perfiles por sector son consistentes
  - Invariantes de lógica: specialty_coffee < cafetería express en dependencia_flujo,
    fine_dining >> express en nivel_precio, etc.
"""
from __future__ import annotations
import pytest
import numpy as np

from scoring.perfil_negocio import (
    PerfilNegocio,
    DEFAULTS_POR_SECTOR,
    FEATURE_NAMES_PERFIL,
    perfil_desde_tags,
    perfil_para_sector,
)


# ════════════════════════════════════════════════════════════════════════════
# 1. PerfilNegocio — construcción y serialización
# ════════════════════════════════════════════════════════════════════════════

class TestPerfilNegocioBasico:
    def test_defaults_neutros(self):
        p = PerfilNegocio()
        assert 0.0 <= p.dependencia_flujo <= 1.0
        assert 0.0 <= p.nivel_precio <= 1.0
        assert 0.0 <= p.clientela_turismo <= 1.0
        assert 0.0 <= p.clientela_vecindario <= 1.0
        assert 0.0 <= p.horario_nocturno <= 1.0
        assert 0.0 <= p.experiencial <= 1.0
        assert 0.0 <= p.citas_previas <= 1.0
        assert 0.0 <= p.sensibilidad_alquiler <= 1.0

    def test_to_array_tiene_8_elementos(self):
        p = PerfilNegocio()
        arr = p.to_array()
        assert isinstance(arr, np.ndarray)
        assert arr.shape == (8,)

    def test_to_array_coincide_con_feature_names(self):
        """El array tiene 8 elementos, igual que FEATURE_NAMES_PERFIL."""
        assert len(FEATURE_NAMES_PERFIL) == 8
        p = PerfilNegocio(dependencia_flujo=0.7, nivel_precio=0.3)
        assert len(p.to_array()) == len(FEATURE_NAMES_PERFIL)

    def test_to_dict_tiene_8_claves(self):
        p = PerfilNegocio()
        d = p.to_dict()
        assert len(d) == 8
        assert "dependencia_flujo" in d
        assert "sensibilidad_alquiler" in d

    def test_to_dict_valores_redondeados(self):
        p = PerfilNegocio(dependencia_flujo=0.123456789)
        d = p.to_dict()
        assert d["dependencia_flujo"] == pytest.approx(0.123, abs=0.001)

    def test_from_dict_roundtrip(self):
        original = PerfilNegocio(
            dependencia_flujo=0.70,
            nivel_precio=0.90,
            clientela_turismo=0.15,
            clientela_vecindario=0.80,
            horario_nocturno=0.30,
            experiencial=0.50,
            citas_previas=0.10,
            sensibilidad_alquiler=0.20,
        )
        reconstructed = PerfilNegocio.from_dict(original.to_dict())
        assert reconstructed.dependencia_flujo == pytest.approx(original.dependencia_flujo, abs=0.001)
        assert reconstructed.nivel_precio == pytest.approx(original.nivel_precio, abs=0.001)
        assert reconstructed.clientela_turismo == pytest.approx(original.clientela_turismo, abs=0.001)


class TestPerfilNegocioClipping:
    """from_dict debe clipear valores fuera de [0,1] sin crashear."""

    def test_valor_mayor_1_se_clipea(self):
        p = PerfilNegocio.from_dict({"dependencia_flujo": 1.5})
        assert p.dependencia_flujo == pytest.approx(1.0)

    def test_valor_negativo_se_clipea(self):
        p = PerfilNegocio.from_dict({"nivel_precio": -0.3})
        assert p.nivel_precio == pytest.approx(0.0)

    def test_valor_none_usa_default(self):
        p = PerfilNegocio.from_dict({"dependencia_flujo": None})
        # default neutro = 0.5
        assert p.dependencia_flujo == pytest.approx(0.5)

    def test_valor_string_invalido_usa_default(self):
        p = PerfilNegocio.from_dict({"nivel_precio": "mucho"})
        assert p.nivel_precio == pytest.approx(0.5)

    def test_dict_vacio_usa_todos_los_defaults(self):
        p = PerfilNegocio.from_dict({})
        p_default = PerfilNegocio()
        assert p.dependencia_flujo == pytest.approx(p_default.dependencia_flujo)
        assert p.horario_nocturno == pytest.approx(p_default.horario_nocturno)
        assert p.experiencial == pytest.approx(p_default.experiencial)


# ════════════════════════════════════════════════════════════════════════════
# 2. DEFAULTS_POR_SECTOR — consistencia
# ════════════════════════════════════════════════════════════════════════════

class TestDefaultsPorSector:
    def test_sectores_esperados_presentes(self):
        sectores_esperados = [
            "restauracion", "moda", "estetica", "tatuajes",
            "shisha_lounge", "salud", "deporte", "educacion",
            "alimentacion", "servicios",
        ]
        for sector in sectores_esperados:
            assert sector in DEFAULTS_POR_SECTOR, f"Sector '{sector}' no en DEFAULTS_POR_SECTOR"

    def test_todos_los_valores_en_rango_0_1(self):
        for sector, perfil in DEFAULTS_POR_SECTOR.items():
            arr = perfil.to_array()
            assert np.all(arr >= 0.0) and np.all(arr <= 1.0), (
                f"Sector '{sector}' tiene valores fuera de [0,1]: {arr}"
            )

    def test_perfil_para_sector_devuelve_default_o_neutro(self):
        p = perfil_para_sector("restauracion")
        assert isinstance(p, PerfilNegocio)

    def test_perfil_para_sector_desconocido_devuelve_neutro(self):
        p = perfil_para_sector("sector_inventado_xyz")
        assert isinstance(p, PerfilNegocio)
        # El neutro tiene valores típicamente 0.5
        assert p.dependencia_flujo == pytest.approx(0.5)

    # ── Invariantes sectoriales ────────────────────────────────────────────
    def test_salud_tiene_citas_previas_altas(self):
        """Un centro médico casi siempre trabaja con cita — citas_previas > 0.8."""
        assert DEFAULTS_POR_SECTOR["salud"].citas_previas > 0.8

    def test_alimentacion_tiene_clientela_vecindario_alta(self):
        """Un supermercado o frutería sirve al barrio — clientela_vecindario > 0.7."""
        assert DEFAULTS_POR_SECTOR["alimentacion"].clientela_vecindario > 0.7

    def test_shisha_lounge_tiene_horario_nocturno_alto(self):
        """Una shisha lounge funciona por la noche — horario_nocturno > 0.7."""
        assert DEFAULTS_POR_SECTOR["shisha_lounge"].horario_nocturno > 0.7

    def test_moda_tiene_dependencia_flujo_alta(self):
        """Una tienda de ropa necesita visibilidad y paso — dependencia_flujo > 0.6."""
        assert DEFAULTS_POR_SECTOR["moda"].dependencia_flujo > 0.6

    def test_estetica_tiene_citas_previas_altas(self):
        """Un salón de belleza funciona con cita — citas_previas > 0.6."""
        assert DEFAULTS_POR_SECTOR["estetica"].citas_previas > 0.6


# ════════════════════════════════════════════════════════════════════════════
# 3. perfil_desde_tags — que los tags producen valores coherentes
# ════════════════════════════════════════════════════════════════════════════

class TestPerfilDesdeTags:
    def test_sin_tags_devuelve_perfil_neutro(self):
        p = perfil_desde_tags([])
        assert isinstance(p, PerfilNegocio)
        # Sin tags, debe usar defaults
        assert 0.0 <= p.dependencia_flujo <= 1.0

    def test_tags_inexistentes_no_crashean(self):
        p = perfil_desde_tags(["tag_inventado_abc", "otro_tag_raro"])
        assert isinstance(p, PerfilNegocio)

    def test_specialty_coffee_da_dependencia_flujo_baja(self):
        """Un café de especialidad no depende del paso masivo."""
        p = perfil_desde_tags(["specialty_coffee"])
        assert p.dependencia_flujo < 0.55, (
            f"specialty_coffee debería tener dependencia_flujo baja, got {p.dependencia_flujo}"
        )

    def test_street_food_da_dependencia_flujo_alta(self):
        """Un street food stall depende 100% del paso."""
        p = perfil_desde_tags(["street_food"])
        assert p.dependencia_flujo > 0.80, (
            f"street_food debería tener dependencia_flujo alta, got {p.dependencia_flujo}"
        )

    def test_gastronomico_premium_da_nivel_precio_alto(self):
        """Un restaurante gastronómico premium tiene precios altos."""
        p = perfil_desde_tags(["gastronomico_premium"])
        assert p.nivel_precio > 0.80

    def test_low_cost_da_nivel_precio_bajo(self):
        p = perfil_desde_tags(["low_cost"])
        assert p.nivel_precio < 0.30

    def test_dog_friendly_da_clientela_vecindario_alta(self):
        """Dog-friendly = clientela local, no turistas."""
        p = perfil_desde_tags(["dog_friendly"])
        assert p.clientela_vecindario > 0.70
        assert p.clientela_turismo < 0.25

    def test_horario_nocturno_tag_da_horario_nocturno_alto(self):
        p = perfil_desde_tags(["horario_nocturno"])
        assert p.horario_nocturno > 0.75

    def test_health_wellness_da_citas_previas_altas(self):
        p = perfil_desde_tags(["health_wellness"])
        assert p.citas_previas > 0.65

    def test_salon_citas_da_citas_previas_muy_altas(self):
        p = perfil_desde_tags(["salon_citas"])
        assert p.citas_previas > 0.75

    def test_tags_contradictorios_se_promedian(self):
        """
        alta_renta y low_cost juntos → nivel_precio debería quedar en el medio.
        """
        p = perfil_desde_tags(["alta_renta", "low_cost"])
        # alta_renta = 0.80, low_cost = 0.15 → promedio ≈ 0.475
        assert 0.30 <= p.nivel_precio <= 0.65, (
            f"Tags contradictorios deberían promediar nivel_precio, got {p.nivel_precio}"
        )

    def test_orientado_turismo_vs_clientela_local(self):
        p_turismo = perfil_desde_tags(["orientado_turismo"])
        p_local = perfil_desde_tags(["clientela_local"])
        assert p_turismo.clientela_turismo > p_local.clientela_turismo

    def test_devuelve_todos_los_valores_en_rango(self):
        tags = [
            "specialty_coffee", "dog_friendly", "alta_renta", "instagrammable",
            "clientela_local", "cultural_artistico",
        ]
        p = perfil_desde_tags(tags)
        arr = p.to_array()
        assert np.all(arr >= 0.0) and np.all(arr <= 1.0), f"Valores fuera de rango: {arr}"

    def test_combinacion_cafeteria_con_perros(self):
        """
        Una cafetería con perros: dog_friendly + clientela_local + specialty_coffee.
        Debe tener alta vecindario, baja turismo, y flujo moderado.
        """
        p = perfil_desde_tags(["dog_friendly", "clientela_local", "specialty_coffee"])
        assert p.clientela_vecindario > 0.65
        assert p.clientela_turismo < 0.25
        assert p.dependencia_flujo < 0.60

    def test_combinacion_fine_dining(self):
        """
        Un restaurante fine dining: gastronomico_premium + alta_renta + destino.
        Debe tener nivel_precio muy alto y dependencia_flujo muy baja.
        """
        p = perfil_desde_tags(["gastronomico_premium", "alta_renta", "destino"])
        assert p.nivel_precio > 0.75
        assert p.dependencia_flujo < 0.35
