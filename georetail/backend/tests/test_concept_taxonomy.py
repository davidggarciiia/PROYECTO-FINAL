from __future__ import annotations

import pytest

from scoring.concepto.taxonomy import (
    BASE_CONCEPTS,
    CATALOG,
    MODIFIER_TAGS,
    VISIBLE_TAGS,
    canonical_tag_count,
    compilar_concepto_negocio,
)


class TestTaxonomiaRica:
    def test_hay_muchos_tags_canonicos(self):
        assert 250 <= canonical_tag_count() <= 400

    def test_base_concepts_en_rango_esperado(self):
        assert 60 <= len(BASE_CONCEPTS) <= 90

    def test_modifiers_en_rango_esperado(self):
        assert 190 <= len(MODIFIER_TAGS) <= 310

    def test_todos_los_tags_tienen_namespace_y_tipo(self):
        for canonical_id, entry in CATALOG.items():
            assert entry["namespace"]
            assert entry["kind"] in {"base", "modifier"}
            assert canonical_id == entry["id"]


class TestCompilacionConceptual:
    def test_specialty_dog_friendly_compila_con_base_y_modifiers(self):
        concepto = compilar_concepto_negocio(
            sector="restauracion",
            idea_tags=["specialty_coffee", "dog_friendly", "clientela_local"],
        )
        assert concepto["sector"] == "restauracion"
        assert concepto["base_concepts"][0]["id"].startswith("base.restauracion.")
        assert "dog_friendly" in concepto["idea_tags"]
        assert concepto["perfil_negocio"]["clientela_vecindario"] > 0.65
        assert concepto["perfil_negocio"]["clientela_turismo"] < 0.25
        assert concepto["zona_ideal"]["zonas_verdes_bonus"] > 0.5

    def test_express_vs_fine_dining_dan_perfiles_distintos(self):
        express = compilar_concepto_negocio(
            sector="restauracion",
            idea_tags=["street_food", "low_cost", "alta_rotacion_clientes"],
        )
        fine = compilar_concepto_negocio(
            sector="restauracion",
            idea_tags=["gastronomico_premium", "alta_renta", "destino"],
        )
        assert express["perfil_negocio"]["dependencia_flujo"] > fine["perfil_negocio"]["dependencia_flujo"]
        assert fine["perfil_negocio"]["nivel_precio"] > express["perfil_negocio"]["nivel_precio"]

    def test_salida_visible_queda_capada(self):
        concepto = compilar_concepto_negocio(
            sector="restauracion",
            modifiers=[
                {"id": "experience.dog_friendly", "weight": 1.0},
                {"id": "experience.instagrammable", "weight": 0.9},
                {"id": "audience.locals_first", "weight": 0.9},
                {"id": "audience.young_adults", "weight": 0.8},
                {"id": "price.premium", "weight": 0.8},
                {"id": "channel.takeaway_delivery", "weight": 0.8},
                {"id": "service.appointment_journeys", "weight": 0.8},
                {"id": "daypart.night", "weight": 0.7},
                {"id": "location.destination", "weight": 0.7},
                {"id": "ops.high_rotation", "weight": 0.7},
                {"id": "space.large_format", "weight": 0.7},
                {"id": "channel.fashion_retail", "weight": 0.6},
                {"id": "experience.wellness", "weight": 0.6},
            ],
        )
        assert len(concepto["idea_tags"]) <= 12

    def test_alias_legacy_se_normaliza(self):
        concepto = compilar_concepto_negocio(
            sector="deporte",
            idea_tags=["fitness_boutique", "citas_previas", "alta_renta", "health_wellness"],
        )
        assert concepto["perfil_negocio"]["citas_previas"] > 0.7
        assert concepto["perfil_negocio"]["nivel_precio"] > 0.6


class TestVisibilidad:
    def test_tags_visibles_siguen_existiendo(self):
        claves = {
            "specialty_coffee",
            "dog_friendly",
            "health_wellness",
            "gastronomico_premium",
            "street_food",
            "orientado_turismo",
            "clientela_local",
            "alta_renta",
            "low_cost",
            "horario_nocturno",
            "takeaway_delivery",
            "salon_citas",
            "retail_moda",
        }
        assert claves.issubset(VISIBLE_TAGS)

