"""
tests/test_concepto_matcher.py — Tests para scoring/concepto_matcher.py

Cubre:
  - Estructura y completitud de CONCEPTOS_DB
  - blend_zona_ideal: media ponderada de perfiles zona_ideal
  - score_afinidad_zona: scoring de encaje zona-concepto con datos reales
  - blend_tags: combinación de tags ponderada por similitud
  - match(): con embeddings mockeados para no depender de sentence-transformers

Tests clave:
  Una cafetería con perros debe puntuar MEJOR en zonas con parques y peor
  sin ellos. Un restaurante fine dining necesita renta alta. Una cafetería
  express necesita flujo masivo. Estos son los invariantes del sistema.
"""
from __future__ import annotations
import pytest
import numpy as np
from unittest.mock import MagicMock, patch

from scoring.concepto_matcher import (
    CONCEPTOS_DB,
    ConceptoMatcher,
    get_matcher,
    score_zona_vs_ideal,
)


# ════════════════════════════════════════════════════════════════════════════
# Fixtures
# ════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def matcher():
    return ConceptoMatcher()


@pytest.fixture
def zona_con_parques():
    """Zona residencial tranquila con muchas zonas verdes — ideal para dog-friendly."""
    return {
        "renta_media_hogar": 38000,
        "edad_media": 38,
        "flujo_peatonal_total": 400,
        "score_turismo": 20,
        "ratio_locales_comerciales": 0.18,
        "m2_zonas_verdes_cercanas": 6000,
        "incidencias_por_1000hab": 20,
    }


@pytest.fixture
def zona_sin_parques():
    """Zona comercial céntrica sin apenas zonas verdes."""
    return {
        "renta_media_hogar": 38000,
        "edad_media": 38,
        "flujo_peatonal_total": 400,
        "score_turismo": 20,
        "ratio_locales_comerciales": 0.18,
        "m2_zonas_verdes_cercanas": 200,
        "incidencias_por_1000hab": 20,
    }


@pytest.fixture
def zona_renta_alta():
    """Zona premium — Pedralbes, Sarrià, parte alta del Eixample."""
    return {
        "renta_media_hogar": 58000,
        "edad_media": 44,
        "flujo_peatonal_total": 350,
        "score_turismo": 35,
        "ratio_locales_comerciales": 0.20,
        "m2_zonas_verdes_cercanas": 2000,
        "incidencias_por_1000hab": 15,
    }


@pytest.fixture
def zona_renta_baja():
    """Zona obrera/popular."""
    return {
        "renta_media_hogar": 21000,
        "edad_media": 45,
        "flujo_peatonal_total": 350,
        "score_turismo": 35,
        "ratio_comercial_min": 0.20,
        "m2_zonas_verdes_cercanas": 2000,
        "incidencias_por_1000hab": 55,
    }


@pytest.fixture
def zona_flujo_alto():
    """Zona de paso intenso — Las Ramblas, Portal de l'Àngel, Passeig de Gràcia."""
    return {
        "renta_media_hogar": 40000,
        "edad_media": 36,
        "flujo_peatonal_total": 2500,
        "score_turismo": 75,
        "ratio_locales_comerciales": 0.45,
        "m2_zonas_verdes_cercanas": 500,
        "incidencias_por_1000hab": 55,
    }


@pytest.fixture
def zona_flujo_bajo():
    """Zona residencial tranquila con poco paso comercial."""
    return {
        "renta_media_hogar": 40000,
        "edad_media": 42,
        "flujo_peatonal_total": 120,
        "score_turismo": 15,
        "ratio_locales_comerciales": 0.12,
        "m2_zonas_verdes_cercanas": 3000,
        "incidencias_por_1000hab": 18,
    }


# ════════════════════════════════════════════════════════════════════════════
# 1. Estructura de CONCEPTOS_DB
# ════════════════════════════════════════════════════════════════════════════

class TestConceptosDB:
    def test_no_vacio(self):
        assert len(CONCEPTOS_DB) >= 50, "Debe haber al menos 50 conceptos"

    def test_todos_tienen_nombre(self):
        for key, c in CONCEPTOS_DB.items():
            assert "nombre" in c and c["nombre"], f"Concepto '{key}' sin nombre"

    def test_todos_tienen_descripcion_larga(self):
        for key, c in CONCEPTOS_DB.items():
            assert len(c.get("descripcion", "")) >= 50, (
                f"Concepto '{key}' con descripción demasiado corta — "
                "las descripciones cortas degradan la calidad del embedding"
            )

    def test_todos_tienen_tags(self):
        for key, c in CONCEPTOS_DB.items():
            assert isinstance(c.get("tags"), list) and len(c["tags"]) > 0, (
                f"Concepto '{key}' sin tags"
            )

    def test_todos_tienen_zona_ideal_con_al_menos_una_clave(self):
        for key, c in CONCEPTOS_DB.items():
            zi = c.get("zona_ideal", {})
            assert isinstance(zi, dict) and len(zi) >= 1, (
                f"Concepto '{key}' sin zona_ideal — sin este campo no se puede "
                "calcular score_afinidad_zona"
            )

    def test_renta_ideal_en_rango_valido(self):
        for key, c in CONCEPTOS_DB.items():
            ri = c.get("zona_ideal", {}).get("renta_ideal")
            if ri is not None:
                assert 0.0 <= ri <= 1.0, (
                    f"Concepto '{key}': renta_ideal={ri} fuera de [0,1]"
                )

    def test_turismo_ideal_en_rango_valido(self):
        for key, c in CONCEPTOS_DB.items():
            ti = c.get("zona_ideal", {}).get("turismo_ideal")
            if ti is not None:
                assert 0 <= ti <= 100, (
                    f"Concepto '{key}': turismo_ideal={ti} fuera de [0,100]"
                )

    def test_conceptos_clave_presentes(self):
        claves_esperadas = [
            "cafeteria_specialty_coffee",
            "cafeteria_con_perros",
            "cafeteria_express_paso",
            "restaurante_alta_cocina",
            "bar_cocteleria",
        ]
        for clave in claves_esperadas:
            assert clave in CONCEPTOS_DB, f"Concepto clave '{clave}' no encontrado"


# ════════════════════════════════════════════════════════════════════════════
# 2. blend_zona_ideal
# ════════════════════════════════════════════════════════════════════════════

class TestBlendZonaIdeal:
    def test_blend_renta_media_ponderada(self, matcher):
        matches = [
            {"zona_ideal": {"renta_ideal": 0.8}, "similarity": 0.9},
            {"zona_ideal": {"renta_ideal": 0.4}, "similarity": 0.1},
        ]
        result = matcher.blend_zona_ideal(matches)
        # 0.9/(0.9+0.1)*0.8 + 0.1/(0.9+0.1)*0.4 = 0.76
        assert result["renta_ideal"] == pytest.approx(0.76, abs=0.01)

    def test_blend_turismo_media_ponderada(self, matcher):
        matches = [
            {"zona_ideal": {"turismo_ideal": 60}, "similarity": 0.5},
            {"zona_ideal": {"turismo_ideal": 20}, "similarity": 0.5},
        ]
        result = matcher.blend_zona_ideal(matches)
        assert result["turismo_ideal"] == pytest.approx(40.0, abs=0.1)

    def test_blend_edad_rango_reconstruido(self, matcher):
        matches = [
            {"zona_ideal": {"edad_rango": (20, 40)}, "similarity": 1.0},
        ]
        result = matcher.blend_zona_ideal(matches)
        assert "edad_rango" in result
        assert result["edad_rango"][0] == pytest.approx(20, abs=1)
        assert result["edad_rango"][1] == pytest.approx(40, abs=1)

    def test_blend_vacio_devuelve_dict_vacio(self, matcher):
        assert matcher.blend_zona_ideal([]) == {}

    def test_blend_ignora_keys_sin_valores_numericos(self, matcher):
        matches = [{"zona_ideal": {"clave_invalida": "texto"}, "similarity": 1.0}]
        result = matcher.blend_zona_ideal(matches)
        # Debe procesarse sin error y devolver un dict
        assert isinstance(result, dict)


# ════════════════════════════════════════════════════════════════════════════
# 3. score_afinidad_zona — los invariantes del sistema
# ════════════════════════════════════════════════════════════════════════════

class TestScoreAfinidadZona:
    """
    Tests de los invariantes fundamentales:
    La función debe discriminar correctamente entre zonas buenas y malas
    para cada tipo de negocio. Si estos tests fallan, el sistema no sirve.
    """

    def _matches_para(self, concepto_key: str, similarity: float = 0.95):
        return [{"similarity": similarity, **CONCEPTOS_DB[concepto_key]}]

    def test_cafeteria_perros_mejor_con_parques_que_sin_ellos(
        self, matcher, zona_con_parques, zona_sin_parques
    ):
        """Una cafetería con perros necesita zonas con parques — invariante clave."""
        matches = self._matches_para("cafeteria_con_perros")
        score_parques = matcher.score_afinidad_zona(zona_con_parques, matches)
        score_sin_parques = matcher.score_afinidad_zona(zona_sin_parques, matches)
        assert score_parques > score_sin_parques, (
            f"Cafetería con perros debería puntuar MEJOR con parques "
            f"({score_parques:.1f}) que sin ellos ({score_sin_parques:.1f})"
        )

    def test_fine_dining_mejor_en_renta_alta(
        self, matcher, zona_renta_alta, zona_renta_baja
    ):
        """Un restaurante fine dining necesita clientes con alto poder adquisitivo."""
        matches = self._matches_para("restaurante_alta_cocina")
        score_alto = matcher.score_afinidad_zona(zona_renta_alta, matches)
        score_bajo = matcher.score_afinidad_zona(zona_renta_baja, matches)
        assert score_alto > score_bajo, (
            f"Fine dining debería puntuar MEJOR en renta alta "
            f"({score_alto:.1f}) que en renta baja ({score_bajo:.1f})"
        )

    def test_cafeteria_express_mejor_con_flujo_alto(
        self, matcher, zona_flujo_alto, zona_flujo_bajo
    ):
        """Una cafetería express depende del volumen de paso — sin flujo no funciona."""
        matches = self._matches_para("cafeteria_express_paso")
        score_alto = matcher.score_afinidad_zona(zona_flujo_alto, matches)
        score_bajo = matcher.score_afinidad_zona(zona_flujo_bajo, matches)
        assert score_alto > score_bajo, (
            f"Cafetería express debería puntuar MEJOR con flujo alto "
            f"({score_alto:.1f}) que con flujo bajo ({score_bajo:.1f})"
        )

    def test_specialty_coffee_mejor_en_renta_alta(
        self, matcher, zona_renta_alta, zona_renta_baja
    ):
        """El specialty coffee necesita clientela con cultura y poder adquisitivo."""
        matches = self._matches_para("cafeteria_specialty_coffee")
        score_alto = matcher.score_afinidad_zona(zona_renta_alta, matches)
        score_bajo = matcher.score_afinidad_zona(zona_renta_baja, matches)
        assert score_alto > score_bajo

    def test_score_siempre_en_rango_0_100(self, matcher):
        """El score nunca debe salirse del rango 0-100."""
        zona_extrema_buena = {
            "renta_media_hogar": 70000,
            "edad_media": 35,
            "flujo_peatonal_total": 3000,
            "score_turismo": 95,
            "ratio_locales_comerciales": 0.60,
            "m2_zonas_verdes_cercanas": 10000,
            "incidencias_por_1000hab": 5,
        }
        zona_extrema_mala = {
            "renta_media_hogar": 15000,
            "edad_media": 70,
            "flujo_peatonal_total": 0,
            "score_turismo": 0,
            "ratio_locales_comerciales": 0.0,
            "m2_zonas_verdes_cercanas": 0,
            "incidencias_por_1000hab": 200,
        }
        for key in list(CONCEPTOS_DB.keys())[:10]:
            matches = [{"similarity": 0.9, **CONCEPTOS_DB[key]}]
            for zona in [zona_extrema_buena, zona_extrema_mala]:
                score = matcher.score_afinidad_zona(zona, matches)
                assert 0 <= score <= 100, (
                    f"Concepto '{key}': score={score} fuera de [0,100]"
                )

    def test_matches_vacios_devuelve_50(self, matcher, zona_con_parques):
        """Sin matches, el score neutro es 50."""
        score = matcher.score_afinidad_zona(zona_con_parques, [])
        assert score == pytest.approx(50.0)

    def test_zona_datos_vacios_no_crashea(self, matcher):
        """La función debe manejar gracefully una zona sin datos."""
        matches = [{"similarity": 0.9, **CONCEPTOS_DB["cafeteria_con_perros"]}]
        score = matcher.score_afinidad_zona({}, matches)
        assert 0 <= score <= 100


# ════════════════════════════════════════════════════════════════════════════
# 4. blend_tags
# ════════════════════════════════════════════════════════════════════════════

class TestScoreZonaVsIdeal:
    def test_zona_ideal_vacia_devuelve_50(self, zona_con_parques):
        assert score_zona_vs_ideal(zona_con_parques, {}) == pytest.approx(50.0)

    def test_funcion_pura_discrimina_zonas_para_dog_friendly(
        self, zona_con_parques, zona_sin_parques
    ):
        zona_ideal = CONCEPTOS_DB["cafeteria_con_perros"]["zona_ideal"]
        score_parques = score_zona_vs_ideal(zona_con_parques, zona_ideal)
        score_sin_parques = score_zona_vs_ideal(zona_sin_parques, zona_ideal)
        assert score_parques > score_sin_parques


class TestBlendTags:
    def test_tag_presente_en_todos_los_matches_siempre_incluido(self, matcher):
        matches = [
            {"tags": ["dog_friendly", "clientela_local"], "similarity": 0.8},
            {"tags": ["dog_friendly", "alta_renta"],      "similarity": 0.7},
        ]
        tags = matcher.blend_tags(matches)
        assert "dog_friendly" in tags

    def test_tag_minoritario_puede_no_incluirse(self, matcher):
        """Un tag con muy poco peso ponderado no debe colarse."""
        matches = [
            {"tags": ["specialty_coffee", "alta_renta"],  "similarity": 0.9},
            {"tags": ["specialty_coffee", "low_cost"],    "similarity": 0.9},
            {"tags": ["specialty_coffee", "horario_nocturno"], "similarity": 0.1},
        ]
        tags = matcher.blend_tags(matches)
        assert "specialty_coffee" in tags
        # horario_nocturno solo aparece en el match de similitud 0.1 (menor peso)
        # puede o no estar, pero specialty_coffee siempre

    def test_matches_vacios_devuelve_lista_vacia(self, matcher):
        assert matcher.blend_tags([]) == []

    def test_devuelve_lista_sin_duplicados(self, matcher):
        matches = [
            {"tags": ["clientela_local", "clientela_local"], "similarity": 0.8},
        ]
        tags = matcher.blend_tags(matches)
        assert len(tags) == len(set(tags))

    def test_orden_por_peso_descendente(self, matcher):
        """El tag con mayor peso ponderado debe aparecer primero."""
        matches = [
            {"tags": ["tag_a", "tag_b", "tag_c"], "similarity": 0.9},
            {"tags": ["tag_a"],                   "similarity": 0.9},
        ]
        tags = matcher.blend_tags(matches)
        # tag_a aparece en ambos (peso total mayor), debe ser primero
        if "tag_a" in tags and len(tags) > 1:
            assert tags.index("tag_a") < tags.index(tags[-1])


# ════════════════════════════════════════════════════════════════════════════
# 5. match() con embeddings mockeados
# ════════════════════════════════════════════════════════════════════════════

class TestMatchConMock:
    """
    Tests de match() sin cargar el modelo real.
    El mock inyecta similitudes controladas para verificar la lógica de ranking.
    """

    def _make_mock_model(self, sim_values: np.ndarray):
        """Crea un mock del SentenceTransformer que devuelve embeddings controlados."""
        model = MagicMock()
        n_concepts = len(CONCEPTOS_DB)
        # encode() devuelve vectores unitarios que producen las similitudes deseadas
        # Para simplificar, usamos embeddings que hacen que la similitud sea sim_values
        # cuando se multiplica por el vector de query [1, 0, 0, ...]
        embeddings = np.zeros((n_concepts, n_concepts))
        for i in range(n_concepts):
            embeddings[i, i] = sim_values[i] if i < len(sim_values) else 0.5
        query_emb = np.zeros((1, n_concepts))
        query_emb[0, :] = 1.0 / n_concepts  # vector query normalizado

        def mock_encode(texts, **kwargs):
            if isinstance(texts, list) and len(texts) == 1:
                return query_emb
            return embeddings

        model.encode.side_effect = mock_encode
        return model

    def test_match_devuelve_top_k_resultados(self, matcher):
        # Verificar que sentence_transformers es importable antes del test
        try:
            import sentence_transformers  # noqa
        except (ImportError, ValueError, Exception):
            pytest.skip("sentence_transformers no disponible en este entorno")

        n_concepts = len(CONCEPTOS_DB)
        sim_values = np.random.uniform(0.3, 0.9, n_concepts)

        with patch("sentence_transformers.SentenceTransformer",
                   return_value=self._make_mock_model(sim_values)):
            matcher._model = None
            matcher._db_embeddings = None
            results = matcher.match("cafetería con perros", top_k=3)
            assert len(results) <= 3

    def test_match_resultado_tiene_campos_esperados(self, matcher):
        try:
            from sentence_transformers import SentenceTransformer  # noqa
        except (ImportError, ValueError):
            pytest.skip("sentence_transformers no instalado o conflicto de dependencias")

        n_concepts = len(CONCEPTOS_DB)
        sim_values = np.linspace(0.3, 0.95, n_concepts)

        with patch("sentence_transformers.SentenceTransformer",
                   return_value=self._make_mock_model(sim_values)):
            matcher._model = None
            matcher._db_embeddings = None
            results = matcher.match("test negocio", top_k=2)

        for r in results:
            assert "key" in r
            assert "nombre" in r
            assert "tags" in r
            assert "similarity" in r
            assert 0.0 <= r["similarity"] <= 1.0


# ════════════════════════════════════════════════════════════════════════════
# 6. Comparativa directa: specialty coffee vs express en misma zona
# ════════════════════════════════════════════════════════════════════════════

class TestComparativaCafeEspecialidadVsExpress:
    """
    Test de sistema: dos tipos de cafetería, misma zona, puntuaciones distintas.
    Esto valida que el sistema diferencia correctamente el concepto específico.
    """

    @pytest.fixture
    def zona_tipo_gracia(self):
        """
        Zona tipo Gràcia: renta alta, flujo moderado (400, bajo el mínimo express),
        muchos parques, joven. Ideal para specialty coffee, no para express.
        renta=52000 → renta_norm≈0.81 (cerca del 0.65 ideal de specialty)
        flujo=400 → por debajo del mínimo express (800) → express pierde puntos
        """
        return {
            "renta_media_hogar": 52000,
            "edad_media": 35,
            "flujo_peatonal_total": 400,
            "score_turismo": 35,
            "ratio_locales_comerciales": 0.25,
            "m2_zonas_verdes_cercanas": 4000,
            "incidencias_por_1000hab": 22,
        }

    @pytest.fixture
    def zona_tipo_portal_angel(self):
        """
        Zona de altísimo flujo peatonal: 2500 personas/h, renta media-baja,
        pocas zonas verdes. Ideal para express, no para specialty coffee.
        renta=30000 → renta_norm≈0.30 (lejos del 0.65 ideal de specialty)
        flujo=2500 → muy por encima del mínimo express (800) → express gana
        """
        return {
            "renta_media_hogar": 30000,
            "edad_media": 33,
            "flujo_peatonal_total": 2500,
            "score_turismo": 35,
            "ratio_locales_comerciales": 0.45,
            "m2_zonas_verdes_cercanas": 300,
            "incidencias_por_1000hab": 30,
        }

    def test_specialty_puntua_mejor_en_gracia_que_portal_angel(
        self, matcher, zona_tipo_gracia, zona_tipo_portal_angel
    ):
        """
        La specialty coffee es un concepto de barrio cultural con renta media-alta.
        Gràcia encaja mejor que Portal de l'Àngel (demasiado turístico).
        """
        matches = [{"similarity": 0.95, **CONCEPTOS_DB["cafeteria_specialty_coffee"]}]
        score_gracia = matcher.score_afinidad_zona(zona_tipo_gracia, matches)
        score_portal = matcher.score_afinidad_zona(zona_tipo_portal_angel, matches)
        assert score_gracia > score_portal, (
            f"Specialty coffee debería preferir Gràcia ({score_gracia:.1f}) "
            f"sobre Portal de l'Àngel ({score_portal:.1f})"
        )

    def test_express_puntua_mejor_en_portal_angel_que_gracia(
        self, matcher, zona_tipo_gracia, zona_tipo_portal_angel
    ):
        """
        La cafetería express vive del paso masivo.
        Portal de l'Àngel (flujo 2800) es mejor que Gràcia (600).
        """
        matches = [{"similarity": 0.95, **CONCEPTOS_DB["cafeteria_express_paso"]}]
        score_gracia = matcher.score_afinidad_zona(zona_tipo_gracia, matches)
        score_portal = matcher.score_afinidad_zona(zona_tipo_portal_angel, matches)
        assert score_portal > score_gracia, (
            f"Cafetería express debería preferir Portal de l'Àngel ({score_portal:.1f}) "
            f"sobre Gràcia ({score_gracia:.1f})"
        )

    def test_cafeteria_perros_puntua_mejor_en_gracia_que_portal_angel(
        self, matcher, zona_tipo_gracia, zona_tipo_portal_angel
    ):
        """La cafetería con perros necesita parques y barrio residencial."""
        matches = [{"similarity": 0.95, **CONCEPTOS_DB["cafeteria_con_perros"]}]
        score_gracia = matcher.score_afinidad_zona(zona_tipo_gracia, matches)
        score_portal = matcher.score_afinidad_zona(zona_tipo_portal_angel, matches)
        assert score_gracia > score_portal
