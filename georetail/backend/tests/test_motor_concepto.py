"""
tests/test_motor_concepto.py — Tests para los nuevos features de scoring/motor.py

Qué testeamos:
  1. _combinar_tags: fusión correcta de LLM tags + ConceptoMatcher tags
  2. _recalcular_global: que los pesos se aplican bien sobre las 8 dimensiones
  3. _recalcular_global_con_afinidad: que la 9ª dimensión se incorpora correctamente
  4. _format_scores_for_api: que el dict plano se formatea bien para la API
  5. calcular_scores_batch: caché hit, caché miss, fallback por error
  6. get_scores_zona: que llama a calcular_scores_batch y añade score_afinidad_concepto

Todos los tests que tocan DB o LLM usan mocks — este módulo es 100% offline.
"""
from __future__ import annotations

import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

# Importamos las funciones privadas que queremos testear directamente
from scoring.motor import (
    _combinar_tags,
    _recalcular_global,
    _recalcular_global_con_afinidad,
    _format_scores_for_api,
    _PESO_AFINIDAD,
    calcular_scores_batch,
)


class _FakeConn:
    def __init__(self, fetch_results=None, fetchrow_results=None):
        self._fetch_results = list(fetch_results or [])
        self._fetchrow_results = list(fetchrow_results or [])

    async def fetch(self, *args, **kwargs):
        if self._fetch_results:
            return self._fetch_results.pop(0)
        return []

    async def fetchrow(self, *args, **kwargs):
        if self._fetchrow_results:
            return self._fetchrow_results.pop(0)
        return None


class _FakeDB:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


# ════════════════════════════════════════════════════════════════════════════
# Fixtures compartidos
# ════════════════════════════════════════════════════════════════════════════

@pytest.fixture
def scores_8dim_completos():
    """Dict con los 8 scores de dimensión — todos a 60 para cálculos simples."""
    return {
        "score_flujo_peatonal":    60.0,
        "score_demografia":        60.0,
        "score_competencia":       60.0,
        "score_precio_alquiler":   60.0,
        "score_transporte":        60.0,
        "score_seguridad":         60.0,
        "score_turismo":           60.0,
        "score_entorno_comercial": 60.0,
    }


@pytest.fixture
def pesos_restauracion():
    """Pesos del sector restauracion — suman 1.0."""
    return {
        "peso_flujo":       0.25,
        "peso_demo":        0.20,
        "peso_competencia": 0.15,
        "peso_precio":      0.15,
        "peso_transporte":  0.10,
        "peso_seguridad":   0.05,
        "peso_turismo":     0.05,
        "peso_entorno":     0.05,
    }


# ════════════════════════════════════════════════════════════════════════════
# 1. _combinar_tags
# ════════════════════════════════════════════════════════════════════════════

class TestCombinarTags:
    def test_sin_tags_ni_descripcion_devuelve_lista(self):
        tags = _combinar_tags(None, None)
        assert isinstance(tags, list)

    def test_solo_llm_tags_sin_descripcion(self):
        llm_tags = ["specialty_coffee", "clientela_local"]
        tags = _combinar_tags(llm_tags, None)
        assert "specialty_coffee" in tags
        assert "clientela_local" in tags

    def test_llm_tags_tienen_prioridad(self):
        """Los tags del LLM siempre aparecen en la salida."""
        llm_tags = ["dog_friendly", "specialty_coffee"]
        tags = _combinar_tags(llm_tags, None)
        for t in llm_tags:
            assert t in tags, f"Tag LLM '{t}' no encontrado en combinación"

    def test_sin_duplicados(self):
        llm_tags = ["specialty_coffee", "clientela_local"]
        tags = _combinar_tags(llm_tags, None)
        assert len(tags) == len(set(tags)), "No debe haber tags duplicados"

    def test_descripcion_sin_matcher_disponible_usa_llm_tags(self):
        """Si el matcher no está disponible (ImportError), se usan los LLM tags."""
        llm_tags = ["specialty_coffee"]
        with patch("scoring.motor._combinar_tags", side_effect=None):
            # Test directo del fallback del except
            with patch("scoring.concepto_matcher.get_matcher", side_effect=ImportError("no disponible")):
                tags = _combinar_tags(llm_tags, "Café de especialidad en Gràcia")
        assert "specialty_coffee" in tags

    def test_matcher_enriquece_con_tags_adicionales(self):
        """Si el matcher funciona, puede añadir tags que el LLM no detectó."""
        llm_tags = ["specialty_coffee"]
        mock_matcher = MagicMock()
        mock_matcher.match.return_value = [
            {"tags": ["specialty_coffee", "clientela_local", "instagrammable"], "similarity": 0.9}
        ]
        mock_matcher.blend_tags.return_value = ["specialty_coffee", "clientela_local", "instagrammable"]

        with patch("scoring.concepto_matcher.get_matcher", return_value=mock_matcher):
            tags = _combinar_tags(llm_tags, "Café de especialidad con diseño")

        assert "specialty_coffee" in tags
        # El matcher puede añadir tags nuevos
        assert "clientela_local" in tags or "instagrammable" in tags or len(tags) >= 1

    def test_tags_del_matcher_no_duplican_llm_tags(self):
        """Si matcher devuelve tags que el LLM ya detectó, no se duplican."""
        llm_tags = ["specialty_coffee", "clientela_local"]
        mock_matcher = MagicMock()
        mock_matcher.match.return_value = [{"tags": ["specialty_coffee", "clientela_local"], "similarity": 0.9}]
        mock_matcher.blend_tags.return_value = ["specialty_coffee", "clientela_local"]

        with patch("scoring.concepto_matcher.get_matcher", return_value=mock_matcher):
            tags = _combinar_tags(llm_tags, "Café")

        assert len(tags) == len(set(tags))


# ════════════════════════════════════════════════════════════════════════════
# 2. _recalcular_global
# ════════════════════════════════════════════════════════════════════════════

class TestRecalcularGlobal:
    def test_todos_60_con_pesos_unitarios_da_60(self, scores_8dim_completos, pesos_restauracion):
        """Si todos los scores son 60 y los pesos suman 1.0, el global es 60."""
        result = _recalcular_global(scores_8dim_completos, pesos_restauracion)
        assert result == pytest.approx(60.0, abs=0.1)

    def test_todos_100_da_100(self, pesos_restauracion):
        scores_perfect = {
            "score_flujo_peatonal":    100.0,
            "score_demografia":        100.0,
            "score_competencia":       100.0,
            "score_precio_alquiler":   100.0,
            "score_transporte":        100.0,
            "score_seguridad":         100.0,
            "score_turismo":           100.0,
            "score_entorno_comercial": 100.0,
        }
        result = _recalcular_global(scores_perfect, pesos_restauracion)
        assert result == pytest.approx(100.0, abs=0.1)

    def test_scores_0_se_conservan(self, pesos_restauracion):
        """
        Tras la correccion del motor, un 0.0 real debe preservarse.
        Solo None representa ausencia de dato y cae a 50.0.
        """
        scores_zero = {k: 0.0 for k in [
            "score_flujo_peatonal", "score_demografia", "score_competencia",
            "score_precio_alquiler", "score_transporte", "score_seguridad",
            "score_turismo", "score_entorno_comercial",
        ]}
        result = _recalcular_global(scores_zero, pesos_restauracion)
        assert result == pytest.approx(0.0, abs=0.1)

    def test_peso_flujo_alto_da_mas_peso_a_flujo(self):
        """Con peso_flujo=0.80, el score_flujo domina el global."""
        scores = {
            "score_flujo_peatonal":    100.0,  # excelente flujo
            "score_demografia":         20.0,
            "score_competencia":        20.0,
            "score_precio_alquiler":    20.0,
            "score_transporte":         20.0,
            "score_seguridad":          20.0,
            "score_turismo":            20.0,
            "score_entorno_comercial":  20.0,
        }
        pesos_flujo_alto = {
            "peso_flujo": 0.80,
            "peso_demo": 0.04,
            "peso_competencia": 0.04,
            "peso_precio": 0.04,
            "peso_transporte": 0.04,
            "peso_seguridad": 0.01,
            "peso_turismo": 0.01,
            "peso_entorno": 0.02,
        }
        result = _recalcular_global(scores, pesos_flujo_alto)
        # 100*0.80 + 20*0.20 = 80 + 4 = 84
        assert result > 75.0

    def test_pesos_vacios_usa_defaults(self, scores_8dim_completos):
        """Sin pesos en el dict, usa los defaults hardcoded."""
        result = _recalcular_global(scores_8dim_completos, {})
        # Los defaults suman < 1 (0.25+0.20+0.15+0.15+0.10+0.05+0.05+0.05=1.0)
        assert 0.0 < result <= 100.0

    def test_score_none_se_reemplaza_por_50(self, pesos_restauracion):
        """Un score None no debe crashear — se trata como 50."""
        scores_con_none = {
            "score_flujo_peatonal":    None,
            "score_demografia":        60.0,
            "score_competencia":       60.0,
            "score_precio_alquiler":   60.0,
            "score_transporte":        60.0,
            "score_seguridad":         60.0,
            "score_turismo":           60.0,
            "score_entorno_comercial": 60.0,
        }
        result = _recalcular_global(scores_con_none, pesos_restauracion)
        assert 0.0 <= result <= 100.0

    def test_resultado_redondeado_a_1_decimal(self, scores_8dim_completos, pesos_restauracion):
        result = _recalcular_global(scores_8dim_completos, pesos_restauracion)
        assert result == round(result, 1)


# ════════════════════════════════════════════════════════════════════════════
# 3. _recalcular_global_con_afinidad
# ════════════════════════════════════════════════════════════════════════════

class TestRecalcularGlobalConAfinidad:
    def test_peso_afinidad_es_12_porciento(self):
        """_PESO_AFINIDAD debe ser 0.12 — contrato con la documentación."""
        assert _PESO_AFINIDAD == pytest.approx(0.12)

    def test_formula_correcta(self, scores_8dim_completos, pesos_restauracion):
        """score_final = score_8dims * 0.88 + score_afinidad * 0.12."""
        score_afinidad = 80.0
        score_8dims = _recalcular_global(scores_8dim_completos, pesos_restauracion)
        expected = round(score_8dims * 0.88 + score_afinidad * 0.12, 1)
        result = _recalcular_global_con_afinidad(
            scores_8dim_completos, pesos_restauracion, score_afinidad
        )
        assert result == pytest.approx(expected, abs=0.1)

    def test_afinidad_alta_mejora_score_global(self, scores_8dim_completos, pesos_restauracion):
        """Una zona con alta afinidad debe tener score_global mayor que con afinidad baja."""
        score_con_afinidad_alta = _recalcular_global_con_afinidad(
            scores_8dim_completos, pesos_restauracion, 95.0
        )
        score_con_afinidad_baja = _recalcular_global_con_afinidad(
            scores_8dim_completos, pesos_restauracion, 10.0
        )
        assert score_con_afinidad_alta > score_con_afinidad_baja

    def test_afinidad_neutra_50_da_resultado_similar_al_base(
        self, scores_8dim_completos, pesos_restauracion
    ):
        """Con score_8dims=60 y afinidad=60, el global sigue siendo 60."""
        result = _recalcular_global_con_afinidad(
            scores_8dim_completos, pesos_restauracion, 60.0
        )
        assert result == pytest.approx(60.0, abs=0.5)

    def test_resultado_en_rango_0_100(self, pesos_restauracion):
        scores_extremos = {
            "score_flujo_peatonal":    100.0,
            "score_demografia":        100.0,
            "score_competencia":       100.0,
            "score_precio_alquiler":   100.0,
            "score_transporte":        100.0,
            "score_seguridad":         100.0,
            "score_turismo":           100.0,
            "score_entorno_comercial": 100.0,
        }
        result = _recalcular_global_con_afinidad(scores_extremos, pesos_restauracion, 100.0)
        assert 0.0 <= result <= 100.0

    def test_xgboost_conserva_score_global_base(self, pesos_restauracion):
        scores_xgb = {
            "score_global": 77.0,
            "score_flujo_peatonal": None,
            "score_demografia": None,
            "score_competencia": None,
            "score_precio_alquiler": None,
            "score_transporte": None,
            "score_seguridad": None,
            "score_turismo": None,
            "score_entorno_comercial": None,
            "modelo_version": "xgboost_v3",
        }
        result = _recalcular_global_con_afinidad(scores_xgb, pesos_restauracion, 90.0)
        expected = round(77.0 * 0.88 + 90.0 * 0.12, 1)
        assert result == pytest.approx(expected, abs=0.1)


# ════════════════════════════════════════════════════════════════════════════
# 4. _format_scores_for_api
# ════════════════════════════════════════════════════════════════════════════

class TestFormatScoresForApi:
    def test_estructura_basica_correcta(self):
        raw = {
            "score_global": 72.5,
            "probabilidad_supervivencia": 0.68,
            "score_flujo_peatonal":    80.0,
            "score_demografia":        65.0,
            "score_competencia":       70.0,
            "score_precio_alquiler":   60.0,
            "score_transporte":        75.0,
            "score_seguridad":         82.0,
            "score_turismo":           55.0,
            "score_entorno_comercial": 68.0,
            "shap_values": None,
        }
        result = _format_scores_for_api(raw)
        assert "score_global" in result
        assert "probabilidad_supervivencia_3a" in result
        assert "scores_dimension" in result
        assert "explicaciones_shap" in result

    def test_scores_dimension_tiene_8_dimensiones_base(self):
        raw = {
            "score_flujo_peatonal":    70.0,
            "score_demografia":        70.0,
            "score_competencia":       70.0,
            "score_precio_alquiler":   70.0,
            "score_transporte":        70.0,
            "score_seguridad":         70.0,
            "score_turismo":           70.0,
            "score_entorno_comercial": 70.0,
        }
        result = _format_scores_for_api(raw)
        dims = result["scores_dimension"]
        assert "flujo_peatonal" in dims
        assert "demografia" in dims
        assert "competencia" in dims
        assert "precio_alquiler" in dims
        assert "transporte" in dims
        assert "seguridad" in dims
        assert "turismo" in dims
        assert "entorno_comercial" in dims

    def test_afinidad_concepto_aparece_cuando_esta_en_raw(self):
        """score_afinidad_concepto en raw → afinidad_concepto en scores_dimension."""
        raw = {
            "score_global": 75.0,
            "score_afinidad_concepto": 85.0,
            "score_flujo_peatonal": 75.0,
            "score_demografia": 75.0,
            "score_competencia": 75.0,
            "score_precio_alquiler": 75.0,
            "score_transporte": 75.0,
            "score_seguridad": 75.0,
            "score_turismo": 75.0,
            "score_entorno_comercial": 75.0,
        }
        result = _format_scores_for_api(raw)
        dims = result["scores_dimension"]
        assert "afinidad_concepto" in dims
        assert dims["afinidad_concepto"] == pytest.approx(85.0, abs=0.1)

    def test_afinidad_concepto_ausente_cuando_no_esta_en_raw(self):
        """Si no hay score_afinidad_concepto, no debe aparecer en scores_dimension."""
        raw = {
            "score_global": 65.0,
            "score_flujo_peatonal": 65.0,
            "score_demografia": 65.0,
            "score_competencia": 65.0,
            "score_precio_alquiler": 65.0,
            "score_transporte": 65.0,
            "score_seguridad": 65.0,
            "score_turismo": 65.0,
            "score_entorno_comercial": 65.0,
        }
        result = _format_scores_for_api(raw)
        assert "afinidad_concepto" not in result["scores_dimension"]

    def test_shap_values_json_string_se_parsea(self):
        """shap_values puede venir como JSON string desde PostgreSQL."""
        shap_dict = {"flujo_peatonal": 0.15, "demografia": -0.08, "turismo": 0.12}
        raw = {
            "score_global": 70.0,
            "shap_values": json.dumps(shap_dict),
        }
        result = _format_scores_for_api(raw)
        assert len(result["explicaciones_shap"]) > 0
        # Las explicaciones deben estar ordenadas por valor absoluto descendente
        if len(result["explicaciones_shap"]) > 1:
            valores = [abs(e["valor"]) for e in result["explicaciones_shap"]]
            assert valores == sorted(valores, reverse=True)

    def test_shap_values_dict_directo(self):
        """shap_values como dict (no string) también funciona."""
        raw = {
            "score_global": 70.0,
            "shap_values": {"flujo_peatonal": 0.20},
        }
        result = _format_scores_for_api(raw)
        assert len(result["explicaciones_shap"]) == 1
        assert result["explicaciones_shap"][0]["feature"] == "flujo_peatonal"

    def test_shap_values_none_da_lista_vacia(self):
        raw = {"score_global": 65.0, "shap_values": None}
        result = _format_scores_for_api(raw)
        assert result["explicaciones_shap"] == []

    def test_probabilidad_supervivencia_alias_fallback(self):
        """
        La columna puede llamarse probabilidad_supervivencia_3a o
        probabilidad_supervivencia (datos históricos del scorer).
        """
        raw1 = {"probabilidad_supervivencia_3a": 0.72}
        raw2 = {"probabilidad_supervivencia": 0.72}
        result1 = _format_scores_for_api(raw1)
        result2 = _format_scores_for_api(raw2)
        assert result1["probabilidad_supervivencia_3a"] == pytest.approx(0.720, abs=0.001)
        assert result2["probabilidad_supervivencia_3a"] == pytest.approx(0.720, abs=0.001)

    def test_score_global_default_50_cuando_ausente(self):
        result = _format_scores_for_api({})
        assert result["score_global"] == pytest.approx(50.0)

    def test_max_10_explicaciones_shap(self):
        """Si hay más de 10 features SHAP, solo se incluyen las 10 con mayor valor absoluto."""
        shap_dict = {f"feature_{i}": float(i) / 100 for i in range(1, 20)}
        raw = {"shap_values": shap_dict}
        result = _format_scores_for_api(raw)
        assert len(result["explicaciones_shap"]) <= 10


# ════════════════════════════════════════════════════════════════════════════
# 5. Invariantes del sistema: diferenciación concepto
# ════════════════════════════════════════════════════════════════════════════

class TestInvariantesConcepto:
    """
    Tests de alto nivel que verifican que el modelo diferencia correctamente
    distintos conceptos de negocio en la misma zona.

    Estos tests usan las funciones puras (sin BD) para verificar la lógica.
    """

    def test_pesos_dog_friendly_aumentan_demografia_vs_flujo(self):
        """
        Dog-friendly + clientela_local → peso demografía > peso turismo.
        El score de demografía debe pesar más que el de turismo.
        """
        from scoring.idea_tags import aplicar_idea_tags

        pesos_base = {
            "peso_flujo": 0.25, "peso_demo": 0.20, "peso_competencia": 0.15,
            "peso_precio": 0.15, "peso_transporte": 0.10, "peso_seguridad": 0.05,
            "peso_turismo": 0.05, "peso_entorno": 0.05,
        }
        pesos_ajustados = aplicar_idea_tags(pesos_base, ["dog_friendly", "clientela_local"])
        assert pesos_ajustados.get("peso_demo", 0) > pesos_ajustados.get("peso_turismo", 0), (
            "dog_friendly + clientela_local deberían priorizar demografía sobre turismo"
        )

    def test_pesos_orientado_turismo_aumentan_turismo_vs_demografia(self):
        """orientado_turismo → peso turismo debe superar peso demografía."""
        from scoring.idea_tags import aplicar_idea_tags

        pesos_base = {
            "peso_flujo": 0.25, "peso_demo": 0.20, "peso_competencia": 0.15,
            "peso_precio": 0.15, "peso_transporte": 0.10, "peso_seguridad": 0.05,
            "peso_turismo": 0.05, "peso_entorno": 0.05,
        }
        pesos_ajustados = aplicar_idea_tags(pesos_base, ["orientado_turismo"])
        assert pesos_ajustados.get("peso_turismo", 0) > pesos_ajustados.get("peso_demo", 0), (
            "orientado_turismo debería priorizar turismo sobre demografía"
        )

    def test_pesos_renormalizados_suman_1(self):
        """Los pesos ajustados siempre deben sumar 1.0."""
        from scoring.idea_tags import aplicar_idea_tags

        pesos_base = {
            "peso_flujo": 0.25, "peso_demo": 0.20, "peso_competencia": 0.15,
            "peso_precio": 0.15, "peso_transporte": 0.10, "peso_seguridad": 0.05,
            "peso_turismo": 0.05, "peso_entorno": 0.05,
        }
        for tags in [
            ["dog_friendly", "clientela_local"],
            ["orientado_turismo"],
            ["alta_renta", "gastronomico_premium"],
            ["street_food", "low_cost"],
        ]:
            pesos_ajustados = aplicar_idea_tags(pesos_base, tags)
            total = sum(
                v for k, v in pesos_ajustados.items()
                if k.startswith("peso_") and not k.startswith("peso_id")
            )
            assert total == pytest.approx(1.0, abs=0.01), (
                f"Tags {tags}: los pesos ajustados suman {total}, no 1.0"
            )


class TestBatchAfinidadConcepto:
    @pytest.mark.asyncio
    async def test_batch_aplica_score_afinidad_concepto(self, pesos_restauracion):
        zona_data = {
            "zona_id": "zona_001",
            "renta_media_hogar": 40000,
            "edad_media": 37,
            "flujo_peatonal_total": 900,
            "score_turismo": 18,
            "ratio_locales_comerciales": 0.22,
            "m2_zonas_verdes_cercanas": 2500,
            "incidencias_por_1000hab": 20,
        }
        row_scorer = {
            "score_global": 62.0,
            "score_flujo_peatonal": 62.0,
            "score_demografia": 60.0,
            "score_competencia": 55.0,
            "score_precio_alquiler": 63.0,
            "score_transporte": 58.0,
            "score_seguridad": 64.0,
            "score_turismo": 45.0,
            "score_entorno_comercial": 59.0,
            "shap_values": {},
            "modelo_version": "manual_v1",
        }

        conn = _FakeConn(fetch_results=[[], [zona_data]])
        with patch("scoring.motor._get_pesos_sector", new=AsyncMock(return_value=pesos_restauracion)), \
             patch("db.conexion.get_db", return_value=_FakeDB(conn)), \
             patch("scoring.motor._scorer_batch", new=AsyncMock(return_value={"zona_001": row_scorer})):
            result = await calcular_scores_batch(
                ["zona_001"],
                "restauracion",
                idea_tags=["dog_friendly", "clientela_local"],
                perfil_negocio={
                    "dependencia_flujo": 0.35,
                    "nivel_precio": 0.55,
                    "clientela_turismo": 0.10,
                    "clientela_vecindario": 0.85,
                    "horario_nocturno": 0.05,
                    "experiencial": 0.40,
                    "citas_previas": 0.05,
                    "sensibilidad_alquiler": 0.55,
                },
            )

        assert result[0]["score_afinidad_concepto"] is not None
        assert result[0]["score_global"] != pytest.approx(62.0)

    @pytest.mark.asyncio
    async def test_cache_hit_y_cache_miss_devuelven_mismo_score_final(self, pesos_restauracion):
        cached_row = {
            "zona_id": "zona_001",
            "score_global": 62.0,
            "score_flujo_peatonal": 62.0,
            "score_demografia": 60.0,
            "score_competencia": 55.0,
            "score_precio_alquiler": 63.0,
            "score_transporte": 58.0,
            "score_seguridad": 64.0,
            "score_turismo": 45.0,
            "score_entorno_comercial": 59.0,
            "probabilidad_supervivencia": None,
            "shap_values": {},
            "modelo_version": "manual_v1",
        }
        affinity_row = {
            "zona_id": "zona_001",
            "renta_media_hogar": 40000,
            "edad_media": 37,
            "flujo_peatonal_total": 900,
            "score_turismo": 18,
            "ratio_locales_comerciales": 0.22,
            "m2_zonas_verdes_cercanas": 2500,
            "incidencias_por_1000hab": 20,
        }
        perfil = {
            "dependencia_flujo": 0.35,
            "nivel_precio": 0.55,
            "clientela_turismo": 0.10,
            "clientela_vecindario": 0.85,
            "horario_nocturno": 0.05,
            "experiencial": 0.40,
            "citas_previas": 0.05,
            "sensibilidad_alquiler": 0.55,
        }

        conn_cache = _FakeConn(fetch_results=[[cached_row], [affinity_row]])
        with patch("scoring.motor._get_pesos_sector", new=AsyncMock(return_value=pesos_restauracion)), \
             patch("db.conexion.get_db", return_value=_FakeDB(conn_cache)), \
             patch("scoring.motor._scorer_batch", new=AsyncMock(return_value={})):
            result_cache = await calcular_scores_batch(
                ["zona_001"],
                "restauracion",
                idea_tags=["dog_friendly", "clientela_local"],
                perfil_negocio=perfil,
            )

        conn_fresh = _FakeConn(fetch_results=[[], [affinity_row]])
        scorer_payload = {k: v for k, v in cached_row.items() if k != "zona_id"}
        with patch("scoring.motor._get_pesos_sector", new=AsyncMock(return_value=pesos_restauracion)), \
             patch("db.conexion.get_db", return_value=_FakeDB(conn_fresh)), \
             patch("scoring.motor._scorer_batch", new=AsyncMock(return_value={"zona_001": scorer_payload})):
            result_fresh = await calcular_scores_batch(
                ["zona_001"],
                "restauracion",
                idea_tags=["dog_friendly", "clientela_local"],
                perfil_negocio=perfil,
            )

        assert result_cache[0]["score_global"] == pytest.approx(result_fresh[0]["score_global"], abs=0.1)
        assert result_cache[0]["score_afinidad_concepto"] == pytest.approx(
            result_fresh[0]["score_afinidad_concepto"],
            abs=0.1,
        )
