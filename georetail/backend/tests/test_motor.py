"""
tests/test_motor.py — Tests para scoring/motor.py (funciones puras).

Cubre la función _format_scores_for_api y calcular_scores_batch
(con mock del scorer para no necesitar DB).

Cubre:
  - _format_scores_for_api: estructura del output para la API
  - Manejo de shap_values como dict / JSON string / inválido
  - Resolución de probabilidad_supervivencia con/sin sufijo _3a
  - calcular_scores_batch: lista vacía, fallback en error, output correcto
"""
import json
import pytest
from unittest.mock import AsyncMock, patch

from scoring.motor import _format_scores_for_api, calcular_scores_batch


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


# ─── Fixture base ──────────────────────────────────────────────────────────────

@pytest.fixture
def raw_completo():
    return {
        "zona_id":                    "zona_001",
        "score_global":               72.5,
        "score_flujo_peatonal":       80.0,
        "score_demografia":           65.0,
        "score_competencia":          70.0,
        "score_precio_alquiler":      55.0,
        "score_transporte":           90.0,
        "score_seguridad":            75.0,
        "score_turismo":              60.0,
        "score_entorno_comercial":    68.0,
        "probabilidad_supervivencia_3a": 0.71,
        "shap_values": {
            "flujo_peatonal_total": 0.15,
            "renta_media_hogar":    0.08,
            "precio_m2_alquiler":  -0.05,
        },
        "modelo_version": "xgboost_v1",
    }


# ─── Tests de _format_scores_for_api ─────────────────────────────────────────

class TestFormatScoresForApi:
    def test_estructura_basica_correcta(self, raw_completo):
        out = _format_scores_for_api(raw_completo)
        # Contract mínimo de claves que debe devolver siempre. Se permiten
        # campos adicionales (extensiones como pesos_modulados) sin romper
        # el contrato base con el frontend.
        claves_minimas = {
            "score_global",
            "probabilidad_supervivencia_3a",
            "scores_dimension",
            "explicaciones_shap",
            "impacto_modelo_por_dimension",
            "shap_values",
            "modelo_version",
        }
        assert claves_minimas.issubset(out.keys())

    def test_score_global_se_preserva(self, raw_completo):
        out = _format_scores_for_api(raw_completo)
        assert out["score_global"] == pytest.approx(72.5)

    def test_scores_dimension_contiene_8_dimensiones(self, raw_completo):
        out = _format_scores_for_api(raw_completo)
        dims = out["scores_dimension"]
        assert set(dims.keys()) == {
            "flujo_peatonal", "demografia", "competencia", "precio_alquiler",
            "transporte", "seguridad", "turismo", "entorno_comercial",
        }

    def test_scores_dimension_valores_correctos(self, raw_completo):
        out = _format_scores_for_api(raw_completo)
        dims = out["scores_dimension"]
        assert dims["flujo_peatonal"]  == pytest.approx(80.0)
        assert dims["transporte"]      == pytest.approx(90.0)

    def test_probabilidad_con_sufijo_3a(self, raw_completo):
        out = _format_scores_for_api(raw_completo)
        assert out["probabilidad_supervivencia_3a"] == pytest.approx(0.71, abs=1e-3)

    def test_probabilidad_sin_sufijo_3a(self):
        raw = {"score_global": 60.0, "probabilidad_supervivencia": 0.55}
        out = _format_scores_for_api(raw)
        assert out["probabilidad_supervivencia_3a"] == pytest.approx(0.55, abs=1e-3)

    def test_probabilidad_none_cuando_no_hay_dato(self):
        raw = {"score_global": 60.0}
        out = _format_scores_for_api(raw)
        assert out["probabilidad_supervivencia_3a"] is None

    def test_shap_values_como_dict_genera_explicaciones(self, raw_completo):
        out = _format_scores_for_api(raw_completo)
        assert len(out["explicaciones_shap"]) > 0

    def test_shap_values_como_json_string_se_parsea(self, raw_completo):
        raw = {**raw_completo, "shap_values": json.dumps(raw_completo["shap_values"])}
        out = _format_scores_for_api(raw)
        assert len(out["explicaciones_shap"]) > 0

    def test_shap_values_json_invalido_no_crashea(self, raw_completo):
        raw = {**raw_completo, "shap_values": "no_es_json_valido{{{{"}
        out = _format_scores_for_api(raw)
        assert out["explicaciones_shap"] == []

    def test_shap_values_none_da_explicaciones_vacias(self, raw_completo):
        raw = {**raw_completo, "shap_values": None}
        out = _format_scores_for_api(raw)
        assert out["explicaciones_shap"] == []

    def test_explicaciones_shap_ordenadas_por_valor_absoluto(self, raw_completo):
        """Las explicaciones deben estar ordenadas de mayor a menor valor absoluto."""
        raw = {
            **raw_completo,
            "shap_values": {
                "feature_a": 0.01,
                "feature_b": 0.20,
                "feature_c": -0.15,
            },
        }
        out = _format_scores_for_api(raw)
        valores_abs = [abs(e["valor"]) for e in out["explicaciones_shap"]]
        assert valores_abs == sorted(valores_abs, reverse=True)

    def test_explicaciones_shap_max_10(self):
        """No más de 10 explicaciones aunque haya más features."""
        shap_grande = {f"feature_{i}": float(i) * 0.01 for i in range(25)}
        raw = {"score_global": 70.0, "shap_values": shap_grande}
        out = _format_scores_for_api(raw)
        assert len(out["explicaciones_shap"]) <= 10

    def test_cada_explicacion_tiene_feature_y_valor(self, raw_completo):
        out = _format_scores_for_api(raw_completo)
        for expl in out["explicaciones_shap"]:
            assert "feature" in expl
            assert "valor" in expl
            assert isinstance(expl["feature"], str)
            assert isinstance(expl["valor"], float)

    def test_score_global_default_50_cuando_falta(self):
        out = _format_scores_for_api({})
        assert out["score_global"] == pytest.approx(50.0)

    def test_probabilidad_redondeada_a_3_decimales(self, raw_completo):
        raw = {**raw_completo, "probabilidad_supervivencia_3a": 0.712345}
        out = _format_scores_for_api(raw)
        # Debe ser round(0.712345, 3) = 0.712
        assert out["probabilidad_supervivencia_3a"] == pytest.approx(0.712, abs=1e-4)


# ─── Tests de calcular_scores_batch ──────────────────────────────────────────

class TestCalcularScoresBatch:
    async def test_lista_vacia_devuelve_lista_vacia(self):
        result = await calcular_scores_batch([], "restauracion")
        assert result == []

    async def test_resultado_contiene_zona_id(self):
        mock_scores = {
            "zona_001": {
                "score_global": 65.0,
                "probabilidad_supervivencia": 0.60,
                "shap_values": {},
                "modelo_version": "test",
            }
        }
        conn = _FakeConn(fetch_results=[[]])
        with patch("scoring.motor._get_pesos_sector", new=AsyncMock(return_value={})), \
             patch("db.conexion.get_db", return_value=_FakeDB(conn)), \
             patch("scoring.motor._scorer_batch", new=AsyncMock(return_value=mock_scores)):
            result = await calcular_scores_batch(["zona_001"], "restauracion")
        assert len(result) == 1
        assert result[0]["zona_id"] == "zona_001"
        assert isinstance(result[0]["score_global"], float)
        assert result[0]["score_global"] > 0

    async def test_multiples_zonas(self):
        mock_scores = {
            f"zona_{i:03d}": {"score_global": float(i * 10), "shap_values": {}}
            for i in range(5)
        }
        conn = _FakeConn(fetch_results=[[]])
        with patch("scoring.motor._get_pesos_sector", new=AsyncMock(return_value={})), \
             patch("db.conexion.get_db", return_value=_FakeDB(conn)), \
             patch("scoring.motor._scorer_batch", new=AsyncMock(return_value=mock_scores)):
            result = await calcular_scores_batch(
                [f"zona_{i:03d}" for i in range(5)], "restauracion"
            )
        assert len(result) == 5

    async def test_fallback_en_excepcion_devuelve_scores_neutros(self):
        conn = _FakeConn(fetch_results=[[]])
        with patch("scoring.motor._get_pesos_sector", new=AsyncMock(return_value={})), \
             patch("db.conexion.get_db", return_value=_FakeDB(conn)), \
             patch("scoring.motor._scorer_batch",
                   new=AsyncMock(side_effect=Exception("DB error"))):
            result = await calcular_scores_batch(["zona_001", "zona_002"], "restauracion")
        assert len(result) == 2
        for r in result:
            assert r["score_global"] == pytest.approx(50.0)
            assert r["modelo_version"] == "fallback_error"

    async def test_fallback_contiene_todas_las_claves(self):
        conn = _FakeConn(fetch_results=[[]])
        with patch("scoring.motor._get_pesos_sector", new=AsyncMock(return_value={})), \
             patch("db.conexion.get_db", return_value=_FakeDB(conn)), \
             patch("scoring.motor._scorer_batch",
                   new=AsyncMock(side_effect=Exception("error"))):
            result = await calcular_scores_batch(["zona_x"], "restauracion")
        claves_esperadas = {
            "zona_id", "score_global", "score_flujo_peatonal", "score_demografia",
            "score_competencia", "score_precio_alquiler", "score_transporte",
            "score_seguridad", "score_turismo", "score_entorno_comercial",
            "probabilidad_supervivencia", "shap_values", "modelo_version",
        }
        assert claves_esperadas.issubset(result[0].keys())

    async def test_m2_opcional_no_afecta_resultado(self):
        mock_scores = {"zona_001": {"score_global": 70.0, "shap_values": {}}}
        conn = _FakeConn(fetch_results=[[], []])
        with patch("scoring.motor._get_pesos_sector", new=AsyncMock(return_value={})), \
             patch("db.conexion.get_db", return_value=_FakeDB(conn)), \
             patch("scoring.motor._scorer_batch", new=AsyncMock(return_value=mock_scores)):
            result_sin_m2 = await calcular_scores_batch(["zona_001"], "restauracion")
            result_con_m2 = await calcular_scores_batch(["zona_001"], "restauracion", m2=80.0)
        assert result_sin_m2[0]["score_global"] == result_con_m2[0]["score_global"]
