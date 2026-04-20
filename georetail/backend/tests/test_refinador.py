"""
tests/test_refinador.py — Tests para agente/refinador.py

Qué testeamos:
  1. Parsing del JSON del LLM a PerfilRefinado con clipping defensivo.
  2. Defaults seguros cuando el LLM devuelve JSON inválido o falla.
  3. Idempotencia ante None/empty description.
  4. Validación de enums (Literal) y clipping de rangos numéricos.
  5. nuances_detected preserva vocabulario abierto.
"""
from __future__ import annotations

import json
import pytest
from unittest.mock import AsyncMock, patch

from agente.refinador import refinar
from schemas.models import PerfilRefinado


def _respuesta_llm_valida(**overrides) -> str:
    base = {
        "publico_objetivo": {
            "edad_rango": "25-35",
            "nivel_socioeconomico": "medio-alto",
            "estilo_vida": ["millennials", "foodies"],
            "horarios_pico": ["brunch", "tarde"],
        },
        "propuesta_valor": {
            "especializacion": "cafe de especialidad con brunch",
            "diferenciadores": ["v60", "cold brew casero"],
            "calidad_percibida_0_5": 4.2,
        },
        "operacion": {
            "modelo_servicio": "mesas",
            "ticket_tier_p1_p5": 3,
            "escala_operativa": "micro",
            "horarios_apertura": ["mañana", "brunch", "tarde"],
        },
        "ubicacion_ideal": {
            "tipo_calle": "comercial secundaria",
            "densidad_preferida": "alta",
            "flujo_tipo": "barrio joven",
        },
        "nuances_detected": ["instagrammable", "brunch culture", "millennials", "specialty coffee"],
        "signal_preservation_score": 85,
    }
    base.update(overrides)
    return json.dumps(base)


def _mock_completar(respuesta: str):
    return patch(
        "agente.refinador.completar",
        new_callable=AsyncMock,
        return_value=respuesta,
    )


class TestParsingBasico:
    @pytest.mark.asyncio
    async def test_perfil_refinado_parsea_campos_principales(self):
        with _mock_completar(_respuesta_llm_valida()):
            perfil = await refinar(
                descripcion="cafeteria de especialidad con brunch",
                sector_detectado="restauracion",
                tags_previos=["specialty_coffee"],
            )
        assert isinstance(perfil, PerfilRefinado)
        assert perfil.publico_objetivo.edad_rango == "25-35"
        assert perfil.publico_objetivo.nivel_socioeconomico == "medio-alto"
        assert perfil.operacion.ticket_tier_p1_p5 == 3
        assert perfil.signal_preservation_score == 85

    @pytest.mark.asyncio
    async def test_nuances_detected_preserva_vocabulario_abierto(self):
        """nuances_detected es open-vocab: llegan como están."""
        with _mock_completar(_respuesta_llm_valida()):
            perfil = await refinar("x " * 20, "restauracion", [])
        assert "instagrammable" in perfil.nuances_detected
        assert "brunch culture" in perfil.nuances_detected

    @pytest.mark.asyncio
    async def test_listas_deduplican(self):
        resp = _respuesta_llm_valida(
            nuances_detected=["instagrammable", "instagrammable", "", "brunch", None],
        )
        with _mock_completar(resp):
            perfil = await refinar("foo bar baz", "restauracion", [])
        assert perfil.nuances_detected == ["instagrammable", "brunch"]


class TestClippingYValidacion:
    @pytest.mark.asyncio
    async def test_ticket_tier_fuera_rango_se_clipea(self):
        resp = _respuesta_llm_valida(operacion={
            "modelo_servicio": "mesas",
            "ticket_tier_p1_p5": 9,  # fuera de [1,5]
            "escala_operativa": "micro",
            "horarios_apertura": [],
        })
        with _mock_completar(resp):
            perfil = await refinar("x" * 40, "restauracion", [])
        assert perfil.operacion.ticket_tier_p1_p5 == 5

    @pytest.mark.asyncio
    async def test_signal_score_clipea_a_rango(self):
        resp = _respuesta_llm_valida(signal_preservation_score=9999)
        with _mock_completar(resp):
            perfil = await refinar("x" * 40, "restauracion", [])
        assert perfil.signal_preservation_score == 100

        resp2 = _respuesta_llm_valida(signal_preservation_score=-20)
        with _mock_completar(resp2):
            perfil = await refinar("x" * 40, "restauracion", [])
        assert perfil.signal_preservation_score == 0

    @pytest.mark.asyncio
    async def test_literal_invalido_se_convierte_en_none(self):
        """Si el LLM devuelve un valor fuera del Literal, se convierte a None."""
        resp = _respuesta_llm_valida(publico_objetivo={
            "edad_rango": "25-35",
            "nivel_socioeconomico": "aristocrata",  # inválido
            "estilo_vida": [],
            "horarios_pico": [],
        })
        with _mock_completar(resp):
            perfil = await refinar("x" * 40, "restauracion", [])
        assert perfil.publico_objetivo.nivel_socioeconomico is None

    @pytest.mark.asyncio
    async def test_calidad_percibida_se_clipea(self):
        resp = _respuesta_llm_valida(propuesta_valor={
            "especializacion": "foo",
            "diferenciadores": [],
            "calidad_percibida_0_5": 12.0,
        })
        with _mock_completar(resp):
            perfil = await refinar("x" * 40, "restauracion", [])
        assert perfil.propuesta_valor.calidad_percibida_0_5 == 5.0


class TestDefaultsYFallback:
    @pytest.mark.asyncio
    async def test_json_invalido_devuelve_perfil_vacio(self):
        with _mock_completar("esto no es JSON"):
            perfil = await refinar("cafeteria brunch", "restauracion", [])
        # Defaults: dataclasses vacíos, score=100 (no hay evidencia de pérdida).
        assert perfil == PerfilRefinado()

    @pytest.mark.asyncio
    async def test_llm_excepcion_devuelve_perfil_vacio(self):
        with patch(
            "agente.refinador.completar",
            new_callable=AsyncMock,
            side_effect=RuntimeError("LLM down"),
        ):
            perfil = await refinar("cafeteria brunch", "restauracion", [])
        assert perfil == PerfilRefinado()
        assert perfil.nuances_detected == []

    @pytest.mark.asyncio
    async def test_descripcion_vacia_devuelve_perfil_vacio_sin_llamar_llm(self):
        """None-idempotence: sin descripción, no se llama al LLM."""
        mock = AsyncMock(return_value=_respuesta_llm_valida())
        with patch("agente.refinador.completar", mock):
            perfil = await refinar("   ", "restauracion", [])
        assert perfil == PerfilRefinado()
        mock.assert_not_called()

    @pytest.mark.asyncio
    async def test_json_parcial_rellena_defaults(self):
        """JSON con campos ausentes → defaults para los que faltan."""
        resp = json.dumps({"nuances_detected": ["foo"], "signal_preservation_score": 55})
        with _mock_completar(resp):
            perfil = await refinar("x" * 40, "restauracion", [])
        assert perfil.nuances_detected == ["foo"]
        assert perfil.signal_preservation_score == 55
        assert perfil.publico_objetivo.edad_rango is None
        assert perfil.operacion.horarios_apertura == []
