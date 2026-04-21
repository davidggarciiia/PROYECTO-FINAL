"""
tests/test_validador_llm.py — Tests para agente/validador.py

Qué testeamos aquí:
  1. Parsing del JSON que devuelve el LLM: que se extraen correctamente
     idea_tags, perfil_numerico, sector, estado, etc.
  2. Fallback a perfil_desde_tags cuando el LLM no devuelve perfil_numerico.
  3. Traducción de motivo_rechazo al español.
  4. Manejo de JSONDecodeError → valores por defecto seguros.
  5. Mapping de estados del LLM a los campos del dict de retorno.

Estos tests mockean completamente el LLM (no hacen llamadas reales a DeepSeek/OpenAI).
Así podemos validar la lógica de parsing sin coste ni latencia.
"""
from __future__ import annotations

import json
import pytest
from unittest.mock import AsyncMock, patch, MagicMock

from agente.validador import validar_negocio


# ════════════════════════════════════════════════════════════════════════════
# Helpers
# ════════════════════════════════════════════════════════════════════════════

def _respuesta_llm_valida(
    estado: str = "ok",
    es_retail: bool = True,
    sector: str = "restauracion",
    idea_tags: list | None = None,
    perfil_numerico: dict | None = None,
    info_suficiente: bool = True,
    motivo_rechazo: str | None = None,
    variables_extraidas: dict | None = None,
    preguntas_pendientes: list | None = None,
) -> str:
    """Genera un JSON como el que devolvería el LLM."""
    payload = {
        "estado": estado,
        "es_retail": es_retail,
        "sector": sector,
        "idea_tags": idea_tags or ["specialty_coffee", "clientela_local"],
        "info_suficiente": info_suficiente,
        "variables_extraidas": variables_extraidas or {"m2": 60, "presupuesto": 2500},
        "preguntas_pendientes": preguntas_pendientes or [],
    }
    if perfil_numerico is not None:
        payload["perfil_numerico"] = perfil_numerico
    if motivo_rechazo:
        payload["motivo_rechazo"] = motivo_rechazo
    return json.dumps(payload)


def _mock_completar(respuesta_json: str):
    """Parchea routers.llm_router.completar para devolver respuesta_json."""
    return patch(
        "agente.validador.completar",
        new_callable=AsyncMock,
        return_value=respuesta_json,
    )


def _mock_traducir(traduccion: str = "Motivo en español"):
    """Parchea agente.validador.traducir."""
    return patch(
        "agente.validador.traducir",
        new_callable=AsyncMock,
        return_value=traduccion,
    )


# ════════════════════════════════════════════════════════════════════════════
# 1. Parsing correcto del JSON del LLM
# ════════════════════════════════════════════════════════════════════════════

class TestParsingJSON:
    @pytest.mark.asyncio
    async def test_campos_base_presentes(self):
        """La respuesta siempre tiene los campos de contrato."""
        respuesta = _respuesta_llm_valida()
        with _mock_completar(respuesta), _mock_traducir():
            result = await validar_negocio("Quiero abrir un café de especialidad", "sess_1")

        assert "es_retail" in result
        assert "inviable_legal" in result
        assert "motivo_legal" in result
        assert "motivo" in result
        assert "informacion_suficiente" in result
        assert "sector_detectado" in result
        assert "idea_tags" in result
        assert "perfil_negocio" in result
        assert "variables_conocidas" in result
        assert "preguntas_necesarias" in result

    @pytest.mark.asyncio
    async def test_es_retail_true(self):
        respuesta = _respuesta_llm_valida(es_retail=True, estado="ok")
        with _mock_completar(respuesta), _mock_traducir():
            result = await validar_negocio("Cafetería con perros en Gràcia", "sess_2")
        assert result["es_retail"] is True

    @pytest.mark.asyncio
    async def test_sector_detectado_correcto(self):
        respuesta = _respuesta_llm_valida(sector="restauracion")
        with _mock_completar(respuesta), _mock_traducir():
            result = await validar_negocio("Cafetería", "sess_3")
        assert result["sector_detectado"] == "restauracion"

    @pytest.mark.asyncio
    async def test_idea_tags_se_extraen(self):
        tags = ["specialty_coffee", "dog_friendly", "clientela_local"]
        respuesta = _respuesta_llm_valida(idea_tags=tags)
        with _mock_completar(respuesta), _mock_traducir():
            result = await validar_negocio("Café con perros", "sess_4")
        assert result["idea_tags"] == tags

    @pytest.mark.asyncio
    async def test_variables_conocidas_se_extraen(self):
        vars_ = {"m2": 80, "presupuesto": 3000, "barrio": "Gràcia"}
        respuesta = _respuesta_llm_valida(variables_extraidas=vars_)
        with _mock_completar(respuesta), _mock_traducir():
            result = await validar_negocio("...", "sess_5")
        assert result["variables_conocidas"]["m2"] == 80
        assert result["variables_conocidas"]["barrio"] == "Gràcia"

    @pytest.mark.asyncio
    async def test_info_suficiente_true(self):
        respuesta = _respuesta_llm_valida(info_suficiente=True)
        with _mock_completar(respuesta), _mock_traducir():
            result = await validar_negocio("...", "sess_6")
        assert result["informacion_suficiente"] is True

    @pytest.mark.asyncio
    async def test_info_suficiente_false(self):
        respuesta = _respuesta_llm_valida(
            info_suficiente=False,
            preguntas_pendientes=["¿Cuánto puedes pagar de alquiler?"]
        )
        with _mock_completar(respuesta), _mock_traducir():
            result = await validar_negocio("...", "sess_7")
        assert result["informacion_suficiente"] is False
        assert len(result["preguntas_necesarias"]) > 0


# ════════════════════════════════════════════════════════════════════════════
# 2. perfil_numerico: extracción y fallback
# ════════════════════════════════════════════════════════════════════════════

class TestPerfilNumerico:
    @pytest.mark.asyncio
    async def test_perfil_numerico_extraido_del_llm(self):
        """Si el LLM devuelve perfil_numerico, se usa directamente."""
        perfil = {
            "dependencia_flujo": 0.35,
            "nivel_precio": 0.70,
            "clientela_turismo": 0.15,
            "clientela_vecindario": 0.75,
            "horario_nocturno": 0.05,
            "experiencial": 0.55,
            "citas_previas": 0.10,
            "sensibilidad_alquiler": 0.45,
        }
        respuesta = _respuesta_llm_valida(perfil_numerico=perfil)
        with _mock_completar(respuesta), _mock_traducir():
            result = await validar_negocio("Café de especialidad", "sess_10")

        assert "perfil_negocio" in result
        p = result["perfil_negocio"]
        assert p["dependencia_flujo"] == pytest.approx(0.35, abs=0.01)
        assert p["nivel_precio"] == pytest.approx(0.70, abs=0.01)
        assert p["clientela_turismo"] == pytest.approx(0.15, abs=0.01)

    @pytest.mark.asyncio
    async def test_perfil_numerico_ausente_usa_fallback_tags(self):
        """Si el LLM no devuelve perfil_numerico, se usa perfil_desde_tags."""
        respuesta = _respuesta_llm_valida(
            idea_tags=["dog_friendly", "clientela_local"],
            perfil_numerico=None,  # LLM no lo devuelve
        )
        with _mock_completar(respuesta), _mock_traducir():
            result = await validar_negocio("Café con perros", "sess_11")

        p = result["perfil_negocio"]
        # dog_friendly + clientela_local → clientela_vecindario alta
        assert p["clientela_vecindario"] > 0.65

    @pytest.mark.asyncio
    async def test_perfil_numerico_invalido_usa_fallback(self):
        """Un perfil con valores malos (strings) no debe crashear."""
        perfil_malo = {
            "dependencia_flujo": "mucho",
            "nivel_precio": None,
        }
        respuesta = _respuesta_llm_valida(perfil_numerico=perfil_malo)
        with _mock_completar(respuesta), _mock_traducir():
            result = await validar_negocio("...", "sess_12")

        p = result["perfil_negocio"]
        assert 0.0 <= p["dependencia_flujo"] <= 1.0
        assert 0.0 <= p["nivel_precio"] <= 1.0

    @pytest.mark.asyncio
    async def test_perfil_numerico_clipeado_a_rango_valido(self):
        """Valores del LLM fuera de [0,1] se clipean."""
        perfil_extremo = {
            "dependencia_flujo": 2.5,   # debe clipear a 1.0
            "nivel_precio": -0.3,       # debe clipear a 0.0
        }
        respuesta = _respuesta_llm_valida(perfil_numerico=perfil_extremo)
        with _mock_completar(respuesta), _mock_traducir():
            result = await validar_negocio("...", "sess_13")

        p = result["perfil_negocio"]
        assert p["dependencia_flujo"] == pytest.approx(1.0)
        assert p["nivel_precio"] == pytest.approx(0.0)

    @pytest.mark.asyncio
    async def test_perfil_tiene_8_dimensiones(self):
        respuesta = _respuesta_llm_valida()
        with _mock_completar(respuesta), _mock_traducir():
            result = await validar_negocio("...", "sess_14")
        assert len(result["perfil_negocio"]) == 8

    @pytest.mark.asyncio
    async def test_idea_tags_invalidos_se_filtran_y_deduplican(self):
        respuesta = _respuesta_llm_valida(
            idea_tags=["dog_friendly", "dog_friendly", "tag_invalido", "", None],
            perfil_numerico=None,
        )
        with _mock_completar(respuesta), _mock_traducir():
            result = await validar_negocio("Cafe con perros", "sess_15")

        assert result["idea_tags"] == ["dog_friendly"]

    @pytest.mark.asyncio
    async def test_perfil_numerico_parcial_se_mezcla_con_tags(self):
        respuesta = _respuesta_llm_valida(
            idea_tags=["dog_friendly", "clientela_local"],
            perfil_numerico={
                "nivel_precio": 0.65,
                "experiencial": 0.55,
                "clientela_turismo": None,
            },
        )
        with _mock_completar(respuesta), _mock_traducir():
            result = await validar_negocio("Cafe con perros", "sess_16")

        p = result["perfil_negocio"]
        assert p["nivel_precio"] == pytest.approx(0.65, abs=0.01)
        assert p["experiencial"] == pytest.approx(0.55, abs=0.01)
        assert p["clientela_vecindario"] > 0.65


# ════════════════════════════════════════════════════════════════════════════
# 3. Estados del LLM → campos de retorno
# ════════════════════════════════════════════════════════════════════════════

class TestEstadosLLM:
    @pytest.mark.asyncio
    async def test_estado_error_tipo_negocio(self):
        """Un negocio no retail → es_retail=False, motivo relleno."""
        respuesta = _respuesta_llm_valida(
            estado="error_tipo_negocio",
            es_retail=False,
            motivo_rechazo="This is a digital service, not a physical retail business.",
        )
        with _mock_completar(respuesta), _mock_traducir("Esto es un servicio digital, no retail."):
            result = await validar_negocio("Quiero crear una app móvil", "sess_20")

        assert result["es_retail"] is False
        assert result["motivo"] is not None
        assert result["inviable_legal"] is False

    @pytest.mark.asyncio
    async def test_estado_inviable_legal(self):
        """Un negocio bloqueado legalmente → inviable_legal=True, motivo_legal relleno."""
        respuesta = _respuesta_llm_valida(
            estado="inviable_legal",
            es_retail=True,
            motivo_rechazo="Tobacco clubs require special licensing.",
        )
        with _mock_completar(respuesta), _mock_traducir("Se requiere licencia especial."):
            result = await validar_negocio("Quiero abrir un club de fumadores", "sess_21")

        assert result["inviable_legal"] is True
        assert result["motivo_legal"] is not None
        assert "licencia" in result["motivo_legal"].lower()

    @pytest.mark.asyncio
    async def test_estado_ok_inviable_legal_false(self):
        """Un negocio normal → inviable_legal=False, motivo_legal=None."""
        respuesta = _respuesta_llm_valida(estado="ok", es_retail=True)
        with _mock_completar(respuesta), _mock_traducir():
            result = await validar_negocio("Cafetería en el Eixample", "sess_22")

        assert result["inviable_legal"] is False
        assert result["motivo_legal"] is None

    @pytest.mark.asyncio
    async def test_estado_cuestionario_info_insuficiente(self):
        """Estado cuestionario → informacion_suficiente=False."""
        respuesta = _respuesta_llm_valida(
            estado="cuestionario",
            info_suficiente=False,
            preguntas_pendientes=["¿Cuánto puedes pagar de alquiler al mes?"],
        )
        with _mock_completar(respuesta), _mock_traducir():
            result = await validar_negocio("Quiero abrir algo", "sess_23")

        assert result["informacion_suficiente"] is False
        assert len(result["preguntas_necesarias"]) > 0

    @pytest.mark.asyncio
    async def test_motivo_none_cuando_negocio_valido(self):
        """Un negocio válido no tiene motivo de rechazo."""
        respuesta = _respuesta_llm_valida(estado="ok", es_retail=True)
        with _mock_completar(respuesta), _mock_traducir():
            result = await validar_negocio("Peluquería canina en Sarrià", "sess_24")

        assert result["motivo"] is None


# ════════════════════════════════════════════════════════════════════════════
# 4. Manejo de errores — JSON inválido del LLM
# ════════════════════════════════════════════════════════════════════════════

class TestErroresLLM:
    @pytest.mark.asyncio
    async def test_json_invalido_devuelve_fallback_seguro(self):
        """Si el LLM devuelve texto no parseable, no debe lanzar excepción."""
        respuesta_rota = "Lo siento, no entiendo la pregunta. Por favor reformula."
        with _mock_completar(respuesta_rota), _mock_traducir():
            result = await validar_negocio("...", "sess_30")

        assert "es_retail" in result
        assert "sector_detectado" in result
        # El fallback tiene es_retail=True (conservador — no bloquear al usuario)
        assert result["es_retail"] is True

    @pytest.mark.asyncio
    async def test_json_parcialmente_valido_no_crashea(self):
        """JSON con campos faltantes → rellena con defaults."""
        respuesta_parcial = json.dumps({"es_retail": True})  # sin sector, sin tags
        with _mock_completar(respuesta_parcial), _mock_traducir():
            result = await validar_negocio("...", "sess_31")

        assert isinstance(result["sector_detectado"], str)
        assert isinstance(result["idea_tags"], list)

    @pytest.mark.asyncio
    async def test_llm_devuelve_json_envuelto_en_markdown(self):
        """El LLM a veces envuelve el JSON en ```json ... ``` — debe parsearse igual."""
        payload = _respuesta_llm_valida()
        respuesta_markdown = f"```json\n{payload}\n```"
        with _mock_completar(respuesta_markdown), _mock_traducir():
            result = await validar_negocio("Cafetería", "sess_32")

        assert result["es_retail"] is True
        assert result["sector_detectado"] == "restauracion"


# ════════════════════════════════════════════════════════════════════════════
# 5. Tipos de negocio concretos — validar lógica end-to-end (con mock LLM)
# ════════════════════════════════════════════════════════════════════════════

class TestNegociosConcretos:
    """
    Tests de integración ligera: dado un resultado del LLM simulado,
    ¿el perfil resultante tiene sentido para ese tipo de negocio?
    """

    @pytest.mark.asyncio
    async def test_specialty_coffee_perfil_coherente(self):
        respuesta = _respuesta_llm_valida(
            sector="restauracion",
            idea_tags=["specialty_coffee", "clientela_local", "instagrammable"],
            perfil_numerico={
                "dependencia_flujo": 0.38,
                "nivel_precio": 0.68,
                "clientela_turismo": 0.15,
                "clientela_vecindario": 0.72,
                "horario_nocturno": 0.05,
                "experiencial": 0.55,
                "citas_previas": 0.05,
                "sensibilidad_alquiler": 0.45,
            }
        )
        with _mock_completar(respuesta), _mock_traducir():
            result = await validar_negocio(
                "Quiero abrir un café de especialidad en un barrio de diseño",
                "sess_40"
            )

        p = result["perfil_negocio"]
        assert p["dependencia_flujo"] < 0.55      # no es de paso masivo
        assert p["nivel_precio"] > 0.55            # precio medio-alto
        assert p["clientela_turismo"] < 0.30       # clientela local principalmente
        assert p["experiencial"] > 0.40            # la experiencia es parte del producto

    @pytest.mark.asyncio
    async def test_cocteleria_premium_perfil_coherente(self):
        respuesta = _respuesta_llm_valida(
            sector="restauracion",
            idea_tags=["bar_cocteleria", "horario_nocturno", "alta_renta", "experiencial"],
            perfil_numerico={
                "dependencia_flujo": 0.30,
                "nivel_precio": 0.88,
                "clientela_turismo": 0.40,
                "clientela_vecindario": 0.35,
                "horario_nocturno": 0.92,
                "experiencial": 0.85,
                "citas_previas": 0.20,
                "sensibilidad_alquiler": 0.25,
            }
        )
        with _mock_completar(respuesta), _mock_traducir():
            result = await validar_negocio(
                "Bar de cócteles de autor con carta de 80 referencias",
                "sess_41"
            )

        p = result["perfil_negocio"]
        assert p["horario_nocturno"] > 0.80
        assert p["nivel_precio"] > 0.75
        assert p["experiencial"] > 0.75

    @pytest.mark.asyncio
    async def test_reformer_pilates_perfil_coherente(self):
        respuesta = _respuesta_llm_valida(
            sector="deporte",
            idea_tags=["fitness_boutique", "citas_previas", "alta_renta", "health_wellness"],
            perfil_numerico={
                "dependencia_flujo": 0.12,
                "nivel_precio": 0.75,
                "clientela_turismo": 0.08,
                "clientela_vecindario": 0.78,
                "horario_nocturno": 0.00,
                "experiencial": 0.65,
                "citas_previas": 0.92,
                "sensibilidad_alquiler": 0.30,
            }
        )
        with _mock_completar(respuesta), _mock_traducir():
            result = await validar_negocio(
                "Studio de reformer pilates boutique con máximo 8 clientes por clase",
                "sess_42"
            )

        p = result["perfil_negocio"]
        assert p["dependencia_flujo"] < 0.30       # cliente de destino, no de paso
        assert p["citas_previas"] > 0.80           # siempre con reserva
        assert p["nivel_precio"] > 0.60
