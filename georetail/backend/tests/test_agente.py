"""
tests/test_agente.py — Tests para los módulos del agente (funciones puras).

Módulos testados:
  - agente/__init__.py  → extraer_json (extracción de JSON de respuestas LLM)
  - agente/analizador.py → _construir_prompt (construcción del prompt de análisis)

No se testean funciones async con LLM (validador, cuestionario, traductor)
ya que requieren mocks de red; esos se cubren en tests de integración.

Cubre extraer_json:
  - Bloque markdown ```json...```
  - Bloque markdown ```...```
  - JSON limpio (objeto)
  - JSON limpio (array)
  - Texto libre con objeto JSON embebido
  - Texto libre con array JSON embebido
  - Texto sin JSON → devuelve el texto original
  - Respuesta vacía
  - JSON multilinea
  - Bloque con texto antes y después

Cubre _construir_prompt:
  - Contiene los campos clave del negocio y la zona
  - Maneja pct_locales_vacios=None sin crashear
  - Maneja zona sin competidores
  - Maneja zona sin alertas
  - Maneja zona sin shap_values
  - Maneja zona sin flujo peatonal
  - El prompt no supera una longitud razonable
"""
import pytest

from agente import extraer_json
from agente.analizador import _construir_prompt


# ─── Tests de extraer_json ────────────────────────────────────────────────────

class TestExtraerJson:
    # --- Bloques markdown ---

    def test_bloque_markdown_json_objeto(self):
        texto = '```json\n{"clave": "valor"}\n```'
        assert extraer_json(texto) == '{"clave": "valor"}'

    def test_bloque_markdown_sin_tipo(self):
        texto = '```\n{"clave": "valor"}\n```'
        assert extraer_json(texto) == '{"clave": "valor"}'

    def test_bloque_markdown_json_array(self):
        texto = '```json\n[1, 2, 3]\n```'
        assert extraer_json(texto) == '[1, 2, 3]'

    def test_bloque_markdown_multilínea(self):
        texto = '```json\n{\n  "a": 1,\n  "b": 2\n}\n```'
        resultado = extraer_json(texto)
        assert '"a"' in resultado
        assert '"b"' in resultado

    def test_bloque_markdown_con_texto_antes(self):
        texto = 'Aquí va el JSON:\n```json\n{"ok": true}\n```'
        assert extraer_json(texto) == '{"ok": true}'

    # --- JSON embebido en texto libre ---

    def test_json_objeto_embebido_en_texto(self):
        texto = 'El sistema responde: {"estado": "ok", "valor": 42} fin.'
        resultado = extraer_json(texto)
        assert '"estado"' in resultado
        assert '"valor"' in resultado

    def test_json_array_embebido_en_texto(self):
        texto = 'Resultado: [{"id": 1}, {"id": 2}] listo.'
        resultado = extraer_json(texto)
        assert '"id"' in resultado

    # --- JSON ya limpio ---

    def test_json_limpio_objeto(self):
        texto = '{"sector": "restauracion", "score": 75}'
        assert extraer_json(texto) == texto

    def test_json_limpio_array(self):
        texto = '[{"id": "a", "categoria": "servicio"}]'
        assert extraer_json(texto) == texto

    # --- Casos especiales ---

    def test_texto_sin_json_devuelve_texto(self):
        texto = "Esto no tiene JSON"
        assert extraer_json(texto) == texto

    def test_texto_vacio_devuelve_cadena_vacia(self):
        assert extraer_json("") == ""

    def test_texto_solo_espacios(self):
        assert extraer_json("   ") == ""

    def test_json_anidado(self):
        texto = '```json\n{"a": {"b": {"c": 1}}}\n```'
        resultado = extraer_json(texto)
        assert '"c"' in resultado

    def test_preserva_caracteres_unicode(self):
        texto = '{"nombre": "café", "barrio": "Gràcia"}'
        assert extraer_json(texto) == texto

    def test_json_con_comillas_escapadas(self):
        texto = '{"mensaje": "dijo \\"hola\\""}'
        assert extraer_json(texto) == texto

    def test_resultado_es_string(self):
        texto = '{"x": 1}'
        resultado = extraer_json(texto)
        assert isinstance(resultado, str)

    def test_no_modifica_json_valido_con_strip(self):
        """Texto con espacios alrededor pero JSON limpio → devuelve el JSON sin espacios."""
        texto = '  \n  {"key": "val"}  \n  '
        resultado = extraer_json(texto)
        # El regex lo debería encontrar
        assert '"key"' in resultado


# ─── Tests de _construir_prompt ───────────────────────────────────────────────

class TestConstruirPrompt:
    @pytest.fixture
    def zona_completa(self):
        return {
            "zona_id":            "zona_001",
            "nombre":             "Carrer de Provença",
            "barrio":             "Dreta de l'Eixample",
            "distrito":           "Eixample",
            "score_global":       72.5,
            "probabilidad_supervivencia": 0.68,
            "renta_media_hogar":  42_000,
            "pct_extranjeros":    0.22,
            "score_turismo":      65,
            "pct_locales_vacios": 0.08,
            "tasa_rotacion_anual": 0.14,
            "num_lineas_transporte": 9,
            "alquiler_mensual":   1_800,
            "m2":                    65,
            "flujo_peatonal_dia": {
                "manana": 250, "tarde": 500, "noche": 180
            },
            "competidores_cercanos": [
                {"nombre": "Bar Ejemplo", "es_competencia_directa": True},
                {"nombre": "Tienda XYZ",  "es_competencia_directa": False},
            ],
            "alertas": [
                {"texto": "Obras en la zona previstas para 2026"}
            ],
            "shap_values": {
                "flujo_peatonal_total":  0.15,
                "renta_media_hogar":     0.08,
                "precio_m2_alquiler":   -0.05,
            },
        }

    @pytest.fixture
    def perfil_completo(self):
        return {
            "sector":          "restauracion",
            "descripcion":     "Bar de tapas moderno con cocina abierta",
            "perfil_cliente":  "jóvenes 25-40 años",
            "precio_objetivo": "medio-alto",
        }

    def test_prompt_contiene_nombre_zona(self, zona_completa, perfil_completo):
        prompt = _construir_prompt(zona_completa, perfil_completo)
        assert "Carrer de Provença" in prompt

    def test_prompt_contiene_barrio_y_distrito(self, zona_completa, perfil_completo):
        prompt = _construir_prompt(zona_completa, perfil_completo)
        assert "Eixample" in prompt

    def test_prompt_contiene_sector_negocio(self, zona_completa, perfil_completo):
        prompt = _construir_prompt(zona_completa, perfil_completo)
        assert "restauracion" in prompt

    def test_prompt_contiene_score_global(self, zona_completa, perfil_completo):
        prompt = _construir_prompt(zona_completa, perfil_completo)
        assert "72.5" in prompt

    def test_prompt_contiene_datos_flujo(self, zona_completa, perfil_completo):
        prompt = _construir_prompt(zona_completa, perfil_completo)
        assert "250" in prompt  # manana
        assert "500" in prompt  # tarde

    def test_prompt_contiene_competidor_directo(self, zona_completa, perfil_completo):
        prompt = _construir_prompt(zona_completa, perfil_completo)
        assert "Bar Ejemplo" in prompt

    def test_prompt_contiene_alerta(self, zona_completa, perfil_completo):
        prompt = _construir_prompt(zona_completa, perfil_completo)
        assert "Obras" in prompt

    def test_prompt_contiene_pct_locales_vacios_formateado(self, zona_completa, perfil_completo):
        prompt = _construir_prompt(zona_completa, perfil_completo)
        assert "8%" in prompt  # 0.08 → "8%"

    def test_pct_locales_vacios_none_no_crashea(self, zona_completa, perfil_completo):
        zona_sin_vacios = {**zona_completa, "pct_locales_vacios": None}
        prompt = _construir_prompt(zona_sin_vacios, perfil_completo)
        assert "N/A" in prompt

    def test_sin_competidores_no_crashea(self, zona_completa, perfil_completo):
        zona_sin_comp = {**zona_completa, "competidores_cercanos": []}
        prompt = _construir_prompt(zona_sin_comp, perfil_completo)
        assert "No direct competition" in prompt

    def test_sin_alertas_muestra_no_alerts(self, zona_completa, perfil_completo):
        zona_sin_alertas = {**zona_completa, "alertas": []}
        prompt = _construir_prompt(zona_sin_alertas, perfil_completo)
        assert "No alerts" in prompt

    def test_sin_shap_values_no_crashea(self, zona_completa, perfil_completo):
        zona_sin_shap = {**zona_completa, "shap_values": None}
        prompt = _construir_prompt(zona_sin_shap, perfil_completo)
        assert prompt  # No está vacío

    def test_shap_values_vacios_no_crashea(self, zona_completa, perfil_completo):
        zona_shap_vacio = {**zona_completa, "shap_values": {}}
        prompt = _construir_prompt(zona_shap_vacio, perfil_completo)
        assert "Not available" in prompt

    def test_sin_flujo_peatonal_dia_no_crashea(self, zona_completa, perfil_completo):
        zona_sin_flujo = {**zona_completa, "flujo_peatonal_dia": {}}
        prompt = _construir_prompt(zona_sin_flujo, perfil_completo)
        assert prompt

    def test_perfil_minimo_no_crashea(self, zona_completa):
        prompt = _construir_prompt(zona_completa, {})
        assert prompt

    def test_zona_minima_no_crashea(self, perfil_completo):
        prompt = _construir_prompt({}, perfil_completo)
        assert prompt

    def test_prompt_es_string(self, zona_completa, perfil_completo):
        prompt = _construir_prompt(zona_completa, perfil_completo)
        assert isinstance(prompt, str)

    def test_prompt_tiene_longitud_razonable(self, zona_completa, perfil_completo):
        """El prompt no debe superar ~3000 chars para mantenerse en límite de tokens."""
        prompt = _construir_prompt(zona_completa, perfil_completo)
        assert len(prompt) < 3_000, f"Prompt demasiado largo: {len(prompt)} chars"

    def test_prompt_no_esta_vacio(self, zona_completa, perfil_completo):
        prompt = _construir_prompt(zona_completa, perfil_completo)
        assert len(prompt.strip()) > 50

    def test_shap_positivos_aparecen_en_prompt(self, zona_completa, perfil_completo):
        prompt = _construir_prompt(zona_completa, perfil_completo)
        # flujo_peatonal_total tiene valor positivo en shap_values
        assert "flujo_peatonal_total" in prompt

    def test_shap_negativos_aparecen_en_prompt(self, zona_completa, perfil_completo):
        prompt = _construir_prompt(zona_completa, perfil_completo)
        # precio_m2_alquiler tiene valor negativo en shap_values
        assert "precio_m2_alquiler" in prompt
