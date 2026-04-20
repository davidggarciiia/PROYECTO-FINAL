"""
tests/test_pesos_modulados.py — Fase 2: modulación declarativa de pesos.

Qué testeamos:
  1. Cada regla de REGLAS_MODULACION matchea y aplica correctamente.
  2. Los pesos modulados suman ~1.0 (re-normalización).
  3. Sin perfil_refinado: comportamiento idempotente (solo renormalización).
  4. Múltiples reglas en cascada.
  5. Predicados `contains:`, `>=N`, equals.
"""
from __future__ import annotations

import pytest

from scoring.motor import (
    REGLAS_MODULACION,
    _normalizar_pesos,
    _predicado_match,
    aplicar_reglas_modulacion,
)


# Pesos del sector de referencia: los defaults del sistema.
PESOS_BASE = {
    "peso_flujo":       0.25,
    "peso_demo":        0.25,
    "peso_competencia": 0.15,
    "peso_precio":      0.00,
    "peso_transporte":  0.15,
    "peso_seguridad":   0.05,
    "peso_turismo":     0.05,
    "peso_dinamismo":   0.10,
}


def _suma(pesos: dict) -> float:
    return round(sum(v for k, v in pesos.items() if k.startswith("peso_")), 4)


# ═══════════════════════════════════════════════════════════════════════════
# Idempotencia
# ═══════════════════════════════════════════════════════════════════════════

class TestIdempotencia:
    def test_sin_perfil_refinado_renormaliza_a_1(self):
        out = aplicar_reglas_modulacion(PESOS_BASE, None)
        assert _suma(out) == pytest.approx(1.0, abs=0.001)

    def test_perfil_vacio_no_cambia_pesos(self):
        out = aplicar_reglas_modulacion(PESOS_BASE, {})
        assert _suma(out) == pytest.approx(1.0, abs=0.001)

    def test_perfil_que_no_matchea_ninguna_regla_es_idempotente(self):
        perfil = {
            "publico_objetivo": {"nivel_socioeconomico": "medio"},
            "operacion": {"ticket_tier_p1_p5": 2, "horarios_apertura": ["mañana"]},
            "ubicacion_ideal": {"flujo_tipo": "cualquiera"},
        }
        out = aplicar_reglas_modulacion(PESOS_BASE, perfil)
        # Solo renormalización: ratios relativos deben preservarse.
        base_norm = _normalizar_pesos(PESOS_BASE)
        for k, v in base_norm.items():
            assert out[k] == pytest.approx(v, abs=0.01)


# ═══════════════════════════════════════════════════════════════════════════
# Reglas individuales
# ═══════════════════════════════════════════════════════════════════════════

class TestReglasIndividuales:
    def test_flujo_premium_residencial_sube_demografia_baja_transporte(self):
        perfil = {"ubicacion_ideal": {"flujo_tipo": "premium residencial"}}
        base_norm = _normalizar_pesos(PESOS_BASE)
        out = aplicar_reglas_modulacion(PESOS_BASE, perfil)
        assert _suma(out) == pytest.approx(1.0, abs=0.001)
        # demografía sube en términos relativos, transporte baja
        assert out["peso_demo"] > base_norm["peso_demo"]
        assert out["peso_transporte"] < base_norm["peso_transporte"]

    def test_horarios_apertura_noche_sube_seguridad_y_turismo(self):
        perfil = {"operacion": {"horarios_apertura": ["tarde", "noche"]}}
        base_norm = _normalizar_pesos(PESOS_BASE)
        out = aplicar_reglas_modulacion(PESOS_BASE, perfil)
        assert out["peso_seguridad"] > base_norm["peso_seguridad"]
        assert out["peso_turismo"] > base_norm["peso_turismo"]

    def test_ticket_tier_alto_baja_peso_precio(self):
        perfil = {"operacion": {"ticket_tier_p1_p5": 5}}
        pesos_con_precio = {**PESOS_BASE, "peso_precio": 0.10, "peso_flujo": 0.15}
        base_norm = _normalizar_pesos(pesos_con_precio)
        out = aplicar_reglas_modulacion(pesos_con_precio, perfil)
        # peso_precio debe bajar (multiplicado por 0.5 antes de renormalizar).
        assert out["peso_precio"] < base_norm["peso_precio"]

    def test_nivel_socioeconomico_alto_sube_demografia(self):
        perfil = {"publico_objetivo": {"nivel_socioeconomico": "alto"}}
        base_norm = _normalizar_pesos(PESOS_BASE)
        out = aplicar_reglas_modulacion(PESOS_BASE, perfil)
        assert out["peso_demo"] > base_norm["peso_demo"]

    def test_estilo_vida_contiene_turista_sube_turismo_y_dinamismo(self):
        perfil = {"publico_objetivo": {"estilo_vida": ["turista", "viajero"]}}
        base_norm = _normalizar_pesos(PESOS_BASE)
        out = aplicar_reglas_modulacion(PESOS_BASE, perfil)
        assert out["peso_turismo"] > base_norm["peso_turismo"]
        assert out["peso_dinamismo"] > base_norm["peso_dinamismo"]


# ═══════════════════════════════════════════════════════════════════════════
# Normalización
# ═══════════════════════════════════════════════════════════════════════════

class TestNormalizacion:
    def test_suma_siempre_cercana_a_1(self):
        """Para cualquier regla, la suma final ≈ 1.0."""
        perfiles = [
            {"ubicacion_ideal": {"flujo_tipo": "premium residencial"}},
            {"operacion": {"horarios_apertura": ["noche"]}},
            {"operacion": {"ticket_tier_p1_p5": 4}},
            {"publico_objetivo": {"nivel_socioeconomico": "alto"}},
            {"publico_objetivo": {"estilo_vida": ["turista"]}},
        ]
        for perfil in perfiles:
            out = aplicar_reglas_modulacion(PESOS_BASE, perfil)
            assert _suma(out) == pytest.approx(1.0, abs=0.001), f"falla con {perfil}"

    def test_cascada_multiples_reglas(self):
        """Un perfil que dispara 2+ reglas produce pesos coherentes y suma 1.0."""
        perfil = {
            "publico_objetivo": {
                "nivel_socioeconomico": "alto",
                "estilo_vida": ["turista"],
            },
            "operacion": {
                "ticket_tier_p1_p5": 5,
                "horarios_apertura": ["noche"],
            },
            "ubicacion_ideal": {"flujo_tipo": "premium residencial"},
        }
        out = aplicar_reglas_modulacion(PESOS_BASE, perfil)
        assert _suma(out) == pytest.approx(1.0, abs=0.001)
        # demografía debe quedar amplificada por dos reglas
        base_norm = _normalizar_pesos(PESOS_BASE)
        assert out["peso_demo"] > base_norm["peso_demo"]


# ═══════════════════════════════════════════════════════════════════════════
# Predicados
# ═══════════════════════════════════════════════════════════════════════════

class TestPredicados:
    def test_equals_case_insensitive(self):
        assert _predicado_match("Premium Residencial", "premium residencial")
        assert not _predicado_match("turistico", "premium residencial")

    def test_contains_en_lista(self):
        assert _predicado_match(["mañana", "noche"], "contains:noche")
        assert _predicado_match(["turista", "foodie"], "contains:turista")
        assert not _predicado_match(["mañana", "tarde"], "contains:noche")

    def test_contains_en_string(self):
        assert _predicado_match("horario nocturno", "contains:noctur")
        assert not _predicado_match("diurno", "contains:nocturno")

    def test_comparaciones_numericas(self):
        assert _predicado_match(5, ">=4")
        assert _predicado_match(4, ">=4")
        assert not _predicado_match(3, ">=4")
        assert _predicado_match(10, ">5")
        assert not _predicado_match(5, ">5")
        assert _predicado_match(0.5, "<=1")

    def test_none_nunca_matchea(self):
        assert not _predicado_match(None, "foo")
        assert not _predicado_match(None, "contains:x")
        assert not _predicado_match(None, ">=4")


# ═══════════════════════════════════════════════════════════════════════════
# Mapeo exhaustivo de reglas declaradas
# ═══════════════════════════════════════════════════════════════════════════

class TestDeclaracionReglas:
    def test_todas_las_reglas_tienen_multiplicadores_validos(self):
        """Cada regla referencia solo claves peso_* existentes."""
        claves_validas = set(PESOS_BASE.keys())
        for (_path, _pred), mult in REGLAS_MODULACION:
            for clave in mult:
                assert clave in claves_validas, f"{clave} no es un peso válido"
                assert mult[clave] > 0, "multiplicadores deben ser positivos"

    def test_cada_regla_produce_cambio_cuando_matchea(self):
        """Para cada regla declarada hay al menos un input que la dispara."""
        inputs_por_path = {
            "ubicacion_ideal.flujo_tipo": {"ubicacion_ideal": {"flujo_tipo": "premium residencial"}},
            "operacion.horarios_apertura": {"operacion": {"horarios_apertura": ["noche"]}},
            "operacion.ticket_tier_p1_p5": {"operacion": {"ticket_tier_p1_p5": 5}},
            "publico_objetivo.nivel_socioeconomico": {"publico_objetivo": {"nivel_socioeconomico": "alto"}},
            "publico_objetivo.estilo_vida": {"publico_objetivo": {"estilo_vida": ["turista"]}},
        }
        # Usamos una base con peso_precio>0 para que la regla de ticket_tier sea
        # medible (el default de 0 no puede encogerse más).
        base_test = {**PESOS_BASE, "peso_precio": 0.10, "peso_flujo": 0.15}
        base_norm = _normalizar_pesos(base_test)
        for (path, _pred), _mult in REGLAS_MODULACION:
            perfil = inputs_por_path.get(path)
            assert perfil, f"falta input de prueba para {path}"
            out = aplicar_reglas_modulacion(base_test, perfil)
            # Debe haber al menos un peso que difiera del baseline renormalizado.
            diffs = any(abs(out[k] - base_norm[k]) > 1e-3 for k in base_norm)
            assert diffs, f"la regla {path} no produjo cambio medible"
