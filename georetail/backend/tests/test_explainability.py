from __future__ import annotations

from scoring.explainability import (
    build_fallback_analysis,
    build_llm_grounding_payload,
    group_shap_by_dimension,
)


def _zona_base() -> dict:
    return {
        "zona_id": "1",
        "nombre": "Plaza Catalunya",
        "barrio": "Dreta de l'Eixample",
        "distrito": "Eixample",
        "flujo_peatonal_total": 1800.0,
        "flujo_popular_times_score": 82.0,
        "vcity_flujo_peatonal": 22000.0,
        "weekend_lift": 1.18,
        "sunday_lift": 1.08,
        "weekday_midday_share": 0.18,
        "weekend_evening_share": 0.22,
        "late_night_share": 0.09,
        "holiday_proxy_score": 74.0,
        "temporal_confianza": 0.8,
        "seasonality_summer_lift": 1.06,
        "seasonality_christmas_lift": 1.08,
        "seasonality_volatility": 0.16,
        "renta_media_hogar": 43000.0,
        "edad_media": 39.5,
        "pct_extranjeros": 0.23,
        "densidad_hab_km2": 21000.0,
        "pct_poblacio_25_44": 0.33,
        "nivel_estudios_alto_pct": 0.46,
        "gini": 34.0,
        "renta_media_uc": 18500.0,
        "pct_locales_vacios": 0.08,
        "tasa_rotacion_anual": 0.12,
        "score_turismo": 72.0,
        "booking_hoteles_500m": 6,
        "airbnb_density_500m": 42,
        "num_lineas_transporte": 14,
        "num_paradas_transporte": 8,
        "num_bicing_400m": 4,
        "tiene_carril_bici": True,
        "transporte_festivo_ratio": 0.82,
        "transporte_lineas_cercanas": [
            {"codigo": "H12", "tipo": "bus", "distancia_m": 120},
            {"codigo": "L1", "tipo": "metro", "distancia_m": 210},
        ],
        "competidores_cercanos": [
            {"nombre": "Café A", "es_competencia_directa": True, "es_vulnerable": False},
            {"nombre": "Café B", "es_competencia_directa": True, "es_vulnerable": True},
        ],
        "incidencias_por_1000hab": 28.0,
        "hurtos_por_1000hab": 16.0,
        "robatoris_por_1000hab": 7.0,
        "danys_por_1000hab": 4.0,
        "incidencias_noche_pct": 0.26,
        "comisarias_1km": 2,
        "dist_comisaria_m": 340.0,
        "score_equipamientos": 78.0,
        "m2_zonas_verdes_cercanas": 1800.0,
        "mercados_municipales_1km": 1,
        "licencias_nuevas_1a": 5.0,
        "ratio_locales_comerciales": 0.31,
        "eventos_culturales_500m": 4.0,
    }


def test_group_shap_by_dimension_agrega_por_dimension():
    grouped = group_shap_by_dimension(
        {
            "flujo_peatonal_total": 0.32,
            "seasonality_summer_lift": 0.10,
            "renta_media_hogar": -0.22,
            "num_lineas_transporte": 0.18,
        }
    )
    assert grouped["flujo_peatonal"]["tendencia"] == "empuja_a_favor"
    assert grouped["demografia"]["tendencia"] == "empuja_en_contra"
    assert grouped["transporte"]["tendencia"] == "empuja_a_favor"


def test_build_llm_grounding_payload_incluye_evidencia_dimensiones():
    payload = build_llm_grounding_payload(
        _zona_base(),
        {
            "score_global": 78.0,
            "probabilidad_supervivencia_3a": 0.74,
            "scores_dimension": {
                "flujo_peatonal": 86.0,
                "demografia": 73.0,
                "competencia": 62.0,
                "precio_alquiler": 55.0,
                "transporte": 91.0,
                "seguridad": 69.0,
                "turismo": 76.0,
                "entorno_comercial": 72.0,
            },
            "shap_values": {
                "flujo_peatonal_total": 0.28,
                "renta_media_hogar": 0.12,
                "num_lineas_transporte": 0.18,
            },
        },
        {"idea_tags": ["weekend_peak"]},
    )
    assert "evidencia_dimensiones" in payload
    assert "flujo_peatonal" in payload["evidencia_dimensiones"]
    assert payload["impacto_modelo_por_dimension"]["flujo_peatonal"]["contribucion"] > 0


def test_build_fallback_analysis_devuelve_explicaciones_legibles():
    grounding = build_llm_grounding_payload(
        _zona_base(),
        {
            "score_global": 78.0,
            "probabilidad_supervivencia_3a": 0.74,
            "scores_dimension": {
                "flujo_peatonal": 86.0,
                "demografia": 73.0,
                "competencia": 62.0,
                "precio_alquiler": 55.0,
                "transporte": 91.0,
                "seguridad": 69.0,
                "turismo": 76.0,
                "entorno_comercial": 72.0,
            },
            "shap_values": {
                "flujo_peatonal_total": 0.28,
                "renta_media_hogar": 0.12,
                "num_lineas_transporte": 0.18,
            },
        },
        {"idea_tags": ["weekend_peak"]},
    )
    fallback = build_fallback_analysis(grounding)
    assert fallback["resumen_global"]
    assert "explicaciones_dimensiones" in fallback
    assert "flujo_peatonal" in fallback["explicaciones_dimensiones"]
    assert fallback["explicaciones_dimensiones"]["transporte"]["titular"]
