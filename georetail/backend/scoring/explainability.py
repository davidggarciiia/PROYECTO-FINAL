"""
scoring/explainability.py - Evidencia determinista y grounding para explicaciones.

La idea es separar dos capas:
  1. Evidencia objetiva y estructurada que sale del sistema.
  2. Redacción final del LLM apoyada exclusivamente en esa evidencia.
"""
from __future__ import annotations

import json
from typing import Any, Mapping

from scoring.dimensiones.demografia import calcular_score_demografia
from scoring.dimensiones.entorno import calcular_score_entorno
from scoring.dimensiones.flujo_peatonal import flujo_peatonal_explain
from scoring.dimensiones.seguridad import calcular_score_seguridad

DIMENSION_ORDER: tuple[str, ...] = (
    "flujo_peatonal",
    "demografia",
    "competencia",
    "precio_alquiler",
    "transporte",
    "seguridad",
    "turismo",
    "entorno_comercial",
)

DIMENSION_LABELS: dict[str, str] = {
    "flujo_peatonal": "Flujo peatonal",
    "demografia": "Demografía",
    "competencia": "Competencia",
    "precio_alquiler": "Precio",
    "transporte": "Transporte",
    "seguridad": "Seguridad",
    "turismo": "Turismo",
    "entorno_comercial": "Entorno comercial",
}

# ─── Percentiles y medias de referencia Barcelona 2024-2025 ────────────────────
# Constantes hardcodeadas para evitar roundtrip a BD en cada explicación.
# Se actualizan cuando cambie la estructura de la ciudad; origen: INE/Idescat,
# OpenData BCN, v_variables_zona agregada.

_BCN_REF: dict[str, float] = {
    "renta_media_hogar":         37_000.0,   # euros/año (mediana BCN)
    "edad_media":                43.0,
    "pct_poblacio_25_44_p50":    0.28,
    "nivel_estudios_alto_p75":   0.45,
    "gini_p75":                  38.0,
    "weekend_lift_media":        1.08,
    "vcity_flujo_p75":           32_000.0,   # peatones/día zona comercial
    "vcity_flujo_p50":           14_000.0,
    "incidencias_media":         22.0,       # por 1.000 hab/año
    "hurtos_media":              8.0,
    "robatoris_media":           4.0,
    "incidencias_noche_p75":     0.35,
    "airbnb_density_p75":        45.0,
    "booking_hoteles_p75":       4.0,
    "pct_locales_vacios_p75":    0.18,
    "precio_m2_restauracion":    22.0,       # euros/m²/mes
    "precio_m2_moda":            28.0,
    "precio_m2_generico":        24.0,
}

# Percentiles de número de competidores directos por sector en radio 300 m.
_PERCENTILES_COMPETENCIA: dict[str, dict[str, int]] = {
    "restauracion":  {"q25": 2, "q50": 5, "q75": 9},
    "moda":          {"q25": 1, "q50": 3, "q75": 7},
    "estetica":      {"q25": 2, "q50": 4, "q75": 8},
    "supermercado":  {"q25": 1, "q50": 2, "q75": 4},
    "cafeteria":     {"q25": 2, "q50": 5, "q75": 10},
    "bar":           {"q25": 3, "q50": 6, "q75": 11},
    "_default":      {"q25": 2, "q50": 4, "q75": 8},
}

# Traducción de features internas → texto humano para drivers desde SHAP.
_FEATURE_LABELS: dict[str, str] = {
    "flujo_peatonal_total":         "flujo peatonal total",
    "flujo_popular_times_score":    "picos Google Popular Times",
    "vcity_flujo_peatonal":         "peatones/día VCity",
    "seasonality_summer_lift":      "tirón estival",
    "seasonality_christmas_lift":   "tirón navideño",
    "seasonality_volatility":       "volatilidad estacional",
    "renta_media_hogar":            "renta media del hogar",
    "edad_media":                   "edad media",
    "pct_poblacio_25_44":           "% población 25-44",
    "nivel_estudios_alto_pct":      "% estudios superiores",
    "gini":                         "desigualdad (Gini)",
    "num_competidores_300m":        "competidores directos en 300 m",
    "rating_medio_competidores":    "rating medio competencia",
    "score_saturacion":             "saturación competitiva",
    "num_lineas_transporte":        "líneas de transporte",
    "num_bicing_400m":              "estaciones Bicing",
    "tiene_carril_bici":            "carril bici cercano",
    "incidencias_por_1000hab":      "incidencias/1.000 hab",
    "hurtos_por_1000hab":           "hurtos/1.000 hab",
    "robatoris_por_1000hab":        "robos/1.000 hab",
    "incidencias_noche_pct":        "% incidencias nocturnas",
    "score_turismo":                "score turismo",
    "airbnb_density_500m":          "densidad Airbnb",
    "booking_hoteles_500m":         "hoteles en 500 m",
    "pct_locales_vacios":           "% locales vacíos",
    "tasa_rotacion_anual":          "rotación comercial anual",
    "ratio_apertura_cierre_1a":     "ratio aperturas/cierres",
    "score_equipamientos":          "equipamientos",
    "m2_zonas_verdes_cercanas":     "zonas verdes cercanas",
}


def _pct_vs_ref(valor: float, ref: float) -> str:
    """Devuelve '+11%' / '-30%' contra la referencia, firmado."""
    if not ref:
        return ""
    diff = (valor - ref) / ref * 100.0
    return f"{diff:+.0f}%"


FEATURE_TO_DIMENSION: dict[str, str] = {
    # Flujo y temporalidad
    "flujo_peatonal_total": "flujo_peatonal",
    "flujo_manana_pct": "flujo_peatonal",
    "flujo_tarde_pct": "flujo_peatonal",
    "flujo_noche_pct": "flujo_peatonal",
    "flujo_peatonal_score": "flujo_peatonal",
    "seasonality_summer_lift": "flujo_peatonal",
    "seasonality_christmas_lift": "flujo_peatonal",
    "seasonality_rebajas_lift": "flujo_peatonal",
    "seasonality_volatility": "flujo_peatonal",
    "seasonality_peak_concentration": "flujo_peatonal",
    # Demografía
    "renta_media_hogar": "demografia",
    "edad_media": "demografia",
    "pct_extranjeros": "demografia",
    "densidad_hab_km2": "demografia",
    "pct_poblacio_25_44": "demografia",
    "delta_renta_3a": "demografia",
    "nivel_estudios_alto_pct": "demografia",
    "indice_potencial_consumo": "demografia",
    "gini": "demografia",
    "p80_p20": "demografia",
    "tamano_hogar": "demografia",
    "hogares_con_menores": "demografia",
    "personas_solas": "demografia",
    "renta_media_uc": "demografia",
    "renta_mediana_uc": "demografia",
    # Competencia
    "num_competidores_300m": "competencia",
    "rating_medio_competidores": "competencia",
    "score_saturacion": "competencia",
    "score_aglomeracion": "competencia",
    "pct_vulnerables": "competencia",
    "ratio_complementarios": "competencia",
    # Precio
    "precio_m2_alquiler": "precio_alquiler",
    # Transporte
    "num_lineas_transporte": "transporte",
    "num_paradas_500m": "transporte",
    "score_transporte_calculado": "transporte",
    "num_bicing_400m": "transporte",
    "tiene_carril_bici": "transporte",
    # Seguridad
    "incidencias_por_1000hab": "seguridad",
    "hurtos_por_1000hab": "seguridad",
    "robatoris_por_1000hab": "seguridad",
    "danys_por_1000hab": "seguridad",
    "incidencias_noche_pct": "seguridad",
    "comisarias_1km": "seguridad",
    # Turismo
    "score_turismo": "turismo",
    "airbnb_density_500m": "turismo",
    "airbnb_occupancy_est": "turismo",
    "booking_hoteles_500m": "turismo",
    "dist_playa_m": "turismo",
    # Entorno
    "pct_locales_vacios": "entorno_comercial",
    "tasa_rotacion_anual": "entorno_comercial",
    "nivel_ruido_db": "entorno_comercial",
    "score_equipamientos": "entorno_comercial",
    "m2_zonas_verdes_cercanas": "entorno_comercial",
    "ratio_locales_comerciales": "entorno_comercial",
    "licencias_nuevas_1a": "entorno_comercial",
    "eventos_culturales_500m": "entorno_comercial",
    "mercados_municipales_1km": "entorno_comercial",
    "score_dinamismo_zona": "entorno_comercial",
    "ratio_apertura_cierre_1a": "entorno_comercial",
    "tasa_supervivencia_3a": "entorno_comercial",
    "renta_variacion_3a": "demografia",
    "hhi_sectorial": "entorno_comercial",
}


def normalize_shap_values(shap_raw: Any) -> dict[str, float]:
    if not shap_raw:
        return {}
    if isinstance(shap_raw, str):
        try:
            shap_raw = json.loads(shap_raw)
        except json.JSONDecodeError:
            return {}
    if not isinstance(shap_raw, Mapping):
        return {}
    normalized: dict[str, float] = {}
    for feature, value in shap_raw.items():
        try:
            normalized[str(feature)] = float(value)
        except (TypeError, ValueError):
            continue
    return normalized


def top_model_features(shap_values: Any, *, limit: int = 5) -> dict[str, list[dict[str, float]]]:
    normalized = normalize_shap_values(shap_values)
    ordered = sorted(normalized.items(), key=lambda item: item[1], reverse=True)
    positives = [
        {"feature": feature, "valor": round(value, 3)}
        for feature, value in ordered
        if value > 0
    ][:limit]
    negatives = [
        {"feature": feature, "valor": round(value, 3)}
        for feature, value in sorted(normalized.items(), key=lambda item: item[1])
        if value < 0
    ][:limit]
    return {"positivas": positives, "negativas": negatives}


def group_shap_by_dimension(shap_values: Any) -> dict[str, dict[str, Any]]:
    normalized = normalize_shap_values(shap_values)
    grouped: dict[str, dict[str, Any]] = {}

    for dimension in DIMENSION_ORDER:
        grouped[dimension] = {
            "contribucion": 0.0,
            "tendencia": "neutral",
            "top_features": [],
        }

    per_dimension_features: dict[str, list[tuple[str, float]]] = {dimension: [] for dimension in DIMENSION_ORDER}

    for feature, value in normalized.items():
        dimension = FEATURE_TO_DIMENSION.get(feature)
        if not dimension:
            continue
        grouped[dimension]["contribucion"] += value
        per_dimension_features[dimension].append((feature, value))

    for dimension, payload in grouped.items():
        contrib = round(float(payload["contribucion"]), 3)
        payload["contribucion"] = contrib
        if contrib > 0.15:
            payload["tendencia"] = "empuja_a_favor"
        elif contrib < -0.15:
            payload["tendencia"] = "empuja_en_contra"
        else:
            payload["tendencia"] = "neutral"
        payload["top_features"] = [
            {"feature": feature, "valor": round(value, 3)}
            for feature, value in sorted(
                per_dimension_features[dimension],
                key=lambda item: abs(item[1]),
                reverse=True,
            )[:3]
        ]

    return grouped


def build_llm_grounding_payload(
    zona: Mapping[str, Any],
    scores_data: Mapping[str, Any],
    perfil_negocio: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    scores_dimension = dict(scores_data.get("scores_dimension") or {})
    impacto = group_shap_by_dimension(scores_data.get("shap_values"))
    evidencia = build_dimension_evidence(
        zona=zona,
        scores_dimension=scores_dimension,
        perfil_negocio=perfil_negocio or {},
        impacto_modelo=impacto,
    )
    return {
        "zona": {
            "zona_id": zona.get("zona_id"),
            "nombre": zona.get("nombre"),
            "barrio": zona.get("barrio"),
            "distrito": zona.get("distrito"),
        },
        "score_global": scores_data.get("score_global"),
        "probabilidad_supervivencia_3a": scores_data.get("probabilidad_supervivencia_3a"),
        "scores_dimensiones": scores_dimension,
        "impacto_modelo_por_dimension": impacto,
        "top_features_modelo": top_model_features(scores_data.get("shap_values")),
        "evidencia_dimensiones": evidencia,
        "perfil_negocio": dict(perfil_negocio or {}),
    }


def build_dimension_evidence(
    *,
    zona: Mapping[str, Any],
    scores_dimension: Mapping[str, Any],
    perfil_negocio: Mapping[str, Any],
    impacto_modelo: Mapping[str, Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    return {
        "flujo_peatonal": _build_flow_evidence(zona, scores_dimension.get("flujo_peatonal"), impacto_modelo.get("flujo_peatonal")),
        "demografia": _build_demography_evidence(zona, scores_dimension.get("demografia"), perfil_negocio, impacto_modelo.get("demografia")),
        "competencia": _build_competition_evidence(zona, scores_dimension.get("competencia"), impacto_modelo.get("competencia")),
        "precio_alquiler": _build_price_evidence(zona, scores_dimension.get("precio_alquiler"), impacto_modelo.get("precio_alquiler")),
        "transporte": _build_transport_evidence(zona, scores_dimension.get("transporte"), impacto_modelo.get("transporte")),
        "seguridad": _build_security_evidence(zona, scores_dimension.get("seguridad"), perfil_negocio, impacto_modelo.get("seguridad")),
        "turismo": _build_tourism_evidence(zona, scores_dimension.get("turismo"), impacto_modelo.get("turismo")),
        "entorno_comercial": _build_environment_evidence(zona, scores_dimension.get("entorno_comercial"), perfil_negocio, impacto_modelo.get("entorno_comercial")),
    }


def build_fallback_analysis(payload: Mapping[str, Any]) -> dict[str, Any]:
    evidencia = payload.get("evidencia_dimensiones") or {}
    score_global = float(payload.get("score_global") or 50.0)

    ordered = sorted(
        (
            (dimension, data.get("score") or 0.0)
            for dimension, data in evidencia.items()
        ),
        key=lambda item: item[1],
        reverse=True,
    )
    fuertes = [DIMENSION_LABELS[dimension] for dimension, score in ordered[:3] if score >= 60]
    debiles = [DIMENSION_LABELS[dimension] for dimension, score in ordered[-2:] if score <= 45]

    recomendacion = "Recomendado" if score_global >= 75 else "Con reservas" if score_global >= 60 else "No recomendado"
    resumen_global = _build_global_summary(payload, fuertes, debiles)

    explicaciones = {
        dimension: _evidence_to_fallback_explanation(dimension, data)
        for dimension, data in evidencia.items()
    }

    return {
        "resumen_global": resumen_global,
        "resumen": resumen_global,
        "puntos_fuertes": [
            f"{DIMENSION_LABELS.get(dimension, dimension)}: {evidencia[dimension]['hechos_clave'][0]}"
            for dimension, _score in ordered[:3]
            if evidencia.get(dimension, {}).get("hechos_clave")
        ][:3],
        "puntos_debiles": [
            f"{DIMENSION_LABELS.get(dimension, dimension)}: {evidencia[dimension]['drivers_negativos'][0]}"
            for dimension, _score in ordered[::-1]
            if evidencia.get(dimension, {}).get("drivers_negativos")
        ][:2],
        "razon_recomendacion": _build_reason(score_global, fuertes, debiles),
        "recomendacion_final": recomendacion,
        "explicaciones_dimensiones": explicaciones,
    }


def _build_flow_evidence(zona: Mapping[str, Any], score: Any, impacto: Mapping[str, Any] | None) -> dict[str, Any]:
    explain = flujo_peatonal_explain(dict(zona))
    hechos: list[str] = []
    positivos: list[str] = []
    negativos: list[str] = []

    weekend_lift = _f(zona.get("weekend_lift"))
    sunday_lift = _f(zona.get("sunday_lift"))
    volatility = _f(zona.get("seasonality_volatility"))
    summer_lift = _f(zona.get("seasonality_summer_lift"))
    christmas_lift = _f(zona.get("seasonality_christmas_lift"))
    sources_available = explain.get("sources_available", 0)

    # VCity como fuente principal — el número bruto es lo más trazable
    vcity = _f(zona.get("vcity_flujo_peatonal"))
    if vcity is not None:
        hechos.append(
            f"VCity estima {int(vcity):,} peatones/día "
            f"({_pct_vs_ref(vcity, _BCN_REF['vcity_flujo_p50'])} vs mediana BCN "
            f"{int(_BCN_REF['vcity_flujo_p50']):,})."
        )
        if vcity >= _BCN_REF["vcity_flujo_p75"]:
            positivos.append(
                f"Flujo muy alto: {int(vcity):,} peatones/día superan el p75 BCN "
                f"({int(_BCN_REF['vcity_flujo_p75']):,})."
            )
        elif vcity <= _BCN_REF["vcity_flujo_p50"] * 0.5:
            negativos.append(
                f"Flujo bajo: {int(vcity):,} peatones/día están muy por debajo de la "
                f"mediana BCN ({int(_BCN_REF['vcity_flujo_p50']):,})."
            )

    if weekend_lift is not None:
        ref = _BCN_REF["weekend_lift_media"]
        hechos.append(
            f"Fin de semana vs laborable: ×{weekend_lift:.2f} "
            f"({_pct_vs_ref(weekend_lift, ref)} vs media BCN ×{ref:.2f})."
        )
        if weekend_lift >= 1.15:
            positivos.append(
                f"Fin de semana +{(weekend_lift - 1) * 100:.0f}%: "
                f"la zona atrae compradores extra en sábado/domingo."
            )
        elif weekend_lift <= 0.90:
            negativos.append(
                f"Fin de semana {(weekend_lift - 1) * 100:.0f}% vs laborable: "
                f"caída clara el finde."
            )
    if sunday_lift is not None and sunday_lift >= 1.05:
        positivos.append(f"Domingo ×{sunday_lift:.2f} vs laborable: sigue hay paso.")
    if volatility is not None:
        hechos.append(f"Volatilidad estacional: {volatility:.2f}.")
        if volatility <= 0.18:
            positivos.append(
                f"Actividad estable (volatilidad {volatility:.2f} ≤ 0.18): "
                f"ingresos predecibles todo el año."
            )
        elif volatility >= 0.35:
            negativos.append(
                f"Actividad irregular (volatilidad {volatility:.2f} ≥ 0.35): "
                f"depende de picos concretos."
            )
    if summer_lift is not None and summer_lift >= 1.05:
        positivos.append(
            f"Verano ×{summer_lift:.2f}: el tirón estival empuja el flujo."
        )
    if christmas_lift is not None and christmas_lift >= 1.05:
        positivos.append(
            f"Navidad ×{christmas_lift:.2f}: pico comercial de diciembre aprovechable."
        )
    if not hechos:
        hechos.append("Sin datos VCity/Popular Times todavía: flujo no medible con precisión.")

    return _pack_evidence(
        score=score,
        hechos_clave=hechos[:4],
        metricas_reales={
            "flujo_peatonal_total": zona.get("flujo_peatonal_total"),
            "flujo_popular_times_score": zona.get("flujo_popular_times_score"),
            "vcity_flujo_peatonal": zona.get("vcity_flujo_peatonal"),
            "weekend_lift": weekend_lift,
            "sunday_lift": sunday_lift,
            "weekday_midday_share": zona.get("weekday_midday_share"),
            "weekend_evening_share": zona.get("weekend_evening_share"),
            "late_night_share": zona.get("late_night_share"),
            "holiday_proxy_score": zona.get("holiday_proxy_score"),
            "sources_available": sources_available,
        },
        drivers_positivos=positivos[:3],
        drivers_negativos=negativos[:3],
        fuentes=["vianants_bcn", "vcity_bsc", "popular_times_google", "aforaments", "dim_calendario_bcn"],
        confianza=_confidence_from_count(sources_available),
        impacto_modelo=impacto,
    )


def _build_transport_evidence(zona: Mapping[str, Any], score: Any, impacto: Mapping[str, Any] | None) -> dict[str, Any]:
    lineas = list(zona.get("transporte_lineas_cercanas") or [])
    hechos = []
    positivos = []
    negativos = []

    if lineas:
        principales = ", ".join(
            str(linea.get("codigo"))
            for linea in lineas[:3]
            if linea.get("codigo")
        )
        hechos.append(f"Líneas cercanas destacadas: {principales}.")
        positivos.append(f"Tienes {len(lineas)} líneas relevantes cerca de la zona.")
    if zona.get("num_lineas_transporte") and float(zona["num_lineas_transporte"]) >= 10:
        positivos.append("La cobertura de transporte público es amplia.")
    if zona.get("num_bicing_400m"):
        positivos.append("La movilidad activa también suma con Bicing cercano.")
    if zona.get("tiene_carril_bici") is False:
        negativos.append("No aparece carril bici próximo como apoyo adicional.")
    if zona.get("transporte_festivo_ratio") is not None and float(zona["transporte_festivo_ratio"]) < 0.65:
        negativos.append("La oferta de transporte baja en festivo frente a laborable.")
    if not hechos:
        hechos.append("La accesibilidad se mide con líneas, paradas, frecuencia, Bicing y carril bici.")

    return _pack_evidence(
        score=score,
        hechos_clave=hechos[:4],
        metricas_reales={
            "num_lineas_transporte": zona.get("num_lineas_transporte"),
            "num_paradas_transporte": zona.get("num_paradas_transporte"),
            "num_bicing_400m": zona.get("num_bicing_400m"),
            "tiene_carril_bici": zona.get("tiene_carril_bici"),
            "transporte_festivo_ratio": zona.get("transporte_festivo_ratio"),
            "lineas_destacadas": lineas[:5],
        },
        drivers_positivos=positivos[:3],
        drivers_negativos=negativos[:3],
        fuentes=["lineas_transporte", "paradas_transporte", "frecuencias_transporte", "estaciones_bicing", "carriles_bici"],
        confianza=_confidence_from_count(1 + bool(lineas) + bool(zona.get("num_bicing_400m")) + (zona.get("tiene_carril_bici") is not None)),
        impacto_modelo=impacto,
    )


def _build_demography_evidence(
    zona: Mapping[str, Any],
    score: Any,
    perfil_negocio: Mapping[str, Any],
    impacto: Mapping[str, Any] | None,
) -> dict[str, Any]:
    demo = calcular_score_demografia(dict(zona), idea_tags=list(perfil_negocio.get("idea_tags") or []), perfil_negocio=dict(perfil_negocio))
    renta = _f(zona.get("renta_media_hogar"))
    edad = _f(zona.get("edad_media"))
    pct_jovenes = _f(zona.get("pct_poblacio_25_44"))
    pct_estudios = _f(zona.get("nivel_estudios_alto_pct"))
    gini = _f(zona.get("gini"))
    renta_uc = _f(zona.get("renta_media_uc"))

    hechos: list[str] = []
    if renta is not None:
        hechos.append(
            f"Renta media hogar: {int(renta):,} € "
            f"({_pct_vs_ref(renta, _BCN_REF['renta_media_hogar'])} vs BCN "
            f"{int(_BCN_REF['renta_media_hogar']):,} €)."
        )
    if edad is not None:
        hechos.append(f"Edad media: {edad:.1f} años (BCN: {_BCN_REF['edad_media']:.0f}).")
    if pct_jovenes is not None:
        hechos.append(
            f"Población 25-44: {pct_jovenes * 100:.0f}% "
            f"(mediana BCN {_BCN_REF['pct_poblacio_25_44_p50'] * 100:.0f}%)."
        )

    positivos: list[str] = []
    negativos: list[str] = []
    if renta is not None and renta >= _BCN_REF["renta_media_hogar"] * 1.10:
        positivos.append(
            f"Renta {int(renta):,} € ({_pct_vs_ref(renta, _BCN_REF['renta_media_hogar'])} "
            f"sobre BCN): refuerza demanda premium."
        )
    elif renta is not None and renta <= _BCN_REF["renta_media_hogar"] * 0.85:
        negativos.append(
            f"Renta {int(renta):,} € ({_pct_vs_ref(renta, _BCN_REF['renta_media_hogar'])} "
            f"bajo BCN): ticket alto difícil de justificar."
        )

    if pct_estudios is not None and pct_estudios >= _BCN_REF["nivel_estudios_alto_p75"]:
        positivos.append(
            f"Estudios superiores {pct_estudios * 100:.0f}% (p75 BCN "
            f"{_BCN_REF['nivel_estudios_alto_p75'] * 100:.0f}%): público cualificado."
        )
    if gini is not None and gini >= _BCN_REF["gini_p75"]:
        negativos.append(
            f"Gini {gini:.1f} ≥ p75 BCN ({_BCN_REF['gini_p75']:.0f}): "
            f"demanda fragmentada por desigualdad."
        )
    if renta_uc is not None and renta_uc >= 18000:
        positivos.append(
            f"Renta por unidad de consumo {int(renta_uc):,} €: "
            f"poder adquisitivo individual alto."
        )

    return _pack_evidence(
        score=score,
        hechos_clave=hechos,
        metricas_reales={
            "renta_media_hogar": zona.get("renta_media_hogar"),
            "renta_media_uc": zona.get("renta_media_uc"),
            "renta_mediana_uc": zona.get("renta_mediana_uc"),
            "edad_media": zona.get("edad_media"),
            "pct_extranjeros": zona.get("pct_extranjeros"),
            "densidad_hab_km2": zona.get("densidad_hab_km2"),
            "pct_poblacio_25_44": zona.get("pct_poblacio_25_44"),
            "nivel_estudios_alto_pct": zona.get("nivel_estudios_alto_pct"),
            "gini": zona.get("gini"),
            "p80_p20": zona.get("p80_p20"),
            "tamano_hogar": zona.get("tamano_hogar"),
            "hogares_con_menores": zona.get("hogares_con_menores"),
            "personas_solas": zona.get("personas_solas"),
            "subscores": demo,
        },
        drivers_positivos=positivos[:3],
        drivers_negativos=negativos[:3],
        fuentes=["vz_demografia", "barcelona_open_data", "ine", "idescat"],
        confianza="alta",
        impacto_modelo=impacto,
    )


def _build_competition_evidence(zona: Mapping[str, Any], score: Any, impacto: Mapping[str, Any] | None) -> dict[str, Any]:
    competidores = list(zona.get("competidores_cercanos") or [])
    directos = [item for item in competidores if item.get("es_competencia_directa")]
    vulnerables = [item for item in directos if item.get("es_vulnerable")]

    # Percentiles por sector (si hay perfil_negocio lo inferimos más abajo,
    # aquí caemos al default genérico para no depender del prompt).
    sector = str(zona.get("sector_codigo") or "_default").lower()
    pct = _PERCENTILES_COMPETENCIA.get(sector) or _PERCENTILES_COMPETENCIA["_default"]

    n = len(directos)
    # Etiqueta de percentil contextual
    if n <= pct["q25"]:
        pct_label = "p25"
    elif n <= pct["q50"]:
        pct_label = "p50"
    elif n <= pct["q75"]:
        pct_label = "p75"
    else:
        pct_label = ">p75"

    hechos = [
        f"{n} competidores directos en el radio (percentil {pct_label} para "
        f"sector {sector}, mediana {pct['q50']})."
    ]
    positivos: list[str] = []
    negativos: list[str] = []

    if n <= pct["q25"]:
        positivos.append(
            f"{n} competidores (p25 {sector}, mediana {pct['q50']}): "
            f"presión contenida, hueco para entrar."
        )
    elif n >= pct["q75"]:
        negativos.append(
            f"{n} competidores (≥p75 {sector}, mediana {pct['q50']}): "
            f"saturación alta, difícil diferenciarse."
        )

    if vulnerables:
        positivos.append(
            f"{len(vulnerables)} competidores marcados como vulnerables "
            f"(baja rotación/reseñas): hueco explotable."
        )

    return _pack_evidence(
        score=score,
        hechos_clave=hechos,
        metricas_reales={
            "num_competidores_directos": len(directos),
            "num_competidores_total": len(competidores),
            "num_vulnerables": len(vulnerables),
            "competidores_destacados": directos[:5],
        },
        drivers_positivos=positivos[:3],
        drivers_negativos=negativos[:3],
        fuentes=["negocios_activos", "competencia_detalle_zona", "google_places"],
        confianza=_confidence_from_count(1 + bool(competidores)),
        impacto_modelo=impacto,
    )


def _build_price_evidence(zona: Mapping[str, Any], score: Any, impacto: Mapping[str, Any] | None) -> dict[str, Any]:
    alquiler = _f(zona.get("alquiler_mensual"))
    m2 = _f(zona.get("m2"))
    precio_m2 = (alquiler / m2) if alquiler and m2 else None
    hechos = []
    if alquiler is not None:
        hechos.append(f"El alquiler visible está en {alquiler:,.0f} €/mes.")
    if precio_m2 is not None:
        hechos.append(f"Eso equivale a {precio_m2:.1f} €/m² al mes.")

    positivos = []
    negativos = []
    if precio_m2 is not None and precio_m2 <= 20:
        positivos.append("El coste visible por metro cuadrado es razonable.")
    elif precio_m2 is not None and precio_m2 >= 35:
        negativos.append("El coste visible por metro cuadrado es exigente.")

    return _pack_evidence(
        score=score,
        hechos_clave=hechos or ["La lectura de precio usa el local representativo actualmente disponible."],
        metricas_reales={
            "alquiler_mensual": zona.get("alquiler_mensual"),
            "m2": zona.get("m2"),
            "precio_m2_estimado": round(precio_m2, 2) if precio_m2 is not None else None,
        },
        drivers_positivos=positivos[:2],
        drivers_negativos=negativos[:2],
        fuentes=["locales", "precios_alquiler_zona"],
        confianza="media",
        impacto_modelo=impacto,
    )


def _build_security_evidence(
    zona: Mapping[str, Any],
    score: Any,
    perfil_negocio: Mapping[str, Any],
    impacto: Mapping[str, Any] | None,
) -> dict[str, Any]:
    seguridad = calcular_score_seguridad(dict(zona), perfil_negocio=dict(perfil_negocio))
    incid = _f(zona.get("incidencias_por_1000hab"))
    hurtos = _f(zona.get("hurtos_por_1000hab"))
    robos = _f(zona.get("robatoris_por_1000hab"))
    pct_noche = _f(zona.get("incidencias_noche_pct"))
    comisarias = _f(zona.get("comisarias_1km"))

    hechos: list[str] = []
    if incid is not None:
        hechos.append(
            f"Incidencias {incid:.1f}/1.000 hab "
            f"({_pct_vs_ref(incid, _BCN_REF['incidencias_media'])} vs media BCN "
            f"{_BCN_REF['incidencias_media']:.0f})."
        )
    if hurtos is not None:
        hechos.append(
            f"Hurtos {hurtos:.1f}/1.000 hab "
            f"({_pct_vs_ref(hurtos, _BCN_REF['hurtos_media'])} vs media BCN "
            f"{_BCN_REF['hurtos_media']:.0f})."
        )
    if pct_noche is not None:
        hechos.append(f"Incidencias nocturnas: {pct_noche * 100:.0f}%.")

    positivos: list[str] = []
    negativos: list[str] = []

    if hurtos is not None and hurtos <= _BCN_REF["hurtos_media"] * 0.7:
        positivos.append(
            f"Hurtos {hurtos:.1f}/1.000 hab ({_pct_vs_ref(hurtos, _BCN_REF['hurtos_media'])} "
            f"vs BCN): riesgo bajo de carterismo."
        )
    if robos is not None and robos >= _BCN_REF["robatoris_media"] * 1.5:
        negativos.append(
            f"Robos {robos:.1f}/1.000 hab ({_pct_vs_ref(robos, _BCN_REF['robatoris_media'])} "
            f"sobre BCN): vigilancia en puerta crítica."
        )
    if comisarias is not None and comisarias >= 2:
        positivos.append(
            f"{int(comisarias)} comisarías en 1 km: presencia policial alta."
        )
    if pct_noche is not None and pct_noche >= _BCN_REF["incidencias_noche_p75"]:
        negativos.append(
            f"Incidencias nocturnas {pct_noche * 100:.0f}% ≥ p75 BCN "
            f"({_BCN_REF['incidencias_noche_p75'] * 100:.0f}%): riesgo relevante "
            f"si horario tarde-noche."
        )

    return _pack_evidence(
        score=score,
        hechos_clave=hechos,
        metricas_reales={
            "incidencias_por_1000hab": zona.get("incidencias_por_1000hab"),
            "hurtos_por_1000hab": zona.get("hurtos_por_1000hab"),
            "robatoris_por_1000hab": zona.get("robatoris_por_1000hab"),
            "danys_por_1000hab": zona.get("danys_por_1000hab"),
            "incidencias_noche_pct": zona.get("incidencias_noche_pct"),
            "comisarias_1km": zona.get("comisarias_1km"),
            "dist_comisaria_m": zona.get("dist_comisaria_m"),
            "subscores": seguridad,
        },
        drivers_positivos=positivos[:3],
        drivers_negativos=negativos[:3],
        fuentes=["vz_entorno", "guardia_urbana", "iermb"],
        confianza="alta",
        impacto_modelo=impacto,
    )


def _build_tourism_evidence(zona: Mapping[str, Any], score: Any, impacto: Mapping[str, Any] | None) -> dict[str, Any]:
    hechos = [
        f"Score turístico actual: {_fmt(zona.get('score_turismo'))}/100.",
        f"Hoteles cercanos: {_fmt(zona.get('booking_hoteles_500m'))}.",
    ]
    positivos = []
    negativos = []
    if _f(zona.get("booking_hoteles_500m")) is not None and float(zona["booking_hoteles_500m"]) >= 4:
        positivos.append("La zona tiene apoyo claro de turismo alojado.")
    if _f(zona.get("airbnb_density_500m")) is not None and float(zona["airbnb_density_500m"]) >= 40:
        positivos.append("La presión turística informal también es relevante.")
    if _f(zona.get("score_turismo")) is not None and float(zona["score_turismo"]) <= 35:
        negativos.append("El tirón turístico es limitado frente a otras zonas.")

    return _pack_evidence(
        score=score,
        hechos_clave=hechos,
        metricas_reales={
            "score_turismo": zona.get("score_turismo"),
            "airbnb_density_500m": zona.get("airbnb_density_500m"),
            "booking_hoteles_500m": zona.get("booking_hoteles_500m"),
            "eventos_culturales_500m": zona.get("eventos_culturales_500m"),
        },
        drivers_positivos=positivos[:3],
        drivers_negativos=negativos[:3],
        fuentes=["vz_turismo", "inside_airbnb", "booking", "venues_ocio"],
        confianza="media",
        impacto_modelo=impacto,
    )


def _build_environment_evidence(
    zona: Mapping[str, Any],
    score: Any,
    perfil_negocio: Mapping[str, Any],
    impacto: Mapping[str, Any] | None,
) -> dict[str, Any]:
    entorno = calcular_score_entorno(dict(zona), perfil_negocio=dict(perfil_negocio))
    hechos = [
        f"Locales vacíos: {_pct(zona.get('pct_locales_vacios'))}.",
        f"Rotación anual: {_pct(zona.get('tasa_rotacion_anual'))}.",
    ]
    positivos = []
    negativos = []
    if _f(zona.get("score_equipamientos")) is not None and float(zona["score_equipamientos"]) >= 70:
        positivos.append("La dotación de equipamientos es fuerte.")
    if _f(zona.get("mercados_municipales_1km")) is not None and float(zona["mercados_municipales_1km"]) >= 1:
        positivos.append("Hay un mercado municipal cercano como ancla comercial.")
    if _f(zona.get("pct_locales_vacios")) is not None and float(zona["pct_locales_vacios"]) >= 0.20:
        negativos.append("El vacío comercial es elevado para una zona sana.")

    return _pack_evidence(
        score=score,
        hechos_clave=hechos,
        metricas_reales={
            "pct_locales_vacios": zona.get("pct_locales_vacios"),
            "tasa_rotacion_anual": zona.get("tasa_rotacion_anual"),
            "licencias_nuevas_1a": zona.get("licencias_nuevas_1a"),
            "ratio_locales_comerciales": zona.get("ratio_locales_comerciales"),
            "score_equipamientos": zona.get("score_equipamientos"),
            "m2_zonas_verdes_cercanas": zona.get("m2_zonas_verdes_cercanas"),
            "mercados_municipales_1km": zona.get("mercados_municipales_1km"),
            "subscores": entorno,
        },
        drivers_positivos=positivos[:3],
        drivers_negativos=negativos[:3],
        fuentes=["vz_comercial", "vz_entorno", "open_data_bcn"],
        confianza="alta",
        impacto_modelo=impacto,
    )


def _pack_evidence(
    *,
    score: Any,
    hechos_clave: list[str],
    metricas_reales: dict[str, Any],
    drivers_positivos: list[str],
    drivers_negativos: list[str],
    fuentes: list[str],
    confianza: str,
    impacto_modelo: Mapping[str, Any] | None,
) -> dict[str, Any]:
    # Añadimos 1 driver SHAP para que el usuario vea el "por qué" del modelo,
    # no sólo las reglas deterministas. Sólo entra si la contribución es
    # significativa (|valor| ≥ 0.05) — evita ruido.
    top_features = list((impacto_modelo or {}).get("top_features", []))
    tendencia = (impacto_modelo or {}).get("tendencia", "neutral")

    shap_positivos: list[str] = []
    shap_negativos: list[str] = []
    for feat in top_features[:2]:
        name = str(feat.get("feature") or "")
        val = _f(feat.get("valor"))
        if name is None or val is None or abs(val) < 0.05:
            continue
        label = _FEATURE_LABELS.get(name, name.replace("_", " "))
        if val > 0:
            shap_positivos.append(
                f"El modelo prioriza {label} (peso SHAP +{val:.2f})."
            )
        else:
            shap_negativos.append(
                f"El modelo penaliza {label} (peso SHAP {val:.2f})."
            )

    # Mezclamos manteniendo los drivers deterministas al frente.
    pos_final = list(drivers_positivos) + shap_positivos[:1]
    neg_final = list(drivers_negativos) + shap_negativos[:1]

    return {
        "score": round(float(score), 1) if score is not None else None,
        "hechos_clave": hechos_clave,
        "metricas_reales": metricas_reales,
        "drivers_positivos": pos_final[:4],
        "drivers_negativos": neg_final[:4],
        "fuentes": fuentes,
        "confianza": confianza,
        "impacto_modelo": {
            "tendencia": tendencia,
            "contribucion": (impacto_modelo or {}).get("contribucion", 0.0),
            "top_features": top_features,
        },
    }


def _evidence_to_fallback_explanation(dimension: str, evidence: Mapping[str, Any]) -> dict[str, Any]:
    label = DIMENSION_LABELS.get(dimension, dimension)
    score = evidence.get("score")
    positivos = list(evidence.get("drivers_positivos") or [])
    negativos = list(evidence.get("drivers_negativos") or [])
    hechos = list(evidence.get("hechos_clave") or [])
    impacto = evidence.get("impacto_modelo") or {}

    impact_phrase = {
        "empuja_a_favor": "Ahora mismo el modelo está empujando esta dimensión a favor de la recomendación.",
        "empuja_en_contra": "Ahora mismo el modelo está frenando la recomendación por esta dimensión.",
        "neutral": "El modelo no está cargando un peso extremo en esta dimensión.",
    }.get(impacto.get("tendencia"), "El modelo no está cargando un peso extremo en esta dimensión.")

    return {
        "score": score,
        "titular": f"{label} {_score_band(score)}",
        "explicacion_corta": _build_short_dimension_explanation(label, score, positivos, negativos, hechos),
        "porque_sube": positivos[:3],
        "porque_baja": negativos[:3],
        "hechos_clave": hechos[:4],
        "impacto_modelo": impact_phrase,
        "confianza": evidence.get("confianza", "media"),
        "fuentes": list(evidence.get("fuentes") or []),
    }


def _build_short_dimension_explanation(
    label: str,
    score: Any,
    positivos: list[str],
    negativos: list[str],
    hechos: list[str],
) -> str:
    band = _score_band(score)
    parts = [f"{label} {score if score is not None else 'N/A'}: nivel {band}."]
    if positivos:
        parts.append(positivos[0])
    elif negativos:
        parts.append(negativos[0])
    elif hechos:
        parts.append(hechos[0])
    return " ".join(parts)


def _build_global_summary(payload: Mapping[str, Any], fuertes: list[str], debiles: list[str]) -> str:
    zona = payload.get("zona") or {}
    score_global = float(payload.get("score_global") or 50.0)
    nombre = zona.get("nombre") or "la zona"
    barrio = zona.get("barrio") or ""
    summary = [f"{nombre} en {barrio} parte de un score global de {score_global:.1f}/100."]
    if fuertes:
        summary.append(f"Lo mejor ahora mismo está en {', '.join(fuertes[:2])}.")
    if debiles:
        summary.append(f"Los principales frenos aparecen en {', '.join(debiles[:2])}.")
    return " ".join(summary)


def _build_reason(score_global: float, fuertes: list[str], debiles: list[str]) -> str:
    if score_global >= 75:
        return f"La recomendación sale bien porque las fortalezas de {', '.join(fuertes[:2]) or 'la zona'} pesan más que los riesgos actuales."
    if score_global >= 60:
        return f"La zona tiene potencial, pero conviene vigilar especialmente {', '.join(debiles[:2]) or 'los puntos débiles detectados'}."
    return f"Hoy la recomendación es prudente porque los frenos de {', '.join(debiles[:2]) or 'varias dimensiones'} pesan más que las fortalezas."


def _score_band(score: Any) -> str:
    if score is None:
        return "sin dato"
    score = float(score)
    if score >= 85:
        return "muy alto"
    if score >= 70:
        return "alto"
    if score >= 55:
        return "medio"
    if score >= 40:
        return "justo"
    return "bajo"


def _confidence_from_count(count: int | bool) -> str:
    value = int(count)
    if value >= 4:
        return "alta"
    if value >= 2:
        return "media"
    return "baja"


def _f(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fmt(value: Any) -> str:
    parsed = _f(value)
    if parsed is None:
        return "N/D"
    if parsed.is_integer():
        return str(int(parsed))
    return f"{parsed:.1f}"


def _pct(value: Any) -> str:
    parsed = _f(value)
    if parsed is None:
        return "N/D"
    return f"{parsed * 100:.0f}%"
