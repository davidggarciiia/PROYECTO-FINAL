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

    if weekend_lift is not None:
        hechos.append(f"El fin de semana rinde {weekend_lift:.2f}x frente a un laborable.")
        if weekend_lift >= 1.15:
            positivos.append("La zona gana intensidad en fin de semana.")
        elif weekend_lift <= 0.90:
            negativos.append("El fin de semana no mejora claramente frente a laborable.")
    if sunday_lift is not None and sunday_lift >= 1.05:
        positivos.append("El domingo sigue teniendo tracción.")
    if volatility is not None:
        hechos.append(f"La volatilidad estacional está en {volatility:.2f}.")
        if volatility <= 0.18:
            positivos.append("La actividad se reparte bien durante el año.")
        elif volatility >= 0.35:
            negativos.append("La actividad es irregular y depende más de picos concretos.")
    if summer_lift is not None and summer_lift >= 1.05:
        positivos.append("Verano empuja el flujo por encima de la media de la zona.")
    if christmas_lift is not None and christmas_lift >= 1.05:
        positivos.append("Navidad aporta un pico comercial aprovechable.")
    if not hechos:
        hechos.append("La explicación de flujo se apoya en las fuentes peatonales disponibles de la zona.")

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
    hechos = [
        f"Renta media del hogar: {int(_f(zona.get('renta_media_hogar')) or 0):,} €.",
        f"Edad media: {_fmt(zona.get('edad_media'))} años.",
        f"Población 25-44: {_pct(zona.get('pct_poblacio_25_44'))}.",
    ]
    positivos = []
    negativos = []
    if _f(zona.get("nivel_estudios_alto_pct")) is not None and float(zona["nivel_estudios_alto_pct"]) >= 0.45:
        positivos.append("El capital humano de la zona es alto.")
    if _f(zona.get("gini")) is not None and float(zona["gini"]) >= 38:
        negativos.append("La desigualdad es elevada y puede fragmentar demanda.")
    if _f(zona.get("renta_media_uc")) is not None and float(zona["renta_media_uc"]) >= 18000:
        positivos.append("La renta por unidad de consumo refuerza poder adquisitivo.")

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
    hechos = [f"Hay {len(directos)} competidores directos en el radio de detalle."]
    positivos = []
    negativos = []
    if len(directos) <= 3:
        positivos.append("La presión competitiva directa es contenida.")
    if len(directos) >= 8:
        negativos.append("La saturación competitiva es alta para el mismo sector.")
    if vulnerables:
        positivos.append("Parte de la competencia cercana muestra fragilidad operativa.")

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
    hechos = [
        f"Incidencias totales: {_fmt(zona.get('incidencias_por_1000hab'))} por 1.000 habitantes.",
        f"Incidencias nocturnas: {_pct(zona.get('incidencias_noche_pct'))}.",
    ]
    positivos = []
    negativos = []
    if _f(zona.get("comisarias_1km")) is not None and float(zona["comisarias_1km"]) >= 2:
        positivos.append("Hay buena presencia policial cerca.")
    if _f(zona.get("incidencias_noche_pct")) is not None and float(zona["incidencias_noche_pct"]) >= 0.40:
        negativos.append("El peso de la noche en incidencias es alto.")

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
    return {
        "score": round(float(score), 1) if score is not None else None,
        "hechos_clave": hechos_clave,
        "metricas_reales": metricas_reales,
        "drivers_positivos": drivers_positivos,
        "drivers_negativos": drivers_negativos,
        "fuentes": fuentes,
        "confianza": confianza,
        "impacto_modelo": {
            "tendencia": (impacto_modelo or {}).get("tendencia", "neutral"),
            "contribucion": (impacto_modelo or {}).get("contribucion", 0.0),
            "top_features": list((impacto_modelo or {}).get("top_features", [])),
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
