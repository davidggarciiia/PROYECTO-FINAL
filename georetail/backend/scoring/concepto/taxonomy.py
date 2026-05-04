"""Taxonomia conceptual unificada para clasificacion de negocio."""
from __future__ import annotations

from copy import deepcopy
import math
import re
import unicodedata

CATALOG_VERSION = "concept_taxonomy_v2"
MAX_VISIBLE_TAGS = 12
MAX_BASE_CONCEPTS = 3
MAX_MODIFIERS = 15

PROFILE_KEYS = (
    "dependencia_flujo",
    "nivel_precio",
    "clientela_turismo",
    "clientela_vecindario",
    "horario_nocturno",
    "experiencial",
    "citas_previas",
    "sensibilidad_alquiler",
)

SCORE_DIMENSIONS = (
    "score_flujo_peatonal",
    "score_demografia",
    "score_competencia",
    "score_precio_alquiler",
    "score_transporte",
    "score_seguridad",
    "score_turismo",
    "score_dinamismo",
)

WEIGHT_FIELD_BY_DIM = {
    "score_flujo_peatonal": "peso_flujo",
    "score_demografia": "peso_demo",
    "score_competencia": "peso_competencia",
    "score_precio_alquiler": "peso_precio",
    "score_transporte": "peso_transporte",
    "score_seguridad": "peso_seguridad",
    "score_turismo": "peso_turismo",
    "score_dinamismo": "peso_dinamismo",
}

DEFAULT_SECTOR_WEIGHTS = {
    "peso_flujo": 0.25,
    "peso_demo": 0.25,
    "peso_competencia": 0.15,
    "peso_precio": 0.00,   # stored in DB but excluded from global score formula
    "peso_transporte": 0.15,
    "peso_seguridad": 0.05,
    "peso_turismo": 0.05,
    "peso_dinamismo": 0.10,
}

NEUTRAL_PROFILE = {
    "dependencia_flujo": 0.50,
    "nivel_precio": 0.50,
    "clientela_turismo": 0.30,
    "clientela_vecindario": 0.50,
    "horario_nocturno": 0.10,
    "experiencial": 0.20,
    "citas_previas": 0.20,
    "sensibilidad_alquiler": 0.50,
}

SECTOR_PROFILE_DEFAULTS: dict[str, dict[str, float]] = {
    "restauracion": {
        "dependencia_flujo": 0.70,
        "nivel_precio": 0.45,
        "clientela_turismo": 0.35,
        "clientela_vecindario": 0.50,
        "horario_nocturno": 0.30,
        "experiencial": 0.30,
        "citas_previas": 0.10,
        "sensibilidad_alquiler": 0.60,
    },
    "moda": {
        "dependencia_flujo": 0.75,
        "nivel_precio": 0.50,
        "clientela_turismo": 0.25,
        "clientela_vecindario": 0.40,
        "horario_nocturno": 0.00,
        "experiencial": 0.20,
        "citas_previas": 0.00,
        "sensibilidad_alquiler": 0.55,
    },
    "estetica": {
        "dependencia_flujo": 0.30,
        "nivel_precio": 0.55,
        "clientela_turismo": 0.10,
        "clientela_vecindario": 0.80,
        "horario_nocturno": 0.00,
        "experiencial": 0.40,
        "citas_previas": 0.80,
        "sensibilidad_alquiler": 0.50,
    },
    "tatuajes": {
        "dependencia_flujo": 0.40,
        "nivel_precio": 0.55,
        "clientela_turismo": 0.20,
        "clientela_vecindario": 0.50,
        "horario_nocturno": 0.00,
        "experiencial": 0.70,
        "citas_previas": 0.90,
        "sensibilidad_alquiler": 0.50,
    },
    "shisha_lounge": {
        "dependencia_flujo": 0.40,
        "nivel_precio": 0.40,
        "clientela_turismo": 0.30,
        "clientela_vecindario": 0.60,
        "horario_nocturno": 0.90,
        "experiencial": 0.80,
        "citas_previas": 0.00,
        "sensibilidad_alquiler": 0.60,
    },
    "salud": {
        "dependencia_flujo": 0.15,
        "nivel_precio": 0.60,
        "clientela_turismo": 0.05,
        "clientela_vecindario": 0.70,
        "horario_nocturno": 0.00,
        "experiencial": 0.10,
        "citas_previas": 0.95,
        "sensibilidad_alquiler": 0.30,
    },
    "deporte": {
        "dependencia_flujo": 0.20,
        "nivel_precio": 0.50,
        "clientela_turismo": 0.10,
        "clientela_vecindario": 0.60,
        "horario_nocturno": 0.00,
        "experiencial": 0.50,
        "citas_previas": 0.50,
        "sensibilidad_alquiler": 0.55,
    },
    "educacion": {
        "dependencia_flujo": 0.10,
        "nivel_precio": 0.50,
        "clientela_turismo": 0.05,
        "clientela_vecindario": 0.75,
        "horario_nocturno": 0.00,
        "experiencial": 0.30,
        "citas_previas": 0.80,
        "sensibilidad_alquiler": 0.45,
    },
    "alimentacion": {
        "dependencia_flujo": 0.65,
        "nivel_precio": 0.35,
        "clientela_turismo": 0.15,
        "clientela_vecindario": 0.90,
        "horario_nocturno": 0.00,
        "experiencial": 0.10,
        "citas_previas": 0.00,
        "sensibilidad_alquiler": 0.70,
    },
    "servicios": {
        "dependencia_flujo": 0.40,
        "nivel_precio": 0.30,
        "clientela_turismo": 0.05,
        "clientela_vecindario": 0.85,
        "horario_nocturno": 0.00,
        "experiencial": 0.00,
        "citas_previas": 0.30,
        "sensibilidad_alquiler": 0.70,
    },
    "otro": deepcopy(NEUTRAL_PROFILE),
    "desconocido": deepcopy(NEUTRAL_PROFILE),
}


def _slug_label(slug: str) -> str:
    return slug.replace("_", " ").strip().title()


def _norm(text: str | None) -> str:
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", str(text))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _clip01(value: float | int | str | None, default: float = 0.5) -> float:
    try:
        return max(0.0, min(1.0, float(value)))
    except (TypeError, ValueError):
        return default


def _clip(value: float | int | str | None, minimum: float, maximum: float, default: float) -> float:
    try:
        return max(minimum, min(maximum, float(value)))
    except (TypeError, ValueError):
        return default


def _round_profile(profile: dict[str, float]) -> dict[str, float]:
    return {key: round(_clip01(profile.get(key), NEUTRAL_PROFILE[key]), 3) for key in PROFILE_KEYS}


def _sector_profile(sector: str | None) -> dict[str, float]:
    return deepcopy(SECTOR_PROFILE_DEFAULTS.get(sector or "desconocido", NEUTRAL_PROFILE))


def zona_ideal_desde_perfil(perfil_negocio: dict | None) -> dict[str, float | tuple[float, float]]:
    if not perfil_negocio:
        return {}

    perfil = {**NEUTRAL_PROFILE, **{k: _clip01(v, NEUTRAL_PROFILE[k]) for k, v in (perfil_negocio or {}).items() if k in PROFILE_KEYS}}
    nivel_precio = perfil["nivel_precio"]
    dependencia_flujo = perfil["dependencia_flujo"]
    clientela_turismo = perfil["clientela_turismo"]
    clientela_vecindario = perfil["clientela_vecindario"]
    experiencial = perfil["experiencial"]
    horario_nocturno = perfil["horario_nocturno"]

    edad_centro = 27.0 + (1.0 - clientela_vecindario) * 12.0 + nivel_precio * 6.0
    edad_apertura = 8.0 + experiencial * 4.0
    edad_min = max(18.0, edad_centro - edad_apertura)
    edad_max = min(65.0, edad_centro + edad_apertura)

    return {
        "renta_ideal": round(0.15 + nivel_precio * 0.75, 3),
        "flujo_min": round(100.0 + dependencia_flujo * 1100.0, 1),
        "turismo_ideal": round(10.0 + clientela_turismo * 80.0, 1),
        "ratio_comercial_min": round(0.10 + (1.0 - clientela_vecindario) * 0.15 + experiencial * 0.05, 3),
        "seguridad_min": round(50.0 + max(horario_nocturno, clientela_vecindario) * 25.0, 1),
        "edad_rango": (round(edad_min, 1), round(edad_max, 1)),
    }


def score_bias_desde_perfil(perfil_negocio: dict | None) -> dict[str, float]:
    if not perfil_negocio:
        return {dim: 1.0 for dim in SCORE_DIMENSIONS}

    perfil = {**NEUTRAL_PROFILE, **{k: _clip01(v, NEUTRAL_PROFILE[k]) for k, v in (perfil_negocio or {}).items() if k in PROFILE_KEYS}}
    dependencia_flujo = perfil["dependencia_flujo"]
    nivel_precio = perfil["nivel_precio"]
    clientela_turismo = perfil["clientela_turismo"]
    clientela_vecindario = perfil["clientela_vecindario"]
    horario_nocturno = perfil["horario_nocturno"]
    experiencial = perfil["experiencial"]
    citas_previas = perfil["citas_previas"]
    sensibilidad_alquiler = perfil["sensibilidad_alquiler"]

    multipliers = {
        "score_flujo_peatonal": 0.70 + dependencia_flujo * 1.55,
        "score_demografia": 0.70 + max(nivel_precio, clientela_vecindario) * 1.20,
        "score_competencia": 0.80 + dependencia_flujo * 0.35 + experiencial * 0.25,
        "score_precio_alquiler": 0.65 + sensibilidad_alquiler * 1.60,
        "score_transporte": 0.75 + (1.0 - clientela_vecindario) * 0.70 + citas_previas * 0.35,
        "score_seguridad": 0.75 + max(horario_nocturno, clientela_vecindario) * 0.95,
        "score_turismo": 0.70 + clientela_turismo * 1.55,
        "score_dinamismo": 0.75 + dependencia_flujo * 0.35 + clientela_vecindario * 0.30,
    }
    return {key: round(max(0.40, min(2.50, value)), 3) for key, value in multipliers.items()}


def _weights_from_multipliers(pesos_sector: dict, multipliers: dict[str, float]) -> dict[str, float]:
    pesos = {}
    for dim, field in WEIGHT_FIELD_BY_DIM.items():
        base_value = pesos_sector.get(field)
        base = float(base_value) if base_value is not None else DEFAULT_SECTOR_WEIGHTS[field]
        pesos[field] = base * float(multipliers.get(dim, 1.0))

    total = sum(pesos.values())
    if total <= 0:
        return deepcopy(DEFAULT_SECTOR_WEIGHTS)
    return {field: round(value / total, 4) for field, value in pesos.items()}


def aplicar_pesos_a_sector(pesos_sector: dict, concepto_negocio: dict | None) -> dict[str, float]:
    if not concepto_negocio:
        return {**DEFAULT_SECTOR_WEIGHTS, **(pesos_sector or {})}

    multipliers = ((concepto_negocio.get("pesos_scoring") or {}).get("multipliers") or {})
    base_pesos = {**DEFAULT_SECTOR_WEIGHTS, **(pesos_sector or {})}
    return _weights_from_multipliers(base_pesos, multipliers)


def _score_bias_entry(bias: dict | None) -> dict[str, float]:
    bias = bias or {}
    return {dim: round(_clip(bias.get(dim), 0.25, 3.0, 1.0), 3) for dim in SCORE_DIMENSIONS if dim in bias}


def _build_entry(
    canonical_id: str,
    *,
    kind: str,
    namespace: str,
    label: str,
    sector: str | None = None,
    aliases: list[str] | tuple[str, ...] | None = None,
    visible_tag: str | None = None,
    profile: dict[str, float] | None = None,
    zone_ideal: dict | None = None,
    score_bias: dict | None = None,
    visible_priority: float = 0.5,
    source: str = "taxonomy",
) -> dict:
    alias_values = set(aliases or [])
    alias_values.add(canonical_id)
    alias_values.add(label)
    if visible_tag:
        alias_values.add(visible_tag)
    return {
        "id": canonical_id,
        "kind": kind,
        "namespace": namespace,
        "label": label,
        "sector": sector,
        "aliases": sorted({_norm(alias) for alias in alias_values if _norm(alias)}),
        "visible_tag": visible_tag,
        "visible_priority": float(visible_priority),
        "profile": _round_profile(profile or _sector_profile(sector)),
        "zone_ideal": deepcopy(zone_ideal or {}),
        "score_bias": _score_bias_entry(score_bias),
        "source": source,
    }


def _make_base(
    sector: str,
    slug: str,
    label: str,
    *,
    aliases: list[str] | tuple[str, ...] | None = None,
    visible_tag: str | None = None,
    profile_updates: dict[str, float] | None = None,
    zone_updates: dict | None = None,
    score_updates: dict | None = None,
):
    profile = _sector_profile(sector)
    profile.update(profile_updates or {})
    zone_ideal = zona_ideal_desde_perfil(profile)
    zone_ideal.update(zone_updates or {})
    return _build_entry(
        f"base.{sector}.{slug}",
        kind="base",
        namespace="base",
        label=label,
        sector=sector,
        aliases=aliases,
        visible_tag=visible_tag,
        profile=profile,
        zone_ideal=zone_ideal,
        score_bias=score_updates,
        visible_priority=0.95,
        source="base_catalog",
    )


def _make_modifier(
    namespace: str,
    slug: str,
    *,
    label: str | None = None,
    aliases: list[str] | tuple[str, ...] | None = None,
    visible_tag: str | None = None,
    profile: dict[str, float] | None = None,
    zone_ideal: dict | None = None,
    score_bias: dict | None = None,
    visible_priority: float = 0.6,
):
    return _build_entry(
        f"{namespace}.{slug}",
        kind="modifier",
        namespace=namespace,
        label=label or _slug_label(slug),
        aliases=aliases,
        visible_tag=visible_tag,
        profile={**NEUTRAL_PROFILE, **(profile or {})},
        zone_ideal=zone_ideal,
        score_bias=score_bias,
        visible_priority=visible_priority,
        source="modifier_catalog",
    )


BASE_VARIANTS_BY_SECTOR = {
    "restauracion": [
        ("tapas_bar", "Tapas bar", ["bar de tapas", "tapas bar", "taberna", "pincheria", "bar de pinchos", "tapas", "bares de tapas"], "tapas_bar", {"dependencia_flujo": 0.85, "nivel_precio": 0.45, "clientela_turismo": 0.30, "clientela_vecindario": 0.60, "horario_nocturno": 0.20, "experiencial": 0.35, "sensibilidad_alquiler": 0.65}, {"flujo_min": 600.0}, None),
        ("specialty_coffee", "Specialty coffee bar", ["specialty coffee", "cafe de especialidad", "third wave coffee", "cafeteria_specialty_coffee"], "specialty_coffee", {"dependencia_flujo": 0.35, "nivel_precio": 0.72, "clientela_turismo": 0.15, "clientela_vecindario": 0.70, "horario_nocturno": 0.05, "experiencial": 0.55, "sensibilidad_alquiler": 0.45}, {"ratio_comercial_min": 0.18}, None),
        ("neighborhood_cafe", "Neighborhood cafe", ["cafeteria de barrio", "bar cafeteria", "coffee shop local"], None, {"dependencia_flujo": 0.78, "nivel_precio": 0.35, "clientela_turismo": 0.10, "clientela_vecindario": 0.88, "horario_nocturno": 0.05, "experiencial": 0.18, "sensibilidad_alquiler": 0.70}, {"ratio_comercial_min": 0.15}, None),
        ("express_cafe", "Express grab-and-go cafe", ["cafeteria express", "grab and go", "cafeteria_express_paso"], None, {"dependencia_flujo": 0.93, "nivel_precio": 0.28, "clientela_turismo": 0.28, "clientela_vecindario": 0.25, "horario_nocturno": 0.02, "experiencial": 0.10, "sensibilidad_alquiler": 0.80}, {"flujo_min": 850.0, "ratio_comercial_min": 0.25}, None),
        ("coworking_cafe", "Coworking cafe", ["coworking cafe", "workspace cafe", "cafeteria coworking", "cafeteria_coworking"], "coworking_cafe", {"dependencia_flujo": 0.24, "nivel_precio": 0.66, "clientela_turismo": 0.18, "clientela_vecindario": 0.48, "experiencial": 0.52, "citas_previas": 0.08, "sensibilidad_alquiler": 0.42}, {"seguridad_min": 58.0}, None),
        ("dog_friendly_cafe", "Dog friendly cafe", ["cafeteria dog friendly", "cafe con perros", "cafeteria_con_perros"], None, {"dependencia_flujo": 0.34, "nivel_precio": 0.52, "clientela_turismo": 0.10, "clientela_vecindario": 0.84, "horario_nocturno": 0.05, "experiencial": 0.40, "sensibilidad_alquiler": 0.56}, {"zonas_verdes_bonus": 0.80, "seguridad_min": 60.0, "ratio_comercial_min": 0.12}, None),
        ("brunch_house", "Brunch house", ["brunch", "brunch house", "all day brunch"], None, {"dependencia_flujo": 0.60, "nivel_precio": 0.62, "clientela_turismo": 0.30, "clientela_vecindario": 0.58, "experiencial": 0.50, "sensibilidad_alquiler": 0.46}, {"flujo_min": 450.0}, None),
        ("fine_dining", "Fine dining restaurant", ["fine dining", "alta cocina", "menu degustacion", "restaurante_alta_cocina"], "gastronomico_premium", {"dependencia_flujo": 0.12, "nivel_precio": 0.96, "clientela_turismo": 0.48, "clientela_vecindario": 0.30, "horario_nocturno": 0.20, "experiencial": 0.78, "citas_previas": 0.80, "sensibilidad_alquiler": 0.12}, {"ratio_comercial_min": 0.16}, None),
        ("cocktail_bar", "Cocktail bar", ["bar de cocteles", "cocktail bar", "bar_cocteleria"], None, {"dependencia_flujo": 0.56, "nivel_precio": 0.68, "clientela_turismo": 0.45, "clientela_vecindario": 0.38, "horario_nocturno": 0.92, "experiencial": 0.70, "sensibilidad_alquiler": 0.40}, {"seguridad_min": 72.0}, None),
        ("street_food_counter", "Street food counter", ["street food", "fast casual", "smash burger", "bao", "street_food"], "street_food", {"dependencia_flujo": 0.95, "nivel_precio": 0.24, "clientela_turismo": 0.40, "clientela_vecindario": 0.28, "experiencial": 0.22, "sensibilidad_alquiler": 0.84}, {"flujo_min": 1000.0, "ratio_comercial_min": 0.28}, None),
        ("vegan_bistro", "Vegan wellness bistro", ["restaurante vegano", "vegan bistro", "plant based restaurant"], None, {"dependencia_flujo": 0.42, "nivel_precio": 0.64, "clientela_turismo": 0.24, "clientela_vecindario": 0.62, "experiencial": 0.48, "sensibilidad_alquiler": 0.44}, {"ratio_comercial_min": 0.18}, None),
    ],
    "moda": [
        ("premium_boutique", "Premium fashion boutique", ["boutique premium", "tienda premium", "alta_renta"], None, {"dependencia_flujo": 0.62, "nivel_precio": 0.86, "clientela_turismo": 0.22, "clientela_vecindario": 0.35, "experiencial": 0.48, "sensibilidad_alquiler": 0.25}, {"ratio_comercial_min": 0.28}, None),
        ("vintage_store", "Vintage store", ["tienda vintage", "second hand boutique"], None, {"dependencia_flujo": 0.60, "nivel_precio": 0.46, "clientela_turismo": 0.18, "clientela_vecindario": 0.44, "experiencial": 0.44, "sensibilidad_alquiler": 0.58}, {"ratio_comercial_min": 0.22}, None),
        ("streetwear_store", "Streetwear store", ["streetwear", "moda urbana"], None, {"dependencia_flujo": 0.78, "nivel_precio": 0.54, "clientela_turismo": 0.22, "clientela_vecindario": 0.34, "experiencial": 0.38, "sensibilidad_alquiler": 0.52}, {"ratio_comercial_min": 0.28}, None),
        ("sneaker_shop", "Sneaker shop", ["tienda sneakers", "zapatillas coleccion", "sneaker boutique"], None, {"dependencia_flujo": 0.76, "nivel_precio": 0.62, "clientela_turismo": 0.20, "clientela_vecindario": 0.32, "experiencial": 0.44, "sensibilidad_alquiler": 0.46}, {"ratio_comercial_min": 0.28}, None),
        ("jewelry_store", "Jewelry boutique", ["joyeria boutique", "jewellery boutique"], None, {"dependencia_flujo": 0.56, "nivel_precio": 0.80, "clientela_turismo": 0.24, "clientela_vecindario": 0.28, "experiencial": 0.50, "sensibilidad_alquiler": 0.30}, {"ratio_comercial_min": 0.22}, None),
        ("home_decor_store", "Home decor store", ["decoracion hogar", "home decor"], None, {"dependencia_flujo": 0.54, "nivel_precio": 0.60, "clientela_turismo": 0.14, "clientela_vecindario": 0.42, "experiencial": 0.38, "sensibilidad_alquiler": 0.48}, {"ratio_comercial_min": 0.18}, None),
        ("concept_store", "Design concept store", ["concept store", "lifestyle store", "tienda concepto"], None, {"dependencia_flujo": 0.58, "nivel_precio": 0.72, "clientela_turismo": 0.24, "clientela_vecindario": 0.36, "experiencial": 0.66, "sensibilidad_alquiler": 0.34}, {"ratio_comercial_min": 0.24}, None),
    ],
    "estetica": [
        ("hair_salon", "Hair salon", ["peluqueria", "hair salon"], None, {"dependencia_flujo": 0.24, "nivel_precio": 0.54, "clientela_turismo": 0.08, "clientela_vecindario": 0.84, "experiencial": 0.36, "citas_previas": 0.82, "sensibilidad_alquiler": 0.48}, {"seguridad_min": 58.0}, None),
        ("barber_shop", "Barber shop", ["barberia", "barber shop"], None, {"dependencia_flujo": 0.34, "nivel_precio": 0.44, "clientela_turismo": 0.10, "clientela_vecindario": 0.78, "experiencial": 0.34, "citas_previas": 0.72, "sensibilidad_alquiler": 0.52}, {"seguridad_min": 56.0}, None),
        ("nail_studio", "Nail studio", ["nail bar", "manicura", "studio de unas"], None, {"dependencia_flujo": 0.22, "nivel_precio": 0.52, "clientela_turismo": 0.08, "clientela_vecindario": 0.80, "experiencial": 0.42, "citas_previas": 0.86, "sensibilidad_alquiler": 0.48}, {"seguridad_min": 58.0}, None),
        ("brow_lash_bar", "Brow and lash bar", ["lash bar", "cejas y pestanas"], None, {"dependencia_flujo": 0.22, "nivel_precio": 0.58, "clientela_turismo": 0.10, "clientela_vecindario": 0.72, "experiencial": 0.48, "citas_previas": 0.88, "sensibilidad_alquiler": 0.44}, {"seguridad_min": 58.0}, None),
        ("beauty_clinic", "Beauty clinic", ["medicina estetica", "beauty clinic"], None, {"dependencia_flujo": 0.12, "nivel_precio": 0.76, "clientela_turismo": 0.10, "clientela_vecindario": 0.66, "experiencial": 0.32, "citas_previas": 0.94, "sensibilidad_alquiler": 0.34}, {"seguridad_min": 60.0}, None),
        ("day_spa", "Day spa", ["spa urbano", "day spa"], None, {"dependencia_flujo": 0.16, "nivel_precio": 0.78, "clientela_turismo": 0.18, "clientela_vecindario": 0.56, "experiencial": 0.82, "citas_previas": 0.78, "sensibilidad_alquiler": 0.34}, {"seguridad_min": 62.0}, None),
        ("skin_clinic", "Skin clinic", ["clinica de la piel", "skin clinic"], None, {"dependencia_flujo": 0.12, "nivel_precio": 0.72, "clientela_turismo": 0.08, "clientela_vecindario": 0.64, "experiencial": 0.26, "citas_previas": 0.96, "sensibilidad_alquiler": 0.36}, {"seguridad_min": 60.0}, None),
    ],
    "tatuajes": [
        ("tattoo_studio", "Tattoo studio", ["tattoo studio", "estudio de tatuajes"], None, {"dependencia_flujo": 0.26, "nivel_precio": 0.56, "clientela_turismo": 0.18, "clientela_vecindario": 0.48, "experiencial": 0.72, "citas_previas": 0.92, "sensibilidad_alquiler": 0.48}, {"seguridad_min": 58.0}, None),
        ("piercing_studio", "Piercing studio", ["piercing studio", "estudio piercing"], None, {"dependencia_flujo": 0.30, "nivel_precio": 0.46, "clientela_turismo": 0.16, "clientela_vecindario": 0.44, "experiencial": 0.58, "citas_previas": 0.86, "sensibilidad_alquiler": 0.54}, {"seguridad_min": 58.0}, None),
        ("tattoo_gallery", "Tattoo gallery", ["tattoo gallery", "fine line tattoo"], None, {"dependencia_flujo": 0.18, "nivel_precio": 0.72, "clientela_turismo": 0.22, "clientela_vecindario": 0.34, "experiencial": 0.82, "citas_previas": 0.94, "sensibilidad_alquiler": 0.34}, {"seguridad_min": 60.0}, None),
        ("cosmetic_tattoo", "Cosmetic tattoo studio", ["microblading", "maquillaje permanente"], None, {"dependencia_flujo": 0.14, "nivel_precio": 0.70, "clientela_turismo": 0.06, "clientela_vecindario": 0.68, "experiencial": 0.44, "citas_previas": 0.96, "sensibilidad_alquiler": 0.38}, {"seguridad_min": 60.0}, None),
        ("fine_line_studio", "Fine line tattoo studio", ["fine line tattoo", "tattoo minimalista"], None, {"dependencia_flujo": 0.20, "nivel_precio": 0.68, "clientela_turismo": 0.20, "clientela_vecindario": 0.38, "experiencial": 0.84, "citas_previas": 0.94, "sensibilidad_alquiler": 0.36}, {"seguridad_min": 60.0}, None),
        ("street_tattoo", "Street tattoo shop", ["tattoo walk in", "tattoo de paso"], None, {"dependencia_flujo": 0.48, "nivel_precio": 0.40, "clientela_turismo": 0.28, "clientela_vecindario": 0.30, "experiencial": 0.54, "citas_previas": 0.58, "sensibilidad_alquiler": 0.60}, {"ratio_comercial_min": 0.20}, None),
        ("body_art_collective", "Body art collective", ["body art collective"], None, {"dependencia_flujo": 0.26, "nivel_precio": 0.58, "clientela_turismo": 0.24, "clientela_vecindario": 0.32, "experiencial": 0.86, "citas_previas": 0.90, "sensibilidad_alquiler": 0.44}, {"seguridad_min": 58.0}, None),
    ],
    "shisha_lounge": [
        ("classic_lounge", "Classic shisha lounge", ["shisha lounge", "hookah lounge"], None, {"dependencia_flujo": 0.44, "nivel_precio": 0.42, "clientela_turismo": 0.28, "clientela_vecindario": 0.58, "horario_nocturno": 0.92, "experiencial": 0.78, "sensibilidad_alquiler": 0.58}, {"seguridad_min": 72.0}, None),
        ("premium_lounge", "Premium shisha lounge", ["shisha premium"], None, {"dependencia_flujo": 0.34, "nivel_precio": 0.68, "clientela_turismo": 0.32, "clientela_vecindario": 0.40, "horario_nocturno": 0.94, "experiencial": 0.84, "sensibilidad_alquiler": 0.38}, {"seguridad_min": 74.0}, None),
        ("terrace_lounge", "Terrace shisha lounge", ["shisha terraza"], None, {"dependencia_flujo": 0.46, "nivel_precio": 0.48, "clientela_turismo": 0.34, "clientela_vecindario": 0.42, "horario_nocturno": 0.90, "experiencial": 0.80, "sensibilidad_alquiler": 0.54}, {"seguridad_min": 72.0}, None),
        ("student_lounge", "Student shisha lounge", ["shisha estudiantes"], None, {"dependencia_flujo": 0.54, "nivel_precio": 0.24, "clientela_turismo": 0.18, "clientela_vecindario": 0.40, "horario_nocturno": 0.90, "experiencial": 0.72, "sensibilidad_alquiler": 0.78}, {"seguridad_min": 70.0}, None),
        ("tourist_lounge", "Tourist shisha lounge", ["shisha turistico"], None, {"dependencia_flujo": 0.60, "nivel_precio": 0.40, "clientela_turismo": 0.72, "clientela_vecindario": 0.18, "horario_nocturno": 0.92, "experiencial": 0.74, "sensibilidad_alquiler": 0.62}, {"turismo_ideal": 70.0}, None),
        ("music_lounge", "Music shisha lounge", ["shisha musica", "lounge dj"], None, {"dependencia_flujo": 0.50, "nivel_precio": 0.46, "clientela_turismo": 0.28, "clientela_vecindario": 0.34, "horario_nocturno": 0.96, "experiencial": 0.86, "sensibilidad_alquiler": 0.50}, {"seguridad_min": 75.0}, None),
        ("food_lounge", "Shisha lounge with kitchen", ["shisha con comida"], None, {"dependencia_flujo": 0.44, "nivel_precio": 0.48, "clientela_turismo": 0.26, "clientela_vecindario": 0.40, "horario_nocturno": 0.92, "experiencial": 0.80, "sensibilidad_alquiler": 0.48}, {"seguridad_min": 72.0}, None),
    ],
    "salud": [
        ("dental_clinic", "Dental clinic", ["clinica dental", "dentista"], None, {"dependencia_flujo": 0.08, "nivel_precio": 0.62, "clientela_turismo": 0.04, "clientela_vecindario": 0.72, "citas_previas": 0.98, "sensibilidad_alquiler": 0.26}, {"seguridad_min": 58.0}, None),
        ("physio_clinic", "Physiotherapy clinic", ["fisioterapia", "physio"], None, {"dependencia_flujo": 0.12, "nivel_precio": 0.56, "clientela_turismo": 0.04, "clientela_vecindario": 0.76, "citas_previas": 0.96, "experiencial": 0.20, "sensibilidad_alquiler": 0.32}, {"seguridad_min": 58.0}, None),
        ("psychology_center", "Psychology center", ["psicologia", "psychology center"], None, {"dependencia_flujo": 0.06, "nivel_precio": 0.60, "clientela_turismo": 0.02, "clientela_vecindario": 0.70, "citas_previas": 0.98, "experiencial": 0.10, "sensibilidad_alquiler": 0.30}, {"seguridad_min": 60.0}, None),
        ("dermatology_clinic", "Dermatology clinic", ["dermatologia", "skin clinic"], None, {"dependencia_flujo": 0.06, "nivel_precio": 0.70, "clientela_turismo": 0.06, "clientela_vecindario": 0.62, "citas_previas": 0.98, "sensibilidad_alquiler": 0.26}, {"seguridad_min": 60.0}, None),
        ("optician", "Optician store", ["optica", "optician"], None, {"dependencia_flujo": 0.28, "nivel_precio": 0.58, "clientela_turismo": 0.08, "clientela_vecindario": 0.68, "citas_previas": 0.54, "experiencial": 0.12, "sensibilidad_alquiler": 0.40}, {"seguridad_min": 58.0}, None),
        ("pharmacy", "Pharmacy", ["farmacia", "pharmacy"], None, {"dependencia_flujo": 0.52, "nivel_precio": 0.24, "clientela_turismo": 0.06, "clientela_vecindario": 0.92, "citas_previas": 0.02, "sensibilidad_alquiler": 0.64}, {"flujo_min": 450.0}, None),
        ("aesthetic_medicine", "Aesthetic medicine clinic", ["medicina estetica", "aesthetic medicine"], None, {"dependencia_flujo": 0.08, "nivel_precio": 0.82, "clientela_turismo": 0.10, "clientela_vecindario": 0.60, "citas_previas": 0.98, "experiencial": 0.26, "sensibilidad_alquiler": 0.24}, {"seguridad_min": 60.0}, None),
    ],
    "deporte": [
        ("boutique_gym", "Boutique gym", ["fitness boutique", "gimnasio boutique", "fitness_boutique"], None, {"dependencia_flujo": 0.16, "nivel_precio": 0.68, "clientela_turismo": 0.08, "clientela_vecindario": 0.56, "experiencial": 0.60, "citas_previas": 0.58, "sensibilidad_alquiler": 0.58}, {"seguridad_min": 58.0}, None),
        ("yoga_studio", "Yoga studio", ["yoga studio", "estudio de yoga"], None, {"dependencia_flujo": 0.10, "nivel_precio": 0.58, "clientela_turismo": 0.06, "clientela_vecindario": 0.66, "experiencial": 0.62, "citas_previas": 0.62, "sensibilidad_alquiler": 0.46}, {"seguridad_min": 58.0}, None),
        ("pilates_reformer", "Pilates reformer studio", ["reformer pilates", "pilates reformer"], None, {"dependencia_flujo": 0.12, "nivel_precio": 0.74, "clientela_turismo": 0.06, "clientela_vecindario": 0.60, "experiencial": 0.64, "citas_previas": 0.84, "sensibilidad_alquiler": 0.56}, {"seguridad_min": 58.0}, None),
        ("crossfit_box", "CrossFit box", ["crossfit", "crossfit box"], None, {"dependencia_flujo": 0.18, "nivel_precio": 0.46, "clientela_turismo": 0.06, "clientela_vecindario": 0.56, "experiencial": 0.56, "citas_previas": 0.42, "sensibilidad_alquiler": 0.64}, {"seguridad_min": 56.0}, None),
        ("martial_arts", "Martial arts academy", ["artes marciales", "martial arts"], None, {"dependencia_flujo": 0.14, "nivel_precio": 0.42, "clientela_turismo": 0.04, "clientela_vecindario": 0.66, "experiencial": 0.44, "citas_previas": 0.64, "sensibilidad_alquiler": 0.56}, {"seguridad_min": 56.0}, None),
        ("personal_training", "Personal training studio", ["personal training studio"], None, {"dependencia_flujo": 0.08, "nivel_precio": 0.72, "clientela_turismo": 0.04, "clientela_vecindario": 0.56, "experiencial": 0.56, "citas_previas": 0.92, "sensibilidad_alquiler": 0.46}, {"seguridad_min": 58.0}, None),
        ("dance_fitness", "Dance fitness studio", ["zumba studio", "dance fitness"], None, {"dependencia_flujo": 0.18, "nivel_precio": 0.48, "clientela_turismo": 0.06, "clientela_vecindario": 0.62, "experiencial": 0.54, "citas_previas": 0.56, "sensibilidad_alquiler": 0.54}, {"seguridad_min": 56.0}, None),
    ],
    "educacion": [
        ("language_academy", "Language academy", ["academia de idiomas", "language academy"], None, {"dependencia_flujo": 0.10, "nivel_precio": 0.52, "clientela_turismo": 0.10, "clientela_vecindario": 0.68, "experiencial": 0.22, "citas_previas": 0.86, "sensibilidad_alquiler": 0.42}, {"seguridad_min": 56.0}, None),
        ("tutoring_center", "Tutoring center", ["academia de refuerzo", "tutoring center"], None, {"dependencia_flujo": 0.08, "nivel_precio": 0.44, "clientela_turismo": 0.02, "clientela_vecindario": 0.78, "experiencial": 0.14, "citas_previas": 0.82, "sensibilidad_alquiler": 0.46}, {"seguridad_min": 58.0}, None),
        ("nursery_school", "Nursery school", ["guarderia", "nursery school"], None, {"dependencia_flujo": 0.06, "nivel_precio": 0.52, "clientela_turismo": 0.00, "clientela_vecindario": 0.92, "experiencial": 0.30, "citas_previas": 0.96, "sensibilidad_alquiler": 0.44}, {"zonas_verdes_bonus": 0.70, "seguridad_min": 72.0}, None),
        ("music_school", "Music school", ["escuela de musica", "music school"], None, {"dependencia_flujo": 0.08, "nivel_precio": 0.50, "clientela_turismo": 0.04, "clientela_vecindario": 0.66, "experiencial": 0.42, "citas_previas": 0.84, "sensibilidad_alquiler": 0.44}, {"seguridad_min": 56.0}, None),
        ("art_school", "Art school", ["escuela de arte", "art school"], None, {"dependencia_flujo": 0.10, "nivel_precio": 0.52, "clientela_turismo": 0.10, "clientela_vecindario": 0.58, "experiencial": 0.56, "citas_previas": 0.78, "sensibilidad_alquiler": 0.42}, {"seguridad_min": 56.0}, None),
        ("coding_academy", "Coding academy", ["coding academy", "academia programacion"], None, {"dependencia_flujo": 0.06, "nivel_precio": 0.60, "clientela_turismo": 0.06, "clientela_vecindario": 0.52, "experiencial": 0.20, "citas_previas": 0.88, "sensibilidad_alquiler": 0.38}, {"seguridad_min": 56.0}, None),
        ("exam_prep_center", "Exam preparation center", ["preparacion examenes", "exam prep"], None, {"dependencia_flujo": 0.08, "nivel_precio": 0.48, "clientela_turismo": 0.02, "clientela_vecindario": 0.74, "experiencial": 0.16, "citas_previas": 0.84, "sensibilidad_alquiler": 0.42}, {"seguridad_min": 56.0}, None),
    ],
    "alimentacion": [
        ("grocery_store", "Neighborhood grocery store", ["supermercado pequeno", "grocery store"], None, {"dependencia_flujo": 0.62, "nivel_precio": 0.30, "clientela_turismo": 0.06, "clientela_vecindario": 0.94, "sensibilidad_alquiler": 0.74}, {"flujo_min": 350.0}, None),
        ("greengrocer", "Greengrocer", ["fruteria", "greengrocer"], None, {"dependencia_flujo": 0.56, "nivel_precio": 0.28, "clientela_turismo": 0.04, "clientela_vecindario": 0.96, "sensibilidad_alquiler": 0.76}, {"flujo_min": 280.0}, None),
        ("butcher_shop", "Butcher shop", ["carniceria", "butcher"], None, {"dependencia_flujo": 0.48, "nivel_precio": 0.34, "clientela_turismo": 0.02, "clientela_vecindario": 0.96, "sensibilidad_alquiler": 0.70}, {"flujo_min": 220.0}, None),
        ("fishmonger", "Fishmonger", ["pescaderia", "fishmonger"], None, {"dependencia_flujo": 0.44, "nivel_precio": 0.38, "clientela_turismo": 0.04, "clientela_vecindario": 0.94, "sensibilidad_alquiler": 0.68}, {"flujo_min": 220.0}, None),
        ("delicatessen", "Delicatessen", ["delicatessen", "gourmet food shop"], None, {"dependencia_flujo": 0.34, "nivel_precio": 0.72, "clientela_turismo": 0.14, "clientela_vecindario": 0.68, "experiencial": 0.28, "sensibilidad_alquiler": 0.42}, {"ratio_comercial_min": 0.16}, None),
        ("wine_shop", "Wine shop", ["vinoteca", "wine shop"], None, {"dependencia_flujo": 0.26, "nivel_precio": 0.68, "clientela_turismo": 0.12, "clientela_vecindario": 0.72, "experiencial": 0.26, "sensibilidad_alquiler": 0.44}, {"ratio_comercial_min": 0.14}, None),
        ("bakery_takeaway", "Bakery takeaway", ["panaderia takeaway", "bakery takeaway"], None, {"dependencia_flujo": 0.82, "nivel_precio": 0.32, "clientela_turismo": 0.10, "clientela_vecindario": 0.86, "experiencial": 0.12, "sensibilidad_alquiler": 0.72}, {"flujo_min": 550.0}, None),
    ],
    "servicios": [
        ("laundromat", "Laundromat", ["lavanderia self service", "laundromat"], None, {"dependencia_flujo": 0.48, "nivel_precio": 0.18, "clientela_turismo": 0.24, "clientela_vecindario": 0.74, "sensibilidad_alquiler": 0.82}, {"flujo_min": 300.0}, None),
        ("mobile_repair", "Mobile repair shop", ["reparacion moviles", "mobile repair"], None, {"dependencia_flujo": 0.44, "nivel_precio": 0.24, "clientela_turismo": 0.10, "clientela_vecindario": 0.80, "citas_previas": 0.18, "sensibilidad_alquiler": 0.74}, {"flujo_min": 280.0}, None),
        ("locksmith", "Locksmith", ["cerrajeria", "locksmith"], None, {"dependencia_flujo": 0.36, "nivel_precio": 0.28, "clientela_turismo": 0.04, "clientela_vecindario": 0.86, "sensibilidad_alquiler": 0.74}, {"flujo_min": 200.0}, None),
        ("florist", "Florist", ["floristeria", "florist"], None, {"dependencia_flujo": 0.42, "nivel_precio": 0.46, "clientela_turismo": 0.10, "clientela_vecindario": 0.80, "experiencial": 0.26, "sensibilidad_alquiler": 0.62}, {"flujo_min": 240.0}, None),
        ("pet_grooming", "Pet grooming", ["peluqueria canina", "pet grooming"], None, {"dependencia_flujo": 0.18, "nivel_precio": 0.42, "clientela_turismo": 0.02, "clientela_vecindario": 0.92, "citas_previas": 0.70, "sensibilidad_alquiler": 0.58}, {"zonas_verdes_bonus": 0.60, "seguridad_min": 60.0}, None),
        ("copy_shop", "Copy shop", ["copisteria", "copy shop"], None, {"dependencia_flujo": 0.78, "nivel_precio": 0.20, "clientela_turismo": 0.04, "clientela_vecindario": 0.72, "sensibilidad_alquiler": 0.78}, {"flujo_min": 650.0}, None),
        ("coworking_office", "Coworking office", ["coworking", "office coworking"], None, {"dependencia_flujo": 0.10, "nivel_precio": 0.66, "clientela_turismo": 0.14, "clientela_vecindario": 0.42, "experiencial": 0.42, "citas_previas": 0.18, "sensibilidad_alquiler": 0.38}, {"seguridad_min": 58.0}, None),
    ],
    "otro": [
        ("generic_retail", "Generic retail business", ["retail fisico", "negocio fisico", "generic retail"], None, deepcopy(NEUTRAL_PROFILE), {}, None),
        ("generic_destination", "Generic destination venue", ["destination venue", "concept venue"], None, {"dependencia_flujo": 0.18, "nivel_precio": 0.56, "clientela_vecindario": 0.42, "experiencial": 0.52, "citas_previas": 0.44, "sensibilidad_alquiler": 0.38}, {}, None),
        ("generic_service", "Generic neighbourhood service", ["servicio de barrio", "local service"], None, {"dependencia_flujo": 0.34, "nivel_precio": 0.34, "clientela_turismo": 0.06, "clientela_vecindario": 0.82, "citas_previas": 0.30, "sensibilidad_alquiler": 0.68}, {}, None),
        ("generic_showroom", "Generic showroom or studio", ["showroom", "studio local"], None, {"dependencia_flujo": 0.22, "nivel_precio": 0.64, "clientela_vecindario": 0.46, "experiencial": 0.58, "citas_previas": 0.34, "sensibilidad_alquiler": 0.42}, {}, None),
        ("generic_food_counter", "Generic food counter", ["food counter", "barra rapida"], None, {"dependencia_flujo": 0.78, "nivel_precio": 0.34, "clientela_turismo": 0.18, "clientela_vecindario": 0.58, "sensibilidad_alquiler": 0.68}, {}, None),
        ("generic_workshop", "Generic workshop or maker space", ["workshop", "maker space"], None, {"dependencia_flujo": 0.18, "nivel_precio": 0.46, "clientela_vecindario": 0.60, "experiencial": 0.50, "citas_previas": 0.52, "sensibilidad_alquiler": 0.48}, {}, None),
        ("generic_premium_space", "Generic premium destination space", ["premium venue", "premium destination"], None, {"dependencia_flujo": 0.20, "nivel_precio": 0.82, "clientela_turismo": 0.22, "clientela_vecindario": 0.34, "experiencial": 0.68, "citas_previas": 0.42, "sensibilidad_alquiler": 0.24}, {}, None),
    ],
}


def _build_base_catalog() -> dict[str, dict]:
    base_catalog: dict[str, dict] = {}
    for sector, variants in BASE_VARIANTS_BY_SECTOR.items():
        for slug, label, aliases, visible_tag, profile_updates, zone_updates, score_updates in variants:
            entry = _make_base(
                sector,
                slug,
                label,
                aliases=list(aliases),
                visible_tag=visible_tag,
                profile_updates=profile_updates,
                zone_updates=zone_updates,
                score_updates=score_updates,
            )
            base_catalog[entry["id"]] = entry
    return base_catalog


SIMPLE_MODIFIER_LIBRARY = {
    "offer": [
        "artisan", "fresh_daily", "organic", "vegan", "halal", "kosher", "gluten_free",
        "seasonal", "local_sourcing", "sustainable", "limited_edition", "curated",
        "collectible", "technical_products", "giftable", "healthy", "premium_ingredients",
        "prepared_food", "specialty_program", "convenience_assortment",
    ],
    "audience": [
        "locals_first", "tourist_first", "young_adults", "families", "professionals",
        "students", "expats", "pet_owners", "seniors", "creatives", "athletes",
        "gamers", "parents", "children", "office_workers", "luxury_buyers",
        "value_seekers", "commuters", "wellness_seekers", "residents_high_income",
    ],
    "price": [
        "low_cost", "value", "midmarket", "premium", "luxury", "subscription",
        "membership", "high_ticket", "affordable_indulgence", "bundle_friendly",
        "promo_driven", "impulse_spend", "margin_sensitive", "rent_tolerant",
        "everyday_spend", "occasion_spend", "budget_conscious", "upscale_gifting",
    ],
    "channel": [
        "takeaway_delivery", "dine_in", "walk_in", "appointment_based", "click_collect",
        "omnichannel", "reserve_online", "corporate_sales", "subscription_box",
        "marketplace_leads", "delivery_apps", "social_commerce", "b2b", "pickup_window",
        "showroom", "impulse_storefront", "community_events", "fashion_retail",
    ],
    "service": [
        "table_service", "self_service", "assisted_sale", "concierge", "repair_service",
        "customization", "classes_workshops", "consultations", "memberships",
        "loyalty_program", "aftersales", "quick_turnaround", "installation_delivery",
        "home_service", "group_bookings", "private_events", "tasting_service",
        "appointment_journeys",
    ],
    "experience": [
        "experiential", "instagrammable", "dog_friendly", "wellness", "cultural",
        "community", "destination_worthy", "kid_friendly", "premium_ambience",
        "minimalist", "cozy", "social", "immersive", "educational", "nightlife",
        "quiet_focus", "luxury_service", "playful", "design_led", "craft_led",
    ],
    "ops": [
        "high_rotation", "low_staff", "high_staff_service", "storage_heavy",
        "extraction_required", "cold_chain", "quiet_operation", "noisy_operation",
        "franchise_ready", "founder_led", "inventory_heavy", "subscription_revenue",
        "seasonal_peak", "weekend_peak", "late_shift", "early_shift",
        "local_supply_chain", "regulated_handling",
    ],
    "daypart": [
        "breakfast", "brunch", "lunch", "afternoon", "afterwork", "dinner", "night",
        "late_night", "weekday_peak", "weekend_peak", "school_peak", "office_peak",
        "morning_peak", "all_day", "seasonal_evening", "siesta_gap",
        "tourist_peak", "daytime_commercial",
    ],
    "space": [
        "compact_format", "large_format", "terrace", "corner_unit", "natural_light",
        "acoustic_treatment", "storage_backroom", "kitchen_extraction", "waiting_area",
        "private_rooms", "flexible_layout", "showroom_layout", "studio_layout",
        "classroom_layout", "high_ceiling", "street_frontage", "accessibility_ready",
        "parking_nearby",
    ],
    "location": [
        "destination", "neighborhood", "transit", "prime_high_street", "tourist_hotspot",
        "residential", "mixed_use", "near_parks", "near_offices", "near_schools",
        "near_hotels", "near_university", "near_hospital", "near_market",
        "district_anchor", "local_convenience", "premium_axis", "evening_cluster",
    ],
    "constraints": [
        "low_capex", "high_capex", "license_sensitive", "smoke_sensitive",
        "noise_sensitive", "ventilation_sensitive", "water_intensive",
        "refrigeration_intensive", "security_sensitive", "parking_sensitive",
        "terrace_license", "heritage_building_risk", "loading_bay_needed",
        "waste_management_heavy", "staffing_sensitive", "occupancy_sensitive",
        "health_inspection_sensitive", "permit_complexity",
    ],
}

RICH_MODIFIER_OVERRIDES = {
    "offer.vegan": {
        "visible_tag": "vegano_organico",
        "aliases": ["vegano", "organico", "vegano organico", "plant based", "healthy food"],
        "profile": {"nivel_precio": 0.62, "clientela_vecindario": 0.60, "experiencial": 0.34},
        "visible_priority": 0.84,
    },
    "audience.locals_first": {
        "visible_tag": "clientela_local",
        "aliases": ["barrio", "vecindario", "local customers", "clientela local"],
        "profile": {"clientela_turismo": 0.08, "clientela_vecindario": 0.90},
        "visible_priority": 0.92,
    },
    "audience.tourist_first": {
        "visible_tag": "orientado_turismo",
        "aliases": ["turistas", "tourist oriented", "tourism first"],
        "profile": {"clientela_turismo": 0.98, "clientela_vecindario": 0.08, "dependencia_flujo": 0.78},
        "zone_ideal": {"turismo_ideal": 72.0},
        "score_bias": {"score_turismo": 3.0, "score_demografia": 0.25},
        "visible_priority": 0.92,
    },
    "audience.young_adults": {
        "visible_tag": "clientela_joven",
        "aliases": ["joven", "gen z", "millennial"],
        "profile": {"clientela_vecindario": 0.36, "experiencial": 0.46},
    },
    "audience.families": {
        "visible_tag": "clientela_familiar",
        "aliases": ["familias", "family oriented", "kids_activity", "children activity"],
        "profile": {"clientela_vecindario": 0.82, "experiencial": 0.34},
        "zone_ideal": {"seguridad_min": 70.0, "zonas_verdes_bonus": 0.55},
        "visible_priority": 0.86,
    },
    "audience.professionals": {
        "visible_tag": "clientela_profesional",
        "aliases": ["oficinas", "profesionales", "office workers"],
        "profile": {"clientela_vecindario": 0.42, "dependencia_flujo": 0.56},
        "zone_ideal": {"flujo_min": 600.0, "ratio_comercial_min": 0.18},
        "visible_priority": 0.86,
    },
    "price.premium": {
        "visible_tag": "alta_renta",
        "aliases": ["premium", "high end", "alta gama", "gourmet"],
        "profile": {"nivel_precio": 0.82, "sensibilidad_alquiler": 0.18},
        "visible_priority": 0.92,
    },
    "price.low_cost": {
        "visible_tag": "low_cost",
        "aliases": ["economico", "barato", "descuento", "budget", "accesible"],
        "profile": {"nivel_precio": 0.15, "sensibilidad_alquiler": 0.88},
        "visible_priority": 0.92,
    },
    "channel.takeaway_delivery": {
        "visible_tag": "takeaway_delivery",
        "aliases": ["delivery", "takeaway", "para llevar", "grab and go"],
        "profile": {"dependencia_flujo": 0.42, "sensibilidad_alquiler": 0.74},
        "visible_priority": 0.88,
    },
    "service.appointment_journeys": {
        "visible_tag": "salon_citas",
        "aliases": ["cita previa", "appointment only", "con citas", "citas_previas"],
        "profile": {"dependencia_flujo": 0.18, "citas_previas": 1.00},
        "visible_priority": 0.88,
    },
    "experience.experiential": {
        "visible_tag": "experiencial",
        "aliases": ["experiencia", "immersive", "venue is the product"],
        "profile": {"experiencial": 0.90},
        "visible_priority": 0.88,
    },
    "experience.instagrammable": {
        "visible_tag": "instagrammable",
        "aliases": ["instagram", "fotogenico", "aesthetic", "design led"],
        "profile": {"dependencia_flujo": 0.72, "clientela_turismo": 0.52, "experiencial": 0.72},
        "visible_priority": 0.88,
    },
    "experience.dog_friendly": {
        "visible_tag": "dog_friendly",
        "aliases": ["pet friendly", "perros", "mascotas", "dog friendly"],
        "profile": {"dependencia_flujo": 0.34, "clientela_turismo": 0.10, "clientela_vecindario": 0.84, "experiencial": 0.40},
        "zone_ideal": {"zonas_verdes_bonus": 0.80, "seguridad_min": 60.0, "ratio_comercial_min": 0.12},
        "visible_priority": 0.96,
    },
    "experience.wellness": {
        "visible_tag": "health_wellness",
        "aliases": ["wellness", "meditacion", "holistico", "bienestar"],
        "profile": {"dependencia_flujo": 0.10, "nivel_precio": 0.60, "experiencial": 0.62, "citas_previas": 0.95},
        "visible_priority": 0.88,
    },
    "experience.cultural": {
        "visible_tag": "cultural_artistico",
        "aliases": ["cultural", "artistico", "galeria", "arte"],
        "profile": {"experiencial": 0.72, "clientela_turismo": 0.34},
        "visible_priority": 0.86,
    },
    "location.destination": {
        "visible_tag": "destino",
        "aliases": ["destination", "cliente viene expresamente"],
        "profile": {"dependencia_flujo": 0.10, "clientela_vecindario": 0.36},
        "zone_ideal": {"flujo_min": 120.0, "ratio_comercial_min": 0.12},
        "visible_priority": 0.90,
    },
    "channel.fashion_retail": {
        "visible_tag": "retail_moda",
        "aliases": ["fashion retail", "tienda de moda", "ropa"],
        "profile": {"dependencia_flujo": 0.80, "experiencial": 0.28},
        "zone_ideal": {"ratio_comercial_min": 0.26},
        "visible_priority": 0.90,
    },
    "daypart.night": {
        "visible_tag": "horario_nocturno",
        "aliases": ["nocturno", "nightlife", "tardeo", "late night"],
        "profile": {"horario_nocturno": 0.92},
        "zone_ideal": {"seguridad_min": 72.0},
        "visible_priority": 0.88,
    },
    "daypart.daytime_commercial": {
        "visible_tag": "horario_diurno_comercial",
        "aliases": ["horario comercial", "daytime only", "business hours"],
        "profile": {"horario_nocturno": 0.02},
        "visible_priority": 0.82,
    },
    "ops.high_rotation": {
        "visible_tag": "alta_rotacion_clientes",
        "aliases": ["alta rotacion", "fast turnover", "high throughput"],
        "profile": {"dependencia_flujo": 0.95},
        "visible_priority": 0.88,
    },
    "space.large_format": {
        "visible_tag": "local_grande",
        "aliases": ["local grande", "amplio", "large format"],
        "profile": {"sensibilidad_alquiler": 0.78},
        "visible_priority": 0.82,
    },
    "space.compact_format": {
        "visible_tag": "local_pequeno",
        "aliases": ["local pequeno", "local pequeño", "compacto", "small format"],
        "profile": {"sensibilidad_alquiler": 0.32},
        "visible_priority": 0.82,
    },
}


def _build_modifier_catalog() -> dict[str, dict]:
    modifiers: dict[str, dict] = {}
    for namespace, slugs in SIMPLE_MODIFIER_LIBRARY.items():
        for slug in slugs:
            canonical_id = f"{namespace}.{slug}"
            override = RICH_MODIFIER_OVERRIDES.get(canonical_id, {})
            entry = _make_modifier(
                namespace,
                slug,
                label=override.get("label"),
                aliases=override.get("aliases"),
                visible_tag=override.get("visible_tag"),
                profile=override.get("profile"),
                zone_ideal=override.get("zone_ideal"),
                score_bias=override.get("score_bias"),
                visible_priority=override.get("visible_priority", 0.55),
            )
            modifiers[entry["id"]] = entry
    return modifiers


BASE_CONCEPTS = _build_base_catalog()
MODIFIER_TAGS = _build_modifier_catalog()
CATALOG = {**BASE_CONCEPTS, **MODIFIER_TAGS}

SECTOR_FALLBACK_BASE = {
    "restauracion": "base.restauracion.neighborhood_cafe",
    "moda": "base.moda.premium_boutique",
    "estetica": "base.estetica.hair_salon",
    "tatuajes": "base.tatuajes.tattoo_studio",
    "shisha_lounge": "base.shisha_lounge.classic_lounge",
    "salud": "base.salud.dental_clinic",
    "deporte": "base.deporte.boutique_gym",
    "educacion": "base.educacion.language_academy",
    "alimentacion": "base.alimentacion.grocery_store",
    "servicios": "base.servicios.copy_shop",
    "otro": "base.otro.generic_retail",
    "desconocido": "base.otro.generic_retail",
}

ALIASES_TO_CANONICAL: dict[str, str] = {}
VISIBLE_TO_CANONICAL: dict[str, str] = {}
VISIBLE_TAGS: set[str] = set()
for canonical_id, entry in CATALOG.items():
    for alias in entry["aliases"]:
        ALIASES_TO_CANONICAL[alias] = canonical_id
    visible_tag = entry.get("visible_tag")
    if visible_tag:
        VISIBLE_TAGS.add(visible_tag)
        VISIBLE_TO_CANONICAL[_norm(visible_tag)] = canonical_id
        ALIASES_TO_CANONICAL[_norm(visible_tag)] = canonical_id


def lookup_canonical_tag(value: str | None) -> str | None:
    if not value:
        return None
    normal = _norm(value)
    if not normal:
        return None
    return ALIASES_TO_CANONICAL.get(normal)


def _extract_candidates_from_text(text: str | None) -> list[dict]:
    if not text:
        return []

    normalized_text = f" {_norm(text)} "
    found: list[dict] = []
    for alias, canonical_id in sorted(ALIASES_TO_CANONICAL.items(), key=lambda item: (-len(item[0]), item[0])):
        if len(alias) < 4:
            continue
        if f" {alias} " in normalized_text:
            found.append({"id": canonical_id, "weight": min(1.0, 0.55 + len(alias) / 30.0), "source": "text"})
    return found


def idea_tags_visibles_desde_texto(text: str | None, max_tags: int = MAX_VISIBLE_TAGS) -> list[str]:
    compiled = compilar_concepto_negocio(descripcion=text or "", max_visible_tags=max_tags)
    return compiled.get("idea_tags", [])


def _normalize_weight(raw_weight, default: float = 1.0) -> float:
    try:
        value = float(raw_weight)
    except (TypeError, ValueError):
        value = default
    return max(0.05, min(1.0, value))


def _iter_candidates(items, default_weight: float, source: str) -> list[dict]:
    result: list[dict] = []
    for item in items or []:
        if isinstance(item, str):
            canonical = lookup_canonical_tag(item)
            if canonical:
                result.append({"id": canonical, "weight": default_weight, "source": source, "raw": item})
            continue
        if not isinstance(item, dict):
            continue
        raw_id = item.get("id") or item.get("tag") or item.get("slug") or item.get("nombre")
        canonical = lookup_canonical_tag(raw_id)
        if canonical:
            result.append({
                "id": canonical,
                "weight": _normalize_weight(item.get("weight") or item.get("score") or item.get("similarity"), default_weight),
                "source": source,
                "raw": raw_id,
            })
    return result


def _iter_matcher_candidates(matches: list[dict] | None) -> list[dict]:
    candidates: list[dict] = []
    for match in matches or []:
        sim = _normalize_weight(match.get("similarity"), 0.6)
        key = match.get("key")
        canonical = lookup_canonical_tag(key)
        if canonical:
            candidates.append({"id": canonical, "weight": sim, "source": "matcher_key", "raw": key})
        for tag in match.get("tags") or []:
            canonical = lookup_canonical_tag(tag)
            if canonical:
                candidates.append({"id": canonical, "weight": sim * 0.85, "source": "matcher_tag", "raw": tag})
    return candidates


def _dedupe_weighted_candidates(candidates: list[dict]) -> list[dict]:
    by_id: dict[str, dict] = {}
    for candidate in candidates:
        entry = CATALOG.get(candidate["id"])
        if not entry:
            continue
        current = by_id.get(candidate["id"])
        if current is None:
            by_id[candidate["id"]] = candidate
            continue
        current["weight"] = max(current["weight"], candidate["weight"])
        current["source"] = f"{current['source']}+{candidate['source']}"
    return sorted(by_id.values(), key=lambda item: (-item["weight"], item["id"]))


def _normalize_ranked_candidates(candidates: list[dict], *, kind: str, limit: int, sector: str | None) -> list[dict]:
    filtered = [candidate for candidate in candidates if CATALOG.get(candidate["id"], {}).get("kind") == kind]
    filtered = _dedupe_weighted_candidates(filtered)
    if kind == "base":
        filtered = [candidate for candidate in filtered if (CATALOG[candidate["id"]].get("sector") in (sector, None) or sector in (None, "desconocido", "otro"))]
        if not filtered:
            fallback_id = SECTOR_FALLBACK_BASE.get(sector or "desconocido", SECTOR_FALLBACK_BASE["desconocido"])
            filtered = [{"id": fallback_id, "weight": 1.0, "source": "sector_fallback", "raw": fallback_id}]
        filtered = filtered[:limit]
        total = sum(candidate["weight"] for candidate in filtered) or 1.0
        return [{**candidate, "weight": round(candidate["weight"] / total, 3)} for candidate in filtered]

    by_namespace: dict[str, int] = {}
    kept: list[dict] = []
    for candidate in filtered:
        namespace = CATALOG[candidate["id"]]["namespace"]
        if by_namespace.get(namespace, 0) >= 3:
            continue
        by_namespace[namespace] = by_namespace.get(namespace, 0) + 1
        kept.append(candidate)
        if len(kept) >= limit:
            break
    return [{**candidate, "weight": round(candidate["weight"], 3)} for candidate in kept]


def _blend_profile(sector: str, base_concepts: list[dict], modifiers: list[dict], perfil_hint: dict | None) -> dict[str, float]:
    contributions: dict[str, list[tuple[float, float]]] = {key: [] for key in PROFILE_KEYS}
    sector_profile = _sector_profile(sector)
    sector_weight = 0.15 if sector in ("desconocido", "otro") else 0.55

    for key, value in sector_profile.items():
        contributions[key].append((float(value), sector_weight))

    for candidate in base_concepts:
        entry = CATALOG[candidate["id"]]
        weight = (0.40 if candidate.get("source") == "sector_fallback" else 1.35) * float(candidate["weight"])
        for key, value in entry["profile"].items():
            contributions[key].append((float(value), weight))

    for candidate in modifiers:
        entry = CATALOG[candidate["id"]]
        weight = 1.90 * float(candidate["weight"])
        for key in PROFILE_KEYS:
            value = entry["profile"].get(key)
            if value is not None and value != NEUTRAL_PROFILE[key]:
                contributions[key].append((float(value), weight))

    result = {}
    for key, pairs in contributions.items():
        total_weight = sum(weight for _, weight in pairs) or 1.0
        result[key] = round(sum(value * weight for value, weight in pairs) / total_weight, 3)

    for key, value in (perfil_hint or {}).items():
        if key in PROFILE_KEYS and value is not None:
            result[key] = round(_clip01(value, result[key]), 3)

    return _round_profile(result)


def _blend_zone(perfil_negocio: dict[str, float], base_concepts: list[dict], modifiers: list[dict]) -> dict:
    zone = zona_ideal_desde_perfil(perfil_negocio)
    buckets: dict[str, list[tuple[float, float]]] = {}

    for candidate in [*base_concepts, *modifiers]:
        entry = CATALOG[candidate["id"]]
        base_factor = 0.30 if candidate.get("source") == "sector_fallback" else 1.00
        weight = float(candidate["weight"]) * (base_factor if entry["kind"] == "base" else 0.55)
        for key, value in (entry.get("zone_ideal") or {}).items():
            if isinstance(value, tuple):
                buckets.setdefault(f"{key}_min", []).append((float(value[0]), weight))
                buckets.setdefault(f"{key}_max", []).append((float(value[1]), weight))
            elif isinstance(value, (float, int)):
                buckets.setdefault(key, []).append((float(value), weight))

    for key, pairs in buckets.items():
        total_weight = sum(weight for _, weight in pairs) or 1.0
        zone[key] = sum(value * weight for value, weight in pairs) / total_weight

    if "edad_rango_min" in zone and "edad_rango_max" in zone:
        zone["edad_rango"] = (round(zone.pop("edad_rango_min"), 1), round(zone.pop("edad_rango_max"), 1))

    if "renta_ideal" in zone:
        zone["renta_ideal"] = round(_clip(zone["renta_ideal"], 0.0, 1.0, 0.5), 3)
    if "turismo_ideal" in zone:
        zone["turismo_ideal"] = round(_clip(zone["turismo_ideal"], 0.0, 100.0, 45.0), 1)
    if "flujo_min" in zone:
        zone["flujo_min"] = round(_clip(zone["flujo_min"], 0.0, 3000.0, 250.0), 1)
    if "ratio_comercial_min" in zone:
        zone["ratio_comercial_min"] = round(_clip(zone["ratio_comercial_min"], 0.05, 0.80, 0.18), 3)
    if "seguridad_min" in zone:
        zone["seguridad_min"] = round(_clip(zone["seguridad_min"], 30.0, 95.0, 55.0), 1)
    if "zonas_verdes_bonus" in zone:
        zone["zonas_verdes_bonus"] = round(_clip(zone["zonas_verdes_bonus"], 0.0, 1.0, 0.0), 3)

    return zone


def _blend_score_bias(perfil_negocio: dict[str, float], base_concepts: list[dict], modifiers: list[dict]) -> dict[str, dict]:
    multipliers = score_bias_desde_perfil(perfil_negocio)
    for candidate in [*base_concepts, *modifiers]:
        entry = CATALOG[candidate["id"]]
        base_factor = 0.25 if candidate.get("source") == "sector_fallback" else 0.90
        factor = float(candidate["weight"]) * (base_factor if entry["kind"] == "base" else 0.45)
        for dim, custom in (entry.get("score_bias") or {}).items():
            current = multipliers.get(dim, 1.0)
            multipliers[dim] = round(current * math.pow(float(custom), factor), 3)
    multipliers = {dim: round(max(0.35, min(2.60, value)), 3) for dim, value in multipliers.items()}
    return {"multipliers": multipliers, "source": "compiled_from_taxonomy"}


def _visible_tags_from_concepts(base_concepts: list[dict], modifiers: list[dict], max_visible_tags: int) -> list[str]:
    ranked: list[tuple[float, str]] = []
    for idx, candidate in enumerate(base_concepts):
        entry = CATALOG[candidate["id"]]
        visible = entry.get("visible_tag")
        if visible:
            score = 3.0 - idx + entry.get("visible_priority", 0.5)
            ranked.append((score, visible))
        else:
            ranked.append((2.2 - idx, candidate["id"].split(".")[-1]))

    for candidate in modifiers:
        entry = CATALOG[candidate["id"]]
        visible = entry.get("visible_tag")
        if visible:
            score = float(candidate["weight"]) + float(entry.get("visible_priority", 0.5))
            ranked.append((score, visible))

    seen: set[str] = set()
    result: list[str] = []
    for _, tag in sorted(ranked, key=lambda item: (-item[0], item[1])):
        if tag in seen:
            continue
        seen.add(tag)
        result.append(tag)
        if len(result) >= max_visible_tags:
            break
    return result


def compilar_concepto_negocio(
    *,
    sector: str | None = None,
    base_concepts: list | None = None,
    modifiers: list | None = None,
    idea_tags: list[str] | None = None,
    perfil_hint: dict | None = None,
    descripcion: str | None = None,
    matcher_matches: list[dict] | None = None,
    confidence: float | int | str | None = None,
    ambiguities: list[str] | None = None,
    justificacion_breve: str | None = None,
    max_visible_tags: int = MAX_VISIBLE_TAGS,
) -> dict:
    normalized_sector = sector or "desconocido"
    if normalized_sector not in SECTOR_PROFILE_DEFAULTS:
        normalized_sector = "otro" if normalized_sector and normalized_sector != "desconocido" else "desconocido"

    candidates: list[dict] = []
    candidates.extend(_iter_candidates(base_concepts, 0.9, "llm_base"))
    candidates.extend(_iter_candidates(modifiers, 0.7, "llm_modifier"))
    candidates.extend(_iter_candidates(idea_tags, 0.65, "legacy_tag"))
    candidates.extend(_iter_matcher_candidates(matcher_matches))
    candidates.extend(_extract_candidates_from_text(descripcion))

    normalized_bases = _normalize_ranked_candidates(candidates, kind="base", limit=MAX_BASE_CONCEPTS, sector=normalized_sector)
    normalized_modifiers = _normalize_ranked_candidates(candidates, kind="modifier", limit=MAX_MODIFIERS, sector=normalized_sector)

    perfil_negocio = _blend_profile(normalized_sector, normalized_bases, normalized_modifiers, perfil_hint)
    zona_ideal = _blend_zone(perfil_negocio, normalized_bases, normalized_modifiers)
    pesos_scoring = _blend_score_bias(perfil_negocio, normalized_bases, normalized_modifiers)
    idea_tags_visibles = _visible_tags_from_concepts(normalized_bases, normalized_modifiers, max_visible_tags=max_visible_tags)

    tags_internos = [candidate["id"] for candidate in normalized_bases] + [candidate["id"] for candidate in normalized_modifiers]
    confidence_value = _clip(confidence, 0.0, 1.0, 0.72 if normalized_bases else 0.40)
    if ambiguities:
        confidence_value = max(0.20, round(confidence_value - min(0.20, 0.03 * len(ambiguities)), 3))

    return {
        "sector": normalized_sector,
        "base_concepts": [{"id": candidate["id"], "weight": candidate["weight"], "label": CATALOG[candidate["id"]]["label"], "source": candidate["source"]} for candidate in normalized_bases],
        "modifiers": [{"id": candidate["id"], "weight": candidate["weight"], "label": CATALOG[candidate["id"]]["label"], "source": candidate["source"]} for candidate in normalized_modifiers],
        "tags_internos": tags_internos,
        "idea_tags": idea_tags_visibles,
        "perfil_negocio": perfil_negocio,
        "zona_ideal": zona_ideal,
        "pesos_scoring": pesos_scoring,
        "confidence_global": round(confidence_value, 3),
        "ambiguities": ambiguities or [],
        "justificacion_breve": justificacion_breve,
        "version": CATALOG_VERSION,
    }


def perfil_desde_tags_visibles(idea_tags: list[str] | None, sector: str | None = None) -> dict[str, float]:
    compiled = compilar_concepto_negocio(sector=sector, idea_tags=idea_tags or [])
    return compiled["perfil_negocio"]


def canonical_tag_count() -> int:
    return len(CATALOG)


__all__ = [
    "ALIASES_TO_CANONICAL",
    "BASE_CONCEPTS",
    "CATALOG",
    "CATALOG_VERSION",
    "DEFAULT_SECTOR_WEIGHTS",
    "MAX_VISIBLE_TAGS",
    "MODIFIER_TAGS",
    "PROFILE_KEYS",
    "SECTOR_PROFILE_DEFAULTS",
    "SCORE_DIMENSIONS",
    "VISIBLE_TAGS",
    "WEIGHT_FIELD_BY_DIM",
    "aplicar_pesos_a_sector",
    "canonical_tag_count",
    "compilar_concepto_negocio",
    "idea_tags_visibles_desde_texto",
    "lookup_canonical_tag",
    "perfil_desde_tags_visibles",
    "score_bias_desde_perfil",
    "zona_ideal_desde_perfil",
]
