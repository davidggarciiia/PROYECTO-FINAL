"""agente/validador.py - Validacion del tipo de negocio con LLM."""
from __future__ import annotations

import json
import logging
import re

from agente import extraer_json
from agente.prompts import VALIDACION_SISTEMA
from agente.traductor import traducir
from routers.llm_router import completar
from scoring.clasificador_libre import clasificar_descripcion_libre
from scoring.concepto.taxonomy import compilar_concepto_negocio, lookup_canonical_tag

logger = logging.getLogger(__name__)

# Modelo operativo por subsector — completa el override del LLM
_SUBSECTOR_OPERATING_MODEL: dict[str, str] = {
    "barber_shop":        "appointment_based",
    "hair_salon":         "appointment_based",
    "nail_studio":        "appointment_based",
    "day_spa":            "appointment_based",
    "beauty_clinic":      "appointment_based",
    "tattoo_studio":      "appointment_based",
    "fine_line_studio":   "appointment_based",
    "street_tattoo":      "hybrid",
    "dental_clinic":      "appointment_based",
    "physio_clinic":      "appointment_based",
    "psychology_center":  "appointment_based",
    "pet_grooming":       "appointment_based",
    "coworking_office":   "hybrid",
    "specialty_coffee":   "restaurant",
    "neighborhood_cafe":  "restaurant",
    "express_cafe":       "restaurant",
    "tapas_bar":          "restaurant",
    "cocktail_bar":       "hybrid",
    "street_food_counter":"restaurant",
    "vegan_bistro":       "restaurant",
    "brunch_house":       "restaurant",
    "fine_dining":        "restaurant",
    "grocery_store":      "retail_walkin",
    "boutique_gym":       "hybrid",
    "yoga_pilates":       "appointment_based",
    "language_academy":   "appointment_based",
}

# Patrones de multi-intención: el negocio combina dos modelos distintos
_MULTI_INTENT_RE = re.compile(
    r"""
    \b(?:
        (?:bar|café|cafetería|restaurante|barbería|peluquería|clínica|gym|academia)\s+
        (?:y|con)\s+
        (?:tienda|venta\s+de|sala|taller|academia|spa)
      |
        (?:tienda|venta)\s+(?:y|con)\s+(?:taller|café|bar|restaurante)
      |
        (?:y\s+también|además\s+de)\s+(?:vend|alquil|ofrec)\w+
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)


def _normalizar_tags_visibles(tags: list | None) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []

    for tag in tags or []:
        if not isinstance(tag, str):
            continue
        clean = tag.strip()
        if not clean or clean in seen:
            continue
        if lookup_canonical_tag(clean) is None:
            continue
        seen.add(clean)
        result.append(clean)

    return result


async def validar_negocio(descripcion: str, session_id: str) -> dict:
    """Analiza la descripcion y devuelve una clasificacion normalizada."""
    descripcion_safe = descripcion[:1000]

    # ── Layer 1: clasificador determinista (<1ms, sin LLM) ────────────────────
    preclasif = clasificar_descripcion_libre(descripcion_safe)
    hint = preclasif.as_hint()  # None si confidence < 0.40

    # Multi-intención: negocios mixtos (barbería + tienda, bar + academia...)
    multi_intent = bool(_MULTI_INTENT_RE.search(descripcion_safe))

    # ── Construir mensaje usuario ─────────────────────────────────────────────
    user_content = f"<business_description>\n{descripcion_safe}\n</business_description>"
    if hint:
        user_content += f"\n<pre_classification>{hint}</pre_classification>"
    if multi_intent:
        user_content += "\n<business_model>mixed — multiple revenue streams detected</business_model>"

    # ── Layer 2: LLM ─────────────────────────────────────────────────────────
    respuesta = await completar(
        mensajes=[{"role": "user", "content": user_content}],
        sistema=VALIDACION_SISTEMA,
        endpoint="validacion",
        session_id=session_id,
        max_tokens=800,
        temperature=0.1,
        requiere_json=True,
    )

    try:
        parsed = json.loads(extraer_json(respuesta))
    except json.JSONDecodeError as exc:
        logger.error("JSON invalido en validar_negocio: %s | respuesta: %s", exc, respuesta[:200])
        concepto_fallback = compilar_concepto_negocio(sector="desconocido", descripcion=descripcion_safe)
        return {
            "es_retail": True,
            "inviable_legal": False,
            "motivo_legal": None,
            "motivo": None,
            "informacion_suficiente": False,
            "sector_detectado": "desconocido",
            "idea_tags": concepto_fallback["idea_tags"],
            "perfil_negocio": concepto_fallback["perfil_negocio"],
            "concepto_negocio": concepto_fallback,
            "variables_conocidas": {},
            "preguntas_necesarias": ["Cuanto puedes pagar de alquiler al mes?"],
            "classification_source": "llm_json_error",
        }

    # ── Override completo cuando el pre-clasificador es muy seguro (≥0.90) ───
    llm_sector_original = parsed.get("sector")
    source: str

    if preclasif.confidence >= 0.90:
        if llm_sector_original != preclasif.sector:
            logger.warning(
                "[CLASSIFICATION] override: LLM sector=%r → pre-classifier sector=%r "
                "(conf=%.2f, keyword=%s)",
                llm_sector_original, preclasif.sector, preclasif.confidence, preclasif.keywords,
            )
            source = "override"
        else:
            source = "preclassifier_strong"
        # Forzar sector + base_concept + modelo operativo
        parsed["sector"] = preclasif.sector
        if preclasif.subsector:
            parsed["base_concepts"] = [{
                "id":     f"base.{preclasif.sector}.{preclasif.subsector}",
                "weight": 0.95,
            }]
        op_model = _SUBSECTOR_OPERATING_MODEL.get(preclasif.subsector or "")
        if op_model:
            parsed["operating_model_hint"] = op_model

    elif preclasif.confidence >= 0.40:
        source = "preclassifier_weak"
    else:
        source = "llm"
        if preclasif.confidence == 0.0 and parsed.get("sector") and parsed.get("sector") != "otro":
            logger.warning(
                "[CLASSIFICATION] confidence=0 pero LLM devolvió sector=%r — revisar reglas pre-clasificador",
                parsed.get("sector"),
            )

    logger.info(
        "[CLASSIFICATION] source=%s sector=%s subsector=%s confidence=%.2f multi_intent=%s",
        source,
        parsed.get("sector", "?"),
        (parsed.get("base_concepts") or [{}])[0].get("id", "?"),
        preclasif.confidence,
        multi_intent,
    )

    # ── Procesar respuesta LLM ────────────────────────────────────────────────
    estado = parsed.get("estado", "cuestionario")
    motivo_en = parsed.get("motivo_rechazo")
    motivo_es = await traducir(motivo_en, session_id) if motivo_en else None

    tags_visibles_llm = _normalizar_tags_visibles(parsed.get("idea_tags"))

    concepto_negocio = compilar_concepto_negocio(
        sector=parsed.get("sector"),
        base_concepts=parsed.get("base_concepts"),
        modifiers=parsed.get("modifiers"),
        idea_tags=tags_visibles_llm,
        perfil_hint=parsed.get("perfil_numerico") if isinstance(parsed.get("perfil_numerico"), dict) else None,
        descripcion=descripcion_safe,
        confidence=parsed.get("confidence"),
        ambiguities=parsed.get("ambiguities") or [],
        justificacion_breve=parsed.get("justificacion_breve"),
    )

    return {
        "es_retail":              parsed.get("es_retail", True) and estado != "error_tipo_negocio",
        "inviable_legal":         estado == "inviable_legal",
        "motivo_legal":           motivo_es if estado == "inviable_legal" else None,
        "motivo":                 motivo_es if estado == "error_tipo_negocio" else None,
        "informacion_suficiente": parsed.get("info_suficiente", False),
        "sector_detectado":       concepto_negocio.get("sector") or parsed.get("sector") or "desconocido",
        "idea_tags":              tags_visibles_llm or concepto_negocio.get("idea_tags") or [],
        "perfil_negocio":         concepto_negocio.get("perfil_negocio") or {},
        "concepto_negocio":       concepto_negocio,
        "variables_conocidas":    parsed.get("variables_extraidas") or {},
        "preguntas_necesarias":   parsed.get("preguntas_pendientes") or [],
        "classification_source":  source,
        "multi_intent":           multi_intent,
        "operating_model_hint":   parsed.get("operating_model_hint"),
    }

