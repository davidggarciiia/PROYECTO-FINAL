"""
agente/prompts/refinamiento.py — Prompts del agente refinador.

Se mantiene en inglés para minimizar tokens (como el resto de prompts).
El LLM devuelve JSON estructurado y el backend no traduce campos — solo se
traducen textos libres que vayan a mostrarse al usuario (cuestionario).
"""

# Prompt del refinador: extrae un perfil estructurado rico del texto del usuario.
# El objetivo NO es clasificar otra vez el sector (eso ya lo hace validador.py),
# sino preservar TODO el matiz que se pierde al colapsar a 8 dims numéricas.
#
# IMPORTANTE: `nuances_detected` es VOCABULARIO ABIERTO — no hay whitelist.
# Así mantenemos "instagrammable", "brunch", "millennials", "de diseño", etc.
REFINADOR_SISTEMA = """You are a business-profile refinement agent for commercial real estate in Barcelona.

Your job is to extract a rich structured profile from a free-text business description WITHOUT losing nuance. A separate agent already classified the sector and some tags; you do NOT need to re-classify. Focus on preserving subtle details (audience, vibe, price tier, daypart, location hints) that a generic sector label loses.

ABSOLUTE RULES:
1. Respond with valid JSON matching the exact schema below.
2. All free-text fields are OPEN VOCABULARY — use the user's own words when possible.
3. `nuances_detected` is the most important field: list the specific qualifiers that make this idea unique (e.g. "instagrammable", "brunch culture", "specialty coffee", "millennials", "zona joven", "post-gym smoothies"). Use the user's own words.
4. Score `signal_preservation_score` HONESTLY: 0 = you understood almost nothing; 100 = the structured profile captures everything. Bias LOW when the description is short, vague, or has ambiguities you could not resolve.
5. Use `null` (not empty strings) when a field cannot be inferred.
6. Horarios: use short slugs: "mañana", "brunch", "tarde", "noche", "madrugada".
7. ticket_tier_p1_p5: 1=very cheap/low-cost, 3=mid-market, 5=premium/luxury.

JSON RESPONSE STRUCTURE:
{
  "publico_objetivo": {
    "edad_rango":           "25-35" | null,
    "nivel_socioeconomico": "bajo" | "medio" | "medio-alto" | "alto" | null,
    "estilo_vida":          ["keyword1", "keyword2"],
    "horarios_pico":        ["mañana", "brunch", "tarde", "noche"]
  },
  "propuesta_valor": {
    "especializacion":       "short phrase or null",
    "diferenciadores":       ["differentiator1", "differentiator2"],
    "calidad_percibida_0_5": 0.0_to_5.0 or null
  },
  "operacion": {
    "modelo_servicio":    "take_away" | "mesas" | "mixto" | "delivery_only" | null,
    "ticket_tier_p1_p5":  1_to_5 or null,
    "escala_operativa":   "solo" | "micro" | "pequeña" | "mediana" | null,
    "horarios_apertura":  ["mañana", "tarde", "noche"]
  },
  "ubicacion_ideal": {
    "tipo_calle":          "short label or null",
    "densidad_preferida":  "baja" | "media" | "alta" | null,
    "flujo_tipo":          "premium residencial" | "turistico" | "paso rapido" | "barrio joven" | ... or null
  },
  "nuances_detected":         ["nuance1", "nuance2", "nuance3"],
  "signal_preservation_score": 0_to_100
}

Examples of nuances_detected (keep the user's language):
- "instagrammable", "brunch culture", "millennials", "specialty coffee", "zona joven"
- "vermuteria", "pintxos bar", "coworking friendly", "post-gym smoothies"
- "luxury appointment only", "destination restaurant", "dog-friendly terrace"
"""


def build_refinador_user_message(
    descripcion: str,
    sector_detectado: str,
    tags_previos: list[str],
) -> str:
    """Mensaje de usuario que se envía al LLM. Entrega el contexto mínimo:
    texto original + lo que ya detectó el validador (sector + tags).
    El LLM NO debe re-clasificar; debe profundizar en los matices.
    """
    tags_joined = ", ".join(tags_previos) if tags_previos else "none"
    return (
        "<business_description>\n"
        f"{descripcion.strip()[:1500]}\n"
        "</business_description>\n"
        "<known_classification>\n"
        f"sector: {sector_detectado}\n"
        f"tags: {tags_joined}\n"
        "</known_classification>\n"
        "Produce the refined profile now."
    )


# Prompt para generar preguntas adaptativas cuando signal_preservation_score < 70.
# Referencia explícitamente los `nuances_detected` con gaps para que la pregunta
# se perciba como coherente con lo que el usuario ya dijo.
PREGUNTA_SENAL_SISTEMA = """You are an assistant helping refine a Barcelona retail business description.

The refiner extracted a structured profile but flagged low signal preservation (ambiguous or incomplete). Generate 1 short follow-up question (max 2 sentences) that references a specific nuance the user mentioned and asks the user to clarify a concrete gap in the structured profile.

RULES:
- ONE question per response, in English (will be translated later).
- Reference at least one nuance from `nuances_detected` so the user feels understood.
- Target the gap with the biggest impact on scoring (ticket tier > daypart > audience socio-economic level > location type).
- Never ask for data already present.

JSON:
{
  "pregunta": "short natural question",
  "gap_objetivo": "field_name you are trying to fill"
}
"""
