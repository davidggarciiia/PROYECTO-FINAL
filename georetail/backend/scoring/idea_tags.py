"""
scoring/idea_tags.py — Especificidad por tipo de negocio.

El LLM (agente/validador.py) detecta el sector genérico ('restauracion', 'moda'…).
Este módulo va un paso más allá: a partir de tags extraídos de la descripción del
negocio, ajusta los pesos de cada dimensión del score para reflejar las
características particulares de la idea concreta.

Ejemplo:
  - Un bar de cócteles frente al mar tiene perfil muy distinto a una cafetería
    de barrio, aunque ambos sean sector 'restauracion'.
  - Una tienda vintage de segunda mano no compite igual que una boutique premium,
    aunque ambas sean 'moda'.

Uso:
    from scoring.idea_tags import aplicar_idea_tags, extraer_tags_de_descripcion

    tags = extraer_tags_de_descripcion(descripcion_llm)  # o pasarlos desde el agente
    pesos_ajustados = aplicar_idea_tags(pesos_sector, tags)
    score_global = _recalcular_global(scores_dim, pesos_ajustados)
"""
from __future__ import annotations
import copy
import logging
from typing import Optional

logger = logging.getLogger(__name__)


# ─── Definición de tags y sus modificadores de peso ───────────────────────────
#
# Estructura: tag → {dimension: multiplicador}
# Los multiplicadores se aplican sobre los pesos base del sector (tabla `sectores`).
# Un multiplicador > 1 aumenta la importancia de esa dimensión para esta idea.
# Un multiplicador < 1 la reduce.
# Después de aplicar todos los tags, los pesos se renormalizan a suma = 1.0.
#
# Cada tag incluye también una clave "_descripcion" para documentación.
# ──────────────────────────────────────────────────────────────────────────────

MODIFICADORES_IDEA: dict[str, dict] = {
    # ── Orientación turística ──────────────────────────────────────────────────
    "orientado_turismo": {
        "_descripcion": "El negocio vive principalmente de turistas (souvenir, bar de playa, etc.)",
        "score_turismo":          3.0,
        "score_flujo_peatonal":   1.5,
        "score_demografia":       0.5,  # menos dependiente del vecindario
    },
    "clientela_local": {
        "_descripcion": "El negocio sirve al vecindario (barbería de barrio, supermercado, etc.)",
        "score_turismo":          0.3,
        "score_demografia":       1.8,
        "score_entorno_comercial":1.3,
    },

    # ── Segmento de renta objetivo ─────────────────────────────────────────────
    "alta_renta": {
        "_descripcion": "Boutique, restaurante gourmet, spa premium, etc.",
        "score_demografia":       2.0,  # renta media importa mucho
        "score_precio_alquiler":  0.6,  # menos sensible al precio del local
        "score_competencia":      0.8,
    },
    "low_cost": {
        "_descripcion": "Comercio de descuento, kebab, mercería, etc.",
        "score_precio_alquiler":  2.0,
        "score_demografia":       0.7,
        "score_flujo_peatonal":   1.4,  # necesita más volumen de paso
    },

    # ── Horario de operación ───────────────────────────────────────────────────
    "horario_nocturno": {
        "_descripcion": "Bar de copas, discoteca, gastrobar de tardear, etc.",
        "score_seguridad":        1.8,  # la seguridad nocturna importa más
        "score_turismo":          1.4,
        "score_flujo_peatonal":   0.8,  # el flujo diurno es menos relevante
    },
    "horario_diurno_comercial": {
        "_descripcion": "Tienda, oficina, clínica — solo horario comercial.",
        "score_transporte":       1.5,  # accesibilidad en hora punta importa
        "score_seguridad":        0.7,
    },

    # ── Clientela objetivo ─────────────────────────────────────────────────────
    "clientela_joven": {
        "_descripcion": "Bubble tea, tatuajes, moda urbana, estudio de yoga millennial.",
        "score_turismo":          1.2,
        "score_entorno_comercial":1.4,
        "score_demografia":       0.8,  # la renta media del barrio importa menos
    },
    "clientela_familiar": {
        "_descripcion": "Ludoteca, restaurante familiar, escuela de idiomas.",
        "score_seguridad":        1.6,
        "score_demografia":       1.4,
        "score_entorno_comercial":1.2,
    },
    "clientela_profesional": {
        "_descripcion": "Restaurante de menú de mediodía, coworking, servicios B2B.",
        "score_transporte":       1.7,  # bien comunicado para desplazamiento al trabajo
        "score_demografia":       1.3,
        "score_flujo_peatonal":   1.2,
    },

    # ── Características del local ──────────────────────────────────────────────
    "local_grande": {
        "_descripcion": "Necesita >100 m² (gimnasio, restaurante con sala, etc.).",
        "score_precio_alquiler":  1.8,  # el precio por m² impacta mucho más
        "score_transporte":       1.3,
    },
    "local_pequeño": {
        "_descripcion": "Puede operar en <40 m² (kiosko, pop-up, tatuajes, etc.).",
        "score_precio_alquiler":  0.7,
        "score_flujo_peatonal":   1.6,  # la visibilidad en calle compensa el espacio
    },

    # ── Modelo de negocio ──────────────────────────────────────────────────────
    "alta_rotacion_clientes": {
        "_descripcion": "Fast food, copistería, quiosco — muchos clientes de corta duración.",
        "score_flujo_peatonal":   2.0,
        "score_transporte":       1.4,
        "score_competencia":      1.2,
    },
    "destino": {
        "_descripcion": "El cliente viene expresamente (clínica especializada, taller, etc.).",
        "score_flujo_peatonal":   0.4,  # no depende del paso casual
        "score_transporte":       1.6,
        "score_precio_alquiler":  1.5,
    },
    "experiencial": {
        "_descripcion": "El local es parte del producto (escape room, estudio fotográfico, etc.).",
        "score_entorno_comercial":1.5,
        "score_turismo":          1.3,
        "score_competencia":      0.7,  # la competencia directa importa menos
    },

    # ── Sector específico refinado ─────────────────────────────────────────────
    "takeaway_delivery": {
        "_descripcion": "El negocio opera principalmente para llevar/delivery.",
        "score_flujo_peatonal":   0.5,
        "score_precio_alquiler":  1.6,
        "score_transporte":       1.4,
    },
    "salon_citas": {
        "_descripcion": "Barbería, salón de belleza, tatuajes — funciona con citas previas.",
        "score_flujo_peatonal":   0.6,
        "score_entorno_comercial":1.5,
        "score_seguridad":        1.3,
    },
    "retail_moda": {
        "_descripcion": "Tienda de ropa/complementos que compite con online.",
        "score_entorno_comercial":1.6,  # necesita estar en zona comercial activa
        "score_flujo_peatonal":   1.5,
        "score_competencia":      1.3,
    },

    # ── Conceptos de cafetería / café ──────────────────────────────────────────
    "specialty_coffee": {
        "_descripcion": "Café de especialidad de tercera ola: origen único, barista, catas.",
        # No depende de turismo de paso — el cliente viene a propósito
        "score_flujo_peatonal":   0.7,
        "score_turismo":          0.6,
        # Importa el barrio: gente con poder adquisitivo y cultura de café
        "score_demografia":       1.6,
        "score_entorno_comercial":1.5,  # zona de diseño/gastronomía sube la marca
    },
    "dog_friendly": {
        "_descripcion": "Establece abiertamente que acepta perros/mascotas dentro.",
        # Buscan barrios residenciales tranquilos con parques cercanos
        "score_entorno_comercial":0.8,  # menos dependiente de zona comercial pura
        "score_demografia":       1.4,  # dueños de mascotas → clase media-alta
        "score_turismo":          0.5,  # el turista no trae el perro
        "score_seguridad":        1.2,
        # El parque y zonas verdes es un factor clave (m2_zonas_verdes_cercanas en features)
    },
    "coworking_cafe": {
        "_descripcion": "Café diseñado también como espacio de trabajo (wifi, enchufes, silencio).",
        "score_transporte":       1.8,  # muy dependiente de buena conexión
        "score_flujo_peatonal":   0.6,  # el cliente viene a trabajar, no de paso
        "score_demografia":       1.5,  # freelancers → renta media-alta
        "score_seguridad":        1.1,
        "score_entorno_comercial":1.2,
    },
    "instagrammable": {
        "_descripcion": "Local con alto atractivo visual — el diseño es parte del producto.",
        # Necesita visibilidad y zona con gente joven y turistas
        "score_flujo_peatonal":   1.5,
        "score_turismo":          1.6,
        "score_entorno_comercial":1.4,
        "score_demografia":       0.9,
    },

    # ── Alimentación / gastronomía ─────────────────────────────────────────────
    "vegano_organico": {
        "_descripcion": "Restaurante, tienda o café con oferta 100% vegana u orgánica.",
        "score_demografia":       1.7,  # barrios con renta media-alta y perfil lifestyle
        "score_turismo":          1.2,  # turismo consciente también lo valora
        "score_entorno_comercial":1.3,
        "score_competencia":      0.8,  # nicho — la saturación genérica no aplica igual
    },
    "gastronomico_premium": {
        "_descripcion": "Restaurante de menú degustación, mesa del chef, alta gastronomía.",
        "score_demografia":       2.2,  # renta altísima imprescindible en el área
        "score_turismo":          1.5,  # también atrae turismo gastronómico
        "score_precio_alquiler":  0.5,  # dispuesto a pagar más por la ubicación
        "score_flujo_peatonal":   0.6,  # el comensal reserva — no es cliente de paso
        "score_competencia":      0.6,
    },
    "street_food": {
        "_descripcion": "Concepto de comida callejera informal: tacos, smash burgers, bao, etc.",
        "score_flujo_peatonal":   2.2,  # altísima dependencia del paso
        "score_turismo":          1.4,
        "score_precio_alquiler":  1.5,  # margen estrecho → precio del local crítico
        "score_demografia":       0.6,
    },

    # ── Cultura, arte y bienestar ──────────────────────────────────────────────
    "cultural_artistico": {
        "_descripcion": "Galería, sala de conciertos, tienda de materiales artísticos, librería.",
        "score_entorno_comercial":1.6,
        "score_turismo":          1.3,
        "score_flujo_peatonal":   0.9,
        "score_competencia":      0.6,
    },
    "health_wellness": {
        "_descripcion": "Centro de bienestar, meditación, terapias holísticas, medicina integrativa.",
        "score_seguridad":        1.4,
        "score_demografia":       1.6,
        "score_flujo_peatonal":   0.5,  # cliente de destino, no casual
        "score_turismo":          0.7,
        "score_entorno_comercial":1.2,
    },

    # ── Deporte ────────────────────────────────────────────────────────────────
    "fitness_boutique": {
        "_descripcion": "Gimnasio boutique, reformer pilates, entrenamiento funcional.",
        "score_demografia":       1.8,  # cuota mensual alta → renta media-alta
        "score_transporte":       1.5,  # antes/después del trabajo
        "score_flujo_peatonal":   0.5,  # cliente de suscripción, no de paso
        "score_precio_alquiler":  1.4,  # necesita espacio → precio importa
        "score_competencia":      1.1,
    },

    # ── Niños y familias ───────────────────────────────────────────────────────
    "kids_activity": {
        "_descripcion": "Ludoteca, taller infantil, gimnasio para niños, guardería.",
        "score_seguridad":        2.0,  # la seguridad del barrio es decisiva
        "score_demografia":       1.5,
        "score_entorno_comercial":1.0,
        "score_turismo":          0.3,
        "score_flujo_peatonal":   0.7,
    },
}

# Tags válidos (sin la clave interna)
TAGS_VALIDOS = {k for k in MODIFICADORES_IDEA if not k.startswith("_")}


def aplicar_idea_tags(pesos_sector: dict, tags: list[str]) -> dict:
    """
    Aplica los modificadores de los tags sobre los pesos base del sector.

    Los pesos resultantes se renormalizan para que sumen 1.0 (como los originales).

    Args:
        pesos_sector: dict con peso_flujo, peso_demo, … (de tabla `sectores`)
        tags: lista de tags extraídos de la descripción del negocio

    Returns:
        dict con los mismos keys que pesos_sector pero valores ajustados
    """
    if not tags:
        return pesos_sector

    pesos = copy.deepcopy(pesos_sector)

    # Mapeo interno dimension_key → campo de `sectores`
    _DIM_MAP = {
        "score_flujo_peatonal":    "peso_flujo",
        "score_demografia":        "peso_demo",
        "score_competencia":       "peso_competencia",
        "score_precio_alquiler":   "peso_precio",
        "score_transporte":        "peso_transporte",
        "score_seguridad":         "peso_seguridad",
        "score_turismo":           "peso_turismo",
        "score_entorno_comercial": "peso_entorno",
    }
    _DEFAULT_PESOS = {
        "peso_flujo": 0.25, "peso_demo": 0.20, "peso_competencia": 0.15,
        "peso_precio": 0.15, "peso_transporte": 0.10,
        "peso_seguridad": 0.05, "peso_turismo": 0.05, "peso_entorno": 0.05,
    }

    for tag in tags:
        if tag not in MODIFICADORES_IDEA:
            logger.debug("Tag desconocido ignorado: %s", tag)
            continue
        for dim_key, multiplicador in MODIFICADORES_IDEA[tag].items():
            if dim_key.startswith("_"):
                continue
            campo = _DIM_MAP.get(dim_key)
            if not campo:
                continue
            valor_actual = pesos.get(campo) or _DEFAULT_PESOS.get(campo, 0.05)
            pesos[campo] = valor_actual * multiplicador

    # Renormalizar para que la suma de pesos siga siendo 1.0
    campos_peso = list(_DIM_MAP.values())
    total = sum(pesos.get(c) or _DEFAULT_PESOS.get(c, 0.05) for c in campos_peso)
    if total > 0:
        for campo in campos_peso:
            valor = pesos.get(campo) or _DEFAULT_PESOS.get(campo, 0.05)
            pesos[campo] = round(valor / total, 4)

    return pesos


def extraer_tags_de_descripcion(descripcion_llm: Optional[str]) -> list[str]:
    """
    Mapeo simple de palabras clave en la descripción a tags.

    Para uso como fallback cuando el agente LLM no devuelve tags explícitos.
    En producción, el agente validador debería devolver los tags directamente.
    """
    if not descripcion_llm:
        return []

    texto = descripcion_llm.lower()
    tags = []

    _PALABRAS_CLAVE: dict[str, list[str]] = {
        "orientado_turismo":      ["turista", "souvenir", "playa", "litoral", "barceloneta",
                                    "poblenou", "turismo", "tourist"],
        "clientela_local":        ["barrio", "vecinos", "vecindario", "local", "comunidad"],
        "alta_renta":             ["premium", "lujo", "gourmet", "exclusivo", "boutique",
                                    "alta gama", "high-end"],
        "low_cost":               ["económic", "barato", "descuento", "lowcost", "low cost",
                                    "accesible", "precio bajo"],
        "horario_nocturno":       ["nocturno", "noche", "copas", "discoteca", "after",
                                    "tardeo", "club"],
        "clientela_joven":        ["joven", "millenni", "gen z", "urbano", "hipster",
                                    "trendy"],
        "clientela_familiar":     ["familiar", "familia", "niños", "infantil", "padres"],
        "clientela_profesional":  ["profesional", "menú del día", "trabajador", "oficinas",
                                    "coworking", "autónomo", "freelance"],
        "local_grande":           ["grande", "amplio", "sala", "gimnasio", "espacio amplio",
                                    "gran formato"],
        "local_pequeño":          ["pequeño", "mini", "kiosko", "pop-up", "compacto"],
        "alta_rotacion_clientes": ["fast food", "rapidez", "copistería", "quiosco",
                                    "panadería", "cafetería rápida"],
        "destino":                ["especializ", "clínica", "taller", "formación",
                                    "estudio especializado"],
        "takeaway_delivery":      ["delivery", "takeaway", "para llevar", "domicilio",
                                    "just eat", "glovo"],
        "salon_citas":            ["cita previa", "citas", "barbería", "peluquería",
                                    "salón", "tatuaje", "micropigmentación"],
        "retail_moda":            ["ropa", "moda", "complementos", "fashion", "tienda de ropa"],
        "experiencial":           ["experiencia", "escape room", "inmersivo", "taller",
                                    "vivencial", "fotografía"],
        # Nuevos
        "specialty_coffee":       ["especialidad", "tercera ola", "single origin", "origin",
                                    "barista", "specialty coffee", "café de autor",
                                    "pour over", "aeropress", "v60"],
        "dog_friendly":           ["perros", "mascotas", "pet friendly", "dog friendly",
                                    "animales", "con perro", "apto mascotas"],
        "coworking_cafe":         ["coworking", "wifi", "trabajo", "enchufes", "silencio",
                                    "portátil", "remote", "nómada digital"],
        "instagrammable":         ["instagram", "fotogénico", "diseño", "aesthetic",
                                    "bonito", "decorado", "visual", "foto"],
        "vegano_organico":        ["vegano", "vegetariano", "orgánico", "plant-based",
                                    "sin gluten", "saludable", "bio", "eco"],
        "gastronomico_premium":   ["degustación", "menú degustación", "chef", "michelin",
                                    "alta cocina", "gastronómico", "fine dining"],
        "street_food":            ["street food", "tacos", "burger", "smash", "bao",
                                    "food truck", "informal", "comida callejera"],
        "cultural_artistico":     ["galería", "arte", "conciertos", "librería", "cultural",
                                    "música en vivo", "exposición", "artístico"],
        "health_wellness":        ["bienestar", "meditación", "yoga", "holístico",
                                    "terapia", "mindfulness", "wellness"],
        "fitness_boutique":       ["pilates", "reformer", "boutique gym", "entrenamiento",
                                    "crossfit", "funcional", "hiit"],
        "kids_activity":          ["niños", "infantil", "ludoteca", "guardería",
                                    "actividades para niños", "bebés"],
    }

    for tag, palabras in _PALABRAS_CLAVE.items():
        if any(p in texto for p in palabras):
            tags.append(tag)

    return tags
