"""api/opciones.py — GET /api/cuestionario/opciones

Catálogo de opciones que alimenta el formulario "tipo test" del frontend.
Es la única fuente de verdad para los selects y chips multiselect del
cuestionario estructurado (alternativa a la textarea libre).

Se lee principalmente desde:
  - scoring/concepto/taxonomy.py → sectores y subsectores canónicos.
  - api/legal.py → flags legales por sector derivados de `_SECTORES`.
  - Constantes estáticas (distritos BCN, enums de PerfilRefinado).

Cacheable (Cache-Control 1h). El catálogo no cambia entre requests.
"""
from __future__ import annotations

from fastapi import APIRouter, Response

from schemas.models import (
    FlagLegalOpcion,
    OpcionesCuestionarioResponse,
    SubsectorOpcion,
)
from scoring.concepto.taxonomy import BASE_VARIANTS_BY_SECTOR

router = APIRouter(tags=["cuestionario"])


# ─── Etiquetas humanas para los sectores de la taxonomía ──────────────────────

_SECTOR_LABELS: dict[str, str] = {
    "restauracion":  "Restauración (bar, cafetería, restaurante)",
    "moda":          "Moda, ropa y complementos",
    "estetica":      "Estética y belleza",
    "tatuajes":      "Tatuajes, piercing y micropigmentación",
    "shisha_lounge": "Shisha lounge / club privado de fumadores",
    "salud":         "Salud y bienestar",
    "deporte":       "Deporte y fitness",
    "educacion":     "Educación y formación",
    "alimentacion":  "Alimentación de proximidad",
    "servicios":     "Servicios de barrio",
    "otro":          "Otro / genérico",
}


# ─── Constantes de dominio (alineadas con los Literal de PerfilRefinado) ─────

_NIVELES_SOCIO = [
    ("bajo",       "Bajo"),
    ("medio",      "Medio"),
    ("medio-alto", "Medio-alto"),
    ("alto",       "Alto"),
]

_ESTILOS_VIDA = [
    ("turistas",      "Turistas"),
    ("residentes",    "Residentes"),
    ("estudiantes",   "Estudiantes"),
    ("familias",      "Familias"),
    ("profesionales", "Profesionales"),
    ("jovenes",       "Jóvenes (18-30)"),
    ("seniors",       "Seniors (60+)"),
    ("creativos",     "Creativos / artistas"),
    ("expats",        "Expats"),
    ("pet_owners",    "Con mascotas"),
]

_HORARIOS_PICO = [
    ("manana",   "Mañana"),
    ("brunch",   "Brunch"),
    ("mediodia", "Mediodía"),
    ("tarde",    "Tarde"),
    ("noche",    "Noche"),
]

_HORARIOS_APERTURA = [
    ("manana",      "Mañana"),
    ("tarde",       "Tarde"),
    ("noche",       "Noche"),
    ("fin_semana",  "Fines de semana"),
    ("domingo",     "Domingo"),
    ("continuo",    "Horario continuo"),
]

_MODELOS_SERVICIO = [
    ("mesas",          "Mesas / consumo en local"),
    ("take_away",      "Take-away / para llevar"),
    ("mixto",          "Mixto (mesas + take-away)"),
    ("delivery_only",  "Solo delivery"),
]

_ESCALAS_OPERATIVAS = [
    ("solo",    "Solo (autoempleo)"),
    ("micro",   "Micro (2-4 personas)"),
    ("pequeña", "Pequeña (5-10 personas)"),
    ("mediana", "Mediana (11+ personas)"),
]

_DENSIDADES = [
    ("baja",   "Baja (zona residencial tranquila)"),
    ("media",  "Media (barrio de vida)"),
    ("alta",   "Alta (flujo intenso, centro)"),
]

_TIPOS_CALLE = [
    ("comercial_principal",   "Eje comercial principal"),
    ("comercial_secundaria",  "Calle comercial secundaria"),
    ("residencial",           "Calle residencial"),
    ("peatonal",              "Peatonal"),
    ("mixta",                 "Mixta"),
    ("plaza",                 "Plaza o esquina destacada"),
]

_TIPOS_FLUJO = [
    ("residentes",  "Residentes del barrio"),
    ("turistas",    "Turistas"),
    ("oficinas",    "Oficinas / workers"),
    ("estudiantes", "Estudiantes"),
    ("mixto",       "Mixto"),
]

_DISTRITOS_BCN = [
    "Ciutat Vella",
    "Eixample",
    "Sants-Montjuïc",
    "Les Corts",
    "Sarrià-Sant Gervasi",
    "Gràcia",
    "Horta-Guinardó",
    "Nou Barris",
    "Sant Andreu",
    "Sant Martí",
]


# ─── Flags legales condicionales por sector ──────────────────────────────────
# Al marcar estos checkboxes, /api/legal/roadmap añade las licencias
# correspondientes. Se persisten en la sesión como `flags_legales`.

_FLAGS_POR_SECTOR: dict[str, list[FlagLegalOpcion]] = {
    "restauracion": [
        FlagLegalOpcion(codigo="servira_alcohol", label="Se servirá alcohol",
                        descripcion="Añade la llicència de venda de begudes alcohòliques."),
        FlagLegalOpcion(codigo="con_terraza", label="Con terraza exterior",
                        descripcion="Requiere ocupació de via pública (renovación anual)."),
        FlagLegalOpcion(codigo="cocina_con_humos", label="Cocina con humos",
                        descripcion="Salida de humos a cubierta obligatoria."),
        FlagLegalOpcion(codigo="emision_musica_alta", label="Música amplificada",
                        descripcion="Estudio acústico adicional si supera umbrales."),
    ],
    "tatuajes": [
        FlagLegalOpcion(codigo="tatuajes_invasivos", label="Tatuajes invasivos",
                        descripcion="No micropigmentación estética — activa Decret 90/2008."),
    ],
    "estetica": [
        FlagLegalOpcion(codigo="aparatologia_sanitaria", label="Aparatología sanitaria (láser, HIFU, IPL)",
                        descripcion="Autorización sanitaria de Generalitat obligatoria."),
    ],
    "shisha_lounge": [
        FlagLegalOpcion(codigo="club_privado_fumadores", label="Modelo club privado de fumadores",
                        descripcion="Asociación sin ánimo de lucro — único modelo legal."),
        FlagLegalOpcion(codigo="emision_musica_alta", label="Música amplificada / DJs",
                        descripcion="Estudio acústico adicional."),
    ],
    "moda":        [],
    "salud":       [],
    "deporte":     [],
    "educacion":   [],
    "alimentacion":[],
    "servicios":   [],
    "otro":        [],
}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _prettify(slug: str) -> str:
    """Convierte 'specialty_coffee' → 'Specialty coffee' para mostrar."""
    return slug.replace("_", " ").capitalize()


def _build_sectores() -> list[SubsectorOpcion]:
    """Lista de sectores canónicos con etiqueta humana."""
    return [
        SubsectorOpcion(codigo=sector, label=_SECTOR_LABELS.get(sector, sector.capitalize()))
        for sector in BASE_VARIANTS_BY_SECTOR.keys()
    ]


# Traducción ES de los labels de subsector. La taxonomía canónica en
# scoring/concepto/taxonomy.py mantiene los labels en inglés porque alimentan
# el matching NLP por embeddings; aquí los traducimos solo para presentación.
# Respetamos calcos ya asentados en el sector retail español: brunch,
# streetwear, spa, coworking, fine dining, take-away, delivery.
_SLUG_SUBSECTOR_ES: dict[str, str] = {
    # restauracion
    "specialty_coffee":     "Café de especialidad",
    "neighborhood_cafe":    "Cafetería de barrio",
    "express_cafe":         "Cafetería take-away",
    "coworking_cafe":       "Café-coworking",
    "dog_friendly_cafe":    "Café pet friendly",
    "brunch_house":         "Brunch",
    "fine_dining":          "Alta cocina / fine dining",
    "cocktail_bar":         "Coctelería",
    "street_food_counter":  "Street food / barra rápida",
    "vegan_bistro":         "Bistró vegano",
    # moda
    "premium_boutique":     "Boutique premium",
    "vintage_store":        "Tienda vintage",
    "streetwear_store":     "Streetwear",
    "sneaker_shop":         "Tienda de sneakers",
    "jewelry_store":        "Joyería boutique",
    "home_decor_store":     "Decoración de hogar",
    "concept_store":        "Concept store / diseño",
    # estetica
    "hair_salon":           "Peluquería",
    "barber_shop":          "Barbería",
    "nail_studio":          "Estudio de uñas",
    "brow_lash_bar":        "Cejas y pestañas",
    "beauty_clinic":        "Clínica estética",
    "day_spa":              "Spa urbano",
    "skin_clinic":          "Clínica de la piel",
    # tatuajes
    "tattoo_studio":        "Estudio de tatuajes",
    "piercing_studio":      "Estudio de piercing",
    "tattoo_gallery":       "Galería de tatuaje",
    "cosmetic_tattoo":      "Micropigmentación",
    "fine_line_studio":     "Tatuaje fine line",
    "street_tattoo":        "Tatuaje walk-in",
    "body_art_collective":  "Colectivo de body art",
    # shisha_lounge
    "classic_lounge":       "Shisha lounge clásico",
    "premium_lounge":       "Shisha lounge premium",
    "terrace_lounge":       "Shisha con terraza",
    "student_lounge":       "Shisha estudiantes",
    "tourist_lounge":       "Shisha turístico",
    "music_lounge":         "Shisha con música / DJ",
    "food_lounge":          "Shisha con cocina",
    # salud
    "dental_clinic":        "Clínica dental",
    "physio_clinic":        "Fisioterapia",
    "psychology_center":    "Centro de psicología",
    "dermatology_clinic":   "Dermatología",
    "optician":             "Óptica",
    "pharmacy":             "Farmacia",
    "aesthetic_medicine":   "Medicina estética",
    # deporte
    "boutique_gym":         "Gimnasio boutique",
    "yoga_studio":          "Estudio de yoga",
    "pilates_reformer":     "Pilates reformer",
    "crossfit_box":         "Box de CrossFit",
    "martial_arts":         "Artes marciales",
    "personal_training":    "Entrenamiento personal",
    "dance_fitness":        "Baile / fitness",
    # educacion
    "language_academy":     "Academia de idiomas",
    "tutoring_center":      "Academia de refuerzo",
    "nursery_school":       "Guardería",
    "music_school":         "Escuela de música",
    "art_school":           "Escuela de arte",
    "coding_academy":       "Academia de programación",
    "exam_prep_center":     "Preparación de exámenes",
    # alimentacion
    "grocery_store":        "Super de barrio",
    "greengrocer":          "Frutería",
    "butcher_shop":         "Carnicería",
    "fishmonger":           "Pescadería",
    "delicatessen":         "Delicatessen",
    "wine_shop":            "Vinoteca",
    "bakery_takeaway":      "Panadería take-away",
    # servicios
    "laundromat":           "Lavandería autoservicio",
    "mobile_repair":        "Reparación de móviles",
    "locksmith":            "Cerrajería",
    "florist":              "Floristería",
    "pet_grooming":         "Peluquería canina",
    "copy_shop":            "Copistería",
    "coworking_office":     "Coworking",
    # otro (genéricos)
    "generic_retail":        "Comercio minorista",
    "generic_destination":   "Local de destino",
    "generic_service":       "Servicio de barrio",
    "generic_showroom":      "Showroom / estudio",
    "generic_food_counter":  "Barra de comida",
    "generic_workshop":      "Taller / maker space",
    "generic_premium_space": "Espacio premium",
}


def _build_subsectores() -> dict[str, list[SubsectorOpcion]]:
    """Subsectores por sector a partir de BASE_VARIANTS_BY_SECTOR.

    El label se toma del diccionario de traducción ES; si falta el slug
    (nuevo subsector aún no mapeado) cae al label canónico en inglés del
    tuple como fallback.
    """
    out: dict[str, list[SubsectorOpcion]] = {}
    for sector, variants in BASE_VARIANTS_BY_SECTOR.items():
        out[sector] = [
            SubsectorOpcion(codigo=slug, label=_SLUG_SUBSECTOR_ES.get(slug, label))
            for (slug, label, *_rest) in variants
        ]
    return out


def _pairs(pairs: list[tuple[str, str]]) -> list[SubsectorOpcion]:
    return [SubsectorOpcion(codigo=c, label=l) for (c, l) in pairs]


# Cache en memoria — el catálogo es puramente derivado y no cambia entre requests.
_CATALOGO_CACHE: OpcionesCuestionarioResponse | None = None


def _build_catalogo() -> OpcionesCuestionarioResponse:
    global _CATALOGO_CACHE
    if _CATALOGO_CACHE is not None:
        return _CATALOGO_CACHE
    _CATALOGO_CACHE = OpcionesCuestionarioResponse(
        sectores=_build_sectores(),
        subsectores_por_sector=_build_subsectores(),
        niveles_socioeconomicos=_pairs(_NIVELES_SOCIO),
        estilos_vida=_pairs(_ESTILOS_VIDA),
        horarios_pico=_pairs(_HORARIOS_PICO),
        horarios_apertura=_pairs(_HORARIOS_APERTURA),
        modelos_servicio=_pairs(_MODELOS_SERVICIO),
        escalas_operativas=_pairs(_ESCALAS_OPERATIVAS),
        densidades=_pairs(_DENSIDADES),
        tipos_calle=_pairs(_TIPOS_CALLE),
        tipos_flujo=_pairs(_TIPOS_FLUJO),
        distritos_bcn=list(_DISTRITOS_BCN),
        flags_legales_por_sector=_FLAGS_POR_SECTOR,
    )
    return _CATALOGO_CACHE


# ─── Endpoint ─────────────────────────────────────────────────────────────────

@router.get(
    "/cuestionario/opciones",
    response_model=OpcionesCuestionarioResponse,
    summary="Catálogo de opciones para el cuestionario estructurado (test)",
)
async def get_opciones(response: Response) -> OpcionesCuestionarioResponse:
    """Devuelve todo el catálogo necesario para renderizar el formulario tipo
    test del frontend: sectores, subsectores, estilos de vida, horarios,
    distritos de Barcelona, flags legales por sector, etc.

    El response es cacheable 1h (el catálogo es estático entre despliegues).
    """
    response.headers["Cache-Control"] = "public, max-age=3600"
    return _build_catalogo()
