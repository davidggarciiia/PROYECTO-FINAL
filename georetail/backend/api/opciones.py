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


def _build_subsectores() -> dict[str, list[SubsectorOpcion]]:
    """Subsectores por sector a partir de BASE_VARIANTS_BY_SECTOR."""
    out: dict[str, list[SubsectorOpcion]] = {}
    for sector, variants in BASE_VARIANTS_BY_SECTOR.items():
        out[sector] = [
            SubsectorOpcion(codigo=slug, label=label)
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
