"""
scoring/clasificador_libre.py — Capa 1 del clasificador híbrido.

Clasificador determinista para texto libre en español.
Cobertura: 80+ tipos de negocio habituales en el mercado español.
Sin llamadas LLM — resultado en <1ms.

Integración:
  validador.py llama a clasificar_descripcion_libre() antes del LLM y
  el resultado se inyecta como <pre_classification> en el mensaje usuario.
  El LLM lo usa como hint, pudiendo confirmarlo o corregirlo.

subsector devuelve códigos de benchmarks_subsector (migration 037) cuando
existe equivalencia exacta; en caso contrario usa slugs descriptivos que
el estimador LLM fallback puede interpretar.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass
class PreClasificacion:
    sector: str | None       # código backend (estetica, restauracion, …) o None
    subsector: str | None    # código DB o slug descriptivo; None si solo sector
    confidence: float        # 0.0–1.0
    keywords: list[str] = field(default_factory=list)

    @property
    def hint_strength(self) -> str:
        """STRONG (≥0.80) / WEAK (0.40–0.79) / NONE (<0.40)."""
        if self.confidence >= 0.80:
            return "STRONG"
        if self.confidence >= 0.40:
            return "WEAK"
        return "NONE"

    @property
    def es_confiable(self) -> bool:
        return self.confidence >= 0.75

    def as_hint(self) -> str | None:
        """Texto inyectable en el mensaje LLM. None si confidence < 0.40."""
        if self.sector is None or self.confidence < 0.40:
            return None
        parts = [
            f"strength={self.hint_strength}",
            f"sector={self.sector}",
            f"confidence={self.confidence:.2f}",
        ]
        if self.subsector:
            parts.append(f"subsector_hint={self.subsector}")
        if self.keywords:
            parts.append(f"matched={'|'.join(self.keywords[:3])}")
        return "; ".join(parts)


# Reglas: (patrón, sector, subsector_o_slug, confidence)
# ORDEN IMPORTA: más específico antes que más genérico.
_REGLAS_RAW: list[tuple[str, str, str | None, float]] = [

    # ── SHISHA / NARGUILE ────────────────────────────────────────────────────
    (r"\b(shisha|hookah|narguil[eé]?)\b",                               "shisha_lounge", None,               0.95),

    # ── TATUAJES ─────────────────────────────────────────────────────────────
    (r"\bfine[\s_-]?line\b",                                            "tatuajes",      "fine_line_studio",  0.95),
    (r"\b(tattoo|tatuaj[ea]|tatuador)\b",                               "tatuajes",      "tattoo_studio",     0.95),
    (r"\bpiercing\b",                                                   "tatuajes",      "tattoo_studio",     0.90),
    (r"\beliminaci[oó]n\s+(?:de\s+)?tatuaj\w*",                        "tatuajes",      "tattoo_studio",     0.90),

    # ── PELUQUERÍA CANINA (antes que barbería/pelo humano) ──────────────────
    (r"\bpeluquer[ií]a\s+(?:canina?|de?\s+(?:perros?|gatos?|mascotas?))\b",
                                                                        "servicios",     "pet_grooming",      0.97),
    (r"\b(?:dog\s+grooming|grooming\s+canino|aseo\s+(?:de\s+)?mascotas?)\b",
                                                                        "servicios",     "pet_grooming",      0.95),

    # ── BARBERÍA / PELUQUERÍA MASCULINA ─────────────────────────────────────
    (r"\b(?:barber[ií]a|barbershop|barber\s+shop)\b",                   "estetica",      "barber_shop",       0.97),
    (r"\bcorte\s+(?:de\s+)?(?:cabello|pelo)\s+(?:de\s+)?(?:hombre|caballero|masculin)\w*",
                                                                        "estetica",      "barber_shop",       0.92),
    (r"\bpeluquer[ií]a\s+(?:de\s+)?(?:caballeros?|hombres?|masculin\w+|barbud\w+)\b",
                                                                        "estetica",      "barber_shop",       0.92),
    (r"\b(?:cortar?\s+(?:el\s+)?(?:pelo|cabello)\s+y\s+(?:la\s+)?barba)\b",
                                                                        "estetica",      "barber_shop",       0.92),
    (r"\b(?:fade\s+(?:haircut|corte)|barba\s+diseñ\w+|afeitado\s+cl[aá]sico)\b",
                                                                        "estetica",      "barber_shop",       0.72),
    (r"\bbarba\b",                                                      "estetica",      "barber_shop",       0.68),
    (r"\bcortar?\s+(?:el\s+)?(?:pelo|cabello)\b",                      "estetica",      "hair_salon",        0.67),

    # ── CLÍNICA DENTAL ───────────────────────────────────────────────────────
    (r"\b(?:cl[ií]nica\s+dental|dentista|odontolog\w+|ortodonc\w+|implantes?\s+dental\w*)\b",
                                                                        "salud",         "dental_clinic",     0.95),
    (r"\b(?:blanqueamiento\s+dental|funda\s+dental|endodoncia|empastes?)\b",
                                                                        "salud",         "dental_clinic",     0.90),

    # ── FISIOTERAPIA / OSTEOPATÍA ─────────────────────────────────────────────
    (r"\b(?:fisioterapi\w+|fisioterapeuta|osteopat\w+|quiroprácti\w+)\b",
                                                                        "salud",         "physio_clinic",     0.95),
    (r"\brehabilita(?:ci[oó]n|dor)\s+(?:f[ií]sico?|deportiv\w+)\b",   "salud",         "physio_clinic",     0.90),

    # ── PSICOLOGÍA ────────────────────────────────────────────────────────────
    (r"\b(?:psicolog[ií]a|ps[ií]colog\w+|psicoterapia|terapeuta\s+cogniti\w+|EMDR)\b",
                                                                        "salud",         "psychology_center", 0.90),

    # ── ÓPTICA / FARMACIA ─────────────────────────────────────────────────────
    (r"\b(?:[oó]ptica|optometri\w+)\b",                                 "salud",         "optica",            0.92),
    (r"\b(?:farmacia|parafarmacia)\b",                                  "salud",         "farmacia",          0.92),

    # ── VETERINARIA ───────────────────────────────────────────────────────────
    (r"\bveterinari[ao]\b",                                             "salud",         "veterinario",       0.90),

    # ── CLÍNICA MÉDICA ────────────────────────────────────────────────────────
    (r"\b(?:cl[ií]nica\s+(?:m[eé]dica|privada|gen[eé]rica?)|m[eé]dico\s+de\s+cabecera\s+privad\w+)\b",
                                                                        "salud",         None,                0.85),

    # ── DEPILACIÓN LÁSER / CLÍNICA ESTÉTICA ──────────────────────────────────
    (r"\b(?:depilaci[oó]n\s+l[aá]ser|l[aá]ser\s+(?:capilar|est[eé]tico))\b",
                                                                        "estetica",      "beauty_clinic",     0.92),
    (r"\b(?:micropigmentaci[oó]n|microblading|semipermanente\s+(?:cejas|labios))\b",
                                                                        "estetica",      "beauty_clinic",     0.92),
    (r"\b(?:cl[ií]nica\s+est[eé]tica|medicina\s+est[eé]tica|botox|rejuvenecimiento\s+facial)\b",
                                                                        "estetica",      "beauty_clinic",     0.90),

    # ── CEJAS / PESTAÑAS ──────────────────────────────────────────────────────
    (r"\b(?:cejas?\s+(?:henna|laminadas?|diseño)|lash(?:es)?\s+(?:extension|lifting|studio))\b",
                                                                        "estetica",      "beauty_clinic",     0.87),

    # ── SPA / MASAJES ─────────────────────────────────────────────────────────
    (r"\b(?:spa\b|day\s+spa|centro\s+(?:de\s+)?bienestar|wellness\s+(?:center|studio))\b",
                                                                        "estetica",      "day_spa",           0.92),
    (r"\b(?:masaj[ea]\w*|masajista)\b",                                 "estetica",      "day_spa",           0.87),

    # ── MANICURA / UÑAS ───────────────────────────────────────────────────────
    (r"\b(?:nail\s+(?:studio|salon|bar)|manicur\w+|pedicur\w+)\b",     "estetica",      "nail_studio",       0.92),
    (r"\bu[nñ]as\s+(?:gel|acr[ií]licas?|semipermanentes?|esculpidas?)\b",
                                                                        "estetica",      "nail_studio",       0.90),
    (r"\bu[nñ]as\b",                                                    "estetica",      "nail_studio",       0.68),

    # ── PELUQUERÍA HUMANA (unisex / femenina) ────────────────────────────────
    (r"\b(?:sal[oó]n\s+(?:de\s+)?belleza|beauty\s+salon|hair\s+salon)\b",
                                                                        "estetica",      "hair_salon",        0.92),
    (r"\b(?:colorista|extensiones\s+(?:de\s+)?(?:pelo|cabello)|tinte\s+capilar|keratina)\b",
                                                                        "estetica",      "hair_salon",        0.85),
    (r"\bpeluquer[ií]a\b",                                              "estetica",      "hair_salon",        0.90),

    # ── YOGA / PILATES ─────────────────────────────────────────────────────────
    (r"\b(?:yoga|pilates|reformer\s+pilates|ashtanga|vinyasa)\b",       "deporte",       "yoga_pilates",      0.92),

    # ── FITNESS BOUTIQUE / CROSSFIT ───────────────────────────────────────────
    (r"\b(?:crossfit|entrenamiento\s+funcional|functional\s+fitness|HIIT\s+studio)\b",
                                                                        "deporte",       "boutique_gym",      0.92),
    (r"\b(?:boxeo\s+fitness|kick[\s-]?boxing|muay\s+thai|artes\s+marciales)\b",
                                                                        "deporte",       "boutique_gym",      0.85),
    (r"\bboxeo\b",                                                      "deporte",       "boutique_gym",      0.80),
    (r"\b(?:clases?\s+(?:de\s+)?(?:boxeo|lucha|judo|karate|jiu[\s-]?jitsu))\b",
                                                                        "deporte",       "boutique_gym",      0.87),

    # ── GIMNASIO ──────────────────────────────────────────────────────────────
    (r"\b(?:gimnasio|gym\b|fitness\s+center|centro\s+(?:de\s+)?fitness)\b",
                                                                        "deporte",       None,                0.87),

    # ── DEPORTES ESPECÍFICOS ──────────────────────────────────────────────────
    (r"\b(?:p[aá]del|pista\s+(?:de\s+)?(?:tenis|p[aá]del)|club\s+(?:de\s+)?tenis)\b",
                                                                        "deporte",       None,                0.88),
    (r"\b(?:escalada|rock?[oó]dromo|climbing\s+gym)\b",                 "deporte",       None,                0.88),
    (r"\b(?:nataci[oó]n|piscina\s+privada)\b",                          "deporte",       None,                0.82),
    (r"\b(?:danza|ballet|academia\s+(?:de\s+)?baile)\b",                "deporte",       None,                0.82),

    # ── ACADEMIA DE IDIOMAS ───────────────────────────────────────────────────
    (r"\b(?:academia\s+(?:de\s+)?idiomas|escuela\s+(?:de\s+)?idiomas|clases?\s+(?:de\s+)?(?:ingl[eé]s|franc[eé]s|alem[aá]n|chino|italiano|japon[eé]s)|academia\s+(?:de\s+)?(?:ingl[eé]s|franc[eé]s|alem[aá]n))\b",
                                                                        "educacion",     "language_academy",  0.92),

    # ── GUARDERÍA / EDUCACIÓN INFANTIL ───────────────────────────────────────
    (r"\b(?:guarder[ií]a|jard[ií]n\s+(?:de\s+)?infancia|escuela\s+infantil|llar\s+d.infants)\b",
                                                                        "educacion",     None,                0.92),

    # ── ACADEMIA GENÉRICA ─────────────────────────────────────────────────────
    (r"\bacademia\s+(?:de\s+)?(?:m[uú]sica|danza|baile|pintura|arte|cocina|fotograf[ií]a)\b",
                                                                        "educacion",     None,                0.90),
    (r"\b(?:academia|autoescuela|clases\s+particulares|formaci[oó]n\s+(?:privada|profesional))\b",
                                                                        "educacion",     None,                0.80),

    # ── DISCOTECA / OCIO NOCTURNO ─────────────────────────────────────────────
    (r"\b(?:discoteca|night\s*club|club\s+nocturno|sala\s+(?:de\s+)?(?:fiestas|discoteca)|sala\s+de\s+baile)\b",
                                                                        "restauracion",  "cocktail_bar",      0.92),

    # ── COCTELERÍA / BAR NOCTURNO ─────────────────────────────────────────────
    (r"\b(?:cocteler[ií]a|c[oó]ctel|cocktail\s+bar|gintonería|bar\s+de\s+copas|pub\s+nocturno)\b",
                                                                        "restauracion",  "cocktail_bar",      0.92),
    (r"\b(?:vinoteca|wine\s+bar|bar\s+(?:de\s+)?vinos?)\b",            "restauracion",  "cocktail_bar",      0.87),

    # ── TAPAS ─────────────────────────────────────────────────────────────────
    (r"\b(?:tapas?|pintxo[s]?|taberna|tasca|bodega\s+gastro)\b",       "restauracion",  "tapas_bar",         0.90),

    # ── VEGANO / VEGETARIANO ─────────────────────────────────────────────────
    (r"\b(?:vegano|vegetariano|plant[\s-]based|bio\s+caf[eé]|zero\s+waste\s+food)\b",
                                                                        "restauracion",  "vegan_bistro",      0.87),

    # ── BRUNCH ────────────────────────────────────────────────────────────────
    (r"\b(?:brunch|bruncher[ií]a)\b",                                   "restauracion",  "brunch_house",      0.92),

    # ── COWORKING CAFÉ ────────────────────────────────────────────────────────
    (r"\b(?:coworking\s+caf[eé]|caf[eé]\s+(?:con\s+)?coworking)\b",   "restauracion",  "coworking_cafe",    0.92),

    # ── SPECIALTY COFFEE ──────────────────────────────────────────────────────
    (r"\b(?:caf[eé]\s+(?:de\s+)?especialidad|specialty\s+coffee|tercera\s+ola)\b",
                                                                        "restauracion",  "specialty_coffee",  0.95),
    (r"\b(?:cafeter[ií]a|caf[eé]\b|coffee\s+shop|espresso\s+bar)\b",   "restauracion",  "specialty_coffee",  0.82),
    (r"\b(?:pastelería|repostería|tartas?\s+artesanales?|croissant\w*)\b",
                                                                        "restauracion",  "neighborhood_cafe", 0.80),

    # ── FAST FOOD / COMIDA RÁPIDA ─────────────────────────────────────────────
    (r"\b(?:kebab|d[oó]ner|falafel|sha[w]?arma)\b",                    "restauracion",  "street_food_counter", 0.92),
    (r"\b(?:hamburgues[aeo]r[ií]a|burger\b|smash\s+burger)\b",         "restauracion",  "street_food_counter", 0.92),
    (r"\b(?:pizzer[ií]a|pizza(?!\s+restaurante))\b",                    "restauracion",  "street_food_counter", 0.87),
    (r"\bpoke\b",                                                       "restauracion",  "street_food_counter", 0.85),
    (r"\b(?:comida\s+r[aá]pida|fast[\s-]?food|takeaway|take[\s-]?away|para\s+llevar)\b",
                                                                        "restauracion",  "street_food_counter", 0.88),
    (r"\b(?:dark\s+kitchen|cocina\s+fantasma|ghost\s+kitchen)\b",       "restauracion",  "street_food_counter", 0.92),
    (r"\b(?:panader[ií]a|obrador|masa\s+madre|pan\s+artesanal)\b",      "restauracion",  "neighborhood_cafe", 0.85),
    (r"\b(?:comida\s+(?:japonesa|coreana|thai|india|vietnamita|china)|sushi|ramen)\b",
                                                                        "restauracion",  "street_food_counter", 0.82),

    # ── FINE DINING ────────────────────────────────────────────────────────────
    (r"\b(?:alta\s+cocina|fine\s+dining|men[uú]\s+de\s+degustaci[oó]n|gastron[oó]mico\s+premium|michelin)\b",
                                                                        "restauracion",  "fine_dining",       0.95),

    # ── RESTAURANTE GENÉRICO ──────────────────────────────────────────────────
    (r"\b(?:restaurante|bistró?|comedor|eatery|brasserie|bar\s+restaurante)\b",
                                                                        "restauracion",  None,                0.82),
    (r"\bbar\b",                                                        "restauracion",  "tapas_bar",         0.62),

    # ── SUPERMERCADO / ALIMENTACIÓN ───────────────────────────────────────────
    (r"\b(?:supermercado|mini[\s-]?mercado|mini[\s-]?super|tienda\s+24\s*h|convenience\s+store)\b",
                                                                        "alimentacion",  None,                0.92),
    (r"\b(?:frutería|verdulería|mercado\s+ecol[oó]gico|ecotienda|frutas?\s+y\s+verduras?)\b",
                                                                        "alimentacion",  None,                0.92),
    (r"\b(?:carnicer[ií]a|charcuter[ií]a|jamoner[ií]a|pescader[ií]a|mariser[ií]a)\b",
                                                                        "alimentacion",  None,                0.92),
    (r"\b(?:gourmet|delicatessen|deli\b|tienda\s+(?:de\s+)?productos?\s+(?:artesanales?|selectos?|locales?))\b",
                                                                        "alimentacion",  None,                0.87),
    (r"\b(?:herbolari[ao]|diet[eé]tica|tienda\s+(?:de\s+)?nutrici[oó]n|suplementos?\s+naturales?)\b",
                                                                        "alimentacion",  None,                0.87),

    # ── MODA / ROPA ────────────────────────────────────────────────────────────
    (r"\b(?:moda\s+(?:sostenible|[eé]tica|circular|ecol[oó]gica)|segunda\s+mano|vintage\b|thrift\s+store)\b",
                                                                        "moda",          None,                0.90),
    (r"\b(?:tienda\s+(?:de\s+)?(?:ropa|moda)|boutique\b|showroom\s+(?:de\s+)?moda)\b",
                                                                        "moda",          None,                0.87),
    (r"\b(?:zapatería|tienda\s+(?:de\s+)?(?:zapatos|calzado))\b",       "moda",          None,                0.90),
    (r"\b(?:complementos\s+(?:de\s+)?moda|accesorios\s+(?:de\s+)?moda|bisutería)\b",
                                                                        "moda",          None,                0.87),
    (r"\b(?:ropa\s+infantil|moda\s+(?:ni[nñ]os?|beb[eé]))\b",           "moda",          None,                0.90),
    (r"\b(?:streetwear|urban\s+fashion|sneaker\s+(?:shop|store))\b",    "moda",          None,                0.87),

    # ── LAVANDERÍA / TINTORERÍA ───────────────────────────────────────────────
    (r"\b(?:lavandería|laundromat|tintorer[ií]a|lavado\s+en\s+seco)\b", "servicios",     None,                0.92),

    # ── COWORKING ─────────────────────────────────────────────────────────────
    (r"\b(?:coworking(?!\s+caf[eé])|oficinas?\s+flexibles?|espacio\s+(?:de\s+)?trabajo\s+compartido)\b",
                                                                        "servicios",     "coworking_office",  0.90),

    # ── TIENDA DE MÓVILES / TECNOLOGÍA ───────────────────────────────────────
    (r"\btienda\s+(?:de\s+)?m[oó]viles?\b",                            "servicios",     None,                0.87),
    (r"\btienda\s+(?:de\s+)?(?:tecnolog[ií]a|electr[oó]nica)\b",       "otro",          None,                0.87),

    # ── REPARACIONES ─────────────────────────────────────────────────────────
    (r"\breparaci[oó]n\s+(?:de\s+)?(?:m[oó]viles?|tel[eé]fonos?|port[aá]tiles?|ordenadores?)\b",
                                                                        "servicios",     None,                0.90),
    (r"\b(?:cerrajería|cerrajero)\b",                                   "servicios",     None,                0.92),
    (r"\b(?:floristería|florista\b)\b",                                 "servicios",     None,                0.90),

    # ── PET SHOP ──────────────────────────────────────────────────────────────
    (r"\b(?:tienda\s+(?:de\s+)?animales?|pet\s+shop|tienda\s+mascotas?)\b",
                                                                        "servicios",     None,                0.87),

    # ── GALERÍA / ESPACIO CULTURAL ────────────────────────────────────────────
    (r"\b(?:galería\s+(?:de\s+)?arte|art\s+gallery|espacio\s+cultural|sala\s+(?:de\s+)?exposici\w+)\b",
                                                                        "otro",          None,                0.82),

    # ── LIBERÍA ───────────────────────────────────────────────────────────────
    (r"\b(?:librer[ií]a|tienda\s+(?:de\s+)?libros|book\s+(?:shop|store))\b",
                                                                        "otro",          None,                0.88),

    # ── INFORMÁTICA / ELECTRÓNICA ─────────────────────────────────────────────
    (r"\b(?:tienda\s+(?:de\s+)?(?:inform[aá]tica|electr[oó]nica|tecnolog[ií]a|gaming))\b",
                                                                        "otro",          None,                0.87),

    # ── OCIO EXPERIENCIAL ─────────────────────────────────────────────────────
    (r"\b(?:escape\s+room|laser\s+tag|realidad\s+virtual|sala\s+(?:de\s+)?videojuegos?)\b",
                                                                        "otro",          None,                0.88),

    # ── PAPELERÍA / IMPRENTA ──────────────────────────────────────────────────
    (r"\b(?:papelería|fotocopister[ií]a|imprenta|print\s+shop)\b",      "otro",          None,                0.87),

    # ── FERRETERÍA ────────────────────────────────────────────────────────────
    (r"\b(?:ferretería|bricolaje\s+tienda)\b",                           "otro",          None,                0.87),

    # ── IMPRESIÓN 3D / MAKER SPACE ───────────────────────────────────────────
    (r"\b(?:impresi[oó]n\s+3d|fabricaci[oó]n\s+digital|maker\s+space|fab\s*lab|prototipado)\b",
                                                                        "otro",          None,                0.82),

    # ── JUGUETERÍA ────────────────────────────────────────────────────────────
    (r"\b(?:jugueter[ií]a|tienda\s+(?:de\s+)?juguetes)\b",              "otro",          None,                0.88),
]

_REGLAS: list[tuple[re.Pattern[str], str, str | None, float]] = [
    (re.compile(p, re.IGNORECASE), sec, sub, conf)
    for p, sec, sub, conf in _REGLAS_RAW
]


def clasificar_descripcion_libre(texto: str) -> PreClasificacion:
    """
    Clasificador determinista para texto libre.

    Recorre las reglas en orden; la primera que matchea gana.
    Si ninguna matchea devuelve PreClasificacion(sector=None, confidence=0.0).
    """
    if not texto or not texto.strip():
        return PreClasificacion(sector=None, subsector=None, confidence=0.0)

    texto_limpio = texto.strip()
    for patron, sector, subsector, confidence in _REGLAS:
        m = patron.search(texto_limpio)
        if m:
            return PreClasificacion(
                sector=sector,
                subsector=subsector,
                confidence=confidence,
                keywords=[m.group(0).strip()],
            )

    return PreClasificacion(sector=None, subsector=None, confidence=0.0)
