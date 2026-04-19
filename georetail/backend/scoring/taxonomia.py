"""
scoring/taxonomia.py — Clasificador sector/subsector desde texto libre de categoría.

Único sitio donde se decide qué es cada cosa. Consumidores:

  - pipelines/scraping/gosom_runner  → categoría que devuelve gosom
  - pipelines/resenas                → categoría que devuelve Google Places o Foursquare
  - scripts/backfill_subsectores     → backfill de negocios_activos
  - scoring/features                 → para documentar el dominio de valores

Diseño:
  - Taxonomía cerrada (SUBSECTORES) con ~25 subsectores. Cardinal reducida para
    no fragmentar el dataset histórico del XGBoost.
  - Reglas ordenadas; la primera que matchea gana. Regla terminal por sector
    ("restaurant", "shop", "hairdresser") con subsector=None → la macro queda
    detectada aunque no podamos afinar subsector.
  - Matching case-insensitive y por subcadena; si gosom dice "Coffee shop (Italian)"
    → `coffee` match → cafeteria.
"""
from __future__ import annotations

import re
from typing import Optional

# ─── Taxonomía ────────────────────────────────────────────────────────────────

SUBSECTORES: dict[str, list[str]] = {
    "restauracion":  ["cafeteria", "pizzeria", "tapas", "asiatica",
                      "hamburguesa", "fast_food", "fine_dining", "panaderia",
                      "bar_copas"],
    "moda":          ["ropa_mujer", "ropa_hombre", "vintage", "zapateria",
                      "complementos", "deportiva"],
    "estetica":      ["peluqueria", "unas", "spa", "cejas_pestanas",
                      "depilacion_laser", "barberia"],
    "tatuajes":      ["tatuaje", "piercing", "eliminacion_laser"],
    "shisha_lounge": ["shisha", "coctel_bar"],
}

SECTORES: tuple[str, ...] = tuple(SUBSECTORES.keys())


def subsector_valido(sector: str, subsector: Optional[str]) -> bool:
    """True si el subsector pertenece al sector declarado (o es None)."""
    if subsector is None:
        return True
    return subsector in SUBSECTORES.get(sector, ())


# ─── Reglas ordenadas (regex compiladas) ──────────────────────────────────────
#
# Cada regla: (patrón, sector, subsector).
# `subsector=None` significa "sabemos el macro pero no el sub — déjalo genérico".
# ORDEN IMPORTA: reglas más específicas van primero para ganar sobre las
# terminales genéricas del final.

_REGLAS_RAW: list[tuple[str, str, Optional[str]]] = [
    # ── Shisha (por delante para que "Hookah bar" no gane `bar_copas`) ───────
    (r"\b(shisha|hookah|narguil|narghile|nargile)\w*",     "shisha_lounge", "shisha"),
    (r"\b(night\s*club|discoteca|lounge\s*bar)\w*",        "shisha_lounge", "coctel_bar"),

    # ── Tatuajes (antes que "bar" por si aparece "tattoo & bar") ─────────────
    (r"\b(tattoo\s*removal|eliminaci[oó]n\s*(?:de\s*)?tatuaj)\w*",
                                                           "tatuajes", "eliminacion_laser"),
    (r"\b(piercing|body\s*piercing)\w*",                   "tatuajes", "piercing"),
    (r"\b(tattoo|tatuaj)\w*",                              "tatuajes", "tatuaje"),

    # ── Restauración, subsectores específicos ─────────────────────────────────
    (r"\b(coffee|caf[eé]|cafeter|espresso)\w*",            "restauracion", "cafeteria"),
    (r"\b(pizz)\w*",                                       "restauracion", "pizzeria"),
    (r"\b(tapas|pintxo|tapeo)\w*",                         "restauracion", "tapas"),
    (r"\b(sushi|ramen|japanese|chinese|asian|thai|korean|vietnamese|indian)\w*",
                                                           "restauracion", "asiatica"),
    (r"\b(burger|hamburgues)\w*",                          "restauracion", "hamburguesa"),
    (r"\b(fast\s*food|meal[_\s]*takeaway|takeaway|kebab|doner|fried\s*chicken)\w*",
                                                           "restauracion", "fast_food"),
    (r"\b(fine\s*dining|gourmet|michelin|haute\s*cuisine)\w*",
                                                           "restauracion", "fine_dining"),
    (r"\b(bakery|panader|patisser|pastel|bolleri)\w*",     "restauracion", "panaderia"),
    (r"\b(cocktail\s*bar|c[oó]ctel|cocteler|wine\s*bar|vineria|pub)\w*",
                                                           "restauracion", "bar_copas"),
    # Terminal: cualquier restaurante/bar no clasificado
    (r"\b(restaurant|bistro|brasserie|eatery|diner)\w*",   "restauracion", None),
    (r"\b(bar)\b",                                         "restauracion", "bar_copas"),

    # ── Moda (específicos antes que los terminales genéricos) ────────────────
    (r"\b(women'?s\s*clothing|ropa\s*mujer|womenswear)\w*",  "moda", "ropa_mujer"),
    (r"\b(men'?s\s*clothing|ropa\s*hombre|menswear)\w*",     "moda", "ropa_hombre"),
    (r"\b(vintage|second[_\s-]*hand|thrift)\w*",             "moda", "vintage"),
    (r"\b(shoe|zapater|footwear|sneaker)\w*",                "moda", "zapateria"),
    (r"\b(sportswear|sporting\s*goods|athletic|deportiv)\w*",
                                                             "moda", "deportiva"),
    (r"\bsports?\s+clothing\w*",                             "moda", "deportiva"),
    (r"\b(jewel|jewell|complemento|accessor|handbag|bolso)\w*",
                                                             "moda", "complementos"),
    (r"\bbag\s*shop\b",                                      "moda", "complementos"),
    # Terminal
    (r"\b(clothing|fashion|boutique|apparel|ropa|tienda\s*de\s*ropa)\w*",
                                                             "moda", None),

    # ── Estética ──────────────────────────────────────────────────────────────
    (r"\b(barber|corte\s*caballero)\w*",                  "estetica", "barberia"),
    (r"\b(nail|manicur|pedicur|u[nñ]as)\w*",              "estetica", "unas"),
    (r"\b(spa|wellness|massag|masaje)\w*",                "estetica", "spa"),
    (r"\b(eyebrow|lash|cejas?|pesta[nñ]a)\w*",            "estetica", "cejas_pestanas"),
    (r"\b(laser\s*hair|depilaci[oó]n\s*l[aá]ser)\w*",     "estetica", "depilacion_laser"),
    (r"\b(hair(?:dress|cut|\s*salon)?|peluquer[ií]a|sal[oó]n\s*de\s*belleza|beauty\s*salon|beauty\s*parlor|beauty)\w*",
                                                          "estetica", "peluqueria"),
    # Terminal
    (r"\b(cosmetic|aesthetic)\w*",                        "estetica", None),
]

# Compilamos una sola vez al importar.
_REGLAS: list[tuple[re.Pattern[str], str, Optional[str]]] = [
    (re.compile(pat, re.IGNORECASE), sector, sub)
    for pat, sector, sub in _REGLAS_RAW
]


# ─── API pública ──────────────────────────────────────────────────────────────

def clasificar(categoria: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """Devuelve `(sector, subsector)`. Ambos pueden ser None.

    Reglas:
      - Si la categoría es None/vacía → (None, None).
      - Primera regla que matchee → su par (sector, subsector).
      - Si ninguna regla matchea → (None, None) — no podemos mapear.

    El subsector puede ser None incluso con sector detectado: significa
    "restaurante genérico" o "tienda de ropa genérica" — utilizable para la
    fila macro de `competencia_por_local` pero no para el agregado por subsector.
    """
    if not categoria:
        return (None, None)
    texto = categoria.strip()
    if not texto:
        return (None, None)
    for patron, sector, sub in _REGLAS:
        if patron.search(texto):
            return (sector, sub)
    return (None, None)


def clasificar_con_fallback(categorias: Optional[list[str]],
                              categoria_principal: Optional[str] = None
                              ) -> tuple[Optional[str], Optional[str]]:
    """Como `clasificar` pero probando varias etiquetas. Útil cuando la fuente
    da una lista (gosom.categories, foursquare.categories) — probamos la
    principal primero y caemos a las secundarias hasta que algo matchee."""
    candidatas: list[str] = []
    if categoria_principal:
        candidatas.append(categoria_principal)
    if categorias:
        candidatas.extend(c for c in categorias if c)
    for c in candidatas:
        sector, sub = clasificar(c)
        if sector is not None:
            return (sector, sub)
    return (None, None)
