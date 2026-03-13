"""
pipelines/scraping/urls.py — Zonas de Barcelona para el pipeline de mercado inmobiliario.

Define los 10 distritos y 20 barrios comerciales clave con sus slugs de Idealista.
Otros portales (Fotocasa, Habitaclia) usan ciudad entera — la granularidad por barrio
la aporta el campo barrio/distrito que extraen sus parsers.
"""
from __future__ import annotations
from typing import Optional

# ── Slugs de Idealista para los 10 distritos de Barcelona ─────────────────────
DISTRITOS: dict[str, str] = {
    "Ciutat Vella":        "barcelona/ciudad-vieja/",
    "Eixample":            "barcelona/ensanche/",
    "Sants-Montjuïc":      "barcelona/sants-montjuic/",
    "Les Corts":           "barcelona/las-corts/",
    "Sarrià-Sant Gervasi": "barcelona/sarria-sant-gervasi/",
    "Gràcia":              "barcelona/gracia/",
    "Horta-Guinardó":      "barcelona/horta-guinardo/",
    "Nou Barris":          "barcelona/nou-barris/",
    "Sant Andreu":         "barcelona/sant-andreu/",
    "Sant Martí":          "barcelona/sant-marti/",
}

# Barrios con mayor actividad comercial (scraping más granular para Idealista)
BARRIOS_COMERCIALES: dict[str, str] = {
    "Eixample Esquerra":       "barcelona/ensanche/izquierda-del-ensanche/",
    "Eixample Dreta":          "barcelona/ensanche/derecha-del-ensanche/",
    "Sant Antoni":             "barcelona/ensanche/sant-antoni/",
    "Sagrada Família":         "barcelona/ensanche/sagrada-familia/",
    "Barri Gòtic":             "barcelona/ciudad-vieja/barrio-gotico/",
    "El Raval":                "barcelona/ciudad-vieja/el-raval/",
    "La Barceloneta":          "barcelona/ciudad-vieja/barceloneta/",
    "Sant Pere i la Ribera":   "barcelona/ciudad-vieja/sant-pere-santa-caterina-i-la-ribera/",
    "Vila de Gràcia":          "barcelona/gracia/vila-de-gracia/",
    "Sants":                   "barcelona/sants-montjuic/sants/",
    "Hostafrancs":             "barcelona/sants-montjuic/hostafrancs/",
    "El Poblenou":             "barcelona/sant-marti/el-poblenou/",
    "22@ (Parc i la Llacuna)": "barcelona/sant-marti/el-parc-i-la-llacuna-del-poblenou/",
    "El Clot":                 "barcelona/sant-marti/el-clot/",
    "Sant Gervasi":            "barcelona/sarria-sant-gervasi/sant-gervasi-la-bonanova/",
    "Sant Andreu (barri)":     "barcelona/sant-andreu/sant-andreu/",
}

# Mapa slug → nombre canónico (para normalizar barrio/distrito en el parser)
SLUG_A_NOMBRE: dict[str, str] = {
    "ciudad-vieja":                            "Ciutat Vella",
    "ensanche":                                "Eixample",
    "sants-montjuic":                          "Sants-Montjuïc",
    "las-corts":                               "Les Corts",
    "sarria-sant-gervasi":                     "Sarrià-Sant Gervasi",
    "gracia":                                  "Gràcia",
    "horta-guinardo":                          "Horta-Guinardó",
    "nou-barris":                              "Nou Barris",
    "sant-andreu":                             "Sant Andreu",
    "sant-marti":                              "Sant Martí",
    "barrio-gotico":                           "Barri Gòtic",
    "el-raval":                                "El Raval",
    "barceloneta":                             "La Barceloneta",
    "sant-pere-santa-caterina-i-la-ribera":    "Sant Pere i la Ribera",
    "izquierda-del-ensanche":                  "Eixample Esquerra",
    "derecha-del-ensanche":                    "Eixample Dreta",
    "sant-antoni":                             "Sant Antoni",
    "sagrada-familia":                         "Sagrada Família",
    "vila-de-gracia":                          "Vila de Gràcia",
    "sants":                                   "Sants",
    "hostafrancs":                             "Hostafrancs",
    "el-poblenou":                             "El Poblenou",
    "el-parc-i-la-llacuna-del-poblenou":       "22@ (Parc i la Llacuna)",
    "el-clot":                                 "El Clot",
    "sant-gervasi-la-bonanova":                "Sant Gervasi",
}


def normalizar_zona(url: str) -> tuple[Optional[str], Optional[str]]:
    """
    Extrae barrio y distrito canónicos desde una URL de Idealista.
    Devuelve (barrio, distrito) — cualquiera puede ser None.
    """
    barrio = None
    distrito = None
    for slug, nombre in SLUG_A_NOMBRE.items():
        if slug in url:
            if nombre in DISTRITOS:
                distrito = nombre
            else:
                barrio = nombre
    return barrio, distrito


def urls_idealista_por_zona(
    tipo: str = "alquiler-locales",
    zonas: Optional[list[str]] = None,
    granular: bool = False,
) -> list[tuple[str, str]]:
    """
    Devuelve lista de (nombre_zona, url_base) para Idealista BCN.

    Args:
        tipo:     "alquiler-locales" | "venta-locales" | "alquiler-viviendas"
        zonas:    lista de nombres. None = todos los distritos.
        granular: True = incluir barrios comerciales además de distritos.

    Returns:
        lista de (nombre, url) lista para pasar a IdealistaScraper.
    """
    fuente = {**DISTRITOS}
    if granular:
        fuente.update(BARRIOS_COMERCIALES)

    if zonas:
        fuente = {k: v for k, v in fuente.items() if k in zonas}

    base = "https://www.idealista.com"
    tipo_path = {
        "alquiler-locales":  "alquiler-locales",
        "venta-locales":     "venta-locales",
        "alquiler-viviendas": "alquiler-viviendas",
    }.get(tipo, "alquiler-locales")

    return [
        (nombre, f"{base}/{tipo_path}/{slug}")
        for nombre, slug in fuente.items()
    ]
