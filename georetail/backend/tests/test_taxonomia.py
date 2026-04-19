"""
tests/test_taxonomia.py — Reglas deterministas de scoring/taxonomia.

Cubre:
  - `clasificar` devuelve `(sector, subsector)` correcto para categorías
    típicas que Google Maps / gosom / Foursquare devuelven.
  - Casos ambigüos (texto genérico) caen a `(sector, None)` — terminales.
  - Categoría desconocida → `(None, None)`.
  - Mayúsculas, tildes, puntuación no rompen el matching.
  - `clasificar_con_fallback` prueba varias candidatas hasta acertar.
  - `subsector_valido` respeta la taxonomía cerrada.
"""
from __future__ import annotations

import pytest

from scoring.taxonomia import (
    SECTORES,
    SUBSECTORES,
    clasificar,
    clasificar_con_fallback,
    subsector_valido,
)


# ─── Restauración ─────────────────────────────────────────────────────────────

class TestRestauracion:
    @pytest.mark.parametrize("entrada,esperado", [
        ("Coffee shop",                ("restauracion", "cafeteria")),
        ("Café de especialidad",       ("restauracion", "cafeteria")),
        ("Specialty coffee roaster",   ("restauracion", "cafeteria")),
        ("Pizzeria",                   ("restauracion", "pizzeria")),
        ("Italian Pizza Restaurant",   ("restauracion", "pizzeria")),
        ("Bar de tapas",               ("restauracion", "tapas")),
        ("Tapas restaurant",           ("restauracion", "tapas")),
        ("Sushi bar",                  ("restauracion", "asiatica")),
        ("Ramen shop",                 ("restauracion", "asiatica")),
        ("Japanese restaurant",        ("restauracion", "asiatica")),
        ("Thai restaurant",            ("restauracion", "asiatica")),
        ("Burger joint",               ("restauracion", "hamburguesa")),
        ("Hamburguesería",             ("restauracion", "hamburguesa")),
        ("Kebab shop",                 ("restauracion", "fast_food")),
        ("Meal takeaway",              ("restauracion", "fast_food")),
        ("Michelin fine dining",       ("restauracion", "fine_dining")),
        ("Bakery",                     ("restauracion", "panaderia")),
        ("Panadería artesanal",        ("restauracion", "panaderia")),
        ("Pastelería",                 ("restauracion", "panaderia")),
        ("Cocktail bar",               ("restauracion", "bar_copas")),
        ("Wine bar",                   ("restauracion", "bar_copas")),
        ("Restaurant",                 ("restauracion", None)),  # terminal
        ("Bistro",                     ("restauracion", None)),
        ("Bar",                        ("restauracion", "bar_copas")),
    ])
    def test_categorias_restauracion(self, entrada, esperado):
        assert clasificar(entrada) == esperado


# ─── Moda ─────────────────────────────────────────────────────────────────────

class TestModa:
    @pytest.mark.parametrize("entrada,esperado", [
        ("Women's clothing",           ("moda", "ropa_mujer")),
        ("Men's clothing store",       ("moda", "ropa_hombre")),
        ("Shoe store",                 ("moda", "zapateria")),
        ("Vintage clothing",           ("moda", "vintage")),
        ("Sports clothing",            ("moda", "deportiva")),
        ("Jewelry store",              ("moda", "complementos")),
        ("Handbag shop",               ("moda", "complementos")),
        ("Clothing store",             ("moda", None)),  # terminal genérico
        ("Fashion boutique",           ("moda", None)),
        ("Tienda de ropa",             ("moda", None)),
    ])
    def test_categorias_moda(self, entrada, esperado):
        assert clasificar(entrada) == esperado


# ─── Estética ─────────────────────────────────────────────────────────────────

class TestEstetica:
    @pytest.mark.parametrize("entrada,esperado", [
        ("Barber shop",                ("estetica", "barberia")),
        ("Peluquería caballero",       ("estetica", "peluqueria")),
        ("Hair salon",                 ("estetica", "peluqueria")),
        ("Beauty salon",               ("estetica", "peluqueria")),
        ("Nail salon",                 ("estetica", "unas")),
        ("Manicure & pedicure",        ("estetica", "unas")),
        ("Spa",                        ("estetica", "spa")),
        ("Wellness center",            ("estetica", "spa")),
        ("Eyebrow threading",          ("estetica", "cejas_pestanas")),
        ("Lash extensions",            ("estetica", "cejas_pestanas")),
        ("Laser hair removal",         ("estetica", "depilacion_laser")),
    ])
    def test_categorias_estetica(self, entrada, esperado):
        assert clasificar(entrada) == esperado


# ─── Tatuajes y Shisha ────────────────────────────────────────────────────────

class TestOtrosSectores:
    @pytest.mark.parametrize("entrada,esperado", [
        ("Tattoo shop",                ("tatuajes", "tatuaje")),
        ("Estudio de tatuajes",        ("tatuajes", "tatuaje")),
        ("Body piercing",              ("tatuajes", "piercing")),
        ("Tattoo removal",             ("tatuajes", "eliminacion_laser")),
        ("Shisha lounge",              ("shisha_lounge", "shisha")),
        ("Hookah bar",                 ("shisha_lounge", "shisha")),
    ])
    def test_categorias(self, entrada, esperado):
        assert clasificar(entrada) == esperado


# ─── Casos límite ─────────────────────────────────────────────────────────────

class TestLimites:
    def test_none_devuelve_none_none(self):
        assert clasificar(None) == (None, None)

    def test_vacio_devuelve_none_none(self):
        assert clasificar("") == (None, None)
        assert clasificar("   ") == (None, None)

    def test_categoria_desconocida(self):
        assert clasificar("Car repair") == (None, None)
        assert clasificar("Dentist") == (None, None)
        assert clasificar("Museum") == (None, None)

    def test_case_insensitive(self):
        assert clasificar("PIZZERIA") == ("restauracion", "pizzeria")
        assert clasificar("pizzeria") == ("restauracion", "pizzeria")
        assert clasificar("Pizzeria") == ("restauracion", "pizzeria")

    def test_extras_en_texto_no_rompen(self):
        # Gosom devuelve a veces paréntesis con variantes
        assert clasificar("Coffee shop (Italian)") == ("restauracion", "cafeteria")
        assert clasificar("Restaurant · Italian · €€") == ("restauracion", None)


# ─── clasificar_con_fallback ──────────────────────────────────────────────────

class TestFallback:
    def test_principal_gana_si_matchea(self):
        r = clasificar_con_fallback(["Fast food"], "Coffee shop")
        # Probamos la principal primero → cafeteria
        assert r == ("restauracion", "cafeteria")

    def test_secundaria_si_principal_no_matchea(self):
        r = clasificar_con_fallback(["Pizzeria"], "Car repair")
        assert r == ("restauracion", "pizzeria")

    def test_ninguna_matchea(self):
        r = clasificar_con_fallback(["Car repair", "Dentist"], "Unknown")
        assert r == (None, None)

    def test_lista_none_y_principal_none(self):
        assert clasificar_con_fallback(None, None) == (None, None)

    def test_solo_principal(self):
        assert clasificar_con_fallback(None, "Barber shop") == ("estetica", "barberia")

    def test_solo_lista(self):
        assert clasificar_con_fallback(["Tattoo shop"], None) == ("tatuajes", "tatuaje")


# ─── Invariantes de la taxonomía ──────────────────────────────────────────────

class TestTaxonomiaInvariantes:
    def test_sectores_unicos(self):
        assert len(SECTORES) == len(set(SECTORES))

    def test_subsectores_unicos_dentro_de_cada_sector(self):
        for sector, subs in SUBSECTORES.items():
            assert len(subs) == len(set(subs)), f"Duplicados en {sector}"

    def test_todas_las_reglas_apuntan_a_un_sector_valido(self):
        # Basta con que clasificar() nunca devuelva un sector fuera de SECTORES
        for entrada in ["Coffee shop", "Pizzeria", "Barber shop",
                        "Shoe store", "Tattoo shop", "Shisha lounge"]:
            sector, sub = clasificar(entrada)
            assert sector in SECTORES
            assert subsector_valido(sector, sub)

    def test_subsector_invalido(self):
        assert subsector_valido("restauracion", "pizzeria") is True
        assert subsector_valido("restauracion", "no_existe") is False
        # None siempre es válido — significa "macro detectado, sub no mapeable"
        assert subsector_valido("restauracion", None) is True
        # Sector inexistente con None también es válido (no hay nada que validar)
        assert subsector_valido("no_existe", None) is True

    def test_max_cardinalidad(self):
        # Decisión de diseño: no más de 30 subsectores totales para no
        # fragmentar el dataset histórico del XGBoost.
        total = sum(len(subs) for subs in SUBSECTORES.values())
        assert total <= 30, f"Demasiados subsectores: {total}"
