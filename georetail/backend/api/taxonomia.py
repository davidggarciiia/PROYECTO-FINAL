"""api/taxonomia.py — GET /api/taxonomia

Expone la taxonomía cerrada sector/subsector de scoring/taxonomia.py.
Es la única fuente de verdad para subsectores que alimentan el clasificador
de competidores y el modelo XGBoost. Cardinal fija: añadir subsectores aquí
requiere también reentrenar el modelo.

Consumidores:
  - Frontend QuickQuestionnaire → selector de subsector del usuario
  - Backend buscar.py           → validación de subsector entrante
"""
from __future__ import annotations

from fastapi import APIRouter, Response

from scoring.taxonomia import SUBSECTORES

router = APIRouter(tags=["taxonomia"])

# Etiquetas legibles en español para cada código de subsector.
_SUBSECTOR_LABELS: dict[str, str] = {
    # restauracion
    "cafeteria":        "Cafetería",
    "pizzeria":         "Pizzería",
    "tapas":            "Bar de tapas / pintxos",
    "asiatica":         "Restaurante asiático",
    "hamburguesa":      "Hamburguesería",
    "fast_food":        "Fast food / comida rápida",
    "fine_dining":      "Alta cocina / fine dining",
    "panaderia":        "Panadería / pastelería",
    "bar_copas":        "Bar de copas / coctelería",
    # moda
    "ropa_mujer":       "Ropa mujer",
    "ropa_hombre":      "Ropa hombre",
    "vintage":          "Tienda vintage / segunda mano",
    "zapateria":        "Zapatería",
    "complementos":     "Complementos y accesorios",
    "deportiva":        "Moda deportiva",
    # estetica
    "peluqueria":       "Peluquería",
    "unas":             "Estudio de uñas",
    "spa":              "Spa / masajes",
    "cejas_pestanas":   "Cejas y pestañas",
    "depilacion_laser": "Depilación láser",
    "barberia":         "Barbería",
    # tatuajes
    "tatuaje":          "Estudio de tatuajes",
    "piercing":         "Piercing",
    "eliminacion_laser":"Eliminación láser de tatuajes",
    # shisha_lounge
    "shisha":           "Shisha lounge",
    "coctel_bar":       "Coctelería / bar nocturno",
}


@router.get(
    "/taxonomia",
    summary="Taxonomía cerrada sector/subsector (fuente de verdad para clasificación)",
)
async def get_taxonomia(response: Response) -> dict[str, list[dict]]:
    """Devuelve los sectores y sus subsectores canónicos tal como los usa
    el clasificador de competidores y las features del modelo XGBoost.

    Estructura de respuesta:
        {
          "restauracion": [
            {"codigo": "cafeteria", "label": "Cafetería"},
            ...
          ],
          ...
        }
    """
    response.headers["Cache-Control"] = "public, max-age=3600"
    return {
        sector: [
            {
                "codigo": sub,
                "label": _SUBSECTOR_LABELS.get(sub, sub.replace("_", " ").capitalize()),
            }
            for sub in subsectores
        ]
        for sector, subsectores in SUBSECTORES.items()
    }
